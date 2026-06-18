//! Lock-free multi-key config cache backed by [`ArcSwap`]`<HashMap>`.
//!
//! Keyed by `(namespace, name)`.  Reads are fully lock-free (atomic pointer
//! load via `arc_swap`).  The 1-2 writers serialise on a `Mutex<()>` that is
//! never held during reads.
//!
//! [`ArcSwap`]: arc_swap::ArcSwap

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;

use crate::cache_key::{BorrowedKey, KeyRef, OwnedKey};
use crate::types::ConfigSnapshot;

// ── ConfigCache ───────────────────────────────────────────────────────────────

type Inner = HashMap<OwnedKey, Arc<ConfigSnapshot>>;

/// Initial capacity for the inner [`HashMap`].  Typical 10–100 configs per
/// namespace × ~10–50 namespaces ⇒ 128 is the next power of two that
/// covers the common single-namespace pod and amortises rehashes on
/// multi-namespace pods.  Capacity propagates across the per-write
/// `.clone()` so this single pre-size avoids `RawTable::reserve_rehash`
/// on early Apply events (CU-86aj37pwx).
pub const INITIAL_CAPACITY: usize = 128;

/// Shared, lock-free multi-key cache for [`ConfigSnapshot`].
///
/// Keyed by `(namespace, name)`.  Reads pay only an atomic pointer load;
/// writes clone the current map, mutate the clone, then swap the pointer.
pub struct ConfigCache {
    inner: ArcSwap<Inner>,
    /// Serialises the 1-2 concurrent writers — never held during reads.
    write_lock: Mutex<()>,
}

impl ConfigCache {
    /// Create a new empty cache.
    ///
    /// The `initial` parameter is accepted for backward-compatibility with
    /// call-sites that pass `ConfigSnapshot::default()`.  If the snapshot has
    /// non-empty `namespace` + `name`, it is pre-inserted; otherwise it is
    /// discarded (default snapshots have no key to insert under).
    pub fn new(initial: ConfigSnapshot) -> Self {
        let mut map = Inner::with_capacity(INITIAL_CAPACITY);
        if !initial.namespace.is_empty() && !initial.name.is_empty() {
            let key = OwnedKey::new(initial.namespace.clone(), initial.name.clone());
            map.insert(key, Arc::new(initial));
        }
        Self {
            inner: ArcSwap::from_pointee(map),
            write_lock: Mutex::new(()),
        }
    }

    /// Look up a snapshot by `(namespace, name)`.
    ///
    /// Returns `None` when no entry has been inserted for this key yet.
    /// Zero locking — atomic pointer load only.  Lookup is allocation-free:
    /// the `BorrowedKey` view passes `(&str, &str)` straight to the
    /// `HashMap` via the `Borrow<dyn KeyRef>` impl on [`OwnedKey`].
    ///
    /// OTEL child span (Phase 7, CU-86ahzwj3k): `level = "debug"` + `skip_all`
    /// so the lock-free read path pays nothing at the production INFO level
    /// (the span is never constructed unless a debug-level subscriber is
    /// active). Records `hit` (bool) only — no per-op heap alloc.
    #[tracing::instrument(level = "debug", name = "konfig.cache_get", skip_all, fields(hit))]
    pub fn get(&self, namespace: &str, name: &str) -> Option<Arc<ConfigSnapshot>> {
        let q = BorrowedKey::new(namespace, name);
        let found = self.inner.load().get(&q as &dyn KeyRef).map(Arc::clone);
        tracing::Span::current().record("hit", found.is_some());
        found
    }

    /// Insert or replace the entry for `snap.namespace` / `snap.name`.
    ///
    /// OTEL child span (Phase 7, CU-86ahzwj3k): `level = "debug"` + `skip_all`
    /// keeps the snapshot out of the span and off the INFO production path.
    #[tracing::instrument(level = "debug", name = "konfig.cache_update", skip_all)]
    pub fn update(&self, snap: ConfigSnapshot) {
        let _guard = crate::sync_util::lock_recovered(&self.write_lock);
        let current = self.inner.load();
        let mut next = (**current).clone();
        next.insert(
            OwnedKey::new(snap.namespace.clone(), snap.name.clone()),
            Arc::new(snap),
        );
        self.inner.store(Arc::new(next));
    }

    /// Remove the entry for `(namespace, name)` if present.
    pub fn remove(&self, namespace: &str, name: &str) {
        let _guard = crate::sync_util::lock_recovered(&self.write_lock);
        let current = self.inner.load();
        let mut next = (**current).clone();
        let q = BorrowedKey::new(namespace, name);
        next.remove(&q as &dyn KeyRef);
        self.inner.store(Arc::new(next));
    }

    /// Return all snapshots whose namespace matches `namespace`.
    /// Zero locking — atomic pointer load only.
    pub fn all_in_namespace(&self, namespace: &str) -> Vec<Arc<ConfigSnapshot>> {
        let guard = self.inner.load();
        let mut out = Vec::with_capacity(guard.len());
        for (k, v) in guard.iter() {
            if k.namespace == namespace {
                out.push(Arc::clone(v));
            }
        }
        out
    }

    /// Return `true` when the cache contains at least one entry.
    /// Zero locking — atomic pointer load only.
    pub fn is_populated(&self) -> bool {
        !self.inner.load().is_empty()
    }

    /// Mark all cached snapshots as stale (watcher lost K8s connection).
    ///
    /// Called by the watcher on stream error.  Each snapshot gets
    /// `stale_since = Some(now)`.  Next `cache.update(snap)` for a fresh
    /// Apply event will insert a snapshot with `stale_since = None`.
    pub fn mark_all_stale(&self) {
        let _guard = crate::sync_util::lock_recovered(&self.write_lock);
        let current = self.inner.load();
        let mut next = (**current).clone();
        let now = std::time::Instant::now();
        for v in next.values_mut() {
            let mut snap = (**v).clone();
            snap.stale_since = Some(now);
            *v = Arc::new(snap);
        }
        self.inner.store(Arc::new(next));
    }

    /// Return any one snapshot — useful for health-gate checks.
    /// Zero locking — atomic pointer load only.
    pub fn load_any(&self) -> Option<Arc<ConfigSnapshot>> {
        self.inner.load().values().next().cloned()
    }

    /// Backward-compat helper: returns any cached snapshot.
    ///
    /// Used by the health-gate in `main.rs` and the legacy single-entry
    /// watcher path.  Returns a default (empty) snapshot when the cache
    /// is unpopulated.
    pub fn load(&self) -> Arc<ConfigSnapshot> {
        self.load_any()
            .unwrap_or_else(|| Arc::new(ConfigSnapshot::default()))
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn snap(namespace: &str, name: &str, v: u32) -> ConfigSnapshot {
        ConfigSnapshot {
            namespace: namespace.to_string(),
            name: name.to_string(),
            schema_version: v,
            content: json!({"version": v}),
            resource_version: format!("rv-{v}"),
            ..Default::default()
        }
    }

    #[test]
    fn get_returns_inserted_entry() {
        let cache = ConfigCache::new(ConfigSnapshot::default());
        cache.update(snap("default", "cfg-a", 1));
        let entry = cache.get("default", "cfg-a").unwrap();
        assert_eq!(entry.schema_version, 1);
    }

    #[test]
    fn get_returns_none_for_missing_key() {
        let cache = ConfigCache::new(ConfigSnapshot::default());
        assert!(cache.get("default", "missing").is_none());
    }

    #[test]
    fn all_in_namespace_filters_correctly() {
        let cache = ConfigCache::new(ConfigSnapshot::default());
        cache.update(snap("ns-a", "cfg-1", 1));
        cache.update(snap("ns-a", "cfg-2", 2));
        cache.update(snap("ns-b", "cfg-3", 3));
        assert_eq!(cache.all_in_namespace("ns-a").len(), 2);
        assert_eq!(cache.all_in_namespace("ns-b").len(), 1);
    }

    #[test]
    fn remove_deletes_entry() {
        let cache = ConfigCache::new(ConfigSnapshot::default());
        cache.update(snap("default", "cfg-a", 1));
        cache.remove("default", "cfg-a");
        assert!(cache.get("default", "cfg-a").is_none());
    }

    #[test]
    fn is_populated_reflects_cache_state() {
        let cache = ConfigCache::new(ConfigSnapshot::default());
        assert!(!cache.is_populated());
        cache.update(snap("ns", "cfg", 1));
        assert!(cache.is_populated());
    }

    #[test]
    fn update_replaces_value() {
        let cache = ConfigCache::new(ConfigSnapshot::default());
        cache.update(snap("ns", "cfg", 1));
        cache.update(snap("ns", "cfg", 2));
        assert_eq!(cache.get("ns", "cfg").unwrap().schema_version, 2);
    }

    #[test]
    fn load_returns_default_when_empty() {
        let cache = ConfigCache::new(ConfigSnapshot::default());
        let loaded = cache.load();
        assert_eq!(loaded.schema_version, 0);
    }

    #[test]
    fn load_returns_any_entry() {
        let cache = ConfigCache::new(ConfigSnapshot::default());
        cache.update(snap("ns", "cfg", 42));
        let loaded = cache.load();
        assert_eq!(loaded.schema_version, 42);
    }

    #[test]
    fn mark_all_stale_sets_stale_since_on_all_entries() {
        let cache = ConfigCache::new(ConfigSnapshot::default());
        cache.update(snap("ns", "cfg-a", 1));
        cache.update(snap("ns", "cfg-b", 2));

        assert!(cache.get("ns", "cfg-a").unwrap().stale_since.is_none());
        assert!(cache.get("ns", "cfg-b").unwrap().stale_since.is_none());

        cache.mark_all_stale();

        assert!(cache.get("ns", "cfg-a").unwrap().stale_since.is_some());
        assert!(cache.get("ns", "cfg-b").unwrap().stale_since.is_some());
    }

    #[test]
    fn update_after_stale_clears_stale_since() {
        let cache = ConfigCache::new(ConfigSnapshot::default());
        cache.update(snap("ns", "cfg", 1));
        cache.mark_all_stale();
        assert!(cache.get("ns", "cfg").unwrap().stale_since.is_some());

        // A fresh Apply clears stale_since (new snapshot, stale_since = None).
        cache.update(snap("ns", "cfg", 2));
        assert!(cache.get("ns", "cfg").unwrap().stale_since.is_none());
    }

    #[test]
    fn concurrent_reads_see_consistent_version() {
        use std::sync::Arc;
        use std::thread;

        let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
        let cache_clone = Arc::clone(&cache);

        let writer = thread::spawn(move || {
            for v in 1u32..=50 {
                cache_clone.update(snap("ns", "cfg", v));
            }
        });

        let reader = thread::spawn({
            let cache = Arc::clone(&cache);
            move || {
                for _ in 0..1000 {
                    let _v = cache.load().schema_version;
                }
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();
        // Final value must be 50.
        assert_eq!(cache.get("ns", "cfg").unwrap().schema_version, 50);
    }
}
