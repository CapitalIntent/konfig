//! Lock-free multi-key config cache backed by [`DashMap`].
//!
//! Keyed by `"namespace\x00name"` (null-byte separator).  Using a flat
//! `String` key instead of `(String, String)` lets `DashMap::get` /
//! `DashMap::remove` accept a borrowed `&str` via the `Borrow<str>` impl on
//! `String` — zero heap allocation per lookup.
//!
//! [`DashMap`]: dashmap::DashMap

use std::sync::Arc;

use dashmap::DashMap;

use crate::types::ConfigSnapshot;

// ── Cache key helper ──────────────────────────────────────────────────────────

/// Build the internal flat key from `(namespace, name)`.
///
/// Uses `\x00` as separator.  ConfigMap namespaces and names follow RFC 1123
/// subdomain / label rules — neither can contain a null byte — so the
/// separator is unambiguous and no escaping is needed.
#[inline]
fn cache_key(namespace: &str, name: &str) -> String {
    let mut key = String::with_capacity(namespace.len() + 1 + name.len());
    key.push_str(namespace);
    key.push('\x00');
    key.push_str(name);
    key
}

/// Build the lookup key as a stack-allocated array so `DashMap::get` /
/// `DashMap::remove` can borrow a `&str` without a heap allocation.
///
/// Returns `(buf, len)` where `&buf[..len]` is the key string.  Callers that
/// need a longer key than `STACK_CAP` fall back to `cache_key`.
const STACK_CAP: usize = 256;

struct StackKey {
    buf: [u8; STACK_CAP],
    len: usize,
}

impl StackKey {
    fn new(namespace: &str, name: &str) -> Option<Self> {
        let total = namespace.len() + 1 + name.len();
        if total > STACK_CAP {
            return None;
        }
        let mut buf = [0u8; STACK_CAP];
        buf[..namespace.len()].copy_from_slice(namespace.as_bytes());
        buf[namespace.len()] = 0;
        buf[namespace.len() + 1..total].copy_from_slice(name.as_bytes());
        Some(Self { buf, len: total })
    }

    #[inline]
    fn as_str(&self) -> &str {
        // SAFETY: composed from valid UTF-8 slices + one ASCII byte.
        unsafe { std::str::from_utf8_unchecked(&self.buf[..self.len]) }
    }
}

// ── ConfigCache ───────────────────────────────────────────────────────────────

/// Shared, lock-free multi-key cache for [`ConfigSnapshot`].
///
/// Keyed by `"namespace\x00name"`.  Sharded by DashMap — concurrent reads
/// and writes do not block across different keys.
pub struct ConfigCache {
    inner: DashMap<String, Arc<ConfigSnapshot>>,
}

impl ConfigCache {
    /// Create a new empty cache.
    ///
    /// The `initial` parameter is accepted for backward-compatibility with
    /// call-sites that pass `ConfigSnapshot::default()`.  If the snapshot has
    /// non-empty `namespace` + `name`, it is pre-inserted; otherwise it is
    /// discarded (default snapshots have no key to insert under).
    pub fn new(initial: ConfigSnapshot) -> Self {
        let map = DashMap::new();
        if !initial.namespace.is_empty() && !initial.name.is_empty() {
            let key = cache_key(&initial.namespace, &initial.name);
            map.insert(key, Arc::new(initial));
        }
        Self { inner: map }
    }

    /// Look up a snapshot by `(namespace, name)`.
    ///
    /// Returns `None` when no entry has been inserted for this key yet.
    ///
    /// Zero heap allocation: uses a stack-allocated key buffer when
    /// `namespace.len() + 1 + name.len() <= 256`; heap-allocates only for
    /// unusually long names (rare in practice for K8s resources).
    pub fn get(&self, namespace: &str, name: &str) -> Option<Arc<ConfigSnapshot>> {
        if let Some(k) = StackKey::new(namespace, name) {
            self.inner.get(k.as_str()).map(|v| Arc::clone(&v))
        } else {
            self.inner
                .get(cache_key(namespace, name).as_str())
                .map(|v| Arc::clone(&v))
        }
    }

    /// Insert or replace the entry for `snap.namespace` / `snap.name`.
    pub fn update(&self, snap: ConfigSnapshot) {
        let key = cache_key(&snap.namespace, &snap.name);
        self.inner.insert(key, Arc::new(snap));
    }

    /// Remove the entry for `(namespace, name)` if present.
    ///
    /// Zero heap allocation: same stack-key optimisation as `get`.
    pub fn remove(&self, namespace: &str, name: &str) {
        if let Some(k) = StackKey::new(namespace, name) {
            self.inner.remove(k.as_str());
        } else {
            self.inner.remove(cache_key(namespace, name).as_str());
        }
    }

    /// Return all snapshots whose namespace matches `namespace`.
    pub fn all_in_namespace(&self, namespace: &str) -> Vec<Arc<ConfigSnapshot>> {
        self.inner
            .iter()
            .filter(|entry| {
                // Key is "namespace\x00name"; split at the first null byte.
                entry
                    .key()
                    .split_once('\x00')
                    .map_or(false, |(ns, _)| ns == namespace)
            })
            .map(|entry| Arc::clone(entry.value()))
            .collect()
    }

    /// Return `true` when the cache contains at least one entry.
    pub fn is_populated(&self) -> bool {
        !self.inner.is_empty()
    }

    /// Mark all cached snapshots as stale (watcher lost K8s connection).
    ///
    /// Called by the watcher on stream error.  Each snapshot gets
    /// `stale_since = Some(now)`.  Next `cache.update(snap)` for a fresh
    /// Apply event will insert a snapshot with `stale_since = None`.
    pub fn mark_all_stale(&self) {
        let now = std::time::Instant::now();
        let keys: Vec<String> = self.inner.iter().map(|e| e.key().clone()).collect();
        for key in keys {
            if let Some(arc) = self.inner.get(key.as_str()) {
                let mut snap = (**arc).clone();
                snap.stale_since = Some(now);
                drop(arc);
                self.inner.insert(key, Arc::new(snap));
            }
        }
    }

    /// Return any one snapshot — useful for health-gate checks.
    pub fn load_any(&self) -> Option<Arc<ConfigSnapshot>> {
        self.inner.iter().next().map(|e| Arc::clone(e.value()))
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
        let mut ns_a = cache.all_in_namespace("ns-a");
        ns_a.sort_by_key(|s| s.schema_version);
        assert_eq!(ns_a.len(), 2);
        assert_eq!(ns_a[0].schema_version, 1);
        assert_eq!(ns_a[1].schema_version, 2);
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

    // old_guard_survives_update is dropped since DashMap doesn't give ArcSwap
    // guard semantics; Arc::clone provides equivalent stability of owned ref.
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
