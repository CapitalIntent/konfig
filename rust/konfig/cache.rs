//! Lock-free multi-key config cache.
//!
//! Thin domain wrapper over the generic [`CowCache`](crate::cow_cache::CowCache)
//! primitive, keyed by `(namespace, name)`.  Reads are fully lock-free; the
//! 1-2 writers serialise on an internal `Mutex<()>` that is never held during
//! reads.  Config-specific bits (the seed-snapshot constructor, the
//! `is_populated` readiness gate, and the `load` default) live here; the
//! copy-on-write mechanics live in [`crate::cow_cache`].

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use crate::cache_key::OwnedKey;
use crate::cow_cache::{CacheEntry, CowCache, INITIAL_CAPACITY, Mutation};
use crate::types::ConfigSnapshot;

/// Label value for this cache's Prometheus metrics.
const CACHE_KIND: &str = "config";

/// A single staged mutation for [`ConfigCache::apply_batch`].
pub type ConfigMutation = Mutation<ConfigSnapshot>;

impl CacheEntry for ConfigSnapshot {
    fn namespace(&self) -> &str {
        &self.namespace
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn set_stale(&mut self, now: Instant) {
        self.stale_since = Some(now);
    }
}

// ── ConfigCache ───────────────────────────────────────────────────────────────

/// Shared, lock-free multi-key cache for [`ConfigSnapshot`].
///
/// Keyed by `(namespace, name)`.  Reads pay only an atomic pointer load;
/// writes clone the current map, mutate the clone, then swap the pointer.
pub struct ConfigCache {
    inner: CowCache<ConfigSnapshot>,
}

impl ConfigCache {
    /// Create a new empty cache.
    ///
    /// The `initial` parameter is accepted for backward-compatibility with
    /// call-sites that pass `ConfigSnapshot::default()`.  If the snapshot has
    /// non-empty `namespace` + `name`, it is pre-inserted; otherwise it is
    /// discarded (default snapshots have no key to insert under).
    pub fn new(initial: ConfigSnapshot) -> Self {
        let mut map = HashMap::with_capacity(INITIAL_CAPACITY);
        if !initial.namespace.is_empty() && !initial.name.is_empty() {
            let key = OwnedKey::new(initial.namespace.clone(), initial.name.clone());
            map.insert(key, Arc::new(initial));
        }
        Self {
            inner: CowCache::from_map(CACHE_KIND, map),
        }
    }

    /// Look up a snapshot by `(namespace, name)`.
    ///
    /// Returns `None` when no entry has been inserted for this key yet.
    /// Zero locking — atomic pointer load only.
    ///
    /// OTEL child span (Phase 7, CU-86ahzwj3k): `level = "debug"` + `skip_all`
    /// so the lock-free read path pays nothing at the production INFO level.
    /// Records `hit` (bool) only — no per-op heap alloc.
    #[tracing::instrument(level = "debug", name = "konfig.cache_get", skip_all, fields(hit))]
    pub fn get(&self, namespace: &str, name: &str) -> Option<Arc<ConfigSnapshot>> {
        let found = self.inner.get(namespace, name);
        tracing::Span::current().record("hit", found.is_some());
        found
    }

    /// Insert or replace the entry for `snap.namespace` / `snap.name`.
    ///
    /// OTEL child span (Phase 7, CU-86ahzwj3k): `level = "debug"` + `skip_all`
    /// keeps the snapshot out of the span and off the INFO production path.
    #[tracing::instrument(level = "debug", name = "konfig.cache_update", skip_all)]
    pub fn update(&self, snap: ConfigSnapshot) {
        self.inner.update(snap);
    }

    /// Remove the entry for `(namespace, name)` if present.
    pub fn remove(&self, namespace: &str, name: &str) {
        self.inner.remove(namespace, name);
    }

    /// Return all snapshots whose namespace matches `namespace`.
    /// Zero locking — atomic pointer load only.
    pub fn all_in_namespace(&self, namespace: &str) -> Vec<Arc<ConfigSnapshot>> {
        self.inner.all_in_namespace(namespace)
    }

    /// Return `true` when the cache contains at least one entry.
    /// Zero locking — atomic pointer load only.
    pub fn is_populated(&self) -> bool {
        self.inner.is_populated()
    }

    /// Mark all cached snapshots as stale (watcher lost K8s connection).
    ///
    /// Called by the watcher on stream error.  Each snapshot gets
    /// `stale_since = Some(now)`.  Next `cache.update(snap)` for a fresh
    /// Apply event will insert a snapshot with `stale_since = None`.
    pub fn mark_all_stale(&self) {
        self.inner.mark_all_stale();
    }

    /// Return any one snapshot — useful for health-gate checks.
    /// Zero locking — atomic pointer load only.
    pub fn load_any(&self) -> Option<Arc<ConfigSnapshot>> {
        self.inner.load_any()
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

    /// Apply many mutations under a SINGLE map clone + swap.
    ///
    /// See [`CowCache::apply_batch`](crate::cow_cache::CowCache::apply_batch).
    /// Empty input is a no-op. Returns the number of mutations applied.
    pub fn apply_batch(&self, mutations: impl IntoIterator<Item = ConfigMutation>) -> usize {
        self.inner.apply_batch(mutations)
    }

    /// Number of write mutations (clone+swap operations) this cache has
    /// performed. A batch counts once. Used by the watcher relist tests and
    /// the burst benchmark.
    #[cfg(test)]
    pub(crate) fn write_count(&self) -> u64 {
        self.inner.write_count()
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

    #[test]
    fn apply_batch_applies_all_under_one_write() {
        let cache = ConfigCache::new(ConfigSnapshot::default());
        let n = cache.apply_batch([
            ConfigMutation::Upsert(snap("ns", "cfg-a", 1)),
            ConfigMutation::Upsert(snap("ns", "cfg-b", 2)),
            ConfigMutation::Upsert(snap("ns", "cfg-c", 3)),
        ]);
        assert_eq!(n, 3);
        assert_eq!(cache.get("ns", "cfg-a").unwrap().schema_version, 1);
        assert_eq!(cache.get("ns", "cfg-b").unwrap().schema_version, 2);
        assert_eq!(cache.get("ns", "cfg-c").unwrap().schema_version, 3);
        // Three upserts, exactly ONE clone+swap — the whole point.
        assert_eq!(cache.write_count(), 1);
    }

    #[test]
    fn apply_batch_empty_is_noop() {
        let cache = ConfigCache::new(ConfigSnapshot::default());
        let n = cache.apply_batch(std::iter::empty());
        assert_eq!(n, 0);
        assert!(!cache.is_populated());
        // Empty batch must not clone/swap or bump the write counter.
        assert_eq!(cache.write_count(), 0);
    }

    #[test]
    fn apply_batch_mixes_upsert_and_remove() {
        let cache = ConfigCache::new(ConfigSnapshot::default());
        cache.update(snap("ns", "old", 9));
        let base = cache.write_count();
        cache.apply_batch([
            ConfigMutation::Upsert(snap("ns", "new", 1)),
            ConfigMutation::Remove {
                namespace: "ns".to_string(),
                name: "old".to_string(),
            },
        ]);
        assert_eq!(cache.get("ns", "new").unwrap().schema_version, 1);
        assert!(cache.get("ns", "old").is_none());
        assert_eq!(cache.write_count(), base + 1);
    }

    /// Burst benchmark (CU-86aj7k61x): measures the copy-on-write clone churn
    /// of N per-event `update` calls versus a single `apply_batch` of the same
    /// N events, at a realistic watch-restart scale. Asserts the deterministic
    /// clone-count property (N swaps vs 1) and prints wall-clock for evidence.
    /// Run with `--nocapture` to see the timings; no timing assertion (avoids
    /// CI flake) — the swap-count assert is the real regression guard.
    #[test]
    fn bench_burst_individual_vs_batch() {
        use std::time::Instant;
        const NAMESPACES: usize = 50;
        const PER_NS: usize = 40; // 50 * 40 = 2000 configs — realistic pod fan-out

        let mk = || {
            let mut v = Vec::with_capacity(NAMESPACES * PER_NS);
            for ns in 0..NAMESPACES {
                for c in 0..PER_NS {
                    v.push(snap(&format!("ns-{ns}"), &format!("cfg-{c}"), 1));
                }
            }
            v
        };

        // Path A: one `update` per event — one whole-map clone each.
        let individual = ConfigCache::new(ConfigSnapshot::default());
        let t0 = Instant::now();
        for s in mk() {
            individual.update(s);
        }
        let dt_individual = t0.elapsed();

        // Path B: one `apply_batch` for the whole burst — a single clone.
        let batched = ConfigCache::new(ConfigSnapshot::default());
        let t1 = Instant::now();
        batched.apply_batch(mk().into_iter().map(ConfigMutation::Upsert));
        let dt_batch = t1.elapsed();

        let total = NAMESPACES * PER_NS;
        assert_eq!(individual.all_in_namespace("ns-0").len(), PER_NS);
        assert_eq!(batched.all_in_namespace("ns-0").len(), PER_NS);
        assert_eq!(individual.write_count(), total as u64);
        assert_eq!(batched.write_count(), 1);

        println!(
            "cache burst bench: {total} events | individual {} swaps in {:?} | batch 1 swap in {:?}",
            individual.write_count(),
            dt_individual,
            dt_batch,
        );
    }
}
