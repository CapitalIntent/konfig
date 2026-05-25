//! Lock-free multi-key config cache backed by [`DashMap`].
//!
//! Keyed by `(namespace, name)`.  Phase 2D upgrade from the single-entry
//! `ArcSwap<ConfigSnapshot>` to support multi-namespace, multi-config deployments.
//!
//! [`DashMap`]: dashmap::DashMap

use std::sync::Arc;

use dashmap::DashMap;

use crate::types::ConfigSnapshot;

// ── ConfigCache ───────────────────────────────────────────────────────────────

/// Shared, lock-free multi-key cache for [`ConfigSnapshot`].
///
/// Keyed by `(namespace, name)`.  Sharded by DashMap — concurrent reads and
/// writes do not block across different keys.
pub struct ConfigCache {
    inner: DashMap<(String, String), Arc<ConfigSnapshot>>,
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
            let key = (initial.namespace.clone(), initial.name.clone());
            map.insert(key, Arc::new(initial));
        }
        Self { inner: map }
    }

    /// Look up a snapshot by `(namespace, name)`.
    ///
    /// Returns `None` when no entry has been inserted for this key yet.
    pub fn get(&self, namespace: &str, name: &str) -> Option<Arc<ConfigSnapshot>> {
        self.inner.get(&(namespace.to_string(), name.to_string())).map(|v| Arc::clone(&v))
    }

    /// Insert or replace the entry for `snap.namespace` / `snap.name`.
    pub fn update(&self, snap: ConfigSnapshot) {
        let key = (snap.namespace.clone(), snap.name.clone());
        self.inner.insert(key, Arc::new(snap));
    }

    /// Remove the entry for `(namespace, name)` if present.
    pub fn remove(&self, namespace: &str, name: &str) {
        self.inner.remove(&(namespace.to_string(), name.to_string()));
    }

    /// Return all snapshots whose namespace matches `namespace`.
    pub fn all_in_namespace(&self, namespace: &str) -> Vec<Arc<ConfigSnapshot>> {
        self.inner
            .iter()
            .filter(|entry| entry.key().0 == namespace)
            .map(|entry| Arc::clone(entry.value()))
            .collect()
    }

    /// Return `true` when the cache contains at least one entry.
    pub fn is_populated(&self) -> bool {
        !self.inner.is_empty()
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
        self.load_any().unwrap_or_else(|| Arc::new(ConfigSnapshot::default()))
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
