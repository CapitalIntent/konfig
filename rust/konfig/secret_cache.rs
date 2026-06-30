//! Lock-free multi-key Secret cache.
//!
//! Thin domain wrapper over the generic [`CowCache`](crate::cow_cache::CowCache)
//! primitive — same pattern as [`ConfigCache`](crate::cache::ConfigCache) but
//! typed for [`SecretSnapshot`].  Reads are fully lock-free; the 1-2 writers
//! serialise on an internal `Mutex<()>` never held during reads.  The
//! copy-on-write mechanics live in [`crate::cow_cache`].

use std::sync::Arc;
use std::time::Instant;

use crate::cow_cache::{CacheEntry, CowCache, INITIAL_CAPACITY, Mutation};
use crate::types::SecretSnapshot;

/// Label value for this cache's Prometheus metrics.
const CACHE_KIND: &str = "secret";

/// A single staged mutation for [`SecretCache::apply_batch`].  Mirrors
/// [`ConfigMutation`](crate::cache::ConfigMutation).
pub type SecretMutation = Mutation<SecretSnapshot>;

impl CacheEntry for SecretSnapshot {
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

// ── SecretCache ─────────────────────────────────────────────────────────────

/// Shared, lock-free multi-key cache for [`SecretSnapshot`].
pub struct SecretCache {
    inner: CowCache<SecretSnapshot>,
}

impl SecretCache {
    pub fn new() -> Self {
        Self {
            inner: CowCache::with_capacity(CACHE_KIND, INITIAL_CAPACITY),
        }
    }

    /// Zero locking — atomic pointer load only.  Lookup is allocation-free
    /// via the `BorrowedKey` / `Borrow<dyn KeyRef>` trick.
    pub fn get(&self, namespace: &str, name: &str) -> Option<Arc<SecretSnapshot>> {
        self.inner.get(namespace, name)
    }

    pub fn update(&self, snap: SecretSnapshot) {
        self.inner.update(snap);
    }

    pub fn remove(&self, namespace: &str, name: &str) {
        self.inner.remove(namespace, name);
    }

    /// Zero locking — atomic pointer load only.
    pub fn all_in_namespace(&self, namespace: &str) -> Vec<Arc<SecretSnapshot>> {
        self.inner.all_in_namespace(namespace)
    }

    /// Return `true` when the cache holds at least one secret. Mirrors
    /// [`ConfigCache::is_populated`](crate::cache::ConfigCache::is_populated) —
    /// used as the SSE readiness gate (serve `503 Retry-After` until the secret
    /// watcher's first list has warmed the cache). Zero locking.
    pub fn is_populated(&self) -> bool {
        self.inner.is_populated()
    }

    /// Mark all cached snapshots as stale (secret watcher lost K8s connection).
    ///
    /// Called from the watcher's `on_disconnect` hook.  Each snapshot gets
    /// `stale_since = Some(now)`.  The next `update(snap)` for a fresh event
    /// inserts a snapshot with `stale_since = None`.  Mirrors
    /// [`ConfigCache::mark_all_stale`](crate::cache::ConfigCache::mark_all_stale).
    pub fn mark_all_stale(&self) {
        self.inner.mark_all_stale();
    }

    /// Apply many mutations under a SINGLE map clone + swap. See
    /// [`CowCache::apply_batch`](crate::cow_cache::CowCache::apply_batch).
    /// Empty input is a no-op. Returns the number of mutations applied.
    pub fn apply_batch(&self, mutations: impl IntoIterator<Item = SecretMutation>) -> usize {
        self.inner.apply_batch(mutations)
    }

    /// Number of write mutations (clone+swap operations) performed. A batch
    /// counts once. Used by tests.
    #[cfg(test)]
    pub(crate) fn write_count(&self) -> u64 {
        self.inner.write_count()
    }
}

impl Default for SecretCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_secret(namespace: &str, name: &str, schema_version: u32) -> SecretSnapshot {
        SecretSnapshot {
            name: name.to_string(),
            namespace: namespace.to_string(),
            schema_version,
            ..Default::default()
        }
    }

    #[test]
    fn get_returns_inserted_entry() {
        let cache = SecretCache::new();
        cache.update(make_secret("default", "my-secret", 2));
        let entry = cache.get("default", "my-secret").unwrap();
        assert_eq!(entry.schema_version, 2);
    }

    #[test]
    fn get_returns_none_for_missing_key() {
        let cache = SecretCache::new();
        assert!(cache.get("default", "missing").is_none());
    }

    #[test]
    fn remove_deletes_entry() {
        let cache = SecretCache::new();
        cache.update(make_secret("ns", "sec", 1));
        cache.remove("ns", "sec");
        assert!(cache.get("ns", "sec").is_none());
    }

    #[test]
    fn all_in_namespace_filters_correctly() {
        let cache = SecretCache::new();
        cache.update(make_secret("ns-a", "sec-1", 1));
        cache.update(make_secret("ns-a", "sec-2", 2));
        cache.update(make_secret("ns-b", "sec-3", 3));
        assert_eq!(cache.all_in_namespace("ns-a").len(), 2);
        assert_eq!(cache.all_in_namespace("ns-b").len(), 1);
    }

    #[test]
    fn update_replaces_existing_entry() {
        let cache = SecretCache::new();
        cache.update(make_secret("ns", "sec", 1));
        cache.update(make_secret("ns", "sec", 5));
        assert_eq!(cache.get("ns", "sec").unwrap().schema_version, 5);
    }

    #[test]
    fn mark_all_stale_sets_stale_since_on_all_entries() {
        let cache = SecretCache::new();
        cache.update(make_secret("ns", "sec-a", 1));
        cache.update(make_secret("ns", "sec-b", 2));

        assert!(cache.get("ns", "sec-a").unwrap().stale_since.is_none());
        assert!(cache.get("ns", "sec-b").unwrap().stale_since.is_none());

        cache.mark_all_stale();

        assert!(cache.get("ns", "sec-a").unwrap().stale_since.is_some());
        assert!(cache.get("ns", "sec-b").unwrap().stale_since.is_some());
    }

    #[test]
    fn update_after_stale_clears_stale_since() {
        let cache = SecretCache::new();
        cache.update(make_secret("ns", "sec", 1));
        cache.mark_all_stale();
        assert!(cache.get("ns", "sec").unwrap().stale_since.is_some());
        // A fresh event re-inserts with stale_since = None (Default).
        cache.update(make_secret("ns", "sec", 2));
        assert!(cache.get("ns", "sec").unwrap().stale_since.is_none());
    }

    #[test]
    fn apply_batch_applies_all_under_one_write() {
        let cache = SecretCache::new();
        let n = cache.apply_batch([
            SecretMutation::Upsert(make_secret("ns", "sec-a", 1)),
            SecretMutation::Upsert(make_secret("ns", "sec-b", 2)),
        ]);
        assert_eq!(n, 2);
        assert_eq!(cache.get("ns", "sec-a").unwrap().schema_version, 1);
        assert_eq!(cache.get("ns", "sec-b").unwrap().schema_version, 2);
        // Two upserts, exactly ONE clone+swap.
        assert_eq!(cache.write_count(), 1);
    }

    #[test]
    fn apply_batch_empty_is_noop() {
        let cache = SecretCache::new();
        let n = cache.apply_batch(std::iter::empty());
        assert_eq!(n, 0);
        assert_eq!(cache.write_count(), 0);
    }
}
