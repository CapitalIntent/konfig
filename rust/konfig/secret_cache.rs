//! Lock-free multi-key Secret cache backed by [`ArcSwap`]`<HashMap>`.
//!
//! Same pattern as [`ConfigCache`] but typed for [`SecretSnapshot`].
//! Reads are fully lock-free (atomic pointer load via `arc_swap`).
//! The 1-2 writers serialise on a `Mutex<()>` that is never held during reads.
//!
//! [`ConfigCache`]: crate::cache::ConfigCache
//! [`ArcSwap`]: arc_swap::ArcSwap

use std::collections::HashMap;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;

use crate::cache_key::{BorrowedKey, KeyRef, OwnedKey};
use crate::metrics::{CACHE_ENTRIES, CACHE_WRITE_MUTATIONS_TOTAL};
use crate::types::SecretSnapshot;

type Inner = HashMap<OwnedKey, Arc<SecretSnapshot>>;

/// Label value for this cache's Prometheus metrics.
const CACHE_KIND: &str = "secret";

/// A single staged mutation for [`SecretCache::apply_batch`].
///
/// Mirrors [`ConfigMutation`](crate::cache::ConfigMutation) so the
/// secret watch-restart relist applies many events under one map clone
/// (CU-86aj7k61x).
pub enum SecretMutation {
    /// Insert or replace the entry for `snap.namespace`/`snap.name`.
    Upsert(SecretSnapshot),
    /// Remove the `(namespace, name)` entry if present.
    Remove { namespace: String, name: String },
}

/// Initial capacity for the inner [`HashMap`].  Typical 10–100 secrets per
/// namespace × ~10–50 namespaces ⇒ 128 is the next power of two that
/// covers the common single-namespace pod and amortises rehashes on
/// multi-namespace pods.  Capacity propagates across the per-write
/// `.clone()` so this single pre-size avoids `RawTable::reserve_rehash`
/// on early Apply events (CU-86aj37pwx).
pub const INITIAL_CAPACITY: usize = 128;

pub struct SecretCache {
    inner: ArcSwap<Inner>,
    /// Serialises the 1-2 concurrent writers — never held during reads.
    write_lock: Mutex<()>,
    /// Count of write mutations (clone+swap operations). A batch counts once.
    #[cfg(test)]
    writes: AtomicU64,
}

impl SecretCache {
    pub fn new() -> Self {
        CACHE_ENTRIES.with_label_values(&[CACHE_KIND]).set(0.0);
        Self {
            inner: ArcSwap::from_pointee(Inner::with_capacity(INITIAL_CAPACITY)),
            write_lock: Mutex::new(()),
            #[cfg(test)]
            writes: AtomicU64::new(0),
        }
    }

    /// Zero locking — atomic pointer load only.  Lookup is allocation-free
    /// via the `BorrowedKey` / `Borrow<dyn KeyRef>` trick.
    pub fn get(&self, namespace: &str, name: &str) -> Option<Arc<SecretSnapshot>> {
        let q = BorrowedKey::new(namespace, name);
        self.inner.load().get(&q as &dyn KeyRef).map(Arc::clone)
    }

    pub fn update(&self, snap: SecretSnapshot) {
        let _guard = crate::sync_util::lock_recovered(&self.write_lock);
        let current = self.inner.load();
        let mut next = (**current).clone();
        next.insert(
            OwnedKey::new(snap.namespace.clone(), snap.name.clone()),
            Arc::new(snap),
        );
        let len = next.len();
        self.inner.store(Arc::new(next));
        self.record_write(len);
    }

    pub fn remove(&self, namespace: &str, name: &str) {
        let _guard = crate::sync_util::lock_recovered(&self.write_lock);
        let current = self.inner.load();
        let mut next = (**current).clone();
        let q = BorrowedKey::new(namespace, name);
        next.remove(&q as &dyn KeyRef);
        let len = next.len();
        self.inner.store(Arc::new(next));
        self.record_write(len);
    }

    /// Zero locking — atomic pointer load only.
    pub fn all_in_namespace(&self, namespace: &str) -> Vec<Arc<SecretSnapshot>> {
        let guard = self.inner.load();
        let mut out = Vec::with_capacity(guard.len());
        for (k, v) in guard.iter() {
            if k.namespace == namespace {
                out.push(Arc::clone(v));
            }
        }
        out
    }

    /// Mark all cached snapshots as stale (secret watcher lost K8s connection).
    ///
    /// Called from the watcher's `on_disconnect` hook.  Each snapshot gets
    /// `stale_since = Some(now)`.  The next `update(snap)` for a fresh event
    /// inserts a snapshot with `stale_since = None`.  Mirrors
    /// [`ConfigCache::mark_all_stale`](crate::cache::ConfigCache::mark_all_stale).
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
        let len = next.len();
        self.inner.store(Arc::new(next));
        self.record_write(len);
    }

    /// Apply many mutations under a SINGLE map clone + swap. See
    /// [`ConfigCache::apply_batch`](crate::cache::ConfigCache::apply_batch).
    /// Empty input is a no-op. Returns the number of mutations applied.
    pub fn apply_batch(&self, mutations: impl IntoIterator<Item = SecretMutation>) -> usize {
        let mutations: Vec<SecretMutation> = mutations.into_iter().collect();
        if mutations.is_empty() {
            return 0;
        }
        let n = mutations.len();
        let _guard = crate::sync_util::lock_recovered(&self.write_lock);
        let current = self.inner.load();
        let mut next = (**current).clone();
        for m in mutations {
            match m {
                SecretMutation::Upsert(snap) => {
                    next.insert(
                        OwnedKey::new(snap.namespace.clone(), snap.name.clone()),
                        Arc::new(snap),
                    );
                }
                SecretMutation::Remove { namespace, name } => {
                    let q = BorrowedKey::new(&namespace, &name);
                    next.remove(&q as &dyn KeyRef);
                }
            }
        }
        let len = next.len();
        self.inner.store(Arc::new(next));
        self.record_write(len);
        n
    }

    /// Number of write mutations (clone+swap operations) performed. A batch
    /// counts once. Deterministic per-instance counter used by tests.
    #[cfg(test)]
    #[cfg(test)]
    pub(crate) fn write_count(&self) -> u64 {
        self.writes.load(Ordering::Relaxed)
    }

    /// Record one write mutation: per-instance counter + clone-churn counter +
    /// entry-count gauge refresh.
    fn record_write(&self, new_len: usize) {
        #[cfg(test)]
        self.writes.fetch_add(1, Ordering::Relaxed);
        CACHE_WRITE_MUTATIONS_TOTAL
            .with_label_values(&[CACHE_KIND])
            .inc();
        CACHE_ENTRIES
            .with_label_values(&[CACHE_KIND])
            .set(new_len as f64);
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
