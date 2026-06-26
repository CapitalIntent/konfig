//! Generic copy-on-write multi-key cache primitive.
//!
//! Shared mechanics behind [`ConfigCache`](crate::cache::ConfigCache) and
//! [`SecretCache`](crate::secret_cache::SecretCache): a lock-free
//! [`ArcSwap`]`<HashMap>` keyed by `(namespace, name)`.  Reads are fully
//! lock-free (atomic pointer load).  The 1-2 writers serialise on a
//! `Mutex<()>` that is never held during reads; each write clones the current
//! map, mutates the clone, then swaps the pointer.
//!
//! The two domain caches differ only in their snapshot type and a few
//! domain-specific helpers (readiness gate, default value).  Everything in
//! this module is mechanics — extracted to kill the config<->secret copy-paste
//! and the drift risk that comes with it (CU-86aj7k6mr).
//!
//! [`ArcSwap`]: arc_swap::ArcSwap

use std::collections::HashMap;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arc_swap::ArcSwap;

use crate::cache_key::{BorrowedKey, KeyRef, OwnedKey};
use crate::metrics::{CACHE_ENTRIES, CACHE_WRITE_MUTATIONS_TOTAL};

/// Initial capacity for the inner [`HashMap`].  Typical 10-100 entries per
/// namespace x ~10-50 namespaces => 128 is the next power of two that covers
/// the common single-namespace pod and amortises rehashes on multi-namespace
/// pods.  Capacity propagates across the per-write `.clone()` so this single
/// pre-size avoids `RawTable::reserve_rehash` on early Apply events
/// (CU-86aj37pwx).
pub const INITIAL_CAPACITY: usize = 128;

/// A domain snapshot that can live in a [`CowCache`].
///
/// Implementors expose their `(namespace, name)` key and how to flag
/// themselves stale.  Both are pure accessors over existing fields — the
/// mechanics never reach into snapshot internals directly.
pub trait CacheEntry: Clone {
    /// The snapshot's namespace component of the cache key.
    fn namespace(&self) -> &str;
    /// The snapshot's name component of the cache key.
    fn name(&self) -> &str;
    /// Mark this snapshot stale as of `now` (watcher lost its K8s connection).
    fn set_stale(&mut self, now: Instant);
}

/// A single staged mutation for [`CowCache::apply_batch`].
///
/// Lets a watch-restart relist apply many events under one map clone instead
/// of one clone per event (CU-86aj7k61x).
pub enum Mutation<S> {
    /// Insert or replace the entry for `snap.namespace()`/`snap.name()`.
    Upsert(S),
    /// Remove the `(namespace, name)` entry if present.
    Remove {
        /// Namespace component of the key to remove.
        namespace: String,
        /// Name component of the key to remove.
        name: String,
    },
}

type Inner<S> = HashMap<OwnedKey, Arc<S>>;

/// Lock-free multi-key copy-on-write cache.
///
/// Reads pay only an atomic pointer load; writes clone the current map,
/// mutate the clone, then swap the pointer.
pub struct CowCache<S> {
    inner: ArcSwap<Inner<S>>,
    /// Serialises the 1-2 concurrent writers — never held during reads.
    write_lock: Mutex<()>,
    /// Prometheus label value identifying this cache (`config` | `secret`).
    kind: &'static str,
    /// Count of write mutations (clone+swap operations). A batch counts once.
    /// Deterministic per-instance mirror of `CACHE_WRITE_MUTATIONS_TOTAL`.
    #[cfg(test)]
    writes: AtomicU64,
}

impl<S: CacheEntry> CowCache<S> {
    /// Create an empty cache pre-sized to `capacity`, labelled `kind`.
    pub fn with_capacity(kind: &'static str, capacity: usize) -> Self {
        Self::from_map(kind, Inner::with_capacity(capacity))
    }

    /// Create a cache wrapping a pre-built `map`, labelled `kind`.
    ///
    /// Used by [`ConfigCache::new`](crate::cache::ConfigCache::new) which may
    /// pre-insert a seed snapshot before handing the map over.
    pub fn from_map(kind: &'static str, map: Inner<S>) -> Self {
        CACHE_ENTRIES
            .with_label_values(&[kind])
            .set(map.len() as f64);
        Self {
            inner: ArcSwap::from_pointee(map),
            write_lock: Mutex::new(()),
            kind,
            #[cfg(test)]
            writes: AtomicU64::new(0),
        }
    }

    /// Look up a snapshot by `(namespace, name)`.
    ///
    /// Zero locking — atomic pointer load only.  Lookup is allocation-free:
    /// the `BorrowedKey` view passes `(&str, &str)` straight to the `HashMap`
    /// via the `Borrow<dyn KeyRef>` impl on [`OwnedKey`].
    pub fn get(&self, namespace: &str, name: &str) -> Option<Arc<S>> {
        let q = BorrowedKey::new(namespace, name);
        self.inner.load().get(&q as &dyn KeyRef).map(Arc::clone)
    }

    /// Return all snapshots whose namespace matches `namespace`.
    /// Zero locking — atomic pointer load only.
    pub fn all_in_namespace(&self, namespace: &str) -> Vec<Arc<S>> {
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

    /// Return any one snapshot — useful for health-gate checks.
    /// Zero locking — atomic pointer load only.
    pub fn load_any(&self) -> Option<Arc<S>> {
        self.inner.load().values().next().cloned()
    }

    /// Insert or replace the entry for `snap.namespace()` / `snap.name()`.
    pub fn update(&self, snap: S) {
        let _guard = crate::sync_util::lock_recovered(&self.write_lock);
        let current = self.inner.load();
        let mut next = (**current).clone();
        next.insert(
            OwnedKey::new(snap.namespace().to_string(), snap.name().to_string()),
            Arc::new(snap),
        );
        let len = next.len();
        self.inner.store(Arc::new(next));
        self.record_write(len);
    }

    /// Remove the entry for `(namespace, name)` if present.
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

    /// Mark all cached snapshots as stale (watcher lost K8s connection).
    ///
    /// Each snapshot gets `set_stale(now)`.  The next `update(snap)` for a
    /// fresh Apply event inserts a snapshot that is not stale.
    pub fn mark_all_stale(&self) {
        let _guard = crate::sync_util::lock_recovered(&self.write_lock);
        let current = self.inner.load();
        let mut next = (**current).clone();
        let now = Instant::now();
        for v in next.values_mut() {
            let mut snap = (**v).clone();
            snap.set_stale(now);
            *v = Arc::new(snap);
        }
        let len = next.len();
        self.inner.store(Arc::new(next));
        self.record_write(len);
    }

    /// Apply many mutations under a SINGLE map clone + swap.
    ///
    /// The per-write path (`update`/`remove`) clones the whole map once each;
    /// applying N watch events that way is N clones. `apply_batch` clones once,
    /// applies every mutation to that clone, then swaps the pointer a single
    /// time — O(map) work once instead of N times. Used on watch restart, where
    /// the relist replays the full watched set as one burst.
    ///
    /// Empty input is a no-op: no clone, no swap, not counted. Returns the
    /// number of mutations applied.
    pub fn apply_batch(&self, mutations: impl IntoIterator<Item = Mutation<S>>) -> usize {
        let mutations: Vec<Mutation<S>> = mutations.into_iter().collect();
        if mutations.is_empty() {
            return 0;
        }
        let n = mutations.len();
        let _guard = crate::sync_util::lock_recovered(&self.write_lock);
        let current = self.inner.load();
        let mut next = (**current).clone();
        for m in mutations {
            match m {
                Mutation::Upsert(snap) => {
                    next.insert(
                        OwnedKey::new(snap.namespace().to_string(), snap.name().to_string()),
                        Arc::new(snap),
                    );
                }
                Mutation::Remove { namespace, name } => {
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

    /// Number of write mutations (clone+swap operations) this cache has
    /// performed. A batch counts once. Deterministic per-instance counter used
    /// by the watcher relist tests and the burst benchmark.
    #[cfg(test)]
    pub(crate) fn write_count(&self) -> u64 {
        self.writes.load(Ordering::Relaxed)
    }

    /// Record one write mutation: bump the per-instance counter (test only),
    /// the process-wide clone-churn counter, and refresh the entry-count gauge.
    fn record_write(&self, new_len: usize) {
        #[cfg(test)]
        self.writes.fetch_add(1, Ordering::Relaxed);
        CACHE_WRITE_MUTATIONS_TOTAL
            .with_label_values(&[self.kind])
            .inc();
        CACHE_ENTRIES
            .with_label_values(&[self.kind])
            .set(new_len as f64);
    }
}
