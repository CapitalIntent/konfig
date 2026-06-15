//! Lock-free multi-key Secret cache backed by [`ArcSwap`]`<HashMap>`.
//!
//! Same pattern as [`ConfigCache`] but typed for [`SecretSnapshot`].
//! Reads are fully lock-free (atomic pointer load via `arc_swap`).
//! The 1-2 writers serialise on a `Mutex<()>` that is never held during reads.
//!
//! [`ConfigCache`]: crate::cache::ConfigCache
//! [`ArcSwap`]: arc_swap::ArcSwap

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;

use crate::cache_key::{BorrowedKey, KeyRef, OwnedKey};
use crate::types::SecretSnapshot;

type Inner = HashMap<OwnedKey, Arc<SecretSnapshot>>;

pub struct SecretCache {
    inner: ArcSwap<Inner>,
    /// Serialises the 1-2 concurrent writers — never held during reads.
    write_lock: Mutex<()>,
}

impl SecretCache {
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::from_pointee(Inner::new()),
            write_lock: Mutex::new(()),
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
        self.inner.store(Arc::new(next));
    }

    pub fn remove(&self, namespace: &str, name: &str) {
        let _guard = crate::sync_util::lock_recovered(&self.write_lock);
        let current = self.inner.load();
        let mut next = (**current).clone();
        let q = BorrowedKey::new(namespace, name);
        next.remove(&q as &dyn KeyRef);
        self.inner.store(Arc::new(next));
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
}
