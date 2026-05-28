//! Lock-free multi-key Secret cache backed by [`DashMap`].
//!
//! Same pattern as [`ConfigCache`] but typed for [`SecretSnapshot`].
//!
//! [`ConfigCache`]: crate::cache::ConfigCache
//! [`DashMap`]: dashmap::DashMap

use std::sync::Arc;

use dashmap::DashMap;

use crate::types::SecretSnapshot;

pub struct SecretCache {
    inner: DashMap<(String, String), Arc<SecretSnapshot>>,
}

impl SecretCache {
    pub fn new() -> Self {
        Self { inner: DashMap::new() }
    }

    pub fn get(&self, namespace: &str, name: &str) -> Option<Arc<SecretSnapshot>> {
        self.inner.get(&(namespace.to_string(), name.to_string())).map(|v| Arc::clone(&v))
    }

    pub fn update(&self, snap: SecretSnapshot) {
        let key = (snap.namespace.clone(), snap.name.clone());
        self.inner.insert(key, Arc::new(snap));
    }

    pub fn remove(&self, namespace: &str, name: &str) {
        self.inner.remove(&(namespace.to_string(), name.to_string()));
    }

    pub fn all_in_namespace(&self, namespace: &str) -> Vec<Arc<SecretSnapshot>> {
        self.inner
            .iter()
            .filter(|e| e.key().0 == namespace)
            .map(|e| Arc::clone(e.value()))
            .collect()
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
