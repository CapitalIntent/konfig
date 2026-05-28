//! Lock-free multi-key Secret cache backed by [`DashMap`].
//!
//! Same pattern as [`ConfigCache`] but typed for [`SecretSnapshot`].
//! Uses the same flat `"namespace\x00name"` key strategy for zero-alloc
//! lookups — see `cache.rs` for the rationale.
//!
//! [`ConfigCache`]: crate::cache::ConfigCache
//! [`DashMap`]: dashmap::DashMap

use std::sync::Arc;

use dashmap::DashMap;

use crate::types::SecretSnapshot;

// ── Cache key helper ──────────────────────────────────────────────────────────

/// Build the internal flat key from `(namespace, name)`.
#[inline]
fn cache_key(namespace: &str, name: &str) -> String {
    let mut key = String::with_capacity(namespace.len() + 1 + name.len());
    key.push_str(namespace);
    key.push('\x00');
    key.push_str(name);
    key
}

/// Stack-allocated key buffer for zero-alloc lookups.
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

// ── SecretCache ───────────────────────────────────────────────────────────────

pub struct SecretCache {
    inner: DashMap<String, Arc<SecretSnapshot>>,
}

impl SecretCache {
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }

    pub fn get(&self, namespace: &str, name: &str) -> Option<Arc<SecretSnapshot>> {
        if let Some(k) = StackKey::new(namespace, name) {
            self.inner.get(k.as_str()).map(|v| Arc::clone(&v))
        } else {
            self.inner
                .get(cache_key(namespace, name).as_str())
                .map(|v| Arc::clone(&v))
        }
    }

    pub fn update(&self, snap: SecretSnapshot) {
        let key = cache_key(&snap.namespace, &snap.name);
        self.inner.insert(key, Arc::new(snap));
    }

    pub fn remove(&self, namespace: &str, name: &str) {
        if let Some(k) = StackKey::new(namespace, name) {
            self.inner.remove(k.as_str());
        } else {
            self.inner.remove(cache_key(namespace, name).as_str());
        }
    }

    pub fn all_in_namespace(&self, namespace: &str) -> Vec<Arc<SecretSnapshot>> {
        self.inner
            .iter()
            .filter(|e| {
                e.key()
                    .split_once('\x00')
                    .map_or(false, |(ns, _)| ns == namespace)
            })
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
