//! Lock-free config cache backed by [`ArcSwap`].

use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::types::ConfigSnapshot;

pub struct ConfigCache {
    inner: ArcSwap<ConfigSnapshot>,
}

impl ConfigCache {
    pub fn new(initial: ConfigSnapshot) -> Self {
        Self { inner: ArcSwap::from_pointee(initial) }
    }

    pub fn load(&self) -> arc_swap::Guard<Arc<ConfigSnapshot>> {
        self.inner.load()
    }

    pub fn update(&self, new: ConfigSnapshot) {
        self.inner.store(Arc::new(new));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn snap(v: u32) -> ConfigSnapshot {
        ConfigSnapshot {
            schema_version: v,
            content: json!({"version": v}),
            resource_version: format!("rv-{v}"),
            ..Default::default()
        }
    }

    #[test]
    fn initial_load() {
        let cache = ConfigCache::new(snap(1));
        assert_eq!(cache.load().schema_version, 1);
    }

    #[test]
    fn update_replaces() {
        let cache = ConfigCache::new(snap(1));
        cache.update(snap(2));
        assert_eq!(cache.load().schema_version, 2);
    }

    #[test]
    fn old_guard_survives_update() {
        let cache = ConfigCache::new(snap(1));
        let old = cache.load();
        cache.update(snap(2));
        assert_eq!(cache.load().schema_version, 2);
        assert_eq!(old.schema_version, 1);
    }
}
