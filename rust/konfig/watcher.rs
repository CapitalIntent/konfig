//! K8s watcher for `Config.konfig.io/v1` CRDs.
//!
//! Streams events via kube-rs and updates `ConfigCache` on each Apply/InitApply.
//! Delete events log a warning and retain the last-known-good value (CP semantics).

use std::sync::Arc;

use futures_util::{StreamExt, TryStreamExt};
use kube::api::ApiResource;
use kube::core::DynamicObject;
use kube::runtime::watcher::{self as kube_watcher, Event, watcher as kube_watch_stream};
use kube::{Api, Client};
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::cache::ConfigCache;
use crate::types::{ConfigSnapshot, ConfigSpec};

// ── Constants ─────────────────────────────────────────────────────────────────

pub const GROUP: &str = "konfig.io";
pub const VERSION: &str = "v1";
pub const KIND: &str = "Config";
pub const PLURAL: &str = "configs";

pub fn config_api_resource() -> ApiResource {
    ApiResource {
        group: GROUP.to_string(),
        version: VERSION.to_string(),
        api_version: format!("{GROUP}/{VERSION}"),
        kind: KIND.to_string(),
        plural: PLURAL.to_string(),
    }
}

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum WatcherError {
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),
    #[error("watcher error: {0}")]
    Watcher(#[from] kube_watcher::Error),
}

// ── Watcher ───────────────────────────────────────────────────────────────────

pub struct Watcher {
    client: Client,
}

impl Watcher {
    pub fn new(client: Client) -> Self {
        Watcher { client }
    }

    /// Run the watcher until the stream ends or errors. Call inside `tokio::spawn`.
    pub async fn run(
        self,
        cache: Arc<ConfigCache>,
        namespace: String,
        config_name: String,
    ) -> Result<(), WatcherError> {
        let ar = config_api_resource();
        let api: Api<DynamicObject> = Api::namespaced_with(self.client, &namespace, &ar);

        let wc = kube_watcher::Config::default().fields(&format!("metadata.name={config_name}"));

        let mut stream = kube_watch_stream(api, wc).boxed();

        info!(namespace = %namespace, name = %config_name, "Config watcher started");

        while let Some(event) = stream.try_next().await? {
            handle_event(event, &cache);
        }

        Ok(())
    }
}

pub(crate) fn handle_event(event: Event<DynamicObject>, cache: &Arc<ConfigCache>) {
    match event {
        Event::Apply(obj) | Event::InitApply(obj) => {
            let name = obj.metadata.name.as_deref().unwrap_or("<unknown>");
            if let Some(snap) = parse_config_object(&obj) {
                info!(name = %name, schema_version = snap.schema_version, "Config applied — cache updated");
                cache.update(snap);
            } else {
                warn!(name = %name, "Config object could not be parsed — cache unchanged");
            }
        }
        Event::Delete(obj) => {
            let name = obj.metadata.name.as_deref().unwrap_or("<unknown>");
            warn!(name = %name, "Config deleted — cache retains last-known-good");
        }
        Event::Init => debug!("Watch stream: initial list phase"),
        Event::InitDone => debug!("Watch stream: initial list complete"),
    }
}

/// Parse a `DynamicObject` (Config CRD) into a `ConfigSnapshot`.
///
/// Expects `obj.data["spec"]` to deserialize as `ConfigSpec`.
pub fn parse_config_object(obj: &DynamicObject) -> Option<ConfigSnapshot> {
    let resource_version = obj.metadata.resource_version.clone().unwrap_or_default();
    let name = obj.metadata.name.clone().unwrap_or_default();
    let namespace = obj.metadata.namespace.clone().unwrap_or_default();

    let spec_value = obj.data.get("spec")?;
    let spec: ConfigSpec = serde_json::from_value(spec_value.clone())
        .map_err(|e| warn!(name = %name, "Failed to parse Config spec: {e}"))
        .ok()?;

    Some(ConfigSnapshot::from_spec(name, namespace, spec, resource_version))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_obj(name: &str, schema_version: u32, content: serde_json::Value) -> DynamicObject {
        let mut obj = DynamicObject::new(name, &config_api_resource());
        obj.metadata.name = Some(name.to_string());
        obj.metadata.namespace = Some("default".to_string());
        obj.metadata.resource_version = Some("rv-001".to_string());
        obj.data = json!({
            "spec": {
                "schema_version": schema_version,
                "content": content,
            }
        });
        obj
    }

    #[test]
    fn parse_valid_object() {
        let obj = make_obj("my-config", 5, json!({"key": "value"}));
        let snap = parse_config_object(&obj).expect("must parse");
        assert_eq!(snap.name, "my-config");
        assert_eq!(snap.namespace, "default");
        assert_eq!(snap.schema_version, 5);
        assert_eq!(snap.content["key"], "value");
        assert_eq!(snap.resource_version, "rv-001");
    }

    #[test]
    fn parse_missing_spec_returns_none() {
        let mut obj = DynamicObject::new("x", &config_api_resource());
        obj.data = json!({});
        assert!(parse_config_object(&obj).is_none());
    }

    #[test]
    fn parse_missing_content_defaults_to_null() {
        let obj = make_obj("cfg", 1, serde_json::Value::Null);
        let snap = parse_config_object(&obj).unwrap();
        assert!(snap.content.is_null());
    }

    #[test]
    fn apply_event_updates_cache() {
        let obj = make_obj("cfg", 7, json!({"x": 1}));
        let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
        handle_event(Event::Apply(obj), &cache);
        assert_eq!(cache.load().schema_version, 7);
    }

    #[test]
    fn delete_event_leaves_cache_unchanged() {
        let obj = make_obj("cfg", 3, json!({}));
        let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
        handle_event(Event::Apply(obj.clone()), &cache);
        assert_eq!(cache.load().schema_version, 3);
        handle_event(Event::Delete(obj), &cache);
        assert_eq!(cache.load().schema_version, 3);
    }
}
