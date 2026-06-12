//! Watches K8s ConfigMaps labeled `konfig.io/managed=true` as a config source.
//!
//! Shares [`ConfigCache`] with the Config CRD watcher.
//! Enabled via `--watch-configmaps` flag in main.rs.

use std::sync::Arc;

use futures_util::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::ConfigMap;
use kube::runtime::watcher::{self as kube_watcher, Event, watcher as kube_watch_stream};
use kube::{Api, Client};
use serde_json::Value;
use tracing::{debug, info, warn};

use crate::cache::ConfigCache;
use crate::types::ConfigSnapshot;

pub const MANAGED_LABEL: &str = "konfig.io/managed";

pub struct ConfigMapWatcher {
    client: Client,
}

impl ConfigMapWatcher {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn run(
        self,
        cache: Arc<ConfigCache>,
        namespace: String,
    ) -> Result<(), kube_watcher::Error> {
        let api: Api<ConfigMap> = Api::namespaced(self.client, &namespace);
        let wc = kube_watcher::Config::default().labels(&format!("{MANAGED_LABEL}=true"));
        let mut stream = kube_watch_stream(api, wc).boxed();

        info!(namespace = %namespace, "ConfigMap watcher started (konfig.io/managed=true)");

        while let Some(event) = stream.try_next().await? {
            match event {
                Event::Apply(cm) | Event::InitApply(cm) => {
                    if let Some(snap) = parse_configmap(&cm, &namespace) {
                        info!(name = %snap.name, "ConfigMap applied → cache updated");
                        cache.update(snap);
                    }
                }
                Event::Delete(cm) => {
                    let name = cm.metadata.name.as_deref().unwrap_or("<unknown>");
                    warn!(name, "ConfigMap deleted — cache retains last-known-good");
                }
                Event::Init | Event::InitDone => debug!("ConfigMap watch stream: init phase"),
            }
        }
        Ok(())
    }
}

fn parse_configmap(cm: &ConfigMap, namespace: &str) -> Option<ConfigSnapshot> {
    let resource_version = cm.metadata.resource_version.clone().unwrap_or_default();
    let name = cm.metadata.name.clone().unwrap_or_default();

    let data = cm.data.as_ref()?;

    let schema_version: u32 = data
        .get("schema_version")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    // If data["content"] key exists, parse it as JSON/YAML.
    // Otherwise treat the entire data map (minus schema_version) as content.
    let content = if let Some(content_str) = data.get("content") {
        match serde_yaml::from_str(content_str) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    name = %name,
                    err = %e,
                    "ConfigMap content key failed to parse as YAML — defaulting to empty object",
                );
                Value::Object(Default::default())
            }
        }
    } else {
        let mut map = serde_json::Map::new();
        for (k, v) in data {
            if k == "schema_version" {
                continue;
            }
            map.insert(k.clone(), crate::value_parse::scalar_value(v));
        }
        Value::Object(map)
    };

    Some(ConfigSnapshot {
        name,
        namespace: namespace.to_string(),
        schema_version,
        content,
        resource_version,
        loaded_at: std::time::Instant::now(),
        stale_since: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn make_cm(name: &str, data: BTreeMap<String, String>) -> ConfigMap {
        let mut cm = ConfigMap::default();
        cm.metadata.name = Some(name.to_string());
        cm.metadata.resource_version = Some("rv-001".to_string());
        cm.data = Some(data);
        cm
    }

    #[test]
    fn parse_flat_data_map() {
        let mut data = BTreeMap::new();
        data.insert("schema_version".into(), "3".into());
        data.insert("log_level".into(), "info".into());
        data.insert("timeout_ms".into(), "5000".into());

        let cm = make_cm("my-config", data);
        let snap = parse_configmap(&cm, "default").unwrap();
        assert_eq!(snap.schema_version, 3);
        assert_eq!(snap.content["log_level"], "info");
        assert_eq!(snap.content["timeout_ms"], 5000);
    }

    #[test]
    fn parse_content_key_takes_priority() {
        let mut data = BTreeMap::new();
        data.insert("schema_version".into(), "1".into());
        data.insert("content".into(), r#"{"key": "value"}"#.into());
        data.insert("other".into(), "ignored".into());

        let cm = make_cm("cfg", data);
        let snap = parse_configmap(&cm, "ns").unwrap();
        assert_eq!(snap.content["key"], "value");
        assert!(snap.content.get("other").is_none());
    }

    #[test]
    fn parse_returns_none_when_no_data() {
        let mut cm = ConfigMap::default();
        cm.metadata.name = Some("cfg".to_string());
        assert!(parse_configmap(&cm, "ns").is_none());
    }

    #[test]
    fn parse_propagates_resource_version() {
        let cm = make_cm("cfg", BTreeMap::new());
        let snap = parse_configmap(&cm, "ns").unwrap();
        assert_eq!(snap.resource_version, "rv-001");
    }
}
