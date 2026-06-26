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
        let stream = kube_watch_stream(api, wc).boxed();

        info!(namespace = %namespace, "ConfigMap watcher started (konfig.io/managed=true)");

        pump_configmap_events(stream, &cache, &namespace).await
    }
}

/// Handle a single ConfigMap watcher event by updating the cache accordingly.
/// `Apply`/`InitApply` insert (or replace) the parsed snapshot; `Delete`
/// leaves the cache's last-known-good entry in place; `Init`/`InitDone` log
/// only. Extracted so [`pump_configmap_events`] stays a thin event loop.
pub(crate) fn handle_configmap_event(
    event: Event<ConfigMap>,
    cache: &Arc<ConfigCache>,
    namespace: &str,
) {
    match event {
        Event::Apply(cm) | Event::InitApply(cm) => {
            if let Some(snap) = parse_configmap(&cm, namespace) {
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

/// Drive a ConfigMap watcher stream to completion or first error.
///
/// Extracted from [`ConfigMapWatcher::run`] so the per-event behaviour is
/// unit-testable against a synthetic stream — no kube API connection
/// required.
pub(crate) async fn pump_configmap_events<S>(
    mut stream: S,
    cache: &Arc<ConfigCache>,
    namespace: &str,
) -> Result<(), kube_watcher::Error>
where
    S: futures_util::stream::TryStream<Ok = Event<ConfigMap>, Error = kube_watcher::Error> + Unpin,
{
    while let Some(event) = stream.try_next().await? {
        handle_configmap_event(event, cache, namespace);
    }
    Ok(())
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
        // Pre-size: `data` is a BTreeMap and we'll insert at most `data.len()`
        // entries (minus the optional `schema_version` key).  Pre-sizing the
        // JSON Map eliminates the per-event `RawTable::reserve_rehash`
        // observed in pyroscope (CU-86aj37pwx).
        let mut map = serde_json::Map::with_capacity(data.len());
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
        ..Default::default()
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

    // ── parse_configmap — malformed-input branch coverage (CU-86aj14yzh) ─────
    //
    // The k8s-openapi `ConfigMap.data` binding is `BTreeMap<String, String>`,
    // so non-UTF8 bytes cannot reach `parse_configmap` through this field —
    // the apiserver/serde rejects them ahead of time. Binary payloads ride
    // `binary_data` (Base64-encoded), which `parse_configmap` does not read.
    // No `parse_configmap_non_utf8_bytes` test is added — the branch is
    // unreachable through the typed binding.

    /// Annotation present but unparseable as `u32` — the `.and_then(|v|
    /// v.parse().ok()).unwrap_or(0)` chain must silently default to 0.
    /// Hand-mutate: change `unwrap_or(0)` → `unwrap()` and re-run; the
    /// `parse().ok()` yields `None`, which panics → test fails.
    #[test]
    fn parse_configmap_malformed_schema_version_defaults_to_zero() {
        let mut data = BTreeMap::new();
        data.insert("schema_version".into(), "v1".into()); // not a u32
        data.insert("log_level".into(), "info".into());

        let cm = make_cm("cfg", data);
        let snap = parse_configmap(&cm, "ns").expect("snapshot must be produced");
        assert_eq!(
            snap.schema_version, 0,
            "unparseable schema_version annotation must silently default to 0",
        );
        // The rest of the content map must still be populated — the default
        // is local to the schema_version branch and must not poison sibling
        // fields.
        assert_eq!(snap.content["log_level"], "info");
    }

    /// `data["content"]` holds a string that is not valid YAML/JSON. The
    /// `serde_yaml::from_str` error path must `warn!` and default `content`
    /// to an empty JSON object (NOT `Value::Null` — distinct from the
    /// "no data" branch upstream). Capturing the `warn!` line is optional
    /// per the ticket; we assert on the structural fallback only.
    /// Hand-mutate: change the `Err(e)` arm's fallback from
    /// `Value::Object(Default::default())` to `Value::Null` and re-run; the
    /// `is_object()` assertion fails.
    #[test]
    fn parse_configmap_invalid_yaml_returns_empty_content() {
        let mut data = BTreeMap::new();
        data.insert("schema_version".into(), "5".into());
        // `: : :` is a YAML parse error (bare colons at the start of a flow
        // node). `serde_yaml::from_str::<Value>` rejects it.
        data.insert("content".into(), ": : :".into());

        let cm = make_cm("cfg", data);
        let snap = parse_configmap(&cm, "ns").expect("snapshot must be produced");

        // schema_version is parsed independently — must survive the content
        // parse failure.
        assert_eq!(snap.schema_version, 5);

        // Fallback is an empty JSON object, NOT Null.
        assert!(
            snap.content.is_object(),
            "invalid-YAML fallback must be a JSON object, got {:?}",
            snap.content,
        );
        assert_eq!(
            snap.content.as_object().unwrap().len(),
            0,
            "invalid-YAML fallback object must be empty",
        );
    }

    // ── pump_configmap_events ────────────────────────────────────────────────

    fn cm(name: &str, schema_version: u32) -> ConfigMap {
        let mut data = BTreeMap::new();
        data.insert("schema_version".to_string(), schema_version.to_string());
        data.insert("k".to_string(), format!("v{schema_version}"));
        make_cm(name, data)
    }

    use crate::watcher::synthetic_watcher_error as watcher_err;

    #[tokio::test]
    async fn pump_applies_events_to_cache_and_completes_on_stream_close() {
        use futures_util::stream;
        let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
        assert!(
            !cache.is_populated(),
            "precondition: empty cache before pump",
        );

        let events: Vec<Result<Event<ConfigMap>, kube_watcher::Error>> = vec![
            Ok(Event::InitApply(cm("cm-a", 1))),
            Ok(Event::Apply(cm("cm-a", 2))),
            Ok(Event::Apply(cm("cm-b", 3))),
        ];

        // Strengthen `res.is_ok()` to an exhaustive match — a regression
        // that wraps the success in a different variant would still pass
        // `is_ok()`.
        let res = pump_configmap_events(stream::iter(events), &cache, "default").await;
        match res {
            Ok(()) => {}
            Err(e) => panic!("clean stream end must be Ok(()), got Err({e:?})"),
        }

        // (a) Cache: last-write-wins on the same key — Apply(cm-a, 2) must
        // overwrite InitApply(cm-a, 1). The synthetic stream put `k=v2` into
        // the data map (cf. `cm()` helper); round-trip through
        // `parse_configmap` → `ConfigSnapshot.content` keeps it a string.
        let cm_a = cache
            .get("default", "cm-a")
            .expect("cm-a must be cached after Apply");
        assert_eq!(
            cm_a.schema_version, 2,
            "last write wins — Apply(v2) must overwrite InitApply(v1)",
        );
        assert_eq!(cm_a.content["k"], "v2");

        let cm_b = cache
            .get("default", "cm-b")
            .expect("cm-b must be cached after Apply");
        assert_eq!(cm_b.schema_version, 3);
        assert_eq!(cm_b.content["k"], "v3");

        // (b) Cache populated flag flips after first Apply event — the
        // readiness probe / `is_populated()` gate depends on this.
        assert!(
            cache.is_populated(),
            "cache must report populated after Apply events",
        );
    }

    #[tokio::test]
    async fn pump_propagates_stream_error_and_halts_processing() {
        use futures_util::stream;
        let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
        let events: Vec<Result<Event<ConfigMap>, kube_watcher::Error>> = vec![
            Ok(Event::Apply(cm("cm-a", 1))),
            Err(watcher_err()),
            // Post-error events must not be consumed.
            Ok(Event::Apply(cm("cm-c", 99))),
        ];

        let res = pump_configmap_events(stream::iter(events), &cache, "default").await;
        assert!(res.is_err(), "stream error must propagate");
        assert_eq!(cache.get("default", "cm-a").unwrap().schema_version, 1);
        assert!(cache.get("default", "cm-c").is_none());
    }

    #[tokio::test]
    async fn pump_delete_event_retains_last_known_good() {
        use futures_util::stream;
        let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
        let events: Vec<Result<Event<ConfigMap>, kube_watcher::Error>> = vec![
            Ok(Event::Apply(cm("cm-a", 7))),
            Ok(Event::Delete(cm("cm-a", 7))),
        ];

        let _ = pump_configmap_events(stream::iter(events), &cache, "default").await;
        // Delete intentionally retains the cached entry (documented design
        // for fail-static behaviour during cluster churn).
        assert_eq!(cache.get("default", "cm-a").unwrap().schema_version, 7);
    }

    #[tokio::test]
    async fn pump_init_events_advance_stream_but_skip_cache_writes() {
        use futures_util::stream;
        let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
        let events: Vec<Result<Event<ConfigMap>, kube_watcher::Error>> =
            vec![Ok(Event::Init), Ok(Event::InitDone)];

        let res = pump_configmap_events(stream::iter(events), &cache, "default").await;
        assert!(res.is_ok());
        assert!(!cache.is_populated());
    }

    #[tokio::test]
    async fn pump_empty_stream_is_ok_no_writes() {
        use futures_util::stream;
        let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
        let events: Vec<Result<Event<ConfigMap>, kube_watcher::Error>> = vec![];

        let res = pump_configmap_events(stream::iter(events), &cache, "default").await;
        assert!(res.is_ok());
        assert!(!cache.is_populated());
    }
}
