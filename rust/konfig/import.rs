//! Onboarding helper: import existing ConfigMaps as `Config.konfig.io/v1` CRDs.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::ConfigMap;
use kube::Client;
use kube::api::{Api, Patch, PatchParams};
use serde_json::json;
use tracing::{info, warn};

use crate::watcher::{GROUP, VERSION, config_api_resource};

pub struct ImportResult {
    pub resource_version: String,
}

/// Default schema_version applied when the source ConfigMap has no
/// `schema_version` key (or the value is non-numeric).
const DEFAULT_SCHEMA_VERSION: u32 = 1;

/// Pure parser — extract `schema_version` from a ConfigMap `data` map.
/// Falls back to [`DEFAULT_SCHEMA_VERSION`] when the key is missing or
/// non-numeric. Separated from the async kube path so its branches are
/// unit-testable without a kube mock.
pub(crate) fn parse_schema_version_from_data(data: &BTreeMap<String, String>) -> u32 {
    data.get("schema_version")
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_SCHEMA_VERSION)
}

/// Pure builder — convert a ConfigMap `data` map into the JSON object
/// stored under `spec.content` of the target Config CRD, applying the
/// shared scalar cascade so live-watcher and import paths agree on type
/// coercion. The `schema_version` key (promoted to a top-level field
/// upstream) is filtered out.
pub(crate) fn content_from_data(
    data: &BTreeMap<String, String>,
) -> serde_json::Map<String, serde_json::Value> {
    let mut content = serde_json::Map::new();
    for (k, v) in data {
        if k == "schema_version" {
            continue;
        }
        content.insert(k.clone(), crate::value_parse::scalar_value(v));
    }
    content
}

/// Pure builder — assemble the server-side-apply patch body for the
/// target `Config` CRD. No I/O, fully unit-testable.
pub(crate) fn build_import_patch_body(
    namespace: &str,
    target_name: &str,
    schema_version: u32,
    content: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Value {
    json!({
        "apiVersion": format!("{GROUP}/{VERSION}"),
        "kind": "Config",
        "metadata": {
            "name": target_name,
            "namespace": namespace,
        },
        "spec": {
            "schema_version": schema_version,
            "content": serde_json::Value::Object(content),
        }
    })
}

/// Import a ConfigMap's `data` field as a `Config` CRD.
///
/// - Reads the ConfigMap by `(namespace, configmap_name)`.
/// - Converts its `data` keys/values to a JSON object.
/// - Creates or patches a `Config` CRD named `target_name` in the same namespace.
/// - Uses `schema_version` from the ConfigMap `data["schema_version"]` key if present; else 1.
pub async fn import_configmap(
    client: Client,
    namespace: &str,
    configmap_name: &str,
    target_name: &str,
) -> Result<ImportResult, Box<dyn std::error::Error>> {
    let cms: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    let cm = cms.get(configmap_name).await?;

    let data = cm.data.unwrap_or_default();

    let schema_version = parse_schema_version_from_data(&data);
    let content = content_from_data(&data);
    let patch_body = build_import_patch_body(namespace, target_name, schema_version, content);

    let ar = config_api_resource();
    let api: Api<kube::core::DynamicObject> = Api::namespaced_with(client, namespace, &ar);

    let pp = PatchParams::apply("konfig.v1").force();
    let patched = api
        .patch(target_name, &pp, &Patch::Apply(patch_body))
        .await?;

    let rv = patched.metadata.resource_version.unwrap_or_default();

    info!(
        namespace = %namespace,
        configmap = %configmap_name,
        target = %target_name,
        resource_version = %rv,
        "ConfigMap imported as Config CRD",
    );

    if cm.binary_data.is_some() {
        warn!(
            configmap = %configmap_name,
            "ConfigMap has binaryData — only `data` keys were imported. \
             Binary fields must be migrated manually.",
        );
    }

    Ok(ImportResult {
        resource_version: rv,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn parse_schema_version_present() {
        let d = data(&[("schema_version", "7")]);
        assert_eq!(parse_schema_version_from_data(&d), 7);
    }

    #[test]
    fn parse_schema_version_missing_defaults_to_one() {
        let d = data(&[("other", "v")]);
        assert_eq!(parse_schema_version_from_data(&d), DEFAULT_SCHEMA_VERSION);
    }

    #[test]
    fn parse_schema_version_non_numeric_defaults_to_one() {
        let d = data(&[("schema_version", "v2")]);
        assert_eq!(parse_schema_version_from_data(&d), DEFAULT_SCHEMA_VERSION);
    }

    #[test]
    fn parse_schema_version_empty_string_defaults_to_one() {
        let d = data(&[("schema_version", "")]);
        assert_eq!(parse_schema_version_from_data(&d), DEFAULT_SCHEMA_VERSION);
    }

    #[test]
    fn content_excludes_schema_version_key() {
        let d = data(&[("schema_version", "3"), ("a", "1"), ("b", "true")]);
        let content = content_from_data(&d);
        assert!(!content.contains_key("schema_version"));
        assert_eq!(content.get("a"), Some(&serde_json::json!(1)));
        assert_eq!(content.get("b"), Some(&serde_json::json!(true)));
    }

    #[test]
    fn content_preserves_string_values_that_do_not_parse_as_scalars() {
        let d = data(&[("greeting", "hello world")]);
        let content = content_from_data(&d);
        assert_eq!(
            content.get("greeting"),
            Some(&serde_json::json!("hello world"))
        );
    }

    #[test]
    fn content_empty_when_data_is_empty() {
        let d = BTreeMap::new();
        let content = content_from_data(&d);
        assert!(content.is_empty());
    }

    #[test]
    fn content_empty_when_data_only_has_schema_version() {
        let d = data(&[("schema_version", "5")]);
        let content = content_from_data(&d);
        assert!(content.is_empty());
    }

    #[test]
    fn build_patch_body_shape() {
        let mut content = serde_json::Map::new();
        content.insert("k".to_string(), serde_json::json!(42));
        let body = build_import_patch_body("ns-a", "cfg-b", 9, content);
        assert_eq!(body["kind"], serde_json::json!("Config"));
        assert_eq!(
            body["apiVersion"],
            serde_json::json!(format!("{GROUP}/{VERSION}"))
        );
        assert_eq!(body["metadata"]["name"], serde_json::json!("cfg-b"));
        assert_eq!(body["metadata"]["namespace"], serde_json::json!("ns-a"));
        assert_eq!(body["spec"]["schema_version"], serde_json::json!(9));
        assert_eq!(body["spec"]["content"]["k"], serde_json::json!(42));
    }
}
