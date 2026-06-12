//! Onboarding helper: import existing ConfigMaps as `Config.konfig.io/v1` CRDs.

use k8s_openapi::api::core::v1::ConfigMap;
use kube::Client;
use kube::api::{Api, Patch, PatchParams};
use serde_json::json;
use tracing::{info, warn};

use crate::watcher::{GROUP, VERSION, config_api_resource};

pub struct ImportResult {
    pub resource_version: String,
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

    let schema_version: u32 = data
        .get("schema_version")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);

    // Convert string map to JSON object, excluding schema_version (promoted to top level).
    // Uses the shared `crate::value_parse::scalar_value` cascade so import
    // and the live ConfigMap watcher stay in lock-step on type coercion.
    let mut content = serde_json::Map::new();
    for (k, v) in &data {
        if k == "schema_version" {
            continue;
        }
        content.insert(k.clone(), crate::value_parse::scalar_value(v));
    }

    let patch_body = json!({
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
    });

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
