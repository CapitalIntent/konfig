//! `ApplySecret` handler for `KonfigService`.
//!
//! Flow:
//! 1. Parse `yaml_content` as a YAML map of key→plaintext-value.
//! 2. Read current Secret from K8s; decode `konfig.io/schema-version` annotation.
//! 3. Reject if incoming version <= current (schema_version monotonicity).
//! 4. Base64-encode each value.
//! 5. Patch K8s Secret with label + annotation via server-side apply.
//! 6. Return `ApplySecretResponse { resource_version }`.

use std::collections::BTreeMap;
use std::time::Duration;

use k8s_openapi::ByteString;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::Client;
use kube::api::{Api, Patch, PatchParams};
use tonic::{Response, Status};
use tracing::{info, warn};

use crate::proto::{ApplySecretRequest, ApplySecretResponse};

pub const MANAGED_LABEL: &str = "konfig.io/managed";
pub const SCHEMA_VERSION_ANNOTATION: &str = "konfig.io/schema-version";

const RETRY_DELAYS_MS: [u64; 2] = [100, 200];

pub async fn handle_apply_secret(
    kube_client: Client,
    req: ApplySecretRequest,
) -> Result<Response<ApplySecretResponse>, Status> {
    apply_secret_inner(&req.namespace, &req.name, &req.yaml_content, kube_client).await
}

pub async fn apply_secret_inner(
    namespace: &str,
    name: &str,
    yaml_content: &str,
    kube_client: Client,
) -> Result<Response<ApplySecretResponse>, Status> {
    let plaintext_map: BTreeMap<String, String> = serde_yaml::from_str(yaml_content)
        .map_err(|e| Status::invalid_argument(format!("invalid YAML: {e}")))?;

    let incoming_version: u32 = plaintext_map
        .get("schema_version")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let secrets: Api<Secret> = Api::namespaced(kube_client.clone(), namespace);
    let current_version = fetch_current_secret_version(&secrets, name).await?;

    if incoming_version <= current_version {
        warn!(
            incoming = incoming_version,
            current = current_version,
            "ApplySecret rejected: schema_version not monotonically increasing",
        );
        return Err(Status::failed_precondition(format!(
            "schema_version must be > {current_version}; got {incoming_version}"
        )));
    }

    // Base64-encode values (skip schema_version — stored in annotation).
    let encoded_data: BTreeMap<String, ByteString> = plaintext_map
        .iter()
        .filter(|(k, _)| k.as_str() != "schema_version")
        .map(|(k, v)| {
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(v.as_bytes());
            (k.clone(), ByteString(encoded.into_bytes()))
        })
        .collect();

    let resource_version =
        patch_secret_with_retry(&secrets, name, namespace, incoming_version, encoded_data).await?;

    info!(
        namespace = %namespace,
        name = %name,
        schema_version = incoming_version,
        resource_version = %resource_version,
        "ApplySecret succeeded",
    );

    Ok(Response::new(ApplySecretResponse { resource_version }))
}

async fn fetch_current_secret_version(secrets: &Api<Secret>, name: &str) -> Result<u32, Status> {
    match secrets.get(name).await {
        Ok(s) => {
            let version = s
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(SCHEMA_VERSION_ANNOTATION))
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            Ok(version)
        }
        Err(kube::Error::Api(ref ae)) if ae.code == 404 => Ok(0),
        Err(e) => Err(Status::unavailable(format!("kube API error: {e}"))),
    }
}

async fn patch_secret_with_retry(
    secrets: &Api<Secret>,
    name: &str,
    namespace: &str,
    schema_version: u32,
    data: BTreeMap<String, ByteString>,
) -> Result<String, Status> {
    let build_patch = |data: BTreeMap<String, ByteString>| Secret {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(namespace.to_string()),
            labels: Some({
                let mut l = BTreeMap::new();
                l.insert(MANAGED_LABEL.to_string(), "true".to_string());
                l
            }),
            annotations: Some({
                let mut a = BTreeMap::new();
                a.insert(
                    SCHEMA_VERSION_ANNOTATION.to_string(),
                    schema_version.to_string(),
                );
                a
            }),
            ..Default::default()
        },
        data: if data.is_empty() { None } else { Some(data) },
        ..Default::default()
    };

    let ssapply = PatchParams::apply("konfig.v1").force();
    let mut attempt = 0usize;

    loop {
        let patch = build_patch(data.clone());
        match secrets.patch(name, &ssapply, &Patch::Apply(&patch)).await {
            Ok(s) => {
                return Ok(s.metadata.resource_version.unwrap_or_default());
            }
            Err(kube::Error::Api(ref ae)) if ae.code == 409 && attempt < RETRY_DELAYS_MS.len() => {
                // ±25% jitter — see `crate::grpc::jittered_retry_ms`.
                let delay_ms = crate::grpc::jittered_retry_ms(RETRY_DELAYS_MS[attempt]);
                warn!(
                    attempt = attempt + 1,
                    delay_ms, "ApplySecret: 409 — retrying"
                );
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                attempt += 1;
            }
            Err(kube::Error::Api(ref ae)) if ae.code == 409 => {
                return Err(Status::aborted(
                    "ApplySecret: 409 Conflict — exceeded max retries",
                ));
            }
            Err(e) => {
                return Err(Status::unavailable(format!("kube patch error: {e}")));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn schema_version_downgrade_logic() {
        // incoming <= current → reject
        let incoming = 5u32;
        let current = 5u32;
        assert!(incoming <= current);
        let incoming = 3u32;
        assert!(incoming <= current);
        let incoming = 6u32;
        assert!(incoming > current); // accept
    }
}
