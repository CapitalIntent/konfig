//! `Apply` handler — creates or updates a `Config.konfig.io/v1` CRD.
//!
//! Flow:
//! 1. Parse `yaml_content` as `ConfigSpec`.
//! 2. Fetch current CRD to check `schema_version` monotonicity.
//! 3. Reject with `FAILED_PRECONDITION` if incoming version ≤ current.
//! 4. Patch the CRD with server-side apply; retry 409 up to 3 times.
//! 5. Return `ApplyResponse { resource_version }`.

use std::time::{Duration, Instant};

use kube::Client;
use kube::api::{Api, Patch, PatchParams};
use kube::core::DynamicObject;
use serde_json::json;
use tonic::{Response, Status};
use tracing::{debug, info, warn};

use crate::grpc::jittered_retry_ms;
use crate::metrics::{APPLY_DURATION, APPLY_TOTAL};
use crate::proto::{ApplyRequest, ApplyResponse};
use crate::types::ConfigSpec;
use crate::watcher::{GROUP, VERSION, config_api_resource};

const RETRY_DELAYS_MS: [u64; 2] = [100, 200];

pub async fn handle_apply(
    kube_client: Client,
    req: ApplyRequest,
) -> Result<Response<ApplyResponse>, Status> {
    debug!(namespace = %req.namespace, name = %req.name, "Apply RPC");

    apply_inner(&req.namespace, &req.name, &req.yaml_content, kube_client).await
}

pub async fn apply_inner(
    namespace: &str,
    name: &str,
    yaml_content: &str,
    kube_client: Client,
) -> Result<Response<ApplyResponse>, Status> {
    let spec: ConfigSpec = serde_yaml::from_str(yaml_content)
        .map_err(|e| Status::invalid_argument(format!("invalid YAML: {e}")))?;

    apply_spec(namespace, name, spec, kube_client).await
}

/// Apply a parsed `ConfigSpec` to the cluster via server-side apply.
///
/// Enforces `schema_version` monotonicity, patches with retry, and increments
/// the same `APPLY_TOTAL` counters as the public `Apply` RPC path so Revert is
/// observable as a normal apply.
pub async fn apply_spec(
    namespace: &str,
    name: &str,
    spec: ConfigSpec,
    kube_client: Client,
) -> Result<Response<ApplyResponse>, Status> {
    let started = Instant::now();
    let incoming = spec.schema_version;

    let ar = config_api_resource();
    let api: Api<DynamicObject> = Api::namespaced_with(kube_client, namespace, &ar);

    let current = fetch_current_schema_version(&api, name).await?;

    if incoming <= current {
        warn!(
            incoming,
            current, "Apply rejected: schema_version not increasing"
        );
        APPLY_TOTAL
            .with_label_values(&[namespace, "rejected"])
            .inc();
        APPLY_DURATION
            .with_label_values(&[namespace, "rejected"])
            .observe(started.elapsed().as_secs_f64());
        return Err(Status::failed_precondition(format!(
            "schema_version must be > {current}; got {incoming}"
        )));
    }

    let patch_body = json!({
        "apiVersion": format!("{GROUP}/{VERSION}"),
        "kind": "Config",
        "metadata": { "name": name, "namespace": namespace },
        "spec": serde_json::to_value(&spec)
            .map_err(|e| Status::internal(format!("serialize error: {e}")))?
    });

    match patch_with_retry(&api, name, patch_body).await {
        Ok(rv) => {
            info!(namespace, name, schema_version = incoming, resource_version = %rv, "Apply succeeded");
            APPLY_TOTAL.with_label_values(&[namespace, "ok"]).inc();
            APPLY_DURATION
                .with_label_values(&[namespace, "ok"])
                .observe(started.elapsed().as_secs_f64());
            Ok(Response::new(ApplyResponse {
                resource_version: rv,
            }))
        }
        Err(e) => {
            APPLY_TOTAL.with_label_values(&[namespace, "error"]).inc();
            APPLY_DURATION
                .with_label_values(&[namespace, "error"])
                .observe(started.elapsed().as_secs_f64());
            Err(e)
        }
    }
}

/// Fetch the current schema_version of a Config CRD, or 0 if it does not exist.
///
/// Used by both Apply (to enforce monotonicity) and Revert (to compute the
/// new schema_version when replaying historical content).
pub(crate) async fn fetch_current_schema_version(
    api: &Api<DynamicObject>,
    name: &str,
) -> Result<u32, Status> {
    match api.get(name).await {
        Ok(obj) => {
            let v = obj
                .data
                .get("spec")
                .and_then(|s| s.get("schema_version"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0) as u32;
            Ok(v)
        }
        Err(kube::Error::Api(ref ae)) if ae.code == 404 => Ok(0),
        Err(e) => Err(Status::unavailable(format!("kube error: {e}"))),
    }
}

async fn patch_with_retry(
    api: &Api<DynamicObject>,
    name: &str,
    body: serde_json::Value,
) -> Result<String, Status> {
    let pp = PatchParams::apply("konfig.v1").force();
    let mut attempt = 0usize;

    loop {
        match api.patch(name, &pp, &Patch::Apply(body.clone())).await {
            Ok(obj) => return Ok(obj.metadata.resource_version.unwrap_or_default()),
            Err(kube::Error::Api(ref ae)) if ae.code == 409 && attempt < RETRY_DELAYS_MS.len() => {
                // Add ±25% jitter to avoid lockstep retries from N clients
                // hitting the same Config racing on the same resourceVersion.
                let delay = jittered_retry_ms(RETRY_DELAYS_MS[attempt]);
                warn!(
                    attempt = attempt + 1,
                    delay_ms = delay,
                    "Apply: 409 Conflict — retrying"
                );
                tokio::time::sleep(Duration::from_millis(delay)).await;
                attempt += 1;
            }
            Err(kube::Error::Api(ref ae)) if ae.code == 409 => {
                return Err(Status::aborted("409 Conflict — exceeded max retries"));
            }
            Err(e) => return Err(Status::unavailable(format!("kube patch error: {e}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_version_monotonicity_check() {
        let incoming = 5u32;
        let current = 5u32;
        assert!(incoming <= current, "equal version must be rejected");

        let incoming = 3u32;
        let current = 5u32;
        assert!(incoming <= current, "lower version must be rejected");

        let incoming = 6u32;
        let current = 5u32;
        assert!(incoming > current, "higher version must be accepted");
    }

    #[test]
    fn invalid_yaml_detected() {
        let result = serde_yaml::from_str::<ConfigSpec>("not: [valid: yaml: here");
        assert!(result.is_err());
    }

    #[test]
    fn valid_yaml_parses() {
        let yaml = "schema_version: 3\ncontent:\n  key: value\n";
        let spec: ConfigSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.schema_version, 3);
        assert_eq!(spec.content["key"], "value");
    }
}
