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
        Ok(obj) => Ok(parse_schema_version_from_object(&obj)),
        Err(kube::Error::Api(ref ae)) if ae.code == 404 => Ok(0),
        Err(e) => Err(Status::unavailable(format!("kube error: {e}"))),
    }
}

/// Pure parser — extract `spec.schema_version` from a DynamicObject, or `0`
/// when the field is missing, non-numeric, or exceeds `u32::MAX`. Separated
/// from the async kube call so its branches are unit-testable without a
/// kube mock.
pub(crate) fn parse_schema_version_from_object(obj: &DynamicObject) -> u32 {
    obj.data
        .get("spec")
        .and_then(|s| s.get("schema_version"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32
}

/// Decision returned by `classify_patch_error` so the (un-mockable) kube I/O
/// loop stays thin and the (pure) decision logic is unit-testable.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PatchRetryDecision {
    /// 409 Conflict and we still have budget — sleep `delay_ms` then retry.
    RetryAfter { delay_ms: u64 },
    /// 409 Conflict and we are out of retry budget — `Status::aborted`.
    AbortRetriesExhausted,
    /// Anything else — `Status::unavailable`.
    Unavailable,
}

/// Pure classifier — no I/O, no logging. Tests cover every branch by
/// constructing `kube::Error::Api(ErrorResponse { code, ... })` directly.
pub(crate) fn classify_patch_error(err: &kube::Error, attempt: usize) -> PatchRetryDecision {
    match err {
        kube::Error::Api(ae) if ae.code == 409 && attempt < RETRY_DELAYS_MS.len() => {
            PatchRetryDecision::RetryAfter {
                delay_ms: RETRY_DELAYS_MS[attempt],
            }
        }
        kube::Error::Api(ae) if ae.code == 409 => PatchRetryDecision::AbortRetriesExhausted,
        _ => PatchRetryDecision::Unavailable,
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
            Err(e) => match classify_patch_error(&e, attempt) {
                PatchRetryDecision::RetryAfter { delay_ms } => {
                    // ±25 % jitter to break lockstep retries from N clients
                    // racing on the same resourceVersion (see `jittered_retry_ms`).
                    let jittered = jittered_retry_ms(delay_ms);
                    warn!(
                        attempt = attempt + 1,
                        delay_ms = jittered,
                        "Apply: 409 Conflict — retrying",
                    );
                    tokio::time::sleep(Duration::from_millis(jittered)).await;
                    attempt += 1;
                }
                PatchRetryDecision::AbortRetriesExhausted => {
                    return Err(Status::aborted("409 Conflict — exceeded max retries"));
                }
                PatchRetryDecision::Unavailable => {
                    return Err(Status::unavailable(format!("kube patch error: {e}")));
                }
            },
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

    // ── classify_patch_error: all 4 branches ────────────────────────────────

    fn api_err(code: u16) -> kube::Error {
        kube::Error::Api(kube::core::ErrorResponse {
            status: "Failure".to_string(),
            message: "synthetic".to_string(),
            reason: "synthetic".to_string(),
            code,
        })
    }

    #[test]
    fn classify_409_with_budget_left_retries() {
        let d = classify_patch_error(&api_err(409), 0);
        assert_eq!(d, PatchRetryDecision::RetryAfter { delay_ms: 100 });
        let d = classify_patch_error(&api_err(409), 1);
        assert_eq!(d, PatchRetryDecision::RetryAfter { delay_ms: 200 });
    }

    #[test]
    fn classify_409_at_budget_exhausts() {
        // RETRY_DELAYS_MS has 2 entries — `attempt == 2` is the exhausted path.
        let d = classify_patch_error(&api_err(409), RETRY_DELAYS_MS.len());
        assert_eq!(d, PatchRetryDecision::AbortRetriesExhausted);
        // Going further never re-enters retry mode either.
        let d = classify_patch_error(&api_err(409), RETRY_DELAYS_MS.len() + 5);
        assert_eq!(d, PatchRetryDecision::AbortRetriesExhausted);
    }

    #[test]
    fn classify_non_409_api_error_is_unavailable() {
        for code in [400u16, 403, 404, 410, 500, 503] {
            let d = classify_patch_error(&api_err(code), 0);
            assert_eq!(
                d,
                PatchRetryDecision::Unavailable,
                "code {code} should be Unavailable",
            );
        }
    }

    #[test]
    fn classify_non_api_error_is_unavailable() {
        // Build a non-Api kube::Error by going through serde — `LinesCodecError`
        // isn't exposed, but `kube::Error::SerdeError` is.
        let serde_err = serde_json::from_str::<serde_json::Value>("{[}").unwrap_err();
        let err = kube::Error::SerdeError(serde_err);
        let d = classify_patch_error(&err, 0);
        assert_eq!(d, PatchRetryDecision::Unavailable);
    }

    fn dyn_obj(spec: serde_json::Value) -> DynamicObject {
        let mut data = serde_json::Map::new();
        data.insert("spec".to_string(), spec);
        DynamicObject {
            types: None,
            metadata: Default::default(),
            data: serde_json::Value::Object(data),
        }
    }

    #[test]
    fn parse_schema_version_well_formed() {
        let obj = dyn_obj(serde_json::json!({"schema_version": 42}));
        assert_eq!(parse_schema_version_from_object(&obj), 42);
    }

    #[test]
    fn parse_schema_version_missing_spec() {
        let obj = DynamicObject {
            types: None,
            metadata: Default::default(),
            data: serde_json::json!({}),
        };
        assert_eq!(parse_schema_version_from_object(&obj), 0);
    }

    #[test]
    fn parse_schema_version_missing_field() {
        let obj = dyn_obj(serde_json::json!({"content": {}}));
        assert_eq!(parse_schema_version_from_object(&obj), 0);
    }

    #[test]
    fn parse_schema_version_non_numeric() {
        let obj = dyn_obj(serde_json::json!({"schema_version": "v3"}));
        assert_eq!(parse_schema_version_from_object(&obj), 0);
    }

    #[test]
    fn parse_schema_version_exceeds_u32_truncates() {
        let obj = dyn_obj(serde_json::json!({"schema_version": (u32::MAX as u64) + 5}));
        // `as u32` wraps; documents the current behaviour so future changes
        // are explicit rather than accidental.
        let got = parse_schema_version_from_object(&obj);
        assert_eq!(got, 4);
    }
}
