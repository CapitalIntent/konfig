//! Audit log for mutating RPCs (CU-86ahrwd6h).
//!
//! Every `Apply` / `ApplySecret` / `Revert` emits one [`AuditRecord`] capturing
//! who (mTLS client identity + peer addr), what (rpc + namespace/name +
//! schema/resource version), and the outcome (`success` / `error:<code>`).
//!
//! Two sinks:
//!   1. **stdout JSON line — always on.** Written with `println!` (NOT
//!      `tracing`) so the audit trail is never suppressed by the tracing
//!      `EnvFilter` / `RUST_LOG` level. The single-line JSON contract lets a
//!      log shipper parse it directly; this is the canonical audit record.
//!   2. **Kubernetes Event — opt-in.** Gated behind `KONFIG_AUDIT_K8S_EVENTS=true`
//!      (default off). Best-effort: a failed Event POST is logged at `warn!`
//!      and never fails the RPC.
//!
//! OTLP export is intentionally out of scope for this PR.

use k8s_openapi::api::core::v1::{Event, ObjectReference};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, Time};
use kube::Client;
use kube::api::{Api, PostParams};
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};
use tonic::{Response, Status};
use tracing::warn;

/// Env flag that opts a pod into mirroring audit records as K8s Events.
pub const K8S_EVENTS_ENV: &str = "KONFIG_AUDIT_K8S_EVENTS";

/// One audit log entry for a mutating RPC. Serialised to a single JSON line.
#[derive(Debug, Clone, Serialize)]
pub struct AuditRecord {
    /// RPC name (`"Apply"`, `"ApplySecret"`, `"Revert"`).
    pub rpc: String,
    pub namespace: String,
    pub name: String,
    /// mTLS-derived client identity (SAN URI / CN / `"anonymous"`).
    pub client_identity: String,
    /// Peer socket address, or `"unknown"`.
    pub client_addr: String,
    /// `"success"` or `"error:<code>"`.
    pub result: String,
    /// Incoming schema_version parsed from the request body (best-effort).
    /// Omitted from JSON when unknown (Revert, or parse failure).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_version: Option<u32>,
    /// resourceVersion returned on success. Omitted on error / when empty.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_version: Option<String>,
    /// Wall-clock emission time in epoch milliseconds.
    pub timestamp_ms: i64,
    /// Correlation id echoed from `x-request-id` or process-local generated.
    pub request_id: String,
}

/// Epoch milliseconds, clamped to `i64`. A clock before the epoch yields `0`
/// rather than a negative value (audit records are forward-only).
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Map a handler `Result` to the audit `result` string: `"success"` on `Ok`,
/// `"error:<snake_code>"` on `Err` (e.g. `error:failed_precondition`).
pub fn result_str<T>(result: &Result<T, Status>) -> String {
    match result {
        Ok(_) => "success".to_string(),
        Err(status) => format!("error:{}", code_snake(status.code())),
    }
}

/// Canonical lowercase-snake name for a `tonic::Code` (matches the gRPC
/// status-code names used across the konfig logs).
fn code_snake(code: tonic::Code) -> &'static str {
    use tonic::Code::*;
    match code {
        Ok => "ok",
        Cancelled => "cancelled",
        Unknown => "unknown",
        InvalidArgument => "invalid_argument",
        DeadlineExceeded => "deadline_exceeded",
        NotFound => "not_found",
        AlreadyExists => "already_exists",
        PermissionDenied => "permission_denied",
        ResourceExhausted => "resource_exhausted",
        FailedPrecondition => "failed_precondition",
        Aborted => "aborted",
        OutOfRange => "out_of_range",
        Unimplemented => "unimplemented",
        Internal => "internal",
        Unavailable => "unavailable",
        DataLoss => "data_loss",
        Unauthenticated => "unauthenticated",
    }
}

/// Convenience: pull the `resource_version` off a successful response, dropping
/// empty strings. Used by the call sites to fill [`AuditRecord::resource_version`].
pub fn resource_version_of<T, F>(result: &Result<Response<T>, Status>, extract: F) -> Option<String>
where
    F: Fn(&T) -> &str,
{
    result
        .as_ref()
        .ok()
        .map(|r| extract(r.get_ref()).to_string())
        .filter(|s| !s.is_empty())
}

/// Emit the audit record to stdout as a single JSON line.
///
/// Uses `println!` deliberately: the audit trail MUST appear regardless of the
/// tracing `EnvFilter` level, so it bypasses the `tracing` subscriber entirely.
/// Serialisation cannot realistically fail (all fields are plain owned data);
/// on the impossible error we fall back to a debug rendering so a record is
/// never silently dropped.
pub fn emit(rec: &AuditRecord) {
    match serde_json::to_string(rec) {
        Ok(json) => println!("{json}"),
        Err(e) => println!(
            "{{\"audit_serialize_error\":\"{e}\",\"rpc\":\"{}\"}}",
            rec.rpc
        ),
    }
}

/// Best-effort: mirror the audit record as a Kubernetes Event on the affected
/// namespace when `KONFIG_AUDIT_K8S_EVENTS=true`.
///
/// Never fails the RPC: a missing flag is a no-op, and a failed Event POST is
/// logged at `warn!` and swallowed. The Event `type` is `Normal` on success
/// and `Warning` on error; `reason` is the RPC name.
pub async fn maybe_emit_k8s_event(client: &Client, rec: &AuditRecord) {
    if std::env::var(K8S_EVENTS_ENV).as_deref() != Ok("true") {
        return;
    }

    let is_error = rec.result.starts_with("error:");
    let event = Event {
        metadata: ObjectMeta {
            generate_name: Some("konfig-audit-".to_string()),
            namespace: Some(rec.namespace.clone()),
            ..Default::default()
        },
        involved_object: ObjectReference {
            api_version: Some("konfig.io/v1".to_string()),
            kind: Some("Config".to_string()),
            namespace: Some(rec.namespace.clone()),
            name: Some(rec.name.clone()),
            ..Default::default()
        },
        reason: Some(rec.rpc.clone()),
        message: Some(format!(
            "{} by {} → {}",
            rec.rpc, rec.client_identity, rec.result
        )),
        type_: Some(if is_error { "Warning" } else { "Normal" }.to_string()),
        reporting_component: Some("konfig".to_string()),
        event_time: Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime(
            k8s_openapi::chrono::Utc::now(),
        )),
        last_timestamp: Some(Time(k8s_openapi::chrono::Utc::now())),
        first_timestamp: Some(Time(k8s_openapi::chrono::Utc::now())),
        count: Some(1),
        ..Default::default()
    };

    let api: Api<Event> = Api::namespaced(client.clone(), &rec.namespace);
    if let Err(e) = api.create(&PostParams::default(), &event).await {
        warn!(
            rpc = %rec.rpc,
            namespace = %rec.namespace,
            name = %rec.name,
            error = %e,
            "audit K8s Event emission failed (best-effort, ignored)"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    fn sample(result: &str, schema_version: Option<u32>, rv: Option<String>) -> AuditRecord {
        AuditRecord {
            rpc: "Apply".to_string(),
            namespace: "default".to_string(),
            name: "cfg-x".to_string(),
            client_identity: "spiffe://konfig/client/test".to_string(),
            client_addr: "10.0.0.1:5443".to_string(),
            result: result.to_string(),
            schema_version,
            resource_version: rv,
            timestamp_ms: 1_700_000_000_000,
            request_id: "abc-123".to_string(),
        }
    }

    /// The serialised record carries every required field with the expected
    /// values — this is the canonical stdout audit-line contract.
    #[test]
    fn record_serializes_with_required_fields() {
        let rec = sample("success", Some(7), Some("rv-42".to_string()));
        let json: Value = serde_json::from_str(&serde_json::to_string(&rec).unwrap()).unwrap();
        assert_eq!(json["rpc"], "Apply");
        assert_eq!(json["namespace"], "default");
        assert_eq!(json["name"], "cfg-x");
        assert_eq!(json["client_identity"], "spiffe://konfig/client/test");
        assert_eq!(json["client_addr"], "10.0.0.1:5443");
        assert_eq!(json["result"], "success");
        assert_eq!(json["schema_version"], 7);
        assert_eq!(json["resource_version"], "rv-42");
        assert_eq!(json["timestamp_ms"], 1_700_000_000_000_i64);
        assert_eq!(json["request_id"], "abc-123");
    }

    /// Optional fields are omitted (not null) when `None` so the line stays
    /// compact and consumers can distinguish absent vs empty.
    #[test]
    fn optional_fields_omitted_when_none() {
        let rec = sample("error:failed_precondition", None, None);
        let json = serde_json::to_string(&rec).unwrap();
        assert!(!json.contains("schema_version"), "got: {json}");
        assert!(!json.contains("resource_version"), "got: {json}");
        // Required fields still present.
        assert!(json.contains("\"result\":\"error:failed_precondition\""));
    }

    /// `result_str` maps Ok → success and each error code → `error:<snake>`.
    #[test]
    fn result_str_maps_ok_and_codes() {
        let ok: Result<(), Status> = Ok(());
        assert_eq!(result_str(&ok), "success");

        let fp: Result<(), Status> = Err(Status::failed_precondition("nope"));
        assert_eq!(result_str(&fp), "error:failed_precondition");

        let inv: Result<(), Status> = Err(Status::invalid_argument("bad"));
        assert_eq!(result_str(&inv), "error:invalid_argument");

        let na: Result<(), Status> = Err(Status::unavailable("draining"));
        assert_eq!(result_str(&na), "error:unavailable");
    }

    /// `resource_version_of` extracts a non-empty RV on success and yields
    /// `None` on error or when the RV is empty.
    #[test]
    fn resource_version_of_extracts_and_filters() {
        let ok: Result<Response<String>, Status> = Ok(Response::new("rv-9".to_string()));
        assert_eq!(
            resource_version_of(&ok, |s| s.as_str()),
            Some("rv-9".to_string())
        );

        let empty: Result<Response<String>, Status> = Ok(Response::new(String::new()));
        assert_eq!(resource_version_of(&empty, |s| s.as_str()), None);

        let err: Result<Response<String>, Status> = Err(Status::internal("boom"));
        assert_eq!(resource_version_of(&err, |s| s.as_str()), None);
    }

    /// `now_ms` returns a positive, plausible epoch-millis value.
    #[test]
    fn now_ms_is_positive() {
        assert!(now_ms() > 1_700_000_000_000);
    }
}
