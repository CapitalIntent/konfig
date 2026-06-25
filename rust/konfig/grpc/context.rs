//! Per-RPC request-context extraction: request-id minting/echo, client addr,
//! entry logging, schema-version read-ahead, and span status recording.

use std::sync::atomic::{AtomicU64, Ordering};

use tonic::{Request, Status};
use tracing::info;

/// Record the gRPC outcome of an RPC handler on the current tracing span as a
/// `status_code` field, then return the result unchanged.
///
/// The `#[tracing::instrument]` attribute on each RPC method declares
/// `status_code = tracing::field::Empty`; this helper fills it in once the
/// handler resolves so the OTLP exporter (and any local subscriber) carries
/// the canonical gRPC status — `"Ok"` on success, the tonic `Code` debug name
/// (`"NotFound"`, `"Unavailable"`, …) on error. Pure pass-through: the
/// `Result` is moved straight back out.
pub(crate) fn record_status<T>(result: Result<T, Status>) -> Result<T, Status> {
    let code = match &result {
        Ok(_) => "Ok".to_string(),
        Err(status) => format!("{:?}", status.code()),
    };
    tracing::Span::current().record("status_code", code.as_str());
    result
}

/// Best-effort extraction of the incoming `schema_version` from an `Apply`
/// request's YAML body, for the audit record (CU-86ahrwd6h). Returns `None`
/// when the YAML does not parse as a `ConfigSpec` — the authoritative parse
/// and its error live in `apply::handle_apply`; this is a cheap read-ahead
/// that must never affect the RPC outcome.
pub(crate) fn parse_config_schema_version(yaml_content: &str) -> Option<u32> {
    serde_yaml::from_str::<crate::types::ConfigSpec>(yaml_content)
        .ok()
        .map(|spec| spec.schema_version)
}

/// Best-effort extraction of the incoming `schema_version` from an
/// `ApplySecret` request's YAML body (a `key→value` string map with a
/// `schema_version` entry), mirroring `secret_apply::apply_secret_inner`'s
/// parse. `None` on any parse / missing-key failure — never gates the RPC.
pub(crate) fn parse_secret_schema_version(yaml_content: &str) -> Option<u32> {
    serde_yaml::from_str::<std::collections::BTreeMap<String, String>>(yaml_content)
        .ok()
        .and_then(|m| m.get("schema_version").and_then(|v| v.parse().ok()))
}

/// Monotonic counter for the locally-generated request-id suffix. Combined
/// with [`PROCESS_START_NANOS`] this yields a process-unique id without
/// pulling in a `uuid` dep (CU-86ahrwd64): collisions would require two RPCs
/// to share the same wrapping `u64` sequence number *and* the same process
/// start instant, which cannot happen within one process lifetime.
static REQUEST_SEQ: AtomicU64 = AtomicU64::new(0);

/// Process start time in nanos since the UNIX epoch — the high bits of a
/// generated request id, so ids never collide across pod restarts (a fresh
/// process re-zeroes [`REQUEST_SEQ`] but starts at a new instant). Computed
/// once on first use.
fn process_start_nanos() -> u128 {
    use std::sync::OnceLock;
    static PROCESS_START_NANOS: OnceLock<u128> = OnceLock::new();
    *PROCESS_START_NANOS.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    })
}

/// Resolve the request id for an inbound RPC: echo the caller's
/// `x-request-id` metadata header when present (so a client-supplied id
/// flows through to logs + traces for end-to-end correlation), otherwise
/// mint a lightweight process-local id. No `uuid` dep — see [`REQUEST_SEQ`].
pub(crate) fn request_id<T>(request: &Request<T>) -> String {
    if let Some(val) = request.metadata().get("x-request-id")
        && let Ok(s) = val.to_str()
        && !s.trim().is_empty()
    {
        return s.trim().to_string();
    }
    let seq = REQUEST_SEQ.fetch_add(1, Ordering::Relaxed);
    // `{start_nanos:x}-{seq:x}` — compact, sortable-ish, grep-friendly.
    format!("{:x}-{:x}", process_start_nanos(), seq)
}

/// Best-effort remote peer address for an inbound RPC, rendered for logs.
/// `unknown` when tonic could not surface a peer addr (e.g. UDS / in-process
/// test transport).
pub(crate) fn client_addr<T>(request: &Request<T>) -> String {
    request
        .remote_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Emit the single entry-level `info!` for an inbound RPC and stamp the
/// matching `client_addr` / `request_id` onto the current root span so the
/// log line and the trace carry the same correlation ids (CU-86ahrwd64).
///
/// `name` is `Some` only for the keyed RPCs (`Get`/`Apply`/`GetSecret`/…);
/// stream-of-namespace RPCs (`GetAll`/`Subscribe`/…) pass `None` and the
/// `name` field is omitted from the log line. The `namespace`/`name` span
/// fields are already populated by the `#[instrument]` macro — only
/// `client_addr`/`request_id` (declared `Empty`) are recorded here.
pub(crate) fn log_rpc_entry<T>(
    rpc: &str,
    request: &Request<T>,
    namespace: Option<&str>,
    name: Option<&str>,
) {
    let addr = client_addr(request);
    let id = request_id(request);
    let span = tracing::Span::current();
    span.record("client_addr", addr.as_str());
    span.record("request_id", id.as_str());
    match name {
        Some(name) => info!(
            rpc,
            namespace = namespace.unwrap_or(""),
            name,
            client_addr = %addr,
            request_id = %id,
            "rpc start"
        ),
        None => info!(
            rpc,
            namespace = namespace.unwrap_or(""),
            client_addr = %addr,
            request_id = %id,
            "rpc start"
        ),
    }
}
