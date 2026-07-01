//! HTTP/JSON gateway for `konfig.v1.KonfigService` (CU-86ahrwd70).
//!
//! Browsers cannot speak gRPC, so the Backstage UI (Phase 6) needs an
//! HTTP-native surface. This module mounts a small [`axum`] server on a
//! **sibling port** (`--http-addr`, see [`crate::startup`]) that accepts plain
//! `POST /konfig.v1.KonfigService/<Method>` requests with a JSON body and
//! returns a JSON response. It reuses the existing [`KonfigServer`] handlers by
//! constructing an in-process `tonic::Request` and calling the same trait
//! methods, so every gate (drain, per-tenant authz, quota, audit, metrics)
//! still runs — the gateway is a thin transcoder, not a second implementation.
//!
//! # Surface
//!
//! Full **unary** parity: `Get`, `GetAll`, `Apply`, `BatchApply`,
//! `DryRunApply`, `Revert`, `GetSecret`, `GetAllSecrets`, `ApplySecret`. The two
//! finite server-streams (`GetAll` / `GetAllSecrets`) are written out as a JSON
//! array one element at a time, so the whole namespace is never buffered in
//! memory at once (see [`stream_json_array`]). The infinite streams
//! (`Subscribe` / `SubscribeSecrets`) return `501` pointing at the SSE endpoint
//! (separate task) — they are not transcodable to a single JSON response.
//!
//! # Security (read before enabling)
//!
//! This port is **plaintext** and carries no mTLS client certificate. With no
//! certificate konfig cannot tell *who* is calling, so it treats every gateway
//! caller as the built-in `anonymous` tenant. Two consequences:
//!   - With `KONFIG_AUTHZ_MODE=disabled` (the default) every method works; under
//!     `enforce`, anonymous is denied (run the gateway behind a trusted caller,
//!     or grant an ACL identity in a follow-up).
//!   - It exposes writes (`Apply`/`Revert`/…) and secret reads (`GetSecret`).
//!     The gateway therefore REQUIRES a bearer token ([`HttpGatewayConfig::auth_token`])
//!     unless the operator passes `--http-insecure`; [`crate::startup`] fails
//!     fast otherwise. Run it cluster-internal.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use futures_util::StreamExt;
use tonic::{Code, Request, Status};
use tracing::{error, info};

use crate::grpc::KonfigServer;
use crate::proto::konfig_service_server::KonfigService;
use crate::proto::{
    ConfigEvent, GetAllRequest, GetRequest, SecretEvent, SubscribeRequest, SubscribeSecretsRequest,
};

/// Max accepted JSON request body. Mirrors tonic's default 4 MiB
/// `max_decoding_message_size` so a payload that the gRPC server would accept
/// is not rejected with a `413` just because it arrived over the gateway.
/// A larger body short-circuits with `413 Payload Too Large` before it is read
/// into memory (axum's `DefaultBodyLimit`), so it also caps per-request RAM.
const MAX_HTTP_BODY_BYTES: usize = 4 * 1024 * 1024;

/// Resolved gateway auth + CORS configuration (built in [`crate::startup`]).
#[derive(Clone, Debug)]
pub struct HttpGatewayConfig {
    /// Sibling listen address (`--http-addr` / `KONFIG_HTTP_ADDR`).
    pub addr: SocketAddr,
    /// Bearer token required in `Authorization: Bearer <token>`. Always `Some`
    /// when [`Self::insecure`] is false (startup enforces this).
    pub auth_token: Option<String>,
    /// Value emitted as `Access-Control-Allow-Origin`. `None` ⇒ no CORS headers
    /// (same-origin only — the safe default).
    pub cors_allow_origin: Option<String>,
    /// When `true` the bearer-token gate is skipped (explicit `--http-insecure`).
    pub insecure: bool,
}

/// Shared handler state: the cloneable server + the resolved config.
struct GatewayState {
    server: KonfigServer,
    cfg: HttpGatewayConfig,
}

/// Run the HTTP/JSON gateway until the process drains. Spawned as a detached
/// background task by [`crate::grpc::serve`] (mirrors the `/metrics` server).
/// The `listener` is bound by `serve` on the startup path, so a port clash
/// fails startup loudly there rather than silently killing this detached task.
/// Errors from the serve loop itself are logged and end the task without
/// crashing the gRPC server.
///
/// On drain (SIGTERM) the gateway stops accepting NEW connections and lets
/// in-flight requests finish, instead of being cut mid-response when the
/// process exits. It shares the gRPC server's drain signal, so requests that
/// are still in flight during the drain window keep returning the same
/// `UNAVAILABLE` (`503`) the handlers already emit while draining.
pub async fn serve_gateway(
    server: KonfigServer,
    cfg: HttpGatewayConfig,
    listener: tokio::net::TcpListener,
) {
    let addr = cfg.addr;
    // Grab the shared drain notifier before `server` is moved into the state.
    // It is woken once, when the gRPC server begins draining (see `serve`).
    let drain_notify = server.drain_notify();
    let state = Arc::new(GatewayState { server, cfg });
    // Single dispatch handler keyed by the `{rpc}` path segment. `any` so the
    // CORS preflight `OPTIONS` reaches us too (method-specific routers would
    // 405 it before we can answer the preflight). The body limit rejects an
    // oversized payload with `413` before it is buffered into memory.
    let app = Router::new()
        .route("/konfig.v1.KonfigService/{rpc}", any(dispatch))
        // SSE watch endpoints (text/event-stream). One config/secret per path;
        // GET-only because EventSource only speaks GET. Each re-frames the same
        // in-process Subscribe/SubscribeSecrets fan-out the gRPC clients use.
        .route(
            "/v1/configs/{namespace}/{name}/watch",
            get(sse_config_watch),
        )
        .route(
            "/v1/secrets/{namespace}/{name}/watch",
            get(sse_secret_watch),
        )
        // REST GET aliases (one-shot JSON) for clients that prefer polling over
        // streaming. Configs only — secret reads stay on the gRPC-style POST
        // path (audit-logged). `/healthz` is the unauthenticated K8s probe.
        .route(
            "/v1/configs/{namespace}/{name}",
            get(rest_get_config).options(cors_preflight_handler),
        )
        .route(
            "/v1/configs/{namespace}",
            get(rest_get_all_configs).options(cors_preflight_handler),
        )
        .route("/healthz", get(healthz))
        .layer(DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES))
        .with_state(state);

    info!(%addr, "HTTP/JSON gateway starting");
    let shutdown = async move { drain_notify.notified().await };
    if let Err(e) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
    {
        error!("HTTP/JSON gateway server error: {e}");
    }
    info!(%addr, "HTTP/JSON gateway stopped");
}

/// Map a gRPC [`Code`] to the closest HTTP status. Pure — unit-tested.
fn status_to_http(code: Code) -> StatusCode {
    match code {
        Code::Ok => StatusCode::OK,
        Code::InvalidArgument => StatusCode::BAD_REQUEST,
        Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        Code::PermissionDenied => StatusCode::FORBIDDEN,
        Code::NotFound => StatusCode::NOT_FOUND,
        Code::AlreadyExists => StatusCode::CONFLICT,
        Code::FailedPrecondition => StatusCode::PRECONDITION_FAILED,
        Code::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        Code::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
        Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
        // Internal, Unknown, DataLoss, Aborted, Cancelled, OutOfRange → 500.
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Stable snake_case token for a gRPC [`Code`], surfaced in the JSON `code`
/// field so HTTP clients can branch on it without parsing the message.
fn code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "ok",
        Code::Cancelled => "cancelled",
        Code::Unknown => "unknown",
        Code::InvalidArgument => "invalid_argument",
        Code::DeadlineExceeded => "deadline_exceeded",
        Code::NotFound => "not_found",
        Code::AlreadyExists => "already_exists",
        Code::PermissionDenied => "permission_denied",
        Code::ResourceExhausted => "resource_exhausted",
        Code::FailedPrecondition => "failed_precondition",
        Code::Aborted => "aborted",
        Code::OutOfRange => "out_of_range",
        Code::Unimplemented => "unimplemented",
        Code::Internal => "internal",
        Code::Unavailable => "unavailable",
        Code::DataLoss => "data_loss",
        Code::Unauthenticated => "unauthenticated",
    }
}

/// Error triple carried out of the per-method runners: HTTP status + the
/// snake_case gRPC code + a human message. Rendered as the JSON error body.
type GatewayError = (StatusCode, &'static str, String);

fn status_to_error(s: tonic::Status) -> GatewayError {
    (
        status_to_http(s.code()),
        code_name(s.code()),
        s.message().to_string(),
    )
}

/// Serialise a JSON error body `{"code": "...", "message": "..."}`.
fn error_body(code: &str, message: &str) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({ "code": code, "message": message }))
        .unwrap_or_else(|_| b"{\"code\":\"internal\",\"message\":\"error encode failed\"}".to_vec())
}

/// Compare two tokens in constant time — i.e. always look at every byte
/// instead of stopping at the first mismatch. A normal `==` returns faster
/// when the first byte differs, and an attacker can measure that timing to
/// guess the token one byte at a time; this loop removes that signal. The
/// length is still allowed to leak (we bail early on a length mismatch).
fn tokens_match(provided: &str, expected: &str) -> bool {
    let (a, b) = (provided.as_bytes(), expected.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Append `Access-Control-Allow-Origin` to a response builder when an origin is
/// configured. No-op (same-origin only) when unset.
fn with_cors(
    builder: axum::http::response::Builder,
    cfg: &HttpGatewayConfig,
) -> axum::http::response::Builder {
    match cfg.cors_allow_origin.as_deref() {
        Some(origin) if !origin.is_empty() => builder
            .header(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin)
            // Tell shared caches the response varies by request `Origin`, so a
            // cached reply for one origin is never replayed to another.
            .header(header::VARY, header::ORIGIN.as_str()),
        _ => builder,
    }
}

/// Build a JSON response with the given status + CORS headers.
fn json_response(cfg: &HttpGatewayConfig, status: StatusCode, body: Vec<u8>) -> Response {
    let builder = with_cors(
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json"),
        cfg,
    );
    builder
        .body(Body::from(body))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// Route handler for the REST GET aliases' CORS preflight — a browser `fetch`
/// with an `Authorization` header preflights, and a `get()`-only route would
/// `405` it. Delegates to [`cors_preflight`] (unauthenticated, by spec).
async fn cors_preflight_handler(State(state): State<Arc<GatewayState>>) -> Response {
    cors_preflight(&state.cfg)
}

/// Answer a CORS preflight (`OPTIONS`) with `204` + the allow-* headers.
fn cors_preflight(cfg: &HttpGatewayConfig) -> Response {
    let builder = with_cors(Response::builder().status(StatusCode::NO_CONTENT), cfg)
        .header(header::ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, OPTIONS")
        .header(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            "content-type, authorization",
        )
        .header(header::ACCESS_CONTROL_MAX_AGE, "600");
    builder
        .body(Body::empty())
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// Pull the token out of an `Authorization` header value, accepting the scheme
/// name in any case (`Bearer` / `bearer` / `BEARER`). RFC 6750 says the scheme
/// is case-insensitive, and some clients send it lowercase. Returns the token
/// part (caller trims surrounding spaces), or `None` when the value is not a
/// `Bearer` credential.
fn strip_bearer_prefix(value: &str) -> Option<&str> {
    let (scheme, token) = value.trim_start().split_once(' ')?;
    scheme.eq_ignore_ascii_case("Bearer").then_some(token)
}

/// Bearer-token gate. Returns `None` when authorized (or when `--http-insecure`
/// is set); `Some(response)` carries the ready-to-return `401` otherwise.
fn check_auth(cfg: &HttpGatewayConfig, headers: &HeaderMap) -> Option<Response> {
    if cfg.insecure {
        return None;
    }
    // `auth_token` is guaranteed `Some` when not insecure (startup enforces it);
    // treat a missing token defensively as "deny everything".
    let expected = cfg.auth_token.as_deref().unwrap_or("");
    let provided = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(strip_bearer_prefix)
        .map(str::trim)
        .unwrap_or("");
    if !expected.is_empty() && tokens_match(provided, expected) {
        None
    } else {
        Some(json_response(
            cfg,
            StatusCode::UNAUTHORIZED,
            error_body("unauthenticated", "missing or invalid bearer token"),
        ))
    }
}

/// Turn a JSON request body into the proto request type `Req`. An empty body
/// becomes the default message (every field at its zero value). This is
/// "proto3-JSON leniency": a field the client leaves out is filled with its
/// default rather than rejected, so `{}` and an empty body are both valid.
fn decode_request<Req>(body: &Bytes) -> Result<Req, GatewayError>
where
    Req: serde::de::DeserializeOwned + Default,
{
    if body.is_empty() {
        return Ok(Req::default());
    }
    serde_json::from_slice::<Req>(body).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            format!("invalid JSON request body: {e}"),
        )
    })
}

fn encode_response<Resp: serde::Serialize>(resp: &Resp) -> Result<Vec<u8>, GatewayError> {
    serde_json::to_vec(resp).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            format!("response serialize failed: {e}"),
        )
    })
}

/// Run a unary RPC: decode JSON → call the handler → encode the response.
async fn run_unary<Req, Resp, Fut>(
    body: &Bytes,
    call: impl FnOnce(Request<Req>) -> Fut,
) -> Result<Vec<u8>, GatewayError>
where
    Req: serde::de::DeserializeOwned + Default,
    Resp: serde::Serialize,
    Fut: std::future::Future<Output = Result<tonic::Response<Resp>, tonic::Status>>,
{
    let req = decode_request::<Req>(body)?;
    let resp = call(Request::new(req)).await.map_err(status_to_error)?;
    encode_response(resp.get_ref())
}

/// Run a finite server-streaming RPC and write the result as a JSON array
/// **streamed one element at a time** — the whole namespace is never collected
/// into memory at once (see [`stream_json_array`]).
///
/// The two "early" failures still map to a proper HTTP status, because they
/// happen before any byte ships: a bad JSON body → `400`, and an error
/// `Status` from the handler call itself (drain `Unavailable`, authz denial,
/// …) → its mapped status. Once the first array byte is on the wire the HTTP
/// status is locked at `200`, so an error that surfaces mid-stream can only
/// close the array — it cannot downgrade the status.
async fn run_server_streaming<Req, Item, S, Fut>(
    cfg: &HttpGatewayConfig,
    body: &Bytes,
    call: impl FnOnce(Request<Req>) -> Fut,
) -> Response
where
    Req: serde::de::DeserializeOwned + Default,
    Item: serde::Serialize + Send + 'static,
    S: futures_util::Stream<Item = Result<Item, tonic::Status>> + Send + Unpin + 'static,
    Fut: std::future::Future<Output = Result<tonic::Response<S>, tonic::Status>>,
{
    let req = match decode_request::<Req>(body) {
        Ok(req) => req,
        Err((status, code, message)) => {
            return json_response(cfg, status, error_body(code, &message));
        }
    };
    let stream = match call(Request::new(req)).await {
        Ok(resp) => resp.into_inner(),
        Err(status) => {
            let (status, code, message) = status_to_error(status);
            return json_response(cfg, status, error_body(code, &message));
        }
    };
    json_streaming_response(cfg, stream_json_array(stream))
}

/// Serialise a finite server-stream into a JSON array incrementally: emit `[`,
/// then each item separated by `,`, then `]`. Only ONE item is ever held in
/// memory at a time — there is no `Vec` collecting the whole namespace.
///
/// The HTTP status is already `200` once the first byte ships (see
/// [`run_server_streaming`]), so a mid-stream gRPC error can no longer change
/// it. We close the array and stop so the body stays valid JSON.
fn stream_json_array<Item, S>(
    stream: S,
) -> impl futures_util::Stream<Item = Result<Vec<u8>, std::convert::Infallible>> + Send + 'static
where
    Item: serde::Serialize + Send + 'static,
    S: futures_util::Stream<Item = Result<Item, tonic::Status>> + Send + Unpin + 'static,
{
    // State threaded through the fold: (the upstream stream, items emitted so
    // far, whether we already wrote the closing `]`).
    futures_util::stream::unfold(
        (stream, 0usize, false),
        |(mut stream, count, finished)| async move {
            if finished {
                return None;
            }
            match stream.next().await {
                Some(Ok(item)) => {
                    let mut buf = Vec::with_capacity(64);
                    buf.push(if count == 0 { b'[' } else { b',' });
                    if serde_json::to_writer(&mut buf, &item).is_err() {
                        // A prost message failing to serialise is not expected;
                        // if it ever does, close the array so the body stays
                        // valid JSON instead of trailing off mid-element.
                        return Some((Ok(close_array(count)), (stream, count, true)));
                    }
                    Some((Ok(buf), (stream, count + 1, false)))
                }
                // Status already on the wire as 200 — cannot downgrade. Close.
                Some(Err(_)) => Some((Ok(close_array(count)), (stream, count, true))),
                None => Some((Ok(close_array(count)), (stream, count, true))),
            }
        },
    )
}

/// Closing bytes for the streamed JSON array: `]` after ≥1 item, `[]` when the
/// stream was empty (so an empty namespace returns a valid empty array).
fn close_array(count: usize) -> Vec<u8> {
    if count == 0 {
        vec![b'[', b']']
    } else {
        vec![b']']
    }
}

/// `200` response whose body is the streamed JSON array from
/// [`stream_json_array`], carrying the usual content-type + CORS headers.
fn json_streaming_response(
    cfg: &HttpGatewayConfig,
    bytes: impl futures_util::Stream<Item = Result<Vec<u8>, std::convert::Infallible>> + Send + 'static,
) -> Response {
    let builder = with_cors(
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json"),
        cfg,
    );
    builder
        .body(Body::from_stream(bytes))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// Single dispatch entry point for every `/konfig.v1.KonfigService/<rpc>` call.
async fn dispatch(
    State(state): State<Arc<GatewayState>>,
    method: Method,
    headers: HeaderMap,
    Path(rpc): Path<String>,
    body: Bytes,
) -> Response {
    let cfg = &state.cfg;

    // CORS preflight is unauthenticated by spec — answer before the token gate.
    if method == Method::OPTIONS {
        return cors_preflight(cfg);
    }
    if method != Method::POST {
        return json_response(
            cfg,
            StatusCode::METHOD_NOT_ALLOWED,
            error_body("method_not_allowed", "only POST and OPTIONS are supported"),
        );
    }
    if let Some(resp) = check_auth(cfg, &headers) {
        return resp;
    }

    // Borrow the shared server — every handler takes `&self` and its future is
    // awaited inline below, so there's no need to clone the (~20-`Arc`)
    // `KonfigServer` per request.
    let server = &state.server;
    let outcome = match rpc.as_str() {
        "Get" => run_unary(&body, |r| server.get(r)).await,
        "GetAll" => return run_server_streaming(cfg, &body, |r| server.get_all(r)).await,
        "Apply" => run_unary(&body, |r| server.apply(r)).await,
        "BatchApply" => run_unary(&body, |r| server.batch_apply(r)).await,
        "DryRunApply" => run_unary(&body, |r| server.dry_run_apply(r)).await,
        "Revert" => run_unary(&body, |r| server.revert(r)).await,
        "GetSecret" => run_unary(&body, |r| server.get_secret(r)).await,
        "GetAllSecrets" => {
            return run_server_streaming(cfg, &body, |r| server.get_all_secrets(r)).await;
        }
        "ApplySecret" => run_unary(&body, |r| server.apply_secret(r)).await,
        // These RPCs exist but are infinite streams — not transcodable to one
        // JSON body. `501` says "the method is real, just not served here".
        "Subscribe" | "SubscribeSecrets" => {
            return json_response(
                cfg,
                StatusCode::NOT_IMPLEMENTED,
                error_body(
                    "unimplemented",
                    "streaming RPCs are served via the SSE endpoint, not the JSON gateway",
                ),
            );
        }
        // No such method on the service (typo / wrong path) → `404`, distinct
        // from the `501` above which means "real method, not over JSON".
        other => {
            return json_response(
                cfg,
                StatusCode::NOT_FOUND,
                error_body("not_found", &format!("unknown method '{other}'")),
            );
        }
    };

    match outcome {
        Ok(json) => json_response(cfg, StatusCode::OK, json),
        Err((status, code, message)) => json_response(cfg, status, error_body(code, &message)),
    }
}

// ── REST GET aliases (one-shot JSON) + readiness ────────────────────────────
//
// `GET /v1/configs/{ns}/{name}` and `GET /v1/configs/{ns}` are polling-friendly
// aliases for the unary `Get` / server-streaming `GetAll` RPCs — same auth gate
// and same in-process handlers as the POST dispatch path. Configs only: secret
// reads stay on the audit-logged POST path.

/// `GET /v1/configs/{namespace}/{name}` — one-shot JSON fetch (unary `Get`).
async fn rest_get_config(
    State(state): State<Arc<GatewayState>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let cfg = &state.cfg;
    if let Some(deny) = check_auth(cfg, &headers) {
        return deny;
    }
    match state
        .server
        .get(Request::new(GetRequest { namespace, name }))
        .await
    {
        Ok(resp) => match encode_response(resp.get_ref()) {
            Ok(json) => json_response(cfg, StatusCode::OK, json),
            Err((status, code, message)) => json_response(cfg, status, error_body(code, &message)),
        },
        Err(status) => {
            let (status, code, message) = status_to_error(status);
            json_response(cfg, status, error_body(code, &message))
        }
    }
}

/// `GET /v1/configs/{namespace}` — list all configs in the namespace as a
/// streamed JSON array (server-streaming `GetAll`; one element at a time, never
/// the whole namespace buffered in memory).
async fn rest_get_all_configs(
    State(state): State<Arc<GatewayState>>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
) -> Response {
    let cfg = &state.cfg;
    if let Some(deny) = check_auth(cfg, &headers) {
        return deny;
    }
    match state
        .server
        .get_all(Request::new(GetAllRequest { namespace }))
        .await
    {
        Ok(resp) => json_streaming_response(cfg, stream_json_array(resp.into_inner())),
        Err(status) => {
            let (status, code, message) = status_to_error(status);
            json_response(cfg, status, error_body(code, &message))
        }
    }
}

/// `GET /healthz` — readiness probe for K8s. **Unauthenticated** (liveness /
/// readiness probes carry no bearer token). `200
/// {"status":"ok","cache_ready":true}` once the config cache has completed its
/// first list (the same gate the gRPC health check + SSE use); `503
/// {"status":"warming","cache_ready":false}` (Retry-After: 1) while warming.
async fn healthz(State(state): State<Arc<GatewayState>>) -> Response {
    let cfg = &state.cfg;
    let ready = state.server.cache.is_populated();
    let (status, status_str) = if ready {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "warming")
    };
    let body = serde_json::to_vec(&serde_json::json!({
        "status": status_str,
        "cache_ready": ready,
    }))
    .unwrap_or_else(|_| b"{\"status\":\"error\",\"cache_ready\":false}".to_vec());
    let mut builder = with_cors(
        Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json"),
        cfg,
    );
    if !ready {
        builder = builder.header(header::RETRY_AFTER, "1");
    }
    builder
        .body(Body::from(body))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

// ── SSE (Server-Sent Events) watch endpoints ────────────────────────────────
//
// `GET /v1/{configs,secrets}/{ns}/{name}/watch` re-frames the SAME in-process
// `Subscribe`/`SubscribeSecrets` stream the gRPC clients use as a long-lived
// `text/event-stream`. No new watch logic, channels, or replay buffer — this is
// a thin presentation layer that turns each proto event into one SSE frame.

/// Heartbeat cadence. Browsers/proxies drop idle connections; an SSE comment
/// (`: keep-alive`) every 15 s keeps the pipe warm without disturbing the
/// client (comment lines are ignored by the EventSource parser).
const SSE_KEEPALIVE: Duration = Duration::from_secs(15);

/// Proto `EventType` (same numbering for config & secret events) → the SSE
/// `event:` name clients switch on. Unknown values fall back to `MODIFIED` (a
/// re-fetch is always safe), never panicking on a future enum addition.
fn event_type_name(event_type: i32) -> &'static str {
    match event_type {
        0 => "ADDED",
        1 => "MODIFIED",
        2 => "DELETED",
        3 => "SNAPSHOT",
        _ => "MODIFIED",
    }
}

/// Build ONE SSE frame: `event:` line, an optional `id:` line (the
/// resource_version — this is what the browser echoes back as `Last-Event-ID`
/// to resume), then the `data:` payload and the blank-line terminator. `data`
/// is single-line JSON (serde never emits raw newlines), so one `data:` line is
/// always valid.
fn sse_frame(event: &str, id: &str, data: &str) -> Vec<u8> {
    let mut buf = String::with_capacity(data.len() + event.len() + id.len() + 24);
    buf.push_str("event: ");
    buf.push_str(event);
    buf.push('\n');
    if !id.is_empty() {
        buf.push_str("id: ");
        buf.push_str(id);
        buf.push('\n');
    }
    buf.push_str("data: ");
    buf.push_str(data);
    buf.push_str("\n\n");
    buf.into_bytes()
}

/// One `ConfigEvent` → its SSE frame. `id` = resource_version (resume cursor),
/// `data` = the proto3-JSON `Config` (same `content_json` shape as `Get`).
fn frame_config_event(event: &ConfigEvent) -> Vec<u8> {
    let name = event_type_name(event.event_type);
    match &event.config {
        Some(config) => {
            let data = serde_json::to_string(config).unwrap_or_else(|_| "{}".to_string());
            sse_frame(name, &config.resource_version, &data)
        }
        None => sse_frame(name, "", "{}"),
    }
}

/// One `SecretEvent` → its SSE frame. Payload is the proto3-JSON
/// `SecretResponse` with `data_json` left base64-encoded — identical to the
/// gRPC `GetSecret` policy (values are never decoded server-side).
fn frame_secret_event(event: &SecretEvent) -> Vec<u8> {
    let name = event_type_name(event.event_type);
    match &event.secret {
        Some(secret) => {
            let data = serde_json::to_string(secret).unwrap_or_else(|_| "{}".to_string());
            sse_frame(name, &secret.resource_version, &data)
        }
        None => sse_frame(name, "", "{}"),
    }
}

/// A mid-stream gRPC error → a TERMINAL SSE frame (the stream ends right after).
/// Lag (`RESOURCE_EXHAUSTED`, the subscriber fell behind the broadcast) becomes
/// a `RELOAD` signal — the client must re-fetch a fresh snapshot. Any other
/// error becomes an `error` frame carrying the `{code,message}`. The HTTP status
/// is already `200` once streaming, so this is the only way to surface a fault.
fn terminal_frame(status: &Status) -> Vec<u8> {
    if status.code() == Code::ResourceExhausted {
        let data = serde_json::json!({ "reason": "lagged", "message": status.message() });
        sse_frame("RELOAD", "", &data.to_string())
    } else {
        let data =
            serde_json::json!({ "code": code_name(status.code()), "message": status.message() });
        sse_frame("error", "", &data.to_string())
    }
}

/// The testable core: adapt a `Subscribe`-style stream into a stream of SSE
/// wire-bytes. `to_frame` renders each successful event; an `Err(Status)` is
/// rendered by [`terminal_frame`] and ends the stream; clean upstream end ends
/// the stream. Between events a keep-alive comment is emitted every
/// [`SSE_KEEPALIVE`] so idle connections survive proxy timeouts.
fn sse_event_stream<Item, S, F>(
    stream: S,
    to_frame: F,
) -> impl futures_util::Stream<Item = Result<Vec<u8>, std::convert::Infallible>> + Send + 'static
where
    Item: Send + 'static,
    S: futures_util::Stream<Item = Result<Item, Status>> + Send + Unpin + 'static,
    F: Fn(&Item) -> Vec<u8> + Send + 'static,
{
    // First tick fires after one full period, not immediately, so a chatty
    // stream never has a stray leading keep-alive comment.
    let keepalive =
        tokio::time::interval_at(tokio::time::Instant::now() + SSE_KEEPALIVE, SSE_KEEPALIVE);
    futures_util::stream::unfold(
        (stream, to_frame, keepalive, false),
        |(mut stream, to_frame, mut keepalive, finished)| async move {
            if finished {
                return None;
            }
            tokio::select! {
                item = stream.next() => match item {
                    Some(Ok(event)) => {
                        let frame = to_frame(&event);
                        Some((Ok(frame), (stream, to_frame, keepalive, false)))
                    }
                    // Terminal: emit the RELOAD/error frame, then stop.
                    Some(Err(status)) => {
                        let frame = terminal_frame(&status);
                        Some((Ok(frame), (stream, to_frame, keepalive, true)))
                    }
                    // Upstream closed (drain, server stop) → end the SSE cleanly.
                    None => None,
                },
                _ = keepalive.tick() => {
                    Some((Ok(b": keep-alive\n\n".to_vec()), (stream, to_frame, keepalive, false)))
                }
            }
        },
    )
}

/// `200 text/event-stream` response carrying the SSE byte stream, the usual CORS
/// headers, `no-cache`, and `X-Accel-Buffering: no` so nginx-style proxies flush
/// each frame instead of buffering the whole (never-ending) response.
fn sse_response(
    cfg: &HttpGatewayConfig,
    bytes: impl futures_util::Stream<Item = Result<Vec<u8>, std::convert::Infallible>> + Send + 'static,
) -> Response {
    let builder = with_cors(
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/event-stream")
            .header(header::CACHE_CONTROL, "no-cache")
            .header("x-accel-buffering", "no"),
        cfg,
    );
    builder
        .body(Body::from_stream(bytes))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// `503 Retry-After: 1` JSON response for the cold-start window before the
/// watcher's first list has warmed the cache (same readiness gate the gRPC
/// health check uses). The client should retry shortly.
fn cache_not_ready(cfg: &HttpGatewayConfig) -> Response {
    let builder = with_cors(
        Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header(header::RETRY_AFTER, "1")
            .header(header::CONTENT_TYPE, "application/json"),
        cfg,
    );
    builder
        .body(Body::from(error_body(
            "unavailable",
            "cache not ready; retry shortly",
        )))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}

/// The `Last-Event-ID` header (the resource_version of the last frame the client
/// saw) → `resume_resource_version`, so a reconnect replays only what was
/// missed. Absent/garbage header → empty string (cold subscribe from snapshot).
fn last_event_id(headers: &HeaderMap) -> String {
    headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

/// Build the in-process `SubscribeRequest` for a single-config SSE watch.
/// `Last-Event-ID` → `resume_resource_version` (the resume cursor).
fn config_subscribe_request(
    namespace: String,
    name: String,
    headers: &HeaderMap,
) -> SubscribeRequest {
    SubscribeRequest {
        namespace,
        names: vec![name],
        resume_resource_version: last_event_id(headers),
        label_selector: String::new(),
    }
}

/// Build the in-process `SubscribeSecretsRequest` for a single-secret SSE watch.
fn secret_subscribe_request(
    namespace: String,
    name: String,
    headers: &HeaderMap,
) -> SubscribeSecretsRequest {
    SubscribeSecretsRequest {
        namespace,
        names: vec![name],
        resume_resource_version: last_event_id(headers),
    }
}

/// `GET /v1/configs/{namespace}/{name}/watch` — stream one config as SSE.
async fn sse_config_watch(
    State(state): State<Arc<GatewayState>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let cfg = &state.cfg;
    if let Some(deny) = check_auth(cfg, &headers) {
        return deny;
    }
    if !state.server.cache.is_populated() {
        return cache_not_ready(cfg);
    }
    let req = config_subscribe_request(namespace, name, &headers);
    match state.server.subscribe(Request::new(req)).await {
        // Box::pin so the concrete SubscribeStream is `Unpin` for `.next()`. The
        // underlying bridge ends this stream on drain (shared `drain_notify`),
        // so an idle SSE connection never blocks graceful shutdown.
        Ok(resp) => sse_response(
            cfg,
            sse_event_stream(Box::pin(resp.into_inner()), frame_config_event),
        ),
        Err(status) => {
            let (status, code, message) = status_to_error(status);
            json_response(cfg, status, error_body(code, &message))
        }
    }
}

/// `GET /v1/secrets/{namespace}/{name}/watch` — stream one secret as SSE. Same
/// auth/resume/RELOAD semantics; payload keeps `data_json` base64.
async fn sse_secret_watch(
    State(state): State<Arc<GatewayState>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let cfg = &state.cfg;
    if let Some(deny) = check_auth(cfg, &headers) {
        return deny;
    }
    if !state.server.secret_cache.is_populated() {
        return cache_not_ready(cfg);
    }
    let req = secret_subscribe_request(namespace, name, &headers);
    match state.server.subscribe_secrets(Request::new(req)).await {
        Ok(resp) => sse_response(
            cfg,
            sse_event_stream(Box::pin(resp.into_inner()), frame_secret_event),
        ),
        Err(status) => {
            let (status, code, message) = status_to_error(status);
            json_response(cfg, status, error_body(code, &message))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    fn cfg_secure() -> HttpGatewayConfig {
        HttpGatewayConfig {
            addr: "127.0.0.1:0".parse().unwrap(),
            auth_token: Some("s3cret".to_string()),
            cors_allow_origin: Some("https://backstage.example".to_string()),
            insecure: false,
        }
    }

    #[test]
    fn status_to_http_maps_known_codes() {
        assert_eq!(status_to_http(Code::Ok), StatusCode::OK);
        assert_eq!(
            status_to_http(Code::InvalidArgument),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            status_to_http(Code::Unauthenticated),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status_to_http(Code::PermissionDenied),
            StatusCode::FORBIDDEN
        );
        assert_eq!(status_to_http(Code::NotFound), StatusCode::NOT_FOUND);
        assert_eq!(status_to_http(Code::AlreadyExists), StatusCode::CONFLICT);
        assert_eq!(
            status_to_http(Code::FailedPrecondition),
            StatusCode::PRECONDITION_FAILED
        );
        assert_eq!(
            status_to_http(Code::ResourceExhausted),
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(
            status_to_http(Code::Unavailable),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            status_to_http(Code::Unimplemented),
            StatusCode::NOT_IMPLEMENTED
        );
        // Catch-all bucket.
        assert_eq!(
            status_to_http(Code::Internal),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            status_to_http(Code::Unknown),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn tokens_match_constant_time_semantics() {
        assert!(tokens_match("abc123", "abc123"));
        assert!(!tokens_match("abc123", "abc124"));
        // Differing length never matches.
        assert!(!tokens_match("abc", "abc123"));
        assert!(!tokens_match("", "x"));
        assert!(tokens_match("", ""));
    }

    /// The `Bearer` scheme name is case-insensitive (RFC 6750) and surrounding
    /// whitespace is tolerated; a non-Bearer or scheme-less value yields `None`.
    #[test]
    fn strip_bearer_prefix_is_case_insensitive() {
        assert_eq!(strip_bearer_prefix("Bearer tok"), Some("tok"));
        assert_eq!(strip_bearer_prefix("bearer tok"), Some("tok"));
        assert_eq!(strip_bearer_prefix("BEARER tok"), Some("tok"));
        // Caller trims the token; extra inner spaces survive until then.
        assert_eq!(
            strip_bearer_prefix("Bearer   tok").map(str::trim),
            Some("tok")
        );
        assert_eq!(strip_bearer_prefix("Basic abc"), None);
        assert_eq!(strip_bearer_prefix("Bearer"), None);
        assert_eq!(strip_bearer_prefix(""), None);
    }

    /// `GetRequest` round-trips through the proto3-JSON shape the gateway
    /// promises (`{"namespace":...,"name":...}`, snake_case).
    #[test]
    fn get_request_deserializes_snake_case() {
        let body = Bytes::from_static(br#"{"namespace":"default","name":"app-config"}"#);
        let req = decode_request::<crate::proto::GetRequest>(&body).expect("decodes");
        assert_eq!(req.namespace, "default");
        assert_eq!(req.name, "app-config");
    }

    /// An empty body decodes to the proto default (proto3-JSON leniency).
    #[test]
    fn empty_body_decodes_to_default() {
        let req = decode_request::<crate::proto::GetAllRequest>(&Bytes::new()).expect("default");
        assert_eq!(req.namespace, "");
    }

    /// A `Config` response serialises with the snake_case field names clients
    /// expect (`content_json`, `schema_version`, `resource_version`).
    #[test]
    fn config_response_serializes_snake_case() {
        let cfg = crate::proto::Config {
            namespace: "ns".into(),
            name: "n".into(),
            schema_version: 3,
            content_json: "{\"k\":1}".into(),
            resource_version: "rv-1".into(),
            age_ms: 5,
            stale_since_ms: -1,
        };
        let bytes = encode_response(&cfg).expect("encodes");
        let v: serde_json::Value = serde_json::from_slice(&bytes).expect("valid json");
        assert_eq!(v["namespace"], "ns");
        assert_eq!(v["schema_version"], 3);
        assert_eq!(v["content_json"], "{\"k\":1}");
        assert_eq!(v["resource_version"], "rv-1");
        assert_eq!(v["stale_since_ms"], -1);
    }

    /// Shared `State` for the handler smoke tests, wired against a
    /// drain-plumbing test server (authz/quota disabled, dummy kube client).
    /// We call [`dispatch`] directly with constructed extractors so the tests
    /// stay dep-free (no `tower`/`oneshot`) while still exercising the real
    /// deserialize → call → status-map → CORS pipeline.
    fn test_state() -> State<Arc<GatewayState>> {
        State(Arc::new(GatewayState {
            server: crate::grpc::test_server(),
            cfg: cfg_secure(),
        }))
    }

    fn bearer(token: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        h
    }

    /// Handler state whose Config cache already holds one entry, so a `Get`
    /// resolves to a real `200` body (the drain-plumbing `test_server` ships an
    /// empty cache, which only exercises the `NotFound` path).
    fn state_with_config(
        namespace: &str,
        name: &str,
        schema_version: u32,
    ) -> State<Arc<GatewayState>> {
        let snap = crate::types::ConfigSnapshot {
            namespace: namespace.into(),
            name: name.into(),
            schema_version,
            content: serde_json::json!({"key": "val"}),
            resource_version: format!("rv-{schema_version}"),
            ..Default::default()
        };
        let mut server = crate::grpc::test_server();
        server.cache = Arc::new(crate::cache::ConfigCache::new(snap));
        State(Arc::new(GatewayState {
            server,
            cfg: cfg_secure(),
        }))
    }

    /// A POST without a bearer token is rejected with 401 before the handler
    /// runs, carrying the CORS allow-origin header.
    #[tokio::test]
    async fn post_without_token_is_401() {
        let resp = dispatch(
            test_state(),
            Method::POST,
            HeaderMap::new(),
            Path("Get".to_string()),
            Bytes::from_static(br#"{"namespace":"default","name":"x"}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            resp.headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|v| v.to_str().ok()),
            Some("https://backstage.example")
        );
    }

    /// An authenticated `Get` against the empty default cache resolves to the
    /// handler and maps `NotFound` → 404 with a JSON error body — proving the
    /// JSON-in → proto → handler → status-map → JSON-out path works.
    #[tokio::test]
    async fn authenticated_get_missing_config_maps_to_404() {
        let resp = dispatch(
            test_state(),
            Method::POST,
            bearer("s3cret"),
            Path("Get".to_string()),
            Bytes::from_static(br#"{"namespace":"default","name":"does-not-exist"}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(resp.into_body(), 64 * 1024).await.expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json error body");
        assert_eq!(v["code"], "not_found");
        assert!(v["message"].is_string());
    }

    /// An `OPTIONS` preflight returns 204 with the CORS allow-* headers and
    /// requires no token.
    #[tokio::test]
    async fn options_preflight_returns_204_with_cors() {
        let resp = dispatch(
            test_state(),
            Method::OPTIONS,
            HeaderMap::new(),
            Path("Get".to_string()),
            Bytes::new(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert_eq!(
            resp.headers()
                .get(header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|v| v.to_str().ok()),
            Some("https://backstage.example")
        );
        assert!(
            resp.headers()
                .contains_key(header::ACCESS_CONTROL_ALLOW_METHODS)
        );
    }

    // ── REST GET aliases + /healthz (CU-86ahqve4c PR2) ──────────────────────

    #[tokio::test]
    async fn rest_get_config_without_token_is_401() {
        let resp = rest_get_config(
            test_state(),
            Path(("default".to_string(), "x".to_string())),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn rest_get_config_found_returns_200_json() {
        let resp = rest_get_config(
            state_with_config("default", "app", 3),
            Path(("default".to_string(), "app".to_string())),
            bearer("s3cret"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), 64 * 1024).await.expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(v["namespace"], "default");
        assert_eq!(v["name"], "app");
        assert_eq!(v["schema_version"], 3);
    }

    #[tokio::test]
    async fn rest_get_config_missing_is_404() {
        let resp = rest_get_config(
            test_state(),
            Path(("default".to_string(), "nope".to_string())),
            bearer("s3cret"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn rest_get_all_configs_without_token_is_401() {
        let resp =
            rest_get_all_configs(test_state(), Path("default".to_string()), HeaderMap::new()).await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn rest_get_all_configs_empty_streams_array() {
        let resp =
            rest_get_all_configs(test_state(), Path("default".to_string()), bearer("s3cret")).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), 64 * 1024).await.expect("body");
        assert_eq!(&body[..], b"[]", "empty namespace → valid empty JSON array");
    }

    /// `/healthz` is unauthenticated (K8s probes carry no token) and returns 503
    /// while the cache is warming.
    #[tokio::test]
    async fn healthz_not_ready_is_503() {
        let resp = healthz(test_state()).await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.headers()
                .get(header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );
        let body = to_bytes(resp.into_body(), 8 * 1024).await.expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(v["status"], "warming");
        assert_eq!(v["cache_ready"], false);
    }

    #[tokio::test]
    async fn healthz_ready_is_200_no_token() {
        let resp = healthz(state_with_config("default", "app", 1)).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), 8 * 1024).await.expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(v["status"], "ok");
        assert_eq!(v["cache_ready"], true);
    }

    /// `GetAll` against the empty default cache streams a valid empty JSON
    /// array (`[]`) with status 200 — exercising the incremental
    /// [`stream_json_array`] path end-to-end (decode → handler → streamed body)
    /// and proving the empty-stream case closes the array correctly.
    #[tokio::test]
    async fn get_all_empty_namespace_streams_empty_array() {
        let resp = dispatch(
            test_state(),
            Method::POST,
            bearer("s3cret"),
            Path("GetAll".to_string()),
            Bytes::from_static(br#"{"namespace":"default"}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), 64 * 1024).await.expect("body");
        assert_eq!(&body[..], b"[]", "empty namespace must stream a valid []");
        // And it must parse back as an empty JSON array.
        let v: serde_json::Value = serde_json::from_slice(&body).expect("valid json array");
        assert_eq!(v, serde_json::json!([]));
    }

    /// An authenticated `Get` against a populated cache returns `200` with the
    /// proto3-JSON body (snake_case fields), `content-type: application/json`,
    /// and the CORS allow-origin + `Vary: Origin` headers — the happy-path
    /// unary pipeline end-to-end.
    #[tokio::test]
    async fn authenticated_get_returns_200_with_config() {
        let resp = dispatch(
            state_with_config("default", "app-config", 3),
            Method::POST,
            bearer("s3cret"),
            Path("Get".to_string()),
            Bytes::from_static(br#"{"namespace":"default","name":"app-config"}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("application/json")
        );
        assert_eq!(
            resp.headers()
                .get(header::VARY)
                .and_then(|v| v.to_str().ok()),
            Some("origin")
        );
        let body = to_bytes(resp.into_body(), 64 * 1024).await.expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(v["namespace"], "default");
        assert_eq!(v["name"], "app-config");
        assert_eq!(v["schema_version"], 3);
    }

    /// A malformed JSON body is rejected with `400 invalid_argument` — the
    /// decode-error path through `dispatch`.
    #[tokio::test]
    async fn invalid_json_body_is_400() {
        let resp = dispatch(
            test_state(),
            Method::POST,
            bearer("s3cret"),
            Path("Get".to_string()),
            Bytes::from_static(b"this is not json"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(resp.into_body(), 64 * 1024).await.expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(v["code"], "invalid_argument");
    }

    /// A non-POST, non-OPTIONS verb is rejected with `405 method_not_allowed`
    /// before the handler runs.
    #[tokio::test]
    async fn non_post_method_is_405() {
        let resp = dispatch(
            test_state(),
            Method::GET,
            bearer("s3cret"),
            Path("Get".to_string()),
            Bytes::new(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
        let body = to_bytes(resp.into_body(), 64 * 1024).await.expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(v["code"], "method_not_allowed");
    }

    /// An unknown method name (typo / not on the service) returns `404
    /// not_found`, distinct from the `501` for the real-but-streaming RPCs.
    #[tokio::test]
    async fn unknown_method_is_404() {
        let resp = dispatch(
            test_state(),
            Method::POST,
            bearer("s3cret"),
            Path("Frobnicate".to_string()),
            Bytes::from_static(br#"{}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(resp.into_body(), 64 * 1024).await.expect("body");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(v["code"], "not_found");
    }

    /// Streaming RPCs are not transcodable to a single JSON response — the
    /// gateway returns 501 pointing at the SSE endpoint.
    #[tokio::test]
    async fn subscribe_returns_501() {
        let resp = dispatch(
            test_state(),
            Method::POST,
            bearer("s3cret"),
            Path("Subscribe".to_string()),
            Bytes::from_static(br#"{"namespace":"default"}"#),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }

    // ── SSE ─────────────────────────────────────────────────────────────────

    fn config_event(event_type: i32, rv: &str) -> ConfigEvent {
        ConfigEvent {
            event_type,
            config: Some(crate::proto::Config {
                namespace: "default".into(),
                name: "app".into(),
                schema_version: 1,
                content_json: "{\"k\":1}".into(),
                resource_version: rv.into(),
                age_ms: 0,
                stale_since_ms: -1,
            }),
        }
    }

    /// Proto `EventType` numbers map to the SSE `event:` names clients switch
    /// on; an unknown number degrades to `MODIFIED` (a re-fetch is always safe).
    #[test]
    fn event_type_name_maps_known_and_unknown() {
        assert_eq!(event_type_name(0), "ADDED");
        assert_eq!(event_type_name(1), "MODIFIED");
        assert_eq!(event_type_name(2), "DELETED");
        assert_eq!(event_type_name(3), "SNAPSHOT");
        assert_eq!(event_type_name(99), "MODIFIED");
    }

    /// A frame is `event:`/optional `id:`/`data:` then a blank line; the `id:`
    /// line is omitted when empty (terminal frames carry no resume cursor).
    #[test]
    fn sse_frame_shape_omits_empty_id() {
        assert_eq!(
            String::from_utf8(sse_frame("RELOAD", "", "{}")).unwrap(),
            "event: RELOAD\ndata: {}\n\n"
        );
        assert_eq!(
            String::from_utf8(sse_frame("ADDED", "rv-1", "{\"a\":1}")).unwrap(),
            "event: ADDED\nid: rv-1\ndata: {\"a\":1}\n\n"
        );
    }

    /// A `ConfigEvent` frames as `event` = type name, `id` = resource_version,
    /// `data` = the proto3-JSON Config (snake_case `content_json`).
    #[test]
    fn frame_config_event_uses_resource_version_as_id() {
        let frame = String::from_utf8(frame_config_event(&config_event(3, "rv-7"))).unwrap();
        assert!(frame.starts_with("event: SNAPSHOT\nid: rv-7\ndata: "));
        assert!(frame.contains("\"content_json\":\"{\\\"k\\\":1}\""));
        assert!(frame.ends_with("\n\n"));
    }

    /// Secret frames keep `data_json` base64 verbatim — the gateway never
    /// decodes secret values (same policy as gRPC `GetSecret`).
    #[test]
    fn frame_secret_event_preserves_base64_data_json() {
        let ev = SecretEvent {
            event_type: 1,
            secret: Some(crate::proto::SecretResponse {
                namespace: "default".into(),
                name: "db".into(),
                schema_version: 2,
                data_json: "{\"password\":\"c2VjcmV0\"}".into(),
                resource_version: "rv-9".into(),
                age_ms: 0,
                stale_since_ms: -1,
            }),
        };
        let frame = String::from_utf8(frame_secret_event(&ev)).unwrap();
        assert!(frame.starts_with("event: MODIFIED\nid: rv-9\ndata: "));
        assert!(
            frame.contains("c2VjcmV0"),
            "base64 value must be preserved verbatim"
        );
    }

    /// Lag (`RESOURCE_EXHAUSTED`) → terminal `RELOAD`; any other error →
    /// terminal `error` frame carrying the snake_case code + message.
    #[test]
    fn terminal_frame_maps_lag_and_errors() {
        let lag = String::from_utf8(terminal_frame(&Status::resource_exhausted("lag"))).unwrap();
        assert!(lag.starts_with("event: RELOAD\n"));
        assert!(lag.contains("\"reason\":\"lagged\""));

        let err = String::from_utf8(terminal_frame(&Status::internal("boom"))).unwrap();
        assert!(err.starts_with("event: error\n"));
        assert!(err.contains("\"code\":\"internal\""));
        assert!(err.contains("\"message\":\"boom\""));
    }

    /// `Last-Event-ID` plumbs into `resume_resource_version`; absent → empty
    /// (cold subscribe from snapshot). `names` is the single requested key.
    #[test]
    fn last_event_id_maps_to_resume_resource_version() {
        let empty = config_subscribe_request("default".into(), "app".into(), &HeaderMap::new());
        assert_eq!(empty.resume_resource_version, "");
        assert_eq!(empty.names, vec!["app".to_string()]);

        let mut h = HeaderMap::new();
        h.insert("last-event-id", "rv-77".parse().unwrap());
        let resumed = config_subscribe_request("default".into(), "app".into(), &h);
        assert_eq!(resumed.resume_resource_version, "rv-77");
        let secret = secret_subscribe_request("default".into(), "db".into(), &h);
        assert_eq!(secret.resume_resource_version, "rv-77");
    }

    /// The adapter frames each event in order and, on lag, emits a single
    /// terminal `RELOAD` then ends — nothing follows it.
    #[tokio::test]
    async fn sse_event_stream_ends_with_reload_on_lag() {
        let items = vec![
            Ok(config_event(3, "rv-1")),
            Ok(config_event(1, "rv-2")),
            Err(Status::resource_exhausted("subscriber lagged")),
        ];
        let mut stream = std::pin::pin!(sse_event_stream(
            futures_util::stream::iter(items),
            frame_config_event
        ));
        let mut out = Vec::new();
        while let Some(Ok(chunk)) = stream.next().await {
            out.extend_from_slice(&chunk);
        }
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("event: SNAPSHOT\nid: rv-1\ndata: "));
        assert!(text.contains("event: MODIFIED\nid: rv-2\ndata: "));
        let reload_at = text.find("event: RELOAD").expect("RELOAD frame present");
        assert_eq!(
            text[reload_at..].matches("event:").count(),
            1,
            "RELOAD is terminal — no frames after it"
        );
    }

    /// A clean upstream end (drain: bridge drops its sender → `None`) ends the
    /// SSE with no terminal frame, so graceful shutdown is not blocked.
    #[tokio::test]
    async fn sse_event_stream_ends_cleanly_when_upstream_closes() {
        let items: Vec<Result<ConfigEvent, Status>> = vec![Ok(config_event(3, "rv-1"))];
        let mut stream = std::pin::pin!(sse_event_stream(
            futures_util::stream::iter(items),
            frame_config_event
        ));
        let mut out = Vec::new();
        while let Some(Ok(chunk)) = stream.next().await {
            out.extend_from_slice(&chunk);
        }
        let text = String::from_utf8(out).unwrap();
        assert!(text.contains("event: SNAPSHOT\n"));
        assert!(!text.contains("event: RELOAD"));
        assert!(!text.contains("event: error"));
    }

    /// Config SSE during cold start (cache not yet warm) returns `503` +
    /// `Retry-After: 1` after auth passes — never reaching `subscribe`.
    #[tokio::test]
    async fn sse_config_watch_cache_not_ready_is_503() {
        let resp = sse_config_watch(
            test_state(),
            Path(("default".to_string(), "app".to_string())),
            bearer("s3cret"),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.headers()
                .get(header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );
    }

    /// Secret SSE is gated by the same uniform bearer check: no token → `401`
    /// before the cache/readiness/subscribe path runs.
    #[tokio::test]
    async fn sse_secret_watch_without_token_is_401() {
        let resp = sse_secret_watch(
            test_state(),
            Path(("default".to_string(), "db".to_string())),
            HeaderMap::new(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }
}
