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
//! This port is **plaintext** and carries no mTLS client certificate, so calls
//! reach konfig as the `anonymous` identity. Two consequences:
//!   - With `KONFIG_AUTHZ_MODE=disabled` (the default) every method works; under
//!     `enforce`, anonymous is denied (run the gateway behind a trusted caller,
//!     or grant an ACL identity in a follow-up).
//!   - It exposes writes (`Apply`/`Revert`/…) and secret reads (`GetSecret`).
//!     The gateway therefore REQUIRES a bearer token ([`HttpGatewayConfig::auth_token`])
//!     unless the operator passes `--http-insecure`; [`crate::startup`] fails
//!     fast otherwise. Run it cluster-internal.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use futures_util::StreamExt;
use tonic::{Code, Request};
use tracing::{error, info};

use crate::grpc::KonfigServer;
use crate::proto::konfig_service_server::KonfigService;

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

/// Run the HTTP/JSON gateway until the process exits. Spawned as a detached
/// background task by [`crate::grpc::serve`] (mirrors the `/metrics` server).
/// The `listener` is bound by `serve` on the startup path, so a port clash
/// fails startup loudly there rather than silently killing this detached task.
/// Errors from the serve loop itself are logged and end the task without
/// crashing the gRPC server.
pub async fn serve_gateway(
    server: KonfigServer,
    cfg: HttpGatewayConfig,
    listener: tokio::net::TcpListener,
) {
    let addr = cfg.addr;
    let state = Arc::new(GatewayState { server, cfg });
    // Single dispatch handler keyed by the `{rpc}` path segment. `any` so the
    // CORS preflight `OPTIONS` reaches us too (method-specific routers would
    // 405 it before we can answer the preflight).
    let app = Router::new()
        .route("/konfig.v1.KonfigService/{rpc}", any(dispatch))
        .with_state(state);

    info!(%addr, "HTTP/JSON gateway starting");
    if let Err(e) = axum::serve(listener, app).await {
        error!("HTTP/JSON gateway server error: {e}");
    }
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

/// Constant-time token comparison (length is allowed to leak). Avoids a
/// byte-wise early-return timing side channel on the bearer token.
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
        Some(origin) if !origin.is_empty() => {
            builder.header(header::ACCESS_CONTROL_ALLOW_ORIGIN, origin)
        }
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

/// Answer a CORS preflight (`OPTIONS`) with `204` + the allow-* headers.
fn cors_preflight(cfg: &HttpGatewayConfig) -> Response {
    let builder = with_cors(Response::builder().status(StatusCode::NO_CONTENT), cfg)
        .header(header::ACCESS_CONTROL_ALLOW_METHODS, "POST, OPTIONS")
        .header(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            "content-type, authorization",
        )
        .header(header::ACCESS_CONTROL_MAX_AGE, "600");
    builder
        .body(Body::empty())
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
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
        .and_then(|v| v.strip_prefix("Bearer "))
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

/// Deserialise a JSON body into the proto request `Req`. Empty body ⇒ the proto
/// default (proto3-JSON: all fields optional).
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
            error_body("unimplemented", "only POST and OPTIONS are supported"),
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
        other => {
            return json_response(
                cfg,
                StatusCode::NOT_IMPLEMENTED,
                error_body("unimplemented", &format!("unknown method '{other}'")),
            );
        }
    };

    match outcome {
        Ok(json) => json_response(cfg, StatusCode::OK, json),
        Err((status, code, message)) => json_response(cfg, status, error_body(code, &message)),
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
}
