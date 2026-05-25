//! gRPC server for `konfig.v1.KonfigService`.
//!
//! Implements the service as a hand-rolled `tower::Service` over
//! `http::Request<tonic::body::Body>`, routing on the URI path to
//! per-RPC handlers.  No protobuf, no tonic-build — [`FlatBuffersCodec`]
//! carries FlatBuffers bytes in both directions (ADR-003, ADR-004).
//!
//! # Layout
//!
//! - [`mod.rs`] — [`KonfigServer`] struct, [`ServerConfig`], routing, startup
//! - [`read`] — `Read` and `ReadAll` handlers
//! - [`apply`] — `Apply` handler (write through kube-rs with 409 retry)

pub mod apply;
pub mod read;

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use kube::Client;
use tonic::Status;
use tracing::info;

use crate::cache::ConfigCache;
use crate::codec::FlatBuffersCodec;

// ── Server configuration ──────────────────────────────────────────────────────

/// Configuration for the gRPC server.
pub struct ServerConfig {
    /// Address to listen on.
    pub addr: SocketAddr,
    /// Shared config cache populated by the watcher.
    pub cache: Arc<ConfigCache>,
    /// Authenticated kube client for Apply writes.
    pub kube_client: Client,
}

// ── KonfigServer ──────────────────────────────────────────────────────────────

/// `tower::Service` implementation for `konfig.v1.KonfigService`.
///
/// Routes each `http::Request` to the appropriate handler based on the
/// URI path.  All message bodies are raw FlatBuffers bytes transported
/// via [`FlatBuffersCodec`].
#[derive(Clone)]
pub struct KonfigServer {
    pub(crate) cache: Arc<ConfigCache>,
    pub(crate) kube_client: Client,
}

impl tonic::server::NamedService for KonfigServer {
    const NAME: &'static str = "konfig.v1.KonfigService";
}

impl tower_service::Service<http::Request<tonic::body::Body>> for KonfigServer {
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let path = req.uri().path().to_string();
        let cache = Arc::clone(&self.cache);
        let kube_client = self.kube_client.clone();

        Box::pin(async move {
            let resp = match path.as_str() {
                "/konfig.v1.KonfigService/Read" => {
                    let handler = read::ReadHandler { cache };
                    tonic::server::Grpc::new(FlatBuffersCodec)
                        .unary(handler, req)
                        .await
                }
                "/konfig.v1.KonfigService/ReadAll" => {
                    let handler = read::ReadAllHandler { cache };
                    tonic::server::Grpc::new(FlatBuffersCodec)
                        .server_streaming(handler, req)
                        .await
                }
                "/konfig.v1.KonfigService/Apply" => {
                    let handler = apply::ApplyHandler { kube_client };
                    tonic::server::Grpc::new(FlatBuffersCodec)
                        .unary(handler, req)
                        .await
                }
                _ => Status::unimplemented(format!("no handler for {path}")).into_http(),
            };
            Ok(resp)
        })
    }
}

// ── Server startup ────────────────────────────────────────────────────────────

/// Start the gRPC server and block until it terminates.
///
/// Wraps [`tonic::transport::Server::builder`] with the [`KonfigServer`]
/// service and serves on `cfg.addr`.
pub async fn serve(cfg: ServerConfig) -> Result<(), tonic::transport::Error> {
    info!(addr = %cfg.addr, "KonfigService gRPC server starting");

    let server = KonfigServer {
        cache: cfg.cache,
        kube_client: cfg.kube_client,
    };

    tonic::transport::Server::builder()
        .add_service(server)
        .serve(cfg.addr)
        .await
}

// ── FlatBuffers message builders (shared across handlers) ─────────────────────

/// Build a `Configuration` FlatBuffers response from a [`TradingConfigSnapshot`].
///
/// Slot layout for `Configuration` (matches `konfig_service.fbs`):
/// ```text
///   slot 4:  namespace  (string)
///   slot 6:  name       (string)
///   slot 8:  config     (TradingConfig — nested table)
///   slot 10: resource_version (string)
///   slot 12: stale_since_ms (uint64)
/// ```
pub(crate) fn build_configuration_response(
    namespace: &str,
    name: &str,
    snapshot: &crate::types::TradingConfigSnapshot,
) -> bytes::Bytes {
    use flatbuffers::FlatBufferBuilder;

    let mut fbb = FlatBufferBuilder::new();

    // Build nested TradingConfig table from snapshot fields.
    // Strategy entries must be built before the vector.
    let mut strategy_offsets = Vec::with_capacity(snapshot.strategies.len());
    for s in &snapshot.strategies {
        let product_id_off = fbb.create_string(&s.product_id);
        let strat_start = fbb.start_table();
        fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(4, product_id_off);
        fbb.push_slot::<f64>(6, s.signal_threshold, 0.0);
        fbb.push_slot::<u64>(8, s.lookback_window_ms, 0);
        fbb.push_slot::<f64>(10, s.max_spread_bps, 0.0);
        fbb.push_slot::<bool>(12, s.enabled, true);
        strategy_offsets.push(fbb.end_table(strat_start));
    }
    let strategies_vec = fbb.create_vector(&strategy_offsets);

    let risk_start = fbb.start_table();
    fbb.push_slot::<f64>(4, snapshot.risk.max_position_usd, 0.0);
    fbb.push_slot::<f64>(6, snapshot.risk.max_order_size_usd, 0.0);
    fbb.push_slot::<f64>(8, snapshot.risk.max_daily_loss_usd, 0.0);
    fbb.push_slot::<u32>(10, snapshot.risk.max_orders_per_second, 0);
    fbb.push_slot::<f64>(12, snapshot.risk.max_notional_per_minute, 0.0);
    fbb.push_slot::<bool>(14, snapshot.risk.enabled, true);
    let risk = fbb.end_table(risk_start);

    let tc_start = fbb.start_table();
    fbb.push_slot::<u32>(4, snapshot.schema_version, 0);
    fbb.push_slot_always::<flatbuffers::WIPOffset<flatbuffers::TableFinishedWIPOffset>>(6, risk);
    fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(8, strategies_vec);
    let trading_config = fbb.end_table(tc_start);

    // Build Configuration outer table.
    let ns_off = fbb.create_string(namespace);
    let name_off = fbb.create_string(name);
    let rv_off = fbb.create_string(&snapshot.resource_version);

    let stale_since_ms: u64 = snapshot
        .loaded_at
        .elapsed()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX);

    let cfg_start = fbb.start_table();
    fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(4, ns_off);
    fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(6, name_off);
    fbb.push_slot_always::<flatbuffers::WIPOffset<flatbuffers::TableFinishedWIPOffset>>(
        8,
        trading_config,
    );
    fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(10, rv_off);
    fbb.push_slot::<u64>(12, stale_since_ms, 0);
    let root = fbb.end_table(cfg_start);
    fbb.finish(root, None);

    bytes::Bytes::copy_from_slice(fbb.finished_data())
}

/// Read a string field from a FlatBuffers buffer at the given slot.
pub(crate) fn read_string_field(bytes: &[u8], slot: u16) -> &str {
    // SAFETY: called with a just-decoded FlatBuffers payload from the tonic codec.
    let table = unsafe { flatbuffers::root_unchecked::<flatbuffers::Table>(bytes) };
    unsafe {
        table
            .get::<flatbuffers::ForwardsUOffset<&str>>(slot, None)
            .unwrap_or("")
    }
}
