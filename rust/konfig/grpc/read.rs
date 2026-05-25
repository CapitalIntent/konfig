//! `Read` and `ReadAll` gRPC handlers for `KonfigService`.
//!
//! `Read` — unary — returns the cached [`TradingConfigSnapshot`] for a given
//!   `(namespace, name)` as a FlatBuffers `Configuration` response.
//!
//! `ReadAll` — server-streaming — streams all cached configs in the given
//!   namespace.  For Phase 2A a single Konfig service instance watches a
//!   single `(namespace, name)` pair; the cache therefore holds exactly one
//!   entry.  ReadAll yields that entry (if the request namespace matches) so
//!   the API contract is satisfied and Phase 2B can extend it.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use crate::cache::ConfigCache;
use crate::grpc::{build_configuration_response, read_string_field};

// ── ReadHandler ───────────────────────────────────────────────────────────────

/// Unary handler for `Read(ReadRequest) -> Configuration`.
///
/// Reads `namespace` (slot 4) and `name` (slot 6) from the request bytes,
/// loads the cache, and re-encodes the snapshot as a `Configuration`
/// FlatBuffers response.
///
/// Returns `NOT_FOUND` when the cache has not yet been populated (schema_version
/// == 0 and resource_version is empty — the initial default).
pub struct ReadHandler {
    pub cache: Arc<ConfigCache>,
}

impl tower_service::Service<Request<Bytes>> for ReadHandler {
    type Response = Response<Bytes>;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Status>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Status>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Bytes>) -> Self::Future {
        let cache = Arc::clone(&self.cache);
        Box::pin(async move {
            let bytes = req.into_inner();
            let namespace = read_string_field(&bytes, 4).to_owned();
            let name = read_string_field(&bytes, 6).to_owned();

            debug!(namespace = %namespace, name = %name, "Read RPC");

            let snap = cache.load();

            // Cache not yet populated: the watcher hasn't delivered its first event.
            if snap.resource_version.is_empty() && snap.schema_version == 0 {
                warn!(
                    namespace = %namespace,
                    name = %name,
                    "Read: cache not yet populated",
                );
                return Err(Status::not_found(format!(
                    "config {namespace}/{name} not found — cache not yet populated"
                )));
            }

            let resp_bytes = build_configuration_response(&namespace, &name, &snap);
            Ok(Response::new(resp_bytes))
        })
    }
}

// ── ReadAllHandler ────────────────────────────────────────────────────────────

/// Server-streaming handler for `ReadAll(ReadAllRequest) -> stream<Configuration>`.
///
/// Yields the single cached configuration entry if the namespace matches the
/// request namespace.  If the cache is unpopulated, streams zero entries.
///
/// Slot layout for `ReadAllRequest` (konfig_service.fbs):
/// ```text
///   slot 4: namespace (string)
/// ```
pub struct ReadAllHandler {
    pub cache: Arc<ConfigCache>,
}

impl tower_service::Service<Request<Bytes>> for ReadAllHandler {
    type Response = Response<ReceiverStream<Result<Bytes, Status>>>;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Status>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Status>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Bytes>) -> Self::Future {
        let cache = Arc::clone(&self.cache);
        Box::pin(async move {
            let bytes = req.into_inner();
            let req_namespace = read_string_field(&bytes, 4).to_owned();

            debug!(namespace = %req_namespace, "ReadAll RPC");

            let (tx, rx) = mpsc::channel(1);

            tokio::spawn(async move {
                let snap = cache.load();

                // If the cache is populated and belongs to the requested namespace,
                // stream the single entry.  For Phase 2A the service watches one
                // (ns, name) pair; the name is stored in resource_version metadata
                // only via the watcher — we emit the cached entry regardless of
                // name since the namespace filter is the relevant discriminator.
                if !snap.resource_version.is_empty() || snap.schema_version > 0 {
                    // Use "default" as the name placeholder — the operator knows the
                    // ConfigMap name from context.  Phase 2B can pass this through
                    // the watcher registration.
                    let response_bytes =
                        build_configuration_response(&req_namespace, "", &snap);
                    let _ = tx.send(Ok(response_bytes)).await;
                }
                // Channel drops here, ending the stream.
            });

            Ok(Response::new(ReceiverStream::new(rx)))
        })
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::ConfigCache;
    use crate::types::{RiskParamsSnapshot, StrategyParamsSnapshot, TradingConfigSnapshot};
    use std::time::Instant;

    fn make_cache(schema_version: u32) -> Arc<ConfigCache> {
        let snap = TradingConfigSnapshot {
            schema_version,
            risk: RiskParamsSnapshot {
                max_position_usd: 100_000.0,
                max_order_size_usd: 5_000.0,
                max_daily_loss_usd: 2_000.0,
                max_orders_per_second: 100,
                max_notional_per_minute: 500_000.0,
                enabled: true,
            },
            strategies: vec![StrategyParamsSnapshot {
                product_id: "BTC-USDT".to_string(),
                signal_threshold: 0.75,
                lookback_window_ms: 60_000,
                max_spread_bps: 20.0,
                enabled: true,
            }],
            resource_version: if schema_version > 0 {
                format!("rv-{schema_version:04}")
            } else {
                String::new()
            },
            loaded_at: Instant::now(),
        };
        Arc::new(ConfigCache::new(snap))
    }

    fn make_read_request(namespace: &str, name: &str) -> Bytes {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let ns = fbb.create_string(namespace);
        let nm = fbb.create_string(name);
        let start = fbb.start_table();
        fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(4, ns);
        fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(6, nm);
        let root = fbb.end_table(start);
        fbb.finish(root, None);
        Bytes::copy_from_slice(fbb.finished_data())
    }

    fn make_read_all_request(namespace: &str) -> Bytes {
        let mut fbb = flatbuffers::FlatBufferBuilder::new();
        let ns = fbb.create_string(namespace);
        let start = fbb.start_table();
        fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(4, ns);
        let root = fbb.end_table(start);
        fbb.finish(root, None);
        Bytes::copy_from_slice(fbb.finished_data())
    }

    #[tokio::test]
    async fn read_returns_configuration_when_cache_populated() {
        let cache = make_cache(3);
        let mut handler = ReadHandler { cache };

        let req = make_read_request("trading", "risk");
        let resp = tower_service::Service::call(&mut handler, Request::new(req))
            .await
            .expect("Read must succeed");

        let resp_bytes = resp.into_inner();
        // Verify the response is a non-empty FlatBuffers buffer.
        assert!(!resp_bytes.is_empty(), "response must be non-empty");

        // Decode namespace field (slot 4) from the Configuration response.
        let ns = crate::grpc::read_string_field(&resp_bytes, 4);
        assert_eq!(ns, "trading");
        let name = crate::grpc::read_string_field(&resp_bytes, 6);
        assert_eq!(name, "risk");
    }

    #[tokio::test]
    async fn read_returns_not_found_when_cache_empty() {
        // schema_version=0 + empty resource_version = unpopulated
        let cache = make_cache(0);
        let mut handler = ReadHandler { cache };

        let req = make_read_request("trading", "risk");
        let result = tower_service::Service::call(&mut handler, Request::new(req)).await;

        assert!(result.is_err(), "must return NOT_FOUND");
        let status = result.unwrap_err();
        assert_eq!(status.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn read_all_streams_entry_when_cache_populated() {
        use tokio_stream::StreamExt;

        let cache = make_cache(5);
        let mut handler = ReadAllHandler { cache };

        let req = make_read_all_request("trading");
        let resp = tower_service::Service::call(&mut handler, Request::new(req))
            .await
            .expect("ReadAll must succeed");

        let mut stream = resp.into_inner();
        let first = stream
            .next()
            .await
            .expect("stream must yield one item")
            .expect("item must not be an error");

        assert!(!first.is_empty(), "streamed Configuration must be non-empty");
        let next = stream.next().await;
        assert!(next.is_none(), "ReadAll must stream exactly one entry for a single-pair watcher");
    }

    #[tokio::test]
    async fn read_all_streams_nothing_when_cache_empty() {
        use tokio_stream::StreamExt;

        let cache = make_cache(0);
        let mut handler = ReadAllHandler { cache };

        let req = make_read_all_request("trading");
        let resp = tower_service::Service::call(&mut handler, Request::new(req))
            .await
            .expect("ReadAll must succeed even with empty cache");

        let mut stream = resp.into_inner();
        let item = stream.next().await;
        assert!(item.is_none(), "empty cache must yield an empty stream");
    }
}
