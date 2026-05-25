//! Integration tests for the Konfig gRPC service (Phase 2A).
//!
//! Spins up a K3s container via Testcontainers, starts the KonfigService
//! gRPC server against it, and verifies Read / Apply behaviour through a
//! real FlatBuffers-over-gRPC connection.
//!
//! Run with:
//! ```sh
//! cargo test --test integration_grpc --features integration -p konfig
//! ```
//!
//! The `integration` feature gate prevents these tests from running in the
//! default `cargo test` invocation (which has no Docker dependency).

#![cfg(feature = "integration")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use flatbuffers::FlatBufferBuilder;
use k8s_openapi::api::core::v1::ConfigMap;
use kube::api::Api;
use tokio::time::timeout;
use tonic::Request;

use konfig::cache::ConfigCache;
use konfig::grpc::{serve, ServerConfig};
use konfig::types::TradingConfigSnapshot;
use konfig::watcher::Watcher;

use common::{k3s_client, maybe_delete, poll_until, upsert_config_map};

const NAMESPACE: &str = "default";
const CM_GRPC_READ: &str = "trading-config-grpc-read";
const CM_GRPC_APPLY: &str = "trading-config-grpc-apply";

// ── FlatBuffers request builders ──────────────────────────────────────────────

fn build_read_request(namespace: &str, name: &str) -> Bytes {
    let mut fbb = FlatBufferBuilder::new();
    let ns = fbb.create_string(namespace);
    let nm = fbb.create_string(name);
    let start = fbb.start_table();
    fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(4, ns);
    fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(6, nm);
    let root = fbb.end_table(start);
    fbb.finish(root, None);
    Bytes::copy_from_slice(fbb.finished_data())
}

fn build_read_all_request(namespace: &str) -> Bytes {
    let mut fbb = FlatBufferBuilder::new();
    let ns = fbb.create_string(namespace);
    let start = fbb.start_table();
    fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(4, ns);
    let root = fbb.end_table(start);
    fbb.finish(root, None);
    Bytes::copy_from_slice(fbb.finished_data())
}

fn build_apply_request(namespace: &str, name: &str, yaml_value: &str) -> Bytes {
    let mut fbb = FlatBufferBuilder::new();
    let ns = fbb.create_string(namespace);
    let nm = fbb.create_string(name);
    let yv = fbb.create_string(yaml_value);
    let start = fbb.start_table();
    fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(4, ns);
    fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(6, nm);
    fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(8, yv);
    let root = fbb.end_table(start);
    fbb.finish(root, None);
    Bytes::copy_from_slice(fbb.finished_data())
}

// ── gRPC client helpers ───────────────────────────────────────────────────────

/// Read a string field from a FlatBuffers table at the given slot.
fn read_string_field(bytes: &[u8], slot: u16) -> &str {
    // SAFETY: bytes came from our own FlatBuffers encoder.
    let table = unsafe { flatbuffers::root_unchecked::<flatbuffers::Table>(bytes) };
    unsafe {
        table
            .get::<flatbuffers::ForwardsUOffset<&str>>(slot, None)
            .unwrap_or("")
    }
}

/// Read a u64 field from a FlatBuffers table at the given slot.
fn read_u32_field(bytes: &[u8], slot: u16) -> u32 {
    let table = unsafe { flatbuffers::root_unchecked::<flatbuffers::Table>(bytes) };
    unsafe { table.get::<u32>(slot, Some(0)).unwrap_or(0) }
}

// ── Shared server setup ───────────────────────────────────────────────────────

/// Start the gRPC server on a random port and return the port number.
///
/// The server runs in a background task.  The caller is responsible for
/// seeding the cache via the watcher before making RPC calls.
async fn start_server(cache: Arc<ConfigCache>, kube_client: kube::Client) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind listener");
    let addr = listener.local_addr().expect("no local addr");
    drop(listener); // Release the port so tonic can bind it.

    let cfg = ServerConfig {
        addr,
        cache,
        kube_client,
    };

    tokio::spawn(async move {
        // Small delay to let the caller finish setup before the server loop.
        tokio::time::sleep(Duration::from_millis(20)).await;
        serve(cfg).await.expect("gRPC server exited with error");
    });

    // Give the server a moment to start accepting connections.
    tokio::time::sleep(Duration::from_millis(100)).await;

    addr.port()
}

/// Create a tonic gRPC channel to a local server.
async fn connect(port: u16) -> tonic::client::Grpc<tonic::transport::Channel> {
    let endpoint = format!("http://127.0.0.1:{port}");
    let channel = tonic::transport::Channel::from_shared(endpoint)
        .expect("invalid endpoint")
        .connect()
        .await
        .expect("failed to connect to gRPC server");
    let mut client = tonic::client::Grpc::new(channel);
    client.ready().await.expect("client not ready");
    client
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Read RPC returns a valid Configuration for a seeded cache.
///
/// Flow:
/// 1. Start K3s + watcher.
/// 2. Create ConfigMap schema_version=1.
/// 3. Poll until cache is populated.
/// 4. Start gRPC server backed by the cache.
/// 5. Call Read(namespace="default", name=CM_GRPC_READ).
/// 6. Assert response namespace and name fields survive the round-trip.
#[tokio::test]
async fn grpc_read_returns_configuration() {
    let (_container, client) = k3s_client().await;
    let cms: Api<ConfigMap> = Api::namespaced(client.clone(), NAMESPACE);

    maybe_delete(&cms, CM_GRPC_READ).await;

    // Start watcher to populate the cache.
    let cache = Arc::new(ConfigCache::new(TradingConfigSnapshot::default()));
    let watcher_cache = Arc::clone(&cache);
    let watcher_client = client.clone();
    let watcher_handle = tokio::spawn(async move {
        Watcher::new(watcher_client)
            .run(
                watcher_cache,
                NAMESPACE.to_string(),
                CM_GRPC_READ.to_string(),
            )
            .await
            .expect("watcher exited with error");
    });

    // Seed the ConfigMap.
    upsert_config_map(&cms, NAMESPACE, CM_GRPC_READ, 1, true)
        .await
        .expect("failed to create ConfigMap schema_version=1");

    // Wait for the cache to be populated.
    let cache_ref = Arc::clone(&cache);
    timeout(Duration::from_secs(15), async move {
        poll_until(Duration::from_secs(15), Duration::from_millis(250), || {
            cache_ref.load().schema_version == 1
        })
        .await;
    })
    .await
    .expect("timed out waiting for cache schema_version=1");

    // Start the gRPC server.
    let port = start_server(Arc::clone(&cache), client.clone()).await;

    // Issue a Read RPC.
    let mut grpc = connect(port).await;

    grpc.ready().await.expect("client not ready");
    let req = build_read_request(NAMESPACE, CM_GRPC_READ);
    let path =
        http::uri::PathAndQuery::from_static("/konfig.v1.KonfigService/Read");
    let resp = grpc
        .unary(Request::new(req), path, konfig::codec::FlatBuffersCodec)
        .await
        .expect("Read RPC failed");

    let resp_bytes = resp.into_inner();
    assert!(!resp_bytes.is_empty(), "Read response must not be empty");

    // Verify namespace (slot 4) and name (slot 6) round-trip correctly.
    let ns = read_string_field(&resp_bytes, 4);
    assert_eq!(ns, NAMESPACE, "namespace must round-trip");
    let nm = read_string_field(&resp_bytes, 6);
    assert_eq!(nm, CM_GRPC_READ, "name must round-trip");

    // Cleanup.
    maybe_delete(&cms, CM_GRPC_READ).await;
    watcher_handle.abort();
}

/// Read RPC returns NOT_FOUND when the cache has not been populated.
#[tokio::test]
async fn grpc_read_not_found_when_cache_empty() {
    let (_container, client) = k3s_client().await;

    // Cache with default (unpopulated) snapshot.
    let cache = Arc::new(ConfigCache::new(TradingConfigSnapshot::default()));
    let port = start_server(Arc::clone(&cache), client.clone()).await;

    let mut grpc = connect(port).await;

    grpc.ready().await.expect("client not ready");
    let req = build_read_request(NAMESPACE, "nonexistent");
    let path = http::uri::PathAndQuery::from_static("/konfig.v1.KonfigService/Read");
    let result = grpc
        .unary(Request::new(req), path, konfig::codec::FlatBuffersCodec)
        .await;

    assert!(result.is_err(), "Read must return NOT_FOUND for empty cache");
    let status = result.unwrap_err();
    assert_eq!(
        status.code(),
        tonic::Code::NotFound,
        "expected NOT_FOUND, got {status:?}"
    );
}

/// ReadAll RPC streams one Configuration for a populated cache.
#[tokio::test]
async fn grpc_read_all_streams_one_entry() {
    use tokio_stream::StreamExt;

    let (_container, client) = k3s_client().await;
    let cms: Api<ConfigMap> = Api::namespaced(client.clone(), NAMESPACE);

    const CM: &str = "trading-config-grpc-readall";
    maybe_delete(&cms, CM).await;

    let cache = Arc::new(ConfigCache::new(TradingConfigSnapshot::default()));
    let watcher_cache = Arc::clone(&cache);
    let watcher_client = client.clone();
    let watcher_handle = tokio::spawn(async move {
        Watcher::new(watcher_client)
            .run(watcher_cache, NAMESPACE.to_string(), CM.to_string())
            .await
            .expect("watcher exited with error");
    });

    upsert_config_map(&cms, NAMESPACE, CM, 3, true)
        .await
        .expect("failed to create ConfigMap");

    let cache_ref = Arc::clone(&cache);
    timeout(Duration::from_secs(15), async move {
        poll_until(Duration::from_secs(15), Duration::from_millis(250), || {
            cache_ref.load().schema_version == 3
        })
        .await;
    })
    .await
    .expect("timed out waiting for cache schema_version=3");

    let port = start_server(Arc::clone(&cache), client.clone()).await;
    let mut grpc = connect(port).await;

    grpc.ready().await.expect("client not ready");
    let req = build_read_all_request(NAMESPACE);
    let path =
        http::uri::PathAndQuery::from_static("/konfig.v1.KonfigService/ReadAll");
    let resp = grpc
        .server_streaming(Request::new(req), path, konfig::codec::FlatBuffersCodec)
        .await
        .expect("ReadAll RPC failed");

    let mut stream = resp.into_inner();
    let first = stream
        .next()
        .await
        .expect("ReadAll must yield at least one item")
        .expect("stream item must not be an error");

    assert!(!first.is_empty(), "ReadAll item must not be empty");

    let next = stream.next().await;
    assert!(
        next.is_none(),
        "ReadAll must stream exactly one entry for a single-pair watcher"
    );

    maybe_delete(&cms, CM).await;
    watcher_handle.abort();
}

/// Apply RPC writes FlatBuffers binaryData to a ConfigMap and returns the
/// resource_version.  A subsequent Read RPC reflects the new schema_version
/// after the watcher delivers the event.
#[tokio::test]
async fn grpc_apply_writes_config_map_and_read_reflects_it() {
    let (_container, client) = k3s_client().await;
    let cms: Api<ConfigMap> = Api::namespaced(client.clone(), NAMESPACE);

    maybe_delete(&cms, CM_GRPC_APPLY).await;

    let cache = Arc::new(ConfigCache::new(TradingConfigSnapshot::default()));
    let watcher_cache = Arc::clone(&cache);
    let watcher_client = client.clone();
    let watcher_handle = tokio::spawn(async move {
        Watcher::new(watcher_client)
            .run(
                watcher_cache,
                NAMESPACE.to_string(),
                CM_GRPC_APPLY.to_string(),
            )
            .await
            .expect("watcher exited with error");
    });

    let port = start_server(Arc::clone(&cache), client.clone()).await;
    let mut grpc = connect(port).await;

    // Apply schema_version=5 via gRPC.
    let yaml = "\
schema_version: 5
risk:
  max_position_usd: 150000.0
  max_order_size_usd: 7500.0
  max_daily_loss_usd: 3000.0
  max_orders_per_second: 75
  max_notional_per_minute: 750000.0
  enabled: true
strategies:
  - product_id: BTC-USDT
    signal_threshold: 0.8
    lookback_window_ms: 45000
    max_spread_bps: 18.0
    enabled: true
";

    grpc.ready().await.expect("client not ready");
    let req = build_apply_request(NAMESPACE, CM_GRPC_APPLY, yaml);
    let path =
        http::uri::PathAndQuery::from_static("/konfig.v1.KonfigService/Apply");
    let resp = grpc
        .unary(Request::new(req), path, konfig::codec::FlatBuffersCodec)
        .await
        .expect("Apply RPC failed");

    let apply_bytes = resp.into_inner();
    assert!(!apply_bytes.is_empty(), "ApplyResponse must not be empty");

    // ApplyResponse.resource_version (slot 4) must be non-empty.
    let rv = read_string_field(&apply_bytes, 4);
    assert!(!rv.is_empty(), "resource_version must be non-empty after Apply");

    // Wait for the watcher to deliver the Apply event to the cache.
    let cache_ref = Arc::clone(&cache);
    timeout(Duration::from_secs(15), async move {
        poll_until(Duration::from_secs(15), Duration::from_millis(250), || {
            cache_ref.load().schema_version == 5
        })
        .await;
    })
    .await
    .expect("timed out waiting for cache schema_version=5 after Apply");

    // Now Read should reflect schema_version=5.
    grpc.ready().await.expect("client not ready");
    let req = build_read_request(NAMESPACE, CM_GRPC_APPLY);
    let path = http::uri::PathAndQuery::from_static("/konfig.v1.KonfigService/Read");
    let read_resp = grpc
        .unary(Request::new(req), path, konfig::codec::FlatBuffersCodec)
        .await
        .expect("Read after Apply failed");

    let read_bytes = read_resp.into_inner();
    assert!(!read_bytes.is_empty());

    // Cleanup.
    maybe_delete(&cms, CM_GRPC_APPLY).await;
    watcher_handle.abort();
}

/// Apply with schema_version <= current returns FAILED_PRECONDITION.
#[tokio::test]
async fn grpc_apply_rejects_schema_version_downgrade() {
    let (_container, client) = k3s_client().await;
    let cms: Api<ConfigMap> = Api::namespaced(client.clone(), NAMESPACE);

    const CM: &str = "trading-config-grpc-downgrade";
    maybe_delete(&cms, CM).await;

    let cache = Arc::new(ConfigCache::new(TradingConfigSnapshot::default()));
    let port = start_server(Arc::clone(&cache), client.clone()).await;
    let mut grpc = connect(port).await;

    // Apply schema_version=10 (initial, should succeed).
    let yaml_v10 = "schema_version: 10\n";
    grpc.ready().await.expect("client not ready");
    let req = build_apply_request(NAMESPACE, CM, yaml_v10);
    let path = http::uri::PathAndQuery::from_static("/konfig.v1.KonfigService/Apply");
    grpc.unary(Request::new(req), path, konfig::codec::FlatBuffersCodec)
        .await
        .expect("initial Apply must succeed");

    // Try Apply schema_version=10 again (equal — must fail).
    grpc.ready().await.expect("client not ready");
    let req = build_apply_request(NAMESPACE, CM, yaml_v10);
    let path = http::uri::PathAndQuery::from_static("/konfig.v1.KonfigService/Apply");
    let result = grpc
        .unary(Request::new(req), path, konfig::codec::FlatBuffersCodec)
        .await;

    assert!(result.is_err(), "Apply with equal schema_version must fail");
    let status = result.unwrap_err();
    assert_eq!(
        status.code(),
        tonic::Code::FailedPrecondition,
        "expected FAILED_PRECONDITION for equal schema_version, got {status:?}"
    );

    // Try Apply schema_version=5 (less than 10 — must also fail).
    grpc.ready().await.expect("client not ready");
    let yaml_v5 = "schema_version: 5\n";
    let req = build_apply_request(NAMESPACE, CM, yaml_v5);
    let path = http::uri::PathAndQuery::from_static("/konfig.v1.KonfigService/Apply");
    let result = grpc
        .unary(Request::new(req), path, konfig::codec::FlatBuffersCodec)
        .await;

    assert!(result.is_err(), "Apply with lesser schema_version must fail");
    let status = result.unwrap_err();
    assert_eq!(
        status.code(),
        tonic::Code::FailedPrecondition,
        "expected FAILED_PRECONDITION for lesser schema_version, got {status:?}"
    );

    maybe_delete(&cms, CM).await;
}
