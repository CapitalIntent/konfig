//! Integration tests for the Konfig gRPC service.
//!
//! Spins up a K3s container via Testcontainers, starts the KonfigService
//! gRPC server against it, and verifies Get / Apply behaviour through a
//! real Protobuf-over-gRPC connection.

#![cfg(feature = "integration")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::time::timeout;
use tonic::Request;

use dashmap::DashMap;

use konfig::cache::ConfigCache;
use konfig::grpc::{ServerConfig, serve};
use konfig::metrics::{LastEventAt, LastEventAtMap};
use konfig::proto::konfig_service_client::KonfigServiceClient;
use konfig::proto::{ApplyRequest, GetAllRequest, GetRequest};
use konfig::types::ConfigSnapshot;
use konfig::watcher::Watcher;

use common::{install_crd, k3s_client, maybe_delete, poll_until, upsert_config};

const NAMESPACE: &str = "default";
const CFG_GRPC_GET: &str = "integ-grpc-get";
const CFG_GRPC_APPLY: &str = "integ-grpc-apply";

// ── Shared server setup ───────────────────────────────────────────────────────

async fn start_server(cache: Arc<ConfigCache>, kube_client: kube::Client) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind listener");
    let addr = listener.local_addr().expect("no local addr");
    drop(listener);

    let cfg = ServerConfig {
        addr,
        cache,
        secret_cache: Arc::new(konfig::secret_cache::SecretCache::new()),
        kube_client,
        health_reporter: None,
        secret_namespace_broadcasts: Arc::new(DashMap::new()),
        last_event_at_map: Arc::new(DashMap::new()) as LastEventAtMap,
    };

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        serve(cfg).await.expect("gRPC server exited with error");
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    addr.port()
}

async fn connect(port: u16) -> KonfigServiceClient<tonic::transport::Channel> {
    KonfigServiceClient::connect(format!("http://127.0.0.1:{port}"))
        .await
        .expect("failed to connect to gRPC server")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Get RPC returns a Config for a seeded cache.
#[tokio::test]
async fn grpc_get_returns_config() {
    let (_container, client) = k3s_client().await;
    install_crd(&client).await;
    maybe_delete(&client, NAMESPACE, CFG_GRPC_GET).await;

    let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
    let watcher_cache = Arc::clone(&cache);
    let watcher_client = client.clone();
    let last_event_at = Arc::new(LastEventAt::new());
    let watcher_handle = tokio::spawn(async move {
        Watcher::new(watcher_client)
            .run(
                watcher_cache,
                NAMESPACE.to_string(),
                CFG_GRPC_GET.to_string(),
                last_event_at,
            )
            .await
            .expect("watcher error");
    });

    upsert_config(&client, NAMESPACE, CFG_GRPC_GET, 1, json!({"env": "prod"}))
        .await
        .expect("create Config v1");

    let cache_ref = Arc::clone(&cache);
    timeout(Duration::from_secs(15), async move {
        poll_until(Duration::from_secs(15), Duration::from_millis(250), || {
            cache_ref.load().schema_version == 1
        })
        .await;
    })
    .await
    .expect("timed out waiting for cache schema_version=1");

    let port = start_server(Arc::clone(&cache), client.clone()).await;
    let mut grpc = connect(port).await;

    let resp = grpc
        .get(Request::new(GetRequest {
            namespace: NAMESPACE.into(),
            name: CFG_GRPC_GET.into(),
        }))
        .await
        .expect("Get RPC failed");

    let cfg = resp.into_inner();
    assert_eq!(cfg.schema_version, 1);
    assert!(!cfg.content_json.is_empty());
    assert_eq!(cfg.name, CFG_GRPC_GET);
    assert_eq!(cfg.namespace, NAMESPACE);

    maybe_delete(&client, NAMESPACE, CFG_GRPC_GET).await;
    watcher_handle.abort();
}

/// Get RPC returns NOT_FOUND when the cache has not been populated.
#[tokio::test]
async fn grpc_get_not_found_when_cache_empty() {
    let (_container, client) = k3s_client().await;

    let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
    let port = start_server(Arc::clone(&cache), client.clone()).await;
    let mut grpc = connect(port).await;

    let result = grpc
        .get(Request::new(GetRequest {
            namespace: NAMESPACE.into(),
            name: "nonexistent".into(),
        }))
        .await;

    assert!(result.is_err(), "Get must return NOT_FOUND for empty cache");
    assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
}

/// GetAll RPC streams one Config for a populated cache.
#[tokio::test]
async fn grpc_get_all_streams_one_entry() {
    use tokio_stream::StreamExt;

    let (_container, client) = k3s_client().await;
    install_crd(&client).await;

    const CFG: &str = "integ-grpc-getall";
    maybe_delete(&client, NAMESPACE, CFG).await;

    let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
    let watcher_cache = Arc::clone(&cache);
    let watcher_client = client.clone();
    let last_event_at = Arc::new(LastEventAt::new());
    let watcher_handle = tokio::spawn(async move {
        Watcher::new(watcher_client)
            .run(
                watcher_cache,
                NAMESPACE.to_string(),
                CFG.to_string(),
                last_event_at,
            )
            .await
            .expect("watcher error");
    });

    upsert_config(&client, NAMESPACE, CFG, 3, json!({"level": "debug"}))
        .await
        .expect("create Config v3");

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

    let resp = grpc
        .get_all(Request::new(GetAllRequest {
            namespace: NAMESPACE.into(),
        }))
        .await
        .expect("GetAll RPC failed");

    let mut stream = resp.into_inner();
    let first = stream
        .next()
        .await
        .expect("must yield one item")
        .expect("no error");
    assert_eq!(first.schema_version, 3);

    assert!(
        stream.next().await.is_none(),
        "must stream exactly one entry"
    );

    maybe_delete(&client, NAMESPACE, CFG).await;
    watcher_handle.abort();
}

/// Apply RPC writes a Config CRD and returns the resource_version.
#[tokio::test]
async fn grpc_apply_writes_config_and_get_reflects_it() {
    let (_container, client) = k3s_client().await;
    install_crd(&client).await;
    maybe_delete(&client, NAMESPACE, CFG_GRPC_APPLY).await;

    let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
    let watcher_cache = Arc::clone(&cache);
    let watcher_client = client.clone();
    let last_event_at = Arc::new(LastEventAt::new());
    let watcher_handle = tokio::spawn(async move {
        Watcher::new(watcher_client)
            .run(
                watcher_cache,
                NAMESPACE.to_string(),
                CFG_GRPC_APPLY.to_string(),
                last_event_at,
            )
            .await
            .expect("watcher error");
    });

    let port = start_server(Arc::clone(&cache), client.clone()).await;
    let mut grpc = connect(port).await;

    let yaml = "schema_version: 5\ncontent:\n  mode: production\n  replicas: 3\n";

    let apply_resp = grpc
        .apply(Request::new(ApplyRequest {
            namespace: NAMESPACE.into(),
            name: CFG_GRPC_APPLY.into(),
            yaml_content: yaml.into(),
        }))
        .await
        .expect("Apply RPC failed");

    let rv = apply_resp.into_inner().resource_version;
    assert!(
        !rv.is_empty(),
        "resource_version must be non-empty after Apply"
    );

    // Wait for watcher to deliver the Apply event.
    let cache_ref = Arc::clone(&cache);
    timeout(Duration::from_secs(15), async move {
        poll_until(Duration::from_secs(15), Duration::from_millis(250), || {
            cache_ref.load().schema_version == 5
        })
        .await;
    })
    .await
    .expect("timed out waiting for cache schema_version=5 after Apply");

    // Get should now reflect schema_version=5.
    let get_resp = grpc
        .get(Request::new(GetRequest {
            namespace: NAMESPACE.into(),
            name: CFG_GRPC_APPLY.into(),
        }))
        .await
        .expect("Get after Apply failed");

    let cfg = get_resp.into_inner();
    assert_eq!(cfg.schema_version, 5);

    maybe_delete(&client, NAMESPACE, CFG_GRPC_APPLY).await;
    watcher_handle.abort();
}

/// Apply with schema_version <= current returns FAILED_PRECONDITION.
#[tokio::test]
async fn grpc_apply_rejects_schema_version_downgrade() {
    let (_container, client) = k3s_client().await;
    install_crd(&client).await;

    const CFG: &str = "integ-grpc-downgrade";
    maybe_delete(&client, NAMESPACE, CFG).await;

    let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
    let port = start_server(Arc::clone(&cache), client.clone()).await;
    let mut grpc = connect(port).await;

    // Apply schema_version=10 (initial — must succeed).
    grpc.apply(Request::new(ApplyRequest {
        namespace: NAMESPACE.into(),
        name: CFG.into(),
        yaml_content: "schema_version: 10\n".into(),
    }))
    .await
    .expect("initial Apply must succeed");

    // Apply schema_version=10 again (equal — must fail).
    let result = grpc
        .apply(Request::new(ApplyRequest {
            namespace: NAMESPACE.into(),
            name: CFG.into(),
            yaml_content: "schema_version: 10\n".into(),
        }))
        .await;

    assert!(result.is_err(), "Apply with equal schema_version must fail");
    assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);

    // Apply schema_version=5 (less than 10 — must also fail).
    let result = grpc
        .apply(Request::new(ApplyRequest {
            namespace: NAMESPACE.into(),
            name: CFG.into(),
            yaml_content: "schema_version: 5\n".into(),
        }))
        .await;

    assert!(
        result.is_err(),
        "Apply with lesser schema_version must fail"
    );
    assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);

    maybe_delete(&client, NAMESPACE, CFG).await;
}
