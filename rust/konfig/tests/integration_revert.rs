//! Integration tests for the `Revert` gRPC RPC.
//!
//! Each test spins up a K3s container via Testcontainers, installs the Config
//! CRD, starts the KonfigService gRPC server against it, and exercises Revert
//! end-to-end through the gRPC client.

#![cfg(feature = "integration")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use serde_json::json;
use tokio::time::timeout;
use tokio_stream::StreamExt;
use tonic::Request;

use konfig::cache::ConfigCache;
use konfig::grpc::{ServerConfig, serve};
use konfig::proto::konfig_service_client::KonfigServiceClient;
use konfig::proto::{ApplyRequest, GetRequest, RevertRequest, SubscribeRequest};
use konfig::types::ConfigSnapshot;
use konfig::watcher::Watcher;

use common::{install_crd, k3s_client, maybe_delete, poll_until};

const NAMESPACE: &str = "default";

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

/// Apply v1, capture the resourceVersion of the historical revision, Apply v2,
/// then Revert to the historical RV.  The resulting Config must carry the
/// historical content AND a schema_version strictly greater than v2.
#[tokio::test]
async fn revert_replays_historical_content() {
    let (_container, client) = k3s_client().await;
    install_crd(&client).await;
    const CFG: &str = "integ-revert-replay";
    maybe_delete(&client, NAMESPACE, CFG).await;

    let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
    let watcher_cache = Arc::clone(&cache);
    let watcher_client = client.clone();
    let watcher_handle = tokio::spawn(async move {
        Watcher::new(watcher_client)
            .run(watcher_cache, NAMESPACE.to_string(), CFG.to_string())
            .await
            .expect("watcher error");
    });

    let port = start_server(Arc::clone(&cache), client.clone()).await;
    let mut grpc = connect(port).await;

    // Apply v1 with content {"k":"a"} — capture its resource_version.
    let v1_rv = grpc
        .apply(Request::new(ApplyRequest {
            namespace: NAMESPACE.into(),
            name: CFG.into(),
            yaml_content: "schema_version: 1\ncontent:\n  k: a\n".into(),
        }))
        .await
        .expect("Apply v1")
        .into_inner()
        .resource_version;

    // Apply v2 with content {"k":"b"}.
    grpc.apply(Request::new(ApplyRequest {
        namespace: NAMESPACE.into(),
        name: CFG.into(),
        yaml_content: "schema_version: 2\ncontent:\n  k: b\n".into(),
    }))
    .await
    .expect("Apply v2");

    // Wait for cache to reflect v2.
    let cache_ref = Arc::clone(&cache);
    timeout(Duration::from_secs(15), async move {
        poll_until(Duration::from_secs(15), Duration::from_millis(250), || {
            cache_ref.load().schema_version == 2
        })
        .await;
    })
    .await
    .expect("timed out waiting for cache schema_version=2");

    // Revert to the v1 resource_version.
    let revert_resp = grpc
        .revert(Request::new(RevertRequest {
            namespace: NAMESPACE.into(),
            name: CFG.into(),
            to_resource_version: v1_rv.clone(),
        }))
        .await
        .expect("Revert RPC")
        .into_inner();

    // New schema_version must be strictly greater than the latest applied (=2).
    assert!(
        revert_resp.schema_version > 2,
        "schema_version must be > 2 after revert; got {}",
        revert_resp.schema_version
    );

    // Wait for the watcher to deliver the reverted state into the cache.
    let cache_ref = Arc::clone(&cache);
    let expected_sv = revert_resp.schema_version;
    timeout(Duration::from_secs(15), async move {
        poll_until(Duration::from_secs(15), Duration::from_millis(250), || {
            cache_ref.load().schema_version == expected_sv
        })
        .await;
    })
    .await
    .expect("timed out waiting for cache to reflect reverted schema_version");

    // The Config must now carry the historical content.
    let cfg = grpc
        .get(Request::new(GetRequest {
            namespace: NAMESPACE.into(),
            name: CFG.into(),
        }))
        .await
        .expect("Get after Revert")
        .into_inner();

    let content: serde_json::Value =
        serde_json::from_str(&cfg.content_json).expect("parse content_json");
    assert_eq!(
        content,
        json!({"k": "a"}),
        "reverted content must match historical revision"
    );
    assert_eq!(cfg.schema_version, expected_sv);

    maybe_delete(&client, NAMESPACE, CFG).await;
    watcher_handle.abort();
}

/// Reverting to a resourceVersion that has never existed returns
/// FAILED_PRECONDITION (the K8s API returns 410 Gone for compacted RVs).
#[tokio::test]
async fn revert_unknown_rv_returns_failed_precondition() {
    let (_container, client) = k3s_client().await;
    install_crd(&client).await;
    const CFG: &str = "integ-revert-bad-rv";
    maybe_delete(&client, NAMESPACE, CFG).await;

    let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
    let port = start_server(Arc::clone(&cache), client.clone()).await;
    let mut grpc = connect(port).await;

    // Seed a config so the name exists.
    grpc.apply(Request::new(ApplyRequest {
        namespace: NAMESPACE.into(),
        name: CFG.into(),
        yaml_content: "schema_version: 1\ncontent:\n  k: a\n".into(),
    }))
    .await
    .expect("Apply v1");

    // A resourceVersion of "1" is essentially always already compacted by the
    // time we ask, so K8s returns 410 → server maps to FAILED_PRECONDITION.
    let result = grpc
        .revert(Request::new(RevertRequest {
            namespace: NAMESPACE.into(),
            name: CFG.into(),
            to_resource_version: "1".into(),
        }))
        .await;

    let err = result.expect_err("Revert to compacted RV must fail");
    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "compacted RV must return FAILED_PRECONDITION, got: {err:?}"
    );

    maybe_delete(&client, NAMESPACE, CFG).await;
}

/// A live Subscribe stream must observe the MODIFIED event emitted by a Revert.
#[tokio::test]
async fn revert_emits_broadcast_event() {
    let (_container, client) = k3s_client().await;
    install_crd(&client).await;
    const CFG: &str = "integ-revert-broadcast";
    maybe_delete(&client, NAMESPACE, CFG).await;

    let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
    let watcher_cache = Arc::clone(&cache);
    let watcher_client = client.clone();
    let watcher_handle = tokio::spawn(async move {
        Watcher::new(watcher_client)
            .run(watcher_cache, NAMESPACE.to_string(), CFG.to_string())
            .await
            .expect("watcher error");
    });

    let port = start_server(Arc::clone(&cache), client.clone()).await;
    let mut grpc = connect(port).await;

    // Apply v1, capture its RV.
    let v1_rv = grpc
        .apply(Request::new(ApplyRequest {
            namespace: NAMESPACE.into(),
            name: CFG.into(),
            yaml_content: "schema_version: 1\ncontent:\n  k: a\n".into(),
        }))
        .await
        .expect("Apply v1")
        .into_inner()
        .resource_version;

    // Apply v2.
    grpc.apply(Request::new(ApplyRequest {
        namespace: NAMESPACE.into(),
        name: CFG.into(),
        yaml_content: "schema_version: 2\ncontent:\n  k: b\n".into(),
    }))
    .await
    .expect("Apply v2");

    // Wait for v2 to land in the cache before subscribing.
    let cache_ref = Arc::clone(&cache);
    timeout(Duration::from_secs(15), async move {
        poll_until(Duration::from_secs(15), Duration::from_millis(250), || {
            cache_ref.load().schema_version == 2
        })
        .await;
    })
    .await
    .expect("timed out waiting for v2");

    // Open a subscribe stream — it will start from "current" (= v2).
    let mut sub_stream = grpc
        .subscribe(Request::new(SubscribeRequest {
            namespace: NAMESPACE.into(),
            names: vec![CFG.into()],
            resume_resource_version: String::new(),
        }))
        .await
        .expect("Subscribe RPC")
        .into_inner();

    // Give the server a moment to attach.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Revert to v1.
    let revert_resp = grpc
        .revert(Request::new(RevertRequest {
            namespace: NAMESPACE.into(),
            name: CFG.into(),
            to_resource_version: v1_rv,
        }))
        .await
        .expect("Revert RPC")
        .into_inner();

    // The next event observed on the subscribe stream must reflect the revert
    // (schema_version = the new bumped value, content = historical {"k":"a"}).
    let evt = timeout(Duration::from_secs(10), sub_stream.next())
        .await
        .expect("timed out waiting for revert event")
        .expect("stream closed")
        .expect("event error");

    let cfg = evt.config.expect("event missing config");
    assert_eq!(cfg.schema_version, revert_resp.schema_version);
    let content: serde_json::Value =
        serde_json::from_str(&cfg.content_json).expect("parse content_json");
    assert_eq!(content, json!({"k": "a"}));

    maybe_delete(&client, NAMESPACE, CFG).await;
    watcher_handle.abort();
}
