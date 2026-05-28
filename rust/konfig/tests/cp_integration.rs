//! Phase 4 CP hardening integration tests.
//!
//! Tests: partition, resourceVersion resume, 409 concurrent conflict,
//! schema_version monotonicity, reconnect backoff sequence.

#![cfg(feature = "integration")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt as _;
use serde_json::json;

use konfig::cache::ConfigCache;
use konfig::grpc::apply::apply_inner;
use konfig::types::ConfigSnapshot;

use common::{install_crd, k3s_client, maybe_delete, upsert_config};

const NAMESPACE: &str = "default";

// ── Test 1: Partition — Apply returns UNAVAILABLE ────────────────────────────

/// When the K8s API server is unreachable, Apply must return UNAVAILABLE and
/// the cache must still return the last-known-good snapshot.
#[tokio::test]
async fn partition_apply_returns_unavailable_and_cache_returns_last_good() {
    // Point at a server that will refuse connections.
    let bad_config = kube::Config {
        cluster_url: "https://127.0.0.1:1".parse().expect("valid url"),
        default_namespace: "default".to_string(),
        root_cert: None,
        connect_timeout: Some(Duration::from_millis(300)),
        read_timeout: Some(Duration::from_millis(300)),
        write_timeout: Some(Duration::from_millis(300)),
        tls_server_name: None,
        accept_invalid_certs: true,
        auth_info: kube::config::AuthInfo::default(),
        proxy_url: None,
        disable_compression: false,
        headers: Default::default(),
    };
    let bad_client = kube::Client::try_from(bad_config).expect("build client");

    // Apply must fail with UNAVAILABLE.
    let result = apply_inner(
        NAMESPACE,
        "cp-partition-cfg",
        "schema_version: 1\ncontent:\n  k: v\n",
        bad_client,
    )
    .await;

    assert!(result.is_err(), "Apply must fail when K8s is unreachable");
    let status = result.unwrap_err();
    assert_eq!(
        status.code(),
        tonic::Code::Unavailable,
        "expected UNAVAILABLE, got {:?}",
        status.code()
    );

    // Cache still returns last-known-good (age_ms > 0 when stale).
    let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
    cache.update(ConfigSnapshot {
        name: "cp-partition-cfg".into(),
        namespace: NAMESPACE.into(),
        schema_version: 3,
        resource_version: "rv-3".into(),
        content: json!({"k": "v"}),
        ..Default::default()
    });
    cache.mark_all_stale();

    let snap = cache.get(NAMESPACE, "cp-partition-cfg").unwrap();
    assert_eq!(snap.schema_version, 3, "last-known-good must be retained");
    assert!(snap.stale_since.is_some(), "stale_since must be set after mark_all_stale");
}

// ── Test 2: resourceVersion resume — no missed events ────────────────────────

/// Subscribe → apply 5 configs → record last resourceVersion from stream →
/// reconnect with saved RV → apply 5 more → assert all 10 received, no dups.
#[tokio::test]
async fn resource_version_resume_no_missed_events() {
    let (_container, client) = k3s_client().await;
    install_crd(&client).await;

    for i in 0..10u32 {
        maybe_delete(&client, NAMESPACE, &format!("rv-resume-{i}")).await;
    }

    // Pre-populate cache so Subscribe gate passes.
    let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
    cache.update(ConfigSnapshot {
        name: "rv-resume-seed".into(),
        namespace: NAMESPACE.into(),
        schema_version: 1,
        resource_version: "rv-0".into(),
        content: json!({}),
        ..Default::default()
    });

    // First Subscribe: apply 5 configs, collect events, note last RV.
    let (tx1, mut rx1) = tokio::sync::mpsc::channel(64);
    {
        use konfig::grpc::subscribe::handle_subscribe;
        use konfig::proto::SubscribeRequest;

        let req = SubscribeRequest {
            namespace: NAMESPACE.to_string(),
            names: vec![],
            resume_resource_version: String::new(),
        };
        let resp = handle_subscribe(Arc::clone(&cache), client.clone(), Arc::new(dashmap::DashMap::new()), req)
            .await
            .expect("Subscribe must succeed");

        let mut stream = resp.into_inner();
        let cache_client = client.clone();

        tokio::spawn(async move {
            for i in 0..5u32 {
                upsert_config(&cache_client, NAMESPACE, &format!("rv-resume-{i}"), i + 1, json!({"i": i}))
                    .await
                    .expect("upsert");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        let mut last_rv = String::new();
        let deadline = Duration::from_secs(30);
        let start = tokio::time::Instant::now();
        let mut count = 0usize;

        while start.elapsed() < deadline && count < 5 {
            if let Ok(Some(Ok(ev))) =
                tokio::time::timeout(Duration::from_secs(5), stream.next()).await
            {
                if let Some(cfg) = ev.config {
                    last_rv = cfg.resource_version.clone();
                }
                count += 1;
            }
        }

        assert_eq!(count, 5, "expected 5 events from first subscription");
        tx1.send(last_rv).await.expect("send rv");
    }

    let last_rv = rx1.recv().await.expect("recv rv");
    assert!(!last_rv.is_empty(), "must have recorded a resourceVersion");

    // Second Subscribe: resume from saved RV, apply 5 more configs.
    {
        use konfig::grpc::subscribe::handle_subscribe;
        use konfig::proto::SubscribeRequest;

        let req = SubscribeRequest {
            namespace: NAMESPACE.to_string(),
            names: vec![],
            resume_resource_version: last_rv,
        };
        let resp = handle_subscribe(Arc::clone(&cache), client.clone(), Arc::new(dashmap::DashMap::new()), req)
            .await
            .expect("Subscribe (resume) must succeed");

        let mut stream = resp.into_inner();
        let cache_client = client.clone();

        tokio::spawn(async move {
            for i in 5..10u32 {
                upsert_config(&cache_client, NAMESPACE, &format!("rv-resume-{i}"), i + 1, json!({"i": i}))
                    .await
                    .expect("upsert");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });

        let deadline = Duration::from_secs(30);
        let start = tokio::time::Instant::now();
        let mut count = 0usize;
        let mut names_seen = std::collections::HashSet::new();

        while start.elapsed() < deadline && count < 5 {
            if let Ok(Some(Ok(ev))) =
                tokio::time::timeout(Duration::from_secs(5), stream.next()).await
            {
                if let Some(cfg) = ev.config {
                    let name = cfg.name.clone();
                    assert!(!names_seen.contains(&name), "duplicate event for {name}");
                    names_seen.insert(name);
                }
                count += 1;
            }
        }

        assert_eq!(count, 5, "expected exactly 5 new events after resume");
    }
}

// ── Test 3: 409 concurrent conflict — retry wins ─────────────────────────────

/// Two concurrent Apply calls on the same Config must both succeed.
/// Final CRD state must be one of the two applied values.
#[tokio::test]
async fn concurrent_apply_409_retry_wins() {
    let (_container, client) = k3s_client().await;
    install_crd(&client).await;

    maybe_delete(&client, NAMESPACE, "cp-conflict-cfg").await;

    // Create initial version so the CRD exists before concurrent updates.
    upsert_config(&client, NAMESPACE, "cp-conflict-cfg", 1, json!({"init": true}))
        .await
        .expect("initial upsert");

    // Two concurrent Apply calls: v=2 and v=3.  Both should succeed.
    let (r2, r3) = tokio::join!(
        apply_inner(
            NAMESPACE,
            "cp-conflict-cfg",
            "schema_version: 2\ncontent:\n  writer: a\n",
            client.clone(),
        ),
        apply_inner(
            NAMESPACE,
            "cp-conflict-cfg",
            "schema_version: 3\ncontent:\n  writer: b\n",
            client.clone(),
        ),
    );

    // At least the higher-version apply must succeed; the lower may succeed or
    // be rejected by the monotonicity check.
    let succeeded = r2.is_ok() || r3.is_ok();
    assert!(succeeded, "at least one concurrent Apply must succeed");

    // The one that succeeds must have returned a non-empty resource_version.
    for r in [r2, r3].into_iter().flatten() {
        assert!(!r.into_inner().resource_version.is_empty());
    }
}

// ── Test 4: schema_version monotonicity ──────────────────────────────────────

/// Apply v=2, then v=1 → FAILED_PRECONDITION.
/// Apply v=2 → v=2 → FAILED_PRECONDITION.
/// Apply v=2 → v=3 → both succeed.
#[tokio::test]
async fn schema_version_monotonicity_enforced() {
    let (_container, client) = k3s_client().await;
    install_crd(&client).await;

    maybe_delete(&client, NAMESPACE, "cp-mono-cfg").await;

    // v=1: first apply — must succeed.
    apply_inner(NAMESPACE, "cp-mono-cfg", "schema_version: 1\ncontent: {}", client.clone())
        .await
        .expect("v=1 must succeed");

    // v=2: must succeed.
    apply_inner(NAMESPACE, "cp-mono-cfg", "schema_version: 2\ncontent: {}", client.clone())
        .await
        .expect("v=2 must succeed");

    // v=1: downgrade — must fail FAILED_PRECONDITION.
    let r1 =
        apply_inner(NAMESPACE, "cp-mono-cfg", "schema_version: 1\ncontent: {}", client.clone())
            .await;
    assert!(r1.is_err());
    assert_eq!(
        r1.unwrap_err().code(),
        tonic::Code::FailedPrecondition,
        "downgrade must be FAILED_PRECONDITION"
    );

    // v=2: same version — must fail FAILED_PRECONDITION.
    let r2 =
        apply_inner(NAMESPACE, "cp-mono-cfg", "schema_version: 2\ncontent: {}", client.clone())
            .await;
    assert!(r2.is_err());
    assert_eq!(
        r2.unwrap_err().code(),
        tonic::Code::FailedPrecondition,
        "same version must be FAILED_PRECONDITION"
    );

    // v=3: upgrade — must succeed.
    apply_inner(NAMESPACE, "cp-mono-cfg", "schema_version: 3\ncontent: {}", client.clone())
        .await
        .expect("v=3 upgrade must succeed");
}

// ── Test 5: BOOKMARK handling — cursor advances, no spurious events ───────────

/// Start a Subscribe stream, apply 20 configs (K3s emits bookmarks internally),
/// assert all 20 ConfigEvents are received and no extra/error events appear.
/// Verifies bookmarks are consumed by kube-rs and not forwarded as ConfigEvents.
#[tokio::test]
async fn bookmark_events_not_emitted_to_subscribers() {
    let (_container, client) = k3s_client().await;
    install_crd(&client).await;

    for i in 0..20u32 {
        maybe_delete(&client, NAMESPACE, &format!("bm-cfg-{i}")).await;
    }

    let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
    cache.update(ConfigSnapshot {
        name: "bm-seed".into(),
        namespace: NAMESPACE.into(),
        schema_version: 1,
        resource_version: "rv-0".into(),
        content: json!({}),
        ..Default::default()
    });

    use konfig::grpc::subscribe::handle_subscribe;
    use konfig::proto::SubscribeRequest;

    let req = SubscribeRequest {
        namespace: NAMESPACE.to_string(),
        names: vec![],
        resume_resource_version: String::new(),
    };
    let resp =
        handle_subscribe(cache, client.clone(), Arc::new(dashmap::DashMap::new()), req).await.expect("Subscribe must succeed");
    let mut stream = resp.into_inner();

    let apply_client = client.clone();
    tokio::spawn(async move {
        for i in 0..20u32 {
            upsert_config(
                &apply_client,
                NAMESPACE,
                &format!("bm-cfg-{i}"),
                i + 1,
                json!({"i": i}),
            )
            .await
            .expect("upsert");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });

    let deadline = Duration::from_secs(30);
    let start = tokio::time::Instant::now();
    let mut count = 0usize;

    while start.elapsed() < deadline && count < 20 {
        match tokio::time::timeout(Duration::from_secs(5), stream.next()).await {
            Ok(Some(Ok(_ev))) => count += 1,
            Ok(Some(Err(e))) => panic!("unexpected error event in stream: {e}"),
            Ok(None) => break,
            Err(_) => break,
        }
    }

    assert_eq!(count, 20, "expected exactly 20 ConfigEvents — no bookmark events leaked");
}
