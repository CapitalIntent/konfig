//! Watch-stream timing integration test for Config CRD watcher.
//!
//! Verifies that the watcher propagates a Config CRD apply event to the cache
//! within 500 ms of the apply on a live K3s cluster.

#![cfg(feature = "integration")]

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::time::timeout;

use konfig::cache::ConfigCache;
use konfig::metrics::LastEventAt;
use konfig::types::ConfigSnapshot;
use konfig::watcher::Watcher;

use common::{install_crd, k3s_client, maybe_delete, poll_until, upsert_config};

const NAMESPACE: &str = "default";
const CFG_TIMING: &str = "integ-timing";

/// Watch-stream timing: subscribe → apply → cache update ≤ 500 ms.
#[tokio::test]
async fn watch_stream_update_propagates_within_500ms() {
    let (_container, client) = k3s_client().await;
    install_crd(&client).await;
    maybe_delete(&client, NAMESPACE, CFG_TIMING).await;

    let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
    let watcher_cache = Arc::clone(&cache);
    let watcher_client = client.clone();
    let last_event_at = Arc::new(LastEventAt::new());
    let watcher = tokio::spawn(async move {
        Watcher::new(watcher_client)
            .run(
                watcher_cache,
                NAMESPACE.to_string(),
                CFG_TIMING.to_string(),
                last_event_at,
            )
            .await
            .expect("watcher error");
    });

    // Give the watcher time to establish the watch stream.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // ── CREATE event ──────────────────────────────────────────────────────────
    let t0 = Instant::now();
    upsert_config(
        &client,
        NAMESPACE,
        CFG_TIMING,
        10,
        json!({"step": "create"}),
    )
    .await
    .expect("create Config v10");

    let cache_ref = Arc::clone(&cache);
    timeout(Duration::from_secs(5), async move {
        poll_until(Duration::from_secs(5), Duration::from_millis(10), || {
            cache_ref.load().schema_version == 10
        })
        .await;
    })
    .await
    .expect("timed out waiting for CREATE event within 5 s");

    let elapsed_create = t0.elapsed();
    assert!(
        elapsed_create <= Duration::from_millis(500),
        "CREATE event propagation took {elapsed_create:?} — must be ≤ 500 ms"
    );

    // ── MODIFIED event ────────────────────────────────────────────────────────
    let t1 = Instant::now();
    upsert_config(
        &client,
        NAMESPACE,
        CFG_TIMING,
        11,
        json!({"step": "modify"}),
    )
    .await
    .expect("update Config v11");

    let cache_ref2 = Arc::clone(&cache);
    timeout(Duration::from_secs(5), async move {
        poll_until(Duration::from_secs(5), Duration::from_millis(10), || {
            cache_ref2.load().schema_version == 11
        })
        .await;
    })
    .await
    .expect("timed out waiting for MODIFIED event within 5 s");

    let elapsed_modified = t1.elapsed();
    assert!(
        elapsed_modified <= Duration::from_millis(500),
        "MODIFIED event propagation took {elapsed_modified:?} — must be ≤ 500 ms"
    );

    // ── Cleanup ───────────────────────────────────────────────────────────────
    maybe_delete(&client, NAMESPACE, CFG_TIMING).await;
    watcher.abort();
}
