//! Integration tests for the Config CRD watcher.

#![cfg(feature = "integration")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::time::timeout;

use konfig::cache::ConfigCache;
use konfig::metrics::LastEventAt;
use konfig::types::ConfigSnapshot;
use konfig::watcher::Watcher;

use common::{install_crd, k3s_client, maybe_delete, poll_until, upsert_config};

const NAMESPACE: &str = "default";
const CFG_APPLY: &str = "integ-apply";
const CFG_DELETE: &str = "integ-delete";

#[tokio::test]
async fn watcher_applies_config_to_cache() {
    let (_container, client) = k3s_client().await;
    install_crd(&client).await;
    maybe_delete(&client, NAMESPACE, CFG_APPLY).await;

    let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
    let watcher_cache = Arc::clone(&cache);
    let watcher_client = client.clone();
    let last_event_at = Arc::new(LastEventAt::new());
    tokio::spawn(async move {
        Watcher::new(watcher_client)
            .run(
                watcher_cache,
                NAMESPACE.to_string(),
                CFG_APPLY.to_string(),
                last_event_at,
            )
            .await
            .expect("watcher error");
    });

    upsert_config(&client, NAMESPACE, CFG_APPLY, 1, json!({"mode": "active"}))
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
    .expect("timed out waiting for schema_version=1");

    let snap = cache.load();
    assert_eq!(snap.schema_version, 1);
    assert_eq!(snap.content["mode"], "active");

    upsert_config(&client, NAMESPACE, CFG_APPLY, 2, json!({"mode": "passive"}))
        .await
        .expect("update Config v2");

    let cache_ref = Arc::clone(&cache);
    timeout(Duration::from_secs(15), async move {
        poll_until(Duration::from_secs(15), Duration::from_millis(250), || {
            cache_ref.load().schema_version == 2
        })
        .await;
    })
    .await
    .expect("timed out waiting for schema_version=2");

    let snap = cache.load();
    assert_eq!(snap.schema_version, 2);
    assert_eq!(snap.content["mode"], "passive");
}

#[tokio::test]
async fn watcher_retains_cache_on_config_delete() {
    let (_container, client) = k3s_client().await;
    install_crd(&client).await;
    maybe_delete(&client, NAMESPACE, CFG_DELETE).await;

    let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
    let watcher_cache = Arc::clone(&cache);
    let watcher_client = client.clone();
    let last_event_at = Arc::new(LastEventAt::new());
    let watcher = tokio::spawn(async move {
        Watcher::new(watcher_client)
            .run(
                watcher_cache,
                NAMESPACE.to_string(),
                CFG_DELETE.to_string(),
                last_event_at,
            )
            .await
            .expect("watcher error");
    });

    upsert_config(&client, NAMESPACE, CFG_DELETE, 5, json!({"mode": "active"}))
        .await
        .expect("create Config v5");

    let cache_ref = Arc::clone(&cache);
    timeout(Duration::from_secs(15), async move {
        poll_until(Duration::from_secs(15), Duration::from_millis(250), || {
            cache_ref.load().schema_version == 5
        })
        .await;
    })
    .await
    .expect("timed out waiting for schema_version=5");

    // Delete the Config CRD.
    maybe_delete(&client, NAMESPACE, CFG_DELETE).await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Cache must retain last-known-good.
    assert_eq!(
        cache.load().schema_version,
        5,
        "cache must retain schema_version=5 after deletion"
    );

    watcher.abort();
}
