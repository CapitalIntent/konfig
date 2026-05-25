//! Integration tests for the K8s ConfigMap watcher.
//!
//! These tests spin up an isolated K3s cluster via Testcontainers — no external
//! kind cluster or pre-existing K8s context is required.  Docker must be running
//! on the test host (CI or local).
//!
//! Run with:
//! ```sh
//! cargo test --test integration_watcher --features integration -p konfig
//! ```
//!
//! The `integration` feature gate prevents these tests from running in the
//! default `cargo test` invocation (which has no Docker dependency).

#![cfg(feature = "integration")]

use std::collections::BTreeMap;
use std::env;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use kube::api::{Api, DeleteParams, PostParams};
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::Config;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::k3s::{K3s, KUBE_SECURE_PORT};
use tokio::time::timeout;

use konfig::cache::ConfigCache;
use konfig::types::TradingConfigSnapshot;
use konfig::watcher::Watcher;

// ── Test constants ────────────────────────────────────────────────────────────

const NAMESPACE: &str = "default";

/// Unique ConfigMap name per test to avoid cross-test interference when tests
/// run concurrently.
const CM_APPLY: &str = "trading-config-integ-apply";
const CM_DELETE: &str = "trading-config-integ-delete";

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Start a K3s container and build a `kube::Client` connected to it.
///
/// Returns the container handle alongside the client.  The caller **must** keep
/// the handle in scope for the duration of the test — dropping it stops the
/// container and invalidates the client.
///
/// The host temp-dir is mounted so K3s can write `k3s.yaml` for us to read
/// back without `exec`-ing into the container.
async fn k3s_client() -> (ContainerAsync<K3s>, kube::Client) {
    // Each test gets its own temp directory so parallel runs do not race on
    // the same `k3s.yaml` file.
    let conf_dir = env::temp_dir().join(format!("k3s-konfig-{}", uuid_simple()));

    std::fs::create_dir_all(&conf_dir).expect("failed to create k3s conf dir");

    let container = K3s::default()
        .with_conf_mount(&conf_dir)
        .with_privileged(true)
        .with_userns_mode("host")
        .start()
        .await
        .expect("failed to start K3s container — is Docker running?");

    // Install the default rustls crypto provider once per process.  The call
    // is a no-op if a provider is already installed.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let kubeconfig_yaml = container
        .image()
        .read_kube_config()
        .expect("failed to read kubeconfig from K3s container");

    // K3s writes 127.0.0.1:6443 as the server address inside the container;
    // we need to rewrite it to the mapped host port so the client can reach it
    // from outside Docker.
    let mut kube_config =
        Kubeconfig::from_yaml(&kubeconfig_yaml).expect("failed to parse kubeconfig YAML");

    let host_port = container
        .get_host_port_ipv4(KUBE_SECURE_PORT)
        .await
        .expect("failed to get K3s host port");

    kube_config.clusters.iter_mut().for_each(|named| {
        if let Some(cluster) = named.cluster.as_mut() {
            if let Some(server) = cluster.server.as_mut() {
                *server = format!("https://127.0.0.1:{host_port}");
            }
        }
    });

    let config =
        Config::from_custom_kubeconfig(kube_config, &KubeConfigOptions::default())
            .await
            .expect("failed to build kube Config from kubeconfig");

    let client = kube::Client::try_from(config).expect("failed to create kube Client");

    (container, client)
}

/// Very small helper that produces a short random-ish hex string suitable for
/// temp-dir uniqueness (avoids pulling in uuid as a test dep).
fn uuid_simple() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    format!("{:08x}", nanos)
}

/// Build a ConfigMap `data` map with the supplied schema_version and strategy.enabled.
///
/// Keys match the `parse_data_map` convention in `watcher.rs`:
/// - `schema_version` (u32)
/// - `risk.*`
/// - `strategy.product_id` / `strategy.signal_threshold` / etc. (single-strategy flat keys)
fn make_data_map(schema_version: u32, enabled: bool) -> BTreeMap<String, String> {
    let mut data = BTreeMap::new();
    data.insert("schema_version".into(), schema_version.to_string());
    data.insert("risk.max_position_usd".into(), "100000.0".into());
    data.insert("risk.max_order_size_usd".into(), "5000.0".into());
    data.insert("risk.max_daily_loss_usd".into(), "2000.0".into());
    data.insert("risk.max_orders_per_second".into(), "100".into());
    data.insert("risk.max_notional_per_minute".into(), "500000.0".into());
    data.insert("risk.enabled".into(), "true".into());
    data.insert("strategy.product_id".into(), "BTC-USDT".into());
    data.insert("strategy.enabled".into(), enabled.to_string());
    data.insert("strategy.signal_threshold".into(), "0.75".into());
    data.insert("strategy.lookback_window_ms".into(), "60000".into());
    data.insert("strategy.max_spread_bps".into(), "20.0".into());
    data
}

/// Create or replace a ConfigMap with the given data.
///
/// Strategy: try `create`; on 409 Conflict delete + recreate (simplest
/// test-only approach — no server-side-apply dependency).
async fn upsert_config_map(
    cms: &Api<ConfigMap>,
    name: &str,
    schema_version: u32,
    enabled: bool,
) -> Result<(), kube::Error> {
    let cm = ConfigMap {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            namespace: Some(NAMESPACE.to_string()),
            ..Default::default()
        },
        data: Some(make_data_map(schema_version, enabled)),
        ..Default::default()
    };

    match cms.create(&PostParams::default(), &cm).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(ref ae)) if ae.code == 409 => {
            // Already exists — delete then recreate.
            cms.delete(name, &DeleteParams::default()).await?;
            // Small pause to let the API server process the deletion before we recreate.
            tokio::time::sleep(Duration::from_millis(300)).await;
            cms.create(&PostParams::default(), &cm).await?;
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Delete a ConfigMap if it exists; ignore Not Found.
async fn maybe_delete(cms: &Api<ConfigMap>, name: &str) {
    let _ = cms.delete(name, &DeleteParams::default()).await;
}

/// Poll `predicate` every `interval` until it returns `true` or `deadline` passes.
async fn poll_until<F>(deadline: Duration, interval: Duration, mut predicate: F)
where
    F: FnMut() -> bool,
{
    let start = tokio::time::Instant::now();
    loop {
        if predicate() {
            return;
        }
        if start.elapsed() >= deadline {
            panic!("poll_until: condition not satisfied within {deadline:?}");
        }
        tokio::time::sleep(interval).await;
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Watcher picks up an Applied ConfigMap event and updates the cache.
///
/// Flow:
/// 1. Spin up a K3s container via Testcontainers.
/// 2. Spawn `run_watcher` against `CM_APPLY` in background.
/// 3. Create ConfigMap schema_version=1 with `strategy.enabled=true`.
/// 4. Poll up to 15 s for `cache.schema_version == 1`.
/// 5. Assert risk + strategy fields are correct.
/// 6. Apply ConfigMap schema_version=2 with `strategy.enabled=false`.
/// 7. Poll up to 15 s for `cache.schema_version == 2`.
/// 8. Assert version and flag are updated.
#[tokio::test]
async fn watcher_applies_config_map_to_cache() {
    let (_container, client) = k3s_client().await;

    let cms: Api<ConfigMap> = Api::namespaced(client.clone(), NAMESPACE);

    // Clean slate.
    maybe_delete(&cms, CM_APPLY).await;

    // Start watcher in a background task.
    let cache = Arc::new(ConfigCache::new(TradingConfigSnapshot::default()));
    let watcher_cache = Arc::clone(&cache);
    let watcher_client = client.clone();
    let watcher = tokio::spawn(async move {
        Watcher::new(watcher_client)
            .run(watcher_cache, NAMESPACE.to_string(), CM_APPLY.to_string())
            .await
            .expect("watcher exited with error");
    });

    // ── Apply schema_version=1, strategy.enabled=true ─────────────────────────
    upsert_config_map(&cms, CM_APPLY, 1, true)
        .await
        .expect("failed to create ConfigMap schema_version=1");

    let cache_ref = Arc::clone(&cache);
    timeout(Duration::from_secs(15), async move {
        poll_until(Duration::from_secs(15), Duration::from_millis(250), || {
            cache_ref.load().schema_version == 1
        })
        .await;
    })
    .await
    .expect("timed out waiting for cache schema_version=1");

    {
        let snap = cache.load();
        assert_eq!(snap.schema_version, 1, "schema_version must be 1");
        assert_eq!(snap.risk.max_position_usd, 100_000.0);
        assert_eq!(snap.risk.max_order_size_usd, 5_000.0);
        assert_eq!(snap.risk.max_daily_loss_usd, 2_000.0);
        assert_eq!(snap.risk.max_orders_per_second, 100);
        assert!(snap.risk.enabled, "risk.enabled must be true");
        assert_eq!(snap.strategies.len(), 1, "must have one strategy");
        assert_eq!(snap.strategies[0].product_id, "BTC-USDT");
        assert!(snap.strategies[0].enabled, "strategy.enabled must be true");
        assert_eq!(snap.strategies[0].signal_threshold, 0.75);
        assert_eq!(snap.strategies[0].max_spread_bps, 20.0);
        assert_eq!(snap.strategies[0].lookback_window_ms, 60_000);
    }

    // ── Apply schema_version=2, strategy.enabled=false ────────────────────────
    upsert_config_map(&cms, CM_APPLY, 2, false)
        .await
        .expect("failed to update ConfigMap to schema_version=2");

    let cache_ref2 = Arc::clone(&cache);
    timeout(Duration::from_secs(15), async move {
        poll_until(Duration::from_secs(15), Duration::from_millis(250), || {
            cache_ref2.load().schema_version == 2
        })
        .await;
    })
    .await
    .expect("timed out waiting for cache schema_version=2");

    {
        let snap = cache.load();
        assert_eq!(snap.schema_version, 2, "schema_version must be 2 after update");
        assert!(
            !snap.strategies[0].enabled,
            "strategy.enabled must be false at schema_version=2"
        );
    }

    // ── Cleanup ───────────────────────────────────────────────────────────────
    maybe_delete(&cms, CM_APPLY).await;
    watcher.abort();
    // _container is dropped here, stopping the K3s container.
}

/// Watcher retains last-known-good config after the ConfigMap is deleted.
///
/// Flow:
/// 1. Spin up a K3s container via Testcontainers.
/// 2. Spawn `run_watcher` against `CM_DELETE`.
/// 3. Create ConfigMap schema_version=5.
/// 4. Poll until `cache.schema_version == 5`.
/// 5. Delete the ConfigMap.
/// 6. Wait 1 s for the Deleted event to be processed.
/// 7. Assert cache still holds schema_version=5 (last-known-good retained on delete).
#[tokio::test]
async fn watcher_retains_cache_on_config_map_delete() {
    let (_container, client) = k3s_client().await;

    let cms: Api<ConfigMap> = Api::namespaced(client.clone(), NAMESPACE);

    maybe_delete(&cms, CM_DELETE).await;

    let cache = Arc::new(ConfigCache::new(TradingConfigSnapshot::default()));
    let watcher_cache = Arc::clone(&cache);
    let watcher_client = client.clone();
    let watcher = tokio::spawn(async move {
        Watcher::new(watcher_client)
            .run(watcher_cache, NAMESPACE.to_string(), CM_DELETE.to_string())
            .await
            .expect("watcher exited with error");
    });

    // Seed the cache with schema_version=5.
    upsert_config_map(&cms, CM_DELETE, 5, true)
        .await
        .expect("failed to create ConfigMap schema_version=5");

    let cache_ref = Arc::clone(&cache);
    timeout(Duration::from_secs(15), async move {
        poll_until(Duration::from_secs(15), Duration::from_millis(250), || {
            cache_ref.load().schema_version == 5
        })
        .await;
    })
    .await
    .expect("timed out waiting for cache to be seeded at schema_version=5");

    // Delete the ConfigMap and let the Deleted event be processed.
    cms.delete(CM_DELETE, &DeleteParams::default())
        .await
        .expect("failed to delete ConfigMap");

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Cache must retain last-known-good (watcher logs a warning but does not clear the cache).
    assert_eq!(
        cache.load().schema_version,
        5,
        "cache must retain schema_version=5 after ConfigMap deletion (last-known-good)"
    );

    watcher.abort();
    // _container is dropped here, stopping the K3s container.
}
