//! K8s ConfigMap watcher that drives [`ConfigCache`] updates.
//!
//! Uses kube-rs `watcher` to stream ConfigMap events. On each `Apply`/`InitApply`
//! event the watcher parses the ConfigMap's `binaryData` (FlatBuffers) or `data`
//! (key=value fallback) into a [`TradingConfigSnapshot`] and calls
//! [`ConfigCache::update`].
//!
//! # CP semantics
//!
//! The watcher uses `resourceVersion`-based resume (kube-rs default). On pod
//! restart the watch stream resumes from the last stored `resourceVersion`,
//! ensuring no events are missed. This is the Raft-backed watch guarantee
//! from K8s etcd (ADR-005).

use std::sync::Arc;

use futures_util::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::ConfigMap;
use kube::runtime::watcher::{self as kube_watcher, watcher as kube_watch_stream, Event};
use kube::{Api, Client};
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::cache::ConfigCache;
use crate::types::{RiskParamsSnapshot, StrategyParamsSnapshot, TradingConfigSnapshot};

// ── Error type ────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum WatcherError {
    #[error("kube client error: {0}")]
    Kube(#[from] kube::Error),
    #[error("kube watcher error: {0}")]
    Watcher(#[from] kube_watcher::Error),
}

// ── Watcher entry point ───────────────────────────────────────────────────────

/// Builder / runner for the K8s ConfigMap watcher.
///
/// Construct with [`Watcher::new`], then call [`Watcher::run`] to start
/// streaming events.  Run inside `tokio::spawn` — the method blocks until
/// the watch stream ends or an error occurs.
pub struct Watcher {
    client: Client,
}

impl Watcher {
    /// Create a new `Watcher` bound to the given authenticated kube client.
    pub fn new(client: Client) -> Self {
        Watcher { client }
    }

    /// Run the ConfigMap watcher until the stream ends or an error occurs.
    ///
    /// Blocks the calling task — run this inside `tokio::spawn`.
    ///
    /// # Arguments
    /// * `cache` — shared cache to update on each `Apply`/`InitApply` event
    /// * `namespace` — K8s namespace to watch
    /// * `config_map_name` — name of the ConfigMap to watch
    pub async fn run(
        self,
        cache: Arc<ConfigCache>,
        namespace: String,
        config_map_name: String,
    ) -> Result<(), WatcherError> {
        let cms: Api<ConfigMap> = Api::namespaced(self.client, &namespace);

        let wc = kube_watcher::Config::default()
            .fields(&format!("metadata.name={config_map_name}"));

        let mut w = kube_watch_stream(cms, wc).boxed();

        info!(
            namespace = %namespace,
            config_map = %config_map_name,
            "ConfigMap watcher started",
        );

        while let Some(event) = w.try_next().await? {
            handle_watcher_event(event, &cache);
        }

        Ok(())
    }
}

/// Dispatch a single watcher event: apply/delete/init handling.
///
/// Extracted to keep `Watcher::run` CC low and to allow unit testing
/// of event-handling logic without a live K8s cluster.
pub(crate) fn handle_watcher_event(event: Event<ConfigMap>, cache: &Arc<ConfigCache>) {
    match event {
        Event::Apply(cm) | Event::InitApply(cm) => {
            let name = cm.metadata.name.as_deref().unwrap_or("<unknown>");
            if let Some(snapshot) = parse_config_map(&cm) {
                info!(
                    config_map = %name,
                    schema_version = snapshot.schema_version,
                    "ConfigMap applied — updating cache",
                );
                cache.update(snapshot);
            } else {
                warn!(
                    config_map = %name,
                    "ConfigMap applied but could not parse trading config — cache unchanged",
                );
            }
        }
        Event::Delete(cm) => {
            let name = cm.metadata.name.as_deref().unwrap_or("<unknown>");
            warn!(config_map = %name, "ConfigMap deleted — cache retains last-known-good");
        }
        Event::Init => {
            debug!("Watch stream starting initial list phase");
        }
        Event::InitDone => {
            debug!("Watch stream initial list complete");
        }
    }
}

// ── ConfigMap parser ──────────────────────────────────────────────────────────

/// Parse a [`ConfigMap`] into a [`TradingConfigSnapshot`].
///
/// Attempts two strategies in order:
///
/// 1. **`binaryData["trading-config"]`** — FlatBuffers-encoded bytes (production
///    path, as per ADR-003).
/// 2. **`data` key-value fallback** — plain text keys for development/bootstrap.
///
/// The `resource_version` is extracted from `cm.metadata.resource_version`
/// (empty string if absent — safe for unit tests that construct ConfigMaps
/// in-memory without metadata).
///
/// Returns `None` when neither strategy yields a valid config.
pub fn parse_config_map(cm: &ConfigMap) -> Option<TradingConfigSnapshot> {
    let resource_version = cm
        .metadata
        .resource_version
        .clone()
        .unwrap_or_default();

    // Strategy 1: binaryData["trading-config"] = FlatBuffers bytes.
    if let Some(binary_data) = &cm.binary_data
        && let Some(bytes_obj) = binary_data.get("trading-config")
    {
        let bytes = &bytes_obj.0;
        if !bytes.is_empty() {
            // SAFETY: binaryData is expected to hold a valid FlatBuffers-encoded
            // TradingConfig. Corrupt bytes produce a warning, not a panic.
            match unsafe {
                crate::types::TradingConfigSnapshot::from_flatbuffers(
                    bytes,
                    resource_version.clone(),
                )
            } {
                Ok(snap) => return Some(snap),
                Err(e) => {
                    warn!("Failed to decode binaryData[trading-config]: {e}");
                }
            }
        }
    }

    // Strategy 2: text data keys (development / bootstrap).
    if let Some(data) = &cm.data {
        return parse_data_map(data, resource_version);
    }

    None
}

/// Parse a ConfigMap `data` map (string key/value pairs) into a snapshot.
///
/// Expected keys (all optional, missing keys use defaults):
///
/// ```text
/// schema_version:                        uint32
/// risk.max_position_usd:                 f64
/// risk.max_order_size_usd:               f64
/// risk.max_daily_loss_usd:               f64
/// risk.max_orders_per_second:            u32
/// risk.max_notional_per_minute:          f64
/// risk.enabled:                          bool ("true"/"false")
/// strategy.product_id:                   string
/// strategy.signal_threshold:             f64
/// strategy.lookback_window_ms:           u64
/// strategy.max_spread_bps:               f64
/// strategy.enabled:                      bool ("true"/"false")
/// ```
fn parse_data_map(
    data: &std::collections::BTreeMap<String, String>,
    resource_version: String,
) -> Option<TradingConfigSnapshot> {
    let parse_f64 = |key: &str| -> f64 {
        data.get(key)
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0)
    };
    let parse_u64 = |key: &str| -> u64 {
        data.get(key)
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
    };
    let parse_u32 = |key: &str| -> u32 {
        data.get(key)
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(0)
    };
    let parse_bool_default_true = |key: &str| -> bool {
        data.get(key)
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(true)
    };

    let risk = RiskParamsSnapshot {
        max_position_usd: parse_f64("risk.max_position_usd"),
        max_order_size_usd: parse_f64("risk.max_order_size_usd"),
        max_daily_loss_usd: parse_f64("risk.max_daily_loss_usd"),
        max_orders_per_second: parse_u32("risk.max_orders_per_second"),
        max_notional_per_minute: parse_f64("risk.max_notional_per_minute"),
        enabled: parse_bool_default_true("risk.enabled"),
    };

    // Build a single strategy from flat keys if any strategy keys are present.
    let strategy_product_id = data
        .get("strategy.product_id")
        .cloned()
        .unwrap_or_default();
    let strategies = if !strategy_product_id.is_empty()
        || data.contains_key("strategy.signal_threshold")
    {
        vec![StrategyParamsSnapshot {
            product_id: strategy_product_id,
            signal_threshold: parse_f64("strategy.signal_threshold"),
            lookback_window_ms: parse_u64("strategy.lookback_window_ms"),
            max_spread_bps: parse_f64("strategy.max_spread_bps"),
            enabled: parse_bool_default_true("strategy.enabled"),
        }]
    } else {
        Vec::new()
    };

    Some(TradingConfigSnapshot {
        schema_version: parse_u32("schema_version"),
        risk,
        strategies,
        resource_version,
        loaded_at: std::time::Instant::now(),
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::ByteString;
    use std::collections::BTreeMap;

    fn make_cm_with_data(data: BTreeMap<String, String>) -> ConfigMap {
        let mut cm = ConfigMap::default();
        cm.data = Some(data);
        cm
    }

    fn make_cm_with_binary(bytes: Vec<u8>) -> ConfigMap {
        let mut cm = ConfigMap::default();
        let mut binary = BTreeMap::new();
        binary.insert("trading-config".to_string(), ByteString(bytes));
        cm.binary_data = Some(binary);
        cm
    }

    #[test]
    fn parse_data_map_risk_keys() {
        let mut data = BTreeMap::new();
        data.insert("schema_version".into(), "3".into());
        data.insert("risk.max_position_usd".into(), "200000.0".into());
        data.insert("risk.max_order_size_usd".into(), "10000.0".into());
        data.insert("risk.max_daily_loss_usd".into(), "5000.0".into());
        data.insert("risk.max_orders_per_second".into(), "50".into());
        data.insert("risk.max_notional_per_minute".into(), "300000.0".into());
        data.insert("risk.enabled".into(), "true".into());

        let cm = make_cm_with_data(data);
        let snap = parse_config_map(&cm).expect("must parse");

        assert_eq!(snap.schema_version, 3);
        assert_eq!(snap.risk.max_position_usd, 200_000.0);
        assert_eq!(snap.risk.max_order_size_usd, 10_000.0);
        assert_eq!(snap.risk.max_daily_loss_usd, 5_000.0);
        assert_eq!(snap.risk.max_orders_per_second, 50);
        assert_eq!(snap.risk.max_notional_per_minute, 300_000.0);
        assert!(snap.risk.enabled);
    }

    #[test]
    fn parse_data_map_with_strategy() {
        let mut data = BTreeMap::new();
        data.insert("strategy.product_id".into(), "ETH-USDT".into());
        data.insert("strategy.signal_threshold".into(), "0.8".into());
        data.insert("strategy.lookback_window_ms".into(), "30000".into());
        data.insert("strategy.max_spread_bps".into(), "15.0".into());
        data.insert("strategy.enabled".into(), "true".into());

        let cm = make_cm_with_data(data);
        let snap = parse_config_map(&cm).unwrap();

        assert_eq!(snap.strategies.len(), 1);
        assert_eq!(snap.strategies[0].product_id, "ETH-USDT");
        assert_eq!(snap.strategies[0].signal_threshold, 0.8);
        assert_eq!(snap.strategies[0].lookback_window_ms, 30_000);
        assert_eq!(snap.strategies[0].max_spread_bps, 15.0);
        assert!(snap.strategies[0].enabled);
    }

    #[test]
    fn parse_data_map_missing_keys_use_defaults() {
        let cm = make_cm_with_data(BTreeMap::new());
        let snap = parse_config_map(&cm).expect("must parse empty data map");
        assert_eq!(snap.schema_version, 0);
        assert_eq!(snap.risk, RiskParamsSnapshot::default());
        assert!(snap.strategies.is_empty());
    }

    #[test]
    fn parse_data_map_bool_case_insensitive() {
        let mut data = BTreeMap::new();
        data.insert("strategy.product_id".into(), "BTC-USDT".into());
        data.insert("strategy.enabled".into(), "TRUE".into());
        let cm = make_cm_with_data(data);
        let snap = parse_config_map(&cm).unwrap();
        assert!(snap.strategies[0].enabled);
    }

    #[test]
    fn parse_binary_data_flatbuffers_round_trip() {
        let buf = crate::types::build_trading_config(
            9,
            500_000.0,
            20_000.0,
            8_000.0,
            200,
            1_000_000.0,
            true,
            "BTC-USDT",
            0.6,
            30_000,
            30.0,
            true,
        );
        let cm = make_cm_with_binary(buf.to_vec());
        let snap = parse_config_map(&cm).expect("must decode binaryData");

        assert_eq!(snap.schema_version, 9);
        assert_eq!(snap.risk.max_position_usd, 500_000.0);
        assert!(snap.risk.enabled);
        assert_eq!(snap.strategies.len(), 1);
        assert_eq!(snap.strategies[0].max_spread_bps, 30.0);
        // resource_version is empty — no metadata on the in-memory ConfigMap.
        assert_eq!(snap.resource_version, "");
    }

    #[test]
    fn binary_data_preferred_over_text_data() {
        let buf = crate::types::build_trading_config(
            77, 1.0, 2.0, 3.0, 4, 5.0, false, "x", 0.1, 0, 0.5, true,
        );
        let mut cm = make_cm_with_binary(buf.to_vec());
        let mut text = BTreeMap::new();
        text.insert("schema_version".into(), "999".into());
        cm.data = Some(text);

        let snap = parse_config_map(&cm).unwrap();
        assert_eq!(snap.schema_version, 77); // binaryData wins
    }

    #[test]
    fn empty_configmap_returns_none() {
        let cm = ConfigMap::default();
        assert!(parse_config_map(&cm).is_none());
    }

    #[test]
    fn invalid_f64_falls_back_to_zero() {
        let mut data = BTreeMap::new();
        data.insert("risk.max_position_usd".into(), "not-a-number".into());
        let cm = make_cm_with_data(data);
        let snap = parse_config_map(&cm).unwrap();
        assert_eq!(snap.risk.max_position_usd, 0.0);
    }

    /// resource_version from ConfigMap metadata is propagated into the snapshot.
    #[test]
    fn resource_version_from_metadata_propagated() {
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
        let mut cm = make_cm_with_data(BTreeMap::new());
        cm.metadata = ObjectMeta {
            resource_version: Some("rv-xyz".to_string()),
            ..Default::default()
        };
        let snap = parse_config_map(&cm).unwrap();
        assert_eq!(snap.resource_version, "rv-xyz");
    }

    /// resource_version defaults to empty string for in-memory ConfigMaps without metadata.
    #[test]
    fn resource_version_empty_when_no_metadata() {
        let cm = make_cm_with_data(BTreeMap::new());
        let snap = parse_config_map(&cm).unwrap();
        assert_eq!(snap.resource_version, "");
    }

    // ── handle_watcher_event tests ────────────────────────────────────────────

    fn make_cache() -> Arc<crate::cache::ConfigCache> {
        Arc::new(crate::cache::ConfigCache::new(
            crate::types::TradingConfigSnapshot::default(),
        ))
    }

    #[test]
    fn apply_event_updates_cache() {
        let mut data = BTreeMap::new();
        data.insert("schema_version".into(), "42".into());
        data.insert("risk.max_position_usd".into(), "100000.0".into());
        let cm = make_cm_with_data(data);
        let cache = make_cache();
        handle_watcher_event(Event::Apply(cm), &cache);
        assert_eq!(cache.load().schema_version, 42);
    }

    #[test]
    fn init_apply_event_updates_cache() {
        let mut data = BTreeMap::new();
        data.insert("schema_version".into(), "7".into());
        let cm = make_cm_with_data(data);
        let cache = make_cache();
        handle_watcher_event(Event::InitApply(cm), &cache);
        assert_eq!(cache.load().schema_version, 7);
    }

    #[test]
    fn apply_event_unparseable_leaves_cache_unchanged() {
        // ConfigMap with no data and no binaryData — parse returns None.
        let cm = ConfigMap::default();
        let cache = make_cache();
        // Cache starts with schema_version == 0 (default).
        handle_watcher_event(Event::Apply(cm), &cache);
        // Cache must be unchanged — still the initial default.
        assert_eq!(cache.load().schema_version, 0);
    }

    #[test]
    fn delete_event_leaves_cache_unchanged() {
        let mut data = BTreeMap::new();
        data.insert("schema_version".into(), "5".into());
        let cm = make_cm_with_data(data);
        let cache = make_cache();
        // Seed cache with schema_version=5.
        handle_watcher_event(Event::Apply(cm.clone()), &cache);
        assert_eq!(cache.load().schema_version, 5);
        // Delete should not touch the cache.
        handle_watcher_event(Event::Delete(cm), &cache);
        assert_eq!(cache.load().schema_version, 5);
    }

    #[test]
    fn init_event_leaves_cache_unchanged() {
        let cache = make_cache();
        handle_watcher_event(Event::Init, &cache);
        assert_eq!(cache.load().schema_version, 0);
    }

    #[test]
    fn init_done_event_leaves_cache_unchanged() {
        let cache = make_cache();
        handle_watcher_event(Event::InitDone, &cache);
        assert_eq!(cache.load().schema_version, 0);
    }
}
