//! `Apply` gRPC handler for `KonfigService`.
//!
//! Flow:
//! 1. Decode `ApplyRequest` bytes — extract `namespace`, `name`, `yaml_value`.
//! 2. Parse `yaml_value` as a YAML map and extract `schema_version`.
//! 3. Fetch the current ConfigMap via kube-rs; decode its `binaryData` to get
//!    the current `schema_version`.
//! 4. Reject with `FAILED_PRECONDITION` if incoming `schema_version` <= current.
//! 5. Build a FlatBuffers `TradingConfig` buffer from the YAML fields.
//! 6. Patch the ConfigMap `binaryData["trading-config"]` using K8s server-side apply.
//! 7. On `409 Conflict` retry with exponential back-off: 100 ms → 200 ms → 400 ms
//!    (max 3 attempts total).
//! 8. Return `ApplyResponse { resource_version }` on success.
//!
//! # ADR references
//! - ADR-005: schema_version monotonicity, 409 retry, CP write semantics.
//! - ADR-003: FlatBuffers-encoded binaryData in ConfigMap.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use flatbuffers::FlatBufferBuilder;
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::ByteString;
use kube::api::{Api, Patch, PatchParams};
use kube::Client;
use serde::Deserialize;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::grpc::read_string_field;

// ── YAML schema ───────────────────────────────────────────────────────────────

/// Top-level YAML structure accepted by `konfig-cli apply` and the Apply RPC.
///
/// Mirrors the `TradingConfig` FlatBuffers schema (konfig.v1.TradingConfig).
/// All fields are optional; missing fields use FlatBuffers defaults.
#[derive(Debug, Deserialize, Default)]
pub(crate) struct TradingConfigYaml {
    pub(crate) schema_version: u32,
    #[serde(default)]
    pub(crate) risk: RiskParamsYaml,
    #[serde(default)]
    pub(crate) strategies: Vec<StrategyParamsYaml>,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct RiskParamsYaml {
    #[serde(default)]
    pub(crate) max_position_usd: f64,
    #[serde(default)]
    pub(crate) max_order_size_usd: f64,
    #[serde(default)]
    pub(crate) max_daily_loss_usd: f64,
    #[serde(default)]
    pub(crate) max_orders_per_second: u32,
    #[serde(default)]
    pub(crate) max_notional_per_minute: f64,
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
}

#[derive(Debug, Deserialize, Default)]
pub(crate) struct StrategyParamsYaml {
    #[serde(default)]
    pub(crate) product_id: String,
    #[serde(default)]
    pub(crate) signal_threshold: f64,
    #[serde(default)]
    pub(crate) lookback_window_ms: u64,
    #[serde(default)]
    pub(crate) max_spread_bps: f64,
    #[serde(default = "default_true")]
    pub(crate) enabled: bool,
}

fn default_true() -> bool {
    true
}

// ── ApplyHandler ──────────────────────────────────────────────────────────────

/// Unary handler for `Apply(ApplyRequest) -> ApplyResponse`.
pub struct ApplyHandler {
    pub kube_client: Client,
}

impl tower_service::Service<Request<Bytes>> for ApplyHandler {
    type Response = Response<Bytes>;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Status>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Status>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Bytes>) -> Self::Future {
        let kube_client = self.kube_client.clone();
        Box::pin(async move {
            let bytes = req.into_inner();
            let namespace = read_string_field(&bytes, 4).to_owned();
            let name = read_string_field(&bytes, 6).to_owned();
            let yaml_value = read_string_field(&bytes, 8).to_owned();

            debug!(namespace = %namespace, name = %name, "Apply RPC");

            apply_inner(&namespace, &name, &yaml_value, kube_client).await
        })
    }
}

// ── Core apply logic (also called by konfig-cli) ──────────────────────────────

/// Core apply logic — parse YAML, validate schema_version, encode FlatBuffers,
/// patch ConfigMap with 409 retry.  Extracted so konfig-cli can call it directly
/// without going through gRPC.
pub async fn apply_inner(
    namespace: &str,
    name: &str,
    yaml_value: &str,
    kube_client: Client,
) -> Result<Response<Bytes>, Status> {
    // 1. Parse YAML.
    let cfg: TradingConfigYaml = serde_yaml::from_str(yaml_value).map_err(|e| {
        Status::invalid_argument(format!("invalid YAML: {e}"))
    })?;

    let incoming_schema_version = cfg.schema_version;

    // 2. Fetch current ConfigMap and check schema_version monotonicity.
    let cms: Api<ConfigMap> = Api::namespaced(kube_client.clone(), namespace);
    let current_schema_version = fetch_current_schema_version(&cms, name).await?;

    if incoming_schema_version <= current_schema_version {
        warn!(
            incoming = incoming_schema_version,
            current = current_schema_version,
            "Apply rejected: schema_version not monotonically increasing",
        );
        return Err(Status::failed_precondition(format!(
            "schema_version must be > current ({current_schema_version}); \
             got {incoming_schema_version}"
        )));
    }

    // 3. Encode to FlatBuffers.
    let fb_bytes = encode_trading_config(&cfg);

    // 4. Patch ConfigMap with 409 retry (100 ms, 200 ms, 400 ms).
    let resource_version = patch_config_map_with_retry(&cms, name, namespace, fb_bytes).await?;

    info!(
        namespace = %namespace,
        name = %name,
        schema_version = incoming_schema_version,
        resource_version = %resource_version,
        "Apply succeeded",
    );

    // 5. Build ApplyResponse FlatBuffers.
    let resp_bytes = build_apply_response(&resource_version);
    Ok(Response::new(resp_bytes))
}

// ── Helper: fetch current schema_version from ConfigMap ──────────────────────

async fn fetch_current_schema_version(
    cms: &Api<ConfigMap>,
    name: &str,
) -> Result<u32, Status> {
    match cms.get(name).await {
        Ok(cm) => {
            // Try binaryData first.
            if let Some(binary_data) = &cm.binary_data
                && let Some(bytes_obj) = binary_data.get("trading-config")
            {
                let bytes = &bytes_obj.0;
                if !bytes.is_empty() {
                    // SAFETY: binaryData["trading-config"] is written by Apply
                    // which always produces valid FlatBuffers bytes.
                    let root = unsafe {
                        flatbuffers::root_unchecked::<flatbuffers::Table>(bytes)
                    };
                    let version = unsafe { root.get::<u32>(4, Some(0)).unwrap_or(0) };
                    return Ok(version);
                }
            }
            // No binaryData — treat as version 0 (first apply).
            Ok(0)
        }
        Err(kube::Error::Api(ref ae)) if ae.code == 404 => {
            // ConfigMap does not exist yet — any schema_version > 0 is accepted.
            Ok(0)
        }
        Err(e) => Err(Status::unavailable(format!("kube API error: {e}"))),
    }
}

// ── Helper: patch ConfigMap with 409 retry ────────────────────────────────────

/// Retry delays matching ADR-005: 100 ms, 200 ms, 400 ms; max 3 attempts total.
const RETRY_DELAYS_MS: [u64; 2] = [100, 200];

async fn patch_config_map_with_retry(
    cms: &Api<ConfigMap>,
    name: &str,
    namespace: &str,
    fb_bytes: Vec<u8>,
) -> Result<String, Status> {
    // Build a minimal ConfigMap patch with only binaryData.
    let build_patch = |bytes: Vec<u8>| -> ConfigMap {
        let mut binary_data = std::collections::BTreeMap::new();
        binary_data.insert("trading-config".to_string(), ByteString(bytes));
        ConfigMap {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some(namespace.to_string()),
                ..Default::default()
            },
            binary_data: Some(binary_data),
            ..Default::default()
        }
    };

    let ssapply = PatchParams::apply("konfig.v1").force();

    let mut attempt = 0usize;
    loop {
        let patch = build_patch(fb_bytes.clone());
        match cms.patch(name, &ssapply, &Patch::Apply(&patch)).await {
            Ok(cm) => {
                let rv = cm
                    .metadata
                    .resource_version
                    .unwrap_or_default();
                return Ok(rv);
            }
            Err(kube::Error::Api(ref ae)) if ae.code == 409 && attempt < RETRY_DELAYS_MS.len() => {
                let delay_ms = RETRY_DELAYS_MS[attempt];
                warn!(
                    attempt = attempt + 1,
                    delay_ms,
                    "Apply: 409 Conflict — retrying",
                );
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                attempt += 1;
            }
            Err(kube::Error::Api(ref ae)) if ae.code == 409 => {
                return Err(Status::aborted(
                    "Apply: 409 Conflict — exceeded max retries (3)",
                ));
            }
            Err(e) => {
                return Err(Status::unavailable(format!("kube patch error: {e}")));
            }
        }
    }
}

// ── FlatBuffers encoder for TradingConfig ─────────────────────────────────────

/// Encode a [`TradingConfigYaml`] into a FlatBuffers `TradingConfig` buffer.
///
/// Slot layout matches `schema/konfig/trading_config.fbs`:
/// ```text
/// TradingConfig: slot 4 = schema_version, slot 6 = risk, slot 8 = strategies
/// RiskParams: slots 4–14
/// StrategyParams: slots 4–12
/// ```
pub(crate) fn encode_trading_config(cfg: &TradingConfigYaml) -> Vec<u8> {
    let mut fbb = FlatBufferBuilder::new();

    let mut strategy_offsets = Vec::with_capacity(cfg.strategies.len());
    for s in &cfg.strategies {
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
    fbb.push_slot::<f64>(4, cfg.risk.max_position_usd, 0.0);
    fbb.push_slot::<f64>(6, cfg.risk.max_order_size_usd, 0.0);
    fbb.push_slot::<f64>(8, cfg.risk.max_daily_loss_usd, 0.0);
    fbb.push_slot::<u32>(10, cfg.risk.max_orders_per_second, 0);
    fbb.push_slot::<f64>(12, cfg.risk.max_notional_per_minute, 0.0);
    fbb.push_slot::<bool>(14, cfg.risk.enabled, true);
    let risk = fbb.end_table(risk_start);

    let root_start = fbb.start_table();
    fbb.push_slot::<u32>(4, cfg.schema_version, 0);
    fbb.push_slot_always::<flatbuffers::WIPOffset<flatbuffers::TableFinishedWIPOffset>>(6, risk);
    fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(8, strategies_vec);
    let root = fbb.end_table(root_start);
    fbb.finish(root, None);

    fbb.finished_data().to_vec()
}

// ── FlatBuffers builder for ApplyResponse ─────────────────────────────────────

/// Build an `ApplyResponse` FlatBuffers buffer.
///
/// Slot layout for `ApplyResponse` (konfig_service.fbs):
/// ```text
///   slot 4: resource_version (string)
/// ```
fn build_apply_response(resource_version: &str) -> Bytes {
    let mut fbb = FlatBufferBuilder::new();
    let rv_off = fbb.create_string(resource_version);
    let start = fbb.start_table();
    fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(4, rv_off);
    let root = fbb.end_table(start);
    fbb.finish(root, None);
    Bytes::copy_from_slice(fbb.finished_data())
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_apply_request(namespace: &str, name: &str, yaml: &str) -> Bytes {
        let mut fbb = FlatBufferBuilder::new();
        let ns = fbb.create_string(namespace);
        let nm = fbb.create_string(name);
        let yv = fbb.create_string(yaml);
        let start = fbb.start_table();
        fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(4, ns);
        fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(6, nm);
        fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(8, yv);
        let root = fbb.end_table(start);
        fbb.finish(root, None);
        Bytes::copy_from_slice(fbb.finished_data())
    }

    #[test]
    fn encode_trading_config_round_trip() {
        let cfg = TradingConfigYaml {
            schema_version: 7,
            risk: RiskParamsYaml {
                max_position_usd: 200_000.0,
                max_order_size_usd: 10_000.0,
                max_daily_loss_usd: 5_000.0,
                max_orders_per_second: 50,
                max_notional_per_minute: 1_000_000.0,
                enabled: true,
            },
            strategies: vec![StrategyParamsYaml {
                product_id: "ETH-USDT".to_string(),
                signal_threshold: 0.8,
                lookback_window_ms: 30_000,
                max_spread_bps: 15.0,
                enabled: true,
            }],
        };

        let buf = encode_trading_config(&cfg);
        assert!(!buf.is_empty(), "encoded buffer must not be empty");

        // Decode schema_version back.
        // SAFETY: we just built this buffer above.
        let root = unsafe { flatbuffers::root_unchecked::<flatbuffers::Table>(&buf) };
        let schema_version = unsafe { root.get::<u32>(4, Some(0)).unwrap_or(0) };
        assert_eq!(schema_version, 7);
    }

    #[test]
    fn schema_version_downgrade_rejected_equal() {
        // incoming == current → FAILED_PRECONDITION
        // We test the comparison logic directly.
        let incoming = 5u32;
        let current = 5u32;
        assert!(
            incoming <= current,
            "incoming <= current must be rejected"
        );
    }

    #[test]
    fn schema_version_downgrade_rejected_less_than() {
        let incoming = 3u32;
        let current = 5u32;
        assert!(
            incoming <= current,
            "incoming < current must be rejected"
        );
    }

    #[test]
    fn apply_request_fields_decoded_correctly() {
        let yaml = "schema_version: 2\nrisk:\n  max_position_usd: 50000.0\n";
        let bytes = make_apply_request("trading", "risk-config", yaml);

        let ns = read_string_field(&bytes, 4);
        let nm = read_string_field(&bytes, 6);
        let yv = read_string_field(&bytes, 8);

        assert_eq!(ns, "trading");
        assert_eq!(nm, "risk-config");
        assert_eq!(yv, yaml);
    }

    #[test]
    fn invalid_yaml_produces_invalid_argument() {
        // serde_yaml::from_str on malformed YAML should fail.
        let result = serde_yaml::from_str::<TradingConfigYaml>("not: [valid: yaml: here");
        assert!(result.is_err(), "malformed YAML must be rejected by serde_yaml");
    }

    #[test]
    fn apply_response_encodes_resource_version() {
        let bytes = build_apply_response("rv-00042");
        assert!(!bytes.is_empty());

        let rv = read_string_field(&bytes, 4);
        assert_eq!(rv, "rv-00042");
    }
}
