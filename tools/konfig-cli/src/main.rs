//! konfig-cli — operator CLI for Konfig V0.
//!
//! Reads and writes trading configs directly via kube-rs, bypassing the gRPC
//! service.  This means `get` works even when the Konfig server is down and
//! shows the source-of-truth ConfigMap state (ADR-003, Decision D3).
//!
//! # Commands
//!
//! ## `apply <namespace> <name> <yaml-file>`
//!
//! Parse the YAML file, validate `schema_version` monotonicity, encode as a
//! FlatBuffers `TradingConfig`, and patch the ConfigMap `binaryData["trading-config"]`
//! via K8s server-side apply.  Retries on 409 Conflict (3x, 100/200/400 ms).
//!
//! ## `get <namespace> <name>`
//!
//! Fetch the ConfigMap `binaryData["trading-config"]`, decode the FlatBuffers
//! bytes, and print the fields as YAML — mirroring the `apply` input format
//! for easy round-trip inspection.
//!
//! # Usage
//!
//! ```sh
//! konfig-cli apply trading risk-config ./risk-v3.yaml
//! konfig-cli get trading risk-config
//! ```

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::ByteString;
use kube::api::Api;
use kube::Client;
use tracing::info;

// ── CLI definition ────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "konfig-cli", about = "Konfig V0 operator CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Apply a YAML config file to a ConfigMap (FlatBuffers-encoded binaryData).
    Apply {
        /// K8s namespace (e.g. "trading")
        namespace: String,
        /// ConfigMap name (e.g. "risk-config")
        name: String,
        /// Path to the YAML config file
        yaml_file: PathBuf,
    },
    /// Get and print a ConfigMap's FlatBuffers config as YAML.
    Get {
        /// K8s namespace (e.g. "trading")
        namespace: String,
        /// ConfigMap name (e.g. "risk-config")
        name: String,
    },
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("konfig_cli=info".parse()?),
        )
        .init();

    let cli = Cli::parse();
    let client = Client::try_default().await?;

    match cli.command {
        Commands::Apply {
            namespace,
            name,
            yaml_file,
        } => {
            let yaml_value = std::fs::read_to_string(&yaml_file)
                .map_err(|e| format!("cannot read {}: {e}", yaml_file.display()))?;
            cmd_apply(&namespace, &name, &yaml_value, client).await?;
        }
        Commands::Get { namespace, name } => {
            cmd_get(&namespace, &name, client).await?;
        }
    }

    Ok(())
}

// ── `apply` command ───────────────────────────────────────────────────────────

/// Apply a YAML config to a ConfigMap without going through gRPC.
///
/// Uses the shared `apply_inner` logic from `konfig::grpc::apply` so the
/// same schema_version monotonicity check, FlatBuffers encoding, and 409 retry
/// logic is exercised by both the gRPC handler and this CLI.
async fn cmd_apply(
    namespace: &str,
    name: &str,
    yaml_value: &str,
    client: Client,
) -> Result<(), Box<dyn std::error::Error>> {
    use konfig::grpc::apply::apply_inner;

    info!(namespace, name, "Applying config");

    let resp = apply_inner(namespace, name, yaml_value, client)
        .await
        .map_err(|s| format!("Apply failed: {s}"))?;

    // Read resource_version from the ApplyResponse FlatBuffers (slot 4).
    let bytes = resp.into_inner();
    let rv = read_apply_response_resource_version(&bytes);
    println!("Applied. resource_version: {rv}");

    Ok(())
}

// ── `get` command ─────────────────────────────────────────────────────────────

/// Fetch a ConfigMap and print its FlatBuffers config as YAML.
async fn cmd_get(
    namespace: &str,
    name: &str,
    client: Client,
) -> Result<(), Box<dyn std::error::Error>> {
    info!(namespace, name, "Getting config");

    let cms: Api<ConfigMap> = Api::namespaced(client, namespace);
    let cm = cms.get(name).await.map_err(|e| format!("kube get error: {e}"))?;

    // Prefer binaryData["trading-config"].
    if let Some(binary_data) = &cm.binary_data
        && let Some(ByteString(bytes)) = binary_data.get("trading-config")
        && !bytes.is_empty()
    {
        let yaml = decode_trading_config_to_yaml(bytes)?;
        println!("{yaml}");
        return Ok(());
    }

    // Fallback: print raw data map.
    if let Some(data) = &cm.data {
        println!("# ConfigMap data keys (no binaryData found):");
        for (k, v) in data {
            println!("{k}: {v}");
        }
        return Ok(());
    }

    eprintln!("ConfigMap {namespace}/{name} has no binaryData or data.");
    std::process::exit(1);
}

// ── FlatBuffers helpers ───────────────────────────────────────────────────────

/// Decode a FlatBuffers `TradingConfig` buffer and format as YAML.
///
/// The output mirrors the apply input format for easy round-trip inspection
/// (Q3 from the State task decisions log).
fn decode_trading_config_to_yaml(bytes: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
    if bytes.is_empty() {
        return Err("empty buffer".into());
    }

    // SAFETY: binaryData["trading-config"] is written by Apply which always
    // produces valid TradingConfig FlatBuffers bytes.
    let snap = unsafe {
        konfig::types::TradingConfigSnapshot::from_flatbuffers(bytes, String::new())
    }?;

    let mut out = String::new();
    out.push_str(&format!("schema_version: {}\n", snap.schema_version));
    out.push_str("risk:\n");
    out.push_str(&format!("  max_position_usd: {}\n", snap.risk.max_position_usd));
    out.push_str(&format!("  max_order_size_usd: {}\n", snap.risk.max_order_size_usd));
    out.push_str(&format!("  max_daily_loss_usd: {}\n", snap.risk.max_daily_loss_usd));
    out.push_str(&format!("  max_orders_per_second: {}\n", snap.risk.max_orders_per_second));
    out.push_str(&format!("  max_notional_per_minute: {}\n", snap.risk.max_notional_per_minute));
    out.push_str(&format!("  enabled: {}\n", snap.risk.enabled));
    if !snap.strategies.is_empty() {
        out.push_str("strategies:\n");
        for s in &snap.strategies {
            out.push_str(&format!("  - product_id: {}\n", s.product_id));
            out.push_str(&format!("    signal_threshold: {}\n", s.signal_threshold));
            out.push_str(&format!("    lookback_window_ms: {}\n", s.lookback_window_ms));
            out.push_str(&format!("    max_spread_bps: {}\n", s.max_spread_bps));
            out.push_str(&format!("    enabled: {}\n", s.enabled));
        }
    } else {
        out.push_str("strategies: []\n");
    }

    Ok(out)
}

/// Read the `resource_version` string from an `ApplyResponse` FlatBuffers buffer.
///
/// ApplyResponse slot layout (konfig_service.fbs): slot 4 = resource_version.
fn read_apply_response_resource_version(bytes: &[u8]) -> &str {
    // SAFETY: bytes came from our FlatBuffers encoder in apply_inner.
    let table = unsafe { flatbuffers::root_unchecked::<flatbuffers::Table>(bytes) };
    unsafe {
        table
            .get::<flatbuffers::ForwardsUOffset<&str>>(4, None)
            .unwrap_or("")
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use flatbuffers::FlatBufferBuilder;

    #[test]
    fn decode_trading_config_to_yaml_round_trip() {
        let mut fbb = FlatBufferBuilder::new();
        let pid = fbb.create_string("ETH-USDT");
        let strat_start = fbb.start_table();
        fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(4, pid);
        fbb.push_slot::<f64>(6, 0.9, 0.0);
        fbb.push_slot::<u64>(8, 45_000, 0);
        fbb.push_slot::<f64>(10, 12.0, 0.0);
        fbb.push_slot::<bool>(12, true, true);
        let strat = fbb.end_table(strat_start);
        let sv = fbb.create_vector(&[strat]);

        let risk_start = fbb.start_table();
        fbb.push_slot::<f64>(4, 300_000.0, 0.0);
        fbb.push_slot::<f64>(6, 15_000.0, 0.0);
        fbb.push_slot::<f64>(8, 6_000.0, 0.0);
        fbb.push_slot::<u32>(10, 120, 0);
        fbb.push_slot::<f64>(12, 1_500_000.0, 0.0);
        fbb.push_slot::<bool>(14, true, true);
        let risk = fbb.end_table(risk_start);

        let root_start = fbb.start_table();
        fbb.push_slot::<u32>(4, 42, 0);
        fbb.push_slot_always::<flatbuffers::WIPOffset<flatbuffers::TableFinishedWIPOffset>>(6, risk);
        fbb.push_slot_always::<flatbuffers::WIPOffset<_>>(8, sv);
        let root = fbb.end_table(root_start);
        fbb.finish(root, None);

        let bytes = fbb.finished_data();
        let yaml = decode_trading_config_to_yaml(bytes).expect("decode must succeed");

        assert!(yaml.contains("schema_version: 42"), "yaml: {yaml}");
        assert!(yaml.contains("max_position_usd: 300000"), "yaml: {yaml}");
        assert!(yaml.contains("ETH-USDT"), "yaml: {yaml}");
        assert!(yaml.contains("enabled: true"), "yaml: {yaml}");
    }

    #[test]
    fn decode_empty_buffer_returns_error() {
        let result = decode_trading_config_to_yaml(&[]);
        assert!(result.is_err(), "empty buffer must return error");
    }
}
