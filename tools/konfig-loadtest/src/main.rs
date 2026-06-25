//! konfig-loadtest — 5-scenario gRPC stress test for Konfig.
//!
//! Profiling stack:
//!   tracing                — structured spans/events
//!
//! Scenarios:
//!   1. subscribe_flood  — 100 subscribers + 200 applies at 100 ms intervals, p99 < 500ms
//!   2. get_flood        — 50 concurrent tasks × 100 Get RPCs, p99 < 50ms
//!   3. reconnect_storm  — 50 subscribers disconnected + resumed with RV
//!   4. secrets_flood    — 20 ApplySecret + 50 SubscribeSecrets streams, p99 < 500ms
//!   5. backpressure     — 50 normal + 5 stalled (1 s/rx) subscribers; observes
//!      replay-buffer high-water + UNAVAILABLE rate + drops.
//!
//! Sustained mode:
//!   --duration N        — when set, scenario_subscribe_flood loops applies for
//!      N seconds (no per-event accounting, drain-only check). Designed for
//!      steady-state RSS / allocator decay runs.
//!
//! Tunables (env, defaults preserve historical shape):
//!   S1_SUBSCRIBERS / S1_APPLIES / S1_INTERVAL_MS — scenario-1 shape
//!     (100 / 200 / 100 ms). For CU-86ahzwhat set 100 / 100 / 6000 (10/min × 10 min).
//!   S1_P99_LIMIT_MS — scenario-1 p99 gate in ms (default 500; set 1000 for
//!     CU-86ahzwhat's budget).
//!
//! Result output:
//!   --results-json PATH (or KONFIG_LOADTEST_RESULTS_JSON env) — opt-in; writes
//!     a JSON summary of per-scenario pass/fail + p50/p95/p99/max for committing
//!     acceptance results (CU-86ahrg75h). Unset = no file, unchanged behavior.

mod args;
mod client;
mod metrics;
mod scenarios;

use clap::Parser;
use tracing::{error, info};

use crate::args::Args;
use crate::metrics::{ScenarioResult, write_results_json};
use crate::scenarios::{
    scenario_backpressure, scenario_get_flood, scenario_reconnect_storm, scenario_secrets_flood,
    scenario_subscribe_flood,
};

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("konfig_loadtest=info".parse()?)
                .add_directive("konfig=info".parse()?),
        )
        .init();

    let args = Args::parse();

    info!(
        addr = %args.addr,
        namespace = %args.namespace,
        config_name = %args.config_name,
        secret_name = %args.secret_name,
        scenario = %args.scenario,
        duration_s = ?args.duration,
        "konfig-loadtest starting"
    );

    let run_all = args.scenario == "all";
    let mut results: Vec<ScenarioResult> = Vec::new();

    if run_all || args.scenario == "subscribe" {
        info!("=== Scenario 1: Subscribe flood + rapid apply ===");
        results.push(
            scenario_subscribe_flood(
                &args.addr,
                &args.namespace,
                &args.config_name,
                args.duration,
            )
            .await,
        );
    }

    if run_all || args.scenario == "get" {
        info!("=== Scenario 2: Get flood ===");
        results.push(scenario_get_flood(&args.addr, &args.namespace, &args.config_name).await);
    }

    if run_all || args.scenario == "reconnect" {
        info!("=== Scenario 3: Reconnect storm (replay buffer) ===");
        results
            .push(scenario_reconnect_storm(&args.addr, &args.namespace, &args.config_name).await);
    }

    if run_all || args.scenario == "secrets" {
        info!("=== Scenario 4: SubscribeSecrets flood ===");
        results.push(scenario_secrets_flood(&args.addr, &args.namespace, &args.secret_name).await);
    }

    // Backpressure is opt-in: not part of `all` because the slow-subscriber
    // sleep skews the wall-clock budget of the CI gate (~30 s vs 60 s overall).
    if args.scenario == "backpressure" {
        info!("=== Scenario 5: Slow-subscriber backpressure ===");
        results.push(scenario_backpressure(&args.addr, &args.namespace, &args.config_name).await);
    }

    // ── Summary table ─────────────────────────────────────────────────────────

    info!("┌─────────────────────────────┬──────────┐");
    info!("│ Scenario                    │ Result   │");
    info!("├─────────────────────────────┼──────────┤");
    let mut any_fail = false;
    for r in &results {
        let status = if r.pass { "PASS" } else { "FAIL" };
        info!("│ {:<27} │ {:<8} │", r.name, status);
        if !r.pass {
            any_fail = true;
            for f in &r.failures {
                error!("  FAIL: {f}");
            }
        }
    }
    info!("└─────────────────────────────┴──────────┘");

    // Opt-in machine-readable summary (CU-86ahrg75h: commit p50/p95/p99). CLI
    // flag takes precedence over the env var; when neither is set nothing is
    // written and behavior is unchanged.
    let results_json_path = args
        .results_json
        .clone()
        .or_else(|| std::env::var("KONFIG_LOADTEST_RESULTS_JSON").ok());
    if let Some(path) = results_json_path {
        match write_results_json(&path, &results, any_fail) {
            Ok(()) => info!(path = %path, "wrote results JSON"),
            // A write failure must not mask a scenario PASS, but it should be
            // loud — surface it as a process-level error after the gate check.
            Err(e) => error!(path = %path, "failed to write results JSON: {e}"),
        }
    }

    if any_fail {
        std::process::exit(1);
    }
    info!("konfig-loadtest ALL PASSED");
    Ok(())
}
