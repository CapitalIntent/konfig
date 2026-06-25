//! CLI argument parsing and environment-tunable config helpers.

use clap::Parser;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "konfig-loadtest")]
pub(crate) struct Args {
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    pub(crate) addr: String,
    #[arg(long, default_value = "default")]
    pub(crate) namespace: String,
    #[arg(long, default_value = "my-config")]
    pub(crate) config_name: String,
    #[arg(long, default_value = "my-config-secret")]
    pub(crate) secret_name: String,
    /// Which scenario to run: all | subscribe | get | reconnect | secrets | backpressure
    #[arg(long, default_value = "all")]
    pub(crate) scenario: String,
    /// Sustained run duration in seconds. When set, scenario_subscribe_flood
    /// loops the apply phase until the deadline elapses (skips per-event
    /// accounting; drain-only success check). Intended for steady-state RSS
    /// and allocator-decay observation, not a CI gate.
    #[arg(long)]
    pub(crate) duration: Option<u64>,
    /// Optional path to write a machine-readable JSON summary of every
    /// scenario's pass/fail + latency percentiles. Opt-in: when unset (and
    /// `KONFIG_LOADTEST_RESULTS_JSON` is also unset) no file is written and
    /// behavior is unchanged. Intended for the acceptance run to commit
    /// p50/p95/p99 results (CU-86ahrg75h).
    #[arg(long)]
    pub(crate) results_json: Option<String>,
}

pub(crate) fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
pub(crate) fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
pub(crate) fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
pub(crate) fn env_u128(key: &str, default: u128) -> u128 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
