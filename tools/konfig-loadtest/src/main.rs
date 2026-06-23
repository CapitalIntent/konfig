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

use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use futures_util::StreamExt as _;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Barrier, Mutex};
use tonic::transport::Channel;
use tracing::{error, info, warn};

use konfig::proto::konfig_service_client::KonfigServiceClient;
use konfig::proto::{
    ApplyRequest, ApplySecretRequest, GetRequest, GetSecretRequest, SubscribeRequest,
    SubscribeSecretsRequest,
};

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "konfig-loadtest")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    addr: String,
    #[arg(long, default_value = "default")]
    namespace: String,
    #[arg(long, default_value = "my-config")]
    config_name: String,
    #[arg(long, default_value = "my-config-secret")]
    secret_name: String,
    /// Which scenario to run: all | subscribe | get | reconnect | secrets | backpressure
    #[arg(long, default_value = "all")]
    scenario: String,
    /// Sustained run duration in seconds. When set, scenario_subscribe_flood
    /// loops the apply phase until the deadline elapses (skips per-event
    /// accounting; drain-only success check). Intended for steady-state RSS
    /// and allocator-decay observation, not a CI gate.
    #[arg(long)]
    duration: Option<u64>,
    /// Optional path to write a machine-readable JSON summary of every
    /// scenario's pass/fail + latency percentiles. Opt-in: when unset (and
    /// `KONFIG_LOADTEST_RESULTS_JSON` is also unset) no file is written and
    /// behavior is unchanged. Intended for the acceptance run to commit
    /// p50/p95/p99 results (CU-86ahrg75h).
    #[arg(long)]
    results_json: Option<String>,
}

// ── Stats ─────────────────────────────────────────────────────────────────────

struct Stats {
    samples: Vec<u128>,
}

impl Stats {
    fn new() -> Self {
        Self {
            samples: Vec::new(),
        }
    }

    fn push(&mut self, ms: u128) {
        self.samples.push(ms);
    }

    fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    fn sorted(&self) -> Vec<u128> {
        let mut v = self.samples.clone();
        v.sort_unstable();
        v
    }

    fn p50(&self) -> u128 {
        let s = self.sorted();
        s[s.len() / 2]
    }

    fn p95(&self) -> u128 {
        let s = self.sorted();
        s[(s.len() as f64 * 0.95) as usize]
    }

    fn p99(&self) -> u128 {
        let s = self.sorted();
        s[(s.len() as f64 * 0.99) as usize]
    }

    fn max(&self) -> u128 {
        *self.sorted().last().unwrap_or(&0)
    }
}

// ── Scenario result ───────────────────────────────────────────────────────────

struct ScenarioResult {
    name: &'static str,
    pass: bool,
    failures: Vec<String>,
    /// Latency percentiles (ms) for the JSON summary. None for scenarios that
    /// do not capture per-event latency (e.g. sustained soak). Does not affect
    /// pass/fail logic — purely for machine-readable result output.
    metrics: Option<LatencyMetrics>,
}

#[derive(Clone, Copy)]
struct LatencyMetrics {
    samples: usize,
    p50_ms: u128,
    p95_ms: u128,
    p99_ms: u128,
    max_ms: u128,
}

impl ScenarioResult {
    fn pass(name: &'static str) -> Self {
        Self {
            name,
            pass: true,
            failures: Vec::new(),
            metrics: None,
        }
    }

    fn fail(name: &'static str, failures: Vec<String>) -> Self {
        Self {
            name,
            pass: false,
            failures,
            metrics: None,
        }
    }

    fn with_metrics(mut self, metrics: LatencyMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }
}

// ── Channel factory ───────────────────────────────────────────────────────────

async fn connect(addr: &str) -> Result<Channel, tonic::transport::Error> {
    Channel::from_shared(addr.to_owned())
        .expect("valid URI")
        .http2_keep_alive_interval(std::time::Duration::from_secs(20))
        .keep_alive_timeout(std::time::Duration::from_secs(10))
        .keep_alive_while_idle(true)
        .connect()
        .await
}

// ── Shared helpers ────────────────────────────────────────────────────────────

/// Resolve the seed schema_version (current + 1) for a config so applies start
/// strictly above any pre-existing state. Returns 1 when the resource is
/// missing or unreadable.
async fn seed_start_seq(addr: &str, namespace: &str, config_name: &str) -> Result<u32, String> {
    let ch = connect(addr)
        .await
        .map_err(|e| format!("connect failed: {e}"))?;
    let mut client = KonfigServiceClient::new(ch);
    let seq = match client
        .get(tonic::Request::new(GetRequest {
            namespace: namespace.to_owned(),
            name: config_name.to_owned(),
        }))
        .await
    {
        Ok(r) => r.into_inner().schema_version + 1,
        Err(_) => 1,
    };
    Ok(seq)
}

/// Drive `start_seq..=end_seq` apply RPCs at `interval_ms` cadence on a single
/// connection. Returns (ok, err) counts. Connection errors are fatal and
/// returned as `Err`.
async fn drive_applies(
    addr: &str,
    namespace: &str,
    config_name: &str,
    start_seq: u32,
    end_seq: u32,
    interval_ms: u64,
    scenario_label: &str,
) -> Result<(u32, u32), String> {
    let ch = connect(addr)
        .await
        .map_err(|e| format!("connect failed: {e}"))?;
    let mut driver = KonfigServiceClient::new(ch);
    let mut ok: u32 = 0;
    let mut err: u32 = 0;
    for seq in start_seq..=end_seq {
        let yaml = format!(
            "schema_version: {seq}\ncontent:\n  iteration: {seq}\n  scenario: {scenario_label}\n"
        );
        match driver
            .apply(tonic::Request::new(ApplyRequest {
                namespace: namespace.to_owned(),
                name: config_name.to_owned(),
                yaml_content: yaml,
            }))
            .await
        {
            Ok(_) => ok += 1,
            Err(e) => {
                err += 1;
                warn!(seq, "{scenario_label}: Apply failed: {e}");
            }
        }
        if seq < end_seq {
            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
        }
    }
    Ok((ok, err))
}

/// Background task that polls `/metrics` and tracks the
/// `konfig_replay_buffer_depth{namespace=...}` high-water mark. Stops when the
/// `stop_rx` watch flips to `true`. Returns (high_water, sample_count, errors).
fn spawn_replay_buffer_poller(
    metrics_url: Option<String>,
    namespace: String,
    mut stop_rx: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<(u64, u64, u64)> {
    tokio::spawn(async move {
        let mut high_water: u64 = 0;
        let mut samples: u64 = 0;
        let mut errors: u64 = 0;
        let Some(url) = metrics_url else {
            return (high_water, samples, errors);
        };
        loop {
            tokio::select! {
                _ = stop_rx.changed() => {
                    if *stop_rx.borrow() { break; }
                }
                _ = tokio::time::sleep(Duration::from_millis(S5_METRICS_POLL_MS)) => {
                    match fetch_replay_buffer_depth(&url, &namespace).await {
                        Ok(depth) => {
                            samples += 1;
                            if depth > high_water { high_water = depth; }
                        }
                        Err(_) => errors += 1,
                    }
                }
            }
        }
        (high_water, samples, errors)
    })
}

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

/// Write a machine-readable JSON summary of all scenario results to `path`.
/// Hand-rolled (no serde dep) — the schema is small and stable:
///
///   {"all_passed":bool,"scenarios":[
///     {"name":str,"pass":bool,
///      "metrics":{"samples":n,"p50_ms":n,"p95_ms":n,"p99_ms":n,"max_ms":n}|null,
///      "failures":[str,...]}
///   ]}
fn write_results_json(
    path: &str,
    results: &[ScenarioResult],
    any_fail: bool,
) -> std::io::Result<()> {
    fn esc(s: &str) -> String {
        // Minimal JSON string escaping: backslash, quote, and control chars
        // that appear in our failure messages. Sufficient for this fixed set
        // of programmatically-built strings.
        let mut out = String::with_capacity(s.len() + 2);
        for c in s.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                c => out.push(c),
            }
        }
        out
    }

    let mut json = String::new();
    json.push_str(&format!("{{\"all_passed\":{},\"scenarios\":[", !any_fail));
    for (i, r) in results.iter().enumerate() {
        if i > 0 {
            json.push(',');
        }
        json.push_str(&format!(
            "{{\"name\":\"{}\",\"pass\":{},",
            esc(r.name),
            r.pass
        ));
        match &r.metrics {
            Some(m) => json.push_str(&format!(
                "\"metrics\":{{\"samples\":{},\"p50_ms\":{},\"p95_ms\":{},\"p99_ms\":{},\"max_ms\":{}}},",
                m.samples, m.p50_ms, m.p95_ms, m.p99_ms, m.max_ms
            )),
            None => json.push_str("\"metrics\":null,"),
        }
        json.push_str("\"failures\":[");
        for (j, f) in r.failures.iter().enumerate() {
            if j > 0 {
                json.push(',');
            }
            json.push_str(&format!("\"{}\"", esc(f)));
        }
        json.push_str("]}");
    }
    json.push_str("]}\n");

    std::fs::write(path, json)
}

// ── Scenario 1: Subscribe flood + rapid apply ─────────────────────────────────

const S1_DRAIN_SECS: u64 = 30;
const S1_P99_LIMIT_MS: u128 = 500;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}
fn env_u128(key: &str, default: u128) -> u128 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

async fn scenario_subscribe_flood(
    addr: &str,
    namespace: &str,
    config_name: &str,
    duration_secs: Option<u64>,
) -> ScenarioResult {
    // S1 knobs — env overrides let the CI gate and the stress profile share
    // one binary. Defaults preserve the historical 100×200×100 ms shape.
    let s1_subscribers: usize = env_usize("S1_SUBSCRIBERS", 100);
    let s1_applies: u32 = env_u32("S1_APPLIES", 200);
    let s1_interval_ms: u64 = env_u64("S1_INTERVAL_MS", 100);
    // p99 gate is env-tunable so the acceptance run (CU-86ahzwhat) can set the
    // 1000 ms budget without a recompile. Default preserves the 500 ms bar.
    let s1_p99_limit_ms: u128 = env_u128("S1_P99_LIMIT_MS", S1_P99_LIMIT_MS);

    if let Some(secs) = duration_secs {
        return scenario_subscribe_flood_sustained(
            addr,
            namespace,
            config_name,
            s1_subscribers,
            s1_interval_ms,
            secs,
        )
        .await;
    }

    // Shared state.
    let latencies: Arc<Mutex<Stats>> = Arc::new(Mutex::new(Stats::new()));
    let event_counts: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(vec![0u32; s1_subscribers]));
    let apply_timestamps: Arc<Mutex<Vec<Option<Instant>>>> =
        Arc::new(Mutex::new(vec![None; s1_applies as usize]));
    let successful_applies: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
    let barrier = Arc::new(Barrier::new(s1_subscribers + 1));

    // Seed: get current schema_version to start above it.
    let start_seq = {
        let ch = match connect(addr).await {
            Ok(c) => c,
            Err(e) => {
                return ScenarioResult::fail(
                    "subscribe_flood",
                    vec![format!("connect failed: {e}")],
                );
            }
        };
        let mut client = KonfigServiceClient::new(ch);
        match client
            .get(tonic::Request::new(GetRequest {
                namespace: namespace.to_owned(),
                name: config_name.to_owned(),
            }))
            .await
        {
            Ok(r) => r.into_inner().schema_version + 1,
            Err(_) => 1,
        }
    };
    let end_seq = start_seq + s1_applies - 1;

    let mut sub_handles = Vec::with_capacity(s1_subscribers);
    for sub_id in 0..s1_subscribers {
        let h = tokio::spawn(s1_subscriber(
            sub_id,
            addr.to_owned(),
            namespace.to_owned(),
            config_name.to_owned(),
            start_seq,
            Arc::clone(&latencies),
            Arc::clone(&event_counts),
            Arc::clone(&apply_timestamps),
            Arc::clone(&barrier),
        ));
        sub_handles.push(h);
    }

    // Wait for all 100 to connect.
    barrier.wait().await;
    info!(
        "S1: all {} subscribers connected — starting apply loop ({}ms interval)",
        s1_subscribers, s1_interval_ms
    );

    // Apply loop: 200 applies at 100 ms intervals.
    let ch = match connect(addr).await {
        Ok(c) => c,
        Err(e) => {
            for h in sub_handles {
                h.abort();
            }
            return ScenarioResult::fail("subscribe_flood", vec![format!("connect failed: {e}")]);
        }
    };
    let mut driver = KonfigServiceClient::new(ch);

    for seq in start_seq..=end_seq {
        let yaml = format!(
            "schema_version: {seq}\ncontent:\n  iteration: {seq}\n  scenario: subscribe_flood\n"
        );
        match driver
            .apply(tonic::Request::new(ApplyRequest {
                namespace: namespace.to_owned(),
                name: config_name.to_owned(),
                yaml_content: yaml,
            }))
            .await
        {
            Ok(_) => {
                let idx = (seq - start_seq) as usize;
                apply_timestamps.lock().await[idx] = Some(Instant::now());
                *successful_applies.lock().await += 1;
            }
            Err(e) => warn!(seq, "S1: Apply failed: {e}"),
        }
        if seq < end_seq {
            tokio::time::sleep(Duration::from_millis(s1_interval_ms)).await;
        }
    }

    let n_ok = *successful_applies.lock().await;
    let total_expected = s1_subscribers as u32 * n_ok;
    info!(
        "S1: apply loop done ({n_ok}/{} succeeded) — draining",
        s1_applies
    );

    // Drain with timeout.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(S1_DRAIN_SECS);
    loop {
        let received: u32 = event_counts.lock().await.iter().sum();
        if received >= total_expected {
            info!("S1: all {total_expected} events drained");
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            warn!(received, total_expected, "S1: drain timeout");
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for h in sub_handles {
        h.abort();
    }

    let lat = latencies.lock().await;
    let counts = event_counts.lock().await;
    let total_received: u32 = counts.iter().sum();
    let missed = total_expected.saturating_sub(total_received);

    if lat.is_empty() {
        return ScenarioResult::fail(
            "subscribe_flood",
            vec!["no latency samples — did subscribers connect?".into()],
        );
    }

    let (p50, p95, p99, max) = (lat.p50(), lat.p95(), lat.p99(), lat.max());
    info!(
        samples = lat.samples.len(),
        p50_ms = p50,
        p95_ms = p95,
        p99_ms = p99,
        max_ms = max,
        total_expected,
        total_received,
        missed,
        "S1 results"
    );

    let mut failures = Vec::new();
    if p99 >= s1_p99_limit_ms {
        failures.push(format!("p99 {p99} ms >= gate {s1_p99_limit_ms} ms"));
    }
    if missed > 0 {
        failures.push(format!("{missed} missed events"));
    }

    let metrics = LatencyMetrics {
        samples: lat.samples.len(),
        p50_ms: p50,
        p95_ms: p95,
        p99_ms: p99,
        max_ms: max,
    };
    if failures.is_empty() {
        ScenarioResult::pass("subscribe_flood").with_metrics(metrics)
    } else {
        ScenarioResult::fail("subscribe_flood", failures).with_metrics(metrics)
    }
}

// ── Scenario 1 (sustained): drain-only check over wall-clock window ───────────
//
// Per-event apply-timestamp accounting is intentionally skipped — over a
// 10-min run at 25k events/s the timestamp vector and the latency histogram
// would themselves leak the loadtest process. The success criterion is "the
// system kept up": applies returned Ok and at the end the broadcast queues
// drain. Steady-state RSS / allocator decay must be observed externally
// (Prometheus, pprof, `ps` slope) — that is the point of the sustained mode.

const S1_SUSTAINED_DRAIN_SECS: u64 = 60;

async fn scenario_subscribe_flood_sustained(
    addr: &str,
    namespace: &str,
    config_name: &str,
    s1_subscribers: usize,
    s1_interval_ms: u64,
    duration_secs: u64,
) -> ScenarioResult {
    let event_counts: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(vec![0u64; s1_subscribers]));
    let barrier = Arc::new(Barrier::new(s1_subscribers + 1));

    // Seed: get current schema_version to start above it.
    let start_seq = {
        let ch = match connect(addr).await {
            Ok(c) => c,
            Err(e) => {
                return ScenarioResult::fail(
                    "subscribe_flood_sustained",
                    vec![format!("connect failed: {e}")],
                );
            }
        };
        let mut client = KonfigServiceClient::new(ch);
        match client
            .get(tonic::Request::new(GetRequest {
                namespace: namespace.to_owned(),
                name: config_name.to_owned(),
            }))
            .await
        {
            Ok(r) => r.into_inner().schema_version + 1,
            Err(_) => 1,
        }
    };

    // Spawn subscribers — they just count, no latency capture.
    let mut sub_handles = Vec::with_capacity(s1_subscribers);
    for sub_id in 0..s1_subscribers {
        let h = tokio::spawn(s1_subscriber_sustained(
            sub_id,
            addr.to_owned(),
            namespace.to_owned(),
            config_name.to_owned(),
            start_seq,
            Arc::clone(&event_counts),
            Arc::clone(&barrier),
        ));
        sub_handles.push(h);
    }
    barrier.wait().await;
    info!(
        "S1 sustained: {} subscribers connected — applying for {} s ({}ms interval)",
        s1_subscribers, duration_secs, s1_interval_ms
    );

    // Apply loop until deadline.
    let ch = match connect(addr).await {
        Ok(c) => c,
        Err(e) => {
            for h in sub_handles {
                h.abort();
            }
            return ScenarioResult::fail(
                "subscribe_flood_sustained",
                vec![format!("connect failed: {e}")],
            );
        }
    };
    let mut driver = KonfigServiceClient::new(ch);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(duration_secs);
    let mut seq = start_seq;
    let mut n_ok: u64 = 0;
    let mut n_err: u64 = 0;
    loop {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        let yaml = format!(
            "schema_version: {seq}\ncontent:\n  iteration: {seq}\n  scenario: subscribe_flood_sustained\n"
        );
        match driver
            .apply(tonic::Request::new(ApplyRequest {
                namespace: namespace.to_owned(),
                name: config_name.to_owned(),
                yaml_content: yaml,
            }))
            .await
        {
            Ok(_) => n_ok += 1,
            Err(e) => {
                n_err += 1;
                warn!(seq, "S1 sustained: Apply failed: {e}");
            }
        }
        seq = seq.wrapping_add(1);
        tokio::time::sleep(Duration::from_millis(s1_interval_ms)).await;
    }

    let total_expected = (s1_subscribers as u64) * n_ok;
    info!(
        n_ok,
        n_err, total_expected, "S1 sustained: apply loop done — draining"
    );

    // Drain.
    let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(S1_SUSTAINED_DRAIN_SECS);
    let drained;
    loop {
        let received: u64 = event_counts.lock().await.iter().sum();
        if received >= total_expected {
            info!("S1 sustained: all {total_expected} events drained");
            drained = true;
            break;
        }
        if tokio::time::Instant::now() >= drain_deadline {
            warn!(
                received,
                total_expected, "S1 sustained: drain timeout — broadcast may have lagged"
            );
            drained = false;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for h in sub_handles {
        h.abort();
    }

    let received_final: u64 = event_counts.lock().await.iter().sum();
    info!(
        applies_ok = n_ok,
        applies_err = n_err,
        subscribers = s1_subscribers,
        total_expected,
        total_received = received_final,
        "S1 sustained results"
    );

    // Sustained mode is a soak: success is "applies returned Ok and queues
    // drained". p99 / per-event miss accounting is not asserted because the
    // observation target is external (RSS slope, allocator decay).
    let mut failures = Vec::new();
    if n_ok == 0 {
        failures.push("zero successful applies".into());
    }
    if !drained {
        failures.push(format!("drain timeout: {received_final}/{total_expected}"));
    }
    if failures.is_empty() {
        ScenarioResult::pass("subscribe_flood_sustained")
    } else {
        ScenarioResult::fail("subscribe_flood_sustained", failures)
    }
}

async fn s1_subscriber_sustained(
    sub_id: usize,
    addr: String,
    namespace: String,
    config_name: String,
    start_seq: u32,
    event_counts: Arc<Mutex<Vec<u64>>>,
    barrier: Arc<Barrier>,
) {
    let ch = match connect(&addr).await {
        Ok(c) => c,
        Err(e) => {
            warn!(sub_id, "S1 sustained: connect failed: {e}");
            barrier.wait().await;
            return;
        }
    };
    let mut client = KonfigServiceClient::new(ch);
    let stream = match client
        .subscribe(tonic::Request::new(SubscribeRequest {
            // Loadtest does not exercise server-side label filtering.
            label_selector: String::new(),
            namespace: namespace.clone(),
            names: vec![config_name.clone()],
            resume_resource_version: String::new(),
        }))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => {
            warn!(sub_id, "S1 sustained: subscribe failed: {e}");
            barrier.wait().await;
            return;
        }
    };
    barrier.wait().await;

    let mut stream = stream;
    while let Some(item) = stream.next().await {
        match item {
            Ok(event) => {
                let version = event.config.as_ref().map(|c| c.schema_version).unwrap_or(0);
                if version < start_seq {
                    continue;
                }
                event_counts.lock().await[sub_id] += 1;
            }
            Err(e) => {
                warn!(sub_id, "S1 sustained: stream error: {e}");
                break;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn s1_subscriber(
    sub_id: usize,
    addr: String,
    namespace: String,
    config_name: String,
    start_seq: u32,
    latencies: Arc<Mutex<Stats>>,
    event_counts: Arc<Mutex<Vec<u32>>>,
    apply_timestamps: Arc<Mutex<Vec<Option<Instant>>>>,
    barrier: Arc<Barrier>,
) {
    let ch = match connect(&addr).await {
        Ok(c) => c,
        Err(e) => {
            warn!(sub_id, "S1: connect failed: {e}");
            barrier.wait().await;
            return;
        }
    };
    let mut client = KonfigServiceClient::new(ch);
    let stream = match client
        .subscribe(tonic::Request::new(SubscribeRequest {
            // Loadtest does not exercise server-side label filtering.
            label_selector: String::new(),
            namespace: namespace.clone(),
            names: vec![config_name.clone()],
            resume_resource_version: String::new(),
        }))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => {
            warn!(sub_id, "S1: subscribe failed: {e}");
            barrier.wait().await;
            return;
        }
    };
    barrier.wait().await;

    let mut stream = stream;
    while let Some(item) = stream.next().await {
        let received_at = Instant::now();
        match item {
            Ok(event) => {
                let version = event.config.as_ref().map(|c| c.schema_version).unwrap_or(0);
                if version < start_seq {
                    continue;
                }
                let idx = (version - start_seq) as usize;
                let lag_ms = {
                    let ts = apply_timestamps.lock().await;
                    ts.get(idx)
                        .and_then(|t| *t)
                        .map(|t| received_at.saturating_duration_since(t).as_millis())
                };
                if let Some(ms) = lag_ms {
                    latencies.lock().await.push(ms);
                }
                event_counts.lock().await[sub_id] += 1;
            }
            Err(e) => {
                warn!(sub_id, "S1: stream error: {e}");
                break;
            }
        }
    }
}

// ── Scenario 2: Get flood ─────────────────────────────────────────────────────

const S2_TASKS: usize = 50;
const S2_GETS_PER_TASK: usize = 100;
const S2_P99_LIMIT_MS: u128 = 50;

async fn scenario_get_flood(addr: &str, namespace: &str, config_name: &str) -> ScenarioResult {
    let latencies: Arc<Mutex<Stats>> = Arc::new(Mutex::new(Stats::new()));
    let error_count: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));

    let mut handles = Vec::with_capacity(S2_TASKS);
    for task_id in 0..S2_TASKS {
        let h = tokio::spawn(s2_get_task(
            task_id,
            addr.to_owned(),
            namespace.to_owned(),
            config_name.to_owned(),
            Arc::clone(&latencies),
            Arc::clone(&error_count),
        ));
        handles.push(h);
    }

    for h in handles {
        let _ = h.await;
    }

    let lat = latencies.lock().await;
    let errors = *error_count.lock().await;

    if lat.is_empty() {
        return ScenarioResult::fail("get_flood", vec!["no latency samples".into()]);
    }

    let (p50, p95, p99, max) = (lat.p50(), lat.p95(), lat.p99(), lat.max());
    info!(
        samples = lat.samples.len(),
        p50_ms = p50,
        p95_ms = p95,
        p99_ms = p99,
        max_ms = max,
        errors,
        "S2 results"
    );

    let mut failures = Vec::new();
    if errors > 0 {
        failures.push(format!("{errors} RPC errors"));
    }
    if p99 >= S2_P99_LIMIT_MS {
        failures.push(format!("p99 {p99} ms >= gate {S2_P99_LIMIT_MS} ms"));
    }

    let metrics = LatencyMetrics {
        samples: lat.samples.len(),
        p50_ms: p50,
        p95_ms: p95,
        p99_ms: p99,
        max_ms: max,
    };
    if failures.is_empty() {
        ScenarioResult::pass("get_flood").with_metrics(metrics)
    } else {
        ScenarioResult::fail("get_flood", failures).with_metrics(metrics)
    }
}

async fn s2_get_task(
    task_id: usize,
    addr: String,
    namespace: String,
    config_name: String,
    latencies: Arc<Mutex<Stats>>,
    error_count: Arc<Mutex<u64>>,
) {
    let ch = match connect(&addr).await {
        Ok(c) => c,
        Err(e) => {
            warn!(task_id, "S2: connect failed: {e}");
            *error_count.lock().await += S2_GETS_PER_TASK as u64;
            return;
        }
    };
    let mut client = KonfigServiceClient::new(ch);

    for _ in 0..S2_GETS_PER_TASK {
        let start = Instant::now();
        match client
            .get(tonic::Request::new(GetRequest {
                namespace: namespace.clone(),
                name: config_name.clone(),
            }))
            .await
        {
            Ok(_) => {
                let ms = start.elapsed().as_millis();
                latencies.lock().await.push(ms);
            }
            Err(e) => {
                warn!(task_id, "S2: Get failed: {e}");
                *error_count.lock().await += 1;
            }
        }
    }
}

// ── Scenario 3: Reconnect storm (replay buffer) ───────────────────────────────

const S3_SUBSCRIBERS: usize = 50;
const S3_WARM_APPLIES: u32 = 5;
const S3_POST_APPLIES: u32 = 10;
const S3_INTERVAL_MS: u64 = 100;
const S3_DRAIN_SECS: u64 = 15;
const S3_WARM_DRAIN_SECS: u64 = 1;
const S3_WARM_RV_QUORUM: usize = S3_SUBSCRIBERS;

async fn scenario_reconnect_storm(
    addr: &str,
    namespace: &str,
    config_name: &str,
) -> ScenarioResult {
    // Phase 1: get current version.
    let ch = match connect(addr).await {
        Ok(c) => c,
        Err(e) => {
            return ScenarioResult::fail("reconnect_storm", vec![format!("connect failed: {e}")]);
        }
    };
    let mut driver = KonfigServiceClient::new(ch);
    let base_seq = match driver
        .get(tonic::Request::new(GetRequest {
            namespace: namespace.to_owned(),
            name: config_name.to_owned(),
        }))
        .await
    {
        Ok(r) => r.into_inner().schema_version + 1,
        Err(_) => 1,
    };

    // Phase 2: apply 5 warm-up events; subscribers connect and watch.
    info!("S3: applying {S3_WARM_APPLIES} warm-up events (base_seq={base_seq})");
    let warm_end = base_seq + S3_WARM_APPLIES - 1;
    for seq in base_seq..=warm_end {
        let yaml = format!("schema_version: {seq}\ncontent:\n  phase: warmup\n  seq: {seq}\n");
        if let Err(e) = driver
            .apply(tonic::Request::new(ApplyRequest {
                namespace: namespace.to_owned(),
                name: config_name.to_owned(),
                yaml_content: yaml,
            }))
            .await
        {
            warn!(seq, "S3: warm apply failed: {e}");
        }
        tokio::time::sleep(Duration::from_millis(S3_INTERVAL_MS)).await;
    }

    // Phase 3: spawn 50 subscribers that connect and record their last RV.
    let last_rvs: Arc<Mutex<Vec<String>>> =
        Arc::new(Mutex::new(vec![String::new(); S3_SUBSCRIBERS]));
    let barrier = Arc::new(Barrier::new(S3_SUBSCRIBERS + 1));
    let mut sub_handles = Vec::with_capacity(S3_SUBSCRIBERS);

    for sub_id in 0..S3_SUBSCRIBERS {
        let h = tokio::spawn(s3_subscriber_phase1(
            sub_id,
            addr.to_owned(),
            namespace.to_owned(),
            config_name.to_owned(),
            Arc::clone(&last_rvs),
            Arc::clone(&barrier),
        ));
        sub_handles.push(h);
    }

    barrier.wait().await;
    info!("S3: {S3_SUBSCRIBERS} subscribers connected — waiting for warm events to land");

    let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(S3_WARM_DRAIN_SECS);
    let (quorum_met, final_populated) = loop {
        let populated = last_rvs
            .lock()
            .await
            .iter()
            .filter(|rv| !rv.is_empty())
            .count();
        if populated >= S3_WARM_RV_QUORUM {
            break (true, populated);
        }
        if tokio::time::Instant::now() >= drain_deadline {
            error!(
                populated,
                quorum = S3_WARM_RV_QUORUM,
                "S3: warm drain timeout — fewer subscribers have RV than quorum"
            );
            break (false, populated);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    // Phase 4: abort all subscribers simultaneously (simulate disconnect).
    info!("S3: aborting all subscribers simultaneously");
    for h in sub_handles {
        h.abort();
    }

    // Collect the highest RV across subscribers — picking the max means we
    // resume from the latest warm event everybody has acknowledged, avoiding
    // duplicate replay and ensuring post-applies are the only events expected.
    let rvs = last_rvs.lock().await.clone();
    let known_rv = rvs
        .iter()
        .filter(|rv| !rv.is_empty())
        .max_by_key(|rv| rv.parse::<u64>().unwrap_or(0))
        .cloned()
        .unwrap_or_default();
    info!(known_rv = %known_rv, "S3: using resume_rv for reconnect");

    // Phase 5: reconnect first, then fire post-applies. Spawning the apply
    // loop before subscribers register lets the leading applies race ahead of
    // the server-side subscribe registration, which surfaces as "missed events
    // post-reconnect" even though resume_resource_version replay is correct.
    let post_start = warm_end + 1;
    let post_end = post_start + S3_POST_APPLIES - 1;

    let apply_timestamps: Arc<Mutex<Vec<Option<Instant>>>> =
        Arc::new(Mutex::new(vec![None; S3_POST_APPLIES as usize]));
    let successful_post: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));

    info!("S3: reconnecting {S3_SUBSCRIBERS} subscribers with resume_rv={known_rv}");
    let event_counts: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(vec![0u32; S3_SUBSCRIBERS]));
    let reconnect_barrier = Arc::new(Barrier::new(S3_SUBSCRIBERS + 1));
    let mut reconnect_handles = Vec::with_capacity(S3_SUBSCRIBERS);

    for sub_id in 0..S3_SUBSCRIBERS {
        let h = tokio::spawn(s3_subscriber_phase2(
            sub_id,
            addr.to_owned(),
            namespace.to_owned(),
            config_name.to_owned(),
            known_rv.clone(),
            post_start,
            Arc::clone(&event_counts),
            Arc::clone(&apply_timestamps),
            Arc::clone(&reconnect_barrier),
        ));
        reconnect_handles.push(h);
    }

    reconnect_barrier.wait().await;
    info!("S3: all {S3_SUBSCRIBERS} subscribers reconnected");

    let apply_ts_clone = Arc::clone(&apply_timestamps);
    let successful_post_clone = Arc::clone(&successful_post);
    let addr2 = addr.to_owned();
    let ns2 = namespace.to_owned();
    let cn2 = config_name.to_owned();
    let apply_handle = tokio::spawn(async move {
        let ch2 = connect(&addr2).await.expect("connect");
        let mut drv2 = KonfigServiceClient::new(ch2);
        for seq in post_start..=post_end {
            let yaml =
                format!("schema_version: {seq}\ncontent:\n  phase: post_reconnect\n  seq: {seq}\n");
            match drv2
                .apply(tonic::Request::new(ApplyRequest {
                    namespace: ns2.clone(),
                    name: cn2.clone(),
                    yaml_content: yaml,
                }))
                .await
            {
                Ok(_) => {
                    let idx = (seq - post_start) as usize;
                    apply_ts_clone.lock().await[idx] = Some(Instant::now());
                    *successful_post_clone.lock().await += 1;
                }
                Err(e) => warn!(seq, "S3: post apply failed: {e}"),
            }
            tokio::time::sleep(Duration::from_millis(S3_INTERVAL_MS)).await;
        }
    });

    // Wait for applies to finish.
    let _ = apply_handle.await;
    let n_ok = *successful_post.lock().await;
    let total_expected = S3_SUBSCRIBERS as u32 * n_ok;

    // Drain with timeout.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(S3_DRAIN_SECS);
    loop {
        let received: u32 = event_counts.lock().await.iter().sum();
        if received >= total_expected {
            info!("S3: all {total_expected} post-reconnect events drained");
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            warn!(received, total_expected, "S3: drain timeout");
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for h in reconnect_handles {
        h.abort();
    }

    let counts = event_counts.lock().await;
    let total_received: u32 = counts.iter().sum();
    let missed = total_expected.saturating_sub(total_received);

    info!(
        post_applies = n_ok,
        total_expected, total_received, missed, "S3 results"
    );

    let mut failures: Vec<String> = Vec::new();
    if !quorum_met {
        failures.push(format!(
            "warm-event RV quorum missed: {final_populated}/{S3_WARM_RV_QUORUM} subscribers reported a resource_version within {S3_WARM_DRAIN_SECS}s — Subscribe should emit a synchronous snapshot on connect"
        ));
    }
    if missed > 0 {
        failures.push(format!("{missed} missed events post-reconnect"));
    }
    if failures.is_empty() {
        ScenarioResult::pass("reconnect_storm")
    } else {
        ScenarioResult::fail("reconnect_storm", failures)
    }
}

/// Phase 1 subscriber: connects, records last RV seen, signals barrier.
async fn s3_subscriber_phase1(
    sub_id: usize,
    addr: String,
    namespace: String,
    config_name: String,
    last_rvs: Arc<Mutex<Vec<String>>>,
    barrier: Arc<Barrier>,
) {
    let ch = match connect(&addr).await {
        Ok(c) => c,
        Err(e) => {
            warn!(sub_id, "S3p1: connect failed: {e}");
            barrier.wait().await;
            return;
        }
    };
    let mut client = KonfigServiceClient::new(ch);
    let stream = match client
        .subscribe(tonic::Request::new(SubscribeRequest {
            // Loadtest does not exercise server-side label filtering.
            label_selector: String::new(),
            namespace: namespace.clone(),
            names: vec![config_name.clone()],
            resume_resource_version: String::new(),
        }))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => {
            warn!(sub_id, "S3p1: subscribe failed: {e}");
            barrier.wait().await;
            return;
        }
    };
    barrier.wait().await;

    let mut stream = stream;
    while let Some(item) = stream.next().await {
        match item {
            Ok(event) => {
                if let Some(cfg) = event.config {
                    last_rvs.lock().await[sub_id] = cfg.resource_version;
                }
            }
            Err(_) => break,
        }
    }
}

/// Phase 2 subscriber: reconnects with resume_rv, counts received post-applies.
#[allow(clippy::too_many_arguments)]
async fn s3_subscriber_phase2(
    sub_id: usize,
    addr: String,
    namespace: String,
    config_name: String,
    resume_rv: String,
    post_start: u32,
    event_counts: Arc<Mutex<Vec<u32>>>,
    apply_timestamps: Arc<Mutex<Vec<Option<Instant>>>>,
    barrier: Arc<Barrier>,
) {
    let ch = match connect(&addr).await {
        Ok(c) => c,
        Err(e) => {
            warn!(sub_id, "S3p2: connect failed: {e}");
            barrier.wait().await;
            return;
        }
    };
    let mut client = KonfigServiceClient::new(ch);
    let stream = match client
        .subscribe(tonic::Request::new(SubscribeRequest {
            // Loadtest does not exercise server-side label filtering.
            label_selector: String::new(),
            namespace: namespace.clone(),
            names: vec![config_name.clone()],
            resume_resource_version: resume_rv.clone(),
        }))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => {
            warn!(sub_id, resume_rv = %resume_rv, "S3p2: reconnect failed: {e}");
            barrier.wait().await;
            return;
        }
    };
    barrier.wait().await;

    let mut stream = stream;
    while let Some(item) = stream.next().await {
        match item {
            Ok(event) => {
                let version = event.config.as_ref().map(|c| c.schema_version).unwrap_or(0);
                if version < post_start {
                    continue;
                }
                let idx = (version - post_start) as usize;
                let _lag_ms = {
                    let ts = apply_timestamps.lock().await;
                    ts.get(idx)
                        .and_then(|t| *t)
                        .map(|t| Instant::now().saturating_duration_since(t).as_millis())
                };
                event_counts.lock().await[sub_id] += 1;
            }
            Err(e) => {
                warn!(sub_id, "S3p2: stream error: {e}");
                break;
            }
        }
    }
}

// ── Scenario 4: SubscribeSecrets flood ────────────────────────────────────────

const S4_APPLIES: u32 = 20;
const S4_SUBSCRIBERS: usize = 50;
const S4_INTERVAL_MS: u64 = 100;
const S4_DRAIN_SECS: u64 = 15;
const S4_P99_LIMIT_MS: u128 = 500;

async fn scenario_secrets_flood(addr: &str, namespace: &str, secret_name: &str) -> ScenarioResult {
    let latencies: Arc<Mutex<Stats>> = Arc::new(Mutex::new(Stats::new()));
    let event_counts: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(vec![0u32; S4_SUBSCRIBERS]));
    let apply_timestamps: Arc<Mutex<Vec<Option<Instant>>>> =
        Arc::new(Mutex::new(vec![None; S4_APPLIES as usize]));
    let successful_applies: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
    let barrier = Arc::new(Barrier::new(S4_SUBSCRIBERS + 1));

    // Get current schema_version to start above it.
    let ch = match connect(addr).await {
        Ok(c) => c,
        Err(e) => {
            return ScenarioResult::fail("secrets_flood", vec![format!("connect failed: {e}")]);
        }
    };
    let mut driver = KonfigServiceClient::new(ch);
    // Seed: read current schema_version so applies start above it (mirrors S1 pattern).
    let start_seq: u32 = match driver
        .get_secret(tonic::Request::new(GetSecretRequest {
            namespace: namespace.to_owned(),
            name: secret_name.to_owned(),
        }))
        .await
    {
        Ok(r) => r.into_inner().schema_version + 1,
        Err(_) => 1, // NotFound or any error — start from 1
    };
    let end_seq = start_seq + S4_APPLIES - 1;

    // Spawn 50 SubscribeSecrets streams.
    let mut sub_handles = Vec::with_capacity(S4_SUBSCRIBERS);
    for sub_id in 0..S4_SUBSCRIBERS {
        let h = tokio::spawn(s4_subscriber(
            sub_id,
            addr.to_owned(),
            namespace.to_owned(),
            secret_name.to_owned(),
            start_seq,
            Arc::clone(&latencies),
            Arc::clone(&event_counts),
            Arc::clone(&apply_timestamps),
            Arc::clone(&barrier),
        ));
        sub_handles.push(h);
    }

    barrier.wait().await;
    info!(
        "S4: {S4_SUBSCRIBERS} SubscribeSecrets streams connected — applying {} secrets",
        S4_APPLIES
    );

    // Apply 20 secrets at 100 ms intervals.
    for seq in start_seq..=end_seq {
        let yaml = format!("schema_version: {seq}\ntoken: loadtest-secret-{seq}\n");
        match driver
            .apply_secret(tonic::Request::new(ApplySecretRequest {
                namespace: namespace.to_owned(),
                name: secret_name.to_owned(),
                yaml_content: yaml,
            }))
            .await
        {
            Ok(_) => {
                let idx = (seq - start_seq) as usize;
                apply_timestamps.lock().await[idx] = Some(Instant::now());
                *successful_applies.lock().await += 1;
            }
            Err(e) => warn!(seq, "S4: ApplySecret failed: {e}"),
        }
        if seq < end_seq {
            tokio::time::sleep(Duration::from_millis(S4_INTERVAL_MS)).await;
        }
    }

    let n_ok = *successful_applies.lock().await;
    let total_expected = S4_SUBSCRIBERS as u32 * n_ok;
    info!(
        "S4: apply loop done ({n_ok}/{} succeeded) — draining",
        S4_APPLIES
    );

    // Drain with timeout.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(S4_DRAIN_SECS);
    loop {
        let received: u32 = event_counts.lock().await.iter().sum();
        if received >= total_expected {
            info!("S4: all {total_expected} secret events drained");
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            warn!(received, total_expected, "S4: drain timeout");
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for h in sub_handles {
        h.abort();
    }

    let lat = latencies.lock().await;
    let counts = event_counts.lock().await;
    let total_received: u32 = counts.iter().sum();
    let missed = total_expected.saturating_sub(total_received);

    if lat.is_empty() {
        return ScenarioResult::fail(
            "secrets_flood",
            vec!["no latency samples — did subscribers connect?".into()],
        );
    }

    let (p50, p95, p99, max) = (lat.p50(), lat.p95(), lat.p99(), lat.max());
    info!(
        samples = lat.samples.len(),
        p50_ms = p50,
        p95_ms = p95,
        p99_ms = p99,
        max_ms = max,
        total_expected,
        total_received,
        missed,
        "S4 results"
    );

    let mut failures = Vec::new();
    if p99 >= S4_P99_LIMIT_MS {
        failures.push(format!("p99 {p99} ms >= gate {S4_P99_LIMIT_MS} ms"));
    }
    if missed > 0 {
        failures.push(format!("{missed} missed secret events"));
    }

    let metrics = LatencyMetrics {
        samples: lat.samples.len(),
        p50_ms: p50,
        p95_ms: p95,
        p99_ms: p99,
        max_ms: max,
    };
    if failures.is_empty() {
        ScenarioResult::pass("secrets_flood").with_metrics(metrics)
    } else {
        ScenarioResult::fail("secrets_flood", failures).with_metrics(metrics)
    }
}

#[allow(clippy::too_many_arguments)]
async fn s4_subscriber(
    sub_id: usize,
    addr: String,
    namespace: String,
    secret_name: String,
    start_seq: u32,
    latencies: Arc<Mutex<Stats>>,
    event_counts: Arc<Mutex<Vec<u32>>>,
    apply_timestamps: Arc<Mutex<Vec<Option<Instant>>>>,
    barrier: Arc<Barrier>,
) {
    let ch = match connect(&addr).await {
        Ok(c) => c,
        Err(e) => {
            warn!(sub_id, "S4: connect failed: {e}");
            barrier.wait().await;
            return;
        }
    };
    let mut client = KonfigServiceClient::new(ch);
    let stream = match client
        .subscribe_secrets(tonic::Request::new(SubscribeSecretsRequest {
            namespace: namespace.clone(),
            names: vec![secret_name.clone()],
            resume_resource_version: String::new(),
        }))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => {
            warn!(sub_id, "S4: SubscribeSecrets failed: {e}");
            barrier.wait().await;
            return;
        }
    };
    barrier.wait().await;

    let mut stream = stream;
    while let Some(item) = stream.next().await {
        let received_at = Instant::now();
        match item {
            Ok(event) => {
                let version = event.secret.as_ref().map(|s| s.schema_version).unwrap_or(0);
                if version < start_seq {
                    continue;
                }
                let idx = (version - start_seq) as usize;
                let lag_ms = {
                    let ts = apply_timestamps.lock().await;
                    ts.get(idx)
                        .and_then(|t| *t)
                        .map(|t| received_at.saturating_duration_since(t).as_millis())
                };
                if let Some(ms) = lag_ms {
                    latencies.lock().await.push(ms);
                }
                event_counts.lock().await[sub_id] += 1;
            }
            Err(e) => {
                warn!(sub_id, "S4: stream error: {e}");
                break;
            }
        }
    }
}

// ── Scenario 5: Slow-subscriber backpressure ──────────────────────────────────
//
// Goal: observe konfig's behavior when a fraction of subscribers cannot keep
// up with the broadcast rate. The broadcast channel has a finite capacity
// (`tokio::sync::broadcast`) so stalled receivers either:
//   - cause `RecvError::Lagged` on their stream (server drops them →
//     server-side warn + the client sees UNAVAILABLE / stream end),
//   - or back-pressure the broadcast send (delaying normal subs).
//
// We measure:
//   - replay-buffer high-water mark via konfig `/metrics`
//     (`konfig_replay_buffer_depth{namespace=...}`) — sampled every 200 ms.
//   - UNAVAILABLE / stream-error rate on the slow subs.
//   - missed events on the NORMAL subs (the population we actually care about).
//
// Hard accept: missed > 0 on normal subs is the failure signal. Stalled subs
// missing events is the expected behavior under backpressure.

const S5_NORMAL_SUBS: usize = 50;
const S5_SLOW_SUBS: usize = 5;
const S5_SLOW_RX_SLEEP_MS: u64 = 1000;
const S5_DRAIN_SECS: u64 = 60;
const S5_METRICS_POLL_MS: u64 = 200;

async fn scenario_backpressure(addr: &str, namespace: &str, config_name: &str) -> ScenarioResult {
    let s5_applies: u32 = env_u32("S1_APPLIES", 200);
    let s5_interval_ms: u64 = env_u64("S1_INTERVAL_MS", 100);
    // Derive metrics endpoint from the gRPC addr: same host, port 9090.
    // Falls back to None if the addr can't be parsed; in that case we record
    // "high-water not measured" in the report.
    let metrics_url = derive_metrics_url(addr);

    let normal_counts: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(vec![0u32; S5_NORMAL_SUBS]));
    let slow_counts: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(vec![0u32; S5_SLOW_SUBS]));
    let slow_errors: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
    let slow_unavailable: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
    let normal_errors: Arc<Mutex<u64>> = Arc::new(Mutex::new(0));
    let barrier = Arc::new(Barrier::new(S5_NORMAL_SUBS + S5_SLOW_SUBS + 1));

    // Seed.
    let start_seq = match seed_start_seq(addr, namespace, config_name).await {
        Ok(seq) => seq,
        Err(msg) => return ScenarioResult::fail("backpressure", vec![msg]),
    };
    let end_seq = start_seq + s5_applies - 1;

    // Spawn 50 normal subs.
    let mut normal_handles = Vec::with_capacity(S5_NORMAL_SUBS);
    for sub_id in 0..S5_NORMAL_SUBS {
        let h = tokio::spawn(s5_subscriber(
            sub_id,
            addr.to_owned(),
            namespace.to_owned(),
            config_name.to_owned(),
            start_seq,
            None, // normal subs: no rx sleep
            Arc::clone(&normal_counts),
            Arc::clone(&normal_errors),
            Arc::clone(&barrier),
            None,
        ));
        normal_handles.push(h);
    }

    // Spawn 5 slow subs.
    let mut slow_handles = Vec::with_capacity(S5_SLOW_SUBS);
    for sub_id in 0..S5_SLOW_SUBS {
        let h = tokio::spawn(s5_subscriber(
            sub_id,
            addr.to_owned(),
            namespace.to_owned(),
            config_name.to_owned(),
            start_seq,
            Some(S5_SLOW_RX_SLEEP_MS),
            Arc::clone(&slow_counts),
            Arc::clone(&slow_errors),
            Arc::clone(&barrier),
            Some(Arc::clone(&slow_unavailable)),
        ));
        slow_handles.push(h);
    }

    barrier.wait().await;
    info!(
        "S5: {} normal + {} slow subs connected — applying {} at {} ms interval",
        S5_NORMAL_SUBS, S5_SLOW_SUBS, s5_applies, s5_interval_ms
    );

    // Spawn metrics poller — samples replay-buffer depth every 200 ms.
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let poller = spawn_replay_buffer_poller(metrics_url.clone(), namespace.to_owned(), stop_rx);

    // Apply driver.
    let (n_applies_ok, n_applies_err) = match drive_applies(
        addr,
        namespace,
        config_name,
        start_seq,
        end_seq,
        s5_interval_ms,
        "backpressure",
    )
    .await
    {
        Ok(counts) => counts,
        Err(e) => {
            let _ = stop_tx.send(true);
            for h in normal_handles.into_iter().chain(slow_handles) {
                h.abort();
            }
            return ScenarioResult::fail("backpressure", vec![e]);
        }
    };

    let total_expected = S5_NORMAL_SUBS as u32 * n_applies_ok;
    info!(
        "S5: apply loop done ({n_applies_ok}/{} OK, {n_applies_err} err) — draining normal subs",
        s5_applies
    );

    // Drain — only require NORMAL subs to catch up. Slow subs are expected
    // to lag or be dropped.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(S5_DRAIN_SECS);
    loop {
        let received: u32 = normal_counts.lock().await.iter().sum();
        if received >= total_expected {
            info!("S5: all {total_expected} events drained on normal subs");
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            warn!(received, total_expected, "S5: drain timeout on normal subs");
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let _ = stop_tx.send(true);
    for h in normal_handles.into_iter().chain(slow_handles) {
        h.abort();
    }
    let (high_water, hw_samples, hw_errors) = poller.await.unwrap_or((0, 0, 0));

    let normal_received: u32 = normal_counts.lock().await.iter().sum();
    let slow_received: u32 = slow_counts.lock().await.iter().sum();
    let normal_missed = total_expected.saturating_sub(normal_received);
    let slow_err = *slow_errors.lock().await;
    let slow_unavail = *slow_unavailable.lock().await;
    let normal_err = *normal_errors.lock().await;
    let slow_expected = S5_SLOW_SUBS as u32 * n_applies_ok;
    let slow_missed = slow_expected.saturating_sub(slow_received);

    let high_water_repr = if metrics_url.is_none() {
        "n/a (no metrics endpoint derived from addr)".to_owned()
    } else if hw_samples == 0 {
        format!("n/a ({hw_errors} fetch errors)")
    } else {
        format!("{high_water} (samples={hw_samples}, errors={hw_errors})")
    };

    info!(
        normal_subs = S5_NORMAL_SUBS,
        slow_subs = S5_SLOW_SUBS,
        applies_ok = n_applies_ok,
        applies_err = n_applies_err,
        normal_received,
        normal_missed,
        normal_stream_errors = normal_err,
        slow_received,
        slow_missed,
        slow_stream_errors = slow_err,
        slow_unavailable = slow_unavail,
        replay_buffer_high_water = %high_water_repr,
        "S5 results"
    );

    // Acceptance:
    //   - normal subs must not miss events (broadcast capacity should
    //     absorb a 5/55 stalled fraction over a 20 s run).
    //   - apply RPCs must not error.
    //   - konfig must not crash (loadtest can't see crash directly —
    //     surfaces as connect-fail or zero applies).
    let mut failures = Vec::new();
    if n_applies_ok == 0 {
        failures.push("zero successful applies".into());
    }
    if normal_missed > 0 {
        failures.push(format!(
            "{normal_missed} missed events on normal subscribers"
        ));
    }
    if failures.is_empty() {
        ScenarioResult::pass("backpressure")
    } else {
        ScenarioResult::fail("backpressure", failures)
    }
}

#[allow(clippy::too_many_arguments)]
async fn s5_subscriber(
    sub_id: usize,
    addr: String,
    namespace: String,
    config_name: String,
    start_seq: u32,
    rx_sleep_ms: Option<u64>,
    event_counts: Arc<Mutex<Vec<u32>>>,
    stream_errors: Arc<Mutex<u64>>,
    barrier: Arc<Barrier>,
    unavailable_counter: Option<Arc<Mutex<u64>>>,
) {
    let ch = match connect(&addr).await {
        Ok(c) => c,
        Err(e) => {
            warn!(
                sub_id,
                slow = rx_sleep_ms.is_some(),
                "S5: connect failed: {e}"
            );
            barrier.wait().await;
            return;
        }
    };
    let mut client = KonfigServiceClient::new(ch);
    let stream = match client
        .subscribe(tonic::Request::new(SubscribeRequest {
            // Loadtest does not exercise server-side label filtering.
            label_selector: String::new(),
            namespace: namespace.clone(),
            names: vec![config_name.clone()],
            resume_resource_version: String::new(),
        }))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => {
            warn!(sub_id, "S5: subscribe failed: {e}");
            *stream_errors.lock().await += 1;
            barrier.wait().await;
            return;
        }
    };
    barrier.wait().await;

    let mut stream = stream;
    while let Some(item) = stream.next().await {
        match item {
            Ok(event) => {
                let version = event.config.as_ref().map(|c| c.schema_version).unwrap_or(0);
                if version < start_seq {
                    continue;
                }
                event_counts.lock().await[sub_id] += 1;
                if let Some(ms) = rx_sleep_ms {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                }
            }
            Err(status) => {
                let code = status.code();
                if code == tonic::Code::Unavailable
                    && let Some(c) = &unavailable_counter
                {
                    *c.lock().await += 1;
                }
                warn!(sub_id, code = ?code, "S5: stream error: {status}");
                *stream_errors.lock().await += 1;
                break;
            }
        }
    }
}

/// Derive `http://<host>:9090/metrics` from a gRPC addr like
/// `http://127.0.0.1:50051`. Returns None if the addr can't be parsed —
/// caller treats that as "high-water not measured".
fn derive_metrics_url(grpc_addr: &str) -> Option<String> {
    // Strip scheme.
    let rest = grpc_addr
        .strip_prefix("http://")
        .or_else(|| grpc_addr.strip_prefix("https://"))
        .unwrap_or(grpc_addr);
    // Cut at the first ':' or '/' to extract host.
    let host_end = rest.find([':', '/']).unwrap_or(rest.len());
    let host = &rest[..host_end];
    if host.is_empty() {
        return None;
    }
    Some(format!("http://{host}:9090/metrics"))
}

/// Fetch konfig `/metrics`, parse `konfig_replay_buffer_depth{namespace="..."}`,
/// return the gauge value as a u64.
async fn fetch_replay_buffer_depth(
    metrics_url: &str,
    namespace: &str,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    // Manual HTTP/1.1 GET — keeps the dep surface to tokio. The endpoint is
    // a small text body so we don't need a real HTTP client here.
    let host_port = metrics_url
        .strip_prefix("http://")
        .ok_or("metrics url must be http://")?;
    let host_port = host_port
        .split('/')
        .next()
        .ok_or("metrics url missing host")?;
    let mut stream =
        tokio::time::timeout(Duration::from_millis(500), TcpStream::connect(host_port)).await??;
    let req = format!("GET /metrics HTTP/1.1\r\nHost: {host_port}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await?;
    let mut buf = Vec::with_capacity(8192);
    tokio::time::timeout(Duration::from_millis(500), stream.read_to_end(&mut buf)).await??;
    let body = std::str::from_utf8(&buf)?;
    // Find the line: konfig_replay_buffer_depth{namespace="<ns>"} <value>
    let needle = format!("konfig_replay_buffer_depth{{namespace=\"{namespace}\"}}");
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix(&needle) {
            let val = rest.trim();
            // Gauge text format is a float; replay buffer depth is an integer
            // count, so truncate.
            let f: f64 = val.parse()?;
            return Ok(f as u64);
        }
    }
    Err("konfig_replay_buffer_depth not found in /metrics".into())
}
