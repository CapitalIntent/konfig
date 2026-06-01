//! konfig-loadtest — 4-scenario gRPC stress test for Konfig.
//!
//! Profiling stack:
//!   tracing                — structured spans/events
//!
//! Scenarios:
//!   1. subscribe_flood  — 100 subscribers + 200 applies at 100 ms intervals, p99 < 500ms
//!   2. get_flood        — 50 concurrent tasks × 100 Get RPCs, p99 < 50ms
//!   3. reconnect_storm  — 50 subscribers disconnected + resumed with RV
//!   4. secrets_flood    — 20 ApplySecret + 50 SubscribeSecrets streams, p99 < 500ms

use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use futures_util::StreamExt as _;
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
    /// Which scenario to run: all | subscribe | get | reconnect | secrets
    #[arg(long, default_value = "all")]
    scenario: String,
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
}

impl ScenarioResult {
    fn pass(name: &'static str) -> Self {
        Self {
            name,
            pass: true,
            failures: Vec::new(),
        }
    }

    fn fail(name: &'static str, failures: Vec<String>) -> Self {
        Self {
            name,
            pass: false,
            failures,
        }
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
        "konfig-loadtest starting"
    );

    let run_all = args.scenario == "all";
    let mut results: Vec<ScenarioResult> = Vec::new();

    if run_all || args.scenario == "subscribe" {
        info!("=== Scenario 1: Subscribe flood + rapid apply ===");
        results
            .push(scenario_subscribe_flood(&args.addr, &args.namespace, &args.config_name).await);
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

    if any_fail {
        std::process::exit(1);
    }
    info!("konfig-loadtest ALL PASSED");
    Ok(())
}

// ── Scenario 1: Subscribe flood + rapid apply ─────────────────────────────────

const S1_SUBSCRIBERS: usize = 100;
const S1_APPLIES: u32 = 200;
const S1_INTERVAL_MS: u64 = 100;
const S1_DRAIN_SECS: u64 = 30;
const S1_P99_LIMIT_MS: u128 = 500;

async fn scenario_subscribe_flood(
    addr: &str,
    namespace: &str,
    config_name: &str,
) -> ScenarioResult {
    // Shared state.
    let latencies: Arc<Mutex<Stats>> = Arc::new(Mutex::new(Stats::new()));
    let event_counts: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(vec![0u32; S1_SUBSCRIBERS]));
    let apply_timestamps: Arc<Mutex<Vec<Option<Instant>>>> =
        Arc::new(Mutex::new(vec![None; S1_APPLIES as usize]));
    let successful_applies: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
    let barrier = Arc::new(Barrier::new(S1_SUBSCRIBERS + 1));

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
    let end_seq = start_seq + S1_APPLIES - 1;

    // Spawn 100 subscribers.
    let mut sub_handles = Vec::with_capacity(S1_SUBSCRIBERS);
    for sub_id in 0..S1_SUBSCRIBERS {
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
        S1_SUBSCRIBERS, S1_INTERVAL_MS
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
            tokio::time::sleep(Duration::from_millis(S1_INTERVAL_MS)).await;
        }
    }

    let n_ok = *successful_applies.lock().await;
    let total_expected = S1_SUBSCRIBERS as u32 * n_ok;
    info!(
        "S1: apply loop done ({n_ok}/{} succeeded) — draining",
        S1_APPLIES
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
    if p99 >= S1_P99_LIMIT_MS {
        failures.push(format!("p99 {p99} ms >= gate {S1_P99_LIMIT_MS} ms"));
    }
    if missed > 0 {
        failures.push(format!("{missed} missed events"));
    }

    if failures.is_empty() {
        ScenarioResult::pass("subscribe_flood")
    } else {
        ScenarioResult::fail("subscribe_flood", failures)
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

    if failures.is_empty() {
        ScenarioResult::pass("get_flood")
    } else {
        ScenarioResult::fail("get_flood", failures)
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
const S3_WARM_DRAIN_SECS: u64 = 5;
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
    loop {
        let populated = last_rvs
            .lock()
            .await
            .iter()
            .filter(|rv| !rv.is_empty())
            .count();
        if populated >= S3_WARM_RV_QUORUM {
            break;
        }
        if tokio::time::Instant::now() >= drain_deadline {
            warn!(
                populated,
                quorum = S3_WARM_RV_QUORUM,
                "S3: warm drain timeout — fewer subscribers have RV than quorum"
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

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

    if missed > 0 {
        ScenarioResult::fail(
            "reconnect_storm",
            vec![format!("{missed} missed events post-reconnect")],
        )
    } else {
        ScenarioResult::pass("reconnect_storm")
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

    if failures.is_empty() {
        ScenarioResult::pass("secrets_flood")
    } else {
        ScenarioResult::fail("secrets_flood", failures)
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
