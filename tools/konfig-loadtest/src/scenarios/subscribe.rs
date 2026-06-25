//! Scenario 1: subscribe flood + rapid apply (and sustained soak variant).

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use futures_util::StreamExt as _;
use tokio::sync::Barrier;
use tokio::sync::Mutex;
use tracing::info;
use tracing::warn;

use konfig::proto::konfig_service_client::KonfigServiceClient;
use konfig::proto::{ApplyRequest, GetRequest, SubscribeRequest};

use crate::args::env_u32;
use crate::args::env_u64;
use crate::args::env_u128;
use crate::args::env_usize;
use crate::client::connect;
use crate::metrics::LatencyMetrics;
use crate::metrics::ScenarioResult;
use crate::metrics::Stats;

// ── Scenario 1: Subscribe flood + rapid apply ─────────────────────────────────

const S1_DRAIN_SECS: u64 = 30;
const S1_P99_LIMIT_MS: u128 = 500;

pub(crate) async fn scenario_subscribe_flood(
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
