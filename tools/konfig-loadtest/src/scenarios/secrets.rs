//! Scenario 4: SubscribeSecrets flood.

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use futures_util::StreamExt as _;
use tokio::sync::Barrier;
use tokio::sync::Mutex;
use tracing::info;
use tracing::warn;

use konfig::proto::konfig_service_client::KonfigServiceClient;
use konfig::proto::{ApplySecretRequest, GetSecretRequest, SubscribeSecretsRequest};

use crate::client::connect;
use crate::metrics::LatencyMetrics;
use crate::metrics::ScenarioResult;
use crate::metrics::Stats;

// ── Scenario 4: SubscribeSecrets flood ────────────────────────────────────────

const S4_APPLIES: u32 = 20;
const S4_SUBSCRIBERS: usize = 50;
const S4_INTERVAL_MS: u64 = 100;
const S4_DRAIN_SECS: u64 = 15;
const S4_P99_LIMIT_MS: u128 = 500;

pub(crate) async fn scenario_secrets_flood(
    addr: &str,
    namespace: &str,
    secret_name: &str,
) -> ScenarioResult {
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
