//! Scenario 2: Get flood.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Mutex;
use tracing::info;
use tracing::warn;

use konfig::proto::GetRequest;
use konfig::proto::konfig_service_client::KonfigServiceClient;

use crate::client::connect;
use crate::metrics::LatencyMetrics;
use crate::metrics::ScenarioResult;
use crate::metrics::Stats;

// ── Scenario 2: Get flood ─────────────────────────────────────────────────────

const S2_TASKS: usize = 50;
const S2_GETS_PER_TASK: usize = 100;
const S2_P99_LIMIT_MS: u128 = 50;

pub(crate) async fn scenario_get_flood(
    addr: &str,
    namespace: &str,
    config_name: &str,
) -> ScenarioResult {
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
