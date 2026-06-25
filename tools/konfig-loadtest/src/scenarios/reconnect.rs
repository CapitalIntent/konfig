//! Scenario 3: reconnect storm (replay buffer resume).

use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use futures_util::StreamExt as _;
use tokio::sync::Barrier;
use tokio::sync::Mutex;
use tracing::error;
use tracing::info;
use tracing::warn;

use konfig::proto::konfig_service_client::KonfigServiceClient;
use konfig::proto::{ApplyRequest, GetRequest, SubscribeRequest};

use crate::client::connect;
use crate::metrics::ScenarioResult;

// ── Scenario 3: Reconnect storm (replay buffer) ───────────────────────────────

const S3_SUBSCRIBERS: usize = 50;
const S3_WARM_APPLIES: u32 = 5;
const S3_POST_APPLIES: u32 = 10;
const S3_INTERVAL_MS: u64 = 100;
const S3_DRAIN_SECS: u64 = 15;
const S3_WARM_DRAIN_SECS: u64 = 1;
const S3_WARM_RV_QUORUM: usize = S3_SUBSCRIBERS;

pub(crate) async fn scenario_reconnect_storm(
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
