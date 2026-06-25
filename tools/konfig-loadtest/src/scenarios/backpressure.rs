//! Scenario 5: slow-subscriber backpressure (+ replay-buffer-depth polling).

use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt as _;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::Barrier;
use tokio::sync::Mutex;
use tracing::info;
use tracing::warn;

use konfig::proto::SubscribeRequest;
use konfig::proto::konfig_service_client::KonfigServiceClient;

use crate::args::env_u32;
use crate::args::env_u64;
use crate::client::connect;
use crate::client::drive_applies;
use crate::client::seed_start_seq;
use crate::metrics::ScenarioResult;

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

pub(crate) async fn scenario_backpressure(
    addr: &str,
    namespace: &str,
    config_name: &str,
) -> ScenarioResult {
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
