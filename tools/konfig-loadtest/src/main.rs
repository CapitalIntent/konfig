//! konfig-loadtest — gRPC load test for Konfig Phase 5.
//!
//! Profiling stack:
//!   dial9-tokio-telemetry — nanosecond runtime traces → `dial9 serve --local-dir /tmp/dial9`
//!   tokio-console          — live task inspector       → `tokio-console`
//!   tracing                — structured spans/events

use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use futures_util::StreamExt as _;
use tokio::sync::{Barrier, Mutex};
use tonic::transport::Channel;
use tracing::{error, info, instrument, warn};
use tracing_subscriber::prelude::*;

use konfig::proto::konfig_service_client::KonfigServiceClient;
use konfig::proto::{ApplyRequest, GetRequest, SubscribeRequest};

// ── Constants ─────────────────────────────────────────────────────────────────

const N_SUBSCRIBERS: usize = 100;
const APPLY_COUNT: u32 = 100;
const APPLY_INTERVAL_MS: u64 = 6_000;
const DRAIN_TIMEOUT_SECS: u64 = 30;
const DRAIN_POLL_MS: u64 = 100;
const P99_LIMIT_MS: u128 = 1_000;
/// Max reconnect attempts before a subscriber gives up.
const MAX_RECONNECT_ATTEMPTS: usize = 5;
/// Base backoff before reconnecting (doubles each attempt, capped at 2s).
const RECONNECT_BASE_MS: u64 = 100;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "konfig-loadtest")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    addr: String,
    #[arg(long, default_value = "default")]
    namespace: String,
    #[arg(long, default_value = "coinbase-trading")]
    config_name: String,
}

fn telemetry_config() -> dial9_tokio_telemetry::Dial9Config {
    dial9_tokio_telemetry::Dial9Config::from_env()
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[dial9_tokio_telemetry::main(config = telemetry_config)]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (console_layer, console_server) = console_subscriber::ConsoleLayer::builder()
        .with_default_env()
        .build();

    tracing_subscriber::registry()
        .with(console_layer)
        .with(dial9_tokio_telemetry::tracing_layer::Dial9TokioLayer::new())
        .with(
            tracing_subscriber::fmt::layer().with_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive("konfig_loadtest=info".parse()?)
                    .add_directive("konfig=info".parse()?),
            ),
        )
        .init();

    tokio::spawn(console_server.serve());

    let args = Args::parse();

    let channel = Channel::from_shared(args.addr.clone())
        .expect("valid URI")
        .connect()
        .await
        .map_err(|e| {
            error!("Failed to connect: {e}");
            e
        })?;

    let mut driver = KonfigServiceClient::new(channel.clone());

    // Query current schema_version so applies always start above the current value.
    let current_version = match driver
        .get(tonic::Request::new(GetRequest {
            namespace: args.namespace.clone(),
            name: args.config_name.clone(),
        }))
        .await
    {
        Ok(resp) => resp.into_inner().schema_version,
        Err(e) => {
            warn!("Get failed ({e}) — assuming schema_version = 0");
            0
        }
    };
    let start_seq = current_version + 1;
    let end_seq = start_seq + APPLY_COUNT - 1;

    info!(
        addr = %args.addr,
        namespace = %args.namespace,
        config_name = %args.config_name,
        subscribers = N_SUBSCRIBERS,
        applies = APPLY_COUNT,
        start_seq,
        "konfig-loadtest starting"
    );

    // ── Shared state ──────────────────────────────────────────────────────────

    let latencies: Arc<Mutex<Vec<u128>>> = Arc::new(Mutex::new(Vec::new()));
    let event_counts: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(vec![0u32; N_SUBSCRIBERS]));

    // Fix 1: indexed by (schema_version - start_seq) so each subscriber can
    // look up the exact Apply return time for the event it just received.
    let apply_timestamps: Arc<Mutex<Vec<Option<Instant>>>> =
        Arc::new(Mutex::new(vec![None; APPLY_COUNT as usize]));

    let successful_applies: Arc<Mutex<u32>> = Arc::new(Mutex::new(0));
    let barrier = Arc::new(Barrier::new(N_SUBSCRIBERS + 1));

    // ── Spawn subscribers ─────────────────────────────────────────────────────

    let mut sub_handles = Vec::with_capacity(N_SUBSCRIBERS);
    for sub_id in 0..N_SUBSCRIBERS {
        let handle = tokio::spawn(run_subscriber(
            sub_id,
            args.addr.clone(),
            args.namespace.clone(),
            args.config_name.clone(),
            start_seq,
            Arc::clone(&latencies),
            Arc::clone(&event_counts),
            Arc::clone(&apply_timestamps),
            Arc::clone(&barrier),
        ));
        sub_handles.push(handle);
    }

    barrier.wait().await;
    info!(
        "All {} subscribers connected — starting Apply loop",
        N_SUBSCRIBERS
    );

    // ── Apply loop ────────────────────────────────────────────────────────────

    for seq in start_seq..=end_seq {
        let yaml =
            format!("schema_version: {seq}\ncontent:\n  iteration: {seq}\n  load_test: true\n");
        let req = ApplyRequest {
            namespace: args.namespace.clone(),
            name: args.config_name.clone(),
            yaml_content: yaml,
        };

        let apply_result = {
            use tracing::Instrument as _;
            driver
                .apply(tonic::Request::new(req))
                .instrument(tracing::info_span!("apply", seq))
                .await
        };

        match apply_result {
            Ok(_) => {
                // Fix 1: store at index (seq - start_seq) for O(1) per-event lookup.
                let idx = (seq - start_seq) as usize;
                apply_timestamps.lock().await[idx] = Some(Instant::now());
                *successful_applies.lock().await += 1;
                info!(seq, "Apply RPC returned");
            }
            Err(e) => {
                warn!(seq, "Apply RPC failed: {e}");
                // Leave the slot None so subscribers skip latency for this apply.
            }
        }

        if seq < end_seq {
            tokio::time::sleep(Duration::from_millis(APPLY_INTERVAL_MS)).await;
        }
    }

    let n_successful = *successful_applies.lock().await;
    let total_expected = N_SUBSCRIBERS as u32 * n_successful;
    info!(
        "All {} Apply RPCs done ({n_successful} succeeded) — draining (up to {DRAIN_TIMEOUT_SECS}s)",
        APPLY_COUNT
    );

    let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(DRAIN_TIMEOUT_SECS);
    loop {
        let received: u32 = event_counts.lock().await.iter().sum();
        if received >= total_expected {
            info!("All {total_expected} events drained");
            break;
        }
        if tokio::time::Instant::now() >= drain_deadline {
            warn!(received, total_expected, "Drain timeout");
            break;
        }
        tokio::time::sleep(Duration::from_millis(DRAIN_POLL_MS)).await;
    }

    for h in sub_handles {
        h.abort();
    }

    // ── Report ────────────────────────────────────────────────────────────────

    let lat = latencies.lock().await;
    let counts = event_counts.lock().await;

    if lat.is_empty() {
        error!("No latency samples — did subscribers connect?");
        std::process::exit(1);
    }

    let mut sorted = lat.clone();
    sorted.sort_unstable();
    let n = sorted.len();
    let p50 = sorted[n / 2];
    let p99 = sorted[(n as f64 * 0.99) as usize];
    let max = *sorted.last().unwrap();

    let total_received: u32 = counts.iter().sum();
    let missed = total_expected.saturating_sub(total_received);

    info!(
        samples = n,
        p50_ms = p50,
        p99_ms = p99,
        max_ms = max,
        total_expected,
        total_received,
        missed,
        "Load test results"
    );

    let mut pass = true;
    if p99 >= P99_LIMIT_MS {
        error!("FAIL: p99 {p99} ms >= {P99_LIMIT_MS} ms");
        pass = false;
    } else {
        info!("PASS: p99 {p99} ms < {P99_LIMIT_MS} ms");
    }
    if missed > 0 {
        error!("FAIL: {missed} missed events");
        pass = false;
    } else {
        info!("PASS: zero missed events");
    }

    if !pass {
        std::process::exit(1);
    }
    info!("konfig-loadtest PASSED");
    Ok(())
}

// ── Subscriber task ───────────────────────────────────────────────────────────

/// A subscriber that reconnects on stream error with exponential backoff.
///
/// Fix 2: reconnect loop prevents missed events from transient stream errors
/// (Docker Desktop network blips, brief API server unavailability).
/// Fix 1: per-event latency uses indexed timestamp lookup by schema_version.
#[allow(clippy::too_many_arguments)]
#[instrument(skip_all, fields(sub_id))]
async fn run_subscriber(
    sub_id: usize,
    addr: String,
    namespace: String,
    config_name: String,
    start_seq: u32,
    latencies: Arc<Mutex<Vec<u128>>>,
    event_counts: Arc<Mutex<Vec<u32>>>,
    apply_timestamps: Arc<Mutex<Vec<Option<Instant>>>>,
    barrier: Arc<Barrier>,
) {
    let channel = match Channel::from_shared(addr.clone())
        .expect("valid URI")
        .connect()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            warn!(sub_id, "Failed to connect: {e}");
            barrier.wait().await;
            return;
        }
    };

    let mut client = KonfigServiceClient::new(channel);
    let req = SubscribeRequest {
        namespace: namespace.clone(),
        names: vec![config_name.clone()],
        resume_resource_version: String::new(),
    };

    // Establish initial stream and signal the barrier.
    let mut stream = match client.subscribe(tonic::Request::new(req.clone())).await {
        Ok(r) => r.into_inner(),
        Err(e) => {
            warn!(sub_id, "Subscribe failed: {e}");
            barrier.wait().await;
            return;
        }
    };

    // Signal: this subscriber's stream is live.
    barrier.wait().await;

    // Fix 2: track resume_resource_version for reconnects.
    let mut last_resource_version = String::new();
    let mut reconnect_attempt = 0usize;

    loop {
        while let Some(item) = stream.next().await {
            let received_at = Instant::now();
            match item {
                Ok(event) => {
                    reconnect_attempt = 0; // reset on successful event
                    let version = event.config.as_ref().map(|c| c.schema_version).unwrap_or(0);
                    if let Some(rv) = event.config.as_ref().map(|c| &c.resource_version) {
                        last_resource_version = rv.clone();
                    }

                    // Only measure/count events from our apply loop.
                    if version < start_seq {
                        continue;
                    }

                    // Fix 1: precise latency — look up the exact Apply return time.
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
                    warn!(sub_id, "Stream error: {e} — reconnecting");
                    break;
                }
            }
        }

        // Fix 2: reconnect with exponential backoff.
        reconnect_attempt += 1;
        if reconnect_attempt > MAX_RECONNECT_ATTEMPTS {
            warn!(
                sub_id,
                reconnect_attempt, "Max reconnects exceeded — subscriber stopping"
            );
            break;
        }

        let delay_ms = (RECONNECT_BASE_MS << reconnect_attempt.min(4)).min(2_000);
        warn!(
            sub_id,
            reconnect_attempt, delay_ms, "Reconnecting after stream error"
        );
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;

        let resume_req = SubscribeRequest {
            namespace: namespace.clone(),
            names: vec![config_name.clone()],
            resume_resource_version: last_resource_version.clone(),
        };
        stream = match client.subscribe(tonic::Request::new(resume_req)).await {
            Ok(r) => r.into_inner(),
            Err(e) => {
                warn!(sub_id, "Reconnect Subscribe failed: {e}");
                continue; // try again next iteration
            }
        };
        info!(sub_id, reconnect_attempt, "Reconnected successfully");
    }
}
