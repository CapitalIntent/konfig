//! konfig-loadtest — gRPC load test for Konfig Phase 5.
//!
//! # What this does
//!
//! 1. Spawns `N_SUBSCRIBERS` (100) concurrent `Subscribe` streams, each in its
//!    own tokio task.
//! 2. Drives `APPLY_COUNT` (100) `Apply` RPCs over `APPLY_DURATION_SECS` (600)
//!    seconds at roughly 10/min.
//! 3. Each subscriber records the wall-clock timestamp when a `ConfigEvent` is
//!    received. The driver records the timestamp when the `Apply` RPC returns.
//!    Delivery latency = event_received_at − apply_returned_at.
//! 4. After all applies complete, wait up to 5 s for in-flight events, then
//!    print the p50/p99/max latency report and assert:
//!      - p99 < 1 000 ms
//!      - zero missed events (every subscriber received every apply)
//!
//! # Usage
//!
//! ```
//! konfig-loadtest --addr http://127.0.0.1:50051 --namespace default \
//!                 --config-name loadtest
//! ```
//!
//! Requires a running kind cluster with the Konfig server and the
//! `loadtest` Config CRD pre-created (schema_version = 0):
//!
//! ```yaml
//! apiVersion: konfig.io/v1
//! kind: Config
//! metadata:
//!   name: loadtest
//!   namespace: default
//! spec:
//!   schema_version: 0
//!   content: {}
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use futures_util::StreamExt as _;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::{error, info, warn};

use konfig::proto::konfig_service_client::KonfigServiceClient;
use konfig::proto::{ApplyRequest, SubscribeRequest};

// ── Constants ─────────────────────────────────────────────────────────────────

const N_SUBSCRIBERS: usize = 100;
const APPLY_COUNT: u32 = 100;
const APPLY_INTERVAL_MS: u64 = 6_000; // 100 applies over 600 s ≈ 10/min
const DRAIN_WAIT_SECS: u64 = 5;
const P99_LIMIT_MS: u128 = 1_000;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "konfig-loadtest", about = "Konfig gRPC load test (100 subscribers, 100 applies)")]
struct Args {
    /// gRPC server address, e.g. http://127.0.0.1:50051
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    addr: String,

    /// Namespace where the Config CRD lives
    #[arg(long, default_value = "default")]
    namespace: String,

    /// Config name (must exist with schema_version = 0 before running)
    #[arg(long, default_value = "coinbase-trading")]
    config_name: String,
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
        subscribers = N_SUBSCRIBERS,
        applies = APPLY_COUNT,
        "konfig-loadtest starting"
    );

    // Each entry: (apply_sequence, delivery_latency_ms).
    // Each subscriber appends its observed latency for every apply it sees.
    let latencies: Arc<Mutex<Vec<u128>>> = Arc::new(Mutex::new(Vec::new()));

    // Per-subscriber event counters.
    let event_counts: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(vec![0u32; N_SUBSCRIBERS]));

    // apply_timestamps[seq] = Instant when Apply RPC returned.
    let apply_timestamps: Arc<Mutex<Vec<Instant>>> =
        Arc::new(Mutex::new(Vec::with_capacity(APPLY_COUNT as usize)));

    // ── Spawn subscribers ─────────────────────────────────────────────────────
    let mut sub_handles = Vec::with_capacity(N_SUBSCRIBERS);

    for sub_id in 0..N_SUBSCRIBERS {
        let addr = args.addr.clone();
        let namespace = args.namespace.clone();
        let config_name = args.config_name.clone();
        let latencies_clone = Arc::clone(&latencies);
        let event_counts_clone = Arc::clone(&event_counts);
        let apply_ts_clone = Arc::clone(&apply_timestamps);

        let handle = tokio::spawn(async move {
            run_subscriber(
                sub_id,
                addr,
                namespace,
                config_name,
                latencies_clone,
                event_counts_clone,
                apply_ts_clone,
            )
            .await;
        });
        sub_handles.push(handle);
    }

    // Brief pause so all subscribers have connected before we start applying.
    tokio::time::sleep(Duration::from_millis(500)).await;
    info!("All {} subscribers spawned — starting Apply loop", N_SUBSCRIBERS);

    // ── Apply loop ────────────────────────────────────────────────────────────
    let channel = match Channel::from_shared(args.addr.clone())
        .expect("valid URI")
        .connect()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to connect to konfig server at {}: {e}", args.addr);
            return Err(e.into());
        }
    };

    let mut client = KonfigServiceClient::new(channel);

    for seq in 1u32..=APPLY_COUNT {
        let yaml_content = format!(
            "schema_version: {seq}\ncontent:\n  iteration: {seq}\n  load_test: true\n"
        );

        let req = ApplyRequest {
            namespace: args.namespace.clone(),
            name: args.config_name.clone(),
            yaml_content,
        };

        match client.apply(tonic::Request::new(req)).await {
            Ok(_resp) => {
                let returned_at = Instant::now();
                apply_timestamps.lock().await.push(returned_at);
                info!(seq, "Apply RPC returned");
            }
            Err(e) => {
                warn!(seq, "Apply RPC failed: {e}");
                // Still push a sentinel so sequence numbering stays correct.
                apply_timestamps.lock().await.push(Instant::now());
            }
        }

        if seq < APPLY_COUNT {
            tokio::time::sleep(Duration::from_millis(APPLY_INTERVAL_MS)).await;
        }
    }

    info!("All {} Apply RPCs completed — waiting {}s for in-flight events", APPLY_COUNT, DRAIN_WAIT_SECS);
    tokio::time::sleep(Duration::from_secs(DRAIN_WAIT_SECS)).await;

    // Cancel subscriber tasks.
    for h in sub_handles {
        h.abort();
    }

    // ── Latency report ────────────────────────────────────────────────────────
    let lat = latencies.lock().await;
    let counts = event_counts.lock().await;

    if lat.is_empty() {
        error!("No delivery latency samples collected — did subscribers connect?");
        std::process::exit(1);
    }

    let mut sorted = lat.clone();
    sorted.sort_unstable();
    let n = sorted.len();
    let p50 = sorted[n / 2];
    let p99 = sorted[(n as f64 * 0.99) as usize];
    let max = *sorted.last().unwrap();

    let total_expected = N_SUBSCRIBERS as u32 * APPLY_COUNT;
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

    // ── Assertions ────────────────────────────────────────────────────────────
    let mut pass = true;

    if p99 >= P99_LIMIT_MS {
        error!("FAIL: p99 delivery latency {} ms >= {} ms limit", p99, P99_LIMIT_MS);
        pass = false;
    } else {
        info!("PASS: p99 delivery latency {} ms < {} ms limit", p99, P99_LIMIT_MS);
    }

    if missed > 0 {
        error!("FAIL: {} missed events across all subscribers", missed);
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

/// A single Subscribe stream. Records a delivery latency sample for each event
/// by comparing event receipt time against the corresponding Apply return time.
async fn run_subscriber(
    sub_id: usize,
    addr: String,
    namespace: String,
    config_name: String,
    latencies: Arc<Mutex<Vec<u128>>>,
    event_counts: Arc<Mutex<Vec<u32>>>,
    apply_timestamps: Arc<Mutex<Vec<Instant>>>,
) {
    let channel = match Channel::from_shared(addr.clone())
        .expect("valid URI")
        .connect()
        .await
    {
        Ok(c) => c,
        Err(e) => {
            warn!(sub_id, "Subscriber failed to connect: {e}");
            return;
        }
    };

    let mut client = KonfigServiceClient::new(channel);

    let req = SubscribeRequest {
        namespace,
        names: vec![config_name],
        resume_resource_version: String::new(),
    };

    let mut stream = match client.subscribe(tonic::Request::new(req)).await {
        Ok(resp) => resp.into_inner(),
        Err(e) => {
            warn!(sub_id, "Subscribe RPC failed: {e}");
            return;
        }
    };

    while let Some(item) = stream.next().await {
        let received_at = Instant::now();
        match item {
            Ok(_event) => {
                // Find the most recent Apply timestamp to compute delivery lag.
                let lag_ms = {
                    let ts = apply_timestamps.lock().await;
                    ts.last().map(|t| received_at.saturating_duration_since(*t).as_millis())
                };

                if let Some(ms) = lag_ms {
                    latencies.lock().await.push(ms);
                }

                let mut counts = event_counts.lock().await;
                counts[sub_id] += 1;
            }
            Err(e) => {
                warn!(sub_id, "Subscriber stream error: {e}");
                break;
            }
        }
    }
}
