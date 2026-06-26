//! Soak scenario (CU-86aj35zxw "added scope"): sustained mixed workload.
//!
//! Unlike the burst scenarios this is a *steady-state* run. Its only job is to
//! generate a realistic mixed workload for the full `--duration` window so an
//! external observer can watch the process under sustained load. Two such
//! observers ride the same soak window (the streaming capture's "natural host"
//! per the ticket):
//!
//!   * whole-process growth / drift — `heap-profile-eval` pprof snapshots +
//!     `tools/profiling/heap_delta.sh` (steady-state delta, startup excluded).
//!   * per-callsite allocation churn — the snmalloc stream-sink JSONL
//!     (`KONFIG_SNMALLOC_STREAM_PATH`, see `konfig::stream_sink`) post-processed
//!     with `snmalloc-tools rate-report --top 20`.
//!
//! Workload mix (all concurrent until the deadline):
//!   * long-lived Subscribe streams (config)  — `SOAK_CONFIG_SUBS`   (50)
//!   * long-lived SubscribeSecrets streams     — `SOAK_SECRET_SUBS`   (25)
//!   * periodic Apply         @ `SOAK_CONFIG_INTERVAL_MS`   (250 ms)
//!   * periodic ApplySecret   @ `SOAK_SECRET_INTERVAL_MS`   (1000 ms)
//!   * reconnect churn: `SOAK_RECONNECT_SUBS` (10) streams that connect, read
//!     for `SOAK_RECONNECT_INTERVAL_MS` (5000 ms), drop, and reconnect —
//!     exercises the per-Subscribe-stream channel-bookkeeping path that the
//!     snapshot heap profile flagged (~517 kB) and that streaming mode should
//!     surface as transient churn.
//!
//! Success is intentionally lenient ("the system kept serving across the whole
//! mix"). Steady-state RSS / heap growth and per-callsite rates are judged
//! out-of-band, not asserted here — a 10-30 min run with per-event bookkeeping
//! would itself leak the loadtest process.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures_util::StreamExt as _;
use tokio::time::Instant;
use tracing::info;
use tracing::warn;

use konfig::proto::konfig_service_client::KonfigServiceClient;
use konfig::proto::{
    ApplyRequest, ApplySecretRequest, GetRequest, GetSecretRequest, SubscribeRequest,
    SubscribeSecretsRequest,
};

use crate::args::{env_u64, env_usize};
use crate::client::connect;
use crate::metrics::ScenarioResult;

/// Default soak length when `--duration` is unset. 10 min is the documented
/// floor ("done when: a documented soak scenario runs >= 10 min").
const DEFAULT_DURATION_SECS: u64 = 600;

/// Post-apply quiesce so in-flight broadcasts land before the event tally.
const DRAIN_SECS: u64 = 10;

pub(crate) async fn scenario_soak(
    addr: &str,
    namespace: &str,
    config_name: &str,
    secret_name: &str,
    duration_secs: Option<u64>,
) -> ScenarioResult {
    let duration_secs = duration_secs.unwrap_or(DEFAULT_DURATION_SECS);
    let config_subs = env_usize("SOAK_CONFIG_SUBS", 50);
    let secret_subs = env_usize("SOAK_SECRET_SUBS", 25);
    let reconnect_subs = env_usize("SOAK_RECONNECT_SUBS", 10);
    let config_interval_ms = env_u64("SOAK_CONFIG_INTERVAL_MS", 250);
    let secret_interval_ms = env_u64("SOAK_SECRET_INTERVAL_MS", 1000);
    let reconnect_interval_ms = env_u64("SOAK_RECONNECT_INTERVAL_MS", 5000);

    info!(
        duration_secs,
        config_subs,
        secret_subs,
        reconnect_subs,
        config_interval_ms,
        secret_interval_ms,
        reconnect_interval_ms,
        "soak: starting sustained mixed workload"
    );

    let deadline = Instant::now() + Duration::from_secs(duration_secs);

    // Seed schema_versions so applies start strictly above pre-existing state.
    let config_seq0 = seed_config(addr, namespace, config_name).await;
    let secret_seq0 = seed_secret(addr, namespace, secret_name).await;

    let config_events = Arc::new(AtomicU64::new(0));
    let secret_events = Arc::new(AtomicU64::new(0));
    let reconnect_cycles = Arc::new(AtomicU64::new(0));
    let config_applies = Arc::new(AtomicU64::new(0));
    let secret_applies = Arc::new(AtomicU64::new(0));

    // Long-lived subscribers + reconnect churn run until aborted post-drain.
    let mut handles = Vec::new();
    for _ in 0..config_subs {
        handles.push(tokio::spawn(sub_config(
            addr.to_owned(),
            namespace.to_owned(),
            config_name.to_owned(),
            Arc::clone(&config_events),
        )));
    }
    for _ in 0..secret_subs {
        handles.push(tokio::spawn(sub_secret(
            addr.to_owned(),
            namespace.to_owned(),
            secret_name.to_owned(),
            Arc::clone(&secret_events),
        )));
    }
    for _ in 0..reconnect_subs {
        handles.push(tokio::spawn(reconnect_churn(
            addr.to_owned(),
            namespace.to_owned(),
            config_name.to_owned(),
            reconnect_interval_ms,
            deadline,
            Arc::clone(&reconnect_cycles),
        )));
    }

    // Apply loops self-terminate at the deadline; join them to drive the clock.
    let apply_cfg = tokio::spawn(apply_config_loop(
        addr.to_owned(),
        namespace.to_owned(),
        config_name.to_owned(),
        config_seq0,
        config_interval_ms,
        deadline,
        Arc::clone(&config_applies),
    ));
    let apply_sec = tokio::spawn(apply_secret_loop(
        addr.to_owned(),
        namespace.to_owned(),
        secret_name.to_owned(),
        secret_seq0,
        secret_interval_ms,
        deadline,
        Arc::clone(&secret_applies),
    ));
    let _ = apply_cfg.await;
    let _ = apply_sec.await;

    info!(
        config_applies = config_applies.load(Ordering::Relaxed),
        secret_applies = secret_applies.load(Ordering::Relaxed),
        "soak: apply loops done — draining for {DRAIN_SECS}s"
    );
    tokio::time::sleep(Duration::from_secs(DRAIN_SECS)).await;
    for h in &handles {
        h.abort();
    }

    let cfg_ev = config_events.load(Ordering::Relaxed);
    let sec_ev = secret_events.load(Ordering::Relaxed);
    let recon = reconnect_cycles.load(Ordering::Relaxed);
    let cfg_ap = config_applies.load(Ordering::Relaxed);
    let sec_ap = secret_applies.load(Ordering::Relaxed);
    info!(
        duration_secs,
        config_applies = cfg_ap,
        secret_applies = sec_ap,
        config_events = cfg_ev,
        secret_events = sec_ev,
        reconnect_cycles = recon,
        "soak results (steady-state; observe RSS/heap + rate-report externally)"
    );

    // Lenient gate: the mix must have served *something*. Growth/drift is
    // judged out-of-band (heap snapshots + delta, rate-report), not here.
    let mut failures = Vec::new();
    if cfg_ap == 0 {
        failures.push("zero successful config applies".into());
    }
    if sec_ap == 0 {
        failures.push("zero successful secret applies".into());
    }
    if cfg_ev == 0 {
        failures.push("config subscribers received zero events".into());
    }
    if sec_ev == 0 {
        failures.push("secret subscribers received zero events".into());
    }
    if failures.is_empty() {
        ScenarioResult::pass("soak")
    } else {
        ScenarioResult::fail("soak", failures)
    }
}

async fn seed_config(addr: &str, namespace: &str, config_name: &str) -> u32 {
    let ch = match connect(addr).await {
        Ok(c) => c,
        Err(_) => return 1,
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
}

async fn seed_secret(addr: &str, namespace: &str, secret_name: &str) -> u32 {
    let ch = match connect(addr).await {
        Ok(c) => c,
        Err(_) => return 1,
    };
    let mut client = KonfigServiceClient::new(ch);
    match client
        .get_secret(tonic::Request::new(GetSecretRequest {
            namespace: namespace.to_owned(),
            name: secret_name.to_owned(),
        }))
        .await
    {
        Ok(r) => r.into_inner().schema_version + 1,
        Err(_) => 1,
    }
}

async fn apply_config_loop(
    addr: String,
    namespace: String,
    config_name: String,
    seq0: u32,
    interval_ms: u64,
    deadline: Instant,
    counter: Arc<AtomicU64>,
) {
    let ch = match connect(&addr).await {
        Ok(c) => c,
        Err(e) => {
            warn!("soak: config apply connect failed: {e}");
            return;
        }
    };
    let mut driver = KonfigServiceClient::new(ch);
    let mut seq = seq0;
    while Instant::now() < deadline {
        let yaml =
            format!("schema_version: {seq}\ncontent:\n  iteration: {seq}\n  scenario: soak\n");
        match driver
            .apply(tonic::Request::new(ApplyRequest {
                namespace: namespace.clone(),
                name: config_name.clone(),
                yaml_content: yaml,
            }))
            .await
        {
            Ok(_) => {
                counter.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => warn!(seq, "soak: Apply failed: {e}"),
        }
        seq = seq.wrapping_add(1);
        tokio::time::sleep(Duration::from_millis(interval_ms)).await;
    }
}

async fn apply_secret_loop(
    addr: String,
    namespace: String,
    secret_name: String,
    seq0: u32,
    interval_ms: u64,
    deadline: Instant,
    counter: Arc<AtomicU64>,
) {
    let ch = match connect(&addr).await {
        Ok(c) => c,
        Err(e) => {
            warn!("soak: secret apply connect failed: {e}");
            return;
        }
    };
    let mut driver = KonfigServiceClient::new(ch);
    let mut seq = seq0;
    while Instant::now() < deadline {
        let yaml = format!("schema_version: {seq}\ntoken: soak-secret-{seq}\n");
        match driver
            .apply_secret(tonic::Request::new(ApplySecretRequest {
                namespace: namespace.clone(),
                name: secret_name.clone(),
                yaml_content: yaml,
            }))
            .await
        {
            Ok(_) => {
                counter.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => warn!(seq, "soak: ApplySecret failed: {e}"),
        }
        seq = seq.wrapping_add(1);
        tokio::time::sleep(Duration::from_millis(interval_ms)).await;
    }
}

async fn sub_config(addr: String, namespace: String, config_name: String, counter: Arc<AtomicU64>) {
    let ch = match connect(&addr).await {
        Ok(c) => c,
        Err(e) => {
            warn!("soak: config subscribe connect failed: {e}");
            return;
        }
    };
    let mut client = KonfigServiceClient::new(ch);
    let mut stream = match client
        .subscribe(tonic::Request::new(SubscribeRequest {
            label_selector: String::new(),
            namespace,
            names: vec![config_name],
            resume_resource_version: String::new(),
        }))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => {
            warn!("soak: subscribe failed: {e}");
            return;
        }
    };
    while let Some(item) = stream.next().await {
        match item {
            Ok(_) => {
                counter.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                warn!("soak: config stream error: {e}");
                break;
            }
        }
    }
}

async fn sub_secret(addr: String, namespace: String, secret_name: String, counter: Arc<AtomicU64>) {
    let ch = match connect(&addr).await {
        Ok(c) => c,
        Err(e) => {
            warn!("soak: secret subscribe connect failed: {e}");
            return;
        }
    };
    let mut client = KonfigServiceClient::new(ch);
    let mut stream = match client
        .subscribe_secrets(tonic::Request::new(SubscribeSecretsRequest {
            namespace,
            names: vec![secret_name],
            resume_resource_version: String::new(),
        }))
        .await
    {
        Ok(r) => r.into_inner(),
        Err(e) => {
            warn!("soak: subscribe_secrets failed: {e}");
            return;
        }
    };
    while let Some(item) = stream.next().await {
        match item {
            Ok(_) => {
                counter.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => {
                warn!("soak: secret stream error: {e}");
                break;
            }
        }
    }
}

async fn reconnect_churn(
    addr: String,
    namespace: String,
    config_name: String,
    window_ms: u64,
    deadline: Instant,
    cycles: Arc<AtomicU64>,
) {
    while Instant::now() < deadline {
        let ch = match connect(&addr).await {
            Ok(c) => c,
            Err(e) => {
                warn!("soak: reconnect connect failed: {e}");
                tokio::time::sleep(Duration::from_millis(window_ms)).await;
                continue;
            }
        };
        let mut client = KonfigServiceClient::new(ch);
        let mut stream = match client
            .subscribe(tonic::Request::new(SubscribeRequest {
                label_selector: String::new(),
                namespace: namespace.clone(),
                names: vec![config_name.clone()],
                resume_resource_version: String::new(),
            }))
            .await
        {
            Ok(r) => r.into_inner(),
            Err(e) => {
                warn!("soak: reconnect subscribe failed: {e}");
                tokio::time::sleep(Duration::from_millis(window_ms)).await;
                continue;
            }
        };
        cycles.fetch_add(1, Ordering::Relaxed);

        // Read for the window, then drop the stream so the server tears down
        // the per-Subscribe-stream bookkeeping — the allocation site we want
        // streaming mode to attribute as transient churn.
        let window_end = Instant::now() + Duration::from_millis(window_ms);
        loop {
            tokio::select! {
                item = stream.next() => {
                    match item {
                        Some(Ok(_)) => {}
                        _ => break,
                    }
                }
                _ = tokio::time::sleep_until(window_end) => break,
            }
        }
    }
}
