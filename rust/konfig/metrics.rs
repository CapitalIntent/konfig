//! Prometheus metrics for the konfig service.
//!
//! All metrics are registered in the default Prometheus registry at startup
//! via `lazy_static!`.  The `/metrics` HTTP endpoint (port 9090) in `main.rs`
//! calls `prometheus::gather()` to serialise them for scraping.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use dashmap::DashMap;
use prometheus::{
    Counter, CounterVec, GaugeVec, Histogram, HistogramVec, register_counter, register_counter_vec,
    register_gauge_vec, register_histogram, register_histogram_vec,
};

/// Latency buckets for Apply and Get RPC handlers, in seconds.
/// Covers sub-ms cache hits up to multi-second kube round-trips.
const RPC_LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Latency buckets for Subscribe broadcast-to-receive end-to-end measurement,
/// in seconds.  Subscribe fan-out is purely in-process so the floor is much
/// lower than for kube-backed RPCs — buckets start at 100 µs.
const SUBSCRIBE_LATENCY_BUCKETS: &[f64] = &[
    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1.0,
];

/// Latency buckets for per-stage Subscribe pipeline histograms (apply→broadcast,
/// broadcast→encode, encode→send), in seconds.  Sized 0.1 ms .. 100 ms per the
/// OBS-2 ticket — fine-grained low end to resolve sub-ms in-process hops, top
/// end caps at 100 ms to detect head-of-line blocking under stress load.
const PIPELINE_STAGE_BUCKETS: &[f64] = &[
    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
];

/// Byte-size buckets for h2 DATA frame payload histogram.  ConfigEvent protos
/// at typical workloads are ~512 B – 4 KiB; we extend up to 64 KiB to capture
/// large-config tail and 256 B floor to catch tombstone / Deleted frames.
const H2_FRAME_BYTE_BUCKETS: &[f64] = &[
    256.0, 512.0, 1024.0, 2048.0, 4096.0, 8192.0, 16384.0, 32768.0, 65536.0,
];

lazy_static::lazy_static! {
    /// Current number of active gRPC Subscribe streams, by namespace.
    pub static ref ACTIVE_SUBSCRIBERS: GaugeVec = register_gauge_vec!(
        "konfig_active_subscribers",
        "Current number of active gRPC Subscribe streams",
        &["namespace"]
    )
    .expect("failed to register konfig_active_subscribers");

    /// Total ConfigEvents broadcast per namespace.
    pub static ref EVENTS_BROADCAST: CounterVec = register_counter_vec!(
        "konfig_events_broadcast_total",
        "Total ConfigEvents broadcast per namespace",
        &["namespace"]
    )
    .expect("failed to register konfig_events_broadcast_total");

    /// Total number of times a subscriber lagged and was disconnected, by namespace.
    pub static ref BROADCAST_LAG: CounterVec = register_counter_vec!(
        "konfig_broadcast_lag_total",
        "Total number of subscriber lag disconnects per namespace",
        &["namespace"]
    )
    .expect("failed to register konfig_broadcast_lag_total");

    /// Depth of the per-namespace replay buffer (sampled every 5 s).
    pub static ref REPLAY_BUFFER_DEPTH: GaugeVec = register_gauge_vec!(
        "konfig_replay_buffer_depth",
        "Number of events currently held in the per-namespace replay buffer",
        &["namespace"]
    )
    .expect("failed to register konfig_replay_buffer_depth");

    /// Seconds since the watcher last received an event from the K8s API server
    /// (0 = fresh / cold-start before any event has been received).
    ///
    /// Updated by the background metric sampler in `grpc::serve` every 5 s
    /// from per-namespace `LastEventAt` entries.  Alert on
    /// `konfig_stale_seconds > 300` to detect a watcher disconnected from the
    /// K8s API server.  See `docs/runbook.md`.
    pub static ref STALE_SECONDS: GaugeVec = register_gauge_vec!(
        "konfig_stale_seconds",
        "Seconds since the watcher last received an event from the K8s API server (0 = fresh)",
        &["namespace"]
    )
    .expect("failed to register konfig_stale_seconds");

    /// Total Apply RPC calls, by namespace and result (ok / rejected / error).
    pub static ref APPLY_TOTAL: CounterVec = register_counter_vec!(
        "konfig_apply_total",
        "Total Apply RPC calls per namespace and result",
        &["namespace", "result"]
    )
    .expect("failed to register konfig_apply_total");

    /// Apply RPC handler latency, by namespace and result (ok / rejected / error).
    pub static ref APPLY_DURATION: HistogramVec = register_histogram_vec!(
        "konfig_apply_duration_seconds",
        "Apply RPC handler duration in seconds",
        &["namespace", "result"],
        RPC_LATENCY_BUCKETS.to_vec()
    )
    .expect("failed to register konfig_apply_duration_seconds");

    /// Get / GetAll RPC handler latency, by namespace.
    pub static ref GET_DURATION: HistogramVec = register_histogram_vec!(
        "konfig_get_duration_seconds",
        "Get/GetAll RPC handler duration in seconds",
        &["namespace"],
        RPC_LATENCY_BUCKETS.to_vec()
    )
    .expect("failed to register konfig_get_duration_seconds");

    /// Subscribe end-to-end latency: from `broadcast::send` in the namespace
    /// watcher to the moment each subscriber's bridge enqueues the event onto
    /// its per-client mpsc channel.  Observed once per subscriber per event.
    pub static ref SUBSCRIBE_E2E_LATENCY: HistogramVec = register_histogram_vec!(
        "konfig_subscribe_e2e_latency_seconds",
        "Subscribe end-to-end latency (broadcast send to subscriber mpsc enqueue) in seconds",
        &["namespace"],
        SUBSCRIBE_LATENCY_BUCKETS.to_vec()
    )
    .expect("failed to register konfig_subscribe_e2e_latency_seconds");

    // ── OBS-2 per-stage Subscribe pipeline histograms ────────────────────────
    //
    // Decomposes the Subscribe path into three measured stages so the next
    // perf experiment can target the actual dominant stage instead of guessing
    // from a flamegraph.  All three are unlabelled (single Histogram, not Vec)
    // because labelled per-namespace cardinality is high under stress load and
    // the stage attribution is namespace-invariant — the dominant stage is a
    // property of the runtime, not the workload.

    /// `apply_to_broadcast`: seconds between a kube watch event being observed
    /// by `run_namespace_watcher` (after parse) and the `broadcast::Sender::send`
    /// call that fans the event out to all subscribers.  Captures the cost of
    /// serialising the proto, wrapping it in the `BroadcastFrame` envelope, and
    /// pushing it into the replay buffer.
    pub static ref APPLY_TO_BROADCAST_SECONDS: Histogram = register_histogram!(
        "konfig_apply_to_broadcast_seconds",
        "Seconds from kube watch event observed to broadcast::send (proto encode + replay-push)",
        PIPELINE_STAGE_BUCKETS.to_vec()
    )
    .expect("failed to register konfig_apply_to_broadcast_seconds");

    /// `broadcast_to_encode`: seconds between `broadcast::Sender::send` (stamped
    /// on `BroadcastFrame.sent_at`) and the moment the subscriber's bridge has
    /// just returned from `bcast_rx.recv()` — i.e. the broadcast fan-out hop
    /// timing observed at each receiver.  Observed once per (subscriber × event)
    /// pair, so a 300-sub × 500-event run produces 150 000 observations.
    pub static ref BROADCAST_TO_ENCODE_SECONDS: Histogram = register_histogram!(
        "konfig_broadcast_to_encode_seconds",
        "Seconds from broadcast::send to subscriber bcast_rx.recv() return (broadcast fan-out hop)",
        PIPELINE_STAGE_BUCKETS.to_vec()
    )
    .expect("failed to register konfig_broadcast_to_encode_seconds");

    /// `encode_to_send`: seconds between bridging receive (right after
    /// `bcast_rx.recv()` returns) and the completion of `tx.try_send` onto the
    /// per-subscriber mpsc channel.  This is the closest in-process proxy for
    /// "prost encode + tonic transport hand-off" without a custom codec —
    /// tonic's Codec::encode runs *after* this point but is opaque from outside
    /// the gRPC transport stack.
    pub static ref ENCODE_TO_SEND_SECONDS: Histogram = register_histogram!(
        "konfig_encode_to_send_seconds",
        "Seconds from bridge recv to per-subscriber mpsc try_send completion (encode + hand-off)",
        PIPELINE_STAGE_BUCKETS.to_vec()
    )
    .expect("failed to register konfig_encode_to_send_seconds");

    /// `writev_calls_total`: total successful per-subscriber mpsc sends.  This
    /// is the closest in-process proxy for tonic / h2 transport `writev` calls
    /// without a custom h2 wrapper — every successful mpsc send corresponds to
    /// at least one h2 DATA frame eventually leaving the socket.  Compare to
    /// `konfig_events_broadcast_total` × active subscriber count to estimate
    /// the per-event writev fan-out (hypothesis: ≥ 1 writev per 3 events).
    pub static ref WRITEV_CALLS_TOTAL: Counter = register_counter!(
        "konfig_writev_calls_total",
        "Total successful per-subscriber mpsc sends (proxy for h2 writev calls)"
    )
    .expect("failed to register konfig_writev_calls_total");

    /// `h2_data_frame_bytes`: histogram of per-event encoded ConfigEvent size in
    /// bytes (`prost::Message::encoded_len`).  Each h2 DATA frame for a single
    /// ConfigEvent carries this payload; tracking distribution helps decide if
    /// the dominant cost is small-frame syscall overhead or large-frame copies.
    pub static ref H2_DATA_FRAME_BYTES: Histogram = register_histogram!(
        "konfig_h2_data_frame_bytes",
        "Encoded ConfigEvent size in bytes (prost::encoded_len) — proxy for h2 DATA frame payload",
        H2_FRAME_BYTE_BUCKETS.to_vec()
    )
    .expect("failed to register konfig_h2_data_frame_bytes");
}

/// RAII guard that decrements `ACTIVE_SUBSCRIBERS` for `namespace` on drop.
///
/// Increment the gauge when a subscriber task starts, construct this guard, and
/// let it fall out of scope (or be explicitly dropped) on every exit path.
pub struct SubGauge(pub String);

impl Drop for SubGauge {
    fn drop(&mut self) {
        ACTIVE_SUBSCRIBERS.with_label_values(&[&self.0]).dec();
    }
}

// ── Last-event-at tracking ────────────────────────────────────────────────────

/// Shared per-namespace timestamp of the most recent watcher event.
///
/// `None` = cold start (no event received yet).  The watcher loops call
/// [`LastEventAt::touch`] on every event; the metric sampler in `grpc::serve`
/// reads via [`LastEventAt::elapsed_secs`] every 5 s and updates the
/// `konfig_stale_seconds` gauge.
///
/// Implementation: `Mutex<Option<Instant>>` rather than a lock-free atomic
/// because the write rate is bounded by the kube event rate (low) and the
/// read rate is bounded by the 5 s sampler — contention is negligible.
#[derive(Default, Debug)]
pub struct LastEventAt(Mutex<Option<Instant>>);

impl LastEventAt {
    pub fn new() -> Self {
        Self(Mutex::new(None))
    }

    /// Record that an event was just received.
    pub fn touch(&self) {
        *self.0.lock().expect("LastEventAt poisoned") = Some(Instant::now());
    }

    /// Seconds since the last event, or `None` if no event has been received
    /// yet (cold start — the sampler treats this as "fresh" / `0.0`).
    pub fn elapsed_secs(&self) -> Option<f64> {
        self.0
            .lock()
            .expect("LastEventAt poisoned")
            .map(|t| t.elapsed().as_secs_f64())
    }
}

/// Per-namespace `LastEventAt` map shared across watchers and the metric sampler.
///
/// The Config watcher (one namespace) and Secret watchers (N namespaces) both
/// upsert and `touch` their entry on every event.  The background sampler in
/// `grpc::serve` iterates the map every 5 s and publishes
/// `konfig_stale_seconds{namespace=…}`.
pub type LastEventAtMap = Arc<DashMap<String, Arc<LastEventAt>>>;

/// Look up the `LastEventAt` for `namespace`, inserting a fresh entry on first
/// touch.  Returns the same `Arc` for subsequent calls so the watcher and
/// sampler observe a shared instance.
pub fn last_event_at_for(map: &LastEventAtMap, namespace: &str) -> Arc<LastEventAt> {
    if let Some(existing) = map.get(namespace) {
        return Arc::clone(existing.value());
    }
    let entry = map
        .entry(namespace.to_string())
        .or_insert_with(|| Arc::new(LastEventAt::new()));
    Arc::clone(entry.value())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_subscribers_gauge_tracks_connections() {
        let ns = "test-ns-gauge";
        // Start from a known baseline.
        let before = ACTIVE_SUBSCRIBERS.with_label_values(&[ns]).get();
        ACTIVE_SUBSCRIBERS.with_label_values(&[ns]).inc();
        assert_eq!(
            ACTIVE_SUBSCRIBERS.with_label_values(&[ns]).get(),
            before + 1.0,
            "gauge must increase after inc()"
        );
        ACTIVE_SUBSCRIBERS.with_label_values(&[ns]).dec();
        assert_eq!(
            ACTIVE_SUBSCRIBERS.with_label_values(&[ns]).get(),
            before,
            "gauge must return to baseline after dec()"
        );
    }

    #[test]
    fn sub_gauge_raii_decrements_on_drop() {
        let ns = "test-ns-raii";
        let before = ACTIVE_SUBSCRIBERS.with_label_values(&[ns]).get();
        ACTIVE_SUBSCRIBERS.with_label_values(&[ns]).inc();
        {
            let _guard = SubGauge(ns.to_string());
            // Guard is live — gauge is still incremented.
            assert_eq!(
                ACTIVE_SUBSCRIBERS.with_label_values(&[ns]).get(),
                before + 1.0
            );
        }
        // Guard dropped — gauge must be back to baseline.
        assert_eq!(
            ACTIVE_SUBSCRIBERS.with_label_values(&[ns]).get(),
            before,
            "SubGauge drop must decrement the gauge"
        );
    }

    #[test]
    fn events_broadcast_counter_increments() {
        let ns = "test-ns-events";
        let before = EVENTS_BROADCAST.with_label_values(&[ns]).get();
        EVENTS_BROADCAST.with_label_values(&[ns]).inc();
        assert_eq!(
            EVENTS_BROADCAST.with_label_values(&[ns]).get(),
            before + 1.0,
        );
    }

    #[test]
    fn broadcast_lag_counter_increments() {
        let ns = "test-ns-lag";
        let before = BROADCAST_LAG.with_label_values(&[ns]).get();
        BROADCAST_LAG.with_label_values(&[ns]).inc();
        assert_eq!(BROADCAST_LAG.with_label_values(&[ns]).get(), before + 1.0,);
    }

    #[test]
    fn apply_total_counter_increments_all_results() {
        let ns = "test-ns-apply";
        for result in &["ok", "rejected", "error"] {
            let before = APPLY_TOTAL.with_label_values(&[ns, result]).get();
            APPLY_TOTAL.with_label_values(&[ns, result]).inc();
            assert_eq!(
                APPLY_TOTAL.with_label_values(&[ns, result]).get(),
                before + 1.0,
                "apply_total[{result}] must increment"
            );
        }
    }

    #[test]
    fn apply_duration_histogram_records_observations() {
        let ns = "test-ns-apply-duration";
        let before = APPLY_DURATION
            .with_label_values(&[ns, "ok"])
            .get_sample_count();
        APPLY_DURATION.with_label_values(&[ns, "ok"]).observe(0.012);
        APPLY_DURATION.with_label_values(&[ns, "ok"]).observe(0.345);
        assert_eq!(
            APPLY_DURATION
                .with_label_values(&[ns, "ok"])
                .get_sample_count(),
            before + 2,
        );
        assert!(
            APPLY_DURATION
                .with_label_values(&[ns, "ok"])
                .get_sample_sum()
                >= 0.357 - f64::EPSILON,
        );
    }

    #[test]
    fn get_duration_histogram_records_observations() {
        let ns = "test-ns-get-duration";
        let before = GET_DURATION.with_label_values(&[ns]).get_sample_count();
        GET_DURATION.with_label_values(&[ns]).observe(0.001);
        assert_eq!(
            GET_DURATION.with_label_values(&[ns]).get_sample_count(),
            before + 1,
        );
    }

    #[test]
    fn subscribe_e2e_latency_histogram_records_observations() {
        let ns = "test-ns-sub-latency";
        let before = SUBSCRIBE_E2E_LATENCY
            .with_label_values(&[ns])
            .get_sample_count();
        SUBSCRIBE_E2E_LATENCY
            .with_label_values(&[ns])
            .observe(0.0003);
        assert_eq!(
            SUBSCRIBE_E2E_LATENCY
                .with_label_values(&[ns])
                .get_sample_count(),
            before + 1,
        );
    }

    #[test]
    fn replay_buffer_depth_gauge_sets_value() {
        let ns = "test-ns-replay";
        REPLAY_BUFFER_DEPTH.with_label_values(&[ns]).set(42.0);
        assert_eq!(REPLAY_BUFFER_DEPTH.with_label_values(&[ns]).get(), 42.0);
        REPLAY_BUFFER_DEPTH.with_label_values(&[ns]).set(0.0);
        assert_eq!(REPLAY_BUFFER_DEPTH.with_label_values(&[ns]).get(), 0.0);
    }

    #[test]
    fn stale_seconds_gauge_exists() {
        let ns = "test-ns-stale";
        // Register-on-first-use: writing then reading proves the metric is
        // registered in the default Prometheus registry and is a labelled gauge.
        STALE_SECONDS.with_label_values(&[ns]).set(42.0);
        assert_eq!(STALE_SECONDS.with_label_values(&[ns]).get(), 42.0);
        // 0 represents "fresh / cold start" — the sampler writes this when
        // no event has been received yet.
        STALE_SECONDS.with_label_values(&[ns]).set(0.0);
        assert_eq!(STALE_SECONDS.with_label_values(&[ns]).get(), 0.0);
    }

    #[test]
    fn last_event_at_cold_start_is_none() {
        let lea = LastEventAt::new();
        assert!(
            lea.elapsed_secs().is_none(),
            "cold-start LastEventAt must report None — sampler treats as fresh"
        );
    }

    #[test]
    fn last_event_at_touch_records_recent_instant() {
        let lea = LastEventAt::new();
        lea.touch();
        let elapsed = lea.elapsed_secs().expect("touched — must be Some");
        assert!(
            elapsed < 1.0,
            "elapsed must be sub-second immediately after touch, got {elapsed}"
        );
    }

    #[test]
    fn last_event_at_for_returns_same_arc_per_namespace() {
        let map: LastEventAtMap = Arc::new(DashMap::new());
        let a = last_event_at_for(&map, "ns-a");
        let a2 = last_event_at_for(&map, "ns-a");
        assert!(
            Arc::ptr_eq(&a, &a2),
            "repeated lookup must return the same Arc — watcher and sampler must share state"
        );
        let b = last_event_at_for(&map, "ns-b");
        assert!(
            !Arc::ptr_eq(&a, &b),
            "different namespaces must have distinct entries"
        );
    }

    // ── OBS-2 per-stage Subscribe pipeline histograms ────────────────────────

    #[test]
    fn apply_to_broadcast_histogram_records_observations() {
        let before = APPLY_TO_BROADCAST_SECONDS.get_sample_count();
        APPLY_TO_BROADCAST_SECONDS.observe(0.0003);
        APPLY_TO_BROADCAST_SECONDS.observe(0.0012);
        assert_eq!(APPLY_TO_BROADCAST_SECONDS.get_sample_count(), before + 2);
        assert!(APPLY_TO_BROADCAST_SECONDS.get_sample_sum() >= 0.0015 - f64::EPSILON);
    }

    #[test]
    fn broadcast_to_encode_histogram_records_observations() {
        let before = BROADCAST_TO_ENCODE_SECONDS.get_sample_count();
        BROADCAST_TO_ENCODE_SECONDS.observe(0.0005);
        assert_eq!(BROADCAST_TO_ENCODE_SECONDS.get_sample_count(), before + 1);
    }

    #[test]
    fn encode_to_send_histogram_records_observations() {
        let before = ENCODE_TO_SEND_SECONDS.get_sample_count();
        ENCODE_TO_SEND_SECONDS.observe(0.0001);
        assert_eq!(ENCODE_TO_SEND_SECONDS.get_sample_count(), before + 1);
    }

    #[test]
    fn writev_calls_counter_increments() {
        let before = WRITEV_CALLS_TOTAL.get();
        WRITEV_CALLS_TOTAL.inc();
        WRITEV_CALLS_TOTAL.inc();
        assert_eq!(WRITEV_CALLS_TOTAL.get(), before + 2.0);
    }

    #[test]
    fn h2_data_frame_bytes_histogram_records_observations() {
        let before = H2_DATA_FRAME_BYTES.get_sample_count();
        H2_DATA_FRAME_BYTES.observe(1024.0);
        H2_DATA_FRAME_BYTES.observe(2048.0);
        assert_eq!(H2_DATA_FRAME_BYTES.get_sample_count(), before + 2);
        assert!(H2_DATA_FRAME_BYTES.get_sample_sum() >= 3072.0 - f64::EPSILON);
    }

    /// The five OBS-2 metrics MUST be present in the default Prometheus
    /// registry that the `/metrics` HTTP endpoint scrapes.  Asserts the metric
    /// family names — guards against accidental rename / unregister regressions.
    #[test]
    fn obs2_metrics_registered_in_default_registry() {
        // Touch each metric once so registration is unambiguously realised.
        APPLY_TO_BROADCAST_SECONDS.observe(0.0);
        BROADCAST_TO_ENCODE_SECONDS.observe(0.0);
        ENCODE_TO_SEND_SECONDS.observe(0.0);
        WRITEV_CALLS_TOTAL.inc();
        H2_DATA_FRAME_BYTES.observe(0.0);

        let names: std::collections::HashSet<String> = prometheus::gather()
            .iter()
            .map(|mf| mf.name().to_string())
            .collect();

        for required in [
            "konfig_apply_to_broadcast_seconds",
            "konfig_broadcast_to_encode_seconds",
            "konfig_encode_to_send_seconds",
            "konfig_writev_calls_total",
            "konfig_h2_data_frame_bytes",
        ] {
            assert!(
                names.contains(required),
                "metric {required} missing from default registry — /metrics endpoint will not expose it"
            );
        }
    }

    /// Simulates one tick of the `konfig_stale_seconds` sampler loop in
    /// `grpc::serve`: a watcher that hasn't received any event yet must
    /// report `0` (cold start = fresh, NOT stale), and a watcher that
    /// received an event some time ago must report the elapsed seconds.
    #[test]
    fn stale_seconds_sampler_logic_matches_grpc_serve() {
        let map: LastEventAtMap = Arc::new(DashMap::new());
        let cold = last_event_at_for(&map, "test-ns-cold");
        let warm = last_event_at_for(&map, "test-ns-warm");

        // Simulate a watcher receiving an event ~0 ms ago.
        warm.touch();

        // Mirror grpc::serve's sampler loop: for each entry, publish
        // elapsed_secs() with `None` mapped to 0.0.
        for entry in map.iter() {
            let secs = entry.value().elapsed_secs().unwrap_or(0.0);
            STALE_SECONDS.with_label_values(&[entry.key()]).set(secs);
        }

        // Cold-start namespace: gauge MUST be 0 (no event yet → not stale).
        assert_eq!(
            STALE_SECONDS.with_label_values(&["test-ns-cold"]).get(),
            0.0,
            "cold-start namespace must report 0 staleness (not yet received any event)"
        );
        // Warm namespace: gauge is sub-second since we just touched.
        let warm_val = STALE_SECONDS.with_label_values(&["test-ns-warm"]).get();
        assert!(
            (0.0..1.0).contains(&warm_val),
            "warm namespace must report sub-second staleness, got {warm_val}"
        );

        // Drop our handle to cold so the next assertion is independent; the
        // map still owns one Arc, so the gauge baseline persists across
        // future ticks.
        drop(cold);
    }
}
