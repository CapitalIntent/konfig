//! Prometheus metrics for the konfig service.
//!
//! All metrics are registered in the default Prometheus registry at startup
//! via `lazy_static!`.  The `/metrics` HTTP endpoint (port 9090) in `main.rs`
//! calls `prometheus::gather()` to serialise them for scraping.

use std::sync::{Arc, Mutex};
use std::time::Instant;

use dashmap::DashMap;
use prometheus::{CounterVec, GaugeVec, register_counter_vec, register_gauge_vec};

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
