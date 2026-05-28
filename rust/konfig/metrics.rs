//! Prometheus metrics for the konfig service.
//!
//! All metrics are registered in the default Prometheus registry at startup
//! via `lazy_static!`.  The `/metrics` HTTP endpoint (port 9090) in `main.rs`
//! calls `prometheus::gather()` to serialise them for scraping.

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
}
