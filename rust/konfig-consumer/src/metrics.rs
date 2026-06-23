//! `konfig_consumer_stale_seconds` gauge helper for konfig consumers.
//!
//! The consumer crate does NOT touch the default Prometheus registry — callers
//! must hand us a `prometheus::Registry` they own (so consumer pods can keep
//! all their metrics in a single registry and avoid double-registration when
//! more than one config stream is driven in-process).

use std::sync::{Arc, Mutex};
use std::time::Instant;

use prometheus::{Gauge, Opts, Registry};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MetricsError {
    #[error("prometheus registration failed: {0}")]
    Register(#[from] prometheus::Error),
}

/// Register `konfig_consumer_stale_seconds` on the supplied registry and return the
/// gauge handle.  Caller is expected to hold the returned `Gauge` for the
/// lifetime of the stream (the sampler task writes it).
pub fn register_stale_seconds(registry: &Registry) -> Result<Gauge, MetricsError> {
    let gauge = Gauge::with_opts(Opts::new(
        "konfig_consumer_stale_seconds",
        "Seconds since the konfig stream last received an event (0 = active / fresh)",
    ))?;
    registry.register(Box::new(gauge.clone()))?;
    Ok(gauge)
}

/// Shared timestamp of the most recent successfully-received stream event.
///
/// `None` = cold start (no event yet) — sampler treats this as 0.0 staleness.
#[derive(Default, Debug)]
pub struct LastEventAt(Mutex<Option<Instant>>);

impl LastEventAt {
    pub fn new() -> Self {
        Self(Mutex::new(None))
    }

    /// Record an event was just received.  Also clears any "stale" state
    /// (the stream driver re-touches on every event so `elapsed_secs` grows
    /// from the disconnect instant rather than the last event instant).
    pub fn touch(&self) {
        *self.0.lock().expect("LastEventAt poisoned") = Some(Instant::now());
    }

    pub fn elapsed_secs(&self) -> Option<f64> {
        self.0
            .lock()
            .expect("LastEventAt poisoned")
            .map(|t| t.elapsed().as_secs_f64())
    }
}

/// Background sampler: writes `elapsed_secs` (or 0 on cold start) into the
/// supplied gauge every `interval`.  Returns the `JoinHandle` so the caller
/// can abort it on shutdown.
pub fn spawn_stale_sampler(
    last_event_at: Arc<LastEventAt>,
    gauge: Gauge,
    interval: std::time::Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(interval);
        loop {
            tick.tick().await;
            let secs = last_event_at.elapsed_secs().unwrap_or(0.0);
            gauge.set(secs);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_stale_seconds_registers_gauge() {
        let registry = Registry::new();
        let gauge = register_stale_seconds(&registry).expect("registers");
        gauge.set(7.5);
        let families = registry.gather();
        let found = families
            .iter()
            .find(|mf| mf.name() == "konfig_consumer_stale_seconds")
            .expect("metric present");
        assert_eq!(found.get_metric()[0].get_gauge().get_value(), 7.5);
    }

    #[test]
    fn register_stale_seconds_rejects_double_registration() {
        let registry = Registry::new();
        register_stale_seconds(&registry).expect("first registers");
        assert!(register_stale_seconds(&registry).is_err());
    }

    #[test]
    fn last_event_at_cold_start_is_none() {
        let lea = LastEventAt::new();
        assert!(lea.elapsed_secs().is_none());
    }

    #[test]
    fn last_event_at_touch_is_sub_second() {
        let lea = LastEventAt::new();
        lea.touch();
        let elapsed = lea.elapsed_secs().expect("touched");
        assert!(elapsed < 1.0, "elapsed = {elapsed}");
    }
}
