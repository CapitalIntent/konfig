//! gRPC `Subscribe`-stream driver for the konfig consumer client.
//!
//! One stream covers N config names in a namespace (`SubscribeRequest.names`
//! is `repeated`). The driver task reads `ConfigEvent`s, demuxes by
//! `config.name`, parses each into a [`ConfigSnapshot`], and publishes it
//! through the matching per-name `ArcSwap`. On stream error it marks every
//! tracked snapshot stale, backs off, and reconnects — resuming from the last
//! observed `resource_version` so missed events replay.
//!
//! Reconnect backoff schedule: 1, 2, 4, 8, 16, 30s (cap).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use tonic::Streaming;
use tonic::transport::Channel;
use tracing::{debug, info, warn};

use crate::metrics::LastEventAt;
use crate::proto::config_event::EventType;
use crate::proto::konfig_service_client::KonfigServiceClient;
use crate::proto::{ConfigEvent, SubscribeRequest};
use crate::snapshot::{ConfigSnapshot, snapshot_from_proto};

/// Reconnect backoff (seconds): 1, 2, 4, 8, 16, then cap at 30.
pub const BACKOFF_STEPS_SECS: &[u64] = &[1, 2, 4, 8, 16, 30];

/// Backoff delay for a given (0-based) reconnect attempt, capped at the last
/// step.
pub fn backoff_delay(attempt: usize) -> Duration {
    let secs = BACKOFF_STEPS_SECS
        .get(attempt)
        .copied()
        .unwrap_or_else(|| *BACKOFF_STEPS_SECS.last().expect("non-empty backoff steps"));
    Duration::from_secs(secs)
}

/// Shared per-name read model: the `ArcSwap` a `ConfigHandle` reads and the
/// stream driver publishes into.
pub(crate) type Store = Arc<ArcSwap<ConfigSnapshot>>;

/// Inputs the spawned driver task owns for the lifetime of a subscription.
pub(crate) struct StreamDriver {
    pub client: KonfigServiceClient<Channel>,
    pub namespace: String,
    pub names: Vec<String>,
    /// `name -> ArcSwap<ConfigSnapshot>` for every name in this subscription.
    pub stores: HashMap<String, Store>,
    pub last_event_at: Arc<LastEventAt>,
}

impl StreamDriver {
    /// Run the reconnect loop forever (until the task is aborted on `Drop`).
    pub async fn run(mut self) {
        let mut attempt: usize = 0;
        // Empty = start from the service's current state; updated to the last
        // observed resource_version so reconnects replay missed events.
        let mut resume_rv = String::new();

        loop {
            let req = SubscribeRequest {
                namespace: self.namespace.clone(),
                names: self.names.clone(),
                resume_resource_version: resume_rv.clone(),
            };

            match self.client.subscribe(req).await {
                Ok(resp) => {
                    info!(
                        namespace = %self.namespace,
                        names = ?self.names,
                        attempt,
                        "konfig-consumer: Subscribe stream opened"
                    );
                    attempt = 0;
                    match self.pump(resp.into_inner(), &mut resume_rv).await {
                        StreamEnd::Clean => {
                            info!("konfig-consumer: Subscribe stream ended cleanly");
                            // Server closed the stream without error; reconnect
                            // immediately rather than treating it as a fault.
                        }
                        StreamEnd::Error => {
                            self.mark_all_stale();
                        }
                    }
                }
                Err(status) => {
                    warn!(
                        attempt,
                        "konfig-consumer: Subscribe RPC failed: {status} — marking stale"
                    );
                    self.mark_all_stale();
                }
            }

            tokio::time::sleep(backoff_delay(attempt)).await;
            attempt = attempt.saturating_add(1);
        }
    }

    /// Drain one connected stream until it ends or errors, publishing each
    /// event into the matching per-name store and advancing `resume_rv`.
    async fn pump(
        &self,
        mut stream: Streaming<ConfigEvent>,
        resume_rv: &mut String,
    ) -> StreamEnd {
        loop {
            match stream.message().await {
                Ok(Some(event)) => {
                    self.last_event_at.touch();
                    self.apply(event, resume_rv);
                }
                Ok(None) => return StreamEnd::Clean,
                Err(status) => {
                    warn!("konfig-consumer: stream recv error: {status}");
                    return StreamEnd::Error;
                }
            }
        }
    }

    /// Demux one event to its per-name store and update `resume_rv`.
    fn apply(&self, event: ConfigEvent, resume_rv: &mut String) {
        let event_type = event.event_type();
        let Some(cfg) = event.config else {
            debug!("konfig-consumer: ConfigEvent with no config payload — ignoring");
            return;
        };

        // Advance the resume cursor regardless of name match: the server
        // streams in monotonic resource_version order, so resuming from the
        // newest value we have seen never replays an event we already applied.
        if !cfg.resource_version.is_empty() {
            resume_rv.clone_from(&cfg.resource_version);
        }

        let Some(store) = self.stores.get(&cfg.name) else {
            // Shouldn't happen — the server only streams names we asked for —
            // but stay defensive against a future server-side fan-out change.
            debug!(name = %cfg.name, "konfig-consumer: event for untracked name — ignoring");
            return;
        };

        match event_type {
            EventType::Deleted => {
                // Retain last-known-good content (CP semantics) but record the
                // delete in the resource_version so a resume is consistent.
                debug!(name = %cfg.name, "konfig-consumer: DELETED — retaining last snapshot");
            }
            EventType::Added | EventType::Modified | EventType::Snapshot => {
                match snapshot_from_proto(&cfg) {
                    Ok(snap) => {
                        info!(
                            name = %cfg.name,
                            schema_version = snap.schema_version,
                            rv = %snap.resource_version,
                            ?event_type,
                            "konfig-consumer: snapshot updated"
                        );
                        store.store(Arc::new(snap));
                    }
                    Err(e) => {
                        warn!(
                            name = %cfg.name,
                            "konfig-consumer: dropping unparseable event, retaining previous: {e}"
                        );
                    }
                }
            }
        }
    }

    /// Mark every tracked snapshot stale (connection lost). Preserves content.
    fn mark_all_stale(&self) {
        let now = Instant::now();
        for store in self.stores.values() {
            let current = store.load();
            if current.stale_since.is_some() {
                continue;
            }
            let mut next = (**current).clone();
            next.stale_since = Some(now);
            store.store(Arc::new(next));
        }
    }
}

/// How a single connected stream terminated.
enum StreamEnd {
    /// Server closed the stream with no error (`Ok(None)`).
    Clean,
    /// Transport / status error mid-stream.
    Error,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::Config as ProtoConfig;
    use serde_json::json;

    fn empty_store() -> Store {
        Arc::new(ArcSwap::from_pointee(ConfigSnapshot::default()))
    }

    fn driver_with(names: &[&str]) -> StreamDriver {
        // The client is never used in these unit tests (we exercise `apply` /
        // `mark_all_stale` directly), but constructing one requires a Channel.
        // `Channel::from_static(...).connect_lazy()` builds without any IO.
        let channel = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        let stores: HashMap<String, Store> =
            names.iter().map(|n| (n.to_string(), empty_store())).collect();
        StreamDriver {
            client: KonfigServiceClient::new(channel),
            namespace: "ns".to_string(),
            names: names.iter().map(|s| s.to_string()).collect(),
            stores,
            last_event_at: Arc::new(LastEventAt::new()),
        }
    }

    fn event(event_type: EventType, name: &str, rv: &str, content_json: &str) -> ConfigEvent {
        ConfigEvent {
            event_type: event_type as i32,
            config: Some(ProtoConfig {
                namespace: "ns".to_string(),
                name: name.to_string(),
                schema_version: 3,
                content_json: content_json.to_string(),
                resource_version: rv.to_string(),
                age_ms: 0,
                stale_since_ms: -1,
            }),
        }
    }

    #[test]
    fn backoff_schedule_matches_contract() {
        let want = [1u64, 2, 4, 8, 16, 30, 30, 30, 30];
        for (i, &s) in want.iter().enumerate() {
            assert_eq!(backoff_delay(i), Duration::from_secs(s), "attempt {i}");
        }
    }

    #[test]
    fn apply_demuxes_by_name() {
        let driver = driver_with(&["a", "b"]);
        let mut rv = String::new();
        driver.apply(
            event(EventType::Modified, "a", "10", r#"{"x": 1}"#),
            &mut rv,
        );
        driver.apply(
            event(EventType::Modified, "b", "11", r#"{"y": 2}"#),
            &mut rv,
        );

        let a = driver.stores["a"].load();
        let b = driver.stores["b"].load();
        assert_eq!(a.content["x"], 1);
        assert_eq!(b.content["y"], 2);
        assert_eq!(rv, "11", "resume cursor advances to newest rv");
    }

    #[test]
    fn snapshot_event_publishes() {
        let driver = driver_with(&["a"]);
        let mut rv = String::new();
        driver.apply(event(EventType::Snapshot, "a", "1", r#"{"k": 9}"#), &mut rv);
        assert_eq!(driver.stores["a"].load().content["k"], 9);
    }

    #[test]
    fn delete_retains_last_known_good() {
        let driver = driver_with(&["a"]);
        let mut rv = String::new();
        driver.apply(event(EventType::Modified, "a", "1", r#"{"k": 5}"#), &mut rv);
        driver.apply(event(EventType::Deleted, "a", "2", ""), &mut rv);
        // Content retained, cursor advanced.
        assert_eq!(driver.stores["a"].load().content["k"], 5);
        assert_eq!(rv, "2");
    }

    #[test]
    fn unparseable_event_retains_previous() {
        let driver = driver_with(&["a"]);
        let mut rv = String::new();
        driver.apply(event(EventType::Modified, "a", "1", r#"{"k": 5}"#), &mut rv);
        driver.apply(event(EventType::Modified, "a", "2", "not-json"), &mut rv);
        assert_eq!(driver.stores["a"].load().content["k"], 5);
    }

    #[test]
    fn event_for_untracked_name_ignored() {
        let driver = driver_with(&["a"]);
        let mut rv = String::new();
        driver.apply(event(EventType::Modified, "z", "1", r#"{"k": 1}"#), &mut rv);
        // 'a' untouched (still default null), cursor still advances.
        assert!(driver.stores["a"].load().content.is_null());
        assert_eq!(rv, "1");
    }

    #[test]
    fn mark_all_stale_sets_then_preserves() {
        let driver = driver_with(&["a", "b"]);
        let mut rv = String::new();
        driver.apply(event(EventType::Modified, "a", "1", r#"{"k": 1}"#), &mut rv);
        driver.mark_all_stale();
        let a = driver.stores["a"].load();
        assert!(a.stale_since.is_some());
        assert_eq!(a.content["k"], 1, "content preserved while stale");
        let first = a.stale_since;
        // Second mark is a no-op (already stale) — instant unchanged.
        driver.mark_all_stale();
        assert_eq!(driver.stores["a"].load().stale_since, first);
        let _ = json!({}); // keep serde_json import used in all cfgs
    }
}
