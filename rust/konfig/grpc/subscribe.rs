//! `Subscribe` handler for `KonfigService`.
//!
//! Architecture: one kube watch stream per namespace, shared via
//! `tokio::sync::broadcast`.  Each subscriber gets a `Receiver` clone — O(1)
//! fan-out instead of O(N) sequential `try_send` per event.
//!
//! `resume_resource_version`: resolved via a per-namespace replay buffer
//! (`VecDeque` of the last `REPLAY_BUFFER_SIZE` events).  When a client
//! reconnects with a non-empty `resume_resource_version`:
//!
//! 1. Buffer hit  — replay only the events after that RV, then join the live
//!    broadcast.  Zero additional kube watch calls regardless of how many
//!    clients reconnect simultaneously.
//! 2. Buffer miss — the RV is too old (compacted by etcd).  Send the full
//!    current cache as MODIFIED events then join the live broadcast.  No error

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use kube::Client;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};
use tracing::debug;

use crate::cache::ConfigCache;
use crate::proto::{ConfigEvent, SubscribeRequest};

mod broadcast;
mod filter;
mod replay;
#[cfg(test)]
mod test_support;
mod watch;

pub use broadcast::{BroadcastFrame, MAX_BROADCAST_SHARDS, MIN_BROADCAST_SHARDS, ShardSet};
pub(crate) use broadcast::{bridge_broadcast, try_send_or_disconnect};
pub(crate) use filter::{SubscribeFilter, filter_allows_event};
pub use replay::{REPLAY_BUFFER_SIZE, ReplayBuffer, ReplayEntry};
pub(crate) use replay::{push_replay, resume_from_buffer};
pub(crate) use watch::get_or_create_broadcast;
#[cfg(test)]
pub(crate) use watch::pump_subscribe_namespace_events;
pub use watch::{gc_task, gc_tick};

/// Per-subscriber mpsc capacity — back-pressure for slow readers.
const CHANNEL_CAPACITY: usize = 256;

#[allow(clippy::too_many_arguments)]
pub async fn handle_subscribe(
    cache: Arc<ConfigCache>,
    kube_client: Client,
    namespace_broadcasts: Arc<DashMap<String, ShardSet>>,
    namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>>,
    watcher_handles: Arc<DashMap<String, JoinHandle<()>>>,
    drain_notify: Arc<Notify>,
    coalesce_window: Duration,
    broadcast_shards: usize,
    req: SubscribeRequest,
) -> Result<Response<ReceiverStream<Result<ConfigEvent, Status>>>, Status> {
    debug!(namespace = %req.namespace, resume_rv = %req.resume_resource_version, "Subscribe RPC");

    if !cache.is_populated() {
        return Err(Status::unavailable("cache not yet populated"));
    }

    // Build the per-subscription filter BEFORE spawning so a malformed
    // `label_selector` surfaces as the RPC's `INVALID_ARGUMENT` result rather
    // than a silently-dropped stream.  `names` + `label_selector` are ANDed;
    // an empty `names` slice keeps the historical "all configs" behaviour for
    // the name dimension, and an empty `label_selector` adds no label
    // constraint.  NOTE: a non-empty `names` now actually filters — clients
    // that previously sent `names` received every config in the namespace
    // (the filter was never applied); they now receive only the named ones.
    let filter = Arc::new(SubscribeFilter::new(&req.names, &req.label_selector)?);

    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    // Move req fields out instead of cloning — req is dropped at function exit.
    // Single clone for the get_or_create_broadcast call; resume_from_buffer
    // takes the original move.
    let namespace = req.namespace;
    let resume_rv = req.resume_resource_version;

    // Get or create the broadcast receiver and replay buffer for this namespace.
    // The returned receiver is attached to ONE shard (round-robin); the watcher
    // fans every event to all shards.
    let (bcast_rx, replay_buf) = get_or_create_broadcast(
        namespace.clone(),
        kube_client,
        Arc::clone(&namespace_broadcasts),
        Arc::clone(&namespace_replay_buffers),
        Arc::clone(&watcher_handles),
        coalesce_window,
        broadcast_shards,
    );

    // Both resume and fresh-subscribe paths route through resume_from_buffer:
    // - Non-empty resume_rv → buffer-hit replays missed events, buffer-miss
    //   sends full snapshot + post-snapshot race-window events.
    // - Empty resume_rv → falls through to buffer-miss path → sends full
    //   snapshot synchronously as the first event(s) so a fresh subscriber
    //   never has to wait for the next apply to receive any state.
    tokio::spawn(resume_from_buffer(
        resume_rv,
        replay_buf,
        cache,
        namespace,
        bcast_rx,
        tx,
        drain_notify,
        filter,
    ));
    Ok(Response::new(ReceiverStream::new(rx)))
}

#[cfg(test)]
mod tests {
    #![allow(unused_imports)]
    use super::*;
    use crate::grpc::subscribe::test_support::*;

    // ── Unit: empty_cache_fails_gate ─────────────────────────────────────────

    #[test]
    fn empty_cache_fails_gate() {
        let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
        assert!(!cache.is_populated());
    }

    #[test]
    fn populated_cache_passes_gate() {
        let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
        cache.update(ConfigSnapshot {
            name: "cfg".into(),
            namespace: "default".into(),
            schema_version: 1,
            resource_version: "001".into(),
            ..Default::default()
        });
        assert!(cache.is_populated());
    }
}
