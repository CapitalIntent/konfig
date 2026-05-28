//! `Subscribe` handler for `KonfigService`.
//!
//! Architecture: one kube watch stream per namespace, shared via
//! `tokio::sync::broadcast`.  Each subscriber gets a `Receiver` clone — O(1)
//! fan-out instead of O(N) sequential `try_send` per event.
//!
//! `resume_resource_version`: keeps the existing raw kube watch path (resume
//! semantics require per-subscriber starting points incompatible with a shared
//! broadcast).

use std::sync::Arc;

use dashmap::DashMap;
use futures_util::{StreamExt, TryStreamExt};
use kube::api::{WatchEvent, WatchParams};
use kube::core::DynamicObject;
use kube::runtime::watcher::{self as kube_watcher, Event, watcher as kube_watch_stream};
use kube::{Api, Client};
use tokio::sync::{broadcast, mpsc};
use tokio::sync::mpsc::error::TrySendError;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};
use tracing::{debug, info, warn};

use crate::cache::ConfigCache;
use crate::grpc::snapshot_to_proto;
use crate::proto::{ConfigEvent, SubscribeRequest, config_event::EventType};
use crate::watcher::config_api_resource;

/// Per-subscriber mpsc capacity — back-pressure for slow readers.
const CHANNEL_CAPACITY: usize = 256;

/// Broadcast ring-buffer capacity per namespace.
/// Sized so that even the slowest subscriber can drain before the ring wraps.
const BROADCAST_CAPACITY: usize = 1_024;

pub async fn handle_subscribe(
    cache: Arc<ConfigCache>,
    kube_client: Client,
    namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<ConfigEvent>>>,
    req: SubscribeRequest,
) -> Result<Response<ReceiverStream<Result<ConfigEvent, Status>>>, Status> {
    debug!(namespace = %req.namespace, resume_rv = %req.resume_resource_version, "Subscribe RPC");

    if !cache.is_populated() {
        return Err(Status::unavailable("cache not yet populated"));
    }

    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let namespace = req.namespace.clone();
    let resume_rv = req.resume_resource_version.clone();

    if !resume_rv.is_empty() {
        // Resume path: per-subscriber raw watch (broadcast can't share resume points).
        tokio::spawn(run_raw_watch(
            kube_client,
            namespace,
            config_api_resource(),
            resume_rv,
            tx,
        ));
        return Ok(Response::new(ReceiverStream::new(rx)));
    }

    // Broadcast path: get or create the shared sender for this namespace.
    let bcast_rx = get_or_create_broadcast(
        namespace.clone(),
        kube_client,
        namespace_broadcasts,
    );

    // Bridge: broadcast::Receiver → mpsc::Sender (this subscriber's gRPC stream).
    tokio::spawn(bridge_broadcast(bcast_rx, tx));

    Ok(Response::new(ReceiverStream::new(rx)))
}

/// Return a broadcast `Receiver` for `namespace`, spinning up a kube watcher
/// if one isn't already running for that namespace.
fn get_or_create_broadcast(
    namespace: String,
    kube_client: Client,
    namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<ConfigEvent>>>,
) -> broadcast::Receiver<ConfigEvent> {
    // Fast path: namespace already has a running watcher.
    if let Some(sender) = namespace_broadcasts.get(&namespace) {
        return sender.subscribe();
    }

    // Slow path: first subscriber for this namespace — create broadcast + watcher.
    match namespace_broadcasts.entry(namespace.clone()) {
        dashmap::mapref::entry::Entry::Occupied(e) => {
            // Another task beat us while we were acquiring the entry lock.
            e.get().subscribe()
        }
        dashmap::mapref::entry::Entry::Vacant(e) => {
            let (bcast_tx, bcast_rx) = broadcast::channel(BROADCAST_CAPACITY);
            e.insert(bcast_tx.clone());

            // The watcher runs until the kube stream ends, then removes itself
            // from the map so the next Subscribe creates a new one.
            tokio::spawn(run_namespace_watcher(
                namespace,
                kube_client,
                bcast_tx,
                namespace_broadcasts.clone(),
            ));

            bcast_rx
        }
    }
}

/// Single kube watch stream per namespace — broadcasts every event to all
/// current subscribers.  Removes itself from `namespace_broadcasts` on exit.
async fn run_namespace_watcher(
    namespace: String,
    kube_client: Client,
    tx: broadcast::Sender<ConfigEvent>,
    namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<ConfigEvent>>>,
) {
    let ar = config_api_resource();
    let api: Api<DynamicObject> = Api::namespaced_with(kube_client, &namespace, &ar);
    let wc = kube_watcher::Config::default();
    let mut stream = kube_watch_stream(api, wc).boxed();

    while let Some(event) = stream.try_next().await.unwrap_or(None) {
        let (event_type, obj) = match event {
            Event::Apply(obj) | Event::InitApply(obj) => (EventType::Modified as i32, obj),
            Event::Delete(obj) => (EventType::Deleted as i32, obj),
            Event::Init | Event::InitDone => continue,
        };
        let Some(snap) = crate::watcher::parse_config_object(&obj) else {
            continue;
        };
        let config_event = ConfigEvent {
            event_type,
            config: Some(snapshot_to_proto(&snap)),
        };
        // `send` returns Err only when there are zero receivers — drop the event.
        let _ = tx.send(config_event);
    }

    // Watcher stream ended — remove from map so next Subscribe creates a new one.
    namespace_broadcasts.remove(&namespace);
    info!(namespace = %namespace, "Namespace watcher ended — removed from broadcast map");
}

/// Forward events from the namespace broadcast to a single subscriber's mpsc.
///
/// Disconnects the subscriber with RESOURCE_EXHAUSTED if:
/// - the mpsc channel is full (subscriber too slow to drain), or
/// - the broadcast ring wrapped before this receiver drained (lagged).
async fn bridge_broadcast(
    mut bcast_rx: broadcast::Receiver<ConfigEvent>,
    tx: mpsc::Sender<Result<ConfigEvent, Status>>,
) {
    loop {
        match bcast_rx.recv().await {
            Ok(event) => match tx.try_send(Ok(event)) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    warn!("Subscriber too slow — disconnecting with RESOURCE_EXHAUSTED");
                    let _ = tx.try_send(Err(Status::resource_exhausted("subscriber too slow")));
                    break;
                }
                Err(TrySendError::Closed(_)) => {
                    info!("Subscriber disconnected");
                    break;
                }
            },
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!(missed = n, "Subscriber lagged — disconnecting");
                let _ = tx.try_send(Err(Status::resource_exhausted("subscriber lagged")));
                break;
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

/// Raw kube watch from a specific `resource_version` (resume path).
async fn run_raw_watch(
    kube_client: Client,
    namespace: String,
    ar: kube::api::ApiResource,
    resource_version: String,
    tx: mpsc::Sender<Result<ConfigEvent, Status>>,
) {
    let api: Api<DynamicObject> = Api::namespaced_with(kube_client, &namespace, &ar);
    let wp = WatchParams::default().timeout(290);

    let stream = match api.watch(&wp, &resource_version).await {
        Ok(s) => s,
        Err(e) => {
            warn!("Subscribe raw watch failed to start: {e}");
            let _ = tx.try_send(Err(Status::unavailable(format!("watch error: {e}"))));
            return;
        }
    };

    let mut stream = stream.boxed();

    while let Some(result) = stream.next().await {
        match result {
            Ok(WatchEvent::Added(obj)) | Ok(WatchEvent::Modified(obj)) => {
                if !emit_to_mpsc(&tx, EventType::Modified as i32, obj).await {
                    break;
                }
            }
            Ok(WatchEvent::Deleted(obj)) => {
                if !emit_to_mpsc(&tx, EventType::Deleted as i32, obj).await {
                    break;
                }
            }
            Ok(WatchEvent::Bookmark(_)) => {
                debug!("Subscribe: BOOKMARK received — cursor advanced");
            }
            Ok(WatchEvent::Error(e)) => {
                warn!("Subscribe raw watch error event: {e}");
                let _ = tx.try_send(Err(Status::internal(format!("watch error: {e}"))));
                break;
            }
            Err(e) => {
                warn!("Subscribe raw watch stream error: {e}");
                break;
            }
        }
    }
}

async fn emit_to_mpsc(
    tx: &mpsc::Sender<Result<ConfigEvent, Status>>,
    event_type: i32,
    obj: DynamicObject,
) -> bool {
    let Some(snap) = crate::watcher::parse_config_object(&obj) else {
        return true;
    };
    let config_event = ConfigEvent { event_type, config: Some(snapshot_to_proto(&snap)) };
    match tx.try_send(Ok(config_event)) {
        Ok(()) => true,
        Err(TrySendError::Full(_)) => {
            warn!("Subscriber too slow — disconnecting with RESOURCE_EXHAUSTED");
            let _ = tx.try_send(Err(Status::resource_exhausted("subscriber too slow")));
            false
        }
        Err(TrySendError::Closed(_)) => {
            info!("Subscriber disconnected — closing watch stream");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::ConfigCache;
    use crate::types::ConfigSnapshot;

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
            resource_version: "rv-001".into(),
            ..Default::default()
        });
        assert!(cache.is_populated());
    }
}
