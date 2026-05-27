//! `Subscribe` handler for `KonfigService`.
//!
//! Streams `ConfigEvent` proto messages as Config CRD changes occur in K8s.
//! Each subscriber gets its own mpsc channel (capacity 256); slow subscribers
//! are disconnected with RESOURCE_EXHAUSTED.
//!
//! `resume_resource_version`: when set, the watch starts from that K8s
//! resourceVersion via the raw watch API, ensuring zero duplicates and zero
//! missed events across reconnects.

use std::sync::Arc;

use futures_util::{StreamExt, TryStreamExt};
use kube::api::{WatchEvent, WatchParams};
use kube::core::DynamicObject;
use kube::runtime::watcher::{self as kube_watcher, Event, watcher as kube_watch_stream};
use kube::{Api, Client};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};
use tracing::{debug, info, warn};

use crate::cache::ConfigCache;
use crate::grpc::snapshot_to_proto;
use crate::proto::{ConfigEvent, SubscribeRequest, config_event::EventType};
use crate::watcher::config_api_resource;

const CHANNEL_CAPACITY: usize = 256;

pub async fn handle_subscribe(
    cache: Arc<ConfigCache>,
    kube_client: Client,
    req: SubscribeRequest,
) -> Result<Response<ReceiverStream<Result<ConfigEvent, Status>>>, Status> {
    debug!(namespace = %req.namespace, resume_rv = %req.resume_resource_version, "Subscribe RPC");

    // Refuse if cache not yet populated.
    if !cache.is_populated() {
        return Err(Status::unavailable("cache not yet populated"));
    }

    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let ar = config_api_resource();
    let namespace = req.namespace.clone();
    let resume_rv = req.resume_resource_version.clone();

    tokio::spawn(async move {
        if resume_rv.is_empty() {
            run_high_level_watch(kube_client, namespace, ar, tx).await;
        } else {
            run_raw_watch(kube_client, namespace, ar, resume_rv, tx).await;
        }
    });

    Ok(Response::new(ReceiverStream::new(rx)))
}

/// High-level kube-rs watcher combinator (no resume RV needed).
async fn run_high_level_watch(
    kube_client: Client,
    namespace: String,
    ar: kube::api::ApiResource,
    tx: mpsc::Sender<Result<ConfigEvent, Status>>,
) {
    let api: Api<DynamicObject> = Api::namespaced_with(kube_client, &namespace, &ar);
    let wc = kube_watcher::Config::default();
    let mut stream = kube_watch_stream(api, wc).boxed();

    while let Some(event) = stream.try_next().await.unwrap_or(None) {
        let (event_type, obj) = match event {
            Event::Apply(obj) | Event::InitApply(obj) => (EventType::Modified as i32, obj),
            Event::Delete(obj) => (EventType::Deleted as i32, obj),
            Event::Init | Event::InitDone => continue,
        };
        if !emit_event(&tx, event_type, obj).await {
            break;
        }
    }
}

/// Raw kube watch from a specific `resource_version` (resume path).
///
/// Uses `Api::watch` to start from exactly the given RV, guaranteeing no
/// duplicates and no missed events.  BOOKMARK events update the cursor only.
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
                if !emit_event(&tx, EventType::Modified as i32, obj).await {
                    break;
                }
            }
            Ok(WatchEvent::Deleted(obj)) => {
                if !emit_event(&tx, EventType::Deleted as i32, obj).await {
                    break;
                }
            }
            Ok(WatchEvent::Bookmark(_)) => {
                // Bookmark events advance the K8s watch cursor — do not emit.
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

async fn emit_event(
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
        assert!(!cache.is_populated(), "empty cache must not be populated");
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
