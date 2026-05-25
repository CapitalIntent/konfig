//! `Subscribe` handler for `KonfigService`.
//!
//! Streams `ConfigEvent` proto messages as Config CRD changes occur in K8s.
//! Each subscriber gets its own mpsc channel (capacity 256); slow subscribers
//! are disconnected with RESOURCE_EXHAUSTED.

use std::sync::Arc;

use futures_util::{StreamExt, TryStreamExt};
use kube::core::DynamicObject;
use kube::runtime::watcher::{self as kube_watcher, Event, watcher as kube_watch_stream};
use kube::{Api, Client};
use tokio::sync::mpsc;
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
    debug!(namespace = %req.namespace, "Subscribe RPC");

    // Refuse if cache not yet populated.
    if !cache.is_populated() {
        return Err(Status::unavailable("cache not yet populated"));
    }

    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let ar = config_api_resource();
    let namespace = req.namespace.clone();

    tokio::spawn(async move {
        let api: Api<DynamicObject> = Api::namespaced_with(kube_client, &namespace, &ar);

        // resource_version resume is a Phase 2B enhancement.
        // For now, start watching from the current cluster state.
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

            let config_event = ConfigEvent { event_type, config: Some(snapshot_to_proto(&snap)) };

            if tx.capacity() == 0 {
                warn!("Subscriber too slow — disconnecting with RESOURCE_EXHAUSTED");
                let _ = tx.send(Err(Status::resource_exhausted("subscriber too slow"))).await;
                break;
            }

            match tx.send(Ok(config_event)).await {
                Ok(_) => {}
                Err(_) => {
                    info!("Subscriber disconnected — closing watch stream");
                    break;
                }
            }
        }
    });

    Ok(Response::new(ReceiverStream::new(rx)))
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
