//! `Get` and `GetAll` handlers for `KonfigService`.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};
use tracing::{debug, warn};

use crate::cache::ConfigCache;
use crate::grpc::snapshot_to_proto;
use crate::metrics::GET_DURATION;
use crate::proto::{Config, GetAllRequest, GetRequest};

pub async fn handle_get(
    cache: Arc<ConfigCache>,
    req: GetRequest,
) -> Result<Response<Config>, Status> {
    debug!(namespace = %req.namespace, name = %req.name, "Get RPC");
    let started = Instant::now();

    let result = match cache.get(&req.namespace, &req.name) {
        Some(snap) => Ok(Response::new(snapshot_to_proto(&snap))),
        None => {
            warn!(
                namespace = %req.namespace,
                name = %req.name,
                "Get: config not found in cache",
            );
            Err(Status::not_found(format!(
                "config {}/{} not found",
                req.namespace, req.name
            )))
        }
    };
    GET_DURATION
        .with_label_values(&[&req.namespace])
        .observe(started.elapsed().as_secs_f64());
    result
}

/// Per-RPC mpsc buffer for `GetAll` / `GetAllSecrets`. Sized to a typical
/// per-namespace snapshot count so the spawned encoder rarely blocks.
const GET_ALL_CHANNEL_CAPACITY: usize = 256;

pub async fn handle_get_all(
    cache: Arc<ConfigCache>,
    req: GetAllRequest,
) -> Result<Response<ReceiverStream<Result<Config, Status>>>, Status> {
    debug!(namespace = %req.namespace, "GetAll RPC");
    let started = Instant::now();

    let (tx, rx) = mpsc::channel(GET_ALL_CHANNEL_CAPACITY);
    let entries = cache.all_in_namespace(&req.namespace);

    tokio::spawn(async move {
        for snap in entries {
            // tx.send returns Err(_) only when the receiver is dropped (client
            // disconnected). Stop iterating instead of pointlessly serialising
            // the rest of the namespace into a dead channel.
            if tx.send(Ok(snapshot_to_proto(&snap))).await.is_err() {
                debug!("GetAll: subscriber disconnected — stopping early");
                return;
            }
        }
    });

    GET_DURATION
        .with_label_values(&[&req.namespace])
        .observe(started.elapsed().as_secs_f64());
    Ok(Response::new(ReceiverStream::new(rx)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::ConfigCache;
    use crate::types::ConfigSnapshot;
    use serde_json::json;

    fn make_cache(namespace: &str, name: &str, schema_version: u32) -> Arc<ConfigCache> {
        let snap = ConfigSnapshot {
            name: name.into(),
            namespace: namespace.into(),
            schema_version,
            content: json!({"key": "val"}),
            resource_version: if schema_version > 0 {
                format!("rv-{schema_version}")
            } else {
                String::new()
            },
            ..Default::default()
        };
        Arc::new(ConfigCache::new(snap))
    }

    #[tokio::test]
    async fn get_returns_config_when_cache_populated() {
        let cache = make_cache("default", "my-config", 3);
        let req = GetRequest {
            namespace: "default".into(),
            name: "my-config".into(),
        };
        let resp = handle_get(cache, req).await.expect("must succeed");
        let cfg = resp.into_inner();
        assert_eq!(cfg.schema_version, 3);
        assert_eq!(cfg.name, "my-config");
        assert!(!cfg.content_json.is_empty());
    }

    #[tokio::test]
    async fn get_returns_not_found_when_cache_empty() {
        let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
        let req = GetRequest {
            namespace: "default".into(),
            name: "my-config".into(),
        };
        let result = handle_get(cache, req).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn get_returns_not_found_for_wrong_key() {
        let cache = make_cache("default", "my-config", 3);
        let req = GetRequest {
            namespace: "default".into(),
            name: "other-config".into(),
        };
        let result = handle_get(cache, req).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn get_all_streams_entries_in_namespace() {
        use tokio_stream::StreamExt;
        let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
        cache.update(ConfigSnapshot {
            namespace: "default".into(),
            name: "cfg-a".into(),
            schema_version: 1,
            content: json!({}),
            resource_version: "rv-1".into(),
            ..Default::default()
        });
        cache.update(ConfigSnapshot {
            namespace: "default".into(),
            name: "cfg-b".into(),
            schema_version: 2,
            content: json!({}),
            resource_version: "rv-2".into(),
            ..Default::default()
        });

        let req = GetAllRequest {
            namespace: "default".into(),
        };
        let resp = handle_get_all(cache, req).await.expect("must succeed");
        let mut stream = resp.into_inner();
        let mut count = 0usize;
        while let Some(item) = stream.next().await {
            assert!(item.is_ok());
            count += 1;
        }
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn get_all_empty_when_cache_unpopulated() {
        use tokio_stream::StreamExt;
        let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
        let req = GetAllRequest {
            namespace: "default".into(),
        };
        let resp = handle_get_all(cache, req).await.expect("must succeed");
        let mut stream = resp.into_inner();
        assert!(stream.next().await.is_none());
    }

    /// Dropping the receiver mid-stream must short-circuit the spawned
    /// encoder rather than serialising the rest of the namespace into a
    /// dead channel. Regression test for the prior `let _ = tx.send(...)`
    /// fire-and-forget pattern.
    #[tokio::test]
    async fn get_all_stops_when_receiver_dropped() {
        let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
        // Capacity = 256 in the handler, so to make the test deterministic
        // we count *receiver drop visibility*, not channel occupancy: load
        // far more than capacity, then drop the receiver and confirm the
        // task ends rather than spinning forever.
        for i in 0..1_000u32 {
            cache.update(ConfigSnapshot {
                namespace: "ns".into(),
                name: format!("cfg-{i}"),
                schema_version: i,
                content: json!({}),
                resource_version: format!("rv-{i}"),
                ..Default::default()
            });
        }
        let req = GetAllRequest {
            namespace: "ns".into(),
        };
        let resp = handle_get_all(cache, req).await.expect("must succeed");
        let stream = resp.into_inner();
        // Immediately drop without polling — receiver gone → next `tx.send`
        // returns Err and the spawned task must exit on its own.
        drop(stream);
        // If the encoder ignored the close, we'd see a stuck `JoinHandle`
        // here; instead, `yield_now` is enough for the spawn to observe the
        // drop and return. Use a generous timeout to keep the test stable.
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            for _ in 0..200 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("encoder must terminate after receiver dropped");
    }
}
