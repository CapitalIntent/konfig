//! `Get` and `GetAll` handlers for `KonfigService`.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};
use tracing::{debug, warn};

use crate::cache::ConfigCache;
use crate::grpc::snapshot_to_proto;
use crate::proto::{Config, GetAllRequest, GetRequest};

pub async fn handle_get(
    cache: Arc<ConfigCache>,
    req: GetRequest,
) -> Result<Response<Config>, Status> {
    debug!(namespace = %req.namespace, name = %req.name, "Get RPC");

    match cache.get(&req.namespace, &req.name) {
        Some(snap) => Ok(Response::new(snapshot_to_proto(&snap))),
        None => {
            warn!(
                namespace = %req.namespace,
                name = %req.name,
                "Get: config not found in cache",
            );
            Err(Status::not_found(format!("config {}/{} not found", req.namespace, req.name)))
        }
    }
}

pub async fn handle_get_all(
    cache: Arc<ConfigCache>,
    req: GetAllRequest,
) -> Result<Response<ReceiverStream<Result<Config, Status>>>, Status> {
    debug!(namespace = %req.namespace, "GetAll RPC");

    let (tx, rx) = mpsc::channel(16);
    let entries = cache.all_in_namespace(&req.namespace);

    tokio::spawn(async move {
        for snap in entries {
            let _ = tx.send(Ok(snapshot_to_proto(&snap))).await;
        }
    });

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
        let req = GetRequest { namespace: "default".into(), name: "my-config".into() };
        let resp = handle_get(cache, req).await.expect("must succeed");
        let cfg = resp.into_inner();
        assert_eq!(cfg.schema_version, 3);
        assert_eq!(cfg.name, "my-config");
        assert!(!cfg.content_json.is_empty());
    }

    #[tokio::test]
    async fn get_returns_not_found_when_cache_empty() {
        let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
        let req = GetRequest { namespace: "default".into(), name: "my-config".into() };
        let result = handle_get(cache, req).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn get_returns_not_found_for_wrong_key() {
        let cache = make_cache("default", "my-config", 3);
        let req = GetRequest { namespace: "default".into(), name: "other-config".into() };
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

        let req = GetAllRequest { namespace: "default".into() };
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
        let req = GetAllRequest { namespace: "default".into() };
        let resp = handle_get_all(cache, req).await.expect("must succeed");
        let mut stream = resp.into_inner();
        assert!(stream.next().await.is_none());
    }
}
