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

    let snap = cache.load();

    if snap.resource_version.is_empty() && snap.schema_version == 0 {
        warn!(namespace = %req.namespace, name = %req.name, "Get: cache not yet populated");
        return Err(Status::not_found(format!(
            "config {}/{} not found — cache not yet populated",
            req.namespace, req.name
        )));
    }

    Ok(Response::new(snapshot_to_proto(&snap)))
}

pub async fn handle_get_all(
    cache: Arc<ConfigCache>,
    req: GetAllRequest,
) -> Result<Response<ReceiverStream<Result<Config, Status>>>, Status> {
    debug!(namespace = %req.namespace, "GetAll RPC");

    let (tx, rx) = mpsc::channel(1);

    tokio::spawn(async move {
        let snap = cache.load();
        if !snap.resource_version.is_empty() || snap.schema_version > 0 {
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

    fn make_cache(schema_version: u32) -> Arc<ConfigCache> {
        let snap = ConfigSnapshot {
            name: "my-config".into(),
            namespace: "default".into(),
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
        let cache = make_cache(3);
        let req = GetRequest { namespace: "default".into(), name: "my-config".into() };
        let resp = handle_get(cache, req).await.expect("must succeed");
        let cfg = resp.into_inner();
        assert_eq!(cfg.schema_version, 3);
        assert_eq!(cfg.name, "my-config");
        assert!(!cfg.content_json.is_empty());
    }

    #[tokio::test]
    async fn get_returns_not_found_when_cache_empty() {
        let cache = make_cache(0);
        let req = GetRequest { namespace: "default".into(), name: "my-config".into() };
        let result = handle_get(cache, req).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn get_all_streams_entry_when_populated() {
        use tokio_stream::StreamExt;
        let cache = make_cache(5);
        let req = GetAllRequest { namespace: "default".into() };
        let resp = handle_get_all(cache, req).await.expect("must succeed");
        let mut stream = resp.into_inner();
        let item = stream.next().await.expect("one item").expect("no error");
        assert_eq!(item.schema_version, 5);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn get_all_empty_when_cache_unpopulated() {
        use tokio_stream::StreamExt;
        let cache = make_cache(0);
        let req = GetAllRequest { namespace: "default".into() };
        let resp = handle_get_all(cache, req).await.expect("must succeed");
        let mut stream = resp.into_inner();
        assert!(stream.next().await.is_none());
    }
}
