//! `GetSecret` and `GetAllSecrets` handlers for `KonfigService`.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};
use tracing::debug;

use crate::proto::{GetAllSecretsRequest, GetSecretRequest, SecretResponse};
use crate::secret_cache::SecretCache;
use crate::types::SecretSnapshot;

pub async fn handle_get_secret(
    cache: Arc<SecretCache>,
    req: GetSecretRequest,
) -> Result<Response<SecretResponse>, Status> {
    debug!(namespace = %req.namespace, name = %req.name, "GetSecret RPC");

    let snap = cache.get(&req.namespace, &req.name).ok_or_else(|| {
        Status::not_found(format!("secret {}/{} not found", req.namespace, req.name))
    })?;

    Ok(Response::new(secret_snapshot_to_proto(&snap)))
}

/// Per-RPC mpsc buffer for `GetAllSecrets`. Mirrors `get::GET_ALL_CHANNEL_CAPACITY`
/// — sized to a typical per-namespace snapshot count.
const GET_ALL_SECRETS_CHANNEL_CAPACITY: usize = 256;

pub async fn handle_get_all_secrets(
    cache: Arc<SecretCache>,
    req: GetAllSecretsRequest,
) -> Result<Response<ReceiverStream<Result<SecretResponse, Status>>>, Status> {
    debug!(namespace = %req.namespace, "GetAllSecrets RPC");

    let (tx, rx) = mpsc::channel(GET_ALL_SECRETS_CHANNEL_CAPACITY);
    let entries = cache.all_in_namespace(&req.namespace);

    tokio::spawn(async move {
        for snap in entries {
            // Stop encoding once the client has disconnected — see
            // `handle_get_all` in `grpc/get.rs` for rationale.
            if tx.send(Ok(secret_snapshot_to_proto(&snap))).await.is_err() {
                debug!("GetAllSecrets: subscriber disconnected — stopping early");
                return;
            }
        }
    });

    Ok(Response::new(ReceiverStream::new(rx)))
}

pub fn secret_snapshot_to_proto(snap: &SecretSnapshot) -> SecretResponse {
    // `data_json()` memoises the serialised payload per snapshot via
    // `Arc<OnceLock<String>>` — see `SecretSnapshot::data_json_cache`.
    // The clone here is one alloc instead of a per-event re-walk of `data`
    // and re-run of `serde_json::to_string`.
    SecretResponse {
        namespace: snap.namespace.clone(),
        name: snap.name.clone(),
        schema_version: snap.schema_version,
        data_json: snap.data_json().to_owned(),
        resource_version: snap.resource_version.clone(),
        age_ms: snap.loaded_at.elapsed().as_millis() as i64,
        // -1 sentinel when fresh; elapsed ms since the watcher disconnected
        // otherwise. Mirrors `grpc::snapshot_to_proto` for Config.
        stale_since_ms: snap
            .stale_since
            .map(|t| t.elapsed().as_millis() as i64)
            .unwrap_or(-1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio_stream::StreamExt;

    fn make_cache_with_secret(
        namespace: &str,
        name: &str,
        schema_version: u32,
    ) -> Arc<SecretCache> {
        let cache = Arc::new(SecretCache::new());
        cache.update(crate::types::SecretSnapshot {
            namespace: namespace.to_string(),
            name: name.to_string(),
            schema_version,
            data: [("key1".to_string(), Bytes::from("dmFsdWUx".to_string()))]
                .into_iter()
                .collect(),
            resource_version: "rv-001".to_string(),
            loaded_at: std::time::Instant::now(),
            ..Default::default()
        });
        cache
    }

    #[tokio::test]
    async fn get_secret_returns_response_when_found() {
        let cache = make_cache_with_secret("trading", "api-keys", 3);
        let req = GetSecretRequest {
            namespace: "trading".into(),
            name: "api-keys".into(),
        };
        let resp = handle_get_secret(cache, req).await.expect("must succeed");
        let sr = resp.into_inner();
        assert_eq!(sr.namespace, "trading");
        assert_eq!(sr.name, "api-keys");
        assert_eq!(sr.schema_version, 3);
        assert!(!sr.data_json.is_empty());
    }

    #[tokio::test]
    async fn get_secret_returns_not_found_for_missing_key() {
        let cache = Arc::new(SecretCache::new());
        let req = GetSecretRequest {
            namespace: "trading".into(),
            name: "nonexistent".into(),
        };
        let result = handle_get_secret(cache, req).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn get_all_secrets_streams_entries_in_namespace() {
        let cache = Arc::new(SecretCache::new());
        cache.update(crate::types::SecretSnapshot {
            namespace: "ns".into(),
            name: "sec-1".into(),
            schema_version: 1,
            ..Default::default()
        });
        cache.update(crate::types::SecretSnapshot {
            namespace: "ns".into(),
            name: "sec-2".into(),
            schema_version: 2,
            ..Default::default()
        });

        let req = GetAllSecretsRequest {
            namespace: "ns".into(),
        };
        let resp = handle_get_all_secrets(cache, req)
            .await
            .expect("must succeed");
        let mut stream = resp.into_inner();
        let mut count = 0usize;
        while let Some(item) = stream.next().await {
            assert!(item.is_ok());
            count += 1;
        }
        assert_eq!(count, 2);
    }

    #[test]
    fn secret_snapshot_to_proto_stale_since_sentinel_minus_one_when_fresh() {
        let snap = SecretSnapshot {
            stale_since: None,
            ..Default::default()
        };
        let proto = secret_snapshot_to_proto(&snap);
        assert_eq!(
            proto.stale_since_ms, -1,
            "fresh (None) `stale_since` must emit the -1 sentinel"
        );
    }

    #[test]
    fn secret_snapshot_to_proto_stale_since_non_negative_when_stale() {
        // stale_since set in the past → elapsed ms is positive at conversion.
        let stale_anchor = std::time::Instant::now() - std::time::Duration::from_millis(10);
        let snap = SecretSnapshot {
            stale_since: Some(stale_anchor),
            ..Default::default()
        };
        let proto = secret_snapshot_to_proto(&snap);
        assert!(
            proto.stale_since_ms >= 0,
            "stale_since_ms must be non-negative when stale_since is Some, got {}",
            proto.stale_since_ms
        );
    }
}
