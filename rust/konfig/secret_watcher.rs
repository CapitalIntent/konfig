//! Watches K8s Secrets labeled `konfig.io/managed=true` across configured namespaces.
//!
//! Spawns one watcher task per namespace.
//! Schema version is read from annotation `konfig.io/schema-version`.
//!
//! Each namespace watcher feeds both the [`SecretCache`] and a shared
//! `broadcast::Sender<SecretEvent>` so that `SubscribeSecrets` subscribers
//! receive live events without a second kube watch stream.

use std::sync::Arc;

use dashmap::DashMap;
use futures_util::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::Secret;
use kube::runtime::watcher::{self as kube_watcher, Event, watcher as kube_watch_stream};
use kube::{Api, Client};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::grpc::secret_get::secret_snapshot_to_proto;
use crate::metrics::{LastEventAt, LastEventAtMap, last_event_at_for};
use crate::proto::{SecretEvent, secret_event::EventType};
use crate::secret_cache::SecretCache;
use crate::types::SecretSnapshot;
use crate::watcher::run_with_reconnect;

pub const MANAGED_LABEL: &str = "konfig.io/managed";
pub const SCHEMA_VERSION_ANNOTATION: &str = "konfig.io/schema-version";

/// Broadcast ring-buffer capacity — must match `subscribe_secrets::BROADCAST_CAPACITY`.
const BROADCAST_CAPACITY: usize = 1_024;

pub struct SecretWatcher {
    client: Client,
}

impl SecretWatcher {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    /// Spawn one watcher task per namespace.  Each runs as a [`tokio::spawn`] task.
    ///
    /// For each namespace a `broadcast::Sender<SecretEvent>` is inserted into
    /// `broadcasts` before the task starts, so `SubscribeSecrets` callers can
    /// subscribe immediately at server startup.
    ///
    /// Each namespace also gets a `LastEventAt` entry inserted into
    /// `last_event_at_map` — touched on every event so the
    /// `konfig_stale_seconds` sampler in `grpc::serve` can observe per-namespace
    /// freshness.
    pub fn spawn_all(
        self,
        cache: Arc<SecretCache>,
        namespaces: Vec<String>,
        broadcasts: Arc<DashMap<String, broadcast::Sender<SecretEvent>>>,
        last_event_at_map: LastEventAtMap,
    ) {
        for namespace in namespaces {
            let client = self.client.clone();
            let cache = Arc::clone(&cache);
            let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
            broadcasts.insert(namespace.clone(), tx.clone());
            let last_event_at = last_event_at_for(&last_event_at_map, &namespace);
            // Outer reconnect loop — `run_namespace_watcher` returns on clean
            // stream end (Ok) or any stream error (Err). Either way, retry
            // with backoff so a single failure does not silently kill secret
            // delivery for the namespace.
            tokio::spawn(async move {
                run_with_reconnect(
                    "secret",
                    namespace.clone(),
                    || {},
                    |_attempt| {
                        run_namespace_watcher(
                            client.clone(),
                            Arc::clone(&cache),
                            namespace.clone(),
                            tx.clone(),
                            Arc::clone(&last_event_at),
                        )
                    },
                )
                .await;
            });
        }
    }
}

async fn run_namespace_watcher(
    client: Client,
    cache: Arc<SecretCache>,
    namespace: String,
    broadcast_tx: broadcast::Sender<SecretEvent>,
    last_event_at: Arc<LastEventAt>,
) -> Result<(), kube_watcher::Error> {
    let api: Api<Secret> = Api::namespaced(client, &namespace);
    let wc = kube_watcher::Config::default().labels(&format!("{MANAGED_LABEL}=true"));
    let mut stream = kube_watch_stream(api, wc).boxed();

    info!(namespace = %namespace, "Secret watcher started");

    while let Some(event) = stream.try_next().await? {
        // Touch freshness on every event — including Init/InitDone — so the
        // konfig_stale_seconds gauge reflects "still connected to the API".
        last_event_at.touch();
        match event {
            Event::Apply(secret) | Event::InitApply(secret) => {
                if let Some(snap) = parse_secret(&secret, &namespace) {
                    info!(
                        name = %snap.name,
                        schema_version = snap.schema_version,
                        "Secret applied",
                    );
                    cache.update(snap.clone());
                    let secret_event = SecretEvent {
                        event_type: EventType::Modified as i32,
                        secret: Some(secret_snapshot_to_proto(&snap)),
                    };
                    // Ignore Err — means zero receivers at the moment.
                    let _ = broadcast_tx.send(secret_event);
                }
            }
            Event::Delete(secret) => {
                let name = secret.metadata.name.as_deref().unwrap_or("<unknown>");
                // Intentionally not removing from cache on delete — CP behavior:
                // serve stale secret rather than returning NotFound during a partition.
                // Tracked in W4 (86ahpgaw3).
                warn!(name, "Secret deleted — cache retains last-known-good");
                if let Some(snap) = parse_secret(&secret, &namespace) {
                    let secret_event = SecretEvent {
                        event_type: EventType::Deleted as i32,
                        secret: Some(secret_snapshot_to_proto(&snap)),
                    };
                    let _ = broadcast_tx.send(secret_event);
                }
            }
            Event::Init | Event::InitDone => debug!("Secret watch stream: init"),
        }
    }
    Ok(())
}

fn parse_secret(secret: &Secret, namespace: &str) -> Option<SecretSnapshot> {
    let resource_version = secret.metadata.resource_version.clone().unwrap_or_default();
    let name = secret.metadata.name.clone().unwrap_or_default();

    let schema_version: u32 = secret
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(SCHEMA_VERSION_ANNOTATION))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    // K8s API provides secret.data as raw bytes; re-encode to base64 to keep
    // values opaque on the wire.
    let data = secret
        .data
        .as_ref()
        .map(|d| {
            d.iter()
                .map(|(k, v)| {
                    use base64::Engine;
                    let b64 = base64::engine::general_purpose::STANDARD.encode(&v.0);
                    (k.clone(), bytes::Bytes::from(b64))
                })
                .collect()
        })
        .unwrap_or_default();

    Some(SecretSnapshot {
        name,
        namespace: namespace.to_string(),
        schema_version,
        data,
        resource_version,
        loaded_at: std::time::Instant::now(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::ByteString;
    use std::collections::BTreeMap;

    fn make_secret_obj(
        name: &str,
        data: BTreeMap<String, ByteString>,
        schema_version: u32,
    ) -> Secret {
        let mut s = Secret::default();
        s.metadata.name = Some(name.to_string());
        s.metadata.resource_version = Some("rv-001".to_string());
        s.metadata.annotations = Some({
            let mut a = BTreeMap::new();
            a.insert(
                SCHEMA_VERSION_ANNOTATION.to_string(),
                schema_version.to_string(),
            );
            a
        });
        s.data = Some(data);
        s
    }

    #[test]
    fn parse_secret_encodes_values_as_base64() {
        let mut data = BTreeMap::new();
        data.insert("api_key".to_string(), ByteString(b"supersecret".to_vec()));
        let secret = make_secret_obj("my-secret", data, 2);
        let snap = parse_secret(&secret, "trading").unwrap();
        assert_eq!(snap.schema_version, 2);
        assert_eq!(snap.name, "my-secret");
        let val = snap.data.get("api_key").unwrap();
        let s = std::str::from_utf8(val).unwrap();
        assert_ne!(s, "supersecret", "value must be base64, not plaintext");
        assert_eq!(s, "c3VwZXJzZWNyZXQ=");
    }

    #[test]
    fn parse_secret_no_data_returns_empty_map() {
        let mut s = Secret::default();
        s.metadata.name = Some("empty".to_string());
        let snap = parse_secret(&s, "ns").unwrap();
        assert!(snap.data.is_empty());
    }

    #[test]
    fn parse_secret_missing_annotation_defaults_to_zero() {
        let mut s = Secret::default();
        s.metadata.name = Some("no-version".to_string());
        let snap = parse_secret(&s, "ns").unwrap();
        assert_eq!(snap.schema_version, 0);
    }
}
