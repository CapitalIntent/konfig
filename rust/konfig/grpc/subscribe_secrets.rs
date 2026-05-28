//! `SubscribeSecrets` handler for `KonfigService`.
//!
//! Architecture: `secret_watcher.rs` runs one kube watch stream per namespace
//! and feeds both `SecretCache` (for Get RPCs) and a shared
//! `broadcast::Sender<SecretEvent>` (for Subscribe RPCs).  This module
//! subscribes to that shared broadcast — no second kube watch stream is opened.
//!
//! Each subscriber gets a `Receiver` clone — O(1) fan-out.
//!
//! `resume_resource_version`: keeps a per-subscriber raw kube watch (resume
//! semantics require per-subscriber starting points that are incompatible with
//! a shared broadcast — same limitation as Config subscribe).

use std::sync::Arc;

use dashmap::DashMap;
use futures_util::StreamExt;
use k8s_openapi::api::core::v1::Secret;
use kube::api::{WatchEvent, WatchParams};
use kube::{Api, Client};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};
use tracing::{debug, info, warn};

use crate::grpc::secret_get::secret_snapshot_to_proto;
use crate::proto::{SecretEvent, SubscribeSecretsRequest, secret_event::EventType};
use crate::secret_watcher::{MANAGED_LABEL, SCHEMA_VERSION_ANNOTATION};
use crate::types::SecretSnapshot;

/// Per-subscriber mpsc capacity — back-pressure for slow readers.
const CHANNEL_CAPACITY: usize = 256;

/// Broadcast ring-buffer capacity — used in tests; kept in sync with
/// `secret_watcher::BROADCAST_CAPACITY`.
#[cfg(test)]
const BROADCAST_CAPACITY: usize = 1_024;

pub async fn handle_subscribe_secrets(
    kube_client: Client,
    namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<SecretEvent>>>,
    req: SubscribeSecretsRequest,
) -> Result<Response<ReceiverStream<Result<SecretEvent, Status>>>, Status> {
    debug!(namespace = %req.namespace, resume_rv = %req.resume_resource_version, "SubscribeSecrets RPC");

    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let namespace = req.namespace.clone();
    let resume_rv = req.resume_resource_version.clone();

    if !resume_rv.is_empty() {
        // Resume path: per-subscriber raw watch (broadcast can't share resume points).
        tokio::spawn(run_raw_watch(kube_client, namespace, resume_rv, tx));
        return Ok(Response::new(ReceiverStream::new(rx)));
    }

    // Broadcast path: subscribe to the shared sender populated by secret_watcher.
    let bcast_rx = get_broadcast_rx(&namespace, &namespace_broadcasts).ok_or_else(|| {
        Status::unavailable(format!(
            "no secret watcher running for namespace {namespace}"
        ))
    })?;

    // Bridge: broadcast::Receiver → mpsc::Sender (this subscriber's gRPC stream).
    tokio::spawn(bridge_broadcast(bcast_rx, tx));

    Ok(Response::new(ReceiverStream::new(rx)))
}

/// Return a broadcast `Receiver` for `namespace` by subscribing to the shared
/// sender that `SecretWatcher::spawn_all` inserted at server startup.
///
/// Returns `None` if the namespace has no running watcher (not configured).
fn get_broadcast_rx(
    namespace: &str,
    namespace_broadcasts: &Arc<DashMap<String, broadcast::Sender<SecretEvent>>>,
) -> Option<broadcast::Receiver<SecretEvent>> {
    namespace_broadcasts.get(namespace).map(|tx| tx.subscribe())
}

/// Forward events from the namespace broadcast to a single subscriber's mpsc.
///
/// Disconnects the subscriber with RESOURCE_EXHAUSTED if:
/// - the mpsc channel is full (subscriber too slow to drain), or
/// - the broadcast ring wrapped before this receiver drained (lagged).
async fn bridge_broadcast(
    mut bcast_rx: broadcast::Receiver<SecretEvent>,
    tx: mpsc::Sender<Result<SecretEvent, Status>>,
) {
    loop {
        match bcast_rx.recv().await {
            Ok(event) => match tx.try_send(Ok(event)) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    warn!("Secret subscriber too slow — disconnecting with RESOURCE_EXHAUSTED");
                    let _ = tx.try_send(Err(Status::resource_exhausted("subscriber too slow")));
                    break;
                }
                Err(TrySendError::Closed(_)) => {
                    info!("Secret subscriber disconnected");
                    break;
                }
            },
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!(missed = n, "Secret subscriber lagged — disconnecting");
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
    resource_version: String,
    tx: mpsc::Sender<Result<SecretEvent, Status>>,
) {
    let api: Api<Secret> = Api::namespaced(kube_client, &namespace);
    let wp = WatchParams::default()
        .timeout(290)
        .labels(&format!("{MANAGED_LABEL}=true"));

    let stream = match api.watch(&wp, &resource_version).await {
        Ok(s) => s,
        Err(e) => {
            warn!("SubscribeSecrets raw watch failed to start: {e}");
            let _ = tx.try_send(Err(Status::unavailable(format!("watch error: {e}"))));
            return;
        }
    };

    let mut stream = stream.boxed();

    while let Some(result) = stream.next().await {
        match result {
            Ok(WatchEvent::Added(secret)) | Ok(WatchEvent::Modified(secret)) => {
                let ns = secret
                    .metadata
                    .namespace
                    .as_deref()
                    .unwrap_or(&namespace)
                    .to_string();
                if !emit_to_mpsc(&tx, EventType::Modified as i32, &secret, &ns).await {
                    break;
                }
            }
            Ok(WatchEvent::Deleted(secret)) => {
                let ns = secret
                    .metadata
                    .namespace
                    .as_deref()
                    .unwrap_or(&namespace)
                    .to_string();
                if !emit_to_mpsc(&tx, EventType::Deleted as i32, &secret, &ns).await {
                    break;
                }
            }
            Ok(WatchEvent::Bookmark(_)) => {
                debug!("SubscribeSecrets: BOOKMARK received — cursor advanced");
            }
            Ok(WatchEvent::Error(e)) => {
                warn!("SubscribeSecrets raw watch error event: {e}");
                let _ = tx.try_send(Err(Status::internal(format!("watch error: {e}"))));
                break;
            }
            Err(e) => {
                warn!("SubscribeSecrets raw watch stream error: {e}");
                break;
            }
        }
    }
}

async fn emit_to_mpsc(
    tx: &mpsc::Sender<Result<SecretEvent, Status>>,
    event_type: i32,
    secret: &Secret,
    namespace: &str,
) -> bool {
    let Some(snap) = parse_secret_object(secret, namespace) else {
        return true;
    };
    let secret_event = SecretEvent {
        event_type,
        secret: Some(secret_snapshot_to_proto(&snap)),
    };
    match tx.try_send(Ok(secret_event)) {
        Ok(()) => true,
        Err(TrySendError::Full(_)) => {
            warn!("Secret subscriber too slow — disconnecting with RESOURCE_EXHAUSTED");
            let _ = tx.try_send(Err(Status::resource_exhausted("subscriber too slow")));
            false
        }
        Err(TrySendError::Closed(_)) => {
            info!("Secret subscriber disconnected — closing watch stream");
            false
        }
    }
}

/// Parse a watched K8s `Secret` object into a `SecretSnapshot`.
///
/// Only returns `Some` for secrets labelled `konfig.io/managed=true`.
/// Values are re-encoded to base64 — never decoded server-side.
fn parse_secret_object(secret: &Secret, namespace: &str) -> Option<SecretSnapshot> {
    // Only process managed secrets.
    let managed = secret
        .metadata
        .labels
        .as_ref()
        .and_then(|l| l.get(MANAGED_LABEL))
        .map(|v| v == "true")
        .unwrap_or(false);
    if !managed {
        return None;
    }

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
    // values opaque on the wire (never decode server-side).
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
    use tokio::sync::broadcast;
    use tokio_stream::StreamExt;

    // ── helpers ───────────────────────────────────────────────────────────────

    fn make_secret_event(namespace: &str, name: &str, event_type: i32) -> SecretEvent {
        let snap = SecretSnapshot {
            name: name.to_string(),
            namespace: namespace.to_string(),
            schema_version: 1,
            resource_version: "rv-001".to_string(),
            ..Default::default()
        };
        SecretEvent {
            event_type,
            secret: Some(secret_snapshot_to_proto(&snap)),
        }
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    /// A single subscriber receives an ADDED event forwarded through the bridge.
    #[tokio::test]
    async fn single_subscriber_receives_added_event() {
        let (bcast_tx, bcast_rx) = broadcast::channel::<SecretEvent>(BROADCAST_CAPACITY);
        let (mpsc_tx, mpsc_rx) = mpsc::channel(CHANNEL_CAPACITY);

        tokio::spawn(bridge_broadcast(bcast_rx, mpsc_tx));

        let event = make_secret_event("trading", "api-keys", EventType::Added as i32);
        bcast_tx.send(event.clone()).unwrap();

        let mut stream = ReceiverStream::new(mpsc_rx);
        let received = stream.next().await.expect("must receive event").unwrap();
        assert_eq!(received.event_type, EventType::Added as i32);
        let secret = received.secret.unwrap();
        assert_eq!(secret.namespace, "trading");
        assert_eq!(secret.name, "api-keys");
    }

    /// A DELETED event is delivered with the correct event_type.
    #[tokio::test]
    async fn deleted_event_is_delivered() {
        let (bcast_tx, bcast_rx) = broadcast::channel::<SecretEvent>(BROADCAST_CAPACITY);
        let (mpsc_tx, mpsc_rx) = mpsc::channel(CHANNEL_CAPACITY);

        tokio::spawn(bridge_broadcast(bcast_rx, mpsc_tx));

        let event = make_secret_event("trading", "api-keys", EventType::Deleted as i32);
        bcast_tx.send(event).unwrap();

        let mut stream = ReceiverStream::new(mpsc_rx);
        let received = stream.next().await.expect("must receive event").unwrap();
        assert_eq!(received.event_type, EventType::Deleted as i32);
    }

    /// Multiple subscribers in the same namespace all receive the same event.
    #[tokio::test]
    async fn multiple_subscribers_all_receive_event() {
        let (bcast_tx, _) = broadcast::channel::<SecretEvent>(BROADCAST_CAPACITY);

        const N: usize = 4;
        let mut streams = Vec::with_capacity(N);
        for _ in 0..N {
            let bcast_rx = bcast_tx.subscribe();
            let (mpsc_tx, mpsc_rx) = mpsc::channel(CHANNEL_CAPACITY);
            tokio::spawn(bridge_broadcast(bcast_rx, mpsc_tx));
            streams.push(ReceiverStream::new(mpsc_rx));
        }

        let event = make_secret_event("trading", "api-keys", EventType::Modified as i32);
        bcast_tx.send(event).unwrap();

        for mut stream in streams {
            let received = stream
                .next()
                .await
                .expect("subscriber must receive event")
                .unwrap();
            assert_eq!(received.event_type, EventType::Modified as i32);
        }
    }

    /// A slow subscriber receives RESOURCE_EXHAUSTED when it lags behind the
    /// broadcast ring.
    ///
    /// Strategy: create a broadcast channel with capacity 1, then flood it with
    /// more events than it can hold.  The receiver that hasn't drained will lag.
    /// When `bridge_broadcast` detects `RecvError::Lagged`, it disconnects the
    /// subscriber with `RESOURCE_EXHAUSTED` — a code path that is guaranteed
    /// regardless of mpsc scheduling.
    #[tokio::test]
    async fn slow_subscriber_is_disconnected_with_resource_exhausted() {
        // Ring capacity of 1: the second send overwrites the first, causing
        // RecvError::Lagged for any receiver that hasn't drained yet.
        let (bcast_tx, bcast_rx) = broadcast::channel::<SecretEvent>(1);
        let (mpsc_tx, mpsc_rx) = mpsc::channel::<Result<SecretEvent, Status>>(CHANNEL_CAPACITY);

        tokio::spawn(bridge_broadcast(bcast_rx, mpsc_tx));

        // Flood the ring: bcast_rx has not drained event 1 before event 2
        // overwrites it.  On the next recv(), bridge gets Lagged.
        for i in 0..3 {
            let event = make_secret_event("trading", &format!("sec-{i}"), EventType::Added as i32);
            let _ = bcast_tx.send(event); // may return Err if no receivers — ignore
        }

        // Drain until we see RESOURCE_EXHAUSTED.
        let mut stream = ReceiverStream::new(mpsc_rx);
        let mut got_exhausted = false;
        while let Some(item) = stream.next().await {
            if let Err(status) = item {
                assert_eq!(status.code(), tonic::Code::ResourceExhausted);
                got_exhausted = true;
                break;
            }
        }
        assert!(
            got_exhausted,
            "expected RESOURCE_EXHAUSTED disconnect for lagged subscriber"
        );
    }

    /// get_broadcast_rx returns Some when a sender exists in the map.
    #[test]
    fn get_broadcast_rx_returns_some_when_sender_exists() {
        let broadcasts = Arc::new(DashMap::new());
        let (tx, _rx) = broadcast::channel::<SecretEvent>(BROADCAST_CAPACITY);
        broadcasts.insert("trading".to_string(), tx);

        let result = get_broadcast_rx("trading", &broadcasts);
        assert!(result.is_some());
    }

    /// get_broadcast_rx returns None when no sender exists.
    #[test]
    fn get_broadcast_rx_returns_none_when_no_sender() {
        let broadcasts = Arc::new(DashMap::<String, broadcast::Sender<SecretEvent>>::new());
        let result = get_broadcast_rx("missing-ns", &broadcasts);
        assert!(result.is_none());
    }

    /// parse_secret_object rejects secrets without the managed label.
    #[test]
    fn parse_secret_object_rejects_unmanaged() {
        let secret = Secret::default();
        assert!(parse_secret_object(&secret, "ns").is_none());
    }

    /// parse_secret_object accepts secrets with the managed label.
    #[test]
    fn parse_secret_object_accepts_managed() {
        use std::collections::BTreeMap;
        let mut secret = Secret::default();
        secret.metadata.name = Some("my-secret".to_string());
        secret.metadata.labels = Some({
            let mut m = BTreeMap::new();
            m.insert(MANAGED_LABEL.to_string(), "true".to_string());
            m
        });
        let snap = parse_secret_object(&secret, "ns");
        assert!(snap.is_some());
        assert_eq!(snap.unwrap().name, "my-secret");
    }

    /// Values are passed through as base64 — never decoded.
    #[test]
    fn parse_secret_object_values_are_base64() {
        use k8s_openapi::ByteString;
        use std::collections::BTreeMap;

        let mut secret = Secret::default();
        secret.metadata.name = Some("sec".to_string());
        secret.metadata.labels = Some({
            let mut m = BTreeMap::new();
            m.insert(MANAGED_LABEL.to_string(), "true".to_string());
            m
        });
        secret.data = Some({
            let mut d = BTreeMap::new();
            d.insert("key".to_string(), ByteString(b"plaintext".to_vec()));
            d
        });

        let snap = parse_secret_object(&secret, "ns").unwrap();
        let val = snap.data.get("key").unwrap();
        let s = std::str::from_utf8(val).unwrap();
        // Must be base64, not plaintext.
        assert_ne!(s, "plaintext");
        assert_eq!(s, "cGxhaW50ZXh0");
    }
}
