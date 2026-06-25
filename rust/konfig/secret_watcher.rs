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
use crate::secret_cache::{SecretCache, SecretMutation};
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
                // Mark every cached secret stale on each disconnect; a fresh
                // event after reconnect clears it via `update`. Mirrors the
                // Config watcher's `cache.mark_all_stale()` on stream error.
                let cache_stale = Arc::clone(&cache);
                run_with_reconnect(
                    "secret",
                    namespace.clone(),
                    move || cache_stale.mark_all_stale(),
                    |attempt| {
                        run_namespace_watcher(
                            client.clone(),
                            Arc::clone(&cache),
                            namespace.clone(),
                            tx.clone(),
                            Arc::clone(&last_event_at),
                            attempt > 0,
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
    batch_relist: bool,
) -> Result<(), kube_watcher::Error> {
    let api: Api<Secret> = Api::namespaced(client, &namespace);
    let wc = kube_watcher::Config::default().labels(&format!("{MANAGED_LABEL}=true"));
    let stream = kube_watch_stream(api, wc).boxed();

    info!(namespace = %namespace, "Secret watcher started");

    pump_secret_events(
        stream,
        &cache,
        &namespace,
        &broadcast_tx,
        &last_event_at,
        batch_relist,
    )
    .await
}

/// Apply one Secret watcher event to the cache + broadcast channel.
/// Extracted so [`pump_secret_events`] stays a thin event loop and the
/// per-event behaviour is unit-testable.
///
/// OTEL child span (Phase 7, CU-86ahzwj3k) per Secret watch event, mirroring
/// the Config watcher. `level = "debug"` keeps it off the INFO production path;
/// `skip_all` keeps the `Secret` payload out of the span. `event_type` /
/// `resource_version` are recorded as borrowed `&str` — no per-event heap alloc.
#[tracing::instrument(
    level = "debug",
    name = "konfig.secret_watch_event",
    skip_all,
    fields(event_type, resource_version)
)]
pub(crate) fn handle_secret_event(
    event: Event<Secret>,
    cache: &Arc<SecretCache>,
    namespace: &str,
    broadcast_tx: &broadcast::Sender<SecretEvent>,
) {
    let span = tracing::Span::current();
    match event {
        Event::Apply(secret) | Event::InitApply(secret) => {
            span.record("event_type", "Apply");
            span.record(
                "resource_version",
                secret.metadata.resource_version.as_deref().unwrap_or(""),
            );
            if let Some(snap) = parse_secret(&secret, namespace) {
                // debug! not info!: this fires per secret event in the
                // watcher hot loop. Operators can flip RUST_LOG=konfig=debug
                // when they need per-event signal.
                debug!(
                    name = %snap.name,
                    schema_version = snap.schema_version,
                    "Secret applied",
                );
                let secret_event = SecretEvent {
                    event_type: EventType::Modified as i32,
                    secret: Some(secret_snapshot_to_proto(&snap)),
                };
                cache.update(snap);
                // Ignore Err — means zero receivers at the moment.
                let _ = broadcast_tx.send(secret_event);
            }
        }
        Event::Delete(secret) => {
            span.record("event_type", "Delete");
            span.record(
                "resource_version",
                secret.metadata.resource_version.as_deref().unwrap_or(""),
            );
            let name = secret.metadata.name.as_deref().unwrap_or("<unknown>");
            // Intentionally not removing from cache on delete — CP behavior:
            // serve stale secret rather than returning NotFound during a
            // partition. Tracked in W4 (86ahpgaw3).
            warn!(name, "Secret deleted — cache retains last-known-good");
            if let Some(snap) = parse_secret(&secret, namespace) {
                let secret_event = SecretEvent {
                    event_type: EventType::Deleted as i32,
                    secret: Some(secret_snapshot_to_proto(&snap)),
                };
                let _ = broadcast_tx.send(secret_event);
            }
        }
        Event::Init | Event::InitDone => {
            span.record("event_type", "Init");
            debug!("Secret watch stream: init");
        }
    }
}

/// Drive a Secret watcher stream to completion or first error.
///
/// On every event: `last_event_at` is touched (including Init/InitDone — the
/// freshness signal reflects API connectivity, not parse validity), then the
/// event is dispatched via [`handle_secret_event`].
///
/// Extracted from [`run_namespace_watcher`] so the per-event behaviour is
/// unit-testable against a synthetic stream — no kube API connection
/// required.
pub(crate) async fn pump_secret_events<S>(
    mut stream: S,
    cache: &Arc<SecretCache>,
    namespace: &str,
    broadcast_tx: &broadcast::Sender<SecretEvent>,
    last_event_at: &Arc<LastEventAt>,
    batch_relist: bool,
) -> Result<(), kube_watcher::Error>
where
    S: futures_util::stream::TryStream<Ok = Event<Secret>, Error = kube_watcher::Error> + Unpin,
{
    // On a watch RESTART (reconnect) the relist (Init -> InitApply* -> InitDone)
    // is committed to the cache with ONE `apply_batch` clone+swap instead of one
    // clone per secret (CU-86aj7k61x). Each secret is STILL broadcast
    // individually so subscribers observe per-secret Modified events exactly as
    // before — only the cache write is coalesced. Cold start keeps the per-event
    // path so the readiness gate is unchanged; a relist cut short by a stream
    // error drops the buffer and the reconnect re-lists.
    let mut relist: Option<Vec<SecretMutation>> = None;
    while let Some(event) = stream.try_next().await? {
        last_event_at.touch();
        route_secret_event(
            event,
            cache,
            namespace,
            broadcast_tx,
            &mut relist,
            batch_relist,
        );
    }
    Ok(())
}

/// Route one Secret watch event, batching the reconnect relist.
///
/// `relist` is `Some` only between `Init` and `InitDone` on a reconnect
/// (`batch_relist`): `InitApply` secrets are buffered (and STILL broadcast
/// individually so subscribers see per-secret Modified events) and `InitDone`
/// commits the buffer with one `apply_batch`. Every other case falls through to
/// the per-event [`handle_secret_event`], preserving the historical path.
fn route_secret_event(
    event: Event<Secret>,
    cache: &Arc<SecretCache>,
    namespace: &str,
    broadcast_tx: &broadcast::Sender<SecretEvent>,
    relist: &mut Option<Vec<SecretMutation>>,
    batch_relist: bool,
) {
    if let Some(buf) = relist.as_mut() {
        match event {
            Event::InitApply(secret) => {
                if let Some(snap) = parse_secret(&secret, namespace) {
                    let secret_event = SecretEvent {
                        event_type: EventType::Modified as i32,
                        secret: Some(secret_snapshot_to_proto(&snap)),
                    };
                    buf.push(SecretMutation::Upsert(snap));
                    // Ignore Err — means zero receivers at the moment.
                    let _ = broadcast_tx.send(secret_event);
                }
            }
            Event::InitDone => {
                let n = cache.apply_batch(relist.take().expect("relist buffer set"));
                debug!(events = n, "Secret watch relist committed as one batch");
            }
            other => handle_secret_event(other, cache, namespace, broadcast_tx),
        }
    } else if batch_relist && matches!(event, Event::Init) {
        *relist = Some(Vec::new());
    } else {
        handle_secret_event(event, cache, namespace, broadcast_tx);
    }
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
        ..Default::default()
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

    // ── pump_secret_events ───────────────────────────────────────────────────

    fn secret_with_data(name: &str, key: &str, value: &[u8], schema_version: u32) -> Secret {
        let mut data = BTreeMap::new();
        data.insert(key.to_string(), ByteString(value.to_vec()));
        make_secret_obj(name, data, schema_version)
    }

    fn watcher_err() -> kube_watcher::Error {
        kube_watcher::Error::WatchFailed(kube::Error::Api(kube::core::ErrorResponse {
            status: "Failure".to_string(),
            message: "synthetic".to_string(),
            reason: "synthetic".to_string(),
            code: 500,
        }))
    }

    #[tokio::test]
    async fn pump_apply_event_updates_cache_and_broadcasts_modified() {
        use futures_util::stream;
        let cache = Arc::new(SecretCache::new());
        let (tx, mut rx) = broadcast::channel(8);
        let lea = Arc::new(LastEventAt::new());
        // Freshness gauge starts uninitialised — must transition to "set"
        // after the first event so `konfig_stale_seconds` reflects API
        // connectivity, not pod-uptime.
        assert!(lea.elapsed_secs().is_none(), "precondition: no event yet");

        let events: Vec<Result<Event<Secret>, kube_watcher::Error>> =
            vec![Ok(Event::Apply(secret_with_data("s-a", "k", b"v1", 1)))];

        // Strengthen `res.is_ok()` to an exhaustive match — a regression
        // that wraps the success in a different variant (or returns Ok(())
        // prematurely without consuming the stream) is still possible after
        // future refactors.
        let res = pump_secret_events(stream::iter(events), &cache, "ns", &tx, &lea, false).await;
        match res {
            Ok(()) => {}
            Err(e) => panic!("clean-stream end must be Ok(()), got Err({e:?})"),
        }

        // (a) Cache: synthetic Apply must materialise the snapshot under the
        // (namespace, name) key with the schema_version from the annotation.
        // The value must round-trip as base64-encoded bytes (`djE=` ==
        // b64("v1")), not as plaintext.
        let cached = cache
            .get("ns", "s-a")
            .expect("Apply must populate (ns, name) entry in cache");
        assert_eq!(cached.schema_version, 1);
        let val = cached
            .data
            .get("k")
            .expect("synthetic data key must round-trip into cache");
        assert_eq!(
            std::str::from_utf8(val).unwrap(),
            "djE=",
            "cache stores base64-encoded value, not plaintext",
        );

        // (b) Broadcast: exactly one event of type Modified — Apply maps to
        // Modified in the proto (Added is reserved for the separate "new
        // subscriber initial snapshot" path).
        let evt = rx.try_recv().expect("must broadcast Modified");
        assert_eq!(evt.event_type, EventType::Modified as i32);
        let bcast_secret = evt.secret.as_ref().expect("event must carry payload");
        assert_eq!(bcast_secret.name, "s-a");
        assert_eq!(bcast_secret.namespace, "ns");
        assert_eq!(bcast_secret.schema_version, 1);
        assert!(
            matches!(rx.try_recv(), Err(broadcast::error::TryRecvError::Empty)),
            "Apply must produce exactly one broadcast event",
        );

        // (c) Freshness: gauge must have been touched (Some, not None)
        // because pump_secret_events touches it on every event before
        // dispatching to handle_secret_event.
        assert!(
            lea.elapsed_secs().is_some(),
            "Apply event must touch last_event_at — konfig_stale_seconds tracks this",
        );
    }

    #[tokio::test]
    async fn pump_delete_event_broadcasts_deleted_but_retains_cache() {
        use futures_util::stream;
        let cache = Arc::new(SecretCache::new());
        let (tx, mut rx) = broadcast::channel(8);
        let lea = Arc::new(LastEventAt::new());
        let s = secret_with_data("s-a", "k", b"v", 7);
        let events: Vec<Result<Event<Secret>, kube_watcher::Error>> =
            vec![Ok(Event::Apply(s.clone())), Ok(Event::Delete(s))];

        let res = pump_secret_events(stream::iter(events), &cache, "ns", &tx, &lea, false).await;
        assert!(res.is_ok());
        // Cache retains last-known-good (documented CP behaviour).
        assert_eq!(cache.get("ns", "s-a").unwrap().schema_version, 7);
        let first = rx.try_recv().expect("apply broadcast");
        assert_eq!(first.event_type, EventType::Modified as i32);
        let second = rx.try_recv().expect("delete broadcast");
        assert_eq!(second.event_type, EventType::Deleted as i32);
    }

    #[tokio::test]
    async fn pump_propagates_stream_error_and_halts() {
        use futures_util::stream;
        let cache = Arc::new(SecretCache::new());
        let (tx, mut rx) = broadcast::channel(8);
        let lea = Arc::new(LastEventAt::new());
        let events: Vec<Result<Event<Secret>, kube_watcher::Error>> = vec![
            Ok(Event::Apply(secret_with_data("s-a", "k", b"v", 1))),
            Err(watcher_err()),
            // Must NOT be observed — pump returns on first Err.
            Ok(Event::Apply(secret_with_data("s-c", "k", b"v", 99))),
        ];

        let res = pump_secret_events(stream::iter(events), &cache, "ns", &tx, &lea, false).await;
        // Strengthen `is_err()` to a precise variant match — the kube
        // runtime watcher distinguishes WatchFailed (this case), InitFailed,
        // TooManyObjects etc. The reconnect loop in `run_with_reconnect`
        // backs off uniformly for now, but downstream metric labelling and
        // alert routing depend on the variant being preserved through the
        // pump.
        let err = res.expect_err("stream error must propagate, not be swallowed");
        assert!(
            matches!(err, kube_watcher::Error::WatchFailed(_)),
            "expected kube::runtime::watcher::Error::WatchFailed, got: {err:?}",
        );

        // Pre-error event landed in cache; post-error event must NOT
        // (pump halts on first Err — `try_next?` short-circuits).
        assert_eq!(cache.get("ns", "s-a").unwrap().schema_version, 1);
        assert!(cache.get("ns", "s-c").is_none());
        let _ = rx.try_recv().expect("pre-error broadcast lands");
        assert!(
            matches!(rx.try_recv(), Err(broadcast::error::TryRecvError::Empty)),
            "no post-error broadcast must land",
        );
    }

    #[tokio::test]
    async fn pump_touches_last_event_at_on_every_event_including_init() {
        use futures_util::stream;
        let cache = Arc::new(SecretCache::new());
        let (tx, _rx) = broadcast::channel(8);
        let lea = Arc::new(LastEventAt::new());
        assert!(lea.elapsed_secs().is_none());

        let events: Vec<Result<Event<Secret>, kube_watcher::Error>> = vec![Ok(Event::Init)];
        let _ = pump_secret_events(stream::iter(events), &cache, "ns", &tx, &lea, false).await;
        assert!(
            lea.elapsed_secs().is_some(),
            "Init must touch freshness — gauge reflects connectivity",
        );
    }

    #[tokio::test]
    async fn pump_broadcast_send_with_zero_receivers_is_silently_dropped() {
        use futures_util::stream;
        let cache = Arc::new(SecretCache::new());
        let (tx, rx) = broadcast::channel(8);
        // Drop receiver immediately so the channel reports zero subscribers.
        drop(rx);
        let lea = Arc::new(LastEventAt::new());
        let events: Vec<Result<Event<Secret>, kube_watcher::Error>> =
            vec![Ok(Event::Apply(secret_with_data("s-a", "k", b"v", 4)))];

        let res = pump_secret_events(stream::iter(events), &cache, "ns", &tx, &lea, false).await;
        assert!(res.is_ok());
        // Cache still updates even when no subscribers — broadcast errs only get logged.
        assert_eq!(cache.get("ns", "s-a").unwrap().schema_version, 4);
    }

    #[tokio::test]
    async fn pump_empty_stream_is_ok() {
        use futures_util::stream;
        let cache = Arc::new(SecretCache::new());
        let (tx, _rx) = broadcast::channel(8);
        let lea = Arc::new(LastEventAt::new());
        let events: Vec<Result<Event<Secret>, kube_watcher::Error>> = vec![];

        let res = pump_secret_events(stream::iter(events), &cache, "ns", &tx, &lea, false).await;
        assert!(res.is_ok());
        assert!(lea.elapsed_secs().is_none());
    }

    #[tokio::test]
    async fn reconnect_relist_commits_as_single_batch_and_broadcasts_each() {
        use futures_util::stream;
        let cache = Arc::new(SecretCache::new());
        let (tx, mut rx) = broadcast::channel(8);
        let lea = Arc::new(LastEventAt::new());
        let events: Vec<Result<Event<Secret>, kube_watcher::Error>> = vec![
            Ok(Event::Init),
            Ok(Event::InitApply(secret_with_data("s-a", "k", b"v", 1))),
            Ok(Event::InitApply(secret_with_data("s-b", "k", b"v", 2))),
            Ok(Event::InitDone),
        ];
        // batch_relist = true: the relist commits to the cache atomically.
        let res = pump_secret_events(stream::iter(events), &cache, "ns", &tx, &lea, true).await;
        assert!(res.is_ok());
        assert_eq!(cache.get("ns", "s-a").unwrap().schema_version, 1);
        assert_eq!(cache.get("ns", "s-b").unwrap().schema_version, 2);
        // Two secrets, exactly ONE cache clone+swap...
        assert_eq!(cache.write_count(), 1);
        // ...but each secret is still broadcast individually to subscribers.
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok());
    }
}
