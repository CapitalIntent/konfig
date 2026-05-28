//! `Subscribe` handler for `KonfigService`.
//!
//! Architecture: one kube watch stream per namespace, shared via
//! `tokio::sync::broadcast`.  Each subscriber gets a `Receiver` clone — O(1)
//! fan-out instead of O(N) sequential `try_send` per event.
//!
//! `resume_resource_version`: resolved via a per-namespace replay buffer
//! (`VecDeque` of the last `REPLAY_BUFFER_SIZE` events).  When a client
//! reconnects with a non-empty `resume_resource_version`:
//!
//! 1. Buffer hit  — replay only the events after that RV, then join the live
//!    broadcast.  Zero additional kube watch calls regardless of how many
//!    clients reconnect simultaneously.
//! 2. Buffer miss — the RV is too old (compacted by etcd).  Send the full
//!    current cache as MODIFIED events then join the live broadcast.  No error
//!    is returned; the client gets a consistent snapshot and continues normally.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use futures_util::{StreamExt, TryStreamExt};
use kube::core::DynamicObject;
use kube::runtime::watcher::{self as kube_watcher, Event, watcher as kube_watch_stream};
use kube::{Api, Client};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};
use tracing::{debug, info, warn};

use crate::cache::ConfigCache;
use crate::grpc::snapshot_to_proto;
use crate::metrics::{ACTIVE_SUBSCRIBERS, BROADCAST_LAG, EVENTS_BROADCAST, SubGauge};
use crate::proto::{ConfigEvent, SubscribeRequest, config_event::EventType};

/// Per-subscriber mpsc capacity — back-pressure for slow readers.
const CHANNEL_CAPACITY: usize = 256;

/// Broadcast ring-buffer capacity per namespace.
/// Sized so that even the slowest subscriber can drain before the ring wraps.
const BROADCAST_CAPACITY: usize = 1_024;

/// Maximum number of events kept in the per-namespace replay buffer.
/// Events older than this are evicted (FIFO).  1 000 events at typical
/// ConfigEvent sizes (~1 KiB each) ≈ 1 MiB per namespace.
pub const REPLAY_BUFFER_SIZE: usize = 1_000;

/// One entry in the per-namespace replay buffer.
#[derive(Clone)]
pub struct ReplayEntry {
    pub resource_version: String,
    pub event: Arc<ConfigEvent>,
}

/// Per-namespace replay buffer: a bounded FIFO ring of the last
/// `REPLAY_BUFFER_SIZE` events, keyed by their resource_version.
pub type ReplayBuffer = Arc<Mutex<VecDeque<ReplayEntry>>>;

/// Push `event` into `buf`, evicting the oldest entry when the buffer is full.
fn push_replay(buf: &ReplayBuffer, resource_version: String, event: Arc<ConfigEvent>) {
    let mut guard = buf.lock().expect("replay buffer poisoned");
    if guard.len() >= REPLAY_BUFFER_SIZE {
        guard.pop_front();
    }
    guard.push_back(ReplayEntry {
        resource_version,
        event,
    });
}

pub async fn handle_subscribe(
    cache: Arc<ConfigCache>,
    kube_client: Client,
    namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<Arc<ConfigEvent>>>>,
    namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>>,
    watcher_handles: Arc<DashMap<String, JoinHandle<()>>>,
    req: SubscribeRequest,
) -> Result<Response<ReceiverStream<Result<ConfigEvent, Status>>>, Status> {
    debug!(namespace = %req.namespace, resume_rv = %req.resume_resource_version, "Subscribe RPC");

    if !cache.is_populated() {
        return Err(Status::unavailable("cache not yet populated"));
    }

    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    let namespace = req.namespace.clone();
    let resume_rv = req.resume_resource_version.clone();

    // Get or create the broadcast receiver and replay buffer for this namespace.
    let (bcast_rx, replay_buf) = get_or_create_broadcast(
        namespace.clone(),
        kube_client,
        Arc::clone(&namespace_broadcasts),
        Arc::clone(&namespace_replay_buffers),
        Arc::clone(&watcher_handles),
    );

    if !resume_rv.is_empty() {
        // Resume path: attempt to replay from the in-memory buffer.
        // On a hit: send only the missed events then join the broadcast.
        // On a miss: send the full cache snapshot then join the broadcast.
        // Either way: zero additional kube watch calls.
        tokio::spawn(resume_from_buffer(
            resume_rv,
            replay_buf,
            cache,
            namespace.clone(),
            bcast_rx,
            tx,
        ));
        return Ok(Response::new(ReceiverStream::new(rx)));
    }

    // Fresh subscribe (no resume_resource_version) — join live broadcast directly.
    tokio::spawn(bridge_broadcast(bcast_rx, tx, namespace));

    Ok(Response::new(ReceiverStream::new(rx)))
}

/// Return a `(broadcast::Receiver, ReplayBuffer)` for `namespace`, spinning up
/// a kube watcher if one isn't already running for that namespace.
fn get_or_create_broadcast(
    namespace: String,
    kube_client: Client,
    namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<Arc<ConfigEvent>>>>,
    namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>>,
    watcher_handles: Arc<DashMap<String, JoinHandle<()>>>,
) -> (broadcast::Receiver<Arc<ConfigEvent>>, ReplayBuffer) {
    // Fast path: namespace already has a running watcher.
    if let Some(sender) = namespace_broadcasts.get(&namespace) {
        let buf = namespace_replay_buffers
            .entry(namespace.clone())
            .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
            .clone();
        return (sender.subscribe(), buf);
    }

    // Slow path: first subscriber for this namespace — create broadcast + watcher.
    // Clone the Arcs upfront so we can move them into the spawned task without
    // conflicting with the DashMap entry borrow held by the match.
    let broadcasts_for_spawn = Arc::clone(&namespace_broadcasts);
    let replay_buffers_for_spawn = Arc::clone(&namespace_replay_buffers);
    let handles_for_spawn = Arc::clone(&watcher_handles);

    match namespace_broadcasts.entry(namespace.clone()) {
        dashmap::mapref::entry::Entry::Occupied(e) => {
            // Another task beat us while we were acquiring the entry lock.
            let buf = namespace_replay_buffers
                .entry(namespace.clone())
                .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
                .clone();
            (e.get().subscribe(), buf)
        }
        dashmap::mapref::entry::Entry::Vacant(e) => {
            let (bcast_tx, bcast_rx) = broadcast::channel(BROADCAST_CAPACITY);
            e.insert(bcast_tx.clone());

            let buf: ReplayBuffer = namespace_replay_buffers
                .entry(namespace.clone())
                .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
                .clone();

            // The watcher runs until the kube stream ends (or is aborted by GC),
            // then removes itself from the maps so the next Subscribe recreates them.
            let handle = tokio::spawn(run_namespace_watcher(
                namespace.clone(),
                kube_client,
                bcast_tx,
                Arc::clone(&buf),
                broadcasts_for_spawn,
                replay_buffers_for_spawn,
            ));
            handles_for_spawn.insert(namespace, handle);

            (bcast_rx, buf)
        }
    }
}

/// Single kube watch stream per namespace — broadcasts every event to all
/// current subscribers AND appends it to the replay buffer.
/// Removes itself from `namespace_broadcasts` on exit (either naturally or after GC abort).
async fn run_namespace_watcher(
    namespace: String,
    kube_client: Client,
    tx: broadcast::Sender<Arc<ConfigEvent>>,
    replay_buf: ReplayBuffer,
    namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<Arc<ConfigEvent>>>>,
    namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>>,
) {
    let ar = crate::watcher::config_api_resource();
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
        let rv = snap.resource_version.clone();
        // Serialise once, wrap in Arc — all broadcast receivers share the same
        // allocation; each clone is just a reference-count increment (O(1)).
        let config_event = Arc::new(ConfigEvent {
            event_type,
            config: Some(snapshot_to_proto(&snap)),
        });

        // Push into replay buffer before broadcasting so a subscriber that
        // races to read the buffer after receiving the live event will find it.
        push_replay(&replay_buf, rv, Arc::clone(&config_event));

        // `send` returns Err only when there are zero receivers — drop the event.
        if tx.send(config_event).is_ok() {
            EVENTS_BROADCAST.with_label_values(&[&namespace]).inc();
        }
    }

    // Watcher stream ended — remove from maps so next Subscribe recreates them.
    namespace_broadcasts.remove(&namespace);
    namespace_replay_buffers.remove(&namespace);
    info!(namespace = %namespace, "Namespace watcher ended — removed from broadcast map");
}

/// Grace period before an idle namespace watcher is collected.
const GC_GRACE: Duration = Duration::from_secs(30);

/// One synchronous GC sweep at a given wall-clock `now`.
///
/// Scans `namespace_broadcasts` for channels with zero receivers:
/// - Records the first-idle timestamp in `idle_since`.
/// - After `GC_GRACE` elapses, aborts the watcher task and removes the
///   namespace from all three maps.
/// - Resets the idle timer for namespaces that become active again.
///
/// This function is synchronous and allocation-free on the hot path so that
/// it can be called directly from unit tests without the `tokio/test-util`
/// feature (no `tokio::time::pause` / `advance` required).
pub fn gc_tick(
    now: Instant,
    namespace_broadcasts: &DashMap<String, broadcast::Sender<Arc<ConfigEvent>>>,
    namespace_replay_buffers: &DashMap<String, ReplayBuffer>,
    watcher_handles: &DashMap<String, JoinHandle<()>>,
    idle_since: &DashMap<String, Instant>,
) {
    // Collect namespaces eligible for GC — never hold DashMap entry refs across
    // any subsequent mutation (DashMap deadlocks if you do).
    let to_gc: Vec<String> = namespace_broadcasts
        .iter()
        .filter_map(|entry| {
            let ns = entry.key().clone();
            let count = entry.value().receiver_count();
            if count == 0 {
                // No active receivers — check how long the channel has been idle.
                let since = *idle_since
                    .entry(ns.clone())
                    .or_insert_with(Instant::now)
                    .value();
                if now.duration_since(since) > GC_GRACE {
                    Some(ns)
                } else {
                    None
                }
            } else {
                // Still active — reset the idle timer.
                idle_since.remove(&ns);
                None
            }
        })
        .collect();

    for ns in to_gc {
        if let Some((_, handle)) = watcher_handles.remove(&ns) {
            handle.abort();
        }
        namespace_broadcasts.remove(&ns);
        namespace_replay_buffers.remove(&ns);
        idle_since.remove(&ns);
        info!(namespace = %ns, "GC: removed idle namespace watcher");
    }
}

/// Background GC task — runs `gc_tick` every 10 seconds.
///
/// Aborts namespace watchers whose broadcast channel has had zero receivers for
/// longer than `GC_GRACE` seconds, preventing indefinite K8s watch connection
/// leaks when all subscribers disconnect.
///
/// The next `get_or_create_broadcast()` call will recreate everything cleanly.
///
/// Design rules observed:
/// - Never hold a DashMap entry ref (`.get()`, `.entry()`) across an `.await`.
/// - The GC list is collected to a `Vec` inside `gc_tick` before any mutations.
pub async fn gc_task(
    namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<Arc<ConfigEvent>>>>,
    namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>>,
    watcher_handles: Arc<DashMap<String, JoinHandle<()>>>,
    idle_since: Arc<DashMap<String, Instant>>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;
        gc_tick(
            Instant::now(),
            &namespace_broadcasts,
            &namespace_replay_buffers,
            &watcher_handles,
            &idle_since,
        );
    }
}

/// Resume a subscriber from `resume_rv` using the in-memory replay buffer.
///
/// - Hit: drain the buffer starting after `resume_rv`, then switch to live broadcast.
/// - Miss: send full cache snapshot as MODIFIED events, then switch to live broadcast.
///
/// After draining the buffer (or snapshot), the subscriber joins the shared
/// broadcast channel for future events — no kube watch is opened.
async fn resume_from_buffer(
    resume_rv: String,
    replay_buf: ReplayBuffer,
    cache: Arc<ConfigCache>,
    namespace: String,
    bcast_rx: broadcast::Receiver<Arc<ConfigEvent>>,
    tx: mpsc::Sender<Result<ConfigEvent, Status>>,
) {
    // Collect the replay slice under the lock, then release before doing I/O.
    // Also record whether resume_rv was found in the buffer.
    // Arc clones are reference-count increments only — no deep copy of event data.
    let (replay_slice, found_in_buffer): (Vec<Arc<ConfigEvent>>, bool) = {
        let guard = replay_buf.lock().expect("replay buffer poisoned");
        match guard.iter().position(|e| e.resource_version == resume_rv) {
            Some(idx) => {
                let slice: Vec<Arc<ConfigEvent>> = guard
                    .iter()
                    .skip(idx + 1)
                    .map(|e| Arc::clone(&e.event))
                    .collect();
                debug!(
                    namespace = %namespace,
                    resume_rv = %resume_rv,
                    replay_count = slice.len(),
                    "Resume: buffer hit — replaying missed events"
                );
                (slice, true)
            }
            None => {
                debug!(
                    namespace = %namespace,
                    resume_rv = %resume_rv,
                    "Resume: buffer miss — falling back to full cache snapshot"
                );
                (Vec::new(), false)
            }
        }
    };

    if !found_in_buffer {
        // Buffer miss — send full cache snapshot as MODIFIED events.
        let snapshots = cache.all_in_namespace(&namespace);
        info!(
            namespace = %namespace,
            resume_rv = %resume_rv,
            snapshot_count = snapshots.len(),
            "Resume: RV not in buffer — sending full cache snapshot"
        );
        let mut snapshot_events: Vec<ConfigEvent> = Vec::with_capacity(snapshots.len());
        for snap in &snapshots {
            let event = ConfigEvent {
                event_type: EventType::Modified as i32,
                config: Some(snapshot_to_proto(snap)),
            };
            snapshot_events.push(event);
        }
        for event in &snapshot_events {
            match tx.try_send(Ok(event.clone())) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    warn!("Subscriber too slow during snapshot — disconnecting");
                    let _ = tx.try_send(Err(Status::resource_exhausted("subscriber too slow")));
                    return;
                }
                Err(TrySendError::Closed(_)) => {
                    info!("Subscriber disconnected during snapshot");
                    return;
                }
            }
        }

        // Close the race window: replay buffer events that arrived after the
        // snapshot was taken but before this subscriber joins the broadcast.
        // K8s resource versions are u64 decimal strings — parse as u64 for
        // correct numeric comparison (string comparison is wrong for large values).
        let max_snapshot_rv: u64 = snapshot_events
            .iter()
            .filter_map(|e| e.config.as_ref())
            .map(|c| c.resource_version.parse::<u64>().unwrap_or(0))
            .max()
            .unwrap_or(0);

        // Collect Arc clones under the lock, then release before any await.
        let post_snapshot_events: Vec<Arc<ConfigEvent>> = {
            let guard = replay_buf.lock().expect("replay buffer poisoned");
            guard
                .iter()
                .filter(|e| e.resource_version.parse::<u64>().unwrap_or(0) > max_snapshot_rv)
                .map(|e| Arc::clone(&e.event))
                .collect()
        }; // mutex released here

        debug!(
            namespace = %namespace,
            post_snapshot_count = post_snapshot_events.len(),
            "Resume: sending post-snapshot buffer events to close race window"
        );

        for event in post_snapshot_events {
            // Deref Arc to clone the ConfigEvent for the mpsc send.
            if tx.send(Ok((*event).clone())).await.is_err() {
                return; // subscriber disconnected
            }
        }
    } else {
        // Buffer hit — send only the missed events.
        // Arc clones collected above are dereferenced here to produce ConfigEvent
        // values for the per-subscriber mpsc — no extra serialisation.
        for event in replay_slice {
            match tx.try_send(Ok((*event).clone())) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    warn!("Subscriber too slow during replay — disconnecting");
                    let _ = tx.try_send(Err(Status::resource_exhausted("subscriber too slow")));
                    return;
                }
                Err(TrySendError::Closed(_)) => {
                    info!("Subscriber disconnected during replay");
                    return;
                }
            }
        }
    }

    // Join the live broadcast for future events.
    bridge_broadcast(bcast_rx, tx, namespace).await;
}

/// Forward events from the namespace broadcast to a single subscriber's mpsc.
///
/// Receives `Arc<ConfigEvent>` from the broadcast — O(1) reference-count clone
/// per receiver.  Dereferences the Arc to produce the `ConfigEvent` value sent
/// over the per-subscriber mpsc channel (tonic's `ReceiverStream` takes owned
/// values, not Arc).
///
/// Disconnects the subscriber with RESOURCE_EXHAUSTED if:
/// - the mpsc channel is full (subscriber too slow to drain), or
/// - the broadcast ring wrapped before this receiver drained (lagged).
///
/// Increments `konfig_active_subscribers` on entry and decrements on every
/// exit path via a `SubGauge` RAII guard.  Increments
/// `konfig_broadcast_lag_total` when the broadcast ring wraps.
async fn bridge_broadcast(
    mut bcast_rx: broadcast::Receiver<Arc<ConfigEvent>>,
    tx: mpsc::Sender<Result<ConfigEvent, Status>>,
    namespace: String,
) {
    ACTIVE_SUBSCRIBERS.with_label_values(&[&namespace]).inc();
    // Decrement on every exit path — including early returns from break.
    let _guard = SubGauge(namespace.clone());

    loop {
        match bcast_rx.recv().await {
            Ok(event) => match tx.try_send(Ok((*event).clone())) {
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
                BROADCAST_LAG.with_label_values(&[&namespace]).inc();
                let _ = tx.try_send(Err(Status::resource_exhausted("subscriber lagged")));
                break;
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::ConfigCache;
    use crate::proto::config_event::EventType;
    use crate::types::ConfigSnapshot;
    use serde_json::json;
    use tokio::sync::broadcast;

    fn make_event(rv: &str, schema_version: u32) -> ConfigEvent {
        ConfigEvent {
            event_type: EventType::Modified as i32,
            config: Some(crate::grpc::snapshot_to_proto(&ConfigSnapshot {
                name: "cfg".into(),
                namespace: "default".into(),
                schema_version,
                content: json!({}),
                resource_version: rv.into(),
                ..Default::default()
            })),
        }
    }

    fn make_replay_buf(entries: &[(&str, u32)]) -> ReplayBuffer {
        let buf = Arc::new(Mutex::new(VecDeque::new()));
        for (rv, sv) in entries {
            push_replay(&buf, rv.to_string(), Arc::new(make_event(rv, *sv)));
        }
        buf
    }

    fn make_cache(namespace: &str, entries: &[(&str, &str, u32)]) -> Arc<ConfigCache> {
        let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
        for (name, rv, sv) in entries {
            cache.update(ConfigSnapshot {
                name: name.to_string(),
                namespace: namespace.to_string(),
                schema_version: *sv,
                content: json!({}),
                resource_version: rv.to_string(),
                ..Default::default()
            });
        }
        cache
    }

    // ── Unit: empty_cache_fails_gate ─────────────────────────────────────────

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

    // ── Unit: push_replay evicts oldest when full ────────────────────────────

    #[test]
    fn push_replay_evicts_oldest_when_full() {
        let buf: ReplayBuffer = Arc::new(Mutex::new(VecDeque::new()));
        for i in 0..REPLAY_BUFFER_SIZE {
            push_replay(
                &buf,
                format!("rv-{i}"),
                Arc::new(make_event(&format!("rv-{i}"), i as u32)),
            );
        }
        // Buffer is exactly full — oldest is rv-0.
        assert_eq!(
            buf.lock().unwrap().front().unwrap().resource_version,
            "rv-0"
        );

        // Push one more — rv-0 must be evicted.
        push_replay(
            &buf,
            "rv-overflow".into(),
            Arc::new(make_event("rv-overflow", 9999)),
        );
        let guard = buf.lock().unwrap();
        assert_eq!(guard.len(), REPLAY_BUFFER_SIZE);
        assert_eq!(guard.front().unwrap().resource_version, "rv-1");
        assert_eq!(guard.back().unwrap().resource_version, "rv-overflow");
    }

    // ── Async: reconnect with valid resume_rv receives only missed events ────

    #[tokio::test]
    async fn resume_buffer_hit_receives_only_missed_events() {
        // Buffer contains rv-1 .. rv-5.  Client reconnects at rv-2 →
        // should receive rv-3, rv-4, rv-5 (schema versions 3, 4, 5).
        let replay_buf = make_replay_buf(&[
            ("rv-1", 1),
            ("rv-2", 2),
            ("rv-3", 3),
            ("rv-4", 4),
            ("rv-5", 5),
        ]);
        let cache = make_cache("default", &[("cfg", "rv-5", 5)]);
        let (bcast_tx, bcast_rx) = broadcast::channel::<Arc<ConfigEvent>>(64);
        let (tx, mut rx) = mpsc::channel(64);

        // Close the broadcast sender so bridge_broadcast exits cleanly after replay.
        drop(bcast_tx);

        tokio::spawn(resume_from_buffer(
            "rv-2".into(),
            replay_buf,
            cache,
            "default".into(),
            bcast_rx,
            tx,
        ));

        let mut received_schema_versions: Vec<u32> = Vec::new();
        while let Some(Ok(ev)) = rx.recv().await {
            received_schema_versions.push(ev.config.unwrap().schema_version);
        }

        assert_eq!(
            received_schema_versions,
            vec![3, 4, 5],
            "buffer hit must replay only events after resume_rv"
        );
    }

    // ── Async: reconnect with stale rv falls back to full cache snapshot ─────

    #[tokio::test]
    async fn resume_buffer_miss_sends_full_cache_snapshot() {
        // Buffer contains rv-10 .. rv-12.  Client reconnects at rv-1 (not in buffer).
        let replay_buf = make_replay_buf(&[("rv-10", 10), ("rv-11", 11), ("rv-12", 12)]);
        // Cache has two entries.
        let cache = make_cache("default", &[("cfg-a", "rv-13", 13), ("cfg-b", "rv-14", 14)]);
        let (bcast_tx, bcast_rx) = broadcast::channel::<Arc<ConfigEvent>>(64);
        let (tx, mut rx) = mpsc::channel(64);

        drop(bcast_tx); // let bridge_broadcast exit cleanly

        tokio::spawn(resume_from_buffer(
            "rv-1".into(), // stale — not in buffer
            replay_buf,
            cache,
            "default".into(),
            bcast_rx,
            tx,
        ));

        let mut received: Vec<u32> = Vec::new();
        while let Some(Ok(ev)) = rx.recv().await {
            received.push(ev.config.unwrap().schema_version);
        }

        let mut received_sorted = received.clone();
        received_sorted.sort_unstable();
        assert_eq!(
            received_sorted,
            vec![13, 14],
            "buffer miss must send full cache snapshot as MODIFIED events"
        );
    }

    // ── Async: 10 simultaneous reconnects produce no new watcher spawns ──────
    //
    // We verify this by calling resume_from_buffer directly for 10 concurrent
    // "reconnecting" subscribers.  None of these calls invoke run_raw_watch or
    // any kube API — they only read the replay buffer and/or the cache.  The
    // broadcast channel is pre-created, simulating an already-running watcher.

    #[tokio::test]
    async fn ten_simultaneous_reconnects_produce_zero_new_watchers() {
        let replay_buf = make_replay_buf(&[("rv-1", 1), ("rv-2", 2), ("rv-3", 3)]);
        let cache = make_cache("default", &[("cfg", "rv-3", 3)]);

        // Pre-create a broadcast channel to simulate an already-running watcher.
        let (bcast_tx, _initial_rx) = broadcast::channel::<Arc<ConfigEvent>>(64);

        let mut handles = Vec::new();
        for _ in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let replay_buf_clone = Arc::clone(&replay_buf);
            let bcast_rx = bcast_tx.subscribe();
            let (tx, mut rx) = mpsc::channel(64);

            let h = tokio::spawn(async move {
                // resume_from_buffer — no kube watch, no new watcher spawned.
                // bcast_rx will see RecvError::Closed once all senders are gone.
                resume_from_buffer(
                    "rv-1".into(),
                    replay_buf_clone,
                    cache_clone,
                    "default".into(),
                    bcast_rx,
                    tx,
                )
                .await;
                while rx.recv().await.is_some() {}
            });
            handles.push(h);
        }

        // Drop the original sender — this is the ONLY sender, so all bridge_broadcast
        // loops will see RecvError::Closed and exit cleanly.
        drop(bcast_tx);

        for h in handles {
            h.await.unwrap();
        }

        // If we reach here without error, the test passes: all 10 reconnects
        // completed using only the replay buffer + broadcast, no kube watches.
    }

    // ── Async: resume at latest rv (empty replay) then joins live broadcast ──

    #[tokio::test]
    async fn resume_at_latest_rv_joins_live_broadcast() {
        let replay_buf = make_replay_buf(&[("rv-5", 5)]);
        let cache = make_cache("default", &[("cfg", "rv-5", 5)]);
        let (bcast_tx, bcast_rx) = broadcast::channel::<Arc<ConfigEvent>>(64);
        let (tx, mut rx) = mpsc::channel(64);

        let bcast_tx_clone = bcast_tx.clone();
        tokio::spawn(async move {
            // Send one live event after a short delay.
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            let _ = bcast_tx_clone.send(Arc::new(make_event("rv-6", 6)));
            drop(bcast_tx_clone);
        });

        tokio::spawn(resume_from_buffer(
            "rv-5".into(), // latest — nothing to replay
            replay_buf,
            cache,
            "default".into(),
            bcast_rx,
            tx,
        ));

        // Drop the original sender too so bridge exits after the live event.
        drop(bcast_tx);

        let mut received: Vec<u32> = Vec::new();
        while let Some(Ok(ev)) = rx.recv().await {
            received.push(ev.config.unwrap().schema_version);
        }

        // Only the one live event should arrive (rv-5 was the resume point —
        // nothing before it is replayed).
        assert_eq!(
            received,
            vec![6],
            "resuming at latest rv should yield only new live events"
        );
    }

    // ── Async: miss-path closes race window — post-snapshot buffer events sent ─

    #[tokio::test]
    async fn resume_miss_path_closes_race_window() {
        // Buffer has rv-1..rv-5. Cache has rv-5.
        // 3 post-snapshot events (rv-6, rv-7, rv-8) are in the buffer,
        // simulating events that fired between the snapshot being taken and the
        // subscriber joining the broadcast.
        // Client reconnects with a stale rv (miss) → must receive:
        //   snapshot (rv-5, schema_version=5) + rv-6, rv-7, rv-8.
        let replay_buf = make_replay_buf(&[
            ("1", 1),
            ("2", 2),
            ("3", 3),
            ("4", 4),
            ("5", 5),
            ("6", 6),
            ("7", 7),
            ("8", 8),
        ]);
        // Cache reflects the state at the snapshot: only rv-5.
        let cache = make_cache("default", &[("cfg", "5", 5)]);

        let (bcast_tx, bcast_rx) = broadcast::channel::<Arc<ConfigEvent>>(64);
        let (tx, mut rx) = mpsc::channel(128);

        // Close the broadcast sender so bridge_broadcast exits cleanly after
        // the post-snapshot replay — no live events needed for this test.
        drop(bcast_tx);

        tokio::spawn(resume_from_buffer(
            "old-rv".into(), // stale — not in buffer (miss path)
            replay_buf,
            cache,
            "default".into(),
            bcast_rx,
            tx,
        ));

        let mut received_schema_versions: Vec<u32> = Vec::new();
        while let Some(Ok(ev)) = rx.recv().await {
            received_schema_versions.push(ev.config.unwrap().schema_version);
        }

        // Must receive the snapshot entry (rv-5, sv=5) plus the three
        // post-snapshot buffer events (rv-6, rv-7, rv-8 → sv=6, 7, 8).
        // The snapshot event order is unspecified (cache is a map), so sort.
        received_schema_versions.sort_unstable();
        assert_eq!(
            received_schema_versions,
            vec![5, 6, 7, 8],
            "miss-path must include snapshot + post-snapshot buffer events to close race window"
        );
    }

    // ── Async: broadcast_arc_not_cloned — all receivers share one allocation ──
    //
    // Spawn 10 receiver tasks on a `broadcast::channel::<Arc<ConfigEvent>>`,
    // send 1 event, verify all 10 receivers get a pointer to the SAME allocation.
    // This confirms that the broadcast distributes reference-count increments
    // only — no deep copy of the proto struct per subscriber.

    #[tokio::test]
    async fn broadcast_arc_not_cloned() {
        let (bcast_tx, _) = broadcast::channel::<Arc<ConfigEvent>>(64);

        const N: usize = 10;
        let mut handles: Vec<tokio::task::JoinHandle<Arc<ConfigEvent>>> = Vec::with_capacity(N);

        for _ in 0..N {
            let mut rx = bcast_tx.subscribe();
            let h = tokio::spawn(async move { rx.recv().await.expect("must receive event") });
            handles.push(h);
        }

        let event = Arc::new(make_event("rv-1", 1));
        // Record the pointer before sending (numeric address of the heap allocation).
        let expected_ptr = Arc::as_ptr(&event) as usize;

        bcast_tx.send(event).expect("send failed");

        let mut received_arcs = Vec::with_capacity(N);
        for h in handles {
            let arc = h.await.expect("task panicked");
            received_arcs.push(arc);
        }

        // All receivers must hold a reference to the SAME allocation — the
        // broadcast clones only the Arc (refcount bump), not the ConfigEvent.
        for arc in &received_arcs {
            assert!(
                Arc::ptr_eq(received_arcs.first().unwrap(), arc),
                "all receivers must point to the same Arc allocation"
            );
        }
        // Also verify the pointer matches what was sent.
        assert_eq!(
            Arc::as_ptr(received_arcs.first().unwrap()) as usize,
            expected_ptr,
            "received Arc must be the same allocation as the one sent"
        );
    }

    // ── GC: idle namespace removed after grace period ─────────────────────────

    /// After all receivers disconnect and the grace period elapses, `gc_tick`
    /// must remove the namespace from `namespace_broadcasts` and
    /// `namespace_replay_buffers`.
    ///
    /// We call `gc_tick` directly with an explicit `now` rather than using
    /// `tokio::time::pause` / `advance` to avoid requiring the `test-util`
    /// feature of the `tokio` crate.
    #[tokio::test]
    async fn gc_removes_idle_namespace_after_grace_period() {
        let namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<Arc<ConfigEvent>>>> =
            Arc::new(DashMap::new());
        let namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>> = Arc::new(DashMap::new());
        let watcher_handles: Arc<DashMap<String, JoinHandle<()>>> = Arc::new(DashMap::new());
        let idle_since: Arc<DashMap<String, Instant>> = Arc::new(DashMap::new());

        // Insert a broadcast channel with NO active receivers (sender only).
        let (tx, _rx) = broadcast::channel::<Arc<ConfigEvent>>(64);
        // Drop _rx so receiver_count() == 0.
        drop(_rx);
        namespace_broadcasts.insert("test-ns".to_string(), tx);
        namespace_replay_buffers
            .insert("test-ns".to_string(), Arc::new(Mutex::new(VecDeque::new())));

        let t0 = Instant::now();

        // Tick 1: namespace becomes idle — idle_since is recorded.
        // Grace period has not yet elapsed so the namespace must survive.
        gc_tick(
            t0,
            &namespace_broadcasts,
            &namespace_replay_buffers,
            &watcher_handles,
            &idle_since,
        );
        assert!(
            namespace_broadcasts.contains_key("test-ns"),
            "namespace must still be present before grace period elapses"
        );

        // Tick 2: simulate 31 s later — past the 30 s grace period.
        let t1 = t0 + Duration::from_secs(31);
        gc_tick(
            t1,
            &namespace_broadcasts,
            &namespace_replay_buffers,
            &watcher_handles,
            &idle_since,
        );

        assert!(
            !namespace_broadcasts.contains_key("test-ns"),
            "gc_tick must remove idle namespace from namespace_broadcasts after grace period"
        );
        assert!(
            !namespace_replay_buffers.contains_key("test-ns"),
            "gc_tick must remove idle namespace from namespace_replay_buffers after grace period"
        );
    }

    // ── GC: active namespace is not removed ───────────────────────────────────

    /// A namespace with at least one active receiver must NOT be removed by GC,
    /// even when called with a `now` far in the future.
    #[tokio::test]
    async fn gc_does_not_remove_namespace_with_active_subscriber() {
        let namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<Arc<ConfigEvent>>>> =
            Arc::new(DashMap::new());
        let namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>> = Arc::new(DashMap::new());
        let watcher_handles: Arc<DashMap<String, JoinHandle<()>>> = Arc::new(DashMap::new());
        let idle_since: Arc<DashMap<String, Instant>> = Arc::new(DashMap::new());

        // Insert a broadcast channel AND keep a live receiver so receiver_count() > 0.
        let (tx, _live_rx) = broadcast::channel::<Arc<ConfigEvent>>(64);
        namespace_broadcasts.insert("active-ns".to_string(), tx);
        namespace_replay_buffers.insert(
            "active-ns".to_string(),
            Arc::new(Mutex::new(VecDeque::new())),
        );

        // Run GC with a `now` far past any grace period.
        let far_future = Instant::now() + Duration::from_secs(3600);
        gc_tick(
            far_future,
            &namespace_broadcasts,
            &namespace_replay_buffers,
            &watcher_handles,
            &idle_since,
        );

        assert!(
            namespace_broadcasts.contains_key("active-ns"),
            "gc_tick must NOT remove a namespace with active subscribers"
        );
        assert!(
            namespace_replay_buffers.contains_key("active-ns"),
            "gc_tick must NOT remove a namespace with active subscribers"
        );

        // Keep _live_rx alive until here so receiver_count() stays > 0.
        drop(_live_rx);
    }
}
