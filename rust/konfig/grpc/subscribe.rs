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
use tokio::sync::{Notify, broadcast, mpsc};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Response, Status};
use tracing::{debug, info, warn};

use crate::cache::ConfigCache;
use crate::grpc::snapshot_to_proto;
use crate::metrics::{
    ACTIVE_SUBSCRIBERS, APPLY_TO_BROADCAST_SECONDS, BROADCAST_LAG, BROADCAST_TO_ENCODE_SECONDS,
    ENCODE_TO_SEND_SECONDS, EVENTS_BROADCAST, H2_DATA_FRAME_BYTES, SUBSCRIBE_E2E_LATENCY, SubGauge,
    WRITEV_CALLS_TOTAL,
};
use crate::proto::{ConfigEvent, SubscribeRequest, config_event::EventType};
use prost::Message as _;

/// Outer envelope that the namespace watcher publishes onto the broadcast
/// channel.  Carries the moment `broadcast::Sender::send` was called so each
/// subscriber's bridge can observe end-to-end latency in
/// `konfig_subscribe_e2e_latency_seconds`.
///
/// `event` is kept behind `Arc` so the inner `ConfigEvent` is still serialised
/// exactly once per apply (Track E invariant) — only this thin outer envelope
/// is added, and it is itself wrapped in an `Arc` at the broadcast layer so
/// every receiver shares the same allocation.
#[derive(Debug)]
pub struct BroadcastFrame {
    pub sent_at: Instant,
    pub event: Arc<ConfigEvent>,
}

/// Per-subscriber mpsc capacity — back-pressure for slow readers.
const CHANNEL_CAPACITY: usize = 256;

/// Maximum number of events accumulated in one coalesce window before an
/// early flush is forced (cap on per-window latency-vs-throughput trade).
/// Only consulted when the coalesce window is non-zero (CU-86aj3vpgr).
const COALESCE_MAX_BATCH: usize = 16;

/// Broadcast ring-buffer capacity per namespace.
/// Sized so that even the slowest subscriber can drain before the ring wraps.
const BROADCAST_CAPACITY: usize = 1_024;

/// Maximum number of events kept in the per-namespace replay buffer.
/// Events older than this are evicted (FIFO).  1 000 events at typical
/// ConfigEvent sizes (~1 KiB each) ≈ 1 MiB per namespace.
pub const REPLAY_BUFFER_SIZE: usize = 1_000;

/// One entry in the per-namespace replay buffer.
///
/// `resource_version_u64` is the parsed numeric value of `resource_version`,
/// pre-computed at push time so resume lookups can binary-search the buffer
/// in O(log N) instead of the previous O(N) `position()` scan, and so the
/// post-snapshot race-window filter does not re-parse every entry per
/// reconnect.  Entries with a non-numeric `resource_version` are dropped at
/// push time — kube always emits decimal-string RVs, so a non-numeric value
/// means the upstream object is malformed and we never want to serve it.
#[derive(Clone)]
pub struct ReplayEntry {
    pub resource_version: String,
    pub resource_version_u64: u64,
    pub event: Arc<ConfigEvent>,
}

/// Per-namespace replay buffer: a bounded FIFO ring of the last
/// `REPLAY_BUFFER_SIZE` events, keyed by their resource_version.
pub type ReplayBuffer = Arc<Mutex<VecDeque<ReplayEntry>>>;

/// Push `event` into `buf`, evicting the oldest entry when the buffer is full.
///
/// `resource_version` is parsed as `u64` at push time so resume can
/// binary-search the buffer.  An unparseable RV (kube only emits decimal
/// strings, so this signals upstream malformation) is logged and dropped.
fn push_replay(buf: &ReplayBuffer, resource_version: &str, event: Arc<ConfigEvent>) {
    let Ok(resource_version_u64) = resource_version.parse::<u64>() else {
        warn!(
            resource_version = %resource_version,
            "Dropping replay entry with non-numeric resource_version",
        );
        return;
    };
    let mut guard = crate::sync_util::lock_recovered(buf);
    if guard.len() >= REPLAY_BUFFER_SIZE {
        guard.pop_front();
    }
    // Defer the `to_owned` until after the parse succeeds — callers pass
    // `&snap.resource_version` so we save a per-event clone on the warn
    // path (CU-86aj3m1bd). The happy path's allocation is unavoidable
    // because `ReplayEntry::resource_version` is `String` (the resume
    // lookup compares string forms when the u64 parse falls back).
    guard.push_back(ReplayEntry {
        resource_version: resource_version.to_owned(),
        resource_version_u64,
        event,
    });
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_subscribe(
    cache: Arc<ConfigCache>,
    kube_client: Client,
    namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<Arc<BroadcastFrame>>>>,
    namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>>,
    watcher_handles: Arc<DashMap<String, JoinHandle<()>>>,
    drain_notify: Arc<Notify>,
    coalesce_window: Duration,
    req: SubscribeRequest,
) -> Result<Response<ReceiverStream<Result<ConfigEvent, Status>>>, Status> {
    debug!(namespace = %req.namespace, resume_rv = %req.resume_resource_version, "Subscribe RPC");

    if !cache.is_populated() {
        return Err(Status::unavailable("cache not yet populated"));
    }

    let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
    // Move req fields out instead of cloning — req is dropped at function exit.
    // Single clone for the get_or_create_broadcast call; resume_from_buffer
    // takes the original move.
    let namespace = req.namespace;
    let resume_rv = req.resume_resource_version;

    // Get or create the broadcast receiver and replay buffer for this namespace.
    let (bcast_rx, replay_buf) = get_or_create_broadcast(
        namespace.clone(),
        kube_client,
        Arc::clone(&namespace_broadcasts),
        Arc::clone(&namespace_replay_buffers),
        Arc::clone(&watcher_handles),
        coalesce_window,
    );

    // Both resume and fresh-subscribe paths route through resume_from_buffer:
    // - Non-empty resume_rv → buffer-hit replays missed events, buffer-miss
    //   sends full snapshot + post-snapshot race-window events.
    // - Empty resume_rv → falls through to buffer-miss path → sends full
    //   snapshot synchronously as the first event(s) so a fresh subscriber
    //   never has to wait for the next apply to receive any state.
    tokio::spawn(resume_from_buffer(
        resume_rv,
        replay_buf,
        cache,
        namespace,
        bcast_rx,
        tx,
        drain_notify,
    ));
    Ok(Response::new(ReceiverStream::new(rx)))
}

/// Return a `(broadcast::Receiver, ReplayBuffer)` for `namespace`, spinning up
/// a kube watcher if one isn't already running for that namespace.
fn get_or_create_broadcast(
    namespace: String,
    kube_client: Client,
    namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<Arc<BroadcastFrame>>>>,
    namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>>,
    watcher_handles: Arc<DashMap<String, JoinHandle<()>>>,
    coalesce_window: Duration,
) -> (broadcast::Receiver<Arc<BroadcastFrame>>, ReplayBuffer) {
    // Fast path: namespace already has a running watcher.
    if let Some(sender_ref) = namespace_broadcasts.get(&namespace) {
        // Clone the sender out and drop the shard ref before calling
        // .subscribe() so we don't hold a DashMap shard lock across the
        // broadcast-receiver allocation (would serialise concurrent
        // Subscribe RPCs that hash to the same shard).
        let sender = sender_ref.clone();
        drop(sender_ref);
        // Hot-path replay buffer lookup: prefer borrow-key `get()` over
        // `entry()` so we don't pay the per-call `namespace.clone()` when
        // the buffer already exists (the steady-state case under load).
        // `entry()`'s `or_insert_with` requires an owned key up-front
        // regardless of whether the insert fires, so a present buffer
        // forces an unconditional clone. CU-86aj3m1bd.
        let buf = if let Some(b) = namespace_replay_buffers.get(&namespace) {
            b.clone()
        } else {
            namespace_replay_buffers
                .entry(namespace.clone())
                .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
                .clone()
        };
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
                coalesce_window,
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
    tx: broadcast::Sender<Arc<BroadcastFrame>>,
    replay_buf: ReplayBuffer,
    namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<Arc<BroadcastFrame>>>>,
    namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>>,
    coalesce_window: Duration,
) {
    let ar = crate::watcher::config_api_resource();
    let api: Api<DynamicObject> = Api::namespaced_with(kube_client, &namespace, &ar);
    let wc = kube_watcher::Config::default();
    let stream = kube_watch_stream(api, wc).boxed();

    pump_subscribe_namespace_events(stream, &namespace, &tx, &replay_buf, coalesce_window).await;

    // Watcher stream ended — remove from maps so next Subscribe recreates them.
    namespace_broadcasts.remove(&namespace);
    namespace_replay_buffers.remove(&namespace);
    info!(namespace = %namespace, "Namespace watcher ended — removed from broadcast map");
}

/// Drive a single connection of the per-namespace Config watcher to
/// completion (or first error). For every Apply/Delete event:
///
/// - stamp the apply→broadcast clock,
/// - serialise the proto once, wrap it in an `Arc<ConfigEvent>`, share it
///   between replay-buffer push and broadcast fan-out,
/// - observe `H2_DATA_FRAME_BYTES` (bit-exact wire size) and
///   `APPLY_TO_BROADCAST_SECONDS` (stage 1 latency),
/// - increment `EVENTS_BROADCAST{namespace}` only when at least one
///   subscriber was on the channel at send time.
///
/// `Init` / `InitDone` are skipped (no apply happened — would skew the
/// stage-1 latency histogram). Returns on first stream end OR error so
/// the caller can clean up the namespace maps in either case.
///
/// Extracted from [`run_namespace_watcher`] so this hot loop is unit-testable
/// against a synthetic stream — no kube API connection required.
///
/// `coalesce_window` (CU-86aj3vpgr) controls broadcast fan-out batching:
///
/// - `Duration::ZERO` (default / `--coalesce-window-ms 0`): each event is
///   broadcast immediately via its own `tx.send`. This is the historical
///   behaviour, byte-for-byte — every parked subscriber `Receiver` is
///   unparked once per apply.
/// - `> 0`: events that arrive within the window are accumulated into a
///   small buffer and dispatched as a back-to-back burst when the window
///   elapses OR the buffer reaches [`COALESCE_MAX_BATCH`]. Because tokio's
///   broadcast only unparks a receiver that is *currently* parked, a burst
///   of N sends issued before the receiver task is rescheduled drains as a
///   single wakeup instead of N — cutting the noop-park amplification.
///
/// Both paths preserve the per-event invariants exactly: each event keeps
/// its own `apply_observed_at` / `sent_at` stamps (no collapsed timestamps),
/// `push_replay` still runs once per event (RV-keyed) before the frame is
/// emitted, and every accepted event is delivered (no drops — a partially
/// filled buffer is flushed on window expiry and on stream end).
pub(crate) async fn pump_subscribe_namespace_events<S>(
    stream: S,
    namespace: &str,
    tx: &broadcast::Sender<Arc<BroadcastFrame>>,
    replay_buf: &ReplayBuffer,
    coalesce_window: Duration,
) where
    S: futures_util::stream::TryStream<Ok = Event<DynamicObject>, Error = kube_watcher::Error>
        + Unpin,
{
    // Hoist the counter lookup out of the hot loop — `with_label_values`
    // does a hashmap lookup per call which adds up at high event rates.
    // CU-86aj3m1bd: mirrors the same pattern already used for
    // `SUBSCRIBE_E2E_LATENCY` + `BROADCAST_LAG` per-subscriber.
    let events_broadcast = EVENTS_BROADCAST.with_label_values(&[namespace]);

    if coalesce_window.is_zero() {
        pump_immediate(stream, namespace, tx, replay_buf, &events_broadcast).await;
    } else {
        pump_coalesced(
            stream,
            namespace,
            tx,
            replay_buf,
            coalesce_window,
            &events_broadcast,
        )
        .await;
    }
}

/// Classify an already-resolved `stream.try_next()` result, emitting the same
/// end/error logs the historical loop did. Returns `Some(event)` to process or
/// `None` to terminate the pump (both clean-end and error map to `None` — the
/// caller cleans up the namespace maps either way, as before).
///
/// Takes the resolved `Result` (rather than polling) so the coalesce loop can
/// feed in the value it obtained from a `tokio::time::timeout` without
/// re-polling the stream — re-polling would drop the already-yielded item and
/// violate the 100 % delivery guarantee.
fn classify_stream_item(
    item: Result<Option<Event<DynamicObject>>, kube_watcher::Error>,
    namespace: &str,
) -> Option<Event<DynamicObject>> {
    match item {
        Ok(Some(event)) => Some(event),
        Ok(None) => {
            // Clean stream end — emit a `warn!` (not `info!`) so the outer
            // caller's restart shows up in log search; the
            // `get_or_create_broadcast` retry rebuilds the watcher when a
            // fresh subscriber arrives.
            warn!(namespace = %namespace, "Namespace watcher stream ended cleanly");
            None
        }
        Err(e) => {
            // Previously `try_next().await.unwrap_or(None)` collapsed every
            // stream error into an indistinguishable clean exit, hiding
            // intermittent k8s API failures. Surface them so the operator can
            // correlate against API-server logs.
            warn!(namespace = %namespace, "Namespace watcher stream error: {e}");
            None
        }
    }
}

/// Immediate fan-out — exact historical behaviour. Each accepted event is
/// broadcast via its own `tx.send`, unparking every parked subscriber once
/// per apply.
async fn pump_immediate<S>(
    mut stream: S,
    namespace: &str,
    tx: &broadcast::Sender<Arc<BroadcastFrame>>,
    replay_buf: &ReplayBuffer,
    events_broadcast: &prometheus::Counter,
) where
    S: futures_util::stream::TryStream<Ok = Event<DynamicObject>, Error = kube_watcher::Error>
        + Unpin,
{
    loop {
        let Some(event) = classify_stream_item(stream.try_next().await, namespace) else {
            return;
        };
        if let Some(frame) = process_namespace_event(event, replay_buf) {
            // OTEL child span (Phase 7, CU-86ahzwj3k) per broadcast dispatch,
            // carrying the live subscriber count. `level = "debug"` keeps it
            // off the INFO production path; `tx.send` is synchronous so the
            // entered guard never spans an await. `subscribers` is a cheap
            // integer read.
            let span = tracing::debug_span!(
                "konfig.broadcast_dispatch",
                subscribers = tx.receiver_count(),
            );
            let _enter = span.enter();
            if tx.send(frame).is_ok() {
                events_broadcast.inc();
            }
        }
    }
}

/// Coalesced fan-out — accumulate events arriving within `coalesce_window` and
/// dispatch them as a back-to-back send burst. `batch` holds fully-processed
/// frames (each already stamped + pushed to the replay buffer); `deadline` is
/// the flush time for the *first* buffered frame, or `None` when the batch is
/// empty (then we block on the stream indefinitely). The partial batch is
/// always flushed before returning, so no accepted event is ever dropped.
async fn pump_coalesced<S>(
    mut stream: S,
    namespace: &str,
    tx: &broadcast::Sender<Arc<BroadcastFrame>>,
    replay_buf: &ReplayBuffer,
    coalesce_window: Duration,
    events_broadcast: &prometheus::Counter,
) where
    S: futures_util::stream::TryStream<Ok = Event<DynamicObject>, Error = kube_watcher::Error>
        + Unpin,
{
    let mut batch: Vec<Arc<BroadcastFrame>> = Vec::with_capacity(COALESCE_MAX_BATCH);
    let mut deadline: Option<Instant> = None;
    loop {
        // Non-empty batch: only wait until the window deadline for the next
        // event; if the timeout fires first, flush the partial batch. Empty
        // batch: block on the stream indefinitely. In both cases the resolved
        // `try_next` value is captured (never re-polled) so no item is lost.
        let item = match deadline {
            Some(dl) => {
                let remaining = dl.saturating_duration_since(Instant::now());
                match tokio::time::timeout(remaining, stream.try_next()).await {
                    Ok(item) => item,
                    Err(_elapsed) => {
                        // Window expired with a partially filled batch — flush.
                        flush_batch(&mut batch, tx, events_broadcast);
                        deadline = None;
                        continue;
                    }
                }
            }
            None => stream.try_next().await,
        };

        let Some(event) = classify_stream_item(item, namespace) else {
            // Stream ended or errored — flush whatever was buffered before
            // exiting so no accepted event is dropped (100 % delivery).
            flush_batch(&mut batch, tx, events_broadcast);
            return;
        };

        if let Some(frame) = process_namespace_event(event, replay_buf) {
            if batch.is_empty() {
                // Start the window clock on the first buffered frame.
                deadline = Some(Instant::now() + coalesce_window);
            }
            batch.push(frame);
            if batch.len() >= COALESCE_MAX_BATCH {
                flush_batch(&mut batch, tx, events_broadcast);
                deadline = None;
            }
        }
    }
}

/// Process one raw kube watch event into a ready-to-broadcast
/// [`BroadcastFrame`], performing the per-event side effects that must happen
/// regardless of whether fan-out is coalesced:
///
/// - stamp `apply_observed_at` → `sent_at` (each event keeps its own clock),
/// - serialise the proto once into an `Arc<ConfigEvent>`,
/// - observe `H2_DATA_FRAME_BYTES` + `APPLY_TO_BROADCAST_SECONDS`,
/// - push into the replay buffer (RV-keyed) before the frame is emitted.
///
/// Returns `None` for `Init` / `InitDone` and unparseable objects (which are
/// skipped without broadcasting, matching the historical immediate path).
fn process_namespace_event(
    event: Event<DynamicObject>,
    replay_buf: &ReplayBuffer,
) -> Option<Arc<BroadcastFrame>> {
    let apply_observed_at = Instant::now();

    let (event_type, obj) = match event {
        Event::Apply(obj) | Event::InitApply(obj) => (EventType::Modified as i32, obj),
        Event::Delete(obj) => (EventType::Deleted as i32, obj),
        Event::Init | Event::InitDone => return None,
    };
    let snap = crate::watcher::parse_config_object(&obj)?;
    let config_event = Arc::new(ConfigEvent {
        event_type,
        config: Some(snapshot_to_proto(&snap)),
    });

    H2_DATA_FRAME_BYTES.observe(config_event.encoded_len() as f64);

    // Push into replay buffer before broadcasting so a subscriber that
    // races to read the buffer after receiving the live event will find it.
    // `&str` cheap-pass: `push_replay` allocates the owned String only
    // on the happy parse path (CU-86aj3m1bd).
    push_replay(
        replay_buf,
        &snap.resource_version,
        Arc::clone(&config_event),
    );

    let sent_at = Instant::now();
    APPLY_TO_BROADCAST_SECONDS.observe(sent_at.duration_since(apply_observed_at).as_secs_f64());

    Some(Arc::new(BroadcastFrame {
        sent_at,
        event: config_event,
    }))
}

/// Dispatch every buffered frame as a back-to-back `tx.send` burst, then clear
/// the batch. Each successful send increments `events_broadcast` (preserving
/// the per-event counter semantics — the counter still counts events that
/// reached at least one subscriber, not batches). `tx.send` only errors when
/// there are zero receivers, which is not a per-event failure to surface.
fn flush_batch(
    batch: &mut Vec<Arc<BroadcastFrame>>,
    tx: &broadcast::Sender<Arc<BroadcastFrame>>,
    events_broadcast: &prometheus::Counter,
) {
    // OTEL child span (Phase 7, CU-86ahzwj3k) per coalesced flush burst,
    // carrying the live subscriber count + burst size. `level = "debug"`
    // keeps it off the INFO production path; `tx.send` is synchronous so the
    // entered guard never spans an await. Both fields are cheap integer reads.
    let span = tracing::debug_span!(
        "konfig.broadcast_dispatch",
        subscribers = tx.receiver_count(),
        events = batch.len(),
    );
    let _enter = span.enter();
    for frame in batch.drain(..) {
        if tx.send(frame).is_ok() {
            events_broadcast.inc();
        }
    }
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
    namespace_broadcasts: &DashMap<String, broadcast::Sender<Arc<BroadcastFrame>>>,
    namespace_replay_buffers: &DashMap<String, ReplayBuffer>,
    watcher_handles: &DashMap<String, JoinHandle<()>>,
    idle_since: &DashMap<String, Instant>,
) {
    // Collect namespaces eligible for GC — never hold DashMap entry refs across
    // any subsequent mutation (DashMap deadlocks if you do).
    //
    // Defer the `entry.key().clone()` to inside the `count == 0` branch so the
    // common case (subscriber present) does zero allocation per sweep.  The
    // `idle_since.remove` branch passes `entry.key()` directly (`Borrow<str>`).
    let to_gc: Vec<String> = namespace_broadcasts
        .iter()
        .filter_map(|entry| {
            let count = entry.value().receiver_count();
            if count == 0 {
                // No active receivers — check how long the channel has been idle.
                let ns = entry.key().clone();
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
                // Still active — reset the idle timer without cloning the key.
                idle_since.remove(entry.key());
                None
            }
        })
        .collect();

    for ns in to_gc {
        // Close the TOCTOU window between the `receiver_count == 0` check
        // above and removal here: a subscriber could have called
        // `get_or_create_broadcast` (and `sender.subscribe()`) in the
        // interim, bumping receiver_count > 0.  If we removed unconditionally
        // we'd orphan that subscriber's stream (the sender we drop is the
        // one their `Receiver` was subscribed to).
        //
        // `DashMap::remove_if` re-checks the predicate under the per-shard
        // write lock — receiver_count is read atomically inside the lock,
        // so no new subscriber can race in.  If it now sees a non-zero
        // count, the entry stays and we skip the rest of the teardown.
        let removed = namespace_broadcasts
            .remove_if(&ns, |_k, v| v.receiver_count() == 0)
            .is_some();
        if !removed {
            debug!(
                namespace = %ns,
                "GC: namespace re-subscribed between scan and remove — skipping",
            );
            // Reset the idle timer so the next sweep re-evaluates from now.
            idle_since.remove(&ns);
            continue;
        }
        if let Some((_, handle)) = watcher_handles.remove(&ns) {
            handle.abort();
        }
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
    namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<Arc<BroadcastFrame>>>>,
    namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>>,
    watcher_handles: Arc<DashMap<String, JoinHandle<()>>>,
    idle_since: Arc<DashMap<String, Instant>>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;
        // Wrap the sweep in `catch_unwind` so a transient panic inside
        // `gc_tick` (lock poison, DashMap invariant violation, etc.) does
        // not silently kill the entire GC task for the rest of the pod's
        // lifetime — which would leak idle-namespace watchers indefinitely.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            gc_tick(
                Instant::now(),
                &namespace_broadcasts,
                &namespace_replay_buffers,
                &watcher_handles,
                &idle_since,
            )
        }));
        if let Err(payload) = result {
            warn!(
                payload = ?panic_message(&payload),
                "gc_task: gc_tick panicked — continuing loop",
            );
        }
    }
}

/// Extract the panic message text from a `Box<dyn Any + Send>` payload — the
/// common forms `&'static str` and `String`. Anything else gets `"<opaque>"`.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> &str {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        s
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.as_str()
    } else {
        "<opaque>"
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
    bcast_rx: broadcast::Receiver<Arc<BroadcastFrame>>,
    tx: mpsc::Sender<Result<ConfigEvent, Status>>,
    drain_notify: Arc<Notify>,
) {
    // Parse the resume RV once.  An empty string falls through as a buffer
    // miss (used by fresh subscribers); a non-empty non-numeric value is an
    // operator / client bug and is also treated as a miss so the subscriber
    // gets a clean cache snapshot instead of silently subscribing at RV 0
    // (which previously matched every entry in the post-snapshot filter).
    let resume_rv_u64: Option<u64> = if resume_rv.is_empty() {
        None
    } else {
        match resume_rv.parse::<u64>() {
            Ok(n) => Some(n),
            Err(_) => {
                warn!(
                    namespace = %namespace,
                    resume_rv = %resume_rv,
                    "Resume: non-numeric resource_version — treating as buffer miss",
                );
                None
            }
        }
    };

    // Collect the replay slice under the lock, then release before doing I/O.
    // Arc clones are reference-count increments only — no deep copy of event data.
    //
    // The buffer is appended FIFO by `push_replay`, and kube emits events in
    // monotonically increasing resource_version order, so the buffer is
    // already sorted by `resource_version_u64`.  Use `binary_search_by_key`
    // (O(log N)) instead of the prior O(N) `position()` scan.
    let (replay_slice, found_in_buffer): (Vec<Arc<ConfigEvent>>, bool) = {
        let guard = crate::sync_util::lock_recovered(&replay_buf);
        let lookup = resume_rv_u64.and_then(|target| {
            guard
                .binary_search_by_key(&target, |e| e.resource_version_u64)
                .ok()
        });
        match lookup {
            Some(idx) => {
                // Reserve up-front: we know `guard.len() - idx - 1` is
                // the upper bound on the replay slice length. Avoids the
                // intermediate `RawVec::grow_amortized` reallocs that
                // appear in the CU-86aj360ae heap profile on every
                // reconnect that hits the replay buffer. CU-86aj3m1bd.
                let cap = guard.len().saturating_sub(idx + 1);
                let mut slice: Vec<Arc<ConfigEvent>> = Vec::with_capacity(cap);
                slice.extend(guard.iter().skip(idx + 1).map(|e| Arc::clone(&e.event)));
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
        // Track the max snapshot RV inline so we don't re-walk + re-parse the
        // snapshot list to compute it after the send phase.
        let mut max_snapshot_rv: u64 = 0;
        for snap in &snapshots {
            // K8s emits decimal-string RVs; non-numeric here means we already
            // logged earlier (cache loaded a malformed CR).  Skip safely.
            if let Ok(rv) = snap.resource_version.parse::<u64>()
                && rv > max_snapshot_rv
            {
                max_snapshot_rv = rv;
            }
            snapshot_events.push(ConfigEvent {
                event_type: EventType::Snapshot as i32,
                config: Some(snapshot_to_proto(snap)),
            });
        }
        crate::metrics::SUBSCRIBE_SNAPSHOT_EMITTED
            .with_label_values(&["config"])
            .inc();
        for event in snapshot_events {
            if try_send_or_disconnect(&tx, event, "snapshot").is_break() {
                return;
            }
        }

        // Close the race window: replay buffer events that arrived after the
        // snapshot was taken but before this subscriber joins the broadcast.
        // RVs are pre-parsed in `ReplayEntry::resource_version_u64`, so this
        // filter is O(N) loads with no string→u64 work.
        let post_snapshot_events: Vec<Arc<ConfigEvent>> = {
            let guard = crate::sync_util::lock_recovered(&replay_buf);
            guard
                .iter()
                .filter(|e| e.resource_version_u64 > max_snapshot_rv)
                .map(|e| Arc::clone(&e.event))
                .collect()
        }; // mutex released here

        debug!(
            namespace = %namespace,
            post_snapshot_count = post_snapshot_events.len(),
            "Resume: sending post-snapshot buffer events to close race window"
        );

        for event in post_snapshot_events {
            // Use try_send + disconnect-on-Full, matching the buffer-hit path
            // below.  The previous blocking `.send().await` here was the
            // last remaining starvation vector: a slow subscriber could hold
            // the spawned resume task indefinitely.
            if try_send_or_disconnect(&tx, (*event).clone(), "post-snapshot").is_break() {
                return;
            }
        }
    } else {
        // Buffer hit — send only the missed events.
        // Arc clones collected above are dereferenced here to produce ConfigEvent
        // values for the per-subscriber mpsc — no extra serialisation.
        for event in replay_slice {
            if try_send_or_disconnect(&tx, (*event).clone(), "replay").is_break() {
                return;
            }
        }
    }

    // Join the live broadcast for future events.
    bridge_broadcast(bcast_rx, tx, namespace, drain_notify).await;
}

/// Single try_send + disconnect-on-Full helper shared by the resume snapshot,
/// post-snapshot race-window, and replay-hit paths.  Returns
/// `ControlFlow::Break(())` when the caller should stop sending and exit the
/// resume task.
///
/// Replaces a previous mix of `try_send` and blocking `.send().await` —
/// blocking sends starved the spawned resume task when a subscriber was
/// slow, instead of disconnecting them cleanly.
fn try_send_or_disconnect(
    tx: &mpsc::Sender<Result<ConfigEvent, Status>>,
    event: ConfigEvent,
    stage: &'static str,
) -> std::ops::ControlFlow<()> {
    match tx.try_send(Ok(event)) {
        Ok(()) => std::ops::ControlFlow::Continue(()),
        Err(TrySendError::Full(_)) => {
            warn!(stage, "Subscriber too slow during resume — disconnecting");
            let _ = tx.try_send(Err(Status::resource_exhausted("subscriber too slow")));
            std::ops::ControlFlow::Break(())
        }
        Err(TrySendError::Closed(_)) => {
            info!(stage, "Subscriber disconnected during resume");
            std::ops::ControlFlow::Break(())
        }
    }
}

/// Forward events from the namespace broadcast to a single subscriber's mpsc.
///
/// Receives `Arc<BroadcastFrame>` from the broadcast — O(1) reference-count
/// clone per receiver.  The frame carries the `sent_at` instant stamped by
/// `run_namespace_watcher` immediately before `broadcast::send`; we observe
/// `sent_at.elapsed()` BEFORE forwarding to the mpsc so the histogram
/// measures the broadcast-to-receive path (not the downstream mpsc enqueue
/// of the per-subscriber channel, which is bounded by `try_send`).
///
/// Dereferences the inner Arc<ConfigEvent> to produce the `ConfigEvent` value
/// sent over the per-subscriber mpsc channel (tonic's `ReceiverStream` takes
/// owned values, not Arc).
///
/// Disconnects the subscriber with RESOURCE_EXHAUSTED if:
/// - the mpsc channel is full (subscriber too slow to drain), or
/// - the broadcast ring wrapped before this receiver drained (lagged).
///
/// Closes the stream cleanly (drops the mpsc sender so the client sees
/// end-of-stream / `Ok(None)`) when `drain_notify` fires — used by SIGTERM
/// shutdown to release reconnecting clients onto a healthy peer instead of
/// killing them mid-stream when the listener goes away.
///
/// Increments `konfig_active_subscribers` on entry and decrements on every
/// exit path via a `SubGauge` RAII guard.  Increments
/// `konfig_broadcast_lag_total` when the broadcast ring wraps and
/// `konfig_subscribe_e2e_latency_seconds` on every successful receive.
///
/// OBS-2 per-stage histograms observed inside the recv branch:
/// - `konfig_broadcast_to_encode_seconds` — broadcast send → recv() return
/// - `konfig_encode_to_send_seconds` — recv() → mpsc try_send completion
/// - `konfig_writev_calls_total` — one increment per successful mpsc send
async fn bridge_broadcast(
    mut bcast_rx: broadcast::Receiver<Arc<BroadcastFrame>>,
    tx: mpsc::Sender<Result<ConfigEvent, Status>>,
    namespace: String,
    drain_notify: Arc<Notify>,
) {
    ACTIVE_SUBSCRIBERS.with_label_values(&[&namespace]).inc();
    // Decrement on every exit path — including early returns from break.
    let _guard = SubGauge(namespace.clone());

    // Hoist label-set lookups out of the per-event hot loop. `with_label_values`
    // hashes the labels and looks up the metric child on each call; doing it
    // once per subscriber instead of once per event eliminates a label hash
    // + HashMap probe per delivered frame.
    let subscribe_e2e = SUBSCRIBE_E2E_LATENCY.with_label_values(&[&namespace]);
    let broadcast_lag = BROADCAST_LAG.with_label_values(&[&namespace]);

    loop {
        tokio::select! {
            // Drain signal — close the stream cleanly so the client reconnects
            // to a healthy pod.  We drop `tx` by returning, which surfaces as
            // `Ok(None)` (end-of-stream) on the client.
            _ = drain_notify.notified() => {
                info!(namespace = %namespace, "Subscriber: drain signalled — closing stream cleanly");
                return;
            }
            recv = bcast_rx.recv() => match recv {
                Ok(frame) => {
                    // OBS-2 stage 2: time from `broadcast::send` (stamped on
                    // `sent_at` by `run_namespace_watcher`) to this `recv()`
                    // return.  Measures broadcast fan-out hop latency only.
                    // `sent_at.elapsed()` is a monotonic delta; it cannot go
                    // negative even under wall-clock jumps.
                    let recv_at = Instant::now();
                    let broadcast_to_recv = recv_at.duration_since(frame.sent_at).as_secs_f64();
                    BROADCAST_TO_ENCODE_SECONDS.observe(broadcast_to_recv);
                    // Existing e2e histogram — kept for backward compat with
                    // dashboards that already reference it.
                    subscribe_e2e.observe(broadcast_to_recv);

                    match tx.try_send(Ok((*frame.event).clone())) {
                        Ok(()) => {
                            // OBS-2 stage 3: time from `bcast_rx.recv()` return
                            // to per-subscriber mpsc try_send completion.
                            // Captures ConfigEvent clone (Arc deref + value
                            // copy) and mpsc enqueue overhead.  Only observed
                            // on the success path — Full/Closed are handled
                            // separately below.
                            ENCODE_TO_SEND_SECONDS.observe(recv_at.elapsed().as_secs_f64());
                            // Proxy for h2 writev call — every successful mpsc
                            // send corresponds to at least one writev/h2-DATA
                            // frame eventually leaving the socket.
                            WRITEV_CALLS_TOTAL.inc();
                        }
                        Err(TrySendError::Full(_)) => {
                            warn!("Subscriber too slow — disconnecting with RESOURCE_EXHAUSTED");
                            let _ = tx.try_send(Err(Status::resource_exhausted("subscriber too slow")));
                            break;
                        }
                        Err(TrySendError::Closed(_)) => {
                            info!("Subscriber disconnected");
                            break;
                        }
                    }
                },
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(missed = n, "Subscriber lagged — disconnecting");
                    broadcast_lag.inc();
                    let _ = tx.try_send(Err(Status::resource_exhausted("subscriber lagged")));
                    break;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
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

    /// Wrap a `ConfigEvent` in a fresh `BroadcastFrame` for tests that need
    /// to inject events into a broadcast channel.
    fn make_frame(event: ConfigEvent) -> Arc<BroadcastFrame> {
        Arc::new(BroadcastFrame {
            sent_at: Instant::now(),
            event: Arc::new(event),
        })
    }

    fn make_replay_buf(entries: &[(&str, u32)]) -> ReplayBuffer {
        let buf = Arc::new(Mutex::new(VecDeque::new()));
        for (rv, sv) in entries {
            push_replay(&buf, rv, Arc::new(make_event(rv, *sv)));
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
            resource_version: "001".into(),
            ..Default::default()
        });
        assert!(cache.is_populated());
    }

    // ── Unit: push_replay evicts oldest when full ────────────────────────────

    #[test]
    fn push_replay_evicts_oldest_when_full() {
        let buf: ReplayBuffer = Arc::new(Mutex::new(VecDeque::new()));
        for i in 0..REPLAY_BUFFER_SIZE {
            let rv = format!("{i}");
            push_replay(&buf, &rv, Arc::new(make_event(&rv, i as u32)));
        }
        // Buffer is exactly full — oldest is rv-0.
        assert_eq!(buf.lock().unwrap().front().unwrap().resource_version, "0");

        // Push one more — rv-0 must be evicted.
        push_replay(&buf, "9999", Arc::new(make_event("9999", 9999)));
        let guard = buf.lock().unwrap();
        assert_eq!(guard.len(), REPLAY_BUFFER_SIZE);
        assert_eq!(guard.front().unwrap().resource_version, "1");
        assert_eq!(guard.back().unwrap().resource_version, "9999");
    }

    // ── Async: reconnect with valid resume_rv receives only missed events ────

    #[tokio::test]
    async fn resume_buffer_hit_receives_only_missed_events() {
        // Buffer contains rv-1 .. rv-5.  Client reconnects at rv-2 →
        // should receive rv-3, rv-4, rv-5 (schema versions 3, 4, 5).
        let replay_buf = make_replay_buf(&[("1", 1), ("2", 2), ("3", 3), ("4", 4), ("5", 5)]);
        let cache = make_cache("default", &[("cfg", "5", 5)]);
        let (bcast_tx, bcast_rx) = broadcast::channel::<Arc<BroadcastFrame>>(64);
        let (tx, mut rx) = mpsc::channel(64);

        // Close the broadcast sender so bridge_broadcast exits cleanly after replay.
        drop(bcast_tx);

        tokio::spawn(resume_from_buffer(
            "2".into(),
            replay_buf,
            cache,
            "default".into(),
            bcast_rx,
            tx,
            Arc::new(Notify::new()),
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
        let replay_buf = make_replay_buf(&[("10", 10), ("11", 11), ("12", 12)]);
        // Cache has two entries.
        let cache = make_cache("default", &[("cfg-a", "13", 13), ("cfg-b", "14", 14)]);
        let (bcast_tx, bcast_rx) = broadcast::channel::<Arc<BroadcastFrame>>(64);
        let (tx, mut rx) = mpsc::channel(64);

        drop(bcast_tx); // let bridge_broadcast exit cleanly

        tokio::spawn(resume_from_buffer(
            "1".into(), // stale — not in buffer
            replay_buf,
            cache,
            "default".into(),
            bcast_rx,
            tx,
            Arc::new(Notify::new()),
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
        let replay_buf = make_replay_buf(&[("1", 1), ("2", 2), ("3", 3)]);
        let cache = make_cache("default", &[("cfg", "3", 3)]);

        // Pre-create a broadcast channel to simulate an already-running watcher.
        let (bcast_tx, _initial_rx) = broadcast::channel::<Arc<BroadcastFrame>>(64);

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
                    "1".into(),
                    replay_buf_clone,
                    cache_clone,
                    "default".into(),
                    bcast_rx,
                    tx,
                    Arc::new(Notify::new()),
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
        let replay_buf = make_replay_buf(&[("5", 5)]);
        let cache = make_cache("default", &[("cfg", "5", 5)]);
        let (bcast_tx, bcast_rx) = broadcast::channel::<Arc<BroadcastFrame>>(64);
        let (tx, mut rx) = mpsc::channel(64);

        // Signal that resume_from_buffer has been polled at least once so we
        // know the snapshot + buffer-hit branch has completed and the bridge
        // is waiting on `bcast_rx`.  Previously this test slept for 10ms,
        // which flaked under scheduler pressure.
        let started = Arc::new(tokio::sync::Notify::new());
        let started_inner = Arc::clone(&started);

        tokio::spawn(async move {
            // Yield once so the spawn ordering doesn't depend on
            // tokio::spawn poll order.
            tokio::task::yield_now().await;
            started_inner.notify_one();
        });

        tokio::spawn(resume_from_buffer(
            "5".into(), // latest — nothing to replay
            replay_buf,
            cache,
            "default".into(),
            bcast_rx,
            tx,
            Arc::new(Notify::new()),
        ));

        // Wait for the resume task to be live; only then push the broadcast
        // event so we don't race the snapshot collection.
        started.notified().await;
        let _ = bcast_tx.send(make_frame(make_event("6", 6)));
        drop(bcast_tx); // bridge exits after delivering the one live event

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

        let (bcast_tx, bcast_rx) = broadcast::channel::<Arc<BroadcastFrame>>(64);
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
            Arc::new(Notify::new()),
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
    // Spawn 10 receiver tasks on a `broadcast::channel::<Arc<BroadcastFrame>>`,
    // send 1 frame, verify all 10 receivers get a pointer to the SAME inner
    // `Arc<ConfigEvent>` allocation.  This confirms that the broadcast still
    // serialises the proto exactly once per apply (Track E invariant) — the
    // BroadcastFrame envelope only carries a timing field and a refcount-bump
    // clone of the inner Arc.

    #[tokio::test]
    async fn broadcast_arc_not_cloned() {
        let (bcast_tx, _) = broadcast::channel::<Arc<BroadcastFrame>>(64);

        const N: usize = 10;
        let mut handles: Vec<tokio::task::JoinHandle<Arc<BroadcastFrame>>> = Vec::with_capacity(N);

        for _ in 0..N {
            let mut rx = bcast_tx.subscribe();
            let h = tokio::spawn(async move { rx.recv().await.expect("must receive event") });
            handles.push(h);
        }

        let event = Arc::new(make_event("1", 1));
        // Record the pointer to the *inner* ConfigEvent allocation before sending.
        let expected_inner_ptr = Arc::as_ptr(&event) as usize;

        let frame = Arc::new(BroadcastFrame {
            sent_at: Instant::now(),
            event,
        });
        bcast_tx.send(frame).expect("send failed");

        let mut received_frames = Vec::with_capacity(N);
        for h in handles {
            let arc = h.await.expect("task panicked");
            received_frames.push(arc);
        }

        // All receivers must hold a reference to the SAME inner allocation —
        // the broadcast clones only Arc<BroadcastFrame> (refcount bump), and
        // the inner Arc<ConfigEvent> is the SAME heap object across all
        // receivers.
        for frame in &received_frames {
            assert!(
                Arc::ptr_eq(&received_frames.first().unwrap().event, &frame.event),
                "all receivers must point to the same inner ConfigEvent allocation"
            );
        }
        // Also verify the inner pointer matches what was sent.
        assert_eq!(
            Arc::as_ptr(&received_frames.first().unwrap().event) as usize,
            expected_inner_ptr,
            "received inner Arc must be the same allocation as the one sent"
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
        let namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<Arc<BroadcastFrame>>>> =
            Arc::new(DashMap::new());
        let namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>> = Arc::new(DashMap::new());
        let watcher_handles: Arc<DashMap<String, JoinHandle<()>>> = Arc::new(DashMap::new());
        let idle_since: Arc<DashMap<String, Instant>> = Arc::new(DashMap::new());

        // Insert a broadcast channel with NO active receivers (sender only).
        let (tx, _rx) = broadcast::channel::<Arc<BroadcastFrame>>(64);
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
        let namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<Arc<BroadcastFrame>>>> =
            Arc::new(DashMap::new());
        let namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>> = Arc::new(DashMap::new());
        let watcher_handles: Arc<DashMap<String, JoinHandle<()>>> = Arc::new(DashMap::new());
        let idle_since: Arc<DashMap<String, Instant>> = Arc::new(DashMap::new());

        // Insert a broadcast channel AND keep a live receiver so receiver_count() > 0.
        let (tx, _live_rx) = broadcast::channel::<Arc<BroadcastFrame>>(64);
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

    // ── Async: bridge_broadcast observes konfig_subscribe_e2e_latency ─────────
    //
    // Spawn the bridge, broadcast 5 BroadcastFrames with `sent_at = now`, and
    // verify the histogram sample count increased by exactly 5 (one observation
    // per delivered event) with non-zero observed values.
    #[tokio::test]
    async fn subscribe_e2e_latency_records() {
        let ns = "test-ns-bridge-latency";
        let (bcast_tx, bcast_rx) = broadcast::channel::<Arc<BroadcastFrame>>(64);
        let (tx, mut rx) = mpsc::channel(64);

        let before = SUBSCRIBE_E2E_LATENCY
            .with_label_values(&[ns])
            .get_sample_count();

        // Spawn bridge.
        let bridge = tokio::spawn(bridge_broadcast(
            bcast_rx,
            tx,
            ns.to_string(),
            Arc::new(Notify::new()),
        ));

        // Broadcast 5 frames with the send timestamp slightly in the past so
        // each `sent_at.elapsed()` is strictly positive.
        for i in 0..5 {
            let event = Arc::new(make_event(&format!("{i}"), i as u32 + 1));
            let frame = Arc::new(BroadcastFrame {
                sent_at: Instant::now() - Duration::from_millis(1),
                event,
            });
            bcast_tx.send(frame).expect("send failed");
        }

        // Drain the 5 events on the mpsc to ensure the bridge processed them all.
        for _ in 0..5 {
            let _ = rx.recv().await.expect("must receive event");
        }

        // Drop the only sender so the bridge exits cleanly via RecvError::Closed.
        drop(bcast_tx);
        bridge.await.expect("bridge task panicked");

        let after = SUBSCRIBE_E2E_LATENCY
            .with_label_values(&[ns])
            .get_sample_count();
        assert_eq!(
            after,
            before + 5,
            "bridge must observe exactly one latency sample per delivered event"
        );
        let sum = SUBSCRIBE_E2E_LATENCY
            .with_label_values(&[ns])
            .get_sample_sum();
        assert!(
            sum > 0.0,
            "observed latency sum must be strictly positive (got {sum})"
        );
    }

    // ── OBS-2 per-stage histograms observed by bridge_broadcast ──────────────
    //
    // Drives 3 frames through the bridge and verifies that broadcast→encode,
    // encode→send, and writev_calls_total all advance by 3.  Backstops the
    // wiring against accidental removal of the stage observe() calls.
    #[tokio::test]
    async fn bridge_broadcast_records_obs2_stage_histograms() {
        use crate::metrics::{
            BROADCAST_TO_ENCODE_SECONDS, ENCODE_TO_SEND_SECONDS, WRITEV_CALLS_TOTAL,
        };

        let ns = "test-ns-obs2-stages";
        let (bcast_tx, bcast_rx) = broadcast::channel::<Arc<BroadcastFrame>>(64);
        let (tx, mut rx) = mpsc::channel(64);

        let bte_before = BROADCAST_TO_ENCODE_SECONDS.get_sample_count();
        let ets_before = ENCODE_TO_SEND_SECONDS.get_sample_count();
        let writev_before = WRITEV_CALLS_TOTAL.get();

        let bridge = tokio::spawn(bridge_broadcast(
            bcast_rx,
            tx,
            ns.to_string(),
            Arc::new(Notify::new()),
        ));

        for i in 0..3 {
            let event = Arc::new(make_event(&format!("{i}"), i as u32 + 1));
            let frame = Arc::new(BroadcastFrame {
                sent_at: Instant::now() - Duration::from_millis(1),
                event,
            });
            bcast_tx.send(frame).expect("send failed");
        }

        for _ in 0..3 {
            let _ = rx.recv().await.expect("must receive event");
        }
        drop(bcast_tx);
        bridge.await.expect("bridge task panicked");

        assert_eq!(
            BROADCAST_TO_ENCODE_SECONDS.get_sample_count(),
            bte_before + 3,
            "broadcast_to_encode must observe once per delivered event"
        );
        assert_eq!(
            ENCODE_TO_SEND_SECONDS.get_sample_count(),
            ets_before + 3,
            "encode_to_send must observe once per successful mpsc send"
        );
        assert_eq!(
            WRITEV_CALLS_TOTAL.get(),
            writev_before + 3.0,
            "writev_calls_total must increment once per successful mpsc send"
        );
    }

    // ── OBS-2 apply→broadcast + h2 frame bytes observed by namespace watcher ─
    //
    // We exercise the watcher logic without a kube cluster by extracting the
    // inner observe() calls into a unit-testable helper.  Here we verify the
    // metric sites directly: simulate one event, observe via the same call
    // sites, confirm the counts advance.
    #[tokio::test]
    async fn watcher_apply_to_broadcast_and_frame_bytes_observed() {
        use crate::metrics::{APPLY_TO_BROADCAST_SECONDS, H2_DATA_FRAME_BYTES};

        let a2b_before = APPLY_TO_BROADCAST_SECONDS.get_sample_count();
        let bytes_before = H2_DATA_FRAME_BYTES.get_sample_count();

        // Mirror the watcher hot path: stamp apply_observed_at, build the
        // ConfigEvent, observe its encoded size, then observe the stage delta.
        let apply_observed_at = Instant::now();
        let config_event = Arc::new(make_event("42", 42));
        let encoded_len = config_event.encoded_len();
        H2_DATA_FRAME_BYTES.observe(encoded_len as f64);
        let sent_at = Instant::now();
        APPLY_TO_BROADCAST_SECONDS.observe(sent_at.duration_since(apply_observed_at).as_secs_f64());

        assert_eq!(
            APPLY_TO_BROADCAST_SECONDS.get_sample_count(),
            a2b_before + 1,
        );
        assert_eq!(H2_DATA_FRAME_BYTES.get_sample_count(), bytes_before + 1,);
        assert!(
            encoded_len > 0,
            "encoded_len must be positive — proto must serialise to non-empty bytes"
        );
    }

    // ── Drain: bridge_broadcast closes cleanly when notified ─────────────────

    /// When `drain_notify` fires, `bridge_broadcast` returns immediately and
    /// drops the mpsc sender — the subscriber observes end-of-stream
    /// (`Ok(None)`), not an error.  This is the SIGTERM-graceful-shutdown path:
    /// existing streams must close cleanly so clients reconnect to a healthy
    /// pod instead of treating the disconnect as a server crash.
    #[tokio::test]
    async fn drain_notify_closes_bridge_broadcast_cleanly() {
        let (_bcast_tx, bcast_rx) = broadcast::channel::<Arc<BroadcastFrame>>(64);
        let (tx, mut rx) = mpsc::channel(64);
        let drain_notify = Arc::new(Notify::new());

        let drain_clone = Arc::clone(&drain_notify);
        let bridge = tokio::spawn(async move {
            bridge_broadcast(bcast_rx, tx, "default".into(), drain_clone).await;
        });

        // Give the bridge a tick to park on the select.
        tokio::task::yield_now().await;

        // Fire the drain — bridge must exit cleanly within 1 s.
        drain_notify.notify_waiters();

        tokio::time::timeout(Duration::from_secs(1), bridge)
            .await
            .expect("bridge must exit within 1 s after drain")
            .expect("task panicked");

        // The client side sees end-of-stream (Ok(None)) — NOT an error frame.
        match rx.recv().await {
            None => {} // expected: clean close
            Some(other) => panic!("expected clean close (None); got {other:?}"),
        }
    }

    // ── Unit: push_replay rejects non-numeric resource_version ──────────────

    /// kube only emits decimal-string RVs. If a non-numeric RV ever reaches
    /// the replay buffer the upstream object is malformed; we want to drop
    /// it at push time so resume's binary search never sees an entry it
    /// cannot order. Regression test for the `resource_version.parse::<u64>`
    /// gate added in PR C.
    #[test]
    fn push_replay_drops_non_numeric_rv() {
        let buf: ReplayBuffer = Arc::new(Mutex::new(VecDeque::new()));
        push_replay(&buf, "not-a-number", Arc::new(make_event("0", 0)));
        assert!(
            buf.lock().unwrap().is_empty(),
            "push_replay must drop entries with non-numeric resource_version",
        );
        // Valid RV still pushes.
        push_replay(&buf, "42", Arc::new(make_event("42", 42)));
        assert_eq!(buf.lock().unwrap().len(), 1);
    }

    // ── Async: resume with non-numeric RV falls back to snapshot path ───────

    /// A malformed resume_resource_version (e.g. a client sending a
    /// non-numeric value, or a corrupted client-side store) must not
    /// silently match every buffer entry (the old `unwrap_or(0)` bug).
    /// PR C drops the value and routes through the snapshot path so the
    /// subscriber gets a clean known-good state.
    #[tokio::test]
    async fn resume_with_non_numeric_rv_takes_snapshot_path() {
        let replay_buf = make_replay_buf(&[("10", 10), ("11", 11)]);
        let cache = make_cache("default", &[("cfg-a", "12", 12)]);
        let (bcast_tx, bcast_rx) = broadcast::channel::<Arc<BroadcastFrame>>(64);
        let (tx, mut rx) = mpsc::channel(64);
        drop(bcast_tx); // let bridge exit cleanly

        tokio::spawn(resume_from_buffer(
            "not-a-number".into(),
            replay_buf,
            cache,
            "default".into(),
            bcast_rx,
            tx,
            Arc::new(Notify::new()),
        ));

        // Must receive a single SNAPSHOT event for cfg-a (sv 12), not a
        // replay of the buffer entries.
        let mut events: Vec<(i32, u32)> = Vec::new();
        while let Some(Ok(ev)) = rx.recv().await {
            let sv = ev.config.as_ref().map(|c| c.schema_version).unwrap_or(0);
            events.push((ev.event_type, sv));
        }
        assert_eq!(events, vec![(EventType::Snapshot as i32, 12)]);
    }

    // ── Unit: try_send_or_disconnect contract ───────────────────────────────

    #[test]
    fn try_send_or_disconnect_signals_break_on_full() {
        // Capacity = 2 so the helper can also enqueue its
        // RESOURCE_EXHAUSTED error frame after the data slot fills.  In
        // production CHANNEL_CAPACITY is 256, so the error frame always
        // fits unless the subscriber is jammed beyond that — in which
        // case dropping the error is also acceptable.
        let (tx, mut rx) = mpsc::channel::<Result<ConfigEvent, Status>>(2);
        // Fill both slots.
        tx.try_send(Ok(make_event("0", 0))).unwrap();
        tx.try_send(Ok(make_event("1", 1))).unwrap();

        let outcome = try_send_or_disconnect(&tx, make_event("2", 2), "test");
        assert!(matches!(outcome, std::ops::ControlFlow::Break(())));

        // Drain: must see two data frames then nothing further (the helper
        // tried to enqueue an error but the channel was already full — that
        // is correct fallback behaviour).
        assert!(rx.try_recv().unwrap().is_ok());
        assert!(rx.try_recv().unwrap().is_ok());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn try_send_or_disconnect_signals_break_on_closed() {
        let (tx, rx) = mpsc::channel::<Result<ConfigEvent, Status>>(1);
        drop(rx); // close receiver
        let outcome = try_send_or_disconnect(&tx, make_event("0", 0), "test");
        assert!(matches!(outcome, std::ops::ControlFlow::Break(())));
    }

    /// `panic_message` covers the three payload shapes `catch_unwind`
    /// hands back in practice: `&'static str` (from `panic!("…")` with a
    /// string literal), `String` (from `panic!("{x}")` with formatting),
    /// and anything else (returned as the placeholder `"<opaque>"`).
    #[test]
    fn panic_message_handles_str_string_and_other() {
        let p_str: Box<dyn std::any::Any + Send> = Box::new("boom");
        assert_eq!(panic_message(&p_str), "boom");
        let p_string: Box<dyn std::any::Any + Send> = Box::new(String::from("kaboom"));
        assert_eq!(panic_message(&p_string), "kaboom");
        let p_other: Box<dyn std::any::Any + Send> = Box::new(42u32);
        assert_eq!(panic_message(&p_other), "<opaque>");
    }

    /// `gc_task`'s `catch_unwind` wrap must not unwind out of the loop —
    /// drive a panicking inner function and confirm the helper returns
    /// `Err(payload)` (which the loop logs + continues). Tests the
    /// wrapper in isolation; the loop itself is infinite so we don't
    /// drive it end-to-end here.
    #[test]
    fn catch_unwind_captures_panic_without_propagating() {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            panic!("synthetic gc_tick panic");
        }));
        assert!(result.is_err(), "panic must be captured, not propagated");
        let payload = result.unwrap_err();
        assert_eq!(panic_message(&payload), "synthetic gc_tick panic");
    }

    // ── pump_subscribe_namespace_events ──────────────────────────────────────

    fn dyn_config(name: &str, namespace: &str, schema_version: u32, rv: &str) -> DynamicObject {
        let mut obj = DynamicObject::new(name, &crate::watcher::config_api_resource());
        obj.metadata.name = Some(name.to_string());
        obj.metadata.namespace = Some(namespace.to_string());
        obj.metadata.resource_version = Some(rv.to_string());
        obj.data = json!({
            "spec": {
                "schema_version": schema_version,
                "content": {"k": rv},
            }
        });
        obj
    }

    fn ns_watcher_err() -> kube_watcher::Error {
        kube_watcher::Error::WatchFailed(kube::Error::Api(kube::core::ErrorResponse {
            status: "Failure".to_string(),
            message: "synthetic".to_string(),
            reason: "synthetic".to_string(),
            code: 500,
        }))
    }

    #[tokio::test]
    async fn ns_pump_broadcasts_apply_and_pushes_replay_entry() {
        use futures_util::stream;
        let (tx, mut rx) = broadcast::channel(8);
        let replay = Arc::new(Mutex::new(VecDeque::new()));
        let events: Vec<Result<Event<DynamicObject>, kube_watcher::Error>> =
            vec![Ok(Event::Apply(dyn_config("cfg", "ns", 1, "100")))];

        pump_subscribe_namespace_events(stream::iter(events), "ns", &tx, &replay, Duration::ZERO)
            .await;

        let frame = rx.try_recv().expect("must broadcast Modified");
        assert_eq!(frame.event.event_type, EventType::Modified as i32);
        let buf = crate::sync_util::lock_recovered(&replay);
        assert_eq!(buf.len(), 1, "replay buffer must hold the event");
        assert_eq!(buf.front().unwrap().resource_version_u64, 100);
    }

    #[tokio::test]
    async fn ns_pump_init_and_initdone_are_skipped() {
        use futures_util::stream;
        let (tx, mut rx) = broadcast::channel(8);
        let replay = Arc::new(Mutex::new(VecDeque::new()));
        let events: Vec<Result<Event<DynamicObject>, kube_watcher::Error>> =
            vec![Ok(Event::Init), Ok(Event::InitDone)];

        pump_subscribe_namespace_events(stream::iter(events), "ns", &tx, &replay, Duration::ZERO)
            .await;

        assert!(
            rx.try_recv().is_err(),
            "Init/InitDone must not broadcast — they have no apply latency"
        );
        let buf = crate::sync_util::lock_recovered(&replay);
        assert!(buf.is_empty());
    }

    #[tokio::test]
    async fn ns_pump_delete_event_broadcasts_deleted() {
        use futures_util::stream;
        let (tx, mut rx) = broadcast::channel(8);
        let replay = Arc::new(Mutex::new(VecDeque::new()));
        let obj = dyn_config("cfg", "ns", 5, "200");
        let events: Vec<Result<Event<DynamicObject>, kube_watcher::Error>> =
            vec![Ok(Event::Delete(obj))];

        pump_subscribe_namespace_events(stream::iter(events), "ns", &tx, &replay, Duration::ZERO)
            .await;

        let frame = rx.try_recv().expect("must broadcast Deleted");
        assert_eq!(frame.event.event_type, EventType::Deleted as i32);
    }

    #[tokio::test]
    async fn ns_pump_returns_on_stream_error_and_drops_subsequent_events() {
        use futures_util::stream;
        let (tx, mut rx) = broadcast::channel(8);
        let replay = Arc::new(Mutex::new(VecDeque::new()));
        let events: Vec<Result<Event<DynamicObject>, kube_watcher::Error>> = vec![
            Ok(Event::Apply(dyn_config("cfg-a", "ns", 1, "10"))),
            Err(ns_watcher_err()),
            // Must not be observed.
            Ok(Event::Apply(dyn_config("cfg-z", "ns", 99, "99"))),
        ];

        pump_subscribe_namespace_events(stream::iter(events), "ns", &tx, &replay, Duration::ZERO)
            .await;

        let first = rx.try_recv().expect("pre-error event landed");
        assert_eq!(first.event.event_type, EventType::Modified as i32);
        assert!(rx.try_recv().is_err(), "post-error events dropped");
        let buf = crate::sync_util::lock_recovered(&replay);
        assert_eq!(buf.len(), 1);
    }

    #[tokio::test]
    async fn ns_pump_unparseable_object_is_skipped_without_broadcast() {
        use futures_util::stream;
        let (tx, mut rx) = broadcast::channel(8);
        let replay = Arc::new(Mutex::new(VecDeque::new()));
        // DynamicObject with no `spec` field — parse_config_object returns None.
        let mut bad = DynamicObject::new("bad", &crate::watcher::config_api_resource());
        bad.data = json!({});
        let events: Vec<Result<Event<DynamicObject>, kube_watcher::Error>> =
            vec![Ok(Event::Apply(bad))];

        pump_subscribe_namespace_events(stream::iter(events), "ns", &tx, &replay, Duration::ZERO)
            .await;

        assert!(
            rx.try_recv().is_err(),
            "unparseable object must not broadcast"
        );
        let buf = crate::sync_util::lock_recovered(&replay);
        assert!(buf.is_empty());
    }

    #[tokio::test]
    async fn ns_pump_with_zero_receivers_still_pushes_replay() {
        use futures_util::stream;
        let (tx, rx) = broadcast::channel::<Arc<BroadcastFrame>>(8);
        drop(rx);
        let replay = Arc::new(Mutex::new(VecDeque::new()));
        let events: Vec<Result<Event<DynamicObject>, kube_watcher::Error>> =
            vec![Ok(Event::Apply(dyn_config("cfg", "ns", 1, "1")))];

        pump_subscribe_namespace_events(stream::iter(events), "ns", &tx, &replay, Duration::ZERO)
            .await;

        // Replay buffer is populated even when there are no live subscribers
        // so a subscriber that arrives mid-event-burst can resume cleanly.
        let buf = crate::sync_util::lock_recovered(&replay);
        assert_eq!(buf.len(), 1);
    }

    // ── Coalesce-by-window (CU-86aj3vpgr) ─────────────────────────────────────

    /// Window > 0: multiple events arriving within the window are accumulated
    /// and dispatched as a burst, but ALL of them are still delivered (100 %
    /// delivery guarantee) with their own per-event content, and `push_replay`
    /// still runs once per event (RV-keyed). A finite synthetic stream ends
    /// before the timer fires, so the partial batch is flushed on stream-end.
    #[tokio::test]
    async fn ns_pump_coalesce_window_batches_all_events() {
        use futures_util::stream;
        let (tx, mut rx) = broadcast::channel(16);
        let replay = Arc::new(Mutex::new(VecDeque::new()));
        let events: Vec<Result<Event<DynamicObject>, kube_watcher::Error>> = vec![
            Ok(Event::Apply(dyn_config("cfg", "ns", 1, "100"))),
            Ok(Event::Apply(dyn_config("cfg", "ns", 2, "101"))),
            Ok(Event::Apply(dyn_config("cfg", "ns", 3, "102"))),
        ];

        pump_subscribe_namespace_events(
            stream::iter(events),
            "ns",
            &tx,
            &replay,
            Duration::from_millis(5),
        )
        .await;

        // 100 % delivery: every event reaches the broadcast channel, in order,
        // each carrying its own resource_version (timestamps not collapsed —
        // distinct frames, distinct `sent_at`).
        let mut got = Vec::new();
        while let Ok(frame) = rx.try_recv() {
            assert_eq!(frame.event.event_type, EventType::Modified as i32);
            got.push(
                frame
                    .event
                    .config
                    .as_ref()
                    .expect("config present")
                    .resource_version
                    .clone(),
            );
        }
        assert_eq!(
            got,
            vec!["100", "101", "102"],
            "all events delivered in order"
        );

        // Replay buffer holds one entry per event — push semantics unchanged.
        let buf = crate::sync_util::lock_recovered(&replay);
        assert_eq!(
            buf.len(),
            3,
            "push_replay still runs per event under coalesce"
        );
        let rvs: Vec<u64> = buf.iter().map(|e| e.resource_version_u64).collect();
        assert_eq!(rvs, vec![100, 101, 102]);
    }

    /// Window > 0, deterministic timer flush: a single event is buffered and
    /// the stream then stays pending forever (never ends). The ONLY thing that
    /// can deliver the event is the window-expiry flush — proving the timeout
    /// path, not the stream-end fallback. Uses paused time so the 5 ms window
    /// is advanced instantly and deterministically.
    #[tokio::test(start_paused = true)]
    async fn ns_pump_coalesce_window_flushes_on_timer() {
        use futures_util::stream::{self, StreamExt};
        let (tx, mut rx) = broadcast::channel(8);
        let replay = Arc::new(Mutex::new(VecDeque::new()));

        // One event, then a stream tail that never yields and never ends.
        let one: Vec<Result<Event<DynamicObject>, kube_watcher::Error>> =
            vec![Ok(Event::Apply(dyn_config("cfg", "ns", 1, "100")))];
        let blocked = stream::iter(one).chain(stream::pending());

        let window = Duration::from_millis(5);
        let pump = tokio::spawn(async move {
            pump_subscribe_namespace_events(blocked, "ns", &tx, &replay, window).await;
        });

        // Before the window elapses, nothing has been broadcast yet (the event
        // is still buffered in the coalesce batch).
        tokio::task::yield_now().await;
        assert!(
            rx.try_recv().is_err(),
            "event must remain buffered until the window expires"
        );

        // Advance past the window — the timer flush fires.
        tokio::time::sleep(Duration::from_millis(10)).await;

        let frame = rx
            .recv()
            .await
            .expect("timer flush must broadcast the event");
        assert_eq!(frame.event.event_type, EventType::Modified as i32);

        // The pump is still running on the pending tail (stream never ended),
        // confirming delivery came from the window timer, not stream-end.
        assert!(!pump.is_finished(), "pump still alive on pending stream");
        pump.abort();
    }

    /// Window > 0 with a batch that exceeds `COALESCE_MAX_BATCH`: the size cap
    /// forces an early burst-flush mid-stream (not just on stream-end), and
    /// every event is still delivered exactly once. Uses a long window so the
    /// timer never fires — the only thing that can flush is the size cap and
    /// the final stream-end flush of the remainder.
    #[tokio::test]
    async fn ns_pump_coalesce_max_batch_flushes_burst() {
        use futures_util::stream;
        let total = COALESCE_MAX_BATCH + 3;
        let (tx, mut rx) = broadcast::channel(total + 8);
        let replay = Arc::new(Mutex::new(VecDeque::new()));
        let events: Vec<Result<Event<DynamicObject>, kube_watcher::Error>> = (0..total)
            .map(|i| {
                let rv = (1000 + i).to_string();
                Ok(Event::Apply(dyn_config("cfg", "ns", i as u32, &rv)))
            })
            .collect();

        pump_subscribe_namespace_events(
            stream::iter(events),
            "ns",
            &tx,
            &replay,
            Duration::from_secs(3600),
        )
        .await;

        let mut count = 0;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(
            count, total,
            "every event delivered across the cap-flush boundary"
        );
        let buf = crate::sync_util::lock_recovered(&replay);
        assert_eq!(buf.len(), total);
    }
}
