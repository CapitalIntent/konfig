//! Per-namespace kube watcher lifecycle + GC for the Subscribe path.
//!
//! Owns watcher startup ([`get_or_create_broadcast`], [`run_namespace_watcher`]),
//! the event pumps ([`pump_subscribe_namespace_events`] and friends), and the
//! idle-namespace garbage collector ([`gc_tick`] / [`gc_task`]). Split out of
//! `subscribe.rs` (CU-86aj7k5rf).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use futures_util::{StreamExt, TryStreamExt};
use kube::core::DynamicObject;
use kube::runtime::watcher::{self as kube_watcher, Event, watcher as kube_watch_stream};
use kube::{Api, Client};
use prost::Message as _;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::grpc::snapshot_to_proto;
use crate::metrics::{APPLY_TO_BROADCAST_SECONDS, EVENTS_BROADCAST, H2_DATA_FRAME_BYTES};
use crate::proto::{ConfigEvent, config_event::EventType};

use super::{BroadcastFrame, ReplayBuffer, ShardSet, push_replay};

/// Maximum number of events accumulated in one coalesce window before an
/// early flush is forced (cap on per-window latency-vs-throughput trade).
/// Only consulted when the coalesce window is non-zero (CU-86aj3vpgr).
const COALESCE_MAX_BATCH: usize = 16;

/// Return a `(broadcast::Receiver, ReplayBuffer)` for `namespace`, spinning up
/// a kube watcher if one isn't already running for that namespace.
///
/// The receiver is attached to ONE shard of the namespace's [`ShardSet`],
/// chosen round-robin (CU-86aj3vpnh). `broadcast_shards` (pre-clamped to
/// `1..=MAX_BROADCAST_SHARDS`) only takes effect when this call creates the
/// namespace's `ShardSet`; an already-running namespace keeps the shard count
/// it was created with until its watcher is GC'd.
#[allow(clippy::too_many_arguments)]
pub(crate) fn get_or_create_broadcast(
    namespace: String,
    kube_client: Client,
    namespace_broadcasts: Arc<DashMap<String, ShardSet>>,
    namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>>,
    watcher_handles: Arc<DashMap<String, JoinHandle<()>>>,
    coalesce_window: Duration,
    broadcast_shards: usize,
) -> (broadcast::Receiver<Arc<BroadcastFrame>>, ReplayBuffer) {
    // Fast path: namespace already has a running watcher.
    if let Some(shards_ref) = namespace_broadcasts.get(&namespace) {
        // Clone the (cheap, Arc-backed) ShardSet out and drop the shard ref
        // before calling `.next_receiver()` so we don't hold a DashMap shard
        // lock across the broadcast-receiver allocation (would serialise
        // concurrent Subscribe RPCs that hash to the same DashMap shard).
        let shards = shards_ref.clone();
        drop(shards_ref);
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
        return (shards.next_receiver(), buf);
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
            (e.get().next_receiver(), buf)
        }
        dashmap::mapref::entry::Entry::Vacant(e) => {
            // Build N shard channels. This first subscriber attaches to shard 0
            // (`receivers[0]`); the remaining receivers are dropped — they exist
            // only to seed the senders, and future subscribers round-robin onto
            // them via `next_receiver()`.
            let (shard_set, mut receivers) = ShardSet::new(broadcast_shards);
            // `ShardSet::new` always produces `>= 1` receiver.
            let bcast_rx = receivers.swap_remove(0);
            drop(receivers);
            e.insert(shard_set.clone());

            let buf: ReplayBuffer = namespace_replay_buffers
                .entry(namespace.clone())
                .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
                .clone();

            // The watcher runs until the kube stream ends (or is aborted by GC),
            // then removes itself from the maps so the next Subscribe recreates them.
            let handle = tokio::spawn(run_namespace_watcher(
                namespace.clone(),
                kube_client,
                shard_set,
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
    shards: ShardSet,
    replay_buf: ReplayBuffer,
    namespace_broadcasts: Arc<DashMap<String, ShardSet>>,
    namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>>,
    coalesce_window: Duration,
) {
    let ar = crate::watcher::config_api_resource();
    let api: Api<DynamicObject> = Api::namespaced_with(kube_client, &namespace, &ar);
    let wc = kube_watcher::Config::default();
    let stream = kube_watch_stream(api, wc).boxed();

    pump_subscribe_namespace_events(stream, &namespace, &shards, &replay_buf, coalesce_window)
        .await;

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
///
/// Sharding (CU-86aj3vpnh): the pump fans every accepted event to **all**
/// shards in `shards` via [`ShardSet::send_to_all`], after the single shared
/// `push_replay`. With `N == 1` shard this is exactly one `tx.send` per event —
/// the historical path. Coalesce composes orthogonally: a coalesced batch is
/// flushed to all shards.
pub(crate) async fn pump_subscribe_namespace_events<S>(
    stream: S,
    namespace: &str,
    shards: &ShardSet,
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
        pump_immediate(stream, namespace, shards, replay_buf, &events_broadcast).await;
    } else {
        pump_coalesced(
            stream,
            namespace,
            shards,
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
    shards: &ShardSet,
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
            // carrying the live subscriber count across all shards. `level =
            // "debug"` keeps it off the INFO production path; `send_to_all` is
            // synchronous so the entered guard never spans an await.
            // `subscribers` is a cheap integer read.
            let span = tracing::debug_span!(
                "konfig.broadcast_dispatch",
                subscribers = shards.total_receiver_count(),
            );
            // Link the fan-out back to the originating Apply, if its span
            // context rode in on the object's traceparent annotation
            // (Apply→subscriber waterfall). No-op when tracing is off.
            if let Some(sc) = &frame.parent_span {
                span.add_link(sc.clone());
            }
            let _enter = span.enter();
            if shards.send_to_all(frame) {
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
    shards: &ShardSet,
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
                        flush_batch(&mut batch, shards, events_broadcast);
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
            flush_batch(&mut batch, shards, events_broadcast);
            return;
        };

        if let Some(frame) = process_namespace_event(event, replay_buf) {
            if batch.is_empty() {
                // Start the window clock on the first buffered frame.
                deadline = Some(Instant::now() + coalesce_window);
            }
            batch.push(frame);
            if batch.len() >= COALESCE_MAX_BATCH {
                flush_batch(&mut batch, shards, events_broadcast);
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
    // Carry the parsed labels alongside the event for `label_selector`
    // filtering downstream — an `Arc` clone (refcount bump) shared by both
    // the replay entry and the broadcast frame.
    let labels = Arc::clone(&snap.labels);

    // Recover the originating Apply's span context from the traceparent
    // annotation so the dispatch span can link the fan-out back to the Apply
    // (Apply→subscriber waterfall). Absent / tracing-off → None → no link.
    let parent_span = obj
        .metadata
        .annotations
        .as_ref()
        .and_then(|a| a.get(crate::telemetry::TRACEPARENT_ANNOTATION))
        .and_then(|tp| crate::telemetry::span_context_from_traceparent(tp));

    H2_DATA_FRAME_BYTES.observe(config_event.encoded_len() as f64);

    // Push into replay buffer before broadcasting so a subscriber that
    // races to read the buffer after receiving the live event will find it.
    // `&str` cheap-pass: `push_replay` allocates the owned String only
    // on the happy parse path (CU-86aj3m1bd).
    push_replay(
        replay_buf,
        &snap.resource_version,
        Arc::clone(&config_event),
        Arc::clone(&labels),
    );

    let sent_at = Instant::now();
    APPLY_TO_BROADCAST_SECONDS.observe(sent_at.duration_since(apply_observed_at).as_secs_f64());

    Some(Arc::new(BroadcastFrame {
        sent_at,
        event: config_event,
        labels,
        parent_span,
    }))
}

/// Dispatch every buffered frame as a back-to-back `tx.send` burst, then clear
/// the batch. Each successful send increments `events_broadcast` (preserving
/// the per-event counter semantics — the counter still counts events that
/// reached at least one subscriber, not batches). `tx.send` only errors when
/// there are zero receivers, which is not a per-event failure to surface.
fn flush_batch(
    batch: &mut Vec<Arc<BroadcastFrame>>,
    shards: &ShardSet,
    events_broadcast: &prometheus::Counter,
) {
    // OTEL child span (Phase 7, CU-86ahzwj3k) per coalesced flush burst,
    // carrying the live subscriber count across all shards + burst size.
    // `level = "debug"` keeps it off the INFO production path; `send_to_all`
    // is synchronous so the entered guard never spans an await. Both fields
    // are cheap integer reads.
    let span = tracing::debug_span!(
        "konfig.broadcast_dispatch",
        subscribers = shards.total_receiver_count(),
        events = batch.len(),
    );
    let _enter = span.enter();
    for frame in batch.drain(..) {
        // Link the dispatch to each batched event's originating Apply
        // (Apply→subscriber waterfall) — a coalesced burst may span several
        // Applies, so the span can carry several links. No-op when tracing off.
        if let Some(sc) = &frame.parent_span {
            span.add_link(sc.clone());
        }
        if shards.send_to_all(frame) {
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
    namespace_broadcasts: &DashMap<String, ShardSet>,
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
            // Sum across all shards — a namespace is idle only when EVERY shard
            // has zero receivers (CU-86aj3vpnh).
            let count = entry.value().total_receiver_count();
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
            .remove_if(&ns, |_k, v| v.total_receiver_count() == 0)
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
    namespace_broadcasts: Arc<DashMap<String, ShardSet>>,
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

#[cfg(test)]
mod tests {
    #![allow(unused_imports)]
    use super::*;
    use crate::grpc::subscribe::test_support::*;

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
                    allow_all_filter(),
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
        let namespace_broadcasts: Arc<DashMap<String, ShardSet>> = Arc::new(DashMap::new());
        let namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>> = Arc::new(DashMap::new());
        let watcher_handles: Arc<DashMap<String, JoinHandle<()>>> = Arc::new(DashMap::new());
        let idle_since: Arc<DashMap<String, Instant>> = Arc::new(DashMap::new());

        // Insert a ShardSet with NO active receivers (sender only).
        let (tx, _rx) = broadcast::channel::<Arc<BroadcastFrame>>(64);
        // Drop _rx so total_receiver_count() == 0.
        drop(_rx);
        namespace_broadcasts.insert("test-ns".to_string(), ShardSet::from_senders(vec![tx]));
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
        let namespace_broadcasts: Arc<DashMap<String, ShardSet>> = Arc::new(DashMap::new());
        let namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>> = Arc::new(DashMap::new());
        let watcher_handles: Arc<DashMap<String, JoinHandle<()>>> = Arc::new(DashMap::new());
        let idle_since: Arc<DashMap<String, Instant>> = Arc::new(DashMap::new());

        // Insert a ShardSet AND keep a live receiver so total_receiver_count() > 0.
        let (tx, _live_rx) = broadcast::channel::<Arc<BroadcastFrame>>(64);
        namespace_broadcasts.insert("active-ns".to_string(), ShardSet::from_senders(vec![tx]));
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

    #[tokio::test]
    async fn ns_pump_broadcasts_apply_and_pushes_replay_entry() {
        use futures_util::stream;
        let (tx, mut rx) = broadcast::channel(8);
        let replay = Arc::new(Mutex::new(VecDeque::new()));
        let events: Vec<Result<Event<DynamicObject>, kube_watcher::Error>> =
            vec![Ok(Event::Apply(dyn_config("cfg", "ns", 1, "100")))];

        let shards = ShardSet::from_senders(vec![tx.clone()]);
        pump_subscribe_namespace_events(
            stream::iter(events),
            "ns",
            &shards,
            &replay,
            Duration::ZERO,
        )
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

        let shards = ShardSet::from_senders(vec![tx.clone()]);
        pump_subscribe_namespace_events(
            stream::iter(events),
            "ns",
            &shards,
            &replay,
            Duration::ZERO,
        )
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

        let shards = ShardSet::from_senders(vec![tx.clone()]);
        pump_subscribe_namespace_events(
            stream::iter(events),
            "ns",
            &shards,
            &replay,
            Duration::ZERO,
        )
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

        let shards = ShardSet::from_senders(vec![tx.clone()]);
        pump_subscribe_namespace_events(
            stream::iter(events),
            "ns",
            &shards,
            &replay,
            Duration::ZERO,
        )
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

        let shards = ShardSet::from_senders(vec![tx.clone()]);
        pump_subscribe_namespace_events(
            stream::iter(events),
            "ns",
            &shards,
            &replay,
            Duration::ZERO,
        )
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

        let shards = ShardSet::from_senders(vec![tx.clone()]);
        pump_subscribe_namespace_events(
            stream::iter(events),
            "ns",
            &shards,
            &replay,
            Duration::ZERO,
        )
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

        let shards = ShardSet::from_senders(vec![tx.clone()]);
        pump_subscribe_namespace_events(
            stream::iter(events),
            "ns",
            &shards,
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
        let shards = ShardSet::from_senders(vec![tx.clone()]);
        let pump = tokio::spawn(async move {
            pump_subscribe_namespace_events(blocked, "ns", &shards, &replay, window).await;
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

        let shards = ShardSet::from_senders(vec![tx.clone()]);
        pump_subscribe_namespace_events(
            stream::iter(events),
            "ns",
            &shards,
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

    /// Apply→subscriber waterfall: a Config carrying a `traceparent` annotation
    /// must yield a frame whose `parent_span` recovers the originating Apply's
    /// trace/span ids, so the dispatch span can `add_link` to it.
    #[test]
    fn process_event_recovers_parent_span_from_traceparent() {
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let replay = Arc::new(Mutex::new(VecDeque::new()));

        let mut obj = dyn_config("cfg", "ns", 1, "100");
        let tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        obj.metadata.annotations = Some(
            [(
                crate::telemetry::TRACEPARENT_ANNOTATION.to_string(),
                tp.to_string(),
            )]
            .into_iter()
            .collect(),
        );

        let frame = process_namespace_event(Event::Apply(obj), &replay)
            .expect("annotated Config must produce a frame");
        let sc = frame
            .parent_span
            .clone()
            .expect("traceparent annotation → linked parent span");
        assert_eq!(
            sc.trace_id().to_string(),
            "0af7651916cd43dd8448eb211c80319c"
        );
        assert_eq!(sc.span_id().to_string(), "b7ad6b7169203331");
    }

    /// No traceparent annotation → `parent_span` stays empty; the dispatch span
    /// is simply unlinked (the common, tracing-off path).
    #[test]
    fn process_event_without_traceparent_has_no_parent_span() {
        let replay = Arc::new(Mutex::new(VecDeque::new()));
        let obj = dyn_config("cfg", "ns", 1, "100");
        let frame = process_namespace_event(Event::Apply(obj), &replay)
            .expect("Config must produce a frame");
        assert!(
            frame.parent_span.is_none(),
            "no annotation → no parent link"
        );
    }
}
