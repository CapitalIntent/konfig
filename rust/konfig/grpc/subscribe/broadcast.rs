//! Broadcast fan-out plumbing for the Subscribe path.
//!
//! Owns the broadcast envelope ([`BroadcastFrame`]), the per-namespace sharded
//! fan-out ([`ShardSet`]), and the per-subscriber send/drop helpers
//! ([`try_send_or_disconnect`], [`bridge_broadcast`]). Split out of the
//! monolithic `subscribe.rs` (CU-86aj7k5rf).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{Notify, broadcast, mpsc};
use tonic::Status;
use tracing::{info, warn};

use crate::metrics::{
    ACTIVE_SUBSCRIBERS, BROADCAST_LAG, BROADCAST_TO_ENCODE_SECONDS, ENCODE_TO_SEND_SECONDS,
    SUBSCRIBE_E2E_LATENCY, SubGauge, WRITEV_CALLS_TOTAL,
};
use crate::proto::ConfigEvent;

use super::{SubscribeFilter, filter_allows_event};

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
    /// The source object's `metadata.labels`, carried alongside the event so
    /// the per-subscriber bridge can apply `label_selector` filtering without
    /// re-parsing the `DynamicObject` or bloating the wire-format
    /// `ConfigEvent`.  Shared `Arc` clone from `ConfigSnapshot::labels` — a
    /// refcount bump per frame, not a `BTreeMap` deep copy.
    pub labels: Arc<BTreeMap<String, String>>,
}

/// Broadcast ring-buffer capacity per namespace.
/// Sized so that even the slowest subscriber can drain before the ring wraps.
const BROADCAST_CAPACITY: usize = 1_024;

/// Lower / upper clamp for the per-namespace broadcast shard count
/// (CU-86aj3vpnh). `1` keeps the historical single-channel path byte-for-byte;
/// `16` caps the per-namespace `Vec<Sender>` + per-apply fan-out cost.
pub const MIN_BROADCAST_SHARDS: usize = 1;

pub const MAX_BROADCAST_SHARDS: usize = 16;

/// Sharded broadcast fan-out for one namespace (CU-86aj3vpnh).
///
/// Holds `N` independent `broadcast::Sender`s (`1..=16`). The watcher pump
/// fans **every** event to **all** `N` senders (after a single shared
/// `push_replay`), but each Subscribe RPC attaches its `Receiver` to exactly
/// **one** shard — picked round-robin via [`ShardSet::next_receiver`]. An event
/// therefore wakes only the ~1/N of subscribers parked on that shard instead of
/// every subscriber in the namespace.
///
/// `N == 1` is the historical single-channel path: the `Vec` has one element,
/// the round-robin index is always `0`, and `send_to_all` does exactly one
/// `tx.send` — byte-for-byte the pre-sharding behaviour.
///
/// The replay buffer is intentionally **NOT** sharded (it stays one
/// `ReplayBuffer` per namespace, owned by the watcher). A reconnecting
/// subscriber resumes from that single shared buffer regardless of which shard
/// it then lands on; per-shard replay would fragment resume semantics.
#[derive(Clone)]
pub struct ShardSet {
    senders: Arc<Vec<broadcast::Sender<Arc<BroadcastFrame>>>>,
    /// Round-robin cursor, advanced once per Subscribe attach. Wrapping is
    /// fine — assignment is `idx % N`, so overflow only re-bases the sequence.
    next: Arc<AtomicUsize>,
}

impl ShardSet {
    /// Build `shards` independent broadcast channels for a namespace, returning
    /// the [`ShardSet`] plus one `Receiver` per shard (the slow-path caller
    /// keeps the receiver for the shard it attaches to and drops the rest).
    ///
    /// `shards` is assumed pre-clamped to `1..=MAX_BROADCAST_SHARDS` by the
    /// caller (`serve` clamps the CLI value once at startup).
    pub(crate) fn new(shards: usize) -> (Self, Vec<broadcast::Receiver<Arc<BroadcastFrame>>>) {
        let shards = shards.max(MIN_BROADCAST_SHARDS);
        let mut senders = Vec::with_capacity(shards);
        let mut receivers = Vec::with_capacity(shards);
        for _ in 0..shards {
            let (tx, rx) = broadcast::channel(BROADCAST_CAPACITY);
            senders.push(tx);
            receivers.push(rx);
        }
        (
            Self {
                senders: Arc::new(senders),
                next: Arc::new(AtomicUsize::new(0)),
            },
            receivers,
        )
    }

    /// Round-robin a `Receiver` onto the next shard. Advances the per-namespace
    /// cursor with `Relaxed` ordering — assignment only needs even spread, not
    /// a happens-before edge, so the cheapest atomic suffices.
    pub(crate) fn next_receiver(&self) -> broadcast::Receiver<Arc<BroadcastFrame>> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.senders.len();
        self.senders[idx].subscribe()
    }

    /// Fan one frame out to **every** shard. Returns `true` if at least one
    /// shard had a live receiver (`broadcast::send` errors only on zero
    /// receivers), preserving the `EVENTS_BROADCAST` counter semantics (counts
    /// events that reached >= 1 subscriber, namespace-wide).
    pub(crate) fn send_to_all(&self, frame: Arc<BroadcastFrame>) -> bool {
        let mut delivered = false;
        for tx in self.senders.iter() {
            if tx.send(Arc::clone(&frame)).is_ok() {
                delivered = true;
            }
        }
        delivered
    }

    /// Total live receivers summed across all shards — for the dispatch span
    /// and GC idle check.
    pub(crate) fn total_receiver_count(&self) -> usize {
        self.senders.iter().map(|tx| tx.receiver_count()).sum()
    }
}

#[cfg(test)]
impl ShardSet {
    /// Number of shards (`>= 1`) — test-only assertions.
    pub(crate) fn len(&self) -> usize {
        self.senders.len()
    }

    /// Wrap a pre-built set of senders (test-only). Lets a test create the
    /// broadcast channels itself, keep the receivers it wants to read from, and
    /// still drive the production pump / GC code through a `ShardSet`.
    pub(crate) fn from_senders(senders: Vec<broadcast::Sender<Arc<BroadcastFrame>>>) -> Self {
        assert!(!senders.is_empty(), "ShardSet needs >= 1 sender");
        Self {
            senders: Arc::new(senders),
            next: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Sender for one shard (test-only) — lets a test push frames or assert on
    /// `receiver_count` of an individual shard.
    pub(crate) fn sender(&self, idx: usize) -> &broadcast::Sender<Arc<BroadcastFrame>> {
        &self.senders[idx]
    }
}

/// Single try_send + disconnect-on-Full helper shared by the resume snapshot,
/// post-snapshot race-window, and replay-hit paths.  Returns
/// `ControlFlow::Break(())` when the caller should stop sending and exit the
/// resume task.
///
/// Replaces a previous mix of `try_send` and blocking `.send().await` —
/// blocking sends starved the spawned resume task when a subscriber was
/// slow, instead of disconnecting them cleanly.
pub(crate) fn try_send_or_disconnect(
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
pub(crate) async fn bridge_broadcast(
    mut bcast_rx: broadcast::Receiver<Arc<BroadcastFrame>>,
    tx: mpsc::Sender<Result<ConfigEvent, Status>>,
    namespace: String,
    drain_notify: Arc<Notify>,
    filter: Arc<SubscribeFilter>,
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
                    // Apply the per-subscriber `names` + `label_selector`
                    // filter.  A filtered-out frame is NORMAL — `continue` to
                    // keep the stream alive (never `break`); only back-pressure
                    // / lag / drain close the stream.  Checked before the
                    // latency observations so a non-matching frame is not
                    // counted as a delivered event.
                    if !filter_allows_event(&filter, &frame.event, &frame.labels) {
                        continue;
                    }
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
    #![allow(unused_imports)]
    use super::*;
    use crate::grpc::subscribe::test_support::*;

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
            labels: Arc::new(BTreeMap::new()),
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
            allow_all_filter(),
        ));

        // Broadcast 5 frames with the send timestamp slightly in the past so
        // each `sent_at.elapsed()` is strictly positive.
        for i in 0..5 {
            let event = Arc::new(make_event(&format!("{i}"), i as u32 + 1));
            let frame = Arc::new(BroadcastFrame {
                sent_at: Instant::now() - Duration::from_millis(1),
                event,
                labels: Arc::new(BTreeMap::new()),
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
            allow_all_filter(),
        ));

        for i in 0..3 {
            let event = Arc::new(make_event(&format!("{i}"), i as u32 + 1));
            let frame = Arc::new(BroadcastFrame {
                sent_at: Instant::now() - Duration::from_millis(1),
                event,
                labels: Arc::new(BTreeMap::new()),
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
            bridge_broadcast(
                bcast_rx,
                tx,
                "default".into(),
                drain_clone,
                allow_all_filter(),
            )
            .await;
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

    // ── Sharded broadcast (CU-86aj3vpnh) ──────────────────────────────────────

    /// `ShardSet::new` clamps the floor to 1 and respects the requested count.
    /// `N == 1` is the historical single-channel path: one shard, every
    /// `next_receiver()` lands on shard 0, `send_to_all` is one send.
    #[tokio::test]
    async fn shardset_n1_is_single_channel_path() {
        let (shards, mut receivers) = ShardSet::new(1);
        assert_eq!(shards.len(), 1, "N=1 has exactly one shard");
        assert_eq!(receivers.len(), 1, "one seed receiver");
        let mut rx0 = receivers.swap_remove(0);

        // Round-robin always returns shard 0 when there is a single shard.
        let mut rx_extra = shards.next_receiver();
        // Both receivers subscribe to the SAME (only) shard, so both see the event.
        let delivered = shards.send_to_all(make_frame(make_event("1", 1)));
        assert!(
            delivered,
            "single shard with live receivers accepts the send"
        );
        assert!(
            rx0.try_recv().is_ok(),
            "seed receiver on shard 0 gets event"
        );
        assert!(
            rx_extra.try_recv().is_ok(),
            "round-robin receiver also on shard 0 gets event"
        );
        assert_eq!(
            shards.total_receiver_count(),
            2,
            "both receivers counted on the single shard"
        );
    }

    /// `send_to_all` fans every event to ALL N shards: a subscriber attached to
    /// each distinct shard receives the event (the fan-out invariant).
    #[tokio::test]
    async fn shardset_fans_event_to_every_shard() {
        const N: usize = 4;
        let (shards, receivers) = ShardSet::new(N);
        assert_eq!(shards.len(), N);
        // Keep one receiver per shard (the seed receivers — one per shard).
        let mut receivers = receivers;

        let delivered = shards.send_to_all(make_frame(make_event("7", 7)));
        assert!(delivered, "at least one shard had a live receiver");

        // Every shard's receiver must observe the single broadcast event.
        for (i, rx) in receivers.iter_mut().enumerate() {
            let frame = rx
                .try_recv()
                .unwrap_or_else(|e| panic!("shard {i} receiver must get the event: {e:?}"));
            assert_eq!(
                frame.event.config.as_ref().unwrap().resource_version,
                "7",
                "shard {i} received the right frame"
            );
        }
        assert_eq!(
            shards.total_receiver_count(),
            N,
            "one live receiver per shard"
        );
    }

    /// Round-robin assignment spreads subscribers evenly: with N shards and
    /// N*k `next_receiver()` calls, each shard ends up with exactly k receivers.
    #[tokio::test]
    async fn shardset_round_robin_spreads_subscribers() {
        const N: usize = 4;
        const PER_SHARD: usize = 5;
        let (shards, seed_receivers) = ShardSet::new(N);
        // Drop the seed receivers so each shard starts at zero, isolating the
        // round-robin assignment count.
        drop(seed_receivers);

        // Attach N * PER_SHARD receivers via round-robin and hold them all live.
        let mut held = Vec::new();
        for _ in 0..(N * PER_SHARD) {
            held.push(shards.next_receiver());
        }

        // Each shard must hold exactly PER_SHARD receivers — even spread.
        for shard_idx in 0..N {
            assert_eq!(
                shards.sender(shard_idx).receiver_count(),
                PER_SHARD,
                "shard {shard_idx} must hold exactly {PER_SHARD} round-robin receivers"
            );
        }
        assert_eq!(shards.total_receiver_count(), N * PER_SHARD);
        // Hold receivers alive until the assertions complete.
        drop(held);
    }

    /// `send_to_all` returns `false` when every shard has zero receivers
    /// (so the `EVENTS_BROADCAST` counter is not bumped for a no-op fan-out),
    /// but the event is still considered processed by the pump (replay push
    /// happens earlier, independent of fan-out).
    #[tokio::test]
    async fn shardset_send_to_all_false_when_no_receivers() {
        let (shards, receivers) = ShardSet::new(4);
        drop(receivers); // no live receivers on any shard
        let delivered = shards.send_to_all(make_frame(make_event("1", 1)));
        assert!(
            !delivered,
            "send_to_all must report false when no shard has a receiver"
        );
        assert_eq!(shards.total_receiver_count(), 0);
    }
}
