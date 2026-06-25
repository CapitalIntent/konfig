//! Per-namespace replay buffer + resume logic for the Subscribe path.
//!
//! Owns the bounded FIFO replay buffer ([`ReplayBuffer`] / [`ReplayEntry`]),
//! the push path ([`push_replay`]), and the resume state machine
//! ([`resume_from_buffer`]). Split out of `subscribe.rs` (CU-86aj7k5rf).

use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, broadcast, mpsc};
use tonic::Status;
use tracing::{debug, info, warn};

use crate::cache::ConfigCache;
use crate::grpc::snapshot_to_proto;
use crate::proto::{ConfigEvent, config_event::EventType};

use super::{
    BroadcastFrame, SubscribeFilter, bridge_broadcast, filter_allows_event, try_send_or_disconnect,
};

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
    /// The source object's `metadata.labels`, mirroring `BroadcastFrame::
    /// labels`.  Kept so the resume/snapshot replay paths can apply
    /// `label_selector` filtering on buffered events without re-parsing.
    /// Shared `Arc` clone — refcount bump, not a deep copy.
    pub labels: Arc<BTreeMap<String, String>>,
}

/// Per-namespace replay buffer: a bounded FIFO ring of the last
/// `REPLAY_BUFFER_SIZE` events, keyed by their resource_version.
pub type ReplayBuffer = Arc<Mutex<VecDeque<ReplayEntry>>>;

/// Push `event` into `buf`, evicting the oldest entry when the buffer is full.
///
/// `resource_version` is parsed as `u64` at push time so resume can
/// binary-search the buffer.  An unparseable RV (kube only emits decimal
/// strings, so this signals upstream malformation) is logged and dropped.
///
/// `labels` is the source object's `metadata.labels` (an `Arc` clone — a
/// refcount bump, not a deep copy) so the resume/snapshot replay paths can
/// apply `label_selector` filtering on buffered events.
pub(crate) fn push_replay(
    buf: &ReplayBuffer,
    resource_version: &str,
    event: Arc<ConfigEvent>,
    labels: Arc<BTreeMap<String, String>>,
) {
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
        labels,
    });
}

/// Resume a subscriber from `resume_rv` using the in-memory replay buffer.
///
/// - Hit: drain the buffer starting after `resume_rv`, then switch to live broadcast.
/// - Miss: send full cache snapshot as MODIFIED events, then switch to live broadcast.
///
/// After draining the buffer (or snapshot), the subscriber joins the shared
/// broadcast channel for future events — no kube watch is opened.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn resume_from_buffer(
    resume_rv: String,
    replay_buf: ReplayBuffer,
    cache: Arc<ConfigCache>,
    namespace: String,
    bcast_rx: broadcast::Receiver<Arc<BroadcastFrame>>,
    tx: mpsc::Sender<Result<ConfigEvent, Status>>,
    drain_notify: Arc<Notify>,
    filter: Arc<SubscribeFilter>,
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
    // Each replay item carries its labels alongside the event so the
    // `label_selector` filter can be applied at the emit point below without
    // re-locking the buffer.  Both fields are `Arc` clones — refcount bumps.
    type ReplayItem = (Arc<ConfigEvent>, Arc<BTreeMap<String, String>>);
    let (replay_slice, found_in_buffer): (Vec<ReplayItem>, bool) = {
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
                let mut slice: Vec<ReplayItem> = Vec::with_capacity(cap);
                slice.extend(
                    guard
                        .iter()
                        .skip(idx + 1)
                        .map(|e| (Arc::clone(&e.event), Arc::clone(&e.labels))),
                );
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
        // snapshot list to compute it after the send phase.  Computed across
        // ALL snapshots (even filtered-out ones) so the race-window boundary
        // stays correct regardless of the per-subscriber filter.
        let mut max_snapshot_rv: u64 = 0;
        for snap in &snapshots {
            // K8s emits decimal-string RVs; non-numeric here means we already
            // logged earlier (cache loaded a malformed CR).  Skip safely.
            if let Ok(rv) = snap.resource_version.parse::<u64>()
                && rv > max_snapshot_rv
            {
                max_snapshot_rv = rv;
            }
            // Apply the `names` + `label_selector` filter — only configs that
            // pass BOTH constraints are emitted to this subscriber.
            if !filter.allow(&snap.name, &snap.labels) {
                continue;
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
        let post_snapshot_events: Vec<ReplayItem> = {
            let guard = crate::sync_util::lock_recovered(&replay_buf);
            guard
                .iter()
                .filter(|e| e.resource_version_u64 > max_snapshot_rv)
                .map(|e| (Arc::clone(&e.event), Arc::clone(&e.labels)))
                .collect()
        }; // mutex released here

        debug!(
            namespace = %namespace,
            post_snapshot_count = post_snapshot_events.len(),
            "Resume: sending post-snapshot buffer events to close race window"
        );

        for (event, labels) in post_snapshot_events {
            // Apply the `names` + `label_selector` filter; a filtered-out
            // event is normal — skip it (do not disconnect the stream).
            if !filter_allows_event(&filter, &event, &labels) {
                continue;
            }
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
        for (event, labels) in replay_slice {
            // Same per-event filter as the snapshot/post-snapshot paths.
            if !filter_allows_event(&filter, &event, &labels) {
                continue;
            }
            if try_send_or_disconnect(&tx, (*event).clone(), "replay").is_break() {
                return;
            }
        }
    }

    // Join the live broadcast for future events.
    bridge_broadcast(bcast_rx, tx, namespace, drain_notify, filter).await;
}

#[cfg(test)]
mod tests {
    #![allow(unused_imports)]
    use super::*;
    use crate::grpc::subscribe::test_support::*;

    // ── Unit: push_replay evicts oldest when full ────────────────────────────

    #[test]
    fn push_replay_evicts_oldest_when_full() {
        let buf: ReplayBuffer = Arc::new(Mutex::new(VecDeque::new()));
        for i in 0..REPLAY_BUFFER_SIZE {
            let rv = format!("{i}");
            push_replay(
                &buf,
                &rv,
                Arc::new(make_event(&rv, i as u32)),
                Arc::new(BTreeMap::new()),
            );
        }
        // Buffer is exactly full — oldest is rv-0.
        assert_eq!(buf.lock().unwrap().front().unwrap().resource_version, "0");

        // Push one more — rv-0 must be evicted.
        push_replay(
            &buf,
            "9999",
            Arc::new(make_event("9999", 9999)),
            Arc::new(BTreeMap::new()),
        );
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
            allow_all_filter(),
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
            allow_all_filter(),
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
            allow_all_filter(),
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
            allow_all_filter(),
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

    // ── Unit: push_replay rejects non-numeric resource_version ──────────────

    /// kube only emits decimal-string RVs. If a non-numeric RV ever reaches
    /// the replay buffer the upstream object is malformed; we want to drop
    /// it at push time so resume's binary search never sees an entry it
    /// cannot order. Regression test for the `resource_version.parse::<u64>`
    /// gate added in PR C.
    #[test]
    fn push_replay_drops_non_numeric_rv() {
        let buf: ReplayBuffer = Arc::new(Mutex::new(VecDeque::new()));
        push_replay(
            &buf,
            "not-a-number",
            Arc::new(make_event("0", 0)),
            Arc::new(BTreeMap::new()),
        );
        assert!(
            buf.lock().unwrap().is_empty(),
            "push_replay must drop entries with non-numeric resource_version",
        );
        // Valid RV still pushes.
        push_replay(
            &buf,
            "42",
            Arc::new(make_event("42", 42)),
            Arc::new(BTreeMap::new()),
        );
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
            allow_all_filter(),
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

    // ── End-to-end (resume path): label_selector filters the live snapshot ───

    /// A labeled `DynamicObject` parses into a `ConfigSnapshot` whose `labels`
    /// flow into the cache; `resume_from_buffer` with a `tier=critical` filter
    /// must emit ONLY the critical config's snapshot. Mirrors the ticket's
    /// integration acceptance using the resume/snapshot path (the live kind
    /// e2e gate is CI/linux-only).
    #[tokio::test]
    async fn resume_snapshot_path_applies_label_selector() {
        // Build a cache directly from labeled snapshots.
        let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
        let mut critical = ConfigSnapshot {
            name: "cfg-critical".into(),
            namespace: "default".into(),
            schema_version: 1,
            resource_version: "10".into(),
            ..Default::default()
        };
        critical.labels = Arc::new(labels(&[("tier", "critical")]));
        cache.update(critical);
        let mut normal = ConfigSnapshot {
            name: "cfg-normal".into(),
            namespace: "default".into(),
            schema_version: 2,
            resource_version: "11".into(),
            ..Default::default()
        };
        normal.labels = Arc::new(labels(&[("tier", "normal")]));
        cache.update(normal);

        let replay_buf: ReplayBuffer = Arc::new(Mutex::new(VecDeque::new()));
        let (bcast_tx, bcast_rx) = broadcast::channel::<Arc<BroadcastFrame>>(64);
        let (tx, mut rx) = mpsc::channel(64);
        drop(bcast_tx); // let bridge exit cleanly after the snapshot phase

        let filter = Arc::new(SubscribeFilter::new(&[], "tier=critical").unwrap());

        tokio::spawn(resume_from_buffer(
            String::new(), // empty resume_rv → buffer miss → full snapshot
            replay_buf,
            cache,
            "default".into(),
            bcast_rx,
            tx,
            Arc::new(Notify::new()),
            filter,
        ));

        let mut received_names: Vec<String> = Vec::new();
        while let Some(Ok(ev)) = rx.recv().await {
            received_names.push(ev.config.unwrap().name);
        }
        assert_eq!(
            received_names,
            vec!["cfg-critical".to_string()],
            "snapshot path must emit only the tier=critical config"
        );
    }
}
