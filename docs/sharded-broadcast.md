# Sharded broadcast per namespace (CU-86aj3vpnh)

Status: mechanism shipped **default-OFF** (`--broadcast-shards 1`). Bench to
validate the win and flip the default is **deferred** to a separate loadtest
session (see [Deferred bench plan](#deferred-bench-plan)).

Sequences after **coalesce-by-window** (CU-86aj3vpgr, merged) and composes with
it — it does not replace it. References the noop-park attribution work
(CU-86aj37prc).

## Problem

Each namespace has ONE `tokio::sync::broadcast::Sender<Arc<BroadcastFrame>>`.
Every Config apply the watcher fans out wakes **every** subscriber `Receiver`
parked on that namespace's channel — even though `tokio::broadcast` only
unparks receivers that are currently parked, a single send still costs one
wake per parked receiver. At high subscriber counts per namespace this is the
dominant source of `noop-park` churn observed in the CU-86aj37prc
tokio-console / parking_lot attribution: N subscribers ⇒ N wakes per event.

Coalesce-by-window (CU-86aj3vpgr) already cuts the *event-rate* axis (a burst
of K applies within the window drains as one wakeup per receiver). Sharding
cuts the orthogonal *subscriber-count* axis: an event wakes only ~1/N of a
namespace's subscribers.

## Mechanism

Per-namespace broadcast state becomes a `ShardSet` of **N** independent
`broadcast::Sender`s (`grpc/subscribe.rs`):

- The single kube watcher per namespace fans **every** event to **all N**
  shard senders (`ShardSet::send_to_all`).
- Each `Subscribe` RPC attaches its `Receiver` to **exactly one** shard,
  chosen round-robin (`ShardSet::next_receiver`).
- An event therefore wakes only the ~1/N of subscribers parked on the shard it
  lands on — wake amplification drops from N to N/shards.

`BroadcastFrame` (the channel item, `Arc`-wrapped) is **unchanged**. Fan-out to
all shards is N `Arc` ref-count bumps of the *same* inner allocation — the
once-per-apply serialisation invariant (Track E) is preserved. Not mutating the
frame is the same discipline that unstalled the coalesce ticket.

### Shard-count knob

| Flag | Env | Default | Clamp |
|------|-----|---------|-------|
| `--broadcast-shards` | `KONFIG_BROADCAST_SHARDS` | `1` | `1..=16` |

- **Default `1`** is byte-for-byte the historical single-channel path: the
  `ShardSet` holds one sender, `next_receiver` always returns shard 0, and
  `send_to_all` issues exactly one `tx.send` per event. The existing hot path
  is untouched when `shards == 1`.
- **Cap `16`** bounds the per-namespace `Vec<Sender>` footprint and the
  per-apply fan-out cost (N ref-count bumps + N `send` calls). Out-of-range CLI
  input is **clamped, not rejected** (a `warn!` is logged once at startup) so a
  typo degrades gracefully rather than crash-looping the pod.
- The clamp is applied once in `serve()`; `ShardSet::new` floor-clamps to 1 as
  a defensive backstop.
- **Default flips to `4` only after** a bench validates the win (deferred,
  out of scope here).

### Shard assignment: round-robin

Subscribe-time round-robin via a per-namespace `AtomicUsize` cursor living
inside the `ShardSet`: `idx = next.fetch_add(1, Relaxed) % N`.

- Simplest even spread; `Relaxed` ordering suffices because assignment only
  needs balance, not a happens-before edge.
- No consistent-hash: there is no key to be stable against (a subscriber is not
  pinned across reconnects — see shared replay buffer below), so the added
  complexity buys nothing here.
- The cursor is **not** reset across the namespace's lifetime; `usize` wrap is
  harmless (`% N` only re-bases the sequence).

### Shared replay buffer (NOT sharded)

The per-namespace replay buffer (`ReplayBuffer`, the `VecDeque` of the last
`REPLAY_BUFFER_SIZE` events used by the `resume_resource_version` reconnect
path) stays **one per namespace, shared across all shards**.

- The watcher pushes each event to the shared replay buffer **once**
  (`push_replay` in `process_namespace_event`), **then** fans the frame to all
  N shard senders.
- A reconnecting subscriber resumes from that single shared buffer regardless
  of which shard it then lands on.
- **Rationale**: per-shard replay would fragment resume semantics — a
  subscriber that resumes after landing on a different shard than last time
  would see a different (partial) event history. One shared buffer keeps
  resume correctness independent of shard assignment.

### Composition with coalesce

The watcher-side pump (`pump_subscribe_namespace_events` →
`pump_immediate` / `pump_coalesced`, `flush_batch`) now takes `&ShardSet`
instead of `&broadcast::Sender`. Each event (immediate path) or coalesced
batch (windowed path) is dispatched to **all N shards** via `send_to_all`.
The two mechanisms are orthogonal:

- coalesce collapses the **event-rate** axis (K applies in a window ⇒ 1 wake),
- sharding collapses the **subscriber-count** axis (N subs ⇒ N/shards wakes),

and they multiply. `EVENTS_BROADCAST{namespace}` still counts events that
reached ≥ 1 subscriber namespace-wide (`send_to_all` returns `true` if any
shard had a live receiver). GC of an idle namespace operates on the whole
namespace: a namespace is collected only when **every** shard has zero
receivers (`ShardSet::total_receiver_count() == 0`).

## Blast radius

Three files: `grpc/mod.rs` (DashMap value type + `ServerConfig` /
`KonfigServer` field + `serve` clamp), `grpc/subscribe.rs` (`ShardSet`, pump /
GC threading), `startup.rs` (CLI flag). The `DashMap<String, broadcast::Sender<…>>`
value type changed to `DashMap<String, ShardSet>`; everything downstream routes
through `ShardSet` methods so the call sites stayed mechanical.

## Tests (in-process, no cluster)

- `shardset_n1_is_single_channel_path` — N=1 is the unchanged single-channel
  path (one shard, round-robin always shard 0, one send).
- `shardset_fans_event_to_every_shard` — N=4, one send reaches a subscriber on
  **each** shard (fan-out-to-all-N invariant).
- `shardset_round_robin_spreads_subscribers` — N·k `next_receiver` calls leave
  exactly k receivers per shard (even spread).
- `shardset_send_to_all_false_when_no_receivers` — no-op fan-out does not bump
  the events counter.
- `args_parse_broadcast_shards_default_one` / `_explicit` — CLI default `1` +
  explicit parse.
- Existing GC + pump tests retargeted onto `ShardSet`.

## Deferred bench plan

NOT run here (no `cargo bench`, no kind cluster, no loadtest). Deferred to a
combined coalesce + shards loadtest session:

- Sweep `--broadcast-shards` ∈ {1, 2, 4, 8} × `--coalesce-window-ms` ∈ {0, 5}
  on a single-replica Deployment (per-pod profiling; scale to 1 replica first).
- Target: **noop-park < 20 %** of park events in the tokio-console /
  parking_lot attribution (CU-86aj37prc methodology).
- Acceptance: loadtest **backpressure unchanged** vs. baseline (per-subscriber
  mpsc `CHANNEL_CAPACITY` back-pressure and `BROADCAST_LAG` must not regress —
  sharding must not trade wake reduction for dropped/lagged events).
- If the win holds, flip the `--broadcast-shards` default from `1` to `4` in a
  follow-up.
