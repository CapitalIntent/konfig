# Cluster evaluation runbook

One sitting on a kind cluster that closes the three remaining
**cluster-only** follow-ups from `docs/codebase-audit-findings.md` and the
observability gaps in `docs/observability/README.md`:

1. **Broadcast-shards bench sweep** — validate the per-namespace shard win and,
   if it holds, flip the `--broadcast-shards` default `1 → 4`
   (`docs/sharded-broadcast.md` → *Deferred bench plan*).
2. **Watcher reconnect/backoff trace** — capture the `konfig.watch_connect`
   span (added in the reconnect-span PR) under a real watch disruption.
3. **Apply→subscriber connected-trace waterfall** — capture one Jaeger trace
   where `konfig.Apply` links through to `konfig.broadcast_dispatch` for the
   same `resourceVersion` (added in the waterfall PR).

> Why a runbook and not CI: all three need a live API server you can disrupt,
> per-pod profiling on a **single replica**, and tokio-console / Pyroscope
> attribution. They are not unit- or loadtest-gate-able. Run this manually and
> attach the artifacts to the relevant ticket.

## 0. Prerequisites

Bring up the cluster exactly as in `docs/observability/README.md` →
*Reproduce locally* (kind + CRD + RBAC + Service + seed + Jaeger all-in-one +
konfig with OTEL export ON). Scale konfig to **one replica** before any
profiling — per `CLAUDE.md` and `feedback_loadtest_replicas.md`, per-pod
profiles are only accurate at 1 replica:

```sh
kubectl scale -n konfig-system deploy/konfig --replicas=1
kubectl rollout status -n konfig-system deploy/konfig --timeout=180s
```

For the shard sweep (Session A) you also want CPU attribution — deploy
Alloy/Pyroscope from `infra/profiling/` (`alloy-rbac.yaml`,
`alloy-daemonset.yaml`, `alloy-config.yaml`, `pyroscope-*.yaml`) as in
`tools/profiling/README.md`.

## Session A — broadcast-shards bench sweep

Goal: confirm sharding cuts `noop-park` churn without regressing backpressure,
then flip the default if the win holds.

Sweep matrix (from `docs/sharded-broadcast.md`): `--broadcast-shards ∈ {1,2,4,8}`
× `--coalesce-window-ms ∈ {0,5}`. For each cell:

```sh
# Patch the konfig Deployment args for this cell (example: shards=4, window=5)
yq '
  (.spec.template.spec.containers[]|select(.name=="konfig")).args
    |= ([.[]|select(test("^--broadcast-shards|^--coalesce-window-ms")|not)]
        + ["--broadcast-shards=4","--coalesce-window-ms=5"])
' infra/konfig/deployment.yaml | kubectl apply -f -
kubectl rollout status -n konfig-system deploy/konfig --timeout=180s

# Drive load: Scenario 1 (subscribe flood) is the one that exercises fan-out.
sed 's/imagePullPolicy: Always/imagePullPolicy: IfNotPresent/' \
  infra/konfig-loadtest/job.yaml | kubectl apply -f -
kubectl wait --for=condition=complete job/konfig-loadtest -n konfig-system --timeout=300s
kubectl logs -n konfig-system job/konfig-loadtest > /tmp/shards-4-w5.json
kubectl delete job/konfig-loadtest -n konfig-system
```

Per cell, capture: the loadtest `--results-json` (p50/p95/p99, missed events,
errors), the Pyroscope CPU flamebearer for `service_name="konfig"`, and the
`konfig_broadcast_lag_total` / `konfig_subscribe_e2e_latency_seconds` metrics
(`curl localhost:9090/metrics` via port-forward).

**Acceptance (to flip default 1 → 4):**

- `noop-park` < 20 % of park events in the tokio-console / parking_lot
  attribution (CU-86aj37prc methodology) at `shards=4` vs the `shards=1`
  baseline.
- Backpressure **unchanged**: `konfig_broadcast_lag_total` and per-subscriber
  mpsc `CHANNEL_CAPACITY` drops do not regress vs baseline.
- p95/p99 subscribe latency does not regress.

If all three hold, flip the default in `rust/konfig/startup.rs`
(`broadcast_shards` `default_value = "1"` → `"4"`) and update the
`docs/sharded-broadcast.md` status line + the shard-count knob table.

## Session B — watcher reconnect/backoff trace

Goal: prove `konfig.watch_connect` spans appear on a real watch disruption and
that the backoff schedule is visible in the trace timeline.

The loadtest S3 (`reconnect_storm`) is a **client** reconnect, not the konfig
watcher reconnecting to the API server — so disrupt the API-server-facing watch
directly:

```sh
# Make konfig's watch stream break and re-establish. On kind, briefly cordon +
# restart the apiserver, or kill the konfig→apiserver connection. Simplest:
# delete the konfig pod's network path to the apiserver for a few seconds, or
# roll the kind control-plane:
docker restart konfig-loadtest-control-plane   # kind node name; ~10-20s outage

# While it recovers, konfig's run_with_reconnect loops with backoff_delay().
kubectl rollout status -n konfig-system deploy/konfig --timeout=180s
```

Then export the trace:

```sh
kubectl port-forward -n konfig-system svc/jaeger 16686:16686 &
# Find a trace with konfig.watch_connect spans where attempt > 0.
curl -s "localhost:16686/api/traces/<traceID>" | jq . \
  > docs/observability/traces/watch-reconnect.json
```

**Acceptance:** `docs/observability/traces/watch-reconnect.json` shows
`konfig.watch_connect` spans with `attempt` incrementing and `backoff_ms`
matching the `BACKOFF_STEPS_SECS` schedule (1s, 2s, 4s, …). Add a row to the
`docs/observability/README.md` captured-traces table and flip the
"Watcher reconnect/backoff trace not captured" gap to resolved.

## Session C — Apply→subscriber connected-trace waterfall

Goal: one Jaeger trace where `konfig.Apply` is linked to the downstream
`konfig.watch_event` / `konfig.broadcast_dispatch` for the **same**
`resourceVersion`, proving end-to-end context propagation across the etcd/watch
boundary and the broadcast channel.

```sh
# 1. Open a subscriber so broadcast_dispatch has a live receiver.
#    (any Subscribe client against the konfig Service; the loadtest S1 works)
# 2. Apply a Config and note the returned resourceVersion.
# 3. Export the Apply trace and follow the linked spans.
kubectl port-forward -n konfig-system svc/jaeger 16686:16686 &
curl -s "localhost:16686/api/traces/<applyTraceID>" | jq . \
  > docs/observability/traces/apply-to-subscriber-waterfall.json
```

**Acceptance:** the exported trace (or its linked traces, if propagation is via
span links rather than a single parent) shows `konfig.Apply` →
`konfig.watch_event`/`konfig.cache_update` → `konfig.broadcast_dispatch` sharing
one trace context for the same `resourceVersion`. Add a row to the
`docs/observability/README.md` table and flip the "Single connected
Apply→subscriber waterfall" gap to resolved.

## Cleanup

```sh
kind delete cluster --name konfig-loadtest
```

Then reclaim Bazel output_bases per the *Cleanup after a body of work* section
in `CLAUDE.md` if the image builds bloated the cache.
