# konfig SLOs / SLIs

Phase 7 observability (CU-86ahrwd61). Service-level objectives for the konfig
config-management service, grounded in the Prometheus metrics actually exported
by `rust/konfig/metrics.rs` and scraped from the `/metrics` endpoint (port 9090).

Every SLI below maps to a **real** metric family. Where a metric the SLO would
ideally use does not yet exist, the row is marked **pending metric** rather than
referencing a fabricated series — do not alert on a metric that does not exist.

## SLO summary

| SLI | Objective | Window | Error budget | Source metric(s) |
|-----|-----------|--------|--------------|------------------|
| Apply availability | 99.9% of Apply RPCs non-error | 30d | 0.1% (~43.2 min/30d) | `konfig_apply_total{result}` |
| Subscribe e2e latency (p99) | p99 ≤ 50 ms | 30d (5m eval) | n/a (latency objective) | `konfig_subscribe_e2e_latency_seconds` |
| Apply latency (p99) | p99 ≤ 1 s | 30d (5m eval) | n/a (latency objective) | `konfig_apply_duration_seconds{result="ok"}` |
| Watcher freshness | `konfig_stale_seconds` ≤ 300 s per namespace | instantaneous | n/a (freshness objective) | `konfig_stale_seconds` |
| Cache hit ratio | ≥ 95% | 30d | pending metric | **pending metric: `konfig_cache_hits_total` / `konfig_cache_lookups_total`** |

Targets are deliberately defensible (not aspirational): Apply availability at
three nines matches the `PodDisruptionBudget` (`maxUnavailable: 0`) and the
multi-replica Deployment; the Subscribe p99 50 ms budget sits well above the
in-process fan-out floor (the histogram's first bucket is 100 µs) yet tight
enough to catch head-of-line blocking; Apply p99 1 s is the kube round-trip
budget (the `RPC_LATENCY_BUCKETS` top is 10 s, so 1 s leaves clear tail
headroom).

---

## SLI definitions

### 1. Apply availability — 99.9% / 30d

An Apply RPC counts as **good** when its result is `ok` or `rejected` (a
`rejected` Apply is a correct validation outcome, not a service failure) and
**bad** only when `result="error"` (an unhandled server error / kube failure).
`konfig_apply_total` is labelled `{namespace, result}` with result in
`{ok, rejected, error}` (see `APPLY_TOTAL` in `metrics.rs`).

Availability (fraction of non-error Applies over 30d):

```promql
1 - (
  sum(increase(konfig_apply_total{result="error"}[30d]))
  /
  sum(increase(konfig_apply_total[30d]))
)
```

Error-budget burn (fast 1h burn-rate, alert when budget is consumed too quickly):

```promql
sum(rate(konfig_apply_total{result="error"}[1h]))
/
sum(rate(konfig_apply_total[1h]))
> 0.001
```

Error budget: 0.1% of all Apply calls over 30d (~43.2 minutes of full outage
equivalent, or 1 bad call per 1000).

### 2. Subscribe end-to-end latency — p99 ≤ 50 ms

`konfig_subscribe_e2e_latency_seconds` is a `HistogramVec{namespace}` measuring
from `broadcast::send` in the namespace watcher to the moment each subscriber's
bridge enqueues the event onto its per-client mpsc channel (one observation per
subscriber per event). Buckets: 100 µs … 1 s (`SUBSCRIBE_LATENCY_BUCKETS`).

p99 over a 5m sliding window, fleet-wide:

```promql
histogram_quantile(
  0.99,
  sum by (le) (rate(konfig_subscribe_e2e_latency_seconds_bucket[5m]))
)
> 0.05
```

Note: this is the in-process fan-out latency, NOT the full client wire RTT. The
later pipeline stages (`konfig_broadcast_to_encode_seconds`,
`konfig_encode_to_send_seconds`) cover encode→socket and are tracked separately
on the trace dashboard.

### 3. Apply latency — p99 ≤ 1 s

`konfig_apply_duration_seconds` is a `HistogramVec{namespace, result}` of Apply
handler duration (`RPC_LATENCY_BUCKETS`, 1 ms … 10 s). The SLI scopes to
`result="ok"` so a slow path is not masked by fast-failing errors.

```promql
histogram_quantile(
  0.99,
  sum by (le) (rate(konfig_apply_duration_seconds_bucket{result="ok"}[5m]))
)
> 1
```

### 4. Watcher freshness — `konfig_stale_seconds` ≤ 300 s

`konfig_stale_seconds` is a `GaugeVec{namespace}`: seconds since the watcher
last received an event from the K8s API server (`0` = fresh / cold-start before
the first event). Sampled every 5 s by the background sampler in `grpc::serve`.
A value climbing past 300 s indicates a watcher disconnected from the API
server (see `docs/runbook.md`).

```promql
max by (namespace) (konfig_stale_seconds) > 300
```

This is an instantaneous freshness objective, not a ratio-over-window — the
budget is "no namespace stale > 5 min".

### 5. Cache hit ratio — pending metric

**There is no Prometheus cache hit/miss counter in `metrics.rs` today.** The
cache read path (`ConfigCache::get`, `cache.rs`) records a `hit` (bool) field on
the `konfig.cache_get` **OTEL span**, but emits no counter. A hit-ratio SLI
therefore cannot be expressed in PromQL until a counter pair is added, e.g.:

```text
pending metric: konfig_cache_hits_total / konfig_cache_lookups_total
```

Intended PromQL once those metrics exist (do NOT enable an alert against this
until the metric ships):

```promql
# sum(rate(konfig_cache_hits_total[5m])) / sum(rate(konfig_cache_lookups_total[5m]))
```

Until then, cache behaviour is observable only via the `hit` attribute on
`konfig.cache_get` spans on the Tempo trace dashboard
(`infra/profiling/grafana-dashboards/konfig-traces.json`).

---

## Alerting

The PromQL above is encoded as a `PrometheusRule` CRD at
`infra/konfig/prometheus-rules.yaml` (namespace `konfig-system`). Alert rules
fire on the budget-burn / threshold expressions; recording rules are omitted to
keep the manifest a plain, reviewable map of SLI → expression.

## Latency / trace investigation

When an Apply or Subscribe latency SLI breaches, drill into the span-level
breakdown via the Tempo trace dashboard — see the **Latency investigation**
section of `docs/runbook.md` and the dashboard provisioning README at
`infra/profiling/grafana-dashboards/README.md`.

## Caveats / deferred verification

- Targets are initial defensible values; tune against 30d of production
  histogram data once available.
- Live "panel returns data" and "alert fires against real series" verification
  is deferred to a live-stack session — no cluster is attached to this change.
- Cache-hit-ratio SLI is blocked on a new counter pair (see SLI 5).
