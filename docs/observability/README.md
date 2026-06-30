# Observability — OTEL trace evidence

Phase 7 (CU-86ahrntyq) wires OTEL tracing into konfig via `tracing-opentelemetry`
→ `opentelemetry-otlp` (see `rust/konfig/telemetry.rs`). This directory holds
**real captured traces** proving the instrumentation end-to-end, plus the
reproducible local setup. Evidence task: CU-86ahzwj5b.

## Captured traces (`traces/`)

All four were exported from Jaeger (`/api/traces/<id>`) after a full
`konfig-loadtest` run (S1 = 100 subscribers + 200 applies, S2 get-flood,
S3 reconnect-storm, S4 secrets-flood) against a single-replica konfig on kind,
with the OTLP exporter pushing to a Jaeger all-in-one.

| File | Trace | Spans | What it proves |
|------|-------|-------|----------------|
| `apply-write.json` | `konfig.Apply` → `konfig.apply_attempt` | 2 | Write path: RPC root (4.3 ms, `namespace`/`name`/`status_code=Ok`) with the K8s patch attempt as a child span (3.0 ms, `attempt=0`). |
| `watch-cache-update.json` | `konfig.watch_event` → `konfig.cache_update` | 2 | Distribution path: the watcher observing the applied change (`event_type=Apply`, `resource_version=1858`) and updating the cache. |
| `broadcast-fanout-100.json` | `konfig.broadcast_dispatch` | 1 | **Fan-out evidence** — one dispatch span carrying `subscribers=100`, i.e. a single coalesced event delivered to all 100 S1 subscribers. |
| `subscribe-stream.json` | `konfig.Subscribe` | 1 | Long-lived subscriber stream RPC root span. |

Exported operations confirmed present in Jaeger for service `konfig`:
`konfig.{Get,Apply,Subscribe,GetSecret,ApplySecret,SubscribeSecrets}` (INFO
RPC roots) plus `konfig.{apply_attempt,cache_get,cache_update,watch_event,broadcast_dispatch}`
(DEBUG child spans).

## Span model (read before interpreting the traces)

konfig's write and distribution paths are **decoupled by the Kubernetes watch
boundary**, so there is intentionally *no* single Apply→per-subscriber waterfall:

1. `Apply` patches the `Config` CRD in etcd and returns — trace #1
   (`konfig.Apply` → `konfig.apply_attempt`).
2. The watcher independently observes that change over its watch stream and
   updates the cache — trace #2 (`konfig.watch_event` → `konfig.cache_update`).
3. A coalesce-flush task fans the event out to all subscriber shards —
   `konfig.broadcast_dispatch` with `subscribers=N`.

The per-event fan-out is therefore represented by the **aggregate
`broadcast_dispatch(subscribers=N)`** span, not by N individual subscriber-receipt
spans. This is the "refuted" branch the parent ticket anticipated
("subscribe.rs broadcast path may need explicit span injection"). A single
connected trace would require context propagation across the etcd/watch boundary
(Apply→watch) and across the broadcast channel (broadcast→subscriber) — tracked
as a follow-up (see "Gaps" below).

## Child spans (DEBUG) export independently of the log level (CU-86aj9pvff)

The OTEL layer carries its **own** filter, decoupled from `RUST_LOG` (which now
gates only the log fmt layer). See `rust/konfig/main.rs` `init_tracing` +
`telemetry::otel_trace_filter_spec`:

- The fmt (log) layer keeps `konfig=info` default / `RUST_LOG` override.
- The OTEL layer filters at **`konfig=debug` by default** (overridable via
  `OTEL_TRACES_LEVEL`), so the rich child spans (`cache_*`, `watch_event`,
  `broadcast_dispatch`, `apply_attempt`) become OTEL spans **without** raising
  log verbosity. No more `RUST_LOG=konfig::cache=debug,…` hack, no debug log
  flood.
- When OTEL export is **off** (no `OTEL_EXPORTER_OTLP_ENDPOINT`) the OTEL layer
  is absent, so no layer is interested in those debug spans and they are *never
  created* — zero added cost. When export is on they are created at 100% and
  the configured `OTEL_TRACES_SAMPLER` decides how many traces are exported
  (so the per-`Get` `cache_get` span only materialises under active tracing).

> The four traces in `traces/` were captured *before* this change, using the
> old shared-filter `RUST_LOG` workaround documented below in the reproduce
> step's commit history; they remain valid evidence of the span model. To
> reproduce now, just enable export — the `RUST_LOG` line is no longer needed.

## Reproduce locally

Requires Docker + kind + kubectl + bazel (Apple Silicon shown; use `:load_amd64`
on linux/amd64).

```sh
# 1. Build + load arm64 images into kind
bazel run //docker/konfig:load_arm64
bazel run //docker/konfig-loadtest:load_arm64
kind create cluster --name konfig-loadtest --wait 120s
kind load docker-image kasa288/konfig:latest --name konfig-loadtest
kind load docker-image kasa288/konfig-loadtest:latest --name konfig-loadtest

# 2. CRD + RBAC + Service + watched-Config name + seed (see loadtest-integration.yml)
kubectl apply -f infra/konfig/crd.yaml
kubectl wait --for=condition=Established crd/configs.konfig.io --timeout=60s
kubectl apply -f infra/konfig/namespace.yaml -f infra/konfig/serviceaccount.yaml \
  -f infra/konfig/clusterrole.yaml -f infra/konfig/clusterrole-configmap.yaml \
  -f infra/konfig/clusterrolebinding.yaml -f infra/konfig/clusterrolebinding-configmap.yaml \
  -f infra/konfig/role-secret.yaml -f infra/konfig/service.yaml
kubectl create configmap konfig-config -n konfig-system \
  --from-literal=namespace=konfig-system --from-literal=name=konfig-loadtest \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f infra/konfig-loadtest/seed-config.yaml

# 3. Jaeger all-in-one (OTLP :4317 + query :16686)
kubectl apply -f infra/profiling/jaeger-dev.yaml
kubectl rollout status -n konfig-system deploy/jaeger --timeout=120s

# 4. konfig with OTEL export ON (TLS off for the harness). The OTEL layer now
#    captures konfig child spans at debug on its own (OTEL_TRACES_LEVEL default
#    konfig=debug) — no RUST_LOG hack needed. always_on so every trace exports.
yq '
  (.spec.template.spec.containers[]|select(.name=="konfig")).imagePullPolicy="IfNotPresent"
  | (.spec.template.spec.containers[]|select(.name=="konfig")).args |= ([.[]|select(test("^--tls")|not)]+["--tls=false"])
  | (.spec.template.spec.containers[]|select(.name=="konfig").env[]|select(.name=="OTEL_SDK_DISABLED")).value="false"
  | (.spec.template.spec.containers[]|select(.name=="konfig").env[]|select(.name=="OTEL_EXPORTER_OTLP_ENDPOINT")).value="http://jaeger.konfig-system:4317"
  | (.spec.template.spec.containers[]|select(.name=="konfig").env[]|select(.name=="OTEL_TRACES_SAMPLER")).value="always_on"
  | del(.spec.template.spec.containers[]|select(.name=="konfig").volumeMounts[]|select(.name=="konfig-tls"))
  | del(.spec.template.spec.volumes[]|select(.name=="konfig-tls"))
' infra/konfig/deployment.yaml | kubectl apply -f -
kubectl rollout status -n konfig-system deploy/konfig --timeout=180s

# 5. Run the loadtest, then export traces
sed 's/imagePullPolicy: Always/imagePullPolicy: IfNotPresent/' infra/konfig-loadtest/job.yaml | kubectl apply -f -
kubectl wait --for=condition=complete job/konfig-loadtest -n konfig-system --timeout=180s
kubectl port-forward -n konfig-system svc/jaeger 16686:16686 &
curl -s "localhost:16686/api/traces/<traceID>" | jq . > traces/<name>.json
```

## Gaps / follow-ups

- **Single connected Apply→subscriber waterfall** is not captured (decoupled by
  the watch boundary + coalesce-flush task). Fan-out is the aggregate
  `broadcast_dispatch(subscribers=N)` span. Connecting the paths needs explicit
  OTEL context propagation through the resource annotation and broadcast channel.
- **Per-subscriber receipt spans** are not emitted (the parent's "if refuted"
  note) — `send_to_all` is synchronous into channels and not individually spanned.
- ~~**OTEL export is dark at INFO**: child spans require per-module debug
  directives that also flood logs.~~ **RESOLVED (CU-86aj9pvff)** — the OTEL
  layer now has its own filter (`konfig=debug` default, `OTEL_TRACES_LEVEL`
  override), independent of `RUST_LOG`. See the section above.
- **Watcher reconnect/backoff trace** not captured here (requires disrupting
  API-server access; the loadtest S3 is *client* reconnect, not the konfig
  watcher reconnecting to the apiserver).
