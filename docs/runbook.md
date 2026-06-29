# Runbook

## Health check

```bash
# gRPC health (SERVING = cache populated and watcher connected)
grpc-health-probe -addr=konfig.konfig-system.svc.cluster.local:50051

# Prometheus metrics
kubectl -n konfig-system port-forward svc/konfig 9090:9090
curl -s localhost:9090/metrics | grep konfig_
```

### Key metrics

| Metric | Alert threshold | Description |
|--------|----------------|-------------|
| `konfig_stale_seconds{namespace}` | > 300 s | Seconds since the watcher last received an event from the K8s API server. `0` means fresh; cold start (before the first event) also reports `0`. Sampled every 5 s. |

## Pod not ready (UNAVAILABLE)

The readiness probe calls the gRPC health endpoint. The pod stays `NotReady`
until the seed Config CRD (`konfig.watchNamespace` / `konfig.watchName`) is
found and cached.

**Fix**: create the seed resource:

```bash
kubectl apply -f - <<EOF
apiVersion: konfig.io/v1
kind: Config
metadata:
  name: app-config       # matches konfig.watchName
  namespace: default     # matches konfig.watchNamespace
spec:
  schema_version: 0
  content: {}
EOF
```

## Partition recovery

On kube API server unreachability:
- Apply RPCs return `UNAVAILABLE` immediately.
- Subscribe streams continue with the last-known-good cache; `stale_since_ms ≥ 0` in responses.
- The watcher reconnects automatically with backoff (1 s → 30 s cap) using a saved `resourceVersion`.

No operator action required unless the partition exceeds your alerting threshold (`konfig_stale_seconds > 300`).

## Upgrading

```bash
kubectl apply -f infra/konfig/crd.yaml
kubectl wait --for=condition=Established crd/configs.konfig.io --timeout=30s
kubectl apply -k infra/konfig/
```

After upgrading, verify existing resources still pass validation:

```bash
kubectl get configs.konfig.io --all-namespaces
```

## Uninstalling

```bash
kubectl delete -k infra/konfig/

# The CRD is NOT deleted by the above (kustomize prune is not enabled).
# Delete it manually only when you are sure no consumers depend on it:
kubectl delete crd configs.konfig.io
```

## Coverage

```bash
bazel coverage --combined_report=lcov //rust/konfig:test
# lcov report at: bazel-out/_coverage/_coverage_report.dat
```

## Dial9 trace analysis

```bash
kubectl -n konfig-system port-forward svc/konfig 9191:9191
dial9 serve --local-dir /tmp/dial9-traces --port 9191
```

Enable tracing first (see [Configuration](configuration.md#telemetry)).

For captured reference traces (Apply write path, watch→cache update, broadcast
fan-out to 100 subscribers, Subscribe stream) and a local Jaeger reproduce-flow,
see [`docs/observability/`](observability/README.md). Note the gotcha documented
there: the rich child spans (`cache_*`, `watch_event`, `broadcast_dispatch`,
`apply_attempt`) are DEBUG-level and gated by the same `EnvFilter` as logs — at
the default `konfig=info` only the bare RPC root spans export. Raise specific
submodules (e.g. `RUST_LOG=konfig::grpc::subscribe=debug`) to surface them.

## Latency investigation

When an Apply or Subscribe latency SLI breaches (see [SLOs](slo.md)), use the
span-level breakdown instead of guessing from metrics alone.

1. **Confirm which SLI burned.** Check the alert label `slo` and the
   corresponding PromQL in [`docs/slo.md`](slo.md) / the firing
   `PrometheusRule` (`infra/konfig/prometheus-rules.yaml`):
   - `apply_latency` → `konfig_apply_duration_seconds` p99 > 1s
   - `subscribe_latency` → `konfig_subscribe_e2e_latency_seconds` p99 > 50ms
   - `apply_availability` → `konfig_apply_total{result="error"}` budget burn

2. **Open the Tempo trace dashboard** —
   `infra/profiling/grafana-dashboards/konfig-traces.json` (provisioning:
   `infra/profiling/grafana-dashboards/README.md`). Panels map directly to the
   span path:
   - *Apply RPC duration p50/p95/p99* — `konfig.Apply` root span.
   - *Subscribe fan-out latency* — `konfig.broadcast_dispatch` leaf under
     `konfig.Subscribe`.
   - *409 retry counts* — `konfig.apply_attempt` spans with `attempt > 0`
     (a spike = resourceVersion contention; cross-check Apply latency).
   - *Watcher reconnect timeline* — `konfig.watch_event` rate (a drop to zero
     corroborates a `konfig_stale_seconds` freshness alert).
   - *Top-N slowest Subscribe streams* — click a row to open the full trace and
     inspect `konfig.cache_get` (`hit` attribute) / `konfig.broadcast_dispatch`
     children.

3. **Enable tracing if no spans appear.** Spans only export when
   `OTEL_EXPORTER_OTLP_ENDPOINT` is set and `OTEL_SDK_DISABLED` is not `true`
   (see [Configuration](configuration.md#telemetry)). The child spans
   (`konfig.cache_get`, `konfig.watch_event`, `konfig.apply_attempt`,
   `konfig.broadcast_dispatch`) are `debug`-level — set the sampler / log level
   accordingly to capture them.

## TLS / cert rotation

mTLS is enforced by default. Certs are issued by cert-manager from
`Issuer/konfig-ca-issuer` in the `konfig-system` namespace, anchored at the
self-signed root in `Secret/konfig-ca-key-pair`. The server reads its
material at startup; cert-manager rewriting the underlying Secret does NOT
hot-reload — the pod must restart to pick up new certs.

> For the full **rollout plan** (turning mTLS on without breaking existing
> plaintext consumers — dual-listener migration) and **zero-downtime rotation**
> procedures, see [mtls-rollout.md](mtls-rollout.md). The quick-reference
> commands below stay here.

### Verify cert expiry

```bash
# Server cert
kubectl -n konfig-system get certificate konfig-server-tls \
  -o jsonpath='{.status.notAfter}'

# Or decode the live Secret
kubectl -n konfig-system get secret konfig-server-tls \
  -o jsonpath='{.data.tls\.crt}' | base64 -d | \
  openssl x509 -noout -enddate -subject -issuer
```

cert-manager renews 30d before expiry (`renewBefore: 720h` in
`infra/konfig/certificate.yaml`). Watch for `CertificateRequest` resources
in `konfig-system` if you want to see renewals in flight.

### Rotate the root CA

The CA cert lives 10y (`duration: 87600h` in `infra/konfig/issuer.yaml`)
and renews 1y before expiry. To force a rotation:

```bash
# 1. Re-issue the CA (cert-manager re-mints into the same Secret).
kubectl -n konfig-system delete certificate konfig-ca
kubectl apply -k infra/konfig/

# 2. Roll the konfig pod so it loads the new chain.
kubectl -n konfig-system rollout restart deployment/konfig

# 3. Every consumer must also roll once its client Certificate is re-issued
#    by the new CA (cert-manager handles re-issue; consumers handle the pod
#    restart).
```

In production, replace the bootstrap self-signed `Issuer` + CA `Certificate`
with a `secretName: konfig-ca-key-pair` you populate from your org PKI
(Vault, AWS PCA, step-ca). The leaf-issuing `Issuer/konfig-ca-issuer` stays
unchanged.

### Pod restart on cert renewal

The current deployment does NOT auto-restart on Secret rotation. Options:

- Annotate the Deployment with [`reloader.stakater.com/auto: "true"`](https://github.com/stakater/Reloader)
  if reloader is installed cluster-wide. (Not enabled by default.)
- Manually run `kubectl rollout restart deployment/konfig` after each
  renewal. cert-manager only renews 30d before expiry, so the cadence is
  predictable.

### cert-manager unreachable

If cert-manager is down or its CRDs are missing when konfig is reinstalled,
the `Certificate/konfig-server-tls` resource will not be reconciled and the
`Secret/konfig-server-tls` will not exist. The konfig pod will fail to start
with `failed to read server cert at /var/run/konfig-tls/tls.crt`.

Existing pods continue to run on whatever cert they loaded at startup — they
do NOT lose mTLS just because cert-manager is unhealthy. The risk window is
"cert expires while cert-manager is down". Monitor cert-manager liveness
separately.

### Disabling TLS for local dev

`--tls=false` skips all of the above and runs the gRPC server in plaintext.
The startup logs include `WARN: TLS disabled; gRPC server is unauthenticated`
once on boot. The production manifests in `infra/konfig/` never set this
flag.
