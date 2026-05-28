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

The CRD is in `chart/crds/` — Helm 3 applies it automatically before templates on `helm upgrade`.

After upgrading, verify existing resources still pass validation:

```bash
kubectl get configs.konfig.io --all-namespaces
```

## Uninstalling

```bash
helm uninstall konfig -n konfig-system

# CRDs are NOT deleted by Helm (to protect data).
# Delete manually only when you are sure no consumers depend on them:
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
