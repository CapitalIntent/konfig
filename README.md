# konfig

Generic Kubernetes config distribution service.

Operators apply `Config.konfig.io/v1` CRDs. Consumer pods receive live updates
within ~2–15 ms via gRPC server-streaming or an embedded kube-rs watcher — no
pod restarts required.

---

## Architecture

```
Operator (kubectl / Backstage / konfig-cli)
    │
    │  Apply RPC  (schema_version monotonicity enforced)
    ▼
Config.konfig.io/v1 CRD  ─── etcd (linearizable writes)
    │
    │  kube-rs watch stream
    ▼
konfig pod
    ├─ ConfigCache  (DashMap, multi-key, lock-free reads)
    ├─ gRPC Subscribe  ──► broadcast::channel (O(1) fan-out)
    │                         └─► 100s of subscribers  p50 < 2 ms
    └─ /metrics  (Prometheus)

Consumer pod (Phase 5 embedded mode — no gRPC hop)
    └─ kube-rs watcher  ──► ArcSwap<ConfigSnapshot>  ──► RiskPipeline
```

### Key design decisions

| ADR | Decision | Rationale |
|-----|----------|-----------|
| ADR-001 | `Config.konfig.io/v1` CRD, not raw ConfigMap | OpenAPI v3 validation; `schema_version` monotonicity; non-konfig consumers (humans, ArgoCD, kubectl) can apply CRDs directly |
| ADR-004 | `spec.content` is opaque JSON | Domain-specific fields prevent adoption; consumers define and validate their own schema |
| ADR-005 | Secrets use `Role` + `RoleBinding` per namespace | K8s Secrets encrypted at rest; namespace-scoped RBAC enforces least privilege. **Never `ClusterRoleBinding` for Secrets.** |
| ADR-006 | ConfigMaps use `ClusterRole` + `ClusterRoleBinding` | Non-sensitive; zero-friction migration path for teams with existing ConfigMap config |
| ADR-008 | CP design: writes linearizable, reads consistent or cached | Partition → writes fail `UNAVAILABLE`; cache returns last-known-good + `stale_since_ms` |

### Consistency model

Konfig is a **CP system**:

- Writes are linearizable — the Apply RPC rejects `schema_version ≤ current`.
- Reads are consistent (gRPC Get) or eventually consistent (Subscribe stream).
- On partition: Apply returns `UNAVAILABLE`. Subscribe/Get return the last-known-good snapshot with `stale_since_ms` set.
- Watch stream reconnects with exponential backoff (1 s → 2 s → 4 s → … → 30 s cap) using a saved `resourceVersion` cursor — zero duplicates, zero missed events on reconnect.

---

## Prerequisites

- Kubernetes ≥ 1.29
- Helm 3.12+
- `kubectl` with cluster access
- The konfig image accessible from cluster nodes (`kasa288/konfig` on Docker Hub, or push to your registry)

---

## Quick start

```bash
# Install into the konfig-system namespace, watching a Config CRD
# named "trading" in the "default" namespace.
helm install konfig ./chart \
  --namespace konfig-system \
  --create-namespace \
  --set konfig.watchNamespace=default \
  --set konfig.watchName=trading
```

Create the seed Config CRD:

```bash
kubectl apply -f - <<EOF
apiVersion: konfig.io/v1
kind: Config
metadata:
  name: trading
  namespace: default
spec:
  schema_version: 1
  content:
    risk:
      max_order_size_usd: 1000
      max_position_usd: 10000
      max_daily_loss_usd: 500
EOF
```

Verify:

```bash
# Check konfig is healthy (cache populated → SERVING)
kubectl -n konfig-system get pods

# Read via CLI
kubectl exec -n konfig-system deploy/konfig -- \
  grpc_health_probe -addr=:50051

# Apply a config update
konfig-cli apply --namespace default --name trading \
  --yaml 'schema_version: 2\ncontent:\n  risk:\n    max_order_size_usd: 2000'
```

---

## Configuration reference

### Core settings

| Parameter | Default | Description |
|-----------|---------|-------------|
| `replicaCount` | `1` | Replica count. Use 2+ for HA (requires `pdb.enabled=true`). |
| `image.repository` | `kasa288/konfig` | Image repository. |
| `image.tag` | `""` (uses `appVersion`) | Image tag. |
| `konfig.watchNamespace` | `default` | Namespace containing the watched Config CRD. |
| `konfig.watchName` | `trading` | Name of the Config CRD to watch. |
| `konfig.watchConfigMaps` | `false` | Enable ConfigMap watching (`konfig.io/managed=true` label required). |
| `konfig.secretNamespaces` | `""` | Comma-separated list of namespaces to watch for Secrets. |

### RBAC

| Parameter | Default | Description |
|-----------|---------|-------------|
| `rbac.createConfigRole` | `true` | ClusterRole for `configs.konfig.io` access. |
| `rbac.createConfigMapRole` | `true` | ClusterRole for ConfigMap access. |
| `rbac.createSecretRole` | `true` | Role per namespace for Secret access (only when `konfig.secretNamespaces` is set). |

### High availability

| Parameter | Default | Description |
|-----------|---------|-------------|
| `pdb.enabled` | `true` | Create a PodDisruptionBudget. |
| `pdb.maxUnavailable` | `0` | Zero downtime during node drains. Set to `1` if `replicaCount=1`. |

### Resources

```yaml
resources:
  requests:
    cpu: 50m
    memory: 64Mi
  limits:
    cpu: 200m
    memory: 256Mi
```

Tune based on subscriber count. At 100 concurrent subscribers and 10 Apply/min,
measured usage on a production cluster is ~30m CPU / 40Mi memory.

### Telemetry

```yaml
telemetry:
  dial9:
    enabled: true               # set DIAL9_ENABLED=true
    traceDir: /tmp/dial9-traces # mount a PVC here for persistence
    maxDiskUsageMb: 512
    rotationSecs: 60

  tokioConsole:
    enabled: true  # development only; exposes port 4242
    port: 4242
```

Run `dial9 serve --local-dir /tmp/dial9-traces` (port-forwarded) to view
nanosecond-level async traces. Connect `tokio-console` to `:4242` for live
task inspection.

---

## RBAC model

```
Config CRD   configs.konfig.io   ClusterRole  +  ClusterRoleBinding   ✓ cross-namespace safe
ConfigMap    configmaps           ClusterRole  +  ClusterRoleBinding   ✓ non-sensitive
Secret       secrets              Role         +  RoleBinding (per ns) ✗ NEVER ClusterRoleBinding
```

Secret RBAC is intentionally namespace-scoped. If you extend `konfig.secretNamespaces`
the chart creates one `Role` + `RoleBinding` per namespace — check the rendered output with
`helm template` before installing.

---

## Consumer integration

### Option A — gRPC Subscribe (multi-subscriber fan-out)

Connect any gRPC client to `:50051` and call `Subscribe`:

```rust
let mut stream = client.subscribe(SubscribeRequest {
    namespace: "default".into(),
    names: vec!["trading".into()],
    resume_resource_version: String::new(),
}).await?.into_inner();

while let Some(event) = stream.next().await {
    let config = event?.config.unwrap();
    // config.content_json contains the live payload
}
```

The server uses a `broadcast::channel` per namespace — all subscribers receive
each event in O(1) regardless of subscriber count. Slow subscribers are
disconnected with `RESOURCE_EXHAUSTED` before the ring buffer wraps.

### Option B — Embedded kube-rs watcher (zero-hop, lowest latency)

For latency-sensitive consumers (e.g., trading risk pipeline), watch the CRD
directly from the consumer pod — no gRPC round-trip:

```rust
// In the consumer pod's main.rs
let kube_client = kube::Client::try_default().await?;
let konfig_cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));

tokio::spawn(async move {
    konfig::watcher::Watcher::new(kube_client)
        .run(Arc::clone(&konfig_cache), "default".into(), "trading".into())
        .await
        .ok();
});

// In the hot path — wait-free ArcSwap read
let config = konfig_cache.load();
let risk = serde_json::from_value::<RiskConfig>(config.content.clone())?;
```

The consumer pod's ServiceAccount needs `get/list/watch` on `configs.konfig.io`:

```yaml
# In consumer's infra/ manifests:
rules:
  - apiGroups: ["konfig.io"]
    resources: ["configs"]
    verbs: ["get", "list", "watch"]
```

### Config update semantics (ADR-D6)

**Strategy params** (signal thresholds, TWAP parameters): apply immediately to
`live_config` via `ArcSwap::store`.

**Risk limits** (`max_position_usd`, `max_daily_loss_usd`, `max_order_size_usd`):
buffered in `pending_config` until all positions are flat (session boundary).
Emergency override via `KonfigBridge::force_apply_config()`.

This prevents the stuck-pod scenario: if `max_position_usd` drops below an
open position, the new limit is held pending rather than applied immediately
(which would freeze new orders without providing an exit path).

---

## Operating runbook

### Health check

```bash
# gRPC health (SERVING = cache populated)
kubectl -n konfig-system exec deploy/konfig -- \
  grpc_health_probe -addr=:50051

# Prometheus metrics
kubectl -n konfig-system port-forward svc/konfig 9090:9090
curl -s localhost:9090/metrics | grep konfig_
```

Key metrics:

| Metric | Description | Alert threshold |
|--------|-------------|-----------------|
| `konfig_stale_seconds` | Seconds since watcher last received an event | > 300 s (5 min) |
| `konfig_pending_risk_update_age_seconds` | Seconds a pending risk-limit change has been waiting for positions to flatten | advisory only |
| `konfig_pending_risk_update_total` | Total risk-limit changes queued at session boundary | — |

### Upgrading

The CRD lives in `chart/crds/` — Helm 3 applies it automatically on upgrade.
If the CRD schema changes between versions, run `helm upgrade` and verify
existing `Config` objects still pass validation:

```bash
kubectl get configs.konfig.io --all-namespaces
```

### Partition recovery

If the kube API server is unreachable:
- Apply RPCs return `UNAVAILABLE` immediately.
- Subscribe streams continue delivering the last-known-good cache with `stale_since_ms` set.
- The watcher reconnects automatically with backoff (1 s → 30 s).

No operator intervention is required unless the partition lasts longer than
`DIAL9_ROTATION_SECS` × `DIAL9_MAX_DISK_USAGE_MB` (trace rotation budget).

### Deleting

```bash
helm uninstall konfig -n konfig-system

# CRDs are NOT deleted by Helm (to protect existing Config data).
# Delete manually when you are sure no consumer depends on them:
kubectl delete crd configs.konfig.io
```

---

## Development

### Load testing

```bash
# Reset seed Config to schema_version=0
kubectl apply -f - <<EOF
apiVersion: konfig.io/v1
kind: Config
metadata:
  name: konfig-loadtest
  namespace: konfig-system
spec:
  schema_version: 0
  content: {}
EOF

# Run loadtest (100 subscribers, 100 applies at 10/min)
kubectl apply -f loadtest/job.yaml
kubectl -n konfig-system logs -f job/konfig-loadtest
```

Measured results (Docker Desktop, 1 replica):

| Metric | Value |
|--------|-------|
| p50 delivery | 1 ms |
| p99 delivery | 6 ms |
| max delivery | 9 ms |
| Missed events | 0 / 10 000 |

### Dial9 trace analysis

```bash
kubectl -n konfig-system port-forward svc/konfig 9191:9191

# View traces in browser
dial9 serve --local-dir /tmp/dial9-traces --port 9191
```

---

## License

See the main repository for licensing terms.
