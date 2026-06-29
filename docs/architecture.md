# Architecture

## How it works

```
Operator (kubectl / konfig-cli / API)
    │
    │  Apply RPC — schema_version monotonicity enforced
    ▼
Config.konfig.io/v1  ──  K8s etcd  (linearizable writes)
    │
    │  kube-rs watch stream
    ▼
konfig server
    ├─ ConfigCache   (DashMap, (namespace, name) → snapshot)
    ├─ SecretCache   (DashMap, per-namespace)
    ├─ ConfigMapCache (shared with ConfigCache, opt-in)
    │
    ├─ gRPC :50051   broadcast::channel per namespace (O(1) fan-out)
    │                 ├─► consumer pods — p50 < 2 ms
    │                 └─► grpc-Web (same port) — Backstage browser plugin
    └─ Prometheus /metrics :9090
```

Consumer pods connect to konfig's gRPC endpoint and call `Subscribe`. The
server starts a single kube watch stream per namespace and fans events out to
all subscribers over a `broadcast::channel` — delivery time is independent of
subscriber count.

Port 50051 serves **both** standard gRPC (HTTP/2) and **grpc-Web** (HTTP/1.1)
on the same listener: the tonic server is built with `accept_http1(true)` and a
`tonic_web::GrpcWebLayer` (CU-86ahzwhg4). This lets the Backstage browser plugin
call konfig directly — no Envoy/grpc-web proxy sidecar. Standard h2 gRPC clients
are unaffected; the layer only translates requests whose `content-type` is
`application/grpc-web*`.

## Config sources

konfig serves config from three sources, unified in a single gRPC API:

| Source | Opt-in mechanism | RBAC | Notes |
|--------|-----------------|------|-------|
| `Config.konfig.io/v1` CRD | Create the resource | ClusterRole | Primary path. Enforces `schema_version` monotonicity. |
| ConfigMap | Label `konfig.io/managed=true` | ClusterRole | Zero-friction migration for existing ConfigMaps. Off by default. |
| Secret | Label `konfig.io/managed=true` | Role per namespace (never ClusterRoleBinding) | Values base64-encoded on wire; consumers decode. Optional KMS envelope encryption proposed — see [kms-encryption.md](kms-encryption.md). |

## Consistency model

konfig is a **CP system**:

- **Writes** are linearizable — the Apply RPC rejects any `schema_version ≤ current stored`. Two concurrent writers cannot create an inconsistent state.
- **Reads** are either consistent (gRPC `Get`) or eventually consistent (Subscribe stream delivering events from the watch).
- **On partition**: Apply returns `UNAVAILABLE` immediately. Subscribe/Get return the last-known-good cache with `stale_since_ms ≥ 0` in the response.
- **Watcher reconnect**: exponential backoff (1 s → 2 s → 4 s → … → 30 s cap) using a saved `resourceVersion` cursor — zero duplicates, zero missed events on reconnect.

## Design decisions

| Decision | Why |
|---|---|
| Config CRD over raw ConfigMap | OpenAPI v3 validation at the API server; `schema_version` monotonicity; RBAC isolation (`konfig.io` API group) |
| Opaque `spec.content` | Domain-specific fields prevent adoption; consumers validate their own schema |
| Secrets via `Role` per namespace, never `ClusterRoleBinding` | Least-privilege; secrets are namespace-scoped by nature |
| ConfigMaps via `ClusterRole` | Non-sensitive; cross-namespace access acceptable |
| Single broadcast channel per namespace | O(1) fan-out regardless of subscriber count; 100 subscribers at p50 < 2 ms on Docker Desktop |
| Stateless Deployment (not StatefulSet) | Watch stream is rebuilt on restart from etcd; no persistent state |
| Raw YAML + Kustomize, not Helm | One topology, no drift between two parallel sources — see [ADR-0001](adr/0001-deployment-raw-yaml.md) |
| KMS envelope encryption for managed Secrets (proposed) | base64 is not encryption; wrap values with a per-secret DEK under a KMS master key, decrypt only on serve — see [ADR-0003](adr/0003-kms-envelope-encryption.md) |

## RBAC model

```
Config CRD   configs.konfig.io   ClusterRole + ClusterRoleBinding   safe cross-namespace
ConfigMap    configmaps           ClusterRole + ClusterRoleBinding   non-sensitive
Secret       secrets              Role + RoleBinding per namespace   NEVER ClusterRoleBinding
```
