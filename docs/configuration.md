# Configuration reference

konfig is configured via two surfaces in `infra/konfig/`:

- **ConfigMap `konfig-config`** (`configmap.yaml`) — values consumed by the
  Deployment via `valueFrom.configMapKeyRef`. Edit + `kubectl apply`.
- **Deployment args** (`deployment.yaml` → `spec.template.spec.containers[0].args`) —
  feature flags (`--watch-configmaps`, `--secret-namespaces=...`).

Customize via a Kustomize overlay (`kustomize edit set ...` or a `patches:`
block); do not fork the manifests.

## ConfigMap keys (`infra/konfig/configmap.yaml`)

| Key | Default | Description |
|-----|---------|-------------|
| `namespace` | `default` | Namespace of the seed Config CRD the server watches at startup. Used only for the readiness gate — does not restrict which configs subscribers can request. |
| `name` | `app-config` | Name of the seed Config CRD. Create this resource before the Deployment becomes ready. |

## Deployment args

| Arg | Default | Description |
|-----|---------|-------------|
| `--grpc-addr` | `0.0.0.0:50051` | gRPC listener. |
| `--metrics-addr` | `0.0.0.0:9090` | Prometheus listener. |
| `--namespace` | from ConfigMap | Seed Config CRD namespace. |
| `--name` | from ConfigMap | Seed Config CRD name. |
| `--secret-namespaces` | `konfig-system` | Comma-separated namespaces to watch for labelled Secrets. |
| `--watch-configmaps` | absent (off) | Add to enable ConfigMap watching. Requires `konfig.io/managed=true` label on each ConfigMap. |

## RBAC

`infra/konfig/clusterrole.yaml` + `clusterrolebinding.yaml`:

- ClusterRole `konfig-config-access` — `get/list/watch/patch/create` on
  `configs.konfig.io` (all namespaces — non-sensitive, cluster-scoped).
- Bound to ServiceAccount `konfig` in `konfig-system`.

`infra/konfig/clusterrole-configmap.yaml` + `clusterrolebinding-configmap.yaml`:

- ClusterRole `konfig-configmap-access` — `get/list/watch` on `configmaps`.
- Apply only when `--watch-configmaps` is set.

`infra/konfig/role-secret.yaml`:

- Role `konfig-secret-access` per namespace — `get/list/watch/patch/create`
  on `secrets`. NEVER cluster-scoped (per ADR-005 in the source ticket).
- Add a `RoleBinding` per Secret-watched namespace in your overlay.

## High availability

`infra/konfig/deployment.yaml` ships `replicas: 1` for load-test
determinism. For HA, patch in your overlay:

```yaml
# overlay/kustomization.yaml
patches:
  - target:
      kind: Deployment
      name: konfig
    patch: |-
      - op: replace
        path: /spec/replicas
        value: 2
```

`infra/konfig/pdb.yaml` ships `maxUnavailable: 0`. Keep as-is for `replicas: 2+`.
For `replicas: 1`, patch `maxUnavailable: 1` to allow node drains.

## Resources

```yaml
resources:
  requests:
    cpu: 50m
    memory: 64Mi
  limits:
    cpu: 200m
    memory: 256Mi
```

At 100 concurrent subscribers and 10 Apply/min, measured usage is ~30m CPU / 40Mi.

Loadtest deployment scales the CPU limit to `1000m` (see commit `7cb3157`);
production overlays should keep the conservative defaults.

## Telemetry

Set via env block in your overlay's Deployment patch:

```yaml
env:
  - name: DIAL9_ENABLED
    value: "true"
  - name: DIAL9_TRACE_DIR
    value: /tmp/dial9-traces
  - name: DIAL9_MAX_DISK_USAGE_MB
    value: "512"
  - name: DIAL9_ROTATION_SECS
    value: "60"
  - name: TOKIO_CONSOLE_ENABLED
    value: "true"          # development only; exposes port 4242
```

Mount a PVC at `traceDir` for trace persistence across restarts.
Connect `tokio-console` to `:4242` for live task inspection.

## ArgoCD

There is no in-tree ArgoCD Application — `chart/templates/argocd-application.yaml`
was deleted alongside the chart. Operators supply their own Application
pointing at this directory:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: konfig
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/jayakasadev/konfig
    targetRevision: HEAD
    path: infra/konfig
  destination:
    server: https://kubernetes.default.svc
    namespace: konfig-system
  syncPolicy:
    syncOptions:
      - CreateNamespace=true
      - ServerSideApply=true
```
