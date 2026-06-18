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

## Observability — structured logging + OTEL tracing

Phase 7. Configured by the env block on the konfig container in
`infra/konfig/deployment.yaml` (override in a Kustomize overlay patch). All
defaults are **prod-safe**: tracing export is OFF and the sampler is pinned
to 1% so enabling it never floods a collector.

### Logging (CU-86ahrwd64)

| Env | Default | Description |
|-----|---------|-------------|
| `RUST_LOG_FORMAT` | `pretty` (deployment ships `json`) | `json` emits one machine-parseable object per log line for aggregation; any other value (`pretty`/unset) keeps the human-readable format for local dev. |
| `RUST_LOG` | `konfig=info` | Standard `tracing-subscriber` env-filter directive. |
| `KONFIG_LOG_SYNC` | unset | `1` swaps the non-blocking stdout writer for a synchronous one so lines survive a `SIGKILL` (debug/CI escape hatch). Prod leaves unset for the non-blocking hot-path win. |

Every gRPC RPC emits exactly one entry-level `info!` carrying `rpc`,
`namespace`, `name` (keyed RPCs), `client_addr`, and `request_id`. The
`request_id` echoes a caller-supplied `x-request-id` metadata header when
present, otherwise a lightweight process-local id is minted (no `uuid` dep).
The same `request_id` / `client_addr` are stamped on the trace span so logs
and traces correlate.

### OTEL tracing (CU-86aj08u7k)

| Env | Default (deployment) | Description |
|-----|----------------------|-------------|
| `OTEL_SDK_DISABLED` | `true` | Kill-switch. `true` skips the exporter entirely **even when an endpoint is set**. Flip to `false` to enable. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `""` | OTLP/gRPC collector endpoint (e.g. `http://otel-collector.observability:4317`). Empty/unset = export off. |
| `OTEL_TRACES_SAMPLER` | `parentbased_traceidratio` | Head sampler. Also: `always_on`/`always_off`, `traceidratio`, `parentbased_always_on`/`parentbased_always_off`. |
| `OTEL_TRACES_SAMPLER_ARG` | `0.01` | Ratio for the `*traceidratio` samplers — 1% prod-safe default. |
| `OTEL_SERVICE_NAME` | `konfig` | `service.name` resource attribute reported to the collector. |

**To enable distributed tracing**, an operator flips two values together
(both must change — with the SDK disabled the endpoint is ignored):

```yaml
# overlay/kustomization.yaml — Deployment patch
env:
  - name: OTEL_SDK_DISABLED
    value: "false"
  - name: OTEL_EXPORTER_OTLP_ENDPOINT
    value: "http://otel-collector.observability:4317"
```

Export uses the `tracing-opentelemetry` bridge over OTLP/gRPC (never
`opentelemetry-prometheus`). With no endpoint the OTEL layer is a no-op and
konfig logs exactly as before.

### tokio-console (dev only)

Build the `konfig-tokio-console` image variant (`--features tokio_console`)
and set `RUST_CONSOLE=1` to install the `console-subscriber` gRPC server on
port 6669 for `tokio-console` clients. Not a production image variant.

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
