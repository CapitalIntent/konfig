# infra/profiling

Pyroscope server + Grafana Alloy eBPF DaemonSet for profiling `app=konfig`
pods. Phase 5 / INFRA-1 of the loadtest-driven perf workstream
(ClickUp `CU-86ahtj1e7`).

## Layout

| File | Resource |
| --- | --- |
| `namespace.yaml` | `Namespace profiling` (labeled `konfig.traversal.com/scope=dev`) |
| `pyroscope-configmap.yaml` | `ConfigMap pyroscope-config` (filesystem storage) |
| `pyroscope-deployment.yaml` | `Deployment pyroscope` (single replica, emptyDir) |
| `pyroscope-service.yaml` | `Service pyroscope` (ClusterIP, :4040) |
| `alloy-rbac.yaml` | `ServiceAccount`, `ClusterRole`, `ClusterRoleBinding` |
| `alloy-config.yaml` | `ConfigMap alloy-config` (`pyroscope.ebpf` targeting `app=konfig`) |
| `alloy-daemonset.yaml` | `DaemonSet alloy` (privileged, `hostPID`) |

Image tags are pinned to multi-arch manifest-list SHA256 digests so ArgoCD
reconcile is deterministic.

## Dev-only by default

These manifests are **dev-only**. They are not intended to run in production:

- Storage is `emptyDir` — profiles vanish on pod restart.
- `alloy` is a privileged DaemonSet with `hostPID` — every node runs an eBPF
  agent. Cluster-wide impact, not opt-in per workload.
- No retention / compaction tuning.
- No auth on the Pyroscope HTTP endpoint.

The `Namespace` carries `konfig.traversal.com/scope: dev`. **Prod ArgoCD
applications must exclude resources matching this label** (e.g. via an
`argocd.argoproj.io/sync-options: SkipDryRunOnMissingResource=true` filter
or a label selector on the Application's `source.directory.exclude`). If a
prod cluster ever syncs `infra/profiling/`, that is a configuration bug in
the prod ArgoCD Application — the dev-only filter ships in this README.

## Local apply (docker-desktop)

```sh
kubectl --context docker-desktop apply -f infra/profiling/
```

To open the Pyroscope UI on `localhost:4040`, switch the Service to
`LoadBalancer` (docker-desktop binds `LoadBalancer` to `localhost`):

```sh
kubectl --context docker-desktop -n profiling patch svc pyroscope \
  -p '{"spec":{"type":"LoadBalancer"}}'
open http://localhost:4040
```

Or just port-forward:

```sh
kubectl --context docker-desktop -n profiling port-forward svc/pyroscope 4040:4040
```

## Cleanup

```sh
kubectl --context docker-desktop delete -f infra/profiling/
```

Per the project loadtest playbook, drop `alloy` after a profiling session
to free the privileged DaemonSet; Pyroscope itself can stay running for
historical comparison across runs.

## Bazel

`//infra/profiling:manifests` is a `filegroup` over the YAMLs, matching
`//infra/konfig:manifests`. It exists so downstream loadtest/e2e tooling
can depend on the manifest set via Bazel runfiles rather than relative
paths.
