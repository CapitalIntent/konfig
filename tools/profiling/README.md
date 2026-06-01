# tools/profiling

Cleanup target for konfig profiling sessions.

## What it does

A profiling session typically runs:

- `konfig-system/deploy/konfig` — the server under test
- `konfig-system/job/konfig-loadtest` — the in-cluster loadtest driver
- `profiling/daemonset/alloy` (+ ClusterRole/ClusterRoleBinding/ConfigMap/ServiceAccount) — the scraper that ships profiles to pyroscope
- `profiling/deploy/pyroscope` (+ Service/ConfigMap) — the pyroscope datastore

`bazel run //tools/profiling:teardown` deletes the first three groups (konfig,
loadtest, alloy) but **keeps pyroscope** so the captured profile data and the
`profiling` namespace survive across sessions.

Pass `--all` to also drop pyroscope and the namespace (full clean).

The script is idempotent — every `kubectl delete` uses `--ignore-not-found`, so
running it twice (or against an already-clean cluster) is a no-op.

## Prerequisites

- `kubectl` on `PATH`. Bazel `sh_binary` inherits the caller's `PATH`, so if
  `kubectl` works in your shell it'll work here. If it's missing, the script
  exits 127 with a clear message.
- A reachable kubeconfig context that points at your test cluster.

## Usage

Default — keep pyroscope:

```sh
bazel run //tools/profiling:teardown
```

Target a specific kubeconfig context:

```sh
bazel run //tools/profiling:teardown -- --context docker-desktop
bazel run //tools/profiling:teardown -- --context kind-konfig
```

Full clean (also drop pyroscope + namespace):

```sh
bazel run //tools/profiling:teardown -- --all --context docker-desktop
```

Show help:

```sh
bazel run //tools/profiling:teardown -- --help
```

## What gets deleted

Default mode:

| Resource | Namespace |
| --- | --- |
| `job/konfig-loadtest` | `konfig-system` |
| `deploy/konfig` | `konfig-system` |
| `daemonset/alloy` | `profiling` |
| `configmap/alloy-config` | `profiling` |
| `serviceaccount/alloy` | `profiling` |
| `clusterrole/alloy` | cluster-scoped |
| `clusterrolebinding/alloy` | cluster-scoped |

`--all` additionally deletes:

| Resource | Namespace |
| --- | --- |
| `deploy/pyroscope` | `profiling` |
| `svc/pyroscope` | `profiling` |
| `configmap/pyroscope-config` | `profiling` |
| `namespace/profiling` | cluster-scoped |
