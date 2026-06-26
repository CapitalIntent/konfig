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

# flamediff.py — top-frame CPU regression gate (CU-86ahtj1a8)

`flamediff.py` compares the top-N self-% frames of a freshly-captured CPU
profile against a checked-in baseline and fails when any frame got measurably
hotter (a perf regression). Pure stdlib; no deps.

## Profile JSON schema

Both `current.json` and the baseline are the pyroscope top-frames export shape:

```json
{ "frames": [ { "name": "konfig::grpc::subscribe::bridge_broadcast", "self_pct": 12.4 }, ... ] }
```

`self_pct` is the self (exclusive) CPU percentage. Order is irrelevant — the
script sorts and takes the top-N from each side.

## Usage

```sh
# Locally-runnable logic test (also a cheap CI step):
python3 tools/profiling/flamediff.py --self-test

# Gate a fresh capture against the baseline (exit 1 on regression):
python3 tools/profiling/flamediff.py current.json .profiling-baseline.json \
    --threshold=0.20 --top=5 --output=flamediff.md
```

Exit `0` = clean, `1` = a top-N frame's self-% rose > `--threshold` (relative)
or a new frame entered the top-N above `--new-frame-floor-pct`, `2` = bad input.
Sub-`--min-base-pct` frames are exempt from the relative gate (sampling noise).

## Remaining work to make this a live CI gate (needs a cluster)

The script is complete + self-tested. Wiring it into `loadtest-integration.yml`
as a blocking gate still needs a Linux/kind run (cannot be produced or verified
on macOS):

1. **Capture the baseline from `main`.** Run the loadtest against a
   `konfig-profiling` Deployment + pyroscope/alloy (see `infra/profiling/`),
   then capture the flamebearer (pyroscope render API, `format=json`) and reduce
   it locally with `flamebearer_to_pprof.py --top-frames 5` (see below), and
   commit the result as `.profiling-baseline.json`.
2. **Add a post-loadtest step** to `loadtest-integration.yml` that captures the
   flamebearer, reduces it with `flamebearer_to_pprof.py --top-frames 5` →
   `current.json`, then `python3 tools/profiling/flamediff.py current.json
   .profiling-baseline.json --output=$GITHUB_STEP_SUMMARY`. Gate the job on its
   exit code. (Also emit `cpu-profile.pprof` via the same converter for
   interactive drill-down on regressions.)
3. **Verify the gate fires:** open a throwaway PR with an intentional hot-path
   regression (e.g. a `sleep` in the apply path) and confirm CI fails; revert.

Until the baseline is captured the gate is intentionally NOT wired into the
required smoke gate — an unverified pyroscope query must not be allowed to break
every PR.

# flamebearer_to_pprof.py — pprof + summary export (CU-86aj7kawc)

The bench captures CPU profiles as Pyroscope **flamebearer JSON**
(`cpu-profile.flamebearer.json`), which `go tool pprof` cannot parse — so ad-hoc
CPU drill-down (`-top`, `-list`, `-http` flamegraph) was not possible.
`flamebearer_to_pprof.py` reconstructs the call tree from the flamebearer and
emits either a **pprof** profile or the **flamediff top-frames** summary. Pure
stdlib (no protobuf dependency); pyroscope frames are already symbolized so no
konfig binary is needed.

## Capture a flamebearer

Query Pyroscope's render API for the konfig CPU profile as flamebearer JSON
(during/after a loadtest with `infra/profiling/` deployed). `format=json` yields
flamebearer; adjust the query selector to your pyroscope app/profile type:

```sh
kubectl -n profiling port-forward svc/pyroscope 4040:4040 &
curl -fsS 'http://localhost:4040/pyroscope/render?query=process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name="konfig"}&from=now-10m&format=json' \
  -o cpu-profile.flamebearer.json
```

## pprof export (interactive drill-down)

```sh
python3 tools/profiling/flamebearer_to_pprof.py cpu-profile.flamebearer.json \
    --out cpu-profile.pprof
go tool pprof -top cpu-profile.pprof            # flat/cum self per frame
go tool pprof -list 'konfig::.*' cpu-profile.pprof
go tool pprof -http=:8080 cpu-profile.pprof     # interactive flamegraph
```

Sample values use `cpu/nanoseconds` when the flamebearer carries a `sampleRate`
(ticks x 1e9/rate); otherwise `samples/count`.

## Top-frames summary (feeds flamediff)

```sh
python3 tools/profiling/flamebearer_to_pprof.py cpu-profile.flamebearer.json \
    --top-frames 5 --out current.json
python3 tools/profiling/flamediff.py current.json .profiling-baseline.json
```

This automates the previously-manual "extract top-N self-% frames" step, so one
captured flamebearer drives both interactive pprof analysis and the flamediff
regression gate.

## Logic test

```sh
python3 tools/profiling/flamebearer_to_pprof.py --self-test
```
