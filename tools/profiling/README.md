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

## CI gate: `.github/workflows/flamediff.yml`

The gate is wired as its own workflow (CU-86ahtj1a8), NOT bolted onto the
required `loadtest-integration.yml` — capturing a profile needs the
`konfig-profiling` image + an in-cluster pyroscope, and adding that to a
required, historically-flaky gate would risk breaking every PR. It reuses the
same capture stack as `profile-release.yml`. Two jobs:

- **`logic-gate`** — cheap + deterministic, runs on **every** PR. Just the two
  `--self-test`s below. This is the part that can break a PR; it needs no
  cluster, so the gate *logic* (threshold, new-frame floor, noise floor,
  malformed-input handling) is always enforced.
- **`capture-gate`** — heavy (kind + pyroscope + saturate loadtest). Captures a
  real CPU profile, reduces it to top-5 self-% frames, and diffs vs the
  checked-in `.profiling-baseline.json`. **Opt-in only** (push to `main`,
  manual `workflow_dispatch`, or a PR labeled `profiling`) so an unverified
  pyroscope capture never blocks an unrelated PR. When no baseline is checked in
  it runs in *seed mode*: uploads `current.json` as the `profiling-baseline-seed`
  artifact and never fails.

```sh
# The two logic self-tests the gate runs on every PR:
python3 tools/profiling/flamediff.py --self-test
python3 tools/profiling/flamebearer_to_pprof.py --self-test

# The capture chain the heavy job runs (validated render query, Pyroscope 1.9.0):
bash tools/profiling/ci_release_capture.sh "$PWD/out" 10m konfig       # -> out/cpu.flamebearer.json
python3 tools/profiling/flamebearer_to_pprof.py out/cpu.flamebearer.json --top-frames 5 -o current.json
python3 tools/profiling/flamediff.py current.json .profiling-baseline.json --output="$GITHUB_STEP_SUMMARY"
```

### Arming the gate (needs one Linux CI run — cannot be done on macOS)

1. **Seed the baseline from `main`.** Run the workflow on `main`
   (`gh workflow run flamediff.yml`), download the `profiling-baseline-seed`
   artifact, and commit its `profiling-baseline-seed.json` as
   `.profiling-baseline.json` at the repo root. This is the remaining blocker —
   see the ingestion finding below.
2. **Verify the gate fires.** Open a throwaway PR labeled `profiling` with an
   intentional hot-path regression (e.g. a `sleep` in the apply path) and confirm
   the `capture-gate` job fails; revert.
3. **Promote to required** (optional) once the capture is proven green on Linux.

### Findings from the local standup (why the baseline needs Linux CI)

- **Ingestion path is alloy eBPF, not the in-process agent.** The
  `konfig-profiling` image's in-process `pyroscope-rs` agent
  (`PYROSCOPE_SERVER_ADDRESS`) does **not** ingest into Grafana Pyroscope 1.9.0
  (legacy `/ingest` format) — a local standup left `service_name=konfig` with
  zero series. The supported path is the **alloy `pyroscope.ebpf`** DaemonSet
  (`infra/profiling/alloy-*.yaml`) scraping konfig pods → pyroscope with
  `service_name=konfig`. eBPF needs host-kernel perf access, which is unavailable
  in kind-on-Apple-Silicon — hence the baseline must come from a Linux CI runner.
- **The in-process agent needs a relaxed seccomp profile.** If you do use it,
  `pprof-rs` fails at startup with `Error: AdHoc("create profiler error")` under
  the default restricted seccomp. The konfig container must run with
  `securityContext: { seccompProfile: { type: Unconfined }, capabilities: { add: ["SYS_PTRACE"] } }`
  for the signal/timer-based sampler to initialize.

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

# import-images.sh — Docker Desktop k8s.io image import (CU-86aj7kawk)

Pushes freshly built bench images from the dockerd store into Docker Desktop's
`k8s.io` containerd namespace, so pods (`imagePullPolicy: IfNotPresent`, `:latest`)
don't silently run a stale image. dockerd and the k8s containerd are separate
stores on Docker Desktop; `bazel run //docker/...:load_<arch>` only populates the
former. `docker save | nsenter (VM PID 1) | ctr -n k8s.io images import -`.

```sh
bazel run //tools/profiling:import_images -- --build      # build+load+import default set
bazel run //tools/profiling:import_images                 # import already-built images
bazel run //tools/profiling:import_images -- --dry-run    # print commands only
bazel run //tools/profiling:import_images -- --help
```

Defaults: `kasa288/konfig{,-loadtest,-heapprof}:latest`. Flags: `--arch
arm64|amd64` (default arm64), positional image refs to override the set; env
`NSENTER_IMAGE` to swap the nsenter helper (default `justincormack/nsenter1`).
See the konfig-loadtest bench runbook for the full local-bench flow.

# Per-release CPU profile archive (CU-86ahtj1v9)

`.github/workflows/profile-release.yml` runs on every `v*` tag push (and manual
dispatch): it stands up kind + an in-cluster pyroscope + the `konfig-profiling`
image (in-process pyroscope agent), drives the `saturate` loadtest profile
(CU-LOAD-1), then captures the CPU profile via pyroscope's flamebearer render
API, converts it to pprof (`flamebearer_to_pprof.py`), and reduces a
`samples.csv` (`tools/profiling/ci_release_capture.sh`).

The bundle (`cpu.pprof`, `cpu.flamebearer.json`, `samples.csv`, `RELEASE.txt`,
`konfig.log`) is uploaded as the **`release-profiles-<tag>`** GitHub Actions
artifact (90-day retention; NOT AWS per memory rule) — the canonical
quarterly-comparison archive, no loadtest re-run needed. Download it from the
workflow run's Artifacts panel for the corresponding tag.

```sh
gh run download --name release-profiles-v1.2.3        # by tag
go tool pprof -http=:0 cpu.pprof                       # inspect locally
```

# Local flamegraph (CU-86ahtj0m5)

`bazel run //rust/konfig:flamegraph -- --duration 60` produces `flamegraph.svg`
from the `konfig` binary (`profiling` feature) via cargo-flamegraph — zero
cluster pyroscope. The wrapper (`rust/konfig/flamegraph.sh`) parses `--duration`
/ `-o`, forwards args after `--` to konfig, and arms a SIGINT timer so the
long-running server flushes the SVG after the window.

Runtime prereqs (cargo-flamegraph's sampler):

- **Linux**: `perf` (linux-tools) + a low enough `perf_event_paranoid`.
- **macOS**: `dtrace`, which requires root — re-run under
  `sudo -E env "PATH=$PATH" bazel run //rust/konfig:flamegraph -- --duration 60`.

For cluster-representative captures, prefer the Linux CI profiling pipeline
(CU-86aj4z43b, `.github/workflows/profiling.yml`).

# Linux coalesce/shards flip gate (CU-86aj4z43b)

Authoritative, reproducible **Linux** profiling path that decides whether the
Subscribe broadcast defaults `--coalesce-window-ms` (0→5) and
`--broadcast-shards` (1→4) are safe to flip on. macOS numbers from the
2026-06-19 run are indicative only (darwin→linux cross-compile is broken); this
builds the `konfig-heapprof` image natively on a Linux amd64 runner.

Driven by `.github/workflows/profiling.yml` (manual dispatch). Per run it
captures two variants in one kind cluster:

| variant | `--coalesce-window-ms` | `--broadcast-shards` |
|---|---|---|
| baseline | 0 | 1 |
| optimized | 5 | 4 |

- `ci_capture_variant.sh <name> <coalesce_ms> <shards> <soak_s> <outdir>` —
  deploys `konfig-heapprof` with the variant config, runs the loadtest
  `subscribe` scenario as traffic, and captures (from the pod's own
  `/metrics` + `/debug/heap-profile.pprof`) tokio park/noop runtime gauges,
  `konfig_broadcast_lag_total` drops, snmalloc heap pprof, and the loadtest
  results JSON (subscribe p99) via a busybox sidecar.
- `profiling_gate.py <captures-root>` — compares baseline vs optimized and
  asserts the CU-86aj3vpgr / CU-86aj3vpnh acceptances (noop-park rate
  `< NOOP_PARK_MAX` **and** below baseline; drop-count not worse; subscribe p99
  `<= P99_BUDGET_MS`), emitting a **FLIP APPROVED / BLOCKED** verdict to the
  job summary. Exits non-zero when the optimized variant fails the gate.

```sh
# Logic self-test (synthetic captures) is covered by the workflow; run the gate
# locally against a captures/ dir produced by ci_capture_variant.sh:
NOOP_PARK_MAX=0.20 P99_BUDGET_MS=50 python3 tools/profiling/profiling_gate.py captures
```
