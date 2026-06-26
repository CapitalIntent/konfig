# konfig-loadtest

gRPC stress/profiling harness for konfig. Five scenarios (subscribe flood, get
flood, reconnect storm, secrets flood, backpressure) driven against a live
konfig deployment. Built with Bazel; shipped as an in-cluster Job.

- Binary: `//tools/konfig-loadtest:konfig_loadtest`
- OCI image: `//docker/konfig-loadtest:konfig_loadtest` (`:load_amd64` / `:load_arm64`)
- Source: `src/main.rs`
- CI gate: `.github/workflows/loadtest-integration.yml`
  (job "konfig-loadtest in-cluster gate (kind)")
- Job manifests: `infra/konfig-loadtest/*.yaml`

## Flags and tunables

CLI flags (`--help` for the full list):

| Flag | Default | Notes |
| --- | --- | --- |
| `--addr` | `http://127.0.0.1:50051` | gRPC endpoint. Plaintext h2; CI disables TLS server-side. |
| `--namespace` | `default` | konfig namespace to target. |
| `--config-name` | `my-config` | Config resource exercised by S1/S2/S3/S5. |
| `--secret-name` | `my-config-secret` | Secret resource exercised by S4. |
| `--scenario` | `all` | `all` \| `subscribe` \| `get` \| `reconnect` \| `secrets` \| `backpressure`. `backpressure` is opt-in (not in `all`). |
| `--duration <secs>` | unset | Sustained soak mode for S1 — drain-only, no p99/missed-event assertion. For RSS/allocator observation, NOT a gate. |
| `--results-json <path>` | unset | Opt-in JSON summary (also via `KONFIG_LOADTEST_RESULTS_JSON`). See below. |

Env tunables (honored by scenario 1; defaults preserve the historical
100 subs / 200 applies / 100 ms shape so unset behavior is unchanged):

| Env | Default | Meaning |
| --- | --- | --- |
| `S1_SUBSCRIBERS` | `100` | Concurrent Subscribe streams. |
| `S1_APPLIES` | `200` | Total Apply RPCs. |
| `S1_INTERVAL_MS` | `100` | Producer cadence between applies (ms). |
| `S1_P99_LIMIT_MS` | `500` | S1 delivery-latency p99 gate (ms). |

`S1_APPLIES` / `S1_INTERVAL_MS` are also reused by the `backpressure` scenario.

## Result output (CU-86ahrg75h)

By default results are only logged via `tracing` (a summary table + per-scenario
`info!` lines with `p50_ms`/`p95_ms`/`p99_ms`/`max_ms`). To commit machine-readable
results, pass `--results-json <path>` or set `KONFIG_LOADTEST_RESULTS_JSON`. The
file is a single JSON object:

```json
{"all_passed":true,"scenarios":[
  {"name":"subscribe_flood","pass":true,
   "metrics":{"samples":10000,"p50_ms":4,"p95_ms":12,"p99_ms":21,"max_ms":88},
   "failures":[]}
]}
```

`metrics` is `null` for scenarios that capture no per-event latency (the
sustained soak). The latency-capturing scenarios are `subscribe_flood`,
`get_flood`, and `secrets_flood`.

To pull the file out of the in-cluster Job pod:

```sh
# have the Job write to an emptyDir or /tmp, then cp before the pod is GC'd
kubectl -n konfig-system cp <loadtest-pod>:/tmp/results.json ./results.json
```

(or run the binary directly against a port-forwarded endpoint and read the file
locally — see "Running locally" below).

## Exit code / gate semantics

`main` builds a PASS/FAIL row per scenario and calls `std::process::exit(1)` if
**any** scenario fails (p99 over limit, missed events, RPC errors, drain
timeout, etc.). The CI gate relies on this: the Job pod exits non-zero, the Job
records `status.failed`, and the workflow fails. A `--results-json` write error
is logged loudly but does NOT mask a PASS, and the JSON `all_passed` field
mirrors the exit decision.

---

# Acceptance runbook (Linux CI runner)

Both acceptance tickets run the same harness against a kind cluster. Mirror the
existing CI gate (`loadtest-integration.yml`) for cluster bring-up and the
TLS-disable patch; only the Job knobs / replica count differ.

## 0. Prerequisites on the runner

- `bazel`/`bazelisk`, `kubectl`, `yq`, `docker`, and `kind` (helm/kind-action in CI).
- Build + load both images natively for the runner arch (CI uses amd64):

```sh
bazel run //docker/konfig:load_amd64
bazel run //docker/konfig-loadtest:load_amd64
kind create cluster --name konfig-loadtest --wait 120s
kind load docker-image kasa288/konfig:latest          --name konfig-loadtest
kind load docker-image kasa288/konfig-loadtest:latest --name konfig-loadtest
```

Wait for the image to appear in containerd and the node to go Ready before
deploying (see the CI gate's "Load images into kind" + "Wait for node Ready"
steps — both guard a first-run crashloop flake).

## 1. CRDs, RBAC, namespace, service, seed

```sh
kubectl apply -f infra/konfig/crd.yaml
kubectl wait --for=condition=Established crd/configs.konfig.io --timeout=60s
kubectl apply -f infra/konfig/namespace.yaml
kubectl apply -f infra/konfig/serviceaccount.yaml
kubectl apply -f infra/konfig/clusterrole.yaml
kubectl apply -f infra/konfig/clusterrole-configmap.yaml
kubectl apply -f infra/konfig/clusterrolebinding.yaml
kubectl apply -f infra/konfig/clusterrolebinding-configmap.yaml
kubectl apply -f infra/konfig/role-secret.yaml
kubectl apply -f infra/konfig/service.yaml

# Point konfig at the loadtest Config name (without mutating the on-disk manifest).
kubectl create configmap konfig-config \
  --namespace=konfig-system \
  --from-literal=namespace=konfig-system \
  --from-literal=name=konfig-loadtest \
  --dry-run=client -o yaml | kubectl apply -f -

# Pre-seed the watched Config CR — konfig health stays NOT_SERVING until the
# cache observes it once, so the Deployment never rolls out without this.
kubectl apply -f infra/konfig-loadtest/seed-config.yaml
```

## 2. Deploy konfig with TLS disabled (mirror the CI patch)

cert-manager is not installed in kind and the loadtest binary speaks plaintext
h2, so the gate strips TLS. The `--tls=false` opt-out is reserved for test
harnesses; prod manifests keep TLS on. This `yq` filter strips the four `--tls*`
args, replaces them with `--tls=false`, and drops the `konfig-tls` volume +
volumeMount so the pod starts without certs. (It also loosens the kind-only
probe timings — keep those for runner stability.)

```sh
yq '
  (.spec.template.spec.containers[] | select(.name == "konfig")).imagePullPolicy = "IfNotPresent"
  | (.spec.template.spec.containers[] | select(.name == "konfig")).args |=
      ([.[] | select(test("^--tls") | not)] + ["--tls=false"])
  | (.spec.template.spec.containers[] | select(.name == "konfig")).readinessProbe.initialDelaySeconds = 15
  | (.spec.template.spec.containers[] | select(.name == "konfig")).livenessProbe.initialDelaySeconds = 15
  | (.spec.template.spec.containers[] | select(.name == "konfig")).livenessProbe.failureThreshold = 6
  | (.spec.template.spec.containers[] | select(.name == "konfig")).env += [{"name": "KONFIG_LOG_SYNC", "value": "1"}]
  | del(.spec.template.spec.containers[] | select(.name == "konfig").volumeMounts[] | select(.name == "konfig-tls"))
  | del(.spec.template.spec.volumes[] | select(.name == "konfig-tls"))
' infra/konfig/deployment.yaml | kubectl apply -f -
```

### Replica count

- **CU-86ahrg75h** runs against **2 replicas**. After applying, scale up:

  ```sh
  kubectl -n konfig-system scale deploy/konfig --replicas=2
  ```

- For **per-pod profiling** runs (pyroscope/pprof/RSS slope) scale to **1
  replica** first so per-pod metrics are not split across pods (project rule):

  ```sh
  kubectl -n konfig-system scale deploy/konfig --replicas=1
  ```

Wait for rollout + pod readiness (readiness gates on the populated cache):

```sh
kubectl rollout status -n konfig-system deploy/konfig --timeout=180s
kubectl wait --for=condition=Available -n konfig-system deploy/konfig --timeout=180s
kubectl wait --for=condition=Ready -n konfig-system pod -l app=konfig --timeout=180s
```

## 3a. CU-86ahzwhat — 100 subs, 10 applies/min × 10 min, p99 < 1000 ms

Spec: 100 Subscribe streams; 10 Apply RPCs/min for 10 min = **100 applies at a
6000 ms interval**; assert p99 < 1000 ms AND zero missed events across all
subscribers. This is the per-event-accounting `subscribe` scenario (NOT
`--duration`, which is soak mode and skips those assertions).

Run as a Job by overriding env on a copy of `infra/konfig-loadtest/job.yaml`
(set `--scenario subscribe` and the four env knobs), or run the binary directly
against a port-forward:

```sh
kubectl -n konfig-system port-forward deploy/konfig 50051:50051 &
S1_SUBSCRIBERS=100 \
S1_APPLIES=100 \
S1_INTERVAL_MS=6000 \
S1_P99_LIMIT_MS=1000 \
bazel run //tools/konfig-loadtest:konfig_loadtest -- \
  --addr http://127.0.0.1:50051 \
  --namespace konfig-system \
  --config-name konfig-loadtest \
  --scenario subscribe \
  --results-json /tmp/acceptance-86ahzwhat.json
```

Pass criteria: process exits 0; `subscribe_flood` row is PASS; the JSON shows
`p99_ms < 1000` and `failures: []` (zero missed events is enforced by the
`missed > 0` gate inside the scenario). Wall time ~10 min (apply loop) + drain.

> Note: in sustained `--duration` mode the harness does NOT assert p99 or
> per-event misses (drain-only soak). Use the `--scenario subscribe` path above
> for the acceptance gate so both assertions are active.

## 3b. CU-86ahrg75h — 2-replica cluster, commit p50/p95/p99

Scale to 2 replicas (step 2 above), then run the harness with `--results-json`
and commit the emitted file. Either reuse the acceptance shape from 3a or run
the full default suite:

```sh
S1_P99_LIMIT_MS=1000 \
bazel run //tools/konfig-loadtest:konfig_loadtest -- \
  --addr http://127.0.0.1:50051 \
  --namespace konfig-system \
  --config-name konfig-loadtest \
  --secret-name konfig-loadtest-secret \
  --scenario all \
  --results-json /tmp/acceptance-86ahrg75h.json
```

Commit `/tmp/acceptance-86ahrg75h.json` (per-scenario `p50_ms`/`p95_ms`/`p99_ms`/
`max_ms`) as the recorded result for the ticket.

## 4. In-cluster Job variant

To run inside the cluster instead of via port-forward, apply a Job manifest with
`imagePullPolicy: IfNotPresent` (kind has the locally-loaded `:latest`):

```sh
sed 's/imagePullPolicy: Always/imagePullPolicy: IfNotPresent/' \
  infra/konfig-loadtest/job.yaml | kubectl apply -f -
kubectl logs -n konfig-system -f job/konfig-loadtest --all-containers=true
```

The Job pod's exit code is reflected in `status.succeeded` / `status.failed`.
To capture `--results-json` from a Job, add the flag to the Job `args`, mount a
volume for the path, and `kubectl cp` the file before the pod is GC'd
(`ttlSecondsAfterFinished: 3600`).

## Known infra flake: SIGILL / exit 132 at snmalloc init

The konfig **server** image (not the loadtest binary) can crash with
**SIGILL, exit 132** on certain runner/kind-node CPU pools. snmalloc compiles
with `-mcx16` (CMPXCHG16B); a CPU model lacking that instruction faults at
allocator init, **before `main` runs** — the symptom is a pod CrashLoopBackOff
with **empty logs**. The CI gate's "CPU capability probe" step prints the host
CPU model and whether the `cx16` flag is present.

This is NOT a loadtest-binary bug and must NOT be "fixed" in Rust. **Guidance:
rerun the job once.** Runners are drawn from heterogeneous CPU pools, so a rerun
usually lands on a `cx16`-capable host. If it recurs deterministically, the
runner pool genuinely lacks `cx16` — escalate the runner image, do not patch the
allocator. Refs CU-86aj4guza, CU-86aj3872a.

## Docker Desktop: import images into the k8s.io containerd ns (CU-86aj7kawk)

When the local bench runs on **Docker Desktop Kubernetes** (`--context
docker-desktop`), pods are served from containerd's `k8s.io` namespace, which is
**separate** from the dockerd image store that `docker build` / the Bazel
`:load_<arch>` targets populate. A freshly built `:latest` stays invisible to
k8s, so a pod with `imagePullPolicy: IfNotPresent` silently keeps running the
**old** image and quietly invalidates the bench. (kind has the same split but
solves it with `kind load docker-image`, used by the CI gates above — this is
the Docker Desktop equivalent.)

Import the current bench images into the Docker Desktop VM's k8s.io containerd
namespace with one command:

```sh
# build+load (arm64) and import the default bench set:
bazel run //tools/profiling:import_images -- --build
# or import already-built images only:
bazel run //tools/profiling:import_images
# preview the docker/ctr commands without running them:
bazel run //tools/profiling:import_images -- --dry-run
```

Defaults to `kasa288/konfig{,-loadtest,-heapprof}:latest`; pass image refs to
override, `--arch amd64` for Intel, `NSENTER_IMAGE=...` to swap the nsenter
helper. It `docker save`s each image and pipes it through `nsenter` (PID 1 of the
Docker Desktop LinuxKit VM) into `ctr -n k8s.io images import -`, then verifies
with `ctr -n k8s.io images ls`. After importing, roll the pods to pick up the
fresh image:

```sh
kubectl -n konfig-system rollout restart deploy/konfig
```

## Heap profiling: startup vs steady-state (CU-86aj7kavv)

The `konfig-heapprof` image variant compiles snmalloc-rs with the `profiling`
feature and serves a gzipped pprof heap snapshot at
`GET :9090/debug/heap-profile.pprof` (the default `konfig` image returns 404 —
rebuild `//rust/konfig:konfig_bin_heapprof`). A *single* snapshot mixes one-time
startup / TLS / first-touch-arena allocations (`konfig::startup::run`, rustls
`handle_new_ticket_impl`) with real per-request growth, so absolute `-top`
attribution cannot isolate steady-state behavior.

The heap eval captures **three phases** and reports a **delta** so startup is
excluded:

| Phase | Snapshot | When |
| --- | --- | --- |
| startup | `startup.pb.gz` | pre-traffic, right after pod-ready (reference) |
| warmup | `warmup.pb.gz` | after a warmup soak under load (one-time init settled) |
| steady-state | `t<N>s.pb.gz`, `final.pb.gz` | repeated, during sustained load |

**Steady-state delta** = `final − warmup`, per call site. This is the "done
when" artifact: per-request growth with startup/warmup excluded.

### CI (recommended)

Dispatch the manual workflow — it builds the images, brings up kind, runs the
loadtest, captures all three phases, computes the delta, and uploads the
`heap-profiles` artifact (the `.pb.gz` snapshots **plus** `steady-state-delta.txt`):

```sh
gh workflow run heap-profile-eval.yml                       # default 90s warmup
gh workflow run heap-profile-eval.yml -f warmup_seconds=120 # longer warmup soak
```

Download + read the precomputed delta:

```sh
gh run download <run-id> -n heap-profiles -D /tmp/heap
cat /tmp/heap/steady-state-delta.txt
```

### Local

Run a `konfig-heapprof` pod under load (mirror steps 0–2 above but swap the
container image to `kasa288/konfig-heapprof:latest` and scale to **1 replica**
for per-pod accuracy — see `feedback_loadtest_replicas.md`). Then, against a
port-forward of `:9090`, drive the loadtest (sections 3a/3b/4) in another shell
and snapshot the three phases:

```sh
kubectl -n konfig-system port-forward deploy/konfig 9090:9090 &

# 1. pre-traffic reference (optional)
curl -fsS -o startup.pb.gz http://localhost:9090/debug/heap-profile.pprof

# 2. start the loadtest, soak ~90s so one-time init settles, then warmup baseline
sleep 90
curl -fsS -o warmup.pb.gz http://localhost:9090/debug/heap-profile.pprof

# 3. keep load running; final steady-state snapshot
curl -fsS -o final.pb.gz http://localhost:9090/debug/heap-profile.pprof

# 4. steady-state delta (startup excluded)
bash tools/profiling/heap_delta.sh --base warmup.pb.gz --profile final.pb.gz
```

`heap_delta.sh` wraps `go tool pprof -top -diff_base` (needs `go`, or a
standalone `pprof`, on PATH). snmalloc emits *pre-symbolized* pprof, so no
konfig binary is needed. Positive rows grew after warmup (the steady-state
signal); negative rows were transient warmup allocations since freed. For an
interactive view: `go tool pprof -http=:8080 -diff_base=warmup.pb.gz final.pb.gz`.

## Soak + streaming snmalloc capture (CU-86aj35zxw)

A steady-state soak (`--scenario soak`) runs a sustained mixed workload — the
natural host for two complementary observers over one window:

- **per-callsite allocation churn** — snmalloc streaming-mode JSONL +
  `snmalloc-tools rate-report` (catches *transient* churn the snapshot heap
  profile is blind to: broadcast emit, `snapshot_to_proto`, cache get/update,
  per-`Subscribe`-stream bookkeeping).
- **whole-process growth/drift** — repeated heap snapshots + the steady-state
  delta (see the heap-profiling section above), which a burst run misses.

### The soak scenario

```sh
konfig-loadtest --scenario soak --duration 600   # 10 min; default when unset
```

Workload mix (all concurrent until the deadline):

| Component | Default | Env tunable |
| --- | --- | --- |
| long-lived config `Subscribe` streams | 50 | `SOAK_CONFIG_SUBS` |
| long-lived `SubscribeSecrets` streams | 25 | `SOAK_SECRET_SUBS` |
| reconnect-churn streams (connect → read → drop → reconnect) | 10 | `SOAK_RECONNECT_SUBS` |
| periodic `Apply` cadence | 250 ms | `SOAK_CONFIG_INTERVAL_MS` |
| periodic `ApplySecret` cadence | 1000 ms | `SOAK_SECRET_INTERVAL_MS` |
| reconnect window | 5000 ms | `SOAK_RECONNECT_INTERVAL_MS` |

It is **opt-in** (never part of `--scenario all`) and the pass/fail gate is
lenient ("the system kept serving the whole mix") — growth and per-callsite
rates are judged out-of-band, not asserted, since 10–30 min of per-event
bookkeeping would leak the loadtest process itself.

### Streaming capture

The `konfig-heapprof` image compiles the snmalloc `profiling` feature, which
includes `konfig::stream_sink`. Set `KONFIG_SNMALLOC_STREAM_PATH` to a writable
path and the process opens a `ProfilingSession` and streams one JSONL line per
sampled alloc/resize (default sampling ~1-per-512 KiB) to that file:

```text
{"ts_ns":<u128>,"kind":"alloc|resize","site":"0x<leaf-frame-hex>","size":<bytes>}
```

The schema matches the fork's `snmalloc-tools rate-report` consumer. Post-process
the captured log to surface the highest-rate call sites:

```sh
snmalloc-tools rate-report --input stream.jsonl --top 20
```

> `site` is the raw innermost return-address (hex); resolve hot sites to symbols
> against the binary (`addr2line`/`atos`) when filing per-callsite findings.

### CI (recommended)

`streamprof-eval.yml` drives the soak against a `konfig-heapprof` Deployment
(stream sink enabled via env + a writable `emptyDir`; a busybox sidecar shares
the volume so the JSONL can be copied out of the distroless pod), captures the
three heap phases + delta, runs `rate-report --top 20` (best-effort), and uploads
the `streamprof` artifact (`stream.jsonl`, `rate-report.txt`, `*.pb.gz`,
`steady-state-delta.txt`, logs):

```sh
gh workflow run streamprof-eval.yml                      # 600s soak, 90s warmup
gh workflow run streamprof-eval.yml -f soak_seconds=1200 # longer soak
gh run download <run-id> -n streamprof -D /tmp/streamprof
cat /tmp/streamprof/rate-report.txt
```

**Acceptance follow-up (human):** from the uploaded `stream.jsonl`, confirm
≥10 MB over a 600 s run, write the `rate-report --top 20` table to the findings
doc, and open a per-callsite ClickUp sub-ticket for any konfig-crate site
> 1 alloc/sec (ignore tonic/hyper frames).

### Local

Mirror the heap-profiling "Local" steps (1-replica `konfig-heapprof` pod), but
add `KONFIG_SNMALLOC_STREAM_PATH=/var/log/konfig-stream/stream.jsonl` to the
container env on a writable `emptyDir` mount, then drive `--scenario soak` and
`rate-report` the copied-out JSONL.

## Teardown / cleanup

Profiling-session cleanup (keeps pyroscope datastore by default):

```sh
bazel run //tools/profiling:teardown -- --context kind-konfig
bazel run //tools/profiling:teardown -- --all --context kind-konfig   # full clean
```

Tear down the kind cluster entirely:

```sh
kind delete cluster --name konfig-loadtest
```
