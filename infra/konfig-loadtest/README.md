# konfig-loadtest profiles

Reusable Kubernetes Job manifests for driving the `konfig-loadtest` binary
against an in-cluster konfig deployment. Each profile is a `kubectl apply`-able
`batch/v1` Job that runs once and cleans itself up after
`ttlSecondsAfterFinished` (1 hour).

## Profiles

| Profile         | Subscribers | Applies | Interval | Events  | CPU limit | Mem limit | Manifest                  |
|-----------------|-------------|---------|----------|---------|-----------|-----------|---------------------------|
| baseline        |         100 |     200 |   100 ms | 20,000  | 2         | 512Mi     | `job.yaml`                |
| stress          |         300 |     500 |    20 ms | 150,000 | 4         | 1Gi       | `job-stress.yaml`         |
| saturate        |       1,000 |   1,000 |     5 ms | 1.0M    | 8         | 2Gi       | `job-saturate.yaml`       |
| backpressure    |         500 |   2,000 |     2 ms | 1.0M    | 6         | 1.5Gi     | `job-backpressure.yaml`   |

`Events = Subscribers × Applies`. `Interval` is the producer cadence
(`S1_INTERVAL_MS`), so total wall time ≈ `Applies × Interval`.

## When to use which

- **baseline (`job.yaml`)** — CI gate. Cheap, deterministic, runs on every PR
  via `.github/workflows/loadtest.yml`. Do not edit; changing it changes the
  pass/fail bar for every PR. Tuned to fit comfortably inside the
  `ubuntu-latest` GHA runner.
- **stress (`job-stress.yaml`)** — Local A/B for tuning passes (h2 server
  window, frame size, polling intervals, allocator swaps). 150k events drain
  in ~13s, fast enough for back-to-back runs but heavy enough that p99 latency
  and CPU% differ visibly between configs.
- **saturate (`job-saturate.yaml`)** — Ceiling-finder. 1M events at 1k
  concurrent subscribers. Used to (a) confirm the server stays correct under
  fanout that exceeds any expected production footprint and (b) record the
  peak CPU/mem and writev rate that the current binary tops out at. Expect
  konfig to consume multi-hundred mCPU here on docker-desktop.
- **backpressure (`job-backpressure.yaml`)** — Producer-faster-than-consumer.
  2ms apply cadence with 500 subscribers deliberately tries to fill h2 send
  buffers faster than streams drain. Use when investigating drops,
  `tonic::Status::resource_exhausted`, h2 flow-control window stalls, or
  unbounded growth in queue depth metrics.

## Required env knobs

All non-baseline profiles depend on the `S1_SUBSCRIBERS`, `S1_APPLIES`, and
`S1_INTERVAL_MS` env vars being honored by the `konfig-loadtest` binary
(`tools/konfig-loadtest/src/main.rs`). When these vars are unset the binary
falls back to the historical baseline defaults — i.e. `job.yaml` still works
even without env-var support, but the other three profiles silently revert
to 100×200×100ms.

## Usage

Apply one profile at a time against a cluster that already has konfig running
in `konfig-system`:

```sh
kubectl --context docker-desktop apply -f infra/konfig-loadtest/job-stress.yaml
kubectl --context docker-desktop -n konfig-system logs -f job/konfig-loadtest-stress
```

Dry-run validation (no cluster mutation, server-side schema check):

```sh
kubectl --context docker-desktop apply -f infra/konfig-loadtest/job-stress.yaml      --dry-run=server
kubectl --context docker-desktop apply -f infra/konfig-loadtest/job-saturate.yaml    --dry-run=server
kubectl --context docker-desktop apply -f infra/konfig-loadtest/job-backpressure.yaml --dry-run=server
```

Cleanup (the Job will auto-clean after 1h via `ttlSecondsAfterFinished`, but
to delete eagerly):

```sh
kubectl --context docker-desktop -n konfig-system delete job konfig-loadtest-stress
kubectl --context docker-desktop -n konfig-system delete job konfig-loadtest-saturate
kubectl --context docker-desktop -n konfig-system delete job konfig-loadtest-backpressure
```

## Measuring konfig CPU during a run

```sh
kubectl --context docker-desktop -n konfig-system top pod -l app=konfig --containers
```

For a continuous sample, loop it:

```sh
while kubectl --context docker-desktop -n konfig-system get job konfig-loadtest-saturate \
  -o jsonpath='{.status.active}' | grep -q 1; do
  kubectl --context docker-desktop -n konfig-system top pod -l app=konfig --no-headers
  sleep 2
done
```
