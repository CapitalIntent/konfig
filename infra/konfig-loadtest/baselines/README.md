# konfig-loadtest perf baselines (CU-86aj08v98)

Committed p99 latency baselines for the **perf-regression gate**
(`//tools/konfig-loadtest:gate`) — the latency sibling of the flamediff CPU gate
(CU-86ahtj1a8). flamediff catches CPU-frame shifts; this catches end-to-end
latency creep (e.g. an extra allocation in the Apply path that flamediff misses
but p99 feels).

## Layout

One file per release tag: `v<MAJOR>.<MINOR>.<PATCH>.json`. Each file is the
loadtest `--results-json` output (same schema), carrying `p99_ms` per scenario.
The gate picks the **newest** baseline by semver (numeric, so `v1.10.0` beats
`v1.9.0`) and fails if any scenario's current `p99_ms` exceeds
`baseline p99 × 1.1` (>10% regression). Only scenarios that emit latency
metrics are compared (`subscribe_flood`, `get_flood`, `secrets_flood`;
`reconnect_storm`/`soak` have null metrics and are skipped).

## Running the gate

```sh
bazel build //tools/konfig-loadtest:gate
GATE=$(bazel cquery --output=files //tools/konfig-loadtest:gate)
"$GATE" --current results.json                       # gate vs newest baseline
"$GATE" --current results.json --threshold 1.15      # looser 15% gate
"$GATE" --current results.json --update v0.2.0        # write a new baseline
```

Exit `0` = no regression / seeded, `1` = a scenario p99 regressed, `2` = bad input.

## Seeding / bumping a baseline (PERF_BASELINE_BUMP)

Baselines are captured on a Linux CI runner (representative p99), not locally.
Run **Loadtest Acceptance (manual)** with the `baseline_bump_tag` input set to
the new tag (e.g. `v0.1.0`); the run writes `baselines/<tag>.json` and uploads
it as the `perf-baseline-<tag>` artifact. Download it, commit it here via a PR
(CI never pushes to main). After an intentional perf change, bump the tag so the
gate re-baselines instead of flagging the expected shift.

## Wiring

- `loadtest-acceptance.yml` runs the gate **non-blocking** (reports the p99 diff
  to the job summary) — p99 on a shared kind runner is noisy, so it is not a
  hard PR gate.
- The future release workflow (CU-86ahzwjpr) invokes `//tools/konfig-loadtest:gate`
  as a **hard** gate: a tag whose p99 regressed >10% fails the release unless the
  commit bumps the baseline.
