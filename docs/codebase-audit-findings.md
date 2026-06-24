# Codebase Audit Findings

Date: 2026-06-23

Scope: read-only static review of the Rust/Bazel codebase and local project ticket memory. No Bazel tests or benchmarks were run.

## Executive Summary

The codebase is generally well tested and has several deliberate performance choices, especially around read-heavy config and secret cache access. The main risks are concentrated complexity, write-path scaling in copy-on-write caches, duplicated config/secret plumbing, and unstructured local ticket state.

The highest-value improvements are:

- Split the largest gRPC subscription and loadtest files into smaller ownership units.
- Batch or abstract cache write mutations to reduce whole-map clone churn.
- Extract shared config/secret cache, watcher, and subscription helpers.
- Convert local ticket memory into a structured cache with freshness metadata.

## Findings

### High: Subscription Path Is Over-Concentrated

`rust/konfig/grpc/subscribe.rs` is about 3k lines and owns too many concerns:

- Replay buffer management.
- Per-namespace watcher lifecycle.
- Broadcast sharding.
- Subscriber filtering.
- Backpressure and lag handling.
- Drain behavior.
- A large body of tests.

This makes performance-sensitive changes hard to review because behavioral, concurrency, and test scaffolding concerns are interleaved.

Recommended split:

- `subscribe/replay.rs` for replay entries, resource-version ordering, and resume lookup.
- `subscribe/broadcast.rs` for shard selection, send/drop behavior, and receiver lifecycle.
- `subscribe/watch.rs` for namespace watcher startup, shutdown, and garbage collection.
- `subscribe/filter.rs` for request filter compilation and matching.
- `subscribe/tests.rs` or helper modules for repeated fixtures.

### High: Loadtest Tool Is Too Large To Maintain Safely

`tools/konfig-loadtest/src/main.rs` is about 2k lines and mixes CLI parsing, scenario definitions, client setup, metrics collection, output writing, and concurrency orchestration. It also has repeated `KonfigServiceClient::new(...)` and scenario setup patterns.

Recommended split:

- `args.rs` for CLI/config parsing.
- `client.rs` for channel and authenticated client setup.
- `scenarios/` for each load scenario.
- `metrics.rs` for latency/sample aggregation and output formatting.

This would make it easier to trust loadtest changes when diagnosing service performance.

### Medium: Cache Design Is Read-Friendly But Write-Expensive

`rust/konfig/cache.rs` and `rust/konfig/secret_cache.rs` use `ArcSwap<HashMap<OwnedKey, Arc<_>>>` with a writer mutex. This is a good read-path design:

- Reads are lock-free after loading the current snapshot.
- `cache_key.rs` avoids per-request namespace/name allocation via borrowed lookup keys.
- Stored values are `Arc<_>`, so read clones are cheap.

The trade-off is that each insert, remove, or stale-mark operation clones the entire `HashMap`. That is acceptable for low watch churn and modest object counts, but it becomes expensive if Kubernetes sends many updates or the cache grows.

Recommended follow-ups:

- Add metrics around cache entry count and write mutation rate.
- Consider batch mutation APIs for watch restart and multi-event phases.
- Extract a shared copy-on-write map helper so config and secret caches do not duplicate mutation logic.
- Benchmark update bursts with realistic namespace/config counts before changing the data structure.

### Medium: Config And Secret Paths Duplicate Architecture

There is repeated structure across:

- `rust/konfig/cache.rs` and `rust/konfig/secret_cache.rs`.
- `rust/konfig/watcher.rs`, `rust/konfig/secret_watcher.rs`, and `rust/konfig/configmap_watcher.rs`.
- `rust/konfig/grpc/subscribe.rs` and `rust/konfig/grpc/subscribe_secrets.rs`.
- Synthetic watcher error and event fixture setup in tests.

Some duplication is expected because config and secret APIs have different semantics. The smell is that low-level mechanics are duplicated too: copy-on-write mutation, watcher event pumping, replay/broadcast setup, and test fixture construction.

Recommended approach:

- Extract small helpers only around mechanics, not domain semantics.
- Start with test fixtures and synthetic watcher errors because those are low risk.
- Then extract shared cache mutation primitives.

### Medium: gRPC Service Module Has Too Many Responsibilities

`rust/konfig/grpc/mod.rs` is about 1.7k lines and coordinates service construction, request handlers, authz, audit, metrics, drain state, watcher handles, replay buffers, and broadcast registries.

This file is not necessarily slow, but it is a maintainability risk because it is the crossing point for most service behavior.

Recommended split:

- Request context extraction.
- Authorization/audit wrappers.
- Drain state.
- Watcher and broadcast registry wiring.

### Medium: Local Ticket Cache Is Not Structured

No dedicated structured Linear ticket cache was found for this repo. The local state appears to be Markdown project memory, including ticket hygiene guidance and a note that ClickUp board status can drift from merged PR reality.

Current smell:

- Ticket state is prose, not machine-checkable data.
- "Linear", "ClickUp", and local memory concepts are mixed.
- Freshness and verification status are implicit.
- "Shipped" or "closed" claims require manual validation against merged PRs.

Recommended structured cache fields:

- `ticket_id`
- `source`
- `title`
- `status`
- `assignee`
- `updated_at`
- `last_checked_at`
- `linked_prs`
- `verified_merged`
- `blocking_ticket_ids`
- `notes`

Markdown memory can then be generated from structured state instead of acting as the source of truth.

### Low: Build Configuration Is Mostly Intentional But Sensitive

The Bazel/Rust setup contains intentional performance choices:

- snmalloc as the default allocator.
- `tokio_unstable` rustc flags.
- frame pointers.
- Bazel disk cache.
- release profile with `panic=abort`, stripped symbols, and native CPU tuning.

There is one current local working-tree change in `MODULE.bazel`: the `snmalloc` git override commit was updated. Allocator changes are performance-sensitive and should be called out in review with test or benchmark evidence.

## Positive Signals

- Test density is strong: roughly 19k Rust lines and 369 Rust tests.
- Hot `Get` cache lookups avoid namespace/name allocation through borrowed cache keys.
- The cache comments document the read/write trade-off clearly.
- Bazel and Cargo dependency configuration includes useful comments explaining non-obvious choices.

## Suggested Priority Order

1. Split `rust/konfig/grpc/subscribe.rs` along replay, broadcast, watcher, and filter boundaries.
2. Split `tools/konfig-loadtest/src/main.rs` into scenario and support modules.
3. Add cache write-path metrics and benchmark bursty watcher updates.
4. Extract shared config/secret cache mutation helpers.
5. Replace prose-only ticket memory with a structured local ticket snapshot.


## Bazel And Benchmark Verification Update

Date: 2026-06-23, follow-up run.

### snmalloc Pin

`MODULE.bazel` is now pinned to the requested `jayakasadev/snmalloc` commit:

- `e64cd355ff4f0291101d495f7e8e7d9d0821bfe9`

This follow-up pin uses Bazel-native `cc_library` targets. The previous local runtime patch is no longer needed because the pinned `jayakasadev/snmalloc` commit carries the runtime fix upstream.

### Bazel Configuration Findings

Bazel 9.1.1 can load and query the repo, but the query produced configuration warnings that should be cleaned up:

- `compatibility_level` in `module()` is a no-op in Bazel 9 and should be removed.
- Several root `bazel_dep` versions are lower than the resolved dependency graph versions:
  - `platforms`: root asks `0.0.11`, resolved `1.0.0`.
  - `bazel_skylib`: root asks `1.8.1`, resolved `1.8.2`.
  - `aspect_bazel_lib`: root asks `2.14.0`, resolved `2.21.1`.
  - `protobuf`: root asks `29.3`, resolved `33.4`.
  - `rules_shell`: root asks `0.3.0`, resolved `0.6.1`.

Those warnings do not block native tests, but they are a cache/reproducibility smell because the root module no longer states the versions Bazel actually selected.

### Linux OCI Image Build Fix

With the latest `snmalloc` pin, Linux OCI image targets initially failed during Bazel analysis on this macOS arm64 host:

- `//docker/konfig:load_arm64`
- `//docker/konfig:load_amd64`
- `//docker/konfig-loadtest:load_arm64`
- `//docker/konfig-heapprof:load_arm64`
- `//docker/konfig-profiling:load_arm64`

Representative original failure:

```text
ERROR: external/snmalloc+/BUILD.bazel:89:6: While resolving toolchains for target @@snmalloc+//:snmalloc-rs: No matching toolchains found for types:
  @@bazel_tools//tools/cpp:toolchain_type
```

Toolchain-resolution debug confirmed the root cause: the image platform transition needed a Linux C++ toolchain for `//platforms:linux_arm64`, while the local host only had the Darwin LLVM C++ toolchain available. This is now fixed for the local arm64 image path by wiring a Bazel-managed Docker-derived Linux sysroot into `toolchains_llvm` and using the matching dynamic `libstdc++` runtime settings.

Current verification rebuilt and loaded these arm64 images successfully:

```text
bazel run //docker/konfig:load_arm64
bazel run //docker/konfig-heapprof:load_arm64
bazel run //docker/konfig-profiling:load_arm64
bazel run //docker/konfig-loadtest:load_arm64
```

### Tests Run

These Bazel tests passed after the `e64cd355ff4f0291101d495f7e8e7d9d0821bfe9` pin and after removing the local snmalloc patch:

```text
bazel test //rust/konfig:test //rust/konfig:test_heapprof
```

Result:

- `//rust/konfig:test` passed.
- `//rust/konfig:test_heapprof` passed.

Build output still includes one warning in `rust/konfig/acl.rs`: an unused local variable named `table` in a test around line 522.

The required pre-PR checks also completed successfully after removing stale missing integration-test stanzas from `rust/konfig/Cargo.toml`:

```text
cargo fmt
cargo clippy
cargo-crap
```

`cargo-crap` still reports complexity hotspots, including `classify_secret_patch_error`, `classify_patch_error`, and `map_list_error`; it exited successfully, so treat these as maintainability follow-ups rather than blockers.

### Benchmark Validity

The benchmark harness is an integration/load benchmark, not a standalone CPU microbenchmark. `//tools/konfig-loadtest:konfig_loadtest --help` shows these scenarios:

- `all`
- `subscribe`
- `get`
- `reconnect`
- `secrets`
- `backpressure`

The documented acceptance run is valid for service-level behavior because it exercises a real gRPC server, Kubernetes watches, replay behavior, secret/config paths, and per-scenario latency reporting via `--results-json`. It is not valid for isolating a single function or data structure because cluster scheduling, Kubernetes API latency, container CPU limits, and loadtest client behavior are part of the measurement.

For profiled service benchmarking, use one `konfig` replica when collecting per-pod CPU or memory profiles. Multi-replica acceptance has a different purpose: validating distributed behavior and aggregate latency.

### Benchmark Run Status

A fresh profiled local benchmark was completed against the requested `snmalloc` pin:

- `jayakasadev/snmalloc` pinned to `e64cd355ff4f0291101d495f7e8e7d9d0821bfe9`.
- Docker Desktop context with three ready arm64 nodes.
- Fresh `linux/arm64` images imported into each Docker Desktop node's `k8s.io` containerd namespace.
- One `konfig-heapprof` server replica for per-pod profiling accuracy and heap-profile endpoint availability.
- Alloy/Pyroscope deployed in the `profiling` namespace for CPU profiling.
- `konfig-loadtest --scenario all` with Scenario 1 set to 100 subscribers, 100 applies, and 6000ms apply interval.
- Artifacts: `/tmp/konfig-benchmark-e64cd355-20260624-005314`.

Results:

| Scenario | Result | Key Metrics |
| --- | --- | --- |
| `subscribe_flood` | PASS | 10,000/10,000 events, p50 1ms, p95 2ms, p99 3ms, max 5ms |
| `get_flood` | PASS | 5,000 samples, 0 errors, p50 1ms, p95 1ms, p99 2ms, max 4ms |
| `reconnect_storm` | PASS | 500/500 post-reconnect events, 0 missed |
| `secrets_flood` | PASS | 1,000/1,000 events, p50 0ms, p95 0ms, p99 1ms, max 1ms |

Collected evidence:

- `acceptance.json`: valid JSON, `all_passed: true`.
- `metrics.prom`: Prometheus snapshot after the run, 196 lines / 12,549 bytes.
- `cpu-profile.flamebearer.json`: scoped Pyroscope flamebearer profile for `service_name="konfig"`, 52,508 bytes, 502 names, 93 levels. This is usable Pyroscope profile data, but it is not a pprof payload and `go tool pprof` cannot parse it.
- `heap-profile.pprof`: valid snmalloc heap profile, 1,584 bytes. `go tool pprof -top` succeeded and attributed 1,030.02kB total `alloc_space`, split between a subscription polling frame and `konfig::grpc::subscribe::SubscribeFilter::new`.
- `top-pod.txt`: unavailable because the Docker Desktop cluster does not have Metrics API installed.

### Fixes Applied During Verification

1. Added a Bazel-managed Docker sysroot repository rule for local Darwin -> Linux arm64 OCI image builds. This removes the previous `/tmp/konfig-linux-aarch64-sysroot` dependency and lets `toolchains_llvm` receive a label-backed `linux-aarch64` sysroot.
2. Corrected `toolchains_llvm` Linux stdlib keys from Rust triples to `linux-aarch64` / `linux-x86_64` and used `dynamic-stdc++` to match the distroless `cc` runtime family.
3. Added explicit GCC 13 libstdc++ include flags for the arm64 sysroot via clang `-iwithsysroot`, because `dynamic-stdc++` does not automatically add those headers during `snmalloc` C++ builds.
4. Added `//rust/konfig:snmalloc_tokio_smoke` and `//docker/snmalloc-tokio-smoke:load_arm64` to isolate allocator/Tokio startup from the full service.
5. Reproduced the original crash with the smoke image: it segfaulted in `sn_rust_dealloc` on the first Tokio worker thread.
6. Debug-built the Linux arm64 smoke binary and confirmed the crash path was `snmalloc::AllocStats::on_remote_dealloc`, reached while Rust dropped `std::thread::lifecycle::ThreadInit` on the new worker thread.
7. Fixed the crashing `SNMALLOC_STATS` / `SNMALLOC_STATS_BASIC` defines in the default allocator runtime. This was originally carried as a local `bazel/patches/snmalloc-linux-runtime.patch` that stripped the defines from the fork's `BUILD.bazel`. It is now fixed upstream in the `jayakasadev/snmalloc` fork: commit `e64cd355ff4f0291101d495f7e8e7d9d0821bfe9` drops both defines from the Bazel `_COMMON_DEFINES` so the Bazel build matches the CMake default (all stats tiers OFF). The per-repo patch has been removed and `git_override` now pins that commit with no patches.
8. Removed stale missing `[[test]]` stanzas from `rust/konfig/Cargo.toml` so `cargo fmt`, `cargo clippy`, and `cargo-crap` can run from the current tree.

### Remaining Bazel Notes

The local arm64 image path now works, but two cleanup items remain:

- The Docker-generated sysroot is Bazel-managed, but still local-development oriented because it depends on Docker and apt during repository fetch. For CI-grade hermeticity, replace it with a pinned `http_archive`/prebuilt sysroot artifact or `@toolchains_llvm//toolchain:sysroot.bzl` flow.
- Bazel 9 still reports module-version drift warnings for `platforms`, `bazel_skylib`, `aspect_bazel_lib`, `protobuf`, and `rules_shell`, plus the no-op `compatibility_level` warning.

### Verification Run

Commands/results verified after the fixes:

```text
bazel run //rust/konfig:snmalloc_tokio_smoke
bazel run //docker/snmalloc-tokio-smoke:load_arm64
bazel run //docker/konfig:load_arm64
bazel run //docker/konfig-heapprof:load_arm64
bazel run //docker/konfig-profiling:load_arm64
bazel run //docker/konfig-loadtest:load_arm64
bazel test //rust/konfig:test //rust/konfig:test_heapprof
cargo fmt
cargo clippy
cargo-crap
```

All commands above completed successfully against `e64cd355ff4f0291101d495f7e8e7d9d0821bfe9` after the fixes described here.

## Verification Gaps

Still not run in this audit pass:

- amd64 OCI image builds/smokes

Run amd64 image verification before opening the PR if the branch is expected to support local amd64 image builds from macOS.
