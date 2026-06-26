#!/usr/bin/env bash
# CU-86ahtj0m5 — local SVG flamegraph of the konfig binary, no cluster pyroscope.
#
# Wraps `cargo flamegraph --bin konfig --features profiling` (cargo-flamegraph +
# inferno) to emit `flamegraph.svg`. konfig is a long-running server, so a timer
# SIGINTs the run after --duration so the SVG is flushed.
#
#   bazel run //rust/konfig:flamegraph -- --duration 60
#   bazel run //rust/konfig:flamegraph -- --duration 60 -o /tmp/fg.svg -- --tls=false
#
# Runtime prereqs (cargo-flamegraph's sampler):
#   * Linux: `perf` (linux-tools) + perf_event_paranoid low enough.
#   * macOS: `dtrace`, which requires root — re-run under sudo (see preflight).
# For cluster-representative captures use the Linux CI profiling pipeline
# (CU-86aj4z43b) instead; this target is a zero-K8s local convenience.
set -euo pipefail

DURATION=30
OUT=flamegraph.svg
BIN_ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --duration) DURATION="${2:?--duration needs a value}"; shift 2 ;;
    --duration=*) DURATION="${1#*=}"; shift ;;
    -o|--output) OUT="${2:?-o needs a path}"; shift 2 ;;
    --) shift; BIN_ARGS+=("$@"); break ;;
    *) BIN_ARGS+=("$1"); shift ;;
  esac
done

# `bazel run` sets BUILD_WORKSPACE_DIRECTORY to the repo root; cargo needs the
# workspace and we want flamegraph.svg written there.
WS="${BUILD_WORKSPACE_DIRECTORY:-$PWD}"
cd "$WS"

OS="$(uname -s)"
if [ "$OS" = "Darwin" ] && [ "$(id -u)" -ne 0 ]; then
  echo "flamegraph on macOS samples via dtrace, which requires root." >&2
  echo "re-run under sudo (keep cargo on PATH):" >&2
  echo "  sudo -E env \"PATH=\$PATH\" bazel run //rust/konfig:flamegraph -- --duration ${DURATION}" >&2
  echo "or capture on Linux via the CI profiling pipeline (CU-86aj4z43b)." >&2
  exit 1
fi
if [ "$OS" = "Linux" ] && ! command -v perf >/dev/null 2>&1; then
  echo "perf not found — install linux-tools-\$(uname -r) (cargo-flamegraph needs it)." >&2
  exit 1
fi
if ! cargo flamegraph --version >/dev/null 2>&1; then
  echo "cargo-flamegraph not installed — \`cargo install flamegraph\` (CU-86ahtj0m5)." >&2
  exit 1
fi

echo "flamegraph: profiling \`konfig\` (features=profiling) for ${DURATION}s -> ${OUT}"
# konfig runs until interrupted; arm a one-shot SIGINT so cargo-flamegraph
# flushes the SVG after the window. `-x konfig` matches the exact process name.
( sleep "$DURATION"; pkill -INT -x konfig 2>/dev/null || true ) &
TIMER=$!
trap 'kill "$TIMER" 2>/dev/null || true' EXIT

cargo flamegraph --bin konfig --features profiling -o "$OUT" -- "${BIN_ARGS[@]}"

echo "wrote ${OUT} under ${WS}"
