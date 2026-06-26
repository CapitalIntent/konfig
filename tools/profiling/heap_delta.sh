#!/usr/bin/env bash
#
# heap_delta.sh — steady-state heap delta from two snmalloc pprof snapshots
# (CU-86aj7kavv).
#
# Subtracts a warmup-baseline heap profile from a later steady-state profile so
# one-time startup / TLS / lazy-init allocations cancel out and only per-request
# steady-state growth survives in the report. The konfig-heapprof endpoint
# (`/debug/heap-profile.pprof`) emits a snmalloc heap profile whose top
# attribution otherwise mixes process-start paths (`konfig::startup::run`,
# rustls `handle_new_ticket_impl`, first-touch arenas) with real steady-state
# allocation, making per-request growth impossible to read in isolation.
#
# Mechanism: `go tool pprof -diff_base=<warmup> <later>` does a per-call-site
# subtraction. snmalloc writes a *pre-symbolized* pprof (function names are
# embedded), so NO konfig binary is needed for symbolization — the two .pb.gz
# snapshots are self-contained. Positive rows = bytes that grew after warmup
# (the steady-state signal); negative rows = transient warmup allocations that
# were freed.
#
# Usage:
#   heap_delta.sh --base WARMUP.pb.gz --profile LATER.pb.gz \
#       [--output REPORT.txt] [--sample SAMPLE_TYPE] [--nodecount N]
#
#   --base       Warmup-baseline snapshot taken AFTER the warmup soak (the
#                snapshot whose startup allocations we want to exclude).
#   --profile    Later steady-state snapshot (typically the final one).
#   --output     Write the report here as well as stdout (default: stdout only).
#   --sample     pprof sample_index (e.g. alloc_space). Default: pprof's own
#                default sample type for the profile.
#   --nodecount  Rows in the -top report (default: 40).
#
# pprof resolution: prefers `go tool pprof`; falls back to a standalone `pprof`
# binary on PATH (`go install github.com/google/pprof@latest`).
#
# Exit: 0 on success; 2 on usage / missing-tool / missing-input error.
set -euo pipefail

BASE=""
PROFILE=""
OUTPUT=""
SAMPLE=""
NODECOUNT=40

usage() {
    sed -n '2,36p' "$0" | sed 's/^# \{0,1\}//'
    exit "${1:-2}"
}

while [ $# -gt 0 ]; do
    case "$1" in
        --base)        BASE="${2:?--base needs a path}"; shift 2 ;;
        --base=*)      BASE="${1#*=}"; shift ;;
        --profile)     PROFILE="${2:?--profile needs a path}"; shift 2 ;;
        --profile=*)   PROFILE="${1#*=}"; shift ;;
        --output)      OUTPUT="${2:?--output needs a path}"; shift 2 ;;
        --output=*)    OUTPUT="${1#*=}"; shift ;;
        --sample)      SAMPLE="${2:?--sample needs a value}"; shift 2 ;;
        --sample=*)    SAMPLE="${1#*=}"; shift ;;
        --nodecount)   NODECOUNT="${2:?--nodecount needs a value}"; shift 2 ;;
        --nodecount=*) NODECOUNT="${1#*=}"; shift ;;
        -h|--help)     usage 0 ;;
        *) echo "heap_delta.sh: unknown arg '$1'" >&2; usage 2 ;;
    esac
done

[ -n "$BASE" ]    || { echo "heap_delta.sh: --base is required" >&2; usage 2; }
[ -n "$PROFILE" ] || { echo "heap_delta.sh: --profile is required" >&2; usage 2; }
[ -f "$BASE" ]    || { echo "heap_delta.sh: --base file not found: $BASE" >&2; exit 2; }
[ -f "$PROFILE" ] || { echo "heap_delta.sh: --profile file not found: $PROFILE" >&2; exit 2; }

# Resolve a pprof front-end: `go tool pprof` first, then standalone `pprof`.
PPROF=()
if command -v go >/dev/null 2>&1 && go tool pprof --help >/dev/null 2>&1; then
    PPROF=(go tool pprof)
elif command -v pprof >/dev/null 2>&1; then
    PPROF=(pprof)
else
    echo "heap_delta.sh: need 'go tool pprof' or a standalone 'pprof' on PATH" >&2
    echo "  install: actions/setup-go (CI) or 'go install github.com/google/pprof@latest'" >&2
    exit 2
fi

ARGS=(-top "-nodecount=$NODECOUNT" "-diff_base=$BASE")
[ -n "$SAMPLE" ] && ARGS+=("-sample_index=$SAMPLE")
ARGS+=("$PROFILE")

REPORT="$("${PPROF[@]}" "${ARGS[@]}" 2>/dev/null)"

{
    echo "# Steady-state heap delta (CU-86aj7kavv)"
    echo "# base (warmup, excluded): $BASE"
    echo "# profile (steady-state):  $PROFILE"
    echo "# positive = grew after warmup; negative = transient warmup freed"
    echo "# tool: ${PPROF[*]} -top -diff_base"
    echo
    echo "$REPORT"
} | if [ -n "$OUTPUT" ]; then tee "$OUTPUT"; else cat; fi
