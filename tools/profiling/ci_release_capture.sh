#!/usr/bin/env bash
# CU-86ahtj1v9 — pull the canonical CPU profile for a tagged release from the
# in-cluster pyroscope. No bespoke sidecar: uses pyroscope's flamebearer render
# API (the path tools/profiling/README documents), converts to pprof with the
# repo converter (CU-86aj7kawc), and reduces a samples.csv via `go tool pprof`.
#
# Usage: ci_release_capture.sh <outdir> [window=10m] [app=konfig]
set -uo pipefail
OUT="${1:?outdir}"
WINDOW="${2:-10m}"
APP="${3:-konfig}"
mkdir -p "$OUT"

kubectl -n profiling port-forward svc/pyroscope 4040:4040 >"$OUT/pyro-portfwd.log" 2>&1 &
PF=$!
trap 'kill "$PF" 2>/dev/null || true' EXIT
for _ in $(seq 1 30); do
  curl -fsS -o /dev/null --max-time 2 http://localhost:4040/ready && break
  sleep 1
done

QUERY="process_cpu:cpu:nanoseconds:cpu:nanoseconds{service_name=\"${APP}\"}"
echo "[release-capture] render flamebearer (query=${QUERY}, window=${WINDOW})"
curl -fsS -G http://localhost:4040/pyroscope/render \
  --data-urlencode "query=${QUERY}" \
  --data-urlencode "from=now-${WINDOW}" \
  --data-urlencode "format=json" \
  -o "$OUT/cpu.flamebearer.json" \
  && echo "  flamebearer: $(stat -c '%s' "$OUT/cpu.flamebearer.json") bytes" \
  || echo "::warning::flamebearer render failed (no samples ingested yet?)"

# Canonical pprof artifact (flamebearer -> pprof, CU-86aj7kawc converter).
if [ -s "$OUT/cpu.flamebearer.json" ]; then
  python3 tools/profiling/flamebearer_to_pprof.py "$OUT/cpu.flamebearer.json" -o "$OUT/cpu.pprof" \
    && echo "  cpu.pprof: $(stat -c '%s' "$OUT/cpu.pprof") bytes" \
    || echo "::warning::flamebearer->pprof conversion failed"
fi

# samples.csv (frame,flat_pct,cum_pct) reduced from the pprof — the long-term
# comparison surface (replaces the never-built sidecar in the original ticket).
if command -v go >/dev/null 2>&1 && [ -s "$OUT/cpu.pprof" ]; then
  {
    echo "frame,flat_pct,cum_pct"
    go tool pprof -top -nodecount=200 "$OUT/cpu.pprof" 2>/dev/null | awk '
      seen { pct=$2; cum=$5; gsub(/%/,"",pct); gsub(/%/,"",cum);
             fn=$6; for(i=7;i<=NF;i++) fn=fn" "$i;
             gsub(/,/," ",fn);
             if (fn != "") print fn","pct","cum }
      /flat[ \t]+flat%/ { seen=1 }
    '
  } > "$OUT/samples.csv"
  echo "  samples.csv: $(wc -l < "$OUT/samples.csv") rows"
else
  echo "::warning::go or cpu.pprof missing — skipping samples.csv"
fi
echo "[release-capture] done -> $OUT"
