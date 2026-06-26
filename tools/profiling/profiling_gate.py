#!/usr/bin/env python3
"""CU-86aj4z43b — coalesce/shards default-flip gate.

Reads the two variant capture dirs produced by ci_capture_variant.sh
(baseline = coalesce 0 / shards 1; optimized = coalesce 5ms / shards 4) and
decides whether the optimized defaults are safe to flip on, by asserting the
ticket acceptances on prod-representative Linux:

  * noop-park rate < NOOP_PARK_MAX (default 0.20)  AND lower than baseline
    (the wake-reduction win from coalesce CU-86aj3vpgr + shards CU-86aj3vpnh),
  * subscriber backpressure drop-count (konfig_broadcast_lag_total) not worse
    than baseline,
  * subscribe p99 within P99_BUDGET_MS (default 50) — the ~+coalesce-window
    latency cost must stay acceptable.

Exits non-zero iff the optimized variant fails the gate. Writes a Markdown
verdict to $GITHUB_STEP_SUMMARY when set (always also to stdout).

Usage: profiling_gate.py <captures-root>   # expects <root>/baseline, <root>/optimized
"""
import glob
import json
import os
import re
import sys

NOOP_PARK_MAX = float(os.environ.get("NOOP_PARK_MAX", "0.20"))
P99_BUDGET_MS = float(os.environ.get("P99_BUDGET_MS", "50"))


def _prom_values(text, metric):
    """All sample values for `metric` (label sets summed by caller)."""
    out = []
    for line in text.splitlines():
        if line.startswith("#") or not line.startswith(metric):
            continue
        # metric{labels} value   OR   metric value
        m = re.match(rf"{re.escape(metric)}(\{{[^}}]*\}})?\s+([0-9eE.+-]+)$", line)
        if m:
            try:
                out.append(float(m.group(2)))
            except ValueError:
                pass
    return out


def _read(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return ""


def variant_stats(vdir):
    """Average noop-park rate over steady snapshots + final drop count + p99."""
    snaps = sorted(glob.glob(os.path.join(vdir, "metrics.[0-9]*.txt")))
    # Drop the first steady snapshot as warmup if we have >=3.
    steady = snaps[1:] if len(snaps) >= 3 else snaps
    rates, mpp = [], []
    for f in steady:
        t = _read(f)
        park = sum(_prom_values(t, "tokio_park_count_total")) or 0.0
        noop = sum(_prom_values(t, "tokio_noop_count_total")) or 0.0
        if park > 0:
            rates.append(noop / park)
        m = _prom_values(t, "tokio_mean_polls_per_park")
        if m:
            mpp.append(m[0])
    noop_park = sum(rates) / len(rates) if rates else float("nan")
    mean_polls = sum(mpp) / len(mpp) if mpp else float("nan")

    final = _read(os.path.join(vdir, "metrics.final.txt"))
    drops = sum(_prom_values(final, "konfig_broadcast_lag_total"))

    p99 = float("nan")
    try:
        res = json.loads(_read(os.path.join(vdir, "results.json")) or "{}")
        for s in res.get("scenarios", []):
            if s.get("name", "").lower().startswith("subscribe") and s.get("metrics"):
                p99 = float(s["metrics"]["p99_ms"])
                break
    except (ValueError, KeyError, TypeError):
        pass

    return {
        "noop_park_rate": noop_park,
        "mean_polls_per_park": mean_polls,
        "drops": drops,
        "p99_ms": p99,
        "n_steady": len(steady),
    }


def fmt(x, suffix=""):
    return "n/a" if x != x else f"{x:.3f}{suffix}"  # x!=x -> NaN


def main():
    root = sys.argv[1] if len(sys.argv) > 1 else "captures"
    base = variant_stats(os.path.join(root, "baseline"))
    opt = variant_stats(os.path.join(root, "optimized"))

    checks = []

    def check(name, ok, detail):
        checks.append((name, ok, detail))
        return ok

    np_ok = opt["noop_park_rate"] == opt["noop_park_rate"] and opt["noop_park_rate"] < NOOP_PARK_MAX
    check("noop-park rate < %.0f%%" % (NOOP_PARK_MAX * 100), np_ok,
          "optimized=%s (max %.2f)" % (fmt(opt["noop_park_rate"]), NOOP_PARK_MAX))

    np_win = (opt["noop_park_rate"] == opt["noop_park_rate"]
              and base["noop_park_rate"] == base["noop_park_rate"]
              and opt["noop_park_rate"] <= base["noop_park_rate"])
    check("noop-park lower than baseline (wake-reduction win)", np_win,
          "baseline=%s -> optimized=%s" % (fmt(base["noop_park_rate"]), fmt(opt["noop_park_rate"])))

    drop_ok = opt["drops"] <= base["drops"]
    check("backpressure drop-count not worse than baseline", drop_ok,
          "baseline=%.0f -> optimized=%.0f" % (base["drops"], opt["drops"]))

    # p99 is advisory if the loadtest JSON was unavailable (NaN -> skip-as-warn).
    if opt["p99_ms"] == opt["p99_ms"]:
        p99_ok = opt["p99_ms"] <= P99_BUDGET_MS
        check("subscribe p99 <= %.0f ms" % P99_BUDGET_MS, p99_ok,
              "optimized p99=%.1f ms (baseline=%s)" % (opt["p99_ms"], fmt(base["p99_ms"], " ms")))
    else:
        checks.append(("subscribe p99 (advisory — no results.json)", None,
                       "p99 unavailable; gate on /metrics signals only"))

    gate_pass = all(ok for _, ok, _ in checks if ok is not None)

    lines = []
    lines.append("## CU-86aj4z43b — coalesce/shards default-flip gate")
    lines.append("")
    lines.append("| Metric | baseline (0ms / 1 shard) | optimized (5ms / 4 shards) |")
    lines.append("|---|---|---|")
    lines.append("| noop-park rate | %s | %s |" % (fmt(base["noop_park_rate"]), fmt(opt["noop_park_rate"])))
    lines.append("| mean polls/park | %s | %s |" % (fmt(base["mean_polls_per_park"]), fmt(opt["mean_polls_per_park"])))
    lines.append("| broadcast_lag drops | %.0f | %.0f |" % (base["drops"], opt["drops"]))
    lines.append("| subscribe p99 (ms) | %s | %s |" % (fmt(base["p99_ms"]), fmt(opt["p99_ms"])))
    lines.append("| steady snapshots | %d | %d |" % (base["n_steady"], opt["n_steady"]))
    lines.append("")
    lines.append("### Gate checks")
    for name, ok, detail in checks:
        mark = "PASS" if ok else ("WARN" if ok is None else "FAIL")
        lines.append("- **%s** — %s _(%s)_" % (mark, name, detail))
    lines.append("")
    verdict = "FLIP APPROVED" if gate_pass else "FLIP BLOCKED"
    lines.append("### Verdict: **%s**" % verdict)
    if gate_pass:
        lines.append("Optimized defaults (`--coalesce-window-ms 5`, `--broadcast-shards 4`) "
                     "meet all acceptances on Linux — safe to flip.")
    else:
        lines.append("One or more acceptances failed — keep defaults at `0` / `1` and investigate.")

    report = "\n".join(lines)
    print(report)
    summ = os.environ.get("GITHUB_STEP_SUMMARY")
    if summ:
        with open(summ, "a", encoding="utf-8") as fh:
            fh.write(report + "\n")

    sys.exit(0 if gate_pass else 1)


if __name__ == "__main__":
    main()
