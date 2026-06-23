#!/usr/bin/env python3
"""tools/profiling/flamediff.py — top-frame CPU-profile regression gate (CU-86ahtj1a8).

Compares the top-N self-% frames of a freshly-captured pyroscope profile
(`current.json`) against a checked-in baseline (`.profiling-baseline.json`) and
fails (non-zero exit) when any frame's self-% has *increased* beyond a relative
threshold — i.e. a frame got measurably hotter. Decreases are improvements, not
regressions, and never fail the gate.

Both inputs are JSON of the shape produced by the pyroscope top-frames export:

    {"frames": [{"name": "<fn>", "self_pct": <float 0..100>}, ...]}

`self_pct` is the self (exclusive) CPU percentage of that frame. Order does not
matter — this script sorts by `self_pct` and takes the top-N from each side.

Usage:
    python3 tools/profiling/flamediff.py <current.json> <baseline.json> \
        [--threshold=0.20] [--top=5] [--min-base-pct=1.0] \
        [--new-frame-floor-pct=5.0] [--output=FILE]
    python3 tools/profiling/flamediff.py --self-test

Exit status:
    0 — no regression (or every delta within threshold / below the noise floor)
    1 — at least one top-N frame regressed (self-% up > threshold), OR a new
        frame entered the top-N above the new-frame floor
    2 — usage / input error

Why relative (not absolute) delta: sampling noise on a 10-min saturate capture
runs ~3-5%; a 20% *relative* gate (e.g. 10.0% -> 12.0%) clears that noise band
while still catching real path regressions (writev / encode / broadcast). Tiny
frames (baseline self-% below --min-base-pct) are exempt from the relative gate
— a 0.4% -> 0.9% jiggle is +125% relative but physically irrelevant — and are
instead only flagged if they cross --new-frame-floor-pct in absolute terms.

The markdown summary (stdout or --output) is suitable for a PR comment.
"""

import argparse
import json
import sys
from pathlib import Path

DEFAULT_THRESHOLD = 0.20  # relative self-% increase that fails the gate
DEFAULT_TOP = 5
DEFAULT_MIN_BASE_PCT = 1.0  # below this baseline self-%, relative gate is noise
DEFAULT_NEW_FRAME_FLOOR_PCT = 5.0  # a new top-N frame this hot counts as a regression


def load_frames(path: str) -> dict:
    """Load a profile JSON into a {frame_name: self_pct} dict.

    Accepts either {"frames": [{"name", "self_pct"}, ...]} or a bare list of
    those objects. Raises ValueError with a clear message on a malformed file
    so the CI step fails loudly rather than silently passing the gate.
    """
    try:
        raw = json.loads(Path(path).read_text())
    except FileNotFoundError as e:
        raise ValueError(f"profile file not found: {path}") from e
    except json.JSONDecodeError as e:
        raise ValueError(f"invalid JSON in {path}: {e}") from e

    frames = raw.get("frames", raw) if isinstance(raw, dict) else raw
    if not isinstance(frames, list):
        raise ValueError(f"{path}: expected a 'frames' list, got {type(frames).__name__}")

    out: dict = {}
    for i, fr in enumerate(frames):
        if not isinstance(fr, dict) or "name" not in fr or "self_pct" not in fr:
            raise ValueError(f"{path}: frame[{i}] must have 'name' and 'self_pct'")
        try:
            out[str(fr["name"])] = float(fr["self_pct"])
        except (TypeError, ValueError) as e:
            raise ValueError(f"{path}: frame[{i}].self_pct not a number: {fr['self_pct']!r}") from e
    return out


def top_names(frames: dict, top: int) -> list:
    """Return the names of the `top` hottest frames (highest self_pct first)."""
    return [name for name, _ in sorted(frames.items(), key=lambda kv: kv[1], reverse=True)[:top]]


def diff(
    current: dict,
    baseline: dict,
    *,
    threshold: float = DEFAULT_THRESHOLD,
    top: int = DEFAULT_TOP,
    min_base_pct: float = DEFAULT_MIN_BASE_PCT,
    new_frame_floor_pct: float = DEFAULT_NEW_FRAME_FLOOR_PCT,
) -> list:
    """Compute per-frame deltas over the union of each side's top-N frames.

    Returns a list of row dicts (sorted hottest-current-first):
        {name, base_pct, cur_pct, rel_delta (or None), regressed (bool), reason}
    A row regresses when:
      * baseline self-% >= min_base_pct AND relative increase > threshold, OR
      * the frame is new/negligible in baseline (< min_base_pct) AND its current
        self-% >= new_frame_floor_pct (a newly-hot frame).
    """
    names = set(top_names(baseline, top)) | set(top_names(current, top))
    rows = []
    for name in names:
        base = baseline.get(name, 0.0)
        cur = current.get(name, 0.0)
        regressed = False
        reason = ""
        rel = None
        if base >= min_base_pct:
            rel = (cur - base) / base
            if rel > threshold:
                regressed = True
                reason = f"self% +{rel * 100:.0f}% (> {threshold * 100:.0f}% gate)"
        elif cur >= new_frame_floor_pct:
            # New or previously-negligible frame that is now hot.
            regressed = True
            reason = f"new hot frame {cur:.1f}% (>= {new_frame_floor_pct:.0f}% floor)"
        rows.append(
            {
                "name": name,
                "base_pct": base,
                "cur_pct": cur,
                "rel_delta": rel,
                "regressed": regressed,
                "reason": reason,
            }
        )
    rows.sort(key=lambda r: r["cur_pct"], reverse=True)
    return rows


def render_markdown(rows: list, *, threshold: float, top: int) -> str:
    regressions = [r for r in rows if r["regressed"]]
    lines = ["## Flamediff — top-frame CPU regression gate", ""]
    if regressions:
        lines.append(f"❌ **{len(regressions)} frame(s) regressed** (relative self-% gate: {threshold * 100:.0f}%, top-{top}).")
    else:
        lines.append(f"✅ No top-{top} frame regressed beyond the {threshold * 100:.0f}% relative self-% gate.")
    lines += ["", "| frame | baseline self% | current self% | Δ | |", "|---|---|---|---|---|"]
    for r in rows:
        if r["rel_delta"] is None:
            delta = "new" if r["cur_pct"] > 0 else "—"
        else:
            delta = f"{r['rel_delta'] * 100:+.0f}%"
        flag = "❌" if r["regressed"] else ""
        note = f" {r['reason']}" if r["reason"] else ""
        lines.append(
            f"| `{r['name']}` | {r['base_pct']:.1f}% | {r['cur_pct']:.1f}% | {delta} |{flag}{note} |"
        )
    return "\n".join(lines) + "\n"


def run(args: argparse.Namespace) -> int:
    try:
        current = load_frames(args.current)
        baseline = load_frames(args.baseline)
    except ValueError as e:
        print(f"flamediff: {e}", file=sys.stderr)
        return 2

    rows = diff(
        current,
        baseline,
        threshold=args.threshold,
        top=args.top,
        min_base_pct=args.min_base_pct,
        new_frame_floor_pct=args.new_frame_floor_pct,
    )
    md = render_markdown(rows, threshold=args.threshold, top=args.top)
    if args.output:
        Path(args.output).write_text(md)
    print(md)
    return 1 if any(r["regressed"] for r in rows) else 0


# ── self-test ───────────────────────────────────────────────────────────────
# No pytest in this repo's tooling (see tools/coverage_pr_comment.py) — the gate
# logic is locked in by an embedded `--self-test` runnable in CI as a cheap
# `python3 flamediff.py --self-test` step.

def _self_test() -> int:
    def frames(*pairs):
        return {n: p for n, p in pairs}

    def regressed_names(cur, base, **kw):
        return {r["name"] for r in diff(cur, base, **kw) if r["regressed"]}

    # 1. Identical profiles → no regression.
    base = frames(("a", 10.0), ("b", 8.0), ("c", 5.0))
    assert regressed_names(base, base) == set(), "identical must not regress"

    # 2. A top frame +30% relative (> 20% gate) → regression.
    cur = frames(("a", 13.0), ("b", 8.0), ("c", 5.0))
    assert regressed_names(cur, base) == {"a"}, "a +30% must regress"

    # 3. A decrease (improvement) → never a regression.
    cur = frames(("a", 4.0), ("b", 8.0), ("c", 5.0))
    assert regressed_names(cur, base) == set(), "a halved must not regress"

    # 4. Within-noise +10% (< 20% gate) → no regression.
    cur = frames(("a", 11.0), ("b", 8.0), ("c", 5.0))
    assert regressed_names(cur, base) == set(), "+10% is within noise band"

    # 5. New hot frame (absent in baseline, current 15% >= 5% floor) → regression.
    cur = frames(("a", 10.0), ("b", 8.0), ("d", 15.0))
    assert "d" in regressed_names(cur, base), "new 15% frame must regress"

    # 6. Tiny frame exempt from relative gate: 0.4% -> 0.9% is +125% but below
    #    min_base_pct (1.0) and below the 5% new-frame floor → no regression.
    base2 = frames(("a", 10.0), ("tiny", 0.4))
    cur2 = frames(("a", 10.0), ("tiny", 0.9))
    assert regressed_names(cur2, base2) == set(), "sub-1% jiggle must not regress"

    # 7. Threshold boundary: exactly +20% is NOT > 20% gate → no regression.
    cur = frames(("a", 12.0), ("b", 8.0), ("c", 5.0))
    assert regressed_names(cur, base) == set(), "exactly +20% must not trip (strict >)"

    # 8. Custom threshold: +30% trips a 25% gate but not a 40% gate.
    cur = frames(("a", 13.0), ("b", 8.0), ("c", 5.0))
    assert regressed_names(cur, base, threshold=0.25) == {"a"}
    assert regressed_names(cur, base, threshold=0.40) == set()

    # 9. Markdown renders + flags the regression row.
    md = render_markdown(diff(frames(("a", 13.0)), frames(("a", 10.0))), threshold=0.20, top=5)
    assert "regressed" in md and "`a`" in md and "+30%" in md, md

    # 10. Malformed input → ValueError (CI fails loudly, never silent-passes).
    import tempfile

    with tempfile.TemporaryDirectory() as d:
        bad = Path(d) / "bad.json"
        bad.write_text('{"frames": [{"name": "x"}]}')  # missing self_pct
        try:
            load_frames(str(bad))
            raise AssertionError("malformed frame must raise ValueError")
        except ValueError:
            pass
        notjson = Path(d) / "notjson.json"
        notjson.write_text("{ not json")
        try:
            load_frames(str(notjson))
            raise AssertionError("invalid JSON must raise ValueError")
        except ValueError:
            pass

    print("flamediff self-test: all 10 checks passed")
    return 0


def main(argv=None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if "--self-test" in argv:
        return _self_test()

    p = argparse.ArgumentParser(description="Top-frame CPU-profile regression gate.")
    p.add_argument("current", help="freshly-captured profile JSON (pyroscope top frames)")
    p.add_argument("baseline", help="checked-in .profiling-baseline.json")
    p.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD,
                   help="relative self-%% increase that fails the gate (default 0.20)")
    p.add_argument("--top", type=int, default=DEFAULT_TOP, help="top-N frames to compare (default 5)")
    p.add_argument("--min-base-pct", type=float, default=DEFAULT_MIN_BASE_PCT,
                   help="baseline self-%% floor below which the relative gate is skipped (default 1.0)")
    p.add_argument("--new-frame-floor-pct", type=float, default=DEFAULT_NEW_FRAME_FLOOR_PCT,
                   help="a new top-N frame at/above this self-%% is a regression (default 5.0)")
    p.add_argument("--output", help="write the markdown summary to FILE (also printed to stdout)")
    return run(p.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
