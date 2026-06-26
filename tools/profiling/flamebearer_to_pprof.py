#!/usr/bin/env python3
"""flamebearer_to_pprof.py — Pyroscope flamebearer JSON -> pprof (or top-frames).

The konfig bench captures CPU profiles as Pyroscope *flamebearer* JSON
(`cpu-profile.flamebearer.json`), which `go tool pprof` cannot parse. This
converter reconstructs the call tree from the flamebearer and emits a
gzipped pprof protobuf so `pprof -top`/`-list`/`-http` work, OR emits the
flamediff top-frames summary JSON. Pure stdlib (no protobuf dep).
"""
import argparse
import gzip
import json
import sys
from pathlib import Path

# ---- minimal protobuf wire encoder -----------------------------------------
def _varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)

def _tag(field, wire):
    return _varint((field << 3) | wire)

def _vfield(field, val):           # varint field
    return _tag(field, 0) + _varint(val)

def _lfield(field, data):          # length-delimited field
    return _tag(field, 2) + _varint(len(data)) + data

def _packed(field, vals):          # packed repeated varint
    body = b"".join(_varint(v) for v in vals)
    return _lfield(field, body)

# ---- flamebearer decode -----------------------------------------------------
def _decode_levels(names, levels):
    """Return nodes: list of dicts {depth, x0, x1, self, name} with parent idx.

    Flamebearer level encoding: each bar is 4 ints [x_off, total, self, name].
    Absolute x within a level accumulates: x += x_off; bar=[x, x+total); x+=total.
    A bar's parent is the depth-1 bar whose x-range contains it.
    """
    per_depth = []
    for depth, lvl in enumerate(levels):
        bars = []
        x = 0
        for j in range(0, len(lvl), 4):
            x += lvl[j]
            total, slf, name = lvl[j + 1], lvl[j + 2], lvl[j + 3]
            bars.append({"depth": depth, "x0": x, "x1": x + total,
                         "self": slf, "name": names[name], "parent": None})
            x += total
        per_depth.append(bars)
    # link parents by x-range containment (both sides sorted by x0)
    for depth in range(1, len(per_depth)):
        parents = per_depth[depth - 1]
        pi = 0
        for bar in per_depth[depth]:
            while pi < len(parents) and parents[pi]["x1"] <= bar["x0"]:
                pi += 1
            if (
                pi < len(parents)
                and parents[pi]["x0"] <= bar["x0"]
                and bar["x1"] <= parents[pi]["x1"]
            ):
                bar["parent"] = parents[pi]
    return per_depth

def _load(path):
    raw = json.loads(Path(path).read_text())
    fb = raw.get("flamebearer", raw)
    meta = raw.get("metadata", {})
    return fb["names"], fb["levels"], int(fb.get("numTicks", 0)), meta

# ---- pprof emit -------------------------------------------------------------
def to_pprof(names, levels, num_ticks, meta):
    per_depth = _decode_levels(names, levels)

    strings = [""]
    s_idx = {"": 0}
    def intern(s):
        i = s_idx.get(s)
        if i is None:
            i = len(strings)
            s_idx[s] = i
            strings.append(s)
        return i

    # one function + one location per unique frame name
    fn_id, loc_id = {}, {}
    functions, locations = [], []
    def loc_for(name):
        lid = loc_id.get(name)
        if lid is not None:
            return lid
        fid = len(functions) + 1
        functions.append(_lfield(5,  # Function
            _vfield(1, fid) + _vfield(2, intern(name)) + _vfield(4, intern(name))))
        fn_id[name] = fid
        lid = len(locations) + 1
        locations.append(_lfield(4,  # Location
            _vfield(1, lid) + _lfield(4, _vfield(1, fid))))  # Line{function_id}
        loc_id[name] = lid
        return lid

    rate = int(meta.get("sampleRate", 0) or 0)
    units = (meta.get("units") or "samples").lower()
    if units == "samples" and rate > 0:
        st_type, st_unit, scale = "cpu", "nanoseconds", round(1e9 / rate)
        period = round(1e9 / rate)
    else:
        st_type, st_unit, scale = "samples", "count", 1
        period = 1

    samples = []
    for depth in per_depth:
        for bar in depth:
            if bar["self"] <= 0:
                continue
            stack, node = [], bar
            while node is not None:
                stack.append(loc_for(node["name"]))
                node = node["parent"]
            samples.append(_lfield(2,  # Sample
                _packed(1, stack) + _packed(2, [bar["self"] * scale])))

    vt = _lfield(11, _vfield(1, intern(st_type)) + _vfield(2, intern(st_unit)))  # period_type
    body = b"".join(
        [_lfield(1, _vfield(1, intern(st_type)) + _vfield(2, intern(st_unit)))]  # sample_type
        + samples + functions + locations
    )
    body += vt + _vfield(12, period)
    body += b"".join(_lfield(6, s.encode()) for s in strings)  # string_table
    return gzip.compress(body)

def to_top_frames(names, levels, num_ticks, n):
    per_depth = _decode_levels(names, levels)
    by_name = {}
    for depth in per_depth:
        for bar in depth:
            by_name[bar["name"]] = by_name.get(bar["name"], 0) + bar["self"]
    total = num_ticks or sum(by_name.values()) or 1
    rows = sorted(by_name.items(), key=lambda kv: kv[1], reverse=True)
    frames = [{"name": nm, "self_pct": round(slf / total * 100, 4)}
              for nm, slf in rows[:n] if slf > 0]
    return {"frames": frames}

def main(argv=None):
    ap = argparse.ArgumentParser(description="Pyroscope flamebearer -> pprof / top-frames.")
    ap.add_argument("input", nargs="?")
    ap.add_argument("--out", "-o")
    ap.add_argument("--top-frames", type=int, metavar="N",
                    help="emit flamediff {frames:[...]} summary JSON instead of pprof")
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args(argv)
    if a.self_test:
        return _self_test()
    if not a.input:
        ap.error("input flamebearer JSON required")
    names, levels, num_ticks, meta = _load(a.input)
    if a.top_frames:
        out = json.dumps(to_top_frames(names, levels, num_ticks, a.top_frames), indent=2)
        (Path(a.out).write_text(out + "\n") if a.out else print(out))
    else:
        blob = to_pprof(names, levels, num_ticks, meta)
        if not a.out:
            ap.error("--out required for pprof output (binary)")
        Path(a.out).write_bytes(blob)
        print(f"wrote {a.out} ({len(blob)} bytes gzipped pprof)", file=sys.stderr)
    return 0

def _self_test():
    names = ["total", "main", "compute", "render"]
    levels = [
        [0, 100, 0, 0],
        [0, 100, 0, 1],
        [0, 60, 60, 2, 0, 40, 40, 3],
    ]
    meta = {"sampleRate": 100, "units": "samples"}
    pd = _decode_levels(names, levels)
    assert pd[2][0]["name"] == "compute" and pd[2][0]["self"] == 60
    assert pd[2][1]["x0"] == 60 and pd[2][1]["parent"]["name"] == "main"
    tf = to_top_frames(names, levels, 100, 5)["frames"]
    assert tf[0] == {"name": "compute", "self_pct": 60.0}, tf
    blob = to_pprof(names, levels, 100, meta)
    assert blob[:2] == b"\x1f\x8b", "not gzip"
    raw = gzip.decompress(blob)
    assert len(raw) > 0
    print("flamebearer_to_pprof self-test: all checks passed")
    return 0

if __name__ == "__main__":
    sys.exit(main())
