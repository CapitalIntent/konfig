//! Flamebearer parsing + two outputs: top-frames JSON and pprof.
//!
//! Caveman docs: a flamebearer packs a flame graph as flat number rows. Each
//! row (a "level") is groups of 4 ints: `[x_offset, total, self, name_index]`.
//! Reading left to right, an `x` cursor adds `x_offset` then `total`, so each
//! bar covers `[x, x+total)`. A bar's parent is the bar one level up whose
//! range swallows it. `self` = CPU spent in that bar's own function (not its
//! children). We sum `self` per function to rank the hottest frames.

use std::collections::HashMap;
use std::path::Path;

use serde_json::{Value, json};

/// A decoded flamebearer: names + raw levels + total ticks + sampling meta.
pub struct Flamebearer {
    names: Vec<String>,
    levels: Vec<Vec<i64>>,
    num_ticks: i64,
    sample_rate: i64,
    units: String,
}

/// One decoded bar. `parent` is the index of the owning bar in the level above
/// (`None` at the root level), used to walk a stack for pprof.
struct Bar {
    x0: i64,
    x1: i64,
    self_: i64,
    name_idx: usize,
    parent: Option<usize>,
}

/// Read + parse a flamebearer file. Accepts either `{"flamebearer": {...}}`
/// (full pyroscope render envelope) or a bare `{names, levels, numTicks}`.
pub fn load(path: &Path) -> Result<Flamebearer, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let raw: Value = serde_json::from_str(&text)
        .map_err(|e| format!("invalid JSON in {}: {e}", path.display()))?;
    let fb = raw.get("flamebearer").unwrap_or(&raw);

    let names = fb
        .get("names")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{}: missing flamebearer.names array", path.display()))?
        .iter()
        .map(|v| v.as_str().unwrap_or_default().to_string())
        .collect();

    let levels = fb
        .get("levels")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{}: missing flamebearer.levels array", path.display()))?
        .iter()
        .map(|lvl| {
            lvl.as_array()
                .map(|row| row.iter().map(|n| n.as_i64().unwrap_or(0)).collect())
                .ok_or_else(|| format!("{}: a level is not an array", path.display()))
        })
        .collect::<Result<Vec<Vec<i64>>, String>>()?;

    let num_ticks = fb.get("numTicks").and_then(Value::as_i64).unwrap_or(0);
    let meta = raw.get("metadata");
    let sample_rate = meta
        .and_then(|m| m.get("sampleRate"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let units = meta
        .and_then(|m| m.get("units"))
        .and_then(Value::as_str)
        .unwrap_or("samples")
        .to_lowercase();

    Ok(Flamebearer {
        names,
        levels,
        num_ticks,
        sample_rate,
        units,
    })
}

impl Flamebearer {
    /// Decode the flat levels into bars, linking each bar to its parent by
    /// x-range containment. Both a level and its parent are sorted by `x0`, so
    /// one forward-only cursor (`pi`) finds parents in a single pass.
    fn decode_levels(&self) -> Vec<Vec<Bar>> {
        let mut per_depth: Vec<Vec<Bar>> = Vec::with_capacity(self.levels.len());
        for lvl in &self.levels {
            let mut bars = Vec::new();
            let mut x = 0i64;
            let mut j = 0;
            while j + 3 < lvl.len() {
                x += lvl[j];
                let total = lvl[j + 1];
                bars.push(Bar {
                    x0: x,
                    x1: x + total,
                    self_: lvl[j + 2],
                    name_idx: lvl[j + 3] as usize,
                    parent: None,
                });
                x += total;
                j += 4;
            }
            per_depth.push(bars);
        }
        for depth in 1..per_depth.len() {
            let (prev, cur) = per_depth.split_at_mut(depth);
            let parents = &prev[depth - 1];
            let mut pi = 0usize;
            for bar in cur[0].iter_mut() {
                while pi < parents.len() && parents[pi].x1 <= bar.x0 {
                    pi += 1;
                }
                if pi < parents.len() && parents[pi].x0 <= bar.x0 && bar.x1 <= parents[pi].x1 {
                    bar.parent = Some(pi);
                }
            }
        }
        per_depth
    }

    /// The N hottest frames by self-%, highest first, dropping zero-self frames.
    /// `self_pct` is rounded to 4 decimals (matches the old python tool).
    pub fn top_frames(&self, n: usize) -> Vec<(String, f64)> {
        let per_depth = self.decode_levels();
        let mut by_name: HashMap<&str, i64> = HashMap::new();
        for depth in &per_depth {
            for bar in depth {
                *by_name
                    .entry(self.names[bar.name_idx].as_str())
                    .or_insert(0) += bar.self_;
            }
        }
        let total = if self.num_ticks > 0 {
            self.num_ticks
        } else {
            let sum: i64 = by_name.values().sum();
            if sum > 0 { sum } else { 1 }
        };
        let mut rows: Vec<(&str, i64)> = by_name.into_iter().collect();
        // Deterministic: hottest first, ties broken by name so output is stable.
        rows.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
        rows.into_iter()
            .filter(|(_, s)| *s > 0)
            .take(n)
            .map(|(name, s)| (name.to_string(), round4(s as f64 / total as f64 * 100.0)))
            .collect()
    }

    /// Rebuild the call tree as a pprof profile (uncompressed protobuf bytes).
    /// `go tool pprof` reads uncompressed profiles fine; pipe through `gzip` if
    /// a compressed `.pprof` is wanted. Hand-rolled protobuf so this stays
    /// dependency-free (no prost/flate2 pulled into a tiny CLI).
    pub fn to_pprof(&self) -> Vec<u8> {
        let per_depth = self.decode_levels();

        // CPU vs sample-count value/period, mirroring pyroscope's own choice.
        let (st_type, st_unit, scale, period): (&str, &str, i64, u64) =
            if self.units == "samples" && self.sample_rate > 0 {
                let ns = (1e9 / self.sample_rate as f64).round() as i64;
                ("cpu", "nanoseconds", ns, ns as u64)
            } else {
                ("samples", "count", 1, 1)
            };

        let mut strings = Interner::new();
        // One Function + Location per unique frame name.
        let mut name_loc: HashMap<&str, u64> = HashMap::new();
        let mut functions: Vec<u8> = Vec::new();
        let mut locations: Vec<u8> = Vec::new();
        for depth in &per_depth {
            for bar in depth {
                let name = self.names[bar.name_idx].as_str();
                if name_loc.contains_key(name) {
                    continue;
                }
                let fid = name_loc.len() as u64 + 1;
                let lid = fid; // one location per function, same running count
                let name_idx = strings.get(name);
                let mut func = Vec::new();
                vfield(1, fid, &mut func); // Function.id
                vfield(2, name_idx, &mut func); // Function.name
                vfield(4, name_idx, &mut func); // Function.filename (reuse name)
                lfield(5, &func, &mut functions); // Profile.function
                let mut line = Vec::new();
                vfield(1, fid, &mut line); // Line.function_id
                let mut loc = Vec::new();
                vfield(1, lid, &mut loc); // Location.id
                lfield(4, &line, &mut loc); // Location.line
                lfield(4, &loc, &mut locations); // Profile.location
                name_loc.insert(name, lid);
            }
        }

        // One Sample per bar that burned self-CPU; stack = leaf..root location ids.
        let mut samples: Vec<u8> = Vec::new();
        for d in 0..per_depth.len() {
            for i in 0..per_depth[d].len() {
                if per_depth[d][i].self_ <= 0 {
                    continue;
                }
                let mut stack: Vec<u64> = Vec::new();
                let (mut cd, mut ci) = (d, i);
                loop {
                    let bar = &per_depth[cd][ci];
                    stack.push(name_loc[self.names[bar.name_idx].as_str()]);
                    match bar.parent {
                        Some(pi) => {
                            cd -= 1;
                            ci = pi;
                        }
                        None => break,
                    }
                }
                let mut sample = Vec::new();
                packed(1, &stack, &mut sample); // Sample.location_id
                packed(2, &[(per_depth[d][i].self_ * scale) as u64], &mut sample); // Sample.value
                lfield(2, &sample, &mut samples); // Profile.sample
            }
        }

        let mut body = Vec::new();
        let mut value_type = Vec::new();
        vfield(1, strings.get(st_type), &mut value_type);
        vfield(2, strings.get(st_unit), &mut value_type);
        lfield(1, &value_type, &mut body); // Profile.sample_type
        body.extend_from_slice(&samples);
        body.extend_from_slice(&functions);
        body.extend_from_slice(&locations);
        let mut period_type = Vec::new();
        vfield(1, strings.get(st_type), &mut period_type);
        vfield(2, strings.get(st_unit), &mut period_type);
        lfield(11, &period_type, &mut body); // Profile.period_type
        vfield(12, period, &mut body); // Profile.period
        for s in &strings.strings {
            lfield(6, s.as_bytes(), &mut body); // Profile.string_table (index 0 = "")
        }
        body
    }
}

/// Serialize the top-frames list as `{"frames":[{name,self_pct}]}` (pretty).
pub fn frames_to_json(frames: &[(String, f64)]) -> String {
    let arr: Vec<Value> = frames
        .iter()
        .map(|(name, pct)| json!({"name": name, "self_pct": pct}))
        .collect();
    serde_json::to_string_pretty(&json!({"frames": arr})).expect("frames json is serializable")
}

fn round4(x: f64) -> f64 {
    (x * 10_000.0).round() / 10_000.0
}

/// Append-only string table for pprof (index 0 is always the empty string).
struct Interner {
    strings: Vec<String>,
    idx: HashMap<String, u64>,
}

impl Interner {
    fn new() -> Self {
        Self {
            strings: vec![String::new()],
            idx: HashMap::from([(String::new(), 0)]),
        }
    }

    fn get(&mut self, s: &str) -> u64 {
        if let Some(&i) = self.idx.get(s) {
            return i;
        }
        let i = self.strings.len() as u64;
        self.idx.insert(s.to_string(), i);
        self.strings.push(s.to_string());
        i
    }
}

// ── minimal protobuf wire encoders ──────────────────────────────────────────
fn varint(mut n: u64, out: &mut Vec<u8>) {
    loop {
        let b = (n & 0x7f) as u8;
        n >>= 7;
        if n != 0 {
            out.push(b | 0x80);
        } else {
            out.push(b);
            break;
        }
    }
}

fn tag(field: u64, wire: u64, out: &mut Vec<u8>) {
    varint((field << 3) | wire, out);
}

fn vfield(field: u64, val: u64, out: &mut Vec<u8>) {
    tag(field, 0, out);
    varint(val, out);
}

fn lfield(field: u64, data: &[u8], out: &mut Vec<u8>) {
    tag(field, 2, out);
    varint(data.len() as u64, out);
    out.extend_from_slice(data);
}

fn packed(field: u64, vals: &[u64], out: &mut Vec<u8>) {
    let mut body = Vec::new();
    for &v in vals {
        varint(v, &mut body);
    }
    lfield(field, &body, out);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_fb() -> Flamebearer {
        Flamebearer {
            names: ["total", "main", "compute", "render"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
            levels: vec![
                vec![0, 100, 0, 0],
                vec![0, 100, 0, 1],
                vec![0, 60, 60, 2, 0, 40, 40, 3],
            ],
            num_ticks: 100,
            sample_rate: 100,
            units: "samples".into(),
        }
    }

    #[test]
    fn decode_links_parents() {
        let fb = sample_fb();
        let pd = fb.decode_levels();
        assert_eq!(fb.names[pd[2][0].name_idx], "compute");
        assert_eq!(pd[2][0].self_, 60);
        assert_eq!(pd[2][1].x0, 60);
        let p = pd[2][1].parent.expect("second depth-2 bar has a parent");
        assert_eq!(fb.names[pd[1][p].name_idx], "main");
    }

    #[test]
    fn top_frames_hottest_first_and_drops_zero() {
        let fb = sample_fb();
        let tf = fb.top_frames(5);
        assert_eq!(tf[0], ("compute".to_string(), 60.0));
        let names: Vec<&str> = tf.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["compute", "render"]); // main/total have self 0
    }

    #[test]
    fn top_frames_uses_num_ticks_as_total() {
        let fb = sample_fb();
        let tf = fb.top_frames(5);
        // render self 40 / 100 ticks = 40.0%
        assert_eq!(tf[1], ("render".to_string(), 40.0));
    }

    #[test]
    fn pprof_is_nonempty_bytes() {
        let fb = sample_fb();
        let bytes = fb.to_pprof();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn json_shape_round_trips() {
        let json = frames_to_json(&[("a".to_string(), 12.0)]);
        let v: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["frames"][0]["name"], "a");
        assert_eq!(v["frames"][0]["self_pct"], 12.0);
    }
}
