//! The regression gate: compare a fresh top-frames file vs a baseline.
//!
//! Caveman docs: we look at the hottest few functions in both files. A function
//! "regressed" (got worse) when its self-% climbed by more than the threshold
//! *relative* to the baseline — e.g. 10.0% -> 12.0% is +20%. Relative, not
//! absolute, because a 10-min capture wobbles ~3-5% from sampling noise; a 20%
//! relative gate clears that noise but still catches a real hot-path slowdown.
//!
//! Two guards keep it honest:
//! * `min_base_pct` — a baseline frame below this is tiny; 0.4% -> 0.9% is +125%
//!   relative but physically nothing, so tiny frames skip the relative gate.
//! * `new_frame_floor_pct` — a frame absent/tiny in the baseline that shows up
//!   this hot now is a regression (a brand-new hot path).
//!
//! Decreases are improvements and never fail the gate.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use serde_json::Value;

/// One comparison row. `rel_delta` is `None` when the relative gate was skipped
/// (tiny/new frame); `regressed` drives the exit code.
pub struct Row {
    pub name: String,
    pub base_pct: f64,
    pub cur_pct: f64,
    pub rel_delta: Option<f64>,
    pub regressed: bool,
    pub reason: String,
}

/// Load a `{name: self_pct}` map from a profile JSON. Accepts either
/// `{"frames":[{name,self_pct}]}` or a bare `[{name,self_pct}]` list. Fails
/// loudly on a malformed file so CI never silently passes the gate.
pub fn load_frames(path: &Path) -> Result<BTreeMap<String, f64>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|_| format!("profile file not found: {}", path.display()))?;
    let raw: Value = serde_json::from_str(&text)
        .map_err(|e| format!("invalid JSON in {}: {e}", path.display()))?;
    let frames = if raw.is_object() {
        raw.get("frames").cloned().unwrap_or(raw)
    } else {
        raw
    };
    let arr = frames
        .as_array()
        .ok_or_else(|| format!("{}: expected a 'frames' list", path.display()))?;

    let mut out = BTreeMap::new();
    for (i, fr) in arr.iter().enumerate() {
        let name = fr.get("name").and_then(Value::as_str);
        let self_pct = fr.get("self_pct");
        match (name, self_pct) {
            (Some(name), Some(v)) => {
                let pct = v.as_f64().ok_or_else(|| {
                    format!("{}: frame[{i}].self_pct not a number", path.display())
                })?;
                out.insert(name.to_string(), pct);
            }
            _ => {
                return Err(format!(
                    "{}: frame[{i}] must have 'name' and 'self_pct'",
                    path.display()
                ));
            }
        }
    }
    Ok(out)
}

/// Names of the `top` hottest frames (highest self-% first, ties by name).
fn top_names(frames: &BTreeMap<String, f64>, top: usize) -> Vec<String> {
    let mut v: Vec<(&String, &f64)> = frames.iter().collect();
    v.sort_by(|a, b| {
        b.1.partial_cmp(a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(b.0))
    });
    v.into_iter().take(top).map(|(n, _)| n.clone()).collect()
}

/// Per-frame deltas over the union of each side's top-N frames, hottest first.
pub fn diff(
    current: &BTreeMap<String, f64>,
    baseline: &BTreeMap<String, f64>,
    threshold: f64,
    top: usize,
    min_base_pct: f64,
    new_frame_floor_pct: f64,
) -> Vec<Row> {
    let mut names: BTreeSet<String> = BTreeSet::new();
    names.extend(top_names(baseline, top));
    names.extend(top_names(current, top));

    let mut rows: Vec<Row> = names
        .into_iter()
        .map(|name| {
            let base = baseline.get(&name).copied().unwrap_or(0.0);
            let cur = current.get(&name).copied().unwrap_or(0.0);
            let mut regressed = false;
            let mut reason = String::new();
            let mut rel_delta = None;
            if base >= min_base_pct {
                let rel = (cur - base) / base;
                rel_delta = Some(rel);
                if rel > threshold {
                    regressed = true;
                    reason = format!(
                        "self% +{:.0}% (> {:.0}% gate)",
                        rel * 100.0,
                        threshold * 100.0
                    );
                }
            } else if cur >= new_frame_floor_pct {
                regressed = true;
                reason = format!("new hot frame {cur:.1}% (>= {new_frame_floor_pct:.0}% floor)");
            }
            Row {
                name,
                base_pct: base,
                cur_pct: cur,
                rel_delta,
                regressed,
                reason,
            }
        })
        .collect();
    rows.sort_by(|a, b| {
        b.cur_pct
            .partial_cmp(&a.cur_pct)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    rows
}

/// Render the rows as a markdown table (suitable for `$GITHUB_STEP_SUMMARY`).
pub fn render_markdown(rows: &[Row], threshold: f64, top: usize) -> String {
    let regressions = rows.iter().filter(|r| r.regressed).count();
    let mut out = String::from("## Flamediff — top-frame CPU regression gate\n\n");
    if regressions > 0 {
        out.push_str(&format!(
            "❌ **{regressions} frame(s) regressed** (relative self-% gate: {:.0}%, top-{top}).\n",
            threshold * 100.0
        ));
    } else {
        out.push_str(&format!(
            "✅ No top-{top} frame regressed beyond the {:.0}% relative self-% gate.\n",
            threshold * 100.0
        ));
    }
    out.push_str("\n| frame | baseline self% | current self% | Δ | |\n|---|---|---|---|---|\n");
    for r in rows {
        let delta = match r.rel_delta {
            None if r.cur_pct > 0.0 => "new".to_string(),
            None => "—".to_string(),
            Some(d) => format!("{:+.0}%", d * 100.0),
        };
        let flag = if r.regressed { "❌" } else { "" };
        let note = if r.reason.is_empty() {
            String::new()
        } else {
            format!(" {}", r.reason)
        };
        out.push_str(&format!(
            "| `{}` | {:.1}% | {:.1}% | {delta} |{flag}{note} |\n",
            r.name, r.base_pct, r.cur_pct
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frames(pairs: &[(&str, f64)]) -> BTreeMap<String, f64> {
        pairs.iter().map(|(n, p)| (n.to_string(), *p)).collect()
    }

    fn regressed_names(
        cur: &BTreeMap<String, f64>,
        base: &BTreeMap<String, f64>,
    ) -> BTreeSet<String> {
        diff(cur, base, 0.20, 5, 1.0, 5.0)
            .into_iter()
            .filter(|r| r.regressed)
            .map(|r| r.name)
            .collect()
    }

    fn tmp() -> std::path::PathBuf {
        std::env::var("TEST_TMPDIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir())
    }

    #[test]
    fn identical_profiles_no_regression() {
        let base = frames(&[("a", 10.0), ("b", 8.0), ("c", 5.0)]);
        assert!(regressed_names(&base, &base).is_empty());
    }

    #[test]
    fn hot_frame_up_30pct_regresses() {
        let base = frames(&[("a", 10.0), ("b", 8.0), ("c", 5.0)]);
        let cur = frames(&[("a", 13.0), ("b", 8.0), ("c", 5.0)]);
        assert_eq!(
            regressed_names(&cur, &base),
            BTreeSet::from(["a".to_string()])
        );
    }

    #[test]
    fn decrease_is_never_a_regression() {
        let base = frames(&[("a", 10.0), ("b", 8.0), ("c", 5.0)]);
        let cur = frames(&[("a", 4.0), ("b", 8.0), ("c", 5.0)]);
        assert!(regressed_names(&cur, &base).is_empty());
    }

    #[test]
    fn within_noise_band_no_regression() {
        let base = frames(&[("a", 10.0), ("b", 8.0), ("c", 5.0)]);
        let cur = frames(&[("a", 11.0), ("b", 8.0), ("c", 5.0)]);
        assert!(regressed_names(&cur, &base).is_empty());
    }

    #[test]
    fn new_hot_frame_regresses() {
        let base = frames(&[("a", 10.0), ("b", 8.0), ("c", 5.0)]);
        let cur = frames(&[("a", 10.0), ("b", 8.0), ("d", 15.0)]);
        assert!(regressed_names(&cur, &base).contains("d"));
    }

    #[test]
    fn sub_one_pct_jiggle_exempt() {
        let base = frames(&[("a", 10.0), ("tiny", 0.4)]);
        let cur = frames(&[("a", 10.0), ("tiny", 0.9)]);
        assert!(regressed_names(&cur, &base).is_empty());
    }

    #[test]
    fn exactly_20pct_does_not_trip_strict_gt() {
        let base = frames(&[("a", 10.0), ("b", 8.0), ("c", 5.0)]);
        let cur = frames(&[("a", 12.0), ("b", 8.0), ("c", 5.0)]);
        assert!(regressed_names(&cur, &base).is_empty());
    }

    #[test]
    fn custom_threshold_boundaries() {
        let base = frames(&[("a", 10.0), ("b", 8.0), ("c", 5.0)]);
        let cur = frames(&[("a", 13.0), ("b", 8.0), ("c", 5.0)]);
        let at_25: BTreeSet<String> = diff(&cur, &base, 0.25, 5, 1.0, 5.0)
            .into_iter()
            .filter(|r| r.regressed)
            .map(|r| r.name)
            .collect();
        assert_eq!(at_25, BTreeSet::from(["a".to_string()]));
        let at_40 = diff(&cur, &base, 0.40, 5, 1.0, 5.0)
            .into_iter()
            .filter(|r| r.regressed)
            .count();
        assert_eq!(at_40, 0);
    }

    #[test]
    fn markdown_flags_the_regression_row() {
        let md = render_markdown(
            &diff(
                &frames(&[("a", 13.0)]),
                &frames(&[("a", 10.0)]),
                0.20,
                5,
                1.0,
                5.0,
            ),
            0.20,
            5,
        );
        assert!(md.contains("regressed"), "{md}");
        assert!(md.contains("`a`"), "{md}");
        assert!(md.contains("+30%"), "{md}");
    }

    #[test]
    fn malformed_input_errors_loudly() {
        let dir = tmp().join(format!("flamediff_gate_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let missing_field = dir.join("bad.json");
        std::fs::write(&missing_field, r#"{"frames":[{"name":"x"}]}"#).unwrap();
        assert!(load_frames(&missing_field).is_err());

        let not_json = dir.join("nj.json");
        std::fs::write(&not_json, "{ not json").unwrap();
        assert!(load_frames(&not_json).is_err());
    }
}
