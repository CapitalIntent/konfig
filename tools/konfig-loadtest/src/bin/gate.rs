//! konfig-loadtest gate — p99 perf-regression gate vs a committed baseline.
//!
//! Caveman docs (short words): the loadtest writes a `results.json` with a
//! `p99_ms` (99th-percentile latency) per scenario. This tool reads that fresh
//! file and the most recent committed baseline (`baselines/<tag>.json`, same
//! shape), and for every scenario present in both it checks:
//!
//!     current p99  >  baseline p99 x threshold   ->  REGRESSION -> exit 1
//!
//! Default threshold 1.1 = "fail if latency got >10% worse". This is the
//! latency sibling of the flamediff CPU gate (CU-86ahtj1a8): flamediff catches
//! CPU-frame shifts; this catches end-to-end latency creep (e.g. an extra
//! allocation in Apply that flamediff misses but p99 feels).
//!
//! Baselines are per release tag. `newest` = highest semver filename (numeric,
//! so `v1.10.0` > `v1.9.0`). To move the baseline after an intentional perf
//! change, run with `--update <tag>` (the `PERF_BASELINE_BUMP` bypass): it
//! writes the current results as `baselines/<tag>.json` and exits 0.
//!
//! Exit: 0 = no regression / seeded / updated, 1 = a scenario regressed,
//! 2 = bad input (missing or malformed file).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;
use serde_json::Value;

const EXIT_OK: u8 = 0;
const EXIT_REGRESSION: u8 = 1;
const EXIT_INPUT: u8 = 2;

#[derive(Parser)]
#[command(
    name = "konfig-loadtest-gate",
    about = "p99 perf-regression gate vs committed per-tag baseline (CU-86aj08v98)"
)]
struct Cli {
    /// Fresh loadtest results JSON (the `--results-json` output).
    #[arg(long)]
    current: PathBuf,
    /// Directory of per-tag baselines (`<tag>.json`); newest by semver is used.
    #[arg(long, default_value = "infra/konfig-loadtest/baselines")]
    baselines_dir: PathBuf,
    /// Explicit baseline file (overrides newest-in-dir).
    #[arg(long)]
    baseline: Option<PathBuf>,
    /// Fail if any scenario's p99 exceeds baseline p99 x this factor.
    #[arg(long, default_value_t = 1.1)]
    threshold: f64,
    /// Write the current results as `baselines/<tag>.json` and exit 0
    /// (the PERF_BASELINE_BUMP bypass — moves the baseline forward).
    #[arg(long)]
    update: Option<String>,
    /// Write the markdown summary here (also printed to stdout).
    #[arg(long, short)]
    output: Option<PathBuf>,
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(code) => ExitCode::from(code),
        Err(msg) => {
            eprintln!("gate: {msg}");
            ExitCode::from(EXIT_INPUT)
        }
    }
}

fn run(cli: Cli) -> Result<u8, String> {
    // --update: copy current results (same schema) to baselines/<tag>.json.
    if let Some(tag) = cli.update.as_deref() {
        let dest = cli.baselines_dir.join(format!("{tag}.json"));
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
        }
        let body = std::fs::read_to_string(&cli.current)
            .map_err(|e| format!("read {}: {e}", cli.current.display()))?;
        std::fs::write(&dest, body).map_err(|e| format!("write {}: {e}", dest.display()))?;
        eprintln!("baseline updated: {}", dest.display());
        return Ok(EXIT_OK);
    }

    let current = load_p99(&cli.current)?;

    let baseline_path = match cli.baseline {
        Some(p) => Some(p),
        None => newest_baseline(&cli.baselines_dir)?,
    };
    let Some(baseline_path) = baseline_path else {
        let md = format!(
            "## Perf gate — no baseline\n\nNo baseline in `{}`. Seed one with `--update <tag>`; nothing to gate.\n",
            cli.baselines_dir.display()
        );
        emit(cli.output.as_deref(), &md)?;
        return Ok(EXIT_OK);
    };
    let baseline = load_p99(&baseline_path)?;

    let rows = diff(&current, &baseline, cli.threshold);
    let md = render_markdown(&rows, cli.threshold, &baseline_path);
    emit(cli.output.as_deref(), &md)?;
    Ok(if rows.iter().any(|r| r.regressed) {
        EXIT_REGRESSION
    } else {
        EXIT_OK
    })
}

/// One scenario's p99 comparison.
struct Row {
    name: String,
    base_p99: f64,
    cur_p99: f64,
    regressed: bool,
}

/// Load `{scenario_name -> p99_ms}` from a loadtest results/baseline JSON,
/// keeping only scenarios that carry latency metrics (reconnect/soak are null).
fn load_p99(path: &Path) -> Result<BTreeMap<String, f64>, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let raw: Value = serde_json::from_str(&text)
        .map_err(|e| format!("invalid JSON in {}: {e}", path.display()))?;
    let scenarios = raw
        .get("scenarios")
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{}: expected a 'scenarios' array", path.display()))?;
    let mut out = BTreeMap::new();
    for (i, sc) in scenarios.iter().enumerate() {
        let name = sc
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{}: scenario[{i}] missing 'name'", path.display()))?;
        // metrics is null for scenarios without per-event latency — skip those.
        if let Some(p99) = sc
            .get("metrics")
            .and_then(|m| m.get("p99_ms"))
            .and_then(Value::as_f64)
        {
            out.insert(name.to_string(), p99);
        }
    }
    Ok(out)
}

/// Newest baseline file by semver filename (numeric, so v1.10.0 > v1.9.0).
fn newest_baseline(dir: &Path) -> Result<Option<PathBuf>, String> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => return Ok(None), // no dir yet = no baseline (seed mode)
    };
    let mut best: Option<((u64, u64, u64), PathBuf)> = None;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let key = semver_key(stem);
        if best.as_ref().is_none_or(|(k, _)| key > *k) {
            best = Some((key, path));
        }
    }
    Ok(best.map(|(_, p)| p))
}

/// Parse `v1.10.3` / `1.10.3` -> (1, 10, 3); missing parts = 0, junk = 0.
fn semver_key(stem: &str) -> (u64, u64, u64) {
    let s = stem.strip_prefix('v').unwrap_or(stem);
    let mut it = s.split('.').map(|p| p.parse::<u64>().unwrap_or(0));
    (
        it.next().unwrap_or(0),
        it.next().unwrap_or(0),
        it.next().unwrap_or(0),
    )
}

/// Compare current vs baseline p99 over the scenarios present in both.
fn diff(
    current: &BTreeMap<String, f64>,
    baseline: &BTreeMap<String, f64>,
    threshold: f64,
) -> Vec<Row> {
    let mut rows: Vec<Row> = baseline
        .iter()
        .filter_map(|(name, &base_p99)| {
            current.get(name).map(|&cur_p99| Row {
                name: name.clone(),
                base_p99,
                cur_p99,
                regressed: cur_p99 > base_p99 * threshold,
            })
        })
        .collect();
    rows.sort_by(|a, b| a.name.cmp(&b.name));
    rows
}

fn render_markdown(rows: &[Row], threshold: f64, baseline_path: &Path) -> String {
    let pct = ((threshold - 1.0) * 100.0).round();
    let regressions = rows.iter().filter(|r| r.regressed).count();
    let mut out = String::from("## Perf gate — p99 latency regression\n\n");
    out.push_str(&format!("Baseline: `{}`\n\n", baseline_path.display()));
    if regressions > 0 {
        out.push_str(&format!(
            "❌ **{regressions} scenario(s) regressed** (p99 > baseline +{pct:.0}%).\n"
        ));
    } else {
        out.push_str(&format!(
            "✅ No scenario p99 exceeded baseline +{pct:.0}%.\n"
        ));
    }
    out.push_str(
        "\n| scenario | baseline p99 (ms) | current p99 (ms) | Δ | |\n|---|---|---|---|---|\n",
    );
    for r in rows {
        let delta = if r.base_p99 > 0.0 {
            format!("{:+.0}%", (r.cur_p99 / r.base_p99 - 1.0) * 100.0)
        } else {
            "—".to_string()
        };
        let flag = if r.regressed { "❌" } else { "" };
        out.push_str(&format!(
            "| `{}` | {} | {} | {delta} |{flag} |\n",
            r.name, r.base_p99 as u128, r.cur_p99 as u128
        ));
    }
    out
}

fn emit(output: Option<&Path>, md: &str) -> Result<(), String> {
    if let Some(path) = output {
        std::fs::write(path, md).map_err(|e| format!("write {}: {e}", path.display()))?;
    }
    print!("{md}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> PathBuf {
        std::env::var("TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir())
            .join(format!("perfgate_{}", std::process::id()))
    }

    const RESULTS: &str = r#"{"all_passed":true,"scenarios":[
        {"name":"subscribe_flood","pass":true,"metrics":{"samples":100,"p50_ms":10,"p95_ms":40,"p99_ms":80,"max_ms":120},"failures":[]},
        {"name":"get_flood","pass":true,"metrics":{"samples":100,"p50_ms":2,"p95_ms":8,"p99_ms":20,"max_ms":30},"failures":[]},
        {"name":"reconnect_storm","pass":true,"metrics":null,"failures":[]}
    ]}"#;

    #[test]
    fn load_p99_keeps_only_metric_bearing_scenarios() {
        let dir = tmp();
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("r.json");
        std::fs::write(&f, RESULTS).unwrap();
        let m = load_p99(&f).unwrap();
        assert_eq!(m.get("subscribe_flood"), Some(&80.0));
        assert_eq!(m.get("get_flood"), Some(&20.0));
        assert!(!m.contains_key("reconnect_storm")); // metrics null -> skipped
    }

    #[test]
    fn semver_newest_is_numeric_not_lexical() {
        assert!(semver_key("v1.10.0") > semver_key("v1.9.0"));
        assert!(semver_key("v2.0.0") > semver_key("v1.99.99"));
        assert_eq!(semver_key("1.2.3"), (1, 2, 3));
    }

    #[test]
    fn newest_baseline_picks_highest_semver() {
        let dir = tmp().join("bl");
        std::fs::create_dir_all(&dir).unwrap();
        for tag in ["v1.9.0", "v1.10.0", "v1.2.0"] {
            std::fs::write(dir.join(format!("{tag}.json")), RESULTS).unwrap();
        }
        let newest = newest_baseline(&dir).unwrap().unwrap();
        assert_eq!(newest.file_stem().unwrap().to_str().unwrap(), "v1.10.0");
    }

    #[test]
    fn diff_flags_only_over_threshold() {
        let base = BTreeMap::from([("a".to_string(), 100.0), ("b".to_string(), 50.0)]);
        // a +9% (within 10% gate), b +40% (regress). c absent from current.
        let cur = BTreeMap::from([("a".to_string(), 109.0), ("b".to_string(), 70.0)]);
        let rows = diff(&cur, &base, 1.1);
        let regressed: Vec<&str> = rows
            .iter()
            .filter(|r| r.regressed)
            .map(|r| r.name.as_str())
            .collect();
        assert_eq!(regressed, vec!["b"]);
    }

    #[test]
    fn diff_boundary_exactly_10pct_does_not_trip() {
        let base = BTreeMap::from([("a".to_string(), 100.0)]);
        let cur = BTreeMap::from([("a".to_string(), 110.0)]); // exactly x1.1, not > 
        assert!(!diff(&cur, &base, 1.1)[0].regressed);
    }

    #[test]
    fn missing_current_scenario_is_not_a_regression() {
        let base = BTreeMap::from([("a".to_string(), 100.0), ("gone".to_string(), 10.0)]);
        let cur = BTreeMap::from([("a".to_string(), 100.0)]);
        // only "a" compared; "gone" absent from current is skipped, not failed.
        let rows = diff(&cur, &base, 1.1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].name, "a");
    }

    #[test]
    fn markdown_flags_regression_row() {
        let base = BTreeMap::from([("subscribe_flood".to_string(), 80.0)]);
        let cur = BTreeMap::from([("subscribe_flood".to_string(), 120.0)]);
        let md = render_markdown(
            &diff(&cur, &base, 1.1),
            1.1,
            Path::new("baselines/v1.0.0.json"),
        );
        assert!(md.contains("regressed"), "{md}");
        assert!(md.contains("`subscribe_flood`"), "{md}");
        assert!(md.contains("+50%"), "{md}");
    }
}
