//! konfig-flamediff — CPU-profile regression gate + pyroscope flamebearer tools.
//!
//! Caveman docs (short words, for beginners):
//!
//! A "flamebearer" is the JSON shape pyroscope's `render?format=json` API spits
//! out. It describe a flame graph: which function ate how much CPU. This tool
//! read that JSON and do three jobs, picked by sub-command:
//!
//! * `top-frames` — find the N hottest functions (by *self* CPU %) and write a
//!   tiny `{ "frames": [...] }` JSON. Small enough to check into git.
//! * `to-pprof`  — rebuild the call tree and write a `pprof` profile so
//!   `go tool pprof` can draw flame graphs / list hot lines. (Uncompressed
//!   protobuf; `go tool pprof` reads it fine. Pipe through `gzip` if you want.)
//! * `gate`      — compare a fresh `top-frames` file against a checked-in
//!   baseline. If a hot function got hotter past a threshold, EXIT 1 so CI
//!   fails the PR. This is the regression gate.
//!
//! This is the Rust port of the old `tools/profiling/flamediff.py` +
//! `flamebearer_to_pprof.py` — same math, but now a real Bazel target with
//! `bazel test` unit tests instead of a bespoke `--self-test`.
//!
//! Exit codes: 0 = ok / no regression, 1 = a frame regressed (gate only),
//! 2 = bad input (missing file, junk JSON, wrong shape).

use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

mod flamebearer;
mod gate;

/// One failure code shared by every sub-command so CI sees a clean 0/1/2.
const EXIT_OK: u8 = 0;
const EXIT_REGRESSION: u8 = 1;
const EXIT_INPUT: u8 = 2;

#[derive(Parser)]
#[command(
    name = "konfig-flamediff",
    about = "Pyroscope flamebearer tools + top-frame CPU regression gate (CU-86ahtj1a8)"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Flamebearer JSON -> {"frames":[{name,self_pct}]} of the N hottest frames.
    TopFrames {
        /// Pyroscope flamebearer JSON (from `render?format=json`).
        input: PathBuf,
        /// How many hot frames to keep.
        #[arg(long, default_value_t = 5)]
        top: usize,
        /// Drop frames whose name matches this regex before the top-N pick.
        /// E.g. `^[^:]*$` keeps only namespaced Rust frames (drops kernel/libc).
        #[arg(long)]
        exclude: Option<String>,
        /// Write here instead of stdout.
        #[arg(long, short)]
        output: Option<PathBuf>,
    },
    /// Flamebearer JSON -> pprof profile (uncompressed protobuf).
    ToPprof {
        /// Pyroscope flamebearer JSON.
        input: PathBuf,
        /// Where to write the pprof bytes.
        #[arg(long, short)]
        output: PathBuf,
    },
    /// Compare a fresh top-frames file vs a baseline; EXIT 1 on regression.
    Gate {
        /// Freshly-captured top-frames JSON.
        current: PathBuf,
        /// Checked-in `.profiling-baseline.json`.
        baseline: PathBuf,
        /// Relative self-% increase that fails the gate (0.20 = +20%).
        #[arg(long, default_value_t = 0.20)]
        threshold: f64,
        /// Compare the top-N frames of each side.
        #[arg(long, default_value_t = 5)]
        top: usize,
        /// Below this baseline self-%, skip the relative gate (sampling noise).
        #[arg(long = "min-base-pct", default_value_t = 1.0)]
        min_base_pct: f64,
        /// A brand-new top-N frame at/above this self-% counts as a regression.
        #[arg(long = "new-frame-floor-pct", default_value_t = 5.0)]
        new_frame_floor_pct: f64,
        /// Write the markdown summary here (also printed to stdout).
        #[arg(long, short)]
        output: Option<PathBuf>,
    },
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(code) => ExitCode::from(code),
        Err(msg) => {
            eprintln!("flamediff: {msg}");
            ExitCode::from(EXIT_INPUT)
        }
    }
}

/// Thin dispatch — the real work lives in the small `cmd_*` helpers so each
/// stays simple + unit-testable (the CLI glue is not where bugs hide).
fn run(cli: Cli) -> Result<u8, String> {
    match cli.cmd {
        Cmd::TopFrames {
            input,
            top,
            exclude,
            output,
        } => cmd_top_frames(&input, top, exclude.as_deref(), output.as_deref()),
        Cmd::ToPprof { input, output } => cmd_to_pprof(&input, &output),
        Cmd::Gate {
            current,
            baseline,
            threshold,
            top,
            min_base_pct,
            new_frame_floor_pct,
            output,
        } => cmd_gate(
            &current,
            &baseline,
            threshold,
            top,
            min_base_pct,
            new_frame_floor_pct,
            output.as_deref(),
        ),
    }
}

fn cmd_top_frames(
    input: &Path,
    top: usize,
    exclude: Option<&str>,
    output: Option<&Path>,
) -> Result<u8, String> {
    let re = match exclude {
        Some(pat) => Some(regex::Regex::new(pat).map_err(|e| format!("bad --exclude regex: {e}"))?),
        None => None,
    };
    let fb = flamebearer::load(input)?;
    let json = flamebearer::frames_to_json(&fb.top_frames(top, re.as_ref()));
    write_or_print(output, &json)?;
    Ok(EXIT_OK)
}

fn cmd_to_pprof(input: &Path, output: &Path) -> Result<u8, String> {
    let bytes = flamebearer::load(input)?.to_pprof();
    std::fs::write(output, &bytes).map_err(|e| format!("write {}: {e}", output.display()))?;
    eprintln!("wrote {} ({} bytes pprof)", output.display(), bytes.len());
    Ok(EXIT_OK)
}

fn cmd_gate(
    current: &Path,
    baseline: &Path,
    threshold: f64,
    top: usize,
    min_base_pct: f64,
    new_frame_floor_pct: f64,
    output: Option<&Path>,
) -> Result<u8, String> {
    let cur = gate::load_frames(current)?;
    let base = gate::load_frames(baseline)?;
    let rows = gate::diff(
        &cur,
        &base,
        threshold,
        top,
        min_base_pct,
        new_frame_floor_pct,
    );
    let md = gate::render_markdown(&rows, threshold, top);
    if let Some(path) = output {
        std::fs::write(path, &md).map_err(|e| format!("write {}: {e}", path.display()))?;
    }
    print!("{md}");
    Ok(if rows.iter().any(|r| r.regressed) {
        EXIT_REGRESSION
    } else {
        EXIT_OK
    })
}

fn write_or_print(output: Option<&Path>, contents: &str) -> Result<(), String> {
    match output {
        Some(path) => {
            std::fs::write(path, contents).map_err(|e| format!("write {}: {e}", path.display()))
        }
        None => {
            println!("{contents}");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FB: &str = r#"{"flamebearer":{"names":["total","a","b"],"levels":[[0,100,0,0],[0,60,60,1,0,40,40,2]],"numTicks":100},"metadata":{"sampleRate":100,"units":"samples"}}"#;

    fn tmp() -> PathBuf {
        std::env::var("TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir())
            .join(format!("flamediff_main_{}", std::process::id()))
    }

    #[test]
    fn handlers_cover_happy_and_regression_paths() {
        let dir = tmp();
        std::fs::create_dir_all(&dir).unwrap();
        let fb = dir.join("fb.json");
        std::fs::write(&fb, FB).unwrap();

        // top-frames to a file and to stdout (both write_or_print arms).
        let cur = dir.join("cur.json");
        assert_eq!(cmd_top_frames(&fb, 5, None, Some(&cur)).unwrap(), EXIT_OK);
        assert_eq!(cmd_top_frames(&fb, 3, None, None).unwrap(), EXIT_OK);
        // a bad --exclude regex is a usage error (surfaced as exit 2).
        assert!(cmd_top_frames(&fb, 5, Some("("), None).is_err());

        // to-pprof writes non-empty bytes.
        let pprof = dir.join("cpu.pprof");
        assert_eq!(cmd_to_pprof(&fb, &pprof).unwrap(), EXIT_OK);
        assert!(std::fs::metadata(&pprof).unwrap().len() > 0);

        // gate vs identical baseline -> clean; vs cooler baseline -> regression.
        assert_eq!(
            cmd_gate(&cur, &cur, 0.20, 5, 1.0, 5.0, None).unwrap(),
            EXIT_OK
        );
        let base = dir.join("base.json");
        std::fs::write(
            &base,
            r#"{"frames":[{"name":"a","self_pct":10.0},{"name":"b","self_pct":40.0}]}"#,
        )
        .unwrap();
        assert_eq!(
            cmd_gate(&cur, &base, 0.20, 5, 1.0, 5.0, None).unwrap(),
            EXIT_REGRESSION
        );

        // missing input file -> Err (exit 2 at the top level).
        assert!(cmd_gate(&dir.join("nope.json"), &cur, 0.20, 5, 1.0, 5.0, None).is_err());
    }
}
