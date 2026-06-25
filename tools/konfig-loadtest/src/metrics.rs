//! Latency sample aggregation, scenario result types, and JSON output formatting.

// ── Stats ─────────────────────────────────────────────────────────────────────

pub(crate) struct Stats {
    pub(crate) samples: Vec<u128>,
}

impl Stats {
    pub(crate) fn new() -> Self {
        Self {
            samples: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, ms: u128) {
        self.samples.push(ms);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    fn sorted(&self) -> Vec<u128> {
        let mut v = self.samples.clone();
        v.sort_unstable();
        v
    }

    pub(crate) fn p50(&self) -> u128 {
        let s = self.sorted();
        s[s.len() / 2]
    }

    pub(crate) fn p95(&self) -> u128 {
        let s = self.sorted();
        s[(s.len() as f64 * 0.95) as usize]
    }

    pub(crate) fn p99(&self) -> u128 {
        let s = self.sorted();
        s[(s.len() as f64 * 0.99) as usize]
    }

    pub(crate) fn max(&self) -> u128 {
        *self.sorted().last().unwrap_or(&0)
    }
}

// ── Scenario result ───────────────────────────────────────────────────────────

pub(crate) struct ScenarioResult {
    pub(crate) name: &'static str,
    pub(crate) pass: bool,
    pub(crate) failures: Vec<String>,
    /// Latency percentiles (ms) for the JSON summary. None for scenarios that
    /// do not capture per-event latency (e.g. sustained soak). Does not affect
    /// pass/fail logic — purely for machine-readable result output.
    pub(crate) metrics: Option<LatencyMetrics>,
}

#[derive(Clone, Copy)]
pub(crate) struct LatencyMetrics {
    pub(crate) samples: usize,
    pub(crate) p50_ms: u128,
    pub(crate) p95_ms: u128,
    pub(crate) p99_ms: u128,
    pub(crate) max_ms: u128,
}

impl ScenarioResult {
    pub(crate) fn pass(name: &'static str) -> Self {
        Self {
            name,
            pass: true,
            failures: Vec::new(),
            metrics: None,
        }
    }

    pub(crate) fn fail(name: &'static str, failures: Vec<String>) -> Self {
        Self {
            name,
            pass: false,
            failures,
            metrics: None,
        }
    }

    pub(crate) fn with_metrics(mut self, metrics: LatencyMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }
}

/// Write a machine-readable JSON summary of all scenario results to `path`.
/// Hand-rolled (no serde dep) — the schema is small and stable:
///
///   {"all_passed":bool,"scenarios":[
///     {"name":str,"pass":bool,
///      "metrics":{"samples":n,"p50_ms":n,"p95_ms":n,"p99_ms":n,"max_ms":n}|null,
///      "failures":[str,...]}
///   ]}
pub(crate) fn write_results_json(
    path: &str,
    results: &[ScenarioResult],
    any_fail: bool,
) -> std::io::Result<()> {
    fn esc(s: &str) -> String {
        // Minimal JSON string escaping: backslash, quote, and control chars
        // that appear in our failure messages. Sufficient for this fixed set
        // of programmatically-built strings.
        let mut out = String::with_capacity(s.len() + 2);
        for c in s.chars() {
            match c {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                c => out.push(c),
            }
        }
        out
    }

    let mut json = String::new();
    json.push_str(&format!("{{\"all_passed\":{},\"scenarios\":[", !any_fail));
    for (i, r) in results.iter().enumerate() {
        if i > 0 {
            json.push(',');
        }
        json.push_str(&format!(
            "{{\"name\":\"{}\",\"pass\":{},",
            esc(r.name),
            r.pass
        ));
        match &r.metrics {
            Some(m) => json.push_str(&format!(
                "\"metrics\":{{\"samples\":{},\"p50_ms\":{},\"p95_ms\":{},\"p99_ms\":{},\"max_ms\":{}}},",
                m.samples, m.p50_ms, m.p95_ms, m.p99_ms, m.max_ms
            )),
            None => json.push_str("\"metrics\":null,"),
        }
        json.push_str("\"failures\":[");
        for (j, f) in r.failures.iter().enumerate() {
            if j > 0 {
                json.push(',');
            }
            json.push_str(&format!("\"{}\"", esc(f)));
        }
        json.push_str("]}");
    }
    json.push_str("]}\n");

    std::fs::write(path, json)
}
