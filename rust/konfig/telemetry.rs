//! OpenTelemetry (OTLP) trace export wiring.
//!
//! Phase 7 distributed-tracing (CU-86ahzwj17). Builds an optional
//! `tracing-opentelemetry` layer that ships spans to an OTLP/gRPC collector.
//! The layer is **opt-in by environment** so a collector is never required to
//! run konfig:
//!
//! * `OTEL_EXPORTER_OTLP_ENDPOINT` unset or empty → [`build_tracer_provider`]
//!   returns `Ok(None)` and the caller installs the plain fmt subscriber only.
//!   No exporter, no background batch task, no extra cost.
//! * `OTEL_EXPORTER_OTLP_ENDPOINT` set (e.g. `http://otel-collector:4317`) →
//!   a batch OTLP/gRPC span exporter is built, the global propagator is
//!   installed, and the returned layer is added **on top of** the existing
//!   `tracing` subscriber (it does not replace the fmt layer).
//!
//! Sampling honours `OTEL_TRACES_SAMPLER` (and `OTEL_TRACES_SAMPLER_ARG`) via
//! the SDK's env-driven [`Sampler::ParentBased`] default chain — see
//! [`resolve_sampler`]. Service name defaults to `konfig`, overridable via
//! `OTEL_SERVICE_NAME` (CU-86aj08u7k). `OTEL_SDK_DISABLED=true` is an
//! operator kill-switch: the exporter is skipped even when an endpoint is
//! set, so a prod overlay can ship endpoint config and toggle export with a
//! single env flip.
//!
//! Hard constraint (repo policy): NEVER `opentelemetry-prometheus`
//! (deprecated, slow). This is the `tracing-opentelemetry` bridge only.

use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};

/// Default service name reported to the collector (`service.name` resource
/// attribute) when `OTEL_SERVICE_NAME` is unset.
const DEFAULT_SERVICE_NAME: &str = "konfig";

/// Env var overriding the reported `service.name` resource attribute. Per the
/// OTEL spec; defaults to [`DEFAULT_SERVICE_NAME`] when unset/blank.
const OTEL_SERVICE_NAME_ENV: &str = "OTEL_SERVICE_NAME";

/// Env var that, when truthy, disables the OTEL SDK entirely (per the OTEL
/// spec). Honoured even when an endpoint is configured so operators flip one
/// flag to kill export without unsetting the endpoint.
const OTEL_SDK_DISABLED_ENV: &str = "OTEL_SDK_DISABLED";

/// Env var holding the OTLP/gRPC collector endpoint. Empty/unset disables the
/// exporter entirely (no-op layer).
const OTEL_ENDPOINT_ENV: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";

/// Default OTLP/gRPC endpoint used only when the operator opts in to tracing
/// but does not override the endpoint. Matches the OTLP spec default port.
const DEFAULT_OTLP_ENDPOINT: &str = "http://localhost:4317";

/// Env var selecting the head sampler. Mirrors the OTEL spec
/// (`always_on`, `always_off`, `traceidratio`, `parentbased_*`).
const OTEL_SAMPLER_ENV: &str = "OTEL_TRACES_SAMPLER";

/// Env var carrying the sampler argument (e.g. the ratio for `traceidratio`).
const OTEL_SAMPLER_ARG_ENV: &str = "OTEL_TRACES_SAMPLER_ARG";

/// Resolve the head [`Sampler`] from `OTEL_TRACES_SAMPLER` /
/// `OTEL_TRACES_SAMPLER_ARG`, defaulting to `parentbased_always_on` (the OTEL
/// spec default) when unset or unrecognised.
///
/// Supported values (per the OpenTelemetry spec):
/// * `always_on` / `always_off`
/// * `traceidratio` — ratio from `OTEL_TRACES_SAMPLER_ARG` (default 1.0)
/// * `parentbased_always_on` (default) / `parentbased_always_off`
/// * `parentbased_traceidratio` — ratio from `OTEL_TRACES_SAMPLER_ARG`
///
/// Pure function of its two string inputs so the parse table is unit-testable
/// without touching process env.
pub fn resolve_sampler(sampler: Option<&str>, arg: Option<&str>) -> Sampler {
    // Ratio parse helper — fall back to 1.0 (sample everything) on missing /
    // malformed arg so a typo never silently drops all traces.
    let ratio = || {
        arg.and_then(|a| a.trim().parse::<f64>().ok())
            .unwrap_or(1.0)
    };

    match sampler.map(str::trim) {
        Some("always_on") => Sampler::AlwaysOn,
        Some("always_off") => Sampler::AlwaysOff,
        Some("traceidratio") => Sampler::TraceIdRatioBased(ratio()),
        Some("parentbased_always_off") => Sampler::ParentBased(Box::new(Sampler::AlwaysOff)),
        Some("parentbased_traceidratio") => {
            Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(ratio())))
        }
        // `parentbased_always_on` (the spec default) + any unset/unknown value.
        _ => Sampler::ParentBased(Box::new(Sampler::AlwaysOn)),
    }
}

/// Read `OTEL_EXPORTER_OTLP_ENDPOINT`. Returns `None` when unset or empty
/// (after trim) — the signal that tracing export is disabled.
fn endpoint_from_env() -> Option<String> {
    match std::env::var(OTEL_ENDPOINT_ENV) {
        Ok(v) if !v.trim().is_empty() => Some(v),
        _ => None,
    }
}

/// Env var selecting the log encoder. `json` → machine-parseable JSON layer;
/// anything else (`pretty`/unset) → the human-readable fmt layer.
pub const RUST_LOG_FORMAT_ENV: &str = "RUST_LOG_FORMAT";

/// Whether `RUST_LOG_FORMAT` selects the JSON log encoder (CU-86ahrwd64).
/// `json` (exact, case-sensitive — matches the OTEL/12-factor convention of
/// lowercase enum env values) selects JSON; every other value keeps the
/// human-readable format. Pure function of the raw env string so `main.rs`'s
/// encoder branch is unit-testable here in the library crate (the binary's
/// own `#[cfg(test)]` does not run under the Bazel `:test` target).
pub fn log_format_is_json(raw: Option<&str>) -> bool {
    raw == Some("json")
}

/// Whether `OTEL_SDK_DISABLED` is truthy. Per the OTEL spec the only
/// SDK-disabling value is `true` (case-insensitive); every other value
/// (`false`, `0`, junk, unset) leaves the SDK enabled. Pure function of the
/// raw env string so the parse is unit-testable without touching process env.
pub fn sdk_disabled(raw: Option<&str>) -> bool {
    raw.map(|v| v.trim().eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Resolve the reported `service.name` from `OTEL_SERVICE_NAME`, defaulting to
/// [`DEFAULT_SERVICE_NAME`] when unset or blank. Pure function of its input.
pub fn service_name(raw: Option<&str>) -> String {
    match raw.map(str::trim) {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => DEFAULT_SERVICE_NAME.to_string(),
    }
}

/// Build the optional OTLP tracer provider from the process environment.
///
/// * Returns `Ok(None)` when `OTEL_EXPORTER_OTLP_ENDPOINT` is unset/empty — the
///   caller then installs only the fmt subscriber (no collector required).
/// * Returns `Ok(Some(provider))` when the endpoint is set: a batch OTLP/gRPC
///   exporter wired to a [`SdkTracerProvider`] with the `konfig` service name
///   and the env-resolved sampler. The W3C trace-context propagator is
///   installed as the global propagator as a side effect so outbound context
///   is carried on downstream calls.
///
/// The returned provider must be kept alive for the process lifetime and
/// `shutdown()` on exit to flush buffered spans — the caller owns it (stashed
/// alongside the tracing guard in `main.rs`).
pub fn build_tracer_provider() -> Result<Option<SdkTracerProvider>, Box<dyn std::error::Error>> {
    // Operator kill-switch: `OTEL_SDK_DISABLED=true` short-circuits before the
    // endpoint check so a prod overlay can keep the endpoint configured and
    // toggle export with one flag (CU-86aj08u7k).
    if sdk_disabled(std::env::var(OTEL_SDK_DISABLED_ENV).ok().as_deref()) {
        return Ok(None);
    }
    let Some(endpoint) = endpoint_from_env() else {
        return Ok(None);
    };
    // Honour an explicit empty default the same way as unset; otherwise the
    // operator-provided endpoint wins. (endpoint_from_env already filtered
    // empties, so `endpoint` is always non-empty here; the const is the
    // fallback for code paths that call the builder directly.)
    let endpoint = if endpoint.trim().is_empty() {
        DEFAULT_OTLP_ENDPOINT.to_string()
    } else {
        endpoint
    };

    // Install the W3C trace-context propagator globally so spans created here
    // can extract/inject context across process boundaries.
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()?;

    let sampler = resolve_sampler(
        std::env::var(OTEL_SAMPLER_ENV).ok().as_deref(),
        std::env::var(OTEL_SAMPLER_ARG_ENV).ok().as_deref(),
    );

    let resource = Resource::builder()
        .with_attribute(KeyValue::new(
            "service.name",
            service_name(std::env::var(OTEL_SERVICE_NAME_ENV).ok().as_deref()),
        ))
        .build();

    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(sampler)
        .with_resource(resource)
        .build();

    Ok(Some(provider))
}

/// Build the `tracing-opentelemetry` layer from a tracer provider.
///
/// Split from [`build_tracer_provider`] so `main.rs` can keep ownership of the
/// provider (for `shutdown()` on exit) while handing the layer to the
/// subscriber registry. The layer is generic over the subscriber `S` so it
/// composes on top of the existing fmt + env-filter stack rather than
/// replacing it.
pub fn otel_layer<S>(
    provider: &SdkTracerProvider,
) -> tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry_sdk::trace::SdkTracer>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    // Instrumentation-scope (library) name — conventionally stable, distinct
    // from the env-overridable `service.name` resource attribute set in
    // `build_tracer_provider`.
    let tracer = provider.tracer(DEFAULT_SERVICE_NAME);
    tracing_opentelemetry::layer().with_tracer(tracer)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Default (unset sampler) must be `parentbased_always_on` per the OTEL
    /// spec — represented as `ParentBased`. We can't compare `Sampler` for
    /// equality (no `PartialEq`), so assert on the `Debug` rendering.
    #[test]
    fn resolve_sampler_defaults_to_parentbased_always_on() {
        let s = resolve_sampler(None, None);
        let dbg = format!("{s:?}");
        assert!(
            dbg.contains("ParentBased"),
            "unset sampler must default to ParentBased(AlwaysOn); got {dbg}"
        );
    }

    #[test]
    fn resolve_sampler_always_off() {
        let dbg = format!("{:?}", resolve_sampler(Some("always_off"), None));
        assert!(dbg.contains("AlwaysOff"), "got {dbg}");
    }

    #[test]
    fn resolve_sampler_always_on() {
        let dbg = format!("{:?}", resolve_sampler(Some("always_on"), None));
        // AlwaysOn (not ParentBased) — the non-parent variant.
        assert!(
            dbg.contains("AlwaysOn") && !dbg.contains("ParentBased"),
            "got {dbg}"
        );
    }

    #[test]
    fn resolve_sampler_traceidratio_reads_arg() {
        let dbg = format!("{:?}", resolve_sampler(Some("traceidratio"), Some("0.25")));
        assert!(
            dbg.contains("TraceIdRatioBased") && dbg.contains("0.25"),
            "ratio sampler must carry the parsed arg; got {dbg}"
        );
    }

    /// Malformed ratio arg must fall back to 1.0 (sample everything) rather
    /// than panicking or silently dropping all traces.
    #[test]
    fn resolve_sampler_traceidratio_bad_arg_falls_back_to_one() {
        let dbg = format!(
            "{:?}",
            resolve_sampler(Some("traceidratio"), Some("not-a-number"))
        );
        assert!(
            dbg.contains("TraceIdRatioBased") && dbg.contains("1.0"),
            "malformed ratio arg must fall back to 1.0; got {dbg}"
        );
    }

    #[test]
    fn resolve_sampler_parentbased_traceidratio() {
        let dbg = format!(
            "{:?}",
            resolve_sampler(Some("parentbased_traceidratio"), Some("0.5"))
        );
        assert!(
            dbg.contains("ParentBased") && dbg.contains("TraceIdRatioBased") && dbg.contains("0.5"),
            "got {dbg}"
        );
    }

    #[test]
    fn resolve_sampler_unknown_value_defaults_parentbased() {
        let dbg = format!("{:?}", resolve_sampler(Some("garbage"), None));
        assert!(
            dbg.contains("ParentBased"),
            "unknown sampler must default to ParentBased(AlwaysOn); got {dbg}"
        );
    }

    /// With the endpoint env unset, `build_tracer_provider` must return
    /// `Ok(None)` — the no-collector-required contract. Guarded behind a
    /// single-threaded env mutation (the `:test` target pins
    /// `RUST_TEST_THREADS=1` in BUILD.bazel, so racing on process env is
    /// impossible).
    #[test]
    fn build_tracer_provider_none_when_endpoint_unset() {
        let prev = std::env::var(OTEL_ENDPOINT_ENV).ok();
        // SAFETY: tests in this crate run single-threaded (RUST_TEST_THREADS=1).
        unsafe {
            std::env::remove_var(OTEL_ENDPOINT_ENV);
        }
        let got = build_tracer_provider().expect("must not error when disabled");
        // Restore before asserting so a failure can't leak env into siblings.
        if let Some(v) = prev {
            unsafe {
                std::env::set_var(OTEL_ENDPOINT_ENV, v);
            }
        }
        assert!(
            got.is_none(),
            "unset OTEL endpoint must yield a no-op (Ok(None)) provider"
        );
    }

    /// An endpoint that is set but whitespace-only is treated as unset.
    #[test]
    fn build_tracer_provider_none_when_endpoint_blank() {
        let prev = std::env::var(OTEL_ENDPOINT_ENV).ok();
        unsafe {
            std::env::set_var(OTEL_ENDPOINT_ENV, "   ");
        }
        let got = build_tracer_provider().expect("must not error on blank endpoint");
        match prev {
            Some(v) => unsafe { std::env::set_var(OTEL_ENDPOINT_ENV, v) },
            None => unsafe { std::env::remove_var(OTEL_ENDPOINT_ENV) },
        }
        assert!(got.is_none(), "blank endpoint must be treated as disabled");
    }

    // ── OTEL_SDK_DISABLED / OTEL_SERVICE_NAME (CU-86aj08u7k) ───────────────────

    #[test]
    fn log_format_is_json_selects_json_only() {
        assert!(log_format_is_json(Some("json")));
        // Case-sensitive + exact: only lowercase `json` selects JSON.
        assert!(!log_format_is_json(Some("JSON")));
        assert!(!log_format_is_json(Some("pretty")));
        assert!(!log_format_is_json(Some("")));
        assert!(!log_format_is_json(None));
    }

    #[test]
    fn sdk_disabled_only_true_disables() {
        assert!(sdk_disabled(Some("true")));
        assert!(sdk_disabled(Some("TRUE")));
        assert!(sdk_disabled(Some("  True ")));
        assert!(!sdk_disabled(Some("false")));
        assert!(!sdk_disabled(Some("0")));
        assert!(!sdk_disabled(Some("1")));
        assert!(!sdk_disabled(Some("")));
        assert!(!sdk_disabled(None));
    }

    #[test]
    fn service_name_defaults_to_konfig() {
        assert_eq!(service_name(None), "konfig");
        assert_eq!(service_name(Some("   ")), "konfig");
    }

    #[test]
    fn service_name_honours_override() {
        assert_eq!(service_name(Some("konfig-canary")), "konfig-canary");
        // Surrounding whitespace is trimmed.
        assert_eq!(service_name(Some("  svc-x  ")), "svc-x");
    }

    /// `OTEL_SDK_DISABLED=true` must yield `Ok(None)` even when an endpoint is
    /// configured — the operator kill-switch. Single-threaded env mutation
    /// (`RUST_TEST_THREADS=1`, see BUILD.bazel) makes the env swap safe.
    #[test]
    fn build_tracer_provider_none_when_sdk_disabled_even_with_endpoint() {
        let prev_ep = std::env::var(OTEL_ENDPOINT_ENV).ok();
        let prev_dis = std::env::var(OTEL_SDK_DISABLED_ENV).ok();
        // SAFETY: tests run single-threaded (RUST_TEST_THREADS=1).
        unsafe {
            std::env::set_var(OTEL_ENDPOINT_ENV, "http://otel-collector:4317");
            std::env::set_var(OTEL_SDK_DISABLED_ENV, "true");
        }
        let got = build_tracer_provider().expect("must not error when SDK disabled");
        // Restore before asserting so a failure can't leak env into siblings.
        match prev_ep {
            Some(v) => unsafe { std::env::set_var(OTEL_ENDPOINT_ENV, v) },
            None => unsafe { std::env::remove_var(OTEL_ENDPOINT_ENV) },
        }
        match prev_dis {
            Some(v) => unsafe { std::env::set_var(OTEL_SDK_DISABLED_ENV, v) },
            None => unsafe { std::env::remove_var(OTEL_SDK_DISABLED_ENV) },
        }
        assert!(
            got.is_none(),
            "OTEL_SDK_DISABLED=true must disable export even with an endpoint set"
        );
    }
}
