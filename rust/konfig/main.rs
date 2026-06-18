//! Konfig service binary.
//!
//! Startup sequence:
//! 1. Parse CLI args / env vars
//! 2. Init kube::Client
//! 3. Spawn Config CRD watcher task
//! 4. Spawn Secret namespace watchers (feed both cache + broadcast channel)
//! 5. Register gRPC health as NOT_SERVING for KonfigService
//! 6. Wait until cache has at least one populated entry
//! 7. Register gRPC health as SERVING
//! 8. Start /metrics HTTP server (port 9090) in background
//! 9. Install SIGTERM / Ctrl-C handler — feeds the shutdown signal that
//!    `grpc::serve` consumes to begin graceful drain
//! 10. Start gRPC server (port 50051) — blocks until shutdown completes
//!
//! Steps 2-10 live in [`konfig::startup::run`] (in the library crate) so
//! they're reachable from tests. This binary entry point covers step 1
//! plus tracing setup, then delegates.

// Gated on the `snmalloc` feature. Bazel `konfig_bin` enables it and pulls
// the crate from `@snmalloc//snmalloc-rs:snmalloc_rs` (jayakasadev fork).
// Cargo builds without the feature fall back to the System allocator.
#[cfg(feature = "snmalloc")]
#[global_allocator]
static GLOBAL: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

use std::sync::OnceLock;

use clap::Parser;
use konfig::startup::{Args, run};
use konfig::telemetry;
use opentelemetry_sdk::trace::SdkTracerProvider;
#[cfg(feature = "profiling")]
use pyroscope::PyroscopeAgent;
#[cfg(feature = "profiling")]
use pyroscope_pprofrs::{PprofConfig, pprof_backend};
use tracing::info;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::prelude::*;

// Holds the `tracing_appender::non_blocking` worker guard for the lifetime
// of the process. Dropping the guard flushes any pending log lines and
// stops the background writer thread; pinning it in a `OnceLock` keeps it
// alive until process exit so we never tear the worker down mid-run.
static TRACING_GUARD: OnceLock<WorkerGuard> = OnceLock::new();

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // OTLP tracer provider — `Some` only when `OTEL_EXPORTER_OTLP_ENDPOINT`
    // is set. Kept alive for the whole process so buffered spans flush; we
    // explicitly `shutdown()` it after `run` returns to drain the batch
    // exporter before exit.
    let tracer_provider = init_tracing()?;

    let args = Args::parse();

    // Pyroscope agent — only compiled into the `konfig-profiling` image
    // variant (`--features profiling`).  Started if PYROSCOPE_SERVER_ADDRESS
    // is set; held until process exit (dropping stops the agent).  The
    // default `konfig` image omits this entirely so the binary stays slim.
    #[cfg(feature = "profiling")]
    let _pyroscope = match std::env::var("PYROSCOPE_SERVER_ADDRESS") {
        Ok(url) if !url.is_empty() => {
            let app = std::env::var("PYROSCOPE_APPLICATION_NAME")
                .unwrap_or_else(|_| "konfig".to_string());
            let pod = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string());
            let agent = PyroscopeAgent::builder(&url, &app)
                .backend(pprof_backend(PprofConfig::new().sample_rate(100)))
                .tags(vec![("pod", Box::leak(pod.into_boxed_str()))])
                .build()?;
            let running = agent.start()?;
            info!(server = %url, application = %app, "pyroscope agent started");
            Some(running)
        }
        _ => None,
    };

    let result = run(args).await;

    // Flush + stop the OTLP batch exporter so spans buffered at shutdown are
    // not lost. No-op when tracing export was disabled (provider is `None`).
    if let Some(provider) = tracer_provider {
        if let Err(e) = provider.shutdown() {
            tracing::warn!("OTLP tracer provider shutdown failed: {e}");
        }
    }

    result
}

/// Install the tracing subscriber and, when enabled, the OTLP trace exporter.
///
/// Layer stack (built on `tracing_subscriber::Registry`):
///   1. env-filter (`konfig=info` default, overridable via `RUST_LOG`)
///   2. fmt layer → non-blocking stdout writer (always present)
///   3. `tracing-opentelemetry` OTLP layer — **only** when
///      `OTEL_EXPORTER_OTLP_ENDPOINT` is set (see [`telemetry`]). The OTLP
///      layer is added *on top of* the fmt layer, never replacing it: with no
///      collector configured konfig logs exactly as before.
///
/// Returns the [`SdkTracerProvider`] (`Some` iff the OTLP layer was installed)
/// so `main` can `shutdown()` it on exit to flush buffered spans.
///
/// The fmt layer writes through `tracing_appender::non_blocking`, which
/// offloads the actual `StdoutRaw::write_all` syscalls onto a dedicated
/// background worker thread. Pyroscope captures (CU-86aj360ae) showed
/// the synchronous `LineWriterShim<StdoutRaw>::write_all` path burning
/// ~30 ms / 1.6% of self-CPU on the runtime workers; the non-blocking
/// writer eliminates that hot path entirely. The returned `WorkerGuard`
/// is stashed in [`TRACING_GUARD`] so it lives for the process lifetime
/// — dropping the guard would shut the worker down and stall logs.
///
/// `tokio-console` (when the `tokio_console` feature is compiled in AND
/// `RUST_CONSOLE=1` at runtime) takes the whole subscriber over via
/// `console_subscriber::init()` and short-circuits before the registry is
/// built — OTLP export is not composed with the console subscriber.
fn init_tracing() -> Result<Option<SdkTracerProvider>, Box<dyn std::error::Error>> {
    #[cfg(feature = "tokio_console")]
    let console_enabled = matches!(std::env::var("RUST_CONSOLE").as_deref(), Ok("1"));
    #[cfg(not(feature = "tokio_console"))]
    let console_enabled = false;

    if console_enabled {
        #[cfg(feature = "tokio_console")]
        {
            console_subscriber::init();
            info!("tokio-console subscriber installed (RUST_CONSOLE=1)");
        }
        return Ok(None);
    }

    let (writer, guard) = tracing_appender::non_blocking(std::io::stdout());
    if TRACING_GUARD.set(guard).is_err() {
        return Err("init_tracing called more than once: TRACING_GUARD already populated".into());
    }

    let env_filter = tracing_subscriber::EnvFilter::from_default_env()
        .add_directive("konfig=info".parse()?);
    let fmt_layer = tracing_subscriber::fmt::layer().with_writer(writer);

    // Build the OTLP provider up front so the layer (which borrows a tracer
    // from it) and the returned provider (for shutdown) share one instance.
    let tracer_provider = telemetry::build_tracer_provider()?;
    let otel_layer = tracer_provider.as_ref().map(telemetry::otel_layer);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    if tracer_provider.is_some() {
        info!("OTLP trace export enabled (OTEL_EXPORTER_OTLP_ENDPOINT set)");
    }

    Ok(tracer_provider)
}
