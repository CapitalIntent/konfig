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
#[cfg(feature = "profiling")]
use pyroscope::PyroscopeAgent;
#[cfg(feature = "profiling")]
use pyroscope_pprofrs::{PprofConfig, pprof_backend};
#[cfg(any(feature = "profiling", feature = "tokio_console"))]
use tracing::info;
use tracing_appender::non_blocking::WorkerGuard;

// Holds the `tracing_appender::non_blocking` worker guard for the lifetime
// of the process. Dropping the guard flushes any pending log lines and
// stops the background writer thread; pinning it in a `OnceLock` keeps it
// alive until process exit so we never tear the worker down mid-run.
static TRACING_GUARD: OnceLock<WorkerGuard> = OnceLock::new();

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing()?;

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

    run(args).await
}

/// Install the tracing subscriber. Uses `console-subscriber` when the
/// `tokio_console` feature is compiled in AND `RUST_CONSOLE=1` at runtime;
/// otherwise installs the plain `tracing_subscriber::fmt` layer with the
/// `konfig=info` filter.
///
/// The fmt layer writes through `tracing_appender::non_blocking`, which
/// offloads the actual `StdoutRaw::write_all` syscalls onto a dedicated
/// background worker thread. Pyroscope captures (CU-86aj360ae) showed
/// the synchronous `LineWriterShim<StdoutRaw>::write_all` path burning
/// ~30 ms / 1.6% of self-CPU on the runtime workers; the non-blocking
/// writer eliminates that hot path entirely. The returned `WorkerGuard`
/// is stashed in [`TRACING_GUARD`] so it lives for the process lifetime
/// — dropping the guard would shut the worker down and stall logs.
fn init_tracing() -> Result<(), Box<dyn std::error::Error>> {
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
        return Ok(());
    }

    let (writer, guard) = tracing_appender::non_blocking(std::io::stdout());
    if TRACING_GUARD.set(guard).is_err() {
        return Err("init_tracing called more than once: TRACING_GUARD already populated".into());
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("konfig=info".parse()?),
        )
        .with_writer(writer)
        .init();
    Ok(())
}
