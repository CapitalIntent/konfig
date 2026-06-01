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

use std::net::SocketAddr;
use std::sync::Arc;

// Per-OS allocator pin (memory rule: jemalloc on Linux pods, snmalloc on macOS dev).
#[cfg(target_os = "linux")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(target_os = "macos")]
#[global_allocator]
static GLOBAL: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

use clap::Parser;
use dashmap::DashMap;
use kube::Client;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{broadcast, oneshot};
use tracing::info;

use konfig::cache::ConfigCache;
use konfig::grpc::{ServerConfig, serve};
use konfig::metrics::{LastEventAtMap, last_event_at_for, spawn_tokio_runtime_sampler};
use konfig::proto::{SecretEvent, konfig_service_server::KonfigServiceServer};
use konfig::secret_cache::SecretCache;
use konfig::secret_watcher::SecretWatcher;
use konfig::types::ConfigSnapshot;
use konfig::watcher::Watcher;
#[cfg(feature = "profiling")]
use pyroscope::PyroscopeAgent;
#[cfg(feature = "profiling")]
use pyroscope_pprofrs::{PprofConfig, pprof_backend};

#[derive(Parser)]
#[command(name = "konfig", about = "Konfig config distribution service")]
struct Args {
    /// gRPC listen address
    #[arg(long, env = "KONFIG_GRPC_ADDR", default_value = "0.0.0.0:50051")]
    grpc_addr: SocketAddr,

    /// Prometheus metrics listen address
    #[arg(long, env = "KONFIG_METRICS_ADDR", default_value = "0.0.0.0:9090")]
    metrics_addr: SocketAddr,

    /// K8s namespace to watch for Config CRDs
    #[arg(long, env = "KONFIG_NAMESPACE", default_value = "default")]
    namespace: String,

    /// Config CRD name to watch.
    /// KONFIG_NAME must be set — no default config name; konfig is domain-agnostic.
    #[arg(long, env = "KONFIG_NAME")]
    name: String,

    /// K8s namespaces to watch for managed Secrets (konfig.io/managed=true).
    /// Comma-separated or repeated flag, e.g. --secret-namespaces trading,risk
    #[arg(
        long,
        env = "KONFIG_SECRET_NAMESPACES",
        value_delimiter = ',',
        default_value = ""
    )]
    secret_namespaces: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // tokio-console: only installed when both compiled in (`--features tokio_console`)
    // AND `RUST_CONSOLE=1` at runtime. `console_subscriber::init()` installs its
    // own tracing layer, so skip the plain fmt subscriber on that path.
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
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::from_default_env()
                    .add_directive("konfig=info".parse()?),
            )
            .init();
    }

    let args = Args::parse();

    // Spawn tokio runtime-metrics sampler — publishes `tokio_*` gauges every
    // 5 s on the same `/metrics` endpoint as the Prometheus app metrics.
    spawn_tokio_runtime_sampler(tokio::runtime::Handle::current());

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

    let kube_client = Client::try_default().await?;

    let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
    let secret_cache = Arc::new(SecretCache::new());

    // Per-namespace freshness map shared by all watchers and the konfig_stale_seconds sampler.
    let last_event_at_map: LastEventAtMap = Arc::new(DashMap::new());

    // Spawn Config CRD watcher.
    let watcher_cache = Arc::clone(&cache);
    let watcher_client = kube_client.clone();
    let namespace = args.namespace.clone();
    let name = args.name.clone();
    let watcher_last_event_at = last_event_at_for(&last_event_at_map, &namespace);
    tokio::spawn(async move {
        Watcher::new(watcher_client)
            .run(watcher_cache, namespace, name, watcher_last_event_at)
            .await
            .expect("watcher exited with error");
    });

    // Spawn Secret namespace watchers.  Each watcher feeds both the SecretCache
    // and a shared broadcast::Sender so SubscribeSecrets uses a single kube
    // watch stream per namespace instead of one per subscriber.
    let secret_namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<SecretEvent>>> =
        Arc::new(DashMap::new());
    let secret_namespaces: Vec<String> = args
        .secret_namespaces
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect();
    if !secret_namespaces.is_empty() {
        info!(namespaces = ?secret_namespaces, "Starting secret namespace watchers");
        SecretWatcher::new(kube_client.clone()).spawn_all(
            Arc::clone(&secret_cache),
            secret_namespaces,
            Arc::clone(&secret_namespace_broadcasts),
            Arc::clone(&last_event_at_map),
        );
    }

    // Health reporter: NOT_SERVING until cache is populated.
    let (health_reporter, _health_server) = tonic_health::server::health_reporter();
    health_reporter
        .set_not_serving::<KonfigServiceServer<konfig::grpc::KonfigServer>>()
        .await;

    // Wait for cache to be populated (at least one entry).
    {
        let cache_ref = Arc::clone(&cache);
        let health_ref = health_reporter.clone();
        tokio::spawn(async move {
            loop {
                if cache_ref.is_populated() {
                    health_ref
                        .set_serving::<KonfigServiceServer<konfig::grpc::KonfigServer>>()
                        .await;
                    info!("Cache populated — health: SERVING");
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        });
    }

    // Metrics HTTP server.
    let metrics_addr = args.metrics_addr;
    tokio::spawn(async move {
        serve_metrics(metrics_addr).await;
    });

    // Install SIGTERM + Ctrl-C handlers.  Either signal triggers a single
    // `oneshot` send into the gRPC server's shutdown channel.  `serve` then
    // flips the drain flag, closes active Subscribe streams, marks the health
    // endpoint NOT_SERVING, and waits up to DRAIN_TIMEOUT before stopping.
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = sigterm.recv() => info!("Received SIGTERM"),
            _ = tokio::signal::ctrl_c() => info!("Received Ctrl-C (SIGINT)"),
        }
        // `send` returns Err only if the receiver was already dropped — fine.
        let _ = shutdown_tx.send(());
    });

    // gRPC server (blocks until shutdown completes).
    info!(addr = %args.grpc_addr, "starting gRPC server");
    serve(ServerConfig {
        addr: args.grpc_addr,
        cache,
        secret_cache,
        kube_client,
        health_reporter: Some(health_reporter),
        secret_namespace_broadcasts,
        last_event_at_map,
        shutdown_signal: Some(Box::pin(async move {
            // Resolve when the signal handler fires.  If `shutdown_tx` was
            // dropped (signal task panicked) treat that as a drain request
            // too — better than hanging forever.
            let _ = shutdown_rx.await;
        })),
    })
    .await?;

    info!("gRPC server stopped cleanly");
    Ok(())
}

async fn serve_metrics(addr: SocketAddr) {
    use axum::{Router, routing::get};

    let app = Router::new().route("/metrics", get(metrics_handler));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind metrics");
    info!(addr = %addr, "metrics server starting");
    axum::serve(listener, app)
        .await
        .expect("metrics server error");
}

async fn metrics_handler() -> String {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buf = Vec::new();
    encoder
        .encode(&metric_families, &mut buf)
        .unwrap_or_default();
    String::from_utf8(buf).unwrap_or_default()
}
