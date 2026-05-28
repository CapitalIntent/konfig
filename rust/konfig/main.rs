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
//! 9. Start gRPC server (port 50051) — blocks until SIGTERM/shutdown

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use dashmap::DashMap;
use kube::Client;
use tokio::sync::broadcast;
use tracing::info;

use konfig::cache::ConfigCache;
use konfig::grpc::{ServerConfig, serve};
use konfig::metrics::{LastEventAtMap, last_event_at_for};
use konfig::proto::{SecretEvent, konfig_service_server::KonfigServiceServer};
use konfig::secret_cache::SecretCache;
use konfig::secret_watcher::SecretWatcher;
use konfig::types::ConfigSnapshot;
use konfig::watcher::Watcher;

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
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env().add_directive("konfig=info".parse()?),
        )
        .init();

    let args = Args::parse();

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

    // gRPC server (blocks until shutdown).
    info!(addr = %args.grpc_addr, "starting gRPC server");
    serve(ServerConfig {
        addr: args.grpc_addr,
        cache,
        secret_cache,
        kube_client,
        health_reporter: Some(health_reporter),
        secret_namespace_broadcasts,
        last_event_at_map,
    })
    .await?;

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
