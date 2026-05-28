//! Konfig service binary.
//!
//! Startup sequence:
//! 1. Parse CLI args / env vars
//! 2. Init kube::Client
//! 3. Spawn Config CRD watcher task
//! 4. Register gRPC health as NOT_SERVING for KonfigService
//! 5. Wait until cache has at least one populated entry
//! 6. Register gRPC health as SERVING
//! 7. Start /metrics HTTP server (port 9090) in background
//! 8. Start gRPC server (port 50051) — blocks until SIGTERM/shutdown

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use kube::Client;
use tracing::info;

use konfig::cache::ConfigCache;
use konfig::grpc::{ServerConfig, serve};
use konfig::proto::konfig_service_server::KonfigServiceServer;
use konfig::secret_cache::SecretCache;
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

    // Spawn Config CRD watcher.
    let watcher_cache = Arc::clone(&cache);
    let watcher_client = kube_client.clone();
    let namespace = args.namespace.clone();
    let name = args.name.clone();
    tokio::spawn(async move {
        Watcher::new(watcher_client)
            .run(watcher_cache, namespace, name)
            .await
            .expect("watcher exited with error");
    });

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
