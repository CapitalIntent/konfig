//! gRPC server for `konfig.v1.KonfigService`.
//!
//! Implements the tonic-generated `KonfigService` trait on `KonfigServer`.
//! All message types are Protobuf (standard tonic codec, no custom codec).

pub mod apply;
pub mod get;
pub mod secret_apply;
pub mod secret_get;
pub mod subscribe;
pub mod subscribe_secrets;

use std::net::SocketAddr;
use std::sync::Arc;

use dashmap::DashMap;
use kube::Client;
use tokio::sync::broadcast;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::cache::ConfigCache;
use crate::grpc::subscribe::ReplayBuffer;
use crate::proto::{
    ApplyRequest, ApplyResponse, ApplySecretRequest, ApplySecretResponse, Config, ConfigEvent,
    GetAllRequest, GetAllSecretsRequest, GetRequest, GetSecretRequest, SecretEvent, SecretResponse,
    SubscribeRequest, SubscribeSecretsRequest,
    konfig_service_server::{KonfigService, KonfigServiceServer},
};
use crate::secret_cache::SecretCache;

// ── Server config ─────────────────────────────────────────────────────────────

pub struct ServerConfig {
    pub addr: SocketAddr,
    pub cache: Arc<ConfigCache>,
    /// Shared secret cache populated by the secret watcher.
    pub secret_cache: Arc<SecretCache>,
    pub kube_client: Client,
    /// Optional tonic-health reporter.  When `Some`, a health endpoint is
    /// registered alongside `KonfigService`.  When `None` the server starts
    /// without a health endpoint (e.g. in unit tests).
    pub health_reporter: Option<tonic_health::server::HealthReporter>,
}

// ── KonfigServer ──────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct KonfigServer {
    pub(crate) cache: Arc<ConfigCache>,
    pub(crate) secret_cache: Arc<SecretCache>,
    pub(crate) kube_client: Client,
    /// One broadcast sender per namespace — shared across all Config subscribers
    /// for that namespace.  A single kube watcher drives the sender; each
    /// subscriber gets a `Receiver` clone (O(1) fan-out).
    pub(crate) namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<ConfigEvent>>>,
    /// Per-namespace replay buffer for the `resume_resource_version` reconnect
    /// path.  Holds the last `REPLAY_BUFFER_SIZE` events so reconnecting clients
    /// can catch up without opening a new kube watch.
    pub(crate) namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>>,
    /// Separate broadcast map for secret events — keyed by namespace.
    /// Intentionally distinct from `namespace_broadcasts` so Config and Secret
    /// streams do not interfere.
    pub(crate) secret_namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<SecretEvent>>>,
}

#[tonic::async_trait]
impl KonfigService for KonfigServer {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<Config>, Status> {
        get::handle_get(Arc::clone(&self.cache), request.into_inner()).await
    }

    type GetAllStream = ReceiverStream<Result<Config, Status>>;

    async fn get_all(
        &self,
        request: Request<GetAllRequest>,
    ) -> Result<Response<Self::GetAllStream>, Status> {
        get::handle_get_all(Arc::clone(&self.cache), request.into_inner()).await
    }

    async fn apply(
        &self,
        request: Request<ApplyRequest>,
    ) -> Result<Response<ApplyResponse>, Status> {
        apply::handle_apply(self.kube_client.clone(), request.into_inner()).await
    }

    type SubscribeStream = ReceiverStream<Result<ConfigEvent, Status>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        subscribe::handle_subscribe(
            Arc::clone(&self.cache),
            self.kube_client.clone(),
            Arc::clone(&self.namespace_broadcasts),
            Arc::clone(&self.namespace_replay_buffers),
            request.into_inner(),
        )
        .await
    }

    // ── Secret RPCs ───────────────────────────────────────────────────────────

    async fn get_secret(
        &self,
        request: Request<GetSecretRequest>,
    ) -> Result<Response<SecretResponse>, Status> {
        secret_get::handle_get_secret(Arc::clone(&self.secret_cache), request.into_inner()).await
    }

    type GetAllSecretsStream = ReceiverStream<Result<SecretResponse, Status>>;

    async fn get_all_secrets(
        &self,
        request: Request<GetAllSecretsRequest>,
    ) -> Result<Response<Self::GetAllSecretsStream>, Status> {
        secret_get::handle_get_all_secrets(Arc::clone(&self.secret_cache), request.into_inner())
            .await
    }

    async fn apply_secret(
        &self,
        request: Request<ApplySecretRequest>,
    ) -> Result<Response<ApplySecretResponse>, Status> {
        secret_apply::handle_apply_secret(self.kube_client.clone(), request.into_inner()).await
    }

    type SubscribeSecretsStream = ReceiverStream<Result<SecretEvent, Status>>;

    async fn subscribe_secrets(
        &self,
        request: Request<SubscribeSecretsRequest>,
    ) -> Result<Response<Self::SubscribeSecretsStream>, Status> {
        subscribe_secrets::handle_subscribe_secrets(
            self.kube_client.clone(),
            Arc::clone(&self.secret_namespace_broadcasts),
            request.into_inner(),
        )
        .await
    }
}

// ── Startup ───────────────────────────────────────────────────────────────────

pub async fn serve(cfg: ServerConfig) -> Result<(), tonic::transport::Error> {
    info!(addr = %cfg.addr, "KonfigService gRPC server starting");

    let server = KonfigServer {
        cache: cfg.cache,
        secret_cache: cfg.secret_cache,
        kube_client: cfg.kube_client,
        namespace_broadcasts: Arc::new(DashMap::new()),
        namespace_replay_buffers: Arc::new(DashMap::new()),
        secret_namespace_broadcasts: Arc::new(DashMap::new()),
    };
    let svc = KonfigServiceServer::new(server);

    let mut builder = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(20)))
        .http2_keepalive_timeout(Some(std::time::Duration::from_secs(10)));

    if let Some(reporter) = cfg.health_reporter {
        let health_svc = tonic_health::pb::health_server::HealthServer::new(
            tonic_health::server::HealthService::from_health_reporter(reporter),
        );
        builder
            .add_service(health_svc)
            .add_service(svc)
            .serve(cfg.addr)
            .await
    } else {
        builder.add_service(svc).serve(cfg.addr).await
    }
}

// ── Shared helper ─────────────────────────────────────────────────────────────

/// Build a `Config` proto message from a `ConfigSnapshot`.
pub(crate) fn snapshot_to_proto(snap: &crate::types::ConfigSnapshot) -> Config {
    Config {
        namespace: snap.namespace.clone(),
        name: snap.name.clone(),
        schema_version: snap.schema_version,
        content_json: snap.content_json(),
        resource_version: snap.resource_version.clone(),
        age_ms: snap.loaded_at.elapsed().as_millis() as i64,
        stale_since_ms: snap
            .stale_since
            .map(|t| t.elapsed().as_millis() as i64)
            .unwrap_or(-1),
    }
}
