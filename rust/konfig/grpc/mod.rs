//! gRPC server for `konfig.v1.KonfigService`.
//!
//! Implements the tonic-generated `KonfigService` trait on `KonfigServer`.
//! All message types are Protobuf (standard tonic codec, no custom codec).

pub mod apply;
pub mod get;

use std::net::SocketAddr;
use std::sync::Arc;

use kube::Client;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::cache::ConfigCache;
use crate::proto::konfig_service_server::{KonfigService, KonfigServiceServer};
use crate::proto::{
    ApplyRequest, ApplyResponse, Config, ConfigEvent, GetAllRequest, GetRequest, SubscribeRequest,
};

// ── Server config ─────────────────────────────────────────────────────────────

pub struct ServerConfig {
    pub addr: SocketAddr,
    pub cache: Arc<ConfigCache>,
    pub kube_client: Client,
}

// ── KonfigServer ──────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct KonfigServer {
    pub(crate) cache: Arc<ConfigCache>,
    pub(crate) kube_client: Client,
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
        _request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        Err(Status::unimplemented("Subscribe is implemented in Phase 2B"))
    }
}

// ── Startup ───────────────────────────────────────────────────────────────────

pub async fn serve(cfg: ServerConfig) -> Result<(), tonic::transport::Error> {
    info!(addr = %cfg.addr, "KonfigService gRPC server starting");

    let server = KonfigServer { cache: cfg.cache, kube_client: cfg.kube_client };

    tonic::transport::Server::builder()
        .add_service(KonfigServiceServer::new(server))
        .serve(cfg.addr)
        .await
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
    }
}
