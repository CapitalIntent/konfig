//! gRPC server for `konfig.v1.KonfigService`.
//!
//! Implements the tonic-generated `KonfigService` trait on `KonfigServer`.
//! All message types are Protobuf (standard tonic codec, no custom codec).
//!
//! # Graceful drain (SIGTERM handling)
//!
//! `KonfigServer` carries a `draining: Arc<AtomicBool>` flag.  When set:
//!   - new `Apply`/`Get`/`GetAll`/`Subscribe`/secret RPCs return `UNAVAILABLE`
//!     so clients reconnect to a healthy pod via DNS / service mesh.
//!   - the gRPC health endpoint flips to `NOT_SERVING` so K8s readiness probes
//!     immediately remove the pod from the Service endpoint list.
//!   - the per-subscriber drain notifier (`drain_notify`) is triggered so
//!     existing Subscribe streams close cleanly (server-side `Ok(())`) rather
//!     than dying mid-stream when the listener is dropped.
//!
//! The drain sequence is owned by the caller of `serve`: pass a future to
//! `ServerConfig::shutdown_signal` that resolves on SIGTERM, and `serve` will
//! orchestrate the transitions then call `Server::serve_with_shutdown`.

pub mod apply;
pub mod get;
pub mod revert;
pub mod secret_apply;
pub mod secret_get;
pub mod subscribe;
pub mod subscribe_secrets;
pub mod tls;

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use kube::Client;
use tokio::sync::{Notify, broadcast};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::cache::ConfigCache;
use crate::grpc::subscribe::{BroadcastFrame, ReplayBuffer, gc_task};
use crate::metrics::{LastEventAtMap, REPLAY_BUFFER_DEPTH, STALE_SECONDS};
use crate::proto::{
    ApplyRequest, ApplyResponse, ApplySecretRequest, ApplySecretResponse, Config, ConfigEvent,
    GetAllRequest, GetAllSecretsRequest, GetRequest, GetSecretRequest, RevertRequest,
    RevertResponse, SecretEvent, SecretResponse, SubscribeRequest, SubscribeSecretsRequest,
    konfig_service_server::{KonfigService, KonfigServiceServer},
};
use crate::secret_cache::SecretCache;

/// Maximum time we wait for in-flight RPCs to complete after SIGTERM before
/// forcing the gRPC server to stop accepting connections.
pub const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

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
    /// Shared broadcast senders for secret events, keyed by namespace.
    /// Populated by `SecretWatcher::spawn_all` before `serve` is called so
    /// that `SubscribeSecrets` subscribers can attach at server startup.
    pub secret_namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<SecretEvent>>>,
    /// Per-namespace freshness tracker.  Watchers touch the entry for their
    /// namespace on every event; the background sampler in `serve` reads it
    /// every 5 s and updates the `konfig_stale_seconds` gauge.
    pub last_event_at_map: LastEventAtMap,
    /// Future that resolves when the process receives SIGTERM (or otherwise
    /// wants to drain).  When it resolves `serve` flips the draining flag,
    /// closes active Subscribe streams, marks the health endpoint NOT_SERVING,
    /// then waits up to `DRAIN_TIMEOUT` before calling `serve_with_shutdown`.
    ///
    /// When `None` the server never drains (test/CLI use).
    pub shutdown_signal: Option<ShutdownSignal>,
    /// Optional TLS configuration. `Some` engages mTLS — every client must
    /// present a cert signed by the configured CA. `None` runs in plaintext
    /// (integration tests + `--tls=false` local dev).
    pub tls_config: Option<tonic::transport::ServerTlsConfig>,
}

/// Type-erased shutdown future.  Boxed so the field doesn't push a generic
/// parameter onto `ServerConfig`.
pub type ShutdownSignal = std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

// ── KonfigServer ──────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct KonfigServer {
    pub(crate) cache: Arc<ConfigCache>,
    pub(crate) secret_cache: Arc<SecretCache>,
    pub(crate) kube_client: Client,
    /// One broadcast sender per namespace — shared across all Config subscribers
    /// for that namespace.  A single kube watcher drives the sender; each
    /// subscriber gets a `Receiver` clone (O(1) fan-out).
    /// Events are wrapped in `Arc` so broadcast clones are reference-count
    /// increments only — serialisation happens once per apply, not per subscriber.
    pub(crate) namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<Arc<BroadcastFrame>>>>,
    /// Per-namespace replay buffer for the `resume_resource_version` reconnect
    /// path.  Holds the last `REPLAY_BUFFER_SIZE` events so reconnecting clients
    /// can catch up without opening a new kube watch.
    pub(crate) namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>>,
    /// JoinHandles for the per-namespace kube watcher tasks.  The GC task uses
    /// these to abort idle watchers and prevent K8s watch connection leaks.
    pub(crate) watcher_handles: Arc<DashMap<String, JoinHandle<()>>>,
    /// Separate broadcast map for secret events — keyed by namespace.
    /// Intentionally distinct from `namespace_broadcasts` so Config and Secret
    /// streams do not interfere.
    pub(crate) secret_namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<SecretEvent>>>,
    /// `true` once `begin_drain` has been called.  Handlers consult this on
    /// entry and short-circuit with `UNAVAILABLE` so the LB drops them onto a
    /// healthy peer.
    pub(crate) draining: Arc<AtomicBool>,
    /// `Notify` triggered by `begin_drain`.  Active subscribe streams `await`
    /// this and exit cleanly (`Ok(())`) when notified.
    pub(crate) drain_notify: Arc<Notify>,
}

impl KonfigServer {
    /// Returns `true` once the server has begun draining (post-SIGTERM).
    pub fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }

    /// Flip the drain flag and wake every active Subscribe stream.  Idempotent
    /// — repeated calls are a no-op.
    pub fn begin_drain(&self) {
        if self
            .draining
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            info!("Drain begun — closing active subscribers and rejecting new RPCs");
            self.drain_notify.notify_waiters();
        }
    }

    /// Returns a clone of the per-subscriber drain notifier so handlers can
    /// `notified().await` to detect drain.
    pub(crate) fn drain_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.drain_notify)
    }
}

/// Helper used at the top of each RPC handler — returns an `Err(Status::unavailable)`
/// when the server is draining so the client reconnects to a healthy pod.
fn check_drain(draining: &AtomicBool) -> Result<(), Status> {
    if draining.load(Ordering::SeqCst) {
        Err(Status::unavailable("server draining"))
    } else {
        Ok(())
    }
}

#[tonic::async_trait]
impl KonfigService for KonfigServer {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<Config>, Status> {
        check_drain(&self.draining)?;
        get::handle_get(Arc::clone(&self.cache), request.into_inner()).await
    }

    type GetAllStream = ReceiverStream<Result<Config, Status>>;

    async fn get_all(
        &self,
        request: Request<GetAllRequest>,
    ) -> Result<Response<Self::GetAllStream>, Status> {
        check_drain(&self.draining)?;
        get::handle_get_all(Arc::clone(&self.cache), request.into_inner()).await
    }

    async fn apply(
        &self,
        request: Request<ApplyRequest>,
    ) -> Result<Response<ApplyResponse>, Status> {
        check_drain(&self.draining)?;
        apply::handle_apply(self.kube_client.clone(), request.into_inner()).await
    }

    async fn revert(
        &self,
        request: Request<RevertRequest>,
    ) -> Result<Response<RevertResponse>, Status> {
        revert::handle_revert(self.kube_client.clone(), request.into_inner()).await
    }

    type SubscribeStream = ReceiverStream<Result<ConfigEvent, Status>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        check_drain(&self.draining)?;
        subscribe::handle_subscribe(
            Arc::clone(&self.cache),
            self.kube_client.clone(),
            Arc::clone(&self.namespace_broadcasts),
            Arc::clone(&self.namespace_replay_buffers),
            Arc::clone(&self.watcher_handles),
            self.drain_notify(),
            request.into_inner(),
        )
        .await
    }

    // ── Secret RPCs ───────────────────────────────────────────────────────────

    async fn get_secret(
        &self,
        request: Request<GetSecretRequest>,
    ) -> Result<Response<SecretResponse>, Status> {
        check_drain(&self.draining)?;
        secret_get::handle_get_secret(Arc::clone(&self.secret_cache), request.into_inner()).await
    }

    type GetAllSecretsStream = ReceiverStream<Result<SecretResponse, Status>>;

    async fn get_all_secrets(
        &self,
        request: Request<GetAllSecretsRequest>,
    ) -> Result<Response<Self::GetAllSecretsStream>, Status> {
        check_drain(&self.draining)?;
        secret_get::handle_get_all_secrets(Arc::clone(&self.secret_cache), request.into_inner())
            .await
    }

    async fn apply_secret(
        &self,
        request: Request<ApplySecretRequest>,
    ) -> Result<Response<ApplySecretResponse>, Status> {
        check_drain(&self.draining)?;
        secret_apply::handle_apply_secret(self.kube_client.clone(), request.into_inner()).await
    }

    type SubscribeSecretsStream = ReceiverStream<Result<SecretEvent, Status>>;

    async fn subscribe_secrets(
        &self,
        request: Request<SubscribeSecretsRequest>,
    ) -> Result<Response<Self::SubscribeSecretsStream>, Status> {
        check_drain(&self.draining)?;
        subscribe_secrets::handle_subscribe_secrets(
            self.kube_client.clone(),
            Arc::clone(&self.secret_cache),
            Arc::clone(&self.secret_namespace_broadcasts),
            self.drain_notify(),
            request.into_inner(),
        )
        .await
    }
}

// ── Startup ───────────────────────────────────────────────────────────────────

pub async fn serve(cfg: ServerConfig) -> Result<(), tonic::transport::Error> {
    info!(addr = %cfg.addr, "KonfigService gRPC server starting");

    let namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<Arc<BroadcastFrame>>>> =
        Arc::new(DashMap::new());
    let namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>> = Arc::new(DashMap::new());
    let watcher_handles: Arc<DashMap<String, JoinHandle<()>>> = Arc::new(DashMap::new());
    let idle_since: Arc<DashMap<String, Instant>> = Arc::new(DashMap::new());

    // Spawn background GC task — cleans up idle namespace watchers to prevent
    // K8s watch connection leaks when all subscribers disconnect.
    tokio::spawn(gc_task(
        Arc::clone(&namespace_broadcasts),
        Arc::clone(&namespace_replay_buffers),
        Arc::clone(&watcher_handles),
        Arc::clone(&idle_since),
    ));

    // Spawn background metric sampler — samples replay buffer depth and
    // watcher freshness every 5 s.  Runs off the hot path to avoid lock
    // contention during event delivery.
    {
        let replay_buffers_for_sampler = Arc::clone(&namespace_replay_buffers);
        let last_event_at_for_sampler = Arc::clone(&cfg.last_event_at_map);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            loop {
                interval.tick().await;
                for entry in replay_buffers_for_sampler.iter() {
                    let depth = entry.value().lock().expect("replay buffer poisoned").len();
                    REPLAY_BUFFER_DEPTH
                        .with_label_values(&[entry.key()])
                        .set(depth as f64);
                }
                // konfig_stale_seconds: seconds since last event per namespace.
                // None = cold start (no event received yet) → publish 0 (fresh).
                for entry in last_event_at_for_sampler.iter() {
                    let secs = entry.value().elapsed_secs().unwrap_or(0.0);
                    STALE_SECONDS.with_label_values(&[entry.key()]).set(secs);
                }
            }
        });
    }

    let draining = Arc::new(AtomicBool::new(false));
    let drain_notify = Arc::new(Notify::new());

    let server = KonfigServer {
        cache: cfg.cache,
        secret_cache: cfg.secret_cache,
        kube_client: cfg.kube_client,
        namespace_broadcasts,
        namespace_replay_buffers,
        watcher_handles,
        secret_namespace_broadcasts: cfg.secret_namespace_broadcasts,
        draining: Arc::clone(&draining),
        drain_notify: Arc::clone(&drain_notify),
    };
    let svc = KonfigServiceServer::new(server);

    let mut builder = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(20)))
        .http2_keepalive_timeout(Some(std::time::Duration::from_secs(10)));

    if let Some(tls) = cfg.tls_config {
        builder = builder.tls_config(tls)?;
    }

    // Compose the shutdown future that `serve_with_shutdown` waits on.
    //
    // When `shutdown_signal` resolves we:
    //   1. flip the `draining` flag — new RPCs immediately fail UNAVAILABLE
    //   2. notify all active Subscribe streams so they close cleanly
    //   3. mark the health endpoint NOT_SERVING (K8s readiness probe fails)
    //   4. wait up to `DRAIN_TIMEOUT` for in-flight RPCs to finish
    // The future then resolves and tonic stops accepting new connections.
    let health_reporter_for_drain = cfg.health_reporter.clone();
    let shutdown_future = async move {
        let Some(signal) = cfg.shutdown_signal else {
            // No shutdown signal supplied — never resolve; tonic runs forever.
            std::future::pending::<()>().await;
            return;
        };
        signal.await;
        info!("Shutdown signal received — beginning drain");
        draining.store(true, Ordering::SeqCst);
        drain_notify.notify_waiters();

        if let Some(reporter) = health_reporter_for_drain {
            reporter
                .set_not_serving::<KonfigServiceServer<KonfigServer>>()
                .await;
            info!("Health endpoint: NOT_SERVING");
        }

        // Give in-flight RPCs DRAIN_TIMEOUT to wind down before tonic stops
        // accepting connections.  We just sleep — handlers either complete
        // naturally (Apply, Get) or were notified above (Subscribe).
        info!(
            timeout_s = DRAIN_TIMEOUT.as_secs(),
            "Waiting for in-flight RPCs to drain"
        );
        tokio::time::sleep(DRAIN_TIMEOUT).await;
        warn!("Drain timeout elapsed — forcing server shutdown");
    };

    if let Some(reporter) = cfg.health_reporter {
        let health_svc = tonic_health::pb::health_server::HealthServer::new(
            tonic_health::server::HealthService::from_health_reporter(reporter),
        );
        builder
            .add_service(health_svc)
            .add_service(svc)
            .serve_with_shutdown(cfg.addr, shutdown_future)
            .await
    } else {
        builder
            .add_service(svc)
            .serve_with_shutdown(cfg.addr, shutdown_future)
            .await
    }
}

// ── Shared helper ─────────────────────────────────────────────────────────────

/// Apply ±25% jitter to a base retry delay (ms) to break lockstep retries
/// across N clients racing on the same Config / Secret resourceVersion.
///
/// Uses `SystemTime` nanos for the jitter entropy source — fine for retry
/// spread, no extra dep, no shared state.
pub(crate) fn jittered_retry_ms(base_ms: u64) -> u64 {
    if base_ms == 0 {
        return 0;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_nanos()))
        .unwrap_or(0);
    let jitter_range = base_ms / 4; // ±25%
    let span = 2u64.saturating_mul(jitter_range).saturating_add(1);
    let offset = nanos % span;
    base_ms.saturating_sub(jitter_range).saturating_add(offset)
}

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

#[cfg(test)]
mod tests {
    use super::*;

    /// Jitter must keep the output within ±25 % of the input base.
    #[test]
    fn jittered_retry_ms_stays_within_band() {
        let base = 200u64;
        // 16 samples to give the SystemTime entropy a chance to vary.
        for _ in 0..16 {
            let v = jittered_retry_ms(base);
            assert!(
                (150..=250).contains(&v),
                "jittered_retry_ms({base}) = {v} outside ±25 % band",
            );
            std::thread::sleep(std::time::Duration::from_micros(1));
        }
    }

    #[test]
    fn jittered_retry_ms_zero_passthrough() {
        assert_eq!(jittered_retry_ms(0), 0);
    }

    /// `is_draining` flips after `begin_drain` and the notify wakes waiters.
    #[tokio::test]
    async fn begin_drain_flips_flag_and_notifies_waiters() {
        let server = test_server();
        assert!(!server.is_draining());

        // Subscribe to the drain notifier *before* triggering — `notify_waiters`
        // only wakes waiters that are already parked.
        let notify = server.drain_notify();
        let waiter = tokio::spawn(async move { notify.notified().await });
        // Yield once so the waiter actually parks before we notify.
        tokio::task::yield_now().await;

        server.begin_drain();
        assert!(server.is_draining());

        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter must wake within 1s")
            .expect("task panicked");
    }

    /// `begin_drain` is idempotent — calling twice does not re-notify.
    #[tokio::test]
    async fn begin_drain_is_idempotent() {
        let server = test_server();
        server.begin_drain();
        server.begin_drain();
        assert!(server.is_draining());
    }

    /// `check_drain` returns `UNAVAILABLE` once the flag is set.
    #[test]
    fn check_drain_returns_unavailable_when_draining() {
        let flag = AtomicBool::new(false);
        assert!(check_drain(&flag).is_ok());
        flag.store(true, Ordering::SeqCst);
        let err = check_drain(&flag).expect_err("must error when draining");
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    /// While draining the `Get` RPC short-circuits with UNAVAILABLE before
    /// touching the cache — clients reconnect to a healthy pod via DNS / LB.
    #[tokio::test]
    async fn draining_get_rpc_returns_unavailable() {
        let server = test_server();
        server.begin_drain();
        let req = Request::new(GetRequest {
            namespace: "default".into(),
            name: "any".into(),
        });
        let err = server.get(req).await.expect_err("must reject during drain");
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    /// While draining the `Apply` RPC short-circuits with UNAVAILABLE before
    /// hitting the kube API.  The dummy client used in this test has no
    /// reachable API server — so the only way this passes is if `check_drain`
    /// fires before the kube call.
    #[tokio::test]
    async fn draining_apply_rpc_returns_unavailable() {
        let server = test_server();
        server.begin_drain();
        let req = Request::new(ApplyRequest {
            namespace: "default".into(),
            name: "cfg".into(),
            yaml_content: "schema_version: 1\n".into(),
        });
        let err = server
            .apply(req)
            .await
            .expect_err("must reject during drain");
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    /// While draining the `Subscribe` RPC short-circuits with UNAVAILABLE so
    /// new clients are bounced onto a healthy peer.
    #[tokio::test]
    async fn draining_subscribe_rpc_returns_unavailable() {
        let server = test_server();
        server.begin_drain();
        let req = Request::new(SubscribeRequest {
            namespace: "default".into(),
            names: Vec::new(),
            resume_resource_version: String::new(),
        });
        let err = server
            .subscribe(req)
            .await
            .expect_err("must reject new subscribers during drain");
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    fn test_server() -> KonfigServer {
        KonfigServer {
            cache: Arc::new(ConfigCache::new(crate::types::ConfigSnapshot::default())),
            secret_cache: Arc::new(SecretCache::new()),
            kube_client: dummy_client(),
            namespace_broadcasts: Arc::new(DashMap::new()),
            namespace_replay_buffers: Arc::new(DashMap::new()),
            watcher_handles: Arc::new(DashMap::new()),
            secret_namespace_broadcasts: Arc::new(DashMap::new()),
            draining: Arc::new(AtomicBool::new(false)),
            drain_notify: Arc::new(Notify::new()),
        }
    }

    /// Build a `kube::Client` from the in-tree default config.  Never actually
    /// connects — the tests above only touch the drain plumbing.
    fn dummy_client() -> kube::Client {
        let cfg = kube::Config::new("http://127.0.0.1:0".parse().expect("valid URL"));
        kube::Client::try_from(cfg).expect("infallible — only constructs HTTP client")
    }
}
