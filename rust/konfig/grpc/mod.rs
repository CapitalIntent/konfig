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

/// Initial capacity for per-namespace `DashMap`s in `serve()`.  Typical pod
/// fans out across 10–50 namespaces; 64 is the next power of two and
/// eliminates the early `RawTable::reserve_rehash` calls (~10 ms self-CPU
/// hit observed in pyroscope profile CU-86aj360ae) before the maps reach
/// steady state.
const NAMESPACE_MAP_INITIAL_CAPACITY: usize = 64;

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
    /// Optional HTTP/2 `SETTINGS_INITIAL_WINDOW_SIZE` override (bench knob
    /// for CU-86aj37q7a). `None` = leave tonic default (65,535). Raising
    /// reduces `h2::Prioritize::poll_complete` self-CPU on large Subscribe
    /// fan-outs but increases per-stream RAM — sweep before changing the
    /// default.
    pub h2_initial_window_bytes: Option<u32>,
    /// Optional HTTP/2 `SETTINGS_MAX_CONCURRENT_STREAMS` override (bench
    /// knob for CU-86aj37q7a). `None` = leave tonic default (unlimited).
    /// Lower caps protect the server from a single client hogging streams;
    /// raising can help when many Subscribe RPCs multiplex on one
    /// connection. Sweep before changing the default.
    pub h2_max_concurrent_streams: Option<u32>,
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
        // The drain flag is a standalone boolean — no piggy-backed data
        // ordering required.  Acquire pairs with the Release store in
        // `begin_drain` so a thread that observes `true` is guaranteed to
        // see any writes that happened-before the drain commenced.
        // Acquire is strictly cheaper than the previous SeqCst on every
        // RPC entry (`check_drain` calls this load on every gRPC handler).
        self.draining.load(Ordering::Acquire)
    }

    /// Flip the drain flag and wake every active Subscribe stream.  Idempotent
    /// — repeated calls are a no-op.
    pub fn begin_drain(&self) {
        if self
            .draining
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
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
    // Acquire-only load — pairs with the Release/AcqRel writer in
    // `begin_drain`/serve.  Runs on every RPC entry, so the cheaper
    // ordering matters.
    if draining.load(Ordering::Acquire) {
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

/// Apply optional HTTP/2 `SETTINGS` overrides to the tonic server builder
/// (bench knob for CU-86aj37q7a). Each argument is `None` by default — only
/// the values the operator passed on the CLI are pushed through; tonic /
/// h2 keep their own defaults otherwise. Raising these reduces
/// `h2::Prioritize::poll_complete` self-CPU on large Subscribe fan-outs but
/// increases per-stream RAM — sweep before changing the default.
fn apply_h2_overrides(
    mut builder: tonic::transport::Server,
    initial_window_bytes: Option<u32>,
    max_concurrent_streams: Option<u32>,
) -> tonic::transport::Server {
    if let Some(window) = initial_window_bytes {
        builder = builder.initial_stream_window_size(Some(window));
    }
    if let Some(max_streams) = max_concurrent_streams {
        builder = builder.max_concurrent_streams(Some(max_streams));
    }
    builder
}

pub async fn serve(cfg: ServerConfig) -> Result<(), tonic::transport::Error> {
    info!(addr = %cfg.addr, "KonfigService gRPC server starting");

    let namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<Arc<BroadcastFrame>>>> =
        Arc::new(DashMap::with_capacity(NAMESPACE_MAP_INITIAL_CAPACITY));
    let namespace_replay_buffers: Arc<DashMap<String, ReplayBuffer>> =
        Arc::new(DashMap::with_capacity(NAMESPACE_MAP_INITIAL_CAPACITY));
    let watcher_handles: Arc<DashMap<String, JoinHandle<()>>> =
        Arc::new(DashMap::with_capacity(NAMESPACE_MAP_INITIAL_CAPACITY));
    let idle_since: Arc<DashMap<String, Instant>> =
        Arc::new(DashMap::with_capacity(NAMESPACE_MAP_INITIAL_CAPACITY));

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
                // Catch panics inside the sweep so a transient
                // prometheus-internal panic, lock poison, etc. does not
                // silently kill the sampler for the lifetime of the pod
                // (which would freeze the konfig_stale_seconds /
                // konfig_replay_buffer_depth gauges).
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    // Pre-collect keys (cheap String clones) before iterating
                    // so we don't hold per-shard DashMap read locks across the
                    // inner `Mutex::lock()` call below. Concurrent writers
                    // (watchers pushing new replay entries / event timestamps)
                    // would otherwise block on this sampler tick. CU-86aj3m24w.
                    let replay_keys: Vec<String> = replay_buffers_for_sampler
                        .iter()
                        .map(|e| e.key().clone())
                        .collect();
                    for ns in &replay_keys {
                        if let Some(buf_ref) = replay_buffers_for_sampler.get(ns) {
                            let depth = buf_ref
                                .value()
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .len();
                            REPLAY_BUFFER_DEPTH
                                .with_label_values(&[ns.as_str()])
                                .set(depth as f64);
                        }
                    }
                    // konfig_stale_seconds: seconds since last event per namespace.
                    // None = cold start (no event received yet) → publish 0 (fresh).
                    // Same pattern: collect keys first, release the iter, then
                    // re-`get()` per key so writers aren't blocked by us.
                    let stale_keys: Vec<String> = last_event_at_for_sampler
                        .iter()
                        .map(|e| e.key().clone())
                        .collect();
                    for ns in &stale_keys {
                        if let Some(v_ref) = last_event_at_for_sampler.get(ns) {
                            let secs = v_ref.value().elapsed_secs().unwrap_or(0.0);
                            STALE_SECONDS.with_label_values(&[ns.as_str()]).set(secs);
                        }
                    }
                }));
                if result.is_err() {
                    warn!("metric sampler: tick panicked — continuing loop");
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

    builder = apply_h2_overrides(
        builder,
        cfg.h2_initial_window_bytes,
        cfg.h2_max_concurrent_streams,
    );

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
        // Release store — pairs with Acquire loads in `is_draining` /
        // `check_drain` so subsequent Subscribe readers see a consistent
        // happens-before edge with the drain commencement.
        draining.store(true, Ordering::Release);
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
        // Clone the cached &str into the proto String; the underlying
        // serde_json::to_string ran exactly once per snapshot, not per RPC.
        content_json: snap.content_json().to_owned(),
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

    /// `apply_h2_overrides` is a pure builder pass-through — it must be safe
    /// to call with both arguments `None` (the default startup path) and with
    /// only one set (partial bench override). The tonic `Server` builder has
    /// no public getters for the SETTINGS values, so this test exercises the
    /// signature + compile contract only — we just verify it returns a usable
    /// builder that we can chain further calls on.
    ///
    /// **Assurance level: compile + chain.** A wire-level SETTINGS-frame
    /// check (open a TCP socket, accept a tonic server's connection, parse
    /// the SETTINGS frame with an `h2` client, assert
    /// `SETTINGS_INITIAL_WINDOW_SIZE` and `SETTINGS_MAX_CONCURRENT_STREAMS`
    /// match the overrides) would require pulling `h2` into dev-deps and
    /// regenerating the Bazel `cargo-bazel-lock.json`. Deferred to a future
    /// integration test (see `apply_h2_overrides_builder_binds_and_serves`
    /// below, which validates the next-best signal: the configured builder
    /// successfully binds a listener and completes an HTTP/2 handshake).
    #[test]
    fn apply_h2_overrides_compiles_and_chains() {
        let b = tonic::transport::Server::builder();
        let _ = apply_h2_overrides(b, None, None);
        let b = tonic::transport::Server::builder();
        let _ = apply_h2_overrides(b, Some(1_048_576), None);
        let b = tonic::transport::Server::builder();
        let _ = apply_h2_overrides(b, None, Some(2048));
        let b = tonic::transport::Server::builder();
        let _ = apply_h2_overrides(b, Some(1_048_576), Some(2048));
    }

    /// End-to-end binding smoke: build a `tonic::transport::Server` with
    /// `apply_h2_overrides`, mount a `tonic_health` service, bind to an
    /// ephemeral port, and confirm a tonic `Channel` can connect and
    /// complete an HTTP/2 preface + SETTINGS exchange.
    ///
    /// This catches builder-pipeline regressions that the
    /// `_compiles_and_chains` test cannot — any mutation that breaks the
    /// h2 layer (e.g. an ICE-style breakage from a future tonic API churn)
    /// fails here, not at compile time. Wire-level SETTINGS-value
    /// introspection itself is still deferred (see the
    /// `_compiles_and_chains` docstring): tonic's `Channel` will accept
    /// nonsense window sizes silently, so this test cannot guarantee the
    /// *exact* SETTINGS values reached the wire — only that the builder is
    /// not totally broken.
    #[tokio::test]
    async fn apply_h2_overrides_builder_binds_and_serves() {
        use tokio::net::TcpListener;
        use tonic::transport::Server as TonicServer;

        // Bind first so the kernel hands us a free port atomically, then
        // immediately drop the listener — `tonic::Server::serve_with_shutdown`
        // wants a `SocketAddr` it can bind itself. Inherent TOCTOU here, but
        // on a CI host with no port pressure this is the standard
        // "ephemeral free port" trick used widely in the tonic ecosystem.
        let addr: SocketAddr = {
            let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            l.local_addr().expect("local_addr")
        };

        // Health reporter is the simplest non-trivial service we can mount
        // without bringing up the full KonfigServer (which needs a kube
        // client). Exercises the same `add_service` path that `serve()` uses
        // in prod.
        let (reporter, health_server) = tonic_health::server::health_reporter();
        reporter
            .set_service_status("ping", tonic_health::ServingStatus::Serving)
            .await;

        let mut builder = apply_h2_overrides(TonicServer::builder(), Some(1_048_576), Some(2_048));

        // Drive the server in the background; shutdown when the oneshot fires.
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_handle = tokio::spawn(async move {
            builder
                .add_service(health_server)
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        // A real tonic `Channel` connect performs the HTTP/2 preface +
        // initial SETTINGS exchange. Retry briefly because the server task
        // may not have reached `bind()` yet when we connect.
        let channel = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
                    .expect("endpoint")
                    .connect()
                    .await
                {
                    Ok(ch) => break ch,
                    Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
                }
            }
        })
        .await
        .expect("h2 handshake must succeed within 3s with valid overrides");
        // Drop the channel before shutting down so the server's graceful
        // shutdown does not race with an in-flight handshake.
        drop(channel);

        let _ = shutdown_tx.send(());
        let res = tokio::time::timeout(Duration::from_secs(2), server_handle)
            .await
            .expect("server task must terminate within 2s after shutdown");
        res.expect("join").expect("server must shut down cleanly");
    }

    /// Jitter must keep the output within ±25 % of the input base AND must
    /// actually vary across calls — a constant return (e.g. someone replaces
    /// `nanos % span` with `0`) would silently satisfy the band check alone.
    ///
    /// Test design (no sleep!):
    ///   - The previous implementation slept 1 µs per iteration to vary the
    ///     `SystemTime::now().subsec_nanos()` entropy source. Microsecond
    ///     sleeps are unreliable on slow CI runners (sleep granularity can
    ///     exceed the requested duration), occasionally producing identical
    ///     samples and a falsely-flaky "no variation" assertion.
    ///   - This rewrite collects 4 096 samples back-to-back. `subsec_nanos()`
    ///     advances by at least one tick between any two `now()` calls on
    ///     every supported platform; with no sleep we still see ≥ hundreds
    ///     of distinct nanos values across the loop, which is plenty to
    ///     exercise the `nanos % span` distribution.
    #[test]
    fn jittered_retry_ms_stays_within_band() {
        use std::collections::HashSet;
        let base = 200u64;
        let mut seen = HashSet::new();
        for _ in 0..4_096 {
            let v = jittered_retry_ms(base);
            assert!(
                (150..=250).contains(&v),
                "jittered_retry_ms({base}) = {v} outside ±25 % band",
            );
            seen.insert(v);
        }
        // Must observe meaningful spread — a constant-return regression
        // (e.g. `offset = 0`) would only ever produce `base - jitter_range`.
        // Demanding > 10 distinct samples out of 4 096 is far above the
        // false-positive floor while still catching collapse to a constant.
        assert!(
            seen.len() > 10,
            "jitter must vary across calls; only {} distinct values in 4 096 samples",
            seen.len(),
        );
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

    /// `check_drain` returns `Ok(())` when not draining and `UNAVAILABLE`
    /// once the flag is set — with a human-readable "draining" message so
    /// operators can distinguish drain-aborts from other `UNAVAILABLE`
    /// returns in client logs.
    #[test]
    fn check_drain_returns_unavailable_when_draining() {
        let flag = AtomicBool::new(false);
        // Exhaustively match instead of `is_ok()` — a regression that wraps
        // `Ok(())` in some other variant would still satisfy `is_ok()`
        // after a future refactor but fail this match.
        match check_drain(&flag) {
            Ok(()) => {}
            Err(s) => panic!("expected Ok when draining=false, got Err({s:?})"),
        }

        flag.store(true, Ordering::Release);
        let err = check_drain(&flag).expect_err("must error when draining");
        assert_eq!(
            err.code(),
            tonic::Code::Unavailable,
            "drain rejects must use UNAVAILABLE so clients reconnect to a healthy pod",
        );
        assert!(
            err.message().contains("draining"),
            "status message must mention 'draining' for operator log greppability; got: {:?}",
            err.message(),
        );
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

    // ── snapshot_to_proto ─────────────────────────────────────────────────────

    /// `snapshot_to_proto` runs on every `Get` + `Subscribe` response (CU-86aj3m14k).
    /// Verify every protobuf field is populated from the right `ConfigSnapshot`
    /// source and that the cached `content_json` is propagated correctly.
    #[test]
    fn snapshot_to_proto_populates_namespace_name_and_resource_version() {
        let snap = crate::types::ConfigSnapshot {
            namespace: "ns-a".to_string(),
            name: "cfg-x".to_string(),
            resource_version: "1234".to_string(),
            schema_version: 7,
            ..Default::default()
        };
        let proto = snapshot_to_proto(&snap);
        assert_eq!(proto.namespace, "ns-a");
        assert_eq!(proto.name, "cfg-x");
        assert_eq!(proto.resource_version, "1234");
        assert_eq!(proto.schema_version, 7);
    }

    #[test]
    fn snapshot_to_proto_propagates_memoised_content_json() {
        let snap = crate::types::ConfigSnapshot {
            content: serde_json::json!({"k": "v", "n": 42}),
            ..Default::default()
        };
        // Force-warm the cache to verify the proto field receives the memoised
        // string, not a fresh serialisation.
        let warmed = snap.content_json().to_owned();
        let proto = snapshot_to_proto(&snap);
        assert_eq!(proto.content_json, warmed);
        // Validate the JSON shape so a future refactor that changes the encoder
        // (e.g. canonical-form ordering) fails this test loudly.
        let v: serde_json::Value = serde_json::from_str(&proto.content_json).expect("valid json");
        assert_eq!(v["k"], "v");
        assert_eq!(v["n"], 42);
    }

    #[test]
    fn snapshot_to_proto_emits_non_negative_age_ms() {
        let snap = crate::types::ConfigSnapshot::default();
        // A freshly constructed snapshot's `loaded_at` is `Instant::now()`.
        // The proto's `age_ms` is the elapsed time at conversion. Both the
        // construct + convert happen in the same test stack, so age_ms is
        // either 0 (fast machine) or a small positive integer — never < 0.
        let proto = snapshot_to_proto(&snap);
        assert!(
            proto.age_ms >= 0,
            "age_ms must be non-negative, got {}",
            proto.age_ms
        );
    }

    #[test]
    fn snapshot_to_proto_stale_since_sentinel_minus_one_when_fresh() {
        let snap = crate::types::ConfigSnapshot {
            stale_since: None,
            ..Default::default()
        };
        let proto = snapshot_to_proto(&snap);
        assert_eq!(
            proto.stale_since_ms, -1,
            "fresh (None) `stale_since` must emit the -1 sentinel"
        );
    }

    #[test]
    fn snapshot_to_proto_stale_since_non_negative_when_stale() {
        // Construct a snapshot whose `stale_since` was set in the past so
        // the elapsed conversion produces a non-negative i64. Use a tiny
        // delay (10ms) inside the same test to keep timing deterministic
        // without sleeping the whole suite.
        let stale_anchor = Instant::now();
        std::thread::sleep(Duration::from_millis(10));
        let snap = crate::types::ConfigSnapshot {
            stale_since: Some(stale_anchor),
            ..Default::default()
        };
        let proto = snapshot_to_proto(&snap);
        assert!(
            proto.stale_since_ms >= 0,
            "stale_since_ms must be non-negative when stale_since is Some, got {}",
            proto.stale_since_ms
        );
        // The 10ms sleep guarantees a strictly-positive elapsed time on
        // any reasonable runner.
        assert!(
            proto.stale_since_ms > 0,
            "stale_since_ms must be > 0 after a 10ms sleep, got {}",
            proto.stale_since_ms
        );
    }
}
