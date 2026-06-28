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
pub mod audit;
pub mod authz;
pub mod get;
pub mod identity;
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

use crate::acl::{AclSynced, AclTable};
use crate::cache::ConfigCache;
use crate::grpc::authz::{Mode as AuthzMode, Verb};
use crate::grpc::subscribe::{
    MAX_BROADCAST_SHARDS, MIN_BROADCAST_SHARDS, ReplayBuffer, ShardSet, gc_task,
};
use crate::metrics::{LastEventAtMap, REPLAY_BUFFER_DEPTH, STALE_SECONDS};
use crate::proto::{
    ApplyRequest, ApplyResponse, ApplySecretRequest, ApplySecretResponse, BatchApplyRequest,
    BatchApplyResponse, Config, ConfigEvent, DryRunApplyRequest, DryRunApplyResponse,
    GetAllRequest, GetAllSecretsRequest, GetRequest, GetSecretRequest, RevertRequest,
    RevertResponse, SecretEvent, SecretResponse, SubscribeRequest, SubscribeSecretsRequest,
    konfig_service_server::{KonfigService, KonfigServiceServer},
};
use crate::quota::{
    Admit, GuardedStream, QuotaMode, QuotaSynced, QuotaTable, SubscriberCounts, SubscriberGuard,
    effective_subscriber_limit,
};
use crate::schema::SchemaTable;
use crate::secret_cache::SecretCache;

mod context;
mod drain;
mod serve;
mod service;

pub(crate) use context::{
    client_addr, log_rpc_entry, parse_config_schema_version, parse_secret_schema_version,
    record_status, request_id,
};
pub use drain::DRAIN_TIMEOUT;
pub(crate) use drain::check_drain;
#[cfg(test)]
pub(crate) use serve::apply_h2_overrides;
pub use serve::serve;

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
    /// Broadcast fan-out coalesce window (CU-86aj3vpgr). `Duration::ZERO`
    /// (the default, `--coalesce-window-ms 0`) disables coalescing — each
    /// apply is broadcast immediately, byte-for-byte the historical path.
    /// `> 0` buffers events arriving within the window in the per-namespace
    /// pump and dispatches them as a burst, cutting per-subscriber wake
    /// amplification at high churn at the cost of up to `window` ms of added
    /// tail latency.
    pub coalesce_window: Duration,
    /// Per-namespace broadcast shard count (CU-86aj3vpnh, `--broadcast-shards`).
    /// Clamped to `1..=16`. `1` (the default) is byte-for-byte the historical
    /// single-channel path. `> 1` splits each namespace into N broadcast
    /// channels: the watcher fans every event to all N, each Subscribe attaches
    /// to one (round-robin), so an event wakes only ~1/N of subscribers. The
    /// shared replay buffer is intentionally NOT sharded.
    pub broadcast_shards: usize,
    /// Per-tenant authorization mode (CU-86ahrwd6f). Resolved once from
    /// `KONFIG_AUTHZ_MODE` at startup; `Disabled` (the default) makes every
    /// RPC's authz guard a zero-overhead short-circuit.
    pub authz_mode: AuthzMode,
    /// Lock-free `identity → rules` ACL table populated by the cluster-scoped
    /// `ConfigACL` watcher. Read by the per-RPC authz guard.
    pub acl_table: Arc<AclTable>,
    /// Initial-sync flag for `acl_table`. In `Enforce`, the guard returns
    /// `UNAVAILABLE` until this flips `true` so the boot window cannot serve
    /// un-authorized.
    pub acl_synced: Arc<AclSynced>,
    /// Lock-free `(namespace, configName) → compiled draft-07 schema` registry
    /// populated by the `ConfigSchema` watcher (CU-86ahrwd5g). Read on the
    /// `Apply` RPC path to validate `content` before patching. Empty registry
    /// (no schema for a key) ⇒ accept anything.
    pub schema_table: Arc<SchemaTable>,
    /// Per-tenant quota enforcement mode (CU-86aj8pvdb, MT-2). Resolved once
    /// from `KONFIG_TENANT_QUOTA_MODE` at startup; `Disabled` (the default)
    /// makes the subscriber-admission guard a zero-overhead short-circuit.
    pub quota_mode: QuotaMode,
    /// Lock-free `identity → budget` table populated by the cluster-scoped
    /// `TenantQuota` watcher (MT-1). Read by the subscriber-admission guard.
    pub quota_table: Arc<QuotaTable>,
    /// Initial-sync flag for `quota_table`. Until it flips, the enforce-mode
    /// guard falls back to the flag default (never denies on un-synced policy).
    pub quota_synced: Arc<QuotaSynced>,
    /// Live per-identity concurrent-subscriber counts (MT-2). Shared so the
    /// RAII guard can decrement on stream end; both stream kinds count here.
    pub subscriber_counts: Arc<SubscriberCounts>,
    /// Default per-tenant concurrent-subscriber cap when no `TenantQuota` names
    /// the identity (MT-2, `--default-max-subscribers`). `0` = unlimited.
    pub default_max_subscribers: u32,
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
    /// One [`ShardSet`] per namespace — N broadcast senders shared across all
    /// Config subscribers for that namespace (CU-86aj3vpnh). A single kube
    /// watcher fans every event to all N shard senders; each subscriber's
    /// `Receiver` is attached to ONE shard (round-robin), so an event wakes only
    /// ~1/N of the namespace's subscribers. `N == 1` is the historical
    /// single-channel path. Events are wrapped in `Arc` so broadcast clones are
    /// reference-count increments only — serialisation happens once per apply.
    pub(crate) namespace_broadcasts: Arc<DashMap<String, ShardSet>>,
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
    /// Broadcast fan-out coalesce window (CU-86aj3vpgr). Threaded from
    /// `ServerConfig` to the per-namespace pump on each `subscribe` call.
    /// `Duration::ZERO` = coalescing disabled (default).
    pub(crate) coalesce_window: Duration,
    /// Per-namespace broadcast shard count (CU-86aj3vpnh). Clamped to `1..=16`
    /// in `serve`. Threaded to `get_or_create_broadcast` so it only takes
    /// effect when a namespace's `ShardSet` is first created. `1` = historical
    /// single-channel path.
    pub(crate) broadcast_shards: usize,
    /// Per-tenant authorization mode (CU-86ahrwd6f). `Disabled` short-circuits
    /// the guard before any ACL/identity work.
    pub(crate) authz_mode: AuthzMode,
    /// Lock-free `identity → rules` ACL table read by [`Self::authorize`].
    pub(crate) acl_table: Arc<AclTable>,
    /// Initial-sync flag for [`Self::acl_table`]; gates the enforce-mode
    /// fail-safe.
    pub(crate) acl_synced: Arc<AclSynced>,
    /// Lock-free `(namespace, configName) → compiled draft-07 schema` registry
    /// (CU-86ahrwd5g). Passed to `apply::handle_apply` so `apply_inner`
    /// validates `content` before patching. No schema for a key ⇒ accept.
    pub(crate) schema_table: Arc<SchemaTable>,
    /// Per-tenant quota enforcement mode (CU-86aj8pvdb, MT-2). `Disabled`
    /// short-circuits [`Self::admit_subscriber`] before any accounting.
    pub(crate) quota_mode: QuotaMode,
    /// Lock-free `identity → budget` table read by [`Self::admit_subscriber`].
    pub(crate) quota_table: Arc<QuotaTable>,
    /// Initial-sync flag for [`Self::quota_table`]; gates the enforce-mode
    /// fail-safe (fall back to the flag default until synced).
    pub(crate) quota_synced: Arc<QuotaSynced>,
    /// Live per-identity concurrent-subscriber counts shared with the RAII
    /// guard attached to each Subscribe / SubscribeSecrets stream.
    pub(crate) subscriber_counts: Arc<SubscriberCounts>,
    /// Default concurrent-subscriber cap when no `TenantQuota` names the caller
    /// (`--default-max-subscribers`). `0` = unlimited.
    pub(crate) default_max_subscribers: u32,
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

    /// Per-tenant authorization guard (CU-86ahrwd6f). Called at the top of each
    /// RPC handler, after `check_drain`, mirroring `log_rpc_entry`/`check_drain`.
    ///
    /// Extracts the mTLS [`identity::ClientIdentity`] from the request and
    /// defers to [`authz::check`] with this server's mode + ACL table + sync
    /// flag. In `Disabled` (the default) `check` short-circuits before any
    /// identity work; only then does this method skip the cert parse too, so
    /// the disabled path stays zero-overhead.
    ///
    /// `name` is `"*"` for the name-less RPCs (`get_all`/`subscribe`/…): they
    /// require the verb across the whole namespace, which a `default/*`-style
    /// pattern grants and a single-name pattern does not.
    fn authorize<T>(
        &self,
        request: &Request<T>,
        verb: Verb,
        namespace: &str,
        name: &str,
    ) -> Result<(), Status> {
        if self.authz_mode == AuthzMode::Disabled {
            return Ok(());
        }
        let identity = identity::extract_identity(request);
        authz::check(
            self.authz_mode,
            &self.acl_table,
            self.acl_synced.is_synced(),
            &identity,
            verb,
            namespace,
            name,
        )
    }

    /// Per-tenant subscriber admission (CU-86aj8pvdb, MT-2). Called at the top
    /// of `subscribe` / `subscribe_secrets`, after `authorize`. Returns the RAII
    /// [`SubscriberGuard`] to attach to the response stream, or
    /// `RESOURCE_EXHAUSTED` when an `Enforce`-mode caller is over budget.
    ///
    /// `None` (no guard, no accounting) when quotas are `Disabled` — the
    /// default — so the disabled path stays zero-overhead, exactly like
    /// `authorize`. Both stream kinds share one [`SubscriberCounts`] keyed by
    /// identity, so a tenant's Subscribe and SubscribeSecrets streams count
    /// against the one `maxSubscribers` budget.
    fn admit_subscriber<T>(&self, request: &Request<T>) -> Result<Option<SubscriberGuard>, Status> {
        if self.quota_mode == QuotaMode::Disabled {
            return Ok(None);
        }
        let identity = identity::extract_identity(request);
        let limit = effective_subscriber_limit(
            &self.quota_table,
            self.quota_synced.is_synced(),
            self.default_max_subscribers,
            &identity.id,
        );
        match self
            .subscriber_counts
            .admit(&identity.id, self.quota_mode, limit)
        {
            Admit::Allowed {
                guard,
                current,
                limit,
                over_budget,
            } => {
                if over_budget {
                    crate::metrics::record_tenant_quota_denied("subscribe", "permissive");
                    warn!(
                        identity = %identity.id,
                        current,
                        limit,
                        mode = "permissive",
                        "tenant subscriber quota would-deny (permissive — allowing)"
                    );
                }
                Ok(Some(guard))
            }
            Admit::Denied { current, limit } => {
                crate::metrics::record_tenant_quota_denied("subscribe", "enforce");
                warn!(
                    identity = %identity.id,
                    current,
                    limit,
                    mode = "enforce",
                    "tenant subscriber quota exhausted — RESOURCE_EXHAUSTED"
                );
                Err(Status::resource_exhausted(format!(
                    "tenant subscriber quota exhausted ({current}/{limit})"
                )))
            }
        }
    }
}

// ── Shared helper ─────────────────────────────────────────────────────────────

/// HTTP status code of a kube API error, or `None` when `err` is not an
/// `Error::Api`.  Lets the retry / list error classifiers be a flat
/// `match code { .. }` instead of repeated `Error::Api(ae) if ae.code == N`
/// guard arms — drops their cyclomatic complexity (CU-86aj7k7fd).
pub(crate) fn api_status_code(err: &kube::Error) -> Option<u16> {
    match err {
        kube::Error::Api(ae) => Some(ae.code),
        _ => None,
    }
}

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

    /// grpc-Web enablement (CU-86ahzwhg4): the production `serve()` builder
    /// mounts `accept_http1(true)` + `tonic_web::GrpcWebLayer` so port 50051
    /// serves BOTH standard gRPC (h2) AND grpc-Web (h1) on one listener. This
    /// test mirrors the prod builder pipeline (apply_h2_overrides → layer →
    /// add_service) and asserts a STANDARD gRPC h2 client still completes the
    /// HTTP/2 preface + SETTINGS handshake through the layered server — i.e.
    /// the grpc-Web layer + h1 acceptance did not regress the existing gRPC
    /// transport. A true browser grpc-Web round-trip needs a JS/grpc-web
    /// client + CORS preflight and is not feasible headless; the h1-acceptance
    /// half is covered by `grpc_web_accepts_http1_request` below.
    #[tokio::test]
    async fn grpc_web_layer_builder_binds_and_serves_standard_grpc() {
        use tokio::net::TcpListener;
        use tonic::transport::Server as TonicServer;

        let addr: SocketAddr = {
            let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            l.local_addr().expect("local_addr")
        };

        let (reporter, health_server) = tonic_health::server::health_reporter();
        reporter
            .set_service_status("ping", tonic_health::ServingStatus::Serving)
            .await;

        // Same builder shape `serve()` uses: h2 overrides, then accept_http1,
        // then the grpc-Web layer, then the service.
        let builder = apply_h2_overrides(TonicServer::builder(), Some(1_048_576), Some(2_048))
            .accept_http1(true);

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_handle = tokio::spawn(async move {
            builder
                .layer(tonic_web::GrpcWebLayer::new())
                .add_service(health_server)
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        // Standard gRPC over h2 must still connect through the layered server.
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
        .expect("standard gRPC h2 handshake must still succeed with grpc-Web layer mounted");
        drop(channel);

        let _ = shutdown_tx.send(());
        let res = tokio::time::timeout(Duration::from_secs(2), server_handle)
            .await
            .expect("server task must terminate within 2s after shutdown");
        res.expect("join").expect("server must shut down cleanly");
    }

    /// grpc-Web enablement (CU-86ahzwhg4): assert the layered server actually
    /// speaks HTTP/1.1 + grpc-Web on the same port. We send a raw HTTP/1.1
    /// request carrying `content-type: application/grpc-web` (the frame a
    /// browser grpc-web client uses) over a plain TCP socket and assert the
    /// server returns an HTTP/1.1 response rather than rejecting the
    /// connection / falling back to an h2-only error.
    ///
    /// A bare-TCP HTTP/1.1 probe (no hyper/reqwest dev-dep, keeping the Bazel
    /// crate lockfile unchanged) is the strongest dep-free signal: WITHOUT
    /// `accept_http1(true)` + the web layer, tonic's h2-only server cannot
    /// parse an HTTP/1.1 request line and the connection yields no HTTP/1.1
    /// status line. WITH them mounted, the grpc-Web layer answers over h1.
    /// We assert only that a well-formed `HTTP/1.1 <status>` line comes back
    /// (the gRPC status itself rides in trailers and depends on the unary
    /// payload, which we deliberately do not construct here).
    #[tokio::test]
    async fn grpc_web_accepts_http1_request() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};
        use tonic::transport::Server as TonicServer;

        let addr: SocketAddr = {
            let l = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            l.local_addr().expect("local_addr")
        };

        let (reporter, health_server) = tonic_health::server::health_reporter();
        reporter
            .set_service_status("ping", tonic_health::ServingStatus::Serving)
            .await;

        let builder = TonicServer::builder().accept_http1(true);

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server_handle = tokio::spawn(async move {
            builder
                .layer(tonic_web::GrpcWebLayer::new())
                .add_service(health_server)
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        // Send a minimal grpc-web framed HTTP/1.1 POST and read the status line.
        let status_line = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let Ok(mut stream) = TcpStream::connect(addr).await else {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                };
                // 5-byte gRPC length-prefixed frame with a zero-length body —
                // enough for the grpc-web layer to accept and start a response.
                let body: [u8; 5] = [0, 0, 0, 0, 0];
                let req = format!(
                    "POST /grpc.health.v1.Health/Check HTTP/1.1\r\n\
                     Host: {addr}\r\n\
                     content-type: application/grpc-web\r\n\
                     accept: application/grpc-web\r\n\
                     content-length: {len}\r\n\
                     connection: close\r\n\r\n",
                    len = body.len(),
                );
                if stream.write_all(req.as_bytes()).await.is_err()
                    || stream.write_all(&body).await.is_err()
                {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                let mut buf = Vec::new();
                // Read just the status line; close-delimited response.
                if stream.read_to_end(&mut buf).await.is_err() || buf.is_empty() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                }
                let text = String::from_utf8_lossy(&buf);
                break text.lines().next().unwrap_or("").to_string();
            }
        })
        .await
        .expect("grpc-Web HTTP/1.1 request must get a response within 3s");

        // The h2-only server (no accept_http1 / no web layer) would never emit
        // a parseable HTTP/1.1 status line. Its presence proves grpc-Web/h1 is
        // live on the port.
        assert!(
            status_line.starts_with("HTTP/1.1 "),
            "expected an HTTP/1.1 status line from the grpc-Web layer, got: {status_line:?}",
        );

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
            label_selector: String::new(),
        });
        let err = server
            .subscribe(req)
            .await
            .expect_err("must reject new subscribers during drain");
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    // ── request_id / client_addr helpers (CU-86ahrwd64) ───────────────────────

    /// A caller-supplied `x-request-id` metadata header must be echoed back
    /// verbatim so a client-generated correlation id flows through logs +
    /// traces unchanged.
    #[test]
    fn request_id_echoes_client_supplied_header() {
        let mut req = Request::new(GetRequest {
            namespace: "default".into(),
            name: "x".into(),
        });
        req.metadata_mut()
            .insert("x-request-id", "abc-123".parse().expect("valid metadata"));
        assert_eq!(request_id(&req), "abc-123");
    }

    /// A blank `x-request-id` is treated as absent — a generated id is minted
    /// rather than echoing an empty string.
    #[test]
    fn request_id_generates_when_header_blank() {
        let mut req = Request::new(GetRequest {
            namespace: "default".into(),
            name: "x".into(),
        });
        req.metadata_mut()
            .insert("x-request-id", "   ".parse().expect("valid metadata"));
        let id = request_id(&req);
        assert_ne!(id.trim(), "", "blank header must yield a generated id");
        assert!(
            id.contains('-'),
            "generated id is `<nanos>-<seq>`; got {id}"
        );
    }

    /// With no metadata at all, a fresh process-local id is generated; two
    /// successive generated ids must differ (the atomic sequence advances).
    #[test]
    fn request_id_generates_unique_ids_without_header() {
        let req = || {
            Request::new(GetRequest {
                namespace: "default".into(),
                name: "x".into(),
            })
        };
        let a = request_id(&req());
        let b = request_id(&req());
        assert_ne!(a, b, "generated ids must be unique across calls");
    }

    /// `client_addr` falls back to `unknown` for the in-process test
    /// transport (no peer socket).
    #[test]
    fn client_addr_unknown_without_peer() {
        let req = Request::new(GetRequest {
            namespace: "default".into(),
            name: "x".into(),
        });
        assert_eq!(client_addr(&req), "unknown");
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
            coalesce_window: Duration::ZERO,
            broadcast_shards: 1,
            // Authz disabled in the drain-plumbing tests — the guard
            // short-circuits, so these tests exercise the original path.
            authz_mode: AuthzMode::Disabled,
            acl_table: Arc::new(AclTable::new()),
            acl_synced: Arc::new(AclSynced::new()),
            schema_table: Arc::new(SchemaTable::new()),
            // Quotas disabled in the drain-plumbing tests — admit_subscriber
            // short-circuits, so these tests exercise the original path.
            quota_mode: QuotaMode::Disabled,
            quota_table: Arc::new(QuotaTable::new()),
            quota_synced: Arc::new(QuotaSynced::new()),
            subscriber_counts: Arc::new(SubscriberCounts::new()),
            default_max_subscribers: 0,
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
