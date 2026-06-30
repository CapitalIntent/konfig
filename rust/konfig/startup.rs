//! Startup wiring for the `konfig` server binary.
//!
//! Lives in the library crate (not `main.rs`) so the orchestration steps
//! 2-10 from `main.rs`'s startup sequence are reachable from tests. The
//! binary entry point (`konfig_bin`) keeps only the tracing init + `Args`
//! parse and immediately defers to [`run`].
//!
//! Startup sequence (`main.rs` doc comment is the source of truth):
//! 1. Parse CLI args / env vars                                  ← in `main.rs`
//! 2. Init kube::Client
//! 3. Spawn Config CRD watcher task
//! 4. Spawn Secret namespace watchers (cache + broadcast)
//! 5. Register gRPC health as NOT_SERVING for KonfigService
//! 6. Wait until cache has at least one populated entry
//! 7. Register gRPC health as SERVING
//! 8. Start /metrics HTTP server (port 9090) in background
//! 9. Install SIGTERM / Ctrl-C handler — feeds the shutdown signal that
//!    `grpc::serve` consumes to begin graceful drain
//! 10. Start gRPC server (port 50051) — blocks until shutdown completes

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use dashmap::DashMap;
use kube::Client;
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{broadcast, oneshot};
use tonic::transport::ServerTlsConfig;
use tracing::{info, warn};

use crate::acl::{AclSynced, AclTable, AclWatcher};
use crate::cache::ConfigCache;
use crate::grpc::authz::Mode as AuthzMode;
use crate::grpc::tls::{TlsPaths, build_server_tls_config, warn_tls_disabled};
use crate::grpc::{HttpGatewayConfig, ServerConfig, serve};
use crate::metrics::{LastEventAtMap, last_event_at_for, spawn_tokio_runtime_sampler};
use crate::proto::{SecretEvent, konfig_service_server::KonfigServiceServer};
use crate::quota::{
    ApplyLimiter, QuotaMode, QuotaSynced, QuotaTable, QuotaWatcher, SubscriberCounts,
};
use crate::schema::{SchemaSynced, SchemaTable, SchemaWatcher};
use crate::secret_cache::SecretCache;
use crate::secret_watcher::SecretWatcher;
use crate::tenant_cache::TenantCacheLedger;
use crate::types::ConfigSnapshot;
use crate::watcher::{Watcher, run_with_reconnect};

/// Initial capacity for per-namespace `DashMap`s allocated during startup.
/// Typical pod fans out across 10–50 namespaces; 64 is the next power of two
/// and eliminates early `RawTable::reserve_rehash` calls before the maps
/// reach steady state (CU-86aj37pwx).
const NAMESPACE_MAP_INITIAL_CAPACITY: usize = 64;

#[derive(Parser, Debug, Clone)]
#[command(name = "konfig", about = "Konfig config distribution service")]
pub struct Args {
    /// gRPC listen address
    #[arg(long, env = "KONFIG_GRPC_ADDR", default_value = "0.0.0.0:50051")]
    pub grpc_addr: SocketAddr,

    /// Prometheus metrics listen address
    #[arg(long, env = "KONFIG_METRICS_ADDR", default_value = "0.0.0.0:9090")]
    pub metrics_addr: SocketAddr,

    /// K8s namespace to watch for Config CRDs
    #[arg(long, env = "KONFIG_NAMESPACE", default_value = "default")]
    pub namespace: String,

    /// Config CRD name to watch.
    /// KONFIG_NAME must be set — no default config name; konfig is domain-agnostic.
    #[arg(long, env = "KONFIG_NAME")]
    pub name: String,

    /// K8s namespaces to watch for managed Secrets (konfig.io/managed=true).
    /// Comma-separated or repeated flag, e.g. --secret-namespaces trading,risk
    #[arg(
        long,
        env = "KONFIG_SECRET_NAMESPACES",
        value_delimiter = ',',
        default_value = ""
    )]
    pub secret_namespaces: Vec<String>,

    /// Enable mutual-TLS on the gRPC server. Default ON. Pass `--tls=false`
    /// only for local dev / integration tests — never in production.
    #[arg(
        long,
        env = "KONFIG_TLS",
        default_value_t = true,
        action = clap::ArgAction::Set,
    )]
    pub tls: bool,

    /// PEM-encoded server certificate (presented on handshake). Required when
    /// `--tls=true`. Ignored when `--tls=false`.
    #[arg(long, env = "KONFIG_TLS_CERT")]
    pub tls_cert: Option<PathBuf>,

    /// PEM-encoded server private key. Required when `--tls=true`.
    #[arg(long, env = "KONFIG_TLS_KEY")]
    pub tls_key: Option<PathBuf>,

    /// PEM-encoded CA bundle used to verify client certificates. Required
    /// when `--tls=true`. Every client must present a cert signed by this CA.
    #[arg(long, env = "KONFIG_TLS_CLIENT_CA")]
    pub tls_client_ca: Option<PathBuf>,

    /// HTTP/2 `SETTINGS_INITIAL_WINDOW_SIZE` override for the gRPC server
    /// (bench knob for CU-86aj37q7a). `None` (default) = leave the tonic
    /// default (65,535). Raising reduces `h2::Prioritize::poll_complete`
    /// self-CPU on large Subscribe fan-outs but increases per-stream RAM —
    /// sweep before changing the default.
    #[arg(long, env = "KONFIG_H2_INITIAL_WINDOW_BYTES")]
    pub h2_initial_window_bytes: Option<u32>,

    /// HTTP/2 `SETTINGS_MAX_CONCURRENT_STREAMS` override for the gRPC
    /// server (bench knob for CU-86aj37q7a). `None` (default) = leave the
    /// tonic default (unlimited). Lower caps protect the server from a
    /// single client monopolising streams; raising can help when many
    /// Subscribe RPCs multiplex on one connection — sweep before changing
    /// the default.
    #[arg(long, env = "KONFIG_H2_MAX_CONCURRENT_STREAMS")]
    pub h2_max_concurrent_streams: Option<u32>,

    /// Broadcast fan-out coalesce window in milliseconds (CU-86aj3vpgr).
    /// `0` (default) disables coalescing — every config apply is broadcast
    /// to subscribers immediately, byte-for-byte the historical behaviour.
    /// `> 0` buffers events arriving within the window in each namespace's
    /// watch pump and dispatches them as a burst, cutting per-subscriber
    /// wake amplification at high churn at the cost of up to this many
    /// milliseconds of added tail latency on event delivery. Konfig's
    /// eventual-consistency contract tolerates the delay; start with 5.
    #[arg(long, env = "KONFIG_COALESCE_WINDOW_MS", default_value = "0")]
    pub coalesce_window_ms: u64,

    /// Per-namespace broadcast shard count (CU-86aj3vpnh). Clamped to `1..=16`.
    /// `1` (the default) is byte-for-byte the historical single broadcast
    /// channel per namespace — every event wakes every subscriber. `> 1`
    /// splits each namespace into N broadcast channels: the watcher fans every
    /// event to all N, and each Subscribe RPC attaches its receiver to ONE
    /// shard (round-robin), so an event wakes only ~1/N of the namespace's
    /// subscribers — cutting wake amplification under fan-out. The per-namespace
    /// replay buffer is shared (NOT sharded), so reconnect/resume semantics are
    /// unchanged. Default flips to 4 only after a bench validates the win.
    #[arg(long, env = "KONFIG_BROADCAST_SHARDS", default_value = "1")]
    pub broadcast_shards: usize,

    /// Default per-tenant concurrent-subscriber cap when no `TenantQuota`
    /// matches the caller identity (CU-86aj8pvdb, MT-2). `0` (the default) means
    /// unlimited. A matching `TenantQuota.maxSubscribers` overrides this once the
    /// quota watcher has synced; until then this flag applies (the boot-window
    /// fail-safe). Only enforced when `KONFIG_TENANT_QUOTA_MODE` is `permissive`
    /// (log-only would-deny) or `enforce` (RESOURCE_EXHAUSTED over budget).
    #[arg(long, env = "KONFIG_DEFAULT_MAX_SUBSCRIBERS", default_value = "0")]
    pub default_max_subscribers: u32,

    /// Default per-tenant Apply refill rate in tokens/second when no
    /// `TenantQuota` matches the caller identity (CU-86aj8pvf1, MT-3). `0` (the
    /// default) means unlimited. A matching `TenantQuota.maxAppliesPerSecond`
    /// overrides this once the quota watcher has synced; until then this flag
    /// applies (boot-window fail-safe). Burst capacity derives from the rate
    /// (one second of tokens) — there is no separate burst flag. Only enforced
    /// when `KONFIG_TENANT_QUOTA_MODE` is `permissive` or `enforce`.
    #[arg(
        long,
        env = "KONFIG_DEFAULT_MAX_APPLIES_PER_SECOND",
        default_value = "0"
    )]
    pub default_max_applies_per_second: u32,

    /// Default per-tenant cache byte budget when no `TenantQuota` matches the
    /// caller identity (CU-86aj8pvg3, MT-4). `0` (the default) means unlimited.
    /// A matching `TenantQuota.cacheMemoryBudgetBytes` overrides this once the
    /// quota watcher has synced; until then this flag applies (boot fail-safe).
    /// Only accounted/enforced when `KONFIG_TENANT_QUOTA_MODE` is `permissive`
    /// or `enforce`.
    #[arg(long, env = "KONFIG_DEFAULT_CACHE_BUDGET_BYTES", default_value = "0")]
    pub default_cache_budget_bytes: u64,

    /// HTTP/JSON gateway listen address (CU-86ahrwd70). When set, a sibling
    /// `axum` server starts on this address and transcodes JSON ⇄ proto for the
    /// full unary `KonfigService` surface (browsers / non-gRPC clients). Unset
    /// (the default) ⇒ gateway OFF; only gRPC + grpc-Web on `--grpc-addr`.
    ///
    /// The gateway is plaintext and reaches konfig as the `anonymous` identity,
    /// and it exposes writes + secret reads — so it REQUIRES `--http-auth-token`
    /// unless `--http-insecure` is explicitly passed (startup fails fast
    /// otherwise, mirroring `--tls`). Run it cluster-internal.
    #[arg(long, env = "KONFIG_HTTP_ADDR")]
    pub http_addr: Option<SocketAddr>,

    /// Bearer token required on every HTTP/JSON gateway request
    /// (`Authorization: Bearer <token>`). REQUIRED when `--http-addr` is set,
    /// unless `--http-insecure` is passed. Ignored when the gateway is off.
    #[arg(long, env = "KONFIG_HTTP_AUTH_TOKEN")]
    pub http_auth_token: Option<String>,

    /// Value returned in `Access-Control-Allow-Origin` on gateway responses and
    /// CORS preflights (e.g. `https://backstage.example`). Unset (the default)
    /// ⇒ no CORS headers emitted (same-origin browser requests only).
    #[arg(long, env = "KONFIG_HTTP_CORS_ALLOW_ORIGIN")]
    pub http_cors_allow_origin: Option<String>,

    /// Explicitly disable the HTTP/JSON gateway's bearer-token requirement.
    /// Only meaningful with `--http-addr`. Leaves writes + secret reads
    /// unauthenticated on a plaintext port — run cluster-internal ONLY. Without
    /// this, `--http-addr` set with no token fails startup (fail-safe). Like
    /// `--tls`, takes an explicit value: `--http-insecure=true`.
    #[arg(
        long,
        env = "KONFIG_HTTP_INSECURE",
        default_value_t = false,
        action = clap::ArgAction::Set,
    )]
    pub http_insecure: bool,
}

/// Resolve a `ServerTlsConfig` from the TLS-related fields on `args`, or
/// `Ok(None)` when `--tls=false` (local dev escape hatch). Fails fast — before
/// any kube API call — when `--tls=true` but a required file path was
/// omitted.
///
/// Extracted so the TLS-resolution branches are unit-testable without
/// invoking the kube client or the full startup.
pub fn resolve_tls_config(
    args: &Args,
) -> Result<Option<ServerTlsConfig>, Box<dyn std::error::Error>> {
    if !args.tls {
        warn_tls_disabled();
        return Ok(None);
    }
    let cert = args.tls_cert.clone().ok_or(
        "TLS enabled but --tls-cert/KONFIG_TLS_CERT not set. \
         Pass --tls=false to disable (local dev only).",
    )?;
    let key = args.tls_key.clone().ok_or(
        "TLS enabled but --tls-key/KONFIG_TLS_KEY not set. \
         Pass --tls=false to disable (local dev only).",
    )?;
    let client_ca = args.tls_client_ca.clone().ok_or(
        "TLS enabled but --tls-client-ca/KONFIG_TLS_CLIENT_CA not set. \
         Pass --tls=false to disable (local dev only).",
    )?;
    let cfg = build_server_tls_config(&TlsPaths {
        cert,
        key,
        client_ca,
    })
    .map_err(|e| -> Box<dyn std::error::Error> { e })?;
    Ok(Some(cfg))
}

/// Resolve the optional HTTP/JSON gateway config (CU-86ahrwd70) from the
/// gateway-related fields on `args`. Returns:
///   - `Ok(None)` when `--http-addr` is unset — the gateway stays off.
///   - `Ok(Some(..))` with `auth_token: None` when `--http-insecure` is set
///     (logs a loud warning — the port then exposes writes + secret reads with
///     no auth).
///   - `Ok(Some(..))` with the bearer token when `--http-addr` + a non-empty
///     `--http-auth-token` are both present.
///   - `Err(..)` when `--http-addr` is set without a token and `--http-insecure`
///     was NOT passed — fail-fast before any kube call, mirroring
///     [`resolve_tls_config`], so an operator cannot accidentally expose an
///     unauthenticated write/secret surface.
///
/// Extracted so the branches are unit-testable without the full startup.
pub fn resolve_http_gateway_config(
    args: &Args,
) -> Result<Option<HttpGatewayConfig>, Box<dyn std::error::Error>> {
    let Some(addr) = args.http_addr else {
        return Ok(None);
    };

    if args.http_insecure {
        warn!(
            %addr,
            "HTTP/JSON gateway: --http-insecure set — bearer-token auth DISABLED. \
             This port exposes writes and secret reads with NO authentication; \
             run it cluster-internal only."
        );
        return Ok(Some(HttpGatewayConfig {
            addr,
            auth_token: None,
            cors_allow_origin: args.http_cors_allow_origin.clone(),
            insecure: true,
        }));
    }

    let token = args
        .http_auth_token
        .clone()
        .filter(|t| !t.is_empty())
        .ok_or(
            "HTTP/JSON gateway enabled (--http-addr) but \
             --http-auth-token/KONFIG_HTTP_AUTH_TOKEN not set. The gateway exposes \
             writes and secret reads over plaintext, so a bearer token is required. \
             Pass --http-insecure=true to explicitly opt out (cluster-internal only).",
        )?;

    Ok(Some(HttpGatewayConfig {
        addr,
        auth_token: Some(token),
        cors_allow_origin: args.http_cors_allow_origin.clone(),
        insecure: false,
    }))
}

/// Filter out empty secret-namespace entries left behind by `--secret-namespaces=`
/// or a trailing comma in `KONFIG_SECRET_NAMESPACES`.
///
/// Pure helper — extracted so the filter behaviour is unit-testable.
pub fn normalize_secret_namespaces(raw: Vec<String>) -> Vec<String> {
    raw.into_iter().filter(|s| !s.is_empty()).collect()
}

/// Run the konfig server end-to-end. Blocks until the gRPC server stops
/// (drain completes, SIGTERM/Ctrl-C delivered).
///
/// Steps 2-10 of the startup sequence in `main.rs`'s module docs. Step 1
/// (CLI parse) and tracing init stay in the binary entry point so this
/// helper is reachable from tests with a synthetic [`Args`].
pub async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    // snmalloc streaming-mode sampler (CU-86aj35zxw). Only compiled into
    // the heapprof binary; runtime-gated by `KONFIG_SNMALLOC_STREAM_PATH`.
    // No-op when the env var is absent. Started up-front so the activation
    // window covers all subsequent allocations during startup.
    #[cfg(feature = "snmalloc_profiling")]
    {
        // start_if_env emits its own info!() on activation. The bool
        // return is only useful when downstream code wants to gate on
        // activation; here we just propagate the error and ignore the
        // success bool.
        let _ = crate::stream_sink::start_if_env()?;
    }

    // Resolve TLS up-front so a misconfig fails startup before we touch
    // the kube API or spawn any watcher.
    let tls_config = resolve_tls_config(&args)?;

    // Resolve the optional HTTP/JSON gateway (CU-86ahrwd70) here too, so an
    // `--http-addr` without a bearer token (and no `--http-insecure`) fails
    // fast before any kube call — same fail-safe contract as TLS.
    let http_gateway = resolve_http_gateway_config(&args)?;

    // Spawn tokio runtime-metrics sampler — publishes `tokio_*` gauges every
    // 5 s on the same `/metrics` endpoint as the Prometheus app metrics.
    spawn_tokio_runtime_sampler(tokio::runtime::Handle::current());

    let kube_client = Client::try_default().await?;

    let cache = Arc::new(ConfigCache::new(ConfigSnapshot::default()));
    let secret_cache = Arc::new(SecretCache::new());

    // Per-namespace freshness map shared by all watchers and the
    // konfig_stale_seconds sampler.
    let last_event_at_map: LastEventAtMap =
        Arc::new(DashMap::with_capacity(NAMESPACE_MAP_INITIAL_CAPACITY));

    // Spawn Config CRD watcher.  The inner `Watcher::run` already retries on
    // transient stream errors; `run_with_reconnect` covers the cases where
    // `Watcher::run` returns at all (clean stream end or terminal Err) so a
    // single failure can never crash the process.
    let watcher_cache = Arc::clone(&cache);
    let watcher_client = kube_client.clone();
    let namespace = args.namespace.clone();
    let name = args.name.clone();
    let watcher_last_event_at = last_event_at_for(&last_event_at_map, &namespace);
    tokio::spawn(async move {
        let on_disconnect = {
            let cache = Arc::clone(&watcher_cache);
            move || cache.mark_all_stale()
        };
        run_with_reconnect("config", namespace.clone(), on_disconnect, |_attempt| {
            Watcher::new(watcher_client.clone()).run(
                Arc::clone(&watcher_cache),
                namespace.clone(),
                name.clone(),
                Arc::clone(&watcher_last_event_at),
            )
        })
        .await;
    });

    // Spawn the cluster-scoped ConfigACL watcher (CU-86ahrwd6f). It populates
    // the per-tenant ACL table read by the gRPC authz guard and flips the
    // `acl_synced` flag once its initial list completes. Resolve the mode once;
    // when `Disabled` (the default) the guard short-circuits, but we still run
    // the watcher so flipping `KONFIG_AUTHZ_MODE` to permissive/enforce via a
    // rolling restart finds a warm, synced table.
    let authz_mode = AuthzMode::from_env();
    let acl_table = Arc::new(AclTable::new());
    let acl_synced = Arc::new(AclSynced::new());
    {
        let acl_client = kube_client.clone();
        let acl_table = Arc::clone(&acl_table);
        let acl_synced = Arc::clone(&acl_synced);
        tokio::spawn(async move {
            run_with_reconnect(
                "configacl",
                // ConfigACL is cluster-scoped — no namespace; pass empty.
                String::new(),
                || {},
                |_attempt| {
                    AclWatcher::new(acl_client.clone())
                        .run(Arc::clone(&acl_table), Arc::clone(&acl_synced))
                },
            )
            .await;
        });
    }

    // Spawn the cluster-scoped TenantQuota watcher (CU-86aj8pvcu, MT-1).
    // Mirror of the ConfigACL watcher: it populates the identity→budget table
    // the forthcoming quota enforcement points (MT-2..) read, and flips
    // `quota_synced` once its initial list completes. Always run it so flipping
    // `KONFIG_TENANT_QUOTA_MODE` to permissive/enforce via a rolling restart
    // finds a warm, synced table — same rationale as the ConfigACL watcher.
    let quota_mode = QuotaMode::from_env();
    let quota_table = Arc::new(QuotaTable::new());
    let quota_synced = Arc::new(QuotaSynced::new());
    // Live per-identity concurrent-subscriber counts (CU-86aj8pvdb, MT-2). Built
    // here so both the gRPC service and the RAII guard attached to every
    // Subscribe / SubscribeSecrets stream share one accounting table.
    let subscriber_counts = Arc::new(SubscriberCounts::new());
    // Per-identity Apply token bucket (CU-86aj8pvf1, MT-3). Shared with the gRPC
    // service's apply rate-limit guard.
    let apply_limiter = Arc::new(ApplyLimiter::new());
    // Per-identity cache byte ledger (CU-86aj8pvg3, MT-4). Shared with the gRPC
    // service's serve-time cache accountant.
    let cache_ledger = Arc::new(TenantCacheLedger::new());
    info!(
        ?quota_mode,
        "TenantQuota watcher: enforcement mode resolved"
    );
    {
        let quota_client = kube_client.clone();
        let quota_table = Arc::clone(&quota_table);
        let quota_synced = Arc::clone(&quota_synced);
        tokio::spawn(async move {
            run_with_reconnect(
                "tenantquota",
                // TenantQuota is cluster-scoped — no namespace; pass empty.
                String::new(),
                || {},
                |_attempt| {
                    QuotaWatcher::new(quota_client.clone())
                        .run(Arc::clone(&quota_table), Arc::clone(&quota_synced))
                },
            )
            .await;
        });
    }

    // Spawn the ConfigSchema watcher (CU-86ahrwd5g). `ConfigSchema` is
    // namespaced, but we watch ALL namespaces (Api::all_with) so a schema in
    // any tenant namespace is enforced; the registry is keyed by the object's
    // namespace + `spec.configName`. It populates the table read on the Apply
    // RPC path. Always run it (no mode gate) — an empty registry just means
    // "no schema" → accept anything, so the watcher is cheap when unused.
    let schema_table = Arc::new(SchemaTable::new());
    let schema_synced = Arc::new(SchemaSynced::new());
    {
        let schema_client = kube_client.clone();
        let schema_table = Arc::clone(&schema_table);
        let schema_synced = Arc::clone(&schema_synced);
        tokio::spawn(async move {
            run_with_reconnect(
                "configschema",
                // Watched cluster-wide across all namespaces — no single
                // namespace; pass empty (matches the ACL watcher convention).
                String::new(),
                || {},
                |_attempt| {
                    SchemaWatcher::new(schema_client.clone())
                        .run(Arc::clone(&schema_table), Arc::clone(&schema_synced))
                },
            )
            .await;
        });
    }

    // Spawn Secret namespace watchers.
    let secret_namespace_broadcasts: Arc<DashMap<String, broadcast::Sender<SecretEvent>>> =
        Arc::new(DashMap::with_capacity(NAMESPACE_MAP_INITIAL_CAPACITY));
    let secret_namespaces = normalize_secret_namespaces(args.secret_namespaces);
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
        .set_not_serving::<KonfigServiceServer<crate::grpc::KonfigServer>>()
        .await;

    // Cache-populated → SERVING gate.
    {
        let cache_ref = Arc::clone(&cache);
        let health_ref = health_reporter.clone();
        tokio::spawn(async move {
            loop {
                if cache_ref.is_populated() {
                    health_ref
                        .set_serving::<KonfigServiceServer<crate::grpc::KonfigServer>>()
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

    // Install SIGTERM + Ctrl-C handlers.
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
            let _ = shutdown_rx.await;
        })),
        tls_config,
        h2_initial_window_bytes: args.h2_initial_window_bytes,
        h2_max_concurrent_streams: args.h2_max_concurrent_streams,
        coalesce_window: std::time::Duration::from_millis(args.coalesce_window_ms),
        broadcast_shards: args.broadcast_shards,
        authz_mode,
        acl_table,
        acl_synced,
        schema_table,
        quota_mode,
        quota_table,
        quota_synced,
        subscriber_counts,
        default_max_subscribers: args.default_max_subscribers,
        apply_limiter,
        default_max_applies_per_second: args.default_max_applies_per_second,
        cache_ledger,
        default_cache_budget_bytes: args.default_cache_budget_bytes,
        http_gateway,
    })
    .await?;

    info!("gRPC server stopped cleanly");
    Ok(())
}

async fn serve_metrics(addr: SocketAddr) {
    use axum::{Router, routing::get};

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/debug/heap-profile.pprof", get(heap_profile_handler));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind metrics");
    info!(addr = %addr, "metrics server starting");
    axum::serve(listener, app)
        .await
        .expect("metrics server error");
}

async fn metrics_handler() -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use prometheus::Encoder;

    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buf = Vec::new();
    if let Err(e) = encoder.encode(&metric_families, &mut buf) {
        // Surface encoder errors as 500 so scrape alerts (absent-target /
        // failed-scrape) fire on the operator side instead of accepting an
        // empty 200 body as "no metrics".
        tracing::warn!("metrics encode failed: {e}");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("metrics encode failed: {e}"),
        )
            .into_response();
    }
    match String::from_utf8(buf) {
        Ok(body) => body.into_response(),
        Err(e) => {
            tracing::warn!("metrics output not UTF-8: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "metrics output not UTF-8".to_string(),
            )
                .into_response()
        }
    }
}

// `/debug/heap-profile.pprof` — gzipped pprof-format heap snapshot from
// snmalloc's sampling profiler. Two variants:
//
// * `snmalloc_profiling` ON  (Bazel `:konfig_bin_heapprof`, library
//   `:konfig_heapprof`, snmalloc-rs built with the `profiling` Cargo
//   feature + C archive `SNMALLOC_PROFILE=ON`):
//     `SnMalloc.snapshot()` returns a live `HeapProfile`;
//     `write_pprof_gz(Weight::Allocated)` streams the gzipped pprof
//     body. Operator can `curl -s host:9090/debug/heap-profile.pprof
//     | go tool pprof -http=:8080 -`.
//
// * `snmalloc_profiling` OFF (default `:konfig_bin` / `:konfig`):
//     Return 404 with a body explaining the rebuild needed. We choose
//     404 (not 501) so scrape rules that conditionally probe the
//     endpoint treat it as "not present on this build" rather than
//     "transient server error"; the body distinguishes the cause for
//     human operators inspecting `curl -v`.
#[cfg(feature = "snmalloc_profiling")]
async fn heap_profile_handler() -> axum::response::Response {
    use axum::http::{StatusCode, header};
    use axum::response::IntoResponse;

    let profile = snmalloc_rs::SnMalloc.snapshot();
    let mut buf = Vec::new();
    if let Err(e) = profile.write_pprof_gz(&mut buf, snmalloc_rs::Weight::Allocated) {
        tracing::warn!("heap-profile encode failed: {e}");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("heap-profile encode failed: {e}"),
        )
            .into_response();
    }
    (
        [
            (header::CONTENT_TYPE, "application/octet-stream"),
            (
                header::CONTENT_DISPOSITION,
                "attachment; filename=\"heap-profile.pprof.gz\"",
            ),
        ],
        buf,
    )
        .into_response()
}

#[cfg(not(feature = "snmalloc_profiling"))]
async fn heap_profile_handler() -> axum::response::Response {
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    (
        StatusCode::NOT_FOUND,
        "heap profiling not compiled in; rebuild with Bazel target //rust/konfig:konfig_bin_heapprof to enable",
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args_with_tls_off() -> Args {
        Args {
            grpc_addr: "0.0.0.0:50051".parse().unwrap(),
            metrics_addr: "0.0.0.0:9090".parse().unwrap(),
            namespace: "default".to_string(),
            name: "cfg".to_string(),
            secret_namespaces: vec![],
            tls: false,
            tls_cert: None,
            tls_key: None,
            tls_client_ca: None,
            h2_initial_window_bytes: None,
            h2_max_concurrent_streams: None,
            coalesce_window_ms: 0,
            broadcast_shards: 1,
            default_max_subscribers: 0,
            default_max_applies_per_second: 0,
            default_cache_budget_bytes: 0,
            http_addr: None,
            http_auth_token: None,
            http_cors_allow_origin: None,
            http_insecure: false,
        }
    }

    #[test]
    fn resolve_tls_disabled_returns_none_and_warns() {
        let cfg = resolve_tls_config(&args_with_tls_off()).expect("ok");
        assert!(cfg.is_none(), "--tls=false yields no ServerTlsConfig");
    }

    #[test]
    fn resolve_tls_enabled_but_no_cert_errors() {
        let mut args = args_with_tls_off();
        args.tls = true;
        // Intentionally leave tls_cert / tls_key / tls_client_ca = None.
        let err = resolve_tls_config(&args).err().expect("must error");
        let msg = err.to_string();
        assert!(msg.contains("tls-cert") || msg.contains("KONFIG_TLS_CERT"));
    }

    #[test]
    fn resolve_tls_enabled_but_no_key_errors() {
        let mut args = args_with_tls_off();
        args.tls = true;
        args.tls_cert = Some(PathBuf::from("/nonexistent/cert.pem"));
        let err = resolve_tls_config(&args).err().expect("must error");
        let msg = err.to_string();
        assert!(msg.contains("tls-key") || msg.contains("KONFIG_TLS_KEY"));
    }

    #[test]
    fn resolve_tls_enabled_but_no_client_ca_errors() {
        let mut args = args_with_tls_off();
        args.tls = true;
        args.tls_cert = Some(PathBuf::from("/nonexistent/cert.pem"));
        args.tls_key = Some(PathBuf::from("/nonexistent/key.pem"));
        let err = resolve_tls_config(&args).err().expect("must error");
        let msg = err.to_string();
        assert!(msg.contains("tls-client-ca") || msg.contains("KONFIG_TLS_CLIENT_CA"));
    }

    // ── HTTP/JSON gateway resolution (CU-86ahrwd70) ───────────────────────────

    /// No `--http-addr` ⇒ gateway off (`Ok(None)`), regardless of token.
    #[test]
    fn resolve_http_gateway_none_when_addr_unset() {
        let cfg = resolve_http_gateway_config(&args_with_tls_off()).expect("ok");
        assert!(cfg.is_none(), "no --http-addr yields no gateway");
    }

    /// `--http-addr` set without a token and without `--http-insecure` must
    /// fail startup — the gateway exposes writes + secret reads.
    #[test]
    fn resolve_http_gateway_addr_without_token_errors() {
        let mut args = args_with_tls_off();
        args.http_addr = Some("0.0.0.0:8080".parse().unwrap());
        let err = resolve_http_gateway_config(&args).expect_err("must error without a token");
        let msg = err.to_string();
        assert!(
            msg.contains("http-auth-token") || msg.contains("KONFIG_HTTP_AUTH_TOKEN"),
            "error must name the missing token flag, got: {msg}"
        );
    }

    /// An empty token string is treated as absent (same fail-fast path).
    #[test]
    fn resolve_http_gateway_empty_token_errors() {
        let mut args = args_with_tls_off();
        args.http_addr = Some("0.0.0.0:8080".parse().unwrap());
        args.http_auth_token = Some(String::new());
        assert!(
            resolve_http_gateway_config(&args).is_err(),
            "an empty token must not satisfy the requirement"
        );
    }

    /// `--http-addr` + a token ⇒ a secure gateway config with the token + CORS.
    #[test]
    fn resolve_http_gateway_addr_with_token_ok() {
        let mut args = args_with_tls_off();
        args.http_addr = Some("0.0.0.0:8080".parse().unwrap());
        args.http_auth_token = Some("s3cret".to_string());
        args.http_cors_allow_origin = Some("https://backstage.example".to_string());
        let cfg = resolve_http_gateway_config(&args)
            .expect("ok")
            .expect("gateway enabled");
        assert_eq!(cfg.addr, "0.0.0.0:8080".parse().unwrap());
        assert_eq!(cfg.auth_token.as_deref(), Some("s3cret"));
        assert_eq!(
            cfg.cors_allow_origin.as_deref(),
            Some("https://backstage.example")
        );
        assert!(!cfg.insecure);
    }

    /// `--http-insecure=true` allows a tokenless gateway (explicit opt-out).
    #[test]
    fn resolve_http_gateway_insecure_allows_no_token() {
        let mut args = args_with_tls_off();
        args.http_addr = Some("0.0.0.0:8080".parse().unwrap());
        args.http_insecure = true;
        let cfg = resolve_http_gateway_config(&args)
            .expect("ok")
            .expect("gateway enabled");
        assert!(cfg.insecure);
        assert!(cfg.auth_token.is_none());
    }

    #[test]
    fn normalize_secret_namespaces_drops_empty_entries() {
        let raw = vec![
            "".to_string(),
            "trading".to_string(),
            "".to_string(),
            "risk".to_string(),
        ];
        let out = normalize_secret_namespaces(raw);
        assert_eq!(out, vec!["trading".to_string(), "risk".to_string()]);
    }

    #[test]
    fn normalize_secret_namespaces_preserves_order() {
        let raw = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let out = normalize_secret_namespaces(raw);
        assert_eq!(out, vec!["a", "b", "c"]);
    }

    #[test]
    fn normalize_secret_namespaces_empty_input_is_empty() {
        assert!(normalize_secret_namespaces(vec![]).is_empty());
    }

    #[test]
    fn normalize_secret_namespaces_all_empty_is_empty() {
        assert!(normalize_secret_namespaces(vec!["".to_string(), "".to_string()]).is_empty());
    }

    #[test]
    fn args_parse_from_minimum_required_env() {
        // `KONFIG_NAME` has no default — every other field has one.
        let args = Args::try_parse_from(["konfig", "--name", "my-config"])
            .expect("must parse with --name");
        assert_eq!(args.name, "my-config");
        assert_eq!(args.namespace, "default");
        assert!(args.tls, "TLS defaults ON");
        // clap's `default_value = ""` yields a single empty-string entry —
        // `normalize_secret_namespaces` is what strips it before use.
        assert!(normalize_secret_namespaces(args.secret_namespaces).is_empty());
    }

    #[test]
    fn args_parse_tls_off_does_not_require_cert_paths() {
        let args =
            Args::try_parse_from(["konfig", "--name", "cfg", "--tls=false"]).expect("must parse");
        assert!(!args.tls);
        assert!(args.tls_cert.is_none());
    }

    /// `--h2-initial-window-bytes` and `--h2-max-concurrent-streams`
    /// default to `None` (bench knobs for CU-86aj37q7a) and parse the
    /// value when supplied. `None` means "leave the tonic default" — no
    /// builder method is called downstream.
    #[test]
    fn args_parse_h2_flags_default_none() {
        let args =
            Args::try_parse_from(["konfig", "--name", "cfg", "--tls=false"]).expect("must parse");
        assert!(args.h2_initial_window_bytes.is_none());
        assert!(args.h2_max_concurrent_streams.is_none());
    }

    #[test]
    fn args_parse_h2_flags_explicit() {
        let args = Args::try_parse_from([
            "konfig",
            "--name",
            "cfg",
            "--tls=false",
            "--h2-initial-window-bytes",
            "1048576",
            "--h2-max-concurrent-streams",
            "2048",
        ])
        .expect("must parse");
        assert_eq!(args.h2_initial_window_bytes, Some(1_048_576));
        assert_eq!(args.h2_max_concurrent_streams, Some(2048));
    }

    /// `--broadcast-shards` defaults to `1` (CU-86aj3vpnh) — the historical
    /// single-channel path. Clear the env var first so a leaked
    /// `KONFIG_BROADCAST_SHARDS` in the runner shell can't perturb the default.
    #[test]
    fn args_parse_broadcast_shards_default_one() {
        // SAFETY: test process; clap reads this env var during parse and we
        // only touch it here to make the default deterministic.
        unsafe {
            std::env::remove_var("KONFIG_BROADCAST_SHARDS");
        }
        let args =
            Args::try_parse_from(["konfig", "--name", "cfg", "--tls=false"]).expect("must parse");
        assert_eq!(args.broadcast_shards, 1, "broadcast-shards defaults to 1");
    }

    #[test]
    fn args_parse_broadcast_shards_explicit() {
        let args = Args::try_parse_from([
            "konfig",
            "--name",
            "cfg",
            "--tls=false",
            "--broadcast-shards",
            "8",
        ])
        .expect("must parse");
        assert_eq!(args.broadcast_shards, 8);
    }

    #[test]
    fn args_parse_secret_namespaces_comma_split() {
        let args = Args::try_parse_from([
            "konfig",
            "--name",
            "cfg",
            "--tls=false",
            "--secret-namespaces",
            "trading,risk,ops",
        ])
        .expect("must parse");
        assert_eq!(args.secret_namespaces, vec!["trading", "risk", "ops"]);
    }

    #[test]
    fn args_parse_missing_name_fails() {
        // No --name + no KONFIG_NAME env → clap rejects.
        // Note: env vars are read by clap so we explicitly clear KONFIG_NAME
        // via the OS env in case the test runner has it set.
        let prev = std::env::var("KONFIG_NAME").ok();
        // SAFETY: tests in this module are gated to single-thread via
        // RUST_TEST_THREADS=1 (BUILD.bazel), so racing on env is impossible.
        unsafe {
            std::env::remove_var("KONFIG_NAME");
        }
        let result = Args::try_parse_from(["konfig", "--tls=false"]);
        // Restore env regardless of assertion outcome.
        if let Some(v) = prev {
            unsafe {
                std::env::set_var("KONFIG_NAME", v);
            }
        }
        assert!(result.is_err(), "missing required --name must error");
    }

    // Default (no `snmalloc_profiling` feature) build of the metrics
    // server must answer `/debug/heap-profile.pprof` with a 404 carrying
    // a body that points operators at the heapprof Bazel target. We
    // exercise the handler directly so the test covers the stub on the
    // default `:konfig` rust_library — Bazel `:test` runs against that
    // crate and never has `snmalloc_profiling` on (the heapprof variant
    // has its own opt-in target with no test entry).
    #[cfg(not(feature = "snmalloc_profiling"))]
    #[tokio::test]
    async fn heap_profile_handler_default_build_returns_404_with_rebuild_hint() {
        use axum::body::to_bytes;
        use axum::http::StatusCode;
        let resp = heap_profile_handler().await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(resp.into_body(), 1024)
            .await
            .expect("body collects");
        let text = std::str::from_utf8(&body).expect("body utf-8");
        assert!(
            text.contains("konfig_bin_heapprof"),
            "404 body must name the rebuild target, got: {text}"
        );
    }

    // Enabled-path smoke. Compiled only when the `snmalloc_profiling`
    // feature is set (i.e. when the test target depends on
    // `:snmalloc_rs_profiling`). Today the in-tree `:test` rust_test
    // target is wired against the default `:konfig` library and won't
    // exercise this branch, but the gate guarantees the enabled
    // surface still compiles in CI follow-ups that flip on the
    // feature for coverage.
    #[cfg(feature = "snmalloc_profiling")]
    #[tokio::test]
    async fn heap_profile_handler_profiling_build_returns_octet_stream() {
        use axum::http::{StatusCode, header};
        let resp = heap_profile_handler().await;
        // Snapshot may legitimately be empty when SNMALLOC_PROFILE was
        // built off at C link time; in that case the writer still
        // emits a valid (empty-sample) gzipped pprof body and we
        // expect 200. The 5xx branch only fires on real encoder
        // failure.
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        assert_eq!(ct, "application/octet-stream");
    }
}
