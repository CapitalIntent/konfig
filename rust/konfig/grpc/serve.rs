//! Server startup/wiring: HTTP/2 SETTINGS overrides + `serve` (drain orchestration,
//! per-namespace map construction, background stale-seconds sampler, graceful shutdown).

use super::*;

/// Initial capacity for per-namespace `DashMap`s in `serve()`.  Typical pod
/// fans out across 10–50 namespaces; 64 is the next power of two and
/// eliminates the early `RawTable::reserve_rehash` calls (~10 ms self-CPU
/// hit observed in pyroscope profile CU-86aj360ae) before the maps reach
/// steady state.
const NAMESPACE_MAP_INITIAL_CAPACITY: usize = 64;

// ── Startup ───────────────────────────────────────────────────────────────────

/// Apply optional HTTP/2 `SETTINGS` overrides to the tonic server builder
/// (bench knob for CU-86aj37q7a). Each argument is `None` by default — only
/// the values the operator passed on the CLI are pushed through; tonic /
/// h2 keep their own defaults otherwise. Raising these reduces
/// `h2::Prioritize::poll_complete` self-CPU on large Subscribe fan-outs but
/// increases per-stream RAM — sweep before changing the default.
pub(crate) fn apply_h2_overrides(
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

    // Clamp the shard count once at startup so every namespace's ShardSet is
    // created with a valid `1..=16` value (CU-86aj3vpnh). Out-of-range CLI
    // input is clamped (not rejected) so a typo degrades gracefully.
    let broadcast_shards = cfg
        .broadcast_shards
        .clamp(MIN_BROADCAST_SHARDS, MAX_BROADCAST_SHARDS);
    if broadcast_shards != cfg.broadcast_shards {
        warn!(
            requested = cfg.broadcast_shards,
            clamped = broadcast_shards,
            "broadcast-shards out of range — clamped to 1..=16"
        );
    }

    let namespace_broadcasts: Arc<DashMap<String, ShardSet>> =
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

    info!(authz_mode = ?cfg.authz_mode, "per-tenant authorization mode");

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
        coalesce_window: cfg.coalesce_window,
        broadcast_shards,
        authz_mode: cfg.authz_mode,
        acl_table: cfg.acl_table,
        acl_synced: cfg.acl_synced,
        schema_table: cfg.schema_table,
        quota_mode: cfg.quota_mode,
        quota_table: cfg.quota_table,
        quota_synced: cfg.quota_synced,
        subscriber_counts: cfg.subscriber_counts,
        default_max_subscribers: cfg.default_max_subscribers,
        apply_limiter: cfg.apply_limiter,
        default_max_applies_per_second: cfg.default_max_applies_per_second,
    };
    let svc = KonfigServiceServer::new(server);

    let mut builder = tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(20)))
        .http2_keepalive_timeout(Some(std::time::Duration::from_secs(10)))
        // Accept HTTP/1.1 in addition to the default h2 prior-knowledge path so
        // grpc-Web clients (the Backstage browser plugin, CU-86ahzwhg4) can
        // reach konfig directly on port 50051 without an Envoy proxy. Standard
        // gRPC clients still negotiate h2 unchanged — h1 is purely additive.
        // The actual grpc-Web ⇄ gRPC frame translation is the `GrpcWebLayer`
        // mounted below via `.layer(..)`. Enabling h1 has no effect on h2 RPCs.
        .accept_http1(true);

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

    // Mount the grpc-Web translation layer (CU-86ahzwhg4). In tonic 0.14
    // `.layer(..)` wraps every service added *after* it, so it must precede
    // the `.add_service(..)` calls. `GrpcWebLayer` inspects the request
    // `content-type`: `application/grpc-web*` requests are translated to/from
    // standard gRPC, while plain `application/grpc` (h2) requests pass through
    // untouched — so existing gRPC clients, the `tonic_health` service, the
    // mTLS path, and the `serve_with_shutdown` drain all keep working.
    if let Some(reporter) = cfg.health_reporter {
        let health_svc = tonic_health::pb::health_server::HealthServer::new(
            tonic_health::server::HealthService::from_health_reporter(reporter),
        );
        builder
            .layer(tonic_web::GrpcWebLayer::new())
            .add_service(health_svc)
            .add_service(svc)
            .serve_with_shutdown(cfg.addr, shutdown_future)
            .await
    } else {
        builder
            .layer(tonic_web::GrpcWebLayer::new())
            .add_service(svc)
            .serve_with_shutdown(cfg.addr, shutdown_future)
            .await
    }
}
