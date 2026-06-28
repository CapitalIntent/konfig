//! `KonfigService` tonic trait implementation: per-RPC drain guard, authz guard,
//! entry logging, OTEL span status, and delegation to the per-RPC handler modules.

use super::*;

use crate::tenant_cache::{
    AccountedStream, config_cost, config_event_cost, secret_cost, secret_event_cost,
};

#[tonic::async_trait]
impl KonfigService for KonfigServer {
    // ── OTEL root spans + entry logging ─────────────────────────────────────
    //
    // Each RPC method carries a `#[tracing::instrument]` root span named after
    // the RPC. `namespace`/`name` are recorded from the request up front;
    // `status_code` starts `Empty` and is filled by `record_status` once the
    // handler resolves. `client_addr`/`request_id` also start `Empty` and are
    // filled by `log_rpc_entry`, which records them on the span (so traces
    // correlate) AND emits exactly one entry-level `info!` per RPC carrying
    // `rpc`, `namespace`, `name` (where applicable), `client_addr`,
    // `request_id` (CU-86ahrwd64 structured logging). `skip_all` keeps the
    // (possibly large) request body and `&self` out of the span — only the
    // explicitly-listed fields are emitted. When the `tracing-opentelemetry`
    // layer is active (OTLP endpoint set), these become OTLP spans; otherwise
    // they are ordinary `tracing` spans on the fmt subscriber. Child spans on
    // watcher/cache/broadcast are a follow-up (Phase 7, CU-86ahzwj3k).
    #[tracing::instrument(
        name = "konfig.Get",
        skip_all,
        fields(namespace = %request.get_ref().namespace, name = %request.get_ref().name, client_addr = tracing::field::Empty, request_id = tracing::field::Empty, status_code = tracing::field::Empty),
    )]
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<Config>, Status> {
        log_rpc_entry(
            "konfig.Get",
            &request,
            Some(&request.get_ref().namespace),
            Some(&request.get_ref().name),
        );
        check_drain(&self.draining)?;
        self.authorize(
            &request,
            Verb::Read,
            &request.get_ref().namespace,
            &request.get_ref().name,
        )?;
        // CU-86aj8pvg3 (MT-4): attribute the served Config's bytes to the tenant.
        let acct = self.cache_accountant(&request, "config");
        let result = get::handle_get(Arc::clone(&self.cache), request.into_inner()).await;
        if let (Some(acct), Ok(resp)) = (&acct, &result) {
            acct.record_cost(config_cost(resp.get_ref()));
        }
        record_status(result)
    }

    type GetAllStream = AccountedStream<ReceiverStream<Result<Config, Status>>, Config>;

    #[tracing::instrument(
        name = "konfig.GetAll",
        skip_all,
        fields(namespace = %request.get_ref().namespace, client_addr = tracing::field::Empty, request_id = tracing::field::Empty, status_code = tracing::field::Empty),
    )]
    async fn get_all(
        &self,
        request: Request<GetAllRequest>,
    ) -> Result<Response<Self::GetAllStream>, Status> {
        log_rpc_entry(
            "konfig.GetAll",
            &request,
            Some(&request.get_ref().namespace),
            None,
        );
        check_drain(&self.draining)?;
        // Name-less RPC: require `read` across the whole namespace ("*").
        self.authorize(&request, Verb::Read, &request.get_ref().namespace, "*")?;
        // CU-86aj8pvg3 (MT-4): wrap the stream so every served Config is
        // attributed to the tenant as it is delivered.
        let acct = self.cache_accountant(&request, "config");
        record_status(
            get::handle_get_all(Arc::clone(&self.cache), request.into_inner())
                .await
                .map(|resp| {
                    Response::new(AccountedStream::new(resp.into_inner(), acct, config_cost))
                }),
        )
    }

    #[tracing::instrument(
        name = "konfig.Apply",
        skip_all,
        fields(namespace = %request.get_ref().namespace, name = %request.get_ref().name, client_addr = tracing::field::Empty, request_id = tracing::field::Empty, status_code = tracing::field::Empty),
    )]
    async fn apply(
        &self,
        request: Request<ApplyRequest>,
    ) -> Result<Response<ApplyResponse>, Status> {
        log_rpc_entry(
            "konfig.Apply",
            &request,
            Some(&request.get_ref().namespace),
            Some(&request.get_ref().name),
        );
        check_drain(&self.draining)?;
        self.authorize(
            &request,
            Verb::Write,
            &request.get_ref().namespace,
            &request.get_ref().name,
        )?;
        // Per-tenant apply rate-limit (CU-86aj8pvf1, MT-3): one token per Apply;
        // RESOURCE_EXHAUSTED when the bucket is empty.
        self.rate_limit_apply(&request, "apply", 1)?;

        // Audit (CU-86ahrwd6h): capture identity + request facets before
        // `into_inner()` consumes the request, run the handler, emit the record.
        let identity = identity::extract_identity(&request);
        let addr = client_addr(&request);
        let rid = request_id(&request);
        let ns = request.get_ref().namespace.clone();
        let name = request.get_ref().name.clone();
        let schema_version = parse_config_schema_version(&request.get_ref().yaml_content);

        let result = apply::handle_apply(
            self.kube_client.clone(),
            Arc::clone(&self.schema_table),
            request.into_inner(),
        )
        .await;
        let rec = audit::AuditRecord {
            rpc: "Apply".into(),
            namespace: ns,
            name,
            client_identity: identity.id,
            client_addr: addr,
            result: audit::result_str(&result),
            schema_version,
            resource_version: audit::resource_version_of(&result, |r| &r.resource_version),
            timestamp_ms: audit::now_ms(),
            request_id: rid,
        };
        audit::emit(&rec);
        audit::maybe_emit_k8s_event(&self.kube_client, &rec).await;
        record_status(result)
    }

    // `BatchApply` — apply several configs as ONE atomic-GATE batch. All items
    // are validated (parse + JSON Schema + schema_version monotonicity) before
    // any write; a single gate failure rejects the whole batch with zero
    // writes. Honest caveat (see `apply::handle_batch_apply` + the proto rpc
    // doc): K8s server-side apply has no cross-object transaction, so a
    // mid-batch apiserver error AFTER the gate can still leave a partial apply.
    // The gate eliminates the COMMON stale-version cause, not the writes'
    // non-transactionality.
    #[tracing::instrument(
        name = "konfig.BatchApply",
        skip_all,
        fields(item_count = request.get_ref().items.len(), client_addr = tracing::field::Empty, request_id = tracing::field::Empty, status_code = tracing::field::Empty),
    )]
    async fn batch_apply(
        &self,
        request: Request<BatchApplyRequest>,
    ) -> Result<Response<BatchApplyResponse>, Status> {
        // No single namespace/name for the whole batch — pass `None, None`.
        log_rpc_entry("konfig.BatchApply", &request, None, None);
        check_drain(&self.draining)?;
        // Per-item authz: EVERY target must be writable — any denial rejects the
        // whole batch (no write happens). When authz is Disabled this is a fast
        // no-op per item, same as `apply`.
        for item in &request.get_ref().items {
            self.authorize(&request, Verb::Write, &item.namespace, &item.name)?;
        }
        // Per-tenant apply rate-limit (CU-86aj8pvf1, MT-3): a batch costs one
        // token per item, so batching cannot bypass the per-second rate. Whole
        // batch rejected if the bucket lacks enough tokens (no partial write).
        self.rate_limit_apply(
            &request,
            "batch_apply",
            request.get_ref().items.len() as u32,
        )?;

        // Audit (CU-86ahrwd6h): the mutation log must cover EVERY target, so we
        // emit one record per item. Capture each item's (namespace, name,
        // schema_version) plus the shared request facets before `into_inner()`
        // consumes the request.
        let identity = identity::extract_identity(&request);
        let addr = client_addr(&request);
        let rid = request_id(&request);
        let item_facets: Vec<(String, String, Option<u32>)> = request
            .get_ref()
            .items
            .iter()
            .map(|item| {
                (
                    item.namespace.clone(),
                    item.name.clone(),
                    parse_config_schema_version(&item.yaml_content),
                )
            })
            .collect();

        let result = apply::handle_batch_apply(
            self.kube_client.clone(),
            Arc::clone(&self.schema_table),
            request.into_inner(),
        )
        .await;

        // One audit record per item. On success each record's resource_version
        // is that item's rv, looked up by (namespace, name) from the response
        // results; on a gate failure every record carries the same error string
        // and `resource_version: None`. Borrow `&result` for the lookup BEFORE
        // it is moved into `record_status`.
        let result_str = audit::result_str(&result);
        let now = audit::now_ms();
        for (namespace, name, schema_version) in item_facets {
            let resource_version = result.as_ref().ok().and_then(|resp| {
                resp.get_ref()
                    .results
                    .iter()
                    .find(|r| r.namespace == namespace && r.name == name)
                    .map(|r| r.resource_version.clone())
                    .filter(|rv| !rv.is_empty())
            });
            let rec = audit::AuditRecord {
                rpc: "BatchApply".into(),
                namespace,
                name,
                client_identity: identity.id.clone(),
                client_addr: addr.clone(),
                result: result_str.clone(),
                schema_version,
                resource_version,
                timestamp_ms: now,
                request_id: rid.clone(),
            };
            audit::emit(&rec);
            audit::maybe_emit_k8s_event(&self.kube_client, &rec).await;
        }
        record_status(result)
    }

    #[tracing::instrument(
        name = "konfig.DryRunApply",
        skip_all,
        fields(namespace = %request.get_ref().namespace, name = %request.get_ref().name, client_addr = tracing::field::Empty, request_id = tracing::field::Empty, status_code = tracing::field::Empty),
    )]
    async fn dry_run_apply(
        &self,
        request: Request<DryRunApplyRequest>,
    ) -> Result<Response<DryRunApplyResponse>, Status> {
        log_rpc_entry(
            "konfig.DryRunApply",
            &request,
            Some(&request.get_ref().namespace),
            Some(&request.get_ref().name),
        );
        check_drain(&self.draining)?;
        // Apply-shaped: previews a write AND reveals the target's current
        // content, so it gates on `Write` like Apply (CU-86ahrg731). NO audit
        // record is emitted — DryRunApply is non-mutating, and the audit log is
        // for mutating RPCs only (the ticket requires it never appears as a
        // write).
        self.authorize(
            &request,
            Verb::Write,
            &request.get_ref().namespace,
            &request.get_ref().name,
        )?;
        record_status(
            apply::handle_dry_run_apply(self.kube_client.clone(), request.into_inner()).await,
        )
    }

    #[tracing::instrument(
        name = "konfig.Revert",
        skip_all,
        fields(namespace = %request.get_ref().namespace, name = %request.get_ref().name, client_addr = tracing::field::Empty, request_id = tracing::field::Empty, status_code = tracing::field::Empty),
    )]
    async fn revert(
        &self,
        request: Request<RevertRequest>,
    ) -> Result<Response<RevertResponse>, Status> {
        log_rpc_entry(
            "konfig.Revert",
            &request,
            Some(&request.get_ref().namespace),
            Some(&request.get_ref().name),
        );
        self.authorize(
            &request,
            Verb::Write,
            &request.get_ref().namespace,
            &request.get_ref().name,
        )?;

        // Audit (CU-86ahrwd6h). Revert derives its new schema_version
        // server-side, so the request carries none — `schema_version: None`.
        let identity = identity::extract_identity(&request);
        let addr = client_addr(&request);
        let rid = request_id(&request);
        let ns = request.get_ref().namespace.clone();
        let name = request.get_ref().name.clone();

        let result = revert::handle_revert(self.kube_client.clone(), request.into_inner()).await;
        let rec = audit::AuditRecord {
            rpc: "Revert".into(),
            namespace: ns,
            name,
            client_identity: identity.id,
            client_addr: addr,
            result: audit::result_str(&result),
            schema_version: None,
            resource_version: audit::resource_version_of(&result, |r| &r.resource_version),
            timestamp_ms: audit::now_ms(),
            request_id: rid,
        };
        audit::emit(&rec);
        audit::maybe_emit_k8s_event(&self.kube_client, &rec).await;
        record_status(result)
    }

    type SubscribeStream =
        GuardedStream<AccountedStream<ReceiverStream<Result<ConfigEvent, Status>>, ConfigEvent>>;

    #[tracing::instrument(
        name = "konfig.Subscribe",
        skip_all,
        fields(namespace = %request.get_ref().namespace, client_addr = tracing::field::Empty, request_id = tracing::field::Empty, status_code = tracing::field::Empty),
    )]
    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        log_rpc_entry(
            "konfig.Subscribe",
            &request,
            Some(&request.get_ref().namespace),
            None,
        );
        check_drain(&self.draining)?;
        // Name-less RPC: require `read` across the whole namespace ("*").
        self.authorize(&request, Verb::Read, &request.get_ref().namespace, "*")?;
        // Per-tenant subscriber quota (CU-86aj8pvdb, MT-2): RESOURCE_EXHAUSTED
        // over budget. The guard rides the response stream and releases the
        // slot when the client disconnects or the server drains.
        let guard = self.admit_subscriber(&request)?;
        // CU-86aj8pvg3 (MT-4): account every event (replay + live) delivered to
        // this subscriber by wrapping its stream; the fan-out itself is untouched.
        let acct = self.cache_accountant(&request, "config");
        record_status(
            subscribe::handle_subscribe(
                Arc::clone(&self.cache),
                self.kube_client.clone(),
                Arc::clone(&self.namespace_broadcasts),
                Arc::clone(&self.namespace_replay_buffers),
                Arc::clone(&self.watcher_handles),
                self.drain_notify(),
                self.coalesce_window,
                self.broadcast_shards,
                request.into_inner(),
            )
            .await
            .map(|resp| {
                Response::new(GuardedStream::new(
                    AccountedStream::new(resp.into_inner(), acct, config_event_cost),
                    guard,
                ))
            }),
        )
    }

    // ── Secret RPCs ───────────────────────────────────────────────────────────

    #[tracing::instrument(
        name = "konfig.GetSecret",
        skip_all,
        fields(namespace = %request.get_ref().namespace, name = %request.get_ref().name, client_addr = tracing::field::Empty, request_id = tracing::field::Empty, status_code = tracing::field::Empty),
    )]
    async fn get_secret(
        &self,
        request: Request<GetSecretRequest>,
    ) -> Result<Response<SecretResponse>, Status> {
        log_rpc_entry(
            "konfig.GetSecret",
            &request,
            Some(&request.get_ref().namespace),
            Some(&request.get_ref().name),
        );
        check_drain(&self.draining)?;
        self.authorize(
            &request,
            Verb::Read,
            &request.get_ref().namespace,
            &request.get_ref().name,
        )?;
        // CU-86aj8pvg3 (MT-4): attribute the served secret's bytes to the tenant.
        let acct = self.cache_accountant(&request, "secret");
        let result =
            secret_get::handle_get_secret(Arc::clone(&self.secret_cache), request.into_inner())
                .await;
        if let (Some(acct), Ok(resp)) = (&acct, &result) {
            acct.record_cost(secret_cost(resp.get_ref()));
        }
        record_status(result)
    }

    type GetAllSecretsStream =
        AccountedStream<ReceiverStream<Result<SecretResponse, Status>>, SecretResponse>;

    #[tracing::instrument(
        name = "konfig.GetAllSecrets",
        skip_all,
        fields(namespace = %request.get_ref().namespace, client_addr = tracing::field::Empty, request_id = tracing::field::Empty, status_code = tracing::field::Empty),
    )]
    async fn get_all_secrets(
        &self,
        request: Request<GetAllSecretsRequest>,
    ) -> Result<Response<Self::GetAllSecretsStream>, Status> {
        log_rpc_entry(
            "konfig.GetAllSecrets",
            &request,
            Some(&request.get_ref().namespace),
            None,
        );
        check_drain(&self.draining)?;
        // Name-less RPC: require `read` across the whole namespace ("*").
        self.authorize(&request, Verb::Read, &request.get_ref().namespace, "*")?;
        // CU-86aj8pvg3 (MT-4): wrap the stream so every served secret is
        // attributed to the tenant as it is delivered.
        let acct = self.cache_accountant(&request, "secret");
        record_status(
            secret_get::handle_get_all_secrets(
                Arc::clone(&self.secret_cache),
                request.into_inner(),
            )
            .await
            .map(|resp| Response::new(AccountedStream::new(resp.into_inner(), acct, secret_cost))),
        )
    }

    #[tracing::instrument(
        name = "konfig.ApplySecret",
        skip_all,
        fields(namespace = %request.get_ref().namespace, name = %request.get_ref().name, client_addr = tracing::field::Empty, request_id = tracing::field::Empty, status_code = tracing::field::Empty),
    )]
    async fn apply_secret(
        &self,
        request: Request<ApplySecretRequest>,
    ) -> Result<Response<ApplySecretResponse>, Status> {
        log_rpc_entry(
            "konfig.ApplySecret",
            &request,
            Some(&request.get_ref().namespace),
            Some(&request.get_ref().name),
        );
        check_drain(&self.draining)?;
        self.authorize(
            &request,
            Verb::Write,
            &request.get_ref().namespace,
            &request.get_ref().name,
        )?;
        // Per-tenant apply rate-limit (CU-86aj8pvf1, MT-3): secret applies share
        // the identity's apply bucket, so they cannot bypass the rate.
        self.rate_limit_apply(&request, "apply_secret", 1)?;

        // Audit (CU-86ahrwd6h).
        let identity = identity::extract_identity(&request);
        let addr = client_addr(&request);
        let rid = request_id(&request);
        let ns = request.get_ref().namespace.clone();
        let name = request.get_ref().name.clone();
        let schema_version = parse_secret_schema_version(&request.get_ref().yaml_content);

        let result =
            secret_apply::handle_apply_secret(self.kube_client.clone(), request.into_inner()).await;
        let rec = audit::AuditRecord {
            rpc: "ApplySecret".into(),
            namespace: ns,
            name,
            client_identity: identity.id,
            client_addr: addr,
            result: audit::result_str(&result),
            schema_version,
            resource_version: audit::resource_version_of(&result, |r| &r.resource_version),
            timestamp_ms: audit::now_ms(),
            request_id: rid,
        };
        audit::emit(&rec);
        audit::maybe_emit_k8s_event(&self.kube_client, &rec).await;
        record_status(result)
    }

    type SubscribeSecretsStream =
        GuardedStream<AccountedStream<ReceiverStream<Result<SecretEvent, Status>>, SecretEvent>>;

    #[tracing::instrument(
        name = "konfig.SubscribeSecrets",
        skip_all,
        fields(namespace = %request.get_ref().namespace, client_addr = tracing::field::Empty, request_id = tracing::field::Empty, status_code = tracing::field::Empty),
    )]
    async fn subscribe_secrets(
        &self,
        request: Request<SubscribeSecretsRequest>,
    ) -> Result<Response<Self::SubscribeSecretsStream>, Status> {
        log_rpc_entry(
            "konfig.SubscribeSecrets",
            &request,
            Some(&request.get_ref().namespace),
            None,
        );
        check_drain(&self.draining)?;
        // Name-less RPC: require `read` across the whole namespace ("*").
        self.authorize(&request, Verb::Read, &request.get_ref().namespace, "*")?;
        // Per-tenant subscriber quota (CU-86aj8pvdb, MT-2): SubscribeSecrets
        // counts against the same per-identity budget as Subscribe.
        let guard = self.admit_subscriber(&request)?;
        // CU-86aj8pvg3 (MT-4): account every secret event (replay + live)
        // delivered to this subscriber by wrapping its stream.
        let acct = self.cache_accountant(&request, "secret");
        record_status(
            subscribe_secrets::handle_subscribe_secrets(
                self.kube_client.clone(),
                Arc::clone(&self.secret_cache),
                Arc::clone(&self.secret_namespace_broadcasts),
                self.drain_notify(),
                request.into_inner(),
            )
            .await
            .map(|resp| {
                Response::new(GuardedStream::new(
                    AccountedStream::new(resp.into_inner(), acct, secret_event_cost),
                    guard,
                ))
            }),
        )
    }
}
