//! `Apply` handler — creates or updates a `Config.konfig.io/v1` CRD.
//!
//! Flow:
//! 1. Parse `yaml_content` as `ConfigSpec`.
//! 2. Validate `content` against the registered JSON Schema for
//!    `(namespace, name)`, if any (CU-86ahrwd5g). No schema ⇒ accept anything.
//! 3. Fetch current CRD to check `schema_version` monotonicity.
//! 4. Reject with `FAILED_PRECONDITION` if incoming version ≤ current.
//! 5. Patch the CRD with server-side apply; retry 409 up to 3 times.
//! 6. Return `ApplyResponse { resource_version }`.
//!
//! ## Why validation lives HERE, not in `apply_spec`
//!
//! Schema validation is wired into the `Apply` RPC path ONLY (`apply_inner`,
//! after the `ConfigSpec` parse, before `apply_spec`). It is deliberately NOT
//! inside the shared `apply_spec`: `revert.rs` calls `apply_spec` to replay
//! HISTORICAL `content`, and a schema registered AFTER that content was first
//! applied must not block a legitimate rollback to it (CU-86ahrwd5g). So
//! `apply_spec`'s signature is unchanged and revert bypasses validation.

use std::sync::Arc;
use std::time::{Duration, Instant};

use kube::Client;
use kube::api::{Api, Patch, PatchParams};
use kube::core::DynamicObject;
use serde_json::json;
use tonic::{Response, Status};
use tracing::{Instrument, debug, info, warn};

use crate::grpc::jittered_retry_ms;
use crate::metrics::{APPLY_DURATION, APPLY_TOTAL};
use crate::proto::{
    ApplyRequest, ApplyResponse, BatchApplyRequest, BatchApplyResponse, BatchApplyResult,
    DryRunApplyRequest, DryRunApplyResponse,
};
use crate::schema::SchemaTable;
use crate::types::ConfigSpec;
use crate::watcher::{GROUP, VERSION, config_api_resource};

const RETRY_DELAYS_MS: [u64; 2] = [100, 200];

pub async fn handle_apply(
    kube_client: Client,
    schema_table: Arc<SchemaTable>,
    req: ApplyRequest,
) -> Result<Response<ApplyResponse>, Status> {
    debug!(namespace = %req.namespace, name = %req.name, "Apply RPC");

    apply_inner(
        &req.namespace,
        &req.name,
        &req.yaml_content,
        kube_client,
        &schema_table,
    )
    .await
}

pub async fn apply_inner(
    namespace: &str,
    name: &str,
    yaml_content: &str,
    kube_client: Client,
    schema_table: &SchemaTable,
) -> Result<Response<ApplyResponse>, Status> {
    let spec: ConfigSpec = serde_yaml::from_str(yaml_content)
        .map_err(|e| Status::invalid_argument(format!("invalid YAML: {e}")))?;

    // JSON Schema gate (CU-86ahrwd5g) — runs on the Apply RPC path ONLY (see the
    // module docs above for why it is NOT in `apply_spec`, which revert reuses).
    // No schema registered for `(namespace, name)` ⇒ `Ok(())`, accept anything.
    validate_against_schema(schema_table, namespace, name, &spec)?;

    apply_spec(namespace, name, spec, kube_client).await
}

/// One parsed item of a [`handle_batch_apply`] batch — the `(namespace, name)`
/// target plus its parsed `ConfigSpec`. Produced by the pure [`parse_batch_items`]
/// gate (which also rejects duplicate targets) so the rest of the batch flow
/// works on validated, deduped items.
#[derive(Debug, Clone)]
pub(crate) struct BatchItem {
    pub namespace: String,
    pub name: String,
    pub spec: ConfigSpec,
}

/// `BatchApply` handler — apply several configs as one atomic-GATE batch.
///
/// ## Honest atomicity note (read this before relying on "all-or-none")
///
/// Kubernetes server-side apply has **no cross-object transaction**: there is
/// no single atomic multi-resource write. So "all-or-none" is achievable ONLY
/// at the **gate** layer below. Every item is validated (YAML parse, JSON
/// Schema, schema_version monotonicity) BEFORE any write; if ANY item fails the
/// gate the whole batch is rejected with the matching `Status` and performs
/// ZERO writes. After the gate passes, items are applied sequentially. A
/// mid-batch apiserver error (e.g. exhausted-409, a concurrent writer) AFTER
/// the gate can still leave a PARTIAL apply — the gate eliminates the common
/// stale-version torn-read cause of a partial write but cannot make the writes
/// themselves transactional. This is NOT true atomicity.
///
/// ## Why apply in (namespace, name) order ⇒ deterministic broadcast order
///
/// Broadcast is watcher-driven: `apply_spec` just patches the CRD; the kube
/// watcher observes the etcd change and broadcasts it (default coalesce window
/// 0 = immediate). So WRITE order = etcd order = watch-event order = broadcast
/// order. Applying items in `(namespace, name)` order therefore makes every
/// subscriber observe a deterministic event sequence. The handler controls
/// broadcast order purely by controlling write order — it never broadcasts
/// directly.
///
/// ## Flow
/// 1. Empty batch → `INVALID_ARGUMENT`.
/// 2. Parse + dedup every item ([`parse_batch_items`]): per-item YAML parse
///    (`INVALID_ARGUMENT` naming the offender) and a duplicate-target reject
///    (two writes to one `(namespace, name)` is ambiguous).
/// 3. Schema gate (all-or-none): [`validate_against_schema`] for every item;
///    first violation rejects the whole batch (`FAILED_PRECONDITION`), zero
///    writes.
/// 4. Monotonicity gate (all-or-none): read the current schema_version for
///    every item ([`fetch_current_schema_version`], read-only gets), then the
///    pure [`gate_batch_versions`] checks [`schema_version_decision`] for each;
///    first failure rejects the whole batch (`FAILED_PRECONDITION`), still zero
///    writes.
/// 5. Sort items by `(namespace, name)` ([`sort_batch_items`]).
/// 6. Apply each sorted item via [`apply_spec`] (which re-checks monotonicity
///    per item — defense-in-depth against a TOCTOU concurrent writer between
///    the gate read and the write; that re-check is intentional).
/// 7. Return one [`BatchApplyResult`] per item, already in sorted order.
pub async fn handle_batch_apply(
    kube_client: Client,
    schema_table: Arc<SchemaTable>,
    req: BatchApplyRequest,
) -> Result<Response<BatchApplyResponse>, Status> {
    debug!(item_count = req.items.len(), "BatchApply RPC");

    // (1)+(2) Parse + dedup. Empty batch is rejected inside the helper.
    let mut items = parse_batch_items(&req.items)?;

    // (3) Schema gate — all-or-none. First violation rejects the whole batch
    // with FAILED_PRECONDITION and performs ZERO writes (no write happens until
    // step 6 below).
    for item in &items {
        validate_against_schema(&schema_table, &item.namespace, &item.name, &item.spec)?;
    }

    // (4) Monotonicity gate — all-or-none. Read the current schema_version for
    // EVERY item first (read-only kube gets; sequential to avoid pulling in a
    // concurrency dep), then run the pure decision over all of them. First
    // stale item rejects the whole batch — still ZERO writes performed.
    let ar = config_api_resource();
    let mut currents = Vec::with_capacity(items.len());
    for item in &items {
        let api: Api<DynamicObject> =
            Api::namespaced_with(kube_client.clone(), &item.namespace, &ar);
        currents.push(fetch_current_schema_version(&api, &item.name).await?);
    }
    gate_batch_versions(&items, &currents)?;

    // (5) Sort so writes land — and subscribers observe events — in a
    // deterministic (namespace, name) order.
    sort_batch_items(&mut items);

    // (6) Apply each item. The gate above guarantees the COMMON failure causes
    // are eliminated; a per-item apiserver error here still surfaces (and can
    // leave a partial apply — see the honesty note in the module/fn docs).
    // `apply_spec` re-checks monotonicity per item (defense-in-depth).
    let mut results = Vec::with_capacity(items.len());
    for item in items {
        let BatchItem {
            namespace,
            name,
            spec,
        } = item;
        let resp = apply_spec(&namespace, &name, spec, kube_client.clone()).await?;
        results.push(BatchApplyResult {
            namespace,
            name,
            resource_version: resp.into_inner().resource_version,
        });
    }

    Ok(Response::new(BatchApplyResponse { results }))
}

/// Pure parse + dedup gate for a [`handle_batch_apply`] batch — no kube I/O, so
/// the parse-error and duplicate-target contracts are unit-testable.
///
/// Rejects, in order:
///   - an empty batch (`INVALID_ARGUMENT` — a batch must apply something);
///   - any item whose `yaml_content` does not parse as a `ConfigSpec`
///     (`INVALID_ARGUMENT` naming the offending `namespace/name`);
///   - a duplicate `(namespace, name)` target within the batch
///     (`INVALID_ARGUMENT` — two writes to one target is ambiguous).
///
/// On success returns the parsed [`BatchItem`]s in INPUT order (the caller
/// sorts later via [`sort_batch_items`]).
pub(crate) fn parse_batch_items(items: &[ApplyRequest]) -> Result<Vec<BatchItem>, Status> {
    if items.is_empty() {
        return Err(Status::invalid_argument(
            "batch must contain at least one item",
        ));
    }

    let mut parsed = Vec::with_capacity(items.len());
    let mut seen = std::collections::HashSet::with_capacity(items.len());
    for req in items {
        if !seen.insert((req.namespace.as_str(), req.name.as_str())) {
            return Err(Status::invalid_argument(format!(
                "duplicate item {}/{} in batch — two writes to one target is ambiguous",
                req.namespace, req.name
            )));
        }
        let spec: ConfigSpec = serde_yaml::from_str(&req.yaml_content).map_err(|e| {
            Status::invalid_argument(format!(
                "invalid YAML for {}/{}: {e}",
                req.namespace, req.name
            ))
        })?;
        parsed.push(BatchItem {
            namespace: req.namespace.clone(),
            name: req.name.clone(),
            spec,
        });
    }
    Ok(parsed)
}

/// Pure monotonicity gate over a whole batch — given the parsed `items` and the
/// `currents[i]` = stored schema_version for `items[i]` (positionally aligned),
/// return `Ok(())` only when EVERY item strictly increases its current version.
///
/// All-or-none: the FIRST stale item (incoming ≤ current) returns its
/// `FAILED_PRECONDITION` and the whole batch is rejected — by contract the
/// caller has performed ZERO writes at this point. Reuses the same
/// [`schema_version_decision`] gate as the single-item Apply path, so the per
/// item accept/reject decision is identical. No kube I/O (the reads live in
/// [`fetch_current_schema_version`]); this is the decision, so it is
/// unit-testable without a live `Api`.
pub(crate) fn gate_batch_versions(items: &[BatchItem], currents: &[u32]) -> Result<(), Status> {
    for (item, &current) in items.iter().zip(currents.iter()) {
        schema_version_decision(item.spec.schema_version, current)?;
    }
    Ok(())
}

/// Pure sort — reorder a batch into `(namespace, name)` ascending so the writes
/// land (and subscribers observe the events) in a deterministic order. The
/// secondary key is `name`, so items in the same namespace are name-ordered.
/// Separated from [`handle_batch_apply`] so the ordering is unit-testable
/// without kube.
pub(crate) fn sort_batch_items(items: &mut [BatchItem]) {
    items.sort_by(|a, b| {
        a.namespace
            .cmp(&b.namespace)
            .then_with(|| a.name.cmp(&b.name))
    });
}

/// Validate a proposed `ConfigSpec`'s `content` against the schema registered
/// for `(namespace, config_name)`, if any. On violation, returns
/// `Status::failed_precondition` carrying a clean, concatenated list of the
/// validation errors. No registered schema ⇒ `Ok(())`.
///
/// Pure decision over the borrowed table — no kube I/O — so the
/// reject-on-violation contract is unit-testable with a fake `SchemaTable`.
pub(crate) fn validate_against_schema(
    schema_table: &SchemaTable,
    namespace: &str,
    config_name: &str,
    spec: &ConfigSpec,
) -> Result<(), Status> {
    match schema_table.validate(namespace, config_name, &spec.content) {
        Ok(()) => Ok(()),
        Err(errors) => {
            warn!(
                namespace,
                name = config_name,
                error_count = errors.len(),
                "Apply rejected: content violates registered JSON Schema"
            );
            Err(Status::failed_precondition(format!(
                "content does not satisfy registered JSON Schema for {namespace}/{config_name}: {}",
                errors.join("; ")
            )))
        }
    }
}

/// `DryRunApply` handler — preview what an `Apply` would change WITHOUT ever
/// patching K8s or touching the cache (CU-86ahrg731).
///
/// Flow:
/// 1. Parse `yaml_content` as `ConfigSpec` (`INVALID_ARGUMENT` on parse error,
///    same gate as `Apply`).
/// 2. `api.get(name)` to read the CURRENT Config (404 → current absent: empty
///    content, schema_version 0). This is the ONLY kube call — a read.
/// 3. Run the SAME `schema_version_decision` monotonicity gate as `Apply`
///    (`FAILED_PRECONDITION` when proposed ≤ current).
/// 4. Assemble the current vs proposed diff and return it.
///
/// Never calls `api.patch` / any write; never updates the cache; never touches
/// the `APPLY_TOTAL` / `APPLY_DURATION` counters (those are reserved for real
/// applies). Idempotent: identical request → byte-identical response.
pub async fn handle_dry_run_apply(
    kube_client: Client,
    req: DryRunApplyRequest,
) -> Result<Response<DryRunApplyResponse>, Status> {
    debug!(namespace = %req.namespace, name = %req.name, "DryRunApply RPC");

    let spec: ConfigSpec = serde_yaml::from_str(&req.yaml_content)
        .map_err(|e| Status::invalid_argument(format!("invalid YAML: {e}")))?;

    let ar = config_api_resource();
    let api: Api<DynamicObject> = Api::namespaced_with(kube_client, &req.namespace, &ar);

    // READ-ONLY: fetch the current object. 404 → current absent (None).
    let current = match api.get(&req.name).await {
        Ok(obj) => Some(obj),
        Err(kube::Error::Api(ref ae)) if ae.code == 404 => None,
        Err(e) => return Err(Status::unavailable(format!("kube error: {e}"))),
    };

    // Pure assembly + monotonicity gate — no further I/O, no writes.
    dry_run_response(current.as_ref(), &spec).map(Response::new)
}

/// Pure dry-run assembler — given the CURRENT object (or `None` when absent)
/// and the PROPOSED `ConfigSpec`, build the `DryRunApplyResponse` or return the
/// `FAILED_PRECONDITION` the monotonicity gate would surface.
///
/// Separated from the single `api.get` call in `handle_dry_run_apply` so the
/// content-extraction + the reused [`schema_version_decision`] gate are
/// unit-testable without a kube mock. Critically, this fn is structurally
/// incapable of a kube write: it takes no `Api`/`Client`, so it can only read
/// the borrowed object and serialize JSON.
pub(crate) fn dry_run_response(
    current: Option<&DynamicObject>,
    spec: &ConfigSpec,
) -> Result<DryRunApplyResponse, Status> {
    let current_schema_version = current.map(parse_schema_version_from_object).unwrap_or(0);
    let current_content_json = current
        .map(extract_current_content_json)
        .transpose()?
        .unwrap_or_default();

    let proposed_schema_version = spec.schema_version;
    let proposed_content_json = serde_json::to_string(&spec.content)
        .map_err(|e| Status::internal(format!("serialize error: {e}")))?;

    // SAME gate as Apply — proposed must strictly exceed current.
    schema_version_decision(proposed_schema_version, current_schema_version)?;

    Ok(DryRunApplyResponse {
        current_content_json,
        proposed_content_json,
        current_schema_version,
        proposed_schema_version,
    })
}

/// Pure extractor — serialize a current object's `spec.content` to a JSON
/// string. Returns `""` when `spec.content` is absent (mirrors the empty/absent
/// convention for a 404 current). Separated from the kube call so it is
/// unit-testable.
pub(crate) fn extract_current_content_json(obj: &DynamicObject) -> Result<String, Status> {
    match obj.data.get("spec").and_then(|s| s.get("content")) {
        Some(content) => serde_json::to_string(content)
            .map_err(|e| Status::internal(format!("serialize error: {e}"))),
        None => Ok(String::new()),
    }
}

/// Apply a parsed `ConfigSpec` to the cluster via server-side apply.
///
/// Enforces `schema_version` monotonicity, patches with retry, and increments
/// the same `APPLY_TOTAL` counters as the public `Apply` RPC path so Revert is
/// observable as a normal apply.
pub async fn apply_spec(
    namespace: &str,
    name: &str,
    spec: ConfigSpec,
    kube_client: Client,
) -> Result<Response<ApplyResponse>, Status> {
    let started = Instant::now();
    let incoming = spec.schema_version;

    let ar = config_api_resource();
    let api: Api<DynamicObject> = Api::namespaced_with(kube_client, namespace, &ar);

    let current = fetch_current_schema_version(&api, name).await?;

    if let Err(status) = schema_version_decision(incoming, current) {
        warn!(
            incoming,
            current, "Apply rejected: schema_version not increasing"
        );
        APPLY_TOTAL
            .with_label_values(&[namespace, "rejected"])
            .inc();
        APPLY_DURATION
            .with_label_values(&[namespace, "rejected"])
            .observe(started.elapsed().as_secs_f64());
        return Err(status);
    }

    let patch_body = json!({
        "apiVersion": format!("{GROUP}/{VERSION}"),
        "kind": "Config",
        "metadata": { "name": name, "namespace": namespace },
        "spec": serde_json::to_value(&spec)
            .map_err(|e| Status::internal(format!("serialize error: {e}")))?
    });

    match patch_with_retry(&api, name, patch_body).await {
        Ok(rv) => {
            info!(namespace, name, schema_version = incoming, resource_version = %rv, "Apply succeeded");
            APPLY_TOTAL.with_label_values(&[namespace, "ok"]).inc();
            APPLY_DURATION
                .with_label_values(&[namespace, "ok"])
                .observe(started.elapsed().as_secs_f64());
            Ok(Response::new(ApplyResponse {
                resource_version: rv,
            }))
        }
        Err(e) => {
            APPLY_TOTAL.with_label_values(&[namespace, "error"]).inc();
            APPLY_DURATION
                .with_label_values(&[namespace, "error"])
                .observe(started.elapsed().as_secs_f64());
            Err(e)
        }
    }
}

/// Fetch the current schema_version of a Config CRD, or 0 if it does not exist.
///
/// Used by both Apply (to enforce monotonicity) and Revert (to compute the
/// new schema_version when replaying historical content).
pub(crate) async fn fetch_current_schema_version(
    api: &Api<DynamicObject>,
    name: &str,
) -> Result<u32, Status> {
    match api.get(name).await {
        Ok(obj) => Ok(parse_schema_version_from_object(&obj)),
        Err(kube::Error::Api(ref ae)) if ae.code == 404 => Ok(0),
        Err(e) => Err(Status::unavailable(format!("kube error: {e}"))),
    }
}

/// Pure parser — extract `spec.schema_version` from a DynamicObject, or `0`
/// when the field is missing, non-numeric, or exceeds `u32::MAX`. Separated
/// from the async kube call so its branches are unit-testable without a
/// kube mock.
pub(crate) fn parse_schema_version_from_object(obj: &DynamicObject) -> u32 {
    obj.data
        .get("spec")
        .and_then(|s| s.get("schema_version"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32
}

/// Pure monotonicity gate — decide whether an incoming `schema_version` may
/// overwrite the `current` one. CP guarantee: an Apply is accepted **only**
/// when it strictly increases the version, so a stale or replayed write can
/// never clobber a newer one.
///
/// Returns `Ok(())` when `incoming > current`; otherwise `Err` carrying a
/// `Status::failed_precondition` with the same message the RPC surfaces.
/// Extracted from `apply_spec` so the comparison + the exact gRPC code it
/// maps to are unit-testable without a live kube `Api` (the I/O lives in
/// `fetch_current_schema_version`, this is the decision).
pub(crate) fn schema_version_decision(incoming: u32, current: u32) -> Result<(), Status> {
    if incoming <= current {
        Err(Status::failed_precondition(format!(
            "schema_version must be > {current}; got {incoming}"
        )))
    } else {
        Ok(())
    }
}

/// Decision returned by `classify_patch_error` so the (un-mockable) kube I/O
/// loop stays thin and the (pure) decision logic is unit-testable.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PatchRetryDecision {
    /// 409 Conflict and we still have budget — sleep `delay_ms` then retry.
    RetryAfter { delay_ms: u64 },
    /// 409 Conflict and we are out of retry budget — `Status::aborted`.
    AbortRetriesExhausted,
    /// Anything else — `Status::unavailable`.
    Unavailable,
}

/// Pure classifier — no I/O, no logging. Tests cover every branch by
/// constructing `kube::Error::Api(ErrorResponse { code, ... })` directly.
pub(crate) fn classify_patch_error(err: &kube::Error, attempt: usize) -> PatchRetryDecision {
    if super::api_status_code(err) != Some(409) {
        return PatchRetryDecision::Unavailable;
    }
    if attempt < RETRY_DELAYS_MS.len() {
        PatchRetryDecision::RetryAfter {
            delay_ms: RETRY_DELAYS_MS[attempt],
        }
    } else {
        PatchRetryDecision::AbortRetriesExhausted
    }
}

async fn patch_with_retry(
    api: &Api<DynamicObject>,
    name: &str,
    body: serde_json::Value,
) -> Result<String, Status> {
    let pp = PatchParams::apply("konfig.v1").force();
    run_patch_retry_loop(|| async {
        api.patch(name, &pp, &Patch::Apply(body.clone()))
            .await
            .map(|obj| obj.metadata.resource_version.unwrap_or_default())
    })
    .await
}

/// Pure retry loop body — drives a single-attempt patch closure through the
/// `RETRY_DELAYS_MS` ladder so the loop-exit path can be tested without a
/// kube mock. Real callers pass a closure that performs one `api.patch(...)`
/// call; the loop-exit boundary test (CU-86aj14yvh) passes a closure that
/// returns 409 every call and asserts the loop exits with `Status::aborted`
/// after exactly `RETRY_DELAYS_MS.len() + 1` invocations.
pub(crate) async fn run_patch_retry_loop<F, Fut>(mut do_patch: F) -> Result<String, Status>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<String, kube::Error>>,
{
    let mut attempt = 0usize;
    loop {
        // OTEL child span (Phase 7, CU-86ahzwj3k) per patch attempt, nested
        // under the `konfig.Apply` root span so a Jaeger trace shows each 409
        // retry. `level = "debug"` keeps it off the INFO production path; the
        // `attempt` field is a cheap integer (zero-indexed — `attempt + 1` is
        // the human-facing count used in the warn! below).
        let attempt_span = tracing::debug_span!("konfig.apply_attempt", attempt);
        match do_patch().instrument(attempt_span).await {
            Ok(rv) => return Ok(rv),
            Err(e) => match classify_patch_error(&e, attempt) {
                PatchRetryDecision::RetryAfter { delay_ms } => {
                    // ±25 % jitter to break lockstep retries from N clients
                    // racing on the same resourceVersion (see `jittered_retry_ms`).
                    let jittered = jittered_retry_ms(delay_ms);
                    warn!(
                        attempt = attempt + 1,
                        delay_ms = jittered,
                        "Apply: 409 Conflict — retrying",
                    );
                    tokio::time::sleep(Duration::from_millis(jittered)).await;
                    attempt += 1;
                }
                PatchRetryDecision::AbortRetriesExhausted => {
                    return Err(Status::aborted("409 Conflict — exceeded max retries"));
                }
                PatchRetryDecision::Unavailable => {
                    return Err(Status::unavailable(format!("kube patch error: {e}")));
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── validate_against_schema: JSON Schema gate (CU-86ahrwd5g) ────────────
    //
    // The schema gate runs on the Apply RPC path only (apply_inner, before
    // apply_spec; NOT in apply_spec, which revert reuses). These tests drive
    // the extracted pure helper `validate_against_schema` directly with a fake
    // `SchemaTable` (no kube) and assert: (a) no schema registered → Ok
    // (accept anything), (b) registered schema + valid content → Ok, (c)
    // registered schema + violating content → FAILED_PRECONDITION carrying the
    // concatenated error list.

    fn schema_spec(content: serde_json::Value) -> ConfigSpec {
        ConfigSpec {
            schema_version: 1,
            content,
        }
    }

    fn object_schema() -> serde_json::Value {
        serde_json::json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": ["port"],
            "properties": { "port": { "type": "integer" } }
        })
    }

    #[test]
    fn validate_against_schema_no_schema_registered_is_ok() {
        let table = crate::schema::SchemaTable::new();
        let spec = schema_spec(serde_json::json!({ "anything": true }));
        assert!(
            validate_against_schema(&table, "trading", "limits", &spec).is_ok(),
            "no registered schema must accept any content"
        );
    }

    #[test]
    fn validate_against_schema_valid_content_is_ok() {
        let table = crate::schema::SchemaTable::new();
        table
            .insert_for_test("trading", "limits", &object_schema())
            .expect("schema compiles");
        let spec = schema_spec(serde_json::json!({ "port": 8080 }));
        assert!(validate_against_schema(&table, "trading", "limits", &spec).is_ok());
    }

    #[test]
    fn validate_against_schema_violation_is_failed_precondition() {
        let table = crate::schema::SchemaTable::new();
        table
            .insert_for_test("trading", "limits", &object_schema())
            .expect("schema compiles");
        // `port` is a string, not an integer → violates the schema.
        let spec = schema_spec(serde_json::json!({ "port": "nope" }));
        let err = validate_against_schema(&table, "trading", "limits", &spec)
            .expect_err("violating content must be rejected");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(
            err.message().contains("JSON Schema") && err.message().contains("trading/limits"),
            "message must name the schema + target; got {:?}",
            err.message()
        );
    }

    #[test]
    fn validate_against_schema_other_key_unaffected() {
        let table = crate::schema::SchemaTable::new();
        table
            .insert_for_test("trading", "limits", &object_schema())
            .expect("schema compiles");
        // Different namespace / name → no schema there → accept even bad content.
        let spec = schema_spec(serde_json::json!({ "port": "nope" }));
        assert!(validate_against_schema(&table, "risk", "limits", &spec).is_ok());
        assert!(validate_against_schema(&table, "trading", "other", &spec).is_ok());
    }

    // ── schema_version_decision: monotonicity gate + gRPC code mapping ──────
    //
    // CP guarantee (CU-86ahzwgjz): an Apply may only land when it strictly
    // increases the stored schema_version, so a stale/replayed write can
    // never clobber a newer one. These tests drive the extracted pure helper
    // directly — no live kube `Api` — and assert BOTH the accept/reject
    // decision AND that a reject maps to `Code::FailedPrecondition` (the
    // contract consumers retry-or-abort on), not some other status.

    #[test]
    fn schema_version_equal_is_rejected_failed_precondition() {
        let err = schema_version_decision(5, 5).expect_err("equal version must be rejected");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(
            err.message().contains("must be > 5"),
            "message should name the current version; got {:?}",
            err.message(),
        );
    }

    #[test]
    fn schema_version_lower_is_rejected_failed_precondition() {
        let err = schema_version_decision(3, 5).expect_err("lower version must be rejected");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn schema_version_higher_is_accepted() {
        assert!(
            schema_version_decision(6, 5).is_ok(),
            "strictly higher version must be accepted",
        );
    }

    #[test]
    fn schema_version_first_write_over_zero_is_accepted() {
        // `fetch_current_schema_version` returns 0 for a not-yet-existing CRD;
        // the very first Apply (any version ≥ 1) must therefore be accepted.
        assert!(schema_version_decision(1, 0).is_ok());
        // version 0 over a 0 baseline is NOT an increase — still rejected.
        assert_eq!(
            schema_version_decision(0, 0)
                .expect_err("0 over 0 is not an increase")
                .code(),
            tonic::Code::FailedPrecondition,
        );
    }

    #[test]
    fn invalid_yaml_detected() {
        let result = serde_yaml::from_str::<ConfigSpec>("not: [valid: yaml: here");
        assert!(result.is_err());
    }

    #[test]
    fn valid_yaml_parses() {
        let yaml = "schema_version: 3\ncontent:\n  key: value\n";
        let spec: ConfigSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(spec.schema_version, 3);
        assert_eq!(spec.content["key"], "value");
    }

    // ── classify_patch_error: all 4 branches ────────────────────────────────

    fn api_err(code: u16) -> kube::Error {
        kube::Error::Api(kube::core::ErrorResponse {
            status: "Failure".to_string(),
            message: "synthetic".to_string(),
            reason: "synthetic".to_string(),
            code,
        })
    }

    #[test]
    fn classify_409_with_budget_left_retries() {
        let d = classify_patch_error(&api_err(409), 0);
        assert_eq!(d, PatchRetryDecision::RetryAfter { delay_ms: 100 });
        let d = classify_patch_error(&api_err(409), 1);
        assert_eq!(d, PatchRetryDecision::RetryAfter { delay_ms: 200 });
    }

    #[test]
    fn classify_409_at_budget_exhausts() {
        // RETRY_DELAYS_MS has 2 entries — `attempt == 2` is the exhausted path.
        let d = classify_patch_error(&api_err(409), RETRY_DELAYS_MS.len());
        assert_eq!(d, PatchRetryDecision::AbortRetriesExhausted);
        // Going further never re-enters retry mode either.
        let d = classify_patch_error(&api_err(409), RETRY_DELAYS_MS.len() + 5);
        assert_eq!(d, PatchRetryDecision::AbortRetriesExhausted);
    }

    #[test]
    fn classify_non_409_api_error_is_unavailable() {
        for code in [400u16, 403, 404, 410, 500, 503] {
            let d = classify_patch_error(&api_err(code), 0);
            assert_eq!(
                d,
                PatchRetryDecision::Unavailable,
                "code {code} should be Unavailable",
            );
        }
    }

    #[test]
    fn classify_non_api_error_is_unavailable() {
        // Build a non-Api kube::Error by going through serde — `LinesCodecError`
        // isn't exposed, but `kube::Error::SerdeError` is.
        let serde_err = serde_json::from_str::<serde_json::Value>("{[}").unwrap_err();
        let err = kube::Error::SerdeError(serde_err);
        let d = classify_patch_error(&err, 0);
        assert_eq!(d, PatchRetryDecision::Unavailable);
    }

    fn dyn_obj(spec: serde_json::Value) -> DynamicObject {
        let mut data = serde_json::Map::new();
        data.insert("spec".to_string(), spec);
        DynamicObject {
            types: None,
            metadata: Default::default(),
            data: serde_json::Value::Object(data),
        }
    }

    #[test]
    fn parse_schema_version_well_formed() {
        let obj = dyn_obj(serde_json::json!({"schema_version": 42}));
        assert_eq!(parse_schema_version_from_object(&obj), 42);
    }

    #[test]
    fn parse_schema_version_missing_spec() {
        let obj = DynamicObject {
            types: None,
            metadata: Default::default(),
            data: serde_json::json!({}),
        };
        assert_eq!(parse_schema_version_from_object(&obj), 0);
    }

    #[test]
    fn parse_schema_version_missing_field() {
        let obj = dyn_obj(serde_json::json!({"content": {}}));
        assert_eq!(parse_schema_version_from_object(&obj), 0);
    }

    #[test]
    fn parse_schema_version_non_numeric() {
        let obj = dyn_obj(serde_json::json!({"schema_version": "v3"}));
        assert_eq!(parse_schema_version_from_object(&obj), 0);
    }

    // ── dry_run_response: content extraction + reused monotonicity gate ─────
    //
    // CU-86ahrg731 DryRunApply previews an Apply without any kube write. These
    // tests drive the PURE assembler `dry_run_response` directly (no kube `Api`
    // — the fn takes no Client, so it is structurally incapable of a write) and
    // assert: (a) proposed > current → Ok with the right current/proposed
    // fields, (b) proposed ≤ current → FAILED_PRECONDITION (SAME gate as
    // Apply), (c) absent current (the 404 path) → schema_version 0 + empty
    // current content, (d) idempotency — N calls → byte-identical response.

    /// Build a current Config object carrying `spec.schema_version` +
    /// `spec.content`, mirroring what `api.get` returns for an existing CRD.
    fn current_obj(schema_version: u64, content: serde_json::Value) -> DynamicObject {
        dyn_obj(serde_json::json!({
            "schema_version": schema_version,
            "content": content,
        }))
    }

    fn spec(schema_version: u32, content: serde_json::Value) -> ConfigSpec {
        ConfigSpec {
            schema_version,
            content,
        }
    }

    #[test]
    fn dry_run_higher_version_returns_diff_ok() {
        let cur = current_obj(5, serde_json::json!({"k": "old"}));
        let proposed = spec(6, serde_json::json!({"k": "new"}));

        let resp = dry_run_response(Some(&cur), &proposed).expect("proposed > current must be Ok");

        assert_eq!(resp.current_schema_version, 5);
        assert_eq!(resp.proposed_schema_version, 6);
        assert_eq!(resp.current_content_json, r#"{"k":"old"}"#);
        assert_eq!(resp.proposed_content_json, r#"{"k":"new"}"#);
    }

    #[test]
    fn dry_run_equal_version_is_failed_precondition() {
        let cur = current_obj(5, serde_json::json!({"k": "v"}));
        let proposed = spec(5, serde_json::json!({"k": "v2"}));

        let err = dry_run_response(Some(&cur), &proposed)
            .expect_err("proposed == current must be rejected");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        assert!(
            err.message().contains("must be > 5"),
            "got {:?}",
            err.message()
        );
    }

    #[test]
    fn dry_run_lower_version_is_failed_precondition() {
        let cur = current_obj(5, serde_json::json!({}));
        let proposed = spec(3, serde_json::json!({}));

        let err = dry_run_response(Some(&cur), &proposed)
            .expect_err("proposed < current must be rejected");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn dry_run_absent_current_is_zero_version_empty_content() {
        // The 404 path: `handle_dry_run_apply` passes `None` for the current.
        let proposed = spec(1, serde_json::json!({"first": true}));

        let resp =
            dry_run_response(None, &proposed).expect("first write (1 > 0) over absent must be Ok");

        assert_eq!(resp.current_schema_version, 0);
        assert_eq!(resp.current_content_json, "");
        assert_eq!(resp.proposed_schema_version, 1);
        assert_eq!(resp.proposed_content_json, r#"{"first":true}"#);
    }

    #[test]
    fn dry_run_absent_current_version_zero_proposed_still_rejected() {
        // proposed 0 over absent (current 0) is NOT an increase — rejected,
        // same as the first-write-over-zero Apply gate.
        let proposed = spec(0, serde_json::json!({}));
        let err = dry_run_response(None, &proposed).expect_err("0 over 0 must be rejected");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[test]
    fn dry_run_current_missing_content_field_yields_empty_string() {
        // Existing object with schema_version but no `spec.content`.
        let cur = dyn_obj(serde_json::json!({"schema_version": 2}));
        let proposed = spec(3, serde_json::json!({"a": 1}));

        let resp = dry_run_response(Some(&cur), &proposed).expect("ok");
        assert_eq!(resp.current_schema_version, 2);
        assert_eq!(resp.current_content_json, "");
    }

    #[test]
    fn dry_run_is_idempotent_byte_identical() {
        // Idempotency invariant: N calls on the same inputs → identical bytes.
        let cur = current_obj(4, serde_json::json!({"nested": {"x": [1, 2, 3]}}));
        let proposed = spec(7, serde_json::json!({"nested": {"x": [4, 5]}}));

        let a = dry_run_response(Some(&cur), &proposed).expect("ok");
        let b = dry_run_response(Some(&cur), &proposed).expect("ok");

        assert_eq!(a.current_content_json, b.current_content_json);
        assert_eq!(a.proposed_content_json, b.proposed_content_json);
        assert_eq!(a.current_schema_version, b.current_schema_version);
        assert_eq!(a.proposed_schema_version, b.proposed_schema_version);
    }

    #[test]
    fn extract_current_content_json_serializes_content() {
        let obj = current_obj(1, serde_json::json!({"k": "v"}));
        assert_eq!(extract_current_content_json(&obj).unwrap(), r#"{"k":"v"}"#);
    }

    #[test]
    fn extract_current_content_json_absent_content_is_empty() {
        let obj = dyn_obj(serde_json::json!({"schema_version": 1}));
        assert_eq!(extract_current_content_json(&obj).unwrap(), "");
    }

    #[test]
    fn parse_schema_version_exceeds_u32_truncates() {
        let obj = dyn_obj(serde_json::json!({"schema_version": (u32::MAX as u64) + 5}));
        // `as u32` wraps; documents the current behaviour so future changes
        // are explicit rather than accidental.
        let got = parse_schema_version_from_object(&obj);
        assert_eq!(got, 4);
    }

    // ── run_patch_retry_loop: max-retry loop-exit boundary (CU-86aj14yvh) ──
    //
    // PR D unit-tested the per-attempt classifier (`classify_patch_error`)
    // but left the loop body uncovered. This test drives the actual loop
    // with a 409-returning stub closure to lock in the loop-exit
    // contract:
    //   1. The closure is invoked EXACTLY `RETRY_DELAYS_MS.len() + 1` times
    //      (initial attempt + N retries — no off-by-one in either direction).
    //   2. The final error is `Status::aborted` (NOT `unavailable`, which
    //      would silently flip retry-eligible behaviour for callers).
    //   3. Total elapsed time is within the ±25 % jitter band around
    //      `sum(RETRY_DELAYS_MS)` — proves the loop actually slept the
    //      configured delays between attempts.
    //
    // Hand-mutation verification (per PR plan): shortening `RETRY_DELAYS_MS`
    // by one element makes this test fail on the call-count assertion,
    // which proves the assertion is load-bearing.
    //
    // Uses `tokio::time::pause()` so the sleeps are advanced instantly by
    // the runtime — no real wall-clock waits, deterministic elapsed
    // measurement via `tokio::time::Instant`.
    #[tokio::test(start_paused = true)]
    async fn run_patch_retry_loop_409_exhausts_with_aborted() {
        use std::cell::Cell;

        // Lock the constant by value here (not just by length) so mutating
        // either entry of RETRY_DELAYS_MS — its length OR an element — makes
        // this test fail loudly. Without this guard the call-count and
        // elapsed-band assertions below silently re-derive their expectations
        // from the mutated constant and would still pass. Compared as a
        // slice so a length-shortening mutation does NOT compile-error out;
        // it falls through to the call-count assertion below (which prints
        // the off-by-one diff a reviewer expects to see).
        assert_eq!(
            RETRY_DELAYS_MS.as_slice(),
            [100u64, 200u64].as_slice(),
            "test pins RETRY_DELAYS_MS to [100, 200]; update the hardcoded \
             EXPECTED_CALLS and SUM_MS below if you intentionally change it",
        );
        const EXPECTED_CALLS: usize = 3; // initial + 2 retries
        const SUM_MS: u64 = 300; // 100 + 200

        let calls = Cell::new(0usize);
        let started = tokio::time::Instant::now();

        let result = run_patch_retry_loop(|| {
            calls.set(calls.get() + 1);
            async { Err::<String, _>(api_err(409)) }
        })
        .await;

        let elapsed = started.elapsed();
        let err = result.expect_err("loop must exit with Status after exhausting retries");

        // (1) exact call count — initial attempt + N retries.
        assert_eq!(
            calls.get(),
            EXPECTED_CALLS,
            "expected {EXPECTED_CALLS} attempts (initial + {} retries), got {}",
            EXPECTED_CALLS - 1,
            calls.get(),
        );

        // (2) final result is Status::aborted (NOT unavailable / NOT internal).
        assert_eq!(
            err.code(),
            tonic::Code::Aborted,
            "loop-exit on consecutive 409s must surface Aborted, got {:?}: {}",
            err.code(),
            err.message(),
        );

        // (3) total elapsed within ±25 % jitter band of sum(RETRY_DELAYS_MS).
        //     `jittered_retry_ms` picks an offset in [base*0.75, base*1.25],
        //     so the sum lies in [sum*0.75, sum*1.25]. Use the conservative
        //     [sum*0.74, sum*1.26] band to absorb integer-division rounding
        //     in `base_ms / 4`.
        let lower = Duration::from_millis((SUM_MS * 74) / 100);
        let upper = Duration::from_millis((SUM_MS * 126) / 100);
        assert!(
            elapsed >= lower && elapsed <= upper,
            "elapsed {:?} outside jitter band [{:?}, {:?}] for sum {} ms",
            elapsed,
            lower,
            upper,
            SUM_MS,
        );
    }

    // ── BatchApply pure gates: parse+dedup, monotonicity, sort (CU-86ahrwd5c) ─
    //
    // BatchApply is an atomic-GATE multi-config apply: every item is validated
    // BEFORE any write, so a single gate failure rejects the whole batch with
    // zero writes. These tests drive the PURE helpers directly (no kube) and
    // lock in the all-or-none + ordering contract:
    //   - parse_batch_items: empty/parse-error/duplicate rejection + input order
    //   - gate_batch_versions: one stale among many → whole batch rejected
    //   - sort_batch_items: deterministic (namespace, name) ordering
    // (apply_spec re-checks monotonicity per item as defense-in-depth against a
    // TOCTOU concurrent writer — exercised on the live kube path, not here.)
    //
    // Acceptance: the ticket's "one stale schema_version → entire batch
    // rejected" is `gate_batch_versions_one_stale_among_many_rejects_all`.
    // The live "BatchApply N configs, all subscribers see consecutive
    // resource_versions" check is a kind e2e gate (CI / linux) — there is no
    // Rust integration harness in this repo, so it is not runnable here.

    fn apply_req(namespace: &str, name: &str, yaml_content: &str) -> ApplyRequest {
        ApplyRequest {
            namespace: namespace.to_string(),
            name: name.to_string(),
            yaml_content: yaml_content.to_string(),
        }
    }

    /// Minimal valid `ConfigSpec` YAML carrying the given schema_version.
    fn item_yaml(schema_version: u32) -> String {
        format!("schema_version: {schema_version}\ncontent:\n  k: v\n")
    }

    #[test]
    fn parse_batch_items_empty_is_invalid_argument() {
        let err = parse_batch_items(&[]).expect_err("empty batch must be rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("at least one item"),
            "got {:?}",
            err.message()
        );
    }

    #[test]
    fn parse_batch_items_valid_multi_preserves_input_order() {
        let reqs = [
            apply_req("trading", "limits", &item_yaml(3)),
            apply_req("risk", "caps", &item_yaml(7)),
        ];
        let items = parse_batch_items(&reqs).expect("valid batch parses");
        assert_eq!(items.len(), 2);
        // Input order preserved (the caller sorts later).
        assert_eq!(
            (items[0].namespace.as_str(), items[0].name.as_str()),
            ("trading", "limits")
        );
        assert_eq!(items[0].spec.schema_version, 3);
        assert_eq!(
            (items[1].namespace.as_str(), items[1].name.as_str()),
            ("risk", "caps")
        );
        assert_eq!(items[1].spec.schema_version, 7);
    }

    #[test]
    fn parse_batch_items_bad_yaml_is_invalid_argument_naming_offender() {
        let reqs = [
            apply_req("trading", "limits", &item_yaml(1)),
            apply_req("risk", "broken", "not: [valid: yaml: here"),
        ];
        let err = parse_batch_items(&reqs).expect_err("bad YAML must be rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("risk/broken"),
            "message must name the offending item; got {:?}",
            err.message()
        );
    }

    #[test]
    fn parse_batch_items_duplicate_target_is_invalid_argument() {
        let reqs = [
            apply_req("trading", "limits", &item_yaml(1)),
            apply_req("trading", "limits", &item_yaml(2)),
        ];
        let err = parse_batch_items(&reqs).expect_err("duplicate target must be rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("duplicate") && err.message().contains("trading/limits"),
            "message must flag the duplicate target; got {:?}",
            err.message()
        );
    }

    /// Build a `BatchItem` directly for the version-gate / sort tests.
    fn batch_item(namespace: &str, name: &str, schema_version: u32) -> BatchItem {
        BatchItem {
            namespace: namespace.to_string(),
            name: name.to_string(),
            spec: ConfigSpec {
                schema_version,
                content: serde_json::json!({}),
            },
        }
    }

    #[test]
    fn gate_batch_versions_all_increasing_is_ok() {
        let items = [
            batch_item("a", "x", 2),
            batch_item("b", "y", 5),
            batch_item("c", "z", 1),
        ];
        // currents positionally aligned: every incoming strictly exceeds.
        let currents = [1u32, 4, 0];
        assert!(
            gate_batch_versions(&items, &currents).is_ok(),
            "all incoming > current must pass the batch gate"
        );
    }

    #[test]
    fn gate_batch_versions_one_stale_among_many_rejects_all() {
        // Ticket acceptance: ONE stale schema_version → entire batch rejected.
        let items = [
            batch_item("a", "1", 5),
            batch_item("a", "2", 6),
            batch_item("a", "3", 4), // stale: 4 ≤ current 4
            batch_item("a", "4", 9),
            batch_item("a", "5", 10),
        ];
        let currents = [1u32, 2, 4, 8, 0];
        let err = gate_batch_versions(&items, &currents)
            .expect_err("one stale item must reject the whole batch");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        // The error names the failing version pair (current 4, got 4) so the
        // reject is attributable to the offending item.
        assert!(
            err.message().contains("must be > 4") && err.message().contains("got 4"),
            "message must name the failing version; got {:?}",
            err.message()
        );
    }

    #[test]
    fn gate_batch_versions_first_write_over_zero_is_accepted() {
        // current 0 (CRD does not exist yet) — the first write (any version ≥ 1)
        // is accepted, same as the single-item Apply gate.
        let items = [batch_item("a", "x", 1), batch_item("b", "y", 1)];
        let currents = [0u32, 0];
        assert!(gate_batch_versions(&items, &currents).is_ok());
    }

    #[test]
    fn sort_batch_items_orders_by_namespace_then_name() {
        // Deliberately scrambled input across namespaces + names.
        let mut items = vec![
            batch_item("trading", "limits", 1),
            batch_item("risk", "caps", 1),
            batch_item("trading", "alerts", 1),
            batch_item("audit", "rules", 1),
            batch_item("risk", "alerts", 1),
        ];
        sort_batch_items(&mut items);
        let order: Vec<(&str, &str)> = items
            .iter()
            .map(|i| (i.namespace.as_str(), i.name.as_str()))
            .collect();
        assert_eq!(
            order,
            vec![
                ("audit", "rules"),
                ("risk", "alerts"),
                ("risk", "caps"),
                ("trading", "alerts"),
                ("trading", "limits"),
            ],
            "items must be reordered to (namespace, name) ascending"
        );
    }

    #[test]
    fn parse_then_sort_reorders_regardless_of_input() {
        // End-to-end of the pure pipeline: parse (input order) → sort. A reverse
        // sorted input ends up forward-sorted.
        let reqs = [
            apply_req("zeta", "b", &item_yaml(1)),
            apply_req("zeta", "a", &item_yaml(1)),
            apply_req("alpha", "z", &item_yaml(1)),
        ];
        let mut items = parse_batch_items(&reqs).expect("parses");
        // parse preserves input order:
        assert_eq!(items[0].namespace, "zeta");
        sort_batch_items(&mut items);
        let order: Vec<(&str, &str)> = items
            .iter()
            .map(|i| (i.namespace.as_str(), i.name.as_str()))
            .collect();
        assert_eq!(order, vec![("alpha", "z"), ("zeta", "a"), ("zeta", "b")],);
    }
}
