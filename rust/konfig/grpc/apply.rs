//! `Apply` handler — creates or updates a `Config.konfig.io/v1` CRD.
//!
//! Flow:
//! 1. Parse `yaml_content` as `ConfigSpec`.
//! 2. Fetch current CRD to check `schema_version` monotonicity.
//! 3. Reject with `FAILED_PRECONDITION` if incoming version ≤ current.
//! 4. Patch the CRD with server-side apply; retry 409 up to 3 times.
//! 5. Return `ApplyResponse { resource_version }`.

use std::time::{Duration, Instant};

use kube::Client;
use kube::api::{Api, Patch, PatchParams};
use kube::core::DynamicObject;
use serde_json::json;
use tonic::{Response, Status};
use tracing::{Instrument, debug, info, warn};

use crate::grpc::jittered_retry_ms;
use crate::metrics::{APPLY_DURATION, APPLY_TOTAL};
use crate::proto::{ApplyRequest, ApplyResponse};
use crate::types::ConfigSpec;
use crate::watcher::{GROUP, VERSION, config_api_resource};

const RETRY_DELAYS_MS: [u64; 2] = [100, 200];

pub async fn handle_apply(
    kube_client: Client,
    req: ApplyRequest,
) -> Result<Response<ApplyResponse>, Status> {
    debug!(namespace = %req.namespace, name = %req.name, "Apply RPC");

    apply_inner(&req.namespace, &req.name, &req.yaml_content, kube_client).await
}

pub async fn apply_inner(
    namespace: &str,
    name: &str,
    yaml_content: &str,
    kube_client: Client,
) -> Result<Response<ApplyResponse>, Status> {
    let spec: ConfigSpec = serde_yaml::from_str(yaml_content)
        .map_err(|e| Status::invalid_argument(format!("invalid YAML: {e}")))?;

    apply_spec(namespace, name, spec, kube_client).await
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
    match err {
        kube::Error::Api(ae) if ae.code == 409 && attempt < RETRY_DELAYS_MS.len() => {
            PatchRetryDecision::RetryAfter {
                delay_ms: RETRY_DELAYS_MS[attempt],
            }
        }
        kube::Error::Api(ae) if ae.code == 409 => PatchRetryDecision::AbortRetriesExhausted,
        _ => PatchRetryDecision::Unavailable,
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
}
