//! `Revert` handler — rolls a Config CRD back to a historical `resourceVersion`.
//!
//! Flow:
//! 1. List the historical Config at the given `resourceVersion` using
//!    `resourceVersionMatch=Exact` with a `metadata.name=` field selector.
//! 2. Extract `spec.content` and `spec.schema_version` from the historical
//!    object.
//! 3. Read the CURRENT stored schema_version (NOT just the historical one).
//! 4. Compute the new schema_version as `max(current, historical) + 1` so the
//!    in-memory cache and any future writers always observe a strictly higher
//!    version than what was previously seen — preserving monotonicity.
//! 5. Apply the historical content via the standard Apply code path
//!    (`apply::apply_spec`), which patches the CRD with the new version.  The
//!    in-process watcher loop picks up the patch and broadcasts the event
//!    automatically.
//!
//! Failure modes:
//! - `to_resource_version` empty               → `INVALID_ARGUMENT`
//! - etcd compaction past the requested RV     → `FAILED_PRECONDITION`
//! - historical revision not found             → `NOT_FOUND`
//! - kube transport / list error               → `UNAVAILABLE`

use kube::Client;
use kube::api::{Api, ListParams, VersionMatch};
use kube::core::DynamicObject;
use tonic::{Response, Status};
use tracing::{debug, info, warn};

use crate::grpc::apply::{apply_spec, fetch_current_schema_version};
use crate::proto::{RevertRequest, RevertResponse};
use crate::types::ConfigSpec;
use crate::watcher::config_api_resource;

pub async fn handle_revert(
    kube_client: Client,
    req: RevertRequest,
) -> Result<Response<RevertResponse>, Status> {
    debug!(
        namespace = %req.namespace,
        name = %req.name,
        to_rv = %req.to_resource_version,
        "Revert RPC"
    );

    if req.to_resource_version.is_empty() {
        return Err(Status::invalid_argument(
            "to_resource_version must not be empty",
        ));
    }

    let ar = config_api_resource();
    let api: Api<DynamicObject> = Api::namespaced_with(kube_client.clone(), &req.namespace, &ar);

    // Fetch the historical revision via a List with resourceVersionMatch=Exact.
    // kube::Api::get does not support a `resourceVersion` query parameter; the
    // standard K8s API contract requires LIST for point-in-time reads.
    let historical = fetch_historical_spec(&api, &req.name, &req.to_resource_version).await?;

    // Look up the current schema_version so we can preserve monotonicity even
    // if the user has applied newer versions since the target revision.
    let current = fetch_current_schema_version(&api, &req.name).await?;
    let historical_version = historical.schema_version;
    let new_version = current.max(historical_version).saturating_add(1);

    info!(
        namespace = %req.namespace,
        name = %req.name,
        to_rv = %req.to_resource_version,
        historical_version,
        current,
        new_version,
        "Revert: replaying historical content"
    );

    let new_spec = ConfigSpec {
        schema_version: new_version,
        content: historical.content,
    };

    let apply_resp = apply_spec(&req.namespace, &req.name, new_spec, kube_client).await?;
    let resource_version = apply_resp.into_inner().resource_version;

    Ok(Response::new(RevertResponse {
        resource_version,
        schema_version: new_version,
    }))
}

/// Fetch the `spec` of a Config CRD as it existed at `resource_version`.
///
/// Uses a LIST with `resourceVersionMatch=Exact` and a `metadata.name` field
/// selector — the only K8s-supported way to read a specific historical
/// resourceVersion (Api::get does not accept a `resourceVersion` query param).
async fn fetch_historical_spec(
    api: &Api<DynamicObject>,
    name: &str,
    resource_version: &str,
) -> Result<ConfigSpec, Status> {
    let lp = ListParams::default()
        .fields(&format!("metadata.name={name}"))
        .at(resource_version)
        .matching(VersionMatch::Exact);

    match api.list(&lp).await {
        Ok(list) => {
            let obj = list
                .items
                .into_iter()
                .find(|o| o.metadata.name.as_deref() == Some(name))
                .ok_or_else(|| {
                    Status::not_found(format!(
                        "Config {name} not found at resource_version={resource_version}"
                    ))
                })?;
            parse_historical_obj(&obj)
        }
        Err(e) => Err(map_list_error(&e, name, resource_version)),
    }
}

/// Extract `spec` from a historical `DynamicObject` and deserialize into
/// `ConfigSpec`. Pure — directly unit-testable.
pub(crate) fn parse_historical_obj(obj: &DynamicObject) -> Result<ConfigSpec, Status> {
    let spec_value = obj
        .data
        .get("spec")
        .cloned()
        .ok_or_else(|| Status::data_loss("historical object missing spec"))?;
    serde_json::from_value::<ConfigSpec>(spec_value)
        .map_err(|e| Status::data_loss(format!("invalid historical spec: {e}")))
}

/// Pure classifier for a kube `list()` failure on the historical-RV fetch.
/// Emits the operator-facing `warn!` for 410 (etcd compaction) inline.
pub(crate) fn map_list_error(err: &kube::Error, name: &str, resource_version: &str) -> Status {
    match err {
        kube::Error::Api(ae) if ae.code == 410 => {
            warn!(
                resource_version,
                "Revert: requested RV has been compacted by etcd",
            );
            Status::failed_precondition(format!(
                "resource_version {resource_version} is no longer available (etcd compaction); \
                 K8s only retains history within the compaction window"
            ))
        }
        kube::Error::Api(ae) if ae.code == 404 => Status::not_found(format!(
            "Config {name} not found at resource_version={resource_version}"
        )),
        _ => Status::unavailable(format!("kube list error: {err}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn new_version_uses_max_of_current_and_historical_plus_one() {
        // Reverting to v1 while current is v5 → 6, not 2.
        let historical_version: u32 = 1;
        let current: u32 = 5;
        let new_version = current.max(historical_version).saturating_add(1);
        assert_eq!(new_version, 6);
    }

    #[test]
    fn new_version_when_historical_is_higher_than_current() {
        // Edge case: current was deleted/reset.  Historical=10, current=0 → 11.
        let historical_version: u32 = 10;
        let current: u32 = 0;
        let new_version = current.max(historical_version).saturating_add(1);
        assert_eq!(new_version, 11);
    }

    #[test]
    fn historical_spec_round_trips() {
        // Sanity: ConfigSpec deserializes the kind of value we expect to read
        // out of obj.data["spec"] on the historical CRD.
        let raw = json!({
            "schema_version": 3,
            "content": { "k": "v" },
        });
        let spec: ConfigSpec = serde_json::from_value(raw).expect("parse");
        assert_eq!(spec.schema_version, 3);
        assert_eq!(spec.content["k"], "v");
    }

    fn api_err(code: u16) -> kube::Error {
        kube::Error::Api(kube::core::ErrorResponse {
            status: "Failure".to_string(),
            message: "synthetic".to_string(),
            reason: "synthetic".to_string(),
            code,
        })
    }

    /// 410 Gone (etcd compaction) → FAILED_PRECONDITION with the
    /// compaction-window explanation in the message.
    #[test]
    fn map_list_error_410_is_failed_precondition() {
        let s = map_list_error(&api_err(410), "cfg", "12345");
        assert_eq!(s.code(), tonic::Code::FailedPrecondition);
        assert!(
            s.message().contains("compaction"),
            "expected message to mention etcd compaction; got {:?}",
            s.message(),
        );
    }

    /// 404 → NOT_FOUND naming the missing RV.
    #[test]
    fn map_list_error_404_is_not_found() {
        let s = map_list_error(&api_err(404), "cfg", "999");
        assert_eq!(s.code(), tonic::Code::NotFound);
        assert!(s.message().contains("cfg"));
        assert!(s.message().contains("999"));
    }

    /// Everything else → UNAVAILABLE.
    #[test]
    fn map_list_error_other_is_unavailable() {
        for code in [400u16, 403, 500, 503] {
            assert_eq!(
                map_list_error(&api_err(code), "cfg", "1").code(),
                tonic::Code::Unavailable,
                "code {code} should be Unavailable",
            );
        }
    }

    fn make_obj(spec: Option<serde_json::Value>) -> DynamicObject {
        let mut obj = DynamicObject::new("cfg", &crate::watcher::config_api_resource());
        obj.metadata.name = Some("cfg".to_string());
        obj.data = match spec {
            Some(s) => json!({ "spec": s }),
            None => json!({}),
        };
        obj
    }

    #[test]
    fn parse_historical_obj_ok_for_valid_spec() {
        let obj = make_obj(Some(json!({"schema_version": 7, "content": {"k": "v"}})));
        let spec = parse_historical_obj(&obj).expect("must parse");
        assert_eq!(spec.schema_version, 7);
        assert_eq!(spec.content["k"], "v");
    }

    #[test]
    fn parse_historical_obj_data_loss_when_spec_missing() {
        let obj = make_obj(None);
        let s = parse_historical_obj(&obj).unwrap_err();
        assert_eq!(s.code(), tonic::Code::DataLoss);
        assert!(s.message().contains("missing spec"));
    }

    #[test]
    fn parse_historical_obj_data_loss_when_spec_malformed() {
        // schema_version typed as string — fails the u32 deserialise.
        let obj = make_obj(Some(json!({"schema_version": "wrong-type", "content": {}})));
        let s = parse_historical_obj(&obj).unwrap_err();
        assert_eq!(s.code(), tonic::Code::DataLoss);
        assert!(s.message().contains("invalid historical spec"));
    }
}
