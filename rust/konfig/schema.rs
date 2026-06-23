//! Per-`(namespace, configName)` JSON Schema validation table + its kube
//! watcher (Phase 8, CU-86ahrwd5g).
//!
//! Lets an operator register a draft-07 JSON Schema for a Config via a new
//! `ConfigSchema.konfig.io/v1` CRD. On the `Apply` RPC path, the incoming
//! `content` is validated against the registered schema before the CRD is
//! patched. No schema registered for a `(namespace, configName)` ⇒ anything is
//! accepted (unchanged behaviour).
//!
//! Mirrors [`crate::acl`] (the closest template) and
//! [`crate::cache::ConfigCache`] (`ArcSwap` lock-free reads), specialised for
//! schema validation:
//!   - `ConfigSchema` is **namespaced** (unlike `ConfigACL`, which is
//!     cluster-scoped). We still watch ALL namespaces with `Api::all_with` and
//!     key the table by the object's `metadata.namespace` + `spec.configName`.
//!   - The published value is a [`CompiledSchema`] — the `spec.schema` is
//!     compiled once at CRD-load time (`jsonschema::draft7::new`); per-Apply
//!     validation reuses the compiled validator, never re-parsing the schema.
//!   - A bad schema (compile failure) is logged at `warn!` and the entry is
//!     skipped — a malformed schema must never crash the watcher.
//!
//! Reads ([`SchemaTable::validate`]) are fully lock-free: the Apply handler
//! does one atomic pointer load. The single watcher writer clones-and-swaps the
//! whole map on each event; the schema set is small (one entry per governed
//! Config), so a full rebuild per event is cheaper than incremental diffing.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arc_swap::ArcSwap;
use futures_util::{StreamExt, TryStreamExt};
use kube::api::ApiResource;
use kube::core::DynamicObject;
use kube::runtime::watcher::{self as kube_watcher, Event, watcher as kube_watch_stream};
use kube::{Api, Client};
use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::watcher::{BACKOFF_STEPS_SECS, backoff_delay};

// ── Constants ─────────────────────────────────────────────────────────────────

/// `ConfigSchema` CRD coordinates. Same `group`/`version` as Config; distinct
/// `kind`/`plural`. Namespaced (like Config, unlike the cluster-scoped
/// `ConfigACL`).
pub const GROUP: &str = "konfig.io";
pub const VERSION: &str = "v1";
pub const KIND: &str = "ConfigSchema";
pub const PLURAL: &str = "configschemas";

// Re-export the shared backoff schedule reference so this module's docs and any
// future tuning stay anchored to the Config watcher's schedule.
const _: &[u64] = BACKOFF_STEPS_SECS;

/// [`ApiResource`] descriptor for the `ConfigSchema` CRD.
pub fn schema_api_resource() -> ApiResource {
    ApiResource {
        group: GROUP.to_string(),
        version: VERSION.to_string(),
        api_version: format!("{GROUP}/{VERSION}"),
        kind: KIND.to_string(),
        plural: PLURAL.to_string(),
    }
}

// ── Spec ──────────────────────────────────────────────────────────────────────

/// Wire shape of `spec` on a `ConfigSchema` object. Deserialized from the
/// `DynamicObject`'s `spec` field. `configName` selects which Config (by
/// `.name`, in the SAME namespace as this ConfigSchema) the schema governs;
/// `schema` is an opaque draft-07 JSON Schema object.
#[derive(Debug, Deserialize)]
struct SchemaSpec {
    #[serde(rename = "configName")]
    config_name: String,
    /// The raw JSON Schema document; compiled with `jsonschema::draft7::new`
    /// at load time. `serde_json::Value` so any valid JSON Schema shape parses.
    schema: serde_json::Value,
}

// ── CompiledSchema ──────────────────────────────────────────────────────────

/// A draft-07 schema compiled once at CRD-load time. Wraps the `jsonschema`
/// [`Validator`](jsonschema::Validator); per-Apply validation reuses it without
/// re-parsing.
pub struct CompiledSchema {
    validator: jsonschema::Validator,
}

impl CompiledSchema {
    /// Compile a `serde_json::Value` as a draft-07 schema. `Err` carries the
    /// compile error message (used only for the `warn!` log line — a bad schema
    /// is skipped, never surfaced to a caller).
    fn compile(schema: &serde_json::Value) -> Result<Self, String> {
        jsonschema::draft7::new(schema)
            .map(|validator| Self { validator })
            .map_err(|e| e.to_string())
    }

    /// Validate `content` against this schema. `Ok(())` when valid; otherwise
    /// `Err` carrying every validation error, each rendered as a human-readable
    /// string (one per failing constraint).
    fn validate(&self, content: &serde_json::Value) -> Result<(), Vec<String>> {
        let errors: Vec<String> = self
            .validator
            .iter_errors(content)
            .map(|e| e.to_string())
            .collect();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

// ── SchemaTable ─────────────────────────────────────────────────────────────

/// `(namespace, configName)` → compiled schema, the whole registry snapshot.
type Inner = HashMap<(String, String), Arc<CompiledSchema>>;

/// Lock-free JSON-Schema registry keyed by `(namespace, configName)`.
///
/// Reads (the per-Apply validate call) pay only an atomic pointer load. The
/// single watcher task rebuilds the full map on each apply/delete and swaps it
/// in — the schema set is small (one entry per governed Config), so a full
/// rebuild per event is cheaper than tracking incremental diffs.
pub struct SchemaTable {
    inner: ArcSwap<Inner>,
}

impl Default for SchemaTable {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaTable {
    /// Create an empty table.
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::from_pointee(Inner::new()),
        }
    }

    /// Validate `content` against the schema registered for
    /// `(namespace, config_name)`.
    ///
    /// Returns `Ok(())` when:
    ///   - no schema is registered for that key (accept anything — unchanged
    ///     behaviour), or
    ///   - a schema is registered AND `content` satisfies it.
    ///
    /// Returns `Err(messages)` with one readable message per failing constraint
    /// when a schema is registered and `content` violates it. Zero locking — an
    /// atomic pointer load only.
    pub fn validate(
        &self,
        namespace: &str,
        config_name: &str,
        content: &serde_json::Value,
    ) -> Result<(), Vec<String>> {
        let guard = self.inner.load();
        // `HashMap<(String, String), _>` has no borrowed `(&str, &str)` lookup
        // (no `Borrow<(&str, &str)>` impl for `(String, String)`), so the key
        // is built owned. The two short `String` allocations are negligible
        // next to the schema validation itself, and only happen per Apply (not
        // a read hot path).
        match guard.get(&(namespace.to_string(), config_name.to_string())) {
            Some(schema) => schema.validate(content),
            None => Ok(()),
        }
    }

    /// Number of registered schemas (diagnostics / tests).
    pub fn schema_count(&self) -> usize {
        self.inner.load().len()
    }

    /// Atomically replace the whole map. Used by the watcher after rebuilding
    /// the snapshot from the live object set.
    fn store(&self, next: Inner) {
        self.inner.store(Arc::new(next));
    }

    /// Register a single compiled draft-07 schema under `(namespace,
    /// config_name)`. Test-only seam so callers in other modules (e.g. the
    /// `apply` handler tests) can build a populated table without a kube
    /// stream. Returns `Err` with the compile error if `schema` is not a valid
    /// draft-07 schema.
    #[cfg(test)]
    pub fn insert_for_test(
        &self,
        namespace: &str,
        config_name: &str,
        schema: &serde_json::Value,
    ) -> Result<(), String> {
        let compiled = CompiledSchema::compile(schema)?;
        let mut next = (**self.inner.load()).clone();
        next.insert(
            (namespace.to_string(), config_name.to_string()),
            Arc::new(compiled),
        );
        self.store(next);
        Ok(())
    }
}

/// Initial-sync flag shared between the ConfigSchema watcher and any reader
/// that wants to know the registry has finished its first relist.
///
/// Starts `false`; the watcher flips it `true` on the first `Event::InitDone`.
/// Validation does NOT gate on this flag (a not-yet-synced registry simply
/// behaves as "no schema registered" → accept), but it is exposed for parity
/// with [`crate::acl::AclSynced`] and for future readiness wiring.
#[derive(Debug, Default)]
pub struct SchemaSynced(AtomicBool);

impl SchemaSynced {
    pub fn new() -> Self {
        Self(AtomicBool::new(false))
    }

    /// Has the initial list completed at least once?
    pub fn is_synced(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    /// Mark synced (idempotent). Release-ordered so a reader that observes
    /// `true` also sees the table writes that happened-before the InitDone.
    fn mark_synced(&self) {
        self.0.store(true, Ordering::Release);
    }
}

// ── Watcher ────────────────────────────────────────────────────────────────────

/// Watcher for `ConfigSchema.konfig.io/v1` across all namespaces.
pub struct SchemaWatcher {
    client: Client,
}

impl SchemaWatcher {
    pub fn new(client: Client) -> Self {
        SchemaWatcher { client }
    }

    /// Run the watcher with exponential-backoff reconnect.
    ///
    /// On stream error: waits `backoff_delay(attempt)` then retries. The table
    /// and sync flag are retained across a reconnect (last-known-good policy),
    /// mirroring the Config/ACL watchers' CP semantics. On a clean stream end it
    /// returns `Ok(())` and the caller's `run_with_reconnect` restarts it.
    pub async fn run(
        self,
        table: Arc<SchemaTable>,
        synced: Arc<SchemaSynced>,
    ) -> Result<(), WatcherError> {
        let ar = schema_api_resource();
        let mut attempt: usize = 0;

        loop {
            // `ConfigSchema` is namespaced, but we watch EVERY namespace so a
            // schema in any tenant namespace is enforced — `Api::all_with`
            // (cluster-wide list/watch of a namespaced kind), keyed below by the
            // object's own `metadata.namespace`.
            let api: Api<DynamicObject> = Api::all_with(self.client.clone(), &ar);
            let wc = kube_watcher::Config::default();
            let stream = kube_watch_stream(api, wc).boxed();

            info!(attempt, "ConfigSchema watcher started");

            match pump_schema_events(stream, &table, &synced).await {
                PumpOutcome::StreamEnded => {
                    info!("ConfigSchema watcher stream ended cleanly");
                    return Ok(());
                }
                PumpOutcome::StreamErrored(e) => {
                    warn!(
                        attempt,
                        "ConfigSchema watcher error: {e} — retaining last-known-good"
                    );
                    tokio::time::sleep(backoff_delay(attempt)).await;
                    attempt = attempt.saturating_add(1);
                }
            }
        }
    }
}

/// Outcome of pumping a single ConfigSchema watcher connection.
#[derive(Debug)]
pub(crate) enum PumpOutcome {
    StreamEnded,
    StreamErrored(kube_watcher::Error),
}

/// One parsed ConfigSchema object: the `(namespace, configName)` it governs +
/// its compiled schema.
struct SchemaEntry {
    key: (String, String),
    schema: Arc<CompiledSchema>,
}

/// Drive a ConfigSchema watcher stream to completion or first error.
///
/// Mirrors [`crate::acl::pump_acl_events`]. Every mutating event rebuilds the
/// full published table from a running mirror of the live objects, keyed by the
/// object's `metadata.name` (the CRD object name — distinct from the
/// `(namespace, configName)` it governs).
///
/// Extracted from [`SchemaWatcher::run`] so the per-event behaviour is unit-
/// testable against a synthetic stream — no kube API connection required.
pub(crate) async fn pump_schema_events<S>(
    mut stream: S,
    table: &Arc<SchemaTable>,
    synced: &Arc<SchemaSynced>,
) -> PumpOutcome
where
    S: futures_util::Stream<Item = Result<Event<DynamicObject>, kube_watcher::Error>> + Unpin,
{
    // Mirror of the live ConfigSchema objects, keyed by CRD object name (which
    // is unique per object, so an object renamed in `configName` does not leak
    // a stale entry). Rebuilt into the published table on each mutation.
    let mut objects: HashMap<String, SchemaEntry> = HashMap::new();

    loop {
        match stream.try_next().await {
            Ok(Some(event)) => {
                if handle_schema_event(event, &mut objects, synced) {
                    publish(&objects, table);
                }
            }
            Ok(None) => return PumpOutcome::StreamEnded,
            Err(e) => return PumpOutcome::StreamErrored(e),
        }
    }
}

/// Apply one event to the in-memory object mirror. Returns `true` when the
/// published table should be rebuilt (an Apply/Delete that changed the set);
/// `false` for lifecycle markers (`Init` / `InitDone`).
fn handle_schema_event(
    event: Event<DynamicObject>,
    objects: &mut HashMap<String, SchemaEntry>,
    synced: &Arc<SchemaSynced>,
) -> bool {
    match event {
        Event::Apply(obj) | Event::InitApply(obj) => {
            let name = obj.metadata.name.clone().unwrap_or_default();
            match parse_schema_object(&obj) {
                Some(entry) => {
                    info!(
                        object = %name,
                        namespace = %entry.key.0,
                        config_name = %entry.key.1,
                        "ConfigSchema applied",
                    );
                    objects.insert(name, entry);
                }
                None => {
                    // Unparseable spec OR un-compilable schema: drop any prior
                    // entry for this object so a malformed edit cannot leave a
                    // stale schema enforced.
                    warn!(object = %name, "ConfigSchema could not be parsed/compiled — entry removed");
                    objects.remove(&name);
                }
            }
            true
        }
        Event::Delete(obj) => {
            let name = obj.metadata.name.clone().unwrap_or_default();
            warn!(object = %name, "ConfigSchema deleted — schema removed");
            objects.remove(&name);
            true
        }
        Event::Init => {
            debug!("ConfigSchema watch stream: relist starting");
            false
        }
        Event::InitDone => {
            synced.mark_synced();
            debug!("ConfigSchema watch stream: initial list complete — schema registry synced");
            false
        }
    }
}

/// Rebuild and atomically publish the `(namespace, configName) → schema` table
/// from the object mirror.
///
/// If two ConfigSchema objects govern the same `(namespace, configName)` (a
/// misconfiguration), last-writer-by-iteration-order wins. The schema set is
/// small enough that we accept this rather than tracking conflicts; operators
/// are expected to register one ConfigSchema per Config.
fn publish(objects: &HashMap<String, SchemaEntry>, table: &Arc<SchemaTable>) {
    let mut next: Inner = HashMap::with_capacity(objects.len());
    for entry in objects.values() {
        next.insert(entry.key.clone(), Arc::clone(&entry.schema));
    }
    table.store(next);
}

/// Parse a `DynamicObject` (ConfigSchema CRD) into a [`SchemaEntry`].
///
/// Expects `obj.data["spec"]` to deserialize as [`SchemaSpec`] and the object to
/// carry a `metadata.namespace`. Returns `None` when the spec is missing /
/// unparseable, the namespace or `configName` is empty, or the `schema` fails to
/// compile as draft-07 (logged at `warn!`).
fn parse_schema_object(obj: &DynamicObject) -> Option<SchemaEntry> {
    let name = obj.metadata.name.clone().unwrap_or_default();

    let namespace = obj.metadata.namespace.clone().unwrap_or_default();
    if namespace.trim().is_empty() {
        warn!(object = %name, "ConfigSchema has no namespace — ignored");
        return None;
    }

    let spec_value = obj.data.get("spec")?;
    let spec: SchemaSpec = serde_json::from_value(spec_value.clone())
        .map_err(|e| warn!(object = %name, "Failed to parse ConfigSchema spec: {e}"))
        .ok()?;

    let config_name = spec.config_name.trim().to_string();
    if config_name.is_empty() {
        warn!(object = %name, "ConfigSchema has empty configName — ignored");
        return None;
    }

    let compiled = CompiledSchema::compile(&spec.schema)
        .map_err(|e| {
            warn!(object = %name, config_name = %config_name, "ConfigSchema schema failed to compile (draft-07): {e} — entry skipped")
        })
        .ok()?;

    Some(SchemaEntry {
        key: (namespace, config_name),
        schema: Arc::new(compiled),
    })
}

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum WatcherError {
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),
    #[error("watcher error: {0}")]
    Watcher(#[from] kube_watcher::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_obj(name: &str, namespace: &str, spec: serde_json::Value) -> DynamicObject {
        let mut obj = DynamicObject::new(name, &schema_api_resource());
        obj.metadata.name = Some(name.to_string());
        obj.metadata.namespace = Some(namespace.to_string());
        obj.data = json!({ "spec": spec });
        obj
    }

    /// A simple object schema: requires `port` (integer) and `host` (string).
    fn sample_schema() -> serde_json::Value {
        json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": ["host", "port"],
            "properties": {
                "host": { "type": "string" },
                "port": { "type": "integer" }
            }
        })
    }

    // ── parse_schema_object ────────────────────────────────────────────────

    #[test]
    fn parse_valid_configschema() {
        let obj = make_obj(
            "cs-a",
            "trading",
            json!({ "configName": "limits", "schema": sample_schema() }),
        );
        let entry = parse_schema_object(&obj).expect("must parse + compile");
        assert_eq!(entry.key, ("trading".to_string(), "limits".to_string()));
    }

    #[test]
    fn parse_missing_namespace_is_none() {
        let mut obj = DynamicObject::new("cs-x", &schema_api_resource());
        obj.metadata.name = Some("cs-x".to_string());
        // No namespace set.
        obj.data = json!({ "spec": { "configName": "limits", "schema": sample_schema() } });
        assert!(parse_schema_object(&obj).is_none());
    }

    #[test]
    fn parse_empty_config_name_is_none() {
        let obj = make_obj(
            "cs-b",
            "trading",
            json!({ "configName": "   ", "schema": sample_schema() }),
        );
        assert!(parse_schema_object(&obj).is_none());
    }

    #[test]
    fn parse_missing_spec_is_none() {
        let mut obj = DynamicObject::new("cs-c", &schema_api_resource());
        obj.metadata.namespace = Some("trading".to_string());
        obj.data = json!({});
        assert!(parse_schema_object(&obj).is_none());
    }

    #[test]
    fn parse_uncompilable_schema_is_skipped() {
        // `type` must be a string or array of strings; `42` is invalid → the
        // draft-07 compile step rejects it, so the entry is skipped (a bad
        // schema must never crash the watcher / leak a half-built entry).
        let obj = make_obj(
            "cs-bad",
            "trading",
            json!({ "configName": "limits", "schema": { "type": 42 } }),
        );
        assert!(
            parse_schema_object(&obj).is_none(),
            "uncompilable schema must be skipped, not panic"
        );
    }

    // ── SchemaTable::validate ──────────────────────────────────────────────

    #[test]
    fn validate_no_schema_registered_accepts_anything() {
        let table = SchemaTable::new();
        // Nothing registered → accept any content for any key.
        assert!(
            table
                .validate("trading", "limits", &json!({ "anything": [1, 2, 3] }))
                .is_ok()
        );
        assert!(
            table
                .validate("trading", "limits", &json!("a bare string"))
                .is_ok()
        );
    }

    #[test]
    fn validate_registered_schema_valid_content_ok() {
        let table = Arc::new(SchemaTable::new());
        let synced = Arc::new(SchemaSynced::new());
        let mut objects = HashMap::new();
        let obj = make_obj(
            "cs-a",
            "trading",
            json!({ "configName": "limits", "schema": sample_schema() }),
        );
        assert!(handle_schema_event(
            Event::Apply(obj),
            &mut objects,
            &synced
        ));
        publish(&objects, &table);

        let valid = json!({ "host": "db.internal", "port": 5432 });
        assert!(
            table.validate("trading", "limits", &valid).is_ok(),
            "content satisfying the schema must validate"
        );
    }

    #[test]
    fn validate_wrong_type_yields_readable_error() {
        let table = Arc::new(SchemaTable::new());
        let synced = Arc::new(SchemaSynced::new());
        let mut objects = HashMap::new();
        let obj = make_obj(
            "cs-a",
            "trading",
            json!({ "configName": "limits", "schema": sample_schema() }),
        );
        handle_schema_event(Event::Apply(obj), &mut objects, &synced);
        publish(&objects, &table);

        // `port` is a string, not an integer.
        let bad = json!({ "host": "db.internal", "port": "nope" });
        let errs = table
            .validate("trading", "limits", &bad)
            .expect_err("wrong type must fail validation");
        assert!(!errs.is_empty(), "must carry at least one error message");
        let joined = errs.join("; ");
        assert!(
            joined.contains("port") || joined.contains("integer") || joined.contains("\"nope\""),
            "error message should reference the offending field/type; got: {joined}"
        );
    }

    #[test]
    fn validate_missing_required_field_yields_error() {
        let table = Arc::new(SchemaTable::new());
        let synced = Arc::new(SchemaSynced::new());
        let mut objects = HashMap::new();
        let obj = make_obj(
            "cs-a",
            "trading",
            json!({ "configName": "limits", "schema": sample_schema() }),
        );
        handle_schema_event(Event::Apply(obj), &mut objects, &synced);
        publish(&objects, &table);

        // Missing the required `port`.
        let bad = json!({ "host": "db.internal" });
        let errs = table
            .validate("trading", "limits", &bad)
            .expect_err("missing required field must fail validation");
        assert!(!errs.is_empty());
        let joined = errs.join("; ");
        assert!(
            joined.contains("port") || joined.contains("required"),
            "error should mention the missing required field; got: {joined}"
        );
    }

    #[test]
    fn validate_only_matches_exact_namespace_and_name() {
        let table = Arc::new(SchemaTable::new());
        let synced = Arc::new(SchemaSynced::new());
        let mut objects = HashMap::new();
        let obj = make_obj(
            "cs-a",
            "trading",
            json!({ "configName": "limits", "schema": sample_schema() }),
        );
        handle_schema_event(Event::Apply(obj), &mut objects, &synced);
        publish(&objects, &table);

        let bad = json!({ "host": "db", "port": "nope" });
        // Same name, DIFFERENT namespace → no schema → accept.
        assert!(table.validate("risk", "limits", &bad).is_ok());
        // Same namespace, DIFFERENT config name → no schema → accept.
        assert!(table.validate("trading", "other", &bad).is_ok());
        // Exact key → schema enforced → reject.
        assert!(table.validate("trading", "limits", &bad).is_err());
    }

    #[test]
    fn delete_removes_schema() {
        let table = Arc::new(SchemaTable::new());
        let synced = Arc::new(SchemaSynced::new());
        let mut objects = HashMap::new();
        let obj = make_obj(
            "cs-a",
            "trading",
            json!({ "configName": "limits", "schema": sample_schema() }),
        );
        handle_schema_event(Event::Apply(obj.clone()), &mut objects, &synced);
        publish(&objects, &table);
        assert_eq!(table.schema_count(), 1);

        handle_schema_event(Event::Delete(obj), &mut objects, &synced);
        publish(&objects, &table);
        assert_eq!(table.schema_count(), 0);
        // After delete: no schema → accept anything again.
        assert!(
            table
                .validate("trading", "limits", &json!({ "port": "nope" }))
                .is_ok()
        );
    }

    #[test]
    fn init_done_marks_synced_and_is_no_rebuild() {
        let synced = Arc::new(SchemaSynced::new());
        let mut objects = HashMap::new();
        assert!(!synced.is_synced());
        assert!(!handle_schema_event(Event::Init, &mut objects, &synced));
        assert!(!synced.is_synced(), "Init alone does not mark synced");
        assert!(!handle_schema_event(Event::InitDone, &mut objects, &synced));
        assert!(synced.is_synced(), "InitDone marks synced");
    }

    #[tokio::test]
    async fn pump_applies_then_marks_synced_on_init_done() {
        let table = Arc::new(SchemaTable::new());
        let synced = Arc::new(SchemaSynced::new());

        let obj = make_obj(
            "cs-a",
            "trading",
            json!({ "configName": "limits", "schema": sample_schema() }),
        );
        let events: Vec<Result<Event<DynamicObject>, kube_watcher::Error>> = vec![
            Ok(Event::Init),
            Ok(Event::InitApply(obj)),
            Ok(Event::InitDone),
        ];
        let stream = futures_util::stream::iter(events);

        let outcome = pump_schema_events(stream, &table, &synced).await;
        assert!(matches!(outcome, PumpOutcome::StreamEnded));
        assert!(synced.is_synced());
        assert_eq!(table.schema_count(), 1);
        assert!(
            table
                .validate("trading", "limits", &json!({ "host": "h", "port": 1 }))
                .is_ok()
        );
    }
}
