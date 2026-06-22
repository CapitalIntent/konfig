//! Cluster-scoped `ConfigACL.konfig.io/v1` ACL table + its kube watcher
//! (Phase 8 PR2, CU-86ahrwd6f).
//!
//! Mirrors [`crate::watcher`] (Config CRD) and [`crate::cache::ConfigCache`]
//! (`ArcSwap` lock-free reads), specialised for authorization policy:
//!   - `ConfigACL` is **cluster-scoped**, so the watcher uses `Api::all_with`
//!     (the Config watcher is namespaced — `Api::namespaced_with`).
//!   - The table is keyed by `identity` (the cert SAN URI / CN that
//!     [`crate::grpc::identity::ClientIdentity`] derives), not by
//!     `(namespace, name)`.
//!   - An [`AclSynced`] flag flips `true` once the initial list completes
//!     (`Event::InitDone`). The enforce-mode guard returns `UNAVAILABLE` until
//!     it does, so the boot window can never serve un-authorized
//!     (see [`crate::grpc::authz::check`]).
//!
//! Reads are fully lock-free: the guard runs on every authz-enabled RPC, so the
//! whole identity→rules map sits behind one [`ArcSwap`] pointer; the single
//! watcher writer clones-and-swaps on each event.

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

use crate::grpc::authz::Verb;
use crate::watcher::{BACKOFF_STEPS_SECS, backoff_delay};

// ── Constants ─────────────────────────────────────────────────────────────────

/// `ConfigACL` CRD coordinates. Same `group`/`version` as Config; distinct
/// `kind`/`plural`. Cluster-scoped (no namespace).
pub const GROUP: &str = "konfig.io";
pub const VERSION: &str = "v1";
pub const KIND: &str = "ConfigACL";
pub const PLURAL: &str = "configacls";

// Re-export the shared backoff schedule reference so this module's docs and any
// future tuning stay anchored to the Config watcher's schedule.
const _: &[u64] = BACKOFF_STEPS_SECS;

/// [`ApiResource`] descriptor for the `ConfigACL` CRD.
pub fn acl_api_resource() -> ApiResource {
    ApiResource {
        group: GROUP.to_string(),
        version: VERSION.to_string(),
        api_version: format!("{GROUP}/{VERSION}"),
        kind: KIND.to_string(),
        plural: PLURAL.to_string(),
    }
}

// ── Rule / spec ─────────────────────────────────────────────────────────────

/// One ACL rule: a set of verbs granted over a set of `"<ns>/<name>"` patterns.
///
/// In-memory form — verbs are parsed to the [`Verb`] enum on load (unknown
/// tokens dropped); patterns are validated to contain a `/` on load (malformed
/// ones dropped) so the hot-path [`crate::grpc::authz::pattern_matches`] never
/// re-checks shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rule {
    pub verbs: Vec<Verb>,
    pub patterns: Vec<String>,
}

impl Rule {
    /// Does this rule grant `verb` on `(namespace, name)`?
    fn grants(&self, verb: Verb, namespace: &str, name: &str) -> bool {
        if !self.verbs.contains(&verb) {
            return false;
        }
        self.patterns
            .iter()
            .any(|p| crate::grpc::authz::pattern_matches(p, namespace, name))
    }
}

/// Wire shape of `spec` on a `ConfigACL` object. Deserialized from the
/// `DynamicObject`'s `spec` field, then lowered into `(identity, Vec<Rule>)`.
#[derive(Debug, Deserialize)]
struct AclSpec {
    identity: String,
    #[serde(default)]
    rules: Vec<RuleSpec>,
}

#[derive(Debug, Deserialize)]
struct RuleSpec {
    #[serde(default)]
    verbs: Vec<String>,
    #[serde(default)]
    patterns: Vec<String>,
}

impl RuleSpec {
    /// Lower a wire rule into the in-memory [`Rule`], dropping unknown verbs and
    /// malformed (slash-less) patterns. Returns `None` if nothing usable
    /// survives (no recognised verb or no valid pattern) — such a rule can
    /// never grant anything, so it is not retained.
    fn lower(self) -> Option<Rule> {
        let verbs: Vec<Verb> = self.verbs.iter().filter_map(|v| Verb::parse(v)).collect();
        let patterns: Vec<String> = self
            .patterns
            .into_iter()
            .filter(|p| p.contains('/'))
            .collect();
        if verbs.is_empty() || patterns.is_empty() {
            return None;
        }
        Some(Rule { verbs, patterns })
    }
}

// ── AclTable ──────────────────────────────────────────────────────────────────

/// Identity → rules, the whole policy snapshot.
type Inner = HashMap<String, Vec<Rule>>;

/// Lock-free authorization table keyed by client identity.
///
/// Reads (the per-RPC guard) pay only an atomic pointer load. The single
/// watcher task rebuilds the full map on each apply/delete and swaps it in —
/// the policy set is small (one entry per onboarded client), so a full rebuild
/// per event is cheaper than tracking incremental diffs and keeps the
/// read path allocation-free.
pub struct AclTable {
    inner: ArcSwap<Inner>,
}

impl Default for AclTable {
    fn default() -> Self {
        Self::new()
    }
}

impl AclTable {
    /// Create an empty table.
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::from_pointee(Inner::new()),
        }
    }

    /// Does `identity` hold a rule granting `verb` on `(namespace, name)`?
    /// Zero locking — atomic pointer load only. An identity with no entry
    /// (including the anonymous identity, which is never keyed here) ⇒ `false`.
    pub fn grants(&self, identity: &str, verb: Verb, namespace: &str, name: &str) -> bool {
        let guard = self.inner.load();
        match guard.get(identity) {
            Some(rules) => rules.iter().any(|r| r.grants(verb, namespace, name)),
            None => false,
        }
    }

    /// Number of distinct identities currently in the table (diagnostics).
    pub fn identity_count(&self) -> usize {
        self.inner.load().len()
    }

    /// Atomically replace the whole map. Used by the watcher after rebuilding
    /// the snapshot from the live object set.
    fn store(&self, next: Inner) {
        self.inner.store(Arc::new(next));
    }

    /// Replace the table contents directly. Test-only seam so `authz` unit
    /// tests can build a fixture without a kube stream.
    #[cfg(test)]
    pub fn replace_for_test(&self, map: Inner) {
        self.store(map);
    }
}

/// Initial-sync flag shared between the ACL watcher and the authz guard.
///
/// Starts `false`; the watcher flips it `true` on the first `Event::InitDone`
/// (relist complete). The enforce-mode guard treats `false` as "do not decide
/// yet" and returns `UNAVAILABLE` — never allow, never panic — for the boot
/// window. Once `true` it stays `true`: a later watch reconnect re-lists but the
/// last-known-good table is retained, so we never regress to "unsynced".
#[derive(Debug, Default)]
pub struct AclSynced(AtomicBool);

impl AclSynced {
    pub fn new() -> Self {
        Self(AtomicBool::new(false))
    }

    /// Has the initial list completed at least once?
    pub fn is_synced(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    /// Mark synced (idempotent). Release-ordered so a guard that observes
    /// `true` also sees the table writes that happened-before the InitDone.
    fn mark_synced(&self) {
        self.0.store(true, Ordering::Release);
    }
}

// ── Watcher ────────────────────────────────────────────────────────────────────

/// Cluster-scoped watcher for `ConfigACL.konfig.io/v1`.
pub struct AclWatcher {
    client: Client,
}

impl AclWatcher {
    pub fn new(client: Client) -> Self {
        AclWatcher { client }
    }

    /// Run the watcher with exponential-backoff reconnect.
    ///
    /// On stream error: waits `backoff_delay(attempt)` then retries. The table
    /// and sync flag are intentionally retained across a reconnect
    /// (last-known-good policy), mirroring the Config watcher's CP semantics. On
    /// a clean stream end it returns `Ok(())` and the caller's
    /// `run_with_reconnect` restarts it.
    pub async fn run(
        self,
        table: Arc<AclTable>,
        synced: Arc<AclSynced>,
    ) -> Result<(), WatcherError> {
        let ar = acl_api_resource();
        let mut attempt: usize = 0;

        loop {
            // Cluster-scoped → Api::all_with (the Config watcher uses
            // Api::namespaced_with; ConfigACL has no namespace).
            let api: Api<DynamicObject> = Api::all_with(self.client.clone(), &ar);
            let wc = kube_watcher::Config::default();
            let stream = kube_watch_stream(api, wc).boxed();

            info!(attempt, "ConfigACL watcher started");

            match pump_acl_events(stream, &table, &synced).await {
                PumpOutcome::StreamEnded => {
                    info!("ConfigACL watcher stream ended cleanly");
                    return Ok(());
                }
                PumpOutcome::StreamErrored(e) => {
                    warn!(
                        attempt,
                        "ConfigACL watcher error: {e} — retaining last-known-good"
                    );
                    tokio::time::sleep(backoff_delay(attempt)).await;
                    attempt = attempt.saturating_add(1);
                }
            }
        }
    }
}

/// Outcome of pumping a single ACL watcher connection.
#[derive(Debug)]
pub(crate) enum PumpOutcome {
    StreamEnded,
    StreamErrored(kube_watcher::Error),
}

/// Drive an ACL watcher stream to completion or first error.
///
/// kube-rs `Event::Apply` / `Event::InitApply` carry one object; `Event::Init`
/// signals relist start; `Event::InitDone` signals relist complete (flip the
/// sync flag); `Event::Delete` carries the removed object. Because the policy
/// set is small, every mutating event rebuilds the full snapshot from a
/// running mirror of the live objects — keyed by the object's `metadata.name`
/// (the CRD object name, distinct from the `spec.identity` it carries).
///
/// Extracted from [`AclWatcher::run`] so the per-event behaviour is unit-
/// testable against a synthetic stream — no kube API connection required.
pub(crate) async fn pump_acl_events<S>(
    mut stream: S,
    table: &Arc<AclTable>,
    synced: &Arc<AclSynced>,
) -> PumpOutcome
where
    S: futures_util::Stream<Item = Result<Event<DynamicObject>, kube_watcher::Error>> + Unpin,
{
    // Mirror of the live ConfigACL objects, keyed by CRD object name. Rebuilt
    // into the published `identity → Vec<Rule>` table on each mutation.
    let mut objects: HashMap<String, AclEntry> = HashMap::new();

    loop {
        match stream.try_next().await {
            Ok(Some(event)) => {
                if handle_acl_event(event, &mut objects, synced) {
                    publish(&objects, table);
                }
            }
            Ok(None) => return PumpOutcome::StreamEnded,
            Err(e) => return PumpOutcome::StreamErrored(e),
        }
    }
}

/// One parsed ConfigACL object: the identity it grants for + its lowered rules.
/// `pub(crate)` to match `parse_acl_object`'s visibility (it is returned from
/// there); the fields stay module-private.
#[derive(Debug, Clone)]
pub(crate) struct AclEntry {
    identity: String,
    rules: Vec<Rule>,
}

/// Apply one event to the in-memory object mirror. Returns `true` when the
/// published table should be rebuilt (an Apply/Delete that changed the set);
/// `false` for lifecycle markers (`Init` / `InitDone`).
fn handle_acl_event(
    event: Event<DynamicObject>,
    objects: &mut HashMap<String, AclEntry>,
    synced: &Arc<AclSynced>,
) -> bool {
    match event {
        Event::Apply(obj) | Event::InitApply(obj) => {
            let key = obj.metadata.name.clone().unwrap_or_default();
            match parse_acl_object(&obj) {
                Some(entry) => {
                    info!(name = %key, identity = %entry.identity, "ConfigACL applied");
                    objects.insert(key, entry);
                }
                None => {
                    // Unparseable spec: drop any prior entry for this object so
                    // a malformed edit cannot leave a stale grant in place.
                    warn!(name = %key, "ConfigACL could not be parsed — entry removed");
                    objects.remove(&key);
                }
            }
            true
        }
        Event::Delete(obj) => {
            let key = obj.metadata.name.clone().unwrap_or_default();
            warn!(name = %key, "ConfigACL deleted — grant removed");
            objects.remove(&key);
            true
        }
        Event::Init => {
            debug!("ConfigACL watch stream: relist starting");
            false
        }
        Event::InitDone => {
            // Relist complete — table now reflects the full live set; allow the
            // enforce-mode guard to start deciding.
            synced.mark_synced();
            debug!("ConfigACL watch stream: initial list complete — authz cache synced");
            false
        }
    }
}

/// Rebuild and atomically publish the `identity → Vec<Rule>` table from the
/// object mirror. Multiple objects may grant the same identity; their rules are
/// concatenated.
fn publish(objects: &HashMap<String, AclEntry>, table: &Arc<AclTable>) {
    let mut next: Inner = HashMap::with_capacity(objects.len());
    for entry in objects.values() {
        next.entry(entry.identity.clone())
            .or_default()
            .extend(entry.rules.iter().cloned());
    }
    table.store(next);
}

/// Parse a `DynamicObject` (ConfigACL CRD) into an [`AclEntry`].
///
/// Expects `obj.data["spec"]` to deserialize as [`AclSpec`]. Returns `None`
/// when the spec is missing / unparseable / has an empty `identity`, or when
/// no usable rule survives lowering.
pub(crate) fn parse_acl_object(obj: &DynamicObject) -> Option<AclEntry> {
    let name = obj.metadata.name.clone().unwrap_or_default();
    let spec_value = obj.data.get("spec")?;
    let spec: AclSpec = serde_json::from_value(spec_value.clone())
        .map_err(|e| warn!(name = %name, "Failed to parse ConfigACL spec: {e}"))
        .ok()?;

    let identity = spec.identity.trim().to_string();
    if identity.is_empty() {
        warn!(name = %name, "ConfigACL has empty identity — ignored");
        return None;
    }

    let rules: Vec<Rule> = spec.rules.into_iter().filter_map(RuleSpec::lower).collect();
    Some(AclEntry { identity, rules })
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

    fn make_obj(name: &str, spec: serde_json::Value) -> DynamicObject {
        let mut obj = DynamicObject::new(name, &acl_api_resource());
        obj.metadata.name = Some(name.to_string());
        obj.data = json!({ "spec": spec });
        obj
    }

    #[test]
    fn parse_valid_acl() {
        let obj = make_obj(
            "acl-a",
            json!({
                "identity": "spiffe://konfig/svc-a",
                "rules": [{ "verbs": ["read"], "patterns": ["default/*"] }],
            }),
        );
        let entry = parse_acl_object(&obj).expect("must parse");
        assert_eq!(entry.identity, "spiffe://konfig/svc-a");
        assert_eq!(entry.rules.len(), 1);
        assert_eq!(entry.rules[0].verbs, vec![Verb::Read]);
        assert_eq!(entry.rules[0].patterns, vec!["default/*".to_string()]);
    }

    #[test]
    fn parse_drops_unknown_verbs_and_malformed_patterns() {
        let obj = make_obj(
            "acl-b",
            json!({
                "identity": "svc-b",
                "rules": [{
                    "verbs": ["read", "delete", "WRITE"],
                    "patterns": ["default/*", "no-slash", "prod/db"],
                }],
            }),
        );
        let entry = parse_acl_object(&obj).expect("must parse");
        assert_eq!(entry.rules[0].verbs, vec![Verb::Read, Verb::Write]);
        assert_eq!(
            entry.rules[0].patterns,
            vec!["default/*".to_string(), "prod/db".to_string()]
        );
    }

    #[test]
    fn parse_empty_identity_is_none() {
        let obj = make_obj("acl-c", json!({ "identity": "  ", "rules": [] }));
        assert!(parse_acl_object(&obj).is_none());
    }

    #[test]
    fn parse_missing_spec_is_none() {
        let mut obj = DynamicObject::new("x", &acl_api_resource());
        obj.data = json!({});
        assert!(parse_acl_object(&obj).is_none());
    }

    #[test]
    fn rule_with_no_usable_verb_or_pattern_is_dropped() {
        // identity valid, but rule has only unknown verbs → lowered away.
        let obj = make_obj(
            "acl-d",
            json!({
                "identity": "svc-d",
                "rules": [{ "verbs": ["delete"], "patterns": ["default/*"] }],
            }),
        );
        let entry = parse_acl_object(&obj).expect("identity valid");
        assert!(entry.rules.is_empty(), "rule with no known verb dropped");
    }

    #[test]
    fn table_grants_after_publish() {
        let table = Arc::new(AclTable::new());
        let synced = Arc::new(AclSynced::new());
        let mut objects = HashMap::new();
        let obj = make_obj(
            "acl-a",
            json!({
                "identity": "svc-a",
                "rules": [{ "verbs": ["read"], "patterns": ["default/*"] }],
            }),
        );
        assert!(handle_acl_event(Event::Apply(obj), &mut objects, &synced));
        publish(&objects, &table);

        assert!(table.grants("svc-a", Verb::Read, "default", "x"));
        assert!(!table.grants("svc-a", Verb::Write, "default", "x"));
        assert!(!table.grants("svc-a", Verb::Read, "prod", "x"));
        assert!(!table.grants("nobody", Verb::Read, "default", "x"));
    }

    #[test]
    fn delete_removes_grant() {
        let table = Arc::new(AclTable::new());
        let synced = Arc::new(AclSynced::new());
        let mut objects = HashMap::new();
        let obj = make_obj(
            "acl-a",
            json!({ "identity": "svc-a", "rules": [{ "verbs": ["read"], "patterns": ["default/*"] }] }),
        );
        handle_acl_event(Event::Apply(obj.clone()), &mut objects, &synced);
        publish(&objects, &table);
        assert!(table.grants("svc-a", Verb::Read, "default", "x"));

        handle_acl_event(Event::Delete(obj), &mut objects, &synced);
        publish(&objects, &table);
        assert!(!table.grants("svc-a", Verb::Read, "default", "x"));
        assert_eq!(table.identity_count(), 0);
    }

    #[test]
    fn init_done_marks_synced_and_is_no_rebuild() {
        let table = Arc::new(AclTable::new());
        let synced = Arc::new(AclSynced::new());
        let mut objects = HashMap::new();
        assert!(!synced.is_synced());
        assert!(!handle_acl_event(Event::Init, &mut objects, &synced));
        assert!(!synced.is_synced(), "Init alone does not mark synced");
        assert!(!handle_acl_event(Event::InitDone, &mut objects, &synced));
        assert!(synced.is_synced(), "InitDone marks synced");
    }

    #[test]
    fn two_objects_same_identity_concatenate_rules() {
        let table = Arc::new(AclTable::new());
        let synced = Arc::new(AclSynced::new());
        let mut objects = HashMap::new();
        handle_acl_event(
            Event::Apply(make_obj(
                "acl-1",
                json!({ "identity": "svc", "rules": [{ "verbs": ["read"], "patterns": ["a/*"] }] }),
            )),
            &mut objects,
            &synced,
        );
        handle_acl_event(
            Event::Apply(make_obj(
                "acl-2",
                json!({ "identity": "svc", "rules": [{ "verbs": ["write"], "patterns": ["b/*"] }] }),
            )),
            &mut objects,
            &synced,
        );
        publish(&objects, &table);
        assert_eq!(table.identity_count(), 1);
        assert!(table.grants("svc", Verb::Read, "a", "x"));
        assert!(table.grants("svc", Verb::Write, "b", "y"));
        assert!(!table.grants("svc", Verb::Write, "a", "x"));
    }

    #[tokio::test]
    async fn pump_applies_then_marks_synced_on_init_done() {
        let table = Arc::new(AclTable::new());
        let synced = Arc::new(AclSynced::new());

        let obj = make_obj(
            "acl-a",
            json!({ "identity": "svc-a", "rules": [{ "verbs": ["read"], "patterns": ["default/*"] }] }),
        );
        let events: Vec<Result<Event<DynamicObject>, kube_watcher::Error>> = vec![
            Ok(Event::Init),
            Ok(Event::InitApply(obj)),
            Ok(Event::InitDone),
        ];
        let stream = futures_util::stream::iter(events);

        let outcome = pump_acl_events(stream, &table, &synced).await;
        assert!(matches!(outcome, PumpOutcome::StreamEnded));
        assert!(synced.is_synced());
        assert!(table.grants("svc-a", Verb::Read, "default", "x"));
    }
}
