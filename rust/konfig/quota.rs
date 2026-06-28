//! Cluster-scoped `TenantQuota.konfig.io/v1` quota table + its kube watcher
//! (Phase 8 multi-tenancy, MT-1, CU-86aj8pvcu).
//!
//! Mirrors [`crate::acl`] (the `ConfigACL` table/watcher): same lock-free
//! `ArcSwap` snapshot, same [`QuotaSynced`] init-sync gate, same cluster-scoped
//! `Api::all_with` watch with exponential-backoff reconnect — specialised for
//! per-tenant *consumption budgets* instead of authorization rules.
//!
//! A tenant is the mTLS client identity (ADR-0002): the table is keyed by the
//! same `identity` string [`crate::grpc::identity::ClientIdentity`] derives and
//! `ConfigACL` is keyed by. This module only *publishes* the budgets; the
//! enforcement points (subscriber gauge, apply token-bucket, cache budget) are
//! the follow-up tickets CU-86aj8pvdb / CU-86aj8pvf1 / CU-86aj8pvg3 and read
//! this table via [`QuotaTable::quota_for`].
//!
//! `0` on any limit means **unlimited** (matches the server-flag defaults), so
//! a `TenantQuota` object present with all-zero limits is an explicit "no
//! bound" declaration, distinct from "no object" (which also falls back to the
//! flag defaults).

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use futures_util::{Stream, StreamExt, TryStreamExt};
use kube::api::ApiResource;
use kube::core::DynamicObject;
use kube::runtime::watcher::{self as kube_watcher, Event, watcher as kube_watch_stream};
use kube::{Api, Client};
use serde::Deserialize;
use tracing::{debug, info, warn};

use crate::watcher::{BACKOFF_STEPS_SECS, backoff_delay};

// ── Constants ─────────────────────────────────────────────────────────────────

/// `TenantQuota` CRD coordinates. Same `group`/`version` as Config/ConfigACL;
/// distinct `kind`/`plural`. Cluster-scoped (no namespace), exactly like
/// `ConfigACL` — a quota bounds a *caller identity* across the cluster.
pub const GROUP: &str = "konfig.io";
pub const VERSION: &str = "v1";
pub const KIND: &str = "TenantQuota";
pub const PLURAL: &str = "tenantquotas";

/// Env var selecting the quota enforcement mode. Mirrors `KONFIG_AUTHZ_MODE`.
pub const MODE_ENV: &str = "KONFIG_TENANT_QUOTA_MODE";

// Anchor this module's reconnect schedule to the Config watcher's (same as acl).
const _: &[u64] = BACKOFF_STEPS_SECS;

/// [`ApiResource`] descriptor for the `TenantQuota` CRD.
pub fn quota_api_resource() -> ApiResource {
    ApiResource {
        group: GROUP.to_string(),
        version: VERSION.to_string(),
        api_version: format!("{GROUP}/{VERSION}"),
        kind: KIND.to_string(),
        plural: PLURAL.to_string(),
    }
}

// ── Mode ────────────────────────────────────────────────────────────────────

/// Quota enforcement mode. Default [`QuotaMode::Disabled`]. Mirrors
/// [`crate::grpc::authz::Mode`] so the quota rollout follows the same
/// `off → permissive → enforce` ladder the authz layer established.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QuotaMode {
    /// Quotas off — no accounting, no enforcement (current behavior).
    #[default]
    Disabled,
    /// Account + emit `konfig_tenant_*` metrics + log would-deny, but ALLOW.
    Permissive,
    /// `RESOURCE_EXHAUSTED` on breach (fail-safe: never deny until synced).
    Enforce,
}

impl QuotaMode {
    /// Resolve from [`MODE_ENV`]. Unset / empty / `off` / `disabled` / any
    /// unrecognised value ⇒ [`QuotaMode::Disabled`].
    pub fn from_env() -> QuotaMode {
        match std::env::var(MODE_ENV) {
            Ok(v) => QuotaMode::parse(&v),
            Err(_) => QuotaMode::Disabled,
        }
    }

    /// Pure parse (extracted so tests need not mutate process env).
    pub fn parse(s: &str) -> QuotaMode {
        match s.trim().to_ascii_lowercase().as_str() {
            "permissive" => QuotaMode::Permissive,
            "enforce" => QuotaMode::Enforce,
            // "off", "disabled", "", and any unknown token fail closed to the
            // zero-overhead default.
            _ => QuotaMode::Disabled,
        }
    }
}

// ── Quota / spec ──────────────────────────────────────────────────────────────

/// Resolved per-tenant consumption budget. `0` on any field = unlimited.
///
/// `Copy` so the hot lookup ([`QuotaTable::quota_for`]) returns by value with
/// no allocation, mirroring the allocation-free `ConfigACL` read path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantQuota {
    /// Max concurrent Subscribe / SubscribeSecrets streams. 0 = unlimited.
    pub max_subscribers: u32,
    /// Apply token-bucket refill rate (tokens/sec). 0 = unlimited.
    pub max_applies_per_second: u32,
    /// Apply token-bucket capacity (burst). 0 = unlimited.
    pub max_applies_burst: u32,
    /// Soft cap on this tenant's attributed cached payload bytes. 0 = unlimited.
    pub cache_memory_budget_bytes: u64,
}

impl TenantQuota {
    /// Combine two budgets for the SAME identity into the most-restrictive one,
    /// treating `0` (unlimited) as "no constraint". Used when more than one
    /// `TenantQuota` object names the same identity — the tightest declared
    /// bound on each independent axis wins, deterministically (vs. relying on
    /// nondeterministic map iteration order).
    fn most_restrictive(self, other: TenantQuota) -> TenantQuota {
        TenantQuota {
            max_subscribers: tighter_u32(self.max_subscribers, other.max_subscribers),
            max_applies_per_second: tighter_u32(
                self.max_applies_per_second,
                other.max_applies_per_second,
            ),
            max_applies_burst: tighter_u32(self.max_applies_burst, other.max_applies_burst),
            cache_memory_budget_bytes: tighter_u64(
                self.cache_memory_budget_bytes,
                other.cache_memory_budget_bytes,
            ),
        }
    }
}

/// Tighter of two limits where `0` = unlimited (so `0` loses to any real bound).
fn tighter_u32(a: u32, b: u32) -> u32 {
    match (a, b) {
        (0, _) => b,
        (_, 0) => a,
        _ => a.min(b),
    }
}

fn tighter_u64(a: u64, b: u64) -> u64 {
    match (a, b) {
        (0, _) => b,
        (_, 0) => a,
        _ => a.min(b),
    }
}

/// Wire shape of `spec` on a `TenantQuota` object. camelCase to match the CRD
/// (`maxSubscribers`, …). Missing fields default to `0` (= unlimited).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct QuotaSpec {
    identity: String,
    #[serde(default)]
    max_subscribers: u32,
    #[serde(default)]
    max_applies_per_second: u32,
    #[serde(default)]
    max_applies_burst: u32,
    #[serde(default)]
    cache_memory_budget_bytes: u64,
}

// ── QuotaTable ──────────────────────────────────────────────────────────────

/// Identity → resolved budget, the whole quota snapshot.
type Inner = HashMap<String, TenantQuota>;

/// Lock-free per-tenant quota table keyed by client identity.
///
/// Reads (the forthcoming enforcement points, one per RPC) pay only an atomic
/// pointer load. The single watcher task rebuilds the full map on each
/// apply/delete and swaps it in — the quota set is small (one entry per
/// onboarded tenant), so a full rebuild per event is cheaper than diffing and
/// keeps the read path allocation-free, exactly like [`crate::acl::AclTable`].
pub struct QuotaTable {
    inner: ArcSwap<Inner>,
}

impl Default for QuotaTable {
    fn default() -> Self {
        Self::new()
    }
}

impl QuotaTable {
    /// Create an empty table.
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::from_pointee(Inner::new()),
        }
    }

    /// The budget declared for `identity`, or `None` when no `TenantQuota`
    /// names it (caller then falls back to the server-flag defaults). Zero
    /// locking — atomic pointer load only.
    pub fn quota_for(&self, identity: &str) -> Option<TenantQuota> {
        self.inner.load().get(identity).copied()
    }

    /// Number of distinct identities currently bounded (diagnostics / metrics).
    pub fn identity_count(&self) -> usize {
        self.inner.load().len()
    }

    /// Atomically replace the whole map.
    fn store(&self, next: Inner) {
        self.inner.store(Arc::new(next));
    }

    /// Replace contents directly. Test-only seam so future enforcement-point
    /// tests can build a fixture without a kube stream.
    #[cfg(test)]
    pub fn replace_for_test(&self, map: Inner) {
        self.store(map);
    }
}

/// Initial-sync flag shared between the quota watcher and the (forthcoming)
/// enforcement points. Identical contract to [`crate::acl::AclSynced`]: starts
/// `false`, flips `true` on the first `Event::InitDone`, never regresses. The
/// `enforce`-mode guard must treat `false` as "fall back to flag defaults,
/// never deny" so the boot window cannot wrongly exhaust a tenant.
#[derive(Debug, Default)]
pub struct QuotaSynced(AtomicBool);

impl QuotaSynced {
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

/// Cluster-scoped watcher for `TenantQuota.konfig.io/v1`.
pub struct QuotaWatcher {
    client: Client,
}

impl QuotaWatcher {
    pub fn new(client: Client) -> Self {
        QuotaWatcher { client }
    }

    /// Run the watcher with exponential-backoff reconnect.
    ///
    /// The table and sync flag are intentionally retained across a reconnect
    /// (last-known-good budgets), mirroring [`crate::acl::AclWatcher::run`]. On
    /// a clean stream end it returns `Ok(())` and the caller's
    /// `run_with_reconnect` restarts it.
    pub async fn run(
        self,
        table: Arc<QuotaTable>,
        synced: Arc<QuotaSynced>,
    ) -> Result<(), WatcherError> {
        let ar = quota_api_resource();
        let mut attempt: usize = 0;

        loop {
            // Cluster-scoped → Api::all_with (TenantQuota has no namespace).
            let api: Api<DynamicObject> = Api::all_with(self.client.clone(), &ar);
            let wc = kube_watcher::Config::default();
            let stream = kube_watch_stream(api, wc).boxed();

            info!(attempt, "TenantQuota watcher started");

            match pump_quota_events(stream, &table, &synced).await {
                PumpOutcome::StreamEnded => {
                    info!("TenantQuota watcher stream ended cleanly");
                    return Ok(());
                }
                PumpOutcome::StreamErrored(e) => {
                    warn!(
                        attempt,
                        "TenantQuota watcher error: {e} — retaining last-known-good"
                    );
                    tokio::time::sleep(backoff_delay(attempt)).await;
                    attempt = attempt.saturating_add(1);
                }
            }
        }
    }
}

/// Outcome of pumping a single quota watcher connection.
#[derive(Debug)]
pub(crate) enum PumpOutcome {
    StreamEnded,
    StreamErrored(kube_watcher::Error),
}

/// Drive a quota watcher stream to completion or first error.
///
/// `Event::Apply` / `Event::InitApply` carry one object; `Event::Init` signals
/// relist start; `Event::InitDone` signals relist complete (flip the sync
/// flag); `Event::Delete` carries the removed object. Because the quota set is
/// small, every mutating event rebuilds the full snapshot from a running mirror
/// of the live objects — keyed by `metadata.name` (the CRD object name,
/// distinct from the `spec.identity` it carries).
///
/// Extracted from [`QuotaWatcher::run`] so the per-event behaviour is unit-
/// testable against a synthetic stream — no kube API connection required.
pub(crate) async fn pump_quota_events<S>(
    mut stream: S,
    table: &Arc<QuotaTable>,
    synced: &Arc<QuotaSynced>,
) -> PumpOutcome
where
    S: futures_util::Stream<Item = Result<Event<DynamicObject>, kube_watcher::Error>> + Unpin,
{
    // Mirror of the live TenantQuota objects, keyed by CRD object name. Rebuilt
    // into the published `identity → TenantQuota` table on each mutation.
    let mut objects: HashMap<String, QuotaEntry> = HashMap::new();

    loop {
        match stream.try_next().await {
            Ok(Some(event)) => {
                if handle_quota_event(event, &mut objects, synced) {
                    publish(&objects, table);
                }
            }
            Ok(None) => return PumpOutcome::StreamEnded,
            Err(e) => return PumpOutcome::StreamErrored(e),
        }
    }
}

/// One parsed TenantQuota object: the identity it bounds + its resolved budget.
#[derive(Debug, Clone)]
pub(crate) struct QuotaEntry {
    identity: String,
    quota: TenantQuota,
}

/// Apply one event to the in-memory object mirror. Returns `true` when the
/// published table should be rebuilt (an Apply/Delete that changed the set);
/// `false` for lifecycle markers (`Init` / `InitDone`).
fn handle_quota_event(
    event: Event<DynamicObject>,
    objects: &mut HashMap<String, QuotaEntry>,
    synced: &Arc<QuotaSynced>,
) -> bool {
    match event {
        Event::Apply(obj) | Event::InitApply(obj) => {
            let key = obj.metadata.name.clone().unwrap_or_default();
            match parse_quota_object(&obj) {
                Some(entry) => {
                    info!(name = %key, identity = %entry.identity, "TenantQuota applied");
                    objects.insert(key, entry);
                }
                None => {
                    // Unparseable spec: drop any prior entry for this object so
                    // a malformed edit cannot leave a stale budget in place.
                    warn!(name = %key, "TenantQuota could not be parsed — entry removed");
                    objects.remove(&key);
                }
            }
            true
        }
        Event::Delete(obj) => {
            let key = obj.metadata.name.clone().unwrap_or_default();
            warn!(name = %key, "TenantQuota deleted — budget removed");
            objects.remove(&key);
            true
        }
        Event::Init => {
            debug!("TenantQuota watch stream: relist starting");
            false
        }
        Event::InitDone => {
            // Relist complete — table now reflects the full live set; allow the
            // enforce-mode guard to start deciding.
            synced.mark_synced();
            debug!("TenantQuota watch stream: initial list complete — quota cache synced");
            false
        }
    }
}

/// Rebuild and atomically publish the `identity → TenantQuota` table from the
/// object mirror. If more than one object names the same identity, the
/// most-restrictive budget per axis wins (see [`TenantQuota::most_restrictive`]).
fn publish(objects: &HashMap<String, QuotaEntry>, table: &Arc<QuotaTable>) {
    let mut next: Inner = HashMap::with_capacity(objects.len());
    for entry in objects.values() {
        next.entry(entry.identity.clone())
            .and_modify(|q| *q = q.most_restrictive(entry.quota))
            .or_insert(entry.quota);
    }
    table.store(next);
}

/// Parse a `DynamicObject` (TenantQuota CRD) into a [`QuotaEntry`].
///
/// Expects `obj.data["spec"]` to deserialize as [`QuotaSpec`]. Returns `None`
/// when the spec is missing / unparseable / has an empty `identity`.
pub(crate) fn parse_quota_object(obj: &DynamicObject) -> Option<QuotaEntry> {
    let name = obj.metadata.name.clone().unwrap_or_default();
    let spec_value = obj.data.get("spec")?;
    let spec: QuotaSpec = serde_json::from_value(spec_value.clone())
        .map_err(|e| warn!(name = %name, "Failed to parse TenantQuota spec: {e}"))
        .ok()?;

    let identity = spec.identity.trim().to_string();
    if identity.is_empty() {
        warn!(name = %name, "TenantQuota has empty identity — ignored");
        return None;
    }

    Some(QuotaEntry {
        identity,
        quota: TenantQuota {
            max_subscribers: spec.max_subscribers,
            max_applies_per_second: spec.max_applies_per_second,
            max_applies_burst: spec.max_applies_burst,
            cache_memory_budget_bytes: spec.cache_memory_budget_bytes,
        },
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

// ── Subscriber accounting (MT-2, CU-86aj8pvdb) ────────────────────────────────

/// Resolve the concurrent-subscriber limit for `identity` (`0` = unlimited).
///
/// Per-tenant `TenantQuota.maxSubscribers` is only trusted once the watcher has
/// completed its initial list (`synced`) — mirroring the authz fail-safe — so
/// the boot window can never wrongly exhaust a tenant. Until then, and whenever
/// no quota names the identity, the server-flag `default_max` applies. A synced
/// quota that names the identity wins even at `0` (an explicit "unlimited for
/// this tenant", overriding the flag default).
pub fn effective_subscriber_limit(
    table: &QuotaTable,
    synced: bool,
    default_max: u32,
    identity: &str,
) -> u32 {
    if synced {
        match table.quota_for(identity) {
            Some(q) => q.max_subscribers,
            None => default_max,
        }
    } else {
        default_max
    }
}

/// Live per-identity concurrent-subscriber counts — runtime state, *not* from
/// the CRD. Shared across the `Subscribe` and `SubscribeSecrets` handlers; both
/// stream kinds count against the one per-tenant `maxSubscribers` budget. A
/// `DashMap` makes the per-key check-and-increment atomic without a global
/// lock, and the `konfig_tenant_subscribers` gauge is mirrored on every
/// admit/release so it always reflects the live count.
#[derive(Debug, Default)]
pub struct SubscriberCounts {
    inner: DashMap<String, u32>,
}

/// Admission decision returned by [`SubscriberCounts::admit`].
pub enum Admit {
    /// Stream allowed — hold `guard` for its lifetime. In `Permissive`,
    /// `over_budget` is `true` when the limit was exceeded (would-deny, but
    /// allowed); the caller logs + bumps the would-deny metric.
    Allowed {
        guard: SubscriberGuard,
        current: u32,
        limit: u32,
        over_budget: bool,
    },
    /// Stream denied (`Enforce`, over budget). Caller returns
    /// `RESOURCE_EXHAUSTED`.
    Denied { current: u32, limit: u32 },
}

impl SubscriberCounts {
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }

    /// Current live stream count for `identity`.
    pub fn current(&self, identity: &str) -> u32 {
        self.inner.get(identity).map(|v| *v).unwrap_or(0)
    }

    /// Decide admission for one new stream from `identity` under `mode` +
    /// resolved `limit` (`0` = unlimited). `Disabled` callers must short-circuit
    /// *before* calling — they skip accounting entirely for zero overhead — so
    /// this only handles `Permissive` / `Enforce`; a defensive `Disabled` call
    /// is treated as an unbounded allow.
    pub fn admit(self: &Arc<Self>, identity: &str, mode: QuotaMode, limit: u32) -> Admit {
        match mode {
            QuotaMode::Enforce => match self.try_increment(identity, limit) {
                Ok(current) => Admit::Allowed {
                    guard: self.guard(identity),
                    current,
                    limit,
                    over_budget: false,
                },
                Err(current) => Admit::Denied { current, limit },
            },
            // Permissive (and the defensive Disabled path) count unconditionally
            // so the gauge and the would-deny signal stay accurate over budget.
            QuotaMode::Permissive | QuotaMode::Disabled => {
                let current = self.increment_unchecked(identity);
                let over_budget = mode == QuotaMode::Permissive && limit != 0 && current > limit;
                Admit::Allowed {
                    guard: self.guard(identity),
                    current,
                    limit,
                    over_budget,
                }
            }
        }
    }

    /// Atomically admit iff under `limit` (`0` = unlimited). `Ok(new_count)` on
    /// success, `Err(current)` on breach (no increment). The per-key DashMap
    /// entry lock makes the check-and-increment race-free across handlers.
    fn try_increment(&self, identity: &str, limit: u32) -> Result<u32, u32> {
        let mut e = self.inner.entry(identity.to_string()).or_insert(0);
        if limit != 0 && *e >= limit {
            return Err(*e);
        }
        *e += 1;
        let n = *e;
        drop(e);
        crate::metrics::set_tenant_subscribers(identity, n);
        Ok(n)
    }

    /// Unconditionally admit one stream. Returns the new count.
    fn increment_unchecked(&self, identity: &str) -> u32 {
        let mut e = self.inner.entry(identity.to_string()).or_insert(0);
        *e += 1;
        let n = *e;
        drop(e);
        crate::metrics::set_tenant_subscribers(identity, n);
        n
    }

    /// Release one stream for `identity` (called by [`SubscriberGuard::drop`]).
    /// Reaps the entry at zero so neither the map nor the gauge series retains
    /// one entry per tenant that ever connected.
    fn release(&self, identity: &str) {
        let now_zero = match self.inner.get_mut(identity) {
            Some(mut e) => {
                *e = e.saturating_sub(1);
                *e == 0
            }
            None => false,
        };
        if now_zero {
            self.inner.remove_if(identity, |_, &v| v == 0);
            crate::metrics::clear_tenant_subscribers(identity);
        } else {
            crate::metrics::set_tenant_subscribers(identity, self.current(identity));
        }
    }

    fn guard(self: &Arc<Self>, identity: &str) -> SubscriberGuard {
        SubscriberGuard {
            counts: Arc::clone(self),
            identity: identity.to_string(),
        }
    }
}

/// RAII guard: releases its identity's subscriber slot on drop. Held for the
/// lifetime of the `Subscribe` / `SubscribeSecrets` stream via [`GuardedStream`];
/// when the client disconnects, the server drains, or the stream otherwise ends,
/// the stream — and hence this guard — drops, freeing the slot.
#[derive(Debug)]
pub struct SubscriberGuard {
    counts: Arc<SubscriberCounts>,
    identity: String,
}

impl Drop for SubscriberGuard {
    fn drop(&mut self) {
        self.counts.release(&self.identity);
    }
}

/// Attaches an optional [`SubscriberGuard`] to a response stream so the guard
/// drops — releasing the tenant's slot — exactly when the stream is dropped.
/// `S: Unpin` (tonic's `ReceiverStream` is) so no pin-projection is required;
/// `poll_next` simply forwards to the inner stream.
pub struct GuardedStream<S> {
    inner: S,
    _guard: Option<SubscriberGuard>,
}

impl<S> GuardedStream<S> {
    pub fn new(inner: S, guard: Option<SubscriberGuard>) -> Self {
        Self {
            inner,
            _guard: guard,
        }
    }
}

impl<S: Stream + Unpin> Stream for GuardedStream<S> {
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().inner).poll_next(cx)
    }
}

// Hand-written so callers can `{:?}` a `Response<GuardedStream<_>>` (the tonic
// trait test paths do) without requiring the wrapped stream `S: Debug` — the
// inner stream has no meaningful debug form anyway.
impl<S> std::fmt::Debug for GuardedStream<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GuardedStream")
            .field("guard", &self._guard)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_obj(name: &str, spec: serde_json::Value) -> DynamicObject {
        let mut obj = DynamicObject::new(name, &quota_api_resource());
        obj.metadata.name = Some(name.to_string());
        obj.data = json!({ "spec": spec });
        obj
    }

    #[test]
    fn mode_parse_ladder() {
        assert_eq!(QuotaMode::parse("permissive"), QuotaMode::Permissive);
        assert_eq!(QuotaMode::parse(" ENFORCE "), QuotaMode::Enforce);
        assert_eq!(QuotaMode::parse("off"), QuotaMode::Disabled);
        assert_eq!(QuotaMode::parse("disabled"), QuotaMode::Disabled);
        assert_eq!(QuotaMode::parse(""), QuotaMode::Disabled);
        assert_eq!(QuotaMode::parse("bogus"), QuotaMode::Disabled);
        assert_eq!(QuotaMode::default(), QuotaMode::Disabled);
    }

    #[test]
    fn tighter_treats_zero_as_unlimited() {
        assert_eq!(tighter_u32(0, 5), 5);
        assert_eq!(tighter_u32(5, 0), 5);
        assert_eq!(tighter_u32(3, 7), 3);
        assert_eq!(tighter_u32(0, 0), 0);
        assert_eq!(tighter_u64(0, 9), 9);
        assert_eq!(tighter_u64(9, 4), 4);
        assert_eq!(tighter_u64(0, 0), 0);
    }

    #[test]
    fn parse_valid_quota() {
        let obj = make_obj(
            "q-payments",
            json!({
                "identity": "spiffe://corp/payments",
                "maxSubscribers": 200,
                "maxAppliesPerSecond": 50,
                "maxAppliesBurst": 100,
                "cacheMemoryBudgetBytes": 67_108_864u64,
            }),
        );
        let entry = parse_quota_object(&obj).expect("must parse");
        assert_eq!(entry.identity, "spiffe://corp/payments");
        assert_eq!(entry.quota.max_subscribers, 200);
        assert_eq!(entry.quota.max_applies_per_second, 50);
        assert_eq!(entry.quota.max_applies_burst, 100);
        assert_eq!(entry.quota.cache_memory_budget_bytes, 67_108_864);
    }

    #[test]
    fn parse_missing_fields_default_to_zero_unlimited() {
        let obj = make_obj("q-min", json!({ "identity": "svc-a" }));
        let entry = parse_quota_object(&obj).expect("identity present");
        assert_eq!(
            entry.quota,
            TenantQuota {
                max_subscribers: 0,
                max_applies_per_second: 0,
                max_applies_burst: 0,
                cache_memory_budget_bytes: 0,
            }
        );
    }

    #[test]
    fn parse_empty_identity_is_none() {
        assert!(parse_quota_object(&make_obj("q", json!({ "identity": "  " }))).is_none());
    }

    #[test]
    fn parse_missing_spec_is_none() {
        let mut obj = DynamicObject::new("x", &quota_api_resource());
        obj.data = json!({});
        assert!(parse_quota_object(&obj).is_none());
    }

    #[test]
    fn table_quota_for_after_publish() {
        let table = Arc::new(QuotaTable::new());
        let synced = Arc::new(QuotaSynced::new());
        let mut objects = HashMap::new();
        let obj = make_obj("q-a", json!({ "identity": "svc-a", "maxSubscribers": 10 }));
        assert!(handle_quota_event(Event::Apply(obj), &mut objects, &synced));
        publish(&objects, &table);

        let q = table.quota_for("svc-a").expect("present");
        assert_eq!(q.max_subscribers, 10);
        assert!(table.quota_for("nobody").is_none());
        assert_eq!(table.identity_count(), 1);
    }

    #[test]
    fn delete_removes_quota() {
        let table = Arc::new(QuotaTable::new());
        let synced = Arc::new(QuotaSynced::new());
        let mut objects = HashMap::new();
        let obj = make_obj("q-a", json!({ "identity": "svc-a", "maxSubscribers": 10 }));
        handle_quota_event(Event::Apply(obj.clone()), &mut objects, &synced);
        publish(&objects, &table);
        assert!(table.quota_for("svc-a").is_some());

        handle_quota_event(Event::Delete(obj), &mut objects, &synced);
        publish(&objects, &table);
        assert!(table.quota_for("svc-a").is_none());
        assert_eq!(table.identity_count(), 0);
    }

    #[test]
    fn init_done_marks_synced_and_is_no_rebuild() {
        let synced = Arc::new(QuotaSynced::new());
        let mut objects = HashMap::new();
        assert!(!synced.is_synced());
        assert!(!handle_quota_event(Event::Init, &mut objects, &synced));
        assert!(!synced.is_synced(), "Init alone does not mark synced");
        assert!(!handle_quota_event(Event::InitDone, &mut objects, &synced));
        assert!(synced.is_synced(), "InitDone marks synced");
    }

    #[test]
    fn two_objects_same_identity_take_most_restrictive() {
        let table = Arc::new(QuotaTable::new());
        let synced = Arc::new(QuotaSynced::new());
        let mut objects = HashMap::new();
        handle_quota_event(
            Event::Apply(make_obj(
                "q-1",
                json!({ "identity": "svc", "maxSubscribers": 100, "maxAppliesPerSecond": 0 }),
            )),
            &mut objects,
            &synced,
        );
        handle_quota_event(
            Event::Apply(make_obj(
                "q-2",
                json!({ "identity": "svc", "maxSubscribers": 40, "maxAppliesPerSecond": 25 }),
            )),
            &mut objects,
            &synced,
        );
        publish(&objects, &table);
        assert_eq!(table.identity_count(), 1);
        let q = table.quota_for("svc").expect("present");
        assert_eq!(q.max_subscribers, 40, "tighter of 100/40");
        assert_eq!(q.max_applies_per_second, 25, "0 (unlimited) loses to 25");
    }

    #[test]
    fn unparseable_reapply_removes_prior_entry() {
        let table = Arc::new(QuotaTable::new());
        let synced = Arc::new(QuotaSynced::new());
        let mut objects = HashMap::new();
        handle_quota_event(
            Event::Apply(make_obj(
                "q-a",
                json!({ "identity": "svc-a", "maxSubscribers": 5 }),
            )),
            &mut objects,
            &synced,
        );
        publish(&objects, &table);
        assert!(table.quota_for("svc-a").is_some());

        // Re-apply the same object name with an empty identity → parse fails →
        // the prior entry must be dropped (no stale budget).
        handle_quota_event(
            Event::Apply(make_obj("q-a", json!({ "identity": "" }))),
            &mut objects,
            &synced,
        );
        publish(&objects, &table);
        assert!(table.quota_for("svc-a").is_none());
        assert_eq!(table.identity_count(), 0);
    }

    #[tokio::test]
    async fn pump_applies_then_marks_synced_on_init_done() {
        let table = Arc::new(QuotaTable::new());
        let synced = Arc::new(QuotaSynced::new());

        let obj = make_obj("q-a", json!({ "identity": "svc-a", "maxSubscribers": 7 }));
        let events: Vec<Result<Event<DynamicObject>, kube_watcher::Error>> = vec![
            Ok(Event::Init),
            Ok(Event::InitApply(obj)),
            Ok(Event::InitDone),
        ];
        let stream = futures_util::stream::iter(events);

        let outcome = pump_quota_events(stream, &table, &synced).await;
        assert!(matches!(outcome, PumpOutcome::StreamEnded));
        assert!(synced.is_synced());
        assert_eq!(table.quota_for("svc-a").unwrap().max_subscribers, 7);
    }

    // ── Subscriber accounting (MT-2) ──────────────────────────────────────

    fn table_with(identity: &str, q: TenantQuota) -> QuotaTable {
        let table = QuotaTable::new();
        let mut m = Inner::new();
        m.insert(identity.to_string(), q);
        table.replace_for_test(m);
        table
    }

    fn quota(max_subscribers: u32) -> TenantQuota {
        TenantQuota {
            max_subscribers,
            max_applies_per_second: 0,
            max_applies_burst: 0,
            cache_memory_budget_bytes: 0,
        }
    }

    #[test]
    fn effective_limit_uses_synced_quota_over_default() {
        let table = table_with("svc-a", quota(3));
        // synced + matching quota → quota wins over the flag default.
        assert_eq!(effective_subscriber_limit(&table, true, 99, "svc-a"), 3);
    }

    #[test]
    fn effective_limit_falls_back_to_default_when_no_quota() {
        let table = table_with("svc-a", quota(3));
        // synced but no quota names "svc-b" → flag default applies.
        assert_eq!(effective_subscriber_limit(&table, true, 7, "svc-b"), 7);
    }

    #[test]
    fn effective_limit_ignores_quota_until_synced() {
        let table = table_with("svc-a", quota(3));
        // un-synced → never trust the CRD value; use the flag default. This is
        // the boot-window fail-safe: a stale/empty table cannot exhaust a tenant.
        assert_eq!(effective_subscriber_limit(&table, false, 7, "svc-a"), 7);
    }

    #[test]
    fn effective_limit_zero_quota_is_explicit_unlimited() {
        let table = table_with("svc-a", quota(0));
        // A synced quota with maxSubscribers:0 means unlimited for that tenant,
        // overriding even a non-zero flag default.
        assert_eq!(effective_subscriber_limit(&table, true, 5, "svc-a"), 0);
    }

    #[test]
    fn enforce_admits_under_limit_and_denies_at_limit() {
        let counts = Arc::new(SubscriberCounts::new());
        let g1 = match counts.admit("mt2-enforce", QuotaMode::Enforce, 2) {
            Admit::Allowed { guard, current, .. } => {
                assert_eq!(current, 1);
                guard
            }
            Admit::Denied { .. } => panic!("first stream must be admitted"),
        };
        let _g2 = match counts.admit("mt2-enforce", QuotaMode::Enforce, 2) {
            Admit::Allowed { guard, current, .. } => {
                assert_eq!(current, 2);
                guard
            }
            Admit::Denied { .. } => panic!("second stream must be admitted (at limit)"),
        };
        // Third over the limit of 2 → denied, count unchanged.
        match counts.admit("mt2-enforce", QuotaMode::Enforce, 2) {
            Admit::Denied { current, limit } => {
                assert_eq!(current, 2);
                assert_eq!(limit, 2);
            }
            Admit::Allowed { .. } => panic!("third stream must be denied"),
        }
        assert_eq!(counts.current("mt2-enforce"), 2);
        // Dropping a guard frees a slot; a new stream is then admitted.
        drop(g1);
        assert_eq!(counts.current("mt2-enforce"), 1);
        let _g3 = match counts.admit("mt2-enforce", QuotaMode::Enforce, 2) {
            Admit::Allowed { guard, .. } => guard,
            Admit::Denied { .. } => panic!("slot freed by drop(g1) must be re-admitted"),
        };
        assert_eq!(counts.current("mt2-enforce"), 2);
    }

    #[test]
    fn enforce_zero_limit_is_unlimited() {
        let counts = Arc::new(SubscriberCounts::new());
        // Hold every guard — dropping them would release the slots and defeat
        // the assertion below.
        let mut guards = Vec::new();
        for _ in 0..50 {
            match counts.admit("mt2-unlimited", QuotaMode::Enforce, 0) {
                Admit::Allowed {
                    guard, over_budget, ..
                } => {
                    assert!(!over_budget, "limit 0 is unlimited — never over budget");
                    guards.push(guard);
                }
                Admit::Denied { .. } => panic!("limit 0 must never deny"),
            }
        }
        assert_eq!(counts.current("mt2-unlimited"), 50);
        assert_eq!(guards.len(), 50);
    }

    #[test]
    fn permissive_allows_over_budget_but_flags_it() {
        let counts = Arc::new(SubscriberCounts::new());
        let _g1 = match counts.admit("mt2-perm", QuotaMode::Permissive, 1) {
            Admit::Allowed { guard, .. } => guard,
            Admit::Denied { .. } => panic!("permissive must never deny"),
        };
        // Second exceeds the limit of 1, but permissive admits it AND flags the
        // would-deny so the operator sees the signal before flipping to enforce.
        let _g2 = match counts.admit("mt2-perm", QuotaMode::Permissive, 1) {
            Admit::Allowed {
                guard,
                current,
                over_budget,
                ..
            } => {
                assert_eq!(current, 2);
                assert!(over_budget, "permissive must flag the breach");
                guard
            }
            Admit::Denied { .. } => panic!("permissive must never deny"),
        };
        assert_eq!(counts.current("mt2-perm"), 2);
    }

    #[test]
    fn guard_release_reaps_identity_at_zero() {
        let counts = Arc::new(SubscriberCounts::new());
        let g = match counts.admit("mt2-reap", QuotaMode::Enforce, 5) {
            Admit::Allowed { guard, .. } => guard,
            Admit::Denied { .. } => unreachable!(),
        };
        assert_eq!(counts.current("mt2-reap"), 1);
        drop(g);
        // Back to zero — the map entry is reaped so idle tenants do not leak.
        assert_eq!(counts.current("mt2-reap"), 0);
    }
}
