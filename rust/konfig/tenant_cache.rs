//! Per-tenant cache byte accounting + LRU-view eviction (CU-86aj8pvg3, MT-4).
//!
//! The [`ConfigCache`](crate::cache::ConfigCache) /
//! [`SecretCache`](crate::secret_cache::SecretCache) stay a single shared,
//! lock-free map (splitting per tenant would multiply memory + watcher
//! fan-out). Instead we *attribute* each served entry's byte cost to the
//! tenant it was served to, in a per-identity [`TenantView`] holding a byte
//! total + an LRU recency ordering.
//!
//! When a tenant's attributed bytes exceed its `cacheMemoryBudgetBytes`, the
//! tenant's least-recently-served entries are evicted **from its view only**
//! (the shared payload stays for other tenants; a cold read re-populates).
//! Config and secret bytes share one per-identity budget, so the ledger keys
//! entries by `(kind, namespace, name)` against a single counter.
//!
//! The budget is a back-pressure signal, never a correctness gate. Accounting
//! runs at *serve* time via [`AccountedStream`], a thin adapter that wraps a
//! response stream and tallies each delivered item — so the lock-free
//! `get`/broadcast hot paths themselves are untouched; the cost is paid on the
//! per-subscriber delivery side, gated by the quota mode.

use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use dashmap::DashMap;
use futures_util::Stream;
use tonic::Status;

use crate::proto::{Config, ConfigEvent, SecretEvent, SecretResponse};
use crate::quota::QuotaMode;

/// Composite ledger key: `(kind, namespace, name)`. `kind` (`"config"` /
/// `"secret"`) disambiguates a config and a secret that share a namespace+name
/// while both counting against the tenant's single budget.
type Key = (&'static str, String, String);

// ── Per-tenant view ─────────────────────────────────────────────────────────

/// One tenant's accounting view: which entries it has been served (each at its
/// latest byte cost), the running byte total, and an LRU recency ordering.
#[derive(Debug, Default)]
struct TenantView {
    by_key: HashMap<Key, Slot>,
    /// Recency index: monotonically-increasing tick → key. The smallest tick is
    /// the least-recently-served entry — the eviction victim.
    by_tick: BTreeMap<u64, Key>,
    total: usize,
    next_tick: u64,
}

#[derive(Debug)]
struct Slot {
    bytes: usize,
    tick: u64,
}

impl TenantView {
    /// Record (or refresh) `key` at `bytes`, making it the most-recently-served
    /// entry. Re-serving an existing key replaces its byte cost rather than
    /// double-counting.
    fn touch(&mut self, key: Key, bytes: usize) {
        if let Some(prev) = self.by_key.get(&key) {
            self.by_tick.remove(&prev.tick);
            self.total -= prev.bytes;
        }
        let tick = self.next_tick;
        self.next_tick += 1;
        self.by_tick.insert(tick, key.clone());
        self.by_key.insert(key, Slot { bytes, tick });
        self.total += bytes;
    }

    /// Evict least-recently-served entries until `total <= budget`, always
    /// keeping at least the most-recently-served entry (so a single entry that
    /// alone exceeds the budget is not pointlessly dropped right after serving
    /// it). `budget == 0` means unlimited — never evicts. Returns the count
    /// evicted.
    fn evict_to(&mut self, budget: u64) -> u64 {
        if budget == 0 {
            return 0;
        }
        let budget = budget as usize;
        let mut evicted = 0;
        while self.total > budget && self.by_key.len() > 1 {
            let Some((&tick, _)) = self.by_tick.iter().next() else {
                break;
            };
            if let Some(key) = self.by_tick.remove(&tick) {
                if let Some(slot) = self.by_key.remove(&key) {
                    self.total -= slot.bytes;
                }
            }
            evicted += 1;
        }
        evicted
    }
}

// ── Ledger ──────────────────────────────────────────────────────────────────

/// Per-identity cache byte ledger. Sharded by identity via [`DashMap`] so
/// distinct tenants never contend; a single tenant's serves serialise on its
/// own view mutex.
#[derive(Debug, Default)]
pub struct TenantCacheLedger {
    views: DashMap<String, std::sync::Mutex<TenantView>>,
}

impl TenantCacheLedger {
    pub fn new() -> Self {
        Self {
            views: DashMap::new(),
        }
    }

    /// Attribute `bytes` for `(kind, namespace, name)` to `identity` and refresh
    /// the per-identity byte gauge. In [`QuotaMode::Enforce`], evict the
    /// tenant's LRU entries until it is back within `budget`, bumping
    /// `konfig_tenant_cache_evictions_total`. In `Permissive` the view is
    /// accounted (gauge reflects over-budget) but never evicted, so operators
    /// can size budgets against real traffic first. Callers never invoke this
    /// for `Disabled` (the accountant is `None`).
    pub fn record(
        &self,
        identity: &str,
        kind: &'static str,
        namespace: &str,
        name: &str,
        bytes: usize,
        mode: QuotaMode,
        budget: u64,
    ) {
        let key: Key = (kind, namespace.to_string(), name.to_string());
        let (total, evicted) = {
            let view = self.views.entry(identity.to_string()).or_default();
            let mut v = crate::sync_util::lock_recovered(&view);
            v.touch(key, bytes);
            let evicted = if mode == QuotaMode::Enforce {
                v.evict_to(budget)
            } else {
                0
            };
            (v.total, evicted)
        };
        crate::metrics::set_tenant_cache_bytes(identity, total);
        if evicted > 0 {
            crate::metrics::record_tenant_cache_evictions(identity, evicted);
        }
    }

    /// Current attributed byte total for `identity` (0 if never served). Test +
    /// introspection helper.
    #[cfg(test)]
    pub fn bytes_of(&self, identity: &str) -> usize {
        self.views
            .get(identity)
            .map(|v| crate::sync_util::lock_recovered(&v).total)
            .unwrap_or(0)
    }
}

// ── Accountant + stream adapter ───────────────────────────────────────────────

/// Bundles everything needed to attribute serves for one RPC: the shared
/// ledger, the caller identity, the enforcement mode, the resolved budget, and
/// the cache `kind`. The budget is resolved once at RPC start (a mid-stream
/// `TenantQuota` edit takes effect on the subscriber's next reconnect).
#[derive(Clone)]
pub struct CacheAccountant {
    ledger: Arc<TenantCacheLedger>,
    identity: String,
    mode: QuotaMode,
    budget: u64,
    kind: &'static str,
}

impl CacheAccountant {
    pub fn new(
        ledger: Arc<TenantCacheLedger>,
        identity: String,
        mode: QuotaMode,
        budget: u64,
        kind: &'static str,
    ) -> Self {
        Self {
            ledger,
            identity,
            mode,
            budget,
            kind,
        }
    }

    /// Attribute one served entry to this RPC's tenant.
    pub fn record(&self, namespace: &str, name: &str, bytes: usize) {
        self.ledger.record(
            &self.identity,
            self.kind,
            namespace,
            name,
            bytes,
            self.mode,
            self.budget,
        );
    }
}

/// Byte cost of a served entry: `(namespace, name, bytes)`, or `None` for an
/// entry with no payload to attribute (e.g. a tombstone event). `bytes` is the
/// JSON payload length plus the key strings — a cheap, stable proxy for the
/// entry's cache footprint.
pub type CostFn<T> = fn(&T) -> Option<(&str, &str, usize)>;

/// Cost extractor for a `Config` (GetAll).
pub fn config_cost(c: &Config) -> Option<(&str, &str, usize)> {
    Some((
        &c.namespace,
        &c.name,
        c.content_json.len() + c.namespace.len() + c.name.len(),
    ))
}

/// Cost extractor for a `ConfigEvent` (Subscribe replay + live).
pub fn config_event_cost(e: &ConfigEvent) -> Option<(&str, &str, usize)> {
    e.config.as_ref().map(|c| {
        (
            c.namespace.as_str(),
            c.name.as_str(),
            c.content_json.len() + c.namespace.len() + c.name.len(),
        )
    })
}

/// Cost extractor for a `SecretResponse` (GetAllSecrets).
pub fn secret_cost(s: &SecretResponse) -> Option<(&str, &str, usize)> {
    Some((
        &s.namespace,
        &s.name,
        s.data_json.len() + s.namespace.len() + s.name.len(),
    ))
}

/// Cost extractor for a `SecretEvent` (SubscribeSecrets replay + live).
pub fn secret_event_cost(e: &SecretEvent) -> Option<(&str, &str, usize)> {
    e.secret.as_ref().map(|s| {
        (
            s.namespace.as_str(),
            s.name.as_str(),
            s.data_json.len() + s.namespace.len() + s.name.len(),
        )
    })
}

/// Wraps a response stream and attributes every delivered `Ok(item)` to the
/// tenant via [`CacheAccountant`]. Covers Subscribe replay **and** live events
/// (both flow out through this one stream) plus GetAll, without touching the
/// lock-free read or broadcast fan-out internals. When `acct` is `None` (quotas
/// off) it is a transparent pass-through with a single branch per item.
/// `S: Unpin` (tonic's `ReceiverStream` is) so no pin-projection is needed.
pub struct AccountedStream<S, T> {
    inner: S,
    acct: Option<CacheAccountant>,
    cost: CostFn<T>,
}

impl<S, T> AccountedStream<S, T> {
    pub fn new(inner: S, acct: Option<CacheAccountant>, cost: CostFn<T>) -> Self {
        Self { inner, acct, cost }
    }
}

impl<S, T> Stream for AccountedStream<S, T>
where
    S: Stream<Item = Result<T, Status>> + Unpin,
{
    type Item = Result<T, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.inner).poll_next(cx);
        if let (Some(acct), Poll::Ready(Some(Ok(item)))) = (&this.acct, &polled) {
            if let Some((namespace, name, bytes)) = (this.cost)(item) {
                acct.record(namespace, name, bytes);
            }
        }
        polled
    }
}

// Hand-written so callers can `{:?}` a `Response<AccountedStream<_>>` without
// requiring the wrapped stream `S: Debug` (the inner stream has none).
impl<S, T> std::fmt::Debug for AccountedStream<S, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountedStream")
            .field("accounting", &self.acct.is_some())
            .finish_non_exhaustive()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn ledger() -> TenantCacheLedger {
        TenantCacheLedger::new()
    }

    #[test]
    fn reserving_same_key_replaces_not_accumulates() {
        let l = ledger();
        l.record("t", "config", "ns", "a", 100, QuotaMode::Permissive, 0);
        l.record("t", "config", "ns", "a", 40, QuotaMode::Permissive, 0);
        // Re-serve replaces the byte cost; the view holds one entry, not two.
        assert_eq!(l.bytes_of("t"), 40);
    }

    #[test]
    fn config_and_secret_same_name_are_distinct_entries() {
        let l = ledger();
        l.record("t", "config", "ns", "x", 10, QuotaMode::Permissive, 0);
        l.record("t", "secret", "ns", "x", 7, QuotaMode::Permissive, 0);
        // Same namespace+name but different kind ⇒ both counted.
        assert_eq!(l.bytes_of("t"), 17);
    }

    #[test]
    fn permissive_accounts_but_never_evicts() {
        let l = ledger();
        // Budget of 10 bytes, but permissive must not evict — total exceeds it.
        l.record("t", "config", "ns", "a", 8, QuotaMode::Permissive, 10);
        l.record("t", "config", "ns", "b", 8, QuotaMode::Permissive, 10);
        assert_eq!(l.bytes_of("t"), 16);
    }

    #[test]
    fn enforce_evicts_lru_until_within_budget() {
        let l = ledger();
        // Budget 10. Serve a(4), b(4), c(4): total would be 12 > 10, so the LRU
        // entry (a) is evicted, leaving b + c = 8.
        l.record("t", "config", "ns", "a", 4, QuotaMode::Enforce, 10);
        l.record("t", "config", "ns", "b", 4, QuotaMode::Enforce, 10);
        l.record("t", "config", "ns", "c", 4, QuotaMode::Enforce, 10);
        assert_eq!(l.bytes_of("t"), 8);
    }

    #[test]
    fn enforce_keeps_recency_alive_on_reserve() {
        let l = ledger();
        // a, b, c with budget 8 → after c, a evicted (b,c = 8). Re-serving b
        // makes it MRU; serving d(4) now evicts c (the new LRU), keeping b + d.
        l.record("t", "config", "ns", "a", 4, QuotaMode::Enforce, 8);
        l.record("t", "config", "ns", "b", 4, QuotaMode::Enforce, 8);
        l.record("t", "config", "ns", "c", 4, QuotaMode::Enforce, 8); // evicts a
        l.record("t", "config", "ns", "b", 4, QuotaMode::Enforce, 8); // b now MRU
        l.record("t", "config", "ns", "d", 4, QuotaMode::Enforce, 8); // evicts c
        assert_eq!(l.bytes_of("t"), 8);
    }

    #[test]
    fn enforce_keeps_single_oversized_entry() {
        let l = ledger();
        // One entry larger than the whole budget is not evicted right after
        // serving it (we always keep ≥1 — back-pressure, not a correctness gate).
        l.record("t", "config", "ns", "big", 100, QuotaMode::Enforce, 10);
        assert_eq!(l.bytes_of("t"), 100);
    }

    #[test]
    fn zero_budget_is_unlimited() {
        let l = ledger();
        for i in 0..50 {
            l.record(
                "t",
                "config",
                "ns",
                &format!("k{i}"),
                1000,
                QuotaMode::Enforce,
                0,
            );
        }
        assert_eq!(l.bytes_of("t"), 50_000);
    }

    #[test]
    fn distinct_identities_are_isolated() {
        let l = ledger();
        l.record("t1", "config", "ns", "a", 30, QuotaMode::Enforce, 0);
        l.record("t2", "config", "ns", "a", 70, QuotaMode::Enforce, 0);
        assert_eq!(l.bytes_of("t1"), 30);
        assert_eq!(l.bytes_of("t2"), 70);
    }

    #[test]
    fn cost_extractors_read_payload_plus_key() {
        let c = Config {
            namespace: "ns".into(),
            name: "n".into(),
            content_json: "abcd".into(),
            ..Default::default()
        };
        assert_eq!(config_cost(&c), Some(("ns", "n", 4 + 2 + 1)));

        let ev = ConfigEvent {
            event_type: 1,
            config: Some(c),
        };
        assert_eq!(config_event_cost(&ev), Some(("ns", "n", 7)));

        // A tombstone event with no payload attributes nothing.
        let empty = ConfigEvent {
            event_type: 2,
            config: None,
        };
        assert_eq!(config_event_cost(&empty), None);
    }

    fn cfg(name: &str, content: &str) -> Result<Config, Status> {
        Ok(Config {
            namespace: "ns".into(),
            name: name.into(),
            content_json: content.into(),
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn accounted_stream_records_each_delivered_item() {
        use futures_util::StreamExt;
        let ledger = Arc::new(TenantCacheLedger::new());
        let acct = CacheAccountant::new(
            Arc::clone(&ledger),
            "tenant".into(),
            QuotaMode::Permissive,
            0,
            "config",
        );
        let inner = futures_util::stream::iter(vec![cfg("a", "12345"), cfg("b", "67")]);
        let mut stream = AccountedStream::new(inner, Some(acct), config_cost);
        let mut delivered = 0;
        while let Some(item) = stream.next().await {
            assert!(item.is_ok());
            delivered += 1;
        }
        assert_eq!(delivered, 2);
        // a: 5 + 2(ns) + 1(name) = 8; b: 2 + 2 + 1 = 5 → 13.
        assert_eq!(ledger.bytes_of("tenant"), 13);
    }

    #[tokio::test]
    async fn accounted_stream_is_noop_when_disabled() {
        use futures_util::StreamExt;
        let ledger = Arc::new(TenantCacheLedger::new());
        let inner = futures_util::stream::iter(vec![cfg("a", "x")]);
        let mut stream: AccountedStream<_, Config> = AccountedStream::new(inner, None, config_cost);
        while stream.next().await.is_some() {}
        // No accountant ⇒ nothing recorded for any identity.
        assert_eq!(ledger.bytes_of("tenant"), 0);
    }
}
