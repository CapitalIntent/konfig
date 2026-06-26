# Multi-tenancy & tenant model (design)

Status: **Design** (CU-86ahrwd6n, Phase 8 milestone CU-86aj4chpd).
Date: 2026-06-26.
Decision record: [ADR-0002](adr/0002-multi-tenancy-tenant-model.md).

This is a design document. It defines konfig's tenant model, quota model, and
isolation guarantees, and enumerates the implementation tickets that follow. It
does **not** ship enforcement code.

## Problem

konfig has no first-class "tenant" concept. Today:

- `(namespace, name)` is the only resource key. It scopes *resources*, not
  *callers* — any number of distinct clients can watch the same namespace.
- Authorization (shipped, CU-86ahrwd6f) keys on the **mTLS cert identity**, but
  there is no notion of a tenant *budget* — an authorized identity can open
  unbounded subscribers / drive unbounded applies.
- Broadcast fan-out is per-namespace (`tokio::sync::broadcast` per namespace,
  with optional coalesce CU-86aj3vpgr + sharding CU-86aj3vpnh). A noisy caller
  in a shared namespace is a noisy neighbor to every other subscriber there.
- Rate-limiting (Phase 4) is **per-IP**, not per-tenant — a single tenant
  behind one IP, or many tenants behind a shared egress IP, are mis-accounted.

## What is a tenant?

**Decision: a tenant is the mTLS client identity** — the same principal the
authz layer already uses. Concretely, [`grpc::identity::ClientIdentity`] derives
it from the presented leaf certificate, in this locked order:

1. first **SAN URI** (e.g. a SPIFFE id `spiffe://trust-domain/workload`);
2. Subject **CN** when no SAN URI is present;
3. literal `anonymous` when no cert is presented / parsing fails.

Rationale (full reasoning in ADR-0002):

- **It already exists and is already authenticated.** mTLS is mandatory on the
  gRPC path; every accepted connection carries a verified identity. No new
  trust root, no new header to spoof.
- **It is already the authorization principal.** The cluster-scoped
  `ConfigACL.konfig.io/v1` table is keyed by this identity (`acl.rs`). Reusing
  it for quotas/isolation means one principal across authn → authz → quota →
  isolation, not three competing notions.
- **Namespace is the wrong axis.** A K8s namespace is a *resource* container;
  multiple tenants legitimately read the same namespace, and one tenant may
  span namespaces. Tenant ≠ namespace.
- **A custom label is weaker.** A label on the Config object is set by whoever
  can write the object; it cannot bound the *caller*. Identity is the only
  caller-bound, server-verified attribute.

Tenant → resource access stays exactly as authz already defines it: the
`ConfigACL` for an identity lists the `(namespace, name)` glob patterns and
verbs (`read`/`write`) it may touch. **The tenant model adds the orthogonal
axis — how much a tenant may consume — on top of the existing what-may-it-touch
axis.**

## Quota model

A per-tenant `TenantQuota` bounds consumption. Proposed shape (new
cluster-scoped CRD `TenantQuota.konfig.io/v1`, keyed by `identity` exactly like
`ConfigACL`, watched lock-free via `ArcSwap` mirroring `acl.rs`):

```yaml
apiVersion: konfig.io/v1
kind: TenantQuota
metadata:
  name: team-payments
spec:
  identity: "spiffe://corp/payments"   # matches ClientIdentity.id
  maxSubscribers: 200                  # concurrent Subscribe/SubscribeSecrets streams
  maxAppliesPerSecond: 50              # token-bucket refill rate for Apply
  maxAppliesBurst: 100                 # token-bucket capacity
  cacheMemoryBudgetBytes: 67108864     # 64 MiB soft cap on this tenant's cached payloads
```

Defaults (when no `TenantQuota` matches an identity) come from server flags so
the cluster has a global ceiling even for un-quota'd tenants:

| Flag | Default | Meaning |
|---|---|---|
| `--default-max-subscribers` | `0` (unlimited) | per-tenant concurrent streams |
| `--default-max-applies-per-second` | `0` (unlimited) | per-tenant apply rate |
| `--default-cache-budget-bytes` | `0` (unlimited) | per-tenant cache budget |

Enforcement points (all keyed by `ClientIdentity`):

- **Subscribers** — increment a per-identity gauge on stream open, decrement on
  close (RAII guard). Over budget ⇒ `RESOURCE_EXHAUSTED` at stream open.
- **Applies** — a per-identity token bucket on the `Apply` path. Empty bucket ⇒
  `RESOURCE_EXHAUSTED` (mirrors the existing backpressure drop semantics so
  clients already handle it).
- **Cache** — see below.

## Cache isolation

Today `ConfigCache` / `SecretCache` are global (`CowCache`, lock-free reads).
Payloads from all tenants share one map; there is no per-tenant accounting.

Design:

- **Per-tenant accounting, not per-tenant maps.** Keep the single lock-free
  cache (splitting it per tenant would multiply memory + watcher fan-out).
  Attribute each cached entry's byte cost to the tenant(s) authorized to read
  it, tracked in a per-identity `AtomicUsize` byte counter.
- **Soft budget + eviction on breach.** When a tenant's attributed bytes exceed
  `cacheMemoryBudgetBytes`, evict that tenant's least-recently-served entries
  from its *view* (the entry stays for other tenants) and emit a
  `konfig_tenant_cache_evictions_total{identity}` metric. The budget is a
  back-pressure signal, never a correctness gate — a cold read re-populates.
- **Shared-entry rule.** An entry readable by N tenants counts against each;
  eviction from one tenant's view does not drop it while another is in budget.

This keeps the hot read path unchanged (one `ArcSwap` load) and confines the new
work to write/serve-time accounting.

## Network isolation

Optional, opt-in per-tenant **egress** control via standard K8s
`NetworkPolicy`, generated from the tenant set:

- A per-tenant `NetworkPolicy` (label-selected) with an **egress allowlist** so
  a compromised tenant workload cannot exfiltrate to arbitrary destinations.
- konfig itself does not enforce network policy (the CNI does); konfig's role is
  to *emit* the policy manifests from the tenant registry as an optional overlay
  under `infra/konfig/overlays/tenants/`.
- Off by default — clusters without a policy-enforcing CNI are unaffected.

## Isolation guarantees (current vs proposed)

| Axis | Today | After this design |
|---|---|---|
| Authn | mTLS leaf cert (shipped) | unchanged |
| Authz (what) | `ConfigACL` identity → (ns,name) verbs (shipped) | unchanged |
| Quota (how much) | none | `TenantQuota` subscribers + apply rate (new) |
| Cache fairness | global, unaccounted | per-tenant soft budget + eviction (new) |
| Broadcast neighbor | per-namespace shard/coalesce (shipped) | unchanged; quota caps the amplifier |
| Network egress | none | optional per-tenant NetworkPolicy (new) |
| Rate-limit axis | per-IP (Phase 4) | per-tenant token bucket supersedes for authn'd paths |

## Rollout

Mirror the authz mode ladder (`off` → `permissive` → `enforce`) so quotas can be
observed before they bite:

1. **off** — no accounting (current behavior).
2. **permissive** — account + emit `konfig_tenant_*` metrics + log would-deny,
   but ALLOW. Lets operators size budgets against real traffic.
3. **enforce** — `RESOURCE_EXHAUSTED` on breach. Fail-safe: if the
   `TenantQuota` cache has not synced, fall back to the flag defaults (never
   deny on un-synced policy), mirroring the authz `UNAVAILABLE`-until-synced
   guard.

## Follow-up implementation tickets

Created under milestone CU-86aj4chpd (see ADR-0002 for the decision link):

1. **TenantQuota CRD + lock-free watcher** (CU-86aj8pvcu) — mirror `acl.rs`
   (`ArcSwap`, `Synced` gate); identity-keyed quota table + `mode` flag.
2. **Subscriber quota enforcement** (CU-86aj8pvdb) — per-identity stream gauge +
   RAII guard on Subscribe / SubscribeSecrets; `RESOURCE_EXHAUSTED` over budget.
3. **Apply rate-limit (per-tenant token bucket)** (CU-86aj8pvf1) — replace/augment
   the Phase 4 per-IP limiter on authenticated paths.
4. **Per-tenant cache budget + eviction** (CU-86aj8pvg3) — per-identity byte
   accounting + serve-time eviction + `konfig_tenant_cache_*` metrics.
5. **Per-tenant NetworkPolicy overlay** (CU-86aj8pvgx) — `infra/konfig/overlays/tenants/`
   generator + docs.
6. **Tenant metrics + dashboard** (CU-86aj8pvj7) — `konfig_tenant_subscribers`,
   `_applies_total`, `_cache_bytes`, `_evictions_total`, `_quota_denied_total`.

KMS envelope encryption for managed Secrets (CU-86ahrwd6m) is tracked
separately under the same milestone; the tenant model informs its key scoping
(key-per-tenant-identity) but it does not block this design.
