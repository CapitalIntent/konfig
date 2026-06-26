# ADR-0002: Tenant identity is the mTLS client certificate identity

Status: Accepted
Date: 2026-06-26
Tracker: CU-86ahrwd6n (Phase 8 milestone CU-86aj4chpd).
Design: [docs/multi-tenancy.md](../multi-tenancy.md).

## Context

konfig is moving toward multi-customer / external use, which requires a
first-class tenant concept for quotas and isolation. Before this ADR, konfig had
three partially-overlapping notions of "who":

- `(namespace, name)` — the resource key (what is stored), not a caller.
- the **mTLS client identity** — derived by `grpc::identity::ClientIdentity`
  (SAN URI → CN → `anonymous`) and already used as the authorization principal
  by the cluster-scoped `ConfigACL.konfig.io/v1` table (CU-86ahrwd6f, shipped).
- per-IP rate-limiting (Phase 4) — bound to network address, not caller.

A tenant model needs one canonical principal to hang quotas, cache budgets, and
network isolation on. Candidates considered: (a) K8s namespace, (b) a custom
label on the Config object, (c) the mTLS cert identity.

## Decision

**A tenant is the mTLS client identity** (`ClientIdentity.id`) — the same
principal the authz layer already enforces against.

Quotas (`TenantQuota` CRD), cache budgets, and optional NetworkPolicy overlays
are all keyed by this identity, mirroring how `ConfigACL` is keyed and watched
(`ArcSwap`, sync-gate). The tenant model adds a *how-much* axis orthogonal to
the existing *what-may-it-touch* (`ConfigACL`) axis.

## Alternatives rejected

- **Tenant = K8s namespace.** Namespaces scope resources, not callers. Multiple
  tenants legitimately read one namespace; one tenant may span namespaces. It
  cannot bound a caller.
- **Tenant = custom label on Config.** Set by whoever can write the object;
  spoofable by the writer and unrelated to the caller. Cannot bound consumption
  by the requesting client.

mTLS identity is the only caller-bound, server-verified attribute, it already
exists on every connection, and it is already the authz principal — so reusing
it keeps authn → authz → quota → isolation on a single principal.

## Consequences

Positive:

- One principal across the whole access-control stack; no new trust root.
- Quota/isolation tables reuse the proven `ConfigACL` watcher pattern
  (lock-free reads, sync-gate, `off`/`permissive`/`enforce` modes).
- KMS envelope encryption (CU-86ahrwd6m) can scope keys per tenant identity.

Negative / follow-ups:

- Anonymous (no-cert) callers have no tenant; they fall under global flag
  defaults and should be denied in `enforce` (already true for authz).
- Per-tenant cache accounting adds serve-time bookkeeping (no hot-read-path
  change). Tracked in the CU-86ahrwd6n follow-up tickets.
- Per-IP rate-limiting (Phase 4) is superseded on authenticated paths by the
  per-tenant token bucket; the per-IP limiter stays for pre-auth defense.
