# Per-tenant egress NetworkPolicy overlay

Phase 8 multi-tenancy (CU-86aj8pvgx, MT-5). Generates an opt-in, default-deny
**egress** `NetworkPolicy` per tenant so a compromised tenant workload cannot
exfiltrate to arbitrary destinations — it may only reach DNS, the konfig gRPC
endpoint, and any per-tenant extras you allowlist.

konfig does **not** enforce network policy; a policy-enforcing CNI (Cilium,
Calico, …) does. This overlay only *emits* the manifests. See
`docs/multi-tenancy.md` → "Network isolation".

## Off by default

This directory is **not** referenced from `infra/konfig/kustomization.yaml`.
Nothing changes for clusters without a policy-enforcing CNI (where a
`NetworkPolicy` is silently inert) unless you apply it explicitly:

```sh
kubectl apply -k infra/konfig/overlays/tenants/
```

> NetworkPolicy is namespaced and additive. Once *any* egress policy selects a
> pod, that pod's egress is restricted to the union of all matching policies.
> Roll out per namespace in `permissive`-style stages (apply, watch CNI drop
> metrics, widen `extraEgress`) before relying on it.

## Files

- `registry.yaml` — **source of truth**: shared egress `defaults` (konfig
  namespace/port, cluster DNS) + the `tenants` list (namespace, pod selector,
  optional `extraEgress`).
- `generate.py` — renders one `networkpolicy-<tenant>.yaml` per tenant plus
  `kustomization.yaml` from `registry.yaml`.
- `networkpolicy-*.yaml`, `kustomization.yaml` — **generated, do not edit**.
  Committed so `kubectl apply -k` works without running Python, and so reviews
  see the concrete policy.

## Adding or changing a tenant

1. Edit `registry.yaml`.
2. Regenerate and commit the result:

```sh
python3 infra/konfig/overlays/tenants/generate.py
```

3. (CI / pre-commit) Verify the committed manifests match the registry:

```sh
python3 infra/konfig/overlays/tenants/generate.py --check   # exits 1 if stale
```

## Registry schema

```yaml
defaults:
  konfigNamespace: konfig-system   # where the konfig Service runs
  konfigPort: 50051                # gRPC port tenants must reach
  dns:                             # cluster DNS egress target
    namespace: kube-system
    selectorLabel: k8s-app
    selectorValue: kube-dns

tenants:
  - name: payments                 # NetworkPolicy is konfig-tenant-egress-<name>
    identity: spiffe://corp/payments  # cross-ref to TenantQuota/ConfigACL; doc-only
    namespace: payments            # namespace the policy lands in
    podSelector:                   # which pods it restricts (matchLabels)
      app.kubernetes.io/part-of: payments
    extraEgress:                   # optional allowlist beyond konfig + DNS
      - description: payments primary Postgres
        namespace: data            # OR: cidr: 10.0.0.0/24
        podSelector: { app: postgres }
        ports: [5432]              # TCP; omit for all ports
```

Every generated policy allowlists DNS (UDP/TCP 53) + konfig gRPC
(`konfigPort`) automatically; `extraEgress` is appended. `identity` ties the
network tenant back to its mTLS `ClientIdentity` (the same key as `TenantQuota`
and `ConfigACL`, per ADR-0002) for humans — the CNI itself selects on namespace
+ pod labels, never the cert.

## Verification (deferred to a live stack)

`kubectl kustomize infra/konfig/overlays/tenants/` validates the manifests at PR
time. Confirming the CNI actually drops disallowed egress requires a live
cluster with a policy-enforcing CNI — deferred to a live-stack session.
