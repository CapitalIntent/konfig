# ADR-0001: Deploy konfig via raw YAML + Kustomize, not Helm

Status: Accepted
Date: 2026-06-11
Supersedes: nothing (no prior committed ADR — the "ADR-008" referenced in the
Phase 3 ClickUp ticket was never written to the repo).

## Context

Two parallel deployment paths existed:

- `chart/` — a Helm chart (added in commit `bddb2e8 feat: Helm chart + README (#1)`)
  with templates for Deployment, Service, RBAC, PDB, ConfigMap, ArgoCD
  Application, and a `crds/` directory.
- `infra/konfig/` — raw YAML manifests covering the same resources plus
  Namespace, NetworkPolicy, separate ClusterRole bindings for the ConfigMap
  watcher, and a per-namespace Role for Secret access.

Every recent change to the deployment topology (image pin to merged SHA in
`b9bcd80`, CPU limit bump in `7cb3157`, netprobe sidecar template in
`CU-86ahtj1p1`, secret RBAC in `908512f`) touched only `infra/konfig/`. The
chart silently drifted: `deployment.yaml`, `configmap.yaml`, `pdb.yaml`,
`service.yaml`, and `serviceaccount.yaml` diverged between the two paths.

README.md install instructions still pointed at `helm install ./chart`, which
shipped users a stale deployment topology with no NetworkPolicy and an
incomplete ConfigMap RBAC binding.

## Decision

`infra/konfig/` is the single source of truth. Install path is
`kubectl apply -k infra/konfig/`. The `chart/` directory is deleted.

Operators who need value-overlay-style customization use Kustomize patches in
a downstream overlay directory, not Helm values.

## Consequences

Positive:

- One topology to keep current. No drift between two parallel sources.
- Resources the chart lacked (Namespace, NetworkPolicy, role-secret, the
  ConfigMap ClusterRoleBinding) are now part of the standard install.
- ArgoCD wiring stays internal (operator-supplied Application pointing at
  `infra/konfig/`), not a chart concern.

Negative:

- External adopters lose the `helm install` ergonomic. They apply raw YAML
  via `kubectl apply -k` or compose a Kustomize overlay.
- No `--set image.tag=...` shorthand. Image tag changes go through commit +
  PR (consistent with `b9bcd80` SHA-pinning policy).

Open follow-ups (not blockers for this ADR):

- If demand for a chart re-emerges, the canonical path is: generate the chart
  programmatically from `infra/konfig/` (e.g. via a Bazel rule that converts
  manifests + a values schema into a versioned chart artifact). Hand-edited
  chart templates are not reintroduced.
