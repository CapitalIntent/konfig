# ADR-0003: KMS envelope encryption for managed Secrets

Status: Proposed
Date: 2026-06-29
Tracker: CU-86ahrwd6m (Phase 8 milestone CU-86aj4chpd).
Design: [docs/kms-encryption.md](../kms-encryption.md).
Related: [ADR-0002](0002-multi-tenancy-tenant-model.md).

## Context

konfig manages K8s-native `Secret` objects (`konfig.io/managed=true`) and stores
their values **base64-encoded on the wire** — base64 is encoding, not
encryption. The only confidentiality control is cluster-level etcd
encryption-at-rest, which is operator-dependent, frequently absent, does not
hide values from `kubectl get secret -o yaml`, and offers no per-tenant key
isolation or decrypt audit.

konfig is moving toward multi-customer use (ADR-0002), where storing secret
material as reversible base64 is unacceptable. We need application-level
confidentiality for managed Secret values that is decryptable only by the konfig
server, layered on top of (not replacing) etcd encryption.

## Decision

**Wrap managed Secret values with envelope encryption: a per-secret 256-bit DEK
encrypts the values (AES-256-GCM); a KMS-held master key wraps the DEK.** The
wrapped form (ciphertext values + envelope metadata in annotations) is what is
stored in etcd and shown by `kubectl get secret`. The server unwraps and serves
plaintext to authenticated, authorized clients; the client contract is unchanged.

- **Single write hook**: `grpc::secret_apply::apply_secret_inner` wraps before
  the server-side apply.
- **Single serve hook**: `grpc::secret_get::secret_snapshot_to_proto` (shared by
  `GetSecret` / `GetAllSecrets` / `SubscribeSecrets`) decrypts on the way out.
- **Pluggable providers**: `aws`, `gcp`, and `local` (file-based key for
  dev/test).
- **Backward compatible** via a `konfig.io/kms-scheme` marker annotation and an
  `off → wrap-new → enforce` rollout ladder (mirrors the authz/quota modes).
- **Per-tenant isolation** reuses the ADR-0002 mTLS identity: KMS encryption
  context binds each DEK/value to `{tenant, namespace, name}`, with an optional
  key-per-tenant ARN mapping for cryptographic separation.

To keep the hot read path synchronous and KMS-free, the DEK is unwrapped once at
watch-ingest and held (zeroizing) in memory; the cache stores ciphertext, never
plaintext values; serve-time decrypt is local AES-GCM.

## Alternatives rejected

- **Native K8s KMS (`EncryptionConfiguration`).** Apiserver-level, cluster-wide,
  operator-owned. No per-tenant keys, no konfig decrypt audit, and it does not
  hide values from `kubectl get secret`. Complementary, not a substitute.
- **SealedSecrets / sops (client-side encryption).** Server never sees
  plaintext — stronger — but breaks konfig's thin-client contract and the
  `GetSecret` plaintext API. Deferred as a possible future client-encrypt mode.
- **Decrypt-at-watch (plaintext in cache).** Simplest serve path but keeps
  plaintext secret values resident in process memory and spends KMS calls on
  watch churn. Rejected for ciphertext-in-cache + unwrapped-DEK-in-memory.

## Consequences

Positive:

- Stored secrets and `kubectl get secret` show ciphertext; disclosure of etcd or
  Secret-read RBAC no longer leaks values.
- Reuses one principal (ADR-0002 mTLS identity) across authn → authz → quota →
  isolation → **encryption**; per-tenant keying needs no new trust root.
- One write hook + one serve hook keep the blast radius small; all read paths
  are covered by the shared `secret_snapshot_to_proto` chokepoint.

Negative / follow-ups:

- The konfig server holds KMS decrypt rights — a server compromise can decrypt.
  Mitigated by least-privilege key policy + decrypt audit; not eliminated.
- KMS is a new runtime dependency on the secret apply + watch-ingest paths;
  fail-closed (`UNAVAILABLE`) on KMS outage, with a DEK cache to ride out blips.
- Unwrapped DEKs live in process memory (zeroizing, bounded). Weaker than
  client-side encryption but far stronger than today's base64.
- Master-key rotation re-wraps DEKs lazily on next apply; bulk re-wrap is an
  `enforce`-mode migration job. Tracked in the design's implementation tickets.
