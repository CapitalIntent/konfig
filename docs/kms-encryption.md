# KMS envelope encryption for managed Secrets

Status: **Draft / proposed** — design only; no code yet.
Tracker: CU-86ahrwd6m (Phase 8 milestone CU-86aj4chpd).
Decision record: [ADR-0003](adr/0003-kms-envelope-encryption.md).
Related: [docs/configmaps-secrets.md](configmaps-secrets.md), [ADR-0002](adr/0002-multi-tenancy-tenant-model.md).

## Problem

konfig manages K8s-native `Secret` objects labeled `konfig.io/managed=true`
(see `configmaps-secrets.md`). Values are stored **base64 on the wire** — the
server never decodes them, and base64 is not encryption. The only confidentiality
control today is cluster-level etcd encryption-at-rest, which:

- is operator-configured and often absent on smaller clusters;
- protects the etcd blob but not `kubectl get secret -o yaml` (anyone with RBAC
  read on the Secret sees the value);
- gives no per-tenant key isolation and no audit of decrypts.

We want konfig to wrap managed Secret values with application-level envelope
encryption so the stored form is ciphertext, decryptable only by the konfig
server via a KMS-held master key.

## Goals

- New `ApplySecret` writes store **ciphertext** in etcd; `kubectl get secret -o
  yaml` shows the wrapped form.
- `GetSecret` / `GetAllSecrets` / `SubscribeSecrets` return **plaintext** to the
  authenticated, authorized client (unchanged client contract).
- Pluggable KMS provider: `aws`, `gcp`, `local` (file-based master key for
  dev/test).
- **Backward compatible**: existing unwrapped Secrets keep working; wrapping is
  opt-in and rolls out per the `off → wrap-new → enforce` ladder.
- Per-tenant key isolation hook, reusing the ADR-0002 tenant identity.

## Non-goals

- Encrypting Config CRDs / ConfigMaps (non-secret data). Out of scope.
- Replacing etcd encryption-at-rest (this is defense-in-depth on top of it).
- Client-side encryption (the server holds KMS decrypt rights; clients stay
  thin). A future SealedSecrets-style client-encrypt mode is a follow-up.
- Protecting against a fully compromised konfig server (it must hold decrypt
  rights to serve `GetSecret`). See Threat model.

## Threat model

Defends against:

- **etcd / backup disclosure** and **`kubectl get secret` disclosure** — the
  stored value is AES-256-GCM ciphertext; the data-encryption key (DEK) is itself
  wrapped by a KMS master key the reader cannot use.
- **Lateral RBAC** — a principal with `get secret` RBAC but no konfig
  `GetSecret` authz (ConfigACL) and no KMS access sees only ciphertext.

Does **not** defend against:

- A compromised konfig server process (holds KMS decrypt rights; mitigated by
  least-privilege KMS key policy + decrypt audit).
- A client authorized via mTLS + `ConfigACL` to call `GetSecret` (that is the
  intended consumer).

## Data model — the wrapped Secret

A managed Secret is wrapped **per value** with a per-secret DEK; the DEK is
wrapped by the KMS master key. Envelope metadata lives in annotations so the
`data` map stays a normal `string → bytes` shape and `kubectl get secret -o
yaml` round-trips.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: api-creds
  namespace: production
  labels:
    konfig.io/managed: "true"
  annotations:
    konfig.io/kms-scheme: "v1"                 # marker — absent ⇒ legacy plaintext
    konfig.io/kms-provider: "aws"
    konfig.io/kms-key-id: "arn:aws:kms:…:key/…" # master key (or per-tenant alias)
    konfig.io/kms-wrapped-dek: "<base64 wrapped DEK>"
    konfig.io/kms-enc-context: "tenant=spiffe://corp/payments;ns=production;name=api-creds"
type: Opaque
data:
  # base64( nonce(12B) || AES-256-GCM ciphertext+tag ), per key.
  api_key: "<base64 ciphertext>"
  api_secret: "<base64 ciphertext>"
```

- **DEK**: random 256-bit key, one per secret object, regenerated on every
  `ApplySecret` (so a value rotation re-keys the object).
- **Value encryption**: AES-256-GCM, fresh 96-bit nonce per value, with the
  encryption context string as AAD — binding each ciphertext to
  `{tenant, namespace, name}` so it cannot be transplanted to another object.
- **DEK wrapping**: provider-specific (KMS `Encrypt`/`GenerateDataKey` with the
  same encryption context as AAD).
- **Marker**: `konfig.io/kms-scheme: v1`. Absent ⇒ treat as today (plaintext
  base64). This is the backward-compat switch and the versioning seam.

## Wrap flow (write path)

Hook: `grpc::secret_apply::apply_secret_inner` (today it base64-encodes the
plaintext map into `BTreeMap<String, ByteString>` and server-side-applies a
`Secret` patch).

```
ApplySecret(plaintext_map)
  └─ if KMS enabled:
       dek            = random 32 bytes
       wrapped_dek    = provider.wrap(dek, enc_context)        // async, KMS
       data[k]        = base64(nonce || aesgcm_seal(dek, nonce, v, aad=enc_context))
       annotations   += {kms-scheme, kms-provider, kms-key-id, kms-wrapped-dek, kms-enc-context}
     else: today's base64 path (unchanged)
  └─ server-side apply the Secret patch (unchanged)
```

Only one KMS call per `ApplySecret` (wrap the single DEK); value encryption is
local AES-GCM.

## Unwrap flow (serve path)

Single chokepoint: `grpc::secret_get::secret_snapshot_to_proto` converts a
cached `SecretSnapshot` → `SecretResponse` and is shared by **all** serve paths
(`GetSecret`, `GetAllSecrets`, `SubscribeSecrets` replay + live). Decrypt here ⇒
every path is covered for free.

To keep the hot read path synchronous and KMS-free:

- The `SecretWatcher` ingests the wrapped Secret into the `SecretCache` holding
  **ciphertext** + parsed envelope (new `SecretSnapshot` fields: wrapped DEK,
  per-key nonces, provider/key id, enc context).
- On ingest (off the read path), the watcher unwraps the DEK once via the
  provider (async KMS `Decrypt`) and caches the **unwrapped DEK** in memory
  (keyed by namespace/name + wrapped-DEK hash). Plaintext **values** are never
  cached.
- `secret_snapshot_to_proto` stays synchronous: it AES-GCM-decrypts each value
  locally using the in-memory DEK and emits plaintext in the proto.

This bounds KMS QPS to secret-change events (not reads), keeps plaintext values
out of the cache, and leaves the existing sync serve/stream plumbing intact. A
bounded DEK cache with TTL handles restarts and master-key rotation.

(Alternative timings — unwrap-at-serve with a short DEK cache, or
decrypt-at-watch storing plaintext — are weighed in "Alternatives".)

## Provider abstraction

```rust
#[async_trait]
trait KmsProvider {
    async fn wrap(&self, dek: &[u8], ctx: &EncContext) -> Result<Vec<u8>>;
    async fn unwrap(&self, wrapped: &[u8], ctx: &EncContext) -> Result<Zeroizing<Vec<u8>>>;
}
```

- **aws** — AWS KMS `Encrypt`/`Decrypt` (or `GenerateDataKey`); `EncryptionContext`
  = the enc-context map; auth via IRSA. Key = `--secrets-kms-key-id` ARN.
- **gcp** — Cloud KMS `encrypt`/`decrypt`; AAD = enc-context; Workload Identity.
- **local** — master key from a file (mounted Secret/volume), AES-256-GCM
  wrap/unwrap. For dev/test and air-gapped clusters; satisfies the epic's
  `provider: local` acceptance criterion. Never for production multi-tenant.

DEKs and unwrapped material are held in `Zeroizing` buffers.

## Per-tenant key isolation (ADR-0002)

The tenant is the mTLS client identity (ADR-0002). Two levers, both optional:

1. **Encryption context binding** (default): the enc context includes
   `tenant=<identity>`. AWS/GCP enforce that decrypt presents the *same* context,
   so a DEK wrapped for tenant A cannot be unwrapped for tenant B even with the
   same master key.
2. **Key-per-tenant** (stronger): map identity → distinct KMS key ARN/alias via
   the tenant registry, so tenants are cryptographically separated at the master
   key. Compromise of one tenant's key never exposes another's.

## Configuration

konfig is configured by **CLI flags / Deployment args** (kustomize), not Helm —
mirroring `--secret-namespaces` / `--watch-configmaps`. New flags:

| Flag | Default | Meaning |
|---|---|---|
| `--secrets-kms-provider` | `off` | `off` \| `local` \| `aws` \| `gcp` |
| `--secrets-kms-mode` | `wrap-new` | `off` \| `wrap-new` \| `enforce` (see ladder) |
| `--secrets-kms-key-id` | — | master key ARN / resource name / local key path |

The epic's `secrets.kms.provider` / `secrets.kms.keyArn` "Helm values" map 1:1
onto these flags (settable via a kustomize patch or a Helm wrapper chart).

## Rollout ladder

Mirror the authz/quota `off → permissive → enforce` ladder so encryption can be
introduced without breaking existing secrets:

1. **off** — today's behavior; no wrapping, no decrypt attempts.
2. **wrap-new** (permissive) — `ApplySecret` wraps new writes; serve path
   decrypts wrapped secrets and **passes through** legacy unwrapped ones. The
   safe default once a provider is set.
3. **enforce** — refuse to serve or apply an unwrapped managed Secret
   (`FAILED_PRECONDITION`); requires all managed Secrets migrated. A one-shot
   migration job re-applies legacy secrets through the wrap path.

## Backward compatibility

A Secret without `konfig.io/kms-scheme` is served exactly as today (base64
passthrough). Wrapping is per-object and lazy: a secret becomes wrapped the next
time it is applied while a provider is configured. No flag day.

## Failure modes

- **KMS unreachable on serve** (DEK cache miss) — fail **closed**: return
  `UNAVAILABLE`; never serve ciphertext as if it were plaintext. The DEK cache
  keeps already-seen secrets serving through brief KMS blips.
- **KMS unreachable on apply** — `ApplySecret` returns `UNAVAILABLE`; nothing is
  written half-wrapped.
- **Bad/rotated master key** — unwrap fails for that object only; `UNAVAILABLE`
  + metric; other secrets unaffected.
- **Startup** — providers are validated (a wrap/unwrap self-test under `local`,
  a `DescribeKey` for cloud) before the server reports ready.

## Metrics (feeds MT-6 dashboards)

- `konfig_secret_kms_wrap_total{provider}` / `_unwrap_total{provider}`
- `konfig_secret_kms_errors_total{op,provider}` (op = wrap|unwrap)
- `konfig_secret_kms_dek_cache{result}` (hit|miss)
- `konfig_secret_kms_op_seconds` (KMS round-trip histogram)

## Proposed implementation tickets

1. **Envelope core + `local` provider** — `KmsProvider` trait, AES-256-GCM
   value crypto, DEK handling, `SecretSnapshot` envelope fields, marker
   annotation; wire wrap into `apply_secret_inner` and unwrap into
   `secret_snapshot_to_proto` (+ DEK cache); `wrap-new` mode + backward-compat
   passthrough.
2. **AWS KMS provider** (IRSA, encryption context).
3. **GCP Cloud KMS provider** (Workload Identity).
4. **`enforce` mode + migration job** to re-wrap legacy managed Secrets.
5. **Per-tenant key-per-identity** mapping + isolation tests.
6. **KMS metrics** + MT-6 dashboard panels + runbook entry.

## Alternatives considered

- **Native K8s KMS (`EncryptionConfiguration`)** — encrypts all etcd Secrets at
  the apiserver. Cluster-wide, operator-owned, no per-tenant keys, no konfig
  audit, and doesn't hide values from `kubectl get secret`. Complementary, not a
  substitute.
- **SealedSecrets / sops (client-side encrypt)** — strong (server never sees
  plaintext) but breaks the thin-client contract and the `GetSecret` plaintext
  API. Tracked as a possible future client-encrypt mode (non-goal here).
- **Decrypt-at-watch (store plaintext in cache)** — simplest serve path but
  keeps plaintext values in process memory and does KMS work on watch churn.
  Rejected in favor of ciphertext-in-cache + unwrapped-DEK (plaintext values
  never at rest in RAM).
- **Unwrap-at-serve with short DEK cache** — viable; would require making the
  serve/stream path async. Deferred in favor of unwrap-at-ingest keeping the hot
  read path synchronous; revisit if ingest-time KMS latency dominates.
