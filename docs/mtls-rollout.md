# mTLS rollout plan + cert rotation runbook

Phase 8 Security. mTLS itself already shipped (cert-manager + tonic `ServerTlsConfig`,
ticket 86ahrg6zh) — this document is the **operational** layer on top of it:
how to turn mTLS on without breaking existing plaintext consumers, and how to
keep certs rotating with zero downtime.

- Server TLS implementation: [`rust/konfig/grpc/tls.rs`](../rust/konfig/grpc/tls.rs)
  (`build_server_tls_config`, `TlsPaths`), wired via `ServerConfig.tls_config`
  in [`rust/konfig/grpc/mod.rs`](../rust/konfig/grpc/mod.rs).
- CLI / arg surface: `--tls`, `--tls-cert`, `--tls-key`, `--tls-client-ca`
  (defined in [`rust/konfig/startup.rs`](../rust/konfig/startup.rs) `Args`;
  see `infra/konfig/deployment.yaml`).
- Client-cert distribution (the consumer side) is documented in
  [consumer-integration.md → mTLS client certs](consumer-integration.md#mtls-client-certs)
  and is not repeated here.
- Day-to-day cert ops also live in [runbook.md → TLS / cert rotation](runbook.md#tls--cert-rotation);
  this file is the deeper rollout + zero-downtime-rotation reference it links to.

---

## 0. Why a rollout phase is needed at all

The konfig binary binds **one** gRPC listener (`--grpc-addr`, a single
`SocketAddr`, default `0.0.0.0:50051`). TLS is all-or-nothing on that listener:
`resolve_tls_config` in `startup.rs` either returns a `ServerTlsConfig`
(mTLS required for every caller) or `None` (`--tls=false`, plaintext). There is
**no in-process second listener** and adding one is out of scope here (it would
be a Rust change).

Consequence: you cannot flip a single Deployment from plaintext to mTLS without
a hard cutover — the instant the server requires client certs, every consumer
that has not yet been issued one starts failing the TLS handshake
(`UNAVAILABLE` / transport error). The dual-listener phase below avoids that by
running **two konfig Deployments** behind **two Services** during migration,
not by changing the binary.

> If you are standing up konfig **greenfield** (no existing plaintext
> consumers), skip Section 1 entirely — deploy straight to the mTLS-only
> manifests in `infra/konfig/` (which already set `--tls=true`) and onboard
> consumers with client certs from day one.

---

## 1. Dual-listener rollout phase (brownfield: live plaintext consumers)

Goal: introduce mTLS with **no consumer downtime**, migrate consumers one at a
time, then drop plaintext.

### Topology during migration

Because one binary = one TLS mode, run two Deployments off the same image,
both watching the same Config/Secret resources (they are stateless readers of
the kube API — running two is safe; the cache is per-pod):

| Deployment        | args                                  | Service           | port  | who connects                  |
| ----------------- | ------------------------------------- | ----------------- | ----- | ----------------------------- |
| `konfig` (mTLS)   | `--tls=true --tls-cert/-key/-client-ca` | `konfig`          | 50051 | migrated consumers (have certs) |
| `konfig-plain`    | `--tls=false`                         | `konfig-plain`    | 50051 | not-yet-migrated consumers      |

`konfig-plain` reuses the existing deployment manifest with two changes:
`metadata.name: konfig-plain`, and the TLS args replaced by `--tls=false`
(drop the `konfig-tls` volume + mount — plaintext needs no cert material). Its
`--tls=false` boot emits the `WARN: TLS disabled; gRPC server is
unauthenticated` line (`warn_tls_disabled` in `tls.rs`) — expected during
migration, alert on it **after** Phase 3 completes.

```
                      ┌─────────────────────────┐
  migrated consumer ──┤ Service/konfig  :50051   ├── Deployment/konfig  (--tls=true)
   (client cert)      └─────────────────────────┘
                      ┌─────────────────────────┐
  legacy consumer  ───┤ Service/konfig-plain     ├── Deployment/konfig-plain (--tls=false)
   (plaintext)        └─────────────────────────┘
```

### Phase 1 — stand up the mTLS listener alongside plaintext

Pre-req: the PKI is installed (`infra/konfig/issuer.yaml` +
`infra/konfig/certificate.yaml`; see the bootstrap chain documented in
`issuer.yaml`). The server cert Secret `konfig-server-tls` must exist before the
mTLS pod starts, or it fails fast with `failed to read server cert at
/var/run/konfig-tls/tls.crt`.

1. Deploy the mTLS `konfig` Deployment + `Service/konfig` (the manifests in
   `infra/konfig/` as-is — they already require mTLS).
2. Keep the existing plaintext workload running, renamed to `konfig-plain`
   behind `Service/konfig-plain`. Existing consumers still dial
   `konfig.konfig-system.svc:50051` → point that name at `konfig-plain` during
   migration (or have legacy consumers target `konfig-plain` explicitly).
3. Smoke-test the mTLS endpoint with a client cert before migrating anyone:

   ```bash
   # From a pod that has a client cert mounted at /var/run/konfig-client-tls
   grpcurl \
     -cacert /var/run/konfig-client-tls/ca.crt \
     -cert   /var/run/konfig-client-tls/tls.crt \
     -key    /var/run/konfig-client-tls/tls.key \
     konfig.konfig-system.svc.cluster.local:50051 list
   ```

### Phase 2 — migrate consumers

For each consumer, in order, with no shared maintenance window:

1. Issue it a client `Certificate` and mount the Secret — full procedure in
   [consumer-integration.md → mTLS client certs](consumer-integration.md#mtls-client-certs)
   (steps 1–3: issue cert, mount, wire TLS into the gRPC client).
2. Repoint that consumer's konfig endpoint from `konfig-plain` to `konfig`
   (the mTLS Service). The client cert + `ca.crt` trust anchor make the
   handshake succeed.
3. Verify the consumer is talking to the mTLS listener:
   - consumer-side: no transport errors, reads succeed.
   - server-side: the mTLS pod logs `mTLS configured: client auth required`
     at boot; the consumer's CN appears in request logs (CN is set per
     ServiceAccount per the consumer-integration guide).
4. Move to the next consumer. A failed migration affects **only** that one
   consumer — roll it back to `konfig-plain` (Section "Rollback") without
   touching anyone else.

Track migration with a checklist of consumer → {issued cert, repointed,
verified}. Do not proceed to Phase 3 until every known consumer is on the mTLS
Service and `konfig-plain` shows zero traffic.

### Phase 3 — drop plaintext

1. Confirm `konfig-plain` is idle. Use connection metrics / access logs, or
   temporarily scale `konfig-plain` to 0 replicas and watch for consumer
   errors for one full traffic cycle (e.g. 24h) before deleting.
2. Delete `Deployment/konfig-plain` + `Service/konfig-plain`.
3. Now enable alerting on the `TLS disabled; gRPC server is unauthenticated`
   log line — after cutover it should never appear in `konfig-system` again.
4. (Optional, defense-in-depth) the `NetworkPolicy` in `infra/konfig/` already
   restricts ingress to :50051; with plaintext gone, mTLS is the only path in.

End state == the shipped `infra/konfig/` manifests: a single mTLS-only
`konfig` Deployment. The dual-listener scaffolding (`konfig-plain`) exists
**only** during migration and leaves no permanent footprint.

---

## 2. Cert rotation (zero-downtime)

cert-manager owns issuance and renewal. The knobs that matter:

### Leaf (server) cert — `infra/konfig/certificate.yaml`

```yaml
duration: 2160h    # 90d  — leaf cert lifetime
renewBefore: 720h  # 30d  — cert-manager starts renewal 30d before expiry
privateKey:
  rotationPolicy: Always   # new key on every renewal (not just new cert)
```

- cert-manager re-issues into the **same** Secret `konfig-server-tls`
  (`tls.crt` / `tls.key` / `ca.crt`) ~30d before expiry.
- The Secret is projected into the pod as the `konfig-tls` volume with
  `defaultMode: 0o400` (owner read-only; the non-root `runAsUser: 65532`
  process can read its private key, no other user can). See
  `infra/konfig/deployment.yaml`.

### How rotation propagates to a running pod

This is the load-bearing fact:

> **The konfig server reads `tls.crt` / `tls.key` / `ca.crt` once at startup
> and does NOT hot-reload.** (`tls.rs`: "Files are read once at startup …
> a pod restart picks up the new material. There is no hot reload here.")

cert-manager rewriting the Secret updates the projected files on disk, but the
already-running process keeps serving the **old** cert until it restarts. The
kubelet refreshes a mounted Secret within ~1 min, but konfig will not notice.

So rotation is "Secret updates silently" + "pod must restart to adopt it". The
old cert keeps working until its real expiry, so the 30d `renewBefore` window
is the slack you have to schedule a restart — there is no instant outage when
the Secret rotates.

### Zero-downtime rotation steps

The Deployment uses a `RollingUpdate` strategy with `maxUnavailable: 0,
maxSurge: 1`, so a restart never drops below the desired replica count.

```bash
# 1. (Usually automatic) cert-manager has already re-issued the Secret.
#    Confirm the Secret's cert is the new one:
kubectl -n konfig-system get secret konfig-server-tls \
  -o jsonpath='{.data.tls\.crt}' | base64 -d | \
  openssl x509 -noout -enddate

# 2. Roll the Deployment so every pod reloads the new material.
kubectl -n konfig-system rollout restart deployment/konfig
kubectl -n konfig-system rollout status  deployment/konfig --timeout=120s
```

With `maxUnavailable: 0` the new (rotated-cert) pod becomes Ready before the
old one is torn down, so consumers see continuous service. Readiness gates on
cache-populated (see deployment.yaml readinessProbe), so a rolled pod is only
sent traffic once it has both loaded the new cert AND warmed its cache.

**Automate the restart** so it isn't a manual chore on a 90d cadence — annotate
the Deployment with [stakater/Reloader](https://github.com/stakater/Reloader)
(if installed cluster-wide):

```yaml
metadata:
  annotations:
    reloader.stakater.com/auto: "true"   # roll the Deployment when its mounted Secret changes
```

Reloader watches `konfig-server-tls` and triggers the same rolling restart
automatically the moment cert-manager rewrites it. Not enabled by default; see
runbook.md → "Pod restart on cert renewal".

### Client-cert rotation (consumer side)

Consumer client certs use the same `duration: 2160h / renewBefore: 720h`
pattern (consumer-integration.md). They rotate independently and on the same
"Secret updates → restart to adopt" model. Because both server and clients
chain to the **same CA** (`Issuer/konfig-ca-issuer`), a leaf rotation on
either side does not require the other to roll — only a **CA** rotation does
(next section).

---

## 3. Runbook

### Cert-expiry alert

Alert before `renewBefore` would even fire, so a stuck cert-manager surfaces
with slack to spare. Two complementary signals:

- **cert-manager native**: the `certmanager_certificate_expiration_timestamp_seconds`
  metric (cert-manager's own exporter). Alert when
  `(expiry - now) < 14d` for `name="konfig-server-tls"` — that is half the
  30d `renewBefore`, so it only fires if renewal is actually failing.
- **Blackbox / from-the-outside**: probe the live endpoint's cert expiry so an
  alert fires even if cert-manager's metrics are themselves down.

Manual expiry check (also in runbook.md → "Verify cert expiry"):

```bash
kubectl -n konfig-system get certificate konfig-server-tls \
  -o jsonpath='{.status.notAfter}'

kubectl -n konfig-system get secret konfig-server-tls \
  -o jsonpath='{.data.tls\.crt}' | base64 -d | \
  openssl x509 -noout -enddate -subject -issuer
```

Watch in-flight renewals:

```bash
kubectl -n konfig-system get certificaterequests
kubectl -n konfig-system describe certificate konfig-server-tls
```

### Manual rotation procedure (forced leaf rotation)

When you must rotate a leaf cert **now** (e.g. suspected key compromise) rather
than wait for `renewBefore`:

```bash
# 1. Force cert-manager to re-issue immediately (cmctl, if installed):
cmctl renew konfig-server-tls -n konfig-system
#    …or trigger by deleting the Secret; cert-manager re-reconciles it:
kubectl -n konfig-system delete secret konfig-server-tls

# 2. Wait for the new Secret, then roll the pod to adopt it:
kubectl -n konfig-system wait --for=condition=Ready \
  certificate/konfig-server-tls --timeout=60s
kubectl -n konfig-system rollout restart deployment/konfig
kubectl -n konfig-system rollout status  deployment/konfig --timeout=120s
```

### CA rotation (rare; touches everyone)

The root CA lives 10y (`infra/konfig/issuer.yaml`, `duration: 87600h`,
`renewBefore: 8760h`). Rotating it re-anchors the whole mesh, so **every**
server and consumer pod must roll once their leaf is re-issued by the new CA.
Full procedure: runbook.md → "Rotate the root CA". In production, replace the
bootstrap self-signed root by populating `Secret/konfig-ca-key-pair` from your
org PKI (Vault / AWS PCA / step-ca) and leave `Issuer/konfig-ca-issuer` as-is.

### Rollback

- **Failed leaf rotation** (new cert broken / wrong SANs): the old pod is still
  serving the previous valid cert until its real expiry. Do **not** force-delete
  the running pod. Fix the `Certificate` spec, let cert-manager re-issue, then
  roll. If you already rolled onto a bad cert, `kubectl rollout undo
  deployment/konfig` returns to the previous ReplicaSet (old cert), buying time.
- **Failed mTLS migration of one consumer** (Phase 2): repoint that consumer
  back to `Service/konfig-plain` and it is plaintext again immediately — no
  effect on already-migrated consumers.
- **Need to abandon the whole mTLS cutover**: keep `konfig-plain` running, point
  all consumers back at it. Never set `--tls=false` on the production `konfig`
  Deployment as a "rollback" — that silently downgrades every migrated consumer
  to unauthenticated; use the separate `konfig-plain` Deployment instead.

### cert-manager unreachable

If cert-manager is down, existing pods keep running on the cert they loaded at
startup — they do **not** lose mTLS. The only risk is "cert expires while
cert-manager is down", which the 30d `renewBefore` + the expiry alert above are
designed to make impossible in practice. Monitor cert-manager liveness
separately. (runbook.md → "cert-manager unreachable".)

### Client-cert distribution

Issuing, mounting, and wiring client certs into Python (`grpcio`) and Rust
(`tonic`) consumers — including the `ca.crt` trust anchor and the SAN/CN
override needed when the target host differs from the cert CN — is fully
covered in
[consumer-integration.md → mTLS client certs](consumer-integration.md#mtls-client-certs).
That is the single source of truth for the consumer side; this runbook only
covers the server + rotation lifecycle.
