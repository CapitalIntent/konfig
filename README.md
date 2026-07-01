# konfig

Live config distribution for Kubernetes. Label your existing ConfigMaps and
Secrets — or apply a `Config.konfig.io/v1` resource — and any consumer pod
receives changes within milliseconds via a gRPC stream. No restarts required.

## Install

konfig requires [cert-manager](https://cert-manager.io/) to mint the gRPC
server cert and the consumer client certs (mTLS is on by default).

```bash
# 1. cert-manager (skip if already installed cluster-wide).
helm repo add jetstack https://charts.jetstack.io
helm repo update
helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager --create-namespace \
  --set crds.enabled=true
kubectl -n cert-manager wait --for=condition=Available deploy --all --timeout=120s

# 2. CRD first — the kube-rs watcher panics at startup if the CRD is not Established.
kubectl apply -f infra/konfig/crd.yaml
kubectl wait --for=condition=Established crd/configs.konfig.io --timeout=30s

# 3. Then the rest.
kubectl apply -k infra/konfig/
```

mTLS is enforced by default — see
[consumer-integration.md → mTLS client certs](docs/consumer-integration.md#mtls-client-certs)
for how to issue a client cert to your consumer pod. For local dev pass
`--tls=false` to the konfig binary (never in production).

Requires Kubernetes ≥ 1.29. See [Configuration](docs/configuration.md) for the
ConfigMap + Deployment args that drive runtime behaviour.

Deployment topology lives in `infra/konfig/`. Customize via a Kustomize
overlay in your own repo that references this directory as a base — do not
fork the manifests. See [ADR-0001](docs/adr/0001-deployment-raw-yaml.md) for
why there is no Helm chart.

## Quick start

### Option 1 — use your existing ConfigMap

Label it once:

```bash
kubectl label configmap my-app-config konfig.io/managed=true -n default
```

Enable ConfigMap watching by adding `--watch-configmaps` to the Deployment
args (default off):

```bash
kubectl -n konfig-system patch deployment konfig --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--watch-configmaps"}]'
```

Subscribe from your app:

```python
from konfig_client import KonfigClient

client = KonfigClient("konfig.konfig-system.svc.cluster.local:50051")
for event in client.subscribe("default", names=["my-app-config"]):
    config = event.config.content   # dict — live payload
```

### Option 2 — use your existing Secret

Label it and add the namespace to the Deployment's `--secret-namespaces` arg:

```bash
kubectl label secret my-app-secret konfig.io/managed=true -n production

kubectl -n konfig-system set env deployment/konfig \
  KONFIG_SECRET_NAMESPACES=konfig-system,production
```

Read from your app:

```python
secret = client.get_secret("production", "my-app-secret")
api_key = secret.data["api_key"]   # bytes — already base64-decoded
```

### Option 3 — native Config CRD (recommended for new config)

Full schema_version enforcement and the Apply RPC write path:

```bash
kubectl apply -f - <<EOF
apiVersion: konfig.io/v1
kind: Config
metadata:
  name: app-config
  namespace: default
spec:
  schema_version: 1
  content:
    feature_flags:
      dark_mode: true
    limits:
      max_connections: 100
EOF
```

```python
for event in client.subscribe("default", names=["app-config"]):
    flags = event.config.content["feature_flags"]
```

## Install konfig-cli

```bash
# macOS ARM64
curl -sSL https://github.com/jayakasadev/konfig/releases/latest/download/konfig-cli-darwin-arm64.tar.gz | tar -xz
sudo mv konfig-cli /usr/local/bin/

# Linux x86_64
curl -sSL https://github.com/jayakasadev/konfig/releases/latest/download/konfig-cli-linux-amd64.tar.gz | tar -xz
sudo mv konfig-cli /usr/local/bin/

# Linux ARM64
curl -sSL https://github.com/jayakasadev/konfig/releases/latest/download/konfig-cli-linux-arm64.tar.gz | tar -xz
sudo mv konfig-cli /usr/local/bin/
```

Verify downloads with the release `SHA256SUMS` + `cosign verify-blob` — see [docs/cli.md](docs/cli.md#verify-the-download-checksum--cosign-keyless).

`konfig-cli` talks directly to the Kubernetes API — it works even when the
konfig server is down.

## Docs

- [Architecture](docs/architecture.md) — how it works, consistency model, design decisions
- [Configuration](docs/configuration.md) — ConfigMap keys, Deployment args, runtime overrides
- [Consumer integration](docs/consumer-integration.md) — gRPC client usage, error handling, reconnect
- [Existing ConfigMaps and Secrets](docs/configmaps-secrets.md) — opt-in label, data formats, migration
- [Secret encryption (KMS envelope)](docs/kms-encryption.md) — **proposed**: envelope-encrypt managed Secret values, decryptable only by the server ([ADR-0003](docs/adr/0003-kms-envelope-encryption.md))
- [konfig-cli reference](docs/cli.md) — all commands and flags
- [Runbook](docs/runbook.md) — health checks, metrics, partition recovery, upgrading
- [mTLS rollout + cert rotation](docs/mtls-rollout.md) — dual-listener rollout, zero-downtime cert rotation, expiry alerting
- [ADR-0001](docs/adr/0001-deployment-raw-yaml.md) — why raw YAML, not Helm

## Development

```bash
# Generate rust-project.json for IDE (rust-analyzer):
bazel run @rules_rust//tools/rust_analyzer:gen_rust_project

# Build rustdoc:
bazel build //rust/konfig:doc

# Run doc tests:
bazel test //rust/konfig:doc_test
```

## License

See [LICENSE](LICENSE).
