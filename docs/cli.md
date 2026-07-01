# konfig-cli reference

`konfig-cli` talks directly to the Kubernetes API — it does not require the
konfig server to be running.

## Install

Prebuilt tarballs are attached to each [GitHub release](https://github.com/jayakasadev/konfig/releases)
for `darwin-arm64`, `linux-amd64`, and `linux-arm64` (CU-86aj08vcu):

```bash
# pick your platform (darwin-arm64 / linux-amd64 / linux-arm64)
curl -sSL https://github.com/jayakasadev/konfig/releases/latest/download/konfig-cli-linux-amd64.tar.gz | tar -xz
sudo mv konfig-cli /usr/local/bin/
```

### Verify the download (checksum + cosign, keyless)

Every release ships a `SHA256SUMS` file and a `<tarball>.cosign.bundle` per
tarball. cosign verification needs **no key** — it checks the Sigstore/Fulcio
cert + Rekor log:

```bash
TAG=v0.1.0   # the release tag
base=https://github.com/jayakasadev/konfig/releases/download/$TAG
curl -sSLO "$base/konfig-cli-linux-amd64.tar.gz"
curl -sSLO "$base/konfig-cli-linux-amd64.tar.gz.cosign.bundle"
curl -sSLO "$base/SHA256SUMS"

sha256sum -c SHA256SUMS --ignore-missing

cosign verify-blob \
  --bundle konfig-cli-linux-amd64.tar.gz.cosign.bundle \
  --certificate-identity "https://github.com/CapitalIntent/konfig/.github/workflows/konfig-cli-release.yml@refs/tags/konfig-cli/$TAG" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  konfig-cli-linux-amd64.tar.gz
```

## Commands

### apply

Create or update a Config CRD. Enforces `schema_version` monotonicity.

```bash
konfig-cli apply default app-config config.yaml
```

`config.yaml`:
```yaml
schema_version: 3
content:
  rate_limit: 100
  feature_flags:
    dark_mode: true
```

### get

Print a Config CRD spec as YAML.

```bash
konfig-cli get default app-config
```

### get-secret

Print a managed Secret. Values are redacted by default.

```bash
konfig-cli get-secret production api-creds
# api_key: [REDACTED]

konfig-cli get-secret production api-creds --reveal
# api_key: sk-live-abc123
```

### apply-secret

Patch a managed Secret from a YAML file. Server base64-encodes values before patching.

```bash
konfig-cli apply-secret production api-creds creds.yaml
```

`creds.yaml`:
```yaml
schema_version: 2
api_key: sk-live-newkey
api_secret: newsecret
```

### import configmap

Onboard an existing ConfigMap as a Config CRD.

```bash
# Dry-run
konfig-cli import configmap default app-config --dry-run

# Apply
konfig-cli import configmap default app-config
```
