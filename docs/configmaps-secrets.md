# Using existing ConfigMaps and Secrets

konfig watches K8s-native ConfigMaps and Secrets alongside Config CRDs. This is
the zero-friction path for teams that already have config stored there.

## Opt-in label

Both ConfigMaps and Secrets require the label:

```
konfig.io/managed: "true"
```

konfig ignores any resource without it.

```bash
kubectl label configmap my-config konfig.io/managed=true -n default
kubectl label secret my-secret konfig.io/managed=true -n production
```

## ConfigMap data format

**Structured** — if `data["content"]` exists, it is parsed as JSON or YAML:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: app-config
  namespace: default
  labels:
    konfig.io/managed: "true"
  annotations:
    konfig.io/schema-version: "3"   # optional monotonicity
data:
  content: |
    { "rate_limit": 100, "feature_flags": { "dark_mode": true } }
```

**Flat** — if no `content` key, all key-value pairs become a flat JSON object.
The key `schema_version` is extracted separately:

```yaml
data:
  schema_version: "2"
  rate_limit: "100"
  dark_mode: "true"
```

## Secret format

Values are stored **base64-encoded on the wire** — the server never decodes them:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: api-creds
  namespace: production
  labels:
    konfig.io/managed: "true"
  annotations:
    konfig.io/schema-version: "1"
type: Opaque
stringData:
  api_key: "sk-live-abc123"
  api_secret: "supersecret"
```

Decode in your consumer:

```python
secret = client.get_secret("production", "api-creds")
# secret.data is dict[str, bytes] — already decoded
api_key = secret.data["api_key"].decode()
```

## Enable

ConfigMaps across all namespaces — add `--watch-configmaps` to the Deployment
args:

```bash
kubectl -n konfig-system patch deployment konfig --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--watch-configmaps"}]'
```

Secrets in specific namespaces — extend `--secret-namespaces` (index `4` is
the position in `infra/konfig/deployment.yaml`; verify with `kubectl -n
konfig-system get deploy konfig -o jsonpath='{.spec.template.spec.containers[0].args}'`):

```bash
kubectl -n konfig-system patch deployment konfig --type=json \
  -p='[{"op":"replace","path":"/spec/template/spec/containers/0/args/4","value":"--secret-namespaces=konfig-system,production,staging"}]'
```

For HA / production deploys, encode both as Kustomize patches in your overlay
instead of imperative `kubectl patch`.

> Config CRD and ConfigMap caches are unified. Secrets use a separate cache.
> Use `Get`/`GetAll` for configs and ConfigMaps; `GetSecret`/`GetAllSecrets` for Secrets.

## Migrate a ConfigMap to a Config CRD

Once you want schema_version enforcement and the Apply RPC write path:

```bash
# Import a ConfigMap as a Config CRD
konfig-cli import configmap --namespace default --name app-config

# Dry-run first
konfig-cli import configmap --namespace default --name app-config --dry-run
```

After importing, remove the `konfig.io/managed` label from the original
ConfigMap — the Config CRD becomes the source of truth.
