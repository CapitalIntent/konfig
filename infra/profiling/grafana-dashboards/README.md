# konfig Grafana dashboards

Phase 7 observability (CU-86aj08ud0). Trace-analysis dashboards for konfig,
backed by a **Tempo** datasource holding the OTLP spans exported by the
`tracing-opentelemetry` bridge (`service.name=konfig`; set
`OTEL_EXPORTER_OTLP_ENDPOINT` to enable export — see `docs/configuration.md`).

## Dashboards

- `konfig-traces.json` — Apply RPC duration p50/p95/p99, Subscribe fan-out
  latency (`konfig.broadcast_dispatch`), watcher reconnect timeline
  (`konfig.watch_event`), 409 retry counts (`konfig.apply_attempt`), and a
  top-N slowest `konfig.Subscribe` streams table. All TraceQL against Tempo.

The TraceQL metrics queries (`quantile_over_time`, `rate()`) require Tempo's
metrics-generator / TraceQL-metrics to be enabled. Without it the latency/rate
panels return no data; the top-N table works with the search API alone.

## Provisioning

### Option A — sidecar (kube-prometheus-stack / Grafana Helm)

The Grafana dashboard sidecar discovers ConfigMaps carrying the
`grafana_dashboard: "1"` label and loads the JSON they contain. Wrap the JSON
in a ConfigMap (kept out of `kustomization.yaml` because Grafana is not a konfig
dependency — apply on the cluster that runs Grafana):

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: konfig-traces-dashboard
  namespace: monitoring        # wherever your Grafana sidecar watches
  labels:
    grafana_dashboard: "1"     # sidecar discovery hint
data:
  konfig-traces.json: |
    # contents of konfig-traces.json
```

One-liner to build that ConfigMap from the committed JSON:

```sh
kubectl create configmap konfig-traces-dashboard \
  --namespace monitoring \
  --from-file=konfig-traces.json=infra/profiling/grafana-dashboards/konfig-traces.json \
  --dry-run=client -o yaml \
  | kubectl label --local -f - grafana_dashboard=1 -o yaml \
  | kubectl apply -f -
```

### Option B — file provisioning

Drop `konfig-traces.json` into Grafana's dashboards provider path
(`/var/lib/grafana/dashboards/...`) referenced by a provisioning provider.
On import, bind the `DS_TEMPO` input to your Tempo datasource.

## Verification (deferred to live stack)

Confirming each panel returns data requires a live Grafana + Tempo with konfig
spans flowing — deferred to a live-stack session. No cluster is attached to the
change that added these files. The JSON is schema-validated
(`schemaVersion: 39`, valid `panels` + `templating`) at PR time.

See also: `docs/slo.md` (SLI/SLO definitions) and `docs/runbook.md`
(Latency investigation).
