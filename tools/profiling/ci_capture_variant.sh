#!/usr/bin/env bash
# CU-86aj4z43b — capture one profiling variant on an already-running kind
# cluster (authoritative Linux path; no darwin->linux cross-compile).
#
# Deploys the `konfig-heapprof` image with a given coalesce/shards config,
# drives the konfig-loadtest subscribe scenario as the traffic generator, and
# captures -- from the konfig pod's own /metrics + /debug endpoints, which are
# the reliable signal on a distroless image:
#
#   * tokio runtime gauges (park / noop / polls / mean_polls_per_park),
#   * konfig_broadcast_lag_total      (subscriber-drop / backpressure count),
#   * latency histograms (apply->broadcast incl. coalesce window; fan-out hop),
#   * snmalloc heap pprof (startup + final),
#   * the loadtest results JSON (p99 per scenario) via a busybox sidecar.
#
# All artifacts land under OUTDIR/<variant>/ for tools/profiling/profiling_gate.py.
#
# Usage:
#   ci_capture_variant.sh <variant-name> <coalesce_window_ms> <broadcast_shards> \
#                         <soak_seconds> <outdir>
#
# Assumes: kind cluster up; CRDs/RBAC/Service/ConfigMap/seed-config already
# applied; images kasa288/konfig-heapprof:latest + kasa288/konfig-loadtest:latest
# already `kind load`ed. Idempotent across variants (tears its own deploy down).
set -uo pipefail

VARIANT="${1:?variant name}"
COALESCE_MS="${2:?coalesce_window_ms}"
SHARDS="${3:?broadcast_shards}"
SOAK="${4:?soak seconds}"
OUTROOT="${5:?outdir}"

NS=konfig-system
OUT="${OUTROOT}/${VARIANT}"
mkdir -p "$OUT"
SCRAPE_INTERVAL="${SCRAPE_INTERVAL:-15}"

echo "::group::[$VARIANT] deploy konfig-heapprof (coalesce=${COALESCE_MS}ms shards=${SHARDS})"
# 1 replica for per-pod profiling accuracy (feedback_loadtest_replicas).
# Append the variant's tuning flags; strip mTLS (no cert-manager in kind).
yq "
  .spec.replicas = 1
  | (.spec.template.spec.containers[] | select(.name == \"konfig\")).image = \"kasa288/konfig-heapprof:latest\"
  | (.spec.template.spec.containers[] | select(.name == \"konfig\")).imagePullPolicy = \"IfNotPresent\"
  | (.spec.template.spec.containers[] | select(.name == \"konfig\")).args |=
      ([.[] | select(test(\"^--tls\") | not)]
        + [\"--tls=false\", \"--coalesce-window-ms\", \"${COALESCE_MS}\", \"--broadcast-shards\", \"${SHARDS}\"])
  | del(.spec.template.spec.containers[] | select(.name == \"konfig\").volumeMounts[] | select(.name == \"konfig-tls\"))
  | del(.spec.template.spec.volumes[] | select(.name == \"konfig-tls\"))
" infra/konfig/deployment.yaml | kubectl apply -f -

kubectl rollout status -n "$NS" deploy/konfig --timeout=180s
kubectl wait --for=condition=Ready -n "$NS" pod -l app=konfig --timeout=180s
# Record the args the pod actually booted with (provenance for the artifact).
kubectl get deploy/konfig -n "$NS" \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="konfig")].args}' > "$OUT/konfig.args" 2>/dev/null || true
echo "::endgroup::"

echo "::group::[$VARIANT] port-forward + startup capture"
kubectl -n "$NS" port-forward svc/konfig 9090:9090 >"$OUT/portfwd.log" 2>&1 &
PF_PID=$!
for _ in $(seq 1 20); do
  curl -fsS -o /dev/null --max-time 2 http://localhost:9090/metrics && break
  sleep 0.5
done
curl -fsS -o "$OUT/startup.pb.gz" http://localhost:9090/debug/heap-profile.pprof \
  && echo "startup heap pprof: $(stat -c '%s' "$OUT/startup.pb.gz") bytes" \
  || echo "::warning::[$VARIANT] startup heap scrape failed"
curl -fsS http://localhost:9090/metrics > "$OUT/metrics.startup.txt" 2>/dev/null || true
echo "::endgroup::"

echo "::group::[$VARIANT] launch loadtest (subscribe, ${SOAK}s) + scrape metrics"
# emptyDir + busybox sidecar so the loadtest results JSON (p99) survives the
# main container exit and can be read with `kubectl exec` before teardown.
LT_POD="konfig-loadtest-${VARIANT}"
LT_IMG=kasa288/konfig-loadtest:latest
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: ${LT_POD}
  namespace: ${NS}
  labels: { app: konfig-loadtest-profiling }
spec:
  restartPolicy: Never
  volumes:
    - name: out
      emptyDir: {}
  containers:
    - name: loadtest
      image: ${LT_IMG}
      imagePullPolicy: IfNotPresent
      args: ["--addr","http://konfig.${NS}.svc.cluster.local:50051",
             "--namespace","${NS}","--config-name","konfig-loadtest",
             "--scenario","subscribe","--duration","${SOAK}"]
      env:
        - { name: KONFIG_LOADTEST_RESULTS_JSON, value: /out/results.json }
      volumeMounts:
        - { name: out, mountPath: /out }
    - name: sink
      image: busybox:1.36
      command: ["sh","-c","sleep 100000"]
      volumeMounts:
        - { name: out, mountPath: /out }
EOF

# Wait for the loadtest container to start driving traffic.
for _ in $(seq 1 60); do
  ph=$(kubectl get pod "$LT_POD" -n "$NS" -o jsonpath='{.status.phase}' 2>/dev/null || true)
  [ "$ph" = "Running" ] || [ "$ph" = "Succeeded" ] || [ "$ph" = "Failed" ] && break
  sleep 2
done

# Scrape /metrics every SCRAPE_INTERVAL until the loadtest container terminates
# (cap a bit beyond SOAK). Each snapshot is a full /metrics text exposition.
deadline=$(( $(date +%s) + SOAK + 60 ))
i=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  sleep "$SCRAPE_INTERVAL"
  i=$((i+1))
  curl -fsS http://localhost:9090/metrics > "$OUT/metrics.$(printf '%03d' "$i").txt" 2>/dev/null \
    || echo "::warning::[$VARIANT] metrics scrape $i failed"
  term=$(kubectl get pod "$LT_POD" -n "$NS" \
           -o jsonpath='{.status.containerStatuses[?(@.name=="loadtest")].state.terminated.reason}' 2>/dev/null || true)
  if [ -n "$term" ]; then
    echo "[$VARIANT] loadtest container terminated ($term) after ~$((i*SCRAPE_INTERVAL))s"
    break
  fi
done
echo "::endgroup::"

echo "::group::[$VARIANT] final capture + extract results"
curl -fsS http://localhost:9090/metrics > "$OUT/metrics.final.txt" 2>/dev/null || true
curl -fsS -o "$OUT/final.pb.gz" http://localhost:9090/debug/heap-profile.pprof \
  && echo "final heap pprof: $(stat -c '%s' "$OUT/final.pb.gz") bytes" \
  || echo "::warning::[$VARIANT] final heap scrape failed"
# Pull the results JSON (p99) out of the still-alive sidecar.
kubectl exec -n "$NS" "$LT_POD" -c sink -- cat /out/results.json > "$OUT/results.json" 2>/dev/null \
  && echo "[$VARIANT] results.json captured ($(stat -c '%s' "$OUT/results.json") bytes)" \
  || echo "::warning::[$VARIANT] results.json not available"
kubectl logs -n "$NS" "$LT_POD" -c loadtest --tail=-1 > "$OUT/loadtest.log" 2>&1 || true
POD=$(kubectl get pod -n "$NS" -l app=konfig -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
[ -n "$POD" ] && kubectl logs -n "$NS" "$POD" --tail=-1 > "$OUT/konfig.log" 2>&1 || true
echo "::endgroup::"

echo "::group::[$VARIANT] teardown (keep cluster)"
kill "$PF_PID" 2>/dev/null || true
kubectl delete pod "$LT_POD" -n "$NS" --wait=false >/dev/null 2>&1 || true
# Delete konfig deploy so the next variant starts from a clean rollout.
kubectl delete deploy/konfig -n "$NS" --wait=true --timeout=60s >/dev/null 2>&1 || true
echo "::endgroup::"

echo "[$VARIANT] capture complete -> $OUT"
