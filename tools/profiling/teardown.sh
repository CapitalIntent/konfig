#!/usr/bin/env bash
#
# teardown.sh — clean up konfig profiling-session workloads.
#
# Default behavior:
#   Deletes the konfig deploy, the konfig-loadtest Job, and the alloy DaemonSet
#   + its RBAC and config. KEEPS the pyroscope deployment, service, configmap,
#   and the profiling namespace — those are expensive to bring back up and the
#   pyroscope datastore should outlive a single loadtest session.
#
# --all:
#   ALSO drops pyroscope (deploy + svc + configmap) and the profiling namespace.
#   Use when you want a fully clean cluster.
#
# --context <name>:
#   Pass through to every kubectl call. Useful when multiple kubeconfig contexts
#   are present (e.g. docker-desktop vs kind-konfig).
#
# Idempotent: every kubectl delete uses --ignore-not-found so re-running on an
# already-clean cluster is a no-op.
#
# Prerequisites: kubectl must be on PATH (this script intentionally does not
# bundle a kubectl — Bazel sandbox inherits PATH for sh_binary executions).
set -euo pipefail

ALL=0
CONTEXT=""

usage() {
    cat >&2 <<EOF
Usage: $0 [--all] [--context <kube-context>]

  --all              Also delete pyroscope (deploy/svc/cm) and the profiling namespace.
  --context <name>   Pass --context=<name> to every kubectl call.
  -h, --help         Show this help.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --all)
            ALL=1
            shift
            ;;
        --context)
            if [[ $# -lt 2 ]]; then
                echo "error: --context requires an argument" >&2
                exit 2
            fi
            CONTEXT="$2"
            shift 2
            ;;
        --context=*)
            CONTEXT="${1#--context=}"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "error: unknown argument: $1" >&2
            usage
            exit 2
            ;;
    esac
done

if ! command -v kubectl >/dev/null 2>&1; then
    echo "error: kubectl not found on PATH — install kubectl before running this teardown" >&2
    exit 127
fi

KUBECTL=(kubectl)
if [[ -n "$CONTEXT" ]]; then
    KUBECTL+=(--context "$CONTEXT")
fi

kdel() {
    # kdel <args-to-kubectl-delete>
    # Always idempotent via --ignore-not-found.
    "${KUBECTL[@]}" delete --ignore-not-found "$@"
}

echo ">>> tearing down konfig loadtest + alloy (keeping pyroscope)"

# konfig-system workloads.
kdel -n konfig-system job/konfig-loadtest
kdel -n konfig-system deploy/konfig

# alloy DaemonSet and its sidecar config + identity.
kdel -n profiling daemonset/alloy
kdel -n profiling configmap/alloy-config
kdel -n profiling serviceaccount/alloy

# alloy cluster-wide RBAC (cluster-scoped — no -n).
kdel clusterrolebinding/alloy
kdel clusterrole/alloy

if [[ "$ALL" -eq 1 ]]; then
    echo ">>> --all: also tearing down pyroscope + profiling namespace"
    kdel -n profiling deploy/pyroscope
    kdel -n profiling svc/pyroscope
    kdel -n profiling configmap/pyroscope-config
    # Namespace deletion cascades anything we missed inside profiling/.
    kdel namespace/profiling
fi

echo ">>> teardown complete"
