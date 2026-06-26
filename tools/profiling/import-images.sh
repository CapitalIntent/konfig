#!/usr/bin/env bash
#
# import-images.sh — import konfig bench images into Docker Desktop's k8s.io
# containerd namespace (CU-86aj7kawk).
#
# Docker Desktop's Kubernetes serves pods from containerd's `k8s.io` namespace,
# which is SEPARATE from the dockerd image store that `docker build` and the
# Bazel `:load_<arch>` targets populate. A freshly built `:latest` therefore
# stays invisible to k8s: pods with `imagePullPolicy: IfNotPresent` silently
# keep running the OLD image, quietly invalidating bench results. This script
# pushes the current dockerd images into the VM's k8s.io containerd namespace
# so the next pod roll picks them up.
#
# Mechanism: `docker save <img>` piped into the Docker Desktop LinuxKit VM (via
# nsenter on PID 1) and `ctr -n k8s.io images import -`. One VM backs every
# Docker Desktop node, so a single import covers all nodes.
#
# Usage:
#   import-images.sh [--build] [--arch arm64|amd64] [--dry-run] [IMAGE ...]
#
#   --build      Build + load each image into dockerd first via
#                `bazel run //docker/<name>:load_<arch>`.
#   --arch A     Arch for --build (default: arm64 — Apple-Silicon bench).
#   --dry-run    Print the docker/ctr commands without running them.
#   IMAGE ...    Images to import (default: the konfig bench set, :latest).
#
# Env:
#   NSENTER_IMAGE  nsenter helper image (default: justincormack/nsenter1).
#
# Default bench image set:
#   kasa288/konfig:latest kasa288/konfig-loadtest:latest kasa288/konfig-heapprof:latest
#
# Exit: 0 on success; 2 on usage error.
set -euo pipefail

ARCH=arm64
DO_BUILD=0
DRY_RUN=0
NSENTER_IMAGE="${NSENTER_IMAGE:-justincormack/nsenter1}"

DEFAULT_IMAGES="kasa288/konfig:latest kasa288/konfig-loadtest:latest kasa288/konfig-heapprof:latest"

usage() { sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-2}"; }

IMAGES=""
while [ $# -gt 0 ]; do
    case "$1" in
        --build)     DO_BUILD=1; shift ;;
        --arch)      ARCH="${2:?--arch needs a value}"; shift 2 ;;
        --arch=*)    ARCH="${1#*=}"; shift ;;
        --dry-run|-n) DRY_RUN=1; shift ;;
        -h|--help)   usage 0 ;;
        -*)          echo "import-images.sh: unknown flag '$1'" >&2; usage 2 ;;
        *)           IMAGES="$IMAGES $1"; shift ;;
    esac
done
[ -n "$IMAGES" ] || IMAGES="$DEFAULT_IMAGES"

run() { if [ "$DRY_RUN" = 1 ]; then echo "  + $*"; else "$@"; fi; }

command -v docker >/dev/null 2>&1 || { echo "import-images.sh: docker not on PATH" >&2; exit 2; }

# Soft guard: the k8s.io ns we import into is Docker Desktop's. Warn (don't fail)
# if the active kube-context isn't docker-desktop, so an `optum`/kind context
# doesn't make someone think the import landed where their pods run.
if command -v kubectl >/dev/null 2>&1; then
    ctx="$(kubectl config current-context 2>/dev/null || true)"
    if [ -n "$ctx" ] && [ "$ctx" != "docker-desktop" ]; then
        echo "::warning:: active kube-context is '$ctx', not 'docker-desktop' — this script imports into the Docker Desktop VM's containerd regardless" >&2
    fi
fi

if [ "$DO_BUILD" = 1 ]; then
    for img in $IMAGES; do
        name="${img##*/}"; name="${name%%:*}"
        target="//docker/${name}:load_${ARCH}"
        echo ">>> build+load $img  ($target)"
        run bazel run "$target"
    done
fi

nsenter_ctr() { run docker run --rm -i --privileged --pid=host "$NSENTER_IMAGE" ctr "$@"; }

for img in $IMAGES; do
    echo ">>> import $img -> k8s.io containerd ns"
    if [ "$DRY_RUN" = 1 ]; then
        echo "  + docker save '$img' | docker run --rm -i --privileged --pid=host '$NSENTER_IMAGE' ctr -n k8s.io images import -"
    else
        docker save "$img" | docker run --rm -i --privileged --pid=host "$NSENTER_IMAGE" ctr -n k8s.io images import -
    fi
done

echo ">>> verify (ctr -n k8s.io images ls):"
if [ "$DRY_RUN" = 1 ]; then
    echo "  + docker run --rm --privileged --pid=host '$NSENTER_IMAGE' ctr -n k8s.io images ls -q | grep -F <each image>"
else
    listed="$(docker run --rm --privileged --pid=host "$NSENTER_IMAGE" ctr -n k8s.io images ls -q 2>/dev/null || true)"
    for img in $IMAGES; do
        if printf '%s\n' "$listed" | grep -qF "${img%%:*}"; then
            echo "  ok   $img"
        else
            echo "  MISS $img (not found in k8s.io ns)" >&2
        fi
    done
fi
echo ">>> done. roll pods (kubectl rollout restart ...) to pick up the fresh image."
