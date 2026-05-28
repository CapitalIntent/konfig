#!/usr/bin/env bash
# bazel run //docker/konfig:push                   → linux/arm64 (Docker Desktop)
# bazel run //docker/konfig:push -- linux/amd64    → linux/amd64 (EKS)
set -euo pipefail
PLATFORM="${1:-linux/arm64}"
SHA=$(git -C "$BUILD_WORKSPACE_DIRECTORY" rev-parse HEAD)
exec docker buildx build \
  --platform "$PLATFORM" \
  -f "$BUILD_WORKSPACE_DIRECTORY/docker/konfig/Dockerfile" \
  --push \
  -t kasa288/konfig:latest \
  -t "kasa288/konfig:${SHA}" \
  "$BUILD_WORKSPACE_DIRECTORY"
