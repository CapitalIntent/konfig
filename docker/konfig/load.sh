#!/usr/bin/env bash
# bazel run //docker/konfig:load                   → linux/arm64 (Docker Desktop)
# bazel run //docker/konfig:load -- linux/amd64    → linux/amd64
set -euo pipefail
PLATFORM="${1:-linux/arm64}"
exec docker buildx build \
  --platform "$PLATFORM" \
  -f "$BUILD_WORKSPACE_DIRECTORY/docker/konfig/Dockerfile" \
  --load \
  -t kasa288/konfig:latest \
  "$BUILD_WORKSPACE_DIRECTORY"
