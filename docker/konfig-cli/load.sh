#!/usr/bin/env bash
set -euo pipefail
PLATFORM="${1:-linux/arm64}"
exec docker buildx build \
  --platform "$PLATFORM" \
  -f "$BUILD_WORKSPACE_DIRECTORY/docker/konfig-cli/Dockerfile" \
  --load \
  -t kasa288/konfig-cli:latest \
  "$BUILD_WORKSPACE_DIRECTORY"
