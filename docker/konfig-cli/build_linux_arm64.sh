#!/usr/bin/env bash
# bazel run //docker/konfig-cli:build_linux_arm64  →  dist/konfig-cli-linux-arm64
set -euo pipefail
OUTDIR="$BUILD_WORKSPACE_DIRECTORY/dist"
mkdir -p "$OUTDIR"
docker buildx build \
  --platform linux/arm64 \
  --file "$BUILD_WORKSPACE_DIRECTORY/docker/konfig-cli/Dockerfile" \
  --target artifact \
  --output "type=local,dest=$OUTDIR/.konfig-cli-linux-arm64-tmp" \
  "$BUILD_WORKSPACE_DIRECTORY"
mv "$OUTDIR/.konfig-cli-linux-arm64-tmp/konfig-cli" "$OUTDIR/konfig-cli-linux-arm64"
rm -rf "$OUTDIR/.konfig-cli-linux-arm64-tmp"
echo "Built: $OUTDIR/konfig-cli-linux-arm64  ($(du -h "$OUTDIR/konfig-cli-linux-arm64" | cut -f1))"
