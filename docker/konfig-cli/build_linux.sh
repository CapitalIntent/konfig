#!/usr/bin/env bash
# bazel run //docker/konfig-cli:build_linux_amd64   → dist/konfig-cli-linux-amd64
# bazel run //docker/konfig-cli:build_linux_arm64   → dist/konfig-cli-linux-arm64
set -euo pipefail
PLATFORM="${1:?usage: build_linux.sh <linux/amd64|linux/arm64>}"
OUTNAME="${2:?usage: build_linux.sh <platform> <output-name>}"
OUTDIR="$BUILD_WORKSPACE_DIRECTORY/dist"
mkdir -p "$OUTDIR"
docker buildx build \
  --platform "$PLATFORM" \
  --file "$BUILD_WORKSPACE_DIRECTORY/docker/konfig-cli/Dockerfile" \
  --target artifact \
  --output "type=local,dest=$OUTDIR/.$OUTNAME-tmp" \
  "$BUILD_WORKSPACE_DIRECTORY"
mv "$OUTDIR/.$OUTNAME-tmp/konfig-cli" "$OUTDIR/$OUTNAME"
rm -rf "$OUTDIR/.$OUTNAME-tmp"
echo "Built: $OUTDIR/$OUTNAME  ($(du -h "$OUTDIR/$OUTNAME" | cut -f1))"
