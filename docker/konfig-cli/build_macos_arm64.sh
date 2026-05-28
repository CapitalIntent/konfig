#!/usr/bin/env bash
# bazel run //docker/konfig-cli:build_macos_arm64  →  dist/konfig-cli-darwin-arm64
set -euo pipefail
OUTDIR="$BUILD_WORKSPACE_DIRECTORY/dist"
mkdir -p "$OUTDIR"
cd "$BUILD_WORKSPACE_DIRECTORY"
bazel build //tools/konfig-cli:konfig_cli --platforms=//bazel/platforms:aarch64_macos
BIN=$(bazel cquery //tools/konfig-cli:konfig_cli \
  --platforms=//bazel/platforms:aarch64_macos \
  --output=files 2>/dev/null | head -1)
cp "$BIN" "$OUTDIR/konfig-cli-darwin-arm64"
echo "Built: $OUTDIR/konfig-cli-darwin-arm64  ($(du -h "$OUTDIR/konfig-cli-darwin-arm64" | cut -f1))"
