#!/usr/bin/env bash
set -euo pipefail
echo "STABLE_GIT_SHA $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
echo "STABLE_BUILD_TIMESTAMP $(date -u +%s)"
echo "STABLE_VERSION $(git describe --tags --always --dirty 2>/dev/null || echo dev)"
