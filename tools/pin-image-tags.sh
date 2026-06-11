#!/usr/bin/env bash
#
# pin-image-tags.sh — rewrite konfig image tags in YAML manifests to a git SHA.
#
# Usage: tools/pin-image-tags.sh <short-sha>
#
# Updates every `kasa288/konfig*:...` reference in:
#   - infra/konfig/deployment.yaml
#   - infra/konfig-loadtest/job.yaml
#
# The CI workflow (.github/workflows/publish-images.yml) calls this after
# pushing the multi-arch images so the committed-back manifests pin to the
# same SHA that was just published as a Docker tag.
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 <short-sha>" >&2
    exit 1
fi

SHA="$1"

if [[ ! "$SHA" =~ ^[0-9a-f]{7,40}$ ]]; then
    echo "error: '$SHA' is not a hex git sha" >&2
    exit 1
fi

# `kasa288/konfig-cli:tag` and `kasa288/konfig-loadtest:tag` both start with
# `kasa288/konfig-` — the trailing word boundary keeps a single regex from
# matching past the repo segment.
sed_inplace() {
    if [[ "$(uname)" == "Darwin" ]]; then
        sed -i '' "$@"
    else
        sed -i "$@"
    fi
}

# Match `kasa288/konfig`, `kasa288/konfig-cli`, `kasa288/konfig-loadtest`
# followed by `:<tag>` and rewrite the tag to $SHA.
PATTERN='s|(kasa288/konfig[a-z-]*):[A-Za-z0-9._-]+|\1:'"$SHA"'|g'

sed_inplace -E "$PATTERN" infra/konfig/deployment.yaml
sed_inplace -E "$PATTERN" infra/konfig-loadtest/job.yaml

echo "Pinned image tags to $SHA in:"
echo "  infra/konfig/deployment.yaml"
echo "  infra/konfig-loadtest/job.yaml"
