#!/usr/bin/env bash

# SPDX-License-Identifier: Apache-2.0

# Re-vendors the upstream attribute registries this package is checked against.
#
#   ./internal/semconv/testdata/upstream/refresh.sh
#
# The registries are converted from YAML to JSON on the way in, so the Go side
# reads them with encoding/json. That is not incidental: the core module has no
# dependencies, and a Go YAML parser would appear in go.mod even as a test-only
# import and end that.
#
# genai-main is pinned to a commit rather than tracking the branch. The point of
# the check is to notice when upstream moves, which requires a fixed thing to
# compare against; a floating fetch would agree with upstream by construction
# and detect nothing.
set -euo pipefail

cd "$(dirname "$0")"

GENAI_REPO=open-telemetry/semantic-conventions-genai
GENAI_REF=${GENAI_REF:-94f432d7126f}          # override to re-pin
SEMCONV_REPO=open-telemetry/semantic-conventions
SEMCONV_REF=v1.41.0                            # frozen; never changes

fetch() {  # repo ref path outfile target
  local repo=$1 ref=$2 path=$3 out=$4 target=$5
  local url="https://raw.githubusercontent.com/$repo/$ref/$path"
  echo "fetching $target from $repo@$ref"
  curl -sfL "$url" -o "$out.yaml"
  SRC_URL="$url" SRC_REPO="$repo" SRC_REF="$ref" TARGET="$target" \
    uv run --quiet ./to_json.py "$out.yaml" "$out"
  rm -f "$out.yaml"
}

fetch "$GENAI_REPO"   "$GENAI_REF"   model/gen-ai/registry.yaml genai-main.registry.json genai-main
fetch "$SEMCONV_REPO" "$SEMCONV_REF" model/gen-ai/registry.yaml v1.41.0.registry.json    v1.41.0

echo
echo "vendored. current upstream head is:"
curl -sfL "https://api.github.com/repos/$GENAI_REPO/commits/main" |
  python3 -c 'import json,sys; d=json.load(sys.stdin); print("  ", d["sha"][:12], d["commit"]["committer"]["date"])'
echo "pinned at: $GENAI_REF"
