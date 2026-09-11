#!/bin/sh
# Drive real traffic through the local stack, ending with a simulated served-model
# change; see scripts/dev-traffic.py.
#
# HONEYCOMB_CONFIG_KEY is passed by name only: docker copies it from this shell
# when it is set, and it never enters .dev/env, which the gateway container reads
# wholesale. controlplane/ is mounted so the script reuses
# controlplane.honeycomb.marker instead of a second copy of that call.
set -eu
cd "$(dirname "$0")/.."
exec docker run --rm -i \
  --network "container:switchboard-taskns-1" \
  --env-file .dev/env \
  -e HONEYCOMB_CONFIG_KEY \
  -e PYTHONPATH=/app \
  -v "$PWD/scripts:/app/scripts:ro" \
  -v "$PWD/controlplane:/app/controlplane:ro" \
  -w /app switchboard-dev:latest scripts/dev-traffic.py
