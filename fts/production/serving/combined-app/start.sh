#!/bin/sh
set -eu

# The two routes share one Container App replica. Quickwit is reachable only
# on loopback; the release-aware router owns the app ingress on port 8080.
mkdir -p "${QW_DATA_DIR:-/tmp/qwdata}"
quickwit run --config /quickwit/serving-node.yaml &
quickwit_pid=$!
trap 'kill "$quickwit_pid" 2>/dev/null || true' EXIT INT TERM

exec /opt/serving-venv/bin/uvicorn \
    fts.production.serving.api.asgi:app \
    --host "${LISTEN_HOST:-0.0.0.0}" \
    --port "${PORT:-8080}" \
    --workers 1
