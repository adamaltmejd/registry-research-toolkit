#!/bin/sh
# reg_webapp container entrypoint: `reg-meta serve` behind a per-deploy smoke gate
# (reg_webapp/DESIGN.md → Deployment).
#
# Why an entrypoint gate, not a HEALTHCHECK: a HEALTHCHECK only flips the container's
# health STATUS after start; it does not stop the container from accepting traffic
# in the meantime. A smoke FAILURE must HALT the container before it serves. So we
# start the server, probe it over loopback, and keep it running only if every probe
# passes; on failure we kill it and exit non-zero.
#
# POSIX sh (the slim base ships no bash). `set -eu`: fail on error / unset var.
set -eu

PORT=8000
BASE="http://127.0.0.1:${PORT}"
# Every probe carries the edge's cache-generation parameter, as proxied requests do,
# so a server that rejects it never serves.
EDGE_V="__edge_v=smoke"
READY_DEADLINE=60

# `/mcp` admits the public host's `Host` header only where the deployment names one
# (the global catalog; the SWECOV deployment serves no public MCP).
set -- serve --db /opt/reg_meta --catalog "$REG_META_CATALOG" \
    --stewards /opt/reg_webapp/stewards --host 0.0.0.0 --port "$PORT"
if [ -n "${REG_META_PUBLIC_HOST:-}" ]; then
    set -- "$@" --public-host "$REG_META_PUBLIC_HOST"
fi
reg-meta "$@" &
SERVER_PID=$!

# If we exit before handing off (smoke failure / signal), don't leak the server.
# shellcheck disable=SC2329  # invoked indirectly via the EXIT/INT/TERM trap
cleanup() {
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

fail() {
    echo "entrypoint: smoke gate failed ($1) — refusing to serve traffic" >&2
    exit 1
}

# Readiness: admission runs before the bind, so a refused artifact exits the server
# (its error document is on stderr) and the wait fails at once.
waited=0
until curl -fsS -o /dev/null "$BASE/api/context?$EDGE_V" 2>/dev/null; do
    kill -0 "$SERVER_PID" 2>/dev/null || fail "reg-meta exited"
    [ "$waited" -lt "$READY_DEADLINE" ] || fail "no answer within ${READY_DEADLINE}s"
    sleep 1
    waited=$((waited + 1))
done
curl -fsS -o /dev/null "$BASE/api/search?q=inkomst&$EDGE_V" || fail "search"
curl -fsS "$BASE/mcp?$EDGE_V" \
    -H 'accept: application/json, text/event-stream' \
    -H 'content-type: application/json' \
    -H 'mcp-protocol-version: 2025-11-25' \
    -d '{"jsonrpc": "2.0", "id": 1, "method": "tools/list"}' \
    | grep -q '"name":"search"' || fail "/mcp tools/list"

# Gate passed. Hand the foreground to the server: clear the EXIT-kill trap, forward
# SIGINT/SIGTERM, and wait until it has exited so its exit code propagates (a trapped
# signal makes `wait` return early while the server is still stopping).
trap - EXIT
trap 'kill -TERM "$SERVER_PID" 2>/dev/null || true' INT TERM
rc=0
while kill -0 "$SERVER_PID" 2>/dev/null; do
    if wait "$SERVER_PID"; then rc=0; else rc=$?; fi
done
exit "$rc"
