#!/bin/sh
# reg_webapp container entrypoint — per-deploy smoke gate (REFACTOR_SPEC.md §6.5
# + "Remaining test coverage → Per-deploy smoke tests").
#
# Why an entrypoint gate, not a HEALTHCHECK: a Docker HEALTHCHECK only flips the
# container's health STATUS after start — it does not stop the container from
# accepting traffic in the meantime, and an orchestrator can route to it before
# the first probe. The spec requires the smoke FAILURE to HALT the container
# before serving. So we start uvicorn, probe it once over loopback, and only
# keep it running if the golden checks pass; on failure we kill it and exit
# non-zero. One process tree, stable exit codes, no extra deps.
#
# POSIX sh (the slim base ships no bash). `set -eu`: fail on error / unset var.
set -eu

HOST="${REG_WEBAPP_HOST:-0.0.0.0}"
PORT="${REG_WEBAPP_PORT:-8000}"
# Probe over loopback regardless of the bind host (0.0.0.0 isn't a connect addr).
SMOKE_URL="http://127.0.0.1:${PORT}"

# Start uvicorn in the background so the smoke gate can probe the real serving
# path (lifespan, baked-DB open, middleware) — not just an in-process app object.
uvicorn reg_webapp.app:create_app \
    --factory \
    --host "$HOST" \
    --port "$PORT" \
    --no-access-log &
SERVER_PID=$!

# If we exit before handing off (smoke failure / signal), don't leak uvicorn.
# Cleared once the gate passes and we transfer to the foreground wait.
# shellcheck disable=SC2329  # invoked indirectly via the EXIT/INT/TERM trap
cleanup() {
    if [ -n "${SERVER_PID:-}" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

# Run the golden gate: waits for readiness, checks /api/context, walks
# /api/catalog. --server-pid lets the readiness wait fail FAST if uvicorn aborts
# on a boot/lifespan failure (broken DB bake) instead of burning the full
# deadline. Non-zero return halts the container (the trap reaps uvicorn).
if ! python -m reg_webapp.smoke --base-url "$SMOKE_URL" --server-pid "$SERVER_PID"; then
    echo "entrypoint: smoke gate failed — refusing to serve traffic" >&2
    exit 1
fi

# Gate passed. Hand the foreground to uvicorn: clear the EXIT-kill trap, keep
# SIGINT/SIGTERM forwarding for graceful shutdown, then wait on the server so
# the container's lifetime tracks uvicorn's (and its exit code propagates).
trap - EXIT
trap 'kill -TERM "$SERVER_PID" 2>/dev/null || true' INT TERM
# A trapped signal makes `wait` return early (128+sig) while uvicorn is still
# draining — exiting then would kill PID 1 and SIGKILL uvicorn mid-shutdown.
# Re-wait until the server has actually exited, then propagate its real exit
# code. The if-form keeps `set -e` from aborting on the early-return status.
rc=0
while kill -0 "$SERVER_PID" 2>/dev/null; do
    if wait "$SERVER_PID"; then rc=0; else rc=$?; fi
done
exit "$rc"
