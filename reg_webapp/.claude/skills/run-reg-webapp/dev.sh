#!/usr/bin/env bash
# Launch the reg_webapp dev servers on auto-selected FREE ports — works in any
# checkout (main or git worktree) with no port collisions. Picks a free backend
# port and frontend port, points the Vite /api proxy at the backend via
# REG_WEBAPP_BACKEND_URL, and starts both from THIS checkout's .venv (so a
# worktree serves its own code, not main's). Ctrl-C stops both.
#
# Ports are automatic: run two of these (two worktrees / lanes) and they won't
# collide. Pin them by exporting BACKEND_PORT / FRONTEND_PORT.
set -uo pipefail

root=$(git rev-parse --show-toplevel 2>/dev/null || echo "$PWD")
cd "$root" || exit 1

# Self-provision so this works standalone (humans, fresh clones) even without the
# worktree bootstrap hook. No-op once .venv / node_modules exist.
if [ -x ".claude/hooks/worktree_bootstrap.sh" ]; then
	.claude/hooks/worktree_bootstrap.sh </dev/null >/dev/null 2>&1 || true
fi

if [ ! -x ".venv/bin/uvicorn" ]; then
	echo "dev: .venv/bin/uvicorn missing — run 'uv sync --frozen' from $root." >&2
	exit 1
fi

freeport() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
backend_port=${BACKEND_PORT:-$(freeport)}
frontend_port=${FRONTEND_PORT:-$(freeport)}

pids=()
cleanup() {
	# Guard the expansion: `"${pids[@]}"` on an empty array under `set -u` errors
	# on bash 3.2 (macOS system bash) if a signal lands before the servers start.
	if [ ${#pids[@]} -gt 0 ]; then
		for p in "${pids[@]}"; do kill "$p" 2>/dev/null; done
	fi
}
trap cleanup INT TERM EXIT

# .venv/bin/uvicorn (not `uv run`) binds THIS checkout's venv directly.
.venv/bin/uvicorn reg_webapp.app:create_app --factory --port "$backend_port" &
pids+=($!)
(
	cd reg_webapp/frontend &&
		REG_WEBAPP_BACKEND_URL="http://localhost:$backend_port" \
			bun run dev -- --port "$frontend_port" --strictPort
) &
pids+=($!)

# Fail fast on startup: a server that never becomes reachable (busy port, missing
# DB, stale deps that break import) must make dev.sh exit NONZERO, not look like it
# succeeded. curl is the real check (immune to whether a dead child is reaped yet);
# bail early if either child has already exited.
ready=""
for _ in $(seq 1 30); do
	if curl -sf -o /dev/null "http://localhost:$backend_port/api/context" 2>/dev/null &&
		curl -sf -o /dev/null "http://localhost:$frontend_port/" 2>/dev/null; then
		ready=1
		break
	fi
	if ! kill -0 "${pids[0]}" 2>/dev/null || ! kill -0 "${pids[1]}" 2>/dev/null; then
		break # a server died during startup
	fi
	sleep 1
done
if [ -z "$ready" ]; then
	echo "dev: a server failed to start (busy port, missing DB, or stale deps?) — see output above." >&2
	exit 1 # the EXIT trap tears down whatever did come up
fi

printf 'reg_webapp dev (Ctrl-C to stop):\n  backend : http://localhost:%s\n  frontend: http://localhost:%s\n  driver  : REG_WEBAPP_DEV_URL=http://localhost:%s\n' \
	"$backend_port" "$frontend_port" "$frontend_port"

# Steady state: block until Ctrl-C (INT trap -> nonzero) or a server exits. A bare
# `wait` is fine here — startup already succeeded; a later single-server crash is a
# rare dev event you'll see and Ctrl-C. (`wait -n` would fail-fast but isn't in
# bash 3.2.)
wait
