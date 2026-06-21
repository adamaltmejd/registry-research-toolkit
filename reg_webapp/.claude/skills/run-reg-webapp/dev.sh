#!/usr/bin/env bash
# Run the reg_webapp dev servers on auto-selected FREE ports — works in any
# checkout (main or git worktree) with no port collisions. Picks a free backend
# and frontend port, points the Vite /api proxy at the backend via
# REG_WEBAPP_BACKEND_URL, and starts both from THIS checkout's .venv (so a
# worktree serves its own code, not main's).
#
# Modes:
#   dev.sh                 interactive — start both servers and block (Ctrl-C stops).
#   dev.sh smoke           ONE-SHOT — start, run the Playwright driver `smoke`, tear
#                          down, exit with the driver's status. For agent visual
#                          verification: random ports + guaranteed cleanup, so it
#                          never collides and never leaks a server.
#   dev.sh shot <route>... ONE-SHOT — screenshot each route, tear down, exit.
#
# Ports are automatic (two of these never collide); pin with BACKEND_PORT /
# FRONTEND_PORT if you need to know them up front.
set -uo pipefail
# Job control so each background server runs in its OWN process group — lets
# cleanup() kill the WHOLE tree (the frontend subshell AND the bun→vite grandchild
# it spawns), not just the direct child. Without this, vite leaks on teardown.
set -m

mode=serve
case "${1:-}" in
smoke)
	mode=smoke
	shift
	;;
shot)
	mode=shot
	shift
	;;
"") mode=serve ;;
*)
	echo "usage: dev.sh [smoke | shot <route>...]" >&2
	exit 2
	;;
esac

if [ "$mode" = shot ] && [ "$#" -eq 0 ]; then
	echo "dev: 'shot' needs at least one route, e.g. dev.sh shot /catalog/scb/lisa" >&2
	exit 2 # validate before booting servers
fi

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
	# `kill -- -PID` signals the job's whole PROCESS GROUP (set -m above), so the
	# frontend subshell's bun→vite grandchild dies too — no leaked dev server.
	if [ ${#pids[@]} -gt 0 ]; then
		for p in "${pids[@]}"; do kill -- -"$p" 2>/dev/null; done
	fi
}
# Tear both servers down on ANY exit — including the one-shot smoke/shot paths, so
# they never leak a dev server (the failure mode that motivated this mode).
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
	# OUR servers must be the ones answering. A pinned port already held by another
	# reg_webapp instance would return 200 even though our uvicorn failed to bind
	# (and exited) — so confirm our pids are alive BEFORE trusting a 200.
	if ! kill -0 "${pids[0]}" 2>/dev/null || ! kill -0 "${pids[1]}" 2>/dev/null; then
		break # a server exited (e.g. a pinned port already in use) — fail fast
	fi
	if curl -sf -o /dev/null "http://localhost:$backend_port/api/context" 2>/dev/null &&
		curl -sf -o /dev/null "http://localhost:$frontend_port/" 2>/dev/null; then
		ready=1
		break
	fi
	sleep 1
done
if [ -z "$ready" ]; then
	echo "dev: a server failed to start (busy port, missing DB, or stale deps?) — see output above." >&2
	exit 1 # the EXIT trap tears down whatever did come up
fi

dev_url="http://localhost:$frontend_port"

case "$mode" in
smoke)
	# One-shot: drive the smoke flow against OUR frontend, then the EXIT trap tears
	# both servers down (no leak). Exit status is the driver's.
	echo "dev: smoke on $dev_url (auto-teardown on exit)" >&2
	(cd reg_webapp/frontend && REG_WEBAPP_DEV_URL="$dev_url" \
		bun ../.claude/skills/run-reg-webapp/driver.mjs smoke)
	exit $?
	;;
shot)
	echo "dev: shot $* on $dev_url (auto-teardown on exit)" >&2
	rc=0
	for route in "$@"; do
		(cd reg_webapp/frontend && REG_WEBAPP_DEV_URL="$dev_url" \
			bun ../.claude/skills/run-reg-webapp/driver.mjs shot "$route") || rc=$?
	done
	exit "$rc"
	;;
serve)
	printf 'reg_webapp dev (Ctrl-C to stop):\n  backend : http://localhost:%s\n  frontend: %s\n  driver  : REG_WEBAPP_DEV_URL=%s\n' \
		"$backend_port" "$dev_url" "$dev_url"
	# Steady state: block until Ctrl-C (INT trap -> nonzero) or a server exits. A
	# bare `wait` is fine — startup already succeeded; a later single-server crash
	# is a rare dev event you'll see and Ctrl-C. (`wait -n` isn't in bash 3.2.)
	wait
	;;
esac
