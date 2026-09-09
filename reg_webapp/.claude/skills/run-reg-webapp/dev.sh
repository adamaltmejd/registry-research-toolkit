#!/usr/bin/env bash
# Run the reg_webapp dev servers on auto-selected FREE ports — works in any
# checkout (main, a git worktree, a Yard lane container) with no port
# collisions. Picks a free backend and frontend port, points the Vite /api proxy
# at the backend via REG_WEBAPP_BACKEND_URL, and starts both from the .venv of
# THIS SCRIPT's own checkout — resolved from this file's path, never from the
# caller's cwd, so an absolute-path launch from elsewhere still serves the
# checkout the script belongs to.
#
# Modes:
#   dev.sh                 interactive — start both servers and block (Ctrl-C stops).
#   dev.sh smoke           ONE-SHOT — start, run the Playwright driver `smoke`, tear
#                          down, exit with the driver's status. For agent visual
#                          verification: random ports + guaranteed cleanup, so it
#                          never collides and never leaks a server.
#   dev.sh flows <out-dir> [scenario...]
#                          ONE-SHOT — start, run the Playwright driver `flows`
#                          (the /project error+retry cases, the catalog-authored
#                          draft cases, the deliberate-replacement case and the
#                          source-period correction, at four viewports), tear down,
#                          exit with the driver's status. The PNGs land in
#                          <out-dir> — each yard gate passes it $YARD_ARTIFACT_DIR
#                          and names the scenarios it retains the PNGs for; no
#                          names runs all seven, which is the local verification
#                          invocation.
#   dev.sh shot [viewport...] <route>...
#                          ONE-SHOT — screenshot each route, tear down, exit.
#                          Viewport flags before the routes capture responsive
#                          breakpoints (default desktop): --mobile (375x812),
#                          --tablet (768x1024), --desktop (1280x900), --wide
#                          (1920x1080), --all (the four), or --viewport WxH
#                          (repeatable). Each viewport × route is shot; non-desktop
#                          shots get a `-<label>` suffix.
#   dev.sh preview         preview_start entry point (.claude/launch.json) — like
#                          interactive serve, but the frontend binds the MCP-assigned
#                          $PORT so preview_start attaches to OUR vite and parallel
#                          sessions don't collide. Not meant to be run by hand.
#
# Leading flag (before the mode), works with every mode:
#   --fixture-db           serve a DETERMINISTIC SYNTHETIC catalog instead of the
#                          resolved reg_meta DB: build the fixture DB pair
#                          (backend/scripts/fixture_db.py — the same builder the
#                          backend tests use) into a temp dir, export it as
#                          REG_META_DB, and delete it on exit. For a container /
#                          checkout where no released DB is reachable.
#
# Ports are automatic (two of these never collide); pin with BACKEND_PORT /
# FRONTEND_PORT if you need to know them up front. smoke/shot screenshots land in
# a UNIQUE per-invocation directory under /tmp — printed on startup, and kept
# after teardown because the pictures are the evidence — unless REG_WEBAPP_SHOTS
# names a directory of your own. So concurrent lanes, and the operator, never
# overwrite each other's captures.
set -uo pipefail
# Job control so each background server runs in its OWN process group — lets
# cleanup() kill the WHOLE tree (the frontend subshell AND the bun→vite grandchild
# it spawns), not just the direct child. Without this, vite leaks on teardown.
set -m

# Leading global flag, consumed before the mode word so every mode accepts it.
# `fixture_db_dir` is the built temp dir (empty when the flag is absent); cleanup()
# removes it, so it must be defined before the trap is installed.
fixture_db=""
fixture_db_dir=""
if [ "${1:-}" = "--fixture-db" ]; then
	fixture_db=1
	shift
fi

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
flows)
	mode=flows
	shift
	# The output directory is a contract (the gate's declared artifact names land
	# in it), so require it up front and hand the driver an ABSOLUTE path — it
	# runs with the frontend as its cwd. Anything after it is a scenario name,
	# passed through unchanged; none means all of them.
	[ "$#" -ge 1 ] || {
		echo "dev: 'flows' needs an output directory, then optional scenario names" >&2
		exit 2
	}
	mkdir -p "$1" || exit 1
	out_dir=$(cd "$1" && pwd) || exit 1
	shift
	;;
preview)
	# preview_start (.claude/launch.json) entry point: same blocking servers as
	# `serve`, but the frontend binds the MCP-assigned $PORT (see port selection
	# below) so preview_start attaches to OUR vite — collision-free across sessions.
	mode=preview
	shift
	;;
"") mode=serve ;;
*)
	echo "usage: dev.sh [--fixture-db] [smoke | shot [--mobile|--tablet|--desktop|--wide|--all|--viewport WxH]... <route>... | flows <out-dir> [scenario...] | preview]" >&2
	exit 2
	;;
esac

# Viewport flags precede the routes in `shot` mode; consume them here so what's
# left in "$@" is the route list. Each viewport is passed to the driver via
# REG_WEBAPP_VIEWPORT (a preset name or raw WxH).
viewports=()
if [ "$mode" = shot ]; then
	while [ "$#" -gt 0 ]; do
		case "$1" in
		--mobile | --tablet | --desktop | --wide)
			viewports+=("${1#--}")
			shift
			;;
		--all)
			# The four widths the design-reviewer skill judges, so its contract
			# needs no extra flag here.
			viewports+=(mobile tablet desktop wide)
			shift
			;;
		--viewport)
			[ "$#" -ge 2 ] || {
				echo "dev: --viewport needs a WxH argument, e.g. --viewport 414x896" >&2
				exit 2
			}
			viewports+=("$2")
			shift 2
			;;
		--viewport=*)
			viewports+=("${1#*=}")
			shift
			;;
		--)
			shift
			break
			;;
		-*)
			echo "dev: unknown shot flag '$1'" >&2
			exit 2
			;;
		*) break ;;
		esac
	done
	[ ${#viewports[@]} -gt 0 ] || viewports=(desktop)
fi

if [ "$mode" = shot ] && [ "$#" -eq 0 ]; then
	echo "dev: 'shot' needs at least one route, e.g. dev.sh shot /catalog/scb/lisa" >&2
	exit 2 # validate before booting servers
fi

# THIS script's checkout (…/reg_webapp/.claude/skills/run-reg-webapp/dev.sh → repo
# root), not the caller's cwd: an operator or a sibling lane running a candidate's
# absolute dev.sh must get the candidate's servers, never whichever checkout they
# happened to be standing in.
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd) || exit 1
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

# The per-invocation shots directory (see the header). Deliberately NOT removed
# by cleanup() — the pictures outliving the servers is the point.
shots_dir=""
if [ "$mode" = smoke ] || [ "$mode" = shot ]; then
	shots_dir=${REG_WEBAPP_SHOTS:-$(mktemp -d "${TMPDIR:-/tmp}/reg-webapp-shots.XXXXXX")} || exit 1
	mkdir -p "$shots_dir" || exit 1
	# ABSOLUTE from here on, because the two ends disagree otherwise: a relative
	# REG_WEBAPP_SHOTS resolves against the repo root here (our cwd) but against
	# reg_webapp/frontend in the driver, so the run would report one directory and
	# write the PNGs into another. Resolve once, after the mkdir that guarantees it
	# exists, and both the report and the driver get the same path.
	shots_dir=$(cd "$shots_dir" && pwd) || exit 1
	export REG_WEBAPP_SHOTS="$shots_dir"
fi

# Provenance, first thing in the transcript: WHICH checkout is being served, at
# WHICH commit, and where this run's evidence lands.
echo "dev: repo $root HEAD $(git -C "$root" rev-parse HEAD 2>/dev/null || echo unknown)${shots_dir:+ shots $shots_dir}" >&2

# .venv/bin/python, not a system `python3`: the lane/gate image ships no system
# Python, and this checkout's venv is already required (checked above).
freeport() { .venv/bin/python -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
backend_port=${BACKEND_PORT:-$(freeport)}
if [ "$mode" = preview ]; then
	# preview_start assigns the frontend port via $PORT (autoPort always exports it,
	# even when it keeps the configured 5173). Bind exactly that so the MCP attaches
	# to OUR vite; the backend stays a private free port, wired to the /api proxy
	# below. The :-5173 fallback only matters if `dev.sh preview` is run by hand
	# outside preview_start.
	frontend_port=${PORT:-${FRONTEND_PORT:-5173}}
else
	frontend_port=${FRONTEND_PORT:-$(freeport)}
fi

pids=()
cleanup() {
	# Guard the expansion: `"${pids[@]}"` on an empty array under `set -u` errors
	# on bash 3.2 (macOS system bash) if a signal lands before the servers start.
	# `kill -- -PID` signals the job's whole PROCESS GROUP (set -m above), so the
	# frontend subshell's bun→vite grandchild dies too — no leaked dev server.
	if [ ${#pids[@]} -gt 0 ]; then
		for p in "${pids[@]}"; do kill -- -"$p" 2>/dev/null; done
	fi
	# The --fixture-db scratch DB pair is per-run and rebuildable — never leave it
	# behind (it's ~0.6 MB of temp per invocation).
	if [ -n "$fixture_db_dir" ]; then rm -rf "$fixture_db_dir"; fi
}
# Tear both servers down on ANY exit — including the one-shot smoke/shot paths, so
# they never leak a dev server (the failure mode that motivated this mode).
trap cleanup INT TERM EXIT

# --fixture-db: build the deterministic synthetic reg_meta DB pair (catalog +
# docs) into a temp dir and point BOTH servers at it via REG_META_DB — reg_meta's
# highest-precedence DB dir, so it wins over whatever the environment resolves.
# Built AFTER the trap so a failure mid-build still gets the dir removed.
if [ -n "$fixture_db" ]; then
	fixture_db_dir=$(mktemp -d "${TMPDIR:-/tmp}/reg-webapp-fixture-db.XXXXXX") || exit 1
	if ! .venv/bin/python reg_webapp/backend/scripts/fixture_db.py "$fixture_db_dir" >/dev/null; then
		echo "dev: --fixture-db build failed — see output above." >&2
		exit 1
	fi
	export REG_META_DB="$fixture_db_dir"
	echo "dev: --fixture-db serving a synthetic catalog from $fixture_db_dir" >&2
fi

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
flows)
	# One-shot, like smoke: the driver owns the cases + their assertions, and the
	# EXIT trap tears both servers (and their process groups) down either way.
	echo "dev: flows ${*:-(all scenarios)} on $dev_url → $out_dir (auto-teardown on exit)" >&2
	(cd reg_webapp/frontend && REG_WEBAPP_DEV_URL="$dev_url" \
		bun ../.claude/skills/run-reg-webapp/driver.mjs flows "$out_dir" "$@")
	exit $?
	;;
shot)
	echo "dev: shot $* on $dev_url viewports=${viewports[*]} (auto-teardown on exit)" >&2
	rc=0
	for vp in "${viewports[@]}"; do
		for route in "$@"; do
			(cd reg_webapp/frontend && REG_WEBAPP_DEV_URL="$dev_url" REG_WEBAPP_VIEWPORT="$vp" \
				bun ../.claude/skills/run-reg-webapp/driver.mjs shot "$route") || rc=$?
		done
	done
	exit "$rc"
	;;
serve | preview)
	# `serve` is interactive (Ctrl-C stops); `preview` is the preview_start entry
	# (the MCP stops it via preview_stop). Both just block on the running servers.
	printf 'reg_webapp dev (%s):\n  backend : http://localhost:%s\n  frontend: %s\n  driver  : REG_WEBAPP_DEV_URL=%s\n' \
		"$([ "$mode" = preview ] && echo 'preview_start' || echo 'Ctrl-C to stop')" \
		"$backend_port" "$dev_url" "$dev_url"
	# Steady state: block until Ctrl-C (INT trap -> nonzero) or a server exits. A
	# bare `wait` is fine — startup already succeeded; a later single-server crash
	# is a rare dev event you'll see and Ctrl-C. (`wait -n` isn't in bash 3.2.)
	wait
	;;
esac
