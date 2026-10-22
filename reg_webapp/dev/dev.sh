#!/usr/bin/env bash
# Run the reg_webapp dev servers on auto-selected FREE ports — works in any
# checkout (main, a git worktree, a container) with no port
# collisions. Picks a free port for each of the two servers — the Rust server
# (`reg-meta serve`, which answers every /api route) and Vite — points the Vite
# /api proxy at the first via REG_META_SERVER_URL, and starts them from the .venv
# and cargo target of THIS SCRIPT's own checkout — resolved from this file's path,
# never from the caller's cwd, so an absolute-path launch from elsewhere still
# serves the checkout the script belongs to.
#
# Modes:
#   dev.sh                 interactive — start the servers and block (Ctrl-C stops).
#   dev.sh smoke           ONE-SHOT — start, drive the catalog smoke flow with
#                          agent-browser (smoke() below), tear down, exit with its
#                          status. For agent visual verification: random ports +
#                          guaranteed cleanup, so it never collides and never leaks
#                          a server.
#   dev.sh flows <out-dir> [scenario...]
#                          ONE-SHOT — start, run the Playwright driver (driver.mjs)
#                          (the /project error+retry cases, the catalog-authored
#                          draft cases, the deliberate-replacement case, the
#                          /project card's source-period edit and the period
#                          card's keyboard focus, at four viewports), tear down,
#                          exit with the driver's status. The PNGs land in
#                          <out-dir>. Named scenarios run only those cases;
#                          no names runs all eight.
#   dev.sh shot [viewport...] <route>...
#                          ONE-SHOT — screenshot each route, tear down, exit.
#                          Viewport flags before the routes capture responsive
#                          breakpoints (default desktop): --mobile (375x812),
#                          --tablet (768x1024), --desktop (1280x900), --wide
#                          (1920x1080), --all (the four), or --viewport WxH
#                          (repeatable). Each viewport × route is captured with
#                          agent-browser: a PNG, the accessibility snapshot
#                          (.snapshot.txt) and an axe audit (.a11y.txt);
#                          non-desktop captures get a `-<label>` suffix.
#   dev.sh preview         preview_start entry point (.claude/launch.json) — like
#                          interactive serve, but the frontend binds the MCP-assigned
#                          $PORT so preview_start attaches to OUR vite and parallel
#                          sessions don't collide. Not meant to be run by hand.
#
# Leading flag (before the mode), works with every mode:
#   --fixture-db           serve a DETERMINISTIC SYNTHETIC catalog instead of the
#                          resolved reg_meta DB: the catalog + docs pair the real
#                          builder makes from conformance/cases/fixtures/webapp-dev,
#                          looked up in the shared fixture cache
#                          (conformance/fixture_cache.py, built on a miss) and exported
#                          as REG_META_DB. With REG_WEBAPP_STEWARD set, the catalog is
#                          the synthetic steward artifact (`reader steward`) instead.
#                          For a container / checkout where no released DB is
#                          reachable.
#
# smoke/shot run agent-browser, pinned in reg_webapp/frontend/package.json, in a
# browser session private to this invocation (closed on exit). It drives an
# installed Google Chrome; with none, run `agent-browser install` once. `flows`
# stays on the Playwright driver (driver.mjs): it injects request failures and
# counts requests per case, which is Playwright's routing surface.
#
# Ports are automatic (two of these never collide); pin with RUST_PORT /
# FRONTEND_PORT if you need to know them up front. smoke/shot screenshots land in
# a UNIQUE per-invocation directory under /tmp — printed on startup, and kept
# after teardown because the pictures are the evidence — unless REG_WEBAPP_SHOTS
# names a directory of your own. So concurrent runs never overwrite each other's
# captures.
set -uo pipefail
# Job control so each background server runs in its OWN process group — lets
# cleanup() kill the WHOLE tree (the frontend subshell AND the bun→vite grandchild
# it spawns), not just the direct child. Without this, vite leaks on teardown.
set -m

# Leading global flag, consumed before the mode word so every mode accepts it.
# `fixture_db_dir` is the session's copy of the fixture (empty when the flag is
# absent); cleanup() removes it, so it must be defined before the trap is installed.
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

# The capture viewports: a named preset or a raw WxH, printed as "W H". The four
# presets are the widths frontend/DESIGN.md designs for; the Playwright `flows`
# driver spells the same four in its VIEWPORTS.
viewport_size() {
	case "$1" in
	mobile) echo "375 812" ;;
	tablet) echo "768 1024" ;;
	desktop) echo "1280 900" ;;
	wide) echo "1920 1080" ;;
	*)
		[[ "$1" =~ ^([0-9]+)x([0-9]+)$ ]] || return 1
		echo "${BASH_REMATCH[1]} ${BASH_REMATCH[2]}"
		;;
	esac
}

# Viewport flags precede the routes in `shot` mode; consume them here so what's
# left in "$@" is the route list.
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
	for vp in "${viewports[@]}"; do
		viewport_size "$vp" >/dev/null || {
			echo "dev: bad viewport '$vp' — use --mobile|--tablet|--desktop|--wide or --viewport WxH" >&2
			exit 2 # validate before booting servers
		}
	done
fi

if [ "$mode" = shot ] && [ "$#" -eq 0 ]; then
	echo "dev: 'shot' needs at least one route, e.g. dev.sh shot /catalog/scb/lisa" >&2
	exit 2 # validate before booting servers
fi

# THIS script's checkout (…/reg_webapp/dev/dev.sh → repo root), not the caller's
# cwd: running another checkout's absolute dev.sh must serve that checkout, never
# whichever one the caller happened to be standing in.
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd) || exit 1
cd "$root" || exit 1

# This checkout's OWN environment, never main's, installed on demand (near-instant
# when already in sync). The venv runs freeport, the --fixture-db builder and the
# default DB resolution; node_modules runs Vite, agent-browser and the driver. `env -u`: an
# inherited UV_PROJECT_ENVIRONMENT would sync another checkout's venv (#1337).
if ! env -u UV_PROJECT_ENVIRONMENT uv sync --frozen --quiet; then
	echo "dev: 'uv sync --frozen' failed — see output above." >&2
	exit 1
fi
if ! (cd reg_webapp/frontend && bun install --frozen-lockfile --silent); then
	echo "dev: 'bun install --frozen-lockfile' failed — see output above." >&2
	exit 1
fi

# The Rust server, built from this checkout. The whole workspace, as `cargo test`
# and CI build it, so the two share one set of compiled dependencies.
if ! cargo build --workspace --locked -q; then
	echo "dev: 'cargo build --workspace' failed — see output above." >&2
	exit 1
fi
# The SPA's reg-core module, rebuilt every run so a reg-core edit never meets a stale
# .wasm (the bootstrap needs it before the app mounts).
if ! (cd reg_webapp/frontend && bun run --silent gen:wasm); then
	echo "dev: 'bun run gen:wasm' failed — see output above." >&2
	exit 1
fi

# The per-invocation shots directory (see the header). Deliberately NOT removed
# by cleanup() — the pictures outliving the servers is the point.
shots_dir=""
if [ "$mode" = smoke ] || [ "$mode" = shot ]; then
	shots_dir=${REG_WEBAPP_SHOTS:-$(mktemp -d "${TMPDIR:-/tmp}/reg-webapp-shots.XXXXXX")} || exit 1
	mkdir -p "$shots_dir" || exit 1
	# ABSOLUTE from here on: a relative REG_WEBAPP_SHOTS resolves against the repo
	# root here (our cwd) but against the agent-browser daemon's own cwd when it
	# writes a capture, so the run would report one directory and write the PNGs
	# into another. Resolve once, after the mkdir that guarantees it exists.
	shots_dir=$(cd "$shots_dir" && pwd) || exit 1
fi

# Provenance, first thing in the transcript: WHICH checkout is being served, at
# WHICH commit, and where this run's evidence lands.
echo "dev: repo $root HEAD $(git -C "$root" rev-parse HEAD 2>/dev/null || echo unknown)${shots_dir:+ shots $shots_dir}" >&2

# .venv/bin/python, not a system `python3`: the gate image ships no system
# Python, and this checkout's venv is already required (checked above).
freeport() { .venv/bin/python -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
rust_port=${RUST_PORT:-$(freeport)}
if [ "$mode" = preview ]; then
	# preview_start assigns the frontend port via $PORT (autoPort always exports it,
	# even when it keeps the configured 5173). Bind exactly that so the MCP attaches
	# to OUR vite; the Rust server stays a private free port, wired to the /api
	# proxy below. The :-5173 fallback only matters if `dev.sh preview` is run by hand
	# outside preview_start.
	frontend_port=${PORT:-${FRONTEND_PORT:-5173}}
else
	frontend_port=${FRONTEND_PORT:-$(freeport)}
fi

# agent-browser in a session named for THIS process: its default session is one
# browser shared machine-wide, so concurrent runs would drive each other's page.
# The idle timeout reaps the daemon even if this script is SIGKILLed.
ab_session=""
if [ "$mode" = smoke ] || [ "$mode" = shot ]; then ab_session="reg-webapp-dev-$$"; fi
ab() {
	bun "$root/reg_webapp/frontend/node_modules/agent-browser/bin/agent-browser.js" \
		--session "$ab_session" --idle-timeout 2m "$@"
}

pids=()
cleanup() {
	# The browser first, while the page it holds still has a server to talk to.
	if [ -n "$ab_session" ]; then ab close >/dev/null 2>&1; fi
	# Guard the expansion: `"${pids[@]}"` on an empty array under `set -u` errors
	# on bash 3.2 (macOS system bash) if a signal lands before the servers start.
	# `kill -- -PID` signals the job's whole PROCESS GROUP (set -m above), so the
	# frontend subshell's bun→vite grandchild dies too — no leaked dev server.
	if [ ${#pids[@]} -gt 0 ]; then
		for p in "${pids[@]}"; do kill -- -"$p" 2>/dev/null; done
	fi
	if [ -n "$fixture_db_dir" ]; then rm -rf "$fixture_db_dir"; fi
}
# Tear every server down on ANY exit — including the one-shot smoke/shot paths, so
# they never leak a dev server (the failure mode that motivated this mode). INT/TERM
# exit so the run stops (a bare cleanup trap would return and carry on driving the
# browser against dead servers); the EXIT trap then cleans up.
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# --fixture-db: point EVERY server at a deterministic synthetic catalog via
# REG_META_DB, so it wins over whatever the environment resolves. The artifact comes
# from the conformance fixture cache (shared across checkouts, built on a miss) and is
# COPIED into a per-session temp dir after the trap: the cache prunes a generation
# idle for 6 hours, and serving requests does not mark it used, so a long session
# served straight from the cache would lose its catalog mid-run. The steward artifact
# carries the same docs DB, so it is its own cache entry beside the gate's docs-less
# `reader steward`.
if [ -n "$fixture_db" ]; then
	fixture=(fixtures/webapp-dev catalog --docs fixtures/webapp-dev/docs)
	if [ "${REG_WEBAPP_STEWARD:-global}" != "global" ]; then
		fixture=(reader steward --docs fixtures/webapp-dev/docs)
	fi
	if ! fixture_catalog=$(.venv/bin/python conformance/fixture_cache.py "${fixture[@]}"); then
		echo "dev: --fixture-db build failed — see output above." >&2
		exit 1
	fi
	fixture_db_dir=$(mktemp -d "${TMPDIR:-/tmp}/reg-webapp-fixture-db.XXXXXX") || exit 1
	cp "$(dirname "$fixture_catalog")"/*.db "$fixture_db_dir"/ || exit 1
	export REG_META_DB="$fixture_db_dir"
	echo "dev: --fixture-db serving a synthetic catalog from $REG_META_DB" >&2
fi

# The Rust server needs its catalog directory explicitly: the fixture's, the
# caller's REG_META_DB, or the directory reg_meta resolves by default (XDG).
# REG_WEBAPP_STEWARD names the catalog.
rust_db=${REG_META_DB:-$(.venv/bin/python -c 'import reg_meta_build.db; print(reg_meta_build.db.db_path_from_args(None).parent)')}
target/debug/reg-meta serve --db "$rust_db" --catalog "${REG_WEBAPP_STEWARD:-global}" \
	--stewards reg_webapp/stewards --port "$rust_port" &
pids+=($!)
(
	cd reg_webapp/frontend &&
		REG_META_SERVER_URL="http://127.0.0.1:$rust_port" \
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
	# reg_webapp instance would return 200 even though our server failed to bind
	# (and exited) — so confirm our pids are alive BEFORE trusting a 200.
	exited=""
	for p in "${pids[@]}"; do kill -0 "$p" 2>/dev/null || exited=1; done
	if [ -n "$exited" ]; then
		break # a server exited (e.g. a pinned port already in use) — fail fast
	fi
	if curl -sf -o /dev/null "http://127.0.0.1:$rust_port/api/context" 2>/dev/null &&
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

# `networkidle` is NOT "rendered": React swaps fetched data in after the network
# settles, so a capture right after navigation shows the loading placeholder.
# Every loading placeholder carries aria-busy="true" (the components' contract
# with this script — don't key on UI copy); wait for the last one to clear.
settled() {
	ab wait --load networkidle >/dev/null &&
		ab wait --fn '!document.querySelector("[aria-busy=\"true\"]")' >/dev/null
}

# One capture is the evidence the design reviewer reads, side by side under one
# name: the picture, the accessibility tree and an axe audit (which exits 0 with
# violations — judging them is the reviewer's job). The page's JS errors print
# with the console's error lines so a broken render can't pass unnoticed.
capture() {
	local base="$shots_dir/$1"
	ab screenshot "$base.png" >/dev/null &&
		ab snapshot -c >"$base.snapshot.txt" &&
		ab a11y >"$base.a11y.txt" || return 1
	echo "shot: $base.png ($(grep -m 1 '^axe-core' "$base.a11y.txt"))"
	ab console | grep '^\[error\]' || true
	# --json: agent-browser 0.39.1's text output for `errors` prints a bare "✗"
	# whatever it holds; revisit on a bump.
	ab errors --json | .venv/bin/python -c 'import json, sys
for e in (json.load(sys.stdin).get("data") or {}).get("errors", []):
    print("[pageerror]", e["text"].splitlines()[0])'
	ab console --clear >/dev/null && ab errors --clear >/dev/null
}

# The catalog smoke: /catalog → provider → register → variable, then narrow the
# leaf's Period slider from the keyboard, apply it, assert what the app wrote to
# the URL and what it shows, and cold-reload the deep link. Desktop only.
smoke() {
	local here period seeded from to deep
	local slider='[role="group"][aria-label="Period window (years)"]'
	ab set viewport 1280 900 >/dev/null &&
		ab open "$dev_url/catalog" >/dev/null && settled && capture 01-root || return 1
	# Drill by clicking the first link STRICTLY DEEPER than the current path —
	# `a[href^="/catalog"]` alone matches the header nav link first and goes
	# nowhere. The SPA's `link` action intercepts these (pushState, no reload).
	for step in 02-provider 03-register 04-variable; do
		here=$(ab eval 'location.pathname.replace(/\/$/, "")' | tr -d '"')
		ab click "a[href^=\"$here/\"]" >/dev/null && settled || return 1
		echo "clicked from $here → $(ab get url)"
		capture "$step" || return 1
	done
	# Real form interaction, the way a keyboard researcher does it: one ArrowRight
	# on "From year" (the thumbs clamp to the subject's own coverage, so the result
	# is a range the catalog delivers). Scoped to the leaf's slider group: the
	# header's project-window slider has the same thumb labels.
	seeded=$(ab get value "$slider input[aria-label=\"From year\"]")
	to=$(ab get value "$slider input[aria-label=\"To year\"]")
	[ "$seeded" -lt "$to" ] || {
		echo "smoke: period slider seeded a single year ($seeded) — no range to narrow" >&2
		return 1
	}
	ab focus "$slider input[aria-label=\"From year\"]" >/dev/null &&
		ab press ArrowRight >/dev/null || return 1
	from=$(ab get value "$slider input[aria-label=\"From year\"]")
	[ "$from" -eq $((seeded + 1)) ] || {
		echo "smoke: From year did not step: $seeded → $from" >&2
		return 1
	}
	period="$from..$to"
	if ! { ab click 'button[aria-label="Apply period"]' >/dev/null &&
		ab wait --fn "new URLSearchParams(location.search).get('period') === '$period'" >/dev/null &&
		settled && ab wait --text "narrowed to $period" >/dev/null; }; then
		echo "smoke: applying period $period did not land in the URL and the page" >&2
		return 1
	fi
	echo "period $seeded..$to → $period at $(ab get url)"
	capture 04b-period-resolved || return 1
	# Deep-link reload: a cold load of the nested path renders the same view
	# (vite's SPA fallback in dev; the edge worker in production).
	deep=$(ab eval 'location.pathname' | tr -d '"')
	ab open "$dev_url$deep" >/dev/null && settled || return 1
	[ "$(ab get text body | wc -c)" -ge 40 ] || {
		echo "smoke: deep link $deep rendered ~empty body" >&2
		return 1
	}
	capture 05-deep-link-reload || return 1
	echo "smoke: OK"
}

case "$mode" in
smoke)
	# One-shot: drive the smoke flow against OUR frontend, then the EXIT trap tears
	# every server down (no leak).
	echo "dev: smoke on $dev_url (auto-teardown on exit)" >&2
	smoke
	exit $?
	;;
flows)
	# One-shot, like smoke: the driver owns the cases + their assertions, and the
	# EXIT trap tears every server (and their process groups) down either way.
	echo "dev: flows ${*:-(all scenarios)} on $dev_url → $out_dir (auto-teardown on exit)" >&2
	(cd reg_webapp/frontend && REG_WEBAPP_DEV_URL="$dev_url" \
		bun ../dev/driver.mjs "$out_dir" "$@")
	exit $?
	;;
shot)
	echo "dev: shot $* on $dev_url viewports=${viewports[*]} (auto-teardown on exit)" >&2
	rc=0
	for vp in "${viewports[@]}"; do
		# shellcheck disable=SC2046 # "W H" splits into the two arguments
		ab set viewport $(viewport_size "$vp") >/dev/null || exit 1
		# Desktop keeps the bare name; other viewports get a `-<label>` suffix so a
		# multi-viewport run captures each breakpoint instead of overwriting.
		suffix=""
		[ "$vp" = desktop ] || suffix="-$vp"
		for route in "$@"; do
			name=${route//\//_}
			{ ab open "$dev_url$route" >/dev/null && settled &&
				capture "${name:-root}$suffix"; } || rc=1
		done
	done
	exit "$rc"
	;;
serve | preview)
	# `serve` is interactive (Ctrl-C stops); `preview` is the preview_start entry
	# (the MCP stops it via preview_stop). Both just block on the running servers.
	printf 'reg_webapp dev (%s):\n  rust    : http://127.0.0.1:%s\n  frontend: %s\n  flows   : REG_WEBAPP_DEV_URL=%s\n' \
		"$([ "$mode" = preview ] && echo 'preview_start' || echo 'Ctrl-C to stop')" \
		"$rust_port" "$dev_url" "$dev_url"
	# Steady state: block until Ctrl-C (INT trap -> nonzero) or a server exits. A
	# bare `wait` is fine — startup already succeeded; a later single-server crash
	# is a rare dev event you'll see and Ctrl-C. (`wait -n` isn't in bash 3.2.)
	wait
	;;
esac
