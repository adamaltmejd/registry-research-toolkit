#!/usr/bin/env bash
# WorktreeCreate + SessionStart hook: provision the checkout's OWN environment so
# dev servers and tooling work in any git worktree, not just the primary checkout.
#
# Why: a git worktree shares the repo's pyproject but NOT its .venv. uv installs
# the workspace packages EDITABLE, so a worktree borrowing main's .venv runs
# MAIN's source — the "fix didn't work, the servers served main" footgun
# (run-reg-webapp -> "Git worktrees are auto-provisioned"). A checkout-local
# .venv (its editable installs point HERE) plus the frontend's node_modules fixes
# it at the root, for every tool: pytest, ty, and the dev servers.
#
# Two trigger modes, by event:
#   WorktreeCreate  -> provision SYNCHRONOUSLY (creation-time, once; a brief wait
#                      there is expected).
#   SessionStart    -> NON-BLOCKING: SessionStart gates the session, so we never
#                      run uv/bun inline. If anything is missing we kick the
#                      provision to the BACKGROUND under a lock and return at
#                      once. `uv run` and dev.sh self-heal synchronously on
#                      demand, so a lost/killed background job is harmless.
#
# Idempotent + fast: when .venv and node_modules already exist it exits at once.
# Always exit 0 — a hook failure must never block the session or the worktree.
set -uo pipefail

self=${BASH_SOURCE[0]}

# --- provisioning primitives (operate on an explicit checkout root) ----------

needs_provision() { # $1 = root; 0 if anything is missing
	[ ! -x "$1/.venv/bin/python" ] && return 0
	[ -f "$1/reg_webapp/frontend/package.json" ] && [ ! -d "$1/reg_webapp/frontend/node_modules" ] && return 0
	return 1
}

provision() { # $1 = root; synchronous; idempotent (re-checks each piece)
	local root=$1
	if [ ! -x "$root/.venv/bin/python" ]; then
		if command -v uv >/dev/null 2>&1; then
			(cd "$root" && uv sync --frozen) >/dev/null 2>&1 ||
				echo "worktree_bootstrap: 'uv sync --frozen' failed in $root" >&2
		else
			echo "worktree_bootstrap: uv not on PATH; skipping .venv" >&2
		fi
	fi
	if [ -f "$root/reg_webapp/frontend/package.json" ] && [ ! -d "$root/reg_webapp/frontend/node_modules" ]; then
		if command -v bun >/dev/null 2>&1; then
			(cd "$root/reg_webapp/frontend" && bun install --frozen-lockfile) >/dev/null 2>&1 ||
				echo "worktree_bootstrap: 'bun install --frozen-lockfile' failed in $root" >&2
		else
			echo "worktree_bootstrap: bun not on PATH; skipping node_modules" >&2
		fi
	fi
}

# Internal re-entry used by the non-blocking SessionStart path (see below) and by
# callers that want a direct synchronous provision (dev.sh): `… --provision <root>`.
if [ "${1:-}" = "--provision" ]; then
	provision "${2:-$PWD}"
	exit 0
fi

# --- hook entry --------------------------------------------------------------

# Read the payload once (only to read the event name; safe with no stdin).
payload=$(cat 2>/dev/null || true)
event=$(printf '%s' "$payload" | python3 -c '
import json, sys
try:
    print(json.load(sys.stdin).get("hook_event_name", ""))
except Exception:
    print("")
' 2>/dev/null || true)

# Provision the checkout the hook runs in. SessionStart runs in the session cwd
# (the worktree); fall back to CLAUDE_PROJECT_DIR, then cwd.
root=$(git rev-parse --show-toplevel 2>/dev/null || echo "${CLAUDE_PROJECT_DIR:-$PWD}")

needs_provision "$root" || exit 0 # fast path: already provisioned

if [ "$event" = "SessionStart" ]; then
	# Non-blocking self-heal: SessionStart blocks the session, so never sync here.
	# Background the provision under a one-per-checkout lock (cksum is POSIX, so no
	# shasum/sha1sum portability worry); nohup so it survives this hook returning.
	lock="${TMPDIR:-/tmp}/wt-bootstrap-$(printf '%s' "$root" | cksum | cut -d' ' -f1).lock"
	if mkdir "$lock" 2>/dev/null; then
		# Single quotes are intentional: $0/$1/$2 must expand inside the inner
		# `bash -c`, bound to the trailing self/root/lock args — not out here.
		# shellcheck disable=SC2016
		nohup bash -c '"$0" --provision "$1"; rmdir "$2" 2>/dev/null' \
			"$self" "$root" "$lock" >/dev/null 2>&1 &
	fi
	python3 -c '
import json
print(json.dumps({
    "hookSpecificOutput": {
        "hookEventName": "SessionStart",
        "additionalContext": "worktree_bootstrap: this checkout is missing .venv and/or node_modules; provisioning in the background. If a tool cannot find .venv yet, run reg_webapp/.claude/skills/run-reg-webapp/dev.sh (it self-provisions) or wait a moment.",
    }
}))
' 2>/dev/null || true
	exit 0
fi

# WorktreeCreate (or direct/manual invocation): provision synchronously.
provision "$root"
exit 0
