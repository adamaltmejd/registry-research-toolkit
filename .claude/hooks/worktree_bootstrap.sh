#!/usr/bin/env bash
# SessionStart hook (+ a `--provision` entry dev.sh reuses): provision the
# checkout's OWN environment so dev servers and tooling work in any git worktree,
# not just the primary checkout.
#
# Why: a git worktree shares the repo's pyproject but NOT its .venv. uv installs
# the workspace packages EDITABLE, so a worktree borrowing main's .venv runs
# MAIN's source — the "fix didn't work, the servers served main" footgun
# (run-reg-webapp -> "Git worktrees are auto-provisioned"). A checkout-local
# .venv (its editable installs point HERE) plus the frontend's node_modules fixes
# it at the root, for every tool: pytest, ty, and the dev servers.
#
# NOT wired to WorktreeCreate: that event REPLACES git's worktree creation (the
# hook must create the worktree and print its path), so a provisioner there would
# abort creation. SessionStart is the right post-creation trigger.
#
# SessionStart is NON-BLOCKING: SessionStart gates the session, so we never run
# uv/bun inline. If anything is missing/stale we background the provision and
# return at once. `uv run` and dev.sh self-heal synchronously on demand, so a
# lost/killed background job is harmless.
#
# Always exit 0 — a hook failure must never block the session.
set -uo pipefail

self=${BASH_SOURCE[0]}

# Completion markers live inside the gitignored .venv / node_modules and store a
# FINGERPRINT of the dependency inputs (the resolved lockfiles). A piece is
# "provisioned" only when its marker exists AND matches the current fingerprint —
# so a partial/interrupted install (no marker) OR a dependency change (marker
# stale) both re-trigger the idempotent sync, while an in-sync checkout is a fast
# skip (no uv/bun churn on every session).
VENV_MARKER=".venv/.wt-provisioned"
NODE_MARKER="reg_webapp/frontend/node_modules/.wt-provisioned"

fingerprint() { # $1 = lockfile; stable digest, or 'none' if absent (cksum is POSIX)
	if [ -f "$1" ]; then cksum <"$1" | cut -d' ' -f1; else echo none; fi
}

venv_ok() { # $1 = root
	[ -f "$1/$VENV_MARKER" ] && [ "$(cat "$1/$VENV_MARKER" 2>/dev/null)" = "$(fingerprint "$1/uv.lock")" ]
}
node_ok() { # $1 = root; vacuously ok when there is no frontend
	[ ! -f "$1/reg_webapp/frontend/package.json" ] && return 0
	[ -f "$1/$NODE_MARKER" ] && [ "$(cat "$1/$NODE_MARKER" 2>/dev/null)" = "$(fingerprint "$1/reg_webapp/frontend/bun.lock")" ]
}

needs_provision() { # $1 = root; 0 if anything is missing/stale
	! venv_ok "$1" || ! node_ok "$1"
}

provision() { # $1 = root; synchronous; idempotent; stamps the fingerprint on success
	local root=$1
	if ! venv_ok "$root"; then
		if command -v uv >/dev/null 2>&1; then
			if (cd "$root" && uv sync --frozen) >/dev/null 2>&1; then
				fingerprint "$root/uv.lock" >"$root/$VENV_MARKER"
			else
				echo "worktree_bootstrap: 'uv sync --frozen' failed in $root" >&2
			fi
		else
			echo "worktree_bootstrap: uv not on PATH; skipping .venv" >&2
		fi
	fi
	if [ -f "$root/reg_webapp/frontend/package.json" ] && ! node_ok "$root"; then
		if command -v bun >/dev/null 2>&1; then
			if (cd "$root/reg_webapp/frontend" && bun install --frozen-lockfile) >/dev/null 2>&1; then
				fingerprint "$root/reg_webapp/frontend/bun.lock" >"$root/$NODE_MARKER"
			else
				echo "worktree_bootstrap: 'bun install --frozen-lockfile' failed in $root" >&2
			fi
		else
			echo "worktree_bootstrap: bun not on PATH; skipping node_modules" >&2
		fi
	fi
}

# Internal re-entry used by the non-blocking SessionStart path below, and by
# callers wanting a direct synchronous provision (dev.sh): `… --provision <root>`.
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

needs_provision "$root" || exit 0 # fast path: already provisioned + in sync

if [ "$event" = "SessionStart" ]; then
	# Non-blocking self-heal: SessionStart blocks the session, so never sync here.
	# Background it (nohup so it survives this hook returning). No external lock —
	# uv/bun self-serialize, and the fingerprint markers make a duplicate run a
	# fast no-op; a killed run leaves the marker unwritten and is retried next
	# SessionStart (an external lock could instead LEAK and wedge provisioning).
	# Single quotes are intentional: $0/$1 expand inside the inner `bash -c`.
	# shellcheck disable=SC2016
	nohup bash -c '"$0" --provision "$1"' "$self" "$root" >/dev/null 2>&1 &
	python3 -c '
import json
print(json.dumps({
    "hookSpecificOutput": {
        "hookEventName": "SessionStart",
        "additionalContext": "worktree_bootstrap: this checkout needs .venv/node_modules (re)provisioned; running it in the background. If a tool cannot find .venv yet, run reg_webapp/.claude/skills/run-reg-webapp/dev.sh (it self-provisions) or wait a moment.",
    }
}))
' 2>/dev/null || true
	exit 0
fi

# Direct/manual invocation (no SessionStart payload, e.g. dev.sh): synchronous.
provision "$root"
exit 0
