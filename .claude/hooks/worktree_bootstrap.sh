#!/usr/bin/env bash
# WorktreeCreate + SessionStart hook: provision the checkout's OWN environment so
# dev servers and tooling work in any git worktree, not just the primary checkout.
#
# Why: a git worktree shares the repo's pyproject but NOT its .venv. uv installs
# the workspace packages EDITABLE, so a worktree borrowing main's .venv runs
# MAIN's source — the "fix didn't work, the servers served main" footgun
# (run-reg-webapp -> "Verifying from a git worktree"). A checkout-local .venv
# (its editable installs point HERE) plus the frontend's node_modules fixes it at
# the root, for every tool: pytest, ty, and the dev servers.
#
# Idempotent + fast: when .venv and node_modules already exist it exits at once,
# so it is safe on EVERY SessionStart; only an un-provisioned (usually fresh)
# worktree pays the one-time sync/install. Non-blocking: a failure warns on
# stderr but never blocks the session or the worktree creation (always exit 0).
set -uo pipefail

# Read the hook payload once (used only to label SessionStart context output).
payload=$(cat 2>/dev/null || true)
event=$(printf '%s' "$payload" | python3 -c '
import json, sys
try:
    print(json.load(sys.stdin).get("hook_event_name", ""))
except Exception:
    print("")
' 2>/dev/null || true)

# Provision the checkout the hook is running in. SessionStart runs in the
# session cwd (the worktree); fall back to CLAUDE_PROJECT_DIR, then cwd.
root=$(git rev-parse --show-toplevel 2>/dev/null || echo "${CLAUDE_PROJECT_DIR:-$PWD}")
cd "$root" || exit 0

did=()

# Python: uv stops at this checkout's pyproject.toml, so the sync creates a
# checkout-local .venv whose editable installs resolve to THIS tree.
if [ ! -x ".venv/bin/python" ]; then
	if command -v uv >/dev/null 2>&1; then
		if uv sync --frozen >/dev/null 2>&1; then
			did+=(".venv")
		else
			echo "worktree_bootstrap: 'uv sync --frozen' failed in $root" >&2
		fi
	else
		echo "worktree_bootstrap: uv not on PATH; skipping .venv" >&2
	fi
fi

# Frontend: bun node_modules for the SPA / Vite dev server.
if [ -f "reg_webapp/frontend/package.json" ] && [ ! -d "reg_webapp/frontend/node_modules" ]; then
	if command -v bun >/dev/null 2>&1; then
		if (cd reg_webapp/frontend && bun install --frozen-lockfile >/dev/null 2>&1); then
			did+=("reg_webapp/frontend/node_modules")
		else
			echo "worktree_bootstrap: 'bun install --frozen-lockfile' failed in $root" >&2
		fi
	else
		echo "worktree_bootstrap: bun not on PATH; skipping node_modules" >&2
	fi
fi

# Tell the session what got provisioned. additionalContext is a SessionStart
# feature; emit it only for that event (WorktreeCreate just provisions silently).
if [ "$event" = "SessionStart" ] && [ ${#did[@]} -gt 0 ]; then
	python3 -c '
import json, sys
print(json.dumps({
    "hookSpecificOutput": {
        "hookEventName": "SessionStart",
        "additionalContext": "worktree_bootstrap provisioned this checkout: " + ", ".join(sys.argv[1:]),
    }
}))
' "${did[@]}" 2>/dev/null || true
fi
exit 0
