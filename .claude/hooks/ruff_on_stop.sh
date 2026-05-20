#!/usr/bin/env bash
# Stop hook: ruff check (no --fix) on Python files changed since HEAD.
#
# Runs on the coherent final state of the turn, so F401/F821 only flag
# genuinely unused/undefined names — not transient mid-refactor states.
#
# On findings, emits JSON `{"decision":"block","reason":...}` so Claude Code
# uses the spec-defined Stop block path: the findings are fed to Claude as
# its next-turn instruction via the dedicated `reason` channel instead of
# stderr. Claude Code caps consecutive blocks at 8, so no manual loop guard.
#
# Scoped to changed + untracked files (not the whole workspace) to stay fast.
set -uo pipefail

cd "${CLAUDE_PROJECT_DIR:-.}" || exit 0

# Bail silently if not in a git repo (won't happen in practice, but graceful).
git rev-parse --git-dir >/dev/null 2>&1 || exit 0

# Tracked-but-modified + untracked .py files. -z handles paths with whitespace.
# No overlap between the two streams (diff = tracked, ls-files --others = untracked).
files=()
while IFS= read -r -d '' f; do
	[ -f "$f" ] && files+=("$f")
done < <(
	git diff --name-only -z HEAD -- '*.py'
	git ls-files --others --exclude-standard -z -- '*.py'
)

[ ${#files[@]} -eq 0 ] && exit 0

if [ -x ".venv/bin/ruff" ]; then
	ruff=(".venv/bin/ruff")
elif command -v uv >/dev/null 2>&1; then
	ruff=(uv run --quiet ruff)
else
	exit 0
fi

if check_output=$("${ruff[@]}" check --quiet "${files[@]}" 2>&1); then
	exit 0
fi

# Findings present. Emit a JSON Stop-block decision on stdout (must be the
# only stdout output for the harness to parse it). python3 builds the JSON
# so arbitrary ruff output (quotes, newlines, ANSI) is escaped safely.
python3 -c '
import json, sys
print(json.dumps({
    "decision": "block",
    "reason": "ruff findings on changed files (fix before stopping):\n" + sys.argv[1],
}))
' "$check_output"
exit 0
