#!/usr/bin/env bash
# PostToolUse hook: ruff --fix + format on edited Python files.
# Exit 2 surfaces any remaining (unfixable) findings to Claude.
set -uo pipefail

file_path=$(python3 -c 'import json, sys; print(json.load(sys.stdin).get("tool_input", {}).get("file_path", ""))')

case "$file_path" in
*.py) ;;
*) exit 0 ;;
esac

# File may have been deleted by the edit (rare, but Write can clobber).
[ -f "$file_path" ] || exit 0

cd "${CLAUDE_PROJECT_DIR:-.}" || exit 0

# Prefer the project venv — no uv startup overhead on the hot path.
# Fall back to `uv run` so a fresh clone without a synced venv still works.
# If neither is available, warn and let the edit through unformatted.
if [ -x ".venv/bin/ruff" ]; then
	ruff=(".venv/bin/ruff")
elif command -v uv >/dev/null 2>&1; then
	ruff=(uv run --quiet ruff)
else
	echo "ruff hook: skipping $file_path (no .venv/bin/ruff and uv not on PATH; run 'uv sync')" >&2
	exit 0
fi

# `ruff check --fix` returns nonzero iff findings remain after autofix.
# Capture once; re-checking is wasted work.
if fix_output=$("${ruff[@]}" check --fix --quiet "$file_path" 2>&1); then
	fix_status=0
else
	fix_status=$?
fi

"${ruff[@]}" format --quiet "$file_path" >/dev/null 2>&1 || true

if [ "$fix_status" -ne 0 ]; then
	printf 'ruff: remaining findings in %s:\n%s\n' "$file_path" "$fix_output" >&2
	exit 2
fi
exit 0
