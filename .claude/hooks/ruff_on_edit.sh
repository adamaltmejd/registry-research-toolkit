#!/usr/bin/env bash
# PostToolUse hook: ruff format + non-blocking lint info on edited Python files.
#
# Two steps, both safe on transient states:
#   1. `ruff format` — cosmetic normalization, never destructive.
#   2. `ruff check --no-fix` — reports findings via `additionalContext` so Claude
#      sees them inline with the tool result and can self-correct on its own.
#      No `--fix` (avoids silent import deletion during multi-file refactors),
#      no blocking (no forced full-file Writes for partial renames). Stop hook
#      handles end-of-turn enforcement via `decision: "block"`.
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

# Format errors are almost exclusively syntax errors; the check step below
# (and ruff_on_stop.sh) will surface them properly. Swallow format errors here.
"${ruff[@]}" format --quiet "$file_path" >/dev/null 2>&1 || true

# Lint check without --fix: just report. exit 0 keeps the edit non-blocking;
# findings ride into Claude's context via `additionalContext` next to the
# tool result, so Claude can choose whether to address them inline.
if check_output=$("${ruff[@]}" check --no-fix --quiet "$file_path" 2>&1); then
	exit 0
fi

python3 -c '
import json, sys
print(json.dumps({
    "hookSpecificOutput": {
        "hookEventName": "PostToolUse",
        "additionalContext": "ruff findings in " + sys.argv[1] + ":\n" + sys.argv[2],
    }
}))
' "$file_path" "$check_output"
exit 0
