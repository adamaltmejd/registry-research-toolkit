#!/usr/bin/env bash
# PreToolUse hook: refuse Bash calls that skip pre-commit / pre-push hooks.
#
# Two layers in this repo work together:
#   - `.claude/settings.json` permissions.deny catches the simplest forms
#     (`git commit --no-verify *`, `git commit -n *`, `git push --no-verify *`)
#     via prefix match.
#   - This hook catches the variants the prefix rules miss — `--no-verify`
#     anywhere in the command line, e.g. `git commit -m "msg" --no-verify`
#     or `npm test && git push --no-verify`.
#
# Scope: any git subcommand using `--no-verify` (commit/push/merge/rebase all
# accept it), plus `-n` specifically on `git commit` (other git subcommands
# use `-n` for harmless things like `push --dry-run`).
#
# Emits the spec-defined PreToolUse JSON deny payload on stdout, exit 0.
# False positives are possible (e.g. `git commit -m "use --no-verify here"`)
# but rare and trivially worked around by rephrasing.
set -uo pipefail

command=$(python3 -c 'import json, sys; print(json.load(sys.stdin).get("tool_input", {}).get("command", ""))')

# Only inspect git invocations.
case "$command" in
*git*) ;;
*) exit 0 ;;
esac

deny() {
	python3 -c '
import json
print(json.dumps({
    "hookSpecificOutput": {
        "hookEventName": "PreToolUse",
        "permissionDecision": "deny",
        "permissionDecisionReason": (
            "Refusing to skip pre-commit/pre-push hooks via --no-verify/-n. "
            "If a hook fails, fix the underlying issue rather than bypassing."
        ),
    }
}))
'
	exit 0
}

if [[ "$command" =~ (^|[[:space:]])--no-verify($|[[:space:]]) ]]; then
	deny
fi

# `-n` shorthand on `git commit` specifically. Require a space before -n so
# we don't false-match on `--name-only` etc.
if [[ "$command" =~ git[[:space:]]+commit([[:space:]].*)?[[:space:]]-n($|[[:space:]]) ]]; then
	deny
fi

exit 0
