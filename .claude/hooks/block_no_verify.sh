#!/usr/bin/env bash
# PreToolUse hook: refuse Bash calls that skip pre-commit / pre-push hooks.
#
# Two layers in this repo work together:
#   - `.claude/settings.json` permissions.deny catches the simplest forms
#     (`git commit --no-verify *`, `git commit -n *`, `git push --no-verify *`)
#     via prefix match.
#   - This hook catches the variants the prefix rules miss — `--no-verify`
#     anywhere in the command line, e.g. `git commit -m "msg" --no-verify`
#     or `npm test && git push --no-verify`, plus the `-n` short flag in
#     combined groups like `git commit -nm "msg"` (git accepts combined
#     single-char flags).
#
# Scope: any git subcommand using `--no-verify` (commit/push/merge/rebase all
# accept it), plus `-n` specifically on `git commit` — standalone (`-n`) or
# combined with other short flags (`-nm`, `-anm`, etc.). Other git subcommands
# use `-n` for harmless things like `push --dry-run`, so we don't match those.
#
# Emits the spec-defined PreToolUse JSON deny payload on stdout, exit 0.
# False positives are possible (e.g. `git commit -m "use --no-verify here"`)
# but rare and trivially worked around by rephrasing.
#
# Failure mode: if python3 cannot parse stdin (extremely rare), we exit 2 with
# a clear stderr message so the bypass attempt isn't silently allowed through.
set -uo pipefail

if ! command=$(python3 -c 'import json, sys; print(json.load(sys.stdin).get("tool_input", {}).get("command", ""))' 2>/dev/null); then
	echo "block_no_verify hook: failed to parse tool input JSON; blocking conservatively." >&2
	exit 2
fi

# Only inspect git invocations.
case "$command" in
*git*) ;;
*) exit 0 ;;
esac

# Deny payload is fixed content, so we hardcode it — no python3 dependency on
# the emit path. (Keep the JSON one-line so harness parsing is robust.)
deny() {
	printf '%s\n' '{"hookSpecificOutput":{"hookEventName":"PreToolUse","permissionDecision":"deny","permissionDecisionReason":"Refusing to skip pre-commit/pre-push hooks via --no-verify/-n. If a hook fails, fix the underlying issue rather than bypassing."}}'
	exit 0
}

# Token boundary: any character that can't continue an option token.
# Whitespace, EOL, and shell operators (`&&`, `||`, `;`, `|`, `>`, `<`, `)`,
# backtick, etc.) all match — without this, `git commit --no-verify&&echo ok`
# would slip past a whitespace-only terminator.
BOUNDARY='($|[^a-zA-Z0-9_-])'

# `--no-verify` as a standalone token in any git subcommand.
if [[ "$command" =~ (^|[^a-zA-Z0-9_-])--no-verify${BOUNDARY} ]]; then
	deny
fi

# `-n` as a short flag on `git commit`, either standalone or in a combined
# short-flag group like `-nm` / `-anm`. Single leading dash ensures we don't
# match `--name-only` etc. (those have two dashes).
#
# Pre-subcommand flag tokens are allowed between `git` and `commit` so that
# the wrapper form `git -c key=value commit -n` (and `git --git-dir=.git
# commit -n`) doesn't slip past. A flag token is one of:
#   - long flag, optionally `=value`:   --foo / --foo=bar
#   - short flag group:                 -a / -abc
#   - `-c`/`-C` with separately-quoted arg: `-c key=value` / `-C path`
# Restricting to these forms keeps arbitrary text between `git` and `commit`
# from matching (so `git status; git commit -n` only matches the real commit).
GIT_PRECMD_FLAG='(-[cC][[:space:]]+[^[:space:]]+|--[a-zA-Z][a-zA-Z0-9-]*(=[^[:space:]]+)?|-[a-zA-Z]+)'
if [[ "$command" =~ git[[:space:]]+(${GIT_PRECMD_FLAG}[[:space:]]+)*commit([[:space:]].*)?[[:space:]]-[a-zA-Z]*n[a-zA-Z]*${BOUNDARY} ]]; then
	deny
fi

exit 0
