#!/usr/bin/env bash
# Ad-hoc verifier for .claude/hooks/block_no_verify.sh.
# Each case feeds the hook a synthetic PreToolUse payload and asserts the
# hook either denies (exit 0 with a "deny" payload) or allows (exit 0 with
# no payload).
set -uo pipefail

HOOK="$(cd "$(dirname "$0")/.." && pwd)/block_no_verify.sh"
if [[ ! -x "$HOOK" ]]; then
	echo "hook script not executable: $HOOK" >&2
	exit 2
fi

fail=0

run_case() {
	local expect="$1" command="$2"
	local payload out rc
	payload=$(python3 -c 'import json,sys; print(json.dumps({"tool_input":{"command":sys.argv[1]}}))' "$command")
	out=$(printf '%s' "$payload" | "$HOOK")
	rc=$?
	if [[ $rc -ne 0 ]]; then
		printf 'FAIL: hook exited %d for: %s\n' "$rc" "$command"
		fail=$((fail + 1))
		return
	fi
	local actual="allow"
	if [[ "$out" == *'"permissionDecision":"deny"'* ]]; then
		actual="deny"
	fi
	if [[ "$actual" != "$expect" ]]; then
		printf 'FAIL: expected %s, got %s: %s\n' "$expect" "$actual" "$command"
		fail=$((fail + 1))
	else
		printf 'ok   (%s): %s\n' "$actual" "$command"
	fi
}

# Must deny.
run_case deny 'git commit -n'
run_case deny 'git commit -nm "msg"'
run_case deny 'git -c foo=bar commit -n'
run_case deny 'git -c foo=bar commit -nm "msg"'
run_case deny 'git --git-dir=.git commit --no-verify'
run_case deny 'git --git-dir=.git commit -n'
run_case deny 'git commit -m "msg" --no-verify'
run_case deny 'npm test && git push --no-verify'
run_case deny 'git -c hooks.allownonascii=true commit -nm "x"'
run_case deny 'git -C /tmp/repo commit -n'
# Separate-arg flag forms (no `=`).
run_case deny 'git --git-dir .git commit -n'
run_case deny 'git --git-dir .git commit --no-verify'
run_case deny 'git --work-tree /tmp commit -n'
run_case deny 'git --work-tree /x --git-dir .git commit -nm hello'
# Bare pre-subcommand flag (takes no arg) followed by commit -n.
run_case deny 'git --paginate commit -n'

# Must allow.
run_case allow 'git push -n'
run_case allow 'git commit -m "msg"'
run_case allow 'git status'
run_case allow 'echo hello'
run_case allow 'git log --name-only'
run_case allow 'git diff --name-only -- file.py'

if [[ $fail -ne 0 ]]; then
	printf '\n%d case(s) failed\n' "$fail" >&2
	exit 1
fi
printf '\nall cases passed\n'
