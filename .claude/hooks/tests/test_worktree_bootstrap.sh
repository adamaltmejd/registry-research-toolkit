#!/usr/bin/env bash
# Ad-hoc verifier for .claude/hooks/worktree_bootstrap.sh.
#
# The hook does real provisioning (uv sync / bun install), so we don't exercise
# those here. We assert the cheap, deterministic contract that makes it safe to
# run on every SessionStart:
#   1. Idempotent fast-path: in a checkout that already has .venv + node_modules,
#      it exits 0 and provisions nothing (no uv/bun invocation).
#   2. SessionStart with nothing to do emits no additionalContext.
#   3. A non-SessionStart event (e.g. WorktreeCreate) never emits structured
#      output (additionalContext is SessionStart-only).
# To keep the test hermetic we run the hook against a TEMP fake checkout (so a
# real un-provisioned worktree is never mutated) with stub `uv`/`bun` on PATH
# that record whether they were called.
set -uo pipefail

HOOK="$(cd "$(dirname "$0")/.." && pwd)/worktree_bootstrap.sh"
if [[ ! -x "$HOOK" ]]; then
	echo "hook script not executable: $HOOK" >&2
	exit 2
fi

fail=0
note() { printf '%s\n' "$1"; }

# Sandbox: a fake git checkout with stub uv/bun that log their invocations.
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
calls="$work/calls.log"
: >"$calls"

mkdir -p "$work/repo" "$work/bin"
(cd "$work/repo" && git init -q && git commit -q --allow-empty -m init 2>/dev/null) || {
	echo "could not init sandbox git repo" >&2
	exit 2
}
mkdir -p "$work/repo/reg_webapp/frontend"
printf '{}' >"$work/repo/reg_webapp/frontend/package.json"

cat >"$work/bin/uv" <<EOF
#!/usr/bin/env bash
echo "uv \$*" >>"$calls"
EOF
cat >"$work/bin/bun" <<EOF
#!/usr/bin/env bash
echo "bun \$*" >>"$calls"
EOF
chmod +x "$work/bin/uv" "$work/bin/bun"

run_hook() { # event -> stdout of hook; cwd is the sandbox repo
	local event="$1"
	printf '{"hook_event_name":"%s","cwd":"%s"}' "$event" "$work/repo" |
		(cd "$work/repo" && PATH="$work/bin:$PATH" "$HOOK")
}

# --- Case 1: already provisioned -> fast no-op, no uv/bun, no output ---
mkdir -p "$work/repo/.venv/bin"
printf '#!/bin/sh\n' >"$work/repo/.venv/bin/python"
chmod +x "$work/repo/.venv/bin/python"
mkdir -p "$work/repo/reg_webapp/frontend/node_modules"
: >"$calls"
out=$(run_hook SessionStart)
rc=$?
if [[ $rc -ne 0 ]]; then
	note "FAIL[1]: provisioned checkout exited $rc"
	fail=1
fi
if [[ -s "$calls" ]]; then
	note "FAIL[1]: provisioned checkout still invoked: $(tr '\n' ';' <"$calls")"
	fail=1
fi
if [[ -n "$out" ]]; then
	note "FAIL[1]: expected no additionalContext when nothing provisioned; got: $out"
	fail=1
fi

# --- Case 2: missing env -> provisions via uv + bun ---
rm -rf "$work/repo/.venv" "$work/repo/reg_webapp/frontend/node_modules"
: >"$calls"
out=$(run_hook SessionStart)
rc=$?
if [[ $rc -ne 0 ]]; then
	note "FAIL[2]: exited $rc"
	fail=1
fi
if ! grep -q '^uv sync --frozen' "$calls"; then
	note "FAIL[2]: expected 'uv sync --frozen'; calls: $(tr '\n' ';' <"$calls")"
	fail=1
fi
if ! grep -q '^bun install --frozen-lockfile' "$calls"; then
	note "FAIL[2]: expected 'bun install --frozen-lockfile'; calls: $(tr '\n' ';' <"$calls")"
	fail=1
fi

# --- Case 3: WorktreeCreate never emits structured output ---
rm -rf "$work/repo/.venv" "$work/repo/reg_webapp/frontend/node_modules"
out=$(run_hook WorktreeCreate)
if [[ -n "$out" ]]; then
	note "FAIL[3]: WorktreeCreate should emit no stdout payload; got: $out"
	fail=1
fi

if [[ $fail -eq 0 ]]; then
	note "ok: worktree_bootstrap contract holds"
fi
exit "$fail"
