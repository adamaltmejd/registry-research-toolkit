#!/usr/bin/env bash
# Ad-hoc verifier for .claude/hooks/worktree_bootstrap.sh (run it directly; like
# test_block_no_verify.sh it is not wired into pytest/CI).
#
# The hook does real provisioning (uv sync / bun install), so we stub uv/bun on
# PATH (they just log their invocation) and run against a TEMP fake checkout, so a
# real worktree is never mutated. We assert the deterministic contract:
#   1. Idempotent fast-path: a checkout with .venv + node_modules triggers no
#      uv/bun and prints nothing — safe to run on every SessionStart.
#   2. WorktreeCreate provisions SYNCHRONOUSLY (uv sync + bun install run inline).
#   3. `--provision <root>` (the path dev.sh uses) provisions synchronously.
#   4. SessionStart is NON-BLOCKING: it emits a well-formed JSON advisory and does
#      NOT run uv/bun inline (provisioning is backgrounded).
set -uo pipefail

HOOK="$(cd "$(dirname "$0")/.." && pwd)/worktree_bootstrap.sh"
if [[ ! -x "$HOOK" ]]; then
	echo "hook script not executable: $HOOK" >&2
	exit 2
fi

fail=0
note() { printf '%s\n' "$1"; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
calls="$work/calls.log"

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

provision_repo() { # mark the sandbox as fully provisioned
	mkdir -p "$work/repo/.venv/bin" "$work/repo/reg_webapp/frontend/node_modules"
	printf '#!/bin/sh\n' >"$work/repo/.venv/bin/python"
	chmod +x "$work/repo/.venv/bin/python"
}
unprovision_repo() { rm -rf "$work/repo/.venv" "$work/repo/reg_webapp/frontend/node_modules"; }

run_event() { # $1 = event name; stdout of hook, run with cwd = sandbox + stub PATH
	printf '{"hook_event_name":"%s","cwd":"%s"}' "$1" "$work/repo" |
		(cd "$work/repo" && PATH="$work/bin:$PATH" "$HOOK")
}

# --- Case 1: already provisioned -> fast no-op, no uv/bun, no output ---
provision_repo
: >"$calls"
out=$(run_event SessionStart)
rc=$?
[[ $rc -eq 0 ]] || {
	note "FAIL[1]: provisioned checkout exited $rc"
	fail=1
}
[[ -s "$calls" ]] && {
	note "FAIL[1]: provisioned checkout invoked: $(tr '\n' ';' <"$calls")"
	fail=1
}
[[ -n "$out" ]] && {
	note "FAIL[1]: expected no output; got: $out"
	fail=1
}

# --- Case 2: WorktreeCreate + missing -> provisions synchronously ---
unprovision_repo
: >"$calls"
run_event WorktreeCreate >/dev/null
grep -q '^uv sync --frozen' "$calls" || {
	note "FAIL[2]: WorktreeCreate should run 'uv sync --frozen'; calls: $(tr '\n' ';' <"$calls")"
	fail=1
}
grep -q '^bun install --frozen-lockfile' "$calls" || {
	note "FAIL[2]: WorktreeCreate should run 'bun install --frozen-lockfile'; calls: $(tr '\n' ';' <"$calls")"
	fail=1
}

# --- Case 3: --provision <root> (dev.sh path) -> synchronous ---
unprovision_repo
: >"$calls"
(cd "$work/repo" && PATH="$work/bin:$PATH" "$HOOK" --provision "$work/repo")
grep -q '^uv sync --frozen' "$calls" || {
	note "FAIL[3]: --provision should run 'uv sync --frozen'; calls: $(tr '\n' ';' <"$calls")"
	fail=1
}
grep -q '^bun install --frozen-lockfile' "$calls" || {
	note "FAIL[3]: --provision should run 'bun install --frozen-lockfile'; calls: $(tr '\n' ';' <"$calls")"
	fail=1
}

# --- Case 4 (last; spawns a detached background job): SessionStart + missing ->
#     non-blocking advisory. Only the SessionStart branch writes stdout (the
#     synchronous branch is silent), so a well-formed advisory IS the proof the
#     non-blocking path was taken. (Asserting "no uv call" would race the fast
#     stub in the detached job, so we don't.) ---
unprovision_repo
: >"$calls"
out=$(run_event SessionStart)
if ! printf '%s' "$out" | python3 -m json.tool >/dev/null 2>&1; then
	note "FAIL[4]: SessionStart advisory is not well-formed JSON: $out"
	fail=1
fi
case "$out" in
*background*) ;;
*)
	note "FAIL[4]: SessionStart advisory should mention background provisioning; got: $out"
	fail=1
	;;
esac

[[ $fail -eq 0 ]] && note "ok: worktree_bootstrap contract holds"
exit "$fail"
