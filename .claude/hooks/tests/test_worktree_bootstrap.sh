#!/usr/bin/env bash
# Ad-hoc verifier for .claude/hooks/worktree_bootstrap.sh (run it directly; like
# test_block_no_verify.sh it is not wired into pytest/CI).
#
# The hook does real provisioning (uv sync / bun install), so we stub uv/bun on
# PATH (they log their invocation and mimic uv/bun creating .venv / node_modules)
# and run against a TEMP fake checkout, so a real worktree is never mutated. We
# assert the deterministic contract:
#   1. Idempotent fast-path: a fully-provisioned checkout (completion markers
#      present) triggers no uv/bun and prints nothing — safe on every SessionStart.
#   2. No-event/direct invocation (how dev.sh calls it) provisions SYNCHRONOUSLY
#      and writes the completion markers.
#   3. `--provision <root>` provisions synchronously.
#   4. SessionStart is NON-BLOCKING: it emits a well-formed JSON advisory (only
#      that branch writes stdout, so the advisory IS proof the non-blocking path
#      was taken).
#   5. A PARTIAL install (.venv dir present but no completion marker) still counts
#      as needs-provision — not mistaken for done.
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

# Stubs log their call AND mimic the tool creating its output dir, so the hook's
# post-install marker write (`: > .venv/.wt-provisioned`) lands on a real dir.
cat >"$work/bin/uv" <<EOF
#!/usr/bin/env bash
echo "uv \$*" >>"$calls"
mkdir -p .venv/bin
EOF
cat >"$work/bin/bun" <<EOF
#!/usr/bin/env bash
echo "bun \$*" >>"$calls"
mkdir -p node_modules
EOF
chmod +x "$work/bin/uv" "$work/bin/bun"

VENV_MARKER="$work/repo/.venv/.wt-provisioned"
NODE_MARKER="$work/repo/reg_webapp/frontend/node_modules/.wt-provisioned"

provision_repo() { # mark fully provisioned (markers present)
	mkdir -p "$work/repo/.venv/bin" "$work/repo/reg_webapp/frontend/node_modules"
	: >"$VENV_MARKER"
	: >"$NODE_MARKER"
}
unprovision_repo() { rm -rf "$work/repo/.venv" "$work/repo/reg_webapp/frontend/node_modules"; }

run_event() { # $1 = event name ("" = no-event/direct); stdout, cwd = sandbox + stubs
	printf '{"hook_event_name":"%s","cwd":"%s"}' "$1" "$work/repo" |
		(cd "$work/repo" && PATH="$work/bin:$PATH" "$HOOK")
}

# --- Case 1: fully provisioned -> fast no-op, no uv/bun, no output ---
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

# --- Case 2: no-event/direct (dev.sh path) + missing -> synchronous + markers ---
unprovision_repo
: >"$calls"
run_event "" >/dev/null
grep -q '^uv sync --frozen' "$calls" || {
	note "FAIL[2]: no-event should run 'uv sync --frozen'; calls: $(tr '\n' ';' <"$calls")"
	fail=1
}
grep -q '^bun install --frozen-lockfile' "$calls" || {
	note "FAIL[2]: no-event should run 'bun install --frozen-lockfile'; calls: $(tr '\n' ';' <"$calls")"
	fail=1
}
{ [[ -f "$VENV_MARKER" ]] && [[ -f "$NODE_MARKER" ]]; } || {
	note "FAIL[2]: completion markers not written after successful provision"
	fail=1
}

# --- Case 3: --provision <root> -> synchronous ---
unprovision_repo
: >"$calls"
(cd "$work/repo" && PATH="$work/bin:$PATH" "$HOOK" --provision "$work/repo")
grep -q '^uv sync --frozen' "$calls" || {
	note "FAIL[3]: --provision should run 'uv sync --frozen'; calls: $(tr '\n' ';' <"$calls")"
	fail=1
}

# --- Case 4: partial install (.venv dir, NO marker) still needs provisioning ---
unprovision_repo
mkdir -p "$work/repo/.venv/bin" # dir present but marker absent (interrupted sync)
: >"$calls"
run_event "" >/dev/null
grep -q '^uv sync --frozen' "$calls" || {
	note "FAIL[4]: partial .venv (no marker) should re-run 'uv sync'; calls: $(tr '\n' ';' <"$calls")"
	fail=1
}

# --- Case 5 (last; spawns a detached bg job): SessionStart + missing ->
#     non-blocking advisory. Only the SessionStart branch writes stdout, so a
#     well-formed advisory IS proof the non-blocking path was taken. ---
unprovision_repo
: >"$calls"
out=$(run_event SessionStart)
if ! printf '%s' "$out" | python3 -m json.tool >/dev/null 2>&1; then
	note "FAIL[5]: SessionStart advisory is not well-formed JSON: $out"
	fail=1
fi
case "$out" in
*background*) ;;
*)
	note "FAIL[5]: SessionStart advisory should mention background provisioning; got: $out"
	fail=1
	;;
esac

[[ $fail -eq 0 ]] && note "ok: worktree_bootstrap contract holds"
exit "$fail"
