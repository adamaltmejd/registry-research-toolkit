#!/usr/bin/env bash
# Ad-hoc verifier for .claude/hooks/worktree_bootstrap.sh (run it directly; like
# test_block_no_verify.sh it is not wired into pytest/CI).
#
# The hook does real provisioning (uv sync / bun install), so we stub uv/bun on
# PATH (they log their call and mimic uv/bun creating .venv / node_modules) and
# run against a TEMP fake checkout, so a real worktree is never mutated. We assert
# the deterministic contract:
#   1. Idempotent fast-path: a fully-provisioned, in-sync checkout (markers match
#      the current lockfile fingerprints) triggers no uv/bun and prints nothing.
#   2. No-event/direct invocation (how dev.sh calls it) provisions SYNCHRONOUSLY
#      and stamps the fingerprint markers.
#   3. `--provision <root>` provisions synchronously.
#   4. Partial install (.venv dir present, marker absent) still needs provisioning.
#   5. Dependency drift (lockfile changed -> marker stale) re-provisions.
#   6. SessionStart is NON-BLOCKING: it emits a well-formed JSON advisory (only
#      that branch writes stdout, so the advisory IS proof of the non-blocking path).
set -uo pipefail

HOOK="$(cd "$(dirname "$0")/.." && pwd)/worktree_bootstrap.sh"
if [[ ! -x "$HOOK" ]]; then
	echo "hook script not executable: $HOOK" >&2
	exit 2
fi

fail=0
note() { printf '%s\n' "$1"; }
fp() { cksum <"$1" | cut -d' ' -f1; } # mirror the hook's fingerprint()

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
calls="$work/calls.log"
repo="$work/repo"

mkdir -p "$repo/reg_webapp/frontend" "$work/bin"
(cd "$repo" && git init -q && git commit -q --allow-empty -m init 2>/dev/null) || {
	echo "could not init sandbox git repo" >&2
	exit 2
}
printf '{}' >"$repo/reg_webapp/frontend/package.json"
printf 'lock-v1\n' >"$repo/uv.lock"
printf 'lock-n1\n' >"$repo/reg_webapp/frontend/bun.lock"

# Stubs log their call AND mimic the tool creating its output dir, so the hook's
# post-install marker write lands on a real dir.
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

VENV_MARKER="$repo/.venv/.wt-provisioned"
NODE_MARKER="$repo/reg_webapp/frontend/node_modules/.wt-provisioned"

mark_provisioned() { # stamp markers with the CURRENT fingerprints
	mkdir -p "$repo/.venv/bin" "$repo/reg_webapp/frontend/node_modules"
	fp "$repo/uv.lock" >"$VENV_MARKER"
	fp "$repo/reg_webapp/frontend/bun.lock" >"$NODE_MARKER"
}
unprovision() { rm -rf "$repo/.venv" "$repo/reg_webapp/frontend/node_modules"; }

run_event() { # $1 = event ("" = no-event/direct); stdout, cwd = sandbox + stubs
	printf '{"hook_event_name":"%s","cwd":"%s"}' "$1" "$repo" |
		(cd "$repo" && PATH="$work/bin:$PATH" "$HOOK")
}

# --- Case 1: provisioned + in sync -> fast no-op ---
mark_provisioned
: >"$calls"
out=$(run_event SessionStart)
rc=$?
[[ $rc -eq 0 ]] || {
	note "FAIL[1]: exited $rc"
	fail=1
}
[[ -s "$calls" ]] && {
	note "FAIL[1]: in-sync checkout invoked: $(tr '\n' ';' <"$calls")"
	fail=1
}
[[ -n "$out" ]] && {
	note "FAIL[1]: expected no output; got: $out"
	fail=1
}

# --- Case 2: no-event/direct + missing -> synchronous + fingerprint markers ---
unprovision
: >"$calls"
run_event "" >/dev/null
grep -q '^uv sync --frozen' "$calls" || {
	note "FAIL[2]: should run 'uv sync --frozen'; calls: $(tr '\n' ';' <"$calls")"
	fail=1
}
grep -q '^bun install --frozen-lockfile' "$calls" || {
	note "FAIL[2]: should run 'bun install --frozen-lockfile'; calls: $(tr '\n' ';' <"$calls")"
	fail=1
}
[[ "$(cat "$VENV_MARKER" 2>/dev/null)" == "$(fp "$repo/uv.lock")" ]] || {
	note "FAIL[2]: venv marker should hold the uv.lock fingerprint"
	fail=1
}

# --- Case 3: --provision <root> -> synchronous ---
unprovision
: >"$calls"
(cd "$repo" && PATH="$work/bin:$PATH" "$HOOK" --provision "$repo")
grep -q '^uv sync --frozen' "$calls" || {
	note "FAIL[3]: --provision should run 'uv sync'; calls: $(tr '\n' ';' <"$calls")"
	fail=1
}

# --- Case 4: partial (.venv dir, no marker) still needs provisioning ---
unprovision
mkdir -p "$repo/.venv/bin"
: >"$calls"
run_event "" >/dev/null
grep -q '^uv sync --frozen' "$calls" || {
	note "FAIL[4]: partial .venv (no marker) should re-run 'uv sync'; calls: $(tr '\n' ';' <"$calls")"
	fail=1
}

# --- Case 5: dependency drift (lockfile changed -> marker stale) re-provisions ---
mark_provisioned
printf 'lock-v2-changed\n' >"$repo/uv.lock" # marker now stale vs new fingerprint
: >"$calls"
run_event "" >/dev/null
grep -q '^uv sync --frozen' "$calls" || {
	note "FAIL[5]: changed uv.lock should re-run 'uv sync'; calls: $(tr '\n' ';' <"$calls")"
	fail=1
}

# --- Case 6 (last; spawns a detached bg job): SessionStart + missing ->
#     non-blocking advisory (only this branch writes stdout) ---
unprovision
: >"$calls"
out=$(run_event SessionStart)
if ! printf '%s' "$out" | python3 -m json.tool >/dev/null 2>&1; then
	note "FAIL[6]: SessionStart advisory is not well-formed JSON: $out"
	fail=1
fi
case "$out" in
*background*) ;;
*)
	note "FAIL[6]: advisory should mention background provisioning; got: $out"
	fail=1
	;;
esac

[[ $fail -eq 0 ]] && note "ok: worktree_bootstrap contract holds"
exit "$fail"
