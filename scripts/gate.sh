#!/usr/bin/env bash
# gate — the full verification gate and the regenerate command.
#
#     bash scripts/gate.sh all            # before opening a PR
#     bash scripts/gate.sh g0 rust
#     bash scripts/gate.sh regen          # then commit the diff
#
# Each named step runs its commands in order from the repository root and stops at the
# first failure. `all` runs g0, rust, flows and frontend; `lint` and `crates` are g0's
# static checks and Rust part, for CI's `lint` and `rust` jobs; `regen` runs only when
# named. CI calls `lint`, `crates`, `rust`, `frontend` and `flows`. `rust` also runs the
# release checks on the synthetic artifact, so the Rust binaries are built once per job.
# `regen` also blesses the plugin project schema.
#
# No `set -e`: a failing command ends its step, and the summary still prints.
set -uo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd) || exit 1
cd "$ROOT" || exit 1
FRONTEND=$ROOT/reg_webapp/frontend
# The binaries `cargo build --workspace` writes; a per-worktree CARGO_TARGET_DIR moves them.
BIN=${CARGO_TARGET_DIR:-$ROOT/target}/debug
ALL="g0 rust flows frontend"
# The builder's CI fixture (crates/reg-build/fixture), built twice by `rust`.
FIXTURE=$ROOT/crates/reg-build/fixture
# The SPA's reg-core .wasm, gzipped (crates/DESIGN.md, "Crates and run modes"). Over
# it, the frontend step fails: raising the budget is a maintainer call. About 68 KB gz
# measured after the search fold left (it was about 145 KB with it).
WASM_GZ_BUDGET=200000
# The markdown checker and its one pin: ci.yml `lint` reads it to install that release;
# locally, `cargo install panache --version <pin> --locked`. ci.yml and Renovate parse
# this exact `PANACHE_VERSION=<version>` line.
PANACHE_VERSION=3.17.1

# The last failure, for the summary line.
failed_cmd=""
failed_rc=0

# Run a command from the current directory; print it and its elapsed seconds.
run() {
	printf '$'
	printf ' %q' "$@"
	printf '\n'
	local started=$SECONDS rc=0
	"$@" || rc=$?
	echo "  elapsed $((SECONDS - started)) s"
	if [ "$rc" -ne 0 ]; then
		failed_rc=$rc
		failed_cmd="$*"
	fi
	return "$rc"
}

# `run` with the command alone in `dir`.
run_in() {
	local dir=$1
	shift
	run env -C "$dir" "$@"
}

# A check that is not a single command: fail the step with `message`.
fail() {
	echo "gate: $1" >&2
	failed_rc=1
	failed_cmd=$1
	return 1
}

crates() {
	run cargo fmt --all --check &&
		run cargo clippy --workspace --all-targets --locked -- -D warnings &&
		# `--all-targets` skips `test = false` targets: the conformance suites, by name.
		run cargo clippy -p reg-conformance --test conformance --test release --locked \
			-- -D warnings &&
		run cargo test --workspace --locked &&
		# Unused crate dependencies; a false positive is ignored in that crate's
		# Cargo.toml (`[package.metadata.cargo-machete] ignored`) with a reason.
		run cargo machete
}

lint() {
	local version
	version=$(panache --version 2>/dev/null) || {
		fail "panache not found; install $PANACHE_VERSION (see PANACHE_VERSION)"
		return 1
	}
	[ "$version" = "panache $PANACHE_VERSION" ] || {
		fail "'$version' is not the pinned panache $PANACHE_VERSION"
		return 1
	}
	run panache format --check . &&
		run panache lint . &&
		run shellcheck scripts/*.sh reg_webapp/dev/*.sh
}

g0() {
	lint && crates
}

rust() {
	run cargo build --workspace --locked || return 1
	# The builder's determinism check: the fixture built twice, compared byte for byte.
	local out rc=0
	out=$(mktemp -d "${TMPDIR:-/tmp}/gate-build.XXXXXX") || return 1
	for n in 1 2; do
		run "$BIN/reg-build" build --inputs "$FIXTURE/inputs" \
			--curation "$FIXTURE/curation" --assets "$FIXTURE/assets" \
			--out "$out/$n.db" || {
			rc=1
			break
		}
	done
	[ "$rc" -ne 0 ] || run cmp "$out/1.db" "$out/2.db" || rc=1
	rm -rf "$out"
	[ "$rc" -eq 0 ] || return 1
	# The conformance suite (crates/reg-conformance), then the release checks with the
	# `base` fixture's artifact standing in for a real one (no `REG_ARTIFACT_DIR`).
	run cargo test -p reg-conformance --test conformance --locked &&
		run cargo test -p reg-conformance --test release --locked
}

flows() {
	# The production bootstrap and interactive flows share the browser-only job.
	run_in "$FRONTEND" bun install --frozen-lockfile &&
		run_in "$FRONTEND" bun run gen:wasm &&
		run_in "$FRONTEND" bun run build &&
		run_in "$FRONTEND" bun scripts/smoke_build.ts || return 1
	local out dev=$ROOT/reg_webapp/dev/dev.sh
	out=$(mktemp -d "${TMPDIR:-/tmp}/gate-flows.XXXXXX") || return 1
	run bash "$dev" --fixture-db flows "$out" </dev/null &&
		run bash "$dev" --paired-fixture flows "$out" steward-held </dev/null || return 1
	# Kept on failure: the screenshots are the evidence.
	rm -rf "$out"
}

frontend() {
	# On demand, so a fresh worktree needs no setup; a no-op when in sync.
	run_in "$FRONTEND" bun install --frozen-lockfile &&
		run_in "$FRONTEND" bun run gen:wasm || return 1
	local wasm=$FRONTEND/src/lib/reg-core-wasm/reg_core_wasm_bg.wasm size
	size=$(gzip -9 -c "$wasm" | wc -c | tr -d ' ') || return 1
	echo "gate: ${wasm##*/} $size bytes gzipped (budget $WASM_GZ_BUDGET)"
	[ "$size" -le "$WASM_GZ_BUDGET" ] || {
		fail "${wasm##*/} is over its gzipped budget; see WASM_GZ_BUDGET"
		return 1
	}
	for script in check lint test build gen:types; do
		run_in "$FRONTEND" bun run "$script" || return 1
	done
	# A diff means the SPA's types drifted from the committed API contract. `ls-files`
	# fails on an untracked one, which `git diff` ignores.
	local generated=src/lib/api-types-rust.ts
	run_in "$FRONTEND" git ls-files --error-unmatch -- "$generated" &&
		run_in "$FRONTEND" git diff --exit-code -- "$generated"
}

regen() {
	run cargo build --workspace --locked &&
		run env REG_META_BLESS=1 cargo test -p reg-meta --test openapi_snapshot &&
		# Cut from the committed openapi.json, so it runs after that file is blessed.
		run env REG_META_BLESS=1 cargo test -p xtask --test project_schema_snapshot &&
		# The MCP `tools/list` golden, as the conformance `mcp` stdio session lists it.
		run env REG_META_BLESS=1 cargo test -p reg-conformance --test conformance \
			--locked stdio_session
}

usage() {
	echo "usage: gate.sh {lint|g0|crates|rust|flows|frontend|regen|all}..." >&2
	exit 2
}

[ "$#" -gt 0 ] || usage
names=()
for step in "$@"; do
	case "$step" in
	all)
		# shellcheck disable=SC2206 # ALL is a fixed word list
		names+=($ALL)
		;;
	lint | g0 | crates | rust | flows | frontend | regen) names+=("$step") ;;
	*) usage ;;
	esac
done

times=""
failed=""
for name in "${names[@]}"; do
	echo "== gate $name"
	started=$SECONDS
	"$name" || failed="$name: exit $failed_rc from $failed_cmd"
	times="${times:+$times, }$name $((SECONDS - started)) s"
	[ -z "$failed" ] || break
done
echo "== gate $times"
if [ -n "$failed" ]; then
	echo "== gate FAILED $failed"
	exit 1
fi
echo "== gate passed"
