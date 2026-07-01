#!/usr/bin/env bash
# Run the chief-of-staff preflight and wake the existing Codex thread only on change.
set -uo pipefail

usage() {
	cat <<'EOF'
Usage: scripts/cos_scheduler_tick.sh [options]

Options:
  --dry-run                 Print the Codex wake command instead of running it.
  --repo PATH               Canonical checkout path.
  --thread ID               Codex chief-of-staff thread/session id to resume.
  --prompt TEXT             Prompt to send on wake.
  --state-file PATH         State file passed to scripts/cos_preflight.py.
  --codex-bin PATH          Codex executable path.
  --uv-bin PATH             uv executable path.
  --preflight-bin PATH      Test hook: executable to use instead of cos_preflight.py.
  --no-canonical-check      Test hook: pass through to cos_preflight.py.
  -h, --help                Show this help.
EOF
}

fail() {
	echo "cos-scheduler: $*" >&2
	exit 2
}

need_value() {
	if [[ $# -lt 2 || -z "$2" ]]; then
		fail "$1 requires a value"
	fi
}

repo="${COS_REPO:-/Users/adam/Code/registry-research-toolkit}"
thread="${COS_THREAD_ID:-}"
prompt="${COS_PROMPT:-}"
codex_bin="${COS_CODEX_BIN:-codex}"
uv_bin="${COS_UV_BIN:-uv}"
preflight_bin="${COS_PREFLIGHT_BIN:-}"
state_file=""
dry_run=0
no_canonical_check=0

while [[ $# -gt 0 ]]; do
	case "$1" in
	--dry-run)
		dry_run=1
		shift
		;;
	--repo)
		need_value "$1" "${2:-}"
		repo="$2"
		shift 2
		;;
	--thread)
		need_value "$1" "${2:-}"
		thread="$2"
		shift 2
		;;
	--prompt)
		need_value "$1" "${2:-}"
		prompt="$2"
		shift 2
		;;
	--state-file)
		need_value "$1" "${2:-}"
		state_file="$2"
		shift 2
		;;
	--codex-bin)
		need_value "$1" "${2:-}"
		codex_bin="$2"
		shift 2
		;;
	--uv-bin)
		need_value "$1" "${2:-}"
		uv_bin="$2"
		shift 2
		;;
	--preflight-bin)
		need_value "$1" "${2:-}"
		preflight_bin="$2"
		shift 2
		;;
	--no-canonical-check)
		no_canonical_check=1
		shift
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		fail "unknown option: $1"
		;;
	esac
done

if [[ -z "$prompt" ]]; then
	prompt="[\$chief-of-staff]($repo/.agents/skills/chief-of-staff/SKILL.md)"
fi

if ! cd "$repo"; then
	fail "cannot cd to repo: $repo"
fi

if [[ -n "$preflight_bin" ]]; then
	preflight_cmd=("$preflight_bin")
else
	preflight_cmd=("$uv_bin" run --no-project python scripts/cos_preflight.py)
fi
if [[ -n "$state_file" ]]; then
	preflight_cmd+=(--state-file "$state_file")
fi
if [[ "$no_canonical_check" -eq 1 ]]; then
	preflight_cmd+=(--no-canonical-check)
else
	preflight_cmd+=(--canonical "$repo")
fi
if [[ "$dry_run" -eq 1 ]]; then
	preflight_cmd+=(--dry-run)
fi

stdout_file="$(mktemp "${TMPDIR:-/tmp}/cos-preflight-out.XXXXXX")" ||
	fail "could not create stdout temp file"
stderr_file="$(mktemp "${TMPDIR:-/tmp}/cos-preflight-err.XXXXXX")" ||
	fail "could not create stderr temp file"
trap 'rm -f "$stdout_file" "$stderr_file"' EXIT

"${preflight_cmd[@]}" >"$stdout_file" 2>"$stderr_file"
preflight_status=$?

case "$preflight_status" in
0)
	exit 0
	;;
10)
	if [[ -s "$stdout_file" ]]; then
		cat "$stdout_file"
	fi
	if [[ -z "$thread" ]]; then
		fail "preflight requested wake but --thread or COS_THREAD_ID is missing"
	fi
	codex_cmd=("$codex_bin" exec -C "$repo" resume "$thread" "$prompt")
	if [[ "$dry_run" -eq 1 ]]; then
		printf 'cos-scheduler: dry-run would run:'
		printf ' %q' "${codex_cmd[@]}"
		printf '\n'
		exit 10
	fi
	echo "cos-scheduler: waking chief-of-staff thread $thread" >&2
	"${codex_cmd[@]}"
	exit $?
	;;
2)
	if [[ -s "$stderr_file" ]]; then
		cat "$stderr_file" >&2
	fi
	if [[ -s "$stdout_file" ]]; then
		cat "$stdout_file" >&2
	fi
	exit 2
	;;
*)
	echo "cos-scheduler: unexpected preflight exit $preflight_status" >&2
	if [[ -s "$stderr_file" ]]; then
		cat "$stderr_file" >&2
	fi
	if [[ -s "$stdout_file" ]]; then
		cat "$stdout_file" >&2
	fi
	exit 2
	;;
esac
