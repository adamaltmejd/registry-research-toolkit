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
  --wake-backend NAME       Wake backend: app-server or exec (default: app-server).
  --app-wake-bin PATH       Test hook: helper to use for app-server wakes.
  --wake-timeout SECONDS    Max seconds to wait for app-server wake completion.
  --codex-bin PATH          Codex executable path.
  --uv-bin PATH             uv executable path.
  --preflight-bin PATH      Test hook: executable to use instead of cos_preflight.py.
  --no-canonical-check      Test hook: pass through to cos_preflight.py.
  -h, --help                Show this help.

The default app-server wake writes to persisted Codex thread history without streaming
the transcript in this terminal. Active Codex clients may need `codex resume THREAD_ID`
or a relaunch to show the injected turn until live refresh is supported. Do not run
`codex resume THREAD_ID` while this script says a wake is active; wait for the wake
finished line first. If the thread is already active and the wake is skipped, do not
resume until that active turn is idle.
Wake-worthy preflight JSON is summarized as one terminal line; raw JSON stays internal.
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

print_preflight_summary() {
	local json_file="$1"
	local summary
	if [[ ! -s "$json_file" ]]; then
		echo "cos-scheduler: wake requested"
		return
	fi
	if summary="$(python3 - "$json_file" <<'PY'
import json
import sys

try:
    with open(sys.argv[1], encoding="utf-8") as fh:
        data = json.load(fh)
except Exception:
    print("cos-scheduler: wake requested")
    raise SystemExit(0)

reasons = data.get("reasons")
if isinstance(reasons, list):
    reasons = [str(reason) for reason in reasons if reason]
else:
    reasons = []

if reasons:
    print("cos-scheduler: wake requested: " + "; ".join(reasons))
else:
    print("cos-scheduler: wake requested")
PY
	)"; then
		echo "$summary"
	else
		echo "cos-scheduler: wake requested"
	fi
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

repo="${COS_REPO:-/Users/adam/Code/registry-research-toolkit}"
thread="${COS_THREAD_ID:-}"
prompt="${COS_PROMPT:-}"
codex_bin="${COS_CODEX_BIN:-codex}"
uv_bin="${COS_UV_BIN:-uv}"
preflight_bin="${COS_PREFLIGHT_BIN:-}"
app_wake_bin="${COS_APP_WAKE_BIN:-$script_dir/cos_app_server_wake.py}"
wake_backend="${COS_WAKE_BACKEND:-app-server}"
wake_timeout="${COS_WAKE_TIMEOUT_SECONDS:-3600}"
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
	--wake-backend)
		need_value "$1" "${2:-}"
		wake_backend="$2"
		shift 2
		;;
	--app-wake-bin)
		need_value "$1" "${2:-}"
		app_wake_bin="$2"
		shift 2
		;;
	--wake-timeout)
		need_value "$1" "${2:-}"
		wake_timeout="$2"
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

case "$wake_backend" in
app-server | exec)
	;;
*)
	fail "--wake-backend must be app-server or exec; got $wake_backend"
	;;
esac

if [[ ! "$wake_timeout" =~ ^[1-9][0-9]*$ ]]; then
	fail "--wake-timeout must be a positive integer; got $wake_timeout"
fi

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

stdout_file="$(mktemp "${TMPDIR:-/tmp}/cos-preflight-out.XXXXXX")" ||
	fail "could not create stdout temp file"
stderr_file="$(mktemp "${TMPDIR:-/tmp}/cos-preflight-err.XXXXXX")" ||
	fail "could not create stderr temp file"
trap 'rm -f "$stdout_file" "$stderr_file"' EXIT

probe_cmd=("${preflight_cmd[@]}" --dry-run)
"${probe_cmd[@]}" >"$stdout_file" 2>"$stderr_file"
preflight_status=$?

case "$preflight_status" in
0)
	exit 0
	;;
10)
	print_preflight_summary "$stdout_file"
	if [[ -z "$thread" ]]; then
		fail "preflight requested wake but --thread or COS_THREAD_ID is missing"
	fi
	if [[ "$wake_backend" == "app-server" ]]; then
		wake_cmd=(
			"$uv_bin" run --no-project python "$app_wake_bin"
			--repo "$repo"
			--thread "$thread"
			--prompt "$prompt"
			--codex-bin "$codex_bin"
			--timeout "$wake_timeout"
		)
	else
		wake_cmd=("$codex_bin" exec -C "$repo" resume "$thread" "$prompt")
	fi
	if [[ "$dry_run" -eq 1 ]]; then
		printf 'cos-scheduler: dry-run would run:'
		printf ' %q' "${wake_cmd[@]}"
		printf '\n'
		exit 10
	fi
	echo "cos-scheduler: waking thread $thread via $wake_backend; do not run codex resume for this thread until this wake finishes"
	: >"$stderr_file"
	"${wake_cmd[@]}" 2>"$stderr_file"
	wake_status=$?
	if [[ "$wake_status" -eq 75 ]]; then
		echo "cos-scheduler: thread $thread is already active; skipped wake; do not run codex resume until that active turn is idle"
		exit 0
	fi
	if [[ "$wake_status" -ne 0 ]]; then
		if [[ -s "$stderr_file" ]]; then
			cat "$stderr_file" >&2
		fi
		exit "$wake_status"
	fi

	: >"$stdout_file"
	: >"$stderr_file"
	"${preflight_cmd[@]}" >"$stdout_file" 2>"$stderr_file"
	commit_status=$?
	if [[ "$commit_status" -eq 0 || "$commit_status" -eq 10 ]]; then
		echo "cos-scheduler: wake finished; safe to relaunch or run codex resume $thread"
		exit 0
	fi
	echo "cos-scheduler: wake succeeded but state commit failed" >&2
	if [[ -s "$stderr_file" ]]; then
		cat "$stderr_file" >&2
	fi
	if [[ -s "$stdout_file" ]]; then
		cat "$stdout_file" >&2
	fi
	exit 2
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
