#!/usr/bin/env bash
# Run chief-of-staff scheduler ticks in the foreground until interrupted.
set -uo pipefail

usage() {
	cat <<'EOF'
Usage: scripts/cos_scheduler_heartbeat.sh [options] THREAD_ID
       scripts/cos_scheduler_heartbeat.sh [options] --thread THREAD_ID

Run scripts/cos_scheduler_tick.sh in a foreground loop. The tick script still owns
the deterministic preflight and wakes Codex only when repo/GitHub state changed.
With the default app-server wake, the agent transcript is written to persisted Codex
thread history, not this terminal; active Codex clients may need `codex resume THREAD_ID`
or a relaunch to show the injected turn until live refresh is supported. Do not run
`codex resume THREAD_ID` while a tick wake is active; wait for the wake finished line.
If a tick reports that the thread is already active and skipped the wake, do not resume
until that active turn is idle.

Options:
  --interval SECONDS        Seconds between ticks (default: 900, or COS_INTERVAL_SECONDS).
  --repo PATH               Canonical checkout path.
  --thread ID               Codex chief-of-staff thread/session id to resume.
  --prompt TEXT             Prompt to send on wake.
  --state-file PATH         State file passed through to scripts/cos_scheduler_tick.sh.
  --wake-backend NAME       Wake backend passed through to the tick script.
  --app-wake-bin PATH       App-server wake helper passed through to the tick script.
  --wake-timeout SECONDS    Max seconds passed through to app-server wakes.
  --codex-bin PATH          Codex executable path.
  --uv-bin PATH             uv executable path.
  --tick-bin PATH           Test hook: executable to use instead of cos_scheduler_tick.sh.
  --max-ticks N             Stop after N ticks. Intended for smoke tests/debugging.
  -h, --help                Show this help.
EOF
}

fail() {
	echo "cos-heartbeat: $*" >&2
	exit 2
}

need_value() {
	if [[ $# -lt 2 || -z "$2" ]]; then
		fail "$1 requires a value"
	fi
}

require_positive_int() {
	local name="$1"
	local value="$2"
	if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
		fail "$name must be a positive integer; got $value"
	fi
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

repo="${COS_REPO:-/Users/adam/Code/registry-research-toolkit}"
thread=""
interval="${COS_INTERVAL_SECONDS:-900}"
prompt=""
state_file=""
wake_backend=""
app_wake_bin=""
wake_timeout=""
codex_bin=""
uv_bin=""
tick_bin="$script_dir/cos_scheduler_tick.sh"
max_ticks=""

while [[ $# -gt 0 ]]; do
	case "$1" in
	--interval)
		need_value "$1" "${2:-}"
		interval="$2"
		shift 2
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
	--tick-bin)
		need_value "$1" "${2:-}"
		tick_bin="$2"
		shift 2
		;;
	--max-ticks)
		need_value "$1" "${2:-}"
		max_ticks="$2"
		shift 2
		;;
	-h | --help)
		usage
		exit 0
		;;
	-*)
		fail "unknown option: $1"
		;;
	*)
		if [[ -n "$thread" ]]; then
			fail "unexpected extra argument: $1"
		fi
		thread="$1"
		shift
		;;
	esac
done

if [[ -z "$thread" ]]; then
	thread="${COS_THREAD_ID:-}"
fi
if [[ -z "$thread" ]]; then
	fail "missing THREAD_ID argument or --thread ID"
fi

require_positive_int "--interval" "$interval"
if [[ -n "$max_ticks" ]]; then
	require_positive_int "--max-ticks" "$max_ticks"
fi

tick_count=0

while true; do
	tick_count=$((tick_count + 1))
	printf '[%s] chief-of-staff scheduler tick %d\n' \
		"$(date '+%Y-%m-%d %H:%M:%S')" "$tick_count"

	tick_cmd=("$tick_bin" --repo "$repo" --thread "$thread")
	if [[ -n "$prompt" ]]; then
		tick_cmd+=(--prompt "$prompt")
	fi
	if [[ -n "$state_file" ]]; then
		tick_cmd+=(--state-file "$state_file")
	fi
	if [[ -n "$wake_backend" ]]; then
		tick_cmd+=(--wake-backend "$wake_backend")
	fi
	if [[ -n "$app_wake_bin" ]]; then
		tick_cmd+=(--app-wake-bin "$app_wake_bin")
	fi
	if [[ -n "$wake_timeout" ]]; then
		tick_cmd+=(--wake-timeout "$wake_timeout")
	fi
	if [[ -n "$codex_bin" ]]; then
		tick_cmd+=(--codex-bin "$codex_bin")
	fi
	if [[ -n "$uv_bin" ]]; then
		tick_cmd+=(--uv-bin "$uv_bin")
	fi

	"${tick_cmd[@]}"
	tick_status=$?
	if [[ "$tick_status" -ne 0 ]]; then
		echo "cos-heartbeat: tick failed with exit $tick_status; stopping" >&2
		exit "$tick_status"
	fi

	if [[ -n "$max_ticks" && "$tick_count" -ge "$max_ticks" ]]; then
		echo "cos-heartbeat: reached max ticks ($max_ticks); stopping"
		exit 0
	fi

	echo "cos-heartbeat: sleeping ${interval}s; press Ctrl-C to stop"
	sleep "$interval" || {
		echo "cos-heartbeat: stopped"
		exit 130
	}
done
