#!/usr/bin/env python3
"""Wake an existing Codex thread through the app-server protocol."""

from __future__ import annotations

import argparse
import json
import select
import subprocess
import sys
import time
from collections import deque
from pathlib import Path
from typing import Any

SETUP_ERROR = 2
THREAD_BUSY = 75
TIMEOUT = 124

APPROVAL_REQUEST_METHODS = {
    "item/commandExecution/requestApproval",
    "item/fileChange/requestApproval",
    "item/permissions/requestApproval",
    "item/tool/requestUserInput",
    "mcpServer/elicitation/request",
}

OPT_OUT_NOTIFICATION_METHODS = [
    "item/agentMessage/delta",
    "item/reasoning/textDelta",
    "item/reasoning/summaryTextDelta",
    "item/plan/delta",
    "command/exec/outputDelta",
    "process/outputDelta",
    "item/commandExecution/outputDelta",
    "item/commandExecution/terminalInteraction",
]


class WakeError(Exception):
    def __init__(self, message: str, exit_code: int = SETUP_ERROR) -> None:
        super().__init__(message)
        self.exit_code = exit_code


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send one prompt to an existing Codex thread via app-server stdio.",
    )
    parser.add_argument(
        "--repo", required=True, help="Checkout path for the resumed turn."
    )
    parser.add_argument(
        "--thread", required=True, help="Codex thread/session id to resume."
    )
    parser.add_argument("--prompt", required=True, help="Prompt to send to the thread.")
    parser.add_argument(
        "--codex-bin",
        default="codex",
        help="Codex executable path. Defaults to PATH lookup.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=3600,
        help="Maximum seconds to wait for the app-server turn to return idle.",
    )
    parser.add_argument(
        "--approval-policy",
        default="on-request",
        choices=["untrusted", "on-failure", "on-request", "never"],
        help="Turn approval policy. Defaults to on-request.",
    )
    parser.add_argument(
        "--approvals-reviewer",
        default="auto_review",
        choices=["user", "auto_review", "guardian_subagent"],
        help="Approval reviewer for unattended turns. Defaults to auto_review.",
    )
    return parser.parse_args()


def _send(
    proc: subprocess.Popen[str],
    request_id: str,
    method: str,
    params: dict[str, Any] | None = None,
) -> None:
    if proc.stdin is None:
        raise WakeError("app-server stdin is unavailable")
    message: dict[str, Any] = {"id": request_id, "method": method}
    if params is not None:
        message["params"] = params
    proc.stdin.write(json.dumps(message, separators=(",", ":")) + "\n")
    proc.stdin.flush()


def _recent_stderr(stderr_tail: deque[str]) -> str:
    if not stderr_tail:
        return ""
    return "\nrecent app-server stderr:\n" + "\n".join(stderr_tail)


def _read_json_line(
    proc: subprocess.Popen[str],
    deadline: float,
    stderr_tail: deque[str],
) -> dict[str, Any]:
    streams = [stream for stream in (proc.stdout, proc.stderr) if stream is not None]
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise WakeError(
                "timed out waiting for app-server protocol response"
                + _recent_stderr(stderr_tail),
                TIMEOUT,
            )
        if proc.poll() is not None:
            raise WakeError(
                f"app-server exited before the wake completed (exit {proc.returncode})"
                + _recent_stderr(stderr_tail)
            )
        readable, _, _ = select.select(streams, [], [], min(1.0, remaining))
        for stream in readable:
            line = stream.readline()
            if not line:
                continue
            if stream is proc.stderr:
                stderr_tail.append(line.rstrip())
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise WakeError(
                    f"invalid app-server JSON: {exc}: {line.rstrip()}"
                ) from exc
            if not isinstance(payload, dict):
                raise WakeError(f"unexpected app-server payload: {payload!r}")
            return payload


def _request(
    proc: subprocess.Popen[str],
    request_id: str,
    method: str,
    params: dict[str, Any] | None,
    deadline: float,
    stderr_tail: deque[str],
) -> dict[str, Any]:
    _send(proc, request_id, method, params)
    while True:
        payload = _read_json_line(proc, deadline, stderr_tail)
        if payload.get("method") in APPROVAL_REQUEST_METHODS:
            raise WakeError(
                f"turn requested interactive approval via {payload['method']}; "
                "the scheduler helper cannot answer that request"
            )
        if payload.get("id") != request_id:
            continue
        if "error" in payload:
            raise WakeError(f"app-server {method} failed: {payload['error']!r}")
        result = payload.get("result")
        if not isinstance(result, dict):
            raise WakeError(f"app-server {method} returned no object result")
        return result


def _status_type(thread: dict[str, Any]) -> str:
    status = thread.get("status")
    if isinstance(status, dict):
        status_type = status.get("type")
        if isinstance(status_type, str):
            return status_type
    return "unknown"


def _wait_for_turn_idle(
    proc: subprocess.Popen[str],
    thread_id: str,
    turn_id: str,
    deadline: float,
    stderr_tail: deque[str],
) -> None:
    while True:
        payload = _read_json_line(proc, deadline, stderr_tail)
        method = payload.get("method")
        if method in APPROVAL_REQUEST_METHODS:
            raise WakeError(
                f"turn requested interactive approval via {method}; "
                "the scheduler helper cannot answer that request"
            )
        if method == "turn/completed":
            params = payload.get("params")
            turn = params.get("turn") if isinstance(params, dict) else None
            if isinstance(turn, dict) and turn.get("id") == turn_id:
                return
        if method == "thread/status/changed":
            params = payload.get("params")
            if not isinstance(params, dict) or params.get("threadId") != thread_id:
                continue
            status = params.get("status")
            if not isinstance(status, dict):
                continue
            status_type = status.get("type")
            if status_type == "idle":
                return
            if status_type == "systemError":
                raise WakeError(f"thread entered systemError: {status!r}")


def _terminate(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def wake_thread(args: argparse.Namespace) -> None:
    repo = Path(args.repo).expanduser().resolve()
    if args.timeout <= 0:
        raise WakeError("--timeout must be positive")

    stderr_tail: deque[str] = deque(maxlen=20)
    proc = subprocess.Popen(
        [args.codex_bin, "app-server", "--stdio"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    deadline = time.monotonic() + args.timeout
    try:
        _request(
            proc,
            "1",
            "initialize",
            {
                "clientInfo": {
                    "name": "registry-cos-scheduler",
                    "version": "0",
                },
                "capabilities": {
                    "experimentalApi": True,
                    "requestAttestation": False,
                    "optOutNotificationMethods": OPT_OUT_NOTIFICATION_METHODS,
                },
            },
            deadline,
            stderr_tail,
        )
        resume = _request(
            proc,
            "2",
            "thread/resume",
            {
                "threadId": args.thread,
                "excludeTurns": True,
                "cwd": str(repo),
                "approvalsReviewer": args.approvals_reviewer,
            },
            deadline,
            stderr_tail,
        )
        thread = resume.get("thread")
        if not isinstance(thread, dict):
            raise WakeError("thread/resume returned no thread")
        status_type = _status_type(thread)
        if status_type != "idle":
            raise WakeError(
                f"thread {args.thread} is {status_type}; skipping overlapping wake",
                THREAD_BUSY,
            )

        start = _request(
            proc,
            "3",
            "turn/start",
            {
                "threadId": args.thread,
                "input": [
                    {
                        "type": "text",
                        "text": args.prompt,
                        "text_elements": [],
                    }
                ],
                "cwd": str(repo),
                "approvalPolicy": args.approval_policy,
                "approvalsReviewer": args.approvals_reviewer,
                "responsesapiClientMetadata": {
                    "source": "registry-cos-scheduler",
                },
            },
            deadline,
            stderr_tail,
        )
        turn = start.get("turn")
        if not isinstance(turn, dict) or not isinstance(turn.get("id"), str):
            raise WakeError("turn/start returned no turn id")
        _wait_for_turn_idle(proc, args.thread, turn["id"], deadline, stderr_tail)
    finally:
        _terminate(proc)


def main() -> int:
    args = _parse_args()
    try:
        wake_thread(args)
    except WakeError as exc:
        if exc.exit_code != THREAD_BUSY:
            print(f"cos-app-server-wake: {exc}", file=sys.stderr)
        return exc.exit_code
    except OSError as exc:
        print(f"cos-app-server-wake: failed to launch codex: {exc}", file=sys.stderr)
        return SETUP_ERROR
    return 0


if __name__ == "__main__":
    sys.exit(main())
