#!/usr/bin/env python3
"""Wake an existing Codex thread through the app-server protocol."""

from __future__ import annotations

import argparse
import json
import os
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
    "tool/requestUserInput",
}

APPROVAL_POLICY_ALIASES = {
    "unlessTrusted": "untrusted",
    "onRequest": "on-request",
    "onFailure": "on-failure",
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
        default="inherit",
        choices=[
            "inherit",
            "untrusted",
            "on-failure",
            "on-request",
            "never",
            "unlessTrusted",
            "onRequest",
            "onFailure",
        ],
        help="Optional turn approval policy override. Defaults to inheriting the thread setting.",
    )
    parser.add_argument(
        "--approvals-reviewer",
        default="auto_review",
        choices=["user", "auto_review", "guardian_subagent"],
        help="Approval reviewer for unattended turns. Defaults to auto_review.",
    )
    args = parser.parse_args()
    args.approval_policy = APPROVAL_POLICY_ALIASES.get(
        args.approval_policy, args.approval_policy
    )
    return args


def _send(
    proc: subprocess.Popen[bytes],
    request_id: str,
    method: str,
    params: dict[str, Any] | None = None,
) -> None:
    if proc.stdin is None:
        raise WakeError("app-server stdin is unavailable")
    message: dict[str, Any] = {"id": request_id, "method": method}
    if params is not None:
        message["params"] = params
    proc.stdin.write(json.dumps(message, separators=(",", ":")).encode() + b"\n")
    proc.stdin.flush()


def _notify(proc: subprocess.Popen[bytes], method: str) -> None:
    if proc.stdin is None:
        raise WakeError("app-server stdin is unavailable")
    proc.stdin.write(
        json.dumps({"method": method}, separators=(",", ":")).encode() + b"\n"
    )
    proc.stdin.flush()


def _recent_stderr(stderr_tail: deque[str]) -> str:
    if not stderr_tail:
        return ""
    return "\nrecent app-server stderr:\n" + "\n".join(stderr_tail)


class ProtocolReader:
    def __init__(
        self,
        proc: subprocess.Popen[bytes],
        deadline: float,
        stderr_tail: deque[str],
    ) -> None:
        if proc.stdout is None or proc.stderr is None:
            raise WakeError("app-server stdout/stderr are unavailable")
        self.proc = proc
        self.deadline = deadline
        self.stderr_tail = stderr_tail
        self.stdout_fd = proc.stdout.fileno()
        self.stderr_fd = proc.stderr.fileno()
        self.open_fds = {self.stdout_fd, self.stderr_fd}
        self.buffers = {
            self.stdout_fd: bytearray(),
            self.stderr_fd: bytearray(),
        }

    def read_json_line(self) -> dict[str, Any]:
        while True:
            line = self._pop_line(self.stdout_fd)
            if line is not None:
                try:
                    payload = json.loads(line.decode())
                except json.JSONDecodeError as exc:
                    raise WakeError(
                        f"invalid app-server JSON: {exc}: {line.decode(errors='replace')}"
                    ) from exc
                if not isinstance(payload, dict):
                    raise WakeError(f"unexpected app-server payload: {payload!r}")
                return payload
            self._drain_stderr_lines()

            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                raise WakeError(
                    "timed out waiting for app-server protocol response"
                    + _recent_stderr(self.stderr_tail),
                    TIMEOUT,
                )
            if self.proc.poll() is not None:
                raise WakeError(
                    f"app-server exited before the wake completed (exit {self.proc.returncode})"
                    + _recent_stderr(self.stderr_tail)
                )
            readable, _, _ = select.select(
                list(self.open_fds), [], [], min(1.0, remaining)
            )
            for fd in readable:
                chunk = os.read(fd, 65536)
                if not chunk:
                    self.open_fds.discard(fd)
                    continue
                self.buffers[fd].extend(chunk)

    def _pop_line(self, fd: int) -> bytes | None:
        buffer = self.buffers[fd]
        try:
            newline_index = buffer.index(b"\n")
        except ValueError:
            return None
        line = bytes(buffer[:newline_index])
        del buffer[: newline_index + 1]
        return line

    def _drain_stderr_lines(self) -> None:
        while True:
            line = self._pop_line(self.stderr_fd)
            if line is None:
                return
            self.stderr_tail.append(line.decode(errors="replace").rstrip())


def _request(
    proc: subprocess.Popen[bytes],
    reader: ProtocolReader,
    request_id: str,
    method: str,
    params: dict[str, Any] | None,
) -> dict[str, Any]:
    _send(proc, request_id, method, params)
    while True:
        payload = reader.read_json_line()
        _raise_on_unattended_request(payload)
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
    reader: ProtocolReader,
    thread_id: str,
    turn_id: str,
) -> None:
    while True:
        payload = reader.read_json_line()
        method = payload.get("method")
        _raise_on_unattended_request(payload)
        if method == "turn/completed":
            params = payload.get("params")
            turn = params.get("turn") if isinstance(params, dict) else None
            if isinstance(turn, dict) and turn.get("id") == turn_id:
                status_type = _turn_status_type(turn)
                if status_type == "completed":
                    return
                if status_type in {"failed", "interrupted"}:
                    raise WakeError(
                        f"turn ended with {status_type}: {turn.get('error')!r}"
                    )
                raise WakeError(f"turn completed with unexpected status: {turn!r}")
        if method == "thread/status/changed":
            params = payload.get("params")
            if not isinstance(params, dict) or params.get("threadId") != thread_id:
                continue
            status = params.get("status")
            if not isinstance(status, dict):
                continue
            status_type = status.get("type")
            if status_type == "systemError":
                raise WakeError(f"thread entered systemError: {status!r}")


def _raise_on_unattended_request(payload: dict[str, Any]) -> None:
    method = payload.get("method")
    if not isinstance(method, str):
        return
    if method in APPROVAL_REQUEST_METHODS:
        raise WakeError(
            f"turn requested interactive approval via {method}; "
            "the scheduler helper cannot answer that request"
        )
    if "id" in payload:
        raise WakeError(
            f"turn requested unsupported app-server request via {method}; "
            "the scheduler helper cannot answer that request"
        )


def _turn_status_type(turn: dict[str, Any]) -> str:
    status = turn.get("status")
    if isinstance(status, str):
        return status
    if isinstance(status, dict):
        status_type = status.get("type")
        if isinstance(status_type, str):
            return status_type
    return "unknown"


def _terminate(proc: subprocess.Popen[bytes]) -> None:
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
        bufsize=0,
    )
    deadline = time.monotonic() + args.timeout
    reader = ProtocolReader(proc, deadline, stderr_tail)
    try:
        _request(
            proc,
            reader,
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
        )
        _notify(proc, "initialized")
        resume = _request(
            proc,
            reader,
            "2",
            "thread/resume",
            {
                "threadId": args.thread,
                "excludeTurns": True,
                "cwd": str(repo),
                "approvalsReviewer": args.approvals_reviewer,
            },
        )
        thread = resume.get("thread")
        if not isinstance(thread, dict):
            raise WakeError("thread/resume returned no thread")
        status_type = _status_type(thread)
        if status_type == "systemError":
            raise WakeError(f"thread {args.thread} is systemError")
        if status_type not in {"idle", "unknown"}:
            raise WakeError(
                f"thread {args.thread} is {status_type}; skipping overlapping wake",
                THREAD_BUSY,
            )

        turn_start_params: dict[str, Any] = {
            "threadId": args.thread,
            "input": [
                {
                    "type": "text",
                    "text": args.prompt,
                    "text_elements": [],
                }
            ],
            "cwd": str(repo),
            "approvalsReviewer": args.approvals_reviewer,
            "responsesapiClientMetadata": {
                "source": "registry-cos-scheduler",
            },
        }
        if args.approval_policy != "inherit":
            turn_start_params["approvalPolicy"] = args.approval_policy
        start = _request(
            proc,
            reader,
            "3",
            "turn/start",
            turn_start_params,
        )
        turn = start.get("turn")
        if not isinstance(turn, dict) or not isinstance(turn.get("id"), str):
            raise WakeError("turn/start returned no turn id")
        _wait_for_turn_idle(reader, args.thread, turn["id"])
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
