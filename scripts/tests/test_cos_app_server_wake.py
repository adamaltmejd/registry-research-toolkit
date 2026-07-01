"""Tests for the app-server chief-of-staff wake helper."""

from __future__ import annotations

import json
import subprocess
import textwrap
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "cos_app_server_wake.py"


def _exe(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    path.chmod(0o755)
    return path


def _fake_codex(path: Path, body: str) -> Path:
    return _exe(
        path,
        "#!/usr/bin/env python3\nfrom __future__ import annotations\n" + body,
    )


def test_app_server_wake_starts_turn_and_stays_quiet(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    messages_path = tmp_path / "messages.jsonl"
    codex = _fake_codex(
        tmp_path / "codex",
        textwrap.dedent(
            f"""
            import json
            import sys

            messages_path = {str(messages_path)!r}
            with open(messages_path, "w", encoding="utf-8") as log:
                for line in sys.stdin:
                    request = json.loads(line)
                    log.write(json.dumps(request, sort_keys=True) + "\\n")
                    log.flush()
                    method = request["method"]
                    if method == "initialized":
                        continue
                    elif method == "initialize":
                        response = {{"id": request["id"], "result": {{"codexHome": "/tmp/codex"}}}}
                    elif method == "thread/resume":
                        response = {{
                            "id": request["id"],
                            "result": {{
                                "thread": {{
                                    "id": request["params"]["threadId"],
                                }}
                            }},
                        }}
                    elif method == "turn/start":
                        response = {{
                            "id": request["id"],
                            "result": {{
                                "turn": {{
                                    "id": "turn-1",
                                    "status": "inProgress",
                                    "items": [],
                                    "itemsView": "notLoaded",
                                }}
                            }},
                        }}
                    else:
                        response = {{"id": request["id"], "error": {{"message": method}}}}
                    print(json.dumps(response), flush=True)
                    if method == "turn/start":
                        print(
                            json.dumps(
                                {{
                                    "method": "turn/completed",
                                    "params": {{
                                        "threadId": request["params"]["threadId"],
                                        "turn": {{
                                            "id": "turn-1",
                                            "status": "completed",
                                            "error": None,
                                        }},
                                    }},
                                }}
                            ),
                            flush=True,
                        )
                        break
            """
        ),
    )

    result = subprocess.run(
        [
            str(SCRIPT),
            "--repo",
            str(repo),
            "--thread",
            "thread-1",
            "--prompt",
            "Run one COS tick",
            "--codex-bin",
            str(codex),
            "--timeout",
            "5",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert result.stdout == ""
    assert result.stderr == ""
    messages = [
        json.loads(line)
        for line in messages_path.read_text(encoding="utf-8").splitlines()
    ]
    assert [message["method"] for message in messages] == [
        "initialize",
        "initialized",
        "thread/resume",
        "turn/start",
    ]
    assert messages[3]["params"]["input"] == [
        {"type": "text", "text": "Run one COS tick", "text_elements": []}
    ]
    assert messages[3]["params"]["cwd"] == str(repo.resolve())
    assert "approvalPolicy" not in messages[3]["params"]
    assert messages[3]["params"]["approvalsReviewer"] == "auto_review"


def test_app_server_wake_skips_busy_thread(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    messages_path = tmp_path / "messages.jsonl"
    codex = _fake_codex(
        tmp_path / "codex",
        textwrap.dedent(
            f"""
            import json
            import sys

            messages_path = {str(messages_path)!r}
            with open(messages_path, "w", encoding="utf-8") as log:
                for line in sys.stdin:
                    request = json.loads(line)
                    log.write(json.dumps(request, sort_keys=True) + "\\n")
                    log.flush()
                    if request["method"] == "initialized":
                        continue
                    elif request["method"] == "initialize":
                        response = {{"id": request["id"], "result": {{}}}}
                    else:
                        response = {{
                            "id": request["id"],
                            "result": {{
                                "thread": {{
                                    "id": request["params"]["threadId"],
                                    "status": {{"type": "active", "activeFlags": []}},
                                }}
                            }},
                        }}
                    print(json.dumps(response), flush=True)
                    if request["method"] == "thread/resume":
                        break
            """
        ),
    )

    result = subprocess.run(
        [
            str(SCRIPT),
            "--repo",
            str(repo),
            "--thread",
            "thread-1",
            "--prompt",
            "Run one COS tick",
            "--codex-bin",
            str(codex),
            "--timeout",
            "5",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 75
    assert result.stdout == ""
    assert result.stderr == ""
    methods = [
        json.loads(line)["method"]
        for line in messages_path.read_text(encoding="utf-8").splitlines()
    ]
    assert methods == ["initialize", "initialized", "thread/resume"]


def test_app_server_wake_fails_on_interactive_approval_request(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    codex = _fake_codex(
        tmp_path / "codex",
        textwrap.dedent(
            """
            import json
            import sys

            for line in sys.stdin:
                request = json.loads(line)
                method = request["method"]
                if method == "initialized":
                    continue
                elif method == "initialize":
                    response = {"id": request["id"], "result": {}}
                elif method == "thread/resume":
                    response = {
                        "id": request["id"],
                        "result": {
                            "thread": {
                                "id": request["params"]["threadId"],
                                "status": {"type": "idle"},
                            }
                        },
                    }
                else:
                    response = {
                        "id": request["id"],
                        "result": {"turn": {"id": "turn-1"}},
                    }
                print(json.dumps(response), flush=True)
                if method == "turn/start":
                    print(
                        json.dumps(
                            {
                                "id": "approval-1",
                                "method": "tool/requestUserInput",
                                "params": {},
                            }
                        ),
                        flush=True,
                    )
                    break
            """
        ),
    )

    result = subprocess.run(
        [
            str(SCRIPT),
            "--repo",
            str(repo),
            "--thread",
            "thread-1",
            "--prompt",
            "Run one COS tick",
            "--codex-bin",
            str(codex),
            "--timeout",
            "5",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "interactive approval" in result.stderr


def test_app_server_wake_fails_when_turn_fails(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    codex = _fake_codex(
        tmp_path / "codex",
        textwrap.dedent(
            """
            import json
            import sys

            for line in sys.stdin:
                request = json.loads(line)
                method = request["method"]
                if method == "initialized":
                    continue
                if method == "initialize":
                    response = {"id": request["id"], "result": {}}
                elif method == "thread/resume":
                    response = {
                        "id": request["id"],
                        "result": {
                            "thread": {
                                "id": request["params"]["threadId"],
                                "status": {"type": "idle"},
                            }
                        },
                    }
                else:
                    response = {
                        "id": request["id"],
                        "result": {"turn": {"id": "turn-1", "status": "inProgress"}},
                    }
                sys.stdout.write(json.dumps(response) + "\\n")
                if method == "turn/start":
                    sys.stdout.write(
                        json.dumps(
                            {
                                "method": "turn/completed",
                                "params": {
                                    "threadId": request["params"]["threadId"],
                                    "turn": {
                                        "id": "turn-1",
                                        "status": "failed",
                                        "error": {"message": "boom"},
                                    },
                                },
                            }
                        )
                        + "\\n"
                    )
                    sys.stdout.flush()
                    break
                sys.stdout.flush()
            """
        ),
    )

    result = subprocess.run(
        [
            str(SCRIPT),
            "--repo",
            str(repo),
            "--thread",
            "thread-1",
            "--prompt",
            "Run one COS tick",
            "--codex-bin",
            str(codex),
            "--timeout",
            "5",
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "turn ended with failed" in result.stderr
