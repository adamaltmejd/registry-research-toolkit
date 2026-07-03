"""Unit tests for scripts/cos_tail.py.

Pins the viewer's display contracts: the rendering policy (agent messages in full,
commands as one dim line with output only on failure, unknown item types as a dim
headline, non-JSON lines passed through), the incremental follower's torn-line
buffering and truncation reset, the slot listing's reuse of the ledger read protocol,
and the tmux manager's pure pane-diff core. No tmux and no live processes — the
orchestration loops themselves are thin shells over these units.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location("cos_tail", _SCRIPTS / "cos_tail.py")
assert _SPEC and _SPEC.loader
ct = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = ct
_SPEC.loader.exec_module(ct)


def _render(payload: dict, **kwargs) -> str | None:
    return ct.Renderer(color=False, **kwargs).render(json.dumps(payload))


def _item(item: dict) -> dict:
    return {"type": "item.completed", "item": item}


# ---------------------------------------------------------------------------
# Rendering policy


def test_agent_message_rendered_in_full() -> None:
    text = "panache lint passed.\nNow fixing the two wrapping differences."
    assert _render(_item({"type": "agent_message", "text": text})) == text


def test_successful_command_is_one_line_without_output() -> None:
    rendered = _render(
        _item(
            {
                "type": "command_execution",
                "command": "/bin/zsh -lc 'git branch --show-current'",
                "aggregated_output": "wt/auto-codex-issue-904\n",
                "exit_code": 0,
                "status": "completed",
            }
        )
    )
    assert rendered == "$ git branch --show-current → rc=0"


def test_failed_command_appends_output_tail() -> None:
    output = "\n".join(f"line{i}" for i in range(30))
    rendered = _render(
        _item(
            {
                "type": "command_execution",
                "command": "/bin/zsh -lc 'uv run pytest'",
                "aggregated_output": output,
                "exit_code": 1,
                "status": "completed",
            }
        )
    )
    assert rendered is not None
    lines = rendered.splitlines()
    assert lines[0] == "$ uv run pytest → rc=1"
    # Only the tail is shown, indented, capped at FAIL_TAIL_LINES.
    assert lines[1:] == [f"  line{i}" for i in range(30 - ct.FAIL_TAIL_LINES, 30)]


def test_verbose_prints_output_even_on_success() -> None:
    rendered = _render(
        _item(
            {
                "type": "command_execution",
                "command": "ls",
                "aggregated_output": "a\nb\n",
                "exit_code": 0,
                "status": "completed",
            }
        ),
        verbose=True,
    )
    assert rendered == "$ ls → rc=0\n  a\n  b"


def test_null_exit_code_renders_as_unknown() -> None:
    rendered = _render(
        _item(
            {
                "type": "command_execution",
                "command": "ls",
                "aggregated_output": "",
                "exit_code": None,
                "status": "completed",
            }
        )
    )
    assert rendered == "$ ls → rc=?"


def test_file_change_lists_repo_relative_paths() -> None:
    rendered = _render(
        _item(
            {
                "type": "file_change",
                "changes": [
                    {
                        "path": "/x/.claude/worktrees/auto-codex-issue-904/"
                        "reg_webapp/frontend/src/lib/Picker.svelte",
                        "kind": "update",
                    }
                ],
                "status": "completed",
            }
        )
    )
    assert rendered == "✎ update reg_webapp/frontend/src/lib/Picker.svelte"


def test_unknown_item_type_degrades_to_dim_headline() -> None:
    rendered = _render(
        _item({"type": "reasoning", "text": "First line of thought.\nMore text."})
    )
    assert rendered == "— reasoning: First line of thought."


def test_item_started_and_todo_list_are_suppressed() -> None:
    renderer = ct.Renderer(color=False)
    started = {"type": "item.started", "item": {"type": "command_execution"}}
    assert renderer.render(json.dumps(started)) is None
    assert _render(_item({"type": "todo_list", "items": []})) is None


def test_turn_and_thread_events_render_as_markers() -> None:
    renderer = ct.Renderer(color=False)
    assert renderer.render('{"type":"thread.started","thread_id":"abc"}') == (
        "— thread abc"
    )
    assert renderer.render(
        '{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":2}}'
    ) == ("— turn completed (in 10, out 2)")
    failed = renderer.render('{"type":"turn.failed"}')
    assert failed is not None and failed.startswith("✗ turn.failed")


def test_non_json_lines_pass_through() -> None:
    renderer = ct.Renderer(color=False)
    line = "warning: `VIRTUAL_ENV=...` does not match the project environment"
    assert renderer.render(line) == line
    assert renderer.render("   \n") is None


def test_color_codes_only_when_enabled() -> None:
    line = '{"type":"turn.started"}'
    assert "\x1b[" in ct.Renderer(color=True).render(line)
    assert "\x1b[" not in ct.Renderer(color=False).render(line)


def test_strip_shell_wrapper_variants() -> None:
    assert ct.strip_shell_wrapper("/bin/zsh -lc 'echo hi'") == "echo hi"
    assert ct.strip_shell_wrapper('/bin/bash -c "echo hi"') == "echo hi"
    assert ct.strip_shell_wrapper("plain command") == "plain command"
    assert ct.strip_shell_wrapper("/bin/zsh -l -c 'echo hi'") == "echo hi"
    # Multi-line payloads inside the wrapper survive.
    assert ct.strip_shell_wrapper("/bin/zsh -lc 'a\nb'") == "a\nb"


def test_short_slug() -> None:
    assert ct.short_slug("auto-codex-issue-1033") == "issue-1033"
    assert ct.short_slug("wizardly-mahavira-ecebe4") == "wizardly-mahavira-ecebe4"


# ---------------------------------------------------------------------------
# Incremental follower


def test_follower_buffers_torn_lines(tmp_path: Path) -> None:
    log = tmp_path / "lane.log"
    follower = ct.LogFollower(log)
    assert follower.poll() == []  # missing file is quiet
    log.write_bytes(b'{"type":"turn.started"}\n{"type":"tu')
    assert follower.poll() == ['{"type":"turn.started"}']
    with log.open("ab") as fh:
        fh.write(b'rn.completed"}\n')
    assert follower.poll() == ['{"type":"turn.completed"}']
    assert follower.poll() == []


def test_follower_skip_to_end_discards_history(tmp_path: Path) -> None:
    log = tmp_path / "lane.log"
    log.write_bytes(b"old1\nold2\npartial")
    follower = ct.LogFollower(log)
    follower.skip_to_end()
    with log.open("ab") as fh:
        fh.write(b"-tail\nnew\n")
    # History (including the torn tail it landed mid-write on) is gone; only
    # complete lines appended after the skip are yielded.
    assert follower.poll() == ["-tail", "new"]


def test_follower_resets_on_truncation(tmp_path: Path) -> None:
    log = tmp_path / "lane.log"
    log.write_bytes(b"one\ntwo\n")
    follower = ct.LogFollower(log)
    assert follower.poll() == ["one", "two"]
    log.write_bytes(b"fresh\n")  # shorter file: rotated/recreated
    assert follower.poll() == ["fresh"]


# ---------------------------------------------------------------------------
# Slot listing / status


def _write_slot(slots_root: Path, slug: str, **extra) -> None:
    slots_root.mkdir(parents=True, exist_ok=True)
    payload = {
        "slot": slug,
        "surface": "codex",
        "tier": "hard",
        "issues": [1033],
        "prs": [1042],
        "session": "abc",
        "dispatched": "2026-07-03T09:22:34+00:00",
    }
    payload.update(extra)
    (slots_root / f"{slug}.json").write_text(json.dumps(payload), encoding="utf-8")


def test_list_slots_follows_ledger_read_protocol(tmp_path: Path) -> None:
    slots_root = tmp_path / "pipeline-slots"
    _write_slot(slots_root, "auto-codex-issue-1033")
    # Stem/slot-field disagreement means the file is absent, per the gate protocol.
    _write_slot(slots_root, "mismatch", slot="other")
    slots = ct.list_slots(slots_root)
    assert [slot.slug for slot in slots] == ["auto-codex-issue-1033"]
    assert slots[0].issues == (1033,)
    assert slots[0].prs == (1042,)


def test_status_table_lists_lanes(tmp_path: Path) -> None:
    slots_root = tmp_path / "pipeline-slots"
    logs_root = tmp_path / "dispatch-logs"
    _write_slot(slots_root, "auto-codex-issue-1033")
    table = ct.status_table(ct.list_slots(slots_root), logs_root, now=0.0)
    assert "auto-codex-issue-1033" in table
    assert "#1033" in table and "#1042" in table
    assert "no log" in table  # no dispatch log written yet
    assert ct.status_table([], logs_root, now=0.0) == "no live pipeline slots"


def test_resolve_roots_override(tmp_path: Path) -> None:
    slots_root, logs_root = ct.resolve_roots(tmp_path)
    assert slots_root == tmp_path / "pipeline-slots"
    assert logs_root == tmp_path / "dispatch-logs"


# ---------------------------------------------------------------------------
# tmux manager pane diff


def test_plan_pane_actions_spawns_and_retires() -> None:
    spawn, retire = ct.plan_pane_actions(
        {"a", "b"},
        {
            "%0": "manager",  # not a lane pane: never touched
            "%1": "lane:a",
            "%2": "lane:gone",
            "%3": "done:earlier",
        },
    )
    assert spawn == ["b"]
    assert retire == ["%2"]


def test_plan_pane_actions_empty_state() -> None:
    assert ct.plan_pane_actions(set(), {}) == ([], [])


def test_self_argv_puts_global_flags_before_subcommand(tmp_path: Path) -> None:
    # Global flags after the subcommand are an argparse error, so the generated
    # pane/session commands must round-trip through the parser.
    args = ct.build_parser().parse_args(
        ["--state-root", str(tmp_path), "--interval", "1", "-v", "manage"]
    )
    argv = ct._self_argv(args, "follow", "some-slug", "--from-start")
    reparsed = ct.build_parser().parse_args(argv[2:])  # drop python + script path
    assert reparsed.command == "follow"
    assert reparsed.slug == "some-slug"
    assert reparsed.state_root == tmp_path
    assert reparsed.interval == 1.0
    assert reparsed.verbose and reparsed.from_start


# ---------------------------------------------------------------------------
# CLI validation


def test_follow_requires_exactly_one_target(tmp_path: Path, capsys) -> None:
    base = ["--state-root", str(tmp_path), "--no-color"]
    assert ct.main([*base, "follow"]) == 2
    assert ct.main([*base, "follow", "slug", "--all"]) == 2
    assert "exactly one" in capsys.readouterr().err


def test_status_command_smoke(tmp_path: Path, capsys) -> None:
    _write_slot(tmp_path / "pipeline-slots", "auto-codex-issue-7")
    assert ct.main(["--state-root", str(tmp_path), "--no-color", "status"]) == 0
    assert "auto-codex-issue-7" in capsys.readouterr().out
