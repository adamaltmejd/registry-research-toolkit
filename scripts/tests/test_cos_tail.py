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
    # History is gone, and so is the SUFFIX of the line the skip landed inside —
    # emitting "-tail" as a standalone line would render a corrupt fragment.
    assert follower.poll() == ["new"]


def test_follower_drain_tail_flushes_unterminated_line(tmp_path: Path) -> None:
    log = tmp_path / "lane.log"
    log.write_bytes(b"complete\nno-newline-final")
    follower = ct.LogFollower(log)
    assert follower.poll() == ["complete"]
    assert follower.drain_tail() == "no-newline-final"
    assert follower.drain_tail() is None  # flushed exactly once
    (tmp_path / "empty.log").write_bytes(b"line\n   ")
    follower = ct.LogFollower(tmp_path / "empty.log")
    follower.poll()
    assert follower.drain_tail() is None  # whitespace-only tail is not a line


def test_follower_skip_at_line_boundary_keeps_next_line(tmp_path: Path) -> None:
    log = tmp_path / "lane.log"
    log.write_bytes(b"old\n")  # writer is at a clean boundary, nothing is torn
    follower = ct.LogFollower(log)
    follower.skip_to_end()
    with log.open("ab") as fh:
        fh.write(b"new\n")
    assert follower.poll() == ["new"]


def test_follower_drain_discards_torn_suffix(tmp_path: Path) -> None:
    log = tmp_path / "lane.log"
    log.write_bytes(b"torn-partial")
    follower = ct.LogFollower(log)
    follower.skip_to_end()
    with log.open("ab") as fh:
        fh.write(b"-suffix")  # lane ends before the newline ever arrives
    follower.poll()
    assert follower.drain_tail() is None  # a fragment, not a line


def test_multiline_command_collapses_to_bounded_headline() -> None:
    heredoc = "python - <<'EOF'\n" + "x = 1\n" * 80 + "EOF"
    rendered = _render(
        _item(
            {
                "type": "command_execution",
                "command": heredoc,
                "aggregated_output": "",
                "exit_code": 0,
                "status": "completed",
            }
        )
    )
    assert rendered is not None
    assert "\n" not in rendered  # one-line contract holds for heredocs
    assert len(rendered) < ct.MAX_COMMAND_CHARS + 20
    assert "…" in rendered


def test_current_run_offset_finds_last_run(tmp_path: Path) -> None:
    log = tmp_path / "lane.log"
    run1 = b'{"type":"thread.started","thread_id":"a"}\n{"type":"turn.started"}\n'
    run2 = b'{"type":"thread.started","thread_id":"b"}\n'
    log.write_bytes(run1 + run2)
    assert ct.current_run_offset(log) == len(run1)
    log.write_bytes(run1)  # single run: current run starts at 0
    assert ct.current_run_offset(log) == 0
    assert ct.current_run_offset(tmp_path / "missing.log") == 0
    # A retry can append its marker right after a prior run's UNTERMINATED tail;
    # the marker must be found without a preceding newline.
    torn = b'{"type":"thread.started","thread_id":"a"}\ntorn-final-no-newline'
    log.write_bytes(torn + run2)
    assert ct.current_run_offset(log) == len(torn)


def test_current_run_offset_prefers_cos_run_sentinel(tmp_path: Path) -> None:
    log = tmp_path / "lane.log"
    old = b'{"type":"thread.started","thread_id":"old"}\nold output\n'
    sentinel = (
        b'{"type":"cos.run.started","slug":"lane","surface":"claude",'
        b'"dispatched":"2026-07-03T00:00:00+00:00"}\n'
    )
    log.write_bytes(old + sentinel + b"plain claude output\n")
    assert ct.current_run_offset(log) == len(old)


def test_lane_is_plain_sniffs_past_leading_stderr(tmp_path: Path) -> None:
    codex_log = tmp_path / "codex.log"
    codex_log.write_bytes(
        b"Reading additional input from stdin...\n"
        b"warning: VIRTUAL_ENV mismatch\n"
        b'{"type":"thread.started","thread_id":"a"}\n'
    )
    assert ct.lane_is_plain(None, codex_log) is False  # stderr prologue, still codex
    plain_log = tmp_path / "plain.log"
    plain_log.write_bytes(b"just narration\nmore text\n")
    assert ct.lane_is_plain(None, plain_log) is True
    assert ct.lane_is_plain("codex", plain_log) is False  # surface wins over sniff
    assert ct.lane_is_plain("claude", codex_log) is True


def test_lane_is_plain_ignores_claude_run_sentinel(tmp_path: Path) -> None:
    plain_log = tmp_path / "done-claude.log"
    plain_log.write_text(
        '{"type":"cos.run.started","slug":"done-claude","surface":"claude"}\n'
        'plain narration\n{"type":"item.started"}\n',
        encoding="utf-8",
    )
    assert ct.lane_is_plain(None, plain_log) is True


def test_lane_is_plain_uses_codex_run_sentinel_without_slot(tmp_path: Path) -> None:
    codex_log = tmp_path / "done-codex.log"
    codex_log.write_text(
        '{"type":"cos.run.started","slug":"done-codex","surface":"codex"}\n'
        '{"type":"item.completed","item":{"type":"agent_message","text":"done"}}\n',
        encoding="utf-8",
    )
    assert ct.lane_is_plain(None, codex_log) is False


def test_lane_is_plain_sniffs_current_reused_run_surface(tmp_path: Path) -> None:
    reused_log = tmp_path / "reused.log"
    reused_log.write_text(
        '{"type":"thread.started","thread_id":"old-codex"}\n'
        '{"type":"item.completed","item":{"type":"agent_message","text":"old"}}\n'
        '{"type":"cos.run.started","slug":"reused","surface":"claude"}\n'
        "plain claude output\n",
        encoding="utf-8",
    )
    assert ct.lane_is_plain(None, reused_log) is True


def test_follow_from_run_start_skips_prior_runs(tmp_path: Path, capsys) -> None:
    logs_root = tmp_path / "dispatch-logs"
    logs_root.mkdir(parents=True)
    msg = '{"type":"item.completed","item":{"type":"agent_message","text":"%s"}}'
    (logs_root / "reused.log").write_text(
        '{"type":"thread.started","thread_id":"a"}\n' + msg % "old-run" + "\n"
        '{"type":"thread.started","thread_id":"b"}\n' + msg % "new-run" + "\n",
        encoding="utf-8",
    )
    rc = ct.main(
        ["--state-root", str(tmp_path), "--no-color", "follow", "reused"]
        + ["--from-run-start"]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "new-run" in out
    assert "old-run" not in out  # a reused slug's prior run is not replayed


def test_claude_lane_passes_json_looking_lines_through(tmp_path: Path, capsys) -> None:
    # claude -p logs are plain text; a line that happens to be valid JSON must not
    # be parsed as a codex event (which would suppress or reshape it).
    slots_root = tmp_path / "pipeline-slots"
    logs_root = tmp_path / "dispatch-logs"
    logs_root.mkdir(parents=True)
    _write_slot(slots_root, "claude-lane", surface="claude")
    (logs_root / "claude-lane.log").write_text(
        'plain narration\n{"type":"item.started"}\n', encoding="utf-8"
    )
    renderer = ct.Renderer(color=False)
    followers: dict[str, ct.LogFollower] = {}
    ct._follow_all_tick(
        slots_root,
        logs_root,
        renderer,
        followers,
        {},
        {},
        raw=False,
        startup_mode="start",
    )
    out = capsys.readouterr().out
    assert '{"type":"item.started"}' in out  # verbatim, not suppressed


def test_list_slots_tolerates_malformed_optional_fields(tmp_path: Path) -> None:
    slots_root = tmp_path / "pipeline-slots"
    _write_slot(slots_root, "bad-fields", issues=1011, prs="42")
    slots = ct.list_slots(slots_root)
    assert slots[0].issues == () and slots[0].prs == ()
    # status must render, not crash, on the malformed ledger entry
    assert "bad-fields" in ct.status_table(slots, tmp_path / "dispatch-logs", now=0.0)


def test_tmux_session_identity_per_state_root(tmp_path: Path) -> None:
    assert ct.tmux_session(None) == "cos"
    override = ct.tmux_session(tmp_path)
    assert override != "cos" and override.startswith("cos-")
    assert ct.tmux_session(tmp_path) == override  # stable
    assert ct.tmux_window(tmp_path) == f"{override}:lanes"


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


def test_follow_exits_after_lane_release(tmp_path: Path, capsys) -> None:
    # A released lane's follower must render the full log, note the release, and
    # EXIT — a lingering follower would print a reused slug's next run into a
    # done: pane (Codex P2 on #1047).
    logs_root = tmp_path / "dispatch-logs"
    logs_root.mkdir(parents=True)
    (logs_root / "gone-lane.log").write_text(
        '{"type":"item.completed","item":{"type":"agent_message","text":"done"}}\n'
        "final stderr without newline",
        encoding="utf-8",
    )
    rc = ct.main(
        ["--state-root", str(tmp_path), "--no-color", "follow", "gone-lane"]
        + ["--from-start"]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "done" in out
    assert "final stderr without newline" in out  # unterminated tail is flushed
    assert "released (lane ended)" in out


def test_follow_all_tick_prunes_and_resumes_reused_slug(tmp_path: Path, capsys) -> None:
    # A released lane is drained (incl. unterminated tail) and pruned; when its
    # slug is reused, the new follower resumes past the prior run's bytes in the
    # append-only per-slug log (Codex P2s on #1047).
    slots_root = tmp_path / "pipeline-slots"
    logs_root = tmp_path / "dispatch-logs"
    logs_root.mkdir(parents=True)
    renderer = ct.Renderer(color=False)
    followers: dict[str, ct.LogFollower] = {}
    resume_pos: dict[str, int] = {}
    plain_lanes: dict[str, bool] = {}

    def tick() -> str:
        ct._follow_all_tick(
            slots_root,
            logs_root,
            renderer,
            followers,
            resume_pos,
            plain_lanes,
            raw=True,
            startup_mode="start",
        )
        return capsys.readouterr().out

    _write_slot(slots_root, "lane-a")
    log = logs_root / "lane-a.log"
    log.write_text("run1-line\nrun1-tail-no-newline", encoding="utf-8")
    out = tick()
    assert "== following lane-a ==" in out and "run1-line" in out

    (slots_root / "lane-a.json").unlink()  # slot released
    out = tick()
    assert "run1-tail-no-newline" in out
    assert "slot released (lane ended)" in out
    assert followers == {}

    _write_slot(slots_root, "lane-a")  # slug reused by a later dispatch
    with log.open("a", encoding="utf-8") as fh:
        fh.write("\nrun2-line\n")
    out = tick()
    assert "run2-line" in out
    assert "run1-line" not in out  # prior run's bytes are not replayed


def test_status_command_smoke(tmp_path: Path, capsys) -> None:
    _write_slot(tmp_path / "pipeline-slots", "auto-codex-issue-7")
    assert ct.main(["--state-root", str(tmp_path), "--no-color", "status"]) == 0
    assert "auto-codex-issue-7" in capsys.readouterr().out
