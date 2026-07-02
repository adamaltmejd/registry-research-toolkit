"""Unit tests for scripts/cos_dispatch.py.

Pins the auto-dispatch launcher's contract: the ordered guards (kill switch → budget →
collision), the exact per-surface launch argv (codex `$pr-pipeline …` with the pinned
sandbox flags; claude `/pr-pipeline …` with a pre-generated --session-id), that the
worktree is materialized off a freshly fetched origin/main, that the slot file is
written LAST with slot==stem shape (so scan_slots accepts it) and only after a
successful launch, codex session-id capture from the JSONL log (plus its bounded-poll
timeout → session=null), and --dry-run's zero-side-effect check-only path.

Real subprocesses are used where the behavior IS the subprocess: a tmp git repo with an
`origin` bare remote so `git fetch origin main` + `git worktree add … origin/main` run
for real, and a stub `codex`/`claude` on a prepended PATH that records its argv+cwd and
emits canned JSONL. The reused cos_preflight slot helpers (scan_slots, default_slots_root,
DEFAULT_MAX_SLOTS) are being hoisted from cos_watch by a sibling change; a fixture stubs
them onto the loaded module handle if the hoist hasn't landed yet, so this suite is
independent of that ordering (the lead's post-assembly union verify runs against the real
hoisted helpers).
"""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "cos_dispatch", _SCRIPTS / "cos_dispatch.py"
)
assert _SPEC and _SPEC.loader
cd = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = cd
_SPEC.loader.exec_module(cd)


@pytest.fixture(autouse=True)
def _ensure_reused_helpers() -> None:
    """Backfill the cos_preflight slot helpers the sibling hoist adds, if not present.

    The contract pins reuse of cos_preflight.scan_slots / default_slots_root /
    DEFAULT_MAX_SLOTS (hoisted from cos_watch by the parallel implementer). Until that
    lands in this worktree, provide equivalents on the loaded module so the launcher and
    these tests run; once the hoist lands, the real symbols take precedence (we only set
    what's missing).
    """
    pre = cd._cos_preflight
    if not hasattr(pre, "DEFAULT_MAX_SLOTS"):
        pre.DEFAULT_MAX_SLOTS = 3
    if not hasattr(pre, "default_slots_root"):
        pre.default_slots_root = lambda: (
            pre.default_gate_root().parent / "pipeline-slots"
        )
    if not hasattr(pre, "scan_slots"):

        def _scan_slots(slots_root: Path) -> set[str]:
            slots: set[str] = set()
            for path in sorted(slots_root.glob("*.json")):
                loaded = pre._read_json_tolerant(path, "pipeline-slot file")
                if loaded is None:
                    continue
                slot = loaded[1]
                if not isinstance(slot, dict) or slot.get("slot") != path.stem:
                    continue
                slots.add(path.stem)
            return slots

        pre.scan_slots = _scan_slots


def _write_slot(slots_root: Path, slug: str) -> Path:
    slots_root.mkdir(parents=True, exist_ok=True)
    path = slots_root / f"{slug}.json"
    path.write_text(
        json.dumps({"slot": slug, "issues": [1], "prs": [], "surface": "codex"}),
        encoding="utf-8",
    )
    return path


def _make_origin(tmp_path: Path) -> Path:
    """A canonical checkout with an `origin` bare remote carrying a `main` branch."""
    bare = tmp_path / "origin.git"
    subprocess.run(["git", "init", "--bare", "-b", "main", str(bare)], check=True)
    canonical = tmp_path / "canonical"
    subprocess.run(["git", "clone", str(bare), str(canonical)], check=True)
    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@t",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@t",
    }
    (canonical / "README.md").write_text("seed\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(canonical), "add", "."], check=True, env=env)
    subprocess.run(
        ["git", "-C", str(canonical), "commit", "-m", "seed"], check=True, env=env
    )
    subprocess.run(
        ["git", "-C", str(canonical), "push", "origin", "main"], check=True, env=env
    )
    return canonical


def _stub_bin(
    tmp_path: Path, name: str, *, jsonl: str = "", exit_code: int = 0
) -> Path:
    """A stub codex/claude that records argv+cwd to <bin>/<name>.record and emits jsonl."""
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    record = bindir / f"{name}.record"
    script = bindir / name
    # Python stub: dump argv + cwd as JSON to the record file, print canned JSONL, exit.
    body = (
        "#!/usr/bin/env python3\n"
        "import json, os, sys\n"
        f"open({str(record)!r}, 'w').write(json.dumps({{'argv': sys.argv[1:], 'cwd': os.getcwd()}}))\n"
        f"sys.stdout.write({jsonl!r})\n"
        "sys.stdout.flush()\n"
        f"sys.exit({exit_code})\n"
    )
    script.write_text(body, encoding="utf-8")
    script.chmod(0o755)
    return record


def _wait_for_record(record: Path, timeout: float = 5.0) -> dict:
    """Poll for a detached stub's record file (the launch does not wait for the child)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if record.exists():
            try:
                return json.loads(record.read_text(encoding="utf-8"))
            except ValueError:
                pass  # torn write; retry
        time.sleep(0.02)
    raise AssertionError(f"stub never wrote {record}")


def _args(tmp_path: Path, canonical: Path, **overrides):
    """A dispatch Namespace with the canonical guard bypassed by default for tests."""
    import argparse

    defaults = {
        "issues": "1011",
        "surface": "codex",
        "slug": None,
        "worktree_root": None,
        "state_root": tmp_path / "state",
        "max_slots": 3,
        "canonical": canonical,
        "no_canonical_check": True,
        "dry_run": False,
    }
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


_CODEX_JSONL = (
    '{"type":"thread.started","thread_id":"019f2334-4455-70a1-bc1b-2e86d5ecfccf"}\n'
    '{"type":"turn.started"}\n'
    '{"type":"turn.completed","usage":{}}\n'
)


# --- pure helpers ---


def test_parse_issues_multi() -> None:
    assert cd.parse_issues("1011, 1012 ,1013") == [1011, 1012, 1013]


def test_parse_issues_rejects_garbage() -> None:
    with pytest.raises(SystemExit):
        cd.parse_issues("1011,abc")


def test_parse_issues_rejects_empty() -> None:
    with pytest.raises(SystemExit):
        cd.parse_issues("  ,  ")


def test_default_slug_shape() -> None:
    assert cd.default_slug("codex", [1011, 1012]) == "auto-codex-issue-1011"


@pytest.mark.parametrize("bad", ["", "a/b", "..", ".", "x/../y"])
def test_validate_slug_rejects_non_stems(bad: str) -> None:
    with pytest.raises(SystemExit):
        cd.validate_slug(bad)


def test_build_launch_argv_codex_pins_flags() -> None:
    argv = cd.build_launch_argv(
        "codex", Path("/wt/lane"), [1011, 1012], Path("/state"), None
    )
    assert argv[0:2] == ["codex", "exec"]
    assert argv[argv.index("-C") + 1] == "/wt/lane"
    assert argv[argv.index("-s") + 1] == "workspace-write"
    assert argv[argv.index("-c") + 1] == "approval_policy=never"
    assert argv[argv.index("--add-dir") + 1] == "/state"
    assert "--json" in argv
    assert argv[-1] == "$pr-pipeline 1011 1012"


def test_build_launch_argv_claude_uses_session_and_slash_prompt() -> None:
    argv = cd.build_launch_argv(
        "claude", Path("/wt/lane"), [1011], Path("/state"), "SID-123"
    )
    assert argv[0] == "claude"
    assert argv[argv.index("--session-id") + 1] == "SID-123"
    assert "--dangerously-skip-permissions" in argv
    assert argv[argv.index("-p") + 1] == "/pr-pipeline 1011"


def test_extract_session_id_from_thread_started() -> None:
    line = '{"type":"thread.started","thread_id":"abc-123"}'
    assert cd._extract_session_id(line) == "abc-123"


def test_extract_session_id_ignores_idless_and_garbage() -> None:
    assert cd._extract_session_id('{"type":"turn.started"}') is None
    assert cd._extract_session_id("not json") is None


def test_poll_codex_session_id_timeout_returns_none(tmp_path: Path) -> None:
    log = tmp_path / "missing.log"
    # No id ever appears; a short timeout must return None fast (not sleep 30s).
    assert cd.poll_codex_session_id(log, timeout=0.05, poll_interval=0.01) is None


# --- guards: kill switch / budget / collision ---


def test_kill_switch_refuses(tmp_path: Path, capsys) -> None:
    canonical = _make_origin(tmp_path)
    state = tmp_path / "state"
    state.mkdir(parents=True)
    (state / "auto-dispatch.off").write_text("", encoding="utf-8")

    rc = cd.dispatch(_args(tmp_path, canonical, state_root=state))

    assert rc == 3
    assert "kill switch" in capsys.readouterr().out
    # No side effects.
    assert not (state / "pipeline-slots").exists()


def test_full_budget_refuses(tmp_path: Path, capsys) -> None:
    canonical = _make_origin(tmp_path)
    slots = tmp_path / "state" / "pipeline-slots"
    for slug in ("lane-a", "lane-b", "lane-c"):
        _write_slot(slots, slug)

    rc = cd.dispatch(_args(tmp_path, canonical, max_slots=3))

    assert rc == 4
    assert "no free slot budget: busy 3/3" in capsys.readouterr().out


def test_slot_collision_maps_to_exit_2(tmp_path: Path, capsys) -> None:
    canonical = _make_origin(tmp_path)
    slots = tmp_path / "state" / "pipeline-slots"
    _write_slot(slots, "auto-codex-issue-1011")

    rc = cd.main(
        [
            "--issues",
            "1011",
            "--state-root",
            str(tmp_path / "state"),
            "--canonical",
            str(canonical),
            "--no-canonical-check",
        ]
    )

    assert rc == 2
    assert "collision" in capsys.readouterr().err


def test_worktree_collision_maps_to_exit_2(tmp_path: Path, capsys) -> None:
    canonical = _make_origin(tmp_path)
    wt = canonical / ".claude" / "worktrees" / "auto-codex-issue-1011"
    wt.mkdir(parents=True)

    rc = cd.main(
        [
            "--issues",
            "1011",
            "--state-root",
            str(tmp_path / "state"),
            "--canonical",
            str(canonical),
            "--no-canonical-check",
        ]
    )

    assert rc == 2
    assert "worktree collision" in capsys.readouterr().err


# --- happy paths ---


def test_happy_path_codex(tmp_path: Path, capsys) -> None:
    canonical = _make_origin(tmp_path)
    record = _stub_bin(tmp_path, "codex", jsonl=_CODEX_JSONL)
    state = tmp_path / "state"
    old_path = os.environ["PATH"]
    os.environ["PATH"] = f"{tmp_path / 'bin'}{os.pathsep}{old_path}"
    try:
        rc = cd.dispatch(
            _args(tmp_path, canonical, state_root=state),
            codex_id_timeout=5.0,
            codex_id_poll=0.02,
        )
    finally:
        os.environ["PATH"] = old_path

    assert rc == 0
    result = json.loads(capsys.readouterr().out)
    slug = "auto-codex-issue-1011"
    assert result["slot"] == slug
    assert result["surface"] == "codex"
    assert result["session"] == "019f2334-4455-70a1-bc1b-2e86d5ecfccf"
    assert isinstance(result["pid"], int)

    # Worktree created off origin/main.
    worktree = canonical / ".claude" / "worktrees" / slug
    assert worktree.is_dir()
    assert (worktree / "README.md").read_text(encoding="utf-8") == "seed\n"

    # Stub recorded the pinned launch flags + cwd.
    rec = _wait_for_record(record)
    assert rec["cwd"] == str(worktree)
    assert "workspace-write" in rec["argv"]
    assert "approval_policy=never" in rec["argv"]
    assert "--add-dir" in rec["argv"]
    assert "--json" in rec["argv"]
    assert "$pr-pipeline 1011" in rec["argv"]

    # Slot file: shape + slot==stem + pid + dispatched.
    slot_file = state / "pipeline-slots" / f"{slug}.json"
    slot = json.loads(slot_file.read_text(encoding="utf-8"))
    assert slot["slot"] == slug == slot_file.stem
    assert slot["issues"] == [1011]
    assert slot["prs"] == []
    assert slot["surface"] == "codex"
    assert slot["session"] == "019f2334-4455-70a1-bc1b-2e86d5ecfccf"
    assert isinstance(slot["pid"], int)
    assert slot["dispatched"]
    # scan_slots accepts it.
    assert cd._cos_preflight.scan_slots(state / "pipeline-slots") == {slug}


def test_happy_path_claude(tmp_path: Path, capsys) -> None:
    canonical = _make_origin(tmp_path)
    record = _stub_bin(tmp_path, "claude")
    state = tmp_path / "state"
    old_path = os.environ["PATH"]
    os.environ["PATH"] = f"{tmp_path / 'bin'}{os.pathsep}{old_path}"
    try:
        rc = cd.dispatch(_args(tmp_path, canonical, surface="claude", state_root=state))
    finally:
        os.environ["PATH"] = old_path

    assert rc == 0
    result = json.loads(capsys.readouterr().out)
    slug = "auto-claude-issue-1011"
    session = result["session"]
    # Pre-generated uuid, known before launch and passed via --session-id.
    assert session and session == str(__import__("uuid").UUID(session))

    rec = _wait_for_record(record)
    assert rec["argv"][rec["argv"].index("--session-id") + 1] == session
    assert "--dangerously-skip-permissions" in rec["argv"]
    assert "/pr-pipeline 1011" in rec["argv"]

    slot = json.loads(
        (state / "pipeline-slots" / f"{slug}.json").read_text(encoding="utf-8")
    )
    assert slot["surface"] == "claude"
    assert slot["session"] == session


def test_codex_id_timeout_yields_null_session_but_writes_slot(
    tmp_path: Path, capsys
) -> None:
    canonical = _make_origin(tmp_path)
    # Stub emits NO id line, so the poll times out.
    _stub_bin(tmp_path, "codex", jsonl='{"type":"turn.started"}\n')
    state = tmp_path / "state"
    old_path = os.environ["PATH"]
    os.environ["PATH"] = f"{tmp_path / 'bin'}{os.pathsep}{old_path}"
    try:
        rc = cd.dispatch(
            _args(tmp_path, canonical, state_root=state),
            codex_id_timeout=0.1,
            codex_id_poll=0.02,
        )
    finally:
        os.environ["PATH"] = old_path

    assert rc == 0
    captured = capsys.readouterr()
    assert "no codex session id" in captured.err
    result = json.loads(captured.out)
    assert result["session"] is None
    # Slot still written, with session null.
    slot = json.loads(
        (state / "pipeline-slots" / "auto-codex-issue-1011.json").read_text(
            encoding="utf-8"
        )
    )
    assert slot["session"] is None


def test_launch_failure_no_slot_and_names_worktree(tmp_path: Path) -> None:
    canonical = _make_origin(tmp_path)
    # Force an OSError at spawn by making `codex` unresolvable: a PATH that has `git`
    # (so the worktree still gets created) but no `codex`. A detached no-wait launch can
    # only fail-fast on a spawn error, not on a post-spawn nonzero exit, so a missing
    # executable is the launch-failure case that must NOT leak a slot.
    state = tmp_path / "state"
    gitonly_bin = tmp_path / "gitonly"
    gitonly_bin.mkdir()
    git_real = subprocess.run(
        ["which", "git"], capture_output=True, text=True, check=True
    ).stdout.strip()
    (gitonly_bin / "git").symlink_to(git_real)
    old_path = os.environ["PATH"]
    os.environ["PATH"] = str(gitonly_bin)
    try:
        with pytest.raises(SystemExit) as exc:
            cd.dispatch(_args(tmp_path, canonical, state_root=state))
    finally:
        os.environ["PATH"] = old_path

    message = str(exc.value.code)
    assert "failed to launch" in message
    slug = "auto-codex-issue-1011"
    worktree = canonical / ".claude" / "worktrees" / slug
    # Worktree was created (leaked) and IS named for adjudication.
    assert str(worktree) in message
    assert worktree.is_dir()
    # NO slot file written.
    assert not (state / "pipeline-slots" / f"{slug}.json").exists()


def test_dry_run_no_side_effects(tmp_path: Path, capsys) -> None:
    canonical = _make_origin(tmp_path)
    state = tmp_path / "state"

    rc = cd.dispatch(_args(tmp_path, canonical, state_root=state, dry_run=True))

    assert rc == 0
    result = json.loads(capsys.readouterr().out)
    assert result["launch_argv"][0] == "codex"
    assert result["slot_path"].endswith("auto-codex-issue-1011.json")
    # Zero side effects: no worktree, no slot file, no log dir.
    slug = "auto-codex-issue-1011"
    assert not (canonical / ".claude" / "worktrees" / slug).exists()
    assert not (state / "pipeline-slots").exists()
    assert not (state / "dispatch-logs").exists()


def test_git_failure_fails_fast(tmp_path: Path, capsys) -> None:
    # A non-repo canonical dir: `git fetch origin main` fails, exit 2, no slot file.
    canonical = tmp_path / "not-a-repo"
    canonical.mkdir()
    state = tmp_path / "state"

    rc = cd.main(
        [
            "--issues",
            "1011",
            "--state-root",
            str(state),
            "--canonical",
            str(canonical),
            "--no-canonical-check",
            "--worktree-root",
            str(tmp_path / "wts"),
        ]
    )

    assert rc == 2
    assert "git" in capsys.readouterr().err
    assert not (state / "pipeline-slots").exists()
