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

HERMETICITY (post-incident, non-negotiable): a prior version's run_git ran git in the
AMBIENT process cwd; when the pre-push hook re-ran the full suite with cwd = the real
worktree, cos_dispatch's git ops mutated the real repo and launched the REAL codex agent.
Two autouse guardrails now make that class impossible regardless of any single bug:
  1. `_hermetic_env` chdir's every test into tmp_path (so an ambient-cwd subprocess lands
     in a harmless empty dir) and prepends a stub-bin with `codex` AND `claude` that FAIL
     LOUDLY (exit 97, record the invocation) — the real binaries are unreachable even if a
     test forgets its own stub. Tests that legitimately launch overwrite those stubs with
     recording no-op stubs in the SAME dir.
  2. Every dispatch/main invocation passes an explicit `--canonical <tmp>` /
     `canonical=<tmp>` under tmp_path — never the real DEFAULT_CANONICAL.
Combined with cos_dispatch's own require_git_checkout guard, a stray canonical can only
fail fast, never escape.
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

# A stub codex/claude that FAILS LOUDLY if invoked, recording the attempt. Any test that
# reaches a real launch without installing its own stub trips this instead of the real
# binary. Exit 97 is a distinctive sentinel so an accidental invocation is obvious.
_FAIL_STUB_BODY = (
    "#!/usr/bin/env python3\n"
    "import json, os, sys\n"
    "rec = os.environ.get('COS_STUB_RECORD')\n"
    "if rec:\n"
    "    with open(rec, 'a') as fh:\n"
    "        fh.write(json.dumps({'name': os.path.basename(sys.argv[0]), "
    "'argv': sys.argv[1:], 'cwd': os.getcwd()}) + chr(10))\n"
    "sys.stderr.write('UNEXPECTED real-binary invocation in test: ' + ' '.join(sys.argv) + chr(10))\n"
    "sys.exit(97)\n"
)


def _install_stub_bin(bindir: Path, names: tuple[str, ...], body: str) -> None:
    bindir.mkdir(parents=True, exist_ok=True)
    for name in names:
        script = bindir / name
        script.write_text(body, encoding="utf-8")
        script.chmod(0o755)


# Git-context env vars that override cwd-based repo discovery. The repo's pre-push hook
# runs this suite with GIT_DIR / GIT_WORK_TREE (and, with the push workaround, more)
# exported — every git subprocess then targets the HOOK's real worktree regardless of the
# tmp cwd/-C the fixtures pass, which is exactly what clobbered the real index. The
# fixture unsets them so the test process AND anything it spawns (fixture git calls, the
# script under test) discover repos by cwd again, independent of how pytest was launched.
_GIT_CONTEXT_ENV = (
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_INDEX_FILE",
    "GIT_COMMON_DIR",
    "GIT_OBJECT_DIRECTORY",
    "GIT_NAMESPACE",
    "GIT_PREFIX",
)


@pytest.fixture(autouse=True)
def _hermetic_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Sandbox EVERY test: harmless cwd + PATH where the real codex/claude are unreachable.

    Returns the stub-bin dir so a test can overwrite the fail-loud stubs with recording
    no-op stubs when it legitimately drives a launch. `git` stays reachable via the
    inherited real PATH appended after the stub dir, so the tmp-repo git ops still work.
    Also unsets inherited GIT_* context vars so a pre-push-hook invocation can't hijack any
    git call to the hook's repo (see _GIT_CONTEXT_ENV).
    """
    stub_bin = tmp_path / "stub-bin"
    _install_stub_bin(stub_bin, ("codex", "claude"), _FAIL_STUB_BODY)
    invocations = tmp_path / "stub-invocations.log"
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PATH", f"{stub_bin}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("COS_STUB_RECORD", str(invocations))
    for var in _GIT_CONTEXT_ENV:
        monkeypatch.delenv(var, raising=False)
    return stub_bin


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


def _no_real_launch(tmp_path: Path) -> None:
    """Assert the fail-loud stub never recorded a real-binary invocation."""
    log = tmp_path / "stub-invocations.log"
    if log.exists():
        raise AssertionError(
            f"a real codex/claude launch was attempted: {log.read_text()!r}"
        )


def _write_slot(slots_root: Path, slug: str) -> Path:
    slots_root.mkdir(parents=True, exist_ok=True)
    path = slots_root / f"{slug}.json"
    path.write_text(
        json.dumps({"slot": slug, "issues": [1], "prs": [], "surface": "codex"}),
        encoding="utf-8",
    )
    return path


_GIT_ENV = {
    "GIT_AUTHOR_NAME": "t",
    "GIT_AUTHOR_EMAIL": "t@t",
    "GIT_COMMITTER_NAME": "t",
    "GIT_COMMITTER_EMAIL": "t@t",
}


def _git(cwd: Path, *args: str) -> None:
    # Every fixture git call runs with an EXPLICIT cwd under tmp_path (never the ambient
    # process cwd) so setup can't touch a repo outside the sandbox.
    subprocess.run(
        ["git", *args], cwd=str(cwd), check=True, env={**os.environ, **_GIT_ENV}
    )


def _make_origin(tmp_path: Path) -> Path:
    """A canonical checkout (worktree root) with an `origin` bare remote + `main` branch."""
    bare = tmp_path / "origin.git"
    canonical = tmp_path / "canonical"
    canonical.mkdir()
    _git(tmp_path, "init", "--bare", "-b", "main", str(bare))
    _git(tmp_path, "clone", str(bare), str(canonical))
    (canonical / "README.md").write_text("seed\n", encoding="utf-8")
    _git(canonical, "add", ".")
    _git(canonical, "commit", "-m", "seed")
    _git(canonical, "push", "origin", "main")
    return canonical


def _stub_bin(
    stub_bin: Path, name: str, *, jsonl: str = "", exit_code: int = 0
) -> Path:
    """Overwrite the fail-loud stub for `name` with a recording no-op that emits jsonl.

    Written into the SAME stub-bin dir the autouse fixture prepended to PATH, so it takes
    precedence over the real binary. Records argv+cwd to <stub_bin>/<name>.record.
    """
    stub_bin.mkdir(parents=True, exist_ok=True)
    record = stub_bin / f"{name}.record"
    script = stub_bin / name
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
        "tier": "hard",
        "surface": None,  # None = use the tier's implied surface (matches argparse)
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
        "codex", Path("/wt/lane"), [1011, 1012], Path("/state"), None, []
    )
    assert argv[0:2] == ["codex", "exec"]
    assert argv[argv.index("-C") + 1] == "/wt/lane"
    assert argv[argv.index("-s") + 1] == "workspace-write"
    assert argv[argv.index("-c") + 1] == "approval_policy=never"
    assert argv[argv.index("--add-dir") + 1] == "/state"
    assert "--json" in argv
    assert argv[-1] == "$pr-pipeline 1011 1012"
    # No profile flags → no model/effort pins.
    assert "-m" not in argv


def test_build_launch_argv_codex_layers_profile_flags_before_prompt() -> None:
    argv = cd.build_launch_argv(
        "codex",
        Path("/wt/lane"),
        [1011],
        Path("/state"),
        None,
        ["-m", "gpt-5.5", "-c", "model_reasoning_effort=xhigh"],
    )
    # Profile flags sit after --json and before the prompt (still the last arg).
    assert argv[argv.index("-m") + 1] == "gpt-5.5"
    assert argv[-1] == "$pr-pipeline 1011"
    assert argv.index("--json") < argv.index("-m")


def test_build_launch_argv_claude_uses_session_and_slash_prompt() -> None:
    argv = cd.build_launch_argv(
        "claude", Path("/wt/lane"), [1011], Path("/state"), "SID-123", []
    )
    assert argv[0] == "claude"
    assert argv[argv.index("--session-id") + 1] == "SID-123"
    assert "--dangerously-skip-permissions" in argv
    assert argv[argv.index("-p") + 1] == "/pr-pipeline 1011"
    # No profile flags → no advisor pins.
    assert "--advisor" not in argv


def test_build_launch_argv_claude_layers_profile_flags() -> None:
    argv = cd.build_launch_argv(
        "claude",
        Path("/wt/lane"),
        [1011],
        Path("/state"),
        "SID-123",
        ["--model", "claude-sonnet-5", "--effort", "high", "--advisor", "opus"],
    )
    assert argv[argv.index("--model") + 1] == "claude-sonnet-5"
    assert argv[argv.index("--effort") + 1] == "high"
    assert argv[argv.index("--advisor") + 1] == "opus"
    assert argv[argv.index("-p") + 1] == "/pr-pipeline 1011"


# --- resolve_profile: tier → (surface, flags), with the --surface override rule ---


def test_resolve_profile_default_hard_is_codex_gpt55() -> None:
    surface, flags = cd.resolve_profile("hard", None)
    assert surface == "codex"
    assert flags == ["-m", "gpt-5.5", "-c", "model_reasoning_effort=xhigh"]


def test_resolve_profile_easy_is_claude_sonnet_opus() -> None:
    surface, flags = cd.resolve_profile("easy", None)
    assert surface == "claude"
    assert flags == [
        "--model",
        "claude-sonnet-5",
        "--effort",
        "high",
        "--advisor",
        "opus",
    ]


def test_resolve_profile_surface_restating_tier_keeps_profile() -> None:
    assert cd.resolve_profile("hard", "codex") == (
        "codex",
        ["-m", "gpt-5.5", "-c", "model_reasoning_effort=xhigh"],
    )


def test_resolve_profile_surface_contradicting_tier_drops_pins() -> None:
    # easy tier (claude) overridden to codex → codex with AMBIENT defaults, no pins.
    assert cd.resolve_profile("easy", "codex") == ("codex", [])
    # hard tier (codex) overridden to claude → claude ambient, no advisor pins.
    assert cd.resolve_profile("hard", "claude") == ("claude", [])


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
    _no_real_launch(tmp_path)


def test_full_budget_refuses(tmp_path: Path, capsys) -> None:
    canonical = _make_origin(tmp_path)
    slots = tmp_path / "state" / "pipeline-slots"
    for slug in ("lane-a", "lane-b", "lane-c"):
        _write_slot(slots, slug)

    rc = cd.dispatch(_args(tmp_path, canonical, max_slots=3))

    assert rc == 4
    assert "no free slot budget: busy 3/3" in capsys.readouterr().out
    _no_real_launch(tmp_path)


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
    _no_real_launch(tmp_path)


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
    _no_real_launch(tmp_path)


# --- happy paths ---


def test_happy_path_codex(tmp_path: Path, capsys, _hermetic_env: Path) -> None:
    canonical = _make_origin(tmp_path)
    record = _stub_bin(_hermetic_env, "codex", jsonl=_CODEX_JSONL)
    state = tmp_path / "state"
    rc = cd.dispatch(
        _args(tmp_path, canonical, state_root=state),
        codex_id_timeout=5.0,
        codex_id_poll=0.02,
    )

    assert rc == 0
    result = json.loads(capsys.readouterr().out)
    slug = "auto-codex-issue-1011"
    assert result["slot"] == slug
    assert result["surface"] == "codex"
    assert result["tier"] == "hard"  # default tier
    assert result["session"] == "019f2334-4455-70a1-bc1b-2e86d5ecfccf"
    assert isinstance(result["pid"], int)

    # Worktree created off origin/main.
    worktree = canonical / ".claude" / "worktrees" / slug
    assert worktree.is_dir()
    assert (worktree / "README.md").read_text(encoding="utf-8") == "seed\n"

    # Stub recorded the pinned launch flags + cwd + hard-tier profile pins.
    rec = _wait_for_record(record)
    assert rec["cwd"] == str(worktree)
    assert "workspace-write" in rec["argv"]
    assert "approval_policy=never" in rec["argv"]
    assert "--add-dir" in rec["argv"]
    assert "--json" in rec["argv"]
    assert "$pr-pipeline 1011" in rec["argv"]
    assert rec["argv"][rec["argv"].index("-m") + 1] == "gpt-5.5"
    assert "model_reasoning_effort=xhigh" in rec["argv"]

    # Slot file: shape + slot==stem + pid + dispatched + tier.
    slot_file = state / "pipeline-slots" / f"{slug}.json"
    slot = json.loads(slot_file.read_text(encoding="utf-8"))
    assert slot["slot"] == slug == slot_file.stem
    assert slot["issues"] == [1011]
    assert slot["prs"] == []
    assert slot["surface"] == "codex"
    assert slot["tier"] == "hard"
    assert slot["session"] == "019f2334-4455-70a1-bc1b-2e86d5ecfccf"
    assert isinstance(slot["pid"], int)
    assert slot["dispatched"]
    # scan_slots accepts it.
    assert cd._cos_preflight.scan_slots(state / "pipeline-slots") == {slug}


def test_happy_path_claude(tmp_path: Path, capsys, _hermetic_env: Path) -> None:
    canonical = _make_origin(tmp_path)
    record = _stub_bin(_hermetic_env, "claude")
    state = tmp_path / "state"
    # easy tier → claude surface with the blessed sonnet-5 + opus-advisor profile.
    rc = cd.dispatch(_args(tmp_path, canonical, tier="easy", state_root=state))

    assert rc == 0
    result = json.loads(capsys.readouterr().out)
    slug = "auto-claude-issue-1011"
    assert result["surface"] == "claude"
    assert result["tier"] == "easy"
    session = result["session"]
    # Pre-generated uuid, known before launch and passed via --session-id.
    assert session and session == str(__import__("uuid").UUID(session))

    rec = _wait_for_record(record)
    assert rec["argv"][rec["argv"].index("--session-id") + 1] == session
    assert "--dangerously-skip-permissions" in rec["argv"]
    assert "/pr-pipeline 1011" in rec["argv"]
    # easy-tier profile pins.
    assert rec["argv"][rec["argv"].index("--model") + 1] == "claude-sonnet-5"
    assert rec["argv"][rec["argv"].index("--effort") + 1] == "high"
    assert rec["argv"][rec["argv"].index("--advisor") + 1] == "opus"

    slot = json.loads(
        (state / "pipeline-slots" / f"{slug}.json").read_text(encoding="utf-8")
    )
    assert slot["surface"] == "claude"
    assert slot["tier"] == "easy"
    assert slot["session"] == session


def test_codex_id_timeout_yields_null_session_but_writes_slot(
    tmp_path: Path, capsys, _hermetic_env: Path
) -> None:
    canonical = _make_origin(tmp_path)
    # Stub emits NO id line, so the poll times out.
    _stub_bin(_hermetic_env, "codex", jsonl='{"type":"turn.started"}\n')
    state = tmp_path / "state"
    rc = cd.dispatch(
        _args(tmp_path, canonical, state_root=state),
        codex_id_timeout=0.1,
        codex_id_poll=0.02,
    )

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


def test_launch_failure_no_slot_and_names_worktree(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    canonical = _make_origin(tmp_path)
    # Force an OSError at spawn by making `codex` unresolvable: a PATH with ONLY `git`
    # (so the worktree still gets created) and no codex/claude at all — not even the
    # fail-loud stub. A detached no-wait launch can only fail-fast on a spawn error, not
    # on a post-spawn nonzero exit, so a missing executable is the launch-failure case
    # that must NOT leak a slot. monkeypatch.setenv keeps the swap test-local + reverted.
    state = tmp_path / "state"
    gitonly_bin = tmp_path / "gitonly"
    gitonly_bin.mkdir()
    git_real = subprocess.run(
        ["which", "git"], capture_output=True, text=True, check=True
    ).stdout.strip()
    (gitonly_bin / "git").symlink_to(git_real)
    monkeypatch.setenv("PATH", str(gitonly_bin))

    with pytest.raises(SystemExit) as exc:
        cd.dispatch(_args(tmp_path, canonical, state_root=state))

    message = str(exc.value.code)
    assert "failed to launch" in message
    slug = "auto-codex-issue-1011"
    worktree = canonical / ".claude" / "worktrees" / slug
    # Worktree was created (leaked) and IS named for adjudication.
    assert str(worktree) in message
    assert worktree.is_dir()
    # NO slot file written.
    assert not (state / "pipeline-slots" / f"{slug}.json").exists()


def test_surface_override_contradicting_tier_drops_pins(
    tmp_path: Path, capsys, _hermetic_env: Path
) -> None:
    # easy tier (claude) forced onto codex → launches codex with AMBIENT defaults: the
    # base pinned flags but NO model/effort/advisor profile pins.
    canonical = _make_origin(tmp_path)
    record = _stub_bin(_hermetic_env, "codex", jsonl=_CODEX_JSONL)
    state = tmp_path / "state"
    rc = cd.dispatch(
        _args(tmp_path, canonical, tier="easy", surface="codex", state_root=state),
        codex_id_timeout=5.0,
        codex_id_poll=0.02,
    )

    assert rc == 0
    result = json.loads(capsys.readouterr().out)
    assert result["surface"] == "codex"
    assert result["tier"] == "easy"  # tier recorded even though its pins were dropped

    rec = _wait_for_record(record)
    # Base flags present, but NONE of the tier's model/effort/advisor pins.
    assert "workspace-write" in rec["argv"]
    assert "$pr-pipeline 1011" in rec["argv"]
    assert "-m" not in rec["argv"]
    assert "gpt-5.5" not in rec["argv"]
    assert "--model" not in rec["argv"]
    assert "--advisor" not in rec["argv"]
    assert "model_reasoning_effort=xhigh" not in rec["argv"]


def test_dry_run_no_side_effects_reflects_tier(tmp_path: Path, capsys) -> None:
    canonical = _make_origin(tmp_path)
    state = tmp_path / "state"

    # Default (hard) tier.
    rc = cd.dispatch(_args(tmp_path, canonical, state_root=state, dry_run=True))
    assert rc == 0
    result = json.loads(capsys.readouterr().out)
    assert result["launch_argv"][0] == "codex"
    assert result["surface"] == "codex"
    assert result["tier"] == "hard"
    assert "gpt-5.5" in result["launch_argv"]
    assert result["slot_path"].endswith("auto-codex-issue-1011.json")
    # Zero side effects: no worktree, no slot file, no log dir.
    slug = "auto-codex-issue-1011"
    assert not (canonical / ".claude" / "worktrees" / slug).exists()
    assert not (state / "pipeline-slots").exists()
    assert not (state / "dispatch-logs").exists()
    _no_real_launch(tmp_path)


def test_dry_run_easy_tier_reflects_claude_profile(tmp_path: Path, capsys) -> None:
    canonical = _make_origin(tmp_path)
    state = tmp_path / "state"

    rc = cd.dispatch(
        _args(tmp_path, canonical, tier="easy", state_root=state, dry_run=True)
    )
    assert rc == 0
    result = json.loads(capsys.readouterr().out)
    assert result["launch_argv"][0] == "claude"
    assert result["surface"] == "claude"
    assert result["tier"] == "easy"
    assert "claude-sonnet-5" in result["launch_argv"]
    assert "opus" in result["launch_argv"]
    assert result["slot_path"].endswith("auto-claude-issue-1011.json")
    _no_real_launch(tmp_path)


def test_git_failure_fails_fast_no_launch(tmp_path: Path, capsys) -> None:
    # A REAL tmp repo (satisfies require_git_checkout) but with NO origin remote, so
    # `git fetch origin main` genuinely fails. Must exit 2, write no slot, and — the
    # incident's core lesson — NEVER reach the launch step (no codex/claude invoked).
    canonical = tmp_path / "canonical"
    canonical.mkdir()
    _git(canonical, "init", "-b", "main")
    (canonical / "seed").write_text("x\n", encoding="utf-8")
    _git(canonical, "add", ".")
    _git(canonical, "commit", "-m", "seed")
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
        ]
    )

    assert rc == 2
    assert "git" in capsys.readouterr().err
    assert not (state / "pipeline-slots").exists()
    # The launch step was never reached: the fail-loud stub recorded nothing.
    _no_real_launch(tmp_path)


def test_non_repo_canonical_refused_before_any_git_mutation(
    tmp_path: Path, capsys
) -> None:
    # require_git_checkout belt-and-braces: a canonical that is NOT a git worktree (an
    # empty dir) is refused BEFORE fetch/worktree-add, so `git -C` can never walk up into
    # an unintended enclosing repo. This is the direct guard against the incident's
    # ambient-cwd escape.
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
        ]
    )

    assert rc == 2
    assert "not a git worktree" in capsys.readouterr().err
    assert not (state / "pipeline-slots").exists()
    _no_real_launch(tmp_path)


def test_cli_default_tier_is_hard(tmp_path: Path, capsys) -> None:
    # Through main()'s real argparse (not the _args helper): omitting --tier defaults to
    # hard, so the resolved surface is codex with the gpt-5.5 xhigh profile.
    canonical = _make_origin(tmp_path)
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
            "--dry-run",
        ]
    )

    assert rc == 0
    result = json.loads(capsys.readouterr().out)
    assert result["tier"] == "hard"
    assert result["surface"] == "codex"
    assert "gpt-5.5" in result["launch_argv"]
    _no_real_launch(tmp_path)


def test_require_git_checkout_rejects_enclosing_repo_subdir(tmp_path: Path) -> None:
    # A subdir INSIDE a repo (not the worktree root) must be rejected: git would resolve
    # its toplevel to the enclosing repo, exactly the unintended-target class.
    canonical = _make_origin(tmp_path)
    subdir = canonical / "nested"
    subdir.mkdir()

    with pytest.raises(SystemExit) as exc:
        cd.require_git_checkout(subdir)

    assert "not a worktree root" in str(exc.value.code)
