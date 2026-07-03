"""Unit tests for scripts/cos_dispatch.py.

Pins the auto-dispatch launcher's contract: the ordered guards (kill switch → budget →
collision), the exact per-surface launch argv (codex `$pr-pipeline …` with the pinned
sandbox flags; claude `/pr-pipeline …` with a pre-generated --session-id), that the
worktree is materialized off a freshly fetched origin/main, that the slot file is
written with slot==stem shape (so scan_slots accepts it) promptly after a successful
launch, codex session-id capture from the JSONL log (plus its bounded-poll timeout →
session=null), and --dry-run's zero-side-effect check-only path. Review-hardening
invariants: the codex session merge AND the initial ownership write both overlay only
their own fields, preserving a fast child pipeline's concurrently-written `prs` (a
vanished/invalid slot falls back to a full write); the id poll parses only bytes past the
pre-launch log offset so a reused per-slug log can't leak a prior run's stale thread id; a
child that exits within the post-spawn grace window is a launch failure (exit 2, no slot);
a dispatch-log setup OSError becomes the exit-2 orphan path (never a traceback); and a
standard `.../registry-research-toolkit` --state-root is propagated to the child as
XDG_STATE_HOME (a non-standard root warns and stays ambient).

Real subprocesses are used where the behavior IS the subprocess: a tmp git repo with an
`origin` bare remote so `git fetch origin main` + `git worktree add … origin/main` run
for real, and a stub `codex`/`claude` on a prepended PATH that records its argv+cwd and
emits canned JSONL. cos_dispatch reuses the cos_preflight slot helpers (scan_slots,
default_slots_root, DEFAULT_MAX_SLOTS), hoisted there from cos_watch on this branch;
`test_reused_cos_preflight_helpers_are_the_real_hoist` pins that integration contract for
real (no backfill fixture masks a missing hoist).

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

import json
import os
import subprocess
import time
from pathlib import Path

import pytest

from conftest import load_scripts_module

cd = load_scripts_module("cos_dispatch")

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


# The real author + visual-lane gates, captured before the autouse no-ops below replace
# them, so the dedicated gate tests can restore the genuine functions (they stub the
# gh_issue surface underneath them).
_REAL_REQUIRE_MAINTAINER = cd.require_maintainer_authored
_REAL_REQUIRE_NO_VISUAL = cd.require_no_visual_lane_on_codex


@pytest.fixture(autouse=True)
def _stub_author_check(monkeypatch: pytest.MonkeyPatch) -> None:
    """No-op the pre-launch network gates by default so existing tests don't hit live `gh`.

    dispatch() now calls two read-only-network guards before the worktree step: the
    maintainer-author gate (require_maintainer_authored, a `gh issue view` per issue) and
    the codex visual-lane routing guard (require_no_visual_lane_on_codex, a body fetch per
    issue). The launch-reaching tests here model neither, so default both to pass; the
    dedicated gate tests below restore the real functions (_REAL_REQUIRE_*) over a stubbed
    gh_issue surface to exercise refuse/proceed.
    """
    monkeypatch.setattr(cd, "require_maintainer_authored", lambda issues: None)
    monkeypatch.setattr(cd, "require_no_visual_lane_on_codex", lambda issues: None)


def _no_real_launch(tmp_path: Path) -> None:
    """Assert the fail-loud stub never recorded a real-binary invocation."""
    log = tmp_path / "stub-invocations.log"
    if log.exists():
        raise AssertionError(
            f"a real codex/claude launch was attempted: {log.read_text()!r}"
        )


# A healthy child must OUTLIVE dispatch's post-spawn launch-grace window (a child that exits
# inside it is treated as a launch failure). Tests run dispatch with a tiny injectable grace
# (_TEST_GRACE); the recording stubs linger a hair longer so they survive the health check
# without adding real seconds to the suite. Both are ~milliseconds.
_TEST_GRACE = 0.05
_TEST_GRACE_POLL = 0.01
_STUB_LINGER = (
    0.3  # comfortably past _TEST_GRACE + a few polls; the stub is detached anyway
)
# Launch-failure tests need a grace CEILING long enough to actually observe the child's
# instant exit — Python interpreter startup alone can exceed _TEST_GRACE, so a 0.05s ceiling
# would elapse before the child even finishes exiting. The health check returns the instant
# it sees proc.poll() != None, so a generous ceiling costs only the real ~startup+exit time
# (~100ms), NOT the full ceiling — the suite stays fast.
_FAIL_GRACE = 3.0


def _patch_fast_grace(
    monkeypatch: pytest.MonkeyPatch, grace: float = _TEST_GRACE
) -> None:
    """Pin launch_detached's grace, for tests that reach launch via main().

    dispatch() takes launch_grace as a kwarg, but main() calls dispatch() with production
    defaults (a 1s grace) and reads only argv — so a main()-path launch test can't inject the
    grace. Wrap launch_detached to pin `grace` regardless, keeping the suite fast: a healthy
    launch pays the full (tiny) grace, while an instant-exit failure returns as soon as
    proc.poll() fires, so a generous ceiling (_FAIL_GRACE) costs only the real exit time.
    """
    real = cd.launch_detached
    pinned_grace = grace

    def fast(argv, worktree, log_path, state_root, *, grace=None, grace_poll=None):
        # Ignore the grace dispatch() passes (main() gives it the 1s production default) and
        # force the pinned test grace instead.
        return real(
            argv,
            worktree,
            log_path,
            state_root,
            grace=pinned_grace,
            grace_poll=_TEST_GRACE_POLL,
        )

    monkeypatch.setattr(cd, "launch_detached", fast)


def _write_slot(slots_root: Path, slug: str, override: dict | None = None) -> Path:
    slots_root.mkdir(parents=True, exist_ok=True)
    path = slots_root / f"{slug}.json"
    payload = {"slot": slug, "issues": [1], "prs": [], "surface": "codex"}
    payload.update(override or {})
    path.write_text(json.dumps(payload), encoding="utf-8")
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
    stub_bin: Path,
    name: str,
    *,
    jsonl: str = "",
    exit_code: int = 0,
    linger: float = _STUB_LINGER,
) -> Path:
    """Overwrite the fail-loud stub for `name` with a recording no-op that emits jsonl.

    Written into the SAME stub-bin dir the autouse fixture prepended to PATH, so it takes
    precedence over the real binary. Records argv+cwd — plus the GIT_* keys it inherited,
    under `git_env`, so the scrub test can assert the child got none — plus XDG_STATE_HOME
    (so the state-root propagation test can assert it) to <stub_bin>/<name>.record. It
    records/emits FIRST, then lingers so it outlives dispatch's launch-grace health check
    (pass linger=0 to model an instant-exit launch failure).
    """
    stub_bin.mkdir(parents=True, exist_ok=True)
    record = stub_bin / f"{name}.record"
    script = stub_bin / name
    body = (
        "#!/usr/bin/env python3\n"
        "import json, os, sys, time\n"
        "git_env = sorted(k for k in os.environ if k.startswith('GIT_'))\n"
        f"open({str(record)!r}, 'w').write(json.dumps({{'argv': sys.argv[1:], "
        "'cwd': os.getcwd(), 'git_env': git_env, "
        "'xdg_state_home': os.environ.get('XDG_STATE_HOME')}))\n"
        f"sys.stdout.write({jsonl!r})\n"
        "sys.stdout.flush()\n"
        f"time.sleep({linger!r})\n"
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
        "codex",
        Path("/wt/lane"),
        [1011, 1012],
        Path("/state"),
        None,
        [],
        Path("/canon"),
    )
    assert argv[0:2] == ["codex", "exec"]
    assert argv[argv.index("-C") + 1] == "/wt/lane"
    assert argv[argv.index("-s") + 1] == "workspace-write"
    assert argv[argv.index("-c") + 1] == "approval_policy=never"
    assert "--json" in argv
    assert argv[-1] == "$pr-pipeline 1011 1012"
    # No profile flags → no model/effort pins.
    assert "-m" not in argv


def test_build_launch_argv_codex_grants_state_root_and_canonical_gitdir() -> None:
    # Two --add-dir grants (#1050): the state root AND the canonical checkout's .git, so
    # the linked worktree's writable git state (which lives under <canonical>/.git, outside
    # the sandboxed cwd) is inside the workspace-write set.
    argv = cd.build_launch_argv(
        "codex", Path("/wt/lane"), [1011], Path("/state"), None, [], Path("/canon")
    )
    add_dirs = [argv[i + 1] for i, a in enumerate(argv) if a == "--add-dir"]
    assert add_dirs == ["/state", "/canon/.git"]


def test_build_launch_argv_codex_layers_profile_flags_before_prompt() -> None:
    argv = cd.build_launch_argv(
        "codex",
        Path("/wt/lane"),
        [1011],
        Path("/state"),
        None,
        ["-m", "gpt-5.5", "-c", "model_reasoning_effort=xhigh"],
        Path("/canon"),
    )
    # Profile flags sit after --json and before the prompt (still the last arg).
    assert argv[argv.index("-m") + 1] == "gpt-5.5"
    assert argv[-1] == "$pr-pipeline 1011"
    assert argv.index("--json") < argv.index("-m")


def test_build_launch_argv_claude_uses_session_and_slash_prompt() -> None:
    argv = cd.build_launch_argv(
        "claude",
        Path("/wt/lane"),
        [1011],
        Path("/state"),
        "SID-123",
        [],
        Path("/canon"),
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
        Path("/canon"),
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


# --- reuse contract: the hoisted cos_preflight slot helpers -------------------


def test_reused_cos_preflight_helpers_are_the_real_hoist(tmp_path: Path) -> None:
    # cos_dispatch reuses cos_preflight.scan_slots / default_slots_root / DEFAULT_MAX_SLOTS
    # (hoisted there from cos_watch on this branch). This test FAILS if any of those symbols
    # vanishes from cos_preflight — there is no backfill fixture to mask a missing hoist.
    pre = cd._cos_preflight
    assert callable(pre.scan_slots)
    assert callable(pre.default_slots_root)
    assert isinstance(pre.DEFAULT_MAX_SLOTS, int)
    # Behavior parity on a shared tmp ledger: cos_dispatch's budget path (busy count via
    # cos_preflight.scan_slots) and a direct scan_slots call must agree. We pin
    # parity-by-behavior rather than identity — the reuse contract is that the same
    # scan_slots logic drives both paths, not merely that they share a module object.
    slots_root = tmp_path / "pipeline-slots"
    _write_slot(slots_root, "lane-a")
    _write_slot(slots_root, "lane-b")
    _write_slot(slots_root, "lane-x", {"slot": "wrong-stem"})  # rejected by both

    direct = pre.scan_slots(slots_root)
    assert direct == {"lane-a", "lane-b"}
    # cos_dispatch reaches the same count through its own budget computation.
    canonical = _make_origin(tmp_path)
    rc = cd.dispatch(
        _args(tmp_path, canonical, state_root=tmp_path, max_slots=len(direct))
    )
    assert rc == 4  # busy == max via the same scan_slots the direct call used


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
        launch_grace=_TEST_GRACE,
        launch_grace_poll=_TEST_GRACE_POLL,
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
    # Both --add-dir grants: the state root AND the canonical checkout's .git (#1050), so
    # the linked worktree's writable git state is inside the workspace-write set.
    add_dirs = [
        rec["argv"][i + 1] for i, a in enumerate(rec["argv"]) if a == "--add-dir"
    ]
    assert add_dirs == [str(state), str(canonical / ".git")]
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
    rc = cd.dispatch(
        _args(tmp_path, canonical, tier="easy", state_root=state),
        launch_grace=_TEST_GRACE,
        launch_grace_poll=_TEST_GRACE_POLL,
    )

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
        launch_grace=_TEST_GRACE,
        launch_grace_poll=_TEST_GRACE_POLL,
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
        launch_grace=_TEST_GRACE,
        launch_grace_poll=_TEST_GRACE_POLL,
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


# --- GIT_* scrub: the load-bearing hermeticity invariant ----------------------


def test_hostile_git_env_is_scrubbed_from_children(
    tmp_path: Path, capsys, _hermetic_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The live-incident invariant: even with GIT_DIR/GIT_WORK_TREE/GIT_INDEX_FILE exported
    # (as the pre-push hook does), (i) the script's own git ops still target the tmp
    # canonical correctly, and (ii) the launched child receives NONE of those repo-targeting
    # GIT_* vars. Deleting _scrubbed_env must fail this test.
    #
    # Build the origin FIRST — the fixture git calls (_git) inherit os.environ, so the
    # hostile GIT_* must not be set until setup is done, or setup itself would be hijacked.
    canonical = _make_origin(tmp_path)
    record = _stub_bin(_hermetic_env, "codex", jsonl=_CODEX_JSONL)
    state = tmp_path / "state"

    decoy = tmp_path / "decoy-repo"
    decoy.mkdir()
    monkeypatch.setenv("GIT_DIR", str(decoy / ".git"))
    monkeypatch.setenv("GIT_WORK_TREE", str(decoy))
    monkeypatch.setenv("GIT_INDEX_FILE", str(decoy / "index"))

    rc = cd.dispatch(
        _args(tmp_path, canonical, state_root=state),
        codex_id_timeout=5.0,
        codex_id_poll=0.02,
        launch_grace=_TEST_GRACE,
        launch_grace_poll=_TEST_GRACE_POLL,
    )

    assert rc == 0
    slug = "auto-codex-issue-1011"
    # (i) The git ops still hit the tmp canonical: the worktree materialized off its
    # origin/main with the seeded content, NOT the empty decoy.
    worktree = canonical / ".claude" / "worktrees" / slug
    assert (worktree / "README.md").read_text(encoding="utf-8") == "seed\n"
    # (ii) The launched child inherited no repo-targeting GIT_* vars.
    rec = _wait_for_record(record)
    assert rec["git_env"] == []


# --- slot-write failure AFTER launch (fix A) ----------------------------------


def _wedge_slots_root(state: Path) -> Path:
    """Make <state>/pipeline-slots un-creatable while leaving <state> a real dir.

    The dispatch log lives under <state>/dispatch-logs (must be creatable so the launch
    SUCCEEDS), but the slot write must fail. So keep state_root a dir and plant a regular
    FILE at pipeline-slots: write_slot_file's mkdir(pipeline-slots, exist_ok=True) then
    raises FileExistsError (an OSError) because the path exists but is not a dir. The
    collision check (slot_path.exists() → False under a file) and budget check
    (slots_root.is_dir() → False) both pass cleanly first.
    """
    state.mkdir(parents=True, exist_ok=True)
    slots_root = state / "pipeline-slots"
    slots_root.write_text("not a dir\n", encoding="utf-8")
    return slots_root


def test_slot_write_failure_after_launch_exits_2_no_tmp_leak(
    tmp_path: Path, capsys, _hermetic_env: Path
) -> None:
    # The agent is ALREADY launched when the final slot write runs. If that write fails,
    # dispatch must convert to exit 2 (NOT crash with a traceback / exit 1) and name the pid
    # + dispatch log so the orphan can be adjudicated. No *.tmp file may be left behind.
    canonical = _make_origin(tmp_path)
    _stub_bin(_hermetic_env, "codex", jsonl=_CODEX_JSONL)
    state = tmp_path / "state"
    slots_root = _wedge_slots_root(state)

    with pytest.raises(SystemExit) as exc:
        cd.dispatch(
            _args(tmp_path, canonical, state_root=state),
            codex_id_timeout=5.0,
            codex_id_poll=0.02,
            launch_grace=_TEST_GRACE,
            launch_grace_poll=_TEST_GRACE_POLL,
        )

    message = str(exc.value.code)
    assert "slot write failed" in message
    assert "ALREADY LAUNCHED" in message
    # Names the pid and the dispatch log for adjudication.
    assert "pid=" in message
    log_path = state / "dispatch-logs" / "auto-codex-issue-1011.log"
    assert str(log_path) in message
    # The launch DID happen (fix A is about a failure AFTER launch): the log exists.
    assert log_path.exists()
    # No stray *.tmp leaked: pipeline-slots is still the plain file we planted, and no
    # sibling temp file was created next to it.
    assert slots_root.is_file()
    assert not list(state.glob("*.tmp"))


def test_slot_write_failure_through_main_is_exit_2(
    tmp_path: Path, capsys, _hermetic_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Same failure through main()'s real argv path: the string-code SystemExit maps to
    # exit 2 with the orphan-adjudication message on stderr, never a traceback/exit 1.
    canonical = _make_origin(tmp_path)
    _stub_bin(_hermetic_env, "codex", jsonl=_CODEX_JSONL)
    state = tmp_path / "state"
    _wedge_slots_root(state)
    _patch_fast_grace(
        monkeypatch
    )  # main() can't inject grace; keep the health check fast

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
    err = capsys.readouterr().err
    assert "slot write failed" in err
    assert "pid=" in err


# --- stale slot still consumes budget -----------------------------------------


def test_stale_slot_still_consumes_budget(
    tmp_path: Path, capsys, _hermetic_env: Path
) -> None:
    # Staleness is an ADJUDICATION signal, not auto-reclaim: a stale slot still occupies a
    # budget seat. 3 slots (one aged well past any stale threshold) → dispatch refuses with
    # exit 4, exactly as if all three were fresh.
    canonical = _make_origin(tmp_path)
    slots = tmp_path / "state" / "pipeline-slots"
    for slug in ("lane-a", "lane-b", "lane-c"):
        _write_slot(slots, slug)
    aged = time.time() - 100_000  # far past any reasonable stale threshold
    os.utime(slots / "lane-c.json", (aged, aged))

    rc = cd.dispatch(
        _args(tmp_path, canonical, state_root=tmp_path / "state", max_slots=3)
    )

    assert rc == 4
    assert "no free slot budget: busy 3/3" in capsys.readouterr().out
    _no_real_launch(tmp_path)


# --- codex session merge does not clobber the child's claim (finding 1) -------


def _stub_bin_child_claims(
    stub_bin: Path, name: str, slot_path: Path, prs: list
) -> Path:
    """A codex stub that mimics the child pipeline registering PRs during the id poll.

    It waits for the parent's step-6 ownership write to land (the slot file appears),
    merges `prs` into that file (its fresher claim), and ONLY THEN emits the thread id —
    so by the time poll_codex_session_id resolves, the child's prs are already on disk.
    dispatch's step-7 merge must preserve those prs while stamping the polled session.
    """
    stub_bin.mkdir(parents=True, exist_ok=True)
    record = stub_bin / f"{name}.record"
    script = stub_bin / name
    body = (
        "#!/usr/bin/env python3\n"
        "import json, os, sys, time\n"
        f"slot = {str(slot_path)!r}\n"
        f"prs = {prs!r}\n"
        "deadline = time.monotonic() + 5.0\n"
        "while time.monotonic() < deadline:\n"
        "    try:\n"
        "        cur = json.loads(open(slot).read())\n"
        "        break\n"
        "    except (OSError, ValueError):\n"
        "        time.sleep(0.01)\n"
        "else:\n"
        "    cur = {}\n"
        "cur['prs'] = prs\n"
        "open(slot, 'w').write(json.dumps(cur))\n"
        f"open({str(record)!r}, 'w').write(json.dumps("
        "{'argv': sys.argv[1:], 'cwd': os.getcwd()}))\n"
        "sys.stdout.write("
        '\'{"type":"thread.started","thread_id":"child-thread-42"}\' + chr(10))\n'
        "sys.stdout.flush()\n"
        "sys.exit(0)\n"
    )
    script.write_text(body, encoding="utf-8")
    script.chmod(0o755)
    return record


def test_codex_child_prs_survive_session_merge(
    tmp_path: Path, capsys, _hermetic_env: Path
) -> None:
    # The child pipeline can register drafts/PRs into the SAME slot during the id-poll
    # window. dispatch's step-7 merge must stamp the polled session WITHOUT clobbering
    # those prs: the final slot has BOTH the child's prs and the polled session.
    canonical = _make_origin(tmp_path)
    state = tmp_path / "state"
    slug = "auto-codex-issue-1011"
    slot_path = state / "pipeline-slots" / f"{slug}.json"
    _stub_bin_child_claims(_hermetic_env, "codex", slot_path, [4242])

    rc = cd.dispatch(
        _args(tmp_path, canonical, state_root=state),
        codex_id_timeout=5.0,
        codex_id_poll=0.02,
        launch_grace=_TEST_GRACE,
        launch_grace_poll=_TEST_GRACE_POLL,
    )

    assert rc == 0
    result = json.loads(capsys.readouterr().out)
    assert result["session"] == "child-thread-42"
    slot = json.loads(slot_path.read_text(encoding="utf-8"))
    # The child's prs survived; the polled session was merged in.
    assert slot["prs"] == [4242]
    assert slot["session"] == "child-thread-42"
    # Ownership fields still intact.
    assert slot["slot"] == slug
    assert slot["surface"] == "codex"
    assert slot["tier"] == "hard"


def test_merge_session_into_slot_vanished_file_rewrites_full_payload(
    tmp_path: Path,
) -> None:
    # If the slot file vanished/became unreadable before the merge, merge_session_into_slot
    # falls back to a full ownership rewrite from its own known-good values — the slot must
    # exist for the launched agent.
    slot_path = tmp_path / "pipeline-slots" / "auto-codex-issue-1011.json"
    slot_path.parent.mkdir(parents=True)
    # File does not exist at all.
    cd.merge_session_into_slot(
        slot_path, "auto-codex-issue-1011", [1011], "codex", "hard", "sid-9", 555
    )
    slot = json.loads(slot_path.read_text(encoding="utf-8"))
    assert slot["slot"] == "auto-codex-issue-1011"
    assert slot["issues"] == [1011]
    assert slot["prs"] == []
    assert slot["surface"] == "codex"
    assert slot["tier"] == "hard"
    assert slot["session"] == "sid-9"
    assert slot["pid"] == 555
    assert slot["dispatched"]


def test_merge_session_into_slot_invalid_json_rewrites_full_payload(
    tmp_path: Path,
) -> None:
    # A torn/garbage slot file is treated like a vanished one: full rewrite, not a crash.
    slot_path = tmp_path / "pipeline-slots" / "auto-codex-issue-1011.json"
    slot_path.parent.mkdir(parents=True)
    slot_path.write_text("{ not json", encoding="utf-8")
    cd.merge_session_into_slot(
        slot_path, "auto-codex-issue-1011", [1011], "codex", "hard", None, 777
    )
    slot = json.loads(slot_path.read_text(encoding="utf-8"))
    assert slot["session"] is None
    assert slot["pid"] == 777
    assert slot["prs"] == []


# --- reused per-slug log must not leak a prior run's thread id (finding 2) -----


def test_poll_codex_session_id_skips_stale_prefix(tmp_path: Path) -> None:
    # A per-slug dispatch log reused from a prior run holds an OLD thread.started; the poll
    # started past that offset must return the NEW id, never the stale one.
    log = tmp_path / "auto-codex-issue-1011.log"
    old = '{"type":"thread.started","thread_id":"OLD-run-id"}\n'
    log.write_text(old, encoding="utf-8")
    offset = len(old.encode("utf-8"))
    with log.open("a", encoding="utf-8") as fh:
        fh.write('{"type":"thread.started","thread_id":"NEW-run-id"}\n')
    got = cd.poll_codex_session_id(
        log, timeout=0.2, poll_interval=0.01, start_offset=offset
    )
    assert got == "NEW-run-id"


def test_poll_codex_session_id_no_new_id_returns_none_not_stale(tmp_path: Path) -> None:
    # Reused log with an old id, but THIS run appends nothing with an id → session null,
    # NOT the old id (which lives before the offset).
    log = tmp_path / "auto-codex-issue-1011.log"
    old = '{"type":"thread.started","thread_id":"OLD-run-id"}\n'
    log.write_text(old, encoding="utf-8")
    offset = len(old.encode("utf-8"))
    with log.open("a", encoding="utf-8") as fh:
        fh.write('{"type":"turn.started"}\n')  # no id this run
    got = cd.poll_codex_session_id(
        log, timeout=0.05, poll_interval=0.01, start_offset=offset
    )
    assert got is None


def test_reused_log_dispatch_records_new_id_not_stale(
    tmp_path: Path, capsys, _hermetic_env: Path
) -> None:
    # End-to-end: pre-seed the per-slug dispatch log with a PRIOR run's thread.started, then
    # dispatch with a stub emitting a NEW id. The pre-launch offset scopes the poll to this
    # run's bytes, so the slot records the new id, not the stale one.
    canonical = _make_origin(tmp_path)
    state = tmp_path / "state"
    slug = "auto-codex-issue-1011"
    log_path = state / "dispatch-logs" / f"{slug}.log"
    log_path.parent.mkdir(parents=True)
    log_path.write_text(
        '{"type":"thread.started","thread_id":"STALE-prior-run"}\n', encoding="utf-8"
    )
    _stub_bin(
        _hermetic_env,
        "codex",
        jsonl='{"type":"thread.started","thread_id":"fresh-run-id"}\n',
    )

    rc = cd.dispatch(
        _args(tmp_path, canonical, state_root=state),
        codex_id_timeout=5.0,
        codex_id_poll=0.02,
        launch_grace=_TEST_GRACE,
        launch_grace_poll=_TEST_GRACE_POLL,
    )

    assert rc == 0
    result = json.loads(capsys.readouterr().out)
    assert result["session"] == "fresh-run-id"
    slot = json.loads(
        (state / "pipeline-slots" / f"{slug}.json").read_text(encoding="utf-8")
    )
    assert slot["session"] == "fresh-run-id"


def test_reused_log_dispatch_no_new_id_records_null_not_stale(
    tmp_path: Path, capsys, _hermetic_env: Path
) -> None:
    # Reused log with a prior id, but this run's stub emits NO id → session null, not the
    # stale prior id.
    canonical = _make_origin(tmp_path)
    state = tmp_path / "state"
    slug = "auto-codex-issue-1011"
    log_path = state / "dispatch-logs" / f"{slug}.log"
    log_path.parent.mkdir(parents=True)
    log_path.write_text(
        '{"type":"thread.started","thread_id":"STALE-prior-run"}\n', encoding="utf-8"
    )
    _stub_bin(_hermetic_env, "codex", jsonl='{"type":"turn.started"}\n')

    rc = cd.dispatch(
        _args(tmp_path, canonical, state_root=state),
        codex_id_timeout=0.15,
        codex_id_poll=0.02,
        launch_grace=_TEST_GRACE,
        launch_grace_poll=_TEST_GRACE_POLL,
    )

    assert rc == 0
    captured = capsys.readouterr()
    assert "no codex session id" in captured.err
    result = json.loads(captured.out)
    assert result["session"] is None
    slot = json.loads(
        (state / "pipeline-slots" / f"{slug}.json").read_text(encoding="utf-8")
    )
    assert slot["session"] is None


# --- immediate launch-failure detection (finding 1) ---------------------------


@pytest.mark.parametrize("exit_code", [1, 0])
def test_instant_child_exit_is_launch_failure_no_slot(
    tmp_path: Path, _hermetic_env: Path, exit_code: int
) -> None:
    # Popen succeeding says nothing about viability: a child that exits within the grace
    # window (a rejected pinned flag, missing auth, bad config) is a launch FAILURE. Either
    # exit status inside the window counts — a real pipeline runs for minutes, so even a
    # clean rc=0 that fast is failure. dispatch must exit 2 naming the rc, the dispatch log
    # (which holds the child's stderr), and the leaked worktree — and write NO slot.
    canonical = _make_origin(tmp_path)
    # linger=0 → the stub records/emits, then exits immediately inside the grace window.
    _stub_bin(
        _hermetic_env, "codex", jsonl=_CODEX_JSONL, exit_code=exit_code, linger=0.0
    )
    state = tmp_path / "state"

    with pytest.raises(SystemExit) as exc:
        cd.dispatch(
            _args(tmp_path, canonical, state_root=state),
            codex_id_timeout=5.0,
            codex_id_poll=0.02,
            launch_grace=_FAIL_GRACE,  # ceiling; the check returns as soon as the child exits
            launch_grace_poll=_TEST_GRACE_POLL,
        )

    message = str(exc.value.code)
    assert "exited immediately" in message
    assert f"rc={exit_code}" in message
    slug = "auto-codex-issue-1011"
    worktree = canonical / ".claude" / "worktrees" / slug
    log_path = state / "dispatch-logs" / f"{slug}.log"
    # Names the dispatch log (child stderr) and the leaked worktree for adjudication.
    assert str(log_path) in message
    assert str(worktree) in message
    # NO slot file written: the dead slot must not squat on budget.
    assert not (state / "pipeline-slots" / f"{slug}.json").exists()


def test_instant_child_exit_through_main_is_exit_2(
    tmp_path: Path, capsys, _hermetic_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Same failure through main()'s argv path: string-code SystemExit → exit 2 on stderr,
    # never a traceback.
    canonical = _make_origin(tmp_path)
    _stub_bin(_hermetic_env, "codex", jsonl=_CODEX_JSONL, exit_code=3, linger=0.0)
    state = tmp_path / "state"
    _patch_fast_grace(monkeypatch, grace=_FAIL_GRACE)

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
    err = capsys.readouterr().err
    assert "exited immediately" in err
    assert "rc=3" in err
    assert not (state / "pipeline-slots" / "auto-codex-issue-1011.json").exists()


# --- initial slot write merges a pre-existing child claim (finding 2) ---------


def test_initial_slot_write_preserves_pre_existing_child_claim(tmp_path: Path) -> None:
    # If the detached child reaches its register-slot step BEFORE the parent's initial
    # ownership write, that write must NOT clobber the child's fresher record. write_slot_file
    # is an overlay: the child's prs survive, and the parent's ownership fields are stamped.
    state = tmp_path / "state"
    slug = "auto-codex-issue-1011"
    slot_path = state / "pipeline-slots" / f"{slug}.json"
    # Pre-create the child's claim (with prs) before the parent's initial write.
    slot_path.parent.mkdir(parents=True)
    slot_path.write_text(
        json.dumps({"slot": slug, "issues": [1011], "prs": [4242], "surface": "codex"}),
        encoding="utf-8",
    )

    cd.write_slot_file(slot_path, slug, [1011], "codex", "hard", "sid-1", 999)

    slot = json.loads(slot_path.read_text(encoding="utf-8"))
    # The child's fresher prs survived the initial ownership write.
    assert slot["prs"] == [4242]
    # The parent's ownership fields are present.
    assert slot["slot"] == slug
    assert slot["surface"] == "codex"
    assert slot["tier"] == "hard"
    assert slot["session"] == "sid-1"
    assert slot["pid"] == 999
    assert slot["dispatched"]


def test_initial_slot_write_full_payload_when_absent(tmp_path: Path) -> None:
    # No pre-existing slot → the initial write lays down the full ownership payload.
    slot_path = tmp_path / "pipeline-slots" / "auto-codex-issue-1011.json"
    slot_path.parent.mkdir(parents=True)
    cd.write_slot_file(
        slot_path, "auto-codex-issue-1011", [1011], "codex", "hard", None, 555
    )
    slot = json.loads(slot_path.read_text(encoding="utf-8"))
    assert slot["prs"] == []
    assert slot["issues"] == [1011]
    assert slot["session"] is None
    assert slot["pid"] == 555


# --- --state-root propagation to the child (finding 3) ------------------------


def test_state_root_propagated_as_xdg_state_home(
    tmp_path: Path, _hermetic_env: Path
) -> None:
    # A standard '.../registry-research-toolkit' --state-root is propagated to the child as
    # XDG_STATE_HOME=<parent>, so the child re-derives exactly this override for its own
    # ledger/gate stores instead of splitting them into the ambient store.
    canonical = _make_origin(tmp_path)
    record = _stub_bin(_hermetic_env, "codex", jsonl=_CODEX_JSONL)
    state = tmp_path / "xdg" / "registry-research-toolkit"

    rc = cd.dispatch(
        _args(tmp_path, canonical, state_root=state),
        codex_id_timeout=5.0,
        codex_id_poll=0.02,
        launch_grace=_TEST_GRACE,
        launch_grace_poll=_TEST_GRACE_POLL,
    )

    assert rc == 0
    rec = _wait_for_record(record)
    assert rec["xdg_state_home"] == str(state.parent)


def test_non_standard_state_root_warns_and_leaves_xdg_ambient(
    tmp_path: Path, capsys, _hermetic_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A non-standard --state-root name can't reconstruct a matching XDG_STATE_HOME, so we
    # leave the child's XDG ambient and warn on stderr. Pin the ambient XDG to a known value
    # to prove we did NOT override it.
    canonical = _make_origin(tmp_path)
    record = _stub_bin(_hermetic_env, "codex", jsonl=_CODEX_JSONL)
    state = tmp_path / "weird-state-root"
    monkeypatch.setenv("XDG_STATE_HOME", "/ambient/xdg")

    rc = cd.dispatch(
        _args(tmp_path, canonical, state_root=state),
        codex_id_timeout=5.0,
        codex_id_poll=0.02,
        launch_grace=_TEST_GRACE,
        launch_grace_poll=_TEST_GRACE_POLL,
    )

    assert rc == 0
    err = capsys.readouterr().err
    assert "not a" in err and "registry-research-toolkit" in err
    rec = _wait_for_record(record)
    # No override: the child kept the ambient XDG_STATE_HOME.
    assert rec["xdg_state_home"] == "/ambient/xdg"


# --- dispatch-log setup failure → exit 2 (finding 4) --------------------------


def test_dispatch_log_setup_failure_exits_2_names_worktree(
    tmp_path: Path, _hermetic_env: Path
) -> None:
    # If opening <state>/dispatch-logs/<slug>.log fails after the worktree exists (here:
    # dispatch-logs wedged as a regular FILE so mkdir raises), the OSError must become the
    # exit-2 orphan-adjudication path naming the leaked worktree — never a traceback (exit 1).
    # No process is launched (the failure precedes spawn) and no slot is written.
    canonical = _make_origin(tmp_path)
    _stub_bin(_hermetic_env, "codex", jsonl=_CODEX_JSONL)
    state = tmp_path / "state"
    # Wedge dispatch-logs as a regular file: log_path.parent.mkdir(exist_ok=True) then
    # raises FileExistsError. Keep pipeline-slots creatable so we prove the slot is absent
    # because the launch never happened, not because the slot write itself failed.
    (state / "dispatch-logs").parent.mkdir(parents=True, exist_ok=True)
    (state / "dispatch-logs").write_text("not a dir\n", encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        cd.dispatch(
            _args(tmp_path, canonical, state_root=state),
            launch_grace=_TEST_GRACE,
            launch_grace_poll=_TEST_GRACE_POLL,
        )

    message = str(exc.value.code)
    assert "dispatch log" in message
    slug = "auto-codex-issue-1011"
    worktree = canonical / ".claude" / "worktrees" / slug
    assert str(worktree) in message
    # No process was launched (failure precedes spawn): the fail-loud/recording stub for
    # codex was never invoked.
    _no_real_launch(tmp_path)
    assert not (_hermetic_env / "codex.record").exists()
    # No slot written.
    assert not (state / "pipeline-slots" / f"{slug}.json").exists()


# --- maintainer-author gate at the dispatch chokepoint (#1026 / #1028) ---------


_MAINT = "adamaltmejd"


def _stub_gh_author(
    monkeypatch: pytest.MonkeyPatch, authors: dict[int, str | None]
) -> None:
    """Drive the REAL require_maintainer_authored via a stubbed gh_issue surface.

    Overrides the autouse no-op so the real author gate runs, and stubs gh_issue's PUBLIC
    surface (`maintainer_login` for the error message + `is_maintainer_authored` for the
    verdict) — no live `gh`, and no reach into the privates the gate no longer touches.
    `authors` maps issue number → author login, None to model a null author, or omission
    to model a missing issue number; each resolves the same fail-closed bool the real
    is_maintainer_authored would.
    """
    monkeypatch.setattr(cd, "require_maintainer_authored", _REAL_REQUIRE_MAINTAINER)
    monkeypatch.setattr(cd._gh_issue, "maintainer_login", lambda: _MAINT)

    def fake_is_authored(number: int) -> bool:
        login = authors.get(number)  # absent OR null author → not maintainer-authored
        return login is not None and login.casefold() == _MAINT.casefold()

    monkeypatch.setattr(cd._gh_issue, "is_maintainer_authored", fake_is_authored)


@pytest.mark.parametrize("bad_author", ["stranger", None])
def test_non_maintainer_issue_refuses_before_side_effects(
    tmp_path: Path,
    capsys,
    _hermetic_env: Path,
    monkeypatch: pytest.MonkeyPatch,
    bad_author: str | None,
) -> None:
    # A non-maintainer (or null-author) issue must refuse with exit 2 naming the issue,
    # BEFORE any side effect: no worktree, no slot, no launch. bad_author=None models a
    # null author (fail-closed); the parametrize also covers a stranger login.
    canonical = _make_origin(tmp_path)
    _stub_bin(_hermetic_env, "codex", jsonl=_CODEX_JSONL)
    state = tmp_path / "state"
    _stub_gh_author(monkeypatch, {1011: bad_author})

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
    assert "#1011 is not maintainer-authored" in capsys.readouterr().err
    slug = "auto-codex-issue-1011"
    # No side effects: no worktree, no slot, no real launch.
    assert not (canonical / ".claude" / "worktrees" / slug).exists()
    assert not (state / "pipeline-slots").exists()
    _no_real_launch(tmp_path)


def test_missing_issue_refuses(
    tmp_path: Path, capsys, _hermetic_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A missing issue number (gh view non-zero → None) is refused just like a stranger's.
    canonical = _make_origin(tmp_path)
    state = tmp_path / "state"
    _stub_gh_author(monkeypatch, {})  # 1011 absent → _fetch_issue returns None

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
    assert "#1011 is not maintainer-authored" in capsys.readouterr().err
    assert not (state / "pipeline-slots").exists()
    _no_real_launch(tmp_path)


def test_one_bad_issue_in_lane_refuses_whole_dispatch(
    tmp_path: Path, capsys, _hermetic_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A multi-issue lane is refused if ANY issue fails the author check — and the error
    # names the offending one, not the maintainer-authored sibling.
    canonical = _make_origin(tmp_path)
    _stub_bin(_hermetic_env, "codex", jsonl=_CODEX_JSONL)
    state = tmp_path / "state"
    _stub_gh_author(monkeypatch, {1011: _MAINT, 1012: "stranger"})

    rc = cd.main(
        [
            "--issues",
            "1011,1012",
            "--state-root",
            str(state),
            "--canonical",
            str(canonical),
            "--no-canonical-check",
        ]
    )

    assert rc == 2
    err = capsys.readouterr().err
    assert "#1012 is not maintainer-authored" in err
    assert "#1011" not in err
    assert not (state / "pipeline-slots").exists()
    _no_real_launch(tmp_path)


def test_all_maintainer_issues_proceed_to_launch(
    tmp_path: Path, capsys, _hermetic_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # When every lane issue is maintainer-authored, the real author gate passes and the
    # dispatch proceeds to a normal launch + slot write.
    canonical = _make_origin(tmp_path)
    _stub_bin(_hermetic_env, "codex", jsonl=_CODEX_JSONL)
    state = tmp_path / "state"
    _stub_gh_author(
        monkeypatch, {1011: _MAINT, 1012: _MAINT.upper()}
    )  # case-insensitive

    rc = cd.dispatch(
        _args(tmp_path, canonical, issues="1011,1012", state_root=state),
        codex_id_timeout=5.0,
        codex_id_poll=0.02,
        launch_grace=_TEST_GRACE,
        launch_grace_poll=_TEST_GRACE_POLL,
    )

    assert rc == 0
    slug = "auto-codex-issue-1011"
    assert (canonical / ".claude" / "worktrees" / slug).is_dir()
    slot = json.loads(
        (state / "pipeline-slots" / f"{slug}.json").read_text(encoding="utf-8")
    )
    assert slot["issues"] == [1011, 1012]


def test_dry_run_skips_author_check(
    tmp_path: Path, capsys, monkeypatch: pytest.MonkeyPatch
) -> None:
    # --dry-run promises zero side effects AND no network: the author gate (read-only `gh`)
    # must NOT run in dry-run. Wire the real gate but make is_maintainer_authored raise if
    # called, then prove dry-run still returns 0 without touching it.
    canonical = _make_origin(tmp_path)
    state = tmp_path / "state"
    monkeypatch.setattr(cd, "require_maintainer_authored", _REAL_REQUIRE_MAINTAINER)
    monkeypatch.setattr(cd._gh_issue, "maintainer_login", lambda: _MAINT)

    def boom(number: int) -> bool:
        raise AssertionError("author check ran during --dry-run (network I/O)")

    monkeypatch.setattr(cd._gh_issue, "is_maintainer_authored", boom)

    rc = cd.dispatch(_args(tmp_path, canonical, state_root=state, dry_run=True))

    assert rc == 0
    assert json.loads(capsys.readouterr().out)["surface"] == "codex"


# --- visual-lane routing guard: codex can't run the browser gate (#1049) -------


@pytest.mark.parametrize(
    "touches, hits",
    [
        (["reg_webapp/frontend/src/App.svelte"], True),  # literal under the surface
        (["reg_webapp/frontend"], True),  # the surface dir itself
        (
            ["reg_webapp/frontend/"],
            True,
        ),  # trailing slash → .rstrip("/") still the surface
        (["reg_webapp"], True),  # literal ancestor dir of the surface
        (["reg_webapp/frontend/**"], True),  # glob rooted at the surface
        (["reg_webapp/**"], True),  # ancestor glob whose wildcard covers the surface
        # A wildcard mid-component: the fixed prefix `reg_webapp/fronten` stops inside the
        # `frontend` component, and the char-class could complete it to `frontend/…`. The
        # pre-fix literal path-boundary check missed this (fixed bug); a glob now uses plain
        # string-prefix relations so it hits.
        (["reg_webapp/fronten[dt]/x.svelte"], True),
        (["**/foo.svelte"], True),  # leading-wildcard glob → could match anywhere
        (["reg_webapp/backend/api.py"], False),  # sibling tree, not the surface
        (["reg_meta/db.py", "scripts/cos_dispatch.py"], False),  # unrelated
        ([], False),  # no touches → no signal
        (
            ["reg_webapp/frontend_notes.md"],
            False,
        ),  # literal prefix look-alike, not a child
    ],
)
def test_touches_visual_surface(touches: list[str], hits: bool) -> None:
    assert cd._touches_visual_surface(touches) is hits


def _stub_visual_bodies(
    monkeypatch: pytest.MonkeyPatch, bodies: dict[int, str | None]
) -> None:
    """Drive the REAL require_no_visual_lane_on_codex via a stubbed maintainer_body.

    Restores the real guard (over the autouse no-op) and stubs gh_issue.maintainer_body so
    each issue resolves to a canned body (None models a missing/non-maintainer/body-less
    issue → no signal). parse_touches stays REAL — the guard must parse a real fenced block.
    """
    monkeypatch.setattr(cd, "require_no_visual_lane_on_codex", _REAL_REQUIRE_NO_VISUAL)
    monkeypatch.setattr(cd._gh_issue, "maintainer_body", lambda n: bodies.get(n))


def _touches_body(*paths: str) -> str:
    inner = "\n".join(paths)
    return f"Some issue prose.\n\n```touches\n{inner}\n```\n"


def test_codex_visual_lane_refused_before_side_effects(
    tmp_path: Path, capsys, _hermetic_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # codex surface + an issue whose touches hit reg_webapp/frontend/** → refuse (exit 2),
    # naming the issue and pointing at the claude surface, BEFORE any side effect.
    canonical = _make_origin(tmp_path)
    _stub_bin(_hermetic_env, "codex", jsonl=_CODEX_JSONL)
    state = tmp_path / "state"
    _stub_visual_bodies(
        monkeypatch, {1011: _touches_body("reg_webapp/frontend/src/App.svelte")}
    )

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
    err = capsys.readouterr().err
    assert "#1011" in err
    assert "reg_webapp/frontend" in err
    assert "claude" in err  # actionable: directs to the claude surface
    slug = "auto-codex-issue-1011"
    # No side effects: no worktree, no slot, no real launch.
    assert not (canonical / ".claude" / "worktrees" / slug).exists()
    assert not (state / "pipeline-slots").exists()
    _no_real_launch(tmp_path)


def test_codex_ancestor_glob_touches_refused(
    tmp_path: Path, capsys, _hermetic_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A glob-y touches entry that covers the frontend tree (reg_webapp/**) → refusal, since
    # its wildcard could expand under the surface.
    canonical = _make_origin(tmp_path)
    _stub_bin(_hermetic_env, "codex", jsonl=_CODEX_JSONL)
    state = tmp_path / "state"
    _stub_visual_bodies(monkeypatch, {1011: _touches_body("reg_webapp/**")})

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
    assert "#1011" in capsys.readouterr().err
    assert not (state / "pipeline-slots").exists()
    _no_real_launch(tmp_path)


def test_codex_one_visual_issue_in_lane_names_only_the_offender(
    tmp_path: Path, capsys, _hermetic_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A multi-issue codex lane is refused if ANY issue touches the visual surface; the
    # error names the offender, not the backend-only sibling.
    canonical = _make_origin(tmp_path)
    _stub_bin(_hermetic_env, "codex", jsonl=_CODEX_JSONL)
    state = tmp_path / "state"
    _stub_visual_bodies(
        monkeypatch,
        {
            1011: _touches_body("reg_webapp/backend/api.py"),
            1012: _touches_body("reg_webapp/frontend/src/lib/ui/Foo.svelte"),
        },
    )

    rc = cd.main(
        [
            "--issues",
            "1011,1012",
            "--state-root",
            str(state),
            "--canonical",
            str(canonical),
            "--no-canonical-check",
        ]
    )

    assert rc == 2
    err = capsys.readouterr().err
    assert "#1012" in err
    assert "#1011" not in err
    _no_real_launch(tmp_path)


def test_codex_non_frontend_touches_proceeds(
    tmp_path: Path, capsys, _hermetic_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # codex surface + non-frontend touches → the guard passes and the dispatch launches.
    canonical = _make_origin(tmp_path)
    _stub_bin(_hermetic_env, "codex", jsonl=_CODEX_JSONL)
    state = tmp_path / "state"
    _stub_visual_bodies(monkeypatch, {1011: _touches_body("reg_meta/db.py")})

    rc = cd.dispatch(
        _args(tmp_path, canonical, state_root=state),
        codex_id_timeout=5.0,
        codex_id_poll=0.02,
        launch_grace=_TEST_GRACE,
        launch_grace_poll=_TEST_GRACE_POLL,
    )

    assert rc == 0
    slug = "auto-codex-issue-1011"
    assert (canonical / ".claude" / "worktrees" / slug).is_dir()


def test_claude_visual_lane_not_blocked(
    tmp_path: Path, capsys, _hermetic_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The guard is codex-only: a claude launch on a frontend-touching lane proceeds (claude
    # is unsandboxed and CAN run the visual gate). maintainer_body raises if called, proving
    # the guard is never reached on the claude surface.
    canonical = _make_origin(tmp_path)
    _stub_bin(_hermetic_env, "claude")
    state = tmp_path / "state"
    monkeypatch.setattr(cd, "require_no_visual_lane_on_codex", _REAL_REQUIRE_NO_VISUAL)

    def boom(number: int) -> str | None:
        raise AssertionError("visual guard ran on the claude surface")

    monkeypatch.setattr(cd._gh_issue, "maintainer_body", boom)

    rc = cd.dispatch(
        _args(tmp_path, canonical, tier="easy", state_root=state),
        launch_grace=_TEST_GRACE,
        launch_grace_poll=_TEST_GRACE_POLL,
    )

    assert rc == 0
    assert json.loads(capsys.readouterr().out)["surface"] == "claude"


def test_dry_run_skips_visual_guard(
    tmp_path: Path, capsys, monkeypatch: pytest.MonkeyPatch
) -> None:
    # --dry-run promises no network: the visual guard (body fetch per issue) must NOT run.
    # Wire the real guard but make maintainer_body raise; dry-run still returns 0 untouched.
    canonical = _make_origin(tmp_path)
    state = tmp_path / "state"
    monkeypatch.setattr(cd, "require_no_visual_lane_on_codex", _REAL_REQUIRE_NO_VISUAL)

    def boom(number: int) -> str | None:
        raise AssertionError("visual guard ran during --dry-run (network I/O)")

    monkeypatch.setattr(cd._gh_issue, "maintainer_body", boom)

    rc = cd.dispatch(_args(tmp_path, canonical, state_root=state, dry_run=True))

    assert rc == 0
    assert json.loads(capsys.readouterr().out)["surface"] == "codex"


def test_author_gate_runs_before_visual_gate(
    tmp_path: Path, capsys, _hermetic_env: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Ordering pin: step 3b (author gate) runs STRICTLY BEFORE step 3c (visual gate). An
    # untrusted issue must be rejected on authorship before the visual guard ever fetches
    # its body — so a stranger's crafted `touches` block never drives a network read. Stub
    # the author gate to refuse (SystemExit) and make the visual guard's body fetch a
    # fail-loud boom; dispatch on codex must exit 2 with boom never called.
    canonical = _make_origin(tmp_path)
    _stub_bin(_hermetic_env, "codex", jsonl=_CODEX_JSONL)
    state = tmp_path / "state"

    def refuse(issues: list[int]) -> None:
        raise SystemExit("author gate refused")

    def boom(number: int) -> str | None:
        raise AssertionError("visual guard ran before the author gate")

    monkeypatch.setattr(cd, "require_maintainer_authored", refuse)
    monkeypatch.setattr(cd, "require_no_visual_lane_on_codex", _REAL_REQUIRE_NO_VISUAL)
    monkeypatch.setattr(cd._gh_issue, "maintainer_body", boom)

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
    assert "author gate refused" in capsys.readouterr().err
    # No side effects, and the visual guard's body fetch was never reached.
    assert not (state / "pipeline-slots").exists()
    _no_real_launch(tmp_path)
