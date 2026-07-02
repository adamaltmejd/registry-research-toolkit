"""Unit tests for scripts/cos_preflight.py.

Pins the deterministic wake contract: probe never writes state, `--commit` writes it,
unchanged snapshots stay idle, lane drift wakes, and per-PR merge-gate changes name only
the PR(s) that actually moved.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "cos_preflight", _SCRIPTS / "cos_preflight.py"
)
assert _SPEC and _SPEC.loader
cpf = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = cpf
_SPEC.loader.exec_module(cpf)

HEAD = "abcdef1234567890"


def _snapshot(*, plan_exit=0, plan_report=None, prs=None, remote="l1"):
    snap = {
        "version": 1,
        "observed_at": "2026-06-30T20:00:00+00:00",
        "local_head": "l1",
        "remote_main": remote,
        "plan_tick": {
            "exit": plan_exit,
            "basis": "basis",
            "report": plan_report
            or "projection delta:\nno status changes\nlanes: fresh",
        },
        "prs": prs or [],
    }
    snap["fingerprint"] = cpf.snapshot_fingerprint(snap)
    return snap


def _ready_pr(number=956, *, draft=False, head=HEAD, **overrides):
    pr = {
        "number": number,
        "claimed": True,
        "issues": [742],
        "head": head,
        "draft": draft,
        "conflicting": False,
        "checks": "passing",
        "gate": {
            "state": "current-ready",
            "status": "ready-to-merge",
            "head": head,
            "current": True,
        },
        "codex_signal": "clean",
        "reviews": [],
    }
    pr.update(overrides)
    return pr


def test_parse_merge_gate_current_ready() -> None:
    body = f"""
    <!-- pr-pipeline-merge-gate -->
    - status: ready-to-merge
    - head: {HEAD}
    <!-- /pr-pipeline-merge-gate -->
    """

    gate = cpf.parse_merge_gate(body, HEAD)

    assert gate.pop("block_hash")
    assert gate == {
        "state": "current-ready",
        "status": "ready-to-merge",
        "head": HEAD,
        "current": True,
    }


def test_parse_merge_gate_stale_ready() -> None:
    body = """
    <!-- pr-pipeline-merge-gate -->
    - status: ready-to-merge
    - head: old
    <!-- /pr-pipeline-merge-gate -->
    """

    assert cpf.parse_merge_gate(body, HEAD)["state"] == "stale-ready"


def test_merge_gate_hash_changes_on_evidence_edit() -> None:
    body_a = f"""
    <!-- pr-pipeline-merge-gate -->
    - status: ready-to-merge
    - head: {HEAD}
    - ci: pass
    <!-- /pr-pipeline-merge-gate -->
    """
    body_b = body_a.replace("- ci: pass", "- ci: pass; refreshed")

    assert (
        cpf.parse_merge_gate(body_a, HEAD)["block_hash"]
        != cpf.parse_merge_gate(body_b, HEAD)["block_hash"]
    )


def test_checks_verdict_buckets() -> None:
    assert cpf.checks_verdict([]) == "none"
    assert cpf.checks_verdict([{"name": "test", "status": "IN_PROGRESS"}]) == "pending"
    assert (
        cpf.checks_verdict(
            [{"name": "test", "status": "COMPLETED", "conclusion": "FAILURE"}]
        )
        == "failing"
    )
    assert (
        cpf.checks_verdict(
            [{"name": "test", "status": "COMPLETED", "conclusion": "SUCCESS"}]
        )
        == "passing"
    )


def test_checks_verdict_legacy_failure_state_is_failing() -> None:
    # Legacy StatusContext: only `state`, no `conclusion`. FAILURE/ERROR must read as
    # failing, not fall through into "pending".
    assert cpf.checks_verdict([{"context": "ci", "state": "FAILURE"}]) == "failing"
    assert cpf.checks_verdict([{"context": "ci", "state": "ERROR"}]) == "failing"
    assert cpf.checks_verdict([{"context": "ci", "state": "SUCCESS"}]) == "passing"
    assert cpf.checks_verdict([{"context": "ci", "state": "PENDING"}]) == "pending"


# --- plan-tick crash vs signal -------------------------------------------------


def _fake_plan_proc(returncode: int, stderr: str):
    def run(cmd, **_kwargs):
        assert "plan_sequence.py" in cmd[1]
        return subprocess.CompletedProcess(
            cmd, returncode, stdout="basis", stderr=stderr
        )

    return run


def test_plan_tick_exit1_with_sentinel_is_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpf,
        "run_cmd",
        _fake_plan_proc(
            1, "projection delta:\nno status changes\nlanes: stale (re-rank)"
        ),
    )
    result = cpf.run_plan_tick(328)
    assert result["exit"] == 1
    assert "lanes: stale (re-rank)" in result["report"]


def test_plan_tick_exit2_with_sentinel_is_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpf,
        "run_cmd",
        _fake_plan_proc(2, "lanes: stale (re-stamp — running-set-only; no re-rank)"),
    )
    assert cpf.run_plan_tick(328)["exit"] == 2


def test_plan_tick_exit1_without_sentinel_is_tool_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An unhandled traceback exits 1 too; without the sentinel it's a tool error, not a
    # re-rank signal — otherwise the crash + its recovery read as two spurious wakes.
    monkeypatch.setattr(
        cpf, "run_cmd", _fake_plan_proc(1, "Traceback (most recent call last):\n  ...")
    )
    with pytest.raises(SystemExit):
        cpf.run_plan_tick(328)


def test_plan_tick_exit2_without_sentinel_is_tool_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpf, "run_cmd", _fake_plan_proc(2, "boom"))
    with pytest.raises(SystemExit):
        cpf.run_plan_tick(328)


def test_plan_tick_unknown_exit_is_tool_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpf, "run_cmd", _fake_plan_proc(3, "crashed"))
    with pytest.raises(SystemExit):
        cpf.run_plan_tick(328)


# --- state file semantics ------------------------------------------------------


def test_missing_executable_maps_to_setup_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing(_cmd, **_kwargs):
        raise FileNotFoundError("gh")

    monkeypatch.setattr(cpf.subprocess, "run", missing)

    with pytest.raises(SystemExit, match="missing executable"):
        cpf.run_cmd(["gh", "version"])


def test_corrupt_state_file_self_heals_as_first_run(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    state = tmp_path / "state.json"
    state.write_text("{", encoding="utf-8")

    assert cpf.load_state(state) is None
    assert "corrupt cos-preflight state file" in capsys.readouterr().err


def test_missing_state_file_is_first_run(tmp_path: Path) -> None:
    assert cpf.load_state(tmp_path / "absent.json") is None


def test_write_state_refuses_to_create_missing_parent(tmp_path: Path) -> None:
    # Guards the --no-canonical-check + missing .git footgun: never conjure the dir.
    missing = tmp_path / "nope" / "state.json"
    with pytest.raises(SystemExit, match="does not exist"):
        cpf.write_state(missing, _snapshot())
    assert not missing.parent.exists()


def test_write_state_round_trips(tmp_path: Path) -> None:
    state = tmp_path / "state.json"
    snap = _snapshot()
    cpf.write_state(state, snap)
    assert cpf.load_state(state) == snap


def test_pr_fetch_cap_hit_is_setup_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cpf, "gh_json", lambda _args: [{"number": 1}])

    with pytest.raises(SystemExit, match="open PR fetch hit"):
        cpf.fetch_pr_summaries(1, "owner/repo")


# --- snapshot noise collapse ---------------------------------------------------


def _raw_pr(number=956, *, mergeable="MERGEABLE", checks, closes_body=True, **extra):
    body = ""
    if closes_body:
        body = f"Closes #{742}"
    raw = {
        "number": number,
        "title": "t",
        "body": body,
        "closingIssuesReferences": [],
        "baseRefName": "main",
        "isDraft": False,
        "mergeable": mergeable,
        "headRefOid": HEAD,
        "statusCheckRollup": checks,
        "latestReviews": [],
    }
    raw.update(extra)
    return raw


def test_per_check_run_churn_does_not_change_entry() -> None:
    # Two snapshots with the SAME overall verdict (pending) but different individual
    # check-run transitions — the entry must not move, so per-check churn never wakes.
    a = cpf.summarize_pr(
        _raw_pr(
            checks=[
                {"name": "a", "status": "COMPLETED", "conclusion": "SUCCESS"},
                {"name": "b", "status": "IN_PROGRESS"},
            ]
        ),
        "owner/repo",
    )
    b = cpf.summarize_pr(
        _raw_pr(
            checks=[
                {"name": "a", "status": "IN_PROGRESS"},
                {"name": "b", "status": "COMPLETED", "conclusion": "SUCCESS"},
            ]
        ),
        "owner/repo",
    )
    assert a["checks"] == b["checks"] == "pending"
    assert a == b
    # No per-check-run list leaks into the entry.
    assert "check_runs" not in a
    # But a genuine overall-verdict flip DOES change the entry.
    passing = cpf.summarize_pr(
        _raw_pr(
            checks=[
                {"name": "a", "status": "COMPLETED", "conclusion": "SUCCESS"},
                {"name": "b", "status": "COMPLETED", "conclusion": "SUCCESS"},
            ]
        ),
        "owner/repo",
    )
    assert a["checks"] != passing["checks"]
    assert a != passing


def test_mergeable_unknown_flap_is_invisible() -> None:
    unknown = cpf.summarize_pr(_raw_pr(mergeable="UNKNOWN", checks=[]), "owner/repo")
    ok = cpf.summarize_pr(_raw_pr(mergeable="MERGEABLE", checks=[]), "owner/repo")
    assert unknown == ok
    assert unknown["conflicting"] is False


def test_conflicting_flip_changes_entry() -> None:
    ok = cpf.summarize_pr(_raw_pr(mergeable="MERGEABLE", checks=[]), "owner/repo")
    conflicting = cpf.summarize_pr(
        _raw_pr(mergeable="CONFLICTING", checks=[]), "owner/repo"
    )
    assert conflicting["conflicting"] is True
    assert ok != conflicting


# --- unclaimed PRs -------------------------------------------------------------


def test_unclaimed_pr_is_minimal_entry() -> None:
    entry = cpf.summarize_pr(
        _raw_pr(number=999, checks=[], closes_body=False), "owner/repo"
    )
    assert entry == {"number": 999, "claimed": False, "draft": False}
    # Deliberately no head SHA, so routine pushes to it don't wake.
    assert "head" not in entry


def test_new_unclaimed_pr_wakes_once_but_push_does_not() -> None:
    unclaimed = {"number": 999, "claimed": False, "draft": False}
    previous = _snapshot(prs=[])
    with_pr = _snapshot(prs=[unclaimed])

    # Appearance wakes.
    assert cpf.actionable_reasons(with_pr, previous) == ["open PR state changed"]
    # A push to it (no head SHA in the entry) leaves the entry unchanged → idle.
    same = _snapshot(prs=[dict(unclaimed)])
    assert cpf.actionable_reasons(same, with_pr) == []


# --- latestReviews -------------------------------------------------------------


def test_latest_reviews_folded_into_issue_closing_pr() -> None:
    raw = _raw_pr(
        checks=[],
        latestReviews=[
            {
                "author": {"login": "chatgpt-codex-connector"},
                "submittedAt": "2026-07-01T00:00:00Z",
            },
        ],
    )
    entry = cpf.summarize_pr(raw, "owner/repo")
    assert entry["reviews"] == [
        {"author": "chatgpt-codex-connector", "submitted_at": "2026-07-01T00:00:00Z"}
    ]


def test_new_review_wakes() -> None:
    before = _snapshot(prs=[_ready_pr(reviews=[])])
    after = _snapshot(
        prs=[
            _ready_pr(
                reviews=[
                    {
                        "author": "chatgpt-codex-connector",
                        "submitted_at": "2026-07-01T00:00:00Z",
                    }
                ]
            )
        ]
    )
    assert "open PR state changed" in cpf.actionable_reasons(after, before)


# --- wake reasons --------------------------------------------------------------


def test_repeated_snapshot_is_idle() -> None:
    snap = _snapshot(
        plan_exit=1,
        plan_report="projection delta:\nno status changes\nlanes: stale (re-rank)",
    )

    assert cpf.actionable_reasons(snap, snap) == []


def test_lane_rerank_wakes_on_new_snapshot() -> None:
    snap = _snapshot(
        plan_exit=1,
        plan_report="projection delta:\nno status changes\nlanes: stale (re-rank)",
    )

    assert cpf.actionable_reasons(snap, None) == ["lanes need re-rank"]


def test_ready_gate_wakes_on_first_observation() -> None:
    snap = _snapshot(prs=[_ready_pr()])

    assert cpf.actionable_reasons(snap, None) == ["ready merge-gate PR changed: #956"]


def test_draft_ready_gate_wakes_on_first_observation() -> None:
    snap = _snapshot(prs=[_ready_pr(draft=True)])

    assert cpf.actionable_reasons(snap, None) == [
        "draft PR has ready merge-gate block: #956"
    ]


def test_gate_reason_names_only_changed_pr() -> None:
    # Two ready PRs; only #957 changes between snapshots. The reason must name #957 only,
    # not the unchanged #956.
    p956 = _ready_pr(956)
    p957 = _ready_pr(957)
    previous = _snapshot(prs=[p956, p957])
    p957_changed = _ready_pr(957, codex_signal="findings")
    snap = _snapshot(prs=[p956, p957_changed])

    reasons = cpf.actionable_reasons(snap, previous)
    assert "ready merge-gate PR changed: #957" in reasons
    assert "#956" not in " ".join(reasons)


def test_remote_main_change_wakes() -> None:
    previous = _snapshot(remote="old")
    snap = _snapshot(remote="new")

    assert cpf.actionable_reasons(snap, previous) == ["origin/main changed"]


def test_first_snapshot_behind_origin_wakes() -> None:
    snap = _snapshot(remote="new")
    snap["local_head"] = "old"
    snap["fingerprint"] = cpf.snapshot_fingerprint(snap)

    assert cpf.actionable_reasons(snap, None) == ["origin/main changed"]


# --- probe vs commit -----------------------------------------------------------


def test_probe_never_writes_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state.json"
    snap = _snapshot(remote="new")
    monkeypatch.setattr(cpf, "require_canonical", lambda _c: None)
    monkeypatch.setattr(cpf, "collect_snapshot", lambda _e, _l: snap)

    rc = cpf.main(["--no-canonical-check", "--state-file", str(state)])

    assert rc == cpf.WAKE_EXIT  # first-run, behind origin → wakes
    assert not state.exists()  # probe MUST NOT write


def test_probe_idle_returns_zero_and_does_not_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state.json"
    snap = _snapshot()
    cpf.write_state(state, snap)
    state.write_text(state.read_text())  # ensure committed
    monkeypatch.setattr(cpf, "require_canonical", lambda _c: None)
    monkeypatch.setattr(cpf, "collect_snapshot", lambda _e, _l: snap)
    before = state.read_text()

    rc = cpf.main(["--no-canonical-check", "--state-file", str(state)])

    assert rc == 0
    assert state.read_text() == before


def test_commit_writes_state_and_exits_zero(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    state = tmp_path / "state.json"
    snap = _snapshot()
    monkeypatch.setattr(cpf, "require_canonical", lambda _c: None)
    monkeypatch.setattr(cpf, "collect_snapshot", lambda _e, _l: snap)

    rc = cpf.main(["--no-canonical-check", "--commit", "--state-file", str(state)])

    assert rc == 0
    assert cpf.load_state(state) == snap
    assert "committed cos-preflight snapshot" in capsys.readouterr().out


def test_commit_tool_error_returns_two(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def boom(_e, _l):
        raise SystemExit("gh exploded")

    monkeypatch.setattr(cpf, "require_canonical", lambda _c: None)
    monkeypatch.setattr(cpf, "collect_snapshot", boom)

    rc = cpf.main(
        ["--no-canonical-check", "--commit", "--state-file", str(tmp_path / "s.json")]
    )
    assert rc == 2


def test_no_dry_run_flag() -> None:
    # --dry-run is retired; probe is now the default no-write mode.
    with pytest.raises(SystemExit):
        cpf.main(["--no-canonical-check", "--dry-run"])
