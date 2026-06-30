"""Unit tests for scripts/cos_preflight.py.

Pins the deterministic wake contract: unchanged snapshots stay idle, lane drift wakes,
and ready merge-gate PR changes wake the real chief-of-staff thread.
"""

from __future__ import annotations

import importlib.util
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


def test_missing_executable_maps_to_setup_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def missing(_cmd, **_kwargs):
        raise FileNotFoundError("gh")

    monkeypatch.setattr(cpf.subprocess, "run", missing)

    with pytest.raises(SystemExit, match="missing executable"):
        cpf.run_cmd(["gh", "version"])


def test_invalid_state_file_is_setup_error(tmp_path: Path) -> None:
    state = tmp_path / "state.json"
    state.write_text("{", encoding="utf-8")

    with pytest.raises(SystemExit, match="invalid cos-preflight state file"):
        cpf.load_state(state)


def test_pr_fetch_cap_hit_is_setup_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cpf, "gh_json", lambda _args: [{"number": 1}])

    with pytest.raises(SystemExit, match="open PR fetch hit"):
        cpf.fetch_pr_summaries(1, "owner/repo")


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
    snap = _snapshot(
        prs=[
            {
                "number": 956,
                "issues": [742],
                "head": HEAD,
                "draft": False,
                "mergeable": "MERGEABLE",
                "checks": "passing",
                "check_runs": [],
                "gate": {
                    "state": "current-ready",
                    "status": "ready-to-merge",
                    "head": HEAD,
                    "current": True,
                },
                "codex_signal": "clean",
            }
        ]
    )

    assert cpf.actionable_reasons(snap, None) == ["ready merge-gate PR changed: #956"]


def test_draft_ready_gate_wakes_on_first_observation() -> None:
    snap = _snapshot(
        prs=[
            {
                "number": 956,
                "issues": [742],
                "head": HEAD,
                "draft": True,
                "mergeable": "MERGEABLE",
                "checks": "passing",
                "check_runs": [],
                "gate": {
                    "state": "current-ready",
                    "status": "ready-to-merge",
                    "head": HEAD,
                    "current": True,
                },
                "codex_signal": "clean",
            }
        ]
    )

    assert cpf.actionable_reasons(snap, None) == [
        "draft PR has ready merge-gate block: #956"
    ]


def test_remote_main_change_wakes() -> None:
    previous = _snapshot(remote="old")
    snap = _snapshot(remote="new")

    assert cpf.actionable_reasons(snap, previous) == ["origin/main changed"]


def test_first_snapshot_behind_origin_wakes() -> None:
    snap = _snapshot(remote="new")
    snap["local_head"] = "old"
    snap["fingerprint"] = cpf.snapshot_fingerprint(snap)

    assert cpf.actionable_reasons(snap, None) == ["origin/main changed"]
