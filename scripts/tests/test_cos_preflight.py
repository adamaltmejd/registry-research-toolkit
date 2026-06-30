"""Unit tests for scripts/cos_preflight.py.

Pins the deterministic wake contract: unchanged snapshots stay idle, lane drift wakes,
and ready merge-gate PR changes wake the real chief-of-staff thread.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "cos_preflight", _SCRIPTS / "cos_preflight.py"
)
assert _SPEC and _SPEC.loader
cpf = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = cpf
_SPEC.loader.exec_module(cpf)

HEAD = "abcdef1234567890"


def _snapshot(*, plan_exit=0, plan_report=None, prs=None, remote="r1"):
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


def test_remote_main_change_wakes() -> None:
    previous = _snapshot(remote="old")
    snap = _snapshot(remote="new")

    assert cpf.actionable_reasons(snap, previous) == ["origin/main changed"]
