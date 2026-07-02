"""Unit tests for scripts/cos_preflight.py.

Pins the deterministic wake contract: the probe stages a candidate (and bootstraps a
baseline only on an IDLE first run), a steady-state probe never writes the state file,
`--commit <fingerprint>` promotes the observed candidate via an atomic rename bound to that
fingerprint (idempotent on retry, refused when stale/mismatched), unchanged snapshots stay
idle, lane drift wakes, and per-PR merge-gate changes name only the PR(s) that moved.
"""

from __future__ import annotations

import importlib.util
import json
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
        "version": cpf.SNAPSHOT_VERSION,
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
        "mergeable": "MERGEABLE",  # gate PRs carry the verbatim tri-state
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


def _plan_tick_stderr(verdict: str) -> str:
    # Compose the fake plan-tick stderr from plan_sequence's own _FRESHNESS_MSG, so the
    # tests pin the sentinel CONTRACT (cos_preflight must accept whatever wording
    # plan_sequence emits) instead of retyping literals that can silently drift.
    return (
        "projection delta:\nno status changes\n"
        f"lanes: {cpf._plan_sequence._FRESHNESS_MSG[verdict]}"
    )


def test_plan_tick_exit1_with_sentinel_is_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cpf, "run_cmd", _fake_plan_proc(1, _plan_tick_stderr("rerank")))
    result = cpf.run_plan_tick(328)
    assert result["exit"] == 1
    assert cpf.PLAN_TICK_SENTINELS[1] in result["report"]


def test_plan_tick_exit2_with_sentinel_is_signal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cpf, "run_cmd", _fake_plan_proc(2, _plan_tick_stderr("restamp"))
    )
    assert cpf.run_plan_tick(328)["exit"] == 2


def test_plan_tick_sentinels_derived_from_plan_sequence() -> None:
    # Reword-resilience: the sentinel map is derived from plan_sequence's _FRESHNESS_MSG,
    # not retyped, so a wording change there flows through instead of misclassifying a real
    # verdict as a crash.
    expected = {
        cpf._plan_sequence._FRESHNESS_EXIT[
            v
        ]: f"lanes: {cpf._plan_sequence._FRESHNESS_MSG[v]}"
        for v in ("rerank", "restamp")
    }
    assert expected == cpf.PLAN_TICK_SENTINELS
    # The derivation actually consumed plan_sequence's wording, not a hardcoded copy.
    assert cpf._plan_sequence._FRESHNESS_MSG["rerank"] in cpf.PLAN_TICK_SENTINELS[1]


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


def test_version_mismatch_state_is_first_run(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    # A schema bump makes an old baseline incomparable; load_state must treat it as
    # first-run so the bootstrap re-baselines instead of comparing incompatible shapes.
    state = tmp_path / "state.json"
    stale = _snapshot()
    stale["version"] = cpf.SNAPSHOT_VERSION - 1
    cpf.write_state(state, stale)

    assert cpf.load_state(state) is None
    assert "incompatible version" in capsys.readouterr().err


def test_write_state_refuses_to_create_missing_parent(tmp_path: Path) -> None:
    # Guards the --no-canonical-check + missing .git footgun: never conjure the dir.
    missing = tmp_path / "nope" / "state.json"
    with pytest.raises(SystemExit, match="is not a directory"):
        cpf.write_state(missing, _snapshot())
    assert not missing.parent.exists()


def test_write_state_refuses_git_file_parent(tmp_path: Path) -> None:
    # A linked worktree's `.git` is a FILE, not a dir. parent.exists() would pass and let
    # NamedTemporaryFile raise an uncaught NotADirectoryError (breaking the exit-2
    # contract); parent.is_dir() must reject it cleanly.
    git_file = tmp_path / ".git"
    git_file.write_text("gitdir: /elsewhere\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="is not a directory"):
        cpf.write_state(git_file / "state.json", _snapshot())


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


def _gate_raw_pr(*, mergeable, checks=None, status="present-only", **extra):
    # A claimed PR that carries a merge-gate block (gate state != "absent") but is NOT
    # current-ready, so summarize_pr does not fetch the Codex signal (no network).
    body = (
        f"Closes #742\n<!-- pr-pipeline-merge-gate -->\n"
        f"- status: {status}\n- head: {HEAD}\n<!-- /pr-pipeline-merge-gate -->"
    )
    return _raw_pr(
        mergeable=mergeable, checks=checks or [], closes_body=False, body=body, **extra
    )


def test_gate_pr_stores_verbatim_tristate_mergeability() -> None:
    entry = cpf.summarize_pr(_gate_raw_pr(mergeable="UNKNOWN"), "owner/repo")
    assert entry["gate"]["state"] != "absent"
    assert entry["mergeable"] == "UNKNOWN"
    assert "conflicting" not in entry  # gate PRs carry the tri-state, not the boolean


def test_gate_pr_unknown_to_mergeable_wakes() -> None:
    # A tick may defer a merge while mergeability is UNKNOWN; it must wake when GitHub
    # resolves UNKNOWN→MERGEABLE. The verbatim tri-state makes that transition visible.
    unknown = cpf.summarize_pr(_gate_raw_pr(mergeable="UNKNOWN"), "owner/repo")
    resolved = cpf.summarize_pr(_gate_raw_pr(mergeable="MERGEABLE"), "owner/repo")

    assert unknown != resolved  # entry moved → the snapshot fingerprint changes
    before = _snapshot(prs=[unknown])
    after = _snapshot(prs=[resolved])
    assert "open PR state changed" in cpf.actionable_reasons(after, before)


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

    # Appearance wakes with the unclaimed-PR named reason (not the generic one).
    assert cpf.actionable_reasons(with_pr, previous) == [
        "unclaimed open PR (no Closes, no gate block): #999"
    ]
    # A push to it (no head SHA in the entry) leaves the entry unchanged → idle.
    same = _snapshot(prs=[dict(unclaimed)])
    assert cpf.actionable_reasons(same, with_pr) == []


def test_first_run_unclaimed_pr_wakes_with_named_reason() -> None:
    # On first run (no previous) every PR is `changed`; a pre-existing unclaimed PR must
    # surface via its named reason, since the generic reason is previous-gated and would
    # otherwise never fire here (the bug: idle bootstrap silently absorbing claim drift).
    snap = _snapshot(prs=[{"number": 999, "claimed": False, "draft": False}])

    assert cpf.actionable_reasons(snap, None) == [
        "unclaimed open PR (no Closes, no gate block): #999"
    ]


def test_steady_state_new_unclaimed_pr_named_not_generic() -> None:
    previous = _snapshot(prs=[])
    snap = _snapshot(prs=[{"number": 999, "claimed": False, "draft": False}])

    reasons = cpf.actionable_reasons(snap, previous)

    assert reasons == ["unclaimed open PR (no Closes, no gate block): #999"]
    assert "open PR state changed" not in reasons


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
    # A claimed, not-yet-ready PR (gate absent → no named bucket): a new review lands, so
    # the generic "open PR state changed" reason must fire.
    plain_pr = {
        "number": 956,
        "claimed": True,
        "draft": False,
        "gate": {"state": "absent"},
        "reviews": [],
    }
    before = _snapshot(prs=[dict(plain_pr)])
    after = _snapshot(
        prs=[
            dict(
                plain_pr,
                reviews=[
                    {
                        "author": "chatgpt-codex-connector",
                        "submitted_at": "2026-07-01T00:00:00Z",
                    }
                ],
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


def test_generic_reason_suppressed_when_all_changed_prs_are_gate_named() -> None:
    # A gate-state PR change already emits its specific named reason; the generic
    # "open PR state changed" must NOT also fire for it.
    previous = _snapshot(prs=[_ready_pr(957)])
    snap = _snapshot(prs=[_ready_pr(957, codex_signal="findings")])

    reasons = cpf.actionable_reasons(snap, previous)

    assert reasons == ["ready merge-gate PR changed: #957"]
    assert "open PR state changed" not in reasons


def _claimed_nongate_pr(number=956, **overrides):
    # A claimed PR whose gate state is not one of the named buckets (e.g. still under
    # review): it hits no named bucket, so a change to it surfaces via the generic reason.
    pr = {
        "number": number,
        "claimed": True,
        "draft": False,
        "gate": {"state": "present"},
        "reviews": [],
    }
    pr.update(overrides)
    return pr


def test_generic_reason_fires_for_non_gate_pr_change() -> None:
    # A claimed, not-yet-ready PR that changes has no named bucket, so the generic reason
    # is the only signal and must fire.
    previous = _snapshot(prs=[_claimed_nongate_pr()])
    snap = _snapshot(prs=[_claimed_nongate_pr(reviews=[{"author": "x"}])])

    assert cpf.actionable_reasons(snap, previous) == ["open PR state changed"]


def test_gate_named_and_non_gate_changes_emit_both_reasons() -> None:
    # A gate PR AND a claimed non-gate PR both change: the gate PR gets its named reason,
    # and the generic reason still fires for the non-gate PR that no bucket named.
    plain = _claimed_nongate_pr(958)
    previous = _snapshot(prs=[_ready_pr(957), plain])
    snap = _snapshot(
        prs=[
            _ready_pr(957, codex_signal="findings"),
            _claimed_nongate_pr(958, reviews=[{"author": "x"}]),
        ]
    )

    reasons = cpf.actionable_reasons(snap, previous)

    assert "ready merge-gate PR changed: #957" in reasons
    assert "open PR state changed" in reasons


def test_remote_main_change_wakes() -> None:
    previous = _snapshot(remote="old")
    snap = _snapshot(remote="new")

    assert cpf.actionable_reasons(snap, previous) == ["origin/main changed"]


def test_first_snapshot_behind_origin_wakes() -> None:
    snap = _snapshot(remote="new")
    snap["local_head"] = "old"
    snap["fingerprint"] = cpf.snapshot_fingerprint(snap)

    assert cpf.actionable_reasons(snap, None) == ["origin/main changed"]


# --- probe: bootstrap + candidate staging -------------------------------------


def _probe_env(monkeypatch: pytest.MonkeyPatch, snap: dict) -> None:
    monkeypatch.setattr(cpf, "require_canonical", lambda _c: None)
    monkeypatch.setattr(cpf, "collect_snapshot", lambda _e, _l: snap)


def _no_collect(monkeypatch: pytest.MonkeyPatch) -> None:
    # --commit must never collect a snapshot or hit the network — a call is a bug.
    monkeypatch.setattr(cpf, "require_canonical", lambda _c: None)
    monkeypatch.setattr(
        cpf, "collect_snapshot", lambda *_a: pytest.fail("--commit must not collect")
    )


def _probe_fingerprint(capsys: pytest.CaptureFixture[str]) -> str:
    # The commit fingerprint the loop passes to `--commit` comes from the probe's own
    # result JSON on stdout.
    return json.loads(capsys.readouterr().out)["fingerprint"]


def test_idle_first_run_bootstraps_baseline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # No baseline AND nothing to handle: the probe baselines the state file directly (safe —
    # no events to burn) and stages the candidate too.
    state = tmp_path / "state.json"
    snap = _snapshot()  # remote == local_head, plan fresh, no PRs → idle
    _probe_env(monkeypatch, snap)

    rc = cpf.main(["--no-canonical-check", "--state-file", str(state)])

    assert rc == 0  # idle
    assert cpf.load_state(state) == snap  # bootstrap wrote the baseline
    assert cpf.load_state(cpf.candidate_file(state)) == snap  # candidate staged too
    assert "bootstrap" in capsys.readouterr().err


def test_waking_first_run_does_not_write_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # No baseline but the first-run reasons WAKE: the probe must NOT baseline the state file
    # (a crash before the end-of-tick commit would then burn these events). It stages only
    # the candidate; the baseline is established by the later `--commit <fp>`.
    state = tmp_path / "state.json"
    snap = _snapshot(remote="new")  # first-run behind origin → wakes
    _probe_env(monkeypatch, snap)

    rc = cpf.main(["--no-canonical-check", "--state-file", str(state)])

    assert rc == cpf.WAKE_EXIT
    assert not state.exists()  # NO bootstrap on a waking first run
    assert cpf.load_state(cpf.candidate_file(state)) == snap  # candidate staged
    assert "bootstrap" not in capsys.readouterr().err


def test_first_run_with_unclaimed_pr_wakes_and_does_not_bootstrap_idle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # The exact PR-987 bug: a first-run probe (which every install hits after the
    # SNAPSHOT_VERSION bump) with a pre-existing unclaimed PR must WAKE on the named reason
    # rather than silently idle-bootstrapping the claim drift into the baseline.
    state = tmp_path / "state.json"
    snap = _snapshot(prs=[{"number": 999, "claimed": False, "draft": False}])
    _probe_env(monkeypatch, snap)

    rc = cpf.main(["--no-canonical-check", "--state-file", str(state)])
    out = capsys.readouterr()

    assert rc == cpf.WAKE_EXIT
    assert "unclaimed open PR (no Closes, no gate block): #999" in out.out
    assert not state.exists()  # NOT idle-bootstrapped
    assert "bootstrap" not in out.err


def test_crashed_first_run_wake_refires_next_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # At-least-once on the bootstrap path: a waking first run whose tick crashes before
    # --commit left no baseline, so the next probe re-observes the same first-run wake.
    state = tmp_path / "state.json"
    snap = _snapshot(remote="new")
    _probe_env(monkeypatch, snap)

    assert (
        cpf.main(["--no-canonical-check", "--state-file", str(state)]) == cpf.WAKE_EXIT
    )
    assert not state.exists()  # tick "crashes" here — no commit ran

    # Next probe: still first-run (no baseline), so it wakes again — events not burned.
    assert (
        cpf.main(["--no-canonical-check", "--state-file", str(state)]) == cpf.WAKE_EXIT
    )


def test_waking_first_run_then_commit_establishes_baseline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # The intended happy path for a waking first run: probe (wake, stage candidate) →
    # --commit <fp> establishes the baseline from the staged candidate.
    state = tmp_path / "state.json"
    snap = _snapshot(remote="new")
    _probe_env(monkeypatch, snap)

    assert (
        cpf.main(["--no-canonical-check", "--state-file", str(state)]) == cpf.WAKE_EXIT
    )
    fp = _probe_fingerprint(capsys)

    _no_collect(monkeypatch)
    assert (
        cpf.main(["--no-canonical-check", "--commit", fp, "--state-file", str(state)])
        == 0
    )
    assert cpf.load_state(state) == snap  # baseline established
    assert not cpf.candidate_file(state).exists()  # candidate consumed


def test_idle_first_run_bootstrap_on_corrupt_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state.json"
    state.write_text("{", encoding="utf-8")  # corrupt → no baseline
    snap = _snapshot()  # idle
    _probe_env(monkeypatch, snap)

    cpf.main(["--no-canonical-check", "--state-file", str(state)])

    assert cpf.load_state(state) == snap  # re-baselined cleanly


def test_idle_first_run_bootstrap_on_version_mismatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state.json"
    stale = _snapshot()
    stale["version"] = cpf.SNAPSHOT_VERSION - 1
    cpf.write_state(state, stale)
    snap = _snapshot()  # idle first-run (mismatch treated as no baseline)
    _probe_env(monkeypatch, snap)

    cpf.main(["--no-canonical-check", "--state-file", str(state)])

    assert cpf.load_state(state) == snap  # re-baselined to the new shape


def test_idle_probe_without_drift_does_not_rewrite_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Fingerprint-equality idle probe: live state == baseline, so the auto-advance
    # invariant must NOT rewrite the file (no spurious writes). It only stages the
    # candidate.
    state = tmp_path / "state.json"
    baseline = _snapshot()
    cpf.write_state(state, baseline)
    before = state.read_text()
    _probe_env(monkeypatch, baseline)

    rc = cpf.main(["--no-canonical-check", "--state-file", str(state)])

    assert rc == 0
    assert state.read_text() == before  # state file untouched (fingerprints equal)
    assert cpf.load_state(cpf.candidate_file(state)) == baseline  # candidate staged


def test_idle_probe_with_drift_advances_baseline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Idle follow-up probe whose live state drifted to a reason-free fingerprint: the
    # invariant advances the baseline directly (safe — zero reasons, nothing to burn), so a
    # later return to a previously-committed fingerprint is not suppressed forever.
    state = tmp_path / "state.json"
    baseline = _snapshot()  # remote == local_head, plan fresh, no PRs → idle
    cpf.write_state(state, baseline)
    # A reason-free drift: the plan_tick `basis` string is part of the fingerprint but is
    # NOT evaluated by actionable_reasons (only plan exit/report are), so moving it shifts
    # the fingerprint while zero actionable reasons fire.
    drifted = _snapshot()
    drifted["plan_tick"] = dict(drifted["plan_tick"], basis="drifted-basis")
    drifted["fingerprint"] = cpf.snapshot_fingerprint(drifted)
    assert drifted["fingerprint"] != baseline["fingerprint"]
    _probe_env(monkeypatch, drifted)

    rc = cpf.main(["--no-canonical-check", "--state-file", str(state)])

    assert rc == 0  # idle
    assert (
        cpf.load_state(state) == drifted
    )  # baseline advanced to the drifted fingerprint
    assert "advanced idle" in capsys.readouterr().err


def test_recurrence_after_idle_drift_still_wakes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # The Codex recurrence scenario end-to-end: wake at fp A → commit A → idle drift to a
    # reason-free B advances the baseline → live state returns to exactly A → the probe
    # WAKES again (not suppressed by the fingerprint-equality early return against A).
    state = tmp_path / "state.json"
    monkeypatch.setattr(cpf, "require_canonical", lambda _c: None)

    # Baseline established at a benign idle fingerprint (idle bootstrap).
    base = _snapshot()  # remote == local_head, plan fresh, no PRs → idle
    monkeypatch.setattr(cpf, "collect_snapshot", lambda _e, _l: base)
    assert cpf.main(["--no-canonical-check", "--state-file", str(state)]) == 0
    capsys.readouterr()

    # Probe wakes on a lane re-rank (fingerprint A) → commit A.
    snap_a = _snapshot(
        plan_exit=1,
        plan_report="projection delta:\nno status changes\nlanes: stale (re-rank)",
    )
    monkeypatch.setattr(cpf, "collect_snapshot", lambda _e, _l: snap_a)
    assert (
        cpf.main(["--no-canonical-check", "--state-file", str(state)]) == cpf.WAKE_EXIT
    )
    fp_a = _probe_fingerprint(capsys)
    _no_collect(monkeypatch)
    assert (
        cpf.main(["--no-canonical-check", "--commit", fp_a, "--state-file", str(state)])
        == 0
    )
    assert cpf.load_state(state)["fingerprint"] == fp_a

    # Idle drift to reason-free B (lanes repaired: plan fresh) → baseline advances to B.
    snap_b = _snapshot()  # fresh plan, no PRs → idle; fingerprint differs from A
    assert snap_b["fingerprint"] != fp_a
    monkeypatch.setattr(cpf, "collect_snapshot", lambda _e, _l: snap_b)
    assert cpf.main(["--no-canonical-check", "--state-file", str(state)]) == 0
    assert cpf.load_state(state)["fingerprint"] == snap_b["fingerprint"]

    # Wake condition recurs: live state returns to EXACTLY A. Because the baseline is now B,
    # the fingerprint-equality early return does not fire, so the recurrence wakes.
    monkeypatch.setattr(cpf, "collect_snapshot", lambda _e, _l: snap_a)
    assert (
        cpf.main(["--no-canonical-check", "--state-file", str(state)]) == cpf.WAKE_EXIT
    )


# --- commit: fingerprint-bound, idempotent, never collects --------------------


def test_commit_promotes_matching_candidate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    state = tmp_path / "state.json"
    candidate = cpf.candidate_file(state)
    snap = _snapshot(remote="new")
    cpf.write_state(candidate, snap)  # probe staged this earlier
    _no_collect(monkeypatch)

    rc = cpf.main(
        [
            "--no-canonical-check",
            "--commit",
            snap["fingerprint"],
            "--state-file",
            str(state),
        ]
    )

    assert rc == 0
    assert cpf.load_state(state) == snap  # candidate promoted to baseline
    assert not candidate.exists()  # candidate consumed by the rename
    assert "committed cos-preflight snapshot" in capsys.readouterr().out


def test_commit_wrong_fingerprint_refuses_and_leaves_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # A candidate is staged, but the caller passes a fingerprint it did not observe (e.g. a
    # stale/abandoned candidate from an earlier tick). Refuse (exit 2); do not promote.
    state = tmp_path / "state.json"
    baseline = _snapshot()
    cpf.write_state(state, baseline)
    stale_candidate = _snapshot(remote="abandoned")
    cpf.write_state(cpf.candidate_file(state), stale_candidate)
    _no_collect(monkeypatch)

    rc = cpf.main(
        ["--no-canonical-check", "--commit", "deadbeef", "--state-file", str(state)]
    )

    assert rc == 2
    assert "does not match" in capsys.readouterr().err
    assert cpf.load_state(state) == baseline  # state file untouched
    assert cpf.candidate_file(state).exists()  # candidate left in place


def test_commit_idempotent_when_baseline_already_matches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Lost-result retry: a prior --commit landed (baseline == fp) but its output was lost,
    # so no candidate remains. Retrying with the same fingerprint is an idempotent success.
    state = tmp_path / "state.json"
    committed = _snapshot(remote="new")
    cpf.write_state(state, committed)  # already committed; no candidate
    _no_collect(monkeypatch)

    rc = cpf.main(
        [
            "--no-canonical-check",
            "--commit",
            committed["fingerprint"],
            "--state-file",
            str(state),
        ]
    )

    assert rc == 0
    assert "already committed" in capsys.readouterr().out
    assert cpf.load_state(state) == committed


def test_commit_no_candidate_and_no_baseline_match_returns_two(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # No candidate and the baseline (if any) doesn't match the fingerprint: genuinely
    # nothing observed this was ever staged → exit 2.
    state = tmp_path / "state.json"
    _no_collect(monkeypatch)

    rc = cpf.main(
        ["--no-canonical-check", "--commit", "deadbeef", "--state-file", str(state)]
    )

    assert rc == 2
    assert "no staged candidate" in capsys.readouterr().err


# --- mid-tick event is caught by the post-tick re-probe ------------------------


def test_mid_tick_event_is_caught(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # The core two-phase win: an event that lands in the probe→commit window is NOT
    # absorbed into the committed baseline. Sequence: baseline exists → probe observes a
    # change (stages candidate, wakes) → tick runs → an event LANDS during the commit
    # phase → --commit still promotes only what the PROBE observed → re-probe compares
    # live-vs-just-committed and wakes again.
    state = tmp_path / "state.json"
    baseline = _snapshot()
    cpf.write_state(state, baseline)
    monkeypatch.setattr(cpf, "require_canonical", lambda _c: None)

    # Probe 1: state moved (remote changed) → wake + stage candidate.
    probed = _snapshot(remote="new")
    monkeypatch.setattr(cpf, "collect_snapshot", lambda _e, _l: probed)
    assert (
        cpf.main(["--no-canonical-check", "--state-file", str(state)]) == cpf.WAKE_EXIT
    )
    fp = _probe_fingerprint(capsys)

    # A DIFFERENT event lands during the commit phase: collect_snapshot now RETURNS the
    # mid-window snapshot. If --commit ever collected, the committed baseline would be this
    # one; it must instead be exactly what probe 1 observed.
    mid_window = _snapshot(remote="mid-window")
    monkeypatch.setattr(cpf, "collect_snapshot", lambda _e, _l: mid_window)
    assert (
        cpf.main(["--no-canonical-check", "--commit", fp, "--state-file", str(state)])
        == 0
    )
    assert cpf.load_state(state) == probed  # committed what the PROBE observed
    assert cpf.load_state(state) != mid_window  # NOT what was live at commit time

    # Re-probe: the mid-window event compares against the just-committed baseline and
    # wakes; it was not burned.
    monkeypatch.setattr(cpf, "collect_snapshot", lambda _e, _l: mid_window)
    assert (
        cpf.main(["--no-canonical-check", "--state-file", str(state)]) == cpf.WAKE_EXIT
    )
