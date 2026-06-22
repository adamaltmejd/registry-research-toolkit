"""Unit tests for scripts/plan_sequence.py — the sequencing-projection engine.

The deterministic core (status classification, the touches-overlap parallel grouping, and
the marked-block splice) is pinned here. The gh/git fetchers are covered by a live run.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1]
_SPEC = importlib.util.spec_from_file_location(
    "plan_sequence", _SCRIPTS / "plan_sequence.py"
)
assert _SPEC and _SPEC.loader
ps = importlib.util.module_from_spec(_SPEC)
# Register before exec: @dataclass resolves sys.modules[__module__] during class build.
sys.modules[_SPEC.name] = ps
_SPEC.loader.exec_module(ps)


def _rec(
    number: int,
    *,
    title: str = "t",
    area: str | None = None,
    is_epic: bool = False,
    blocked_label: bool = False,
    touches: list[str] | None = None,
    open_blockers: list[int] | None = None,
    open_prs: list[int] | None = None,
    relationships: list[tuple[str, int]] | None = None,
    priority: str = "normal",
):
    rec = ps.Rec(
        number=number,
        title=title,
        area=area,
        is_epic=is_epic,
        blocked_label=blocked_label,
        touches=touches or [],
        parent=None,
        open_blockers=open_blockers or [],
        open_prs=open_prs or [],
        relationships=relationships or [],
        priority=priority,
    )
    rec.status = ps.classify(rec)
    return rec


# --- classify ------------------------------------------------------------------------


def test_classify_running_wins_over_blocked() -> None:
    assert ps.classify(_rec(1, open_prs=[9], open_blockers=[2])) == "running"


def test_classify_blocked_by_open_blocker() -> None:
    assert ps.classify(_rec(1, open_blockers=[2])) == "blocked"


def test_classify_blocked_by_label() -> None:
    assert ps.classify(_rec(1, blocked_label=True)) == "blocked"


def test_classify_ready() -> None:
    assert ps.classify(_rec(1)) == "ready"


# --- touches overlap -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("a", "b", "overlap"),
    [
        (["x/y.py"], ["x/y.py"], True),  # identical
        (["x/"], ["x/y.py"], True),  # dir contains file
        (["a/b.py"], ["a/c.py"], False),  # siblings, disjoint
        (["a/b"], ["a/bc"], False),  # prefix string but not a path segment
        ([], ["x"], False),  # empty
        (["reg_webapp/**"], ["reg_webapp/backend/app.py"], True),  # ** matches the file
        (["a/*.py"], ["a/b.py"], True),  # * matches a sibling file
        (["a/*.py"], ["b/c.py"], False),  # different dir
        (["reg_webapp/**"], ["reg_meta/**"], False),  # disjoint globs
        (["reg_webapp/**"], ["reg_webapp/frontend/**"], True),  # nested globs
    ],
)
def test_touches_overlap(a: list[str], b: list[str], overlap: bool) -> None:
    assert ps.touches_overlap(a, b) is overlap


# --- parallel groups -----------------------------------------------------------------


def test_parallel_groups_disjoint_are_separate() -> None:
    recs = [_rec(1, touches=["a.py"]), _rec(2, touches=["b.py"])]
    assert ps.parallel_groups(recs) == [[1], [2]]


def test_parallel_groups_overlap_merges() -> None:
    recs = [
        _rec(1, touches=["pkg/db.py"]),
        _rec(2, touches=["pkg/db.py"]),
        _rec(3, touches=["other.py"]),
    ]
    assert ps.parallel_groups(recs) == [[1, 2], [3]]


def test_parallel_groups_excludes_touchless() -> None:
    recs = [_rec(1, touches=["a.py"]), _rec(2)]  # #2 has no touches
    assert ps.parallel_groups(recs) == [[1]]


def test_parallel_groups_merges_glob_and_concrete() -> None:
    # A `**` glob must conflict with a concrete file under it (the P2 regression).
    recs = [
        _rec(1, touches=["reg_webapp/**"]),
        _rec(2, touches=["reg_webapp/backend/app.py"]),
        _rec(3, touches=["reg_meta/db.py"]),
    ]
    assert ps.parallel_groups(recs) == [[1, 2], [3]]


# --- splice --------------------------------------------------------------------------


def test_splice_replaces_marked_region_preserving_surroundings() -> None:
    body = f"intro\n\n{ps.START}\nold\n{ps.END}\n\noutro\n"
    block = f"{ps.START}\nnew\n{ps.END}"
    result = ps.splice_block(body, block)
    assert "intro" in result and "outro" in result
    assert "old" not in result and "new" in result


def test_splice_appends_when_markers_absent() -> None:
    block = f"{ps.START}\nblock\n{ps.END}"
    result = ps.splice_block("just narrative\n", block)
    assert result.startswith("just narrative")
    assert block in result


def test_splice_is_idempotent() -> None:
    block = f"{ps.START}\nblock\n{ps.END}"
    once = ps.splice_block("narrative\n", block)
    twice = ps.splice_block(once, block)
    assert once == twice


def test_splice_lone_end_marker_appends_not_corrupts() -> None:
    # A stray END (no START) in the human narrative must not scramble the body.
    block = f"{ps.START}\nnew\n{ps.END}"
    body = f"stray {ps.END} in prose\nnarrative\n"
    result = ps.splice_block(body, block)
    assert result.startswith("stray ")  # original text preserved, not reordered
    assert block in result


def test_splice_lanes_region_independent_of_sequence_region() -> None:
    # The two marked regions coexist: writing one must not disturb the other.
    body = (
        f"{ps.START}\nseq\n{ps.END}\n\nnarrative\n\n"
        f"{ps.LANES_START}\nold lanes\n{ps.LANES_END}\n"
    )
    new_lanes = f"{ps.LANES_START}\nnew lanes\n{ps.LANES_END}"
    result = ps.splice_block(body, new_lanes, ps.LANES_START, ps.LANES_END)
    assert "new lanes" in result and "old lanes" not in result
    assert f"{ps.START}\nseq\n{ps.END}" in result  # sequence region untouched
    assert "narrative" in result


# --- lanes framing -------------------------------------------------------------------


def test_render_lanes_block_frames_and_strips() -> None:
    block = ps.render_lanes_block("  1. lane A — #1\n")
    assert block.startswith(ps.LANES_START) and block.endswith(ps.LANES_END)
    assert "1. lane A — #1" in block
    assert "overwritten" in block  # carries the do-not-edit header
    # No timestamp: identical input renders byte-identical (diff-stable).
    assert ps.render_lanes_block("  1. lane A — #1\n") == block


def test_basis_comment_round_trips_through_parse() -> None:
    block = ps.render_lanes_block("lanes", ps.basis_comment({3, 1}, {2}, "abc123"))
    assert ps.parse_basis(block) == ({1, 3}, {2}, "abc123")  # sorted in, set out
    # Empty sets are representable (and distinct from "no basis").
    assert ps.parse_basis(
        ps.render_lanes_block("x", ps.basis_comment(set(), set(), "deadbeef"))
    ) == (
        set(),
        set(),
        "deadbeef",
    )


def test_parse_basis_absent_is_none() -> None:
    assert ps.parse_basis(ps.render_lanes_block("no basis here")) is None


def test_parse_basis_pre_signature_reads_empty_sig() -> None:
    # A basis written before the `sig` field existed must still parse — with sig="", which
    # differs from any live signature, so the block reads stale and self-upgrades once.
    legacy = ps.render_lanes_block(
        "lanes", "<!-- plan-lanes:basis ready=1,2 running=3 -->"
    )
    assert ps.parse_basis(legacy) == ({1, 2}, {3}, "")
    assert ps.lanes_freshness(legacy, {1, 2}, {3}, "livesig") == "rerank"  # sig differs


# --- lanes content signature (FU-2 + running-set-only re-stamp, #468) -----------------


def test_lanes_content_signature_is_stable_and_order_independent() -> None:
    a = _rec(1, area="reg_meta", touches=["x.py"], priority="high")
    b = _rec(2, touches=["y.py"], relationships=[("related to", 9)])
    assert ps.lanes_content_signature([a, b]) == ps.lanes_content_signature([b, a])
    assert ps.lanes_content_signature([a, b]) == ps.lanes_content_signature([a, b])


def test_lanes_content_signature_changes_on_each_lane_affecting_input() -> None:
    # Every input `/plan-lanes` ranks on must flip the signature (FU-2 + the Codex P2s:
    # area grouping, full Relationships graph — not just touches/priority).
    base = ps.lanes_content_signature([_rec(1, area="reg_meta", touches=["x.py"])])
    assert base != ps.lanes_content_signature(
        [_rec(1, area="reg_meta", touches=["y.py"])]
    )
    assert base != ps.lanes_content_signature(
        [_rec(1, area="reg_webapp", touches=["x.py"])]
    )
    assert base != ps.lanes_content_signature(
        [_rec(1, area="reg_meta", touches=["x.py"], priority="high")]
    )
    # A non-blocking coherence tie (Related to / Follow-up to) — Codex P2 #3.
    assert base != ps.lanes_content_signature(
        [_rec(1, area="reg_meta", touches=["x.py"], relationships=[("related to", 7)])]
    )


def test_lanes_content_signature_catches_blocked_dependent_edge_rewrite() -> None:
    # Codex P2 #1: a still-blocked issue's `Blocked by` rewrite changes which ready issue
    # has unblocking power, though no section moves. Signing blocked (non-running) records
    # alongside the candidates catches it.
    ready = _rec(1, touches=["x.py"])  # the candidate
    dep_old = _rec(50, blocked_label=True, relationships=[("blocked by", 1)])
    dep_new = _rec(50, blocked_label=True, relationships=[("blocked by", 2)])
    assert ps.lanes_content_signature([ready, dep_old]) != ps.lanes_content_signature(
        [ready, dep_new]
    )


def test_content_signature_invariant_to_running_issue_leaving() -> None:
    # The #468 core: a running issue that holds NO ready candidate contributes nothing to
    # the content signature, so its PR merging + issue closing (it leaves the corpus) does
    # NOT flip the signature — that delta is running-set-only and re-stamps, not re-ranks.
    before = [
        _rec(1, area="reg_meta", touches=["a.py"]),  # free candidate
        _rec(
            9, area="reg_meta", open_prs=[100], touches=["z.py"]
        ),  # in-flight, disjoint
    ]
    after = [_rec(1, area="reg_meta", touches=["a.py"])]  # #9 merged + closed → gone
    assert ps.lanes_content_signature(before) == ps.lanes_content_signature(after)


def test_content_signature_flips_when_running_merge_unholds_a_candidate() -> None:
    # The held↔free guard: when the leaving running issue WAS holding a ready issue (shared
    # touches), that issue becomes a free candidate → content moved → must re-rank, not
    # re-stamp. The free flag in the signature catches it though no section label moves.
    before = [
        _rec(1, touches=["shared.py"]),  # ready but HELD by #9
        _rec(9, open_prs=[100], touches=["shared.py"]),  # in-flight, holds #1
    ]
    after = [_rec(1, touches=["shared.py"])]  # #9 gone → #1 now free
    assert ps.lanes_content_signature(before) != ps.lanes_content_signature(after)


def test_content_signature_excludes_running_own_projection() -> None:
    # A running issue's own area/touches/priority sign nothing (it's never a candidate);
    # only the held-set effect would. Two corpora identical but for a running issue's area
    # must hash the same.
    a = [
        _rec(1, touches=["a.py"]),
        _rec(9, area="reg_meta", open_prs=[5], touches=["z.py"]),
    ]
    b = [
        _rec(1, touches=["a.py"]),
        _rec(9, area="reg_webapp", open_prs=[5], touches=["z.py"]),
    ]
    assert ps.lanes_content_signature(a) == ps.lanes_content_signature(b)


def test_content_signature_signs_running_blocking_edge_onto_candidate() -> None:
    # A running (in-flight) issue's BLOCKING edge onto a ready candidate confers unblocking
    # power on that candidate — rewriting which candidate it points at re-shapes ranking
    # with no section move, so it must flip the sig (the regression the review caught).
    cand1, cand2 = _rec(1, touches=["a.py"]), _rec(2, touches=["b.py"])
    run_old = _rec(
        9, open_prs=[100], touches=["z.py"], relationships=[("blocked by", 1)]
    )
    run_new = _rec(
        9, open_prs=[100], touches=["z.py"], relationships=[("blocked by", 2)]
    )
    assert ps.lanes_content_signature(
        [cand1, cand2, run_old]
    ) != ps.lanes_content_signature([cand1, cand2, run_new])


def test_content_signature_ignores_running_nonblocking_and_off_candidate_edges() -> (
    None
):
    # The churn guard: a running issue's `Part of #<epic>` (non-blocking) tie, or a blocking
    # edge onto a NON-candidate, signs nothing — so the common sub-issue merge (its only tie
    # is `Part of`) stays a re-stamp, not a re-rank.
    base = ps.lanes_content_signature([_rec(1, touches=["a.py"])])  # candidate alone
    # Running #9 tied only `Part of #328` (epic, not a candidate) → no effect.
    assert base == ps.lanes_content_signature(
        [
            _rec(1, touches=["a.py"]),
            _rec(9, open_prs=[5], relationships=[("part of", 328)]),
        ]
    )
    # Running #9 blocked by #99, which is NOT a ready candidate (absent) → no effect.
    assert base == ps.lanes_content_signature(
        [
            _rec(1, touches=["a.py"]),
            _rec(9, open_prs=[5], relationships=[("blocked by", 99)]),
        ]
    )


def test_reject_lanes_stdin() -> None:
    assert ps.reject_lanes_stdin("  \n\t") is not None  # empty/whitespace
    assert (
        ps.reject_lanes_stdin(f"prose {ps.LANES_END} more") is not None
    )  # marker leak
    assert ps.reject_lanes_stdin(f"{ps.LANES_START}\nx") is not None
    assert ps.reject_lanes_stdin("1. lane A — #1") is None  # clean content passes


# --- lanes completeness guard (#662) -------------------------------------------------


def test_reject_incomplete_lanes() -> None:
    basis = ps.basis_comment(
        {1, 2, 3}, set(), "sig"
    )  # ready={1,2,3}, nothing in flight

    # Every ready candidate placed in a lane → accepted.
    assert ps.reject_incomplete_lanes("1. lane — #1, #2\n2. lane — #3", basis) is None

    # A dropped candidate is refused, and the message names exactly what's missing.
    why = ps.reject_incomplete_lanes("1. lane — #1, #2", basis)
    assert why is not None and "#3" in why
    assert "#1" not in why and "#2" not in why  # only the missing one is named

    # Whole-number matching (#3 ≠ #30): a superstring of a candidate id doesn't satisfy it —
    # #30 alone leaves #3 missing (reject), but adding #3 back accepts (locks `\d+` vs `\d`).
    assert ps.reject_incomplete_lanes("1. lane — #1, #2, #30", basis) is not None
    assert ps.reject_incomplete_lanes("1. lane — #1, #2, #30, #3", basis) is None
    # Extra non-candidate references (e.g. epic/follow-up ids) are harmless.
    assert ps.reject_incomplete_lanes("1. lane — #1, #2, #3 (see #99)", basis) is None
    # No parsable basis → nothing to check against (callers guard well-formedness).
    assert ps.reject_incomplete_lanes("1. lane — #1", "garbage") is None

    # In-flight work: a ready issue can be HELD (excluded from /plan-lanes' floor), and the
    # all-held floor lists no IDs — so the guard abstains rather than false-reject + wedge.
    inflight = ps.basis_comment({1, 2, 3}, {9}, "sig")  # running={9}
    assert (
        ps.reject_incomplete_lanes("1. lane — #1", inflight) is None
    )  # #2/#3 absent, abstained


# --- lanes freshness: fresh / re-stamp / re-rank (#468) -------------------------------


def test_lanes_freshness_three_way() -> None:
    block = ps.render_lanes_block("lanes", ps.basis_comment({1, 2}, {3}, "sig"))
    assert ps.lanes_freshness(block, {1, 2}, {3}, "sig") == "fresh"  # nothing moved
    # Running-set-only delta (PR #3 merged → its issue cleared): content sig + ready set
    # unchanged, only `running` moved → cheap re-stamp, no /plan-lanes.
    assert ps.lanes_freshness(block, {1, 2}, set(), "sig") == "restamp"
    assert ps.lanes_freshness(block, {1, 2}, {3, 4}, "sig") == "restamp"  # PR opened
    # Content moved → re-rank.
    assert ps.lanes_freshness(block, {1, 2, 4}, {3}, "sig") == "rerank"  # ready grew
    assert (
        ps.lanes_freshness(block, {1, 2}, {3}, "other") == "rerank"
    )  # content sig moved
    assert ps.lanes_freshness("", {1}, set(), "sig") == "rerank"  # no parsable basis


def test_signature_flips_freshness_on_touches_edit_no_section_move() -> None:
    # The FU-2 fix carried over: a `touches` edit that moves no issue between sections must
    # still re-rank, where a membership-only basis (same ready/running sets) would miss it.
    ready, running = {1}, set()
    old_sig = ps.lanes_content_signature([_rec(1, touches=["a.py"])])
    new_sig = ps.lanes_content_signature([_rec(1, touches=["a.py", "b.py"])])
    block = ps.render_lanes_block("lanes", ps.basis_comment(ready, running, old_sig))
    assert ps.lanes_freshness(block, ready, running, old_sig) == "fresh"  # unchanged
    assert (
        ps.lanes_freshness(block, ready, running, new_sig) == "rerank"
    )  # touches moved


def test_running_set_only_merge_is_restamp_end_to_end() -> None:
    # End-to-end of the #468 fix: build the basis from a corpus, merge an in-flight PR (its
    # issue leaves), recompute, and confirm the live state classifies as re-stamp — the
    # exact two-tick scenario that used to force a re-rank.
    before = [
        _rec(1, area="reg_meta", touches=["a.py"]),  # free candidate
        _rec(
            9, area="reg_meta", open_prs=[100], touches=["z.py"]
        ),  # in-flight, disjoint
    ]
    basis = ps.basis_comment({1}, {9}, ps.lanes_content_signature(before))
    block = ps.render_lanes_block("1. lane — #1", basis)
    after = [_rec(1, area="reg_meta", touches=["a.py"])]  # #9 merged + closed
    assert (
        ps.lanes_freshness(block, {1}, set(), ps.lanes_content_signature(after))
        == "restamp"
    )


# --- re-stamp (keep ranked content, swap basis) (#468) -------------------------------


def test_extract_lanes_content_round_trips() -> None:
    content = "1. lane A — #1\n2. lane B — #2"
    block = ps.render_lanes_block(content, ps.basis_comment({1, 2}, set(), "sig"))
    assert ps.extract_lanes_content(block) == content


def test_extract_lanes_content_empty_without_basis() -> None:
    # A pre-stamp/legacy block has no basis to anchor on → '' so the caller re-ranks.
    assert ps.extract_lanes_content(ps.render_lanes_block("lanes")) == ""
    assert ps.extract_lanes_content("") == ""


def test_restamp_lanes_block_keeps_content_swaps_basis(monkeypatch) -> None:
    # Re-stamp must preserve the agentic ranking verbatim and only refresh the basis stamp
    # (new `running=`, same `sig`) — no /plan-lanes, no content churn.
    old = ps.render_lanes_block("1. lane A — #1", ps.basis_comment({1}, {9}, "sig"))
    body = f"intro\n\n{old}\n\nnarrative\n"
    captured: dict[str, str] = {}

    def fake_write(epic: int, block: str) -> int:
        captured["block"] = block
        return 0

    monkeypatch.setattr(ps, "epic_body", lambda epic: body)
    monkeypatch.setattr(ps, "write_lanes_block", fake_write)

    new_basis = ps.basis_comment({1}, set(), "sig")  # running cleared (PR #9 merged)
    assert ps.restamp_lanes_block(328, new_basis) == 0
    block = captured["block"]
    assert "1. lane A — #1" in block  # ranked content preserved verbatim
    assert ps.parse_basis(block) == ({1}, set(), "sig")  # new basis stamped
    assert "running=9" not in block  # stale in-flight set gone


def test_restamp_lanes_block_falls_back_to_rerank_without_block(monkeypatch) -> None:
    monkeypatch.setattr(ps, "epic_body", lambda epic: "intro, no lanes block\n")
    # Exit 1 signals the caller to re-rank instead.
    assert ps.restamp_lanes_block(328, ps.basis_comment({1}, set(), "s")) == 1


@pytest.mark.parametrize("basis", ["", "garbage", "<!-- plan-lanes:basis bad -->"])
def test_restamp_lanes_cli_refuses_malformed_basis(monkeypatch, basis: str) -> None:
    # A basis that fails _BASIS_RE must NOT be stamped — writing it would yield a block that
    # parses as no-basis, pinning every later tick to `rerank`. The CLI guard returns 2
    # before any gh fetch (epic_body would explode the test otherwise).
    def boom(*a, **k):  # pragma: no cover - must never be reached
        raise AssertionError("malformed basis reached the writer")

    monkeypatch.setattr(ps, "epic_body", boom)
    monkeypatch.setattr(
        sys,
        "argv",
        ["plan_sequence.py", "--restamp-lanes", "--epic", "1", "--basis", basis],
    )
    assert ps.main() == 2


# --- render --------------------------------------------------------------------------


def test_render_block_sections_and_counts() -> None:
    recs = [
        _rec(1, area="reg_webapp"),  # ready
        _rec(2, open_prs=[10]),  # running
        _rec(3, open_blockers=[1]),  # blocked
    ]
    block = ps.render_block(recs, debt=None)
    assert block.startswith(ps.START) and block.rstrip().endswith(ps.END)
    assert "### Ready now" in block
    assert "1 ready · 1 running · 1 blocked" in block
    assert "PR #10" in block  # running link
    assert "blocked by #1" in block


def test_render_block_orders_sections_by_number() -> None:
    # Order-independence: render must sort, not rely on caller order.
    recs = [_rec(30, area="a"), _rec(10, area="b"), _rec(20, area="c")]
    block = ps.render_block(recs, None)
    assert block.index("#10") < block.index("#20") < block.index("#30")
    assert ps.render_block(recs, None) == ps.render_block(list(reversed(recs)), None)


def test_extract_block() -> None:
    body = f"intro\n{ps.START}\nx\n{ps.END}\nouter"
    assert ps.extract_block(body) == f"{ps.START}\nx\n{ps.END}"
    assert ps.extract_block("no markers here") == ""


def test_diff_report_added_and_removed() -> None:
    old = f"{ps.START}\n### Ready now\n- #1 a\n### Running\n_none_\n### Blocked\n- #3 c ← x\n{ps.END}"
    new = f"{ps.START}\n### Ready now\n- #1 a\n- #2 b\n### Running\n_none_\n### Blocked\n_none_\n{ps.END}"
    report = ps.diff_report(old, new)
    assert "newly ready: #2" in report
    assert "left blocked: #3" in report


def test_diff_report_no_change() -> None:
    block = ps.render_block([_rec(1, area="x")], None)
    assert ps.diff_report(block, block) == "no status changes"


def test_render_block_excludes_epics_from_work() -> None:
    recs = [_rec(328, is_epic=True), _rec(10, area="reg_meta")]
    block = ps.render_block(recs, None)
    assert "**Epics:** #328" in block
    assert "1 work · 1 ready" in block  # epic not counted as work
    # the epic is not listed as a ready work item
    ready_section = block.split("### Ready now")[1].split("### Running")[0]
    assert "#328" not in ready_section
    assert "#10" in ready_section


# --- dispatch view -------------------------------------------------------------------


def test_dispatch_view_groups_ready_by_area_excludes_epics() -> None:
    recs = [
        _rec(1, area="reg_webapp", touches=["a.py"]),
        _rec(2, area="reg_meta_build", touches=["b.py"]),
        _rec(99, is_epic=True),  # epic excluded
        _rec(3, open_prs=[5], touches=["c.py"]),  # running, excluded
    ]
    view = ps.dispatch_view(recs)
    assert "reg_webapp (1):" in view and "#1" in view
    assert "reg_meta_build (1):" in view and "#2" in view
    assert "#99" not in view and "#3" not in view


def _candidate_line(view: str) -> str:
    return next(ln for ln in view.splitlines() if ln.startswith("Candidate set ("))


def test_dispatch_view_emits_flat_candidate_set_line() -> None:
    # The flat `Candidate set (N) …` line is the authoritative floor /plan-lanes
    # self-checks against — it must list every free candidate, sorted, ON THE LINE (so
    # the literal "every ranked number is on that line" check works), and count them,
    # while excluding epics and in-flight/held issues.
    recs = [
        _rec(3, area="reg_webapp", touches=["a.py"]),
        _rec(1, area="reg_meta_build", touches=["b.py"]),
        _rec(99, is_epic=True),  # epic excluded
        _rec(7, open_prs=[5], touches=["c.py"]),  # running, excluded
    ]
    view = ps.dispatch_view(recs)
    line = _candidate_line(view)
    assert line.startswith("Candidate set (2) — rank ONLY these")
    assert line.endswith("#1 #3")  # numbers on the line, sorted, free-only
    assert "#99" not in view and "#7" not in view


def test_dispatch_view_candidate_set_excludes_held() -> None:
    recs = [
        _rec(9, area="reg_webapp", open_prs=[100], touches=["shared.py"]),  # in-flight
        _rec(1, area="reg_webapp", touches=["shared.py"]),  # conflicts → held
        _rec(2, area="reg_webapp", touches=["other.py"]),  # free
    ]
    line = _candidate_line(ps.dispatch_view(recs))
    assert line.endswith(": #2")  # held #1 is not a candidate


def test_dispatch_view_holds_issues_touching_in_flight() -> None:
    recs = [
        _rec(9, area="reg_webapp", open_prs=[100], touches=["shared.py"]),  # in-flight
        _rec(1, area="reg_webapp", touches=["shared.py"]),  # conflicts → held
        _rec(2, area="reg_webapp", touches=["other.py"]),  # free
    ]
    view = ps.dispatch_view(recs)
    # Held line carries the holding PR so the agent never sources PR numbers from the
    # epic narrative; the in-flight PR set is also surfaced for the return-format header.
    assert "Held — touch in-flight work: #1 ← PR #100" in view
    assert "In-flight PRs: #100" in view
    assert "#2" in view.split("Held")[0]  # #2 is a free candidate


def test_dispatch_view_flags_must_serialize() -> None:
    recs = [
        _rec(1, area="reg_meta_build", touches=["db.py"]),
        _rec(2, area="reg_meta_build", touches=["db.py"]),
    ]
    assert "Must serialize (share files): #1+#2" in ps.dispatch_view(recs)


def test_dispatch_view_empty_when_nothing_free() -> None:
    recs = [_rec(1, open_prs=[1]), _rec(2, blocked_label=True)]
    assert ps.dispatch_view(recs) == "No ready issues free of in-flight conflicts."


# --- priority (FU-1) -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("labels", "expected"),
    [
        (set(), "normal"),
        ({"priority:high"}, "high"),
        ({"priority:low"}, "low"),
        (
            {"priority:high", "priority:low"},
            "high",
        ),  # both → high wins (hygiene flags it)
        ({"reg_meta", "bug"}, "normal"),  # unrelated labels
    ],
)
def test_priority_of(labels: set[str], expected: str) -> None:
    assert ps.priority_of(labels) == expected


def test_dispatch_view_annotates_priority_and_summarizes() -> None:
    recs = [
        _rec(1, area="reg_meta", touches=["a.py"], priority="high"),
        _rec(2, area="reg_meta", touches=["b.py"], priority="low"),
        _rec(3, area="reg_meta", touches=["c.py"]),  # normal — no tag
    ]
    view = ps.dispatch_view(recs)
    assert "[high] " in view and "[low] " in view
    assert "#3 t" in view and "[normal]" not in view  # normal stays quiet
    # The explicit ranking-hint summary lists the non-normal buckets.
    assert "Priority (rank by this first): high: #1; low: #2" in view
