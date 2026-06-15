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
    block = ps.render_lanes_block("lanes", ps.basis_comment({3, 1}, {2}))
    assert ps.parse_basis(block) == ({1, 3}, {2})  # sorted in, set out
    # Empty sets are representable (and distinct from "no basis").
    assert ps.parse_basis(
        ps.render_lanes_block("x", ps.basis_comment(set(), set()))
    ) == (
        set(),
        set(),
    )


def test_parse_basis_absent_is_none() -> None:
    assert ps.parse_basis(ps.render_lanes_block("no basis here")) is None


def test_lanes_are_stale_against_live_sets() -> None:
    fresh = ps.render_lanes_block("lanes", ps.basis_comment({1, 2}, {3}))
    assert not ps.lanes_are_stale(fresh, {1, 2}, {3})  # basis matches → fresh
    assert ps.lanes_are_stale(fresh, {1, 2, 4}, {3})  # ready moved → stale
    assert ps.lanes_are_stale(fresh, {1, 2}, set())  # running cleared → stale
    assert ps.lanes_are_stale("", {1}, set())  # no block at all → stale


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


def test_dispatch_view_holds_issues_touching_in_flight() -> None:
    recs = [
        _rec(9, area="reg_webapp", open_prs=[100], touches=["shared.py"]),  # in-flight
        _rec(1, area="reg_webapp", touches=["shared.py"]),  # conflicts → held
        _rec(2, area="reg_webapp", touches=["other.py"]),  # free
    ]
    view = ps.dispatch_view(recs)
    assert "Held — touch in-flight work: #1" in view
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
