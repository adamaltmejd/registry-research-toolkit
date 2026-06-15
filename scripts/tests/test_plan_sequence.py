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


def test_render_block_excludes_epics_from_work() -> None:
    recs = [_rec(328, is_epic=True), _rec(10, area="reg_meta")]
    block = ps.render_block(recs, None)
    assert "**Epics:** #328" in block
    assert "1 work · 1 ready" in block  # epic not counted as work
    # the epic is not listed as a ready work item
    ready_section = block.split("### Ready now")[1].split("### Running")[0]
    assert "#328" not in ready_section
    assert "#10" in ready_section
