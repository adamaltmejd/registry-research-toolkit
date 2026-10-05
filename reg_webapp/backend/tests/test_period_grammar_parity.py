"""Cross-grammar parity gate: ``reg_meta.fqid`` vs ``reg_schema.validate_structural``.

The period-token grammar is DUPLICATED on purpose, for two reasons that survive:
(a) LAYERING — ``reg_schema`` is the lightweight canonical-schema package, and
making it depend on the heavier catalog-query ``reg_meta`` (which owns a DB) would
be a backwards layering inversion; ``reg_schema`` uses string IDs and leaves
resolution to the consumer (one-way dependency — see ``reg_schema/DESIGN.md`` and
``ARCHITECTURE.md``); and (b) the TS SPA mirrors the grammar regardless of either
package. So each side carries its own copy of the period grammar and of the
token-to-bounds expansion (``reg_meta.fqid.is_period`` /
``period_token_to_bounds``, and the private period helpers behind
``reg_schema.structural``). A looser copy on either side would let a spec pass
one gate yet fail the other (a structurally "valid" spec that reg_meta's
resolver rejects, or vice versa).

Both grammars carry a sync comment saying "keep these two in sync". This test
turns that comment into a CI gate, checked through the PUBLIC validation result
(``validate_structural`` issues on ``Source.period``) rather than reg_schema's
private helpers: for one shared corpus of period strings — valid tokens,
calendar-impossible full dates, and syntactic junk — reg_meta's verdict MUST
equal whether ``validate_structural`` accepts the string as a period, and for
every ordered pair of valid tokens the list-form sorted/non-overlap verdict MUST
equal the one reg_meta's bounds imply. This is the single drift mitigation for
issue #239 (which split the duplicated grammars apart by adding calendar-day
validation to both) and #307 (which duplicated the bounds expansion); if a
future change touches one copy and not the other, this test fails.
"""

from __future__ import annotations

import calendar
import copy
from datetime import date, timedelta
from itertools import product

import pytest
from reg_meta.fqid import is_period, period_token_to_bounds

from reg_schema import validate_structural

# One shared corpus spanning the three classes the two grammars must agree on.
# Each verdict is asserted by AGREEMENT, not a hard-coded expected value, so the
# test stays a pure parity check; the in-line groupings are documentation only.
_CORPUS: tuple[str, ...] = (
    # — valid tokens, every form —
    "2018",
    "1999",
    "2018-01",
    "2018-12",
    "2018-02",  # non-leap February month token (no author day — valid)
    "2020-02",  # leap February month token — its upper bound meets 2020-02-29
    "HT2020",
    "VT2019",
    "LA2004",
    "2020-Q1",
    "2020-Q4",
    "2020-H1",
    "2020-H2",
    "2014-12-31",
    "2002-10-15",
    "2018-01-01",
    "2020-02-29",  # 2020 IS a leap year — a real Feb 29
    "2000-02-29",  # ÷400 century leap — a real Feb 29
    # — calendar-impossible full dates (pass the 01-31 day regex, not real dates) —
    "2019-02-29",  # 2019 is NOT a leap year
    "1900-02-29",  # ÷100 not ÷400 — NOT a leap year (and in the 19xx range)
    "2018-02-30",  # February never has 30 days
    "2021-04-31",  # April has 30 days
    "2019-04-31",
    "2021-06-31",  # June has 30 days
    "2021-09-31",  # September has 30 days
    "2021-11-31",  # November has 30 days
    # — out-of-bounds / syntactic junk —
    "",
    "abc",
    "20188",
    "2018-1",
    "2020-13",
    "2020-00",
    "2020-Q0",
    "2020-Q5",
    "2020-H0",
    "2020-H3",
    "1899",
    "2100",
    "HT9999",
    "LA",
    "2018-13-01",
    "2018-01-00",
    "2018-01-32",
    "2018-1-1",
    "2020\n",  # trailing newline
    "_default",  # snapshot sentinel — not a token endpoint on either side
)

_VALID: tuple[str, ...] = tuple(v for v in _CORPUS if is_period(v))

# Minimal clean project document (mirrors reg_schema/test_corpus/minimal); only
# ``sources[0].period`` varies per case.
_CLEAN_DOC: dict[str, object] = {
    "schema_version": "2.0.0",
    "steward": "global",
    "reg_meta_version": "reg_meta/v1.0.0",
    "name": "period_parity",
    "sources": [
        {
            "name": "lisa_2018",
            "register_variant": "scb/lisa/individer-15plus",
            "period": 2018,
            "bindings": [
                {
                    "variable": "scb/lisa/kon",
                    "type": "categorical",
                    "value_set": "class/sun2020",
                }
            ],
        }
    ],
}


def _period_issue_paths(period: object) -> list[str]:
    doc = copy.deepcopy(_CLEAN_DOC)
    doc["sources"][0]["period"] = period  # type: ignore[index]
    return [
        issue.path
        for issue in validate_structural(doc).issues
        if issue.code == "invalid_period" and issue.path.startswith("/sources/0/period")
    ]


def test_clean_document_with_range_period_has_no_issues() -> None:
    """The base document is clean, so a period verdict is the only signal."""
    doc = copy.deepcopy(_CLEAN_DOC)
    doc["sources"][0]["period"] = {"from": "2018", "to": "2018"}  # type: ignore[index]
    assert not validate_structural(doc).issues


@pytest.mark.parametrize("value", _CORPUS)
def test_period_grammars_agree(value: str) -> None:
    """reg_meta accepts a token iff validate_structural accepts it as a range endpoint."""
    # Range form: the scalar form special-cases "_default".
    accepted = not _period_issue_paths({"from": value, "to": value})
    assert is_period(value) == accepted, (
        f"period grammar drift for {value!r}: reg_meta.is_period={is_period(value)} "
        f"but validate_structural accepts={accepted}"
    )


def test_period_list_ordering_agrees_with_reg_meta_bounds() -> None:
    """For every ordered pair of valid tokens, the list period ``[a, b]`` is
    rejected at member 1 iff reg_meta's bounds say b starts before a (unsorted)
    or inside a (overlap) — so the structural interval verdicts cannot drift
    from reg_meta's expansion (incl. the synthesized Feb-29 upper bound both
    sides share)."""
    mismatches = []
    for a, b in product(_VALID, repeat=2):
        a_lo, a_hi = period_token_to_bounds(a)
        b_lo, _ = period_token_to_bounds(b)
        expected_rejected = b_lo < a_lo or b_lo <= a_hi
        paths = _period_issue_paths([a, b])
        # Single tokens are never inverted, so member 0 must never be flagged.
        assert "/sources/0/period/0" not in paths, (a, b, paths)
        rejected = "/sources/0/period/1" in paths
        if rejected != expected_rejected:
            mismatches.append(
                f"[{a!r}, {b!r}]: reg_meta bounds imply rejected={expected_rejected}, "
                f"validate_structural rejected={rejected}"
            )
    assert not mismatches, "period bounds drift:\n" + "\n".join(mismatches)


def _day(value: str, shift: int) -> str:
    """``value`` shifted by ``shift`` days. reg_meta synthesizes a Feb-29 upper
    bound in non-leap years; that edge is read as the last real February day."""
    year, month, day = (int(part) for part in value.split("-"))
    if (month, day) == (2, 29) and not calendar.isleap(year):
        day = 28
    return (date(year, month, day) + timedelta(days=shift)).isoformat()


@pytest.mark.parametrize("token", _VALID)
def test_period_list_edges_follow_reg_meta_bounds(token: str) -> None:
    """Full-date neighbours of reg_meta's own bounds for ``token``: a day at its
    lower or upper bound overlaps it, the day before/after does not. Pins each
    token form's expansion edges, not just the relative order of the corpus."""
    lo, hi = period_token_to_bounds(token)
    first, last = _day(lo, 0), _day(hi, 0)
    cases: dict[tuple[int | str, ...], bool] = {
        (_day(lo, -1), token): False,
        (first, token): True,
        (token, last): True,
        (token, _day(hi, 1)): False,
    }
    if token.isdigit():
        # A bare year is also authored as an int literal; same expansion.
        cases |= {
            tuple(int(t) if t == token else t for t in pair): rejected
            for pair, rejected in cases.items()
        }
    verdicts = {
        pair: "/sources/0/period/1" in _period_issue_paths(list(pair)) for pair in cases
    }
    assert verdicts == cases
