"""Cross-grammar parity gate: ``reg_meta.fqid`` vs ``reg_schema.structural``.

The period-token grammar is DUPLICATED on purpose. ``reg_schema`` is reg_meta-free
by design (one-way dependency + MONA amalgamation — see ``reg_schema/DESIGN.md``
and ``reg_monabundle/DESIGN.md``), and ``reg_meta`` stays Pydantic-free; neither
can import the other, so each carries its own copy of the period grammar
(``reg_meta.fqid._PERIOD_PATTERNS`` + the ``is_period`` calendar check, and
``reg_schema.structural._PERIOD_TOKEN`` + ``_is_period_endpoint``). A looser copy
on either side would let a spec pass one gate yet fail the other (a structurally
"valid" spec that reg_meta's resolver rejects, or vice versa).

Both grammars carry a sync comment saying "keep these two in sync". This test
turns that comment into a CI gate: for one shared corpus of period strings —
valid tokens, calendar-impossible full dates, and syntactic junk — the two
verdicts MUST agree for every string. This is the single drift mitigation for
issue #239 (which split the duplicated grammars apart by adding calendar-day
validation to both); if a future change touches one grammar and not the other,
this test fails.

``reg_schema._is_period_endpoint`` is private, but reaching a structural-layer
internal for a parity assertion is acceptable (mirrors how other tests reach
internals) — it IS the predicate the structural layer uses for ``Source.period``
endpoints. We feed only strings, so its int-literal arm never fires; the
comparison is grammar-against-grammar.
"""

from __future__ import annotations

import pytest
from reg_meta.fqid import is_period
from reg_schema.structural import _is_period_endpoint

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
    "HT2020",
    "VT2019",
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
    "2018-13-01",
    "2018-01-00",
    "2018-01-32",
    "2018-1-1",
    "2020\n",  # trailing newline
    "_default",  # snapshot sentinel — not a token endpoint on either side
)


@pytest.mark.parametrize("value", _CORPUS)
def test_period_grammars_agree(value: str) -> None:
    assert is_period(value) == _is_period_endpoint(value), (
        f"period grammar drift for {value!r}: "
        f"reg_meta.is_period={is_period(value)} but "
        f"reg_schema._is_period_endpoint={_is_period_endpoint(value)}"
    )
