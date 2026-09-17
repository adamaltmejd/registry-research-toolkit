"""End-to-end coverage for the period column-family merge (#319).

Fabricates a small monthly family (a stem + jan/feb/mars columns across two
delivery years) in the SCB build fixtures, activates a curated
`curation/period_family_merges.toml` for it, runs a REAL `build_db`, and asserts the
12→1 merge shape: one variable slugged as the stem with ANNUAL states (not
per-month), the per-month alias windows, the resolver expansion, the deleted
siblings, no dangling FK, and the validator closure. Loader-shape tests live
alongside.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import _var_row
from _shared_fixtures import vm_rows
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.period_family_merges import (
    PeriodFamily,
    load_period_family_merges,
)

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

# The merged family: stem "lonfink", three month columns, two delivery years.
# `derive_variable_slug("LonFinkJan")` → "lonfinkjan" → stem "lonfink" + jan.
_MONTHS = [("jan", 1), ("feb", 2), ("mars", 3)]
_YEARS = [2018, 2019]
_CODES = [("1", "Låg"), ("2", "Hög")]


def _family_ri_vm() -> tuple[list[str], list[str]]:
    """Registerinformation + Vardemangder rows for the 3-month × 2-year family in
    TESTREG (register_id 1, variant 10). Each month is its own variable (distinct
    var_id) delivering the same column name across both years (so it folds to one
    variable with two annual states); all share one value set."""
    ri: list[str] = []
    vm: list[str] = []
    for mi, (token, _month) in enumerate(_MONTHS):
        var_id = 800 + mi
        colname = f"LonFink{token.capitalize()}"
        for yi, year in enumerate(_YEARS):
            cvid = 8000 + mi * 10 + yi
            ri.append(
                _var_row(
                    colname=colname,
                    cvid=cvid,
                    var_id=var_id,
                    varname=f"Inkomst {token}",
                    year=str(year),
                    regver_id=800 + mi * 10 + yi,
                    data_length="1",
                )
            )
            vm.extend(vm_rows(cvid, f"LonFink{year}", _CODES))
    return ri, vm


_LISA_FAMILY = PeriodFamily(
    provider="scb", register="testreg", family_stem="lonfink", label="Lön per månad"
)


def _survivor(conn: sqlite3.Connection) -> sqlite3.Row:
    row = conn.execute(
        "SELECT variable_id, name, slug FROM variable WHERE slug = 'lonfink'"
    ).fetchone()
    assert row is not None, "merged survivor variable should be slugged 'lonfink'"
    return row


# ── loader shape ──────────────────────────────────────────────────────────────


def test_load_period_family_merges_parses(tmp_path: Path) -> None:
    path = tmp_path / "period_family_merges.toml"
    path.write_text(
        '[[period_family]]\nregister = "scb/lisa"\n'
        'family_stem = "lonfink"\nlabel = "Lön"\n',
        encoding="utf-8",
    )
    families = load_period_family_merges(path)
    assert len(families) == 1
    assert families[0] == PeriodFamily("scb", "lisa", "lonfink", "Lön")


def test_load_period_family_merges_empty_when_no_file() -> None:
    assert load_period_family_merges(None) == ()


@pytest.mark.parametrize(
    "body",
    [
        'register = "lisa"\nfamily_stem = "x"\nlabel = "L"',  # 1-seg register
        'register = "scb/lisa/x"\nfamily_stem = "x"\nlabel = "L"',  # 3-seg register
        'register = "scb/lisa"\nlabel = "L"',  # missing family_stem
        'register = "scb/lisa"\nfamily_stem = "x"',  # missing label
    ],
)
def test_load_period_family_merges_rejects_malformed(tmp_path: Path, body: str) -> None:
    path = tmp_path / "period_family_merges.toml"
    path.write_text(f"[[period_family]]\n{body}\n", encoding="utf-8")
    with pytest.raises(RegMetaError) as exc:
        load_period_family_merges(path)
    assert exc.value.exit_code == EXIT_CONFIG
    assert exc.value.code == "period_family_merges_invalid"


def test_load_period_family_merges_rejects_duplicate(tmp_path: Path) -> None:
    path = tmp_path / "period_family_merges.toml"
    path.write_text(
        '[[period_family]]\nregister = "scb/lisa"\nfamily_stem = "x"\nlabel = "A"\n'
        '[[period_family]]\nregister = "scb/lisa"\nfamily_stem = "x"\nlabel = "B"\n',
        encoding="utf-8",
    )
    with pytest.raises(RegMetaError) as exc:
        load_period_family_merges(path)
    assert exc.value.code == "period_family_merges_invalid"


# ── merge mechanics (end-to-end build) ────────────────────────────────────────


def _divergent_ri_vm() -> tuple[list[str], list[str]]:
    """Like `_family_ri_vm`, but the `mars` column carries a DIFFERENT value set in
    the shared year 2018 (distinct codes → distinct value_set_id), while jan/feb
    share one — an intra-year value-set divergence the merge must reject."""
    ri: list[str] = []
    vm: list[str] = []
    for mi, (token, _month) in enumerate(_MONTHS):
        var_id = 800 + mi
        colname = f"LonFink{token.capitalize()}"
        for yi, year in enumerate(_YEARS):
            cvid = 8000 + mi * 10 + yi
            ri.append(
                _var_row(
                    colname=colname,
                    cvid=cvid,
                    var_id=var_id,
                    varname=f"Inkomst {token}",
                    year=str(year),
                    regver_id=800 + mi * 10 + yi,
                    data_length="1",
                )
            )
            # `mars` in 2018 gets disjoint codes → a distinct value_set_id from the
            # jan/feb columns' shared set, within the same delivery year.
            codes = (
                [("7", "Annan"), ("8", "Extra")]
                if token == "mars" and year == 2018
                else _CODES
            )
            vm.extend(vm_rows(cvid, f"LonFink{year}", codes))
    return ri, vm
