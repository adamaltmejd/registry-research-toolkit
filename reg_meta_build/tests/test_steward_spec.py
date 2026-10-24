"""The steward spec (`scope.toml` + `series/*.toml`): load and its refusals.

The SWECOV generator is the spec's only reader (`_swecov_fixtures` covers what it
emits from one); these cases pin the load boundary itself: the committed spec
loads, and the two located refusals that are the spec's own rules.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from reg_meta_build.errors import RegMetaError
from reg_meta_build.inventory import WINDOW_INVALID
from reg_meta_build.steward_spec import (
    SERIES_REGISTER_MISMATCH,
    SPEC_INVALID,
    TABLE_DISPOSITION_INVALID,
    load_steward_spec,
)

COMMITTED_SPEC = (
    Path(__file__).resolve().parents[2] / "reg_webapp" / "stewards" / "swecov"
)


def _spec(root: Path, series: dict[str, str], scope: str = "") -> Path:
    (root / "series").mkdir(parents=True)
    (root / "scope.toml").write_text(f'census_sha256 = "{"0" * 64}"\n{scope}')
    for series_id, body in series.items():
        (root / "series" / f"{series_id}.toml").write_text(body)
    return root


def _series(series_id: str, register: str, variants: list[str], tables: list[str]):
    members = "".join(
        f'\n[[table]]\ntable = "{t}"\nedition = {t[-4:]}\n' for t in tables
    )
    quoted = ", ".join(f'"{v}"' for v in variants)
    return (
        f'id = "{series_id}"\nregister = "{register}"\nvariants = [{quoted}]\n{members}'
    )


def test_committed_swecov_spec_loads() -> None:
    """Fails if any committed scope or series file breaks the schema, a series
    rule or the one-disposition-per-table rule."""
    spec = load_steward_spec(COMMITTED_SPEC)
    assert spec.series and spec.scope.flavors


def test_series_spanning_two_registers_is_refused_and_its_split_loads(
    tmp_path: Path,
) -> None:
    """A series names one register: one admitting a variant of a second register
    (the shape of a family whose years moved between registers) is refused with
    the series file and both registers named; the same tables split into one
    series per register load. Fails if a variant's register is not checked
    against the series register."""
    spanning = _spec(
        tmp_path / "spanning",
        {
            "prover": _series(
                "prover",
                "inera/bestallda-prover",
                ["inera/bestallda-prover/_default", "inera/samtal/_default"],
                ["T2019", "T2020"],
            )
        },
    )
    with pytest.raises(RegMetaError) as refused:
        load_steward_spec(spanning)
    assert refused.value.code == SERIES_REGISTER_MISMATCH
    assert "series/prover.toml" in refused.value.message
    assert "inera/samtal" in refused.value.message

    split = _spec(
        tmp_path / "split",
        {
            "prover": _series(
                "prover",
                "inera/bestallda-prover",
                ["inera/bestallda-prover/_default"],
                ["T2019"],
            ),
            "samtal": _series(
                "samtal", "inera/samtal", ["inera/samtal/_default"], ["T2020"]
            ),
        },
    )
    assert [s.id for s in load_steward_spec(split).series] == ["prover", "samtal"]


@pytest.mark.parametrize(
    ("own", "admitted"),
    [
        ('["inera/bestallda-prover/andra"]', False),
        ('["inera/bestallda-prover/_default"]', True),
    ],
)
def test_table_variants_must_be_a_subset_of_the_series_variants(
    tmp_path: Path, own: str, admitted: bool
) -> None:
    """A table narrows its admissible variants within its series instead of
    starting a new series; a table variant the series does not list is
    refused, located at that table entry. Fails if a table's own variants are
    not checked against the series set."""
    body = (
        _series(
            "prover",
            "inera/bestallda-prover",
            ["inera/bestallda-prover/_default", "inera/bestallda-prover/fortsatt"],
            ["T2019"],
        )
        + f'\n[[table]]\ntable = "T2020"\nvariants = {own}\nedition = 2020\n'
    )
    spec_dir = _spec(tmp_path, {"prover": body})
    if admitted:
        (series,) = load_steward_spec(spec_dir).series
        assert series.table_variants()["T2020"] == ["inera/bestallda-prover/_default"]
        return
    with pytest.raises(RegMetaError) as refused:
        load_steward_spec(spec_dir)
    assert refused.value.code == SERIES_REGISTER_MISMATCH
    assert "table[2] (T2020)" in refused.value.message


def test_table_in_a_series_and_a_scope_exclusion_is_refused(tmp_path: Path) -> None:
    """A table has exactly one disposition: a series member that the scope also
    excludes is refused, naming the table and both places; without the
    exclusion it loads. Fails if dispositions are checked only within one file."""
    series = {
        "prover": _series(
            "prover",
            "inera/bestallda-prover",
            ["inera/bestallda-prover/_default"],
            ["T2019", "T2020"],
        )
    }
    exclude = '[[exclude]]\ntable = "T2020"\nreason = "Superseded re-delivery."\n'
    with pytest.raises(RegMetaError) as refused:
        load_steward_spec(_spec(tmp_path / "twice", series, exclude))
    assert refused.value.code == TABLE_DISPOSITION_INVALID
    assert "T2020" in refused.value.message
    assert "series/prover.toml" in refused.value.message
    assert "[[exclude]]" in refused.value.message

    assert load_steward_spec(_spec(tmp_path / "once", series)).dispositions() == {
        "T2019": ["series/prover.toml"],
        "T2020": ["series/prover.toml"],
    }


@pytest.mark.parametrize(("selected", "admitted"), [("T2020", True), ("T2021", False)])
def test_column_decision_selects_only_tables_of_its_series(
    tmp_path: Path, selected: str, admitted: bool
) -> None:
    """A decision's `tables` selects whole member tables of its own series; a
    table another series (or none) holds is refused, located at the column,
    since the decision would never reach it. Fails if `tables` is not checked
    against the series membership."""
    body = _series(
        "prover",
        "inera/bestallda-prover",
        ["inera/bestallda-prover/_default"],
        ["T2019", "T2020"],
    ) + (f'\n[[column]]\ncolumn = "Kol"\ntables = ["{selected}"]\nunmap = "Gone."\n')
    spec_dir = _spec(tmp_path, {"prover": body})
    if admitted:
        (series,) = load_steward_spec(spec_dir).series
        assert set(series.decisions()) == {("T2020", "Kol")}
        return
    with pytest.raises(RegMetaError) as refused:
        load_steward_spec(spec_dir)
    assert refused.value.code == SPEC_INVALID
    assert "column[1] (Kol): tables outside the series: T2021" in (
        refused.value.message
    )


@pytest.mark.parametrize(
    ("decision", "admitted"),
    [
        ('unmap = "No catalog owner."', True),
        ('hold = { follow_up = "#1603", reason = "No catalog owner yet." }', False),
    ],
)
def test_only_an_all_unmap_series_may_omit_its_register(
    tmp_path: Path, decision: str, admitted: bool
) -> None:
    """A series declares its register; only tables with no catalog owner, every
    column unmapped, may omit it. A hold there is refused, located at the column.
    Fails if a series without a register accepts a decision other than unmap."""
    body = (
        'id = "barn"\n\n[[table]]\ntable = "T2020"\nedition = 2020\n\n'
        f'[[column]]\ncolumn = "Kol"\n{decision}\n'
    )
    spec_dir = _spec(tmp_path, {"barn": body})
    if admitted:
        (series,) = load_steward_spec(spec_dir).series
        assert series.register_coord is None
        return
    with pytest.raises(RegMetaError) as refused:
        load_steward_spec(spec_dir)
    assert refused.value.code == SERIES_REGISTER_MISMATCH
    assert "column[1] (Kol)" in refused.value.message


@pytest.mark.parametrize(("window", "admitted"), [("2019", True), ("2020", False)])
def test_mapping_warning_window_must_meet_a_decided_table(
    tmp_path: Path, window: str, admitted: bool
) -> None:
    """A decision's mapping warning (#1684) is clipped to each of its tables, so
    a window meeting none of them is refused, located at the column. The decision
    covers T2019 only: a 2020 window meets the series' T2020 but no decided table.
    Fails if the window is checked against every series table, or not at all."""
    body = _series(
        "prover",
        "inera/bestallda-prover",
        ["inera/bestallda-prover/_default"],
        ["T2019", "T2020"],
    ) + (
        '\n[[column]]\ncolumn = "Kol"\ntables = ["T2019"]\nreason = "Reviewed."\n'
        'mappings = [{ register_variant = "inera/bestallda-prover/_default", '
        'variable = "inera/bestallda-prover/kol", representation = "Kol", '
        'data_warning = { summary = "Recoded", detail = "Recoded later.", '
        f"window = {window} }} }}]\n"
    )
    spec_dir = _spec(tmp_path, {"prover": body})
    if admitted:
        (series,) = load_steward_spec(spec_dir).series
        (mapping,) = series.columns[0].mappings
        assert mapping.data_warning.window == window
        return
    with pytest.raises(RegMetaError) as refused:
        load_steward_spec(spec_dir)
    assert refused.value.code == WINDOW_INVALID
    assert "series/prover.toml: series 'prover' column[1] (Kol)" in (
        refused.value.message
    )
    assert "data_warning window 2020 meets no period" in refused.value.message
