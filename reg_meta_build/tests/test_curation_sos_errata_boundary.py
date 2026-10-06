"""SOS `[[errata.data_type]]` and `[[errata.classification_reference]]` at the build boundary.

One synthetic SOS workbook (derived from the `_sos_fixtures` SYN shape) carries, for
each declared correction, either the exact original row it names or one changed fact,
plus a second register that repeats the corrected row. Cases assert on the report event
ledger and the built SQLite artifact. Expected values come from the entries' documented
rule (`curation_tree.ErrataDataTypeEntry` / `ErrataClassificationReferenceEntry`): a
correction applies only to the one original record of its Deldatamängd/variable whose
column, original value and representation (and optional description witness) still
match; otherwise the entry is stale, or over-broad when the subset holds several peers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _curation_sos_boundary_support import (
    prepare,
    sos_register_toml,
    subset,
    variable,
    write_workbook,
)

if TYPE_CHECKING:
    from pathlib import Path

    from _curation_sos_boundary_support import Build, Prepared

TITLE = "Syntetiskt felregister"
OTHER_TITLE = "Syntetiskt grannregister"
SOURCE = f"Socialstyrelsen/Metadata {TITLE} (SYX)_webb.xlsx"
OTHER_SOURCE = f"Socialstyrelsen/Metadata {OTHER_TITLE} (SYY)_webb.xlsx"
CASES = "curation/registers/sos/syx.toml#/errata"
LINK = "https://example.test/klassifikationer/yrken"
CODES = "1 = ja\n0 = nej"
WITNESS = "Skapad med kontrollsiffra"
EVIDENCE = "Workbook row declares a date."
REFERENCE_EVIDENCE = "The linked classification cannot classify this output."

# Rows named once per (Deldatamängd, variable). Each stale row changes exactly one
# fact the correction checks; DUP repeats its subset/variable with other facts.
TYPE_ROWS = (
    variable("DATUM", "SYX_A", data_type="Decimal", value_set="YYYY-MM-DD"),
    variable("DATUM", "SYX_B", data_type="Decimal", value_set="YYYY-MM-DD"),
    variable("TYP", "SYX_A", data_type="Datum", value_set="YYYY-MM-DD"),
    variable("REPR", "SYX_A", data_type="Decimal", value_set="YYYYMMDD"),
    variable("UTANTYP", "SYX_A", data_type=None, value_set="YYYY-MM-DD"),
    variable("UTANREPR", "SYX_A", data_type="Decimal", value_set=None),
    variable("FORMEL", "SYX_A", data_type='="Decimal"', value_set="YYYY-MM-DD"),
    variable("KOLUMN", "SYX_A", data_type="Decimal", value_set="YYYY-MM-DD"),
    variable("DUP", "SYX_A", data_type="Decimal", value_set="YYYY-MM-DD"),
    variable("DUP", "SYX_A", data_type="Text", value_set="X"),
)
REFERENCE_ROWS = (
    variable("MARK", "SYX_A", value_set=CODES, link=LINK),
    variable("MARK", "SYX_B", value_set=CODES, link=LINK),
    variable("ANNANLANK", "SYX_A", value_set=CODES, link=f"{LINK}/annan"),
    variable("ANNANREPR", "SYX_A", value_set="1 = ja", link=LINK),
    variable("UTANLANK", "SYX_A", value_set=CODES),
    variable("VITTNE", "SYX_A", value_set=CODES, link=LINK, description=WITNESS),
    variable("ANNATVITTNE", "SYX_A", value_set=CODES, link=LINK, description="Annan"),
    variable("UTANVITTNE", "SYX_A", value_set=CODES, link=LINK),
    variable("FLER", "SYX_A", value_set=CODES, link=LINK),
    variable("FLER", "SYX_A", value_set="2 = kanske", link=LINK),
)
VARIABLES = tuple(dict.fromkeys(row.name for row in TYPE_ROWS + REFERENCE_ROWS))


def _data_type(name: str, *, column: str | None = None) -> str:
    return (
        f'[[errata.data_type]]\ndeldatamangd = "SYX_A"\nvariable = "{name}"\n'
        f'column = "{column or name}"\nexpected_type = "Decimal"\n'
        'expected_representation = "YYYY-MM-DD"\ndata_type = "date"\n'
        f'evidence = "{EVIDENCE}"\nnoted = "2026-09-29"\n'
    )


def _reference(name: str, *, column: str | None = None, witness: bool = False) -> str:
    return (
        '[[errata.classification_reference]]\ndeldatamangd = "SYX_A"\n'
        f'variable = "{name}"\ncolumn = "{column or name}"\n'
        f'expected_reference = "{LINK}"\nexpected_representation = """{CODES}"""\n'
        + (f'expected_description = "{WITNESS}"\n' if witness else "")
        + f'evidence = "{REFERENCE_EVIDENCE}"\nnoted = "2026-09-29"\n'
    )


# Entry order fixes each case id (`#/errata.<table>/<index>`).
TYPE_ENTRIES = (
    ("DATUM", _data_type("DATUM")),
    ("TYP", _data_type("TYP")),
    ("REPR", _data_type("REPR")),
    ("UTANTYP", _data_type("UTANTYP")),
    ("UTANREPR", _data_type("UTANREPR")),
    ("FORMEL", _data_type("FORMEL")),
    ("KOLUMN", _data_type("KOLUMN", column="ANNAN")),
    ("SAKNAS", _data_type("SAKNAS")),
    ("DUP", _data_type("DUP")),
)
REFERENCE_ENTRIES = (
    ("MARK", _reference("MARK")),
    ("ANNANLANK", _reference("ANNANLANK")),
    ("ANNANREPR", _reference("ANNANREPR")),
    ("UTANLANK", _reference("UTANLANK")),
    ("KOLUMN", _reference("MARK", column="ANNAN")),
    ("VITTNE", _reference("VITTNE", witness=True)),
    ("ANNATVITTNE", _reference("ANNATVITTNE", witness=True)),
    ("UTANVITTNE", _reference("UTANVITTNE", witness=True)),
    ("FLER", _reference("FLER")),
)


def _ids(table: str, entries, names) -> list[str]:
    return sorted(
        f"{CASES}.{table}/{index}"
        for index, (name, _) in enumerate(entries, 1)
        if name in names
    )


def _write(source: Path) -> None:
    write_workbook(
        source,
        "SYX",
        TITLE,
        TYPE_ROWS + REFERENCE_ROWS,
        (subset("SYX_A"), subset("SYX_B")),
    )
    # A second register repeats the corrected subset/variable rows exactly.
    write_workbook(
        source,
        "SYY",
        OTHER_TITLE,
        (TYPE_ROWS[0], REFERENCE_ROWS[0]),
        (subset("SYX_A"),),
    )


@pytest.fixture(scope="module")
def prepared(tmp_path_factory) -> Prepared:
    return prepare(tmp_path_factory.mktemp("sos-errata"), _write)


def _build(prepared: Prepared, tmp_path: Path, body: str = "") -> Build:
    return prepared.build(
        tmp_path,
        "build",
        {
            "sos/syx.toml": sos_register_toml(
                "syx",
                TITLE,
                variants=("SYX_A", "SYX_B"),
                variables=VARIABLES,
                body=body,
            ),
            "sos/syy.toml": sos_register_toml(
                "syy", OTHER_TITLE, variants=("SYX_A",), variables=("DATUM", "MARK")
            ),
        },
    )


def _state(build: Build, register: str, name: str, variant: str) -> tuple:
    """(data_type, provenance, value-set codes) of one built state."""
    ((data_type, provenance, value_set),) = build.rows(
        "SELECT s.data_type, s.provenance, s.value_set_id FROM variable_state s "
        "JOIN variable v USING (variable_id) "
        "JOIN register_variant rv USING (register_variant_id) "
        "JOIN register r ON r.register_id = v.register_id "
        "WHERE r.slug = ? AND v.slug = ? AND rv.slug = ?",
        register,
        name.lower(),
        variant,
    )
    codes = build.rows(
        "SELECT c.code, c.label FROM value_set_member m "
        "JOIN value_code c USING (code_id) WHERE m.value_set_id = ? ORDER BY c.code",
        value_set,
    )
    return data_type, provenance, codes


def _unresolved_references(build: Build) -> list[tuple[str, str, str]]:
    """(source, subset, variable) of each unbound classification declaration."""
    return sorted(
        (ref["source"], ref["semantic_record_key"][1], ref["semantic_record_key"][2])
        for e in build.issues("unresolved_classification_reference")
        for ref in e["refs"]
    )


def test_data_type_correction_replaces_only_the_named_original_type(
    prepared: Prepared, tmp_path: Path
) -> None:
    """The one matching SYX_A original becomes `date` with the entry's attribution;
    the same variable under SYX_B and in another register keeps its delivered type."""
    case = f"{CASES}.data_type/1"
    build = _build(prepared, tmp_path, TYPE_ENTRIES[0][1])
    assert build.case_status() == {case: "applicable"}
    assert build.applied_cases() == [case]
    assert not build.issues("stale_curation_entry")
    assert _state(build, "syx", "DATUM", "syx-a") == (
        "date",
        f"{case}: {EVIDENCE}",
        [],
    )
    assert _state(build, "syx", "DATUM", "syx-b")[:2] == ("Decimal", None)
    assert _state(build, "syy", "DATUM", "syx-a")[:2] == ("Decimal", None)


def test_data_type_entry_with_a_changed_or_absent_original_is_stale(
    prepared: Prepared, tmp_path: Path
) -> None:
    """A changed type, representation or column, a blank or formula type cell, a blank
    representation, or no such variable leaves the entry stale and the type unchanged."""
    build = _build(prepared, tmp_path, "".join(text for _, text in TYPE_ENTRIES))
    stale = {"TYP", "REPR", "UTANTYP", "UTANREPR", "FORMEL", "KOLUMN", "SAKNAS"}
    assert build.issue_cases("stale_curation_entry") == _ids(
        "data_type", TYPE_ENTRIES, stale
    )
    assert build.applied_cases() == [f"{CASES}.data_type/1"]
    assert _state(build, "syx", "REPR", "syx-a")[:2] == ("Decimal", None)
    assert _state(build, "syx", "KOLUMN", "syx-a")[:2] == ("Decimal", None)
    assert _state(build, "syx", "TYP", "syx-a")[:2] == ("date", None)


def test_data_type_entry_over_several_subset_peers_is_over_broad(
    prepared: Prepared, tmp_path: Path
) -> None:
    """Two SYX_A rows for one variable make the entry over-broad, citing both."""
    build = _build(prepared, tmp_path, TYPE_ENTRIES[-1][1])
    (issue,) = build.issues("overbroad_curation_entry")
    assert issue["case_id"] == f"{CASES}.data_type/1"
    assert [ref["semantic_record_key"] for ref in issue["refs"]] == [
        ["register:" + TITLE, "deldatamangd:SYX_A", "variable:DUP"]
    ] * 2
    assert build.applied_cases() == []
    assert not build.issues("stale_curation_entry")


def test_classification_reference_correction_clears_only_the_declaration(
    prepared: Prepared, tmp_path: Path
) -> None:
    """The matching SYX_A original no longer binds its link (an unspecified
    declaration instead of an unresolved one) and keeps its inline value set; the
    SYX_B peer and the other register still report their link as unresolved."""
    case = f"{CASES}.classification_reference/1"
    before = _build(prepared, tmp_path / "before")
    build = _build(prepared, tmp_path / "after", REFERENCE_ENTRIES[0][1])
    assert build.case_status() == {case: "applicable"}
    assert build.applied_cases() == [case]
    mark = [
        (source, subset)
        for source, subset, name in _unresolved_references(build)
        if name == "variable:MARK"
    ]
    assert (SOURCE, "deldatamangd:SYX_A") not in mark
    assert sorted(mark) == [
        (SOURCE, "deldatamangd:SYX_B"),
        (OTHER_SOURCE, "deldatamangd:SYX_A"),
    ]
    assert (SOURCE, "deldatamangd:SYX_A", "variable:MARK") in _unresolved_references(
        before
    )
    assert [
        e["refs"][0]["semantic_record_key"]
        for e in build.issues("unknown_classification_declaration")
        if e["refs"][0]["semantic_record_key"][2] == "variable:MARK"
    ] == [["register:" + TITLE, "deldatamangd:SYX_A", "variable:MARK"]]
    data_type, provenance, codes = _state(build, "syx", "MARK", "syx-a")
    assert provenance == f"{case}: {REFERENCE_EVIDENCE}"
    assert (data_type, codes) == _state(before, "syx", "MARK", "syx-a")[::2]
    assert codes == [("0", "nej"), ("1", "ja")]


def test_classification_reference_entry_fails_closed_on_changed_facts(
    prepared: Prepared, tmp_path: Path
) -> None:
    """A changed link, representation or column, a blank link, a missing or changed
    description witness, or several subset peers apply nothing; only the exact
    original and the matching witness are corrected."""
    build = _build(prepared, tmp_path, "".join(text for _, text in REFERENCE_ENTRIES))
    stale = {"ANNANLANK", "ANNANREPR", "UTANLANK", "KOLUMN"}
    stale |= {"ANNATVITTNE", "UTANVITTNE"}
    assert build.issue_cases("stale_curation_entry") == _ids(
        "classification_reference", REFERENCE_ENTRIES, stale
    )
    assert build.issue_cases("overbroad_curation_entry") == _ids(
        "classification_reference", REFERENCE_ENTRIES, {"FLER"}
    )
    assert build.applied_cases() == _ids(
        "classification_reference", REFERENCE_ENTRIES, {"MARK", "VITTNE"}
    )
    unresolved = {name for _, _, name in _unresolved_references(build)}
    for name in ("ANNATVITTNE", "UTANVITTNE", "ANNANREPR", "FLER"):
        assert f"variable:{name}" in unresolved
    assert "variable:VITTNE" not in unresolved
