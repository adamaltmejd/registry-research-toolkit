"""Checked corrections: the FDB two-spelling column ownership converts through the production entry."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import scb_record
from reg_meta_build.curation_compile import convert_column_partitions
from reg_meta_build.source_effects import (
    apply_occurrence_cases,
    record_ref,
)
from reg_meta_build.source_occurrences import source_occurrence

from reg_meta_build.fqid_slugs import (
    declared_column_ownership,
    load_provider_toml,
    load_slug_dir,
    repo_slug_dir,
)

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta_build.source_records import (
        SourceRecord,
    )

# Y-167: FDB VarId 830 literal column ownership. GatuRest/Gaturest share one
# identity, PGaturest keeps its own, on both FDB variants (424/427). The
# tracked declaration feeds the converter; nothing is folded or inferred.
_FDB_DECLARATION = (
    '[[identity.partition]]\nvariable = "1.830"\n'
    'columns = { GatuRest = "1.830.gaturest", Gaturest = "1.830.gaturest", '
    'PGaturest = "1.830.pgaturest" }\n'
    'columns_ref = "Y-167 fixture reference"\n'
)


def _fdb_record(
    column: str, *, row: int = 1, cvid: int = 20, year: str = "2020", variant: int = 424
) -> SourceRecord:
    return scb_record(
        row,
        cvid=cvid,
        var_id=830,
        colname=column,
        register=("FDB", 1, variant),
        regver_id=int(year),
        year=year,
    )


def _fdb_family() -> tuple[SourceRecord, ...]:
    return (
        _fdb_record("GatuRest", row=1, cvid=20, year="1999", variant=424),
        _fdb_record("Gaturest", row=2, cvid=21, year="2005", variant=424),
        _fdb_record("GatuRest", row=3, cvid=22, year="2010", variant=427),
        _fdb_record("Gaturest", row=4, cvid=23, year="2015", variant=427),
        _fdb_record("PGaturest", row=5, cvid=24, year="2020", variant=424),
        _fdb_record("PGaturest", row=6, cvid=25, year="2025", variant=427),
    )


def _fdb_entries(tmp_path: Path, body: str = _FDB_DECLARATION):
    slug_dir = tmp_path / "fqid_slugs"
    slug_dir.mkdir(exist_ok=True)
    declaration = slug_dir / "scb.toml"
    declaration.write_text(
        '[register."1"]\nslug = "fdb"\n'
        '[variable."1.830.gaturest"]\nslug = "gaturest"\n'
        '[variable."1.830.pgaturest"]\nslug = "pgaturest"\n',
        encoding="utf-8",
    )
    register_file = tmp_path / "curation" / "registers" / "scb" / "fdb.toml"
    register_file.parent.mkdir(parents=True, exist_ok=True)
    register_file.write_text(
        '[register]\nprovider = "scb"\nslug = "fdb"\nnative_id = "1"\n\n' + body,
        encoding="utf-8",
    )
    return load_provider_toml(declaration)


_FDB_SLUGS_ONLY = ""


def _fdb_convert(
    records: tuple[SourceRecord, ...],
    tmp_path: Path,
    *,
    body: str = _FDB_DECLARATION,
    split_ids: tuple[str, ...] = ("1.830.gaturest", "1.830.pgaturest"),
):
    # The production entry point: the tracked declaration is loaded from the
    # complete entry set, never hand-plumbed.
    ownership = declared_column_ownership(
        _fdb_entries(tmp_path, body),
        provider="scb",
        source_id="1.830",
        curation_dir=tmp_path / "curation",
    )
    return convert_column_partitions(
        records,
        source_id="1.830",
        split_ids=split_ids,
        declared_columns=dict(ownership.declared_columns),
        declaration_reference=ownership.declaration_reference,
    )


def test_fdb_two_spelling_ownership_converts_both_partitions(
    tmp_path: Path,
) -> None:
    records = _fdb_family()
    converted = _fdb_convert(records, tmp_path)
    assert converted.case is not None and converted.diagnostics == ()
    assert [b.source_id for b in converted.bindings] == [
        "1.830.gaturest",
        "1.830.pgaturest",
    ]
    keys = {b.source_id: b.target.source_key for b in converted.bindings}
    assert "Y-167 fixture reference" in converted.case.decision.provenance
    assert {item.ref for item in converted.case.targets} == {
        record_ref(record) for record in records
    }
    assert converted.case.support == ()
    (guard,) = converted.case.peer_guards
    assert set(guard.expected_members) == {record_ref(record) for record in records}
    result = apply_occurrence_cases(records, (converted.case,))
    assert result.diagnostics == ()
    assert all(
        o.fields == r.fields for o, r in zip(result.occurrences, records, strict=True)
    )
    # The two spellings share one identity; PGaturest keeps its own.
    assert result.occurrences[0].variable_key == keys["1.830.gaturest"]
    assert result.occurrences[1].variable_key == keys["1.830.gaturest"]
    assert result.occurrences[2].variable_key == keys["1.830.gaturest"]
    assert result.occurrences[3].variable_key == keys["1.830.gaturest"]
    assert result.occurrences[4].variable_key == keys["1.830.pgaturest"]
    assert result.occurrences[5].variable_key == keys["1.830.pgaturest"]
    assert keys["1.830.gaturest"] != keys["1.830.pgaturest"]


def test_fdb_committed_declaration_converts_through_the_production_entry() -> None:
    # The committed scb.toml/scb.auto.toml pair — the complete loaded entry
    # set a real build reads — drives the wired conversion, not a fixture copy.
    slug_dir = repo_slug_dir()
    assert slug_dir is not None
    records = _fdb_family()
    ownership = declared_column_ownership(
        load_slug_dir(slug_dir), provider="scb", source_id="1.830"
    )
    converted = convert_column_partitions(
        records,
        source_id="1.830",
        split_ids=("1.830.gaturest", "1.830.pgaturest"),
        declared_columns=dict(ownership.declared_columns),
        declaration_reference=ownership.declaration_reference,
    )
    assert converted.case is not None and converted.diagnostics == ()
    assert [b.source_id for b in converted.bindings] == [
        "1.830.gaturest",
        "1.830.pgaturest",
    ]
    assert converted.case.decision.kind == "correct_occurrences"
    assert "Y-167 2026-09-18" in converted.case.decision.provenance
    (guard,) = converted.case.peer_guards
    assert set(guard.expected_members) == {record_ref(record) for record in records}


def test_fdb_missing_declaration_raises_instead_of_plain_converting(
    tmp_path: Path,
) -> None:
    # Fail-fast, no fallback: asking for a declared partition when the family
    # has no tracked `columns` declaration raises instead of silently
    # converting without ownership (a relation-owned family lost its ownership
    # map that way). Plain conversion stays reachable only through
    # `convert_column_partitions`.
    with pytest.raises(ValueError, match="no declared literal column ownership"):
        _fdb_convert(_fdb_family(), tmp_path, body=_FDB_SLUGS_ONLY)


def test_fdb_fourth_spelling_rejects_new_intersecting_evidence(
    tmp_path: Path,
) -> None:
    records = _fdb_family()
    renamed = _fdb_record("GATU-REST", row=7, cvid=26, year="2025", variant=427)
    with pytest.raises(ValueError, match="complete columns and split keys"):
        _fdb_convert((*records, renamed), tmp_path)
    # The converted case is source-guarded too: a new peer makes it stale.
    converted = _fdb_convert(records, tmp_path)
    assert converted.case is not None
    assert (
        apply_occurrence_cases((*records, renamed), (converted.case,))
        .accounting[0]
        .disposition
        == "stale"
    )


def test_fdb_declared_split_mismatch_rejects_instead_of_half_converting(
    tmp_path: Path,
) -> None:
    # The tracked declaration never invents or drops splits: accepted split_ids
    # disagreeing with the declared owners fail instead of half-converting.
    with pytest.raises(ValueError, match="complete columns and split keys"):
        _fdb_convert(_fdb_family(), tmp_path, split_ids=("1.830.gaturest",))


def test_fdb_unassigned_spelling_diagnoses_without_blocking_owned_identity() -> None:
    # Diagnostic vs strict: an explicitly unassigned spelling keeps its native
    # unresolved identity and raises an error diagnostic, while the declared
    # two-spelling ownership still converts.
    records = (
        _fdb_record("GatuRest", row=1, cvid=20, year="2020"),
        _fdb_record("Gaturest", row=2, cvid=21, year="2021"),
        _fdb_record("GATU-REST", row=3, cvid=22, year="2021"),
    )
    converted = convert_column_partitions(
        records,
        source_id="1.830",
        split_ids=("1.830.gaturest",),
        declared_columns={
            "GatuRest": "1.830.gaturest",
            "Gaturest": "1.830.gaturest",
            "GATU-REST": None,
        },
        declaration_reference="Y-167 fixture reference",
    )
    assert converted.case is not None and len(converted.bindings) == 1
    assert [(d.code, d.severity) for d in converted.diagnostics] == [
        ("unassigned_original_columns", "error")
    ]
    result = apply_occurrence_cases(records, (converted.case,))
    assert not result.diagnostics
    assert (
        result.occurrences[0].variable_key
        == result.occurrences[1].variable_key
        == converted.bindings[0].target.source_key
    )
    assert result.occurrences[2] == source_occurrence(records[2])
