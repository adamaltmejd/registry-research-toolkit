"""Ordinary native families form catalog content without whole-variable cases: types, coverage, native quantities and FDB partitions."""

from __future__ import annotations

from contextlib import closing
from dataclasses import replace
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import SCB_REVISION
from _curation_fixtures import write_fdb_partition_curation
from _prepared_fixtures import accept_prepared
from catalog_manifest import synthetic_manifest
from reg_meta_build.curation_compile import convert_column_partitions
from reg_meta_build.db import open_built_db
from reg_meta_build.prepared_sources import (
    open_prepared_source_records,
    prepare_source_records,
)
from reg_meta_build.resolved_catalog import (
    ResolvedVariant,
    write_resolved_catalog,
)
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    resolve_code_membership,
)
from reg_meta_build.source_coordinates import (
    native_column_key,
    native_variable_key,
    native_variant_key,
)
from reg_meta_build.source_effects import apply_occurrence_cases
from reg_meta_build.source_formation import form_native_variable
from reg_meta_build.source_occurrences import (
    EffectiveOccurrence,
    effective_occurrence,
)
from reg_meta_build.source_periods import source_scopes
from reg_meta_build.source_records import (
    ScopeInterval,
    SourceRecord,
    TemporalScope,
    value_field,
)
from reg_meta_build.sources.swecov_column_types import StewardColumnStorage

from reg_meta_build.fqid_slugs import declared_column_ownership, load_provider_toml

if TYPE_CHECKING:
    from pathlib import Path
from _source_formation_support import (
    FLAGS as _FLAGS,
    REGISTER as _REGISTER,
    form_family as _form,
    formation_record as _record,
)


def test_widened_segment_records_documented_and_storage_provenance() -> None:
    records = tuple(
        _record(2020, row=label).model_copy(
            update={
                "fields": _record(2020, row=label).fields.model_copy(
                    update={
                        "data_type": value_field(kind),
                        "data_length": value_field(length),
                    }
                )
            }
        )
        for label, kind, length in (("a", "varchar", "18"), ("b", "float", "53"))
    )
    storage = {
        "value": StewardColumnStorage(
            classes=frozenset({"integer", "decimal"}),
            provenance="SWECOV storage csv: CIS2018=int, CIS2020=float",
        )
    }
    result = _form(records, storage=storage)
    assert result.variable is not None
    (state,) = result.variable.states
    assert (state.data_type, state.data_length) == ("decimal", None)
    assert state.provenance is not None
    assert "Documented Datatyp: float, varchar" in state.provenance
    assert "CIS2018=int, CIS2020=float" in state.provenance
    assert not any(
        item.code in {"unknown_data_type", "conflicting_occurrence_facts"}
        for item in result.diagnostics
    )


def test_incompatible_documented_types_retain_detail_and_conflict() -> None:
    records = tuple(
        record.model_copy(
            update={
                "fields": record.fields.model_copy(
                    update={"data_type": value_field(kind)}
                )
            }
        )
        for record, kind in zip(
            (_record(2020, row="a"), _record(2020, row="b")),
            ("date", "integer"),
        )
    )
    result = _form(
        records,
        storage={
            "value": StewardColumnStorage(
                classes=frozenset({"integer"}),
                provenance="SWECOV storage csv: CIS2020=int",
            )
        },
    )
    (unknown,) = (
        item for item in result.diagnostics if item.code == "unknown_data_type"
    )
    assert "Documented Datatyp: date, integer" in unknown.detail
    assert "CIS2020=int" in unknown.detail
    assert any(
        item.code == "conflicting_occurrence_facts" for item in result.diagnostics
    )


def test_explicit_open_ended_coverage_survives_formation_and_storage(
    tmp_path: Path,
) -> None:
    scope = TemporalScope(
        kind="intervals", intervals=(ScopeInterval(start="2004-01-01", end=None),)
    )
    record = _record(2004).model_copy(update={"edition_period_scope": scope})
    claim = CodeListClaim(
        claim_id="accepted-list",
        scope=scope,
        members=(
            CodeMembershipClaim("01", "One", TemporalScope(kind="year_independent")),
        ),
    )
    result = _form((record,), claims=(claim,))
    assert result.variable is not None and result.diagnostics == ()
    state = result.variable.states[0]
    assert (state.valid_from, state.valid_to) == ("2004-01-01", "9999-12-31")
    assert result.coverage[0].coding_claim == (
        state.value_set,
        state.value_set_version_label,
    )
    output = tmp_path / "catalog.db"
    write_resolved_catalog((result.variable,), output, manifest=synthetic_manifest())
    with closing(open_built_db(output)) as conn:
        assert tuple(
            conn.execute("SELECT valid_from, valid_to FROM variable_state").fetchone()
        ) == ("2004-01-01", "9999-12-31")


def test_prepared_native_family_forms_and_writes_without_handwritten_cases(
    tmp_path: Path,
) -> None:
    records = (_record(2020), _record(2022))
    artifact = tmp_path / "inputs" / "records"
    manifest = prepare_source_records(
        artifact,
        records=records,
        revisions=(SCB_REVISION,),
        scope="complete synthetic source",
    )
    commit = accept_prepared(artifact)
    prepared = open_prepared_source_records(
        artifact, expected_sha256=manifest.sha256, input_commit=commit
    )
    result = _form(tuple(prepared.records))
    assert result.variable is not None
    assert result.diagnostics == ()
    assert result.occurrences == records
    output = tmp_path / "catalog" / "reg_meta.db"
    write_resolved_catalog(
        (result.variable,),
        output,
        manifest=synthetic_manifest() | {"source": manifest.sha256},
    )
    with closing(open_built_db(output)) as conn:
        assert tuple(
            conn.execute(
                "SELECT provider_key, slug, name, definition FROM variable"
            ).fetchone()
        ) == ("4", "value", "Source variable", "Source definition")
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT valid_from, valid_to, delivery_column_name, value_set_id FROM variable_state ORDER BY valid_from"
            )
        ] == [
            ("2020-01-01", "2020-12-31", "VALUE", None),
            ("2022-01-01", "2022-12-31", "VALUE", None),
        ]


def test_pooled_only_edition_forms_one_marked_state(tmp_path: Path) -> None:
    """Y-202: a genuinely pooled multi-year edition forms ONE marked state.

    The whole pooled range lands in the DB with `pooled = 1` — no annual
    inference inside the range, no unsupported-occurrence error."""
    edition, period, issue = source_scopes("2012 - 2014")
    assert issue == "pooled_period"
    record = _record(2012, row="-pooled").model_copy(
        update={"edition_scope": edition, "edition_period_scope": period}
    )
    result = _form((record,))
    assert result.variable is not None and result.diagnostics == ()
    (state,) = result.variable.states
    assert (state.valid_from, state.valid_to) == ("2012-01-01", "2014-12-31")
    assert state.pooled is True
    output = tmp_path / "catalog.db"
    write_resolved_catalog((result.variable,), output, manifest=synthetic_manifest())
    with closing(open_built_db(output)) as conn:
        assert tuple(
            conn.execute(
                "SELECT valid_from, valid_to, pooled FROM variable_state"
            ).fetchone()
        ) == ("2012-01-01", "2014-12-31", 1)


def test_pooled_edition_state_carries_its_value_set() -> None:
    """Y-207: a pooled variable's one marked state carries its bound codes.

    The coding resolves over the whole pooled range, so formation finds the
    membership for the occurrence period — no `missing_coding_period`."""
    edition, period, issue = source_scopes("2020 - 2022")
    assert issue == "pooled_period"
    record = _record(2020, row="-pooled").model_copy(
        update={"edition_scope": edition, "edition_period_scope": period}
    )
    claim = CodeListClaim(
        claim_id="pooled-list",
        scope=period,
        members=(
            CodeMembershipClaim("01", "One", TemporalScope(kind="year_independent")),
        ),
    )
    result = _form((record,), claims=(claim,))
    assert result.variable is not None
    assert result.diagnostics == ()
    (state,) = result.variable.states
    assert (state.valid_from, state.valid_to) == ("2020-01-01", "2022-12-31")
    assert state.pooled is True
    assert state.value_set is not None
    assert state.value_set.members == (("01", "One"),)


def test_native_identity_is_scoped_and_preserves_native_primitive_type() -> None:
    integer, string, distinct = (
        _record(2020),
        _record(2020, native_id="4"),
        _record(2020, native_id=5),
    )
    assert len({native_variable_key(r) for r in (integer, string, distinct)}) == 3
    assert native_variable_key(integer) == native_variable_key(_record(2021, variant=3))
    assert native_column_key(integer) != native_column_key(distinct)
    with pytest.raises(ValueError, match="one complete native"):
        _form((integer, string))


def test_source_native_quantity_retains_sequential_literal_columns() -> None:
    records = (_record(2020, column="OLD"), _record(2021, column="NEW"))
    result = _form(records)
    assert result.variable is not None
    assert {state.delivery_column_name for state in result.variable.states} == {
        "OLD",
        "NEW",
    }
    assert result.occurrences == records
    assert not any(d.severity == "error" for d in result.diagnostics)
    assert [d.code for d in result.diagnostics] == ["source_native_identity_retained"]


def test_case_only_native_quantity_preserves_literal_deliveries() -> None:
    """Stable native quantity metadata retains each literal source column."""
    records = (
        _record(2004, column="v0115"),
        _record(2005, column="V0115"),
        _record(2006, column="V0115"),
    )
    result = _form(records)
    assert result.variable is not None
    assert not any(d.severity == "error" for d in result.diagnostics)
    assert [(s.valid_from, s.delivery_column_name) for s in result.variable.states] == [
        ("2004-01-01", "v0115"),
        ("2005-01-01", "V0115"),
        ("2006-01-01", "V0115"),
    ]
    assert [d.code for d in result.diagnostics] == ["source_native_identity_retained"]
    assert {r.fields.column_name.value for r in result.occurrences} == {
        "v0115",
        "V0115",
    }


def test_diacritic_native_quantity_preserves_literal_deliveries() -> None:
    """Name/definition identity does not rewrite diacritics in delivery columns."""
    result = _form((_record(2020, column="Kön"), _record(2021, column="Kon")))
    assert result.variable is not None
    assert not any(d.severity == "error" for d in result.diagnostics)
    assert {s.delivery_column_name for s in result.variable.states} == {"Kön", "Kon"}
    assert [d.code for d in result.diagnostics] == ["source_native_identity_retained"]


def test_co_delivered_twins_keep_the_error_and_form_nothing() -> None:
    """Y-220: `Niva`/`Nivå` side by side in one edition are two columns.

    Sequential twins keep folding; one edition co-delivering two spellings of
    one fold key keeps the pre-Y-210 `unresolved_native_identity` error and
    forms nothing."""
    probe = effective_occurrence(_record(2001, column="Niva"))
    assert probe.variant_key is not None
    edition_key = (*probe.variant_key, "edition", "native-int", 14946)

    def _edition_record(column: str, data_length: str) -> EffectiveOccurrence:
        record = _record(2001, column=column)
        return replace(
            effective_occurrence(
                record.model_copy(
                    update={
                        "fields": record.fields.model_copy(
                            update={"data_length": value_field(data_length)}
                        )
                    }
                )
            ),
            edition_key=edition_key,
        )

    result = _form((_edition_record("Niva", "3"), _edition_record("Nivå", "1")))
    assert result.variable is None
    assert result.coverage == ()
    assert [d.code for d in result.diagnostics] == ["unresolved_native_identity"]
    assert result.diagnostics[0].severity == "error"
    distinct = _form((_record(2020, column="OLD"), _record(2020, column="NEW")))
    assert result.diagnostics[0].detail == distinct.diagnostics[0].detail
    assert result.diagnostics[0].fields == distinct.diagnostics[0].fields


def test_source_native_quantity_does_not_require_spelling_similarity() -> None:
    """Native quantity identity does not infer equivalence from column strings."""
    for old, new in (("BLK", "BLKFTG"), ("H56", "H5_6")):
        result = _form((_record(2020, column=old), _record(2021, column=new)))
        assert result.variable is not None
        assert {state.delivery_column_name for state in result.variable.states} == {
            old,
            new,
        }
        assert not any(d.severity == "error" for d in result.diagnostics)


def test_checked_column_keeps_its_literal_when_twins_fold() -> None:
    """Y-210 a1: folding never rewrites an explicit partition/alias decision.

    The checked 2002 `V0115` state keeps its literal while the unchecked twins
    take the most recent spelling (`v0115` from 2005); coverage follows each
    output's own origin. Origin is tracked explicitly, never by literal."""
    checked = replace(
        effective_occurrence(_record(2002, column="V0115")), identity_checked=True
    )
    result = _form(
        (checked, _record(2004, column="V0115"), _record(2005, column="v0115"))
    )
    assert result.variable is not None
    assert not any(d.severity == "error" for d in result.diagnostics)
    assert [(s.valid_from, s.delivery_column_name) for s in result.variable.states] == [
        ("2002-01-01", "V0115"),
        ("2004-01-01", "v0115"),
        ("2005-01-01", "v0115"),
    ]
    (warning,) = [d for d in result.diagnostics if d.code == "column_spelling_folded"]
    assert "most recent spelling (v0115)" in warning.detail
    assert sorted((c.valid_from, c.column) for c in result.coverage) == [
        ("2002-01-01", "V0115"),
        ("2004-01-01", "v0115"),
        ("2005-01-01", "v0115"),
    ]


def _fdb_record(year: int, *, column: str, variant: int) -> SourceRecord:
    return _record(year, column=column, native_id=830, variant=variant)


def test_fdb_two_spelling_ownership_forms_both_partitions(tmp_path: Path) -> None:
    """Y-167: the tracked 1.830 ownership lets both partitions form.

    Without a declaration stable source-native quantity metadata preserves both
    sequential literals. An explicit partition remains authoritative and forms
    separate owners instead of being re-merged by that default.
    """
    pair = (
        _fdb_record(1999, column="GatuRest", variant=424),
        _fdb_record(2005, column="Gaturest", variant=424),
    )
    pair_variant = native_variant_key(pair[0])
    assert pair_variant is not None
    variants = {
        pair_variant: ResolvedVariant(
            slug="arbetsstalleenheter", name="Arbetsställeenheter"
        )
    }
    pair_columns = [native_column_key(record) for record in pair]
    assert len(pair_columns) == 2 and None not in pair_columns
    folded = form_native_variable(
        pair,
        register=_REGISTER,
        variants=variants,
        slug="gaturest",
        provider_key="830.gaturest",
        flags=_FLAGS,
        coding={
            key: resolve_code_membership(()) for key in pair_columns if key is not None
        },
    )
    assert folded.variable is not None
    assert [(s.valid_from, s.delivery_column_name) for s in folded.variable.states] == [
        ("1999-01-01", "GatuRest"),
        ("2005-01-01", "Gaturest"),
    ]
    (warning,) = [
        d for d in folded.diagnostics if d.code == "source_native_identity_retained"
    ]
    assert warning.severity == "warning"
    assert not any(d.severity == "error" for d in folded.diagnostics)
    # The register-scoped declaration feeds the production entry point.
    declaration = tmp_path / "scb.toml"
    declaration.write_text(
        '[register."1"]\nslug = "fdb"\n'
        '[variable."1.830.gaturest"]\nslug = "gaturest"\n'
        '[variable."1.830.pgaturest"]\nslug = "pgaturest"\n',
        encoding="utf-8",
    )
    curation_dir = write_fdb_partition_curation(tmp_path / "curation")
    records = (
        *pair,
        _fdb_record(2010, column="GatuRest", variant=427),
        _fdb_record(2015, column="Gaturest", variant=427),
        _fdb_record(2020, column="PGaturest", variant=424),
        _fdb_record(2025, column="PGaturest", variant=427),
    )
    ownership = declared_column_ownership(
        load_provider_toml(declaration),
        provider="scb",
        source_id="1.830",
        curation_dir=curation_dir,
    )
    converted = convert_column_partitions(
        records,
        source_id="1.830",
        split_ids=("1.830.gaturest", "1.830.pgaturest"),
        declared_columns=dict(ownership.declared_columns),
        declaration_reference=ownership.declaration_reference,
    )
    assert converted.case is not None and converted.diagnostics == ()
    assert converted.case.decision.provenance.endswith(
        "column ownership: Y-167 fixture reference"
    )
    applied = apply_occurrence_cases(records, (converted.case,))
    assert applied.diagnostics == ()
    slugs = {"1.830.gaturest": "gaturest", "1.830.pgaturest": "pgaturest"}
    formed = {}
    for binding in converted.bindings:
        members = tuple(
            o
            for o in applied.occurrences
            if o.variable_key == binding.target.source_key
        )
        assert members
        group_variants = {}
        coding = {}
        for occurrence in members:
            assert occurrence.variant_key is not None
            last = occurrence.variant_key[-1]
            group_variants[occurrence.variant_key] = ResolvedVariant(
                slug="arbetsstalleenheter" if last == 424 else "juridiska-enheter",
                name="Arbetsställeenheter" if last == 424 else "Juridiska enheter",
            )
            assert occurrence.column_key is not None
            coding[occurrence.column_key] = resolve_code_membership(())
        result = form_native_variable(
            members,
            register=_REGISTER,
            variants=group_variants,
            slug=slugs[binding.source_id],
            provider_key=binding.source_id,
            flags=_FLAGS,
            coding=coding,
        )
        assert result.variable is not None and result.diagnostics == ()
        formed[binding.source_id] = result.variable
    assert {
        state.delivery_column_name for state in formed["1.830.gaturest"].states
    } == {"GatuRest", "Gaturest"}
    assert {
        state.delivery_column_name for state in formed["1.830.pgaturest"].states
    } == {"PGaturest"}
    assert sorted(
        (state.valid_from, state.valid_to) for state in formed["1.830.gaturest"].states
    ) == [
        ("1999-01-01", "1999-12-31"),
        ("2005-01-01", "2005-12-31"),
        ("2010-01-01", "2010-12-31"),
        ("2015-01-01", "2015-12-31"),
    ]
