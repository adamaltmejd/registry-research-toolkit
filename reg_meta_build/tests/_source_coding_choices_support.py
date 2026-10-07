"""Shared synthetic records, list claims, choice cases and the compile entry for the coding-choice tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

from _csv_fixtures import SCB_REVISION, scb_record
from reg_meta_build.curation_compile import compile_coding_register
from reg_meta_build.curation_tree import RegisterCuration
from reg_meta_build.pipeline import CompiledScope
from reg_meta_build.source_coding import (
    CodeListClaim,
    CodeMembershipClaim,
    coding_content_sha256,
)
from reg_meta_build.source_coding_choices import apply_coding_choices, coding_for_period
from reg_meta_build.source_coordinates import (
    NativeKey,
    column_identity,
    source_register_key,
)
from reg_meta_build.source_curation import (
    CodingDecision,
    CodingSelection,
    CurationCase,
    PeerGuard,
    capture_expectations,
)
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_naming import NamingDeclaration, NativeNamingTarget
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import (
    NativeCoordinates,
    ScopeInterval,
    SourceFields,
    SourceRecord,
    TemporalScope,
)

from reg_meta_build.fqid_slugs import SlugEntry

if TYPE_CHECKING:
    from collections.abc import Mapping


def column_scopes(
    columns: Mapping[NativeKey, tuple[SourceRecord, ...]],
) -> dict[NativeKey, frozenset[TemporalScope]]:
    return {
        key: frozenset(
            record.edition_period_scope
            if record.edition_period_scope.kind != "not_applicable"
            else record.edition_scope
            for record in records
        )
        for key, records in columns.items()
    }


def choice_record(
    year: int = 2020, column: str = "VALUE", *, variable: int = 5
) -> SourceRecord:
    return scb_record(
        cvid=year,
        var_id=variable,
        colname=column,
        register=("TEST", 1, 2),
        regver_id=year,
        year=str(year),
    )


def list_claim(
    name: str, code: str, start: str = "2020-01-01", end: str = "2020-12-31"
) -> CodeListClaim:
    return CodeListClaim(
        name,
        TemporalScope(
            kind="intervals", intervals=(ScopeInterval(start=start, end=end),)
        ),
        (CodeMembershipClaim(code, "Label", TemporalScope(kind="year_independent")),),
        version_label=name,
    )


def choice_case(
    record: SourceRecord,
    claims: tuple[CodeListClaim, ...],
    *,
    start: str = "2020-01-01",
    end: str = "2020-12-31",
    selected: int = 0,
    name: str = "accepted",
) -> CurationCase:
    projected = coding_for_period(claims, start, end)
    digests = []
    for claim in projected:
        digest = coding_content_sha256(claim)
        assert digest is not None
        digests.append(digest)
    assert digests
    column = source_occurrence(record).column_key
    assert column is not None
    return CurationCase(
        case_id=name,
        targets=capture_expectations((record,), fields=("column_name",)),
        peer_guards=(
            PeerGuard(
                guard_id=name,
                source=record.source,
                native=NativeCoordinates(register_id=1, variable_id=5),
                edition_scopes=(record.edition_scope,),
                expected_members=(record_ref(record),),
            ),
        ),
        decision=CodingDecision(
            reviewed=True,
            column_key=column,
            valid_from=start,
            valid_to=end,
            expected_codings=tuple(sorted(set(digests))),
            selection=CodingSelection(
                valid_from=start,
                valid_to=end,
                expected_codings=tuple(sorted(set(digests))),
                selected_coding=digests[selected],
            ),
            reason="Existing accepted list selection",
            provenance="accepted.toml entry 1",
        ),
    )


def apply_choices(
    record: SourceRecord, claims: tuple[CodeListClaim, ...], *cases: CurationCase
):
    key = source_occurrence(record).column_key
    assert key is not None
    return apply_coding_choices((record,), cases, coding={key: claims})


def compile_entry(
    kind: str,
    values: dict,
    claims: tuple[CodeListClaim, ...],
    *,
    record: SourceRecord | None = None,
    split: bool = False,
):
    record = record or choice_record()
    register_key = source_register_key(record)
    occurrence = source_occurrence(record)
    assert register_key is not None
    assert occurrence.variable_key is not None and occurrence.variant_key is not None
    variable = (
        (*occurrence.variable_key, "accepted-partition", "1.5.part")
        if split
        else occurrence.variable_key
    )
    column = column_identity(variable, occurrence.variant_key, "VALUE")
    scope = CompiledScope(
        source=record.source,
        register_key=register_key,
        naming=(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="register",
                    provider="scb",
                    source_key=register_key,
                ),
                naming=SlugEntry("register", "1", "sample", "scb"),
                contributors=(),
            ),
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="register_variant",
                    provider="scb",
                    source_key=occurrence.variant_key,
                    register_key=register_key,
                ),
                naming=SlugEntry("register_variant", "1.2", "people", "scb"),
                contributors=(),
            ),
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="variable",
                    provider="scb",
                    source_key=variable,
                    register_key=register_key,
                ),
                naming=SlugEntry(
                    "variable", "1.5.part" if split else "1.5", "value", "scb"
                ),
                contributors=(),
            ),
        ),
    )
    register = RegisterCuration.model_validate(
        {
            "register": {"provider": "scb", "slug": "sample", "native_id": "1"},
            "coding": {
                kind: [
                    {
                        "variable": "1.5",
                        "variant": "people",
                        "column": "VALUE",
                        "periods": [["2020-01-01", "2020-12-31"]],
                        "reason": "Reviewed coding",
                        "source": "fixture",
                        **values,
                    }
                ]
            },
        }
    )
    register._source_file = "curation/registers/scb/sample.toml"
    columns = {column: (record,)}
    cases, diagnostics = compile_coding_register(
        register,
        scope,
        originals=(record,),
        columns=columns,
        column_scopes=column_scopes(columns),
        coding={column: claims},
    )
    return cases, diagnostics, register, scope, columns, column


def documented_values(members=None):
    return {
        "members": members
        if members is not None
        else [["1", "ja"], ["", "inte tillfrågad"]],
        "version_label": "Official 2020 questionnaire",
        "document_url": "https://example.org/official.pdf",
        "document_sha256": "a" * 64,
        "document_pages": [12],
    }


def row_authority(record, claims, source_scope=None):
    from reg_meta_build.curation_tree import PreparedCodingAuthority
    from reg_meta_build.source_coding import copied_coding_fingerprints

    return PreparedCodingAuthority(
        revision=SCB_REVISION,
        source_scope=source_scope,
        locators=list(record.locators),
        records=list(
            capture_expectations(
                (record,),
                fields=tuple(SourceFields.model_fields),
                parents=True,
                coding=True,
            )
        ),
        codings=list(copied_coding_fingerprints(claims)),
    )
