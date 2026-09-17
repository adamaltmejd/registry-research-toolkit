"""Resolve literal export metadata without inventing catalog relationships."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta_build.resolved_metadata import (
    ResolvedIdentifierMetadata,
    ResolvedMetadata,
    ResolvedSourceColumn,
    ResolvedSourceJoinKey,
    ResolvedTimeseriesEvent,
)
from reg_meta_build.source_curation import ResolutionDiagnostic, SourceRecordRef
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_reference_records import (
    SourceColumnTypeDeclaration,
    SourceEventDeclaration,
    SourceJoinKeyDeclaration,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from reg_meta_build.source_records import SourceRecord

type ExportDeclaration = (
    SourceColumnTypeDeclaration | SourceEventDeclaration | SourceJoinKeyDeclaration
)


@dataclass(frozen=True)
class ReferenceMetadataResolution:
    metadata: ResolvedMetadata
    diagnostics: tuple[ResolutionDiagnostic, ...]
    withheld: tuple[SourceRecordRef, ...]


def resolve_identifier_metadata(
    records: Iterable[SourceRecord],
) -> ReferenceMetadataResolution:
    """Resolve explicitly selected native identifier descriptions, field by field.

    The selected source role supplies integer native IDs. This is reference
    metadata, not assignment of catalog identities or flags to other records.
    """
    grouped = defaultdict(list)
    for record in records:
        identifier = record.subject.variable.native_id
        if type(identifier) is not int or identifier < 1:
            raise ValueError("identifier metadata requires positive integer native IDs")
        grouped[identifier].append(record)
    result, diagnostics, withheld = [], [], set()
    for identifier, group in sorted(grouped.items()):
        fields = {}
        for name in ("name", "definition"):
            candidates = {
                field.value
                for record in group
                if (field := getattr(record.fields, name)) is not None
                and field.status == "value"
            }
            if any(not isinstance(value, str) for value in candidates):
                raise TypeError("identifier name and definition must be text")
            fields[name] = next(iter(candidates)) if len(candidates) == 1 else None
            if len(candidates) > 1:
                refs = tuple(sorted({record_ref(r) for r in group}, key=repr))
                withheld.update(refs)
                output = f"identifier:{identifier}:{name}"
                diagnostics.append(
                    ResolutionDiagnostic(
                        code="conflicting_identifier_metadata",
                        severity="error",
                        subject=output,
                        detail="Competing identifier descriptions require an explicit correction.",
                        refs=refs,
                        fields=(name,),
                        withheld_output=(output,),
                    )
                )
        result.append(
            ResolvedIdentifierMetadata(native_variable_id=identifier, **fields)
        )
    return ReferenceMetadataResolution(
        ResolvedMetadata(identifiers=tuple(result)),
        tuple(diagnostics),
        tuple(sorted(withheld, key=repr)),
    )


def resolve_export_metadata(
    declarations: Iterable[ExportDeclaration],
) -> ReferenceMetadataResolution:
    """Retain explicit schema/event facts, withholding disputed metadata only.

    Call over the complete selected export declarations. Equal repeated schema
    assertions coalesce; conflicting assertions never choose an input order winner.
    Events remain literal records and create no succession or identity decisions.
    Crosswalks and derivations require separate resolution and are not accepted here.
    The caller retains the original declarations and their physical locators.
    """
    columns: dict[
        tuple[str, str], dict[ResolvedSourceColumn, list[SourceRecordRef]]
    ] = defaultdict(lambda: defaultdict(list))
    keys: dict[tuple[str, str], dict[ResolvedSourceJoinKey, list[SourceRecordRef]]] = (
        defaultdict(lambda: defaultdict(list))
    )
    events = []
    diagnostics = []
    withheld = set()

    def issue(
        code: str, refs: tuple[SourceRecordRef, ...], detail: str, output: str
    ) -> None:
        withheld.update(refs)
        diagnostics.append(
            ResolutionDiagnostic(
                code=code,
                severity="error",
                subject=output,
                detail=detail,
                refs=tuple(sorted(set(refs), key=repr)),
                withheld_output=(output,),
            )
        )

    for declaration in declarations:
        ref = SourceRecordRef(
            source=declaration.revision.dataset,
            semantic_record_key=declaration.locator.semantic_record_key,
        )
        if isinstance(declaration, SourceEventDeclaration):
            data = {
                target: getattr(declaration, source).value
                if getattr(declaration, source).status == "value"
                else None
                for target, source in (
                    ("name", "name"),
                    ("event", "action_label"),
                    ("description", "description"),
                    ("entity", "entity_label"),
                )
            }
            for target, source in (
                ("first_token", "first_token"),
                ("second_token", "second_token"),
                ("file_token", "document_token"),
            ):
                cell = getattr(declaration, source)
                data[target] = (
                    cell.interpreted_value.strip() or None if cell.present else None
                )
            events.append(ResolvedTimeseriesEvent.model_validate(data))
            continue
        if not isinstance(
            declaration, SourceColumnTypeDeclaration | SourceJoinKeyDeclaration
        ):
            raise TypeError(
                "export metadata resolution requires schema or event declarations"
            )
        names = {
            name: field.value if field.status == "value" else None
            for name in ("table_name", "column_name")
            for field in (getattr(declaration, name),)
        }
        output = (
            "source_column_type"
            if isinstance(declaration, SourceColumnTypeDeclaration)
            else "source_join_key"
        )
        if any(value is None for value in names.values()):
            issue(
                "unknown_export_coordinate",
                (ref,),
                "Export metadata lacks its literal table or column name.",
                output,
            )
            continue
        if isinstance(declaration, SourceColumnTypeDeclaration):
            dtype, width, nullable = (
                declaration.data_type,
                declaration.declared_width,
                declaration.nullable,
            )
            if dtype.status != "value" or nullable.status != "value":
                issue(
                    "unknown_export_column_type",
                    (ref,),
                    "Export column type or nullability is unspecified; no default was inferred.",
                    output,
                )
                continue
            if not isinstance(dtype.value, str):
                raise TypeError("export data type must be text")
            sql_type = dtype.value
            if width.status == "value":
                if type(width.value) is not int or width.value < 1:
                    raise ValueError("declared export width must be a positive integer")
                sql_type += f"({width.value})"
            value = ResolvedSourceColumn.model_validate(
                {**names, "sql_type": sql_type, "nullable": nullable.value}
            )
            columns[value.table_name, value.column_name][value].append(ref)
        else:
            description = declaration.description
            key = ResolvedSourceJoinKey.model_validate(
                {
                    **names,
                    "description": description.value
                    if description.status == "value"
                    else None,
                }
            )
            keys[key.table_name, key.column_name][key].append(ref)

    resolved_columns = {}
    resolved_keys = []
    for coordinate, candidates in sorted(columns.items()):
        if len(candidates) != 1:
            issue(
                "conflicting_export_column_type",
                tuple(r for refs in candidates.values() for r in refs),
                f"Competing type/nullability declarations for {coordinate!r}: {tuple(candidates)!r}.",
                "source_column_type",
            )
        else:
            resolved_columns[coordinate] = next(iter(candidates))
    for coordinate, candidates in sorted(keys.items()):
        refs = tuple(r for group in candidates.values() for r in group)
        descriptions = {
            key.description for key in candidates if key.description is not None
        }
        if len(descriptions) > 1:
            issue(
                "conflicting_export_join_key",
                refs,
                f"Competing descriptions for {coordinate!r}: {tuple(candidates)!r}.",
                "source_join_key",
            )
        elif coordinate not in resolved_columns:
            issue(
                "unsupported_export_join_key",
                refs,
                f"Join-key column {coordinate!r} has no independently supported export column declaration.",
                "source_join_key",
            )
        else:
            description = next(iter(descriptions), None)
            resolved_keys.append(
                next(key for key in candidates if key.description == description)
            )
    return ReferenceMetadataResolution(
        ResolvedMetadata(
            source_columns=tuple(resolved_columns.values()),
            source_join_keys=tuple(resolved_keys),
            timeseries_events=tuple(events),
        ),
        tuple(diagnostics),
        tuple(sorted(withheld, key=repr)),
    )
