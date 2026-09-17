"""Source-faithful variable records from parsed Socialstyrelsen workbooks.

This boundary performs only source-format cleaning. It does not invent a missing
deldatamängd, group same-name rows, choose a code list, or assign catalog identity.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from datetime import date, datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Literal, overload

from reg_meta_build.normalization import normalize_text, normalize_token
from reg_meta_build.source_records import (
    DeliveredCell,
    NativeCoordinates,
    RecordLocator,
    ScopeInterval,
    SourceCoordinate,
    SourceEvidenceRow,
    SourceEvidenceTable,
    SourceField,
    SourceFieldCells,
    SourceFields,
    SourceParentObservation,
    SourceRecord,
    SourceSubject,
    TemporalScope,
    canonical_sha256,
    value_field,
)
from reg_meta_build.source_reference_records import (
    SourceCodeCrosswalkDeclaration,
    SourceCodeOperand,
    SourceDerivationClause,
    SourceDerivationDeclaration,
)
from reg_meta_build.source_value_periods import value_period, value_window
from reg_meta_build.source_values import (
    SourceMemberHint,
    SourceValue,
    SourceValueAssociation,
    SourceValueDescriptor,
    SourceValueValidity,
    SourceValueWindow,
)
from reg_meta_build.sources.sos import _classify_value_set_text

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from reg_meta_build.source_records import FieldScalar, SourceRevision
    from reg_meta_build.source_reference_records import SourceReferenceDeclaration
    from reg_meta_build.sources.sos import (
        SosCellEvidence,
        SosEvidenceRole,
        SosRegister,
        SosRowEvidence,
        SosVariable,
    )


_DATA_TYPES = {
    "datum": "date",
    "heltal": "integer",
    "sträng (text)": "text",
}


def _cell(evidence: SosRowEvidence | None, field_name: str) -> SosCellEvidence | None:
    if evidence is None:
        return None
    return next(
        (cell for cell in evidence.cells if cell.field_name == field_name),
        None,
    )


def _raw_scalar(cell: SosCellEvidence | None) -> FieldScalar | None:
    if cell is None or cell.raw_value is None:
        return None
    if isinstance(cell.raw_value, (bool, int, str)):
        return cell.raw_value
    return cell.display_value


@overload
def _text_field(
    cell: SosCellEvidence,
    *,
    token: bool = False,
    multiline: bool = False,
) -> SourceField: ...


@overload
def _text_field(
    cell: SosCellEvidence | None,
    *,
    token: bool = False,
    multiline: bool = False,
) -> SourceField | None: ...


def _text_field(
    cell: SosCellEvidence | None,
    *,
    token: bool = False,
    multiline: bool = False,
) -> SourceField | None:
    if cell is None:
        return None
    raw = _raw_scalar(cell)
    if cell.data_type == "f":
        return SourceField(status="unknown", raw_value=raw)
    value = cell.raw_value if isinstance(cell.raw_value, str) else cell.display_value
    if isinstance(cell.raw_value, (date, datetime)):
        value = cell.raw_value.isoformat()
    if value is None:
        return SourceField(status="unknown", raw_value=raw)
    normalized = (
        normalize_token(value) if token else normalize_text(value, multiline=multiline)
    )
    if not normalized:
        return SourceField(status="unknown", raw_value=raw)
    return value_field(normalized, raw=raw)


def _data_type_field(evidence: SosRowEvidence) -> SourceField | None:
    cell = _cell(evidence, "data_type")
    field = _text_field(cell, token=True)
    if field is None or field.status != "value" or not isinstance(field.value, str):
        return field
    normalized = _DATA_TYPES.get(field.value.casefold(), field.value)
    return SourceField(
        status="value",
        value=normalized,
        raw_value=field.raw_value,
    )


def _coverage_scope(evidence: SosRowEvidence) -> TemporalScope:
    from_cell = _cell(evidence, "data_from")
    to_cell = _cell(evidence, "data_to")
    if from_cell is None and to_cell is None:
        return TemporalScope(kind="not_applicable")

    def boundary(cell: SosCellEvidence | None) -> str | None:
        if cell is None or cell.data_type == "f":
            return None
        if isinstance(cell.raw_value, datetime):
            if cell.raw_value.time() != datetime.min.time():
                return None
            return cell.raw_value.date().isoformat()
        if isinstance(cell.raw_value, date):
            return cell.raw_value.isoformat()
        field = _text_field(cell, token=True)
        if field is None or field.status != "value" or not isinstance(field.value, str):
            return None
        return field.value

    start = boundary(from_cell)
    end = boundary(to_cell)
    # Unlike code validity, blank variable coverage has no established open-bound
    # meaning. Interpret only two supplied bounds using the actual date formats.
    if start is not None and end is not None:
        window = value_window(start, end, compact_dates=True)
        if (
            window.status == "known"
            and window.start is not None
            and window.end is not None
        ):
            years_only = all(re.fullmatch(r"[0-9]{4}", bound) for bound in (start, end))
            return TemporalScope(
                kind="intervals",
                intervals=(
                    ScopeInterval(
                        start=start if years_only else window.start,
                        end=end if years_only else window.end,
                    ),
                ),
            )

    def shown(cell: SosCellEvidence | None) -> str:
        if cell is None:
            return "<not delivered>"
        return cell.display_value if cell.display_value is not None else "<blank>"

    return TemporalScope(
        kind="unknown",
        label=f"Data från={shown(from_cell)}; Data till={shown(to_cell)}",
    )


def _delivered_cells(evidence: SosRowEvidence | None) -> tuple[DeliveredCell, ...]:
    if evidence is None:
        return ()
    return tuple(
        DeliveredCell(
            name=cell.header,
            present=True,
            raw_value=("" if cell.raw_value is None else str(cell.raw_value)),
            interpreted_value=cell.display_value or "",
            raw_type=(
                type(cell.raw_value).__name__ if cell.raw_value is not None else "none"
            ),
            storage_type=cell.data_type,
            number_format=cell.number_format,
            hyperlink_target=cell.hyperlink_target,
            hyperlink_location=cell.hyperlink_location,
            cached_raw_value=(
                "" if cell.cached_raw_value is None else str(cell.cached_raw_value)
            )
            if cell.cached_raw_type is not None
            else None,
            cached_raw_type=cell.cached_raw_type,
        )
        for cell in evidence.cells
    )


def _register_name(register: SosRegister) -> str | None:
    names = {
        normalize_text(cell.display_value)
        for sheet in register.source_sheets
        if sheet.kind == "general"
        for row in sheet.rows
        for cell in row.source_evidence.cells
        if cell.field_name == "dataset_name" and cell.display_value
    }
    return next(iter(names)) if len(names) == 1 else None


def _locator(
    evidence: SosRowEvidence,
    revision: SourceRevision,
    semantic_key: tuple[str, ...],
) -> RecordLocator:
    return RecordLocator(
        semantic_record_key=semantic_key,
        physical_file=revision.artifact_path,
        physical_table=evidence.sheet_name,
        physical_record=f"row:{evidence.row_number}",
        physical_cells=tuple(
            f"{evidence.sheet_name}!{cell.coordinate}" for cell in evidence.cells
        ),
    )


def clean_sos_variable(
    register: SosRegister,
    variable: SosVariable,
    revision: SourceRevision,
) -> SourceRecord:
    """Normalize one delivered variable row without resolving SOS semantics."""

    evidence = variable.source_evidence
    if evidence is None:
        raise ValueError("SOS source records require parser row evidence")

    register_name = _register_name(register)
    variant_name = (
        normalize_token(variable.deldatamangd) if variable.deldatamangd else None
    )
    member_name = normalize_token(variable.name)
    label = _text_field(_cell(evidence, "label"))
    semantic_key = (
        f"register:{register_name or '<unknown>'}",
        f"deldatamangd:{variant_name or '<unknown>'}",
        f"variable:{member_name}",
    )
    return SourceRecord.create(
        revision=revision,
        locators=(_locator(evidence, revision, semantic_key),),
        subject=SourceSubject(
            provider="sos",
            register=(
                SourceCoordinate(status="value", name=register_name)
                if register_name
                else SourceCoordinate(status="unknown")
            ),
            variant=(
                SourceCoordinate(status="value", name=variant_name)
                if variant_name
                else SourceCoordinate(status="unknown")
            ),
            population=SourceCoordinate(status="unknown"),
            variable=SourceCoordinate(
                status="value",
                native_id=member_name,
                name=label.value
                if label is not None and isinstance(label.value, str)
                else None,
            ),
            member=SourceCoordinate(status="value", name=member_name),
            native=NativeCoordinates(),
        ),
        edition_scope=_coverage_scope(evidence),
        edition_period_scope=TemporalScope(kind="not_applicable"),
        fields=SourceFields(
            availability=value_field(True),
            column_name=_text_field(
                _cell(evidence, "name"),
                token=True,
            ),
            name=_text_field(_cell(evidence, "label")),
            description=_text_field(
                _cell(evidence, "description"),
                multiline=True,
            ),
            data_type=_data_type_field(evidence),
            coverage_from=_text_field(_cell(evidence, "data_from"), token=True),
            coverage_to=_text_field(_cell(evidence, "data_to"), token=True),
            representation=_text_field(
                _cell(evidence, "value_set_text"),
                multiline=True,
            ),
            classification_declared=_text_field(
                _cell(evidence, "external_classification"), multiline=True
            ),
            source_attribution=_text_field(
                _cell(evidence, "source_detail"),
                multiline=True,
            ),
        ),
        delivered_cells=_delivered_cells(evidence),
    )


def iter_sos_variable_records(
    register: SosRegister,
    revision: SourceRevision,
) -> Iterator[SourceRecord]:
    """Yield every physical SOS variable occurrence in workbook order."""

    for variable in register.variables:
        yield clean_sos_variable(register, variable, revision)


_GENERAL_FIELDS = {
    "dataset_name": "name",
    "dataset_version": "source_version",
    "dataset_date": "source_date",
    "contact_email": "contact",
}
_DCAT_FIELDS = {
    "title": "name",
    "description": "description",
    "temporal_coverage": "coverage",
    "geographic_coverage": "geographic_coverage",
    "population": "population_definition",
    "update_frequency": "update_frequency",
    "publisher": "source_attribution",
    "contact": "contact",
    "documentation_url": "documentation_url",
    "landing_page": "landing_page",
    "access_url": "access_url",
    "access_rights": "access_rights",
    "legislation": "legislation",
}
_SUBSET_FIELDS = {
    "label": "name",
    "description": "description",
    "data_from": "coverage_from",
    "data_to": "coverage_to",
    "update_frequency": "update_frequency",
    "aggregation_level": "aggregation_level",
}
_PARAGRAPH_FIELDS = {"description", "population_definition", "legislation"}


def _metadata_record(
    register: SosRegister,
    revision: SourceRevision,
    evidence: SosRowEvidence,
    fields: dict[str, SourceField],
    field_cells: tuple[SourceFieldCells, ...],
    *,
    kind: str,
    language: Literal["sv", "en"] | None = None,
) -> SourceRecord:
    register_name = _register_name(register)
    subset = _cell(evidence, "name") if kind == "subsets" else None
    subset_name = (
        normalize_token(subset.display_value)
        if subset and subset.display_value
        else None
    )
    semantic_key = (
        f"register:{register_name or '<unknown>'}",
        f"metadata:{kind}",
        f"subset:{subset_name or '<not applicable>'}",
        f"fields:{','.join(sorted(fields))}",
        f"language:{language or '<not declared>'}",
    )
    register_coordinate = (
        SourceCoordinate(status="value", name=register_name)
        if register_name
        else SourceCoordinate(status="unknown")
    )
    variant_coordinate = (
        SourceCoordinate(status="value", name=subset_name)
        if subset_name
        else SourceCoordinate(
            status="unknown" if kind == "subsets" else "not_applicable"
        )
    )
    return SourceRecord.create(
        revision=revision,
        locators=(_locator(evidence, revision, semantic_key),),
        subject=SourceSubject(
            provider="sos",
            register=register_coordinate,
            variant=variant_coordinate,
            population=SourceCoordinate(status="unknown"),
            variable=SourceCoordinate(status="not_applicable"),
            member=SourceCoordinate(status="not_applicable"),
            native=NativeCoordinates(),
        ),
        edition_scope=(
            _coverage_scope(evidence)
            if kind == "subsets"
            else TemporalScope(kind="not_applicable")
        ),
        edition_period_scope=TemporalScope(kind="not_applicable"),
        fields=SourceFields(),
        parent_facts=(
            SourceParentObservation(
                kind="variant" if kind == "subsets" else "register",
                coordinate=variant_coordinate
                if kind == "subsets"
                else register_coordinate,
                register=register_coordinate,
                variant=variant_coordinate if kind == "subsets" else None,
                fields=SourceFields.model_validate(fields),
                field_cells=field_cells,
            ),
        ),
        language=language,
        delivered_cells=_delivered_cells(evidence),
    )


def iter_sos_metadata_records(
    register: SosRegister, revision: SourceRevision
) -> Iterator[SourceRecord]:
    """Keep repeated parent claims and explicitly supplied languages separate."""
    for sheet in register.source_sheets:
        if sheet.kind not in {"general", "dcat", "subsets"}:
            continue
        for row in sheet.rows:
            if row.role not in {"metadata", "attribute", "subset"}:
                continue
            evidence = row.source_evidence
            if sheet.kind == "dcat":
                for position, cell in enumerate(evidence.cells):
                    if cell.language is None or cell.field_name is None:
                        continue
                    stem = cell.field_name.removesuffix(f"_{cell.language}")
                    field = _DCAT_FIELDS.get(stem)
                    if field is None:
                        continue
                    cleaned = _text_field(cell, multiline=field in _PARAGRAPH_FIELDS)
                    if cleaned is not None:
                        yield _metadata_record(
                            register,
                            revision,
                            evidence,
                            {field: cleaned},
                            (SourceFieldCells(field=field, positions=(position,)),),
                            kind=sheet.kind,
                            language=cell.language,
                        )
            else:
                mapping = _GENERAL_FIELDS if sheet.kind == "general" else _SUBSET_FIELDS
                fields = {}
                field_cells = []
                for source_field, field in mapping.items():
                    position = next(
                        (
                            index
                            for index, cell in enumerate(evidence.cells)
                            if cell.field_name == source_field
                        ),
                        None,
                    )
                    if position is not None:
                        fields[field] = _text_field(
                            evidence.cells[position],
                            multiline=field in _PARAGRAPH_FIELDS,
                        )
                        field_cells.append(
                            SourceFieldCells(field=field, positions=(position,))
                        )
                if fields:
                    yield _metadata_record(
                        register,
                        revision,
                        evidence,
                        fields,
                        tuple(field_cells),
                        kind=sheet.kind,
                    )


_ROW_ROLES: dict[
    SosEvidenceRole,
    Literal["header", "section", "declaration", "data", "unparsed", "note"],
] = {
    "header": "header",
    "section": "section",
    "metadata": "declaration",
    "attribute": "declaration",
    "subset": "data",
    "variable": "data",
    "preamble": "declaration",
    "period_section": "section",
    "code": "data",
    "crosswalk": "declaration",
    "derivation": "declaration",
    "raw": "unparsed",
    "quality": "note",
}


def _source_tables(
    register: SosRegister, revision: SourceRevision
) -> tuple[SourceEvidenceTable, ...]:
    return tuple(
        SourceEvidenceTable(
            source=revision.dataset,
            source_revision_id=revision.revision_id,
            name=sheet.sheet_name,
            rows=tuple(
                SourceEvidenceRow(
                    locator=_locator(
                        row.source_evidence, revision, (f"table:{sheet.sheet_name}",)
                    ),
                    role=_ROW_ROLES[row.role],
                    cells=_delivered_cells(row.source_evidence),
                )
                for row in sheet.rows
            ),
        )
        for sheet in register.source_sheets
    )


@dataclass(frozen=True)
class CleanedSosSource:
    revision: SourceRevision
    records: tuple[SourceRecord, ...]
    tables: tuple[SourceEvidenceTable, ...]
    descriptors: Mapping[str, SourceValueDescriptor]
    values: Mapping[str, SourceValue]
    associations: tuple[SourceValueAssociation, ...]
    validity: tuple[SourceValueValidity, ...]
    declarations: tuple[SourceReferenceDeclaration, ...]


def _declaration_text(
    cell: SosCellEvidence | None, *, token: bool = False
) -> str | None:
    if cell is None or cell.raw_value is None or cell.data_type == "f":
        return None
    return (normalize_token if token else normalize_text)(str(cell.raw_value))


def _period_window(cell: SosCellEvidence | None) -> SourceValueWindow | None:
    if cell is not None and cell.data_type == "f":
        return SourceValueWindow("unknown")
    return value_period(_declaration_text(cell))


def clean_sos_source(
    register: SosRegister, revision: SourceRevision
) -> CleanedSosSource:
    """Prepare common observations; retain every list occurrence without bindings."""
    if register.parse_issues:
        detail = "; ".join(
            f"{issue.sheet_name}: {issue.detail}" for issue in register.parse_issues
        )
        raise ValueError(f"SOS source cleaning blocked by parser failures: {detail}")
    if not register.source_sheets:
        raise ValueError("SOS source cleaning requires original sheet evidence")
    descriptors: dict[str, SourceValueDescriptor] = {}
    values: dict[str, SourceValue] = {}
    associations: list[SourceValueAssociation] = []
    validity: list[SourceValueValidity] = []
    references: list[SourceReferenceDeclaration] = []
    for sheet in register.source_sheets:
        if sheet.kind != "codelist":
            continue
        descriptor_key = f"sheet:{sheet.sheet_name}"
        suffix = sheet.sheet_name.split("_", 1)[-1].split("!", 1)[0].strip()
        hints = [SourceMemberHint(role="sheet_suffix", value=normalize_token(suffix))]
        declarations: list[DeliveredCell] = []
        declaration_locators: list[RecordLocator] = []
        section_period: str | None = None
        section_period_field: SourceField | None = None
        section_window: SourceValueWindow | None = None
        section_locator: RecordLocator | None = None
        for row in sheet.rows:
            evidence = row.source_evidence
            locator = _locator(evidence, revision, (descriptor_key,))
            if row.role in {"preamble", "header"}:
                declarations.extend(_delivered_cells(evidence))
                declaration_locators.append(locator)
                member = _cell(evidence, "variable_header")
                if member is not None:
                    hints.append(
                        SourceMemberHint(
                            role="list_header",
                            value=_declaration_text(member, token=True),
                            locator=locator,
                        )
                    )
            if row.role == "period_section":
                section_period = _declaration_text(_cell(evidence, "tidsperiod"))
                section_period_field = _text_field(_cell(evidence, "tidsperiod"))
                section_window = _period_window(_cell(evidence, "tidsperiod"))
                section_locator = locator
            if row.role == "crosswalk":
                operand_roles: dict[str, Literal["input", "output", "peer"]] = {
                    "input_code": "input",
                    "output_code": "output",
                    "peer_code": "peer",
                }
                references.append(
                    SourceCodeCrosswalkDeclaration(
                        revision=revision,
                        locator=locator,
                        delivered_cells=_delivered_cells(evidence),
                        member_name=_text_field(
                            _cell(evidence, "variable_name"), token=True
                        ),
                        supplied_period=_text_field(_cell(evidence, "tidsperiod")),
                        section_period=section_period_field,
                        section_locator=section_locator,
                        description=_text_field(_cell(evidence, "beskrivning")),
                        operands=tuple(
                            SourceCodeOperand(
                                role=operand_roles[cell.field_name],
                                name=normalize_text(cell.header),
                                code=_text_field(cell, token=True),
                            )
                            for cell in evidence.cells
                            if cell.field_name in operand_roles
                        ),
                    )
                )
            elif row.role == "derivation":
                references.append(
                    SourceDerivationDeclaration(
                        revision=revision,
                        locator=locator,
                        delivered_cells=_delivered_cells(evidence),
                        member_name=_text_field(
                            _cell(evidence, "variable_name"), token=True
                        ),
                        supplied_period=_text_field(_cell(evidence, "tidsperiod")),
                        description=_text_field(_cell(evidence, "beskrivning")),
                        clauses=tuple(
                            SourceDerivationClause(
                                name=normalize_text(cell.header),
                                content=_text_field(cell, multiline=True),
                            )
                            for cell in evidence.cells
                            if cell.field_name == "clause"
                        ),
                    )
                )
            if row.role != "code":
                continue
            code = _cell(evidence, "kod")
            label = _cell(evidence, "beskrivning")
            if code is None or not code.display_value:
                raise ValueError(
                    f"{sheet.sheet_name} row {evidence.row_number}: structured code lacks source code cell"
                )
            selected = tuple(
                cell
                for cell in evidence.cells
                if cell.field_name in {"kod", "beskrivning"}
            )
            payload = replace(evidence, cells=selected)
            delivered = _delivered_cells(payload)
            key = canonical_sha256([cell.model_dump(mode="json") for cell in delivered])
            occurrence_locator = _locator(
                evidence, revision, (descriptor_key, f"value:{key}")
            )
            if key in values:
                values[key] = replace(
                    values[key], locators=(*values[key].locators, occurrence_locator)
                )
            else:
                values[key] = SourceValue(
                    payload_key=key,
                    code=normalize_token(code.display_value),
                    label=(_declaration_text(label) if label is not None else None),
                    locators=(occurrence_locator,),
                    delivered_cells=delivered,
                )
            member = _cell(evidence, "variable_name")
            row_hints = (
                (
                    SourceMemberHint(
                        role="row",
                        value=_declaration_text(member, token=True),
                        locator=locator,
                    ),
                )
                if member is not None
                else ()
            )
            associations.append(
                SourceValueAssociation(
                    row_number=evidence.row_number,
                    descriptor_key=descriptor_key,
                    value_key=key,
                    source_file=revision.artifact_path,
                    source_table=sheet.sheet_name,
                    member_hints=row_hints,
                    member_references=tuple(
                        hint.value for hint in row_hints if hint.value is not None
                    ),
                    supplied_window=_period_window(_cell(evidence, "tidsperiod")),
                    section_window=section_window,
                    supplied_period=_declaration_text(_cell(evidence, "tidsperiod")),
                    section_period=section_period,
                    section_locator=section_locator,
                    delivered_cells=_delivered_cells(evidence),
                )
            )
            if (
                _cell(evidence, "valid_from") is not None
                or _cell(evidence, "valid_to") is not None
            ):
                validity.append(
                    SourceValueValidity(
                        row_number=evidence.row_number,
                        item_id=None,
                        valid_from=_declaration_text(
                            _cell(evidence, "valid_from"), token=True
                        ),
                        valid_to=_declaration_text(
                            _cell(evidence, "valid_to"), token=True
                        ),
                        source_file=revision.artifact_path,
                        source_table=sheet.sheet_name,
                        locators=(occurrence_locator,),
                        delivered_cells=_delivered_cells(evidence),
                        window=SourceValueWindow("unknown")
                        if any(
                            cell is not None and cell.data_type == "f"
                            for cell in (
                                _cell(evidence, "valid_from"),
                                _cell(evidence, "valid_to"),
                            )
                        )
                        else value_window(
                            _declaration_text(
                                _cell(evidence, "valid_from"), token=True
                            ),
                            _declaration_text(_cell(evidence, "valid_to"), token=True),
                            compact_dates=True,
                        ),
                    )
                )
        descriptors[descriptor_key] = SourceValueDescriptor(
            payload_key=descriptor_key,
            name=normalize_text(sheet.sheet_name),
            member_hints=tuple(hints),
            member_references=tuple(
                hint.value
                for hint in hints
                if hint.role == "list_header" and hint.value is not None
            ),
            locators=tuple(declaration_locators),
            delivered_cells=tuple(declarations),
        )
    records = (
        *iter_sos_metadata_records(register, revision),
        *iter_sos_variable_records(register, revision),
    )
    for record in records:
        representation = record.fields.representation
        if representation is None or not isinstance(representation.value, str):
            continue
        pairs = _classify_value_set_text(representation.value)
        if pairs is None:
            continue
        descriptor_key = f"inline:{record.record_id}"
        descriptors[descriptor_key] = SourceValueDescriptor(
            payload_key=descriptor_key,
            record_ids=(record.record_id,),
            locators=record.locators,
            delivered_cells=record.delivered_cells,
        )
        locator = record.locators[0]
        for code, label in pairs:
            key = canonical_sha256([code, label, record.record_id])
            values[key] = SourceValue(
                key,
                normalize_token(code),
                normalize_text(label) if label is not None else None,
                locators=record.locators,
                delivered_cells=record.delivered_cells,
            )
            associations.append(
                SourceValueAssociation(
                    row_number=int(locator.physical_record.removeprefix("row:")),
                    descriptor_key=descriptor_key,
                    value_key=key,
                    source_file=locator.physical_file,
                    source_table=locator.physical_table,
                    delivered_cells=record.delivered_cells,
                )
            )
    return CleanedSosSource(
        revision=revision,
        records=records,
        tables=_source_tables(register, revision),
        descriptors=MappingProxyType(descriptors),
        values=MappingProxyType(values),
        associations=tuple(associations),
        validity=tuple(validity),
        declarations=tuple(references),
    )


__all__ = [
    "CleanedSosSource",
    "clean_sos_source",
    "clean_sos_variable",
    "iter_sos_metadata_records",
    "iter_sos_variable_records",
]
