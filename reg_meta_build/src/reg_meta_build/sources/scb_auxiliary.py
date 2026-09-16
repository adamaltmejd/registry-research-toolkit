"""Source declarations from SCB's variable summary and identifier exports.

Summary rows have name-based coordinates, not native variable IDs. Their period
tokens and flags remain separate assertions; joining them to occurrences and
reconciling repeated or conflicting assertions belongs to shared curation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from reg_meta_build.db import _open_scb_csv_prepared
from reg_meta_build.normalization import normalize_text, normalize_token
from reg_meta_build.source_records import (
    NativeCoordinates,
    RecordLocator,
    SourceCoordinate,
    SourceField,
    SourceFields,
    SourceRecord,
    SourceRevision,
    SourceSubject,
    TemporalScope,
    canonical_sha256,
    value_field,
)
from reg_meta_build.source_support import SourceSupportJoin
from reg_meta_build.sources.scb_records import (
    _cell_field,
    _delivered_cells,
    _scb_native_id,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from reg_meta_build.input_snapshot import ScbSnapshotReader

type SourceCells = Mapping[str, tuple[bool, str | None, str]]

_UNIKA = "UnikaRegisterOchVariabler.csv"
_IDENTIFIERS = "Identifierare.csv"


def scb_support_joins(sources: dict[str, str]) -> tuple[SourceSupportJoin, ...]:
    """Describe only the relationships present in the selected SCB export."""
    target = sources.get("Registerinformation.csv")
    if target is None:
        return ()
    result = []
    if source := sources.get(_UNIKA):
        result.append(
            SourceSupportJoin(
                source=source,
                target_sources=(target,),
                keys=("register_name", "variant_name", "variable_name", "column_name"),
                fields=("identifier", "sensitivity", "conditional_sensitivity"),
                unique_variable=True,
                rule="Literal register, variant, variable and column names identify the summary's native variable. Ambiguous matches supply no flags.",
                provenance=(
                    "UnikaRegisterOchVariabler.csv: Registernamn, Registervariantnamn, Variabelnamn, Kolumnnamn",
                ),
            )
        )
    if source := sources.get(_IDENTIFIERS):
        result.append(
            SourceSupportJoin(
                source=source,
                target_sources=(target,),
                keys=("variable_id",),
                fields=("identifier",),
                unique_variable=False,
                rule="VarID is source-wide; an identifier declaration applies to that native variable across the export's registers.",
                provenance=(
                    "Identifierare.csv: VarID -> Registerinformation.csv: VarId",
                ),
            )
        )
    return tuple(result)


def _named(value: str) -> SourceCoordinate:
    name = normalize_text(value)
    return (
        SourceCoordinate(status="value", name=name)
        if name
        else SourceCoordinate(status="unknown")
    )


def _boolean(cell: tuple[bool, str | None, str]) -> SourceField:
    present, raw, interpreted = cell
    token = normalize_token(interpreted)
    if present and token in {"0", "1"}:
        return value_field(token == "1", raw=raw)
    return SourceField(status="unknown", raw_value=raw)


def _locator(
    filename: str,
    row: int,
    header: Sequence[str],
    semantic_key: tuple[str, ...],
) -> RecordLocator:
    return RecordLocator(
        semantic_record_key=semantic_key,
        physical_file=filename,
        physical_table=filename,
        physical_record=f"row:{row}",
        physical_cells=tuple(f"row:{row}:{name}" for name in header),
    )


def clean_unika_row(
    header: Sequence[str], row: int, cells: SourceCells, revision: SourceRevision
) -> SourceRecord:
    """Preserve one summary declaration, without OR-ing flags or choosing bounds."""
    register = _named(cells["Registernamn"][2])
    variant = _named(cells["Registervariantnamn"][2])
    variable = _named(cells["Variabelnamn"][2])
    column = _cell_field(cells["Kolumnnamn"])
    # Typed field values avoid delimiter collisions in provider names. Flags
    # stay outside the key so conflicting declarations retain one semantic member.
    key = canonical_sha256(
        {
            "register": register.model_dump(mode="json"),
            "variant": variant.model_dump(mode="json"),
            "variable": variable.model_dump(mode="json"),
            "column": None if column is None else (column.status, column.value),
            "first": normalize_token(cells["VersionForsta"][2]),
            "last": normalize_token(cells["VersionSista"][2]),
        }
    )
    return SourceRecord.create(
        revision=revision,
        locators=(_locator(_UNIKA, row, header, ("summary", key)),),
        subject=SourceSubject(
            provider="scb",
            register=register,
            variant=variant,
            variable=variable,
            population=SourceCoordinate(status="unknown"),
            member=variable,
            native=NativeCoordinates(),
        ),
        edition_scope=TemporalScope(kind="not_applicable"),
        edition_period_scope=TemporalScope(kind="not_applicable"),
        fields=SourceFields(
            name=_cell_field(cells["Variabelnamn"], label=True),
            column_name=column,
            coverage_from=_cell_field(cells["VersionForsta"]),
            coverage_to=_cell_field(cells["VersionSista"]),
            sensitivity=_boolean(cells["KansligVariabel"]),
            conditional_sensitivity=_boolean(cells["KansligVariabelIbland"]),
            identifier=_boolean(cells["Identitetsvariabel"]),
        ),
        delivered_cells=_delivered_cells(cells),
    )


def clean_identifier_row(
    header: Sequence[str], row: int, cells: SourceCells, revision: SourceRevision
) -> SourceRecord:
    """A source-wide VarID declaration; it does not select a catalog register."""
    variable_id = _scb_native_id(cells["VarID"][2], "VarID", row, filename=_IDENTIFIERS)
    variable = SourceCoordinate(
        status="value",
        native_id=variable_id,
        name=normalize_text(cells["Variabelnamn"][2]) or None,
    )
    return SourceRecord.create(
        revision=revision,
        locators=(
            _locator(_IDENTIFIERS, row, header, ("identifier", str(variable_id))),
        ),
        subject=SourceSubject(
            provider="scb",
            register=SourceCoordinate(status="not_applicable"),
            variant=SourceCoordinate(status="not_applicable"),
            population=SourceCoordinate(status="unknown"),
            variable=variable,
            member=variable,
            native=NativeCoordinates(variable_id=variable_id),
        ),
        edition_scope=TemporalScope(kind="not_applicable"),
        edition_period_scope=TemporalScope(kind="not_applicable"),
        fields=SourceFields(
            name=_cell_field(cells["Variabelnamn"], label=True),
            definition=_cell_field(cells["Variabeldefinition"], label=True),
            identifier=value_field(True),
        ),
        delivered_cells=_delivered_cells(cells),
    )


def iter_scb_auxiliary_records(
    snapshot: ScbSnapshotReader, filename: str, revision: SourceRevision
) -> Iterator[SourceRecord]:
    """Stream one supported actual SCB declaration table in original source order."""
    cleaner = {_UNIKA: clean_unika_row, _IDENTIFIERS: clean_identifier_row}.get(
        filename
    )
    if cleaner is None:
        raise ValueError(f"unsupported SCB declaration table: {filename}")
    with _open_scb_csv_prepared(snapshot.root / filename, snapshot) as (header, rows):
        for row, cells in rows:
            yield cleaner(header, row, cells, revision)
