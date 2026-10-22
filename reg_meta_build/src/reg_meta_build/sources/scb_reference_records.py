"""SCB event declarations before catalog resolution."""

from __future__ import annotations

from typing import TYPE_CHECKING

from reg_meta_build.errors import SourceFormatError
from reg_meta_build.normalization import normalize_text, normalize_token
from reg_meta_build.source_evidence import (
    DeliveredCell,
    RecordLocator,
    SourceField,
    SourceRevision,
    canonical_sha256,
)
from reg_meta_build.source_files import EXPECTED_HEADERS, _open_scb_csv_prepared
from reg_meta_build.source_records import (
    SourceEvidenceRow,
    SourceEvidenceTable,
    value_field,
)
from reg_meta_build.source_reference_records import (
    CleanedSourceReferences,
    SourceEntityKind,
    SourceEventAction,
    SourceEventDeclaration,
)
from reg_meta_build.sources.scb_records import _delivered_cells

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from reg_meta_build.input_snapshot import ScbSnapshotReader


class ScbReferenceSourceError(SourceFormatError):
    """A selected SCB reference input has an unsupported source contract."""


_ACTIONS: dict[str, SourceEventAction] = {
    "Avslutad": "retired",
    "Tidsseriebrott": "series_break",
    "Ersatt av": "replaced_by",
    "Ersätter": "replaces",
}
_ENTITIES: dict[str, SourceEntityKind] = {
    "Register": "register",
    "RegisterVariant": "variant",
    "RegisterVersion": "edition",
    "Variabel": "variable",
    # The legacy source schema documents AktuellVariabel as the native CVID.
    "AktuellVariabel": "member",
    "Population": "population",
}


def _text(cell: DeliveredCell, *, multiline: bool = False) -> SourceField:
    if not cell.present:
        return SourceField(status="unknown", raw_value=cell.raw_value)
    text = normalize_text(cell.interpreted_value, multiline=multiline)
    return (
        value_field(text, raw=cell.raw_value)
        if text
        else SourceField(status="unknown", raw_value=cell.raw_value)
    )


def clean_timeseries_row(
    header: Sequence[str],
    row_number: int,
    cells: Mapping[str, tuple[bool, str | None, str]],
    revision: SourceRevision,
) -> SourceEventDeclaration:
    """Decode event vocabulary without parsing, redirecting or resolving IDs."""
    if list(header) != EXPECTED_HEADERS["Timeseries.csv"] or set(cells) != set(header):
        # Invariant: `_open_scb_csv_prepared` already refuses another header
        # (csv_bad_header) and a row of another width (csv_bad_row).
        raise ValueError("unsupported Timeseries.csv fields")
    delivered = _delivered_cells({name: cells[name] for name in header})
    fields = {cell.name: cell for cell in delivered}
    return SourceEventDeclaration(
        revision=revision,
        locator=RecordLocator(
            semantic_record_key=(
                "event",
                canonical_sha256([cell.model_dump(mode="json") for cell in delivered]),
            ),
            # The bare file name, as SCB occurrence records carry it: the
            # revision's artifact_path names the input layout, and evidence
            # digests keep file names (#1763).
            physical_file="Timeseries.csv",
            physical_table="Timeseries.csv",
            physical_record=f"row:{row_number}",
            physical_cells=tuple(f"row:{row_number}:{name}" for name in header),
        ),
        delivered_cells=delivered,
        name=_text(fields["Namn"]),
        action=_ACTIONS.get(
            normalize_token(fields["Handelse"].interpreted_value), "unknown"
        ),
        action_label=_text(fields["Handelse"]),
        description=_text(fields["Beskrivning"], multiline=True),
        entity_kind=_ENTITIES.get(
            normalize_token(fields["Entitet"].interpreted_value), "opaque"
        ),
        entity_label=_text(fields["Entitet"]),
        first_token=fields["ID1"],
        second_token=fields["ID2"],
        document_token=fields["FilID"],
    )


def read_scb_events(
    snapshot: ScbSnapshotReader, revision: SourceRevision
) -> CleanedSourceReferences:
    """Read the accepted event stream with the selected raw-artifact revision."""
    from reg_meta_build.input_snapshot import _decode_cell

    filename = "Timeseries.csv"
    artifact = next(
        (item for item in snapshot.manifest.files if item.name == filename), None
    )
    if (
        artifact is None
        or not artifact.present
        or revision.artifact_sha256 != artifact.raw_sha256
        or revision.artifact_size != artifact.raw_size
    ):
        raise ScbReferenceSourceError(
            "Timeseries.csv differs from its declared revision",
            code="source_revision_changed",
        )
    with _open_scb_csv_prepared(snapshot.root / filename, snapshot) as (header, rows):
        declarations = tuple(
            clean_timeseries_row(header, row, cells, revision) for row, cells in rows
        )
    header_cells = tuple(
        DeliveredCell(
            name=f"column:{index}",
            present=raw is not None,
            raw_value=raw,
            interpreted_value=raw or "",
        )
        for index, raw in enumerate(map(_decode_cell, artifact.header), start=1)
    )
    header_locator = RecordLocator(
        semantic_record_key=("source_header",),
        physical_file=filename,
        physical_table=filename,
        physical_record="row:1",
        physical_cells=tuple(f"row:1:{cell.name}" for cell in header_cells),
    )
    evidence = (
        SourceEvidenceRow(locator=header_locator, role="header", cells=header_cells),
    ) + tuple(
        SourceEvidenceRow(
            locator=item.locator, role="declaration", cells=item.delivered_cells
        )
        for item in declarations
    )
    return CleanedSourceReferences(
        revision=revision,
        declarations=declarations,
        tables=(
            SourceEvidenceTable(
                source=revision.dataset,
                source_revision_id=revision.revision_id,
                name=filename,
                rows=evidence,
            ),
        ),
    )


__all__ = [
    "ScbReferenceSourceError",
    "clean_timeseries_row",
    "read_scb_events",
]
