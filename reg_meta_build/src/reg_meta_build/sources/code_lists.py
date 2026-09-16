"""Lossless observations from the three delivered canonical code-list layouts.

A list name and its row occurrences are source coordinates, not a classification
binding. Additional labels, parent codes and validity cells remain evidence;
cleaning neither interprets a hierarchy nor chooses an effective code membership.
"""

from __future__ import annotations

import csv
import hashlib
import io
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import TYPE_CHECKING

from reg_meta_build.normalization import normalize_text, normalize_token
from reg_meta_build.source_records import (
    DeliveredCell,
    RecordLocator,
    SourceEvidenceRow,
    SourceEvidenceTable,
    SourceRevision,
    canonical_sha256,
)
from reg_meta_build.source_value_periods import value_window
from reg_meta_build.source_values import (
    SourceValue,
    SourceValueAssociation,
    SourceValueDescriptor,
    SourceValueValidity,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path


class CodeListSourceError(ValueError):
    """A selected code-list input violates its actual file-format contract."""


@dataclass(frozen=True)
class CleanedCodeList:
    revision: SourceRevision
    tables: tuple[SourceEvidenceTable, ...]
    descriptors: Mapping[str, SourceValueDescriptor]
    values: Mapping[str, SourceValue]
    associations: tuple[SourceValueAssociation, ...]
    validity: tuple[SourceValueValidity, ...] = ()


def read_selected_bytes(
    path: Path,
    revision: SourceRevision,
    *,
    error_type: type[ValueError] = CodeListSourceError,
) -> bytes:
    """Bind a machine-readable source read to its exact selected artifact bytes."""
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise error_type(f"cannot read {revision.artifact_path}: {exc}") from exc
    if (
        len(payload) != revision.artifact_size
        or hashlib.sha256(payload).hexdigest() != revision.artifact_sha256
    ):
        raise error_type(
            f"source differs from its declared revision: {revision.artifact_path}"
        )
    return payload


_LAYOUTS = {
    ("code", "label"),
    ("vardekod", "vardebenamning"),
    ("code", "label", "label_en", "parent_code", "valid_from", "valid_to"),
}


def _cells(names: tuple[str, ...], row: list[str]) -> tuple[DeliveredCell, ...]:
    return tuple(
        DeliveredCell(
            name=name,
            present=True,
            raw_value=raw,
            interpreted_value=raw,
            raw_type="str",
            storage_type="csv",
        )
        for name, raw in zip(names, row, strict=True)
    )


def _locator(
    revision: SourceRevision,
    table: str,
    row_number: int,
    key: tuple[str, ...],
    cells: tuple[DeliveredCell, ...],
) -> RecordLocator:
    path = f"row:{row_number}"
    return RecordLocator(
        semantic_record_key=key,
        physical_file=revision.artifact_path,
        physical_table=table,
        physical_record=path,
        physical_cells=tuple(f"{path}.{cell.name}" for cell in cells),
    )


def read_code_list(
    path: Path, revision: SourceRevision, *, name: str
) -> CleanedCodeList:
    """Retain a named CSV and every duplicate or conflicting row occurrence."""
    payload = read_selected_bytes(path, revision)
    descriptor_key = normalize_token(name)
    if not descriptor_key:
        raise CodeListSourceError("code-list name must be non-empty")
    try:
        reader = csv.reader(
            io.StringIO(payload.decode("utf-8-sig"), newline=""), strict=True
        )
        header = next(reader, None)
        if (
            header is None
            or tuple(normalize_token(value).casefold() for value in header)
            not in _LAYOUTS
        ):
            raise CodeListSourceError(
                f"{revision.artifact_path}: unsupported code-list CSV layout {header!r}; "
                "expected code,label; vardekod,vardebenamning; or "
                "code,label,label_en,parent_code,valid_from,valid_to"
            )
        header_cells = _cells(
            tuple(f"column:{index}" for index in range(1, len(header) + 1)), header
        )
        header_locator = _locator(
            revision, path.name, 1, (f"code-list:{descriptor_key}",), header_cells
        )
        rows = [
            SourceEvidenceRow(locator=header_locator, role="header", cells=header_cells)
        ]
        values: dict[str, SourceValue] = {}
        associations = []
        validity = []
        for row_number, cells in enumerate(reader, start=2):
            if not cells:
                locator = _locator(
                    revision,
                    path.name,
                    row_number,
                    (f"code-list:{descriptor_key}", "blank-row"),
                    (),
                )
                rows.append(SourceEvidenceRow(locator=locator, role="note", cells=()))
                continue
            if len(cells) != len(header):
                raise CodeListSourceError(
                    f"{revision.artifact_path}: row {row_number} has {len(cells)} fields; "
                    f"expected {len(header)}"
                )
            delivered = _cells(tuple(header), cells)
            value_key = canonical_sha256(cells)
            locator = _locator(
                revision,
                path.name,
                row_number,
                (f"code-list:{descriptor_key}", f"value:{value_key}"),
                delivered,
            )
            rows.append(
                SourceEvidenceRow(locator=locator, role="data", cells=delivered)
            )
            if value_key in values:
                values[value_key] = replace(
                    values[value_key], locators=(*values[value_key].locators, locator)
                )
            else:
                values[value_key] = SourceValue(
                    payload_key=value_key,
                    code=normalize_token(cells[0]),
                    label=normalize_text(cells[1]),
                    raw_cells=tuple(cells),
                    locators=(locator,),
                    delivered_cells=delivered,
                )
            if len(header) == 6:
                validity.append(
                    SourceValueValidity(
                        row_number=row_number,
                        item_id=None,
                        valid_from=cells[4],
                        valid_to=cells[5],
                        source_file=revision.artifact_path,
                        source_table=path.name,
                        raw_cells=tuple(cells),
                        locators=(locator,),
                        delivered_cells=delivered,
                        window=value_window(cells[4], cells[5]),
                    )
                )
            associations.append(
                SourceValueAssociation(
                    row_number=row_number,
                    descriptor_key=descriptor_key,
                    value_key=value_key,
                    source_file=revision.artifact_path,
                    source_table=path.name,
                    delivered_cells=delivered,
                )
            )
    except (UnicodeDecodeError, csv.Error) as exc:
        raise CodeListSourceError(
            f"invalid code CSV {revision.artifact_path}: {exc}"
        ) from exc
    descriptor = SourceValueDescriptor(
        payload_key=descriptor_key,
        name=name,
        raw_cells=tuple(header),
        locators=(header_locator,),
        delivered_cells=header_cells,
    )
    return CleanedCodeList(
        revision=revision,
        tables=(
            SourceEvidenceTable(
                source=revision.dataset,
                source_revision_id=revision.revision_id,
                name=path.name,
                rows=tuple(rows),
            ),
        ),
        descriptors=MappingProxyType({descriptor_key: descriptor}),
        values=MappingProxyType(values),
        associations=tuple(associations),
        validity=tuple(validity),
    )


__all__ = [
    "CleanedCodeList",
    "CodeListSourceError",
    "read_code_list",
    "read_selected_bytes",
]
