"""Lossless source cleaning for prepared SCB value records."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING

from reg_meta_build.db import _decode_cp1252, _validated_scb_header
from reg_meta_build.input_snapshot import SnapshotError
from reg_meta_build.normalization import normalize_text, normalize_token
from reg_meta_build.source_values import (
    SourceValue,
    SourceValueAssociation,
    SourceValueDescriptor,
    SourceValueValidity,
)

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from reg_meta_build.input_snapshot import (
        PreparedVardemangder,
        RawCell,
        ScbSnapshotReader,
    )

_VALUE_FILE = "Vardemangder.csv"
_VALIDITY_FILE = "VardemangderValidDates.csv"


@dataclass(frozen=True, slots=True)
class CleanedScbValues:
    """Decoded dictionaries, exact validity rows, and streamed associations."""

    provenance: Mapping[str, str]
    header: tuple[RawCell, ...]
    descriptors: Mapping[str, SourceValueDescriptor]
    values: Mapping[str, SourceValue]
    validity: tuple[SourceValueValidity, ...]
    _prepared: PreparedVardemangder = field(repr=False, compare=False)

    def associations(self) -> Iterator[SourceValueAssociation]:
        """Stream every occurrence in source order, including duplicates."""
        for row_number, association in enumerate(
            self._prepared.occurrences(
                value_set_keys=self.descriptors,
                value_keys=self.values,
            ),
            start=2,
        ):
            cvid, item_id, descriptor_key, value_key = association
            yield SourceValueAssociation(
                row_number=row_number,
                source_file=_VALUE_FILE,
                member_id=cvid,
                member_id_field="CVID",
                item_id=item_id,
                descriptor_key=descriptor_key,
                value_key=value_key,
            )


def _clean_cell(cell: RawCell, *, token: bool = False) -> str | None:
    if cell is None:
        return None
    decoded = _decode_cp1252(cell)
    return normalize_token(decoded) if token else normalize_text(decoded)


def _clean_descriptor(
    payload_key: str, cells: tuple[RawCell, ...]
) -> SourceValueDescriptor:
    if len(cells) != 2:
        raise SnapshotError(
            f"{_VALUE_FILE} descriptor payload {payload_key} has {len(cells)} cells, expected 2"
        )
    version, level = cells
    return SourceValueDescriptor(
        payload_key=payload_key,
        raw_cells=(version, level),
        version=_clean_cell(version),
        level=_clean_cell(level),
    )


def _clean_value(payload_key: str, cells: tuple[RawCell, ...]) -> SourceValue:
    if len(cells) != 2:
        raise SnapshotError(
            f"{_VALUE_FILE} value payload {payload_key} has {len(cells)} cells, expected 2"
        )
    code, label = cells
    return SourceValue(
        payload_key=payload_key,
        raw_cells=(code, label),
        code=_clean_cell(code, token=True),
        label=_clean_cell(label),
    )


def _read_validity(reader: ScbSnapshotReader) -> tuple[SourceValueValidity, ...]:
    records: list[SourceValueValidity] = []
    with reader.open_csv(_VALIDITY_FILE) as (raw_header, rows):
        header = ["" if cell is None else cell for cell in raw_header]
        _validated_scb_header(_VALIDITY_FILE, header)
        for row_number, cells in enumerate(rows, start=2):
            if len(cells) != 3:
                raise SnapshotError(
                    f"{_VALIDITY_FILE}: row {row_number} has {len(cells)} cells, expected 3"
                )
            item_id, valid_from, valid_to = cells
            records.append(
                SourceValueValidity(
                    row_number=row_number,
                    source_file=_VALIDITY_FILE,
                    raw_cells=(item_id, valid_from, valid_to),
                    item_id=_clean_cell(item_id, token=True),
                    valid_from=_clean_cell(valid_from, token=True),
                    valid_to=_clean_cell(valid_to, token=True),
                )
            )
    return tuple(records)


def clean_scb_values(reader: ScbSnapshotReader) -> CleanedScbValues:
    """Decode unique payloads once without selecting or projecting associations."""
    prepared = reader.open_vardemangder()
    raw_header = tuple("" if cell is None else cell for cell in prepared.header)
    _validated_scb_header(_VALUE_FILE, raw_header)
    descriptors = {
        key: _clean_descriptor(key, cells) for key, cells in prepared.value_sets()
    }
    values = {key: _clean_value(key, cells) for key, cells in prepared.values()}
    return CleanedScbValues(
        provenance=MappingProxyType(dict(reader.provenance)),
        header=prepared.header,
        descriptors=MappingProxyType(descriptors),
        values=MappingProxyType(values),
        validity=_read_validity(reader),
        _prepared=prepared,
    )
