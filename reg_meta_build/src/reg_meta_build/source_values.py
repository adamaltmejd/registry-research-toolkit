"""Compact source value observations before membership or binding decisions.

Payload keys identify source-local dictionary entries, not catalog code sets. The
ordered association stream retains duplicates and connects those entries to the
native coordinates supplied by a source. Complete source collections may compare
normalized content separately; that never replaces occurrence evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from reg_meta_build.source_records import DeliveredCell, RecordLocator


type NormalizedValue = tuple[str | None, str | None]


@dataclass(frozen=True, slots=True)
class SourceMemberHint:
    """A supplied variable-name hint whose role must not imply a binding."""

    role: Literal["sheet_suffix", "list_header", "row"]
    value: str | None
    locator: RecordLocator | None = None


@dataclass(frozen=True, slots=True)
class SourceValueDescriptor:
    """A source descriptor or list declaration, independent of its members."""

    payload_key: str
    raw_cells: tuple[str | None, ...] = ()
    name: str | None = None
    version: str | None = None
    level: str | None = None
    member_hints: tuple[SourceMemberHint, ...] = ()
    locators: tuple[RecordLocator, ...] = ()
    delivered_cells: tuple[DeliveredCell, ...] = ()


@dataclass(frozen=True, slots=True)
class SourceValue:
    """One source code/label payload, retaining its original representation."""

    payload_key: str
    code: str | None
    label: str | None
    raw_cells: tuple[str | None, ...] = ()
    locators: tuple[RecordLocator, ...] = ()
    delivered_cells: tuple[DeliveredCell, ...] = ()

    @property
    def normalized_content(self) -> NormalizedValue:
        return self.code, self.label


def _row_locator(source_file: str, source_table: str | None, row_number: int) -> str:
    table = f":{source_table}" if source_table else ""
    return f"{source_file}{table}:row:{row_number}"


@dataclass(frozen=True, slots=True)
class SourceValueAssociation:
    """One native occurrence, not a resolved catalog membership statement.

    Native member and item IDs retain source spelling, including absent and empty
    cells. A section period remains separate from the row's supplied period, with
    the section's own locator; cleaning does not select an effective period.
    """

    row_number: int
    descriptor_key: str
    value_key: str
    source_file: str
    source_table: str | None = None
    member_id: str | None = None
    member_id_field: str | None = None
    item_id: str | None = None
    member_hints: tuple[SourceMemberHint, ...] = ()
    supplied_period: str | None = None
    section_period: str | None = None
    section_locator: RecordLocator | None = None
    delivered_cells: tuple[DeliveredCell, ...] = ()

    @property
    def locator(self) -> str:
        return _row_locator(self.source_file, self.source_table, self.row_number)


@dataclass(frozen=True, slots=True)
class SourceValueValidity:
    """One exact native-item validity assertion without interval projection."""

    row_number: int
    item_id: str | None
    valid_from: str | None
    valid_to: str | None
    source_file: str
    source_table: str | None = None
    raw_cells: tuple[str | None, ...] = ()
    locators: tuple[RecordLocator, ...] = ()
    delivered_cells: tuple[DeliveredCell, ...] = ()

    @property
    def locator(self) -> str:
        return _row_locator(self.source_file, self.source_table, self.row_number)


__all__ = [
    "NormalizedValue",
    "SourceMemberHint",
    "SourceValue",
    "SourceValueAssociation",
    "SourceValueDescriptor",
    "SourceValueValidity",
]
