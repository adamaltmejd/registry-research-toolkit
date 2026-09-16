"""Compact source value observations before membership or binding decisions.

Payload keys identify source-local dictionary entries, not catalog code sets. The
ordered association stream retains duplicates and connects those entries to the
native coordinates supplied by a source. Complete source collections may compare
normalized content separately; that never replaces occurrence evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from reg_meta_build.source_records import DeliveredCell, RecordLocator


type NormalizedValue = tuple[str | None, str | None]


@dataclass(frozen=True, slots=True)
class SourceValueWindow:
    """Interpreted source bounds; known empty bounds are explicitly unrestricted."""

    status: Literal["known", "unknown"]
    start: str | None = None
    end: str | None = None

    def __post_init__(self) -> None:
        if self.status not in {"known", "unknown"}:
            raise ValueError("invalid source value window status")
        if self.status == "unknown" and (
            self.start is not None or self.end is not None
        ):
            raise ValueError("unknown value window cannot carry inferred bounds")
        for bound in (self.start, self.end):
            if bound is not None and date.fromisoformat(bound).isoformat() != bound:
                raise ValueError("value validity bounds must be exact ISO dates")
        if self.start is not None and self.end is not None and self.start > self.end:
            raise ValueError("value validity bounds are reversed")


@dataclass(frozen=True, slots=True)
class SourceValueJoin:
    """A source-format relation, pinned to explicit target source namespaces.

    This declares a structural join, never catalog variable equivalence. Integer
    conversion occurs during preparation, while all original tokens remain stored.
    """

    record_sources: tuple[str, ...]
    member_target: Literal["native_member", "member_name", "declared_list", "unbound"]
    member_format: Literal["integer", "none"]
    validity_target: Literal["item", "row", "none"]
    missing_validity: Literal["unknown", "unrestricted"]
    rule: str
    provenance: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.rule or not self.provenance or any(not p for p in self.provenance):
            raise ValueError("source value joins require rule and provenance")
        if len(set(self.record_sources)) != len(self.record_sources) or any(
            not source for source in self.record_sources
        ):
            raise ValueError("value join source namespaces must be unique and nonempty")
        if (self.member_target == "unbound") != (not self.record_sources):
            raise ValueError(
                "bound value joins require explicit record source namespaces"
            )
        if (self.member_target == "native_member") != (self.member_format == "integer"):
            raise ValueError("native member joins require declared integer identifiers")


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
    member_references: tuple[str, ...] = ()
    record_ids: tuple[str, ...] = ()
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
    member_references: tuple[str, ...] = ()
    supplied_period: str | None = None
    supplied_window: SourceValueWindow | None = None
    section_window: SourceValueWindow | None = None
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
    window: SourceValueWindow | None = None

    @property
    def locator(self) -> str:
        return _row_locator(self.source_file, self.source_table, self.row_number)


__all__ = [
    "NormalizedValue",
    "SourceMemberHint",
    "SourceValue",
    "SourceValueAssociation",
    "SourceValueDescriptor",
    "SourceValueJoin",
    "SourceValueValidity",
    "SourceValueWindow",
]
