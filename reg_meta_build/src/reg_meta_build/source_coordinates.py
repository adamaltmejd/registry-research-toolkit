"""Typed source-native grouping coordinates, without catalog identity inference."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from reg_meta_build.source_records import (
        SourceCoordinate,
        SourceParentObservation,
        SourceRecord,
    )


type NativeKey = tuple[str | int, ...]


def _coordinate_key(coordinate: SourceCoordinate) -> NativeKey | None:
    if coordinate.status != "value":
        return None
    if isinstance(coordinate.native_id, int):
        return "native-int", coordinate.native_id
    if isinstance(coordinate.native_id, str):
        return "native-str", coordinate.native_id
    if coordinate.name:
        return "name", coordinate.name
    return None


def source_register_key(record: SourceRecord) -> NativeKey | None:
    coordinate = _coordinate_key(record.subject.register_name)
    return (
        None
        if coordinate is None
        else (record.source, record.subject.provider, "register", *coordinate)
    )


def native_variable_key(record: SourceRecord) -> NativeKey | None:
    register = source_register_key(record)
    coordinate = _coordinate_key(record.subject.variable)
    return (
        None
        if register is None or coordinate is None
        else (*register, "variable", *coordinate)
    )


def native_variant_key(record: SourceRecord) -> NativeKey | None:
    register = source_register_key(record)
    coordinate = _coordinate_key(record.subject.variant)
    if record.subject.variant.status == "not_applicable":
        coordinate = ("not-applicable",)
    return (
        None
        if register is None or coordinate is None
        else (*register, "variant", *coordinate)
    )


def native_column_key(record: SourceRecord) -> NativeKey | None:
    variable = native_variable_key(record)
    variant = native_variant_key(record)
    field = record.fields.column_name
    if (
        variable is None
        or variant is None
        or field is None
        or field.status != "value"
        or not isinstance(field.value, str)
        or not field.value
    ):
        return None
    return (*variable, "variant-key", *variant, "column", field.value)


def native_parent_key(
    source: str, provider: str, parent: SourceParentObservation
) -> NativeKey | None:
    """Use the same native topology for parent claims and variable occurrences."""
    register = _coordinate_key(parent.register_name)
    if register is None:
        return None
    register_key = (source, provider, "register", *register)
    if parent.kind == "register":
        return register_key
    variant = _coordinate_key(parent.variant) if parent.variant is not None else None
    if variant is None:
        return None
    variant_key = (*register_key, "variant", *variant)
    if parent.kind == "variant":
        return variant_key
    edition = _coordinate_key(parent.edition) if parent.edition is not None else None
    if edition is None:
        return None
    edition_key = (*variant_key, "edition", *edition)
    if parent.kind == "edition":
        return edition_key
    coordinate = _coordinate_key(parent.coordinate)
    return (*edition_key, parent.kind, *coordinate) if coordinate is not None else None
