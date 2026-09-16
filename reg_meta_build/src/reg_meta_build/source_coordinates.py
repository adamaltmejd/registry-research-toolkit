"""Typed source-native grouping coordinates, without catalog identity inference."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from reg_meta_build.source_records import SourceCoordinate, SourceRecord


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
