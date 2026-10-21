"""Render a `cases/build/` curation tree's authoring placeholders.

Each placeholder is filled from the accepted prepared records with the public capture
helpers a curator uses; `cases/build/README.md` lists the placeholders.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from reg_meta_build.source_coding import (
    copied_coding_fingerprints,
)
from reg_meta_build.source_coordinates import native_variant_key
from reg_meta_build.source_curation import (
    acknowledgement_evidence_sha256,
    capture_expectations,
    field_projection_sha256,
    record_projections_sha256,
    toml_inline,
)
from reg_meta_build.source_evidence import evidence_sha256
from reg_meta_build.source_naming import authored_naming_id
from reg_meta_build.source_occurrences import source_occurrence
from reg_meta_build.source_records import SourceFields
from reg_meta_build.source_value_bindings import (
    marker_binding_fingerprints,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from _build_case_sources import PreparedSet
    from reg_meta_build.source_records import SourceRecord


PROSE = ("name", "definition", "description", "operational_definition")
_PLACEHOLDER = re.compile(r"\{\{\s*([a-z0-9_]+)((?:\s+[a-z_]+=[^\s}]*)*)\s*\}\}")


def _column(record: SourceRecord) -> str:
    column = record.fields.column_name
    return (column and column.value) or ""


RECORD_FILTERS: dict[str, Callable[[SourceRecord], object]] = {
    "source": lambda r: r.source,
    "register": lambda r: str(r.subject.native.register_id),
    "key": lambda r: r.locators[0].semantic_record_key[-1],
    "column": _column,
    "member": lambda r: r.subject.member.name,
    "variable": lambda r: str(
        r.subject.native.variable_id
        if r.subject.native.variable_id is not None
        else r.subject.variable.native_id
    ),
    "edition": lambda r: str(r.subject.native.edition_id),
}


def _select(records: tuple[SourceRecord, ...], args: dict[str, str]) -> tuple:
    """Records matching every filter argument; a value may list alternatives."""
    filters = {
        key: value.split(",") for key, value in args.items() if key in RECORD_FILTERS
    }
    selected = tuple(
        record
        for record in records
        if all(
            str(RECORD_FILTERS[key](record)).startswith(tuple(values))
            if key == "source"
            else str(RECORD_FILTERS[key](record)) in values
            for key, values in filters.items()
        )
    )
    assert selected, f"placeholder selects no prepared record: {args}"
    return selected


def _one(records: tuple[SourceRecord, ...], args: dict[str, str]) -> SourceRecord:
    selected = _select(records, args)
    assert len(selected) == 1, f"placeholder needs exactly one record: {args}"
    return selected[0]


def _fields(args: dict[str, str]) -> tuple[str, ...]:
    named = args.get("fields", "all")
    return (
        tuple(SourceFields.model_fields)
        if named == "all"
        else tuple(PROSE if named == "prose" else named.split(","))
    )


def _marker_bindings(s: PreparedSet, record: SourceRecord, args: dict[str, str]):
    """The fingerprints of the record's marker bindings in ``from``..``to``."""
    scope = source_occurrence(record).edition_period_scope
    markers = marker_binding_fingerprints(
        ((scope, binding) for binding in s.bindings(record).bindings),
        args["from"],
        args["to"],
    )
    assert markers is not None, f"placeholder binds no marker: {args}"
    return list(markers)


def _association(s: PreparedSet, args: dict[str, str]):
    """The one source-list association behind member ``code=``/``label=`` (blank if
    omitted) of the claims bound to the selected records."""
    found = [
        association
        for claim in s.claims(_select(s.records(), args))
        for member in claim.members
        if member.code == args["code"] and (member.label or "") == args.get("label", "")
        for association in member.associations
    ]
    assert len(found) == 1, f"placeholder needs exactly one association: {args}"
    return found[0]


def _field_projection_sha256(
    records: tuple[SourceRecord, ...], fields: tuple[str, ...]
) -> str:
    """The one field digest every selected record shares, as an owner pins it."""
    digests = {
        field_projection_sha256(projection.fields)
        for expectation in capture_expectations(records, fields=fields)
        for projection in expectation.alternatives
    }
    assert len(digests) == 1, f"selected records differ in their fields: {digests}"
    return digests.pop()


def _scope(record: SourceRecord, args: dict[str, str], attribute: str) -> dict:
    scope = getattr(record, attribute).model_dump(mode="json")
    if "end" in args:
        scope["intervals"][0]["end"] = args["end"]
    return scope


_DIRECTIVES: dict[str, Callable[[PreparedSet, dict[str, str]], object]] = {
    "evidence_sha256": lambda s, a: acknowledgement_evidence_sha256(
        _select(s.records(), a)
    ),
    "coding_evidence_sha256": lambda s, a: acknowledgement_evidence_sha256(
        selected := _select(s.records(), a), s.coding_sha256(selected)
    ),
    "expected_records": lambda s, a: [
        expectation.model_dump(mode="json")
        for expectation in capture_expectations(
            _select(s.records(), a), fields=_fields(a), parents=True
        )
    ],
    "record_projections_sha256": lambda s, a: record_projections_sha256(
        capture_expectations(_select(s.records(), a), fields=_fields(a), parents=True)
    ),
    "field_projection_sha256": lambda s, a: _field_projection_sha256(
        _select(s.records(), a), _fields(a)
    ),
    "expected_fields": lambda s, a: [
        field.model_dump(mode="json")
        for field in capture_expectations((_one(s.records(), a),), fields=_fields(a))[0]
        .alternatives[0]
        .fields
    ],
    "period_text": lambda s, a: _one(s.records(), a).original_period_text,
    "edition_scope": lambda s, a: _scope(_one(s.records(), a), a, "edition_scope"),
    "period_scope": lambda s, a: _scope(
        _one(s.records(), a), a, "edition_period_scope"
    ),
    "marker_bindings": lambda s, a: _marker_bindings(s, _one(s.records(), a), a),
    "raw_codings": lambda s, a: sorted(set(s.coding_sha256(_select(s.records(), a)))),
    "source_codings": lambda s, a: list(
        copied_coding_fingerprints(s.claims(_select(s.records(), a)))
    ),
    "association": lambda s, a: _association(s, a).locator,
    "association_sha256": lambda s, a: evidence_sha256(_association(s, a)),
    "relationship_row": lambda s, a: s.relationship(a["table"]).locator.physical_record,
    "relationship_sha256": lambda s, a: evidence_sha256(s.relationship(a["table"])),
    "table_sha256": lambda s, a: s.table_sha256(a["table"]),
    "naming_id": lambda s, a: authored_naming_id(
        a["kind"],
        provider=a["provider"],
        register_key=a["register_key"],
        member_key=a.get("member_key"),
    ),
    "variant_key": lambda s, a: _variant_key(_one(s.records(), a)),
}


def _variant_key(record: SourceRecord) -> list:
    key = native_variant_key(record)
    assert key is not None, f"record has no native variant: {record.record_id}"
    return list(key)


def render_curation(
    source: Path, target: Path, authored: PreparedSet, built: PreparedSet
) -> Path:
    """Copy a case's curation tree, resolving each `{{directive arg=value}}`.

    A directive reads ``authored`` unless it says ``read=built``.
    """

    def resolve(match: re.Match[str]) -> str:
        name, raw = match.group(1), match.group(2).split()
        args = dict(item.split("=", 1) for item in raw)
        assert name in _DIRECTIVES, f"unknown curation placeholder: {name}"
        read = args.pop("read", "authored")
        assert read in {"authored", "built"}, f"unknown placeholder read: {read}"
        return toml_inline(
            _DIRECTIVES[name](built if read == "built" else authored, args)
        )

    for path in source.rglob("*"):
        if path.is_file():
            out = target / path.relative_to(source)
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(
                _PLACEHOLDER.sub(resolve, path.read_text(encoding="utf-8")),
                encoding="utf-8",
            )
    (target / "classifications").mkdir(parents=True, exist_ok=True)
    return target
