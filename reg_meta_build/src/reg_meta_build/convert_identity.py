"""Convert accepted column-discriminated names into checked identity assignments.

This is an offline bridge for the existing naming format. Its discriminators must
identify unique literal columns, or existing declarations must supply complete
literal ownership. A folded name does not establish that distinct spellings mean
the same variable.
Other rename clusters, shape splits and discriminator collisions need their existing
decisions converted separately; no source identity is inferred from similar names.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta.fqid import derive_variable_slug

from reg_meta_build.convert_errata import capture_expectations
from reg_meta_build.source_coordinates import native_variable_key, source_register_key
from reg_meta_build.source_curation import (
    CheckedIdentityChange,
    CurationCase,
    FieldExpectation,
    OccurrenceCorrectionDecision,
    PeerGuard,
    ResolutionDiagnostic,
)
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_naming import LegacyNamingBinding, NativeNamingTarget

if TYPE_CHECKING:
    from collections.abc import Mapping

    from reg_meta_build.source_records import SourceRecord


@dataclass(frozen=True)
class ColumnPartitionConversion:
    bindings: tuple[LegacyNamingBinding, ...]
    case: CurationCase | None
    diagnostics: tuple[ResolutionDiagnostic, ...]


def convert_column_partitions(
    records: tuple[SourceRecord, ...],
    *,
    source_id: str,
    split_ids: tuple[str, ...],
    declared_columns: Mapping[str, str] | None = None,
    declaration_reference: str | None = None,
) -> ColumnPartitionConversion:
    """Bind a complete native family to its already accepted naming discriminators.

    ``split_ids`` must include every active split key for this native family. Blank
    source columns remain original evidence; they are not assigned to a sibling.
    New members or changed relevant facts invalidate the entire checked decision.
    ``declared_columns`` is a complete literal column-to-split-key map already
    asserted by accepted curation, with its original declaration reference. This
    converter verifies its coverage; it never discovers column equivalence.
    """
    keys = {native_variable_key(record) for record in records}
    if not records or None in keys or len(keys) != 1:
        raise ValueError("partition conversion requires one complete native family")
    if (
        len(set(split_ids)) != len(split_ids)
        or not split_ids
        or any(
            not key.startswith(source_id + ".") or not key[len(source_id) + 1 :]
            for key in split_ids
        )
    ):
        raise ValueError("split keys must uniquely discriminate this source identity")
    columns: dict[str, list[SourceRecord]] = defaultdict(list)
    for record in records:
        column = record.fields.column_name
        if column is not None and column.status == "value" and column.value:
            assert isinstance(column.value, str)
            columns[column.value].append(record)
    candidates: dict[str, list[str]] = defaultdict(list)
    for column in columns:
        candidates[derive_variable_slug(column) or "x"].append(column)
    suffixes = {key[len(source_id) + 1 :] for key in split_ids}
    if declared_columns is not None:
        if not declaration_reference or not declaration_reference.strip():
            raise ValueError(
                "explicit column ownership needs its declaration reference"
            )
        if set(declared_columns) != set(columns) or set(
            declared_columns.values()
        ) != set(split_ids):
            raise ValueError(
                "explicit column ownership must cover the complete columns and split keys"
            )
        partition_columns = {
            key: tuple(
                sorted(
                    column for column, owner in declared_columns.items() if owner == key
                )
            )
            for key in split_ids
        }
    elif declaration_reference is not None:
        raise ValueError("a declaration reference requires explicit column ownership")
    elif set(candidates) != suffixes or any(
        len(items) != 1 for items in candidates.values()
    ):
        return ColumnPartitionConversion(
            (),
            None,
            (
                ResolutionDiagnostic(
                    code="split_identity_conversion_pending",
                    severity="error",
                    subject=source_id,
                    detail=(
                        "The accepted naming discriminators do not identify a unique complete column partition. "
                        f"Accepted suffixes={sorted(suffixes)!r}; source columns={dict(candidates)!r}. "
                        "Convert the existing rename/shape/identity decision; do not guess from column similarity."
                    ),
                    refs=tuple(
                        sorted({record_ref(record) for record in records}, key=str)
                    ),
                    fields=("identity", "column_name"),
                    withheld_output=("identity_conversion",),
                ),
            ),
        )
    else:
        partition_columns = {
            key: tuple(candidates[key[len(source_id) + 1 :]]) for key in split_ids
        }
    first = records[0]
    native = native_variable_key(first)
    register = source_register_key(first)
    assert native is not None and register is not None
    expectations = capture_expectations(records, fields=("column_name",))
    guard = PeerGuard(
        guard_id=f"accepted-partitions:{first.source}:{source_id}",
        source=first.source,
        coordinates=(
            ("register", first.subject.register_name),
            ("variable", first.subject.variable),
        ),
        expected_members=tuple(item.ref for item in expectations),
    )
    effects = {}
    bindings = []
    for split_id in sorted(split_ids):
        key = (*native, "accepted-partition", split_id)
        for column in partition_columns[split_id]:
            for record in columns[column]:
                ref = record_ref(record)
                effect = CheckedIdentityChange(
                    ref=ref,
                    variable_key=key,
                    when=(
                        FieldExpectation(
                            name="column_name", status="value", value=column
                        ),
                    ),
                )
                effects[ref, column] = effect
        bindings.append(
            LegacyNamingBinding(
                kind="variable",
                provider=first.subject.provider,
                source_id=split_id,
                target=NativeNamingTarget(
                    kind="variable",
                    provider=first.subject.provider,
                    source_key=key,
                    register_key=register,
                    expectations=expectations,
                    peer_guards=(guard,),
                ),
            )
        )
    selected = {ref for ref, _column in effects}
    case = CurationCase(
        case_id=f"accepted-column-partitions:{first.source}:{source_id}",
        targets=tuple(item for item in expectations if item.ref in selected),
        support=tuple(item for item in expectations if item.ref not in selected),
        peer_guards=(guard,),
        decision=OccurrenceCorrectionDecision(
            reviewed=True,
            effects=tuple(effects[ref] for ref in sorted(effects, key=str)),
            reason="Preserve the exact column partitions already named by the accepted split entries.",
            provenance=(
                "existing naming entries: "
                + ", ".join(sorted(split_ids))
                + (
                    "; column ownership: " + declaration_reference
                    if declaration_reference
                    else ""
                )
            ),
        ),
    )
    return ColumnPartitionConversion(tuple(bindings), case, ())
