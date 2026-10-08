"""Checked corrections: delivery statements and additional deliveries type columns from guarded evidence."""

from __future__ import annotations

from typing import TYPE_CHECKING

from _source_effects_support import (
    effect_record as _record,
    storage_columns as _storage_columns,
)
from reg_meta_build.scb_errata import (
    ErrataDelivered,
    ErrataEditionBinding,
    ErrataVariantContext,
    convert_delivered_entry,
)
from reg_meta_build.source_effects import (
    record_ref,
)
from reg_meta_build.source_occurrences import source_occurrence

if TYPE_CHECKING:
    from reg_meta_build.source_records import SourceRecord
    from reg_meta_build.source_reference_records import SourceColumnTypeDeclaration


def _convert_delivered(
    records: tuple[SourceRecord, ...],
    target_edition: SourceRecord,
    *,
    native_variable_id: int | None = None,
    prefixes: tuple[str, ...] = (),
    storage_columns: dict[tuple[str, str], SourceColumnTypeDeclaration] | None = None,
    additional_physical_column: bool = False,
    storage_column: str | None = None,
):
    assert target_edition.original_period_text is not None
    target = source_occurrence(target_edition)
    assert target.edition_key is not None
    return convert_delivered_entry(
        ErrataDelivered(
            register_id=1,
            register_variant_id=2,
            column="VALUE",
            versions=(target_edition.original_period_text,),
            provenance="errata:accepted\nExisting evidence",
            native_variable_id=native_variable_id,
        ),
        case_id="accepted/delivered/0",
        context=ErrataVariantContext(
            records,
            1,
            2,
            (
                ErrataEditionBinding(
                    key=target.edition_key,
                    name=target_edition.original_period_text,
                    edition_scope=target.edition_scope,
                    edition_period_scope=target.edition_period_scope,
                    support=(record_ref(target_edition),),
                    native_id=target_edition.subject.native.edition_id,
                ),
            ),
        ),
        steward_table_prefixes=prefixes,
        storage_columns=storage_columns,
        additional_physical_column=additional_physical_column,
        storage_column=storage_column,
    )


def test_delivered_type_is_order_independent_across_records_and_storage() -> None:
    early = _record(cvid=10, column="VALUE", year="2018", data_type="integer")
    late = _record(cvid=20, column="value", year="2022", data_type="decimal")
    edition = _record(cvid=30, variable=99, column="EDITION", year="2020")
    first = _storage_columns(
        ("CIS2004", "smallint"), ("CIS2012", "varchar"), column="VALUE"
    )
    reversed_storage = dict(reversed(tuple(first.items())))
    a = _convert_delivered(
        (early, late, edition), edition, prefixes=("CIS",), storage_columns=first
    )
    b = _convert_delivered(
        (edition, late, early),
        edition,
        prefixes=("CIS",),
        storage_columns=reversed_storage,
    )
    assert a.case is not None and b.case is not None
    assert a.case.decision.provenance == b.case.decision.provenance
    assert a.case.decision.effects == b.case.decision.effects
    assert "CIS2004=smallint, CIS2012=varchar" in a.case.decision.provenance
