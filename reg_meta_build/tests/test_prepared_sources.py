"""Accepted compact evidence is lossless, indexed and cheap to reopen."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _prepared_fixtures import (
    accept_prepared,
    prepared_record as _record,
    prepared_revision as _revision,
)
from reg_meta_build.prepared_sources import (
    open_prepared_source_records,
    prepare_source_records,
)
from reg_meta_build.source_coordinates import native_variable_key, source_register_key
from reg_meta_build.source_curation import acknowledgement_evidence_sha256
from reg_meta_build.source_records import (
    SourceCoordinate,
    SourceFieldCells,
    SourceFields,
    SourceParentObservation,
    SourceRecord,
    value_field,
)
from reg_meta_build.source_support import SourceSupportBindings, SourceSupportJoin

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta.source_evidence import SourceRevision


@pytest.mark.parametrize(
    "keys",
    [
        ("column_name",),
        ("variable_id",),
        ("register_name", "variant_name", "variable_name", "column_name"),
    ],
)
def test_support_projection_matches_full_record_cardinalities(
    tmp_path: Path, keys
) -> None:
    revision = _revision("source-a", "a")

    def occurrence(row, native, column="VALUE", edition="2020"):
        original = _record(
            revision, row=row, member="Source variable", raw_value="Source variable"
        )
        subject = original.subject.model_copy(
            update={
                "variable": SourceCoordinate(status="unknown")
                if native is None
                else SourceCoordinate(
                    status="value", native_id=native, name="Variable"
                ),
                "variant": SourceCoordinate(status="value", name="People"),
            }
        )
        arguments = {
            field: getattr(original, field)
            for field in SourceRecord.model_fields
            if field
            not in {
                "record_id",
                "source",
                "source_revision_id",
                "subject",
                "fields",
                "original_period_text",
            }
        }
        return SourceRecord.create(
            revision=revision,
            subject=subject,
            fields=SourceFields(column_name=value_field(column)),
            original_period_text=edition,
            **arguments,
        )

    first, second, unknown = (
        occurrence(1, 7),
        occurrence(2, "7", edition="2021"),
        occurrence(3, None, edition="2022"),
    )
    records = (first, first, second, unknown, occurrence(4, 8, "OTHER"))
    root = tmp_path / "inputs" / "records"
    manifest = prepare_source_records(
        root, records=records, revisions=(revision,), scope="complete targets"
    )
    commit = accept_prepared(root)
    reader = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=commit
    )
    join = SourceSupportJoin(
        source="summary",
        target_sources=(first.source,),
        keys=keys,
        fields=("identifier",),
        unique_variable=True,
        discriminator=("coverage_from", "coverage_to"),
        rule="literal fixture join",
        provenance=("fixture",),
    )
    # Support and delivery collections remain distinct, even with equal coordinates.
    # The endpoints separate the two renumbered identities under the literal key.
    support_record = first.model_copy(
        update={
            "source": "summary",
            "fields": first.fields.model_copy(
                update={
                    "coverage_from": value_field("2020"),
                    "coverage_to": value_field("2020"),
                }
            ),
        }
    )
    full, projected = (
        SourceSupportBindings((join,), (support_record,)),
        SourceSupportBindings((join,), (support_record,)),
    )
    for item in records:
        full.observe(item)
    full.seal()
    targets = tuple(reader.iter_support_targets((join,)))
    assert len(targets) < len(records)
    assert any(t.variable_key is None for t in targets)
    assert {t.edition_name for t in targets} == {"2020", "2021", "2022"}
    for target in targets:
        projected.observe_target(target)
    projected.seal()
    assert projected.accounting == full.accounting
    assert projected.diagnostics == full.diagnostics
    assert [projected.bind(item) for item in records] == [
        full.bind(item) for item in records
    ]
    with pytest.raises(ValueError, match="already complete"):
        projected.observe_target(targets[0])


def test_native_family_index_groups_ids_across_variants_without_losing_other_records(
    tmp_path: Path,
) -> None:
    first_revision = _revision("source-a", "a")
    second_revision = _revision("source-b", "b")

    def occurrence(
        revision: SourceRevision,
        row: int,
        native: int | str | None,
        *,
        variant: str = "first",
        label: str = "Name",
    ) -> SourceRecord:
        original = _record(
            revision, row=row, member="Source variable", raw_value="Source variable"
        )
        subject = original.subject.model_copy(
            update={
                "variable": SourceCoordinate(status="unknown")
                if native is None
                else SourceCoordinate(status="value", native_id=native, name=label),
                "variant": SourceCoordinate(status="value", native_id=variant),
            }
        )
        arguments = {
            field: getattr(original, field)
            for field in SourceRecord.model_fields
            if field not in {"record_id", "source", "source_revision_id", "subject"}
        }
        return SourceRecord.create(revision=revision, subject=subject, **arguments)

    first = occurrence(first_revision, 2, 7)
    other = occurrence(first_revision, 3, "7")
    changed = occurrence(first_revision, 4, 7, variant="second", label="Changed label")
    unplaced = occurrence(first_revision, 5, None)
    second = occurrence(second_revision, 2, 7)
    records = (first, other, changed, unplaced, first, second)
    root = tmp_path / "inputs" / "records"
    manifest = prepare_source_records(
        root,
        records=records,
        revisions=(first_revision, second_revision),
        scope="complete native grouping",
    )
    commit = accept_prepared(root)
    reader = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=commit
    )
    stream = reader.iter_native_families("source-a")
    key, family = next(stream)
    assert key == native_variable_key(first)
    assert family == (first, changed, first)
    remaining = dict(stream)
    assert remaining == {native_variable_key(other): (other,)}
    assert tuple(reader.iter_without_native_family("source-a")) == (unplaced,)
    assert dict(reader.iter_native_families("source-b")) == {
        native_variable_key(second): (second,)
    }
    assert tuple(reader.records) == records
    assert tuple(reader.iter_native_families("missing-source")) == ()


def test_family_records_are_reused_across_views_until_cleared(tmp_path: Path) -> None:
    revision = _revision("source-a", "a")
    records = tuple(
        _record(revision, row=row, member="Same", raw_value=f"value {row}")
        for row in (1, 2, 3)
    )
    root = tmp_path / "inputs" / "records"
    manifest = prepare_source_records(
        root, records=records, revisions=(revision,), scope="batch reuse"
    )
    commit = accept_prepared(root)
    reader = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=commit
    )
    family = next(reader.iter_native_families(revision.dataset))[1]
    assert family == records
    assert next(reader.iter_native_families(revision.dataset))[1] == family
    assert next(reader.iter_register_slices(revision.dataset))[1] == family
    assert tuple(reader.iter_records(source=revision.dataset)) == family
    assert all(
        left is right
        for left, right in zip(
            family, tuple(reader.iter_records(source=revision.dataset)), strict=True
        )
    )

    snapshot = tuple(record.model_dump_json() for record in family)
    fingerprint = acknowledgement_evidence_sha256(family, ("coding evidence",))
    reader.clear_decoded_records()
    reread = next(reader.iter_native_families(revision.dataset))[1]
    assert tuple(record.model_dump_json() for record in reread) == snapshot
    assert acknowledgement_evidence_sha256(reread, ("coding evidence",)) == fingerprint
    assert all(left is not right for left, right in zip(family, reread, strict=True))
    assert next(reader.iter_register_slices(revision.dataset))[1] == reread
    assert (
        tuple(
            reader.lookup(revision.dataset, records[0].locators[0].semantic_record_key)
        )
        == reread
    )
    assert (
        acknowledgement_evidence_sha256(reread[:-1], ("coding evidence",))
        != fingerprint
    )
    assert acknowledgement_evidence_sha256(reread, ("changed coding",)) != fingerprint
    changed = reread[0].model_copy(
        update={
            "fields": reread[0].fields.model_copy(
                update={"name": value_field("changed")}
            )
        }
    )
    assert (
        acknowledgement_evidence_sha256((changed, *reread[1:]), ("coding evidence",))
        != fingerprint
    )


def test_decoded_record_eviction_past_the_shipped_bound_rebuilds_complete_originals(
    tmp_path: Path,
) -> None:
    # One record past the reader's decoded-record bound (8,192), so the first family
    # member is evicted and a lookup must rebuild it from the accepted payload.
    revision = _revision("source-a", "a")
    records = tuple(
        _record(revision, row=row, member="Same", raw_value=f"value {row}")
        for row in range(1, 8194)
    )
    root = tmp_path / "inputs" / "records"
    manifest = prepare_source_records(
        root, records=records, revisions=(revision,), scope="bounded decoded records"
    )
    reader = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=accept_prepared(root)
    )
    family = next(reader.iter_native_families(revision.dataset))[1]
    assert family == records
    fingerprint = acknowledgement_evidence_sha256(family, ("coding evidence",))
    reread = tuple(
        reader.lookup(revision.dataset, records[0].locators[0].semantic_record_key)
    )
    assert reread[0] is not family[0]
    assert tuple(record.model_dump_json() for record in reread) == tuple(
        record.model_dump_json() for record in records
    )
    assert acknowledgement_evidence_sha256(reread, ("coding evidence",)) == fingerprint


def test_native_family_register_filter_preserves_grouping_and_order(
    tmp_path: Path,
) -> None:
    revision = _revision("source-a", "a")
    first = _record(
        revision,
        row=1,
        member="First",
        raw_value="First",
        fields=SourceFields(
            name=value_field("First"),
            definition=value_field("Exact supplied definition"),
            operational_definition=value_field("Exact supplied operation"),
            measurement_unit=value_field("100-tal kronor"),
            source_attribution=value_field("Supplied origin"),
            data_length=value_field("8"),
        ),
    )
    second = _record(revision, row=2, member="Second", raw_value="Second")
    other_subject = second.subject.model_copy(
        update={"register_name": SourceCoordinate(status="value", name="other")}
    )
    arguments = {
        field: getattr(second, field)
        for field in SourceRecord.model_fields
        if field not in {"record_id", "source", "source_revision_id", "subject"}
    }
    other = SourceRecord.create(revision=revision, subject=other_subject, **arguments)
    unrelated = _record(revision, row=3, member="Unrelated", raw_value="Unrelated")
    records = (first, other, first, unrelated)
    root = tmp_path / "inputs" / "records"
    manifest = prepare_source_records(
        root, records=records, revisions=(revision,), scope="filtered families"
    )
    commit = accept_prepared(root)
    reader = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=commit
    )
    all_families = tuple(reader.iter_native_families(revision.dataset))
    first_register = source_register_key(first)
    assert first_register is not None
    assert tuple(
        reader.iter_native_families(revision.dataset, {first_register})
    ) == tuple(family for family in all_families if family[0][:5] == first_register)
    assert tuple(reader.iter_native_families(revision.dataset, set())) == ()

    needed = native_variable_key(first)
    assert needed is not None
    filtered = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=commit
    )
    selected_native = tuple(
        filtered.iter_native_families(
            revision.dataset, {first_register}, select_family={needed}.__contains__
        )
    )
    assert selected_native == tuple(f for f in all_families if f[0] == needed)
    assert (
        tuple(
            filtered.iter_native_families(
                revision.dataset, {first_register}, families=(needed,)
            )
        )
        == selected_native
    )
    assert (
        tuple(
            filtered.iter_native_families(
                revision.dataset, families=(needed[:-1] + ("absent",),)
            )
        )
        == ()
    )
    assert (
        tuple(
            filtered.iter_native_families(
                revision.dataset, {first_register}, select_family=set().__contains__
            )
        )
        == ()
    )

    narrow = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=commit
    )
    naming_families = tuple(
        narrow.iter_naming_families(revision.dataset, {first_register})
    )
    expected = tuple(
        family for family in all_families if family[0][:5] == first_register
    )
    assert tuple(key for key, _ in naming_families) == tuple(key for key, _ in expected)
    for (_, projected), (_, complete) in zip(naming_families, expected, strict=True):
        assert tuple(
            (
                item.source,
                item.subject,
                item.parent_facts,
                item.edition_scope,
                item.edition_period_scope,
                item.locators[0].semantic_record_key,
            )
            for item in projected
        ) == tuple(
            (
                item.source,
                item.subject,
                item.parent_facts,
                item.edition_scope,
                item.edition_period_scope,
                item.locators[0].semantic_record_key,
            )
            for item in complete
        )
    partition_families = tuple(
        narrow.iter_partition_families(revision.dataset, {first_register})
    )
    assert tuple(key for key, _ in partition_families) == tuple(
        key for key, _ in expected
    )
    for (_, projected), (_, complete) in zip(partition_families, expected, strict=True):
        assert tuple(item.fields for item in projected) == tuple(
            item.fields for item in complete
        )
        assert tuple(
            (
                item.source,
                item.subject,
                item.parent_facts,
                item.edition_scope,
                item.edition_period_scope,
                item.locators[0].semantic_record_key,
                item.fields.column_name,
                item.fields.name,
                item.fields.data_type,
            )
            for item in projected
        ) == tuple(
            (
                item.source,
                item.subject,
                item.parent_facts,
                item.edition_scope,
                item.edition_period_scope,
                item.locators[0].semantic_record_key,
                item.fields.column_name,
                item.fields.name,
                item.fields.data_type,
            )
            for item in complete
        )
    needed_key = native_variable_key(first)
    unrelated_key = native_variable_key(unrelated)
    assert needed_key is not None and unrelated_key is not None
    selected = tuple(
        narrow.iter_partition_families(
            revision.dataset,
            {first_register},
            select_family={needed_key}.__contains__,
        )
    )
    assert tuple(key for key, _ in selected) == (needed_key,)
    assert len(selected[0][1]) == 2
    assert (
        tuple(
            narrow.iter_partition_families(
                revision.dataset, {first_register}, select_family=set().__contains__
            )
        )
        == ()
    )
    assert tuple(
        key
        for key, _ in narrow.iter_partition_families(revision.dataset, {first_register})
    ) == (needed_key, unrelated_key)
    assert tuple(narrow.iter_naming_register_slices(revision.dataset, {first_register}))
    registerless = tuple(narrow.iter_naming_register_slices(revision.dataset, {None}))
    assert len(registerless) == 1 and registerless[0][0] is None
    assert len(registerless[0][1]) == len(records)


def test_parent_fields_round_trip_through_prepared_record(tmp_path: Path) -> None:
    revision = _revision("source-a", "a")
    original = _record(revision, row=1, member="Child", raw_value="child")
    register = original.subject.register_name
    parent = SourceParentObservation(
        kind="register",
        coordinate=register,
        register=register,
        fields=SourceFields(name=value_field("Parent name")),
        field_cells=(SourceFieldCells(field="name", positions=(0,)),),
    )
    record = SourceRecord.create(
        revision=revision,
        locators=original.locators,
        subject=original.subject,
        edition_scope=original.edition_scope,
        edition_period_scope=original.edition_period_scope,
        fields=original.fields,
        parent_facts=(parent,),
        language=original.language,
        code_set_references=original.code_set_references,
        original_period_text=original.original_period_text,
        context=original.context,
        delivered_cells=original.delivered_cells[:2],
    )
    root = tmp_path / "inputs" / "records"
    manifest = prepare_source_records(
        root, records=(record,), revisions=(revision,), scope="parent fields"
    )
    commit = accept_prepared(root)
    reader = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=commit
    )
    decoded = next(reader.iter_records())
    assert decoded == record
    assert decoded.parent_facts[0].fields.name == value_field("Parent name")


def test_register_slices_preserve_native_identity_parent_rows_and_unknowns(
    tmp_path: Path,
) -> None:
    revision = _revision("source-a", "a")

    def record(row: int, coordinate: SourceCoordinate) -> SourceRecord:
        original = _record(
            revision, row=row, member="Unplaced variable", raw_value="raw"
        )
        arguments = {
            field: getattr(original, field)
            for field in SourceRecord.model_fields
            if field not in {"record_id", "source", "source_revision_id", "subject"}
        }
        arguments["locators"] = original.locators + (
            original.locators[0].model_copy(update={"physical_record": f"copy:{row}"}),
        )
        return SourceRecord.create(
            revision=revision,
            subject=original.subject.model_copy(update={"register_name": coordinate}),
            **arguments,
        )

    first = record(
        1, SourceCoordinate(status="value", native_id=7, name="Original label")
    )
    other = record(2, SourceCoordinate(status="value", native_id="7"))
    changed = record(
        3, SourceCoordinate(status="value", native_id=7, name="Changed label")
    )
    unknown = record(4, SourceCoordinate(status="unknown"))
    records = (first, other, changed, unknown, first)
    root = tmp_path / "inputs" / "records"
    manifest = prepare_source_records(
        root, records=records, revisions=(revision,), scope="register slices"
    )
    commit = accept_prepared(root)
    reader = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=commit
    )
    assert dict(reader.iter_register_slices(revision.dataset)) == {
        source_register_key(first): (first, changed, first),
        source_register_key(other): (other,),
        None: (unknown,),
    }
    assert tuple(reader.iter_register_slices("missing")) == ()
    for selection, expected in (
        (
            {source_register_key(first)},
            ((source_register_key(first), (first, changed, first)),),
        ),
        ({None}, ((None, (unknown,)),)),
        (
            {source_register_key(other), None},
            ((None, (unknown,)), (source_register_key(other), (other,))),
        ),
        (set(), ()),
    ):
        selected = open_prepared_source_records(
            root, expected_sha256=manifest.sha256, input_commit=commit
        )
        primed = next(selected.iter_records())
        assert primed == first
        observed = tuple(selected.iter_register_slices(revision.dataset, selection))
        assert observed == expected
        if source_register_key(first) in selection:
            assert observed[0][1][0] is primed
        assert (
            tuple(selected.iter_register_slices(revision.dataset, selection))
            == expected
        )
    assert tuple(reader.records) == records
    assert reader.input_commit == commit
