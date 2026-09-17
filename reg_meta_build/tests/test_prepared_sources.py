"""Accepted compact evidence is lossless, indexed and cheap to reopen."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
from typing import TYPE_CHECKING

import pytest
from _prepared_fixtures import accept_prepared
from reg_meta_build.prepared_sources import (
    PreparedSourceError,
    PreparedSourceRecords,
    open_prepared_source_records,
    prepare_source_records,
    prepared_source_paths,
)
from reg_meta_build.source_coordinates import native_variable_key, source_register_key
from reg_meta_build.source_records import (
    CodeSetReference,
    DeliveredCell,
    NativeCoordinates,
    RecordLocator,
    SourceCoordinate,
    SourceEvidenceRow,
    SourceEvidenceTable,
    SourceFields,
    SourceRecord,
    SourceRevision,
    SourceSubject,
    TemporalScope,
    value_field,
)
from reg_meta_build.source_support import SourceSupportBindings, SourceSupportJoin

from reg_meta_build import _accepted_prepared, prepared_sources

if TYPE_CHECKING:
    from pathlib import Path
    from typing import Literal


def _revision(dataset: str, marker: str) -> SourceRevision:
    return SourceRevision.create(
        dataset=dataset,
        publisher="fixture publisher",
        purpose="prepared source artifact test",
        upstream_revision=f"revision-{marker}",
        artifact_path=f"{dataset}-{marker}.xlsx",
        artifact_size=123,
        artifact_sha256=marker * 64,
    )


def _record(
    revision: SourceRevision,
    *,
    row: int,
    member: str,
    raw_value: str,
    fields: SourceFields | None = None,
) -> SourceRecord:
    return SourceRecord.create(
        revision=revision,
        locators=(
            RecordLocator(
                semantic_record_key=(revision.dataset, member),
                physical_file=revision.artifact_path,
                physical_table="Variables",
                physical_record=f"row:{row}",
                physical_cells=(f"Variables!A{row}", f"Variables!B{row}"),
            ),
        ),
        subject=SourceSubject(
            provider="fixture",
            register=SourceCoordinate(status="value", name=revision.dataset),
            variant=SourceCoordinate(status="not_applicable"),
            variant_references=(
                SourceCoordinate(status="value", native_id="first"),
                SourceCoordinate(status="value", native_id="second"),
            ),
            population=SourceCoordinate(status="unknown"),
            variable=SourceCoordinate(status="value", native_id=f"variable:{member}"),
            member=SourceCoordinate(status="value", name=member),
            native=NativeCoordinates(),
        ),
        edition_scope=TemporalScope(kind="unknown", label="source did not declare"),
        edition_period_scope=TemporalScope(
            kind="unknown", label="source did not declare"
        ),
        fields=fields or SourceFields(name=value_field(member, raw=raw_value)),
        context=("fixture context",),
        language="sv",
        original_period_text=" original scope ",
        code_set_references=(
            CodeSetReference(
                reference_id="declared-list",
                content_sha256="1" * 64,
                physical_locator="Codes!A1:B8",
            ),
        ),
        delivered_cells=(
            DeliveredCell(
                name="Variable",
                present=True,
                raw_value=raw_value,
                interpreted_value=member,
                raw_type="str",
                storage_type="s",
                number_format="@",
                hyperlink_target="https://example.org/evidence",
                hyperlink_location="Code list!A1",
            ),
            DeliveredCell(
                name="Absent", present=False, raw_value=None, interpreted_value=""
            ),
            DeliveredCell(
                name="Empty", present=True, raw_value="", interpreted_value=""
            ),
        ),
    )


def _prepare(root: Path, *, records: tuple[SourceRecord, ...] | None = None):
    revision = _revision("source-a", "a")
    if records is None:
        records = (_record(revision, row=2, member="First", raw_value=" First "),)
    return prepare_source_records(
        root, records=iter(records), revisions=(revision,), scope="test selection"
    )


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


@pytest.mark.parametrize(
    "keys",
    [
        ("column_name",),
        ("variable_id",),
        ("register_name", "variant_name", "variable_name", "column_name"),
    ],
)
def test_support_projection_matches_full_record_cardinalities_without_hydration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    keys,
) -> None:
    revision = _revision("source-a", "a")

    def occurrence(row, native, column="VALUE"):
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
            not in {"record_id", "source", "source_revision_id", "subject", "fields"}
        }
        return SourceRecord.create(
            revision=revision,
            subject=subject,
            fields=SourceFields(column_name=value_field(column)),
            **arguments,
        )

    first, second, unknown = occurrence(1, 7), occurrence(2, "7"), occurrence(3, None)
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
        rule="literal fixture join",
        provenance=("fixture",),
    )
    # Support and delivery collections remain distinct, even with equal coordinates.
    support_record = first.model_copy(update={"source": "summary"})
    full, projected = (
        SourceSupportBindings((join,), (support_record,)),
        SourceSupportBindings((join,), (support_record,)),
    )
    for item in records:
        full.observe(item)
    full.seal()

    def no_hydration(*args):
        raise AssertionError("support projection hydrated a physical source record")

    monkeypatch.setattr(prepared_sources, "_read_record", no_hydration)
    targets = tuple(reader.iter_support_targets((join,)))
    assert len(targets) < len(records)
    assert any(t.variable_key is None for t in targets)
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
    monkeypatch: pytest.MonkeyPatch,
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
    decoded = []
    original_reader = prepared_sources._read_record

    def tracked(*args):
        record = original_reader(*args)
        decoded.append(record)
        return record

    monkeypatch.setattr(prepared_sources, "_read_record", tracked)
    stream = reader.iter_native_families("source-a")
    key, family = next(stream)
    assert key == native_variable_key(first)
    assert family == (first, changed, first)
    assert decoded == list(family)
    remaining = dict(stream)
    assert remaining == {native_variable_key(other): (other,)}
    assert tuple(reader.iter_without_native_family("source-a")) == (unplaced,)
    assert dict(reader.iter_native_families("source-b")) == {
        native_variable_key(second): (second,)
    }
    assert tuple(reader.records) == records
    assert tuple(reader.iter_native_families("missing-source")) == ()


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
    assert tuple(reader.records) == records
    assert reader.input_commit == commit


def test_prepared_artifact_preserves_order_duplicates_and_exact_evidence(
    tmp_path: Path,
) -> None:
    first_revision = _revision("source-a", "a")
    second_revision = _revision("source-b", "b")
    first = _record(first_revision, row=9, member="First", raw_value=" First ")
    second = _record(second_revision, row=3, member="Second", raw_value="Second")
    records = (second, first, second)
    first_path = tmp_path / "a" / "records"
    second_path = tmp_path / "b" / "records"
    manifest = prepare_source_records(
        first_path,
        records=iter(records),
        revisions=(second_revision, first_revision),
        scope="two sources",
    )
    repeated = prepare_source_records(
        second_path,
        records=iter(records),
        revisions=(first_revision, second_revision),
        scope="two sources",
    )
    assert manifest == repeated
    for left, right in zip(
        prepared_source_paths(first_path),
        prepared_source_paths(second_path),
        strict=True,
    ):
        assert left.read_bytes() == right.read_bytes()
    commit = accept_prepared(first_path)
    opened = open_prepared_source_records(
        first_path, expected_sha256=manifest.sha256, input_commit=commit
    )
    assert len(opened) == 3
    assert tuple(opened.records) == records
    assert tuple(opened.records) == records  # Each access starts a fresh stream.
    assert tuple(
        opened.lookup(second.source, second.locators[0].semantic_record_key)
    ) == (second, second)
    assert tuple(opened.iter_records(source=first.source)) == (first,)
    assert tuple(opened.lookup(first.source, ("absent",))) == ()


def test_preparation_keeps_occurrence_order_as_evidence(tmp_path: Path) -> None:
    revision = _revision("source-a", "a")
    first = _record(revision, row=9, member="First", raw_value="First")
    second = _record(revision, row=3, member="Second", raw_value="Second")
    original = _prepare(tmp_path / "a" / "records", records=(first, second))
    reversed_manifest = _prepare(tmp_path / "b" / "records", records=(second, first))
    assert original.database_sha256 != reversed_manifest.database_sha256


def test_warm_open_neither_hashes_payloads_nor_rechecks_record_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root)
    commit = accept_prepared(root)

    def forbidden(*_args: object, **_kwargs: object):
        raise AssertionError("expensive preparation work called during warm use")

    monkeypatch.setattr(prepared_sources, "_file_sha256", forbidden)
    monkeypatch.setattr(SourceRecord, "_record_id", staticmethod(forbidden))
    git = _accepted_prepared._git

    def no_hash_object(repo: Path, *args: str) -> str:
        assert "hash-object" not in args
        return git(repo, *args)

    monkeypatch.setattr(_accepted_prepared, "_git", no_hash_object)
    opened = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=commit
    )
    assert len(tuple(opened.records)) == 1
    name = next(opened.lookup("source-a", ("source-a", "First"))).fields.name
    assert name is not None and name.value == "First"


def test_prepare_validates_identity_revision_membership_and_cleans_failure(
    tmp_path: Path,
) -> None:
    revision = _revision("source-a", "a")
    record = _record(revision, row=2, member="First", raw_value="First")
    output = tmp_path / "records"
    with pytest.raises(PreparedSourceError, match="revision absent"):
        prepare_source_records(
            output, records=(record,), revisions=(), scope="missing revision"
        )
    assert not output.exists()
    assert list(tmp_path.iterdir()) == []
    invalid = record.model_copy(update={"record_id": "tampered"})
    with pytest.raises(PreparedSourceError, match="SourceRecord contract"):
        prepare_source_records(
            output, records=(invalid,), revisions=(revision,), scope="invalid identity"
        )
    assert list(tmp_path.iterdir()) == []
    with pytest.raises(PreparedSourceError, match="duplicate source revision"):
        prepare_source_records(
            output,
            records=(record,),
            revisions=(revision, revision),
            scope="duplicate revision",
        )


def test_existing_candidate_is_immutable_and_atomic_failure_leaves_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "existing"
    _prepare(output)
    before = tuple(path.read_bytes() for path in prepared_source_paths(output))
    with pytest.raises(PreparedSourceError, match="already exists"):
        _prepare(output)
    assert tuple(path.read_bytes() for path in prepared_source_paths(output)) == before

    def fail_replace(*_args: object, **_kwargs: object) -> None:
        raise OSError("injected replace failure")

    monkeypatch.setattr(type(output), "replace", fail_replace)
    with pytest.raises(OSError, match="injected replace failure"):
        _prepare(tmp_path / "new")
    assert sorted(path.name for path in tmp_path.iterdir()) == ["existing"]


def test_open_requires_accepted_exact_commit_and_manifest(tmp_path: Path) -> None:
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root)
    with pytest.raises(PreparedSourceError, match="Git|git"):
        open_prepared_source_records(
            root, expected_sha256=manifest.sha256, input_commit="a" * 40
        )
    commit = accept_prepared(root)
    with pytest.raises(PreparedSourceError, match="commit pin mismatch"):
        open_prepared_source_records(
            root, expected_sha256=manifest.sha256, input_commit="a" * 40
        )
    with pytest.raises(PreparedSourceError, match="hash mismatch"):
        open_prepared_source_records(
            root, expected_sha256="0" * 64, input_commit=commit
        )
    path = root / "manifest.json"
    document = json.loads(path.read_bytes())
    document["schema_version"] = 999
    payload = json.dumps(document).encode()
    path.write_bytes(payload)
    commit = accept_prepared(root)
    with pytest.raises(PreparedSourceError, match="schema_version"):
        open_prepared_source_records(
            root,
            expected_sha256=hashlib.sha256(payload).hexdigest(),
            input_commit=commit,
        )


@pytest.mark.parametrize(
    "change", ["dirty", "missing", "untracked", "assume-unchanged", "skip-worktree"]
)
def test_accepted_worktree_changes_and_hidden_index_flags_fail_closed(
    tmp_path: Path, change: str
) -> None:
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root)
    commit = accept_prepared(root)
    database = root / "files" / "records.sqlite"
    if change == "dirty":
        with database.open("ab") as handle:
            handle.write(b"changed")
    elif change == "missing":
        database.unlink()
    elif change == "untracked":
        (root / "files" / "extra.sqlite").write_bytes(b"unexpected")
    else:
        _git(root.parent, "update-index", f"--{change}", "records/files/records.sqlite")
    with pytest.raises(PreparedSourceError, match="clean|materialized"):
        open_prepared_source_records(
            root, expected_sha256=manifest.sha256, input_commit=commit
        )


def test_wrong_kind_payload_fails_at_decode_boundary(tmp_path: Path) -> None:
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root)
    with sqlite3.connect(root / "files" / "records.sqlite") as conn:
        conn.execute("UPDATE occurrence SET fields_payload = scope_payload")
    # Exercise the decoder directly: the public opener rejects any committed
    # database change that lacks a matching preparation proof.
    opened = PreparedSourceRecords(root=root, manifest=manifest, input_commit="a" * 40)
    with pytest.raises(PreparedSourceError, match="wrong-kind"):
        tuple(opened.records)


def test_committed_same_kind_payload_edit_invalidates_preparation_proof(
    tmp_path: Path,
) -> None:
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root)
    accept_prepared(root)
    database = root / "files" / "records.sqlite"
    with sqlite3.connect(database) as conn:
        key, body = conn.execute(
            "SELECT id, body FROM payload WHERE kind='field'"
        ).fetchone()
        changed = json.loads(body)
        changed["value"] = "Other"
        conn.execute("UPDATE payload SET body=? WHERE id=?", (json.dumps(changed), key))
    assert database.stat().st_size == manifest.database_size
    commit = accept_prepared(root)
    with pytest.raises(PreparedSourceError, match="preparation proof"):
        open_prepared_source_records(
            root, expected_sha256=manifest.sha256, input_commit=commit
        )


@pytest.mark.parametrize("accepted", [False, True])
def test_extra_payload_is_rejected_even_when_ignored_or_committed(
    tmp_path: Path, accepted: bool
) -> None:
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root)
    commit = accept_prepared(root)
    (root / "files" / "extra.sqlite").write_bytes(b"unexpected")
    if accepted:
        commit = accept_prepared(root)
    else:
        (root.parent / ".git" / "info" / "exclude").write_text("extra.sqlite\n")
    with pytest.raises(PreparedSourceError, match="inventory"):
        open_prepared_source_records(
            root, expected_sha256=manifest.sha256, input_commit=commit
        )


@pytest.mark.parametrize("member", ["manifest.json", "files/records.sqlite"])
def test_committed_symlink_cannot_replace_a_prepared_member(
    tmp_path: Path, member: str
) -> None:
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root)
    path = root / member
    outside = tmp_path / "original"
    outside.write_bytes(path.read_bytes())
    path.unlink()
    path.symlink_to(outside)
    commit = accept_prepared(root)
    with pytest.raises(PreparedSourceError):
        open_prepared_source_records(
            root, expected_sha256=manifest.sha256, input_commit=commit
        )


@pytest.mark.parametrize("flag", ["assume-unchanged", "skip-worktree"])
def test_manifest_index_flags_cannot_hide_its_worktree_state(
    tmp_path: Path, flag: str
) -> None:
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root)
    commit = accept_prepared(root)
    _git(root.parent, "update-index", f"--{flag}", "records/manifest.json")
    with pytest.raises(PreparedSourceError, match="materialized"):
        open_prepared_source_records(
            root, expected_sha256=manifest.sha256, input_commit=commit
        )


def test_repeated_large_evidence_is_interned_without_eager_record_collection(
    tmp_path: Path,
) -> None:
    revision = _revision("source-a", "a")
    evidence = "Long provider documentation. " * 400
    consumed = 0

    def records():
        nonlocal consumed
        for row in range(2, 302):
            consumed += 1
            yield _record(revision, row=row, member="Repeated", raw_value=evidence)

    root = tmp_path / "inputs" / "records"
    manifest = prepare_source_records(
        root, records=records(), revisions=(revision,), scope="repeated evidence"
    )
    assert consumed == manifest.record_count == 300
    expanded = sum(len(record.model_dump_json().encode()) for record in records())
    assert manifest.database_size < expanded // 8
    commit = accept_prepared(root)
    opened = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=commit
    )
    assert not isinstance(opened.records, tuple | list)
    assert next(opened.records).delivered_cells[0].raw_value == evidence


@pytest.mark.parametrize(
    "cells",
    [
        (),
        ("Sheet!A1",),
        ("A1", "B9"),
        ("source:row:234:name", "source:row:234:definition"),
        ("Årsdata!Ö2", "Årsdata!Ö2", "Årsdata!Ö3"),
    ],
)
def test_locator_factoring_preserves_every_physical_coordinate(
    tmp_path: Path, cells: tuple[str, ...]
) -> None:
    revision = _revision("source-a", "a")
    record = _record(revision, row=2, member="First", raw_value=" First ")
    record = record.model_copy(
        update={
            "locators": (
                record.locators[0].model_copy(update={"physical_cells": cells}),
            )
        }
    )
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root, records=(record,))
    opened = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=accept_prepared(root)
    )
    assert tuple(opened.records) == (record,)


def test_shared_field_text_is_not_repeated_for_changed_field_combinations(
    tmp_path: Path,
) -> None:
    revision = _revision("source-a", "a")
    definition = value_field("Long shared source definition. " * 800)
    records = tuple(
        _record(
            revision,
            row=row,
            member="First",
            raw_value=" First ",
            fields=SourceFields(
                definition=definition,
                operational_definition=value_field(f"edition {row}"),
            ),
        )
        for row in range(2, 102)
    )
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root, records=records)
    expanded_size = sum(len(record.model_dump_json().encode()) for record in records)
    assert manifest.database_size < expanded_size // 4
    opened = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=accept_prepared(root)
    )
    assert tuple(opened.records) == records


def test_field_factoring_preserves_raw_boolean_and_integer_types(
    tmp_path: Path,
) -> None:
    revision = _revision("source-a", "a")
    records = tuple(
        _record(
            revision,
            row=row,
            member="Same",
            raw_value="Same",
            fields=SourceFields(name=value_field("Same", raw=raw)),
        )
        for row, raw in enumerate((True, 1, False, 0), start=2)
    )
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root, records=records)
    opened = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=accept_prepared(root)
    )
    # Pydantic/Python equality equates True with 1; JSON preserves the source type.
    assert tuple(record.model_dump_json() for record in opened.records) == tuple(
        record.model_dump_json() for record in records
    )


def _evidence_table(
    revision: SourceRevision, *, name: str = "Raw source sheet"
) -> SourceEvidenceTable:
    record = _record(revision, row=4, member="Item", raw_value=" Item ")
    rows = []
    roles: tuple[Literal["header", "declaration", "unparsed", "data"], ...] = (
        "header",
        "declaration",
        "unparsed",
        "data",
    )
    for position, role in enumerate(roles, start=1):
        locator = RecordLocator(
            semantic_record_key=(revision.dataset, name, role),
            physical_file=revision.artifact_path,
            physical_table=name,
            physical_record=f"row:{position}",
            physical_cells=tuple(
                f"{name}!{column}{position}" for column in ("A", "B", "C")
            ),
        )
        rows.append(
            SourceEvidenceRow(locator=locator, role=role, cells=record.delivered_cells)
        )
    rows.append(rows[-1])  # Repeated physical observations must not be deduplicated.
    return SourceEvidenceTable(
        source=revision.dataset,
        source_revision_id=revision.revision_id,
        name=name,
        rows=tuple(rows),
    )


def test_evidence_tables_stream_and_roundtrip_with_rows_roles_and_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = _revision("source-a", "a")
    second = _revision("source-b", "b")
    table = _evidence_table(first)
    empty = SourceEvidenceTable(
        source=second.dataset,
        source_revision_id=second.revision_id,
        name="Empty sheet",
        rows=(),
    )
    tables = (table, empty, table)
    root = tmp_path / "inputs" / "records"
    repeated = tmp_path / "repeated" / "records"
    manifests = tuple(
        prepare_source_records(
            path,
            records=(),
            revisions=(first, second),
            scope="evidence tables",
            tables=iter(tables),
        )
        for path in (root, repeated)
    )
    assert manifests[0] == manifests[1]
    assert manifests[0].record_count == 0
    assert manifests[0].table_count == 3
    assert manifests[0].table_row_count == 10
    for left, right in zip(
        prepared_source_paths(root), prepared_source_paths(repeated), strict=True
    ):
        assert left.read_bytes() == right.read_bytes()
    commit = accept_prepared(root)

    def forbidden(*_args: object, **_kwargs: object):
        raise AssertionError("preparation-only hash called by evidence reader")

    monkeypatch.setattr(prepared_sources, "_file_sha256", forbidden)
    monkeypatch.setattr(SourceRecord, "_record_id", staticmethod(forbidden))
    reader = open_prepared_source_records(
        root, expected_sha256=manifests[0].sha256, input_commit=commit
    )
    assert tuple(reader.records) == ()
    assert tuple(reader.iter_tables()) == tables
    assert tuple(reader.iter_tables()) == tables
    assert tuple(reader.iter_tables(source=first.dataset)) == (table, table)
    assert tuple(reader.iter_tables(source=second.dataset)) == (empty,)
    assert tuple(reader.iter_tables(source="absent-source")) == ()


@pytest.mark.parametrize("fault", ["missing-revision", "wrong-source", "invalid-row"])
def test_evidence_preparation_validates_source_membership_and_row_contracts(
    tmp_path: Path, fault: str
) -> None:
    revision = _revision("source-a", "a")
    table = _evidence_table(revision)
    if fault == "missing-revision":
        table = table.model_copy(
            update={"source_revision_id": "source-a@sha256:" + "f" * 64}
        )
    elif fault == "wrong-source":
        table = table.model_copy(update={"source": "another-source"})
    else:
        table = table.model_copy(
            update={
                "rows": (table.rows[0].model_copy(update={"role": "invented-role"}),)
            }
        )
    with pytest.raises(
        PreparedSourceError, match="source table|SourceEvidenceTable contract"
    ):
        prepare_source_records(
            tmp_path / "records",
            records=(),
            revisions=(revision,),
            scope="invalid evidence",
            tables=(table,),
        )
    assert list(tmp_path.iterdir()) == []


def test_evidence_reader_decodes_only_requested_tables_and_checks_payload_refs(
    tmp_path: Path,
) -> None:
    first = _revision("source-a", "a")
    second = _revision("source-b", "b")
    first_table = _evidence_table(first)
    second_table = _evidence_table(second)
    root = tmp_path / "inputs" / "records"
    manifest = prepare_source_records(
        root,
        records=(),
        revisions=(first, second),
        scope="lazy table decoder",
        tables=(first_table, second_table),
    )
    with sqlite3.connect(root / "files/records.sqlite") as conn:
        # Exercise structural decoding directly, below the public opener's
        # immutable preparation proof. A strings payload is not a DeliveredCell.
        conn.execute(
            "UPDATE evidence_cell SET payload = (SELECT cells_payload FROM evidence_row WHERE ordinal = evidence_cell.row_ordinal) WHERE row_ordinal IN (SELECT ordinal FROM evidence_row WHERE table_ordinal = 2)"
        )
    reader = PreparedSourceRecords(root=root, manifest=manifest, input_commit="a" * 40)
    assert tuple(reader.iter_tables(source=first.dataset)) == (first_table,)
    stream = reader.iter_tables()
    assert next(stream) == first_table
    with pytest.raises(PreparedSourceError, match="wrong-kind"):
        next(stream)


def test_records_and_tables_share_cells_without_losing_either_surface(
    tmp_path: Path,
) -> None:
    revision = _revision("source-a", "a")
    record = _record(revision, row=4, member="Item", raw_value=" Item ")
    table = _evidence_table(revision)
    root = tmp_path / "inputs" / "records"
    manifest = prepare_source_records(
        root,
        records=(record,),
        revisions=(revision,),
        scope="shared raw evidence",
        tables=(table,),
    )
    with sqlite3.connect(root / "files/records.sqlite") as conn:
        assert conn.execute(
            "SELECT count(*) FROM payload WHERE kind='cell'"
        ).fetchone()[0] == len(record.delivered_cells)
    commit = accept_prepared(root)
    reader = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=commit
    )
    assert tuple(reader.records) == (record,)
    assert tuple(reader.iter_tables()) == (table,)
