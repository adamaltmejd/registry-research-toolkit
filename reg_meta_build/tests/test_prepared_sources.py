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
    open_prepared_source_records,
    prepare_source_records,
    prepared_source_paths,
)
from reg_meta_build.source_records import (
    CodeSetReference,
    DeliveredCell,
    NativeCoordinates,
    RecordLocator,
    SourceCoordinate,
    SourceFields,
    SourceRecord,
    SourceRevision,
    SourceSubject,
    TemporalScope,
    value_field,
)

from reg_meta_build import prepared_sources

if TYPE_CHECKING:
    from pathlib import Path


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
    revision: SourceRevision, *, row: int, member: str, raw_value: str
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
            population=SourceCoordinate(status="unknown"),
            variable=SourceCoordinate(status="value", native_id=f"variable:{member}"),
            member=SourceCoordinate(status="value", name=member),
            native=NativeCoordinates(),
        ),
        edition_scope=TemporalScope(kind="unknown", label="source did not declare"),
        edition_period_scope=TemporalScope(
            kind="unknown", label="source did not declare"
        ),
        fields=SourceFields(name=value_field(member, raw=raw_value)),
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
    document["schema_version"] = 3
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
    # A committed fixture intentionally violates preparation guarantees. The
    # decoder still checks structural references without replaying semantic hashes.
    commit = accept_prepared(root)
    opened = open_prepared_source_records(
        root, expected_sha256=manifest.sha256, input_commit=commit
    )
    with pytest.raises(PreparedSourceError, match="wrong-kind"):
        tuple(opened.records)


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
