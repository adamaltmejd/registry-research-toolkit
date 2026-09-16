"""Prepared source artifacts for bounded warm-read slices."""

from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from typing import TYPE_CHECKING, Any

import pytest
from reg_meta_build.prepared_sources import (
    PreparedSourceError,
    open_prepared_source_records,
    prepare_source_records,
)
from reg_meta_build.source_records import (
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

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
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
            member=SourceCoordinate(status="value", name=member),
            native=NativeCoordinates(),
        ),
        edition_scope=TemporalScope(kind="unknown", label="source did not declare"),
        edition_period_scope=TemporalScope(
            kind="unknown", label="source did not declare"
        ),
        fields=SourceFields(name=value_field(member, raw=raw_value)),
        context=("bounded fixture slice",),
        delivered_cells=(
            DeliveredCell(
                name="Variable",
                present=True,
                raw_value=raw_value,
                interpreted_value=member,
                raw_type="str",
                storage_type="s",
                number_format="@",
                hyperlink_location="Code list!A1",
            ),
        ),
    )


def _serialized_models(
    values: Iterable[SourceRecord | SourceRevision],
) -> Counter[str]:
    return Counter(
        json.dumps(value.model_dump(mode="json"), ensure_ascii=False, sort_keys=True)
        for value in values
    )


def _rewrite_artifact(path: Path, transform: Callable[[dict[str, Any]], None]) -> str:
    payload = json.loads(path.read_bytes())
    transform(payload)
    encoded = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode()
    path.write_bytes(encoded)
    return hashlib.sha256(encoded).hexdigest()


def test_prepared_source_artifact_is_deterministic_and_lossless(tmp_path: Path) -> None:
    first_revision = _revision("source-a", "a")
    second_revision = _revision("source-b", "b")
    first = _record(first_revision, row=9, member="First", raw_value=" First ")
    second = _record(second_revision, row=3, member="Second", raw_value="Second")
    records = (first, second, second)
    revisions = (second_revision, first_revision)

    first_path = tmp_path / "first.json"
    second_path = tmp_path / "second.json"
    first_manifest = prepare_source_records(
        first_path,
        records=records,
        revisions=revisions,
        scope="two-source variable fixture; partial slice",
    )
    second_manifest = prepare_source_records(
        second_path,
        records=reversed(records),
        revisions=tuple(reversed(revisions)),
        scope="two-source variable fixture; partial slice",
    )

    assert first_path.read_bytes() == second_path.read_bytes()
    assert first_manifest.artifact_sha256 == second_manifest.sha256
    assert first_manifest.partial is True
    assert first_manifest.record_count == 3

    opened = open_prepared_source_records(
        first_path, expected_sha256=first_manifest.sha256
    )
    assert opened.manifest == first_manifest
    assert _serialized_models(opened.manifest.revisions) == _serialized_models(
        revisions
    )
    assert _serialized_models(opened.records) == _serialized_models(records)
    assert sum(record == second for record in opened.records) == 2
    preserved = next(record for record in opened.records if record == first)
    assert preserved.locators == first.locators
    assert preserved.delivered_cells == first.delivered_cells


def test_prepare_rejects_bad_identity_and_absent_revision_without_replacing(
    tmp_path: Path,
) -> None:
    revision = _revision("source-a", "a")
    record = _record(revision, row=2, member="First", raw_value="First")
    output = tmp_path / "prepared.json"
    output.write_bytes(b"prior artifact")

    with pytest.raises(PreparedSourceError, match="revision absent"):
        prepare_source_records(
            output,
            records=(record,),
            revisions=(),
            scope="missing revision fixture",
        )
    assert output.read_bytes() == b"prior artifact"

    invalid = record.model_copy(update={"record_id": "tampered"})
    with pytest.raises(PreparedSourceError, match="SourceRecord contract"):
        prepare_source_records(
            output,
            records=(invalid,),
            revisions=(revision,),
            scope="invalid identity fixture",
        )
    assert output.read_bytes() == b"prior artifact"


def test_open_checks_exact_hash_version_and_revision_membership(tmp_path: Path) -> None:
    revision = _revision("source-a", "a")
    record = _record(revision, row=2, member="First", raw_value="First")
    output = tmp_path / "prepared.json"
    manifest = prepare_source_records(
        output,
        records=(record,),
        revisions=(revision,),
        scope="reader checks fixture",
    )

    with pytest.raises(PreparedSourceError, match="hash mismatch"):
        open_prepared_source_records(output, expected_sha256="0" * 64)

    unsupported_sha256 = _rewrite_artifact(
        output, lambda payload: payload.update(schema_version=2)
    )
    with pytest.raises(PreparedSourceError, match="unsupported.*version 2"):
        open_prepared_source_records(output, expected_sha256=unsupported_sha256)

    prepare_source_records(
        output,
        records=(record,),
        revisions=(revision,),
        scope="reader checks fixture",
    )
    absent_sha256 = _rewrite_artifact(
        output, lambda payload: payload.update(revisions=[])
    )
    with pytest.raises(PreparedSourceError, match="revision absent"):
        open_prepared_source_records(output, expected_sha256=absent_sha256)

    assert manifest.sha256 != absent_sha256


def test_atomic_replace_failure_preserves_prior_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    revision = _revision("source-a", "a")
    record = _record(revision, row=2, member="First", raw_value="First")
    output = tmp_path / "prepared.json"
    output.write_bytes(b"prior artifact")

    def fail_replace(*_args: object, **_kwargs: object) -> None:
        raise OSError("injected replace failure")

    monkeypatch.setattr(os, "replace", fail_replace)
    with pytest.raises(OSError, match="injected replace failure"):
        prepare_source_records(
            output,
            records=(record,),
            revisions=(revision,),
            scope="atomic failure fixture",
        )

    assert output.read_bytes() == b"prior artifact"
    assert list(tmp_path.iterdir()) == [output]
