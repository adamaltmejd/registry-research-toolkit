"""Accept synthetic prepared artifacts at the same Git boundary as real inputs.

Also holds the shared source-record inputs and the process-boundary recorders (Git
argv via ``subprocess.run``, Python file opens via ``io.open``/``builtins.open``) that
the prepared-input tests use to observe warm opens and preparation.
"""

from __future__ import annotations

import builtins
import io
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING

from reg_meta.source_evidence import DeliveredCell, RecordLocator, SourceRevision
from reg_meta_build.prepared_sources import prepare_source_records
from reg_meta_build.source_records import (
    CodeSetReference,
    NativeCoordinates,
    SourceCoordinate,
    SourceFields,
    SourceRecord,
    SourceSubject,
    TemporalScope,
    value_field,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    import pytest


def accept_prepared(root: Path) -> str:
    repository = root.parent
    if not (repository / ".git").exists():
        subprocess.run(["git", "init", "-q", "-b", "main", str(repository)], check=True)
        for key, value in (
            ("user.name", "Prepared fixture"),
            ("user.email", "prepared@example.invalid"),
            ("core.autocrlf", "false"),
        ):
            subprocess.run(
                ["git", "-C", str(repository), "config", key, value], check=True
            )
    subprocess.run(["git", "-C", str(repository), "add", "--", root.name], check=True)
    subprocess.run(
        ["git", "-C", str(repository), "commit", "-q", "-m", "Accept fixture inputs"],
        check=True,
    )
    return subprocess.check_output(
        ["git", "-C", str(repository), "rev-parse", "HEAD"], text=True
    ).strip()


def record_git_calls(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, ...]]:
    """Record the argv of every Git process started through ``subprocess.run``."""
    calls: list[tuple[str, ...]] = []
    real = subprocess.run

    def recorded(args, *rest, **kwargs):
        if isinstance(args, list | tuple) and args and args[0] == "git":
            calls.append(tuple(map(str, args)))
        return real(args, *rest, **kwargs)

    monkeypatch.setattr(subprocess, "run", recorded)
    return calls


def record_file_opens(
    monkeypatch: pytest.MonkeyPatch,
    on_open: Callable[[Path, str], None] | None = None,
) -> list[tuple[Path, str]]:
    """Record Python-level opens of paths; ``on_open`` runs before each one.

    SQLite opens its databases in C, so a Python open of a prepared ``.sqlite`` payload
    is a hash or copy, never a query.
    """
    opened: list[tuple[Path, str]] = []
    real = io.open

    def recorded(file, mode="r", *args, **kwargs):
        if isinstance(file, str | Path):
            opened.append((Path(file), mode))
            if on_open is not None:
                on_open(Path(file), mode)
        return real(file, mode, *args, **kwargs)

    monkeypatch.setattr(io, "open", recorded)
    monkeypatch.setattr(builtins, "open", recorded)
    return opened


def prepared_revision(dataset: str, marker: str) -> SourceRevision:
    return SourceRevision.create(
        dataset=dataset,
        publisher="fixture publisher",
        purpose="prepared source artifact test",
        upstream_revision=f"revision-{marker}",
        artifact_path=f"{dataset}-{marker}.xlsx",
        artifact_size=123,
        artifact_sha256=marker * 64,
    )


def prepared_record(
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


def prepare_records(root: Path, *, records: tuple[SourceRecord, ...] | None = None):
    revision = prepared_revision("source-a", "a")
    if records is None:
        records = (
            prepared_record(revision, row=2, member="First", raw_value=" First "),
        )
    return prepare_source_records(
        root, records=iter(records), revisions=(revision,), scope="test selection"
    )
