"""Write `records.json`, the Rust digest golden, from the Python builder.

Synthetic records go through the real preparation writer and reader, and the Python
`evidence_sha256` of each read-back record is the expected digest. The fixture holds
the v16 schema, every row of the prepared database and those digests, so the Rust test
rebuilds the same database and must reproduce each digest.

Run from the repository root: `uv run python crates/reg-build-model/tests/digest/generate.py`.
Rerun only when the v16 contract or the digest rule changes; review the diff.
"""

from __future__ import annotations

import json
import sqlite3
import tempfile
from pathlib import Path

from reg_meta_build.prepared_sources import (
    PreparedSourceRecords,
    prepare_source_records,
)
from reg_meta_build.source_evidence import (
    DeliveredCell,
    RecordLocator,
    SourceField,
    SourceRevision,
    evidence_sha256,
)
from reg_meta_build.source_records import (
    NativeCoordinates,
    ScopeInterval,
    SourceCoordinate,
    SourceFieldCells,
    SourceFields,
    SourceParentObservation,
    SourceRecord,
    SourceSubject,
    TemporalScope,
    value_field,
)

OUT = Path(__file__).with_name("records.json")
TABLES = ("payload", "occurrence", "locator", "delivered_cell")


def revision(dataset: str) -> SourceRevision:
    return SourceRevision.create(
        dataset=dataset,
        publisher="fixture publisher",
        purpose="digest golden",
        upstream_revision="fixture",
        artifact_path=f"{dataset}.xlsx",
        artifact_size=1,
        artifact_sha256="a" * 64,
    )


def coordinate(native_id: int | str | None = None, name: str | None = None):
    return SourceCoordinate(status="value", native_id=native_id, name=name)


UNKNOWN = SourceCoordinate(status="unknown")
NOT_APPLICABLE = SourceCoordinate(status="not_applicable")
NONE_SCOPE = TemporalScope(kind="not_applicable")


def cell(name: str, raw: str | None, **extra: str) -> DeliveredCell:
    return DeliveredCell(
        name=name,
        present=raw is not None,
        raw_value=raw,
        interpreted_value=raw or "",
        **extra,
    )


def locator(key: tuple[str, ...], file: str, record: str, cells: tuple[str, ...]):
    return RecordLocator(
        semantic_record_key=key,
        physical_file=file,
        physical_table="Sheet",
        physical_record=record,
        physical_cells=cells,
    )


def scb() -> SourceRecord:
    """Native ints, an edition parent with cell positions, unsorted multi-locators."""
    edition = coordinate(18774, "Lisa 2019 (preliminär)")
    register, variant = (
        coordinate(34, "LISA"),
        coordinate(1, "Individer 16 år och äldre"),
    )
    key = ("34", "1", "18774", "3811")
    cells = (cell("Kolumnnamn", "DispInk04"), cell("Registerversionnamn", "Lisa 2019"))
    return SourceRecord.create(
        revision=revision("scb-fixture"),
        locators=(
            locator(key, "b.csv", "9", ("DispInk04", "Lisa 2019")),
            locator(key, "a.csv", "2", ("DispInk04", "Lisa 2019")),
        ),
        subject=SourceSubject(
            provider="scb",
            register=register,
            variant=variant,
            population=coordinate(name="Personer"),
            variable=coordinate(3811, "Disponibel inkomst"),
            member=coordinate(77, None),
            native=NativeCoordinates(
                register_id=34,
                register_variant_id=1,
                edition_id=18774,
                variable_id=3811,
                member_id=77,
            ),
        ),
        edition_scope=TemporalScope(
            kind="intervals",
            intervals=(ScopeInterval(start="2019-01-01", end="2019-12-31"),),
        ),
        edition_period_scope=NONE_SCOPE,
        fields=SourceFields(
            column_name=value_field("DispInk04"),
            name=value_field("Disponibel inkomst"),
            identifier=value_field(False),
            data_length=value_field("12", raw=12),
        ),
        parent_facts=(
            SourceParentObservation(
                kind="edition",
                coordinate=edition,
                register=register,
                variant=variant,
                edition=edition,
                fields=SourceFields(source_version=value_field("Lisa 2019")),
                field_cells=(SourceFieldCells(field="source_version", positions=(1,)),),
            ),
        ),
        context=("34", "1", "Lisa 2019 (preliminär)"),
        delivered_cells=cells,
    )


def sos() -> SourceRecord:
    """Names only, non-ASCII and non-BMP text, control characters, cached cells."""
    return SourceRecord.create(
        revision=revision("sos-fixture"),
        locators=(locator(("LÖN", "Åtgärd"), "sos.xlsx", "R4", ("C4",)),),
        subject=SourceSubject(
            provider="sos",
            register=coordinate(name="Läkemedelsregistret 𝔘"),
            variant=coordinate(name="Uttag\tvecka"),
            population=UNKNOWN,
            variable=coordinate(name="ÅTGÄRD"),
            member=coordinate(name="ÅTGÄRD"),
            native=NativeCoordinates(),
        ),
        edition_scope=TemporalScope(
            kind="pooled",
            label="1995–2023",
            pooled_start="1995-01-01",
            pooled_end="2023-12-31",
        ),
        edition_period_scope=NONE_SCOPE,
        fields=SourceFields(
            column_name=value_field("ÅTGÄRD"),
            description=value_field('Rad 1\nRad 2\t"citat" \\ \u0001\u001f\u007f'),
            sensitivity=SourceField(status="unknown", raw_value="okänd"),
        ),
        language="sv",
        original_period_text="1995-",
        delivered_cells=(
            cell(
                "Kolumn",
                "=A1",
                raw_type="f",
                cached_raw_value="ÅTGÄRD",
                cached_raw_type="s",
                hyperlink_target="https://example.invalid/ä",
            ),
        ),
    )


def thin() -> SourceRecord:
    """Authored keys, a not-applicable variant, references, a negative column."""
    return SourceRecord.create(
        revision=revision("providers/fk.toml"),
        locators=(locator(("aktivitetsstod", "BELOPP"), "fk.toml", "7", ("BELOPP",)),),
        subject=SourceSubject(
            provider="fk",
            register=coordinate("aktivitetsstod", "Aktivitetsstöd"),
            variant=NOT_APPLICABLE,
            variant_references=(coordinate("utbetalningar"), UNKNOWN),
            population=UNKNOWN,
            variable=coordinate(name="BELOPP"),
            member=coordinate(name="BELOPP"),
            native=NativeCoordinates(),
        ),
        edition_scope=TemporalScope(kind="year_independent"),
        edition_period_scope=TemporalScope(
            kind="intervals",
            intervals=(
                ScopeInterval(start="2010-01-01", end="2012-12-31"),
                ScopeInterval(start="2015-01-01", end=None),
            ),
        ),
        fields=SourceFields(
            column_name=SourceField(status="negative", raw_value="—"),
            identifier=value_field(True),
        ),
        language="en",
        context=(),
    )


def unknown_scope() -> SourceRecord:
    """An unknown scope label and every coordinate unknown."""
    return SourceRecord.create(
        revision=revision("sos-fixture"),
        locators=(locator(("X",), "sos.xlsx", "R9", ("C9",)),),
        subject=SourceSubject(
            provider="sos",
            register=coordinate(name="Cancerregistret"),
            variant=UNKNOWN,
            population=UNKNOWN,
            member=UNKNOWN,
            native=NativeCoordinates(),
        ),
        edition_scope=TemporalScope(kind="unknown", label="se kommentar"),
        edition_period_scope=NONE_SCOPE,
        fields=SourceFields(availability=value_field(True)),
    )


def main() -> None:
    records = (scb(), sos(), thin(), unknown_scope())
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "records"
        manifest = prepare_source_records(
            root,
            records=records,
            revisions=tuple(
                {
                    record.source_revision_id: revision(record.source)
                    for record in records
                }.values()
            ),
            scope="digest golden",
        )
        # Constructed directly: acceptance is a Git boundary the digest does not need.
        reader = PreparedSourceRecords(
            root=root, manifest=manifest, input_commit="0" * 40
        )
        expected = [
            {"record_id": record.record_id, "evidence_sha256": evidence_sha256(record)}
            for record in reader.iter_records()
        ]
        with sqlite3.connect(root / "files/records.sqlite") as conn:
            schema = [
                sql
                for (sql,) in conn.execute(
                    "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY rowid"
                )
            ]
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            rows = {
                table: [
                    [
                        value.hex() if isinstance(value, bytes) else value
                        for value in row
                    ]
                    for row in conn.execute(f"SELECT * FROM {table} ORDER BY rowid")
                ]
                if table in {"payload", "occurrence"}
                else [list(row) for row in conn.execute(f"SELECT * FROM {table}")]
                for table in TABLES
            }
    OUT.write_text(
        json.dumps(
            {
                "user_version": version,
                "schema": schema,
                "rows": rows,
                "expected": expected,
            },
            ensure_ascii=False,
            indent=1,
        )
        + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
