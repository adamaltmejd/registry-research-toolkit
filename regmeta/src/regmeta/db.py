"""Database schema, CSV import, and connection management for regmeta."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from .errors import EXIT_CONFIG, RegmetaError

SCHEMA_VERSION = "1.1.0"
DB_FILENAME = "regmeta.db"

# Bytes undefined in cp1252 but present in SCB data as DOS cp850 remnants.
# Map to their cp850 equivalents rather than rejecting.
_CP850_FIXUP = {0x8F: "Å", 0x90: "É", 0x9D: "Ø", 0x81: "ü", 0x8D: "ì"}

EXPECTED_HEADERS: dict[str, list[str]] = {
    "Registerinformation.csv": [
        "Registernamn",
        "Registerrubrik",
        "Registersyfte",
        "Registervariantrubrik",
        "Registervariantnamn",
        "Registervariantbeskrivning",
        "RegistervariantSekretess",
        "Registerversionnamn",
        "Registerversionbeskrivning",
        "Registerversionmätinformation",
        "Registerversion_DocStaus",
        "Registerversion_ForstaGodkannandeDatum",
        "Registerversion_SenastGodkandDatum",
        "Populationnamn",
        "Populationdefinition",
        "Populationkommentar",
        "Populationdatum",
        "Objekttypnamn",
        "Objekttypdefinition",
        "Variabelnamn",
        "Variabeldefinition",
        "Variabelbeskrivning",
        "VariabelOperationell_definition",
        "VariabelReferenstid",
        "VariabelHämtadFrån",
        "VariabelRegister_Källa",
        "VariabelExtern_kommentar",
        "Mattenhet",
        "Kolumnnamn",
        "Datatyp",
        "Datalängd",
        "CVID",
        "RegisterId",
        "RegVarID",
        "RegVerID",
        "VarId",
    ],
    "UnikaRegisterOchVariabler.csv": [
        "Registernamn",
        "Registerrubrik",
        "Registervariantnamn",
        "Registervariantrubrik",
        "Variabelnamn",
        "Kolumnnamn",
        "VersionForsta",
        "VersionSista",
        "KansligVariabel",
        "KansligVariabelIbland",
        "Identitetsvariabel",
    ],
    "Identifierare.csv": ["VarID", "Variabelnamn", "Variabeldefinition"],
    "Timeseries.csv": [
        "Namn",
        "Handelse",
        "Beskrivning",
        "Entitet",
        "ID1",
        "ID2",
        "FilID",
    ],
    "Vardemangder.csv": [
        "Värdemängdsversion",
        "Värdemängdsnivå",
        "Värdekod",
        "Värdebenämning",
        "CVID",
        "ItemId",
    ],
    "VardemangderValidDates.csv": ["ItemID", "ValidFrom", "ValidTo"],
}

# Files that must be present for build-db
REQUIRED_FILES = ["Registerinformation.csv"]
ENRICHMENT_FILES = [
    "UnikaRegisterOchVariabler.csv",
    "Identifierare.csv",
    "Timeseries.csv",
    "Vardemangder.csv",
    "VardemangderValidDates.csv",
]

DDL = """\
-- Core tables
CREATE TABLE register (
    register_id TEXT PRIMARY KEY,
    registernamn TEXT NOT NULL,
    registerrubrik TEXT,
    registersyfte TEXT
);

CREATE TABLE register_variant (
    regvar_id TEXT PRIMARY KEY,
    register_id TEXT NOT NULL REFERENCES register(register_id),
    registervariantnamn TEXT,
    registervariantrubrik TEXT,
    registervariantbeskrivning TEXT,
    registervariantsekretess TEXT
);

CREATE TABLE register_version (
    regver_id TEXT PRIMARY KEY,
    regvar_id TEXT NOT NULL REFERENCES register_variant(regvar_id),
    registerversionnamn TEXT,
    registerversionbeskrivning TEXT,
    registerversionmatinformation TEXT,
    registerversion_docstaus TEXT,
    registerversion_forstagodkannandedatum TEXT,
    registerversion_senastgodkanddatum TEXT
);

CREATE TABLE population (
    regver_id TEXT NOT NULL REFERENCES register_version(regver_id),
    populationnamn TEXT NOT NULL,
    populationdefinition TEXT,
    populationkommentar TEXT,
    populationdatum TEXT,
    PRIMARY KEY (regver_id, populationnamn)
);

CREATE TABLE object_type (
    regver_id TEXT NOT NULL REFERENCES register_version(regver_id),
    objekttypnamn TEXT NOT NULL,
    objekttypdefinition TEXT,
    PRIMARY KEY (regver_id, objekttypnamn)
);

CREATE TABLE variable (
    register_id TEXT NOT NULL REFERENCES register(register_id),
    var_id TEXT NOT NULL,
    variabelnamn TEXT,
    variabeldefinition TEXT,
    variabelbeskrivning TEXT,
    variabeloperationell_definition TEXT,
    variabelreferenstid TEXT,
    variabelhamtadfran TEXT,
    variabelregister_kalla TEXT,
    variabelextern_kommentar TEXT,
    mattenhet TEXT,
    PRIMARY KEY (register_id, var_id)
);

CREATE TABLE variable_instance (
    cvid TEXT PRIMARY KEY,
    register_id TEXT NOT NULL,
    regvar_id TEXT NOT NULL,
    regver_id TEXT NOT NULL,
    var_id TEXT NOT NULL,
    datatyp TEXT,
    datalangd TEXT,
    FOREIGN KEY (register_id, var_id) REFERENCES variable(register_id, var_id)
);

CREATE TABLE variable_alias (
    cvid TEXT NOT NULL REFERENCES variable_instance(cvid),
    kolumnnamn TEXT NOT NULL,
    PRIMARY KEY (cvid, kolumnnamn)
);

CREATE TABLE variable_context (
    cvid TEXT NOT NULL REFERENCES variable_instance(cvid),
    populationnamn TEXT NOT NULL,
    objekttypnamn TEXT NOT NULL,
    PRIMARY KEY (cvid, populationnamn, objekttypnamn)
);

-- Enrichment tables
CREATE TABLE value_item (
    value_item_id INTEGER PRIMARY KEY AUTOINCREMENT,
    cvid TEXT NOT NULL,
    vardemangdsversion TEXT,
    vardemangdsniva TEXT,
    vardekod TEXT,
    vardebenamning TEXT,
    item_id TEXT
);

CREATE TABLE value_item_validity (
    item_id TEXT NOT NULL,
    valid_from TEXT,
    valid_to TEXT
);
CREATE INDEX idx_value_item_validity_item ON value_item_validity(item_id);

CREATE TABLE unika_summary (
    register_id TEXT,
    regvar_id TEXT,
    kolumnnamn TEXT,
    variabelnamn TEXT,
    version_forsta TEXT,
    version_sista TEXT,
    kanslig_variabel TEXT,
    kanslig_variabel_ibland TEXT,
    identitetsvariabel TEXT,
    PRIMARY KEY (register_id, regvar_id, kolumnnamn, variabelnamn)
);

CREATE TABLE identifier_semantics (
    var_id TEXT PRIMARY KEY,
    variabelnamn TEXT,
    variabeldefinition TEXT
);

CREATE TABLE timeseries_event (
    timeseries_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    namn TEXT,
    handelse TEXT,
    beskrivning TEXT,
    entitet TEXT,
    id1 TEXT,
    id2 TEXT,
    fil_id TEXT
);

-- Search indexes
CREATE VIRTUAL TABLE register_fts USING fts5(
    register_id,
    registernamn,
    registerrubrik,
    registersyfte,
    content='register',
    content_rowid='rowid'
);

CREATE VIRTUAL TABLE variable_fts USING fts5(
    register_id,
    var_id,
    variabelnamn,
    variabeldefinition,
    variabelbeskrivning,
    tokenize='unicode61'
);

-- Performance indexes
CREATE INDEX idx_register_variant_register ON register_variant(register_id);
CREATE INDEX idx_register_version_regvar ON register_version(regvar_id);
CREATE INDEX idx_variable_instance_register ON variable_instance(register_id);
CREATE INDEX idx_variable_instance_var ON variable_instance(register_id, var_id);
CREATE INDEX idx_variable_instance_regvar ON variable_instance(regvar_id);
CREATE INDEX idx_variable_instance_regver ON variable_instance(regver_id);
CREATE INDEX idx_variable_alias_kolumnnamn ON variable_alias(kolumnnamn);
CREATE INDEX idx_value_item_cvid ON value_item(cvid);

-- Reference tables
CREATE TABLE source_column_type (
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    sql_type TEXT NOT NULL,
    nullable INTEGER NOT NULL,
    PRIMARY KEY (table_name, column_name)
);

CREATE TABLE source_join_key (
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    description TEXT,
    PRIMARY KEY (table_name, column_name)
);

-- Import metadata
CREATE TABLE import_manifest (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def default_db_dir() -> Path:
    return Path(os.environ.get("REGMETA_DB", "~/.local/share/regmeta")).expanduser()


def db_path_from_args(db_arg: str | None) -> Path:
    if db_arg:
        return Path(db_arg).expanduser().resolve() / DB_FILENAME
    return default_db_dir().resolve() / DB_FILENAME


def open_db(db_path: Path) -> sqlite3.Connection:
    if not db_path.exists():
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="db_not_found",
            error_class="configuration",
            message=f"Database not found: {db_path}",
            remediation="Run `regmeta maintain build-db --csv-dir <path>` first.",
        )
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def get_manifest(conn: sqlite3.Connection) -> dict[str, str]:
    rows = conn.execute("SELECT key, value FROM import_manifest").fetchall()
    return {row["key"]: row["value"] for row in rows}


# ---------------------------------------------------------------------------
# CSV reading
# ---------------------------------------------------------------------------


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


@contextmanager
def _open_scb_csv(
    path: Path,
) -> Iterator[tuple[list[str], Iterator[tuple[int, dict[str, str]]]]]:
    """Open a pipe-delimited cp1252 CSV and yield (header, row_iterator).

    Reads bytes as latin-1 (single-byte passthrough), validates against
    known-invalid cp1252 bytes, then decodes to proper cp1252 text.
    """
    with path.open("rb") as raw_handle:
        text_handle = io.TextIOWrapper(raw_handle, encoding="latin-1", newline="")
        reader = csv.reader(text_handle, delimiter="|", quotechar='"')
        try:
            raw_header = next(reader)
        except StopIteration as exc:
            raise RegmetaError(
                exit_code=EXIT_CONFIG,
                code="csv_empty",
                error_class="configuration",
                message=f"CSV file is empty: {path.name}",
                remediation="Re-export the file from mikrometadata.scb.se.",
            ) from exc

        header = [_decode_cp1252(v) for v in raw_header]

        expected = EXPECTED_HEADERS.get(path.name)
        if expected and header != expected:
            raise RegmetaError(
                exit_code=EXIT_CONFIG,
                code="csv_bad_header",
                error_class="configuration",
                message=f"Unexpected header in {path.name}.",
                remediation="Ensure the file is an unmodified SCB metadata export.",
            )

        def row_iter() -> Iterator[tuple[int, dict[str, str]]]:
            for row_number, fields in enumerate(reader, start=2):
                if len(fields) != len(header):
                    raise RegmetaError(
                        exit_code=EXIT_CONFIG,
                        code="csv_bad_row",
                        error_class="configuration",
                        message=f"Row {row_number} in {path.name} has {len(fields)} fields, expected {len(header)}.",
                        remediation="Re-export the file from mikrometadata.scb.se.",
                    )
                yield (
                    row_number,
                    {h: _decode_cp1252(v) for h, v in zip(header, fields, strict=True)},
                )

        yield header, row_iter()


def _decode_cp1252(raw: str) -> str:
    """Decode a latin-1-read string to proper cp1252.

    Bytes undefined in cp1252 but present as DOS cp850 remnants are mapped
    to their cp850 equivalents instead of rejecting the whole import.
    """
    raw_bytes = raw.encode("latin-1")
    if not any(b in _CP850_FIXUP for b in raw_bytes):
        return raw_bytes.decode("cp1252")
    return "".join(
        _CP850_FIXUP[b] if b in _CP850_FIXUP else bytes([b]).decode("cp1252")
        for b in raw_bytes
    )


def _progress(msg: str) -> None:
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()


# ---------------------------------------------------------------------------
# Normalization from Registerinformation.csv → core tables
# ---------------------------------------------------------------------------


def _first_non_empty(current: str | None, candidate: str) -> str | None:
    if current:
        return current
    return candidate or current


def _import_registerinformation(
    conn: sqlite3.Connection, path: Path
) -> tuple[int, dict[tuple[str, str, str, str], tuple[str, str]], set[str]]:
    """Import Registerinformation.csv into all core normalized tables.

    Returns (row_count, regname_to_ids, varname_to_key_set) for cross-file joining.
    """
    registers: dict[str, dict[str, Any]] = {}
    variants: dict[str, dict[str, Any]] = {}
    versions: dict[str, dict[str, Any]] = {}
    variables: dict[tuple[str, str], dict[str, Any]] = {}
    instances: dict[str, dict[str, Any]] = {}
    aliases: set[tuple[str, str]] = set()
    populations: set[tuple[str, str, str, str, str]] = set()
    object_types: set[tuple[str, str, str]] = set()
    contexts: set[tuple[str, str, str]] = set()

    # For joining UnikaRegisterOchVariabler later
    unika_join: dict[tuple[str, str, str, str], tuple[str, str]] = {}

    row_count = 0
    _progress("Importing Registerinformation.csv...")

    with _open_scb_csv(path) as (_, rows):
        for row_number, row in rows:
            row_count += 1
            if row_count % 500_000 == 0:
                _progress(f"  ...{row_count:,} rows")

            rid = row["RegisterId"]
            rvid = row["RegVarID"]
            rveid = row["RegVerID"]
            vid = row["VarId"]
            cvid = row["CVID"]

            registers.setdefault(
                rid,
                {
                    "register_id": rid,
                    "registernamn": row["Registernamn"],
                    "registerrubrik": row["Registerrubrik"],
                    "registersyfte": row["Registersyfte"],
                },
            )

            variants.setdefault(
                rvid,
                {
                    "regvar_id": rvid,
                    "register_id": rid,
                    "registervariantnamn": row["Registervariantnamn"],
                    "registervariantrubrik": row["Registervariantrubrik"],
                    "registervariantbeskrivning": row["Registervariantbeskrivning"],
                    "registervariantsekretess": row["RegistervariantSekretess"],
                },
            )

            versions.setdefault(
                rveid,
                {
                    "regver_id": rveid,
                    "regvar_id": rvid,
                    "registerversionnamn": row["Registerversionnamn"],
                    "registerversionbeskrivning": row["Registerversionbeskrivning"],
                    "registerversionmatinformation": row[
                        "Registerversionmätinformation"
                    ],
                    "registerversion_docstaus": row["Registerversion_DocStaus"],
                    "registerversion_forstagodkannandedatum": row[
                        "Registerversion_ForstaGodkannandeDatum"
                    ],
                    "registerversion_senastgodkanddatum": row[
                        "Registerversion_SenastGodkandDatum"
                    ],
                },
            )

            var = variables.setdefault(
                (rid, vid),
                {
                    "register_id": rid,
                    "var_id": vid,
                    "variabelnamn": row["Variabelnamn"],
                    "variabeldefinition": row["Variabeldefinition"],
                    "variabelbeskrivning": row["Variabelbeskrivning"],
                    "variabeloperationell_definition": row[
                        "VariabelOperationell_definition"
                    ],
                    "variabelreferenstid": row["VariabelReferenstid"],
                    "variabelhamtadfran": row["VariabelHämtadFrån"],
                    "variabelregister_kalla": row["VariabelRegister_Källa"],
                    "variabelextern_kommentar": row["VariabelExtern_kommentar"],
                    "mattenhet": row["Mattenhet"],
                },
            )
            # Fill empty fields from later rows
            for tgt, src in [
                ("variabelnamn", "Variabelnamn"),
                ("variabeldefinition", "Variabeldefinition"),
                ("variabelbeskrivning", "Variabelbeskrivning"),
                ("variabeloperationell_definition", "VariabelOperationell_definition"),
                ("variabelreferenstid", "VariabelReferenstid"),
                ("variabelhamtadfran", "VariabelHämtadFrån"),
                ("variabelregister_kalla", "VariabelRegister_Källa"),
                ("variabelextern_kommentar", "VariabelExtern_kommentar"),
                ("mattenhet", "Mattenhet"),
            ]:
                var[tgt] = _first_non_empty(var[tgt], row[src])

            instances.setdefault(
                cvid,
                {
                    "cvid": cvid,
                    "register_id": rid,
                    "regvar_id": rvid,
                    "regver_id": rveid,
                    "var_id": vid,
                    "datatyp": row["Datatyp"],
                    "datalangd": row["Datalängd"],
                },
            )

            aliases.add((cvid, row["Kolumnnamn"]))
            populations.add(
                (
                    rveid,
                    row["Populationnamn"],
                    row["Populationdefinition"],
                    row["Populationkommentar"],
                    row["Populationdatum"],
                )
            )
            object_types.add((rveid, row["Objekttypnamn"], row["Objekttypdefinition"]))
            contexts.add((cvid, row["Populationnamn"], row["Objekttypnamn"]))

            unika_join.setdefault(
                (
                    row["Registernamn"],
                    row["Registervariantnamn"],
                    row["Kolumnnamn"],
                    row["Variabelnamn"],
                ),
                (rid, rvid),
            )

    _progress(f"  {row_count:,} rows read")

    # Bulk insert all normalized tables
    _progress("Writing core tables...")
    conn.executemany(
        "INSERT INTO register VALUES (:register_id, :registernamn, :registerrubrik, :registersyfte)",
        list(registers.values()),
    )
    conn.executemany(
        "INSERT INTO register_variant VALUES (:regvar_id, :register_id, :registervariantnamn, "
        ":registervariantrubrik, :registervariantbeskrivning, :registervariantsekretess)",
        list(variants.values()),
    )
    conn.executemany(
        "INSERT INTO register_version VALUES (:regver_id, :regvar_id, :registerversionnamn, "
        ":registerversionbeskrivning, :registerversionmatinformation, :registerversion_docstaus, "
        ":registerversion_forstagodkannandedatum, :registerversion_senastgodkanddatum)",
        list(versions.values()),
    )
    conn.executemany(
        "INSERT INTO variable VALUES (:register_id, :var_id, :variabelnamn, :variabeldefinition, "
        ":variabelbeskrivning, :variabeloperationell_definition, :variabelreferenstid, "
        ":variabelhamtadfran, :variabelregister_kalla, :variabelextern_kommentar, :mattenhet)",
        list(variables.values()),
    )
    conn.executemany(
        "INSERT INTO variable_instance VALUES (:cvid, :register_id, :regvar_id, :regver_id, "
        ":var_id, :datatyp, :datalangd)",
        list(instances.values()),
    )
    conn.executemany(
        "INSERT INTO variable_alias VALUES (?, ?)",
        sorted(aliases),
    )
    conn.executemany(
        "INSERT INTO population VALUES (?, ?, ?, ?, ?)",
        sorted(populations),
    )
    conn.executemany(
        "INSERT INTO object_type VALUES (?, ?, ?)",
        sorted(object_types),
    )
    conn.executemany(
        "INSERT INTO variable_context VALUES (?, ?, ?)",
        sorted(contexts),
    )

    counts = {
        "register": len(registers),
        "register_variant": len(variants),
        "register_version": len(versions),
        "variable": len(variables),
        "variable_instance": len(instances),
        "variable_alias": len(aliases),
        "population": len(populations),
        "object_type": len(object_types),
        "variable_context": len(contexts),
    }
    _progress(f"  Core tables: {counts}")

    return row_count, unika_join, set(instances.keys())


def _import_unika(
    conn: sqlite3.Connection,
    path: Path,
    unika_join: dict[tuple[str, str, str, str], tuple[str, str]],
) -> int:
    _progress("Importing UnikaRegisterOchVariabler.csv...")
    row_count = 0
    batch: list[tuple[str, ...]] = []

    with _open_scb_csv(path) as (_, rows):
        for _, row in rows:
            row_count += 1
            key = (
                row["Registernamn"],
                row["Registervariantnamn"],
                row["Kolumnnamn"],
                row["Variabelnamn"],
            )
            ids = unika_join.get(key)
            if ids is None:
                continue
            register_id, regvar_id = ids
            batch.append(
                (
                    register_id,
                    regvar_id,
                    row["Kolumnnamn"],
                    row["Variabelnamn"],
                    row["VersionForsta"],
                    row["VersionSista"],
                    row["KansligVariabel"],
                    row["KansligVariabelIbland"],
                    row["Identitetsvariabel"],
                )
            )

    conn.executemany(
        "INSERT OR IGNORE INTO unika_summary VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        batch,
    )
    _progress(f"  {row_count:,} rows read, {len(batch):,} matched")
    return row_count


def _import_identifierare(conn: sqlite3.Connection, path: Path) -> int:
    _progress("Importing Identifierare.csv...")
    row_count = 0
    batch: list[tuple[str, ...]] = []

    with _open_scb_csv(path) as (_, rows):
        for _, row in rows:
            row_count += 1
            batch.append((row["VarID"], row["Variabelnamn"], row["Variabeldefinition"]))

    conn.executemany(
        "INSERT OR IGNORE INTO identifier_semantics VALUES (?, ?, ?)",
        batch,
    )
    _progress(f"  {row_count:,} rows")
    return row_count


def _import_timeseries(conn: sqlite3.Connection, path: Path) -> int:
    _progress("Importing Timeseries.csv...")
    row_count = 0
    batch: list[tuple[str, ...]] = []

    with _open_scb_csv(path) as (_, rows):
        for _, row in rows:
            row_count += 1
            batch.append(
                (
                    row["Namn"],
                    row["Handelse"],
                    row["Beskrivning"],
                    row["Entitet"],
                    row["ID1"],
                    row["ID2"],
                    row["FilID"],
                )
            )

    conn.executemany(
        "INSERT INTO timeseries_event (namn, handelse, beskrivning, entitet, id1, id2, fil_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        batch,
    )
    _progress(f"  {row_count:,} rows")
    return row_count


def _import_vardemangder(
    conn: sqlite3.Connection, path: Path, known_cvids: set[str]
) -> int:
    _progress("Importing Vardemangder.csv (this may take a while)...")
    row_count = 0
    inserted = 0
    batch: list[tuple[str, ...]] = []
    batch_size = 50_000

    with _open_scb_csv(path) as (_, rows):
        for _, row in rows:
            row_count += 1
            if row_count % 5_000_000 == 0:
                _progress(f"  ...{row_count:,} rows read, {inserted:,} inserted")

            if row["CVID"] not in known_cvids:
                continue

            batch.append(
                (
                    row["CVID"],
                    row["Värdemängdsversion"],
                    row["Värdemängdsnivå"],
                    row["Värdekod"],
                    row["Värdebenämning"],
                    row["ItemId"],
                )
            )

            if len(batch) >= batch_size:
                conn.executemany(
                    "INSERT INTO value_item (cvid, vardemangdsversion, vardemangdsniva, vardekod, vardebenamning, item_id) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    batch,
                )
                inserted += len(batch)
                batch.clear()

    if batch:
        conn.executemany(
            "INSERT INTO value_item (cvid, vardemangdsversion, vardemangdsniva, vardekod, vardebenamning, item_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            batch,
        )
        inserted += len(batch)

    _progress(f"  {row_count:,} rows read, {inserted:,} inserted")
    return row_count


def _import_vardemangder_valid_dates(conn: sqlite3.Connection, path: Path) -> int:
    """Import value-item validity date ranges from VardemangderValidDates.csv.

    Items absent from this file have no temporal restriction (always valid).
    """
    _progress("Importing VardemangderValidDates.csv...")
    row_count = 0
    batch: list[tuple[str, str, str]] = []
    batch_size = 50_000

    with _open_scb_csv(path) as (_, rows):
        for _, row in rows:
            row_count += 1
            batch.append((
                row["ItemID"],
                row["ValidFrom"] or None,
                row["ValidTo"] or None,
            ))

            if len(batch) >= batch_size:
                conn.executemany(
                    "INSERT INTO value_item_validity (item_id, valid_from, valid_to) "
                    "VALUES (?, ?, ?)",
                    batch,
                )
                batch.clear()

    if batch:
        conn.executemany(
            "INSERT INTO value_item_validity (item_id, valid_from, valid_to) "
            "VALUES (?, ?, ?)",
            batch,
        )

    _progress(f"  {row_count:,} rows")
    return row_count


def _populate_fts(conn: sqlite3.Connection) -> None:
    """Populate FTS5 search indexes."""
    _progress("Building search indexes...")

    # register_fts: content-synced with register table
    conn.execute(
        "INSERT INTO register_fts(register_id, registernamn, registerrubrik, registersyfte) "
        "SELECT register_id, registernamn, registerrubrik, registersyfte FROM register"
    )

    # variable_fts: semantic content only (name, definition, description).
    # Column names are excluded — they contain technical suffixes like _LISA
    # that pollute search results. Column name matching is handled by `resolve`.
    conn.execute("""
        INSERT INTO variable_fts(register_id, var_id, variabelnamn, variabeldefinition, variabelbeskrivning)
        SELECT
            v.register_id,
            v.var_id,
            v.variabelnamn,
            v.variabeldefinition,
            v.variabelbeskrivning
        FROM variable v
    """)
    _progress("  FTS indexes built")


# ---------------------------------------------------------------------------
# Reference imports
# ---------------------------------------------------------------------------

_SQL_CREATE_RE = re.compile(
    r"CREATE\s+TABLE\s+\[dbo\]\.\[(\w+)\]\s*\((.*?)\)\s*ON\s+\[PRIMARY\]",
    re.DOTALL | re.IGNORECASE,
)
_SQL_COL_RE = re.compile(
    r"\[(\w+)\]\s+\[(\w+)\](?:\((\d+)\))?\s*(NULL|NOT\s+NULL)?",
)


def _import_tabelldefinitioner(conn: sqlite3.Connection, path: Path) -> int:
    """Parse Tabelldefinitioner.sql for column types and constraints."""
    _progress("Importing Tabelldefinitioner.sql...")
    raw = path.read_bytes().decode("cp1252").replace("\r\n", "\n")
    row_count = 0
    for table_match in _SQL_CREATE_RE.finditer(raw):
        table_name = table_match.group(1)
        body = table_match.group(2)
        for col_match in _SQL_COL_RE.finditer(body):
            col_name = col_match.group(1)
            sql_type = col_match.group(2)
            if col_match.group(3):
                sql_type += f"({col_match.group(3)})"
            nullable = 1
            if col_match.group(4) and "NOT" in col_match.group(4).upper():
                nullable = 0
            conn.execute(
                "INSERT OR IGNORE INTO source_column_type VALUES (?, ?, ?, ?)",
                (table_name, col_name, sql_type, nullable),
            )
            row_count += 1
    _progress(f"  {row_count} column definitions")
    return row_count


def _import_id_kolumner(conn: sqlite3.Connection, path: Path) -> int:
    """Parse ID-kolumner.xlsx for join-key semantics."""
    try:
        import openpyxl
    except ImportError:
        _progress(
            "Skipping ID-kolumner.xlsx (openpyxl not installed; install with: pip install regmeta[xlsx])"
        )
        return 0

    _progress("Importing ID-kolumner.xlsx...")
    wb = openpyxl.load_workbook(path, read_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    wb.close()

    if not rows:
        return 0

    # Skip header row
    row_count = 0
    for row in rows[1:]:
        if len(row) >= 3 and row[0] and row[1]:
            conn.execute(
                "INSERT OR IGNORE INTO source_join_key VALUES (?, ?, ?)",
                (str(row[0]), str(row[1]), str(row[2]) if row[2] else None),
            )
            row_count += 1
    _progress(f"  {row_count} join-key definitions")
    return row_count


def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_db(csv_dir: Path, db_dir: Path) -> dict[str, Any]:
    """Build the regmeta database from SCB CSV exports.

    Returns a summary dict for the CLI to display.
    """
    csv_dir = csv_dir.expanduser().resolve()
    db_dir = db_dir.expanduser().resolve()

    if not csv_dir.is_dir():
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="csv_dir_not_found",
            error_class="configuration",
            message=f"CSV directory not found: {csv_dir}",
            remediation="Provide the directory containing SCB metadata CSV exports.",
        )

    ri_path = csv_dir / "Registerinformation.csv"
    if not ri_path.exists():
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="csv_missing_backbone",
            error_class="configuration",
            message="Registerinformation.csv not found in the CSV directory.",
            remediation="Export all metadata files from mikrometadata.scb.se.",
        )

    db_dir.mkdir(parents=True, exist_ok=True)
    final_path = db_dir / DB_FILENAME
    tmp_path = final_path.with_suffix(".db.tmp")

    if tmp_path.exists():
        tmp_path.unlink()

    conn = sqlite3.connect(tmp_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=OFF")  # Enable after import for speed
    try:
        conn.executescript(DDL)

        source_checksums: dict[str, str] = {}
        row_counts: dict[str, int] = {}

        # Core: Registerinformation.csv (required)
        source_checksums["Registerinformation.csv"] = _file_sha256(ri_path)
        ri_count, unika_join, known_cvids = _import_registerinformation(conn, ri_path)
        row_counts["Registerinformation.csv"] = ri_count

        # Enrichment files (optional)
        for filename in ENRICHMENT_FILES:
            path = csv_dir / filename
            if not path.exists():
                _progress(f"Skipping {filename} (not found)")
                continue
            source_checksums[filename] = _file_sha256(path)

            if filename == "UnikaRegisterOchVariabler.csv":
                row_counts[filename] = _import_unika(conn, path, unika_join)
            elif filename == "Identifierare.csv":
                row_counts[filename] = _import_identifierare(conn, path)
            elif filename == "Timeseries.csv":
                row_counts[filename] = _import_timeseries(conn, path)
            elif filename == "Vardemangder.csv":
                row_counts[filename] = _import_vardemangder(conn, path, known_cvids)
            elif filename == "VardemangderValidDates.csv":
                row_counts[filename] = _import_vardemangder_valid_dates(conn, path)

        # Reference files (optional)
        sql_path = csv_dir / "Tabelldefinitioner.sql"
        if sql_path.exists():
            row_counts["Tabelldefinitioner.sql"] = _import_tabelldefinitioner(
                conn, sql_path
            )
        else:
            _progress("Skipping Tabelldefinitioner.sql (not found)")

        xlsx_path = csv_dir / "ID-kolumner.xlsx"
        if xlsx_path.exists():
            row_counts["ID-kolumner.xlsx"] = _import_id_kolumner(conn, xlsx_path)
        else:
            _progress("Skipping ID-kolumner.xlsx (not found)")

        _populate_fts(conn)

        # Write manifest
        manifest_data = {
            "schema_version": SCHEMA_VERSION,
            "import_date": utc_now(),
            "csv_dir": str(csv_dir),
            "source_checksums": source_checksums,
            "row_counts": row_counts,
        }
        for key, value in manifest_data.items():
            conn.execute(
                "INSERT INTO import_manifest VALUES (?, ?)",
                (key, json.dumps(value) if isinstance(value, dict) else str(value)),
            )

        conn.execute("PRAGMA foreign_keys=ON")
        conn.commit()
        _progress("Database built successfully.")
    except Exception:
        conn.close()
        if tmp_path.exists():
            tmp_path.unlink()
        raise
    else:
        conn.close()

    # Atomic replace
    if final_path.exists():
        final_path.unlink()
    tmp_path.rename(final_path)
    _progress(f"Database written to {final_path}")

    return {
        "db_path": str(final_path),
        "schema_version": SCHEMA_VERSION,
        "import_date": manifest_data["import_date"],
        "source_checksums": source_checksums,
        "row_counts": row_counts,
    }
