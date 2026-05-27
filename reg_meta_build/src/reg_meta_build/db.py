"""Build pipeline for the reg_meta SQLite database.

DDL, SCB CSV import, classification + slug seeding, FK validation, and
the atomic-replace write. Connection management and schema-compat checks
live in `reg_meta.db`; this module imports them rather than redefining.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import sqlite3
import struct
import sys
import unicodedata
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from reg_meta.db import (
    DB_FILENAME,
    SCHEMA_VERSION,
    utc_now,
)
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.fqid import derive_variable_slug
from reg_meta.queries import extract_year

from .classifications import populate_classifications, repo_seed_path
from .fqid_slugs import (
    materialize_related_to_edges,
    materialize_same_as_edges,
    populate_slugs,
    repo_slug_dir,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

# Built-in data providers. `provider_id` values are stable: rows reference them
# from `register.provider_id`. Add new providers by appending — never renumber.
PROVIDER_ID_SCB = 1
PROVIDER_ID_SOS = 2
_PROVIDER_SEED: tuple[tuple[int, str, str], ...] = (
    (PROVIDER_ID_SCB, "scb", "Statistics Sweden"),
    (PROVIDER_ID_SOS, "sos", "Socialstyrelsen"),
)

# SCB ships rows in Vardemangder.csv where Värdekod == Värdemängdsversion. Two
# disjoint cases observed; build-db classifies each row using these allowlists:
#
# _VARDEMANGDER_SENTINELS — placeholder strings stuffed into Värdekod to mean
# "no enumerated code list." Not real value codes; dropped silently:
#   - "Tal"               variable is numeric
#   - "Beskrivande text"  variable is free-form text
#
# _VARDEMANGDER_REAL_SHAPED — kods that *happen* to equal their version label
# but are real single-code value sets. Kept silently:
#   - "1"  ("Hade ingen anställning före YH-utbildningen")
#   - "2"  ("Övriga civilstånd")
#
# Any other kod==version row is treated as drift (unknown placeholder) and
# fails the build with code "vardemangder_drift" — see the drift block in
# _import_vardemangder. Audit script: scripts/audit_vardemangder.py.
_VARDEMANGDER_SENTINELS = frozenset({"Tal", "Beskrivande text"})
_VARDEMANGDER_REAL_SHAPED = frozenset({"1", "2"})


def _value_set_hash(pairs: list[tuple[str, str]]) -> bytes:
    """Content-addressed sha256 over sorted (vardekod, vardebenamning) pairs.

    Length-prefixed encoding so no byte assumption is needed about source text.
    Stable across rebuilds given identical inputs (kod/label are stable strings
    independent of code_id assignment order).
    """
    h = hashlib.sha256()
    h.update(struct.pack(">I", len(pairs)))
    for kod, label in sorted(pairs):
        kb = kod.encode("utf-8")
        lb = label.encode("utf-8")
        h.update(struct.pack(">I", len(kb)))
        h.update(kb)
        h.update(struct.pack(">I", len(lb)))
        h.update(lb)
    return h.digest()


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
]

DDL = """\
-- Core tables (all IDs stored as INTEGER for compact storage)

-- Data providers (publishers): scb, sos, ... See _PROVIDER_SEED for the seed.
-- Promoted to first-class in schema v3.1 for FQID grammar (REFACTOR_SPEC.md §5.1).
CREATE TABLE provider (
    provider_id INTEGER PRIMARY KEY,
    slug        TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL
);

-- FQID slug columns (`slug` on register / register_variant / classification)
-- are nullable in 3.1. Curated values land in step 1c; the build refuses to
-- compile with NULL slugs from then on. The §5.1 `_default` placeholder for
-- variant-less registers is synthesized at FQID-resolve time (catalog.py),
-- never persisted. See REFACTOR_SPEC.md §5.1 and §5.3.
CREATE TABLE register (
    register_id INTEGER PRIMARY KEY,
    provider_id INTEGER NOT NULL REFERENCES provider(provider_id),
    -- Universal English columns per REFACTOR_SPEC §5.11 vocabulary glossary.
    -- Values remain provider-native strings (SCB's literal `Registernamn`
    -- text such as "LISA"). `registerrubrik` is dropped (redundant with `name`).
    name TEXT NOT NULL,
    purpose TEXT,
    slug         TEXT
);

CREATE TABLE register_variant (
    regvar_id INTEGER PRIMARY KEY,
    register_id INTEGER NOT NULL REFERENCES register(register_id),
    -- §5.11 rename. `registervariantrubrik` (redundant with name) and
    -- `registervariantsekretess` (legal text → reg-meta-docs) are dropped.
    name TEXT,
    description TEXT,
    slug          TEXT,
    -- Presentation-only grouping label (§5.3 field reference). Drift-tolerant.
    display_group TEXT
);

CREATE TABLE register_version (
    regver_id INTEGER PRIMARY KEY,
    regvar_id INTEGER NOT NULL REFERENCES register_variant(regvar_id),
    -- §5.2 version slot: either a derived period token (`2018`, `HT2020`, …)
    -- or a curated slug for rows the period regex can't disambiguate
    -- (unperiodized aux tables, year-range projections, sub-topic siblings
    -- sharing a year). NULL only between INSERT and populate_slugs's
    -- auto-derive + curated-override pass.
    slug TEXT,
    registerversionnamn TEXT,
    registerversionbeskrivning TEXT,
    registerversionmatinformation TEXT,
    registerversion_docstaus TEXT,
    registerversion_forstagodkannandedatum TEXT,
    registerversion_senastgodkanddatum TEXT,
    -- §5.3: slug is unique within parent variant. Most slugs come from
    -- auto-derive (period regex extended with Swedish termin grammar),
    -- not TOML; the TOML-load `seen_slugs` check doesn't catch sibling
    -- collisions between two auto-derived rows or between a curated
    -- override and an auto-derived sibling. SQLite treats NULLs as
    -- distinct, so this is safe during the INSERT → populate_slugs window.
    UNIQUE (regvar_id, slug)
);

CREATE TABLE population (
    regver_id INTEGER NOT NULL REFERENCES register_version(regver_id),
    -- §5.11 rename. `populationdatum` is a free-text date range, not a parsed
    -- date — provider-native string preserved.
    name TEXT NOT NULL,
    definition TEXT,
    comment TEXT,
    date_range TEXT,
    PRIMARY KEY (regver_id, name)
);

CREATE TABLE object_type (
    regver_id INTEGER NOT NULL REFERENCES register_version(regver_id),
    name TEXT NOT NULL,
    definition TEXT,
    PRIMARY KEY (regver_id, name)
);

CREATE TABLE variable (
    register_id INTEGER NOT NULL REFERENCES register(register_id),
    var_id INTEGER NOT NULL,
    -- §5.11 rename. Values stay provider-native. `variabeloperationell_definition`
    -- merges into `description` at ingest when distinct + non-empty;
    -- `variabelreferenstid`, `variabelhamtadfran`, and `variabelextern_kommentar`
    -- are dropped per §5.11. `variabelregister_kalla` (raw attribution text)
    -- becomes `source_register_text`.
    name TEXT,
    definition TEXT,
    description TEXT,
    source_register_text TEXT,
    measurement_unit TEXT,
    source_register_id INTEGER REFERENCES register(register_id),
    source_label TEXT,
    -- A1.2: sensitivity flags lifted from unika_summary so A2.1 can drop that
    -- table cleanly. Populated by `_populate_sensitivity_flags` after
    -- unika_summary import. SCB ships `kanslig_variabel` and
    -- `kanslig_variabel_ibland` as separate columns; the 22 "sometimes
    -- sensitive" rows aren't worth a third column, so both fold into
    -- `is_sensitive`. ANY 'Ja' row across the unika_summary group for a
    -- (register_id, var_id) sets the flag.
    is_sensitive INTEGER NOT NULL DEFAULT 0,
    is_identifier INTEGER NOT NULL DEFAULT 0,
    -- PK doubles as the join index for get_schema's JOIN on (register_id, var_id).
    -- Do not add a redundant explicit index.
    PRIMARY KEY (register_id, var_id)
);

CREATE TABLE variable_instance (
    cvid INTEGER PRIMARY KEY,
    register_id INTEGER NOT NULL,
    regvar_id INTEGER NOT NULL,
    regver_id INTEGER NOT NULL,
    var_id INTEGER NOT NULL,
    -- A2.1: per-cvid raw `Variabelnamn` from the source CSV row. `variable.name`
    -- is the canonical (first-non-empty) name across all rows for a var_id;
    -- when SCB renames a variable mid-life (e.g. an active variable's name
    -- changes between editions), the canonical loses the post-rename name and
    -- the coalescer's unika_summary lookup (keyed by raw variabelnamn) misses
    -- the renamed-era unika row. This column preserves the per-cvid raw name
    -- so the lookup matches. Dropped together with variable_instance in A2.7.
    variabelnamn TEXT,
    -- §5.11 rename of `datatyp` / `datalangd` / `vardemangdsversion`.
    -- `vardemangdsniva` stays Swedish: it's a transient pre-triage carrier
    -- through A2.2 (the coalescer's `grain` field) and gets dropped from the
    -- final `variable_state` schema in A2.1. Keeping the Swedish name signals
    -- "do not depend on this column" to downstream code.
    data_type TEXT,
    data_length TEXT,
    value_set_version_label TEXT,
    vardemangdsniva TEXT,
    classification_id INTEGER REFERENCES classification(id),
    -- value_set_id links to the cvid's deduplicated, year-projected code list.
    -- NULL when the cvid has no codes (sentinel-only or every union pair
    -- excluded by year projection). No reverse index — every consumer reaches
    -- here from the cvid PK side, so the forward path is already optimal.
    value_set_id INTEGER REFERENCES value_set(value_set_id),
    -- §5.6 lineage edge: source cvid for consumer-side bindings (e.g. LISA
    -- pulling Kön from RTB). NULL on canonical/source instances. Populated
    -- by `link_consumer_side_bindings` after CSV import.
    via_source_id INTEGER REFERENCES variable_instance(cvid),
    FOREIGN KEY (register_id, var_id) REFERENCES variable(register_id, var_id)
);

-- A2.1: per-era shape of a variable (§5.1). One row per coalesced
-- `(register_id, regvar_id, var_id, data_type, data_length, value_set_id,
-- value_set_version_label, grain)` tuple over `variable_instance`; populated
-- by `_coalesce_variable_states` after CSV import. Resolver still uses
-- `variable_instance` at this stage — A2.5 flips it to `variable_state`.
-- FK shifts to `(register_id, regvar_id, var_id)` once A2.4 lands the
-- variant-scoped variable PK.
--
-- valid_from / valid_to are TEXT NOT NULL `YYYY-MM-DD` always (storage
-- contract from §5.1); coarser SCB inputs like the year "2020" expand at
-- ingest into 2020-01-01..2020-12-31. Open-ended states use the sentinel
-- valid_to = '9999-12-31' (never NULL). Lexical string comparison is
-- chronologically correct because every stored value is full-date.
--
-- A `grain` column is intentionally absent — pre-triage rows that differ
-- only on SCB's `vardemangdsniva` are kept distinct in the coalescer's
-- in-memory group key so A2.2 can later promote them into sibling slugs,
-- but grain itself never lands in the universal schema (it becomes part
-- of the variable slug when a split fires).
CREATE TABLE variable_state (
    state_id INTEGER PRIMARY KEY AUTOINCREMENT,
    register_id INTEGER NOT NULL,
    regvar_id INTEGER NOT NULL,
    var_id INTEGER NOT NULL,
    valid_from TEXT NOT NULL,
    valid_to TEXT NOT NULL DEFAULT '9999-12-31',
    data_type TEXT,
    data_length TEXT,
    delivery_column_name TEXT,
    value_set_id INTEGER REFERENCES value_set(value_set_id),
    value_set_version_label TEXT,
    FOREIGN KEY (register_id, var_id) REFERENCES variable(register_id, var_id),
    -- Full-date contract: ten-character ISO 8601 strings only. Length check
    -- is a cheap structural guard; a stricter regex isn't worth the runtime
    -- cost because the coalescer is the only writer.
    CHECK (length(valid_from) = 10),
    CHECK (length(valid_to) = 10),
    CHECK (valid_to >= valid_from)
);
CREATE INDEX idx_variable_state_variable
    ON variable_state(register_id, var_id);
CREATE INDEX idx_variable_state_regvar
    ON variable_state(register_id, regvar_id, var_id);
CREATE INDEX idx_variable_state_value_set
    ON variable_state(value_set_id)
    WHERE value_set_id IS NOT NULL;

CREATE TABLE variable_alias (
    cvid INTEGER NOT NULL REFERENCES variable_instance(cvid),
    -- §5.11: `kolumnnamn` → `delivery_column_name`. The SCB delivery column
    -- header (e.g. `PersonNr`, `Kon`, `LopNr_PersonNr`). SCB pseudonymizes
    -- identifier columns at delivery with the `LopNr_` prefix; the metadata
    -- stores the un-prefixed name.
    delivery_column_name TEXT NOT NULL,
    PRIMARY KEY (cvid, delivery_column_name)
);

CREATE TABLE variable_context (
    cvid INTEGER NOT NULL REFERENCES variable_instance(cvid),
    population_name TEXT NOT NULL,
    object_type_name TEXT NOT NULL,
    PRIMARY KEY (cvid, population_name, object_type_name)
);

-- Classifications: normalized code systems (SUN2000, SSYK2012, SNI2007, ...).
-- Populated at build time from a maintainer-curated seed (classifications.toml)
-- that maps raw variable_instance.vardemangdsversion labels to normalized
-- classification rows. See DESIGN.md § "Classifications".
CREATE TABLE classification (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    short_name       TEXT NOT NULL UNIQUE,
    name             TEXT NOT NULL,
    name_en          TEXT,
    publisher        TEXT,
    version          TEXT,
    valid_from       INTEGER,
    valid_to         INTEGER,
    description      TEXT,
    url              TEXT,
    supersedes_id    INTEGER REFERENCES classification(id),
    code_count       INTEGER NOT NULL DEFAULT 0,
    -- Number of canonical codes when a valid_codes CSV was provided; NULL
    -- otherwise. valid_code_count <= code_count is *not* invariant: canonical
    -- codes that never appeared in any observed instance still count, but
    -- observed-only noise codes inflate code_count.
    valid_code_count INTEGER,
    -- classification FQID is `class/<slug>/<version>`; `version` is already
    -- populated from the existing seed.
    slug             TEXT
);

-- is_valid: 1 = canonical (listed in the classification's valid_codes CSV),
-- 0 = observed-only (seen in data but not in the CSV), NULL = no CSV exists
-- for this classification (validity unknown).
CREATE TABLE classification_code (
    classification_id INTEGER NOT NULL REFERENCES classification(id),
    code_id           INTEGER NOT NULL REFERENCES value_code(code_id),
    level             INTEGER,
    is_valid          INTEGER,
    PRIMARY KEY (classification_id, code_id)
) WITHOUT ROWID;
CREATE INDEX idx_classification_code_code ON classification_code(code_id);

-- Enrichment tables
CREATE TABLE value_code (
    code_id INTEGER PRIMARY KEY,
    -- §5.11: SCB's `värdekod` / `värdebenämning` become universal `code` / `label`.
    -- Values stay provider-native (SCB code strings like "01", "Man", "").
    code TEXT NOT NULL,
    label TEXT NOT NULL,
    UNIQUE (code, label)
);

-- value_set: one row per distinct year-projected membership.
-- member_hash = sha256 of length-prefixed sorted (code, label) pairs (see
-- _value_set_hash in this module). Stable across rebuilds given identical
-- inputs. SCB validity windows (VardemangderValidDates.csv) are applied at
-- build time; the union of all historical codes is *not* preserved.
CREATE TABLE value_set (
    value_set_id INTEGER PRIMARY KEY,
    member_hash  BLOB NOT NULL UNIQUE,
    CHECK (length(member_hash) = 32)
);

CREATE TABLE value_set_member (
    value_set_id INTEGER NOT NULL REFERENCES value_set(value_set_id),
    code_id      INTEGER NOT NULL REFERENCES value_code(code_id),
    PRIMARY KEY (value_set_id, code_id)
) WITHOUT ROWID;
CREATE INDEX idx_value_set_member_code ON value_set_member(code_id);

CREATE TABLE unika_summary (
    register_id INTEGER,
    regvar_id INTEGER,
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
    var_id INTEGER PRIMARY KEY,
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

-- Search indexes (both content-synced to avoid storing text twice).
-- Columns mirror the renamed `register` table. `registerrubrik` was dropped,
-- so the index no longer references it.
CREATE VIRTUAL TABLE register_fts USING fts5(
    register_id,
    name,
    purpose,
    content='register',
    content_rowid='rowid'
);

CREATE VIRTUAL TABLE variable_fts USING fts5(
    register_id,
    var_id,
    name,
    definition,
    description,
    content='variable',
    content_rowid='rowid',
    tokenize='unicode61'
);

CREATE VIRTUAL TABLE classification_fts USING fts5(
    short_name,
    name,
    name_en,
    description,
    content='classification',
    content_rowid='id',
    tokenize='unicode61'
);

-- Performance indexes
CREATE INDEX idx_register_variant_register ON register_variant(register_id);
CREATE INDEX idx_register_version_regvar ON register_version(regvar_id);
CREATE INDEX idx_variable_instance_register ON variable_instance(register_id);
CREATE INDEX idx_variable_instance_var ON variable_instance(register_id, var_id);
CREATE INDEX idx_variable_instance_regvar ON variable_instance(regvar_id);
CREATE INDEX idx_variable_instance_regver ON variable_instance(regver_id);
CREATE INDEX idx_variable_instance_classification ON variable_instance(classification_id)
    WHERE classification_id IS NOT NULL;
CREATE INDEX idx_variable_alias_delivery_column_name ON variable_alias(delivery_column_name);
CREATE INDEX idx_value_code_code ON value_code(code);

-- Pre-aggregated code→variable mapping for search --value. Built from
-- the year-projected value_set_member rows joined through
-- variable_instance.value_set_id, so a code only appears here for
-- (register, var) pairs where it was valid at some cvid year.
CREATE TABLE code_variable_map (
    code_id INTEGER NOT NULL REFERENCES value_code(code_id),
    register_id INTEGER NOT NULL,
    var_id INTEGER NOT NULL,
    PRIMARY KEY (code_id, register_id, var_id)
) WITHOUT ROWID;

-- Curated cross-rename equivalence edges (§5.5). Edges are slug-anchored,
-- not cvid-anchored, so the link survives across rebuilds even if the
-- underlying provider IDs shift. Each TOML same_as entry becomes two
-- rows (A→B and B→A) so the resolver does a single forward lookup.
-- a_variant / a_period and b_variant / b_period default to '' (empty
-- string) rather than NULL because SQLite's UNIQUE/PRIMARY KEY treats
-- NULLs as distinct, which would let duplicate edges sneak in; '' as
-- the sentinel keeps the (PROVIDER, REGISTER, VARIANT, PERIOD, VARIABLE)
-- tuple a strict equality key.
CREATE TABLE variable_same_as (
    a_provider     TEXT NOT NULL,
    a_register     TEXT NOT NULL,
    a_variant      TEXT NOT NULL DEFAULT '',
    a_period       TEXT NOT NULL DEFAULT '',
    a_variable     TEXT NOT NULL,
    b_provider     TEXT NOT NULL,
    b_register     TEXT NOT NULL,
    b_variant      TEXT NOT NULL DEFAULT '',
    b_period       TEXT NOT NULL DEFAULT '',
    b_variable     TEXT NOT NULL,
    PRIMARY KEY (
        a_provider, a_register, a_variant, a_period, a_variable,
        b_provider, b_register, b_variant, b_period, b_variable
    )
) WITHOUT ROWID;
CREATE INDEX idx_variable_same_as_a ON variable_same_as(
    a_provider, a_register, a_variable
);

-- §5.5 / §5.7 variable_related_to: weaker edge than same_as. Used by A2.2's
-- build-time triage to record the (N choose 2) symmetric relationships
-- between siblings split from one source variable (e.g. `Hemkommun` vs
-- `Skolkommun` on a shared variable id). Also a curation slot in slug TOMLs
-- for cross-register relationships the algorithm can't see — `relation_kind`
-- is part of the PK so the same (A, B) pair can carry multiple kinds
-- (e.g. both `same_definition_different_column` and `code_vs_label_pair`).
-- `note` carries provenance: 'auto:triage' for algorithm-emitted edges,
-- curator-supplied text for TOML edges. Edges are symmetric: insert A→B
-- and B→A so the resolver does a single forward lookup.
CREATE TABLE variable_related_to (
    a_provider TEXT NOT NULL,
    a_register TEXT NOT NULL,
    a_variant  TEXT NOT NULL,
    a_variable TEXT NOT NULL,
    b_provider TEXT NOT NULL,
    b_register TEXT NOT NULL,
    b_variant  TEXT NOT NULL,
    b_variable TEXT NOT NULL,
    relation_kind TEXT NOT NULL,
    note          TEXT,
    PRIMARY KEY (a_provider, a_register, a_variant, a_variable,
                 b_provider, b_register, b_variant, b_variable, relation_kind)
) WITHOUT ROWID;
CREATE INDEX idx_variable_related_to_a ON variable_related_to(
    a_provider, a_register, a_variable
);

CREATE TABLE classification_same_as (
    a_provider              TEXT NOT NULL,
    a_classification_slug   TEXT NOT NULL,
    b_provider              TEXT NOT NULL,
    b_classification_slug   TEXT NOT NULL,
    PRIMARY KEY (
        a_provider, a_classification_slug,
        b_provider, b_classification_slug
    )
) WITHOUT ROWID;

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


# Sibling provenance DB (REFACTOR_SPEC §4.4 / §5.8). Maintainer-only artifact;
# not shipped to consumers. Sits next to the universal DB. Per-provider source
# linkage tables (e.g. scb_register_id_map) and adapter parse warning tables
# land in later stages (A4.x) once concrete adapters emit IR; for A1.3 the
# only required table is build_manifest which ties provenance to a specific
# universal DB by sha256.
PROVENANCE_DB_FILENAME = "reg_meta.provenance.db"

PROVENANCE_DDL = """\
CREATE TABLE build_manifest (
    schema_version TEXT NOT NULL,
    universal_db_path TEXT NOT NULL,
    universal_db_sha256 TEXT NOT NULL,
    build_date TEXT NOT NULL
);
"""


def create_empty_provenance_db(path: Path) -> None:
    """Create an empty provenance DB with just the build_manifest schema.

    The materializer (A4.x) will populate build_manifest after the universal
    DB is finalized and its sha256 is known. For A1.3 this is a pure
    scaffolding helper — it creates the file and applies the DDL, nothing
    more. Idempotent only in the sense that callers are expected to call
    `rotate_db_to_prev` first; this function refuses to overwrite an
    existing file to keep the rotation contract obvious.
    """
    if path.exists():
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="provenance_db_exists",
            error_class="configuration",
            message=f"Provenance DB already exists: {path}",
            remediation="Call rotate_db_to_prev first, or delete the file.",
        )
    conn = sqlite3.connect(path)
    try:
        conn.executescript(PROVENANCE_DDL)
        conn.commit()
    finally:
        conn.close()


def rotate_db_to_prev(db_path: Path) -> None:
    """Rename `<db_path>` to `<db_path>.prev`, evicting any prior `.prev`.

    Used before the materializer writes the new universal DB / provenance
    DB so a single previous generation survives a rebuild. No auto-cleanup
    of older generations — maintainers `mv` the `.prev` aside if they
    want to keep more than one (REFACTOR_SPEC §4.4 / §5.8).

    No-op if `<db_path>` does not exist (first-ever build).
    """
    if not db_path.exists():
        return
    prev_path = db_path.with_suffix(db_path.suffix + ".prev")
    # Drop the previous-generation file if present — single-generation
    # rotation, per spec. SCB rebuilds are coarse, so an explicit "you
    # asked for this" rename overwrite is fine.
    if prev_path.exists():
        prev_path.unlink()
    db_path.rename(prev_path)


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
            raise RegMetaError(
                exit_code=EXIT_CONFIG,
                code="csv_empty",
                error_class="configuration",
                message=f"CSV file is empty: {path.name}",
                remediation="Re-export the file from mikrometadata.scb.se.",
            ) from exc

        header = [_decode_cp1252(v) for v in raw_header]

        expected = EXPECTED_HEADERS.get(path.name)
        if expected and header != expected:
            raise RegMetaError(
                exit_code=EXIT_CONFIG,
                code="csv_bad_header",
                error_class="configuration",
                message=f"Unexpected header in {path.name}.",
                remediation="Ensure the file is an unmodified SCB metadata export.",
            )

        def row_iter() -> Iterator[tuple[int, dict[str, str]]]:
            for row_number, fields in enumerate(reader, start=2):
                if len(fields) != len(header):
                    raise RegMetaError(
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


_PAREN_ABBREV_RE = re.compile(r"\(([^)]+)\)")


def _extract_abbrev(register_name: str) -> str | None:
    """Extract parenthesized abbreviation from a register name, e.g. '(RTB)' → 'RTB'."""
    m = _PAREN_ABBREV_RE.search(register_name)
    return m.group(1).strip() if m else None


def _build_register_lookup(
    registers: dict[int, dict[str, Any]],
) -> tuple[dict[str, tuple[int, str]], dict[str, tuple[int, str]]]:
    """Build lookup tables for source register resolution.

    Returns (name_lookup, abbrev_lookup):
    - name_lookup: lowercase full name → (register_id, display abbreviation)
    - abbrev_lookup: lowercase "(ABBREV)" → (register_id, display abbreviation)
    """
    name_lookup: dict[str, tuple[int, str]] = {}
    abbrev_lookup: dict[str, tuple[int, str]] = {}
    for rid, rinfo in registers.items():
        rname = rinfo["name"] or ""
        paren = _extract_abbrev(rname)
        entry = (rid, paren or rname)
        name_lookup[rname.lower()] = entry
        if paren:
            abbrev_lookup[paren.lower()] = entry
    return name_lookup, abbrev_lookup


def _resolve_source_register(
    kalla: str,
    name_lookup: dict[str, tuple[int, str]],
    abbrev_lookup: dict[str, tuple[int, str]],
) -> tuple[int | None, str | None]:
    """Resolve variabelregister_kalla to (source_register_id, source_label).

    Deterministic matching only. On match, returns (FK, display_abbrev).
    On no match, returns (None, raw_kalla). On empty input, returns (None, None).
    """
    kalla = (kalla or "").strip()
    if not kalla:
        return None, None

    # Strategy 1: parenthesized abbreviation in kalla
    # e.g. "Befolkningsregistret (RTB) : Folkbokförda personer" → "RTB"
    m = _PAREN_ABBREV_RE.search(kalla)
    if m:
        abbrev = m.group(1).strip().lower()
        # Match against known register abbreviations
        if abbrev in abbrev_lookup:
            return abbrev_lookup[abbrev]
        # Exact match of abbreviation against full register name
        if abbrev in name_lookup:
            return name_lookup[abbrev]

    # Strategy 2: text before " : " as exact register name
    if " : " in kalla:
        before = kalla.split(" : ", 1)[0].strip().lower()
        if before in name_lookup:
            return name_lookup[before]

    # Strategy 3: whole kalla as exact register name
    kalla_lower = kalla.lower()
    if kalla_lower in name_lookup:
        return name_lookup[kalla_lower]

    # No deterministic match — store raw text
    return None, kalla


def _import_registerinformation(
    conn: sqlite3.Connection, path: Path
) -> tuple[int, dict[tuple[str, str, str, str], tuple[int, int]], set[int]]:
    """Import Registerinformation.csv into all core normalized tables.

    Returns (row_count, unika_join, known_cvids) for cross-file joining.
    """
    registers: dict[int, dict[str, Any]] = {}
    variants: dict[int, dict[str, Any]] = {}
    versions: dict[int, dict[str, Any]] = {}
    variables: dict[tuple[int, int], dict[str, Any]] = {}
    instances: dict[int, dict[str, Any]] = {}
    aliases: set[tuple[int, str]] = set()
    populations: set[tuple[int, str, str, str, str]] = set()
    object_types: set[tuple[int, str, str]] = set()
    contexts: set[tuple[int, str, str]] = set()

    # For joining UnikaRegisterOchVariabler later
    unika_join: dict[tuple[str, str, str, str], tuple[int, int]] = {}

    row_count = 0
    _progress("Importing Registerinformation.csv...")

    with _open_scb_csv(path) as (_, rows):
        for row_number, row in rows:
            row_count += 1
            if row_count % 500_000 == 0:
                _progress(f"  ...{row_count:,} rows")

            rid = int(row["RegisterId"])
            rvid = int(row["RegVarID"])
            rveid = int(row["RegVerID"])
            vid = int(row["VarId"])
            cvid = int(row["CVID"])

            registers.setdefault(
                rid,
                {
                    "register_id": rid,
                    # `Registernamn` (CSV header) is the wire format; the
                    # in-memory dict and DB column are universal English.
                    # `Registerrubrik` is dropped per §5.11.
                    "name": row["Registernamn"],
                    "purpose": row["Registersyfte"],
                },
            )

            variants.setdefault(
                rvid,
                {
                    "regvar_id": rvid,
                    "register_id": rid,
                    # `Registervariantrubrik` and `RegistervariantSekretess`
                    # are dropped per §5.11.
                    "name": row["Registervariantnamn"],
                    "description": row["Registervariantbeskrivning"],
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
                    # §5.11: the operational definition merges into
                    # `description` at ingest when distinct + non-empty (see
                    # `_merge_operational_definition` after the fill loop).
                    # `variabelreferenstid`, `variabelhamtadfran`, and
                    # `variabelextern_kommentar` are dropped entirely.
                    "name": row["Variabelnamn"],
                    "definition": row["Variabeldefinition"],
                    "description": row["Variabelbeskrivning"],
                    "_operational_definition": row["VariabelOperationell_definition"],
                    "source_register_text": row["VariabelRegister_Källa"],
                    "measurement_unit": row["Mattenhet"],
                },
            )
            # Fill empty fields from later rows. The CSV header column on the
            # right stays Swedish (wire format); the in-memory key on the left
            # is universal English. `_operational_definition` is a transient
            # carrier folded into `description` after this loop.
            for tgt, src_col in [
                ("name", "Variabelnamn"),
                ("definition", "Variabeldefinition"),
                ("description", "Variabelbeskrivning"),
                ("_operational_definition", "VariabelOperationell_definition"),
                ("source_register_text", "VariabelRegister_Källa"),
                ("measurement_unit", "Mattenhet"),
            ]:
                var[tgt] = _first_non_empty(var[tgt], row[src_col])

            instances.setdefault(
                cvid,
                {
                    "cvid": cvid,
                    "register_id": rid,
                    "regvar_id": rvid,
                    "regver_id": rveid,
                    "var_id": vid,
                    # Per-cvid raw variabelnamn. SCB ships one row per
                    # (cvid, kolumnnamn) tuple — multiple rows per cvid agree
                    # on Variabelnamn, so the first wins and `setdefault`
                    # captures the intended value.
                    "variabelnamn": row["Variabelnamn"],
                    "data_type": row["Datatyp"],
                    "data_length": row["Datalängd"],
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

    # Resolve source register for composite variables
    _progress("Resolving source registers...")
    name_lookup, abbrev_lookup = _build_register_lookup(registers)
    for var in variables.values():
        src_id, src_label = _resolve_source_register(
            var["source_register_text"], name_lookup, abbrev_lookup
        )
        var["source_register_id"] = src_id
        var["source_label"] = src_label

    # Bulk insert all normalized tables
    _progress("Writing core tables...")
    conn.executemany(
        "INSERT INTO register (register_id, provider_id, name, purpose) "
        f"VALUES (:register_id, {PROVIDER_ID_SCB}, :name, :purpose)",
        list(registers.values()),
    )
    conn.executemany(
        "INSERT INTO register_variant ("
        "regvar_id, register_id, name, description"
        ") VALUES ("
        ":regvar_id, :register_id, :name, :description"
        ")",
        list(variants.values()),
    )
    conn.executemany(
        "INSERT INTO register_version "
        "(regver_id, regvar_id, registerversionnamn, "
        "registerversionbeskrivning, registerversionmatinformation, "
        "registerversion_docstaus, registerversion_forstagodkannandedatum, "
        "registerversion_senastgodkanddatum) VALUES ("
        ":regver_id, :regvar_id, :registerversionnamn, "
        ":registerversionbeskrivning, :registerversionmatinformation, "
        ":registerversion_docstaus, :registerversion_forstagodkannandedatum, "
        ":registerversion_senastgodkanddatum)",
        list(versions.values()),
    )
    # Named INSERT (explicit columns). Two reasons the list is explicit:
    # (1) the variable dict carries the transient `_operational_definition`
    # key which we merge into `description` below then drop before binding;
    # (2) A1.2's `is_sensitive` / `is_identifier` columns on `variable`
    # are intentionally omitted here so they keep their DDL DEFAULT 0 —
    # `_populate_sensitivity_flags` writes them later after unika_summary
    # is loaded.
    for var in variables.values():
        op = (var.pop("_operational_definition", None) or "").strip()
        desc = (var.get("description") or "").strip()
        # §5.11: operational definition folds into description when distinct
        # + non-empty (e.g. SCB's "OperationellDef" carries a refinement over
        # the plain definition). Use a newline-separated concat that survives
        # round-trips through CSV / JSON. The `op not in desc` guard catches
        # the partial-substring case as well as exact duplicates — important
        # when a rebuild re-imports a CSV whose `description` already carries
        # the previously-merged operational text.
        if op and op not in desc:
            var["description"] = f"{desc}\n\n{op}".strip() if desc else op
    conn.executemany(
        "INSERT INTO variable (register_id, var_id, name, definition, description, "
        "source_register_text, measurement_unit, source_register_id, source_label) "
        "VALUES (:register_id, :var_id, :name, :definition, :description, "
        ":source_register_text, :measurement_unit, :source_register_id, :source_label)",
        list(variables.values()),
    )
    conn.executemany(
        "INSERT INTO variable_instance "
        "(cvid, register_id, regvar_id, regver_id, var_id, variabelnamn, "
        " data_type, data_length) "
        "VALUES (:cvid, :register_id, :regvar_id, :regver_id, "
        ":var_id, :variabelnamn, :data_type, :data_length)",
        list(instances.values()),
    )
    conn.executemany(
        "INSERT INTO variable_alias (cvid, delivery_column_name) VALUES (?, ?)",
        sorted(aliases),
    )
    conn.executemany(
        "INSERT INTO population (regver_id, name, definition, comment, date_range) "
        "VALUES (?, ?, ?, ?, ?)",
        sorted(populations),
    )
    conn.executemany(
        "INSERT INTO object_type (regver_id, name, definition) VALUES (?, ?, ?)",
        sorted(object_types),
    )
    conn.executemany(
        "INSERT INTO variable_context (cvid, population_name, object_type_name) "
        "VALUES (?, ?, ?)",
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
    unika_join: dict[tuple[str, str, str, str], tuple[int, int]],
) -> int:
    _progress("Importing UnikaRegisterOchVariabler.csv...")
    row_count = 0
    batch: list[tuple[int | str, ...]] = []

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


def _populate_sensitivity_flags(conn: sqlite3.Connection) -> int:
    """Propagate sensitivity / identifier flags from `unika_summary` to
    `variable.is_sensitive` / `variable.is_identifier` (A1.2).

    Aggregation rule: a variable inherits a flag if ANY `unika_summary` row
    for the same `(register_id, var_id)` carries the truthy text. SCB ships
    these columns as the Swedish text literals `'Ja'` / `'Nej'`; any other
    value (including empty) is treated as falsy. `kanslig_variabel` and
    `kanslig_variabel_ibland` both fold into `is_sensitive` — the ~22 rows
    flagged only-sometimes don't justify a third column (see MIGRATION_PLAN
    A1.2). Returns the number of variable rows whose flags were refreshed
    from `unika_summary` — i.e. rows that had at least one matching
    unika_summary entry. This count includes variables whose flags resolved
    to 0/0 (all-`Nej` entries); the function is deterministic in the
    unika_summary state, not a "received at least one true flag" gauge.

    Must run after both `_import_registerinformation` (creates `variable`
    rows) and `_import_unika` (populates the source). Idempotent: re-running
    on a populated DB resets every row from `unika_summary` again.

    `unika_summary` stores `(register_id, regvar_id, kolumnnamn, variabelnamn)`
    but not `var_id`. To resolve `var_id` we route the join through
    `variable_instance × variable_alias`: the `(register_id, regvar_id,
    kolumnnamn)` triple narrows to a single cvid in the source CSV, and
    `variable_instance.var_id` then carries the `var_id` we need. We also
    join `variable` on `variabelnamn` (now `variable.name` post-A1.1) so
    that when the same `kolumnnamn` is reused across distinct variables
    under one variant (rename / id split mid-variant), each `unika_summary`
    row maps to exactly one `var_id` instead of fanning sensitivity flags
    sideways onto siblings. Joining `variable_alias` on the full
    `(cvid, delivery_column_name)` PK rather than `delivery_column_name`
    alone also lets SQLite use the PK index instead of falling back to a
    scan / auto-index.
    """
    _progress("Populating variable sensitivity flags from unika_summary...")
    cur = conn.execute(
        "UPDATE variable SET "
        "    is_sensitive = COALESCE(flags.is_sensitive, 0), "
        "    is_identifier = COALESCE(flags.is_identifier, 0) "
        "FROM ("
        "    SELECT "
        "        vi.register_id, vi.var_id, "
        "        MAX(CASE WHEN us.kanslig_variabel = 'Ja' "
        "                  OR us.kanslig_variabel_ibland = 'Ja' "
        "                 THEN 1 ELSE 0 END) AS is_sensitive, "
        "        MAX(CASE WHEN us.identitetsvariabel = 'Ja' "
        "                 THEN 1 ELSE 0 END) AS is_identifier "
        "    FROM unika_summary us "
        "    JOIN variable_instance vi "
        "      ON vi.register_id = us.register_id "
        "     AND vi.regvar_id = us.regvar_id "
        # `unika_summary` keeps Swedish column names — A1.1 didn't touch
        # that table because A2.1 drops it. `variable_alias` and
        # `variable` were renamed: `kolumnnamn` → `delivery_column_name`,
        # `variabelnamn` → `name`.
        "    JOIN variable_alias va "
        "      ON va.cvid = vi.cvid "
        "     AND va.delivery_column_name = us.kolumnnamn "
        "    JOIN variable v "
        "      ON v.register_id = vi.register_id "
        "     AND v.var_id = vi.var_id "
        "     AND v.name = us.variabelnamn "
        "    GROUP BY vi.register_id, vi.var_id"
        ") AS flags "
        "WHERE variable.register_id = flags.register_id "
        "  AND variable.var_id = flags.var_id"
    )
    refreshed = cur.rowcount or 0
    _progress(f"  {refreshed:,} variable rows refreshed from unika_summary")
    return refreshed


# A2.1: SCB ships VersionForsta/VersionSista as plain year strings ("2020").
# §5.1 requires `variable_state.valid_from`/`valid_to` as full YYYY-MM-DD.
# Expansion rules are deterministic — year N → first day Jan / last day Dec.
# Open-ended (no upper bound observable) → sentinel '9999-12-31'.
_VALID_TO_OPEN_SENTINEL = "9999-12-31"
# Lower bound when no year is derivable from any signal (yearless cvids
# like "Person-År" with no unika_summary backing). Picked to sort before
# any real date so range queries treat the row as "valid since forever".
_VALID_FROM_UNKNOWN = "0001-01-01"


def _year_to_iso_from(year: int | None) -> str | None:
    """Year N → 'N-01-01'. Returns None when year is None so callers can
    distinguish "no signal" from a real year."""
    if year is None:
        return None
    return f"{year:04d}-01-01"


def _year_to_iso_to(year: int | None) -> str | None:
    """Year N → 'N-12-31'. None preserved for the same reason as
    `_year_to_iso_from`."""
    if year is None:
        return None
    return f"{year:04d}-12-31"


def _parse_unika_year(raw: str | None) -> int | None:
    """Parse a `VersionForsta` / `VersionSista` cell. Empty / unparseable
    yields None so the coalescer can fall back to the register_version
    name; we don't second-guess what a stray non-year string means."""
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        # SCB occasionally writes a date instead of a year; be permissive.
        match = re.search(r"\b(\d{4})\b", raw)
        return int(match.group(1)) if match else None


def _coalesce_variable_states(conn: sqlite3.Connection) -> dict[str, int]:
    """Coalesce `variable_instance` rows into `variable_state` per §5.1.

    Group key: `(register_id, regvar_id, var_id, data_type, data_length,
    value_set_id, value_set_version_label, grain)`.

    `grain` is the transient pre-triage carrier for SCB's `vardemangdsniva`
    (still present on `variable_instance` through A2.2). Keeping it in the
    group key here means multi-grain variables stay distinct so A2.2's
    triage can split them into sibling slugs; grain itself does not land in
    the final schema.

    For each group:

    1. Resolve `(register_id, regvar_id, kolumnnamn, variabelnamn)` from the
       cvids in the group via `variable_alias × variable`. Note the cross-
       product: a single cvid can have N aliases (rare; cross-edition
       drift), so the group's unika lookup keys are the *union* of all
       (regvar, alias, name) triples for the cvids.
    2. Look up `unika_summary` rows for each triple; take `min(VersionForsta)`
       → `valid_from`, `max(VersionSista)` → `valid_to`. Expand year → full
       ISO 8601 (`'2020-01-01'` / `'2020-12-31'`).
    3. Fall back to `register_version.registerversionnamn` (via the cvids'
       `regver_id`) when no unika row matched — year extracted by
       `extract_year`. Fallback gives both bounds the same min/max behavior.
    4. Final fallback (yearless cvids with no unika row): valid_from =
       '0001-01-01', valid_to = '9999-12-31'. Rare; not observed in the
       SCB corpus but the build must produce a writable row regardless.
    5. `delivery_column_name` = the alias attached to the cvid with the
       highest `regver_id` in the group (most-recent era). Resolves ties
       lexically for determinism.

    Returns a stats dict for the manifest.
    """
    _progress("Coalescing variable instances into variable_state...")

    # Pull the candidate set in one query rather than per-group: this is
    # the same ~515K rows the resolver walks today, and SQLite's nested-
    # loop join across `variable_instance × variable_alias × variable
    # × register_version` is faster as a single sweep than re-issued
    # per-group queries. Memory is bounded — each row is small.
    #
    # `variable_alias` is LEFT JOINed: a cvid with no alias row (rare but
    # observed for cvids that only carry a `variabelnamn` and no
    # `kolumnnamn` in the raw CSV) still surfaces so the group is captured
    # with a NULL delivery_column_name instead of being dropped silently.
    #
    # `build_db`'s connection writes with the default tuple row_factory;
    # we use a local cursor with `sqlite3.Row` so the column-name access
    # below stays readable. Doesn't touch the parent connection's setting.
    # `vi.variabelnamn` (per-cvid raw Variabelnamn) is the right key for the
    # unika_summary lookup — NOT `variable.name`. `variable.name` is the
    # first-non-empty canonical chosen at import time, so SCB renaming a
    # variable mid-life leaves the canonical pinned to the old name and the
    # post-rename unika row (often the open-ended "still active" one) fails
    # to match. variable_instance carries the raw per-row name for exactly
    # this reason.
    cur = conn.cursor()
    cur.row_factory = sqlite3.Row
    rows = cur.execute(
        "SELECT vi.cvid, vi.register_id, vi.regvar_id, vi.var_id, vi.regver_id, "
        "       vi.data_type, vi.data_length, vi.value_set_id, "
        "       vi.value_set_version_label, vi.vardemangdsniva AS grain, "
        "       vi.variabelnamn, va.delivery_column_name, "
        "       rv.registerversionnamn "
        "FROM variable_instance vi "
        "LEFT JOIN variable_alias va ON va.cvid = vi.cvid "
        "JOIN register_version rv ON rv.regver_id = vi.regver_id"
    ).fetchall()

    # Group accumulator: key → mutable state. We iterate `rows` once and
    # update `min_year`/`max_year`/`latest_alias` in place. Using a dict
    # rather than itertools.groupby because rows aren't pre-sorted and the
    # group key has 8 components — pre-sorting + groupby would be slower
    # than the dict path.
    @dataclass
    class _StateGroup:
        register_id: int
        regvar_id: int
        var_id: int
        data_type: str | None
        data_length: str | None
        value_set_id: int | None
        value_set_version_label: str | None
        # grain is part of the *group key*, not stored on the output row,
        # so we don't keep it on the accumulator — the dict key carries it.
        unika_min: int | None = None
        unika_max: int | None = None
        # Sticky bit: True iff at least one unika_summary row applied to this
        # group, regardless of whether either bound was populated. Lets the
        # materializer distinguish "no unika row matched → fall back to
        # register_version" from "unika row matched but VersionSista blank →
        # open-ended (sentinel)". A missing upper bound from SCB means the
        # variable is still active; defaulting to regver_max would clamp
        # currently-live variables to the latest observed export year and
        # make A2.5's resolver miss any future-period query.
        unika_matched: bool = False
        # Sticky bit: True iff at least one matching unika row left
        # `VersionSista` blank. The "still active" signal must survive even
        # when OTHER unika rows for the same group carry a bounded
        # VersionSista (e.g. a mid-life rename where the OriginalName row
        # is bounded but the RenamedName row is open) — without this
        # tracker, `unika_max` populated from the bounded row would mask
        # the open-ended signal from the renamed row.
        unika_has_open_top: bool = False
        regver_min: int | None = None
        regver_max: int | None = None
        # Track the cvid → alias mapping with regver_id so we can pick the
        # latest alias deterministically (highest regver_id, then lexically
        # smallest delivery_column_name on ties — both of which are stored
        # below). regver_id alone is sufficient for the ordering; the row's
        # year is only used transiently to update regver_min/max, never as
        # alias-selection input, so it doesn't need to live on the group.
        latest_alias: str | None = None
        latest_alias_regver: int | None = None

    groups: dict[
        tuple[int, int, int, str, str, int | None, str | None, str],
        _StateGroup,
    ] = {}
    # Map (register_id, regvar_id, kolumnnamn, variabelnamn) → set of
    # group keys so the unika fan-out below stays proportional to distinct
    # groups, not raw instance-row hits (a wide variable with many cvids
    # / aliases would otherwise have its single unika row replayed once
    # per row even though min/max is idempotent).
    unika_index: dict[tuple[int, int, str, str], set[tuple]] = {}

    # Max regver year observed per (register_id, regvar_id, var_id). A
    # single unika row covers a variable's entire lifetime, but when the
    # coalescer splits a variable into multiple groups by shape (different
    # data_type / data_length / value_set_id across versions), each split
    # group should only claim the years it was actually observed. The
    # group whose `regver_max` matches this variable-wide max is the
    # "latest era" — only that group can carry unika's open-ended signal,
    # because only that shape was still active at the variable's last
    # known year.
    var_max_regver: dict[tuple[int, int, int], int] = {}

    for row in rows:
        grain = row["grain"] or ""
        gkey = (
            row["register_id"],
            row["regvar_id"],
            row["var_id"],
            row["data_type"] or "",
            row["data_length"] or "",
            row["value_set_id"],
            row["value_set_version_label"] or "",
            grain,
        )
        grp = groups.get(gkey)
        if grp is None:
            grp = _StateGroup(
                register_id=row["register_id"],
                regvar_id=row["regvar_id"],
                var_id=row["var_id"],
                data_type=row["data_type"],
                data_length=row["data_length"],
                value_set_id=row["value_set_id"],
                value_set_version_label=row["value_set_version_label"],
            )
            groups[gkey] = grp

        # Track register_version year per cvid (fallback signal) on the
        # group, and also on the per-variable max so the materializer can
        # identify the latest-era group when clamping unika ranges.
        rver_year = extract_year(row["registerversionnamn"] or "")
        if rver_year is not None:
            grp.regver_min = (
                rver_year if grp.regver_min is None else min(grp.regver_min, rver_year)
            )
            grp.regver_max = (
                rver_year if grp.regver_max is None else max(grp.regver_max, rver_year)
            )
            vkey = (row["register_id"], row["regvar_id"], row["var_id"])
            cur_max = var_max_regver.get(vkey)
            if cur_max is None or rver_year > cur_max:
                var_max_regver[vkey] = rver_year

        # Track the latest alias for the era. "Latest" = highest regver_id
        # in the group; ties broken by lexically smallest alias for
        # reproducibility.
        alias = row["delivery_column_name"]
        if alias:
            regver = row["regver_id"]
            cur_regver = grp.latest_alias_regver
            cur_alias = grp.latest_alias
            # Replace the latest alias when: no alias yet, prior alias had no
            # regver year (unknown beats nothing), strictly newer regver, or
            # same regver but lexically smaller alias for determinism.
            replace = (
                cur_alias is None
                or cur_regver is None
                or regver > cur_regver
                or (regver == cur_regver and alias < (cur_alias or ""))
            )
            if replace:
                grp.latest_alias = alias
                grp.latest_alias_regver = regver

        # Stage the unika lookup. unika_summary's PK is
        # (register_id, regvar_id, kolumnnamn, variabelnamn); we need an
        # alias to build the triple, so cvids without an alias contribute
        # only via the fallback path. The unika_index is a set so that
        # repeat (alias, variabelnamn) sightings across cvids in the same
        # group don't fan a single unika row out into duplicate updates.
        if alias and row["variabelnamn"]:
            ukey = (
                row["register_id"],
                row["regvar_id"],
                alias,
                row["variabelnamn"],
            )
            unika_index.setdefault(ukey, set()).add(gkey)

    _progress(f"  {len(groups):,} state groups from {len(rows):,} instance rows")

    # Pull all relevant unika_summary rows in one shot. The PK lookup is
    # fast (~5 unika rows per group on average), but doing it per-group
    # would issue ~100K SELECTs; one sweep is materially cheaper.
    unika_cur = conn.cursor()
    unika_cur.row_factory = sqlite3.Row
    unika_rows = unika_cur.execute(
        "SELECT register_id, regvar_id, kolumnnamn, variabelnamn, "
        "       version_forsta, version_sista FROM unika_summary"
    ).fetchall()
    for ur in unika_rows:
        ukey = (
            ur["register_id"],
            ur["regvar_id"],
            ur["kolumnnamn"],
            ur["variabelnamn"],
        )
        gkeys = unika_index.get(ukey)
        if not gkeys:
            continue
        first = _parse_unika_year(ur["version_forsta"])
        last = _parse_unika_year(ur["version_sista"])
        for gkey in gkeys:
            grp = groups[gkey]
            # Sticky regardless of which field was populated — even a
            # row that's blank on both sides counts as "unika spoke",
            # though that's a rare case (SCB normally fills VersionForsta).
            grp.unika_matched = True
            if first is not None:
                grp.unika_min = (
                    first if grp.unika_min is None else min(grp.unika_min, first)
                )
            if last is not None:
                grp.unika_max = (
                    last if grp.unika_max is None else max(grp.unika_max, last)
                )
            else:
                # SCB left VersionSista blank for this (kolumnnamn, variabelnamn)
                # tuple → still active. Even if another row in the same group is
                # bounded, the open-ended signal wins (mid-life rename case).
                grp.unika_has_open_top = True

    # Materialize the variable_state rows.
    #
    # Each group's range = its OWN observed `regver_min`/`regver_max`. A
    # `unika_summary` row covers a variable's entire lifetime across all
    # shape groups, so applying its range to every group would (Codex P1
    # on PR #130) produce overlapping ranges between shape groups for
    # multi-era variables and assign earlier-era states the full lifetime
    # they were never present in. Letting each group claim only its own
    # observed years sidesteps both pathologies and stays robust to
    # unika/regver disagreement (e.g. an SCB `HT2020` cvid for a
    # variable whose unika row only mentions 2021 — regver wins, we
    # observed the cvid).
    #
    # Unika still contributes in two narrow ways:
    #   - **Open-ended sentinel** for the latest-era group only. When the
    #     group's `regver_max` matches the variable's overall max
    #     (`var_max_regver`) AND unika matched but left `VersionSista`
    #     blank, set `valid_to = '9999-12-31'` ("still active"). Earlier-
    #     era groups must end at their observed `regver_max` — unika's
    #     open signal applies to the variable's current shape only.
    #   - **Fallback for yearless cvids**: when no row in the group had a
    #     parseable `registerversionnamn` year, unika_min/unika_max
    #     stand in. Rare in the SCB corpus but the build must produce
    #     writable rows regardless.
    batch: list[tuple] = []
    sentinel_count = 0
    fallback_only_count = 0
    open_top_from_unika = 0
    for grp in groups.values():
        vkey = (grp.register_id, grp.regvar_id, grp.var_id)
        var_max = var_max_regver.get(vkey)
        # `None == None` is True — a yearless single-group variable counts
        # as the latest era of itself, so the open-ended sentinel can
        # still apply there.
        is_latest_era = grp.regver_max == var_max

        # Lower bound: regver is authoritative (the years we actually
        # observed the group). Unika is fallback for yearless cvids.
        from_year = grp.regver_min if grp.regver_min is not None else grp.unika_min

        # Upper bound: latest-era group with unika-open → sentinel.
        # Otherwise the group's observed regver_max wins. Unika upper
        # only stands in when regver is unparseable.
        #
        # The open-ended trigger covers two shapes: (a) unika matched the
        # group and `unika_max` was never populated (only blank
        # VersionSista rows applied); (b) `unika_has_open_top` flagged
        # that at least one matching unika row left VersionSista blank
        # even when others did populate `unika_max`. Case (b) is the
        # mid-life-rename pattern — a bounded OriginalName row and an
        # open-ended RenamedName row both apply to the same group. The
        # "still active" signal from the rename must win over the
        # bounded-out OriginalName row.
        if (
            is_latest_era
            and grp.unika_matched
            and (grp.unika_max is None or grp.unika_has_open_top)
        ):
            to_year = None  # forces sentinel
            open_top_from_unika += 1
        elif grp.regver_max is not None:
            to_year = grp.regver_max
        else:
            to_year = grp.unika_max  # yearless fallback; may be None → sentinel

        valid_from = _year_to_iso_from(from_year) or _VALID_FROM_UNKNOWN
        valid_to = _year_to_iso_to(to_year) or _VALID_TO_OPEN_SENTINEL

        if valid_to == _VALID_TO_OPEN_SENTINEL or valid_from == _VALID_FROM_UNKNOWN:
            sentinel_count += 1
        if not grp.unika_matched:
            fallback_only_count += 1

        batch.append(
            (
                grp.register_id,
                grp.regvar_id,
                grp.var_id,
                valid_from,
                valid_to,
                grp.data_type,
                grp.data_length,
                grp.latest_alias,
                grp.value_set_id,
                grp.value_set_version_label,
            )
        )

    conn.executemany(
        "INSERT INTO variable_state (register_id, regvar_id, var_id, "
        "    valid_from, valid_to, data_type, data_length, delivery_column_name, "
        "    value_set_id, value_set_version_label) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        batch,
    )
    state_count = conn.execute("SELECT COUNT(*) FROM variable_state").fetchone()[0]
    _progress(
        f"  {state_count:,} variable_state rows "
        f"({fallback_only_count:,} from register_version fallback, "
        f"{open_top_from_unika:,} open-ended from unika, "
        f"{sentinel_count:,} carry a date sentinel)"
    )
    return {
        "n_state_groups": len(groups),
        "n_variable_states": state_count,
        "n_from_unika": state_count - fallback_only_count,
        "n_from_regver_fallback": fallback_only_count,
        # Currently-active variables: SCB matched a unika row but left
        # VersionSista blank → state.valid_to = '9999-12-31'. A2.5's
        # resolver needs these to survive future-period queries.
        "n_open_top_from_unika": open_top_from_unika,
        "n_with_sentinel": sentinel_count,
    }


# ---------------------------------------------------------------------------
# A2.2: build-time triage (§5.7)
# ---------------------------------------------------------------------------

# Regex patterns for `vardemangdsniva`-derived sibling slug suffixes (§5.7).
# Order matters: the position-pattern is the most specific and the most
# common in SCB data; the rest are ordered so the more-specific patterns
# win when a niva string contains overlapping tokens.
_NIVA_POSITION_RE = re.compile(r"\b(\d+)\s*position(er)?\b", re.IGNORECASE)
_NIVA_NIVAOLD_RE = re.compile(r"\bnivaold\b", re.IGNORECASE)
_NIVA_GROV_RE = re.compile(r"\bgrov(?:\s+gruppering)?\b", re.IGNORECASE)
_NIVA_DETALJ_RE = re.compile(r"\bdetalj(?:grupp(er)?)?\b", re.IGNORECASE)
_NIVA_ALFA_RE = re.compile(r"\b(alfa|alpha)\b", re.IGNORECASE)
_NIVA_HUVUDGRUPP_RE = re.compile(r"\bhuvudgrupp\b", re.IGNORECASE)
_NIVA_AVDELNING_RE = re.compile(r"\bavdelning\b", re.IGNORECASE)
_NIVA_UNDERGRUPP_RE = re.compile(r"\bundergrupp\b", re.IGNORECASE)


def _niva_suffix(niva: str | None) -> str | None:
    """Map a `vardemangdsniva` text to a sibling slug suffix per §5.7 rule 2.

    Returns the suffix (e.g. `-3pos`, `-grov`) or None if no pattern matches.
    Suffix delimiter is `-` so slugs stay inside the §5.2 grammar.
    """
    if not niva:
        return None
    m = _NIVA_POSITION_RE.search(niva)
    if m is not None:
        return f"-{m.group(1)}pos"
    if _NIVA_NIVAOLD_RE.search(niva):
        return "-old"
    if _NIVA_GROV_RE.search(niva):
        return "-grov"
    if _NIVA_DETALJ_RE.search(niva):
        return "-detalj"
    if _NIVA_ALFA_RE.search(niva):
        return "-alfa"
    if _NIVA_HUVUDGRUPP_RE.search(niva):
        return "-huvud"
    if _NIVA_AVDELNING_RE.search(niva):
        return "-avd"
    if _NIVA_UNDERGRUPP_RE.search(niva):
        return "-under"
    return None


def _kolumnnamn_suffixes(kolumnnamn_set: list[set[str]]) -> list[str] | None:
    """Derive sibling slug suffixes from N distinct kolumnnamn groups.

    Returns a parallel list of slugs (already including any common-prefix
    stripping) or None when no usable kolumnnamn-derived split is possible
    (e.g. one of the groups carries an empty set, or the resulting slugs
    fail the §5.2 grammar). Caller falls back to niva / datalangd / hash
    when this returns None.

    Strategy:

    1. Pick one representative kolumnnamn per group (lexically smallest, for
       determinism). A group with no kolumnnamn carries no information and
       forces a fallback path — return None.
    2. Compute a common ASCII-folded prefix across representatives. If the
       prefix is non-trivial (≥ 2 chars and ends on a word boundary in at
       least one representative), strip it from each name and use the
       remainder as the discriminator suffix.
    3. Strip leading punctuation/underscores from the discriminator before
       folding to a slug. Heuristic word-end normalization: `St` → `start`,
       `Sl` → `slut` when the stripped remainder matches the literal token
       (so `Utbild_St` becomes `utbild-start`, `Utbild_Sl` becomes
       `utbild-slut`). Otherwise lowercase and replace underscores with `-`.
    4. The final suffix becomes `<prefix>-<remainder>` when a common prefix
       was found; otherwise it's the lowercased-and-hyphenated full name
       (caller still adds the conflict tiebreaker `-a`/`-b` if needed).

    Empty representative sets → None (caller falls back to niva-pattern).
    """
    if any(not s for s in kolumnnamn_set):
        return None
    reps = [sorted(s)[0] for s in kolumnnamn_set]
    # Common-prefix heuristic: longest shared character prefix across the
    # ASCII-folded lowercase reps. Tracked as a length over the folded
    # representation so the original-case prefix can still be re-extracted
    # from one of the reps when we need it.
    folded = [_ascii_fold_lower(r) for r in reps]
    common_len = 0
    if folded:
        shortest = min(len(f) for f in folded)
        for i in range(shortest):
            ch = folded[0][i]
            if all(f[i] == ch for f in folded):
                common_len += 1
            else:
                break
    # `Hemkommun` / `Skolkommun` share NO prefix (`H`/`S` differ at 0);
    # we still want to land on `kommun-hem` / `kommun-skol`. When the
    # forward prefix is short, fall back to a common-SUFFIX search and
    # use the suffix as the stem.
    if common_len < 2:
        common_suffix = _common_suffix_len(folded)
        if common_suffix >= 3:
            stem = folded[0][len(folded[0]) - common_suffix :]
            suffixes = []
            for rep, fr in zip(reps, folded, strict=True):
                prefix_part = fr[: len(fr) - common_suffix]
                prefix_part = prefix_part.strip("_- ")
                if not prefix_part:
                    return None
                suffixes.append(f"-{stem}-{_word_to_slug(prefix_part)}")
            return suffixes
        # No usable common pattern. Fall through to per-rep slugging.
        return _per_rep_slugs(reps)
    # Forward-prefix path. Strip the common prefix from each rep and use
    # the remainder. Reject when stripping would leave nothing on any
    # rep (common prefix == full string → no discriminator).
    suffixes: list[str] = []
    for rep, fr in zip(reps, folded, strict=True):
        remainder = fr[common_len:].strip("_- ")
        if not remainder:
            return None
        suffixes.append(f"-{_word_to_slug(remainder)}")
    return suffixes


def _common_suffix_len(folded: list[str]) -> int:
    """Longest shared character suffix across the ASCII-folded reps."""
    if not folded:
        return 0
    shortest = min(len(f) for f in folded)
    suffix = 0
    for i in range(1, shortest + 1):
        ch = folded[0][-i]
        if all(f[-i] == ch for f in folded):
            suffix += 1
        else:
            break
    return suffix


def _word_to_slug(raw: str) -> str:
    """Lowercase + heuristic abbrev expansion + hyphenated slug body.

    `St` / `Sl` on standalone-token boundaries expand to `start` / `slut`
    (the §5.7 example from `Utbild_St` / `Utbild_Sl`). Other tokens get
    lowercased with underscores becoming hyphens. NFKD-ASCII folding so
    Swedish diacritics survive into the §5.2 slug grammar.
    """
    folded = (
        unicodedata.normalize("NFKD", raw)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    # Heuristic expansions BEFORE the underscore replacement so the token
    # boundary is preserved. SCB uses `St` / `Sl` as `start` / `slut`
    # abbreviations on `Utbild_St` / `Utbild_Sl`.
    tokens = re.split(r"[_\s\-]+", folded)
    expanded = []
    for tok in tokens:
        if tok == "st":
            expanded.append("start")
        elif tok == "sl":
            expanded.append("slut")
        elif tok:
            expanded.append(tok)
    body = "-".join(expanded)
    # Replace any leftover non-alphanumerics with `-`, collapse runs,
    # and trim.
    return re.sub(r"[^a-z0-9]+", "-", body).strip("-")


def _per_rep_slugs(reps: list[str]) -> list[str]:
    """Fallback when no common prefix/suffix carves the kolumnnamn group:
    each rep becomes its own slug (lowercased, hyphenated, ASCII-folded).
    """
    return [f"-{_word_to_slug(r)}" for r in reps]


def _ascii_fold_lower(s: str) -> str:
    """Lowercase NFKD-ASCII fold; helper for the prefix/suffix overlap math."""
    return (
        unicodedata.normalize("NFKD", s)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )


def _triage_year_from_iso(iso_date: str) -> int | None:
    """Extract a year from a `YYYY-MM-DD` ISO date. Sentinel dates return None."""
    if not iso_date or iso_date == _VALID_FROM_UNKNOWN:
        return None
    try:
        return int(iso_date[:4])
    except ValueError:
        return None


def _triage_year_bucket(valid_from: str, valid_to: str) -> tuple[int, ...]:
    """Years a state row covers (used to group `variable_state` rows into
    same-year buckets). Returns the lexical year-range covered by
    `[valid_from, valid_to]`, capped at 200 years to defend against the
    sentinel `9999-12-31` blowing up memory if a state somehow spans the
    full range.

    Returns an empty tuple for fully-sentinel rows
    (`valid_from = '0001-01-01'` AND `valid_to = '9999-12-31'`) — these
    are unparseable and the triage skips them.
    """
    yf = _triage_year_from_iso(valid_from)
    yt = _triage_year_from_iso(valid_to)
    if yf is None and yt is None:
        return ()
    # After the all-None bail we have at least one bound; backfill the
    # missing side from the populated one so downstream arithmetic
    # stays well-typed.
    if yf is None:
        assert yt is not None
        yf = yt
    if yt is None or yt > 9000:
        # Sentinel open-end: collapse to lower bound only. Triage's job
        # is finding same-year multi-state collisions; a state that
        # extends to the open sentinel only shares its lower bound's
        # year (and the natural extension years up to the variable's
        # latest observed year — but we can't know that without the
        # cross-state context, and the same-year detection only needs
        # to see the lower bound's year).
        return (yf,)
    if yt < yf:
        return ()
    if yt - yf > 200:
        return tuple(range(yf, yf + 200))
    return tuple(range(yf, yt + 1))


def _triage_same_year_collisions(
    conn: sqlite3.Connection,
    slug_dir: Path | None = None,
) -> dict[str, int]:
    """Resolve same-year multi-state collisions per §5.7.

    Reads `variable_state` (the just-emitted A2.1 rows) plus the
    `variable_instance × variable_alias` join so each state carries the
    transient SCB-source fields (`kolumnnamn`, `vardemangdsniva`,
    `data_type`, `data_length`) the §5.7 algorithm inspects.

    The result is a mix of in-place edits to `variable_state` (sibling
    re-keying, drops, collapses), inserts to `variable` (new sibling
    variable rows), and inserts to `variable_related_to` (provenance
    edges with `note = 'auto:triage'`).

    TOML override is "light" at A2.2: if a `[variable."<id>"]` block in
    a provider TOML carries an explicit `slug`, that wins over the
    auto-derived sibling slug for siblings allocated against that
    variable id. Bulk curation comes in a separate PR.
    """
    _progress("Triaging same-year variable_state collisions (§5.7)...")

    # Pre-load TOML variable slugs for the override-light path.
    # Shape: { (provider_slug, register_source_id, var_source_id): slug }
    # where source IDs are the raw TOML keys (`"34"`, `"34.137"`).
    toml_variable_slugs: dict[tuple[str, str], str] = {}
    if slug_dir is not None:
        toml_variable_slugs = _load_toml_variable_slugs(slug_dir)

    cur = conn.cursor()
    cur.row_factory = sqlite3.Row

    # Pull the candidate set. variable_state carries the post-coalesce
    # shape; variable_instance + variable_alias carry the discriminators
    # (kolumnnamn, vardemangdsniva, classification_id, value_set_id).
    # We join through (register_id, regvar_id, var_id, value_set_id,
    # value_set_version_label, data_type, data_length) which is the
    # post-coalesce identity of a state — one instance maps to exactly
    # one state when those fields all match.
    #
    # We use LEFT JOIN on variable_alias because cvids may have no alias
    # (rare; pure-name pre-triage rows). NULL kolumnnamn rows still need
    # to surface so the "drop truly-empty stubs" rule can fire.
    state_rows = cur.execute(
        "SELECT state_id, register_id, regvar_id, var_id, valid_from, valid_to, "
        "       data_type, data_length, delivery_column_name, "
        "       value_set_id, value_set_version_label "
        "FROM variable_state"
    ).fetchall()

    # Map (register_id, regvar_id, var_id, data_type, data_length,
    #      value_set_id, value_set_version_label) → list[state_id].
    # That's the post-coalesce identity. Used to join state → instance.
    state_by_identity: dict[tuple, list[int]] = {}
    state_meta: dict[int, dict[str, Any]] = {}
    for r in state_rows:
        identity = (
            r["register_id"],
            r["regvar_id"],
            r["var_id"],
            r["data_type"] or "",
            r["data_length"] or "",
            r["value_set_id"],
            r["value_set_version_label"] or "",
        )
        state_by_identity.setdefault(identity, []).append(r["state_id"])
        state_meta[r["state_id"]] = {
            "register_id": r["register_id"],
            "regvar_id": r["regvar_id"],
            "var_id": r["var_id"],
            "valid_from": r["valid_from"],
            "valid_to": r["valid_to"],
            "data_type": r["data_type"],
            "data_length": r["data_length"],
            "delivery_column_name": r["delivery_column_name"],
            "value_set_id": r["value_set_id"],
            "value_set_version_label": r["value_set_version_label"],
            # Filled in below from variable_instance joins.
            "kolumnnamn_set": set(),
            "vardemangdsniva_set": set(),
            "classification_id_set": set(),
            "cvid_set": set(),
        }

    # Walk variable_instance × variable_alias to populate the discriminator
    # sets per state. The join key matches what _coalesce_variable_states
    # used (minus grain — grain is the discriminator we're now exposing
    # via vardemangdsniva_set).
    #
    # We also accumulate per-cvid alias sets (not just the merged
    # per-state union) so the kolumnnamn-component graph builds edges
    # only between aliases that co-occur for the SAME cvid. Aliases
    # from different cvids that happen to coalesce into one state row
    # (because the coalescer's grain key doesn't include kolumnnamn)
    # must NOT be joined — that's the genuine kolumnnamn-split signal.
    inst_rows = cur.execute(
        "SELECT vi.cvid, vi.register_id, vi.regvar_id, vi.var_id, "
        "       vi.data_type, vi.data_length, vi.value_set_id, "
        "       vi.value_set_version_label, vi.vardemangdsniva, "
        "       vi.classification_id, va.delivery_column_name "
        "FROM variable_instance vi "
        "LEFT JOIN variable_alias va ON va.cvid = vi.cvid"
    ).fetchall()
    # Per-cvid alias accumulation; later used to seed the kolumnnamn
    # graph's edges. Multiple aliases from the same cvid form one
    # connected component; aliases from different cvids stay separate
    # unless an explicit overlap exists.
    cvid_aliases: dict[int, set[str]] = {}
    sid_to_cvids: dict[int, set[int]] = {}
    for ir in inst_rows:
        identity = (
            ir["register_id"],
            ir["regvar_id"],
            ir["var_id"],
            ir["data_type"] or "",
            ir["data_length"] or "",
            ir["value_set_id"],
            ir["value_set_version_label"] or "",
        )
        # NOTE: a single instance identity can map to multiple state_ids
        # only when the coalescer's grain key (vardemangdsniva) differs
        # across the cvids. In that case, the alias / niva fan-out below
        # adds the same alias to multiple state rows — which is fine for
        # the kolumnnamn-intersection grouping. The triage then splits
        # those states out into siblings using vardemangdsniva.
        state_ids = state_by_identity.get(identity, [])
        if ir["delivery_column_name"]:
            cvid_aliases.setdefault(ir["cvid"], set()).add(ir["delivery_column_name"])
        for sid in state_ids:
            meta = state_meta[sid]
            if ir["delivery_column_name"]:
                meta["kolumnnamn_set"].add(ir["delivery_column_name"])
            if ir["vardemangdsniva"]:
                meta["vardemangdsniva_set"].add(ir["vardemangdsniva"])
            if ir["classification_id"] is not None:
                meta["classification_id_set"].add(ir["classification_id"])
            meta["cvid_set"].add(ir["cvid"])
            sid_to_cvids.setdefault(sid, set()).add(ir["cvid"])
    # Attach the per-state cvid → alias mapping so
    # `_kolumnnamn_components` can build edges per cvid (not per state).
    for sid, cvids in sid_to_cvids.items():
        state_meta[sid]["cvid_alias_map"] = {
            c: cvid_aliases.get(c, set()) for c in cvids
        }

    # Bucket by (register_id, regvar_id, var_id, year) — §5.7 collision
    # buckets. A state row contributes to every year it covers; the
    # bucket cardinality is what triggers the algorithm.
    #
    # A bucket is collision-prone when EITHER (a) it carries multiple
    # state rows, OR (b) a single state row's aliases form disjoint
    # kolumnnamn groups (the kolumnnamn-multi-alias case, which the
    # coalescer collapses into one row because its grain key doesn't
    # include kolumnnamn). The kolumnnamn-disjoint case is detected
    # post-bucketing by `_kolumnnamn_intersection_groups`.
    buckets: dict[tuple[int, int, int, int], list[int]] = {}
    for sid, meta in state_meta.items():
        for year in _triage_year_bucket(meta["valid_from"], meta["valid_to"]):
            bkey = (meta["register_id"], meta["regvar_id"], meta["var_id"], year)
            buckets.setdefault(bkey, []).append(sid)

    # Stats counters wired into the manifest.
    stats: dict[str, int] = {
        "n_collision_buckets": 0,
        "n_buckets_resolved_by_kolumnnamn": 0,
        "n_buckets_resolved_by_niva": 0,
        "n_buckets_resolved_by_datalangd": 0,
        "n_buckets_resolved_by_hash_fallback": 0,
        "n_siblings_created": 0,
        "n_related_to_edges": 0,
        "n_stubs_dropped": 0,
        "n_collapsed_data_type_drift": 0,
        "n_kept_overlapping_multi_vintage": 0,
        "n_kept_overlapping_multi_classification": 0,
        "n_collapsed_value_set_drift": 0,
        "n_duplicate_slugs_resolved": 0,
    }

    # Tracking sets so the per-bucket pass doesn't double-count when a
    # variable triple's states appear in multiple year buckets (e.g. a
    # bucket is collision-prone in 2020 AND 2021).
    dropped_state_ids: set[int] = set()
    collapsed_state_ids: set[int] = set()
    # Per-triple (register_id, regvar_id, var_id) → list of sibling
    # decisions. A decision records which states form a sibling group
    # and the discriminator type used. Decisions are computed per
    # bucket but reconciled per triple — the same kolumnnamn split
    # should fire once per variable across all its year buckets.
    triple_sibling_decisions: dict[
        tuple[int, int, int],
        dict[str, Any],
    ] = {}
    # Track per-triple bucket-resolution kind so the stats counter
    # increments once per triple, not once per (triple, year) bucket.
    triple_resolution_kind: dict[tuple[int, int, int], str] = {}

    for bkey, sids in sorted(buckets.items()):
        # Skip dropped states from earlier bucket iterations.
        live_sids = [
            s
            for s in sids
            if s not in dropped_state_ids and s not in collapsed_state_ids
        ]
        if not live_sids:
            continue
        triple = bkey[:3]

        # ----- Rule 2 (precondition): kolumnnamn-component split -----
        # First check whether the bucket has a kolumnnamn split — this
        # fires even on a single-state bucket if that state's aliases
        # form disjoint connected components. The coalescer's group key
        # doesn't include kolumnnamn, so two cvids with the same
        # (data_type, data_length, value_set_id, value_set_version_label,
        # grain) but different aliases collapse into ONE state row with
        # multiple aliases. The triage detects and unwinds that here.
        kolumnnamn_components = _kolumnnamn_components(live_sids, state_meta)
        has_kolumnnamn_split = len(kolumnnamn_components) > 1

        # A "collision" exists when (a) the bucket has >1 live state OR
        # (b) a single state's aliases form a disjoint kolumnnamn graph.
        if len(live_sids) <= 1 and not has_kolumnnamn_split:
            continue
        stats["n_collision_buckets"] += 1

        # ----- Rule 1: drop truly-empty stubs -----
        stub_sids = []
        non_stub_sids = []
        for sid in live_sids:
            meta = state_meta[sid]
            is_stub = not meta["data_type"] and not meta["kolumnnamn_set"]
            if is_stub:
                stub_sids.append(sid)
            else:
                non_stub_sids.append(sid)
        # Only drop stubs when at least one non-stub survives. An
        # all-stub bucket can't be resolved further; leave it alone
        # (rare edge case in practice).
        if stub_sids and non_stub_sids:
            for sid in stub_sids:
                dropped_state_ids.add(sid)
                stats["n_stubs_dropped"] += 1
            live_sids = non_stub_sids
            # Recompute the kolumnnamn split after dropping stubs.
            kolumnnamn_components = _kolumnnamn_components(live_sids, state_meta)
            has_kolumnnamn_split = len(kolumnnamn_components) > 1

        if len(live_sids) <= 1 and not has_kolumnnamn_split:
            continue

        # ----- Rule 2: kolumnnamn-component sibling split -----
        if has_kolumnnamn_split:
            # Each connected component becomes a sibling. Build the
            # state→component assignment: a state's component is the
            # smallest-state-id component its aliases intersect with.
            # (When a state's aliases span multiple components, that
            # state itself must be split into N — handled by carving
            # an alias-partition view per component.)
            if triple not in triple_sibling_decisions:
                groups = _assign_states_to_components(
                    live_sids, kolumnnamn_components, state_meta
                )
                triple_sibling_decisions[triple] = {
                    "kind": "kolumnnamn",
                    "groups": groups,
                    "components": kolumnnamn_components,
                    "relation_kind": "same_definition_different_column",
                }
                triple_resolution_kind[triple] = "kolumnnamn"
            continue

        # Single kolumnnamn group from here on → secondary rules.
        # ----- Rule 4: secondary discriminators -----

        # Rule 4a: empty vardemangdsniva alongside populated → drop empty.
        empty_niva = [
            sid for sid in live_sids if not state_meta[sid]["vardemangdsniva_set"]
        ]
        populated_niva = [
            sid for sid in live_sids if state_meta[sid]["vardemangdsniva_set"]
        ]
        if empty_niva and populated_niva:
            for sid in empty_niva:
                dropped_state_ids.add(sid)
                stats["n_stubs_dropped"] += 1
            live_sids = populated_niva
            if len(live_sids) <= 1:
                continue

        # Rule 4b: vardemangdsniva differs → split via niva-pattern.
        niva_groups = _vardemangdsniva_groups(live_sids, state_meta)
        if len(niva_groups) > 1:
            if triple not in triple_sibling_decisions:
                triple_sibling_decisions[triple] = {
                    "kind": "niva",
                    "groups": niva_groups,
                    "relation_kind": "same_definition_different_grain",
                }
                triple_resolution_kind[triple] = "niva"
            continue

        # Rule 4c: value_set_version_label differs → keep overlapping
        # (true multi-vintage; no edge).
        vsvl_set = {
            state_meta[sid]["value_set_version_label"] or "" for sid in live_sids
        }
        if len(vsvl_set) > 1:
            # No-op: states stay distinct, no sibling allocation.
            if triple_resolution_kind.get(triple) != "multi_vintage":
                stats["n_kept_overlapping_multi_vintage"] += 1
                triple_resolution_kind[triple] = "multi_vintage"
            continue

        # Rule 4d: classification_id differs → keep overlapping, emit
        # `same_concept_different_grain` edges between the surviving
        # state rows' variable triples (which are the same triple here,
        # so the edge is a self-relation on the same variable id).
        cls_sets = [
            frozenset(state_meta[sid]["classification_id_set"]) for sid in live_sids
        ]
        if len({s for s in cls_sets if s}) > 1:
            # Multiple distinct classification sets among states. Keep
            # overlapping; no sibling allocation. We don't emit an edge
            # here because both states belong to the same variable_slug
            # (no second sibling to point at). The §5.7 spec mentions
            # the edge for cross-grain classification *splits*, which
            # the kolumnnamn path already covers above.
            if triple_resolution_kind.get(triple) != "multi_classification":
                stats["n_kept_overlapping_multi_classification"] += 1
                triple_resolution_kind[triple] = "multi_classification"
            continue

        # Rule 4e: datalangd code/label pair takes precedence over the
        # generic data_length-collapse rule. The pair pattern (e.g.
        # `Lid` length 4 + `LNamn` length 20 — short id alongside long
        # label) is a deliberate co-encoded sibling, not metadata drift.
        if _looks_like_code_label_pair(live_sids, state_meta):
            length_groups = _datalangd_pair_groups(live_sids, state_meta)
            if len(length_groups) > 1 and triple not in triple_sibling_decisions:
                triple_sibling_decisions[triple] = {
                    "kind": "datalangd",
                    "groups": length_groups,
                    "relation_kind": "code_vs_label_pair",
                }
                triple_resolution_kind[triple] = "datalangd"
            continue

        # Rule 4f: data_type / data_length differs only → collapse.
        dt_set = {state_meta[sid]["data_type"] or "" for sid in live_sids}
        dl_set = {state_meta[sid]["data_length"] or "" for sid in live_sids}
        if len(dt_set) > 1 or len(dl_set) > 1:
            _collapse_states(conn, live_sids, state_meta, collapsed_state_ids)
            stats["n_collapsed_data_type_drift"] += 1
            continue

        # Rule 4g: only value_set_id differs → collapse (code-list drift).
        vs_set = {state_meta[sid]["value_set_id"] for sid in live_sids}
        if len(vs_set) > 1:
            _collapse_states(conn, live_sids, state_meta, collapsed_state_ids)
            stats["n_collapsed_value_set_drift"] += 1
            continue

        # No rule fired. The bucket stays multi-state. Counted under
        # `n_collision_buckets` but not under any resolution counter;
        # surfaces in the manifest as a curation target.

    # Apply drops first so downstream sibling allocation doesn't see
    # them. delete_state_ids carries both stub drops and rule-4a drops;
    # apply both with a single DELETE.
    if dropped_state_ids:
        conn.executemany(
            "DELETE FROM variable_state WHERE state_id = ?",
            [(sid,) for sid in sorted(dropped_state_ids)],
        )

    # Allocate siblings per triple decision. Decision records sibling
    # groups in deterministic order; sibling 1 keeps the original
    # var_id, siblings 2..N get freshly-minted var_ids.
    _allocate_sibling_variables(
        conn,
        triple_sibling_decisions,
        triple_resolution_kind,
        state_meta,
        toml_variable_slugs,
        stats,
    )

    _progress(
        f"  {stats['n_collision_buckets']:,} collision buckets, "
        f"{stats['n_siblings_created']:,} siblings created, "
        f"{stats['n_related_to_edges']:,} variable_related_to edges, "
        f"{stats['n_stubs_dropped']:,} stubs dropped"
    )
    return stats


def _kolumnnamn_components(
    sids: list[int], state_meta: dict[int, dict[str, Any]]
) -> list[frozenset[str]]:
    """Connected components of the bucket's kolumnnamn graph.

    Nodes are kolumnnamn strings across all states in the bucket; an
    edge connects two columns whenever they co-occur for the **same
    cvid** (the per-cvid alias map preserved on `state_meta[sid]
    ['cvid_alias_map']`). Same-cvid co-occurrence is rare cross-edition
    drift where one CVID carries multiple aliases — those genuinely
    name the same column. Aliases from DIFFERENT cvids stay in
    separate components even when those cvids coalesce into one state,
    because that's precisely the kolumnnamn-split signal §5.7 is built
    around.

    The §5.7 kolumnnamn-split fires when this graph has >1 component:
    the bucket carries data for genuinely different physical columns
    that the coalescer collapsed into one or more state rows.

    States with an empty kolumnnamn_set contribute no nodes. Their
    sibling assignment is handled by `_assign_states_to_components`.

    Returns components in deterministic order (sorted by the
    lexically-smallest member of each).
    """
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            # Lexically-smaller root wins for determinism across runs.
            if ra < rb:
                parent[rb] = ra
            else:
                parent[ra] = rb

    # Seed nodes from every state's kolumnnamn (so singleton-cvid
    # aliases still anchor their component) but seed edges from the
    # per-cvid alias map only.
    for sid in sids:
        for k in state_meta[sid]["kolumnnamn_set"]:
            parent.setdefault(k, k)
        cvid_aliases = state_meta[sid].get("cvid_alias_map", {})
        for aliases in cvid_aliases.values():
            aliases_sorted = sorted(aliases)
            for a, b in zip(aliases_sorted, aliases_sorted[1:], strict=False):
                parent.setdefault(a, a)
                parent.setdefault(b, b)
                union(a, b)

    components_map: dict[str, set[str]] = {}
    for k in parent:
        root = find(k)
        components_map.setdefault(root, set()).add(k)
    return sorted(
        (frozenset(c) for c in components_map.values()),
        key=lambda c: min(c),
    )


def _assign_states_to_components(
    sids: list[int],
    components: list[frozenset[str]],
    state_meta: dict[int, dict[str, Any]],
) -> list[list[int]]:
    """Group state ids by which kolumnnamn component each belongs to.

    Per-cvid aliasing means a state can carry kolumnnamn from multiple
    components (the coalesced-from-different-cvids case the triage
    exists to detect). When that happens we assign the state to the
    component holding the largest share of its aliases — the
    `_materialize_empty_component_clones` step then backfills the
    other components with clone state rows so each component still
    has at least one row.

    Empty-kolumnnamn states are appended to the first component as a
    safe default — they have no signal to distinguish them and the
    caller has already determined a split is warranted.

    Returns one list of state ids per component, in component order.
    """
    groups: list[list[int]] = [[] for _ in components]
    component_idx: dict[str, int] = {}
    for i, comp in enumerate(components):
        for k in comp:
            component_idx[k] = i
    for sid in sids:
        kset = state_meta[sid]["kolumnnamn_set"]
        if not kset:
            # Empty-alias state — append to the first component.
            groups[0].append(sid)
            continue
        # Count component hits across this state's aliases. Pick the
        # component with the most hits; ties broken by lowest index
        # for determinism.
        hits: dict[int, int] = {}
        for k in kset:
            idx = component_idx[k]
            hits[idx] = hits.get(idx, 0) + 1
        chosen_idx = min(hits, key=lambda i: (-hits[i], i))
        groups[chosen_idx].append(sid)
    # Sort each group for stable downstream iteration; preserve
    # component-order in the outer list.
    return [sorted(g) for g in groups]


def _vardemangdsniva_groups(
    sids: list[int], state_meta: dict[int, dict[str, Any]]
) -> list[list[int]]:
    """Group state ids by their `vardemangdsniva` set (frozenset key).

    Two states with identical niva sets go to one group; distinct sets
    are distinct groups regardless of overlap. We don't apply the same
    intersection logic as kolumnnamn here because niva is the grain
    discriminator — by spec each grain is its own sibling, even when
    two grains happen to share a token (`SSYK 3 positioner` vs
    `SSYK 5 positioner` share `SSYK` but should split into 3pos / 5pos).
    """
    groups_map: dict[frozenset[str], list[int]] = {}
    for sid in sids:
        key = frozenset(state_meta[sid]["vardemangdsniva_set"])
        groups_map.setdefault(key, []).append(sid)
    return sorted(
        (sorted(g) for g in groups_map.values()),
        key=lambda g: g[0],
    )


def _looks_like_code_label_pair(
    sids: list[int], state_meta: dict[int, dict[str, Any]]
) -> bool:
    """Heuristic: two states with distinct data_length (one short ≤4, one
    long ≥10) sharing the same kolumnnamn group are a code/label pair.

    Only fires for exactly two states; longer chains don't fit the pattern.
    """
    if len(sids) != 2:
        return False
    lengths = []
    for sid in sids:
        dl = state_meta[sid]["data_length"]
        try:
            lengths.append(int(dl)) if dl is not None else lengths.append(None)
        except (ValueError, TypeError):
            return False
    if None in lengths:
        return False
    short, long_ = sorted(lengths)
    return short <= 4 and long_ >= 10


def _datalangd_pair_groups(
    sids: list[int], state_meta: dict[int, dict[str, Any]]
) -> list[list[int]]:
    """Bucket sids into [code_state], [label_state] by data_length.

    Caller has already validated via `_looks_like_code_label_pair` that
    we have exactly two states with distinct short/long lengths.
    Determinism: shorter-length state lands in the first group.
    """
    sorted_sids = sorted(
        sids,
        key=lambda s: (int(state_meta[s]["data_length"]), s),
    )
    return [[sorted_sids[0]], [sorted_sids[1]]]


def _collapse_states(
    conn: sqlite3.Connection,
    sids: list[int],
    state_meta: dict[int, dict[str, Any]],
    collapsed_state_ids: set[int],
) -> None:
    """Collapse a set of overlapping state rows into one row.

    Keeps the row with the largest `state_id` (most-recent autoinc) and
    drops the rest. Updates the survivor's `valid_from` / `valid_to` to
    the union range. Metadata (data_type, data_length, value_set_id,
    value_set_version_label) is preserved from the survivor — the §5.7
    rule says "pick the latest values" and largest state_id is our
    proxy for latest (since autoincrement reflects insertion order
    after the coalescer's grouped emit).

    Marks the dropped sids in `collapsed_state_ids` so downstream
    bucket iterations skip them.
    """
    if len(sids) <= 1:
        return
    survivor = max(sids)
    losers = [s for s in sids if s != survivor]
    # Union range across all sids.
    union_from = min(state_meta[s]["valid_from"] for s in sids)
    union_to = max(state_meta[s]["valid_to"] for s in sids)
    conn.execute(
        "UPDATE variable_state SET valid_from = ?, valid_to = ? WHERE state_id = ?",
        (union_from, union_to, survivor),
    )
    state_meta[survivor]["valid_from"] = union_from
    state_meta[survivor]["valid_to"] = union_to
    for loser in losers:
        conn.execute("DELETE FROM variable_state WHERE state_id = ?", (loser,))
        collapsed_state_ids.add(loser)


def _allocate_sibling_variables(
    conn: sqlite3.Connection,
    decisions: dict[tuple[int, int, int], dict[str, Any]],
    resolution_kinds: dict[tuple[int, int, int], str],
    state_meta: dict[int, dict[str, Any]],
    toml_variable_slugs: dict[tuple[str, str], str],
    stats: dict[str, int],
) -> None:
    """Materialize sibling variable rows + relink variable_state rows.

    For each triple decision:
      1. Allocate fresh var_ids for siblings 2..N. Sibling 1 keeps the
         original var_id (so deep-linked references survive).
      2. Insert a new `variable` row per fresh sibling, copying fields
         from the original variable.
      3. Update the `variable_state` rows in groups 2..N to reference
         the new var_id. (Sibling 1's state rows stay put.)
      4. Emit (N choose 2) symmetric `variable_related_to` edges with
         `note = 'auto:triage'` and the decision's relation_kind. The
         emitted edges use the variable slugs (auto-derived per §5.7)
         so the edges survive across rebuilds even if var_ids shift.

    The `variable.name` text on the new siblings copies from the
    original. Sibling-specific text (e.g. "<original> (kommun-hem)") is
    a curator concern — the build doesn't try to invent semantic
    differences from the column name alone.
    """
    if not decisions:
        return

    # Look up provider slugs + register slug per triple once.
    # variable.register_id × register.slug × register.provider_id ×
    # provider.slug — needed to write `variable_related_to` rows.
    # `build_db`'s connection uses the default tuple row_factory; use a
    # local Row-cursor so the column-name access stays readable without
    # touching the caller's row_factory setting.
    triples = sorted(decisions.keys())
    register_ids = {t[0] for t in triples}
    register_meta: dict[int, tuple[str | None, str | None]] = {}
    if register_ids:
        reg_cur = conn.cursor()
        reg_cur.row_factory = sqlite3.Row
        for r in reg_cur.execute(
            "SELECT r.register_id, r.slug AS register_slug, p.slug AS provider_slug "
            "FROM register r JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE r.register_id IN (" + ",".join("?" * len(register_ids)) + ")",
            tuple(register_ids),
        ).fetchall():
            register_meta[r["register_id"]] = (
                r["provider_slug"],
                r["register_slug"],
            )

    # Resolve current max var_id once. New siblings allocate from
    # `next_var_id`. Per-triple var_id space is shared across the DB
    # (variable PK is (register_id, var_id)) — but stays unique
    # globally because we add per row.
    row = conn.execute("SELECT COALESCE(MAX(var_id), 0) FROM variable").fetchone()
    next_var_id = (row[0] or 0) + 1

    # Collect rows to insert / update.
    new_variable_rows: list[tuple] = []
    state_relink: list[tuple[int, int]] = []  # (new_var_id, state_id)
    edges_to_emit: list[tuple] = []

    for triple in triples:
        register_id, regvar_id, original_var_id = triple
        decision = decisions[triple]
        groups = decision["groups"]  # list[list[state_id]]
        relation_kind = decision["relation_kind"]
        kind = decision["kind"]

        # Resolve the original variable's metadata once so each sibling
        # copies the same provenance.
        orig = conn.execute(
            "SELECT name, definition, description, source_register_text, "
            "       measurement_unit, source_register_id, source_label, "
            "       is_sensitive, is_identifier "
            "FROM variable WHERE register_id = ? AND var_id = ?",
            (register_id, original_var_id),
        ).fetchone()
        if orig is None:
            # Should not happen — every variable_state row points to a
            # real variable. Skip silently rather than crashing the build.
            continue
        orig_name = orig[0]
        orig_def = orig[1]
        orig_desc = orig[2]
        orig_src_text = orig[3]
        orig_unit = orig[4]
        orig_src_reg = orig[5]
        orig_src_label = orig[6]
        orig_sensitive = orig[7]
        orig_identifier = orig[8]

        # Derive sibling slugs per kind. For kolumnnamn splits we use
        # the precomputed components (the actual disjoint kolumnnamn
        # graphs) rather than re-walking state aliases — this preserves
        # the per-component ordering and lets `_kolumnnamn_suffixes`
        # operate on a clean, deterministic representative set.
        if kind == "kolumnnamn":
            components = decision["components"]
            sibling_slugs = _derive_sibling_slugs_kolumnnamn(
                components, groups, state_meta, stats
            )
            # Kolumnnamn-component split: when a state was assigned to
            # one component but the bucket has aliases for another
            # component on that same state (the single-coalesced-state-
            # with-disjoint-aliases case), we need to materialize a
            # CLONE of that state row for each empty group so each
            # sibling carries at least one state. Builds `groups` in
            # place with cloned state ids appended for empty groups.
            groups = _materialize_empty_component_clones(
                conn, groups, components, state_meta
            )
            decision["groups"] = groups
        else:
            sibling_slugs = _derive_sibling_slugs(
                kind, groups, state_meta, original_var_id, stats
            )

        # Dedupe slugs deterministically: if two siblings derive the
        # same slug, append `-a` / `-b` / ... in group-order (first
        # group keeps the bare slug). cvid order is implicit: groups
        # are already sorted by their lowest state_id, which derives
        # from coalescer's insertion order over cvids.
        sibling_slugs = _dedupe_sibling_slugs(sibling_slugs, stats)

        # TOML override path: if the original variable id has an
        # explicit `slug` in the provider TOML, the SIBLING 1 inherits
        # that — the original variable gets the curated slug, fresh
        # siblings keep their auto-derived suffixes. Reason: pre-A2.2
        # the TOML curator could only know about the canonical variable
        # id; allocating fresh ids for siblings 2..N happens at build
        # time, so siblings 2..N can't have TOML overrides yet.
        provider_slug, register_slug = register_meta.get(register_id, (None, None))
        if provider_slug is not None:
            # Source ID for variable is `<register_id>.<var_id>`.
            toml_key = (provider_slug, f"{register_id}.{original_var_id}")
            toml_slug = toml_variable_slugs.get(toml_key)
            if toml_slug is not None:
                sibling_slugs[0] = toml_slug

        # Sibling 0 keeps original var_id. Siblings 1..N-1 get fresh ids.
        sibling_var_ids: list[int] = [original_var_id]
        for _ in groups[1:]:
            sibling_var_ids.append(next_var_id)
            next_var_id += 1

        # Stage new variable rows and state relinks.
        # For kolumnnamn splits we also update each state's
        # `delivery_column_name` to a representative from its
        # component (the lex-smallest alias). This corrects the
        # coalescer's arbitrary pick — sibling Kommun-hem's state
        # row should advertise `Hemkommun`, not whichever alias
        # the coalescer happened to grab.
        components = decision.get("components") if kind == "kolumnnamn" else None
        for i, (group_sids, new_var_id) in enumerate(
            zip(groups, sibling_var_ids, strict=True)
        ):
            component_rep = (
                sorted(components[i])[0]
                if components is not None and components[i]
                else None
            )
            if i != 0:
                new_variable_rows.append(
                    (
                        register_id,
                        new_var_id,
                        orig_name,
                        orig_def,
                        orig_desc,
                        orig_src_text,
                        orig_unit,
                        orig_src_reg,
                        orig_src_label,
                        orig_sensitive,
                        orig_identifier,
                    )
                )
            for sid in group_sids:
                # Always relink (sibling 0 stays on the original var_id;
                # cloned states from `_materialize_empty_component_clones`
                # carry the original var_id at insert time, so relinking
                # to original_var_id is a no-op there).
                state_relink.append((new_var_id, sid))
                state_meta[sid]["var_id"] = new_var_id
                if component_rep is not None:
                    conn.execute(
                        "UPDATE variable_state SET delivery_column_name = ? "
                        "WHERE state_id = ?",
                        (component_rep, sid),
                    )
                    state_meta[sid]["delivery_column_name"] = component_rep

        stats["n_siblings_created"] += len(sibling_var_ids) - 1

        # Count the bucket-resolution toward the right stat.
        rk = resolution_kinds.get(triple)
        if rk == "kolumnnamn":
            stats["n_buckets_resolved_by_kolumnnamn"] += 1
        elif rk == "niva":
            stats["n_buckets_resolved_by_niva"] += 1
        elif rk == "datalangd":
            stats["n_buckets_resolved_by_datalangd"] += 1

        # Emit (N choose 2) symmetric edges, slug-anchored. variant slug
        # is left blank — siblings are anchored at variable-level, and
        # the §5.5 grammar allows the variant slot to be `''` (empty)
        # for "applies across all variants of this register".
        if provider_slug is None or register_slug is None:
            # Skip edge emission when the register has no slug — this
            # is an honest signal that populate_slugs hasn't run, and
            # we don't want to emit untrackable edges. Sibling
            # allocation still happens.
            continue
        for i in range(len(sibling_slugs)):
            for j in range(i + 1, len(sibling_slugs)):
                a_slug, b_slug = sibling_slugs[i], sibling_slugs[j]
                if a_slug == b_slug:
                    # Defensive: dedupe pass should have prevented this.
                    continue
                edges_to_emit.append(
                    (
                        provider_slug,
                        register_slug,
                        "",
                        a_slug,
                        provider_slug,
                        register_slug,
                        "",
                        b_slug,
                        relation_kind,
                        "auto:triage",
                    )
                )
                edges_to_emit.append(
                    (
                        provider_slug,
                        register_slug,
                        "",
                        b_slug,
                        provider_slug,
                        register_slug,
                        "",
                        a_slug,
                        relation_kind,
                        "auto:triage",
                    )
                )
                stats["n_related_to_edges"] += 2

    if new_variable_rows:
        conn.executemany(
            "INSERT INTO variable (register_id, var_id, name, definition, "
            "description, source_register_text, measurement_unit, "
            "source_register_id, source_label, is_sensitive, is_identifier) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            new_variable_rows,
        )
    if state_relink:
        conn.executemany(
            "UPDATE variable_state SET var_id = ? WHERE state_id = ?",
            state_relink,
        )
    if edges_to_emit:
        # INSERT OR IGNORE: triple decisions are deduped per triple but
        # a downstream curation pass might already insert the same edge.
        conn.executemany(
            "INSERT OR IGNORE INTO variable_related_to ("
            "a_provider, a_register, a_variant, a_variable, "
            "b_provider, b_register, b_variant, b_variable, "
            "relation_kind, note) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            edges_to_emit,
        )


def _materialize_empty_component_clones(
    conn: sqlite3.Connection,
    groups: list[list[int]],
    components: list[frozenset[str]],
    state_meta: dict[int, dict[str, Any]],
) -> list[list[int]]:
    """Backfill empty kolumnnamn-component groups with cloned state rows.

    When the coalescer collapses two cvids with disjoint kolumnnamn into
    one state row (because the alias isn't part of its grain key), the
    triage's `_assign_states_to_components` assigns that single state to
    the first component its alias lands in — leaving the other component
    with an empty group. The §5.7 split semantics require each component
    to have at least one state row, so we clone the source state into
    every empty group.

    The clone copies all metadata from the source state (the largest-
    state-id row in the first non-empty group); the caller's downstream
    relink will reassign the clone's `var_id` to the new sibling and
    update its `delivery_column_name` to a member of its component.

    Returns the groups list with empty entries replaced by `[clone_sid]`.
    """
    # Pick a source state — first non-empty group's largest state id.
    source_sid = None
    for g in groups:
        if g:
            source_sid = max(g)
            break
    if source_sid is None:
        # Should not happen — by the time we get here at least one
        # group has a state. Return unchanged as defensive fallback.
        return groups

    src_meta = state_meta[source_sid]
    new_groups: list[list[int]] = []
    for g, comp in zip(groups, components, strict=True):
        if g:
            new_groups.append(g)
            continue
        # Insert a fresh state row that mirrors the source. AUTOINCREMENT
        # gives us a new state_id we can capture from lastrowid.
        cur = conn.execute(
            "INSERT INTO variable_state ("
            "register_id, regvar_id, var_id, valid_from, valid_to, "
            "data_type, data_length, delivery_column_name, "
            "value_set_id, value_set_version_label"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                src_meta["register_id"],
                src_meta["regvar_id"],
                src_meta["var_id"],
                src_meta["valid_from"],
                src_meta["valid_to"],
                src_meta["data_type"],
                src_meta["data_length"],
                sorted(comp)[0] if comp else src_meta["delivery_column_name"],
                src_meta["value_set_id"],
                src_meta["value_set_version_label"],
            ),
        )
        new_sid = cur.lastrowid
        assert new_sid is not None
        # Mirror the source state's meta with a component-correct alias
        # set so downstream slug derivation finds the right component.
        state_meta[new_sid] = {
            **src_meta,
            "kolumnnamn_set": set(comp),
            "vardemangdsniva_set": set(src_meta["vardemangdsniva_set"]),
            "classification_id_set": set(src_meta["classification_id_set"]),
            "cvid_set": set(src_meta["cvid_set"]),
            "delivery_column_name": sorted(comp)[0]
            if comp
            else src_meta["delivery_column_name"],
        }
        new_groups.append([new_sid])
    return new_groups


def _derive_sibling_slugs_kolumnnamn(
    components: list[frozenset[str]],
    groups: list[list[int]],
    state_meta: dict[int, dict[str, Any]],
    stats: dict[str, int] | None = None,
) -> list[str]:
    """Sibling slug derivation when the decision kind is kolumnnamn.

    Operates on the precomputed kolumnnamn components rather than each
    group's merged alias set — components carry the genuine disjoint
    column graphs and produce stable representatives, while a group's
    merged alias set could miss components when an empty-alias state
    was bucket-assigned to component 0 by `_assign_states_to_components`.

    Falls back to the BLAKE2b hash when the kolumnnamn-derived slugs
    fail the §5.2 grammar (rare in SCB data; defensive). `stats` is the
    triage stats dict; when the hash fallback fires the per-bucket
    counter `n_buckets_resolved_by_hash_fallback` increments by 1.
    """
    col_sets = [set(c) for c in components]
    suffixes = _kolumnnamn_suffixes(col_sets)
    if suffixes is not None:
        return [s.lstrip("-") for s in suffixes]
    if stats is not None:
        stats["n_buckets_resolved_by_hash_fallback"] += 1
    return [_hash_fallback_slug(g, state_meta) for g in groups]


def _derive_sibling_slugs(
    kind: str,
    groups: list[list[int]],
    state_meta: dict[int, dict[str, Any]],
    original_var_id: int,
    stats: dict[str, int] | None = None,
) -> list[str]:
    """Apply the §5.7 4-level fallback to produce N parallel sibling slugs.

    `kind` is the decision kind (`kolumnnamn` / `niva` / `datalangd`);
    fallback to the BLAKE2b hash kicks in when the chosen kind can't
    produce a usable slug. Returns a list parallel to `groups`. When
    the hash fallback fires, `stats['n_buckets_resolved_by_hash_fallback']`
    increments — distinguishes "kolumnnamn-derived slug" from
    "fell-through to hash" in the manifest.
    """
    if kind == "kolumnnamn":
        # One representative kolumnnamn set per group.
        col_sets = [_merge_kolumnnamn_sets(g, state_meta) for g in groups]
        suffixes = _kolumnnamn_suffixes(col_sets)
        if suffixes is not None:
            return [s.lstrip("-") for s in suffixes]
    if kind == "niva":
        # One representative niva text per group (lexically smallest).
        niva_texts = [
            sorted(_merge_niva_sets(g, state_meta))[0]
            if _merge_niva_sets(g, state_meta)
            else ""
            for g in groups
        ]
        suffixes = [_niva_suffix(n) for n in niva_texts]
        if all(s is not None for s in suffixes):
            return [s.lstrip("-") for s in suffixes]  # type: ignore[union-attr]
    if kind == "datalangd" and len(groups) == 2:
        # Two-group code/label pair: short → `id`, long → `namn`.
        return ["id", "namn"]

    # Hash fallback. Compute a stable 6-hex suffix per group from a
    # canonical key derived from its smallest cvid (or state_id when
    # no cvid is available). BLAKE2b with digest_size=3 yields exactly
    # 6 hex chars per `_x000000`.
    if stats is not None:
        stats["n_buckets_resolved_by_hash_fallback"] += 1
    return [_hash_fallback_slug(g, state_meta) for g in groups]


def _merge_kolumnnamn_sets(
    sids: list[int], state_meta: dict[int, dict[str, Any]]
) -> set[str]:
    merged: set[str] = set()
    for sid in sids:
        merged |= state_meta[sid]["kolumnnamn_set"]
    return merged


def _merge_niva_sets(
    sids: list[int], state_meta: dict[int, dict[str, Any]]
) -> set[str]:
    merged: set[str] = set()
    for sid in sids:
        merged |= state_meta[sid]["vardemangdsniva_set"]
    return merged


def _hash_fallback_slug(sids: list[int], state_meta: dict[int, dict[str, Any]]) -> str:
    """6-hex BLAKE2b suffix derived from the group's smallest cvid+state_id."""
    smallest_sid = min(sids)
    cvid_set = state_meta[smallest_sid]["cvid_set"]
    smallest_cvid = min(cvid_set) if cvid_set else smallest_sid
    canonical_key = f"{smallest_sid}:{smallest_cvid}".encode()
    h = hashlib.blake2b(canonical_key, digest_size=3).hexdigest()
    return f"x{h}"


def _dedupe_sibling_slugs(slugs: list[str], stats: dict[str, int]) -> list[str]:
    """Append `-a`/`-b`/... to duplicate slugs in group order.

    The first occurrence of a slug keeps its bare form; later ones get
    `-a`, `-b`, ... suffixes. Counts each unique duplicate-resolved
    sibling once via `n_duplicate_slugs_resolved`. Emits a build warning
    to stderr per the §5.7 contract.
    """
    seen_counts: dict[str, int] = {}
    out: list[str] = []
    warned = False
    for slug in slugs:
        if slug in seen_counts:
            seen_counts[slug] += 1
            # 0-th 'a', 1-th 'b', ...
            tiebreak = chr(ord("a") + seen_counts[slug] - 1)
            out.append(f"{slug}-{tiebreak}")
            stats["n_duplicate_slugs_resolved"] += 1
            if not warned:
                _progress(
                    f"  WARN: triage produced duplicate sibling slugs "
                    f"({slug!r} repeated); appended tiebreaker suffixes."
                )
                warned = True
        else:
            seen_counts[slug] = 0
            out.append(slug)
    return out


def _load_toml_variable_slugs(slug_dir: Path) -> dict[tuple[str, str], str]:
    """Pre-load `[variable."<id>"]` explicit slugs from per-provider TOMLs.

    Returns `{ (provider, source_id): slug }` where source_id is the
    literal TOML key (e.g. `"34.137"`). Skips entries without an explicit
    slug — the auto-derived sibling slug from triage wins for those.

    Used by the override-light path: a TOML-curated slug pre-empts the
    auto-derived sibling slug for that variable id, but only at the
    canonical sibling slot (sibling 1, which keeps the original var_id).
    Bulk curation of siblings 2..N is deferred per §15.
    """
    from .fqid_slugs import load_provider_toml

    out: dict[tuple[str, str], str] = {}
    if not slug_dir.is_dir():
        return out
    for path in sorted(slug_dir.glob("*.toml")):
        if path.name == "classifications.toml":
            continue
        try:
            entries = load_provider_toml(path)
        except Exception:
            # Loading errors will surface again in populate_slugs / the
            # main TOML-load path; here we just need the slug overrides
            # we can read cleanly. Be permissive at the override-light
            # stage; A2.2 doesn't add new validation.
            continue
        provider = path.stem
        for entry in entries:
            if entry.kind == "variable" and entry.slug is not None:
                out[(provider, entry.source_id)] = entry.slug
    return out


def _import_identifierare(conn: sqlite3.Connection, path: Path) -> int:
    _progress("Importing Identifierare.csv...")
    row_count = 0
    batch: list[tuple[int | str, ...]] = []

    with _open_scb_csv(path) as (_, rows):
        for _, row in rows:
            row_count += 1
            batch.append(
                (int(row["VarID"]), row["Variabelnamn"], row["Variabeldefinition"])
            )

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


def _load_validity_map(path: Path) -> tuple[dict[int, list[tuple[int, int]]], int]:
    """Load VardemangderValidDates.csv into an in-memory ItemId → year-windows map.

    Returns (validity_map, row_count). validity_map[item_id] is a list of
    (year_from, year_to) tuples — one per validity row for that ItemId.
    NULL valid_from → 1, NULL valid_to → 9999 (per SCB rule §2.1: NULL means
    "no boundary"). Year overlap covers SCB's sub-year boundaries losslessly
    (a window starting 1995-09-01 still covers year 1995). Tracked = present
    in this map; an ItemId from Vardemangder.csv with no entry here is
    "untracked" and contributes no constraint.
    """
    _progress("Loading VardemangderValidDates.csv into memory...")
    validity_map: dict[int, list[tuple[int, int]]] = {}
    row_count = 0
    with _open_scb_csv(path) as (_, rows):
        for _, row in rows:
            row_count += 1
            item_id = int(row["ItemID"])
            vf = row["ValidFrom"]
            vt = row["ValidTo"]
            year_from = int(vf[:4]) if vf else 1
            year_to = int(vt[:4]) if vt else 9999
            validity_map.setdefault(item_id, []).append((year_from, year_to))
    _progress(
        f"  {row_count:,} validity rows over {len(validity_map):,} distinct ItemIDs"
    )
    return validity_map, row_count


def _import_vardemangder(
    conn: sqlite3.Connection,
    path: Path,
    known_cvids: set[int],
) -> tuple[int, dict[int, tuple[str, str]]]:
    """Import Vardemangder.csv: write value_code, stage (cvid, code_id, item_id)
    triples to ``staging._build_cvid_pair`` for the year-projection pass.

    The caller must have ATTACHed the staging DB and created the staging table
    before invoking this function. The actual minting of value_set /
    value_set_member rows happens later in ``_project_and_mint_value_sets``.

    Returns (row_count, cvid_value_set_info) where cvid_value_set_info maps
    cvid → (vardemangdsversion, vardemangdsniva). CVIDs whose only Vardemangder
    rows were sentinels or fully-empty get no entry here, so their
    variable_instance.vardemangds{version,niva} stay NULL.
    """
    _progress("Importing Vardemangder.csv (this may take a while)...")
    row_count = 0
    batch_size = 50_000

    # Build value_code lookup: (vardekod, vardebenamning) → code_id
    code_lookup: dict[tuple[str, str], int] = {}
    next_code_id = 0

    cvid_value_set_info: dict[int, tuple[str, str]] = {}

    # Stage triples (cvid, code_id, item_id) for projection. item_id=0 is the
    # sentinel for "Vardemangder.csv shipped this row with empty ItemId" (SCB's
    # actual ItemIds are positive integers, so 0 is unambiguous). The PK
    # (cvid, code_id, item_id) dedups identical triples and groups rows for
    # the per-cvid projection pass.
    stage_batch: list[tuple[int, int, int]] = []

    skipped_sentinel = 0
    skipped_empty = 0
    drift_samples: dict[str, int] = {}

    with _open_scb_csv(path) as (_, rows):
        for _, row in rows:
            row_count += 1
            if row_count % 5_000_000 == 0:
                _progress(f"  ...{row_count:,} rows read")

            cvid = int(row["CVID"])
            if cvid not in known_cvids:
                continue

            vardekod = row["Värdekod"]
            vardebenamning = row["Värdebenämning"]
            version = row["Värdemängdsversion"]
            niva = row["Värdemängdsnivå"]
            raw_item = row["ItemId"]

            # Drop SCB type-tag rows masquerading as value codes. Tight match
            # on the documented sentinel shape (kod==version==niva). Looser
            # variants fall through to the drift detector below.
            if vardekod == version == niva and vardekod in _VARDEMANGDER_SENTINELS:
                skipped_sentinel += 1
                continue

            # Drift detector: kod==version with kod in neither allowlist (or in
            # SENTINELS but with niva diverging from the tight skip rule). See
            # `_VARDEMANGDER_*` docstrings.
            if (
                vardekod
                and vardekod == version
                and vardekod not in _VARDEMANGDER_REAL_SHAPED
            ):
                drift_samples[vardekod] = drift_samples.get(vardekod, 0) + 1

            # Drop fully-empty rows (kod, label, item all empty).
            if not (vardekod or vardebenamning or raw_item):
                skipped_empty += 1
                continue

            code_key = (vardekod, vardebenamning)

            if code_key not in code_lookup:
                code_lookup[code_key] = next_code_id
                next_code_id += 1

            code_id = code_lookup[code_key]

            if cvid not in cvid_value_set_info:
                cvid_value_set_info[cvid] = (version, niva)

            item_id = int(raw_item) if raw_item else 0
            stage_batch.append((cvid, code_id, item_id))

            if len(stage_batch) >= batch_size:
                conn.executemany(
                    "INSERT OR IGNORE INTO staging._build_cvid_pair "
                    "(cvid, code_id, item_id) VALUES (?, ?, ?)",
                    stage_batch,
                )
                stage_batch.clear()

    if stage_batch:
        conn.executemany(
            "INSERT OR IGNORE INTO staging._build_cvid_pair "
            "(cvid, code_id, item_id) VALUES (?, ?, ?)",
            stage_batch,
        )

    _progress(f"  Writing {len(code_lookup):,} value codes...")
    conn.executemany(
        "INSERT INTO value_code (code_id, code, label) VALUES (?, ?, ?)",
        [(cid, k[0], k[1]) for k, cid in code_lookup.items()],
    )

    _progress(
        f"  {row_count:,} rows read, {len(code_lookup):,} unique codes, "
        f"{len(cvid_value_set_info):,} CVIDs with values"
    )
    if skipped_sentinel or skipped_empty:
        _progress(
            f"  Skipped {skipped_sentinel:,} SCB type-tag rows "
            f"({sorted(_VARDEMANGDER_SENTINELS)}) "
            f"and {skipped_empty:,} fully-empty rows."
        )
    if drift_samples:
        sample = ", ".join(
            f"{k!r} ({n} rows)"
            for k, n in sorted(drift_samples.items(), key=lambda x: -x[1])[:5]
        )
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="vardemangder_drift",
            error_class="configuration",
            message=(
                f"Vardemangder drift: {len(drift_samples)} sentinel-shape "
                f"vardekod value(s) (kod==version) require human review. "
                f"Sample: {sample}."
            ),
            remediation=(
                "Inspect the listed vardekod values in Vardemangder.csv. "
                "(a) New SCB type-tag placeholder → add to "
                "_VARDEMANGDER_SENTINELS in reg_meta_build/src/reg_meta_build/db.py. "
                "(b) New real single-code value set sharing the shape → "
                "add to _VARDEMANGDER_REAL_SHAPED. (c) Already in "
                "_VARDEMANGDER_SENTINELS but appeared with niva!=version "
                "→ SCB changed the sentinel shape; broaden the skip rule "
                "to match. Then rerun build-db."
            ),
        )
    return row_count, cvid_value_set_info


@dataclass
class _GroupState:
    """In-flight group while streaming staging triples grouped by (cvid, code_id).

    ``items`` accumulates item_ids for the current code; ``accepted`` accumulates
    code_ids that survived projection for the current cvid.
    """

    cvid: int | None = None
    code_id: int | None = None
    items: list[int] = field(default_factory=list)
    accepted: list[int] = field(default_factory=list)


@dataclass
class _ProjectionStats:
    cvids_with_set: int = 0
    cvids_empty_after_projection: int = 0
    n_value_sets: int = 0


def _accept_code(
    item_ids: list[int],
    cvid_year: int | None,
    validity_map: dict[int, list[tuple[int, int]]],
) -> bool:
    """Apply the projection rule for one (cvid, code_id) group.

    Single pass: short-circuits on the first covering window. Avoids
    materializing a windows list — this runs ~50M times during a real-data
    rebuild, so per-call allocations matter.
    """
    has_tracked = False
    for iid in item_ids:
        if iid == 0:
            continue
        w = validity_map.get(iid)
        if not w:
            continue
        has_tracked = True
        if cvid_year is None:
            return True
        for yf, yt in w:
            if yf <= cvid_year <= yt:
                return True
    return not has_tracked


def _project_and_mint_value_sets(
    conn: sqlite3.Connection,
    validity_map: dict[int, list[tuple[int, int]]],
) -> _ProjectionStats:
    """Project staging triples by cvid year, mint deduplicated value_sets,
    and update variable_instance.value_set_id.

    Reads ``staging._build_cvid_pair`` and a cvid → year map joined from
    variable_instance × register_version. For each (cvid, code_id):

    - Collect validity windows of all *tracked* item_ids for the pair (a
      tracked item_id has at least one entry in ``validity_map``; item_id=0
      and unknown nonzero ids are *untracked*).
    - If no tracked windows → include (always-valid).
    - Otherwise → include iff at least one window covers the cvid year
      (year overlap, sub-year boundaries absorbed losslessly).

    Yearless cvids (regver name has no plausible 4-digit year) include all
    union pairs. Mixed tracked+untracked pairs let the tracked window decide;
    untracked siblings do not relax the constraint.

    Sets are deduplicated by content-addressed sha256 over sorted
    (vardekod, vardebenamning) pairs.
    """
    _progress("Projecting validity windows and minting value_sets...")

    cvid_to_year: dict[int, int | None] = {
        cvid: extract_year(version_name or "")
        for cvid, version_name in conn.execute(
            "SELECT vi.cvid, rv.registerversionnamn "
            "FROM variable_instance vi "
            "JOIN register_version rv ON vi.regver_id = rv.regver_id"
        )
    }
    yearless = sum(1 for y in cvid_to_year.values() if y is None)
    _progress(
        f"  cvid → year map: {len(cvid_to_year):,} entries "
        f"({yearless:,} yearless, fall back to all-codes)"
    )

    code_pair: dict[int, tuple[str, str]] = {
        code_id: (kod, label)
        for code_id, kod, label in conn.execute(
            "SELECT code_id, code, label FROM value_code"
        )
    }

    cur = conn.execute(
        "SELECT cvid, code_id, item_id FROM staging._build_cvid_pair "
        "ORDER BY cvid, code_id, item_id"
    )

    state = _GroupState()
    stats = _ProjectionStats()
    set_id_by_hash: dict[bytes, int] = {}
    cvid_set_assignments: list[tuple[int, int]] = []
    member_batch: list[tuple[int, int]] = []
    member_batch_size = 50_000

    def _flush_members() -> None:
        if member_batch:
            conn.executemany(
                "INSERT INTO value_set_member (value_set_id, code_id) VALUES (?, ?)",
                member_batch,
            )
            member_batch.clear()

    def _finish_code() -> None:
        if state.code_id is None or state.cvid is None:
            return
        if _accept_code(state.items, cvid_to_year.get(state.cvid), validity_map):
            state.accepted.append(state.code_id)

    def _finish_cvid() -> None:
        if state.cvid is None:
            return
        if not state.accepted:
            stats.cvids_empty_after_projection += 1
            return
        pairs = [code_pair[c] for c in state.accepted]
        h = _value_set_hash(pairs)
        set_id = set_id_by_hash.get(h)
        if set_id is None:
            cur = conn.execute("INSERT INTO value_set (member_hash) VALUES (?)", (h,))
            assert cur.lastrowid is not None  # sqlite always populates after INSERT
            set_id = cur.lastrowid
            set_id_by_hash[h] = set_id
            member_batch.extend((set_id, cid) for cid in state.accepted)
            if len(member_batch) >= member_batch_size:
                _flush_members()
        cvid_set_assignments.append((set_id, state.cvid))
        stats.cvids_with_set += 1

    for cvid, code_id, item_id in cur:
        if cvid != state.cvid:
            _finish_code()
            _finish_cvid()
            state.cvid = cvid
            state.code_id = code_id
            state.items = [item_id]
            state.accepted = []
            continue
        if code_id != state.code_id:
            _finish_code()
            state.code_id = code_id
            state.items = [item_id]
            continue
        state.items.append(item_id)

    _finish_code()
    _finish_cvid()
    _flush_members()

    if cvid_set_assignments:
        conn.executemany(
            "UPDATE variable_instance SET value_set_id = ? WHERE cvid = ?",
            cvid_set_assignments,
        )

    stats.n_value_sets = len(set_id_by_hash)
    _progress(
        f"  {stats.n_value_sets:,} distinct value_sets minted, "
        f"{stats.cvids_with_set:,} cvids linked, "
        f"{stats.cvids_empty_after_projection:,} cvids empty after projection."
    )
    return stats


def _populate_fts(conn: sqlite3.Connection) -> None:
    """Populate FTS5 search indexes."""
    _progress("Building search indexes...")

    # register_fts: content-synced — rowid must match register.rowid
    # (register_id is INTEGER PRIMARY KEY, so rowid = register_id)
    conn.execute(
        "INSERT INTO register_fts(rowid, register_id, name, purpose) "
        "SELECT rowid, register_id, name, purpose FROM register"
    )

    # variable_fts: content-synced with variable table. Delivery column names
    # are excluded (they contain technical suffixes like _LISA that pollute
    # search results).
    conn.execute("""
        INSERT INTO variable_fts(rowid, register_id, var_id, name, definition, description)
        SELECT
            v.rowid,
            v.register_id,
            v.var_id,
            v.name,
            v.definition,
            v.description
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
    import openpyxl

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


def seed_providers(conn: sqlite3.Connection) -> None:
    """Insert the built-in `provider` rows.

    Must run before any `register` insert because `register.provider_id`
    REFERENCES `provider`. Idempotent across repeat calls (e.g. when a test
    fixture seeds before `build_db`): existing rows are verified to match
    `_PROVIDER_SEED`; a mismatched slug/name raises rather than silently
    leaving foreign data in place.
    """
    existing = {
        row[0]: (row[1], row[2])
        for row in conn.execute(
            "SELECT provider_id, slug, name FROM provider"
        ).fetchall()
    }
    to_insert: list[tuple[int, str, str]] = []
    for provider_id, slug, name in _PROVIDER_SEED:
        prev = existing.get(provider_id)
        if prev is None:
            to_insert.append((provider_id, slug, name))
        elif prev != (slug, name):
            raise RuntimeError(
                f"provider.{provider_id} already present with "
                f"slug/name {prev!r}, expected {(slug, name)!r}"
            )
    if to_insert:
        conn.executemany(
            "INSERT INTO provider (provider_id, slug, name) VALUES (?, ?, ?)",
            to_insert,
        )


def link_consumer_side_bindings(conn: sqlite3.Connection) -> int:
    """Materialize §5.6 consumer-side binding lineage edges.

    Sets `variable_instance.via_source_id` to the canonical source cvid for
    every instance whose underlying variable was sourced from a different
    register, keyed on (`register_version.slug`, variable slug). Returns
    the edge count.

    Slug-only. When a consumer's slug doesn't exactly match a source
    sibling's slug, no edge forms — that's the correct outcome: the
    consumer data hasn't disambiguated which source sibling it came from,
    so the linker shouldn't guess. Maintainers can curate matching slugs
    on both sides to force a precise edge.

    Runs after `populate_slugs` so `rver.slug` is non-NULL.
    """
    # One row per (cvid, alias) so cvids with multiple aliases that derive
    # to different variable slugs are visible under each. ORDER BY pins
    # tie-breaks: when two source-side instances key the same (rid,
    # version_slug, var_slug), the lowest cvid wins — but that case only
    # arises if two source siblings share a slug, which UNIQUE(regvar_id,
    # slug) on register_version forbids; effectively the setdefault is
    # never contested in a strict-built DB.
    rows = conn.execute(
        "SELECT vi.cvid, vi.register_id, v.source_register_id, "
        "rver.slug AS version_slug, va.delivery_column_name "
        "FROM variable_instance vi "
        "JOIN variable v ON vi.register_id = v.register_id AND vi.var_id = v.var_id "
        "JOIN register_version rver ON vi.regver_id = rver.regver_id "
        "LEFT JOIN variable_alias va ON vi.cvid = va.cvid "
        "ORDER BY vi.cvid, va.delivery_column_name"
    ).fetchall()

    by_key: dict[tuple[int, str, str], int] = {}
    consumer_attempts: list[tuple[int, int, str, str]] = []

    for cvid, rid, src_rid, version_slug, delivery_column_name in rows:
        variable_slug = derive_variable_slug(delivery_column_name)
        if version_slug is None or variable_slug is None:
            continue
        by_key.setdefault((rid, version_slug, variable_slug), cvid)
        if src_rid is not None and src_rid != rid:
            consumer_attempts.append((cvid, src_rid, version_slug, variable_slug))

    # First match per consumer cvid wins (stable: input is sorted).
    resolved: dict[int, int] = {}
    for cvid, src_rid, version_slug, variable_slug in consumer_attempts:
        if cvid in resolved:
            continue
        src_cvid = by_key.get((src_rid, version_slug, variable_slug))
        if src_cvid is not None and src_cvid != cvid:
            resolved[cvid] = src_cvid

    if resolved:
        conn.executemany(
            "UPDATE variable_instance SET via_source_id = ? WHERE cvid = ?",
            [(src_cvid, cvid) for cvid, src_cvid in resolved.items()],
        )

    # Surface the skipped count so a regression — e.g. a future delivery
    # introducing a curated source-side slug with no matching consumer-side
    # entry — is visible at build time. Without this, edges silently
    # disappear from `via_source_id` and only show up via downstream
    # lineage-query gaps.
    candidate_cvids = {cvid for cvid, *_ in consumer_attempts}
    skipped = len(candidate_cvids - resolved.keys())
    if candidate_cvids:
        _progress(
            f"  Consumer-side binding edges: {len(resolved):,} linked, "
            f"{skipped:,} skipped (no source-side slug match)"
        )
    return len(resolved)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_db(
    input_dir: Path,
    db_dir: Path,
    *,
    seed_path: Path | None = None,
    skip_classifications: bool = False,
    slug_dir: Path | None = None,
    skip_slugs: bool = False,
    pre_rename_hook: Callable[[Path], None] | None = None,
) -> dict[str, Any]:
    """Build the reg_meta database from SCB CSV exports.

    ``input_dir`` must contain:
      - ``<input_dir>/SCB/*.csv``             — SCB metadata CSV exports
      - ``<input_dir>/classifications/*.csv`` — canonical classification CSVs
        (optional; required only for seed entries that set ``valid_codes_file``)

    Classification population is controlled by:
      - ``skip_classifications=True`` — skip entirely (tests only).
      - ``seed_path`` — explicit seed file. Defaults to ``repo_seed_path()``
        when running from a repo checkout; the build errors out if neither
        is available (build-db is maintainer-only and requires the seed).

    Raises ``RegMetaError(code="vardemangder_drift")`` if Vardemangder.csv
    contains unknown sentinel-shape vardekod values — see
    ``_VARDEMANGDER_SENTINELS`` / ``_VARDEMANGDER_REAL_SHAPED``.

    Returns a summary dict for the CLI to display.
    """
    input_dir = input_dir.expanduser().resolve()
    db_dir = db_dir.expanduser().resolve()
    scb_dir = input_dir / "SCB"
    cls_dir = input_dir / "classifications"

    if not input_dir.is_dir():
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="input_dir_not_found",
            error_class="configuration",
            message=f"Input directory not found: {input_dir}",
            remediation="Provide a directory containing SCB/ and classifications/ subdirectories.",
        )

    if not scb_dir.is_dir():
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="scb_dir_not_found",
            error_class="configuration",
            message=f"SCB subdirectory not found: {scb_dir}",
            remediation="Place SCB metadata CSV exports under <input_dir>/SCB/.",
        )

    ri_path = scb_dir / "Registerinformation.csv"
    if not ri_path.exists():
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="csv_missing_backbone",
            error_class="configuration",
            message=f"Registerinformation.csv not found in {scb_dir}.",
            remediation="Export all metadata files from mikrometadata.scb.se.",
        )

    db_dir.mkdir(parents=True, exist_ok=True)
    final_path = db_dir / DB_FILENAME
    tmp_path = final_path.with_suffix(".db.tmp")
    # Sibling staging file holds the (cvid, code_id, item_id) triples consumed
    # by year-projection. Lives outside the published DB so its pages don't
    # bloat the freelist of the asset shipped to users (PRAGMA temp_store=MEMORY
    # would put SQL TEMP tables in RAM, but a 32M-row staging table won't fit).
    staging_path = tmp_path.with_suffix(".staging.sqlite")

    if tmp_path.exists():
        tmp_path.unlink()
    if staging_path.exists():
        staging_path.unlink()

    conn = sqlite3.connect(tmp_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")  # classification build uses temp tables
    conn.execute("PRAGMA foreign_keys=OFF")  # Enable after import for speed
    build_failed = True
    try:
        conn.executescript(DDL)
        seed_providers(conn)

        # ATTACH staging DB and create the per-build pair table. The PK groups
        # rows for the per-cvid projection pass and dedups identical triples.
        # FK declarations don't work across attached DBs — fine, no main-DB
        # rows reference the staging table. Path is bound (not interpolated)
        # so quotes/specials in the parent dir can't break or inject SQL.
        conn.execute("ATTACH DATABASE ? AS staging", (str(staging_path),))
        conn.execute(
            "CREATE TABLE staging._build_cvid_pair ("
            "cvid INTEGER NOT NULL,"
            "code_id INTEGER NOT NULL,"
            "item_id INTEGER NOT NULL,"  # 0 = empty ItemId in the source CSV
            "PRIMARY KEY (cvid, code_id, item_id)"
            ") WITHOUT ROWID"
        )

        source_checksums: dict[str, str] = {}
        row_counts: dict[str, int] = {}

        # Core: Registerinformation.csv (required)
        source_checksums["Registerinformation.csv"] = _file_sha256(ri_path)
        ri_count, unika_join, known_cvids = _import_registerinformation(conn, ri_path)
        row_counts["Registerinformation.csv"] = ri_count

        # Pre-load validity windows (consumed by Vardemangder year-projection).
        # Loaded ahead of the enrichment loop so projection has it ready when
        # Vardemangder.csv finishes streaming. Required whenever Vardemangder.csv
        # is present: without it, projection silently degrades to the historical
        # union (every code "always-valid"), defeating the schema contract that
        # `get values` returns the year-projected set.
        validity_map: dict[int, list[tuple[int, int]]] = {}
        vvd_path = scb_dir / "VardemangderValidDates.csv"
        vm_path = scb_dir / "Vardemangder.csv"
        if vm_path.exists() and not vvd_path.exists():
            raise RegMetaError(
                exit_code=EXIT_CONFIG,
                code="csv_missing_validity",
                error_class="configuration",
                message=(
                    f"Vardemangder.csv is present but VardemangderValidDates.csv "
                    f"is missing in {scb_dir}. Year-projection requires both."
                ),
                remediation=(
                    "Re-export VardemangderValidDates.csv from "
                    "mikrometadata.scb.se alongside Vardemangder.csv."
                ),
            )
        if vvd_path.exists():
            source_checksums["VardemangderValidDates.csv"] = _file_sha256(vvd_path)
            validity_map, validity_row_count = _load_validity_map(vvd_path)
            row_counts["VardemangderValidDates.csv"] = validity_row_count

        # Enrichment files (optional). VardemangderValidDates.csv is handled
        # above (pre-loaded into memory, not written to DB).
        projection_stats = _ProjectionStats()
        for filename in ENRICHMENT_FILES:
            path = scb_dir / filename
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
                vm_count, cvid_vs_info = _import_vardemangder(conn, path, known_cvids)
                row_counts[filename] = vm_count
                if cvid_vs_info:
                    _progress(
                        f"  Updating {len(cvid_vs_info):,} variable instances with value set info..."
                    )
                    conn.executemany(
                        "UPDATE variable_instance "
                        "SET value_set_version_label = ?, vardemangdsniva = ? "
                        "WHERE cvid = ?",
                        [
                            (ver, niva, cvid)
                            for cvid, (ver, niva) in cvid_vs_info.items()
                        ],
                    )
                # Year-project staging pairs and link variable_instance.value_set_id.
                # Must run after the vardemangds{version,niva} UPDATE because the
                # projection joins variable_instance × register_version.
                projection_stats = _project_and_mint_value_sets(conn, validity_map)

        # A1.2: lift sensitivity / identifier flags from unika_summary into the
        # variable table. Runs after the enrichment loop so both _import_unika
        # (source) and _import_registerinformation (target) have populated their
        # tables; harmless no-op when UnikaRegisterOchVariabler.csv is absent
        # (unika_summary stays empty, every variable keeps its DEFAULT 0).
        _populate_sensitivity_flags(conn)

        # A2.1: coalesce variable_instance rows into variable_state. Reads
        # `unika_summary` for VersionForsta/VersionSista and `register_version`
        # for the year fallback — must run after both are populated. Sensitivity
        # flags must already be lifted (above) because the next step drops
        # `unika_summary` entirely; flipping the order would leave the table
        # gone before its second consumer runs.
        state_stats = _coalesce_variable_states(conn)
        row_counts["variable_state"] = state_stats["n_variable_states"]

        # A2.2's triage runs LATER (after populate_slugs) so the edges
        # it emits can be slug-anchored. We pre-allocate the stats slot
        # here so the manifest assembly downstream can assume the key
        # exists even when the triage path was skipped under `--skip-slugs`.
        triage_stats: dict[str, int] = {}

        # A2.1: drop the now-unused unika_summary table. Both consumers
        # (`_populate_sensitivity_flags` and `_coalesce_variable_states`) have
        # extracted what they need. Dropping after population keeps the
        # build-time loading code simple (single CREATE / INSERT path) while
        # ensuring the shipped DB carries no dead data. The `variable_state`
        # rows we just wrote are the universal-schema home for version_forsta
        # / version_sista.
        conn.execute("DROP TABLE unika_summary")
        _progress("Dropped unika_summary (consumed by A1.2 + A2.1).")

        # Classifications — maintainer-curated normalized code systems.
        if skip_classifications:
            _progress("Skipping classifications (skip_classifications=True)")
        else:
            seed = seed_path or repo_seed_path()
            if seed is None:
                raise RegMetaError(
                    exit_code=EXIT_CONFIG,
                    code="classification_seed_not_found",
                    error_class="configuration",
                    message=(
                        "Classification seed not found. build-db requires the "
                        "in-repo classifications.toml; it is a maintainer-only "
                        "command and is not supported from wheel installs."
                    ),
                    remediation=(
                        "Run from a repo checkout, or run "
                        "`reg-meta update` to fetch the prebuilt DB."
                    ),
                )
            valid_codes_dir = cls_dir if cls_dir.is_dir() else None
            row_counts["classifications.toml"] = populate_classifications(
                conn, seed, valid_codes_dir=valid_codes_dir
            )

        # Slug TOMLs (§5.3): populate slug columns on register / register_variant /
        # classification. Run after classifications so the classification table
        # is populated before its slugs are written.
        if skip_slugs:
            _progress("Skipping slug TOMLs (skip_slugs=True)")
        else:
            slug_root = slug_dir or repo_slug_dir()
            if slug_root is None:
                raise RegMetaError(
                    exit_code=EXIT_CONFIG,
                    code="slug_dir_not_found",
                    error_class="configuration",
                    message=(
                        "Slug TOMLs not found. build-db requires the in-repo "
                        "reg_meta_build/fqid_slugs/ directory; it is a maintainer-only "
                        "command and is not supported from wheel installs."
                    ),
                    remediation=(
                        "Run from a repo checkout, pass --slug-dir, or run "
                        "`reg-meta update` to fetch the prebuilt DB."
                    ),
                )
            populate_slugs(conn, slug_root, strict=True)

        # A2.2: build-time triage per §5.7. Runs AFTER populate_slugs so
        # the edges it emits can be slug-anchored (register.slug /
        # variant.slug are populated by that point). Resolves same-year
        # multi-state collisions by splitting variables into siblings
        # (kolumnnamn / vardemangdsniva / datalangd discriminators),
        # collapsing metadata drift, and emitting
        # `variable_related_to` provenance edges. Reads
        # `variable_instance` × `variable_alias` for the SCB-source
        # discriminators (kolumnnamn, vardemangdsniva, classification_id)
        # that don't land on the universal schema.
        #
        # `slug_dir` is forwarded so the TOML-override light-path (a
        # `[variable."<id>"]` block with an explicit `slug` field wins
        # over the auto-derived sibling slug). Bulk curation backlog is
        # deferred to a separate PR after an empirical run.
        #
        # `--skip-slugs` honest-failure stance: triage still runs to
        # carry out the in-DB sibling split / collapse work (those
        # decisions are correctness-relevant regardless of slug
        # availability), but slug-anchored edge emission requires
        # register.slug — under skip-slugs the edges are skipped
        # silently inside `_allocate_sibling_variables`.
        triage_slug_dir = None if skip_slugs else (slug_dir or repo_slug_dir())
        triage_stats = _triage_same_year_collisions(
            conn,
            slug_dir=triage_slug_dir,
        )

        # §5.5 same_as edges. Runs *after* populate_slugs so register /
        # variant / version slug columns are populated — the materializer
        # validates target slugs against them. Skip-slugs takes the same
        # honest-failure stance as link_consumer_side_bindings below.
        if skip_slugs:
            _progress("Skipping same_as edges (skip_slugs=True)")
        else:
            sa_counts = materialize_same_as_edges(conn, slug_root)
            _progress(
                f"  {sa_counts['variable']:,} variable same_as edges, "
                f"{sa_counts['classification']:,} classification same_as edges"
            )
            # §5.5 / §5.7 curator-supplied related_to edges. Triage already
            # emitted its auto edges with `note='auto:triage'` before unika
            # dropped; the TOML pass here inserts curator edges with
            # whatever `note` they wrote (default NULL). Same skip-slugs
            # stance as same_as: without TOML access we'd silently produce
            # zero TOML edges, an honest miss is better than a quiet one.
            rt_count = materialize_related_to_edges(conn, slug_root)
            _progress(f"  {rt_count:,} variable related_to TOML edges")

        # §5.6 lineage edges. Runs *after* populate_slugs so the lookup keys
        # on `register_version.slug` — the canonical disambiguator. Keying on
        # `derive_period(name)` would collapse siblings that γ's curated
        # overrides exist to separate (e.g. two `LISA 2018 …` rows in the
        # same variant) and silently link consumers to the wrong source cvid.
        # `source_register_id` was populated by the Registerinformation.csv
        # import far above; no intermediate step depends on `via_source_id`.
        #
        # Skip under `--skip-slugs`: the linker is slug-only and every
        # `rver.slug` is NULL in that mode, so running would silently
        # produce zero edges instead of an honest "this build is
        # incomplete" signal. Run `build-db` without `--skip-slugs` (the
        # default) to materialize lineage.
        if skip_slugs:
            _progress("Skipping consumer-side binding edges (skip_slugs=True)")
        else:
            link_consumer_side_bindings(conn)

        # Populate code_variable_map from year-projected value_set_member rows
        # joined through variable_instance.value_set_id. A code only appears
        # for (register, var) pairs where it was valid at some cvid year.
        _progress("Building code_variable_map...")
        conn.execute(
            "INSERT INTO code_variable_map (code_id, register_id, var_id) "
            "SELECT DISTINCT vsm.code_id, vi.register_id, vi.var_id "
            "FROM variable_instance vi "
            "JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id "
            "WHERE vi.value_set_id IS NOT NULL"
        )
        cvm_count = conn.execute("SELECT COUNT(*) FROM code_variable_map").fetchone()[0]
        _progress(f"  {cvm_count:,} code×variable mappings")

        # Reference files (optional)
        sql_path = scb_dir / "Tabelldefinitioner.sql"
        if sql_path.exists():
            row_counts["Tabelldefinitioner.sql"] = _import_tabelldefinitioner(
                conn, sql_path
            )
        else:
            _progress("Skipping Tabelldefinitioner.sql (not found)")

        xlsx_path = scb_dir / "ID-kolumner.xlsx"
        if xlsx_path.exists():
            row_counts["ID-kolumner.xlsx"] = _import_id_kolumner(conn, xlsx_path)
        else:
            _progress("Skipping ID-kolumner.xlsx (not found)")

        _populate_fts(conn)

        # Write manifest
        manifest_data = {
            "schema_version": SCHEMA_VERSION,
            "import_date": utc_now(),
            "input_dir": str(input_dir),
            "source_checksums": source_checksums,
            "row_counts": row_counts,
            "projection_stats": {
                "n_value_sets": projection_stats.n_value_sets,
                "cvids_with_set": projection_stats.cvids_with_set,
                "cvids_empty_after_projection": projection_stats.cvids_empty_after_projection,
            },
            # A2.1 coalescer stats — let maintainers eyeball the empirical
            # 5× shrink and unika-vs-fallback split without re-running.
            "coalesce_stats": state_stats,
            # A2.2 triage stats — collision-bucket resolution counts +
            # sibling/edge cardinality, so curators can spot when the
            # auto-handle rate slips below the ~99% target.
            "triage_stats": triage_stats,
        }
        for key, value in manifest_data.items():
            conn.execute(
                "INSERT INTO import_manifest VALUES (?, ?)",
                (key, json.dumps(value) if isinstance(value, dict) else str(value)),
            )

        # Validate FK invariants. The build runs with foreign_keys=OFF for
        # speed; toggling within an active transaction is a no-op, so FK
        # declarations alone don't validate the data. PRAGMA foreign_key_check
        # returns rows on violation; the enabling-flip below is cosmetic.
        violations = list(conn.execute("PRAGMA foreign_key_check"))
        if violations:
            sample = ", ".join(f"{v[0]}#{v[1]}" for v in violations[:5])
            raise RegMetaError(
                exit_code=EXIT_CONFIG,
                code="foreign_key_violation",
                error_class="configuration",
                message=(
                    f"PRAGMA foreign_key_check returned {len(violations)} "
                    f"violation(s) before commit. Sample: {sample}."
                ),
                remediation=(
                    "Inspect the build logs for missing parent rows. This "
                    "usually means a CVID referenced by Vardemangder.csv "
                    "was not present in Registerinformation.csv, or a "
                    "value_set_id was assigned without a corresponding "
                    "value_set row."
                ),
            )
        conn.execute("PRAGMA foreign_keys=ON")

        conn.commit()

        # A2.1: VACUUM reclaims the pages freed by `DROP TABLE unika_summary`
        # so the shipped DB doesn't carry a fat freelist. `validate.py` flags
        # >= 1% freelist as staging-bloat; on the synthetic fixture the drop
        # alone leaves ~2.7%. VACUUM must run outside a transaction — the
        # preceding commit ensures it does. ATTACH-staging was already
        # detached implicitly when the staging path was passed (or stays
        # attached harmlessly; VACUUM only touches `main`).
        conn.execute("VACUUM")

        _progress("Database built successfully.")
        build_failed = False
    finally:
        conn.close()
        staging_path.unlink(missing_ok=True)
        if build_failed:
            tmp_path.unlink(missing_ok=True)

    # Pre-rename hook runs against the staging DB so a failing check
    # can abort *before* the atomic rename replaces the installed DB.
    # If the hook raises, drop the tmp file and let the prior DB stand.
    if pre_rename_hook is not None:
        try:
            pre_rename_hook(tmp_path)
        except BaseException:
            tmp_path.unlink(missing_ok=True)
            raise

    # Rotate the prior universal DB aside before the atomic replace
    # (REFACTOR_SPEC §4.4 / §5.8: single-generation `.prev`, no auto-cleanup).
    rotate_db_to_prev(final_path)
    tmp_path.rename(final_path)

    # Sibling provenance DB scaffolding. `build_manifest` stays empty until
    # A4.x populates it; this PR just guarantees the file exists alongside
    # the universal DB it was built against — otherwise a rebuild on an env
    # with a prior provenance DB would leave only `.prev` and break
    # downstream tooling that expects the live file.
    #
    # Wrapped in try/except: this runs AFTER the universal DB has already
    # been swapped in, so any IOError here (disk full, perms) must not flip
    # the build's exit code to "failed" — the primary artifact succeeded.
    # Surface as a warning instead. A4.x will hook into this same block
    # for real provenance writes; that path will need its own atomicity
    # story (rename-into-place from a tmp), but for now an empty schema
    # file is cheap to recreate manually and unblocks the build contract.
    provenance_path = db_dir / PROVENANCE_DB_FILENAME
    try:
        rotate_db_to_prev(provenance_path)
        create_empty_provenance_db(provenance_path)
    except (OSError, RegMetaError) as e:
        _progress(
            f"  WARNING: provenance DB scaffolding failed ({type(e).__name__}: {e}); "
            f"universal DB was written successfully — re-run or manually create "
            f"{provenance_path.name} to restore provenance."
        )
    _progress(f"Database written to {final_path}")

    return {
        "db_path": str(final_path),
        "schema_version": SCHEMA_VERSION,
        "import_date": manifest_data["import_date"],
        "source_checksums": source_checksums,
        "row_counts": row_counts,
    }
