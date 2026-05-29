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
    materialize_same_as_edges,
    populate_slugs,
    populate_variable_slugs,
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
    register_variant_id INTEGER PRIMARY KEY,
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
    register_variant_id INTEGER NOT NULL REFERENCES register_variant(register_variant_id),
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
    UNIQUE (register_variant_id, slug)
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
    -- A2.1.5 (DECISION POINT 1, §5.1): synthetic PK so variable_state's FK is
    -- single-column and the edge tables stay stable as the natural key varies
    -- per provider. The natural key is (register_id, slug); `provider_key`
    -- (SCB `str(var_id)`; SOS the merged variable name) is demoted from the PK
    -- to a NON-unique join hint — a §5.7 triage split (A2.2) puts several
    -- variables under one source key.
    variable_id INTEGER PRIMARY KEY AUTOINCREMENT,
    register_id INTEGER NOT NULL REFERENCES register(register_id),
    -- SCB str(var_id), TEXT so SOS can key by merged variable name (§5.1).
    -- NON-unique join hint, not a key; variable_instance.var_id (INTEGER) joins
    -- via CAST-to-TEXT until variable_instance is dropped in A2.7.
    provider_key TEXT NOT NULL,
    -- §5.3 register-unique FQID leaf. NULL until the A2.1.5 slug follow-up PR
    -- populates it; SQLite treats NULLs as distinct, so the transient all-NULL
    -- window doesn't trip the unique index below.
    slug TEXT,
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
    is_identifier INTEGER NOT NULL DEFAULT 0
);
-- Natural key: register-unique slug (the FQID leaf, §5.3). Stays unique after
-- an A2.2 triage split because siblings get distinct slugs. The one UNIQUE
-- constraint on the table (DECISION POINT 1).
CREATE UNIQUE INDEX idx_variable_slug ON variable(register_id, slug);
-- `provider_key` is a NON-unique join hint, not a key: A2.2 triage siblings
-- share one source key. Plain index, not UNIQUE. Serves the resolver's
-- (register_id, provider_key) join from variable_instance.var_id (CAST to TEXT).
CREATE INDEX idx_variable_natkey ON variable(register_id, provider_key);

CREATE TABLE variable_instance (
    cvid INTEGER PRIMARY KEY,
    register_id INTEGER NOT NULL,
    register_variant_id INTEGER NOT NULL,
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
    via_source_id INTEGER REFERENCES variable_instance(cvid)
    -- A2.1.5: no FK to `variable` — its natural key moved to the synthetic
    -- `variable_id` PK + register-unique `slug`, so `(register_id, var_id)` is
    -- no longer a UNIQUE/PK target. The join is by convention (and the
    -- `idx_variable_natkey` index) until `variable_instance` is dropped in A2.7.
);

-- A2.1: per-era shape of a variable (§5.1). One row per coalesced
-- `(register_id, register_variant_id, var_id, data_type, data_length, value_set_id,
-- value_set_version_label, grain)` tuple over `variable_instance`; populated
-- by `_coalesce_variable_states` after CSV import. Resolver still uses
-- `variable_instance` at this stage — A2.5 flips it to `variable_state`.
-- A2.1.5 re-parented this onto the synthetic `variable_id` FK (was FK
-- `(register_id, var_id)` in A2.1) and made `register_variant_id` an explicit
-- delivery coordinate; the coalescer resolves each group's `variable_id` from
-- `(register_id, var_id)` via the promoted `variable` table.
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
    variable_id INTEGER NOT NULL REFERENCES variable(variable_id),
    register_variant_id INTEGER NOT NULL REFERENCES register_variant(register_variant_id),
    valid_from TEXT NOT NULL,
    valid_to TEXT NOT NULL DEFAULT '9999-12-31',
    data_type TEXT,
    data_length TEXT,
    delivery_column_name TEXT,
    value_set_id INTEGER REFERENCES value_set(value_set_id),
    -- §5.7 overlap discriminator (multi-vintage / grain / coding). NOT NULL
    -- DEFAULT '' so the uniqueness index below bites in the common
    -- single-version case — SQLite treats NULLs as distinct, which would let
    -- duplicate non-multi-vintage states slip through. Mirrors '9999-12-31'.
    value_set_version_label TEXT NOT NULL DEFAULT '',
    -- Full-date contract: ten-character ISO 8601 strings only. Length check
    -- is a cheap structural guard; a stricter regex isn't worth the runtime
    -- cost because the coalescer is the only writer.
    CHECK (length(valid_from) = 10),
    CHECK (length(valid_to) = 10),
    CHECK (valid_to >= valid_from)
);
CREATE INDEX idx_variable_state_variable
    ON variable_state(variable_id);
CREATE INDEX idx_variable_state_register_variant
    ON variable_state(register_variant_id);
-- NOTE: the §5.1 state-uniqueness index — UNIQUE(variable_id,
-- register_variant_id, valid_from, value_set_version_label) — is intentionally
-- NOT created here. `_coalesce_variable_states` emits one PRE-TRIAGE row per
-- (… data_type, data_length, value_set_id, value_set_version_label, grain)
-- group, so a same-year variable with multiple grains / codings / shapes
-- produces several rows that share (variable_id, register_variant_id,
-- valid_from) and carry value_set_version_label = '' — they'd collide on that
-- index before A2.2 can fold them (→ value_set_version_label-discriminated
-- states) or split them (→ sibling variable_ids). The uniqueness invariant
-- only holds POST-triage, so the unique index is added in A2.2. value_set_
-- version_label stays NOT NULL DEFAULT '' here so the index bites once added.
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
    register_variant_id INTEGER,
    kolumnnamn TEXT,
    variabelnamn TEXT,
    version_forsta TEXT,
    version_sista TEXT,
    kanslig_variabel TEXT,
    kanslig_variabel_ibland TEXT,
    identitetsvariabel TEXT,
    PRIMARY KEY (register_id, register_variant_id, kolumnnamn, variabelnamn)
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
    provider_key,
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
CREATE INDEX idx_register_version_register_variant ON register_version(register_variant_id);
CREATE INDEX idx_variable_instance_register ON variable_instance(register_id);
CREATE INDEX idx_variable_instance_var ON variable_instance(register_id, var_id);
CREATE INDEX idx_variable_instance_register_variant ON variable_instance(register_variant_id);
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
                    "register_variant_id": rvid,
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
                    "register_variant_id": rvid,
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
                    "register_variant_id": rvid,
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
        "register_variant_id, register_id, name, description"
        ") VALUES ("
        ":register_variant_id, :register_id, :name, :description"
        ")",
        list(variants.values()),
    )
    conn.executemany(
        "INSERT INTO register_version "
        "(regver_id, register_variant_id, registerversionnamn, "
        "registerversionbeskrivning, registerversionmatinformation, "
        "registerversion_docstaus, registerversion_forstagodkannandedatum, "
        "registerversion_senastgodkanddatum) VALUES ("
        ":regver_id, :register_variant_id, :registerversionnamn, "
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
        "INSERT INTO variable (register_id, provider_key, name, definition, description, "
        "source_register_text, measurement_unit, source_register_id, source_label) "
        "VALUES (:register_id, CAST(:var_id AS TEXT), :name, :definition, :description, "
        ":source_register_text, :measurement_unit, :source_register_id, :source_label)",
        list(variables.values()),
    )
    conn.executemany(
        "INSERT INTO variable_instance "
        "(cvid, register_id, register_variant_id, regver_id, var_id, variabelnamn, "
        " data_type, data_length) "
        "VALUES (:cvid, :register_id, :register_variant_id, :regver_id, "
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
            register_id, register_variant_id = ids
            batch.append(
                (
                    register_id,
                    register_variant_id,
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

    `unika_summary` stores `(register_id, register_variant_id, kolumnnamn, variabelnamn)`
    but not `var_id`. To resolve `var_id` we route the join through
    `variable_instance × variable_alias`: the `(register_id, register_variant_id,
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
        "     AND vi.register_variant_id = us.register_variant_id "
        # `unika_summary` keeps Swedish column names — A1.1 didn't touch
        # that table because A2.1 drops it. `variable_alias` and
        # `variable` were renamed: `kolumnnamn` → `delivery_column_name`,
        # `variabelnamn` → `name`.
        "    JOIN variable_alias va "
        "      ON va.cvid = vi.cvid "
        "     AND va.delivery_column_name = us.kolumnnamn "
        "    JOIN variable v "
        "      ON v.register_id = vi.register_id "
        "     AND v.provider_key = CAST(vi.var_id AS TEXT) "
        "     AND v.name = us.variabelnamn "
        "    GROUP BY vi.register_id, vi.var_id"
        ") AS flags "
        "WHERE variable.register_id = flags.register_id "
        "  AND variable.provider_key = CAST(flags.var_id AS TEXT)"
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

    Group key: `(register_id, register_variant_id, var_id, data_type, data_length,
    value_set_id, value_set_version_label, grain)`.

    `grain` is the transient pre-triage carrier for SCB's `vardemangdsniva`
    (still present on `variable_instance` through A2.2). Keeping it in the
    group key here means multi-grain variables stay distinct so A2.2's
    triage can split them into sibling slugs; grain itself does not land in
    the final schema.

    For each group:

    1. Resolve `(register_id, register_variant_id, kolumnnamn, variabelnamn)` from the
       cvids in the group via `variable_alias × variable`. Note the cross-
       product: a single cvid can have N aliases (rare; cross-edition
       drift), so the group's unika lookup keys are the *union* of all
       (register_variant, alias, name) triples for the cvids.
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
        "SELECT vi.cvid, vi.register_id, vi.register_variant_id, vi.var_id, vi.regver_id, "
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
        register_variant_id: int
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
    # Map (register_id, register_variant_id, kolumnnamn, variabelnamn) → set of
    # group keys so the unika fan-out below stays proportional to distinct
    # groups, not raw instance-row hits (a wide variable with many cvids
    # / aliases would otherwise have its single unika row replayed once
    # per row even though min/max is idempotent).
    unika_index: dict[tuple[int, int, str, str], set[tuple]] = {}

    # Max regver year observed per (register_id, register_variant_id, var_id). A
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
            row["register_variant_id"],
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
                register_variant_id=row["register_variant_id"],
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
            vkey = (row["register_id"], row["register_variant_id"], row["var_id"])
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
        # (register_id, register_variant_id, kolumnnamn, variabelnamn); we need an
        # alias to build the triple, so cvids without an alias contribute
        # only via the fallback path. The unika_index is a set so that
        # repeat (alias, variabelnamn) sightings across cvids in the same
        # group don't fan a single unika row out into duplicate updates.
        if alias and row["variabelnamn"]:
            ukey = (
                row["register_id"],
                row["register_variant_id"],
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
        "SELECT register_id, register_variant_id, kolumnnamn, variabelnamn, "
        "       version_forsta, version_sista FROM unika_summary"
    ).fetchall()
    for ur in unika_rows:
        ukey = (
            ur["register_id"],
            ur["register_variant_id"],
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
    # A2.1.5: resolve each group's `variable_id` from its (register_id, var_id)
    # via the promoted `variable` table. var_id is 1:1 with a variable until
    # A2.2 triage splits land, so the lookup is unambiguous here.
    # The CAST is SCB-only-safe: SCB provider_keys are str(var_id) and round-trip
    # to the integer var_id this coalescer groups by. SOS provider_keys are
    # merged variable *names* — `CAST('name' AS INTEGER)` → 0 in SQLite — but
    # this SCB-specific coalescer is replaced by the per-provider IR adapters in
    # A4, so the numeric-key assumption never reaches SOS.
    vid_map: dict[tuple[int, int], int] = {
        (r[0], r[1]): r[2]
        for r in conn.execute(
            "SELECT register_id, CAST(provider_key AS INTEGER), variable_id FROM variable"
        )
    }

    batch: list[tuple] = []
    sentinel_count = 0
    fallback_only_count = 0
    open_top_from_unika = 0
    for grp in groups.values():
        vkey = (grp.register_id, grp.register_variant_id, grp.var_id)
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

        variable_id = vid_map.get((grp.register_id, grp.var_id))
        if variable_id is None:
            # Defensive: `variable` and `variable_instance` derive from the same
            # source rows, so every coalesced state has a parent variable. The
            # FK used to catch an orphan at insert; surface an actionable error
            # instead of a bare KeyError if that invariant ever breaks.
            raise RegMetaError(
                exit_code=EXIT_CONFIG,
                code="coalesce_missing_variable",
                error_class="configuration",
                message=(
                    f"variable_state group (register_id={grp.register_id}, "
                    f"var_id={grp.var_id}) has no matching `variable` row."
                ),
                remediation=(
                    "`variable` and `variable_instance` disagree — rebuild from "
                    "source with `reg-meta-build build-db`."
                ),
            )
        batch.append(
            (
                variable_id,
                grp.register_variant_id,
                valid_from,
                valid_to,
                grp.data_type,
                grp.data_length,
                grp.latest_alias,
                grp.value_set_id,
                # NOT NULL DEFAULT '' on the column; coalesce here so the
                # uniqueness index bites for the common single-version case.
                grp.value_set_version_label or "",
            )
        )

    conn.executemany(
        "INSERT INTO variable_state (variable_id, register_variant_id, "
        "    valid_from, valid_to, data_type, data_length, delivery_column_name, "
        "    value_set_id, value_set_version_label) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
        INSERT INTO variable_fts(rowid, register_id, provider_key, name, definition, description)
        SELECT
            v.rowid,
            v.register_id,
            v.provider_key,
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
    # arises if two source siblings share a slug, which UNIQUE(register_variant_id,
    # slug) on register_version forbids; effectively the setdefault is
    # never contested in a strict-built DB.
    rows = conn.execute(
        "SELECT vi.cvid, vi.register_id, v.source_register_id, "
        "rver.slug AS version_slug, va.delivery_column_name "
        "FROM variable_instance vi "
        "JOIN variable v ON vi.register_id = v.register_id "
        "    AND CAST(vi.var_id AS TEXT) = v.provider_key "
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

            # A2.1.5 (§5.3): stored `variable.slug`. Runs after populate_slugs
            # (register/variant slugs feed collision messages) and after
            # _coalesce_variable_states (reads variable_state.delivery_column_name),
            # but before materialize_same_as_edges (which reads the stored slug
            # via _variable_source_slug). Curated `[variable]` overrides in
            # scb.toml win; the rest auto-derive into scb.auto.toml.
            var_slug_counts = populate_variable_slugs(conn, slug_root, strict=True)
            row_counts["variable_slugs_curated"] = var_slug_counts["curated"]
            row_counts["variable_slugs_auto"] = (
                var_slug_counts["auto_existing"] + var_slug_counts["auto_new"]
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
