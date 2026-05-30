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
from collections import Counter, defaultdict
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
    load_lineage_config,
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

-- A2.6: BUILD-TIME-ONLY (dropped before ship, like `unika_summary`). The
-- coalescer reads `registerversionnamn` for the variable_state valid_from/to
-- year fallback, and the lineage linkers derive a per-edition period from it;
-- both run before `DROP TABLE register_version`. The FQID grammar no longer
-- has a version segment (§5.2), so this table carries NO `slug` column — period
-- is a delivery coordinate, not identity. Per-edition prose/artifacts move to
-- the provenance DB (A4.2, deferred); nothing in the shipped catalog reads it.
CREATE TABLE register_version (
    regver_id INTEGER PRIMARY KEY,
    register_variant_id INTEGER NOT NULL REFERENCES register_variant(register_variant_id),
    registerversionnamn TEXT,
    registerversionbeskrivning TEXT,
    registerversionmatinformation TEXT,
    registerversion_docstaus TEXT,
    registerversion_forstagodkannandedatum TEXT,
    registerversion_senastgodkanddatum TEXT
);

-- A2.6: BUILD-TIME-ONLY, dropped before ship together with `register_version`
-- (they FK it). Write-only debug tables — nothing in the shipped catalog or the
-- query layer reads them; their content belongs in the provenance DB (§5.1,
-- A4.2). Kept build-time only because the importer still populates them from the
-- same Registerinformation.csv pass.
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
    -- NON-unique join hint, not a key: the build-time `variable_instance.var_id`
    -- (INTEGER) joins via CAST-to-TEXT, and `code_variable_map.var_id` carries it
    -- into the shipped DB.
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
-- share one source key. Plain index, not UNIQUE. Serves the build-time
-- (register_id, provider_key) joins from `variable_instance.var_id` (CAST to
-- TEXT) and the query-layer lookups that match a variable by its source var_id.
CREATE INDEX idx_variable_natkey ON variable(register_id, provider_key);

-- A2.7: BUILD-TIME-ONLY (dropped before ship, like `register_version` /
-- `unika_summary`). The coalescer reads it to produce `variable_state`,
-- `populate_classifications` tags `classification_id` here, value-set projection
-- writes `value_set_id` here, and `code_variable_map` is materialized from it —
-- all BEFORE `DROP TABLE variable_instance`. The shipped query layer reads
-- `variable_state` / `variable` / re-parented `variable_alias` instead (the
-- per-cvid grain has no FQID — the 3-seg binding FQID is variable-grained).
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
    -- so the lookup matches.
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
    value_set_id INTEGER REFERENCES value_set(value_set_id)
    -- A2.1.5: no FK to `variable` — its natural key moved to the synthetic
    -- `variable_id` PK + register-unique `slug`, so `(register_id, var_id)` is
    -- no longer a UNIQUE/PK target. The join is by convention (and the
    -- `idx_variable_natkey` index). A2.7 dropped the v0.11 `via_source_id`
    -- self-FK lineage column (superseded by `variable_state_lineage`, A2.4).
);

-- A2.7: BUILD-TIME-ONLY cvid-grained alias staging. The import pass writes one
-- row per (cvid, delivery_column_name); the coalescer + sensitivity + replaced_by
-- passes read it by `cvid`; then `_reparent_variable_alias` projects it onto the
-- shipped `variable_id`-keyed `variable_alias` and DROPs it before ship. (Kept
-- separate from the shipped table because the cvid grain has no FK target once
-- `variable_instance` is dropped.)
CREATE TABLE variable_alias_build (
    cvid INTEGER NOT NULL REFERENCES variable_instance(cvid),
    delivery_column_name TEXT NOT NULL,
    PRIMARY KEY (cvid, delivery_column_name)
);

-- A2.1: per-era shape of a variable (§5.1). One row per coalesced
-- `(register_id, register_variant_id, var_id, data_type, data_length, value_set_id,
-- value_set_version_label, grain)` tuple over `variable_instance`; populated
-- by `_coalesce_variable_states` after CSV import. A2.5/A2.6 flipped the
-- resolver onto this table (keyed by `variable_id`); A2.7 drops the now-unused
-- `variable_instance` after the coalescer + downstream build passes consume it.
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
    -- A2.7: §5.7 classification family for this era's value set. The coalescer
    -- can't set it (it runs before `populate_classifications`); a build step
    -- backfills it after classifications + value-set minting, correlating each
    -- state to its constituent `variable_instance` rows by (variable_id,
    -- value_set_id) — see `_backfill_state_classifications`. NULL for code-less
    -- or unclassified states. The query layer reads it from `variable_state`
    -- (which has `variable_id`), so classification lookups sibling-isolate after
    -- the A2.2 split — resolving the A2.6 `classifications_for_variable`
    -- limitation that `variable_instance` (no `variable_id`) couldn't.
    classification_id INTEGER REFERENCES classification(id),
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
-- NOT in this base DDL. `_coalesce_variable_states` emits one PRE-TRIAGE row
-- per (… data_type, data_length, value_set_id, value_set_version_label, grain)
-- group, so a same-year variable with multiple grains / codings / shapes
-- produces several rows that share (variable_id, register_variant_id,
-- valid_from) and carry value_set_version_label = '' — they'd collide before
-- A2.2 triage folds them (→ value_set_version_label-discriminated states),
-- splits them (→ sibling variable_ids), or collapses drift. The invariant only
-- holds POST-triage, so `_coalesce_variable_states` CREATEs the unique index
-- itself, after triage runs (idx_variable_state_unique). value_set_version_label
-- stays NOT NULL DEFAULT '' so the index bites in the common single-version case.
CREATE INDEX idx_variable_state_value_set
    ON variable_state(value_set_id)
    WHERE value_set_id IS NOT NULL;
-- A2.7: serves `search_variables_by_classification` (filter states by family).
-- Partial — most states carry no classification.
CREATE INDEX idx_variable_state_classification
    ON variable_state(classification_id)
    WHERE classification_id IS NOT NULL;

-- A2.7: the FULL delivery-column alias history, keyed by `variable_id` (was
-- `cvid` through A2.6). It SURVIVES into the shipped DB — `get_datacolumns`
-- surfaces every historical column, which the coalesced
-- `variable_state.delivery_column_name` (latest era only) can't. The build
-- seeds the cvid-grained rows during import, then RE-PARENTS onto
-- `variable_id` + `register_variant_id` (`_reparent_variable_alias`) right
-- before `DROP TABLE variable_instance`, so the cvid FK never dangles. A
-- post-A2.2 `var_id` can be non-unique (split siblings share it), so an alias
-- whose cvid can't be attributed to a specific sibling attaches to all siblings
-- sharing the key — acceptable for alias *search/history* recall (the only
-- consumers); it does not feed resolution (the resolver reads `variable_state`).
CREATE TABLE variable_alias (
    variable_id INTEGER NOT NULL REFERENCES variable(variable_id),
    -- The delivering variant. Lets `get_datacolumns` group columns per variant
    -- as it did off `variable_instance.register_variant_id`.
    register_variant_id INTEGER NOT NULL REFERENCES register_variant(register_variant_id),
    -- §5.11: `kolumnnamn` → `delivery_column_name`. The SCB delivery column
    -- header (e.g. `PersonNr`, `Kon`, `LopNr_PersonNr`). SCB pseudonymizes
    -- identifier columns at delivery with the `LopNr_` prefix; the metadata
    -- stores the un-prefixed name.
    delivery_column_name TEXT NOT NULL,
    PRIMARY KEY (variable_id, register_variant_id, delivery_column_name)
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
    -- A2.6.1: slug carries the vintage (version baked in, §5.2): 'sun2020',
    -- 'lkf2007'. The classification FQID is the 2-segment `class/<slug>` —
    -- the standalone `version` column is gone (vintage lives in slug + name +
    -- valid_from/valid_to). UNIQUE, not NOT NULL: `populate_slugs` UPDATEs
    -- this after `populate_classifications` INSERTs the row (NULL at insert),
    -- and SQLite allows multiple NULLs under UNIQUE; the strict NULL-slug guard
    -- in `populate_slugs` enforces presence at build end.
    slug             TEXT UNIQUE
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

-- Curated cross-register / cross-provider equivalence edges (§5.5).
-- **Variable grain**: endpoints are `(provider, register, variable)` slug
-- triples. Slug-anchored (not cvid-anchored), so the link survives rebuilds
-- even if provider IDs shift. Each TOML same_as entry becomes two rows
-- (A→B and B→A) so the resolver does a single forward lookup.
--
-- A2.1.5 dropped the v0.11 `a_variant`/`b_variant` and `a_period`/`b_period`
-- slots: a variable is register-scoped, so one edge covers every variant that
-- delivers either variable, and period was never load-bearing for same_as
-- semantics — validity is implicit in both variables' state histories (§5.5).
-- (§5.5 also reserves a `note` column for curator annotations; not added here
-- because the TOML same_as form carries no note field to populate it yet.)
CREATE TABLE variable_same_as (
    a_provider     TEXT NOT NULL,
    a_register     TEXT NOT NULL,
    a_variable     TEXT NOT NULL,
    b_provider     TEXT NOT NULL,
    b_register     TEXT NOT NULL,
    b_variable     TEXT NOT NULL,
    PRIMARY KEY (
        a_provider, a_register, a_variable,
        b_provider, b_register, b_variable
    )
) WITHOUT ROWID;
-- No separate a-side index: this is a WITHOUT ROWID table, so the PRIMARY KEY
-- is the clustered index, and its leading (a_provider, a_register, a_variable)
-- prefix already serves the resolver's source-side lookup.

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

-- §5.5/§5.7 sibling edges (variable grain). A2.2 triage emits these between
-- the distinct `variable` rows a *split* produced (disjoint columns lumped
-- under one source `var_id`): one variable per column, linked here so a
-- consumer can discover "these are the same definition delivered as different
-- columns." Folds do NOT appear here — they stay one variable (§5.7). Stored
-- in BOTH directions (like `variable_same_as`) so the a-side PK prefix serves
-- `Catalog.related(x)` without a second b-side scan; the (N choose 2) sibling
-- pairs each yield two rows. `relation_kind` reflects the split reason
-- (`same_definition_different_column` for SCB disjoint-column splits); `note`
-- carries provenance (`auto:triage` for build-emitted edges, vs. a curated
-- TOML override).
CREATE TABLE variable_related_to (
    a_provider     TEXT NOT NULL,
    a_register     TEXT NOT NULL,
    a_variable     TEXT NOT NULL,
    b_provider     TEXT NOT NULL,
    b_register     TEXT NOT NULL,
    b_variable     TEXT NOT NULL,
    relation_kind  TEXT NOT NULL,
    note           TEXT,
    PRIMARY KEY (
        a_provider, a_register, a_variable,
        b_provider, b_register, b_variable
    )
) WITHOUT ROWID;

-- A2.3: directional succession edges (§5.5). Auto-derived from SCB
-- `timeseries_event` rows with `handelse IN ('Ersatt av', 'Ersätter')` by
-- `_materialize_replaced_by_edges`. Three sibling tables, one per entity grain
-- (register / variant / variable). Slug-anchored so an edge survives rebuilds
-- even if the underlying provider IDs shift. `note = 'auto:timeseries_event'`
-- distinguishes the auto-derive path from future TOML-curated cross-provider
-- rows (A4).
--
-- Unlike `same_as` (an equivalence, stored both ways), `replaced_by` is
-- DIRECTIONAL: SCB's paired `Ersatt av` / `Ersätter` rows collapse to one
-- predecessor → successor edge. Each table is WITHOUT ROWID with a
-- predecessor-first PK, so the clustered PK prefix already serves the forward
-- "what replaced X?" lookup — no separate predecessor index (mirrors
-- `variable_same_as`). The reverse "what did X replace?" (successor-keyed)
-- lookup is served by `idx_variable_replaced_by_successor` (added below for the
-- A2.5 `.predecessors()` accessor; only the variable grain has an accessor that
-- needs it, so register/variant stay index-free on the successor side).
--
-- #142: `beskrivning` carries the human transition reason from
-- `timeseries_event.beskrivning` (e.g. "2001 byttes SUN96 till SUN2000"),
-- alongside the `auto:timeseries_event` provenance in `note` (kept distinct so
-- the A4 TOML-curation path can still tell auto from curated). All three sibling
-- tables carry it so they stay structurally identical and the materializer can
-- resolve it uniformly. `effective_year` is populated for the AktuellVariabel
-- variable grain (the successor edition's year); other grains leave it NULL
-- (no edition to derive a year from — see `_materialize_replaced_by_edges`).
CREATE TABLE register_replaced_by (
    predecessor_provider TEXT NOT NULL,
    predecessor_register TEXT NOT NULL,
    successor_provider   TEXT NOT NULL,
    successor_register   TEXT NOT NULL,
    effective_year       INTEGER,
    note                 TEXT,
    beskrivning          TEXT,
    PRIMARY KEY (predecessor_provider, predecessor_register,
                 successor_provider, successor_register)
) WITHOUT ROWID;

CREATE TABLE variant_replaced_by (
    predecessor_provider TEXT NOT NULL,
    predecessor_register TEXT NOT NULL,
    predecessor_variant  TEXT NOT NULL,
    successor_provider   TEXT NOT NULL,
    successor_register   TEXT NOT NULL,
    successor_variant    TEXT NOT NULL,
    effective_year       INTEGER,
    note                 TEXT,
    beskrivning          TEXT,
    PRIMARY KEY (predecessor_provider, predecessor_register, predecessor_variant,
                 successor_provider, successor_register, successor_variant)
) WITHOUT ROWID;

-- Variable grain: 3-part (provider, register, variable) endpoints — NO variant.
-- A2.1.5's two-level model made the variable register-scoped (the variant left
-- the binding FQID), so succession is a register-level fact about the variable:
-- one edge covers every variant that delivered either side. Mirrors the 3-part
-- `variable_same_as` shape.
CREATE TABLE variable_replaced_by (
    predecessor_provider TEXT NOT NULL,
    predecessor_register TEXT NOT NULL,
    predecessor_variable TEXT NOT NULL,
    successor_provider   TEXT NOT NULL,
    successor_register   TEXT NOT NULL,
    successor_variable   TEXT NOT NULL,
    effective_year       INTEGER,
    note                 TEXT,
    beskrivning          TEXT,
    PRIMARY KEY (predecessor_provider, predecessor_register, predecessor_variable,
                 successor_provider, successor_register, successor_variable)
) WITHOUT ROWID;
-- A2.5 `.predecessors()`: the successor-keyed reverse lookup the clustered
-- predecessor-first PK can't serve.
CREATE INDEX idx_variable_replaced_by_successor
    ON variable_replaced_by(successor_provider, successor_register, successor_variable);

-- §5.6 consumer-side binding lineage (STATE grain). Materialized by
-- `link_variable_state_lineage`. One edge per (consumer_state, source_state)
-- pair whose validity ranges intersect; (valid_from, valid_to) is the
-- intersection. Source register comes from `variable.source_register_id`
-- (shared metadata, A2.1.5); source-side variable matching traverses
-- variable-grain `variable_same_as` via the build-side `_variable_set_via_same_as`
-- BFS. Replaced v0.11's per-cvid `via_source_id` edges (dropped with
-- `variable_instance` in A2.7). NOT WITHOUT ROWID:
-- both directions get explicit indexes (consumer- AND source-keyed lookups),
-- and idx_..._source is a true secondary lookup the clustered PK prefix can't
-- serve (the consumer-keyed PK prefix can't answer "what feeds source state X").
CREATE TABLE variable_state_lineage (
    consumer_state_id INTEGER NOT NULL REFERENCES variable_state(state_id),
    source_state_id   INTEGER NOT NULL REFERENCES variable_state(state_id),
    valid_from        TEXT    NOT NULL,    -- ISO 8601 'YYYY-MM-DD', inclusive start of intersection
    valid_to          TEXT    NOT NULL,    -- ISO 8601 'YYYY-MM-DD', inclusive end of intersection ('9999-12-31' for open-ended)
    PRIMARY KEY (consumer_state_id, source_state_id)
);
CREATE INDEX idx_variable_state_lineage_consumer ON variable_state_lineage(consumer_state_id);
CREATE INDEX idx_variable_state_lineage_source ON variable_state_lineage(source_state_id);

CREATE TABLE variable_state_lineage_warning (
    consumer_state_id INTEGER NOT NULL REFERENCES variable_state(state_id),
    warning_kind      TEXT    NOT NULL,    -- 'no_source_state', 'ambiguous_source_variant'
    message           TEXT    NOT NULL,
    PRIMARY KEY (consumer_state_id, warning_kind)
);
CREATE INDEX idx_variable_state_lineage_warning_consumer ON variable_state_lineage_warning(consumer_state_id);

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
    # A2.7: cvid-grained alias staging; re-parented onto the shipped
    # `variable_alias` (variable_id-keyed) before ship by `_reparent_variable_alias`.
    conn.executemany(
        "INSERT INTO variable_alias_build (cvid, delivery_column_name) VALUES (?, ?)",
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

    counts = {
        "register": len(registers),
        "register_variant": len(variants),
        "register_version": len(versions),
        "variable": len(variables),
        "variable_instance": len(instances),
        "variable_alias": len(aliases),
        "population": len(populations),
        "object_type": len(object_types),
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
    `variable_instance × variable_alias_build`: the `(register_id, register_variant_id,
    kolumnnamn)` triple narrows to a single cvid in the source CSV, and
    `variable_instance.var_id` then carries the `var_id` we need. We also
    join `variable` on `variabelnamn` (now `variable.name` post-A1.1) so
    that when the same `kolumnnamn` is reused across distinct variables
    under one variant (rename / id split mid-variant), each `unika_summary`
    row maps to exactly one `var_id` instead of fanning sensitivity flags
    sideways onto siblings. Joining `variable_alias_build` (the cvid-grained
    build staging table, A2.7) on the full `(cvid, delivery_column_name)` PK
    rather than `delivery_column_name` alone also lets SQLite use the PK index
    instead of falling back to a scan / auto-index.
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
        # that table because A2.1 drops it. `variable_alias_build` and
        # `variable` were renamed: `kolumnnamn` → `delivery_column_name`,
        # `variabelnamn` → `name`.
        "    JOIN variable_alias_build va "
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


@dataclass
class _StateGroup:
    """One pre-triage coalesced state group (lifted to module scope so the
    §5.7 triage below can read it). The 8-component group key lives in the
    `groups` dict; the accumulator carries the year-range signals plus the
    latest-era delivery column and classification for triage."""

    register_id: int
    register_variant_id: int
    var_id: int
    data_type: str | None
    data_length: str | None
    value_set_id: int | None
    value_set_version_label: str | None
    # grain is part of the *group key* (gkey position 7), not stored here.
    # classification_id: first non-null seen for the group — the §5.7 fold
    # primary signal (same classification family → fold). Correlates with
    # value_set_id (in the key), so it's stable within a group.
    classification_id: int | None = None
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
    # Sticky bit: True iff at least one matching unika row left `VersionSista`
    # blank. The "still active" signal must survive even when OTHER unika rows
    # for the same group carry a bounded VersionSista (mid-life rename: bounded
    # OriginalName row + open RenamedName row) — without this, `unika_max` from
    # the bounded row would mask the open-ended signal.
    unika_has_open_top: bool = False
    regver_min: int | None = None
    regver_max: int | None = None
    # Latest-era alias: highest regver_id, ties broken by lexically smallest
    # delivery_column_name. regver_id alone orders alias selection; the row's
    # year only updates regver_min/max.
    latest_alias: str | None = None
    latest_alias_regver: int | None = None
    # The set of register_version (edition) ids this group was observed in. The
    # §5.7 contested-column gate buckets by *edition*, not calendar year, so a
    # sub-annual variant (e.g. one with both HT2018 and VT2018) doesn't treat a
    # term-to-term column rename as a same-year co-delivery (Codex #139).
    regvers: set[int] = field(default_factory=set)


# ─────────────────────────────────────────────────────────────────────────
# §5.7 build-time triage
# ─────────────────────────────────────────────────────────────────────────
# The coalescer groups variable_instance rows into pre-triage states (one per
# shape/grain/column 8-tuple). A single source `var_id` can carry several
# states that collide on the universal invariant key
# (variable_id, register_variant_id, valid_from, value_set_version_label).
# Triage resolves every such collision three ways (§5.7) so the uniqueness
# index below can be created:
#   FOLD     — same concept in different *representations* (classification
#              vintage, SUN/SSYK grain, coding variant): keep ONE variable;
#              give each colliding state a distinct value_set_version_label
#              token; the variable slug derives from the shared column stem.
#   SPLIT    — genuinely different concepts under a generic var_id (disjoint
#              column stems): mint distinct sibling `variable` rows (sharing the
#              source provider_key), reassign each column's states to its
#              sibling, link siblings with variable_related_to edges.
#   COLLAPSE — residual same-column metadata drift (data_type / value_set_id
#              re-delivery churn that survived grouping): keep the latest-era
#              state, drop the rest.
# `Variabelnamn` is a generic family label and is never the fold/split signal
# (§5.7): the classification family (then the column stem) carries the concept
# boundary. The discriminator that routes a column to its sibling is build-time
# in-memory only (the per-column assignment here) — never a shipped table.

# §5.7 rule 2 grain patterns → fold label token (the token discriminates the
# folded states of one variable; it does NOT suffix a slug, post-redesign).
_NIVA_POSITION_RE = re.compile(r"\b(\d+)\s*position(er)?\b", re.IGNORECASE)
_NIVA_NIVAOLD_RE = re.compile(r"\bnivaold\b", re.IGNORECASE)
_NIVA_GROV_RE = re.compile(r"\bgrov(?:\s+gruppering)?\b", re.IGNORECASE)
_NIVA_DETALJ_RE = re.compile(r"\bdetalj(?:grupp(er)?)?\b", re.IGNORECASE)
_NIVA_ALFA_RE = re.compile(r"\b(alfa|alpha)\b", re.IGNORECASE)
_NIVA_HUVUDGRUPP_RE = re.compile(r"\bhuvudgrupp\b", re.IGNORECASE)
_NIVA_AVDELNING_RE = re.compile(r"\bavdelning\b", re.IGNORECASE)
_NIVA_UNDERGRUPP_RE = re.compile(r"\bundergrupp\b", re.IGNORECASE)

# A fold needs ≥ this many shared leading chars to treat differing columns as
# one stem + representation axis. Below it the columns are disjoint → split.
# Tuned against real SCB columns during build-db validation; a TOML override
# adjudicates the fuzzy boundary (§5.7 curation backlog).
_FOLD_MIN_STEM = 3


def _fold_token_from_grain(grain: str | None) -> str | None:
    """Map an SCB `vardemangdsniva` grain string to a fold label token
    (e.g. ``3pos``, ``grov``). None when no grain pattern matches — the caller
    falls back to the column-suffix token."""
    if not grain:
        return None
    m = _NIVA_POSITION_RE.search(grain)
    if m is not None:
        return f"{m.group(1)}pos"
    for rx, token in (
        (_NIVA_NIVAOLD_RE, "old"),
        (_NIVA_GROV_RE, "grov"),
        (_NIVA_DETALJ_RE, "detalj"),
        (_NIVA_ALFA_RE, "alfa"),
        (_NIVA_HUVUDGRUPP_RE, "huvud"),
        (_NIVA_AVDELNING_RE, "avd"),
        (_NIVA_UNDERGRUPP_RE, "under"),
    ):
        if rx.search(grain):
            return token
    return None


def _ascii_fold_lower(s: str | None) -> str:
    if not s:
        return ""
    return (
        unicodedata.normalize("NFKD", s)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )


def _common_prefix_len(strings: list[str]) -> int:
    """Longest shared leading-character run across the (already folded) strings."""
    if not strings:
        return 0
    shortest = min(len(s) for s in strings)
    n = 0
    for i in range(shortest):
        ch = strings[0][i]
        if all(s[i] == ch for s in strings):
            n += 1
        else:
            break
    return n


def _group_from_year(grp: _StateGroup) -> int | None:
    """The group's lower-bound year — observed `regver_min`, else `unika_min`.
    Mirrors the materializer's `from_year`, so `_collapse_residual` buckets on
    the exact `valid_from` the uniqueness index keys on."""
    return grp.regver_min if grp.regver_min is not None else grp.unika_min


def _classification_roots(conn: sqlite3.Connection) -> dict[int, int]:
    """Map each ``classification.id`` to its family-root id, following the
    ``supersedes_id`` chain. Two columns are the same classification family
    (the §5.7 fold *primary* signal) iff their classification_ids share a root
    — e.g. SNI 2002 supersedes SNI 92 supersedes SNI 69, so all three resolve
    to the SNI-69 root and fold.

    INERT TODAY (documented follow-up, MIGRATION_PLAN A2.2): triage runs inside
    the coalescer, *before* `populate_classifications`, so the `classification`
    table is empty here → this returns ``{}`` and `_decide_fold_or_split` falls
    to the column-stem signal. Stem-folding covers every §5.7 fold example
    (`FtgSni69`/`FtgSni92`, `Ssyk3`/`Ssyk5`, `BCIV`/`BCIVRED` all share a stem),
    so the primary signal only matters for same-family columns with *disjoint*
    stems. Activating it = moving triage to a post-classifications materialize
    step (keeps the coalescer's in-memory grain, which the final schema drops)."""
    parent = {
        r[0]: r[1] for r in conn.execute("SELECT id, supersedes_id FROM classification")
    }

    def root(cid: int) -> int:
        seen: set[int] = set()
        while parent.get(cid) is not None and cid not in seen:
            seen.add(cid)
            cid = parent[cid]
        return cid

    return {cid: root(cid) for cid in parent}


@dataclass
class _TriageResult:
    # gkey → variable_id the state materializes under (siblings for splits).
    # None when the parent variable is missing (seeded via vid_map.get); the
    # coalescer's materializer raises a clear error on that invariant break.
    assignments: dict[tuple, int | None]
    # gkey → value_set_version_label override (fold tokens).
    labels: dict[tuple, str]
    # gkeys collapsed into a sibling and not materialized.
    dropped: set[tuple]
    # variable_id → shared-stem slug base for folded variables (consumed by
    # populate_variable_slugs; a single-column variable is absent → derives
    # from its column as usual).
    fold_slug_hints: dict[int, str]
    # (variable_id_a, variable_id_b, relation_kind) sibling edges (both
    # directions emitted at materialization, after slugs exist).
    related_edges: list[tuple[int, int, str]]
    stats: Counter


# Column-suffix tokens that mark a *representation* axis (coding variant /
# grain), not a different concept — so a shared stem + one of these suffixes
# folds. Pure-digit suffixes (SSYK grain `3`/`5`, SNI vintage `69`/`92`) and
# the empty suffix (the base column itself, `BCIV` vs `BCIVRED`) also count.
# Modest by design; the fuzzy boundary is the §5.7 curation backlog.
_REP_SUFFIX_TOKENS = frozenset(
    {"red", "old", "ny", "grov", "detalj", "alfa", "alpha", "huvud", "avd", "under"}
)


def _is_representation_suffix(suffix: str) -> bool:
    """True when a column's suffix-past-the-shared-stem is a representation
    axis (empty base, pure digits, or a known coding/grain token) rather than a
    distinct-concept word (`hem` / `skol`)."""
    s = suffix.strip("-_").lower()
    return not s or s.isdigit() or s in _REP_SUFFIX_TOKENS


def _decide_fold_or_split(folded_cols: list[str], class_roots_present: set[int]) -> str:
    """Decide *fold* vs *split* for one source var_id delivered under ≥2
    distinct columns (§5.7 rule 3). Classification family is the primary
    signal; column stem + representation suffix is the fallback.
    `class_roots_present` is the set of classification family-roots across the
    columns (empty when unclassified)."""
    # Primary: all classified columns belong to one classification family →
    # versions of the same classification → fold.
    if class_roots_present and len(class_roots_present) == 1:
        return "fold"
    # Fallback: shared stem AND every column's differing suffix is a
    # representation token → fold (`Ssyk3`/`Ssyk5`, `BCIV`/`BCIVRED`). A
    # concept-word suffix (`Hemkommun`/`Skolkommun`) or no shared stem → split.
    # (Mixed/multiple classification families also fall here → split.)
    if len(class_roots_present) <= 1:
        prefix = _common_prefix_len(folded_cols)
        if prefix >= _FOLD_MIN_STEM and all(
            _is_representation_suffix(c[prefix:]) for c in folded_cols
        ):
            return "fold"
    return "split"


def _triage_groups(
    conn: sqlite3.Connection,
    groups: dict[tuple, _StateGroup],
    vid_map: dict[tuple[int, int], int],
) -> _TriageResult:
    """Resolve pre-triage state collisions per §5.7. Mutates the DB (mints
    split-sibling `variable` rows) and returns the per-gkey routing the
    coalescer applies when it materializes `variable_state`."""
    class_roots = _classification_roots(conn)
    res = _TriageResult({}, {}, set(), {}, [], Counter())

    by_var: dict[tuple[int, int], list[tuple]] = defaultdict(list)
    for gkey, grp in groups.items():
        # .get → None for a group whose parent variable is missing; the
        # coalescer's materializer raises a clear error on that invariant break.
        res.assignments[gkey] = vid_map.get((grp.register_id, grp.var_id))
        by_var[(grp.register_id, grp.var_id)].append(gkey)

    for (register_id, var_id), gkeys in by_var.items():
        if len(gkeys) <= 1:
            continue
        orig_vid = vid_map.get((register_id, var_id))
        if orig_vid is None:
            continue

        # §5.7 triage acts ONLY on a genuine same-(variant, year) collision
        # between distinct columns. A `var_id` whose columns never co-occur in
        # one (register_variant_id, valid_from-year) bucket is a single
        # longitudinal variable — e.g. SCB renamed the delivery column between
        # editions — and triaging it would shard the variable's history across
        # siblings (Codex P1 #139). The *gate* is the same-year collision: a
        # var_id with <2 contested columns isn't a split container and is skipped.
        # But once it IS a container, EVERY distinct column-component becomes its
        # own variable (split) or folds with the contested set — a non-contested
        # column must NOT keep the seeded original assignment, or a later/renamed
        # split column's history is mis-attributed to the lex-first sibling
        # (Codex P2 #139).
        col_of = {gk: gk[8] for gk in gkeys}  # column component (gkey index 8)
        # Bucket each group by the EDITIONS (register_version ids) it was
        # delivered in — not the calendar year. Two columns co-deliver iff they
        # share an edition: this both catches a multi-edition span (`Hemkommun`
        # 2018-2019 + `Skolkommun` 2019 share the 2019 edition → contested) AND
        # avoids treating a term-to-term rename in a sub-annual variant
        # (`HT2018` → `VT2018`, distinct editions, same year) as a co-delivery
        # (Codex #139).
        bucket_cols: dict[tuple[int, int], set[str]] = defaultdict(set)
        for gk in gkeys:
            grp = groups[gk]
            for regver in grp.regvers:
                bucket_cols[(grp.register_variant_id, regver)].add(col_of[gk])
        contested: set[str] = set()
        for cols in bucket_cols.values():
            if len(cols) > 1:
                contested |= cols
        contested.discard("")  # empty-column stubs aren't fold/split contestants
        if len(contested) < 2:
            continue  # no real multi-column same-year collision → not a container

        by_col: dict[str, list[tuple]] = defaultdict(list)
        for gk in gkeys:
            if col_of[gk]:
                by_col[col_of[gk]].append(gk)
        all_cols = sorted(by_col)
        contested_cols = sorted(contested)
        non_contested_cols = [c for c in all_cols if c not in contested]

        # The fold/split DECISION is made on the *contested* columns only (the
        # real collision); the assignment then covers all columns.
        folded = [_ascii_fold_lower(c) for c in contested_cols]
        roots = {
            class_roots[grp.classification_id]
            for c in contested_cols
            for gk in by_col[c]
            if (grp := groups[gk]).classification_id is not None
            and grp.classification_id in class_roots
        }
        if _decide_fold_or_split(folded, roots) == "fold":
            # Contested columns fold into the original variable; each
            # non-contested column splits off into its own.
            _apply_fold(groups, by_col, contested_cols, folded, orig_vid, res)
            _split_off_non_contested(
                conn,
                groups,
                by_col,
                non_contested_cols,
                register_id,
                var_id,
                orig_vid,
                res,
            )
            res.stats["folds"] += 1
        else:
            # Split: every distinct column-component becomes its own variable.
            _apply_split(
                conn, groups, by_col, all_cols, register_id, var_id, orig_vid, res
            )
            res.stats["splits"] += 1

    # Universal residual collapse: after fold/split assigned variable_ids and
    # fold labels, guarantee the §5.1 state-uniqueness invariant holds by making
    # every (variable_id, register_variant_id, valid_from-year) scope carry
    # distinct labels. Catches single-column shape drift AND split-sibling
    # within-column drift (a split sibling can still carry same-year drift that
    # _apply_split alone wouldn't resolve).
    _collapse_residual(groups, res)
    return res


def _collapse_residual(groups: dict[tuple, _StateGroup], res: _TriageResult) -> None:
    """§5.7 rule 4 — final collision resolution. Every materializing group is
    scoped by its FINAL uniqueness coordinate (assigned variable_id,
    register_variant_id, valid_from-year); within a scope, each surviving group
    must carry a distinct `value_set_version_label` or the unique index fails.

    Per group, the preferred label is its fold label (if triage set one), else a
    grain token, else its own value_set_version_label. Processing latest-era
    first, a label that's free is kept; a *meaningful* label already taken is
    disambiguated (`-N`); an empty/uninformative collision is pure shape/value
    drift and the group is dropped. This preserves multi-vintage (distinct
    labels) and multi-grain (distinct grain tokens) while collapsing drift —
    and, running after fold/split, also resolves split-sibling within-column
    drift.

    INTERIM scope limit: groups are scoped by `valid_from`-year (the index key).
    Two same-label groups under one variable with *different* lower bounds but a
    shared edition — e.g. a 2018-2019 stub and a 2019+ shape — land in separate
    scopes, so this overlapping-but-not-index-colliding pair isn't reconciled
    here. The unique index still holds (distinct `valid_from`); the interim point
    resolver picks the later state at the overlap year (sensible supersession)
    and A2.5 `resolve_at` surfaces both. A full fix range-clamps the older
    overlap (not a whole-group drop, which would lose its non-overlap coverage),
    so it belongs with the A2.5 state-model rework. Triage *detection* already
    buckets by edition (`regver_id`); only this residual pass stays year-scoped."""
    scopes: dict[tuple, list[tuple]] = defaultdict(list)
    for gkey, grp in groups.items():
        if gkey in res.dropped:
            continue
        vid = res.assignments.get(gkey)
        if vid is None:
            continue
        scopes[(vid, grp.register_variant_id, _group_from_year(grp))].append(gkey)

    for scope_gkeys in scopes.values():
        if len(scope_gkeys) <= 1:
            continue
        # Latest era first so it keeps the cleanest label; deterministic ties.
        # The gkey tiebreaker is stringified per element — a raw gkey carries
        # `value_set_id` (int | None), and a None-vs-int compare across two
        # groups in the scope would raise TypeError.
        ordered = sorted(
            scope_gkeys,
            key=lambda gk: (
                -(groups[gk].regver_max or -1),
                tuple("" if x is None else str(x) for x in gk),
            ),
        )
        used: set[str] = set()
        for gk in ordered:
            grp = groups[gk]
            preferred = (
                res.labels.get(gk)
                or _fold_token_from_grain(gk[7])
                or (grp.value_set_version_label or "")
            )
            if preferred and preferred not in used:
                used.add(preferred)
                res.labels[gk] = preferred
            elif preferred:  # meaningful token already taken → disambiguate
                n = 1
                while f"{preferred}-{n}" in used:
                    n += 1
                used.add(f"{preferred}-{n}")
                res.labels[gk] = f"{preferred}-{n}"
            elif "" not in used:  # first uninformative state keeps ''
                used.add("")
                res.labels[gk] = ""
            else:  # uninformative drift that can't be distinguished → drop
                res.dropped.add(gk)


def _apply_fold(
    groups: dict[tuple, _StateGroup],
    by_col: dict[str, list[tuple]],
    named_cols: list[str],
    folded: list[str],
    orig_vid: int,
    res: _TriageResult,
) -> None:
    """FOLD: keep ONE variable; its states stay overlapping, discriminated by
    `value_set_version_label`. A state with a grain token or a non-empty source
    label already has the right discriminator and is **preserved verbatim** by
    `_collapse_residual` (slugifying a meaningful source version like
    `Fabrikat personbilar 2019` into an opaque token would destroy the
    version semantics the resolver needs). Only a state with NEITHER gets a
    column-suffix token here, so genuinely-different columns folded together
    (`Ssyk3`/`Ssyk5`, empty source label) don't collide. `_collapse_residual`
    handles any residual same-label collision. The slug derives from the stem."""
    stem_len = _common_prefix_len(folded)
    for col in named_cols:
        fcol = _ascii_fold_lower(col)
        suffix = re.sub(r"[^a-z0-9]+", "-", fcol[stem_len:]).strip("-") or re.sub(
            r"[^a-z0-9]+", "-", fcol
        ).strip("-")
        for gk in by_col[col]:
            grp = groups[gk]
            if not _fold_token_from_grain(gk[7]) and not (
                grp.value_set_version_label or ""
            ):
                res.labels[gk] = suffix
    # Fold slug hint: the shared stem. Validate through derive_variable_slug so
    # a digit-leading / all-digit / reserved stem (`2501`/`2502` → `250`) is
    # rejected (returns None) and populate_variable_slugs falls back to its
    # name/provider_key chain instead of emitting a slug that fails the §5.2
    # grammar. (Split siblings already route through derive_variable_slug; only
    # this fold-stem path bypassed it.)
    stem_raw = _ascii_fold_lower(named_cols[0])[:stem_len].strip("-_")
    hint = derive_variable_slug(stem_raw) or derive_variable_slug(named_cols[0])
    if hint:
        res.fold_slug_hints[orig_vid] = hint


def _apply_split(
    conn: sqlite3.Connection,
    groups: dict[tuple, _StateGroup],
    by_col: dict[str, list[tuple]],
    named_cols: list[str],
    register_id: int,
    var_id: int,
    orig_vid: int,
    res: _TriageResult,
) -> None:
    """SPLIT: each distinct column becomes its own sibling `variable` (sharing
    the source provider_key); link siblings with variable_related_to edges.
    Sibling slugs derive later from each sibling's own reassigned column."""
    # First column (lexically) keeps the original variable; the rest mint new
    # sibling variables. Name is the shared generic Variabelnamn (variable.name
    # is already populated; reuse it for the siblings).
    name_row = conn.execute(
        "SELECT name FROM variable WHERE variable_id = ?", (orig_vid,)
    ).fetchone()
    shared_name = name_row[0] if name_row else None
    sibling_vids = [orig_vid]
    for col in named_cols[1:]:
        cur = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (?, CAST(? AS TEXT), ?)",
            (register_id, var_id, shared_name),
        )
        new_vid = cur.lastrowid
        assert new_vid is not None  # lastrowid is set after an INSERT
        sibling_vids.append(new_vid)
        for gk in by_col[col]:
            res.assignments[gk] = new_vid
    # (N choose 2) edges, both directions, between all siblings.
    # TODO(§5.5): every split currently emits `same_definition_different_column`.
    # §5.5/§5.7 differentiate the split relation_kind — `code_vs_label_pair`
    # (`Lid`/`LNamn` — order the code OR the name) and `import_bug_suspect` — but
    # detecting those needs the code/label-pair + datatype heuristics (see the
    # old #132 `_looks_like_code_label_pair`). Flattening to the generic kind is
    # interim (it's in the allowed set and correctly never the fold-only
    # `same_concept_different_grain`); refine when the split heuristics land.
    for i, a in enumerate(sibling_vids):
        for b in sibling_vids[i + 1 :]:
            res.related_edges.append((a, b, "same_definition_different_column"))


def _split_off_non_contested(
    conn: sqlite3.Connection,
    groups: dict[tuple, _StateGroup],
    by_col: dict[str, list[tuple]],
    non_contested_cols: list[str],
    register_id: int,
    var_id: int,
    orig_vid: int,
    res: _TriageResult,
) -> None:
    """Once a var_id is a split container, a column-component that doesn't itself
    collide same-year (a later or renamed column) must NOT keep the seeded
    original assignment — that lumps its history onto the lex-first sibling and
    a consumer querying that sibling's slug gets the wrong data (Codex P2 #139).
    Give each such column its own variable, linked to `orig_vid` by a
    variable_related_to edge. Perfect rename *continuity* (re-joining a renamed
    column to its sibling) needs A2.3 tracking; own-variable avoids the
    mis-attribution."""
    if not non_contested_cols:
        return
    name_row = conn.execute(
        "SELECT name FROM variable WHERE variable_id = ?", (orig_vid,)
    ).fetchone()
    shared_name = name_row[0] if name_row else None
    vids = [orig_vid]
    for col in non_contested_cols:
        cur = conn.execute(
            "INSERT INTO variable (register_id, provider_key, name) "
            "VALUES (?, CAST(? AS TEXT), ?)",
            (register_id, var_id, shared_name),
        )
        nvid = cur.lastrowid
        assert nvid is not None
        vids.append(nvid)
        for gk in by_col[col]:
            res.assignments[gk] = nvid
    for i, a in enumerate(vids):
        for b in vids[i + 1 :]:
            res.related_edges.append((a, b, "same_definition_different_column"))


def _materialize_variable_related_to(
    conn: sqlite3.Connection, edges: list[tuple[int, int, str]]
) -> int:
    """Insert §5.7 split-sibling edges (both directions) into
    `variable_related_to`. Runs after `populate_variable_slugs` so each
    `variable_id` resolves to its (provider, register, variable) slug FQID.
    Returns the row count inserted."""
    if not edges:
        return 0
    fqid_of = {
        r[0]: (r[1], r[2], r[3])
        for r in conn.execute(
            "SELECT v.variable_id, p.slug, r.slug, v.slug "
            "FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id"
        )
    }
    rows: list[tuple] = []
    for a, b, kind in edges:
        fa, fb = fqid_of.get(a), fqid_of.get(b)
        # A sibling whose slug never populated (skip_slugs build) can't form an
        # FQID-keyed edge; skip rather than insert a NULL endpoint.
        if fa is None or fb is None or None in fa or None in fb:
            continue
        rows.append((*fa, *fb, kind, "auto:triage"))
        rows.append((*fb, *fa, kind, "auto:triage"))
    conn.executemany(
        "INSERT OR IGNORE INTO variable_related_to "
        "(a_provider, a_register, a_variable, b_provider, b_register, b_variable, "
        " relation_kind, note) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    return len(rows)


def _coalesce_variable_states(conn: sqlite3.Connection) -> dict[str, Any]:
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
       cvids in the group via `variable_alias_build × variable`. Note the cross-
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
    # loop join across `variable_instance × variable_alias_build × variable
    # × register_version` is faster as a single sweep than re-issued
    # per-group queries. Memory is bounded — each row is small.
    #
    # `variable_alias_build` (cvid-grained build staging, A2.7) is LEFT JOINed:
    # a cvid with no alias row (rare but observed for cvids that only carry a
    # `variabelnamn` and no `kolumnnamn` in the raw CSV) still surfaces so the
    # group is captured with a NULL delivery_column_name instead of being
    # dropped silently.
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
        "       vi.classification_id, "
        "       vi.variabelnamn, va.delivery_column_name, "
        "       rv.registerversionnamn "
        "FROM variable_instance vi "
        "LEFT JOIN variable_alias_build va ON va.cvid = vi.cvid "
        "JOIN register_version rv ON rv.regver_id = vi.regver_id"
    ).fetchall()

    # §5.7 rule 2 — kolumnnamn connectivity per (register, variant, var_id).
    # Two delivery columns are one concept-candidate iff some cvid carries both
    # as aliases (set *intersection*); union them. The component representative
    # (lex-smallest column) enters the group key below, so the coalescer keeps
    # genuinely-disjoint columns (a split candidate) as distinct pre-triage
    # groups, while a single cvid's diacritic aliases (`Kon`/`Kön`) stay one
    # group. Columns that never co-occur form separate components.
    _ColNode = tuple[int, int, int, str]
    col_parent: dict[_ColNode, _ColNode] = {}

    def _col_find(node: _ColNode) -> _ColNode:
        root = node
        while col_parent[root] != root:
            root = col_parent[root]
        while col_parent[node] != root:  # path-compress
            col_parent[node], node = root, col_parent[node]
        return root

    def _col_union(a: _ColNode, b: _ColNode) -> None:
        ra, rb = _col_find(a), _col_find(b)
        if ra != rb:
            lo, hi = (ra, rb) if ra <= rb else (rb, ra)  # lex-smallest is root
            col_parent[hi] = lo

    cvid_anchor: dict[int, _ColNode] = {}
    for row in rows:
        col = row["delivery_column_name"]
        if not col:
            continue
        node = (row["register_id"], row["register_variant_id"], row["var_id"], col)
        col_parent.setdefault(node, node)
        anchor = cvid_anchor.get(row["cvid"])
        if anchor is None:
            cvid_anchor[row["cvid"]] = node
        else:
            _col_union(anchor, node)

    # Group accumulator: key → mutable `_StateGroup` (module scope; the §5.7
    # triage reads it). We iterate `rows` once and update the year-range
    # signals / latest alias in place. A dict rather than itertools.groupby
    # because rows aren't pre-sorted and the key has 9 components (the trailing
    # one is the column component above; grain stays at index 7).
    groups: dict[
        tuple[int, int, int, str, str, int | None, str | None, str, str],
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
        col = row["delivery_column_name"]
        # Column component (§5.7 rule 2): disjoint columns get distinct
        # components → distinct groups → triage can fold/split them. A cvid
        # with no alias contributes the "" component (a stub group).
        component = (
            _col_find(
                (row["register_id"], row["register_variant_id"], row["var_id"], col)
            )[3]
            if col
            else ""
        )
        gkey = (
            row["register_id"],
            row["register_variant_id"],
            row["var_id"],
            row["data_type"] or "",
            row["data_length"] or "",
            row["value_set_id"],
            row["value_set_version_label"] or "",
            grain,
            component,
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
                classification_id=row["classification_id"],
            )
            groups[gkey] = grp
        elif grp.classification_id is None and row["classification_id"] is not None:
            # First non-null classification across the group's cvids — the §5.7
            # fold primary signal.
            grp.classification_id = row["classification_id"]

        # Editions this group was delivered in — the §5.7 contested gate buckets
        # by edition, not year (regver_id is NOT NULL on variable_instance).
        grp.regvers.add(row["regver_id"])

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

    # §5.7 triage: resolve pre-triage collisions (fold/split/collapse) before
    # materializing. Mints split-sibling `variable` rows (so vid_map above is
    # stale for them — triage.assignments carries the per-gkey target) and
    # routes each group to its variable_id + (folded) value_set_version_label.
    triage = _triage_groups(conn, groups, vid_map)

    batch: list[tuple] = []
    sentinel_count = 0
    fallback_only_count = 0
    open_top_from_unika = 0
    for gkey, grp in groups.items():
        if gkey in triage.dropped:
            continue  # collapsed into a sibling state (§5.7 rule 4 drift)
        vkey = (grp.register_id, grp.register_variant_id, grp.var_id)
        var_max = var_max_regver.get(vkey)
        # `None == None` is True — a yearless single-group variable counts
        # as the latest era of itself, so the open-ended sentinel can
        # still apply there.
        is_latest_era = grp.regver_max == var_max

        # Lower bound: regver is authoritative (the years we actually
        # observed the group). Unika is fallback for yearless cvids. Shared
        # with triage's collision bucketing via _group_from_year.
        from_year = _group_from_year(grp)

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

        variable_id = triage.assignments.get(gkey)
        if variable_id is None:
            # Defensive: `variable` and `variable_instance` derive from the same
            # source rows, so every coalesced state has a parent variable (triage
            # seeds assignments from vid_map). Surface an actionable error
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
                # Fold token from triage wins; else the group's own label
                # (NOT NULL DEFAULT '' — coalesce NULL→'' so the uniqueness
                # index bites for the common single-version case).
                triage.labels.get(gkey, grp.value_set_version_label or ""),
            )
        )

    conn.executemany(
        "INSERT INTO variable_state (variable_id, register_variant_id, "
        "    valid_from, valid_to, data_type, data_length, delivery_column_name, "
        "    value_set_id, value_set_version_label) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        batch,
    )

    # §5.1 state-uniqueness index (deferred from A2.1.5 — see the DDL note on
    # variable_state). Created here, after triage has folded/split/collapsed
    # every same-year multi-shape collision, so the 4-tuple is now unique. A
    # CREATE that raises here means triage left a residual collision — a build
    # bug, surfaced loudly rather than shipped.
    conn.execute(
        "CREATE UNIQUE INDEX idx_variable_state_unique ON variable_state("
        "variable_id, register_variant_id, valid_from, value_set_version_label)"
    )

    state_count = conn.execute("SELECT COUNT(*) FROM variable_state").fetchone()[0]
    _progress(
        f"  {state_count:,} variable_state rows "
        f"({fallback_only_count:,} from register_version fallback, "
        f"{open_top_from_unika:,} open-ended from unika, "
        f"{sentinel_count:,} carry a date sentinel)"
    )
    _progress(
        f"  triage: {triage.stats.get('folds', 0):,} folds, "
        f"{triage.stats.get('splits', 0):,} splits, "
        f"{len(triage.dropped):,} states collapsed"
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
        "n_triage_folds": triage.stats.get("folds", 0),
        "n_triage_splits": triage.stats.get("splits", 0),
        "n_triage_collapsed": len(triage.dropped),
        # Build-only routing consumed downstream by build_db (NOT manifest
        # values): fold-slug hints → populate_variable_slugs; sibling edges →
        # _materialize_variable_related_to (after slugs exist).
        "_fold_slug_hints": triage.fold_slug_hints,
        "_related_edges": triage.related_edges,
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


# A2.3: SCB ships succession in `timeseries_event` under two handelse values.
# `Ersatt av` is the canonical direction (id1 was replaced by id2); `Ersätter`
# is the inverse (id1 replaces id2). SCB usually emits both rows for a single
# transition, so the materializer collapses them onto one predecessor →
# successor edge.
_REPLACED_BY_HANDELSE = frozenset({"Ersatt av", "Ersätter"})
# Four entity grains. `Register` / `RegisterVariant` land in their own tables;
# `AktuellVariabel` (cvid) and `Variabel` (var_id) both resolve to the variable
# grain and land in `variable_replaced_by`.
_REPLACED_BY_ENTITET = frozenset(
    {"Register", "RegisterVariant", "AktuellVariabel", "Variabel"}
)
# Source-of-truth marker for the auto-derive path. Distinguishes from future
# TOML-curated rows (A4 — cross-provider succession not visible in SCB's
# `timeseries_event`).
_REPLACED_BY_NOTE_AUTO = "auto:timeseries_event"

# Manifest stat keys for replaced_by materialization. Single source so the
# `skip_slugs` zero-fill (in `build_db`) and the materializer's real return
# can't drift apart — `test_replaced_by_stats_in_manifest` pins the exact set.
_REPLACED_BY_STAT_KEYS = (
    "n_timeseries_event_rows_scanned",
    "n_register_replaced_by",
    "n_variant_replaced_by",
    "n_variable_replaced_by",
    "n_skipped_unresolved",
    # A2.2 triage split where the source key can't pick a sibling: a bare
    # `Variabel` var_id always, or an `AktuellVariabel` cvid whose delivery
    # column doesn't uniquely match one sibling. Always 0 pre-A2.2.
    "n_skipped_ambiguous_variable",
    # Genuine `Ersätter` rows collapsing onto an already-emitted edge — the
    # expected SCB paired-row case (see `_classify_duplicate`).
    "n_skipped_collapsed_inverse",
    # `Ersatt av` rows duplicating an already-emitted edge (repeated source
    # rows), kept distinct from the inverse-collapse count.
    "n_skipped_duplicate",
)


def _empty_replaced_by_stats() -> dict[str, int]:
    """Zeroed replaced_by stats. The `skip_slugs` build path returns this as-is;
    the materializer fills it in, so both share one key set."""
    return dict.fromkeys(_REPLACED_BY_STAT_KEYS, 0)


def _materialize_replaced_by_edges(conn: sqlite3.Connection) -> dict[str, int]:
    """Materialize §5.5 succession edges from `timeseries_event` (A2.3).

    Scans `timeseries_event` for `handelse IN ('Ersatt av', 'Ersätter')` on
    `entitet IN ('Register', 'RegisterVariant', 'AktuellVariabel', 'Variabel')`,
    resolves each row to slug-anchored (predecessor, successor) endpoints, and
    inserts into the matching `*_replaced_by` table.

    Direction rule:
      - `Ersatt av`: predecessor = id1, successor = id2
      - `Ersätter`:  predecessor = id2, successor = id1 (inverse)

    Both directions collapse to the same slug-PK so SCB's redundant paired rows
    produce a single edge.

    All three grains read STORED slug columns — `register.slug`,
    `register_variant.slug`, and (A2.1.5 §5.3) `variable.slug`. The variable
    grain is 3-part `(provider, register, variable)`: the two-level model put
    the variant out of the binding, so a cvid / var_id resolves to its
    register-scoped variable, not a variant-qualified one. (This is the core of
    the A2.3 respec — the old draft derived the variable slug from
    `delivery_column_name` at build time because none was stored; A2.1.5 stores
    it, so we just read the column the resolver itself reads.)

    Skip taxonomy — every skip is best-effort (never fails the build;
    `timeseries_event` is historical data):
      - `n_skipped_unresolved`: an id doesn't resolve to a live, slugged entity
        (dropped, not imported, self-loop — raw-id OR two distinct ids resolving
        to the same variable at slug grain, empty/non-integer id, or a bare
        `Variabel` var_id that isn't register-unique so its register is
        ambiguous).
      - `n_skipped_ambiguous_variable`: a `(register_id, var_id)` source key
        maps to >1 variable — an A2.2 triage split. A bare `Variabel` id can't
        pick a sibling, so the edge is dropped (mirrors `_variable_source_slug`).
        An `AktuellVariabel` cvid *can* — split siblings own disjoint delivery
        columns, so the cvid's column (`variable_alias_build`) selects its
        sibling; it only lands here if that column is missing or doesn't match
        exactly one sibling. Always 0 pre-A2.2.
      - `n_skipped_collapsed_inverse`: an `Ersätter` row whose (predecessor,
        successor) already landed from its `Ersatt av` twin — the expected SCB
        paired-row collapse.
      - `n_skipped_duplicate`: an `Ersatt av` row duplicating an already-emitted
        edge (repeated source row), kept distinct from the inverse-collapse
        count so the inverse counter means what it says.

    #142: `beskrivning` (the human transition reason, e.g. "2001 byttes SUN96
    till SUN2000") is carried verbatim from `timeseries_event.Beskrivning` into
    every edge, alongside the `auto:timeseries_event` provenance in `note`.

    `effective_year` (#142): derived for the **AktuellVariabel** variable grain
    only — the cvid endpoints resolve to a `register_version` edition, and the
    edge takes the **successor**'s edition year (the year the succession took
    effect for the consumer, matching the `slk` SUN acceptance "2001 byttes"
    where 2001 is SUN2000's first edition). The bare `Variabel` + `Register` +
    `RegisterVariant` grains have no edition (a var_id / register / variant id
    carries no year), so they leave `effective_year` NULL — a deliberate
    asymmetry: only the cvid grain names an edition. Timeseries.csv itself
    carries no year column.

    Returns a stats dict for the manifest.

    TODO(A4): cross-provider edges (e.g. SOS→SCB) won't appear in SCB's
    timeseries_event. The slug TOML form (§5.5) carries these as inline rows;
    wire that loader in alongside the SOS adapter work.
    """
    _progress("Materializing replaced_by edges from timeseries_event...")

    # register_id -> (provider, register) slug pair.
    register_lookup: dict[int, tuple[str, str]] = {}
    for register_id, r_slug, p_slug in conn.execute(
        "SELECT r.register_id, r.slug, p.slug "
        "FROM register r JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE r.slug IS NOT NULL"
    ).fetchall():
        register_lookup[register_id] = (p_slug, r_slug)

    # register_variant_id -> (provider, register, variant) slug triple. SCB's
    # RegVarID is a globally-unique surrogate, so the id alone keys it.
    variant_lookup: dict[int, tuple[str, str, str]] = {}
    for rvid, rv_slug, r_slug, p_slug in conn.execute(
        "SELECT rv.register_variant_id, rv.slug, r.slug, p.slug "
        "FROM register_variant rv "
        "JOIN register r ON rv.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE rv.slug IS NOT NULL AND r.slug IS NOT NULL"
    ).fetchall():
        variant_lookup[rvid] = (p_slug, r_slug, rv_slug)

    # Variable grain reads the STORED `variable.slug` (A2.1.5 §5.3) — no
    # resolve-time derivation. `provider_key` is `str(var_id)`; cast it back to
    # the integer ids `timeseries_event` carries. Keyed by (register_id, var_id)
    # -> (provider, register, variable) slug triple.
    variable_lookup: dict[tuple[int, int], tuple[str, str, str]] = {}
    # var_id -> set of register_ids carrying it: a bare `Variabel` id (a
    # per-register var_id with no register context) resolves only when this is
    # a singleton.
    var_id_registers: dict[int, set[int]] = {}
    # (register_id, var_id) keys mapping to >1 variable — an A2.2 triage split
    # makes the bare key ambiguous. Skip-not-guess.
    ambiguous_variable: set[tuple[int, int]] = set()
    for register_id, provider_key, var_slug, r_slug, p_slug in conn.execute(
        "SELECT v.register_id, v.provider_key, v.slug, r.slug, p.slug "
        "FROM variable v "
        "JOIN register r ON v.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL"
    ).fetchall():
        try:
            var_id = int(provider_key)
        except (TypeError, ValueError):
            # Non-integer provider_key (e.g. a SOS merged variable name): no
            # integer `timeseries_event` id maps to it.
            continue
        key = (register_id, var_id)
        var_id_registers.setdefault(var_id, set()).add(register_id)
        if key in variable_lookup:
            ambiguous_variable.add(key)
            continue
        variable_lookup[key] = (p_slug, r_slug, var_slug)

    # cvid -> (register_id, var_id): an AktuellVariabel row carries a globally
    # unique cvid; map it to its variable's source key.
    cvid_to_var: dict[int, tuple[int, int]] = {}
    for cvid, register_id, var_id in conn.execute(
        "SELECT cvid, register_id, var_id FROM variable_instance"
    ).fetchall():
        cvid_to_var[cvid] = (register_id, var_id)

    # #142: cvid -> edition year, for `effective_year` on AktuellVariabel-grain
    # edges. A2.6 dropped `register_version.slug`, so the year comes straight
    # from `registerversionnamn` (build-time; the table is dropped before ship).
    # Yearless editions map to None → effective_year stays NULL.
    cvid_to_year: dict[int, int | None] = {
        cvid: extract_year(name or "")
        for cvid, name in conn.execute(
            "SELECT vi.cvid, rv.registerversionnamn "
            "FROM variable_instance vi "
            "JOIN register_version rv ON vi.regver_id = rv.regver_id"
        )
    }

    # Split-sibling disambiguation for the cvid grain. An A2.2 split maps one
    # (register_id, var_id) to several siblings owning DISJOINT delivery columns;
    # the bare key can't pick one, but an AktuellVariabel cvid names one instance
    # whose column (`variable_alias_build`, copied verbatim into
    # `variable_state.delivery_column_name` by the coalescer) selects its
    # sibling. Keyed (register_id, var_id, delivery_column_name) -> slug triple,
    # built ONLY for split groups so it stays small; the cvid's own column is
    # looked up on demand in the rare ambiguous branch rather than materializing
    # a column map over every cvid in the corpus.
    variable_by_column: dict[tuple[int, int, str], tuple[str, str, str]] = {}
    # The map rests on the §5.7 split invariant that siblings own DISJOINT
    # columns, so a column picks exactly one sibling. Guard it: should a column
    # ever map to two *different* siblings (a future split-heuristic regression),
    # it can no longer disambiguate — poison the key so the cvid path
    # skips-not-guesses rather than silently taking the last-scanned sibling.
    # (Same-variable repeats across editions yield the same triple, not a clash.)
    column_collisions: set[tuple[int, int, str]] = set()
    if ambiguous_variable:
        for register_id, provider_key, var_slug, r_slug, p_slug, col in conn.execute(
            "SELECT v.register_id, v.provider_key, v.slug, r.slug, p.slug, "
            "vs.delivery_column_name "
            "FROM variable v "
            "JOIN variable_state vs ON vs.variable_id = v.variable_id "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL "
            "AND vs.delivery_column_name IS NOT NULL"
        ).fetchall():
            try:
                var_id = int(provider_key)
            except (TypeError, ValueError):
                continue
            if (register_id, var_id) not in ambiguous_variable:
                continue
            ckey = (register_id, var_id, col)
            triple = (p_slug, r_slug, var_slug)
            if variable_by_column.get(ckey, triple) != triple:
                column_collisions.add(ckey)
            variable_by_column[ckey] = triple
    for ckey in column_collisions:
        del variable_by_column[ckey]

    def _resolve_variable(
        entitet: str, raw_id: int
    ) -> tuple[tuple[str, str, str] | None, str | None]:
        """Resolve a variable-grain id to its (provider, register, variable)
        slug triple. Returns (triple, None) on success, else (None, bucket)
        naming the skip counter ('unresolved' / 'ambiguous')."""
        if entitet == "AktuellVariabel":
            key = cvid_to_var.get(raw_id)
            if key is None:
                return None, "unresolved"
            if key in ambiguous_variable:
                # The bare (register_id, var_id) hit an A2.2 split, but this cvid
                # names one instance. Siblings own disjoint columns, so the
                # cvid's column (`variable_alias_build`) picks the sibling. Skip
                # only if the column is missing or doesn't match exactly one
                # sibling (then it degrades to the bare-grain ambiguity).
                register_id, var_id = key
                matches = {
                    variable_by_column[(register_id, var_id, col)]
                    for (col,) in conn.execute(
                        "SELECT delivery_column_name FROM variable_alias_build "
                        "WHERE cvid = ?",
                        (raw_id,),
                    )
                    if (register_id, var_id, col) in variable_by_column
                }
                if len(matches) == 1:
                    return next(iter(matches)), None
                return None, "ambiguous"
        else:  # 'Variabel': a bare per-register var_id — no cvid, no column
            regs = var_id_registers.get(raw_id)
            if regs is None or len(regs) != 1:
                return None, "unresolved"  # missing, or cross-register ambiguous
            key = (next(iter(regs)), raw_id)
            if key in ambiguous_variable:
                return None, "ambiguous"
        triple = variable_lookup.get(key)
        if triple is None:
            return None, "unresolved"
        return triple, None

    # Push the handelse/entitet scope reduction down to SQLite. The ORDER BY
    # is load-bearing for the inverse/duplicate split: processing the canonical
    # `Ersatt av` row before its `Ersätter` twin makes the edge land from the
    # canonical direction, so the colliding `Ersätter` is always the one
    # `_classify_duplicate` sees as the inverse collapse — never miscounted as a
    # plain duplicate just because SCB happened to emit the rows reversed. The
    # `timeseries_event_id` tiebreak keeps it fully deterministic.
    placeholders_h = ", ".join("?" * len(_REPLACED_BY_HANDELSE))
    placeholders_e = ", ".join("?" * len(_REPLACED_BY_ENTITET))
    candidate_rows = conn.execute(
        f"SELECT timeseries_event_id, handelse, entitet, id1, id2, beskrivning "  # noqa: S608 -- placeholders bound below
        f"FROM timeseries_event "
        f"WHERE handelse IN ({placeholders_h}) "
        f"AND entitet IN ({placeholders_e}) "
        f"ORDER BY CASE handelse WHEN 'Ersatt av' THEN 0 ELSE 1 END, "
        f"timeseries_event_id",
        (*_REPLACED_BY_HANDELSE, *_REPLACED_BY_ENTITET),
    ).fetchall()

    n_scanned = len(candidate_rows)
    n_register = 0
    n_variant = 0
    n_variable = 0
    n_skipped_unresolved = 0
    n_skipped_ambiguous_variable = 0
    n_skipped_collapsed_inverse = 0
    n_skipped_duplicate = 0

    # Seen-PK sets per grain are the sole dedup authority — every INSERT below
    # is plain (no OR IGNORE), because a PK only reaches it after a seen-set
    # miss. They also classify a duplicate-PK skip by `handelse`: an `Ersätter`
    # row hitting an existing edge is the expected inverse collapse; an
    # `Ersatt av` row hitting one is a plain duplicate. The canonical-first scan
    # order (above) guarantees this means what it says regardless of SCB's row
    # order.
    seen_register: set[tuple[str, str, str, str]] = set()
    seen_variant: set[tuple[str, str, str, str, str, str]] = set()
    seen_variable: set[tuple[str, str, str, str, str, str]] = set()

    def _classify_duplicate(handelse: str) -> None:
        """Bump the right skip counter for an already-seen PK."""
        nonlocal n_skipped_collapsed_inverse, n_skipped_duplicate
        if handelse == "Ersätter":
            n_skipped_collapsed_inverse += 1
        else:  # 'Ersatt av' duplicate of an already-emitted edge
            n_skipped_duplicate += 1

    for ts_event_id, handelse, entitet, id1_raw, id2_raw, besk_raw in candidate_rows:
        # #142: the human transition reason. Empty string → None (SCB ships many
        # rows with no Beskrivning); stored verbatim otherwise.
        beskrivning = besk_raw or None
        # id1/id2 are TEXT in the source CSV. Empty strings are common (one
        # direction of a pair, or rows where SCB knew only one endpoint).
        if not id1_raw or not id2_raw:
            n_skipped_unresolved += 1
            _progress(
                f"  replaced_by: skipping {entitet} row #{ts_event_id} — "
                f"empty id1/id2 (id1={id1_raw!r}, id2={id2_raw!r})"
            )
            continue
        try:
            id1 = int(id1_raw)
            id2 = int(id2_raw)
        except ValueError:
            n_skipped_unresolved += 1
            _progress(
                f"  replaced_by: skipping {entitet} row #{ts_event_id} — "
                f"non-integer id (id1={id1_raw!r}, id2={id2_raw!r})"
            )
            continue

        # Direction collapse: Ersatt av is canonical, Ersätter is its inverse.
        if handelse == "Ersatt av":
            pred_id, succ_id = id1, id2
        else:  # 'Ersätter'
            pred_id, succ_id = id2, id1

        # A self-replacement is meaningless and would self-loop the graph. SCB
        # doesn't ship these, but guard anyway (counted as unresolved — it can't
        # produce a valid edge).
        if pred_id == succ_id:
            n_skipped_unresolved += 1
            _progress(
                f"  replaced_by: skipping {entitet} row #{ts_event_id} — "
                f"self-loop (id1 == id2 == {pred_id})"
            )
            continue

        if entitet == "Register":
            pred = register_lookup.get(pred_id)
            succ = register_lookup.get(succ_id)
            if pred is None or succ is None:
                n_skipped_unresolved += 1
                _progress(
                    f"  replaced_by: skipping Register row #{ts_event_id} — "
                    f"unresolved register_id (pred={pred_id}, succ={succ_id})"
                )
                continue
            pk = (*pred, *succ)
            if pk in seen_register:
                _classify_duplicate(handelse)
                continue
            seen_register.add(pk)
            # Register grain has no edition → effective_year NULL (#142 asymmetry).
            conn.execute(
                "INSERT INTO register_replaced_by ("
                "predecessor_provider, predecessor_register, "
                "successor_provider, successor_register, "
                "effective_year, note, beskrivning) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (*pred, *succ, None, _REPLACED_BY_NOTE_AUTO, beskrivning),
            )
            n_register += 1

        elif entitet == "RegisterVariant":
            pred = variant_lookup.get(pred_id)
            succ = variant_lookup.get(succ_id)
            if pred is None or succ is None:
                n_skipped_unresolved += 1
                _progress(
                    f"  replaced_by: skipping RegisterVariant row #{ts_event_id} — "
                    f"unresolved register_variant_id (pred={pred_id}, succ={succ_id})"
                )
                continue
            pk = (*pred, *succ)
            if pk in seen_variant:
                _classify_duplicate(handelse)
                continue
            seen_variant.add(pk)
            # Variant grain has no edition → effective_year NULL (#142 asymmetry).
            conn.execute(
                "INSERT INTO variant_replaced_by ("
                "predecessor_provider, predecessor_register, predecessor_variant, "
                "successor_provider, successor_register, successor_variant, "
                "effective_year, note, beskrivning) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (*pred, *succ, None, _REPLACED_BY_NOTE_AUTO, beskrivning),
            )
            n_variant += 1

        else:  # entitet IN ('AktuellVariabel', 'Variabel') -> variable grain
            pred, pred_skip = _resolve_variable(entitet, pred_id)
            succ, succ_skip = _resolve_variable(entitet, succ_id)
            if pred is None or succ is None:
                # 'ambiguous' (an A2.2 split) is more specific than the generic
                # 'unresolved'; surface it whenever either side hit it.
                if "ambiguous" in (pred_skip, succ_skip):
                    n_skipped_ambiguous_variable += 1
                    _progress(
                        f"  replaced_by: skipping {entitet} row #{ts_event_id} — "
                        f"var_id maps to >1 variable / A2.2 split "
                        f"(pred={pred_id}, succ={succ_id})"
                    )
                else:
                    n_skipped_unresolved += 1
                    _progress(
                        f"  replaced_by: skipping {entitet} row #{ts_event_id} — "
                        f"unresolved variable (pred={pred_id}, succ={succ_id})"
                    )
                continue
            if pred == succ:
                # Slug-grain self-loop the raw-id guard above can't catch: two
                # DISTINCT ids resolved to the same variable (most plausibly two
                # AktuellVariabel cvids of one unsplit var_id re-coded across
                # editions). A self-edge is meaningless and would corrupt
                # traversal; skip it (counted with the id-level self-loop).
                n_skipped_unresolved += 1
                _progress(
                    f"  replaced_by: skipping {entitet} row #{ts_event_id} — "
                    f"slug-grain self-loop (pred==succ=={pred})"
                )
                continue
            pk = (*pred, *succ)
            if pk in seen_variable:
                _classify_duplicate(handelse)
                continue
            seen_variable.add(pk)
            # #142: effective_year for the AktuellVariabel grain only — the
            # successor cvid resolves to an edition, and we take ITS year (the
            # year the new variable first appeared = when the succession took
            # effect). The bare `Variabel` grain has no cvid/edition → NULL.
            effective_year = (
                cvid_to_year.get(succ_id) if entitet == "AktuellVariabel" else None
            )
            conn.execute(
                "INSERT INTO variable_replaced_by ("
                "predecessor_provider, predecessor_register, predecessor_variable, "
                "successor_provider, successor_register, successor_variable, "
                "effective_year, note, beskrivning) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (*pred, *succ, effective_year, _REPLACED_BY_NOTE_AUTO, beskrivning),
            )
            n_variable += 1

    _progress(
        f"  {n_register:,} register / {n_variant:,} variant / "
        f"{n_variable:,} variable replaced_by edges "
        f"({n_skipped_collapsed_inverse:,} inverse-direction collapsed, "
        f"{n_skipped_duplicate:,} duplicate, "
        f"{n_skipped_ambiguous_variable:,} ambiguous variable, "
        f"{n_skipped_unresolved:,} unresolved)"
    )
    # Keys (and their meaning) live on `_REPLACED_BY_STAT_KEYS`; fill the zeroed
    # base so this return and the `skip_slugs` zero-fill share one shape.
    stats = _empty_replaced_by_stats()
    stats.update(
        n_timeseries_event_rows_scanned=n_scanned,
        n_register_replaced_by=n_register,
        n_variant_replaced_by=n_variant,
        n_variable_replaced_by=n_variable,
        n_skipped_unresolved=n_skipped_unresolved,
        n_skipped_ambiguous_variable=n_skipped_ambiguous_variable,
        n_skipped_collapsed_inverse=n_skipped_collapsed_inverse,
        n_skipped_duplicate=n_skipped_duplicate,
    )
    return stats


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


def _reparent_variable_alias(conn: sqlite3.Connection) -> None:
    """A2.7: project the cvid-grained `variable_alias_build` staging onto the
    shipped `variable_id`-keyed `variable_alias`, then leave the staging table
    for the caller to DROP.

    Runs as one of the LAST readers of `variable_instance` (it resolves each
    cvid's `(register_id, var_id)` to a `variable_id` through the promoted
    `variable` table). `variable_alias` carries the FULL delivery-column history
    keyed by variable + delivering variant — the source `get_datacolumns` reads,
    which the coalesced `variable_state.delivery_column_name` (latest era only)
    can't supply.

    Post-A2.2 a `var_id` can be NON-unique (split siblings share one
    `provider_key`), so an alias attaches to every sibling sharing the key. That
    is acceptable for alias *search / history* recall (the only consumers); it
    does not feed resolution (the resolver reads `variable_state`). `DISTINCT`
    collapses the fan-out into the `(variable_id, register_variant_id,
    delivery_column_name)` PK.
    """
    _progress("Re-parenting variable_alias onto variable_id...")
    conn.execute(
        "INSERT OR IGNORE INTO variable_alias "
        "    (variable_id, register_variant_id, delivery_column_name) "
        "SELECT DISTINCT v.variable_id, vi.register_variant_id, "
        "       vab.delivery_column_name "
        "FROM variable_alias_build vab "
        "JOIN variable_instance vi ON vab.cvid = vi.cvid "
        "JOIN variable v ON vi.register_id = v.register_id "
        "    AND CAST(vi.var_id AS TEXT) = v.provider_key"
    )
    n = conn.execute("SELECT COUNT(*) FROM variable_alias").fetchone()[0]
    _progress(f"  {n:,} variable_alias rows (variable_id-grained)")


def _backfill_state_classifications(conn: sqlite3.Connection) -> None:
    """A2.7: tag `variable_state.classification_id` from its constituent
    `variable_instance` rows.

    The coalescer can't write it — `_coalesce_variable_states` runs *before*
    `populate_classifications` sets `variable_instance.classification_id` (the
    A2.2-documented "classification-family signal inert at coalesce time"). This
    backfill runs after both, as one of the last readers of `variable_instance`.

    Correlation key: `(variable_id, value_set_id)`. A state's `value_set_id` is
    part of the coalescer's group key, so every `variable_instance` row folded
    into that state shares it — the join is exact. `value_set_id` is preferred
    over `value_set_version_label` (the design's secondary signal) because the
    fold logic overwrites the label with a synthetic column-suffix token for
    states that never carried a classification; the value-set link is immune to
    that. Code-less states (NULL `value_set_id`) legitimately have no
    classification → left NULL.

    `ORDER BY vi.classification_id LIMIT 1` makes the pick build-stable. The
    correlated set *should* be single-valued (same value_set_id ⇒ identical
    content-hashed codes ⇒ same `populate_classifications` derivation), so the
    ORDER BY is defensive, not load-bearing; but a bare `LIMIT 1` would be
    formally non-deterministic if that invariant ever broke (PR-review NIT).
    """
    _progress("Backfilling variable_state.classification_id...")
    conn.execute(
        "UPDATE variable_state SET classification_id = ("
        "    SELECT vi.classification_id FROM variable_instance vi "
        "    JOIN variable v ON vi.register_id = v.register_id "
        "        AND CAST(vi.var_id AS TEXT) = v.provider_key "
        "    WHERE v.variable_id = variable_state.variable_id "
        "      AND vi.value_set_id IS variable_state.value_set_id "
        "      AND vi.classification_id IS NOT NULL "
        "    ORDER BY vi.classification_id "
        "    LIMIT 1"
        ") "
        "WHERE EXISTS ("
        "    SELECT 1 FROM variable_instance vi "
        "    JOIN variable v ON vi.register_id = v.register_id "
        "        AND CAST(vi.var_id AS TEXT) = v.provider_key "
        "    WHERE v.variable_id = variable_state.variable_id "
        "      AND vi.value_set_id IS variable_state.value_set_id "
        "      AND vi.classification_id IS NOT NULL"
        ")"
    )
    n = conn.execute(
        "SELECT COUNT(*) FROM variable_state WHERE classification_id IS NOT NULL"
    ).fetchone()[0]
    _progress(f"  {n:,} variable_state rows tagged with a classification")


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


def _variable_set_via_same_as(
    conn: sqlite3.Connection,
    *,
    consumer_provider: str,
    consumer_register: str,
    source_provider: str,
    source_register: str,
    variable_slug: str,
) -> set[str]:
    """Multi-seed BFS over variable-grain `variable_same_as`, returning the set
    of variable slugs *in `source_register`* equivalent to the consumer variable
    (§5.6 "same_as on the source side").

    Two seed nodes, so all three equivalence kinds are covered:
      - the SOURCE-side identity node `(source_provider, source_register,
        variable_slug)` — the slug-equality identity match (LISA `kon` → RTB
        `kon`, which needs no curated edge) plus any within-source rename
        reachable from it (RTB `kon` ↔ `kon-v2`);
      - the CONSUMER node `(consumer_provider, consumer_register,
        variable_slug)` — any curated cross-register / cross-provider `same_as`
        edge whose endpoints have *different* slugs (LISA `foo` ↔ RTB `bar`,
        §5.5), plus renames transitively reachable from the matched source node.

    The earlier single-seed form (source node only) silently missed the
    mismatched-slug cross-register edge — a latent gap while `variable_same_as`
    is empty, fixed here per the A2.4 review. The COMMON case is no `same_as`
    edge at all → the result is just `{variable_slug}` (the identity match).

    Edges are stored both directions (per `materialize_same_as_edges`), so a
    single forward adjacency query per node suffices. A `visited` set makes the
    walk cycle-safe — required, not merely defensive: both-directions storage
    makes every A↔B equivalence a 2-cycle in the materialized table by design.
    Provider is part of every node because `same_as` crosses provider
    boundaries (a SOS consumer sourcing from SCB follows edges in SCB's rows).
    """
    seeds = [
        (source_provider, source_register, variable_slug),
        (consumer_provider, consumer_register, variable_slug),
    ]
    visited: set[tuple[str, str, str]] = set(seeds)
    frontier: list[tuple[str, str, str]] = list(seeds)
    result: set[str] = set()
    # A seed already in the source register is the identity match (the source
    # node always is; the consumer node is, only in the degenerate self-source
    # case the linker filters out). Seed the result before expanding.
    for provider, register, variable in seeds:
        if provider == source_provider and register == source_register:
            result.add(variable)

    while frontier:
        provider, register, variable = frontier.pop()
        for row in conn.execute(
            "SELECT b_provider, b_register, b_variable FROM variable_same_as "
            "WHERE a_provider = ? AND a_register = ? AND a_variable = ? "
            "ORDER BY b_provider, b_register, b_variable",
            (provider, register, variable),
        ).fetchall():
            node = (row[0], row[1], row[2])
            if node in visited:
                continue
            visited.add(node)
            frontier.append(node)
            # Collect only nodes in the source PROVIDER's source register. The
            # BFS deliberately traverses cross-provider edges, so a hop into a
            # different provider that reuses the same register slug must NOT
            # contribute a slug — it would be applied back to the source
            # provider's register (the source-states query keys on
            # source_register_id) and could false-match a same-named variable
            # there. register.slug is not globally unique, so provider is
            # load-bearing (Codex/Copilot P2 on #144).
            if row[0] == source_provider and row[1] == source_register:
                result.add(row[2])
    return result


def link_variable_state_lineage(
    conn: sqlite3.Connection,
    slug_dir: Path,
) -> dict[str, int]:
    """Materialize §5.6 state-pair interval-overlap lineage edges.

    For every consumer `variable_state` whose variable has a populated
    `variable.source_register_id` (pointing at a *different* register), find
    the matching source variable(s) in that register — the consumer's own slug
    plus any variable-grain `same_as` expansion (`_variable_set_via_same_as`) —
    gather their states in the pinned source variant, and emit one
    `variable_state_lineage` edge per state pair whose validity ranges
    intersect. The edge's `(valid_from, valid_to)` is the intersection.

    Source-variant pinning (`LineageConfig`, TOML-only — no SQL table):
      1. Per-`(consumer_register, variable_slug)` override → exactly one variant.
      2. `[lineage_defaults]` per source register → one variant.
      3. No curated rule → all source-side variants carrying a matching state,
         plus an `ambiguous_source_variant` warning naming the candidates.

    When no source state is found at all, emits a `no_source_state` warning. A
    source state that is found but does NOT overlap the consumer state is a
    legitimate empty result — zero edges, zero warnings (distinct from
    `no_source_state`).

    A2.7 made this the SOLE lineage linker — the v0.11 `link_consumer_side_bindings`
    (which only set the now-dropped `variable_instance.via_source_id`) was deleted.
    Returns {'edges', 'warnings_ambiguous', 'warnings_no_source'} counts.

    Raises `RegMetaError` on contradictory curation: an override whose
    `source_register` disagrees with the variable's resolved
    `source_register_id`, or a pin naming a variant that doesn't exist in the
    source register.
    """
    config = load_lineage_config(slug_dir)

    # Resolve every override's source register/variant to a register_variant_id
    # up front; this also validates the named variants exist (fail-fast). Cache
    # variant-slug→id per register so we touch the DB once per source register.
    # Keyed (provider_slug, register_slug): register.slug is NOT globally unique,
    # so a pin for a source register must not resolve to another provider's
    # variant that happens to share the register + variant slug (Codex P2 on #144).
    variant_ids_by_register: dict[tuple[str, str], dict[str, int]] = {}

    def _variants_for(provider_slug: str, register_slug: str) -> dict[str, int]:
        key = (provider_slug, register_slug)
        cached = variant_ids_by_register.get(key)
        if cached is not None:
            return cached
        rows = conn.execute(
            "SELECT rv.register_variant_id, rv.slug FROM register_variant rv "
            "JOIN register r ON rv.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND rv.slug IS NOT NULL "
            "ORDER BY rv.slug",
            (provider_slug, register_slug),
        ).fetchall()
        mapping = {row[1]: row[0] for row in rows}
        variant_ids_by_register[key] = mapping
        return mapping

    def _pin_variant_id(
        provider_slug: str, register_slug: str, variant_slug: str, *, source: str
    ) -> int:
        variant_id = _variants_for(provider_slug, register_slug).get(variant_slug)
        if variant_id is None:
            raise RegMetaError(
                exit_code=EXIT_CONFIG,
                code="lineage_pin_unknown_variant",
                error_class="configuration",
                message=(
                    f"{source}: source variant {variant_slug!r} not found in "
                    f"register {register_slug!r}."
                ),
                remediation=(
                    "Fix the lineage pin to name a real variant slug in the "
                    "source register (check the register's [register_variant] "
                    "entries)."
                ),
            )
        return variant_id

    # Consumer states whose variable is sourced from a *different* register.
    # ORDER BY state_id pins deterministic warning-emission order.
    consumer_rows = conn.execute(
        "SELECT vs.state_id, vs.valid_from, vs.valid_to, "
        "       v.variable_id, v.slug AS consumer_slug, "
        "       cp.slug AS consumer_provider, cr.slug AS consumer_register, "
        "       v.source_register_id, sp.slug AS source_provider, "
        "       sr.slug AS source_register "
        "FROM variable_state vs "
        "JOIN variable v   ON vs.variable_id = v.variable_id "
        "JOIN register cr  ON v.register_id = cr.register_id "
        "JOIN provider cp  ON cr.provider_id = cp.provider_id "
        "JOIN register sr  ON v.source_register_id = sr.register_id "
        "JOIN provider sp  ON sr.provider_id = sp.provider_id "
        "WHERE v.source_register_id IS NOT NULL "
        "  AND v.source_register_id != v.register_id "
        "  AND v.slug IS NOT NULL "
        "ORDER BY vs.state_id"
    ).fetchall()

    # Per-variable memo: the candidate source-variable slug set (consumer slug
    # ∪ same_as expansion). Keyed on variable_id (many states share a variable).
    src_slugs_by_variable: dict[int, set[str]] = {}

    # PK-keyed dedup: a (consumer_state_id, source_state_id) pair can only arise
    # once per consumer-state loop (a state belongs to exactly one variant, so
    # the unpinned all-variants path can't surface it twice), but we key a dict
    # on the PK so an unexpected duplicate is a no-op INSERT rather than a PK
    # violation — defense, not an expected case.
    edges: dict[tuple[int, int], tuple[int, int, str, str]] = {}
    warnings_ambiguous: list[tuple[int, str, str]] = []
    warnings_no_source: list[tuple[int, str, str]] = []

    for row in consumer_rows:
        (
            c_state_id,
            c_valid_from,
            c_valid_to,
            variable_id,
            consumer_slug,
            consumer_provider,
            consumer_register,
            source_register_id,
            source_provider,
            source_register,
        ) = row

        # Candidate source-variable slug set (memoized per variable). The slug
        # identifies the source variable by IDENTITY (LISA `kon` → RTB `kon`, no
        # curated edge needed); the multi-seed BFS additionally follows the
        # source register's own renames (RTB `kon` ↔ `kon-v2`) AND any curated
        # cross-register `same_as` edge whose slugs differ (LISA `foo` ↔ RTB
        # `bar`, §5.5). The no-rename common case yields just {consumer_slug}.
        src_slugs = src_slugs_by_variable.get(variable_id)
        if src_slugs is None:
            src_slugs = _variable_set_via_same_as(
                conn,
                consumer_provider=consumer_provider,
                consumer_register=consumer_register,
                source_provider=source_provider,
                source_register=source_register,
                variable_slug=consumer_slug,
            )
            src_slugs_by_variable[variable_id] = src_slugs

        # Resolve the pinned source variant(s). None = register-level fallback
        # (all variants that carry a matching state) + ambiguous warning.
        override = config.overrides.get(
            (consumer_provider, consumer_register, consumer_slug)
        )
        pinned_variant_ids: list[int] | None
        if override is not None:
            override_register, override_variant = override
            if override_register != source_register:
                raise RegMetaError(
                    exit_code=EXIT_CONFIG,
                    code="lineage_override_register_mismatch",
                    error_class="configuration",
                    message=(
                        f'[lineage."{consumer_register}.{consumer_slug}"]: '
                        f"source_register {override_register!r} contradicts the "
                        f"variable's resolved source register {source_register!r}."
                    ),
                    remediation=(
                        "Fix the override's source_register to match the "
                        "variable's variable_register_kalla attribution, or "
                        "remove the override."
                    ),
                )
            pinned_variant_ids = [
                _pin_variant_id(
                    source_provider,
                    source_register,
                    override_variant,
                    source=f'[lineage."{consumer_register}.{consumer_slug}"]',
                )
            ]
        elif (source_provider, source_register) in config.defaults:
            pinned_variant_ids = [
                _pin_variant_id(
                    source_provider,
                    source_register,
                    config.defaults[(source_provider, source_register)],
                    source=f"[lineage_defaults] {source_provider}/{source_register}",
                )
            ]
        else:
            pinned_variant_ids = None  # register-level fallback

        # Fetch source states: variable in the source register, slug in the
        # candidate set, optionally narrowed to the pinned variant(s). Carry
        # the variant slug so the fallback warning can name candidates.
        slug_placeholders = ",".join("?" for _ in src_slugs)
        params: list[Any] = [source_register_id, *sorted(src_slugs)]
        variant_clause = ""
        if pinned_variant_ids is not None:
            variant_placeholders = ",".join("?" for _ in pinned_variant_ids)
            variant_clause = f" AND vs.register_variant_id IN ({variant_placeholders})"
            params.extend(pinned_variant_ids)
        source_states = conn.execute(
            "SELECT vs.state_id, vs.valid_from, vs.valid_to, rv.slug AS variant_slug "
            "FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "JOIN register_variant rv ON vs.register_variant_id = rv.register_variant_id "
            f"WHERE v.register_id = ? AND v.slug IN ({slug_placeholders}){variant_clause} "
            "ORDER BY vs.state_id",
            params,
        ).fetchall()

        if not source_states:
            # The variable was sought but no source state exists in the pinned
            # variant(s) / register — distinct from a found-but-non-overlapping
            # state (which yields zero edges below, no warning).
            if pinned_variant_ids is None:
                pin_desc = "any variant"
            else:
                slug_by_id = {
                    v: k
                    for k, v in _variants_for(source_provider, source_register).items()
                }
                pinned_slugs = sorted(
                    slug_by_id.get(v, str(v)) for v in pinned_variant_ids
                )
                pin_desc = f"pinned variant(s) {pinned_slugs!r}"
            warnings_no_source.append(
                (
                    c_state_id,
                    "no_source_state",
                    f"No source state in register {source_register!r} for "
                    f"variable slug(s) {sorted(src_slugs)!r} ({pin_desc}).",
                )
            )
            continue

        # Fallback (no pin): the source states may span several variants. Warn
        # if they do, naming the candidates (sorted for determinism). The
        # warning is about the UNPINNED ambiguity (which source variant feeds
        # this consumer), independent of whether this particular consumer state
        # overlaps any of them — so it fires on multi-variant candidates even
        # if the interval check below emits no edge for this state.
        if pinned_variant_ids is None:
            candidate_variants = sorted({s[3] for s in source_states})
            if len(candidate_variants) > 1:
                warnings_ambiguous.append(
                    (
                        c_state_id,
                        "ambiguous_source_variant",
                        f"No source-variant pin for "
                        f"{consumer_register}.{consumer_slug}; matched source "
                        f"states across all variants: {candidate_variants!r}. Add "
                        f'a [lineage_defaults] or [lineage."{consumer_register}.'
                        f'{consumer_slug}"] pin to disambiguate.',
                    )
                )

        # Interval-overlap emit. ISO full-date strings compare chronologically
        # by lexical order (full-date contract + '9999-12-31' sentinel).
        for s_state_id, s_valid_from, s_valid_to, _variant_slug in source_states:
            lo = max(c_valid_from, s_valid_from)
            hi = min(c_valid_to, s_valid_to)
            if lo <= hi:
                edges[(c_state_id, s_state_id)] = (c_state_id, s_state_id, lo, hi)

    if edges:
        conn.executemany(
            "INSERT INTO variable_state_lineage "
            "(consumer_state_id, source_state_id, valid_from, valid_to) "
            "VALUES (?, ?, ?, ?)",
            list(edges.values()),
        )
    warnings = warnings_ambiguous + warnings_no_source
    if warnings:
        conn.executemany(
            "INSERT INTO variable_state_lineage_warning "
            "(consumer_state_id, warning_kind, message) VALUES (?, ?, ?)",
            warnings,
        )

    counts = {
        "edges": len(edges),
        "warnings_ambiguous": len(warnings_ambiguous),
        "warnings_no_source": len(warnings_no_source),
    }
    if consumer_rows:
        _progress(
            f"  Variable state lineage edges: {counts['edges']:,} emitted, "
            f"{counts['warnings_ambiguous']:,} ambiguous-variant, "
            f"{counts['warnings_no_source']:,} no-source warnings"
        )
    return counts


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
            var_slug_counts = populate_variable_slugs(
                conn, slug_root, fold_slugs=state_stats["_fold_slug_hints"]
            )
            row_counts["variable_slugs_curated"] = var_slug_counts["curated"]
            row_counts["variable_slugs_auto"] = (
                var_slug_counts["auto_existing"] + var_slug_counts["auto_new"]
            )

            # §5.7 split-sibling edges. Runs after variable slugs so each
            # sibling variable_id resolves to its FQID slug; the triage emitted
            # the (variable_id, variable_id, kind) pairs during coalescing.
            n_related = _materialize_variable_related_to(
                conn, state_stats["_related_edges"]
            )
            row_counts["variable_related_to"] = n_related
            _progress(f"  {n_related:,} variable_related_to edges (auto:triage)")

        # §5.5 same_as edges. Runs *after* populate_slugs so register /
        # variant / version slug columns are populated — the materializer
        # validates target slugs against them. Skip-slugs takes the
        # honest-failure stance shared by the slug-keyed linkers below
        # (replaced_by + lineage): skip cleanly rather than emit zero edges
        # silently from NULL slug columns.
        if skip_slugs:
            _progress("Skipping same_as edges (skip_slugs=True)")
        else:
            sa_counts = materialize_same_as_edges(conn, slug_root)
            _progress(
                f"  {sa_counts['variable']:,} variable same_as edges, "
                f"{sa_counts['classification']:,} classification same_as edges"
            )

        # A2.3: §5.5 replaced_by edges. Runs *after* populate_variable_slugs
        # (above) — every grain resolves off a stored slug column, and the
        # variable grain reads `variable.slug`. Under `--skip-slugs` those
        # columns are NULL, so the materializer would emit zero edges silently;
        # mirror the same_as honest-failure stance and skip cleanly with zeroed
        # stats instead.
        if skip_slugs:
            _progress("Skipping replaced_by edges (skip_slugs=True)")
            replaced_by_stats: dict[str, int] = _empty_replaced_by_stats()
        else:
            replaced_by_stats = _materialize_replaced_by_edges(conn)

        # §5.6 lineage edges. Runs *after* populate_variable_slugs so
        # `variable.slug` is non-NULL on both sides. `source_register_id` was
        # populated by the Registerinformation.csv import far above.
        #
        # A2.7 removed the v0.11 `link_consumer_side_bindings` (it only set the
        # now-dropped `variable_instance.via_source_id`); `link_variable_state_lineage`
        # (A2.4, state grain) is the sole lineage linker.
        #
        # Skip under `--skip-slugs`: every `variable.slug` is NULL in that mode,
        # so running would silently produce zero edges instead of an honest
        # "this build is incomplete" signal. Run `build-db` without
        # `--skip-slugs` (the default) to materialize lineage.
        if skip_slugs:
            _progress("Skipping variable_state lineage edges (skip_slugs=True)")
        else:
            # A2.4 (§5.6): state-pair interval-overlap lineage. Ordering: after
            # populate_variable_slugs (reads variable.slug on both sides),
            # materialize_same_as_edges (the BFS reads variable_same_as), and
            # _coalesce_variable_states (reads the finished variable_state rows
            # it joins) — so it must be among the last passes. Shares the
            # skip_slugs guard: every slug is NULL under --skip-slugs, so the
            # linker would silently emit zero edges instead of an honest
            # incompleteness signal. `slug_root` is in scope from the slug
            # branch above.
            lineage_counts = link_variable_state_lineage(conn, slug_root)
            row_counts["variable_state_lineage"] = lineage_counts["edges"]
            row_counts["variable_state_lineage_warnings"] = (
                lineage_counts["warnings_ambiguous"]
                + lineage_counts["warnings_no_source"]
            )

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

        # A2.7: re-parent the cvid-grained alias staging onto the shipped
        # `variable_alias` (variable_id-keyed) and backfill
        # `variable_state.classification_id`. BOTH are the LAST readers of
        # `variable_instance` — they must run before the DROP below. `_progress`
        # is emitted inside each helper.
        _reparent_variable_alias(conn)
        _backfill_state_classifications(conn)

        # A2.7: drop `variable_instance` + its cvid-grained alias staging before
        # ship. Every build-time reader has run: `_coalesce_variable_states`
        # (→ `variable_state`), `populate_classifications` (tags
        # `classification_id`), value-set projection (`value_set_id`),
        # `code_variable_map` (above), and the two A2.7 backfills (just above).
        # The shipped query layer reads `variable_state` / `variable` /
        # re-parented `variable_alias`. `variable_alias_build` FKs
        # `variable_instance(cvid)`, so it must drop FIRST (child before parent)
        # or `PRAGMA foreign_key_check` (below) flags the dangling cvids.
        # `variable_alias` (shipped) FKs `variable`/`register_variant`, not the
        # dropped tables, so it survives clean. (`variable_context` was dropped
        # from the DDL outright in A2.7 — a write-only debug table with no
        # consumer that would have orphaned on this drop.)
        conn.execute("DROP TABLE variable_alias_build")
        conn.execute("DROP TABLE variable_instance")
        _progress("Dropped variable_instance + variable_alias_build (A2.7).")

        # A2.6: drop the build-only register-edition tables before ship (mirrors
        # the `unika_summary` drop above). `register_version` fed the coalescer's
        # valid_from/to year fallback and the lineage linkers (`*_replaced_by`,
        # `link_variable_state_lineage`), all of which ran above; `population` /
        # `object_type` are write-only debug tables nothing in the shipped
        # catalog reads. The FQID grammar no longer has a version segment (§5.2),
        # so none of these belong in the shipped DB. Drop order is FK-safe:
        # children (`population`, `object_type` FK `register_version`) first,
        # then the parent — `PRAGMA foreign_key_check` (below) flags children of
        # a dropped parent, so leaving them would fail the build.
        conn.execute("DROP TABLE population")
        conn.execute("DROP TABLE object_type")
        conn.execute("DROP TABLE register_version")
        _progress("Dropped register_version + population + object_type (A2.6).")

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
            # A2.3 replaced_by stats — fan-out per entity grain plus the
            # skipped-row counters. Lets maintainers verify the inverse-
            # direction collapse worked and spot regressions in the
            # unresolved/ambiguous-id rate without re-running the build.
            "replaced_by_stats": replaced_by_stats,
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

        # A2.1: VACUUM reclaims the pages freed by the build-time-only `DROP
        # TABLE`s above — `unika_summary` (A2.1), `register_version` +
        # `population` + `object_type` (A2.6), plus `variable_instance` +
        # `variable_alias_build` (A2.7) — so the shipped DB doesn't carry
        # a fat freelist. `validate.py` flags
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
