"""Catalog schema, source-file IO, search indexes and atomic publication.

The three-stage driver lives in pipeline.py. Steward extension uses the small
IR graph inserter here; global builds write resolved_catalog directly.
"""

from __future__ import annotations

import csv
import hashlib
import io
import os
import sqlite3
import struct
import sys
import time
from contextlib import closing, contextmanager
from typing import TYPE_CHECKING, cast

from reg_meta.db import (
    get_manifest,
)
from reg_meta.errors import EXIT_CONFIG, RegMetaError

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from pathlib import Path

    from .input_snapshot import (
        ScbSnapshotReader,
    )
    from .ir import (
        IRRegister,
        IRVariable,
        IRVariableAlias,
        IRVariableAliasWindow,
        IRVariableState,
        IRVariant,
    )

# Built-in data providers. `provider_id` values are stable: rows reference them
# from `register.provider_id`. Add new providers by appending — never renumber.
PROVIDER_ID_SCB = 1
PROVIDER_ID_SOS = 2
PROVIDER_ID_FOHM = 3
PROVIDER_ID_FK = 4
PROVIDER_ID_LV = 5
PROVIDER_ID_PLIKT = 6
PROVIDER_ID_RA = 7
PROVIDER_ID_UMU = 8
_PROVIDER_SEED: tuple[tuple[int, str, str], ...] = (
    (PROVIDER_ID_SCB, "scb", "Statistiska Centralbyrån"),
    (PROVIDER_ID_SOS, "sos", "Socialstyrelsen"),
    (PROVIDER_ID_FOHM, "fohm", "Folkhälsomyndigheten"),
    (PROVIDER_ID_FK, "fk", "Försäkringskassan"),
    (PROVIDER_ID_LV, "lakemedelsverket", "Läkemedelsverket"),
    (PROVIDER_ID_PLIKT, "pliktverket", "Pliktverket"),
    (PROVIDER_ID_RA, "riksarkivet", "Riksarkivet"),
    (PROVIDER_ID_UMU, "umu", "Umeå universitet"),
)

# Thin CURATED global providers (#422): public agencies with no machine-readable
# native export — their catalog content is a maintainer-authored TOML read by the
# shared `CuratedAdapter` (sources/curated.py). Each entry is
# (provider_slug, input_data subdir holding `<provider_slug>.toml`). Unlike the
# untracked SCB/SOS seed, this TOML is committed, so the subdir always exists on
# any checkout — which requires a per-agency `.gitignore` un-ignore line (the
# `input_data/*` rule otherwise hides it). See DESIGN.md → Curated thin providers.
_CURATED_PROVIDERS: tuple[tuple[str, str], ...] = (
    ("fohm", "Folkhalsomyndigheten"),
    ("fk", "Forsakringskassan"),
    ("lakemedelsverket", "Lakemedelsverket"),
    ("pliktverket", "Pliktverket"),
    ("riksarkivet", "Riksarkivet"),
    ("umu", "UMU"),
)

# Committed canonical-SCB seed (#444) — SCB registers SWECOV holds but SCB's
# machine export lacks. Both the #556 stale-seed preflight and the adapter guard
# resolve the seed from here so the two can't drift apart.

# Exact SCB type tokens shared by source cleaning and the standalone source audit.
# They are not enumerated codes when code == version == level. Source cleaning
# preserves their rows as non-membership evidence (sources/scb_values.py).
_VARDEMANGDER_SENTINELS = frozenset({"Tal", "Beskrivande text"})

# value_code label-search stoplist (#352). Junk labels excluded from the
# value_code_fts INDEX ONLY at population time — the leaf value_code / value_set
# tables keep every row (this hides them from search, it does NOT drop data).
# Separate from the source type markers interpreted by sources/scb_values.py.
# This is a whole-LABEL exclusion, not an FTS tokenizer stopword
# list: a label matches iff it equals one of the exact strings OR matches one of
# the prefix families. Initial curated dozen per #352; broader curation is out of
# scope. The frequency head mixes junk with legitimate concepts (Småort, school
# names), so frequency is NOT the hiding criterion — only this explicit list is.
_VALUE_CODE_STOPLIST_EXACT = frozenset(
    {"NULL", "Uppgift saknas", "Vill ej svara", "Ja", "Nej", "Ej tillämplig"}
)
# Prefix families: SCB stuffs the missing/erroneous-value sentinels in many
# variants ("Okänt värde", "Okänd kommun", "Felaktigt värde", ...). A prefix is
# justified HERE (and only here) because these are open SCB sentinel FAMILIES, not
# a fixed label set — matched with SQLite `label LIKE 'Okänt%'` etc. The stem
# (not the full word) is DELIBERATE: it must catch both the bare sentinel ("Okänd"),
# the space-separated form ("Okänt värde"), AND the inflected form — "Felaktigt
# värde" is only caught by `Felaktig%`, since "Felaktigt" != "Felaktig" so a
# word-boundary match (`= p OR LIKE 'p %'`) would miss it. Accepted coarseness: a
# hypothetical legit label starting with one of these stems as a longer single word
# (e.g. "Okäntköping") would also be hidden — no such label occurs in the corpus,
# and broader stoplist curation is out of #352 scope (initial dozen only).
_VALUE_CODE_STOPLIST_PREFIXES = ("Okänt", "Okänd", "Felaktig")


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

# str.translate table mapping each DOS-remnant byte (read as a latin-1
# codepoint) to its cp1252-twin codepoint — the same char a normal cp1252 byte
# would decode to (0x8F→Å is also reachable as 0xC5→Å, etc.). Applying it to a
# raw latin-1 string yields a dedup key whose equality is IDENTICAL to comparing
# `_decode_cp1252` results: `_decode_cp1252` is injective on every byte EXCEPT it
# folds each fixup byte onto its twin, so canonicalizing exactly those five bytes
# induces the same equivalence — without paying a full per-row decode. Lets the
# Vardemangder hot loop key value_code dedup on raw fields and defer decode to
# first-occurrence while staying byte-identical to the decoded-key build.
_CP850_CANON = {b: ord(ch) for b, ch in _CP850_FIXUP.items()}

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

DDL = """\
-- Core tables (all IDs stored as INTEGER for compact storage)

-- Data providers (publishers): scb, sos, ... See _PROVIDER_SEED for the seed.
-- Promoted to first-class in schema v3.1 for FQID grammar (see reg_meta/DESIGN.md → FQID grammar).
CREATE TABLE provider (
    provider_id INTEGER PRIMARY KEY,
    slug        TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL
);

-- FQID slug columns (`slug` on register / register_variant / classification)
-- are nullable in 3.1. Curated values land in step 1c; the build refuses to
-- compile with NULL slugs from then on. The `_default` placeholder for
-- variant-less registers is synthesized at FQID-resolve time (catalog.py),
-- never persisted. See reg_meta/DESIGN.md → FQID grammar and DESIGN.md → Slug curation.
CREATE TABLE register (
    register_id INTEGER PRIMARY KEY,
    provider_id INTEGER NOT NULL REFERENCES provider(provider_id),
    -- Universal English columns; see reg_meta/DESIGN.md → Glossary and Swedish↔English crosswalk.
    -- Values remain provider-native strings (SCB's literal `Registernamn`
    -- text such as "LISA"). `registerrubrik` is dropped (redundant with `name`).
    name TEXT NOT NULL,
    purpose TEXT,
    slug         TEXT
);

CREATE TABLE register_variant (
    register_variant_id INTEGER PRIMARY KEY,
    register_id INTEGER NOT NULL REFERENCES register(register_id),
    -- Universal-vocabulary rename. `registervariantrubrik` (redundant with name) and
    -- `registervariantsekretess` (legal text → reg-meta-docs) are dropped.
    name TEXT,
    description TEXT,
    slug          TEXT,
    -- Presentation-only grouping label. Drift-tolerant.
    display_group TEXT,
    -- A4.4c panel-shape coordinates. MUTABLE (curated via TOML, not slug-frozen
    -- — they don't enter the slug snapshot). Nullable: most variants carry no
    -- panel data and stay NULL (curation is a later seam). `populate_slugs` is
    -- the sole writer.
    --   panel_entity_key: a bare variable-slug (simple case) OR a json.dumps'd
    --     list of variable-slugs (composite case); reg_meta decodes on read.
    --   panel_time_key: literal "period" (delivery-aligned) OR a variable-slug
    --     (row-level time column).
    --   panel_time_grain: 'delivery' or 'row'.
    panel_entity_key TEXT,
    panel_time_key   TEXT,
    panel_time_grain TEXT CHECK (panel_time_grain IN ('delivery', 'row'))
);

-- Register-version metadata. The FQID grammar has no version segment (see
-- reg_meta/DESIGN.md → FQID grammar), so this table carries NO `slug` column:
-- period is a delivery coordinate, not identity. The coalescer also reads
-- `registerversionnamn` for the variable_state valid_from/to year fallback, and
-- the lineage linkers derive a per-edition period from it. Since #799, the
-- shipped catalog keeps the prose fields for register/variant metadata display.
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

-- Population and object-type prose scoped to a register version (#799). These
-- are shipped read-only catalog metadata, not FQID-addressable entities.
CREATE TABLE population (
    regver_id INTEGER NOT NULL REFERENCES register_version(regver_id),
    -- Universal-vocabulary rename. `populationdatum` is a free-text date range, not a parsed
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
    -- Synthetic PK so variable_state's FK is
    -- single-column and the edge tables stay stable as the natural key varies
    -- per provider. The natural key is (register_id, slug); `provider_key`
    -- (SCB `str(var_id)`; SOS the merged variable name) is demoted from the PK
    -- to a NON-unique join hint — a triage split puts several
    -- variables under one source key.
    variable_id INTEGER PRIMARY KEY AUTOINCREMENT,
    register_id INTEGER NOT NULL REFERENCES register(register_id),
    -- SCB str(var_id), TEXT so SOS can key by merged variable name.
    -- NON-unique join hint, not a key: the build-time `variable_instance.var_id`
    -- (INTEGER) joins via CAST-to-TEXT, and `code_variable_map.var_id` carries it
    -- into the shipped DB.
    provider_key TEXT NOT NULL,
    -- Register-unique FQID leaf. NULL until the slug follow-up PR
    -- populates it; SQLite treats NULLs as distinct, so the transient all-NULL
    -- window doesn't trip the unique index below.
    slug TEXT,
    -- Universal-vocabulary rename. Values stay provider-native. SCB's
    -- `variabeloperationell_definition` is a FIRST-CLASS column
    -- (`operational_definition`) carrying the per-(split-)variable distinguishing
    -- meaning, NOT merged into `description` (#892) — it survives the
    -- parallel-column split so each sibling keeps its own. `variabelreferenstid`,
    -- `variabelhamtadfran`, and `variabelextern_kommentar` are dropped.
    -- `variabelregister_kalla` (raw attribution text) becomes `source_register_text`.
    name TEXT,
    definition TEXT,
    description TEXT,
    operational_definition TEXT,
    source_register_text TEXT,
    measurement_unit TEXT,
    source_register_id INTEGER REFERENCES register(register_id),
    source_label TEXT,
    -- Curated slug-TOML marker for retired/deprecated variables that should
    -- remain resolvable but produce semantic authoring hints.
    deprecated INTEGER NOT NULL DEFAULT 0,
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
-- Natural key: register-unique slug (the FQID leaf). Stays unique after
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
-- A4.4e: BUILD-TIME-ONLY provider-blind classification linkage. Every adapter
-- feeds it (SCB projects `variable_instance` verbatim; SOS resolves
-- `external_classification`; curated thin providers name a catalog short_name
-- directly — #446), and `_backfill_state_classifications` reads ONLY this table —
-- so the backfill no longer knows which provider supplied a candidate (the GAP-1
-- close-out). A (variable_id, value_set_id) state key MAY have SEVERAL candidate
-- rows — cvids that share the key but carry different value-set-version labels
-- resolve to different classifications — so the backfill folds them to the min()
-- classification_id per state key. `variable_id` is NOT NULL: a candidate with no
-- owning variable could never apply (the feed pre-filters such rows), so the
-- constraint also guards a future SOS feed. `value_set_id` is NULLABLE: a
-- code-less state keys on (variable_id, NULL). NO foreign key (so it drops cleanly
-- before `PRAGMA foreign_key_check`) and NO index (a single sequential read at
-- backfill time). Dropped before ship with the other scratch.
CREATE TABLE classification_candidate (
    variable_id INTEGER NOT NULL,
    value_set_id INTEGER,
    classification_id INTEGER NOT NULL
);

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
    -- Universal-vocabulary rename of `datatyp` / `datalangd` / `vardemangdsversion`.
    -- `vardemangdsniva` stays Swedish: it's a transient pre-triage carrier
    -- through A2.2 (the coalescer's `grain` field) and gets dropped from the
    -- final `variable_state` schema in A2.1. Keeping the Swedish name signals
    -- "do not depend on this column" to downstream code.
    data_type TEXT,
    data_length TEXT,
    value_set_version_label TEXT,
    vardemangdsniva TEXT,
    -- #892: per-cvid carrier for SCB's `VariabelOperationell_definition`. The
    -- ingest pass writes it here (the per-cvid grain is the right carrier — it
    -- distinguishes parallel columns of one var_id), then `_coalesce_variable_states`
    -- aggregates it to each OWNING (post-split-sibling) `variable.operational_definition`
    -- via the same ground-truth `variable_id` stamp that routes aliases. Dropped
    -- with this build-time-only table before ship.
    operational_definition TEXT,
    -- Raw per-cvid source attribution. SCB may use the operational-definition
    -- cell for opaque source/questionnaire codes (E22/E60); the importer routes
    -- those here so they can be preserved at state grain.
    source_register_text TEXT,
    -- State provenance carrier used by the SCB coalescer. NULL for provider
    -- export rows; dropped with this build-only table after IR emission.
    provenance TEXT,
    classification_id INTEGER REFERENCES classification(id),
    -- value_set_id links to the cvid's deduplicated, year-projected code list.
    -- NULL when the cvid has no codes (sentinel-only or every union pair
    -- excluded by year projection). No reverse index — every consumer reaches
    -- here from the cvid PK side, so the forward path is already optimal.
    value_set_id INTEGER REFERENCES value_set(value_set_id),
    -- The cvid's OWNING `variable_id` — GROUND TRUTH, stamped by
    -- `_coalesce_variable_states` AFTER triage (NULL until then). A2.2
    -- triage can split one source `var_id` into sibling variables that SHARE
    -- the `(register_id, var_id)` provider key, so `var_id` alone can't name the
    -- owning variable; but the coalescer builds each `variable_state` FROM these
    -- cvids and therefore KNOWS the exact cvid→sibling assignment, which it
    -- records here. `SCBAdapter._emit_variable_aliases` (→ IRVariableAlias →
    -- materializer) and `_backfill_state_classifications` read it to attribute
    -- each cvid's delivery columns / classification to the right sibling — no
    -- post-hoc column-tie heuristic, no skip. No FK: build-time-only (dropped
    -- with the table, before `PRAGMA foreign_key_check`) and values valid by
    -- construction. No CREATE-time index: the column is NULL until triage. Once
    -- stamped, the coalescer creates a transient `variable_id, cvid` scratch index
    -- for sibling-routed post-stamp readers. Distinct from the natural-key note
    -- below — that's about the absent `(register_id, var_id)` → `variable` FK.
    variable_id INTEGER
    -- A2.1.5: no FK on the `(register_id, var_id)` natural key to `variable` —
    -- it moved to the synthetic `variable_id` PK + register-unique `slug`, so
    -- `(register_id, var_id)` is no longer a UNIQUE/PK target. That join is by
    -- convention (and the `idx_variable_natkey` index). A2.7 dropped the v0.11
    -- `via_source_id` self-FK lineage column (superseded by
    -- `variable_state_lineage`, A2.4).
);

-- A2.7: BUILD-TIME-ONLY cvid-grained alias staging. The import pass writes one
-- row per (cvid, delivery_column_name); the coalescer + sensitivity + replaced_by
-- passes read it by `cvid`; then `SCBAdapter._emit_variable_aliases` projects it
-- (joined through `variable_instance.variable_id`) onto IRVariableAlias, which
-- the materializer writes into the shipped `variable_id`-keyed `variable_alias`
-- (A4.3a). Both scratch tables DROP before ship. (Kept separate from the shipped
-- table because the cvid grain has no FK target once `variable_instance` is
-- dropped.)
CREATE TABLE variable_alias_build (
    cvid INTEGER NOT NULL REFERENCES variable_instance(cvid),
    delivery_column_name TEXT NOT NULL,
    PRIMARY KEY (cvid, delivery_column_name)
);

-- Per-era shape of a variable (see reg_meta/DESIGN.md → Two-level variable model). One row per coalesced
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
-- contract); coarser SCB inputs like the year "2020" expand at
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
    source_register_text TEXT,
    -- Per-state delivery-column meaning (#736). Parallel same-period
    -- multi-response members can share one variable and value set while each
    -- column carries a distinct SCB `VariabelOperationell_definition`.
    operational_definition TEXT,
    -- NULL for ordinary provider-exported states. Curated corrections carry a
    -- stable `errata:<class>\\n<evidence>` value; when corrected source editions
    -- overlap a documented claim, `errata:scoped-attributions` carries JSON
    -- records pairing each edition set with its class/evidence; correction-only
    -- overlaps use `errata:overlapping-attributions`. A source-less interval
    -- retained by resolution carries `inferred:resolution-gap`, never provider
    -- NULL. Steward-only states may carry their `steward:<label>` origin. Kept at
    -- state grain so corrected, inferred, and documented spans cannot conflate.
    provenance TEXT,
    value_set_id INTEGER REFERENCES value_set(value_set_id),
    -- Overlap discriminator (multi-vintage / grain / coding). NOT NULL
    -- DEFAULT '' so the uniqueness index below bites in the common
    -- single-version case — SQLite treats NULLs as distinct, which would let
    -- duplicate non-multi-vintage states slip through. Mirrors '9999-12-31'.
    value_set_version_label TEXT NOT NULL DEFAULT '',
    -- Classification family for this era's value set. The coalescer
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
-- State-uniqueness index — UNIQUE(variable_id, register_variant_id,
-- valid_from, value_set_version_label). A4.3b moved it into the base DDL (was
-- created by `_coalesce_variable_states` after SCB triage). Rationale: it is a
-- structural invariant of the universal `variable_state` shape, not an SCB
-- artifact — two adapters (SCB coalescer, SOS reinsert) each CREATE-ing it is a
-- footgun, and with it in the DDL from table creation BOTH the SCB coalescer's
-- post-triage bulk INSERT and the materializer's `_reinsert_core_graph_from_ir`
-- get the loud-collision guarantee with no per-adapter coordination.
--   The invariant only holds POST-triage: `_coalesce_variable_states` emits one
-- PRE-TRIAGE row per (… data_type, data_length, value_set_id,
-- value_set_version_label, grain) group, so a same-year variable with multiple
-- grains / codings / shapes produces several rows that share (variable_id,
-- register_variant_id, valid_from) and carry value_set_version_label = '' — they
-- collide before A2.2 triage folds them (→ value_set_version_label-discriminated
-- states), splits them (→ sibling variable_ids), or collapses drift. SCB triage
-- writes its rows POST-fold/split (collision-free), and SOS emits one state per
-- distinct windowed (variable, variant, valid_from, version_label), so both feed
-- the index collision-free; a CREATE-time collision would surface a residual
-- triage/era bug loudly. value_set_version_label stays NOT NULL DEFAULT '' so the
-- index bites in the common single-version case. Byte-identity: SQLite stores
-- the CREATE text verbatim in sqlite_master.sql, so this statement is kept on a
-- single line to match the exact text the A4.3a SCB coalescer submitted (a
-- reflowed multi-line form is a real dbdiff schema diff even though the index is
-- semantically identical). Confirmed exit-0 vs the A4.3a baseline.
CREATE UNIQUE INDEX idx_variable_state_unique ON variable_state(variable_id, register_variant_id, valid_from, value_set_version_label);
CREATE INDEX idx_variable_state_value_set
    ON variable_state(value_set_id)
    WHERE value_set_id IS NOT NULL;
-- A2.7: serves `search_variables_by_classification` (filter states by family).
-- Partial — most states carry no classification.
CREATE INDEX idx_variable_state_classification
    ON variable_state(classification_id)
    WHERE classification_id IS NOT NULL;
-- #371: covering index for the #351 coverage aggregates
-- (MIN(valid_from)/MAX(valid_to) span per variable / per register). With
-- (variable_id, valid_from, valid_to) the MIN/MAX is satisfied index-only — no
-- table b-tree lookup — since the leading variable_id groups and the two window
-- bounds are both in the index.
CREATE INDEX idx_variable_state_coverage
    ON variable_state(variable_id, valid_from, valid_to);

-- A2.7: the FULL delivery-column alias history, keyed by `variable_id` (was
-- `cvid` through A2.6). It SURVIVES into the shipped DB — `get_datacolumns`
-- surfaces every historical column, which the coalesced
-- `variable_state.delivery_column_name` (latest era only) can't. A4.3a: the
-- adapter projects the cvid-grained staging onto `variable_id` +
-- `register_variant_id` in `SCBAdapter._emit_variable_aliases` (one
-- IRVariableAlias per historical column), and the MATERIALIZER writes this table
-- from that IR (sole writer). A post-A2.2 `var_id` can be non-unique (split
-- siblings share it), so the projection attributes each cvid's alias to the
-- specific sibling via the ground-truth `variable_instance.variable_id` the
-- coalescer stamped — each sibling surfaces only its own columns in
-- `get_datacolumns`, with no column-tie heuristic and no skip (every cvid
-- resolves). It does not feed resolution (the resolver reads `variable_state`).
CREATE TABLE variable_alias (
    variable_id INTEGER NOT NULL REFERENCES variable(variable_id),
    -- The delivering variant. Lets `get_datacolumns` group columns per variant
    -- as it did off `variable_instance.register_variant_id`.
    register_variant_id INTEGER NOT NULL REFERENCES register_variant(register_variant_id),
    -- `kolumnnamn` → `delivery_column_name`. The SCB delivery column
    -- header (e.g. `PersonNr`, `Kon`, `LopNr_PersonNr`). SCB pseudonymizes
    -- identifier columns at delivery with the `LopNr_` prefix; the metadata
    -- stores the un-prefixed name.
    delivery_column_name TEXT NOT NULL,
    PRIMARY KEY (variable_id, register_variant_id, delivery_column_name)
);

-- Alias validity windows for delivery-column representations. #319 monthly-family
-- merges write sub-annual month windows for 12 month-named columns folded into one
-- annual variable. #945 multi-alias SCB cvids write one state-window row per
-- co-delivered alias column so aliases that can appear in delivered data are
-- picker/order-visible representations instead of search-only headers. Y-132 adds
-- exact source-edition intervals for already-owned aliases absent from SCB's edition
-- metadata. EMPTY for variables with no alias windows (the resolver no-ops then,
-- leaving those variables' behaviour byte-identical). SHIPS — the query layer reads it.
CREATE TABLE variable_alias_window (
    variable_id INTEGER NOT NULL REFERENCES variable(variable_id),
    register_variant_id INTEGER NOT NULL REFERENCES register_variant(register_variant_id),
    delivery_column_name TEXT NOT NULL,
    valid_from TEXT NOT NULL,   -- 'YYYY-MM-DD' inclusive
    valid_to TEXT NOT NULL,     -- 'YYYY-MM-DD' inclusive
    -- NULL for source-derived replacement windows. Curated corrections use
    -- the same `errata:<class>\\n<evidence>` contract as variable_state; the
    -- read side treats non-NULL provenance as an additive representation and
    -- projects it onto VariableState.provenance.
    provenance TEXT,
    PRIMARY KEY (variable_id, register_variant_id, delivery_column_name, valid_from)
);
CREATE INDEX idx_variable_alias_window_lookup
    ON variable_alias_window(variable_id, register_variant_id);

-- External-content projection for `variable_fts`. `variable_alias` stays the
-- normalized source of truth; this view contributes a search-only, deterministic
-- aggregate of historical delivery column names so FTS rebuilds can index the
-- aliases without denormalizing `variable`.
CREATE VIEW variable_fts_content AS
SELECT
    v.variable_id,
    v.register_id,
    v.provider_key,
    v.name,
    v.definition,
    v.description,
    v.operational_definition,
    (
        SELECT json_group_array(delivery_column_name)
        FROM (
            SELECT DISTINCT va.delivery_column_name
            FROM variable_alias va
            WHERE va.variable_id = v.variable_id
            ORDER BY va.delivery_column_name
        )
    ) AS delivery_column_names
FROM variable v;

-- Classifications: normalized code systems (SUN2000, SSYK2012, SNI2007, ...).
-- Populated at build time from a maintainer-curated seed (curation/classifications.toml)
-- that maps raw variable_instance.vardemangdsversion labels to normalized
-- classification rows. See DESIGN.md → Classification seed.
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
    -- Number of canonical codes from the required valid_codes CSV. Fresh builds
    -- keep this equal to the canonical-only code_count.
    valid_code_count INTEGER,
    -- Slug carries the vintage (version baked in): 'sun2020',
    -- 'lkf2007'. The classification FQID is the 2-segment `class/<slug>` —
    -- the standalone `version` column is gone (vintage lives in slug + name +
    -- valid_from/valid_to). UNIQUE, not NOT NULL: `populate_slugs` UPDATEs
    -- this after `populate_classifications` INSERTs the row (NULL at insert),
    -- and SQLite allows multiple NULLs under UNIQUE; the strict NULL-slug guard
    -- in `populate_slugs` enforces presence at build end.
    slug             TEXT UNIQUE
);

-- is_valid: 1 = canonical (listed in the classification's valid_codes CSV).
-- Fresh builds do not emit unknown-validity classification_code rows;
-- nonconforming observed value-set codes live on
-- `classification_conformance_code`, keyed by variable_state.
CREATE TABLE classification_code (
    classification_id INTEGER NOT NULL REFERENCES classification(id),
    code_id           INTEGER NOT NULL REFERENCES value_code(code_id),
    level             INTEGER,
    is_valid          INTEGER,
    PRIMARY KEY (classification_id, code_id)
) WITHOUT ROWID;
CREATE INDEX idx_classification_code_code ON classification_code(code_id);

-- Per-state value-set/classification conformance (#656). A row exists only for
-- a state whose value set declared a CSV-backed classification. `declared_*`
-- preserves the source claim even when the coverage gate clears
-- `variable_state.classification_id` below the overlap floor.
CREATE TABLE classification_conformance (
    state_id INTEGER PRIMARY KEY REFERENCES variable_state(state_id),
    declared_classification_id INTEGER NOT NULL REFERENCES classification(id),
    status TEXT NOT NULL CHECK (status IN ('kept', 'severed')),
    checked_code_count INTEGER NOT NULL,
    matched_code_count INTEGER NOT NULL,
    nonconforming_code_count INTEGER NOT NULL,
    overlap REAL NOT NULL CHECK (overlap >= 0.0 AND overlap <= 1.0)
);
CREATE INDEX idx_classification_conformance_declared
    ON classification_conformance(declared_classification_id);

-- The concrete value-set members that did not belong to the declared canonical
-- classification. Stored instead of recomputed at read time so the UI warning
-- exactly matches the build gate.
CREATE TABLE classification_conformance_code (
    state_id INTEGER NOT NULL REFERENCES classification_conformance(state_id),
    code_id  INTEGER NOT NULL REFERENCES value_code(code_id),
    PRIMARY KEY (state_id, code_id)
) WITHOUT ROWID;
CREATE INDEX idx_classification_conformance_code_code
    ON classification_conformance_code(code_id);

-- Enrichment tables
CREATE TABLE value_code (
    code_id INTEGER PRIMARY KEY,
    -- SCB's `värdekod` / `värdebenämning` become universal `code` / `label`.
    -- Values stay provider-native (SCB code strings like "01", "Man", "").
    code TEXT NOT NULL,
    label TEXT NOT NULL,
    -- Precomputed count of variables carrying this (code, label) from
    -- code_variable_map (#352). Build-time UPDATE after code_variable_map is
    -- complete — search downweights high counts (a generic enum label shared by
    -- many variables is less discriminative than a rare one). Never aggregated
    -- over the 4.1M-row map at query time; JOINed from here instead.
    mapping_count INTEGER NOT NULL DEFAULT 0,
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
    operational_definition,
    delivery_column_names,
    content='variable_fts_content',
    content_rowid='variable_id',
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

-- value_code label search (#352). External-content over value_code, indexing
-- ONLY `label` — the `code` column is matched separately via idx_value_code_code
-- (exact/prefix), since ~55% of codes are purely numeric and useless under FTS.
-- Stoplisted junk labels (see _VALUE_CODE_STOPLIST_EXACT / _PREFIXES) are
-- excluded at population time, so this index has fewer rows than value_code; the
-- leaf value_code / value_set tables keep every row (search-only hiding).
CREATE VIRTUAL TABLE value_code_fts USING fts5(
    label,
    content='value_code',
    content_rowid='code_id',
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

-- Pre-aggregated code→variable mapping for search --value. Built from the
-- year-projected value_set_member rows joined through
-- variable_instance.value_set_id, so a code only appears here for the
-- variables whose value set actually contained it at some cvid year.
-- VARIABLE-grained (not the source `(register, var_id)`): an A2.2 triage split
-- makes sibling variables SHARE one `provider_key`, so a `(register, var_id)`
-- key would fan each code across EVERY sibling — including ones whose own value
-- set excludes it (over-attribution / false positives in value→variable
-- search). The populating cvid belongs to exactly ONE sibling, carried via the
-- coalescer's ground-truth `variable_instance.variable_id` stamp (#150), so the
-- map attributes each code to its true owning sibling. FK to `variable` (not
-- dropped before ship); `register_id`/`var_id` are recoverable through that
-- join, so they're not stored.
CREATE TABLE code_variable_map (
    code_id INTEGER NOT NULL REFERENCES value_code(code_id),
    variable_id INTEGER NOT NULL REFERENCES variable(variable_id),
    PRIMARY KEY (code_id, variable_id)
) WITHOUT ROWID;
-- WITHOUT ROWID, so the (code_id, variable_id) PK can't serve a bare
-- `variable_id` lookup. The #352 codes search annotates each code hit with its
-- owning variables, whose per-variable count correlated-subquery
-- (`COUNT(*) ... WHERE variable_id = ?`) full-scans this 4.1M-row table without
-- this index (inkomst 286s → 0.51s with it). Mirrors idx_value_set_member_code.
-- Additive index → SCHEMA_VERSION stays 5.4.0 (like #371's covering index): an
-- old DB works fine without it, just slower, so it's NOT incompatible — the index
-- lands in the deployed DB at the next reg_meta DB rebuild/release. The released
-- 5.4.0 DB lacks it until then; the codes search falls back to the slow full-scan
-- meanwhile.
CREATE INDEX idx_code_variable_map_variable ON code_variable_map(variable_id);

-- Curated cross-register / cross-provider equivalence edges (see reg_meta/DESIGN.md → Composite registers and source tracking).
-- **Variable grain**: endpoints are `(provider, register, variable)` slug
-- triples. Slug-anchored (not cvid-anchored), so the link survives rebuilds
-- even if provider IDs shift. Each TOML same_as entry becomes two rows
-- (A→B and B→A) so the resolver does a single forward lookup.
--
-- A2.1.5 dropped the v0.11 `a_variant`/`b_variant` and `a_period`/`b_period`
-- slots: a variable is register-scoped, so one edge covers every variant that
-- delivers either variable, and period was never load-bearing for same_as
-- semantics — validity is implicit in both variables' state histories.
-- (the same_as model also reserves a `note` column for curator annotations; not added here
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

-- Non-temporal classification derivation / variant edges (#779). Directional:
-- `derived_slug` is the specialized classification (for example KS87-P),
-- `source_slug` is the classification it derives from (for example ICD-9-KS87).
-- This is deliberately NOT part of `classification_replaced_by`: these edges must
-- never participate in terminal edition walks, `supersedes_id`, or the
-- classification-vintage lift that mints variable_replaced_by.
CREATE TABLE classification_derived_from (
    derived_slug TEXT NOT NULL,
    source_slug  TEXT NOT NULL,
    note         TEXT,
    PRIMARY KEY (derived_slug, source_slug)
) WITHOUT ROWID;
CREATE INDEX idx_classification_derived_from_source
    ON classification_derived_from(source_slug);

-- Derived concept groups (#303): PRESENTATION-ONLY grouping of near-identical
-- catalog rows for browse. Identity is untouched — bindings/orders/stats keep
-- leaf FQIDs and `value_set: "class/<slug>"` keeps referencing the exact
-- vintage; a wrong group is a cosmetic curation bug, not the identity
-- corruption that killed identity-level folding (#223 part 2). Three
-- derivation sources, in priority order (see `concept_groups.py`):
--   'edge'    — connected components of within-register split siblings (ground
--               truth minted by the A2.2 split machinery; zero inference). Fed by
--               the in-build sibling sets the triage minted (`edge_siblings`),
--               never persisted to any shipped table; a curated
--               `[[variable_group]]` claiming a member excludes it from the
--               component (curated precedence). Since #923, curated code↔label
--               decode pairs (`curation/concept_groups.toml`) are ALSO appended to
--               `edge_siblings`, so an `edge` group is NOT exclusively an auto
--               same-definition split — a future feature must not assume that
--               (e.g. must not auto-merge edge-group members into one variable
--               identity).
--   'token'   — exact curated vocabularies only (no regex name-patterns):
--               Swedish month slug tails for variables; 4-digit vintage-year
--               slug tails for classifications (lkf1980…, sni2007).
--   'curated' — maintainer TOML (`reg_meta_build/curation/concept_groups.toml`), e.g.
--               the LISA agi{1,2,3} rank facet over the month groups.
-- A variable/classification belongs to AT MOST ONE group. For classifications
-- the single-column member PK enforces it; for variables the surrogate-keyed
-- member table no longer can, so the validator re-enforces "one group per
-- variable_id" (#819). Derived every build from edges/slugs/TOML;
-- regenerate-not-migrate.
CREATE TABLE concept_group (
    group_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL CHECK (kind IN ('variable', 'classification')),
    -- Scope: variable groups are register-scoped (sibling edges are
    -- within-register; token stems are register-unique slug prefixes);
    -- classification groups are catalog-scoped (slug is globally unique) and
    -- carry NULL.
    register_id INTEGER REFERENCES register(register_id),
    -- Deterministic scope-unique key: min member slug ('edge'), the shared
    -- slug stem ('token'), or the curated key. NOT an FQID segment — a group
    -- is not an addressable entity, only a browse affordance.
    group_key   TEXT NOT NULL,
    label       TEXT NOT NULL,
    source      TEXT NOT NULL CHECK (source IN ('edge', 'token', 'curated')),
    CHECK ((kind = 'variable') = (register_id IS NOT NULL))
);
CREATE UNIQUE INDEX idx_concept_group_key
    ON concept_group(kind, COALESCE(register_id, 0), group_key);
-- Serves `Catalog.list_concept_groups`' per-register lookup. The unique key
-- above leads with `kind` then a COALESCE *expression*, which the bare
-- `register_id` join predicate can't use — without this, every register page
-- load full-scans the ~2,200 groups.
CREATE INDEX idx_concept_group_register ON concept_group(register_id);

-- A group's ordered NAMED facet axes (#819, reversing #585's single-axis
-- collapse). Zero rows for an axis-less group (edge group, or an axis-less
-- curated `[[classification_group]]` umbrella); ONE row for a single-axis group
-- (token 'month', the LISA curated rank facet, a classification umbrella that
-- declares an axis); N rows for a multi-axis curated variable family (the iot
-- disposable-income group: enhet × hushållsbegrepp × kapitalvinst). `ordinal`
-- orders the axes for display; `label` is the axis's human name (e.g. 'månad',
-- 'Enhet'). The classification member facet stays inline on
-- `concept_group_classification`, but its single axis DECLARATION moves here too,
-- so there is ONE read shape for axes across both kinds.
CREATE TABLE concept_group_axis (
    group_id INTEGER NOT NULL REFERENCES concept_group(group_id),
    axis     TEXT NOT NULL,
    ordinal  INTEGER NOT NULL,
    label    TEXT NOT NULL,
    PRIMARY KEY (group_id, axis)
);

-- Variable membership at REPRESENTATION grain (#819): a member is a
-- `(variable_id, delivery_column_name)` point, NOT a whole variable. The
-- representation grain is load-bearing — one variable can hold two coordinates
-- (iot's `delkomponent-disponibel-inkomst` delivers both `CDISP` (incl. capital
-- gains) and `CDISP5` (excl.) under one variable), which a variable-grained
-- member can't express. `delivery_column_name` NULL = a whole-variable member
-- (edge / month / single-axis curated families: the variable IS the member);
-- non-NULL = one representation of the variable (multi-axis curated families).
-- Surrogate `member_id` PK (a plain rowid alias, NOT AUTOINCREMENT — the table is
-- regenerated every build, so no sqlite_sequence gap-freeness is needed) so a
-- variable can appear under several coordinates; the per-member facet coordinates
-- live on `concept_group_variable_facet`. The "at most one group per variable"
-- invariant the old single-column PK enforced is now a validator check (#819).
CREATE TABLE concept_group_variable (
    member_id   INTEGER PRIMARY KEY,
    group_id    INTEGER NOT NULL REFERENCES concept_group(group_id),
    variable_id INTEGER NOT NULL REFERENCES variable(variable_id),
    delivery_column_name TEXT
);
-- COALESCE expression index (mirrors `idx_concept_group_key`): a bare composite
-- UNIQUE over a nullable `delivery_column_name` lets SQLite treat every NULL as
-- distinct, silently admitting duplicate whole-variable members. Folding NULL to
-- '' closes that footgun so `(group_id, variable_id, NULL)` is unique.
CREATE UNIQUE INDEX idx_concept_group_variable_member
    ON concept_group_variable(group_id, variable_id, COALESCE(delivery_column_name, ''));
CREATE INDEX idx_concept_group_variable_group
    ON concept_group_variable(group_id);
-- #819: both indexes above lead with `group_id`, so a probe by `variable_id`
-- ALONE (Catalog._group_ref_for_variable, search member→group folding,
-- _derive_month_groups' per-variable NOT EXISTS) would full-scan without this.
CREATE INDEX idx_concept_group_variable_variable
    ON concept_group_variable(variable_id);

-- Per-member-per-axis facet coordinate (#819, the #585 reversal that restores a
-- `concept_group_variable_facet` table). A whole-variable member on an axis-less
-- group carries ZERO rows; a single-axis member ONE; a multi-axis member ONE per
-- declared axis (the iot members carry 3). `axis` must be one of the member's
-- group's `concept_group_axis` axes (validated). `value` sorts (zero-padded month
-- '05', rank '1', 'individ'); `label` displays.
CREATE TABLE concept_group_variable_facet (
    member_id INTEGER NOT NULL REFERENCES concept_group_variable(member_id),
    axis      TEXT NOT NULL,
    value     TEXT NOT NULL,
    label     TEXT NOT NULL,
    PRIMARY KEY (member_id, axis)
);

-- Classification members carry their facet value/label INLINE (unchanged from
-- #585) — the umbrella members are distinct classifications, one facet each. The
-- group's single axis DECLARATION lives on `concept_group_axis` now (#819), not
-- a `facet_axis` column; an axis-less umbrella (SUN/ISCED/NordDRG) has zero axis
-- rows while members still keep their own short `facet_value`/`facet_label`.
CREATE TABLE concept_group_classification (
    classification_id INTEGER PRIMARY KEY REFERENCES classification(id),
    group_id          INTEGER NOT NULL REFERENCES concept_group(group_id),
    facet_value       TEXT NOT NULL,
    facet_label       TEXT NOT NULL
);
CREATE INDEX idx_concept_group_classification_group
    ON concept_group_classification(group_id);

-- Curated cross-register THEMATIC tag layer (#311). Orthogonal to concept_group
-- (which folds column families *structurally* within one register): a tag cuts
-- *across* providers/registers ("income", "health", …) for discovery without
-- knowing the register. The selected resolved metadata supplies vocabulary and
-- memberships to the common writer; tags leave catalog identity untouched.
--
-- ONE global vocabulary (a tag slug is globally unique — cross-register discovery
-- is the whole point) + ONE polymorphic membership table spanning both grains:
-- register-grain rows for coarse thematic browse, variable-grain rows for the
-- "golden/starred" recommendations (curation says *why* via `note`).
CREATE TABLE tag (
    tag_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    slug        TEXT NOT NULL UNIQUE,
    label       TEXT NOT NULL,
    description TEXT
);

-- Polymorphic membership: EXACTLY ONE of register_id / variable_id is set (the
-- CHECK). `rank` orders members within a tag (curated); `starred` flags a
-- "golden"/recommended member; `note` carries the one-line curation rationale
-- ("primary income measure"). `starred`/`note` are meaningful at the variable
-- grain (a recommended variable) but the columns stay grain-agnostic.
CREATE TABLE tag_member (
    tag_id      INTEGER NOT NULL REFERENCES tag(tag_id),
    register_id INTEGER REFERENCES register(register_id),
    variable_id INTEGER REFERENCES variable(variable_id),
    rank        INTEGER NOT NULL DEFAULT 0,
    starred     INTEGER NOT NULL DEFAULT 0,
    note        TEXT,
    -- Exactly one grain per row (XOR): SQLite has no native XOR, so `!=` over the
    -- two NULL tests does it (one NULL, one non-NULL → 1/true).
    CHECK ((register_id IS NULL) != (variable_id IS NULL))
);
-- Uniqueness per grain: a (tag, register) and a (tag, variable) pair must each be
-- unique. A plain composite key won't enforce it — SQLite treats NULLs as
-- distinct, so the unused-grain NULL would let duplicates through. Two partial
-- UNIQUE indexes (each over the rows where that grain is present) enforce it, and
-- double as the "members-of-tag" lookup for each grain.
CREATE UNIQUE INDEX idx_tag_member_register
    ON tag_member(tag_id, register_id) WHERE register_id IS NOT NULL;
CREATE UNIQUE INDEX idx_tag_member_variable
    ON tag_member(tag_id, variable_id) WHERE variable_id IS NOT NULL;
-- Reverse lookups: tags-of-variable / tags-of-register (the variable/register
-- page surfaces). Partial so they only index the rows of that grain.
CREATE INDEX idx_tag_member_by_variable
    ON tag_member(variable_id) WHERE variable_id IS NOT NULL;
CREATE INDEX idx_tag_member_by_register
    ON tag_member(register_id) WHERE register_id IS NOT NULL;

-- Directional succession edges. Auto-derived from SCB
-- `timeseries_event` rows with `handelse IN ('Ersatt av', 'Ersätter')` by
-- `_materialize_replaced_by_edges`, PLUS curated `type = "replaced_by"` edges
-- from `curation/relations.toml` by `relations.materialize_curated_replaced_by`
-- (#440/#522 — the register/variable grains only). Three sibling tables, one per
-- entity grain
-- (register / variant / variable). Slug-anchored so an edge survives rebuilds
-- even if the underlying provider IDs shift. `note` distinguishes the source:
-- `'auto:timeseries_event'` (auto-derived) vs `'curated:slug_toml'` (the
-- cross-provider / dead-predecessor rows `timeseries_event` can't carry).
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
-- the #440 TOML-curation path can still tell auto from curated; a curated row's
-- own `note` lands here in `beskrivning`). All three sibling tables carry it so
-- they stay structurally identical and the materializer can resolve it
-- uniformly. `effective_year` is populated for the AktuellVariabel variable
-- grain (the successor edition's year) and for any curated row that declares it;
-- the other auto grains leave it NULL (no edition to derive a year from — see
-- `_materialize_replaced_by_edges`).
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

-- Representation-grain succession (#843, feeds #846/#838). The representation
-- grain = a `(variable, delivery_column)` PAIR: a single variable can deliver
-- several columns (parallel representations of one concept — e.g. the iot
-- disposable-income family, the monthly column families), and a column-level era
-- rename ("the disposable-income concept moved from delivery column X to column
-- Y") is a fact the 3-part `variable_replaced_by` grain CANNOT express — both
-- endpoints there collapse to the same variable FQID, so the within-variable
-- column move is invisible. This table records that move. It was the build-side
-- precondition for retiring `column_merge` (#805/#825/#846, now removed): the old
-- column-merge surface unified never-co-occurring era-rename column twins into one
-- variable; expressing that rename as a succession edge between two representations
-- is the navigation-grade replacement for the order-bearing merge.
--
-- An endpoint is a variable-grain FQID `(provider, register, variable)` PLUS a
-- `*_column` segment — NO new FQID grammar (#843 keeps `reg_meta/fqid.py` and the
-- `replaced_by` grain set untouched); the column-ness rides these sibling fields,
-- exactly like #819's `concept_group_variable.delivery_column_name`.
--
-- CURATED-ONLY: there is no auto/event-derived representation grain (SCB's
-- `timeseries_event` succession is entity-grained, never column-level), so every
-- row carries `note = 'curated:slug_toml'` provenance and the human transition
-- reason lands in `beskrivning` (same convention as the register/variable arms).
-- BOTH endpoints must be live (a within-build column rename observes both
-- columns) — stricter than the register/variable "dead predecessor allowed" rule,
-- and intentionally so.
--
-- Graph consumers read representation edges touching an anchor from both
-- directions. The successor-keyed reverse branch needs its own index; the
-- predecessor-first primary key only serves the outbound branch.
--
-- #846 OPTIONAL VARIANT SCOPE: a representation rename usually holds for the
-- WHOLE variable (the default — e.g. RTB `PNR -> PersonNr` across every variant),
-- but some hold only within ONE register-variant. FRIDA's firm key is delivered
-- as `borgnr` (2007-13, 2018-23) and `persorgnr` (2014-17) ONLY in the
-- `punktskatter-for-energi` variant, while 15 sibling variants deliver `borgnr`
-- continuously. Modeled at variable grain that is a false GLOBAL succession + an
-- illegal cycle; scoped to the one variant it is a faithful time-monotone
-- round-trip `borgnr -> persorgnr -> borgnr`. `variant` is a `register_variant`
-- slug (resolved within the edge's register); the default `''` = UNSCOPED, i.e.
-- variable-level (the existing whole-variable semantics). It is in the PK so a
-- variant-scoped edge is DISTINCT from the variable-level one on the same column
-- pair. WITHOUT ROWID forbids a NULL PK column, hence the `''` sentinel rather
-- than a nullable column. A variant-scoped edge always carries `effective_year`
-- (the materializer's time-monotone cycle check needs the ordering).
CREATE TABLE representation_replaced_by (
    predecessor_provider TEXT NOT NULL,
    predecessor_register TEXT NOT NULL,
    predecessor_variable TEXT NOT NULL,
    predecessor_column   TEXT NOT NULL,
    successor_provider   TEXT NOT NULL,
    successor_register   TEXT NOT NULL,
    successor_variable   TEXT NOT NULL,
    successor_column     TEXT NOT NULL,
    -- #846: register_variant slug scoping the succession; '' = unscoped
    -- (variable-level, the default). NOT NULL (WITHOUT ROWID PK column).
    variant              TEXT NOT NULL DEFAULT '',
    effective_year       INTEGER,
    note                 TEXT,
    beskrivning          TEXT,
    PRIMARY KEY (predecessor_provider, predecessor_register, predecessor_variable,
                 predecessor_column,
                 successor_provider, successor_register, successor_variable,
                 successor_column,
                 variant)
) WITHOUT ROWID;
CREATE INDEX idx_representation_replaced_by_successor
    ON representation_replaced_by(
        successor_provider, successor_register, successor_variable
    );

-- Classification EDITION succession (#571): a temporal chain over vintages of
-- ONE classification (ssyk1996→ssyk2012, lkf1980…lkf2026, sun2000-niva→
-- sun2020-niva). Auto-derived from the slug vintage families by
-- `concept_groups.derive_classification_succession` — adjacent-edition edges
-- (y0→y1, y1→y2, …), NOT a presentation facet-picker (editions are a
-- succession, not parallel facets; see #571). A classification slug is GLOBALLY
-- unique, so the edge anchors on slug alone — no provider segment (unlike the
-- entity `*_replaced_by` tables, whose slugs are register-/provider-scoped).
-- DIRECTIONAL like the other `replaced_by` edges: WITHOUT ROWID with a
-- predecessor-first PK, so the clustered prefix serves the forward "what
-- replaced X?" lookup; the reverse "what did X replace?" is served by the
-- successor index below. `effective_year` is the successor edition's year.
-- `note` is PROVENANCE-ONLY for every row: the auto #571 rows stamp
-- `derived:vintage_chain` (incl. the SUN within-dimension chains, whose mid-slug
-- vintage `sun2020-niva` the #747 SUN-scoped stem override keeps bucketing as a
-- family), the CURATED #579 rows (the sun1996 → nivå / inriktning / grupp 1→many
-- split that the same-stem auto rule can't produce, from `curation/relations.toml`
-- `type = "replaced_by"` `class/<slug>` edges) stamp `curated:slug_toml`. Unlike
-- the entity tables there is NO `beskrivning`
-- column, so a classification edge carries no human transition reason (it lives in
-- a `#` comment in relations.toml). (A later PR #516 adds CURATED umbrella
-- classification groups — e.g. SUN — via the retained
-- `concept_group_classification` table; this succession layer is orthogonal.)
CREATE TABLE classification_replaced_by (
    predecessor_slug TEXT NOT NULL,
    successor_slug   TEXT NOT NULL,
    effective_year   INTEGER,
    note             TEXT,
    PRIMARY KEY (predecessor_slug, successor_slug)
) WITHOUT ROWID;
CREATE INDEX idx_classification_replaced_by_successor
    ON classification_replaced_by(successor_slug);

-- Consumer-side binding lineage (STATE grain; see DESIGN.md → Consumer-side lineage (variable_state_lineage)). Materialized by
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


# Sibling provenance DB (see DESIGN.md → Provenance DB sibling). Maintainer-only artifact;
# NOT shipped to consumers, and structurally outside the dbdiff gate — dbdiff
# only ever opens the universal `reg_meta.db`, so populating this sibling file
# is dbdiff-neutral by construction (A4.2). Sits next to the universal DB.
#
# A4.2 populates the tables below from the adapter's emitted IR
# (IRDeliveryProvenance / IRWarning). The tables live ONLY in this sibling DB —
# they touch no universal-schema DDL, so there is NO SCHEMA_VERSION bump
# (SCHEMA_VERSION gates the universal DB only).

# build-db page cache (negative = KiB, so ~2 GiB) applied per-database to both
# `main` and the attached `staging` schema. Keeps the heavy index maintenance and
# the projection DISTINCT/ORDER-BY sorts off disk during the bulk build.


def _unlink_wal_sidecars(db_path: Path) -> None:
    """Remove a SQLite DB's WAL `-wal`/`-shm` sidecar files if present.

    A clean `close()` deletes them, but a subsequent read-only open
    (`open_db(..., mode=ro)`) re-creates them, and a read-only close leaves
    them on disk. They must be cleared before an atomic base-file replace,
    which moves only `<db>` and would otherwise orphan `<db>-wal`/`<db>-shm`.
    """
    for sidecar in ("-wal", "-shm"):
        db_path.with_name(db_path.name + sidecar).unlink(missing_ok=True)


def _require_publishable_catalog(conn: sqlite3.Connection, db_path: Path) -> None:
    """Protect builder activation while leaving explicit diagnostic reads possible."""
    manifest = get_manifest(conn)
    if (
        manifest.get("catalog_artifact_kind", "catalog") != "catalog"
        or manifest.get("catalog_publishable", "true") != "true"
        or manifest.get("catalog_completeness", "complete") != "complete"
    ):
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="catalog_not_publishable",
            error_class="configuration",
            message=f"Diagnostic or incomplete catalog cannot be installed: {db_path}",
            remediation="Use an explicit local inspection path; resolve its blockers and run a strict build before publication.",
        )


def publish_db(tmp_path: Path, final_path: Path) -> None:
    """Install the staged DB at `tmp_path` as the live DB at `final_path`.

    The single publication step shared by `build_db` (universal + provenance)
    and `extend_db`. Callers finish building and validating the staged file
    first; this only publishes it.

    The live name must never disappear: readers hold `<final_path>` while a
    maintainer rebuilds, so the previous generation is preserved by
    HARD-LINKING it aside to `<final_path>.prev` — the published file is
    immutable and a link is O(1) on a multi-GB DB — and the new generation is
    then installed by ONE atomic `Path.replace`. Both failure modes leave the
    live DB present with its original bytes: a failing backup raises before
    the replace, and a failing replace never touched the live name.

    `.prev` keeps a single previous generation (any prior one is evicted), with
    no auto-cleanup of older ones — maintainers `mv` the `.prev` aside if they
    want to keep more than one. It is the weaker promise: a failure after the
    eviction leaves it missing or holding the still-live generation, and the
    re-run restores it. The live DB is what must survive.

    The staged file's WAL sidecars are dropped first (see
    `_unlink_wal_sidecars`) — the post-build validator's read-only open leaves
    them behind, and the replace moves only the base file.
    """
    with closing(
        sqlite3.connect(tmp_path.resolve().as_uri() + "?mode=ro", uri=True)
    ) as conn:
        conn.row_factory = sqlite3.Row
        # The companion source-provenance DB uses this atomic placement helper
        # too, but has no catalog manifest or publication contract.
        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='import_manifest'"
        ).fetchone():
            _require_publishable_catalog(conn, tmp_path)
    _unlink_wal_sidecars(tmp_path)
    if final_path.exists():
        prev_path = final_path.with_name(final_path.name + ".prev")
        prev_path.unlink(missing_ok=True)
        prev_path.hardlink_to(final_path)
    tmp_path.replace(final_path)


# ---------------------------------------------------------------------------
# CSV reading
# ---------------------------------------------------------------------------


def _scb_snapshot_error(exc: Exception) -> RegMetaError:
    from .input_snapshot import SnapshotMaterializationError

    if isinstance(exc, SnapshotMaterializationError):
        return RegMetaError(
            exit_code=EXIT_CONFIG,
            code="scb_snapshot_materialization_required",
            error_class="configuration",
            message=str(exc),
            remediation=exc.hydration_action,
        )
    return RegMetaError(
        exit_code=EXIT_CONFIG,
        code="scb_snapshot_invalid",
        error_class="configuration",
        message=f"Selected SCB input snapshot is invalid: {exc}",
        remediation=(
            "Run the explicit snapshot verifier. If the selected identity changed "
            "or is unsupported, prepare, verify, and accept a new snapshot, then "
            "pass its exact Git commit and manifest SHA-256."
        ),
    )


def _paths_overlap(destinations: set[Path], inputs: set[Path]) -> bool:
    # The report temporary file is opened before replacement; a symlink or hard
    # link there must not turn a distinct-looking report into an input overwrite.
    return any(
        destination == input_path
        or (input_path.is_dir() and destination.is_relative_to(input_path))
        or (
            destination.exists()
            and input_path.exists()
            and destination.samefile(input_path)
        )
        for destination in destinations
        for input_path in inputs
    )


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


@contextmanager
def _open_scb_source_raw(
    path: Path, snapshot: ScbSnapshotReader | None
) -> Iterator[tuple[list[str], Iterator[list[str | None]]]]:
    if snapshot is None:
        with path.open("rb") as raw_handle:
            text_handle = io.TextIOWrapper(raw_handle, encoding="latin-1", newline="")
            reader = csv.reader(text_handle, delimiter="|", quotechar='"')
            try:
                header = next(reader)
            except StopIteration as exc:
                raise RegMetaError(
                    exit_code=EXIT_CONFIG,
                    code="csv_empty",
                    error_class="configuration",
                    message=f"CSV file is empty: {path.name}",
                    remediation="Re-export the file from mikrometadata.scb.se.",
                ) from exc

            yield header, cast("Iterator[list[str | None]]", reader)
        return

    from .input_snapshot import SnapshotError

    try:
        with snapshot.open_csv(path.name) as (raw_header, raw_rows):
            header = ["" if value is None else value for value in raw_header]

            yield header, raw_rows
    except SnapshotError as exc:
        raise _scb_snapshot_error(exc) from exc


@contextmanager
def _open_scb_csv_rows(
    path: Path,
    snapshot: ScbSnapshotReader | None = None,
) -> Iterator[tuple[list[str], Iterator[tuple[int, list[str | None]]]]]:
    """Share SCB header and row-width validation before cell interpretation."""
    with _open_scb_source_raw(path, snapshot) as (raw_header, reader):
        header = _validated_scb_header(path.name, raw_header)
        ncols = len(header)

        def rows() -> Iterator[tuple[int, list[str | None]]]:
            for row_number, fields in enumerate(reader, start=2):
                if len(fields) != ncols:
                    raise RegMetaError(
                        exit_code=EXIT_CONFIG,
                        code="csv_bad_row",
                        error_class="configuration",
                        message=f"Row {row_number} in {path.name} has {len(fields)} fields, expected {ncols}.",
                        remediation="Re-export the file from mikrometadata.scb.se.",
                    )
                yield row_number, fields

        yield header, rows()


@contextmanager
def _open_scb_csv_raw(
    path: Path,
    snapshot: ScbSnapshotReader | None = None,
) -> Iterator[tuple[list[str], Iterator[tuple[int, list[str]]]]]:
    """Open a pipe-delimited cp1252 CSV; yield (header, raw-field-list iterator).

    Same open + header/field-count validation as `_open_scb_csv`, but each row
    is the RAW latin-1 field LIST — NOT decoded, NOT keyed into a dict. The
    102M-row Vardemangder loop indexes columns positionally and decodes only the
    few it keeps; per-row dict-building and per-field `_decode_cp1252` otherwise
    dominate the whole build. Prepared NULLs are normalized in place so this
    hot default traversal retains the source row list without a per-row copy or
    presence sidecar. The header IS decoded (cheap, once).
    """
    with _open_scb_csv_rows(path, snapshot) as (header, source_rows):
        if snapshot is None:
            yield header, cast("Iterator[tuple[int, list[str]]]", source_rows)
            return

        def raw_rows() -> Iterator[tuple[int, list[str]]]:
            for row_number, fields in source_rows:
                for index, value in enumerate(fields):
                    if value is None:
                        fields[index] = ""
                yield row_number, cast("list[str]", fields)

        yield header, raw_rows()


def _validated_scb_header(filename: str, raw_header: Sequence[str]) -> list[str]:
    """Decode and validate one SCB CSV header at the interpretation boundary."""
    header = [_decode_cp1252(value) for value in raw_header]
    expected = EXPECTED_HEADERS.get(filename)
    if expected and header != expected:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="csv_bad_header",
            error_class="configuration",
            message=f"Unexpected header in {filename}.",
            remediation="Ensure the file is an unmodified SCB metadata export.",
        )
    return header


@contextmanager
def _open_scb_csv(
    path: Path,
    snapshot: ScbSnapshotReader | None = None,
) -> Iterator[tuple[list[str], Iterator[tuple[int, dict[str, str]]]]]:
    """Open a pipe-delimited cp1252 CSV and yield (header, row_iterator).

    Reads bytes as latin-1 (single-byte passthrough), validates against
    known-invalid cp1252 bytes, then decodes to proper cp1252 text. Each row is
    a fully-decoded ``{column: value}`` dict. Built on `_open_scb_csv_raw`; hot
    paths that don't need every column decoded should use the raw helper.
    """
    with _open_scb_csv_raw(path, snapshot) as (header, raw_rows):

        def row_iter() -> Iterator[tuple[int, dict[str, str]]]:
            for row_number, fields in raw_rows:
                yield (
                    row_number,
                    {h: _decode_cp1252(v) for h, v in zip(header, fields, strict=True)},
                )

        yield header, row_iter()


@contextmanager
def _open_scb_csv_prepared(
    path: Path,
    snapshot: ScbSnapshotReader,
) -> Iterator[
    tuple[list[str], Iterator[tuple[int, dict[str, tuple[bool, str | None, str]]]]]
]:
    """Yield lossless prepared cells through the normal SCB validation traversal.

    Each cell is ``(present, raw, interpreted)``.  ``present`` distinguishes a
    prepared NULL from a supplied empty scalar; interpreted values use the same
    cp1252 repair as :func:`_open_scb_csv`.
    """
    with _open_scb_csv_rows(path, snapshot) as (header, raw_rows):

        def rows() -> Iterator[tuple[int, dict[str, tuple[bool, str | None, str]]]]:
            for row_number, fields in raw_rows:
                yield (
                    row_number,
                    {
                        name: (
                            value is not None,
                            value,
                            _decode_cp1252(value or ""),
                        )
                        for name, value in zip(header, fields, strict=True)
                    },
                )

        yield header, rows()


def _decode_cp1252(raw: str) -> str:
    """Decode a latin-1-read string to proper cp1252.

    Bytes undefined in cp1252 but present as DOS cp850 remnants are mapped
    to their cp850 equivalents instead of rejecting the whole import.

    ASCII fast path: for a pure-ASCII string (the overwhelmingly common case
    across every SCB CSV), latin-1, cp1252, and the read string all agree on
    0x00–0x7F and none of the DOS-remnant fixup bytes (all >= 0x81) can occur —
    so the input is already correct and the encode + per-byte scan are skipped.
    """
    if raw.isascii():
        return raw
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


def _timing_enabled() -> bool:
    """True when per-stage build timing should be emitted.

    Opt-in via ``--timing`` (build-db) / ``REG_META_BUILD_TIMING=1`` — off by
    default so normal builds stay quiet. Checked at call time, not import, so the
    CLI flag (which sets the env var) takes effect.
    """
    return os.environ.get("REG_META_BUILD_TIMING") == "1"


def _emit_timing(label: str, t0: float) -> None:
    """Emit a greppable ``[timing] <label>: <s>`` stderr line if timing is on."""
    if _timing_enabled():
        _progress(f"[timing] {label}: {time.perf_counter() - t0:.1f}s")


# A2.3: SCB ships succession in `timeseries_event` under two handelse values.
# `Ersatt av` is the canonical direction (id1 was replaced by id2); `Ersätter`
# is the inverse (id1 replaces id2). SCB usually emits both rows for a single
# transition, so the materializer collapses them onto one predecessor →
# successor edge.
# Four entity grains. `Register` / `RegisterVariant` land in their own tables;
# `AktuellVariabel` (cvid) and `Variabel` (var_id) both resolve to the variable
# grain and land in `variable_replaced_by`.
# Source-of-truth marker for the auto-derive path. Distinguishes from the
# TOML-curated rows (#440 — cross-provider / dead-predecessor succession not
# visible in SCB's `timeseries_event`).
# Provenance marker for the curated-TOML path (#440). A row's own `note` (the
# human transition reason) lands in `beskrivning`; this fixed marker lands in
# `note`, mirroring the auto path so a consumer can tell curated from auto-derived.

# Manifest stat keys for replaced_by materialization. Single source so the
# `skip_slugs` zero-fill (in `build_db`) and the materializer's real return
# can't drift apart — `test_replaced_by_stats_in_manifest` pins the exact set.


# A4.4e: the SCB feed of the provider-blind `classification_candidate` table — a
# verbatim projection of exactly the rows `_backfill_state_classifications` used
# to read directly off `variable_instance`. Shared as a single source of truth so
# the byte-identical-gated filter (`classification_id IS NOT NULL AND variable_id
# IS NOT NULL`) cannot drift between the build feed and its test.


def _populate_fts(conn: sqlite3.Connection, *, include_value_code: bool = True) -> None:
    """Populate FTS5 search indexes.

    ``include_value_code=False`` skips ONLY the ``value_code_fts`` INSERT — the
    register_fts + variable_fts inserts always run. The extend-db overlay
    (#365 PR2) uses this: it never inserts ``value_code`` rows, so the
    value_code_fts index copied from the base DB is already in sync and
    re-populating its ~4M rows would be pure build-time waste. The full build
    keeps the default ``True``, so its ``_populate_fts(conn)`` call is unchanged.
    """
    _progress("Building search indexes...")

    # register_fts: content-synced — rowid must match register.rowid
    # (register_id is INTEGER PRIMARY KEY, so rowid = register_id)
    conn.execute(
        "INSERT INTO register_fts(rowid, register_id, name, purpose) "
        "SELECT rowid, register_id, name, purpose FROM register"
    )

    # variable_fts: content-synced with `variable_fts_content`, which derives a
    # delivery-column aggregate from `variable_alias` without storing it on
    # `variable`.
    conn.execute("""
        INSERT INTO variable_fts(rowid, register_id, provider_key, name, definition, description, operational_definition, delivery_column_names)
        SELECT
            variable_id,
            register_id,
            provider_key,
            name,
            definition,
            description,
            operational_definition,
            delivery_column_names
        FROM variable_fts_content
    """)

    if include_value_code:
        # value_code_fts (#352): content-synced — rowid must match
        # value_code.code_id. Indexes only `label`; stoplisted junk labels are
        # excluded HERE so they never surface in search, while value_code keeps
        # every row. The exclusion is a whole-label match (exact set OR sentinel
        # prefix family); built from the two stoplist constants so the curated
        # list lives in one place.
        exact_placeholders = ",".join("?" * len(_VALUE_CODE_STOPLIST_EXACT))
        prefix_clauses = " OR ".join(
            "label LIKE ?" for _ in _VALUE_CODE_STOPLIST_PREFIXES
        )
        # Owner filter (#478): a code is indexed only if it has an owner —
        # `mapping_count = 0` ⟺ no variable owner (mapping_count is the
        # code_variable_map count, UPDATEd above this call), and the
        # `OR classification_code` arm keeps classification-owned codes
        # searchable (they have no value_set_member after the year-projection,
        # yet are findable ONLY via value_code_fts because classification search
        # is name-only). This MIRRORS the query-side owner definition in
        # reg_meta/queries.py `_code_owner_annotations_batch` (variables via
        # code_variable_map ∪ classifications via classification_code), which the
        # register-scoped drop at queries.py:944 already applies — the unscoped
        # path defers owner annotation to the shown page and so cannot drop the
        # ~2,562 ownerless year-projection orphans there. Mirroring at index
        # build is source-agnostic: every orphaning pass converges here. The
        # correlated reference is qualified `value_code.code_id` (NOT bare
        # `code_id`, which would bind to classification_code.code_id inside the
        # subquery); `idx_classification_code_code` makes the EXISTS fast.
        owner_clause = (
            "(mapping_count > 0 "
            "OR EXISTS (SELECT 1 FROM classification_code cc "
            "WHERE cc.code_id = value_code.code_id))"
        )
        stoplist_where = (
            f"label NOT IN ({exact_placeholders}) AND NOT ({prefix_clauses})"
        )
        stoplist_params = (
            *sorted(_VALUE_CODE_STOPLIST_EXACT),
            *(f"{p}%" for p in _VALUE_CODE_STOPLIST_PREFIXES),
        )
        conn.execute(
            "INSERT INTO value_code_fts(rowid, label) "
            "SELECT code_id, label FROM value_code "
            f"WHERE {stoplist_where} AND {owner_clause}",
            stoplist_params,
        )
        # Drift visibility (#478): report how many codes the owner filter
        # EXCLUDED that would otherwise have passed the stoplist. Reuses the SAME
        # stoplist construction; only the NEGATION of the owner clause is added.
        (n_excluded,) = conn.execute(
            "SELECT COUNT(*) FROM value_code "
            f"WHERE {stoplist_where} AND NOT {owner_clause}",
            stoplist_params,
        ).fetchone()
        if n_excluded > 0:
            _progress(
                f"  {n_excluded:,} context-less value_codes excluded "
                "from value search (#478)"
            )
    _progress("  FTS indexes built")


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


# ---------------------------------------------------------------------------
# Provider-blind materializer (A4.1)
# ---------------------------------------------------------------------------

# Sentinels the universal DDL applies as NOT NULL DEFAULTs; the IR contract
# carries None / open-ended, so the materializer reconciles them at the insert
# site ("None-to-sentinel reconciliation").
_VALID_TO_SENTINEL = "9999-12-31"  # variable_state.valid_to open-ended
_VALID_FROM_UNKNOWN = "0001-01-01"  # variable_state.valid_from start unknown


def _insert_core_graph_from_ir(
    conn: sqlite3.Connection,
    *,
    registers: list[IRRegister],
    variants: list[IRVariant],
    variables: list[IRVariable],
    states: list[IRVariableState],
    aliases: list[IRVariableAlias],
    alias_windows: list[IRVariableAliasWindow],
    provider_ids: dict[str, int] | None = None,
) -> None:
    """Insert a provider-shaped core graph from adapter IR.

    The global materializer calls this after clearing adapter-written rows; the
    steward overlay calls it additively on a released DB. ``provider_ids`` is
    supplied only for the latter because steward providers are not global seed
    rows.
    """

    def provider_id(provider: str) -> int:
        if provider_ids is None:
            return _provider_id_for(provider)
        try:
            return provider_ids[provider]
        except KeyError as exc:
            raise RegMetaError(
                exit_code=EXIT_CONFIG,
                code="unknown_provider",
                error_class="configuration",
                message=f"No provider_id for provider {provider!r}.",
                remediation="Declare the provider before inserting its register IR.",
            ) from exc

    conn.executemany(
        "INSERT INTO register (register_id, provider_id, name, purpose, slug) "
        "VALUES (:register_id, :provider_id, :name, :purpose, NULL)",
        [
            {
                "register_id": r.register_id,
                "provider_id": provider_id(r.provider),
                "name": r.name,
                "purpose": r.purpose,
            }
            for r in registers
        ],
    )

    conn.executemany(
        "INSERT INTO register_variant "
        "(register_variant_id, register_id, name, description, slug) "
        "VALUES (:register_variant_id, :register_id, :name, :description, NULL)",
        [
            {
                "register_variant_id": v.register_variant_id,
                "register_id": v.register_id,
                "name": v.name,
                "description": v.description,
            }
            for v in variants
        ],
    )

    # `is_sensitive`/`is_identifier` are INTEGER columns; the IR carries bools.
    # `provider_key` is the NON-unique join hint. `slug` inserts NULL (the
    # populate_variable_slugs UPDATE pass fills it). `source_label` is the
    # resolved source-register display label (IRVariable.source_label).
    conn.executemany(
        "INSERT INTO variable "
        "(variable_id, register_id, provider_key, slug, name, definition, "
        " description, operational_definition, source_register_text, "
        " measurement_unit, source_register_id, "
        " source_label, is_sensitive, is_identifier) "
        "VALUES (:variable_id, :register_id, :provider_key, NULL, :name, "
        " :definition, :description, :operational_definition, "
        " :source_register_text, :measurement_unit, "
        " :source_register_id, :source_label, :is_sensitive, :is_identifier)",
        [
            {
                "variable_id": v.variable_id,
                "register_id": v.register_id,
                "provider_key": v.provider_key,
                "name": v.name,
                "definition": v.definition,
                "description": v.description,
                "operational_definition": v.operational_definition,
                "source_register_text": v.source_register_text,
                "measurement_unit": v.measurement_unit,
                "source_register_id": v.source_register_id,
                "source_label": v.source_label,
                "is_sensitive": int(v.is_sensitive),
                "is_identifier": int(v.is_identifier),
            }
            for v in variables
        ],
    )

    # None→sentinel reconciliation at the insert site: valid_to=None →
    # '9999-12-31', value_set_version_label=None → '' (the DDL NOT NULL
    # DEFAULTs). classification_id is left NULL; _backfill_state_classifications
    # tags it after classifications + value-set linkage exist.
    conn.executemany(
        "INSERT INTO variable_state "
        "(state_id, variable_id, register_variant_id, valid_from, valid_to, "
        " data_type, data_length, delivery_column_name, source_register_text, "
        " operational_definition, provenance, value_set_id, "
        " value_set_version_label, classification_id) "
        "VALUES (:state_id, :variable_id, :register_variant_id, :valid_from, "
        " :valid_to, :data_type, :data_length, :delivery_column_name, "
        " :source_register_text, :operational_definition, :provenance, :value_set_id, "
        " :value_set_version_label, NULL)",
        [
            {
                "state_id": s.state_id,
                "variable_id": s.variable_id,
                "register_variant_id": s.register_variant_id,
                "valid_from": s.valid_from
                if s.valid_from is not None
                else _VALID_FROM_UNKNOWN,
                "valid_to": s.valid_to
                if s.valid_to is not None
                else _VALID_TO_SENTINEL,
                "data_type": s.data_type,
                "data_length": s.data_length,
                "delivery_column_name": s.delivery_column_name,
                "source_register_text": s.source_register_text,
                "operational_definition": s.operational_definition,
                "provenance": s.provenance,
                "value_set_id": s.value_set_id,
                "value_set_version_label": s.value_set_version_label or "",
            }
            for s in states
        ],
    )
    # The post-triage state-uniqueness index was created by the ACTIVE
    # ADAPTER's coalescer (e.g. SCBAdapter, scb.py) on the now-deleted rows; the
    # DELETE above leaves the index in place, so it continues to bite on the
    # re-inserted rows (an INSERT collision raises — the same loud failure the
    # coalescer's CREATE provided). NOTE: this is an adapter-side precondition,
    # NOT a universal-DDL guarantee — A4.3b's SOS adapter must create the same
    # index (or it moves to universal DDL) for this guard to hold provider-blind.

    # variable_alias: the FULL historical column set (one row per historical
    # column). INSERT OR IGNORE dedups (the IR already emits DISTINCT rows; the
    # guard mirrors the old re-parent pass's dedup defensively).
    conn.executemany(
        "INSERT OR IGNORE INTO variable_alias "
        "(variable_id, register_variant_id, delivery_column_name) "
        "VALUES (?, ?, ?)",
        [
            (a.variable_id, a.register_variant_id, a.delivery_column_name)
            for a in aliases
        ],
    )

    conn.executemany(
        "INSERT OR IGNORE INTO variable_alias_window "
        "(variable_id, register_variant_id, delivery_column_name, valid_from, valid_to, "
        "provenance) VALUES (?, ?, ?, ?, ?, ?)",
        [
            (
                w.variable_id,
                w.register_variant_id,
                w.delivery_column_name,
                w.valid_from if w.valid_from is not None else _VALID_FROM_UNKNOWN,
                w.valid_to if w.valid_to is not None else _VALID_TO_SENTINEL,
                w.provenance,
            )
            for w in alias_windows
        ],
    )


# The directional code-less ↔ code-bearing overlap JOIN, shared by the three
# code-less overlap SELECTs in this module (`_drop_fullcover_codeless_states`'
# main overlap query and `_resolve_curated_codeless_overlaps`' main overlap query
# + its post-resolution mandatory-curation gate). Centralizes the overlap
# semantics so they can't drift apart: null-safe `delivery_column_name IS` column
# match (two NULL delivery columns still pair); code-less (`cl.value_set_id IS
# NULL`) ↔ code-bearing (`cb.value_set_id IS NOT NULL`) direction; closed-interval
# intersection. The ON clause references only `cl`/`cb`, so it is valid in any of
# the three SELECTs regardless of the slug JOINs (`variable`/`register`/`provider`)
# that #2/#3 prepend. Leading/trailing spaces keep the assembled SQL valid when
# concatenated.
#
# MUST stay in sync with validate.py's `_check_one_value_set_per_period` /
# `_check_no_codeless_codebearing_overlap`, which assert the same overlap class
# with a structurally DIFFERENT *symmetric* `a`/`b` form (not this directional
# `cl`/`cb` form) — so they are NOT folded into this constant; a change here must
# be mirrored there by hand.


def _provider_id_for(provider: str) -> int:
    """Map an IR provider slug to its stable `provider.provider_id` seed value."""
    for pid, slug, _name in _PROVIDER_SEED:
        if slug == provider:
            return pid
    raise RegMetaError(
        exit_code=EXIT_CONFIG,
        code="unknown_provider",
        error_class="configuration",
        message=f"No provider_id seed for provider {provider!r}.",
        remediation="Add the provider to _PROVIDER_SEED.",
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _reject_input_repository_destination(
    path: Path, repository: Path, *, label: str
) -> None:
    """Reject a build output that would dirty the accepted input checkout."""
    if path == repository or path.is_relative_to(repository):
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="catalog_input_output_conflict",
            error_class="configuration",
            message=f"{label} must stay outside the accepted input repository: {path}",
            remediation=(
                "Choose a scratch/output path outside the catalog-inputs Git checkout."
            ),
        )
