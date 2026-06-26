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
import os
import sqlite3
import struct
import sys
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from reg_meta.db import (
    DB_FILENAME,
    SCHEMA_VERSION,
    utc_now,
)
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.queries import extract_year

from .classification_links import (
    load_classification_links,
    materialize_classification_links,
    repo_classification_links_path,
)
from .classifications import (
    derive_supersedes_from_edges,
    link_value_set_classifications,
    populate_classifications,
    repo_seed_path,
)
from .concept_groups import (
    EDGE_RELATION_KIND,
    derive_classification_succession,
    load_classification_groups,
    load_concept_group_accepts,
    load_concept_groups,
    materialize_concept_groups,
    repo_concept_groups_auto_path,
    repo_concept_groups_path,
)
from .delivery_enrichment import (
    apply_delivery_enrichment,
    load_delivery_enrichment,
    repo_delivery_enrichment_path,
)
from .fqid_slugs import (
    load_lineage_config,
    populate_slugs,
    populate_variable_slugs,
    repo_slug_dir,
    slug_dir_curates_canonical_scb,
)
from .ir import (
    IRDeliveryProvenance,
    IRRegister,
    IRValueSet,
    IRVariable,
    IRVariableAlias,
    IRVariableState,
    IRVariant,
    IRWarning,
)
from .period_family_merges import (
    load_period_family_merges,
    materialize_period_family_merges,
    repo_period_family_merges_path,
)
from .relations import (
    derive_variable_vintage_succession,
    load_relations,
    materialize_curated_replaced_by,
    materialize_related_to,
    materialize_same_as,
    repo_relations_path,
)
from .tags import (
    load_tags,
    materialize_tags,
    repo_tags_path,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

    from .relations import CuratedReplacedBy

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
    (PROVIDER_ID_SCB, "scb", "Statistics Sweden"),
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
_CANONICAL_SCB_DIRNAME = "scb_canonical"

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

# value_code label-search stoplist (#352). Junk labels excluded from the
# value_code_fts INDEX ONLY at population time — the leaf value_code / value_set
# tables keep every row (this hides them from search, it does NOT drop data).
# UNRELATED to `_VARDEMANGDER_SENTINELS` above (those gate value-set hashing /
# row import). This is a whole-LABEL exclusion, not an FTS tokenizer stopword
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

-- A2.6: BUILD-TIME-ONLY (dropped before ship, like `unika_summary`). The
-- coalescer reads `registerversionnamn` for the variable_state valid_from/to
-- year fallback, and the lineage linkers derive a per-edition period from it;
-- both run before `DROP TABLE register_version`. The FQID grammar no longer
-- has a version segment (see reg_meta/DESIGN.md → FQID grammar), so this table carries NO `slug` column — period
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
-- query layer reads them; their content belongs in the provenance DB (see
-- DESIGN.md → Provenance DB sibling). Kept build-time only because the importer still populates them from the
-- same Registerinformation.csv pass.
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
    -- Universal-vocabulary rename. Values stay provider-native. `variabeloperationell_definition`
    -- merges into `description` at ingest when distinct + non-empty;
    -- `variabelreferenstid`, `variabelhamtadfran`, and `variabelextern_kommentar`
    -- are dropped. `variabelregister_kalla` (raw attribution text)
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
    -- post-hoc column-tie heuristic, no skip. No FK and no
    -- index: build-time-only (dropped with the table, before
    -- `PRAGMA foreign_key_check`), values valid by construction, and every
    -- reader joins from the cvid PK side. Distinct from the natural-key note
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

-- #319: per-month alias windows for CURATED MONTHLY-FAMILY merges. A monthly
-- family (12 month-named delivery columns, e.g. lisa `agi1lonfink{jan..dec}`,
-- shipped inside ANNUAL editions) is merged at build time into ONE variable
-- carrying an annual `variable_state` per delivery year (NOT 12 — the per-month
-- dimension is a representation/alias concern, not a coding boundary; see
-- reg_meta_build/DESIGN.md → Consumers: monthly column families). This sibling
-- table records each month column's validity window so `resolve_at` can pick the
-- column for a queried sub-annual period: `resolve_at("2024-03")` → the `mar`
-- column. EMPTY for every non-merged variable (the resolver no-ops then, leaving
-- those variables' behaviour byte-identical). Windows are `YYYY-MM` expanded via
-- `period_token_to_bounds` (same `_MONTH_LAST_DAY` ends the display formatter
-- reads back). SHIPS — the query layer reads it.
CREATE TABLE variable_alias_window (
    variable_id INTEGER NOT NULL REFERENCES variable(variable_id),
    register_variant_id INTEGER NOT NULL REFERENCES register_variant(register_variant_id),
    delivery_column_name TEXT NOT NULL,
    valid_from TEXT NOT NULL,   -- 'YYYY-MM-DD' inclusive
    valid_to TEXT NOT NULL,     -- 'YYYY-MM-DD' inclusive
    PRIMARY KEY (variable_id, register_variant_id, delivery_column_name, valid_from)
);
CREATE INDEX idx_variable_alias_window_lookup
    ON variable_alias_window(variable_id, register_variant_id);

-- Classifications: normalized code systems (SUN2000, SSYK2012, SNI2007, ...).
-- Populated at build time from a maintainer-curated seed (classifications.toml)
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
    -- Number of canonical codes when a valid_codes CSV was provided; NULL
    -- otherwise. valid_code_count <= code_count is *not* invariant: canonical
    -- codes that never appeared in any observed instance still count, but
    -- observed-only noise codes inflate code_count.
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

-- Meaningful "see also" links (variable grain). Carries TWO kinds of row:
--   * A2.2 triage `auto:triage` edges between the distinct `variable` rows a
--     *split* produced (disjoint columns lumped under one source `var_id`), but
--     ONLY the NON-FOLDABLE split reasons — `code_vs_label_pair` (a code/label
--     column pair) and `import_bug_suspect`. The bulk MECHANICAL
--     `same_definition_different_column` siblings are NOT stored here (#591):
--     they only ever fed the concept-group edge fold, which now reads the
--     in-build sibling sets directly (`edge_siblings`), so persisting ~134k
--     rows the read side never surfaced as anything but the concept group was
--     dead weight.
--   * curated `[[edge]] type = "related_to"` see-also links (a DISJOINT,
--     non-foldable `relation_kind` vocabulary; `note` distinguishes them).
-- Stored in BOTH directions (like `variable_same_as`) so the a-side PK prefix
-- serves `Catalog.related(x)` without a second b-side scan; each pair yields two
-- rows. `relation_kind` reflects the split/curation reason; `note` carries
-- provenance (`auto:triage` for build-emitted edges, vs. a curated marker).
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

-- Derived concept groups (#303): PRESENTATION-ONLY grouping of near-identical
-- catalog rows for browse. Identity is untouched — bindings/orders/stats keep
-- leaf FQIDs and `value_set: "class/<slug>"` keeps referencing the exact
-- vintage; a wrong group is a cosmetic curation bug, not the identity
-- corruption that killed identity-level folding (#223 part 2). Three
-- derivation sources, in priority order (see `concept_groups.py`):
--   'edge'    — connected components of within-register
--               `same_definition_different_column` split siblings (ground truth
--               minted by the A2.2 split machinery; zero inference). Fed by the
--               in-build sibling sets, NOT a `variable_related_to` round-trip
--               (#591); a curated `[[variable_group]]` claiming a member excludes
--               it from the component (curated precedence).
--   'token'   — exact curated vocabularies only (no regex name-patterns):
--               Swedish month slug tails for variables; 4-digit vintage-year
--               slug tails for classifications (lkf1980…, sni2007).
--   'curated' — maintainer TOML (`reg_meta_build/concept_groups.toml`), e.g.
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
-- knowing the register. Curated from `tags.toml`; derived every build
-- (regenerate-not-migrate); a presentation/discovery overlay that leaves identity
-- untouched. Tables ship EMPTY until curation content lands (machinery first).
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
PROVENANCE_DB_FILENAME = "reg_meta.provenance.db"

# build-db page cache (negative = KiB, so ~2 GiB) applied per-database to both
# `main` and the attached `staging` schema. Keeps the heavy index maintenance and
# the projection DISTINCT/ORDER-BY sorts off disk during the bulk build.
_BUILD_PAGE_CACHE_KIB = -2_000_000

PROVENANCE_DDL = """\
-- Ties this provenance DB to the exact universal DB it was built against.
CREATE TABLE build_manifest (
    schema_version TEXT NOT NULL,
    universal_db_path TEXT NOT NULL,
    universal_db_sha256 TEXT NOT NULL,
    build_date TEXT NOT NULL
);

-- Per-provider source-ID linkage. For SCB the
-- universal register_id IS the source RegisterId, so this records the native
-- register name alongside it for maintainer debugging.
CREATE TABLE scb_register_id_map (
    register_id INTEGER NOT NULL,
    scb_registernamn TEXT NOT NULL,
    scb_imported_at TEXT NOT NULL,
    PRIMARY KEY (register_id)
);

-- Adapter parse warnings (the IRWarning sink).
CREATE TABLE adapter_warning (
    provider TEXT NOT NULL,
    entity_kind TEXT NOT NULL,
    entity_id INTEGER NOT NULL,
    code TEXT NOT NULL,
    detail TEXT
);

-- Per-variant Registerversion delivery/approval dates (the IRDeliveryProvenance
-- sink). Re-grained per register_variant (A4.2 resolved fork (c)): the A4.1
-- per-register keying collapsed variants sharing a `registerversionnamn` token.
-- `period_token` is the Registerversionnamn; first/last approval are the SCB
-- forsta/senast godkannandedatum.
--
-- A4.3a: the four IRDeliveryProvenance delivery fields (`source_file`,
-- `delivery_version`, `delivery_date`, `template_version`) are wired here. They
-- are per-variant (not per period_token), so they repeat across a variant's
-- period rows — acceptable for a maintainer-only debug DB; SOS (A4.3b) populates
-- all four, SCB sets only `source_file`. This grows the provenance DDL only;
-- dbdiff never opens this sibling DB, so SCHEMA_VERSION does not bump.
CREATE TABLE delivery_approval (
    register_id INTEGER NOT NULL,
    register_variant_id INTEGER NOT NULL,
    period_token TEXT NOT NULL,
    first_approved_date TEXT,
    last_approved_date TEXT,
    source_file TEXT,
    delivery_version TEXT,
    delivery_date TEXT,
    template_version TEXT,
    PRIMARY KEY (register_variant_id, period_token)
);
"""


def create_empty_provenance_db(path: Path) -> None:
    """Create an empty provenance DB with the full provenance schema.

    Applies `PROVENANCE_DDL` and nothing else; `write_provenance_db` is the
    populating variant A4.2 wires into the build. Refuses to overwrite an
    existing file — callers must `rotate_db_to_prev` first — to keep the
    rotation contract obvious.
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


def write_provenance_db(path: Path, payload: dict[str, Any]) -> None:
    """Create and populate the sibling provenance DB from a build payload.

    `payload` is the dict `materialize()` collects (provenance IR objects,
    warning IR objects, the SCB register-name map) plus the finalized
    universal-DB sha256/path the caller stamps in after the swap. Refuses to
    overwrite (rotate first), mirroring
    `create_empty_provenance_db`. The caller wraps this in a non-fatal
    try/except: a provenance write failure must NOT flip the build exit code,
    since the universal DB is already swapped in.
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

        conn.execute(
            "INSERT INTO build_manifest VALUES (?, ?, ?, ?)",
            (
                payload["schema_version"],
                payload["universal_db_path"],
                payload["universal_db_sha256"],
                payload["build_date"],
            ),
        )

        imported_at = payload["build_date"]
        conn.executemany(
            "INSERT INTO scb_register_id_map VALUES (?, ?, ?)",
            [
                (register_id, registernamn, imported_at)
                for register_id, registernamn in payload["scb_register_id_map"]
            ],
        )

        conn.executemany(
            "INSERT INTO adapter_warning VALUES (?, ?, ?, ?, ?)",
            payload["adapter_warnings"],
        )

        conn.executemany(
            "INSERT INTO delivery_approval VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            payload["delivery_approvals"],
        )

        conn.commit()
    finally:
        conn.close()


def rotate_db_to_prev(db_path: Path) -> None:
    """Rename `<db_path>` to `<db_path>.prev`, evicting any prior `.prev`.

    Used before the materializer writes the new universal DB / provenance
    DB so a single previous generation survives a rebuild. No auto-cleanup
    of older generations — maintainers `mv` the `.prev` aside if they
    want to keep more than one.

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


def _unlink_wal_sidecars(db_path: Path) -> None:
    """Remove a SQLite DB's WAL `-wal`/`-shm` sidecar files if present.

    A clean `close()` deletes them, but a subsequent read-only open
    (`open_db(..., mode=ro)`) re-creates them, and a read-only close leaves
    them on disk. They must be cleared before an atomic base-file rename,
    which moves only `<db>` and would otherwise orphan `<db>-wal`/`<db>-shm`.
    """
    for sidecar in ("-wal", "-shm"):
        db_path.with_name(db_path.name + sidecar).unlink(missing_ok=True)


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
def _open_scb_csv_raw(
    path: Path,
) -> Iterator[tuple[list[str], Iterator[tuple[int, list[str]]]]]:
    """Open a pipe-delimited cp1252 CSV; yield (header, raw-field-list iterator).

    Same open + header/field-count validation as `_open_scb_csv`, but each row
    is the RAW latin-1 field LIST — NOT decoded, NOT keyed into a dict. The
    102M-row Vardemangder loop indexes columns positionally and decodes only the
    few it keeps; per-row dict-building and per-field `_decode_cp1252` otherwise
    dominate the whole build. The header IS decoded (cheap, once).
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

        ncols = len(header)

        def raw_iter() -> Iterator[tuple[int, list[str]]]:
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

        yield header, raw_iter()


@contextmanager
def _open_scb_csv(
    path: Path,
) -> Iterator[tuple[list[str], Iterator[tuple[int, dict[str, str]]]]]:
    """Open a pipe-delimited cp1252 CSV and yield (header, row_iterator).

    Reads bytes as latin-1 (single-byte passthrough), validates against
    known-invalid cp1252 bytes, then decodes to proper cp1252 text. Each row is
    a fully-decoded ``{column: value}`` dict. Built on `_open_scb_csv_raw`; hot
    paths that don't need every column decoded should use the raw helper.
    """
    with _open_scb_csv_raw(path) as (header, raw_rows):

        def row_iter() -> Iterator[tuple[int, dict[str, str]]]:
            for row_number, fields in raw_rows:
                yield (
                    row_number,
                    {h: _decode_cp1252(v) for h, v in zip(header, fields, strict=True)},
                )

        yield header, row_iter()


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


@contextmanager
def _stage_timer(label: str):  # noqa: ANN201 - internal timing context manager
    """Time a build stage, emitting ``[timing] <label>: <s>`` when timing is on.

    Near-zero cost when off (a `perf_counter` + the `_emit_timing` env lookup);
    the ``[timing]`` prefix is greppable so a build log yields a per-stage
    breakdown — a profiler-free way to locate build-time hot spots. Enable with
    ``--timing`` / ``REG_META_BUILD_TIMING=1``. Delegates the format + gate to
    `_emit_timing` so the line shape lives in exactly one place.
    """
    t0 = time.perf_counter()
    try:
        yield
    finally:
        _emit_timing(label, t0)


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
# Source-of-truth marker for the auto-derive path. Distinguishes from the
# TOML-curated rows (#440 — cross-provider / dead-predecessor succession not
# visible in SCB's `timeseries_event`).
_REPLACED_BY_NOTE_AUTO = "auto:timeseries_event"
# Provenance marker for the curated-TOML path (#440). A row's own `note` (the
# human transition reason) lands in `beskrivning`; this fixed marker lands in
# `note`, mirroring the auto path so a consumer can tell curated from auto-derived.
_REPLACED_BY_NOTE_CURATED = "curated:slug_toml"

# Manifest stat keys for replaced_by materialization. Single source so the
# `skip_slugs` zero-fill (in `build_db`) and the materializer's real return
# can't drift apart — `test_replaced_by_stats_in_manifest` pins the exact set.
_REPLACED_BY_STAT_KEYS = (
    "n_timeseries_event_rows_scanned",
    "n_register_replaced_by",
    "n_variant_replaced_by",
    "n_variable_replaced_by",
    "n_skipped_unresolved",
    # A2.2 triage split the source key can't pick a sibling for: a bare
    # `Variabel` var_id (no cvid/column → irreducible). `AktuellVariabel` cvids
    # resolve via the coalescer's `variable_instance.variable_id` stamp (PR #150)
    # and never reach here. Always 0 pre-A2.2.
    "n_skipped_ambiguous_variable",
    # Genuine `Ersätter` rows collapsing onto an already-emitted edge — the
    # expected SCB paired-row case (see `_classify_duplicate`).
    "n_skipped_collapsed_inverse",
    # `Ersatt av` rows duplicating an already-emitted edge (repeated source
    # rows), kept distinct from the inverse-collapse count.
    "n_skipped_duplicate",
    # #440/#522 curated-TOML pass. `n_curated_register_replaced_by` /
    # `n_curated_variable_replaced_by` count edges INSERTED from
    # `curation/relations.toml` `type = "replaced_by"` edges (a subset of
    # `n_*_replaced_by` above — the curated rows roll into the same totals).
    # `n_curated_classification_replaced_by` (#579) counts curated classification
    # edges inserted into `classification_replaced_by` alongside the auto #571
    # edges (the sun1996 → sun-niva/inriktning/grupp 1→many split). `n_curated_skipped_
    # duplicate` counts curated rows that collapse onto an already-seen edge
    # (event-derived, an auto classification edge, or another curated row), via the
    # SHARED seen-PK sets (per-grain).
    "n_curated_register_replaced_by",
    "n_curated_variable_replaced_by",
    "n_curated_classification_replaced_by",
    "n_curated_skipped_duplicate",
    # A curated edge whose SUCCESSOR's provider isn't in this (partial) build is
    # SKIPPED, not failed — a `--providers=sos` build must not crash on an scb
    # successor. The predecessor's provider is never gated (it may be dead /
    # cross-provider; it's inserted verbatim, never resolved).
    "n_curated_skipped_inactive_provider",
)


def _empty_replaced_by_stats() -> dict[str, int]:
    """Zeroed replaced_by stats. The `skip_slugs` build path returns this as-is;
    the materializer fills it in, so both share one key set."""
    return dict.fromkeys(_REPLACED_BY_STAT_KEYS, 0)


def _materialize_replaced_by_edges(
    conn: sqlite3.Connection,
    curated_replaced_by: tuple[CuratedReplacedBy, ...],
    *,
    providers: frozenset[str],
) -> dict[str, int]:
    """Materialize succession edges from `timeseries_event`.

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
    `register_variant.slug`, and `variable.slug`. The variable
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
      - `n_skipped_ambiguous_variable`: a bare `Variabel` `(register_id, var_id)`
        source key maps to >1 variable — an A2.2 triage split. A bare var_id
        carries no cvid/column, so it can't pick a sibling and the edge is dropped
        (mirrors `_variable_source_slug`). An `AktuellVariabel` cvid never lands
        here: its coalescer-stamped `variable_instance.variable_id` (PR #150)
        names the exact sibling, so it resolves directly or skips as
        `unresolved` (unstamped / slug-less). Only the irreducible bare-`Variabel`
        split reaches this counter. Always 0 pre-A2.2.
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

    #440/#522: after this event-derived pass,
    `relations.materialize_curated_replaced_by` runs the curated
    `type = "replaced_by"` edges (`curated_replaced_by`, from
    `curation/relations.toml`), SHARING the `seen_*` PK sets below so a curated
    edge dedups against an event-derived one (and vice versa). The curated path
    carries the cross-provider (SOS→SCB) and dead-predecessor edges
    `timeseries_event` cannot express; its counts roll into the returned stats.
    See that helper for the curated-side rules.
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

    # Variable grain reads the STORED `variable.slug` — no
    # resolve-time derivation. `provider_key` is `str(var_id)`; cast it back to
    # the integer ids `timeseries_event` carries. Keyed by (register_id, var_id)
    # -> (provider, register, variable) slug triple.
    variable_lookup: dict[tuple[int, int], tuple[str, str, str]] = {}
    # var_id -> set of register_ids carrying it: a bare `Variabel` id (a
    # per-register var_id with no register context) resolves only when this is
    # a singleton.
    var_id_registers: dict[int, set[int]] = {}
    # (register_id, var_id) keys mapping to >1 variable — an A2.2 triage split
    # makes the bare `Variabel` key ambiguous (the cvid grain resolves via the
    # `variable_instance.variable_id` stamp instead, so this is bare-grain only).
    # Skip-not-guess.
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
        except TypeError, ValueError:
            # Non-integer provider_key (e.g. a SOS merged variable name): no
            # integer `timeseries_event` id maps to it.
            continue
        key = (register_id, var_id)
        var_id_registers.setdefault(var_id, set()).add(register_id)
        if key in variable_lookup:
            ambiguous_variable.add(key)
            continue
        variable_lookup[key] = (p_slug, r_slug, var_slug)

    # cvid -> (provider, register, variable) slug triple, via the coalescer's
    # GROUND-TRUTH `variable_instance.variable_id` stamp (PR #150). An
    # AktuellVariabel row names one cvid, and its stamped owning variable_id
    # picks the exact split sibling directly — no column-tie, no ambiguity
    # skip. Excludes cvids whose variable carries no slug (→ unresolved).
    # Stream the cursor (no `.fetchall()`): the join spans every cvid (~515K on
    # the real corpus), so materializing the full row list before building the
    # dict is a needless allocation — matches `cvid_to_year` below. The cursor is
    # fully drained here before any INSERT, so no read/write interleaving.
    cvid_to_slug: dict[int, tuple[str, str, str]] = {}
    for cvid, var_slug, r_slug, p_slug in conn.execute(
        "SELECT vi.cvid, v.slug, r.slug, p.slug "
        "FROM variable_instance vi "
        "JOIN variable v ON v.variable_id = vi.variable_id "
        "JOIN register r ON v.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
        "WHERE vi.variable_id IS NOT NULL AND v.slug IS NOT NULL "
        "AND r.slug IS NOT NULL"
    ):
        cvid_to_slug[cvid] = (p_slug, r_slug, var_slug)

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

    def _resolve_variable(
        entitet: str, raw_id: int
    ) -> tuple[tuple[str, str, str] | None, str | None]:
        """Resolve a variable-grain id to its (provider, register, variable)
        slug triple. Returns (triple, None) on success, else (None, bucket)
        naming the skip counter ('unresolved' / 'ambiguous')."""
        if entitet == "AktuellVariabel":
            # A cvid names one instance; its coalescer-stamped owning variable_id
            # (PR #150) resolves the exact split sibling — no column-tie, no
            # ambiguity. Missing / unstamped / slug-less cvid → unresolved.
            triple = cvid_to_slug.get(raw_id)
            return (triple, None) if triple is not None else (None, "unresolved")
        # 'Variabel': a bare per-register var_id — no cvid, no column. A split
        # makes the bare key ambiguous and unpickable (the irreducible skip; the
        # cvid grain above never reaches it).
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

    # #440/#522 curated pass — runs RIGHT AFTER the event-derived pass, sharing
    # the `seen_*` PK sets above so curated edges dedup against event-derived
    # ones. (Curated rows are register/variable grain only — no variant — so the
    # variant seen-set is not passed.) The curated `replaced_by` edges now come
    # from `curation/relations.toml` (the typed `[[edge]]` surface), loaded once
    # in `build_db` and threaded in.
    curated = materialize_curated_replaced_by(
        conn,
        curated_replaced_by,
        seen_register,
        seen_variable,
        providers=providers,
        progress=_progress,
    )

    # Keys (and their meaning) live on `_REPLACED_BY_STAT_KEYS`; fill the zeroed
    # base so this return and the `skip_slugs` zero-fill share one shape. Curated
    # edge counts roll into the same `n_*_replaced_by` totals (the curated counts
    # are reported separately too, for visibility).
    stats = _empty_replaced_by_stats()
    stats.update(
        n_timeseries_event_rows_scanned=n_scanned,
        n_register_replaced_by=n_register + curated["register"],
        n_variant_replaced_by=n_variant,
        n_variable_replaced_by=n_variable + curated["variable"],
        n_skipped_unresolved=n_skipped_unresolved,
        n_skipped_ambiguous_variable=n_skipped_ambiguous_variable,
        n_skipped_collapsed_inverse=n_skipped_collapsed_inverse,
        n_skipped_duplicate=n_skipped_duplicate,
        n_curated_register_replaced_by=curated["register"],
        n_curated_variable_replaced_by=curated["variable"],
        n_curated_classification_replaced_by=curated["classification"],
        n_curated_skipped_duplicate=curated["skipped_duplicate"],
        n_curated_skipped_inactive_provider=curated["skipped_inactive_provider"],
    )
    return stats


def _materialize_variable_related_to(
    conn: sqlite3.Connection, edges: list[tuple[int, int, str]]
) -> int:
    """Insert the NON-FOLDABLE split-sibling edges (both directions) into
    `variable_related_to`. Runs after `populate_variable_slugs` so each
    `variable_id` resolves to its (provider, register, variable) slug FQID.
    Returns the row count inserted.

    A4.1: the SCB adapter emits these as `IRRelatedToEdge` (variable-grain) and
    hands the build-only `(variable_id, variable_id, kind)` list to the
    materializer via `adapter.related_edges`; this materializer post-pass
    resolves variable_id → slug at write time (after slugs exist).

    #591: the foldable `same_def` kind (`EDGE_RELATION_KIND`) is NO LONGER
    persisted — it fed only the concept-group edge pass, which now reads the
    in-build sibling sets directly (`edge_siblings`). Only the meaningful
    non-foldable kinds (`code_vs_label_pair`, `import_bug_suspect`) land here, so
    the table is the meaningful-links surface (code/label pairs, import-bug hints,
    plus the curated see-also rows the related_to pass adds)."""
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
        if kind == EDGE_RELATION_KIND:
            continue  # foldable — fed to the concept-group pass, not persisted
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


# A4.4e: the SCB feed of the provider-blind `classification_candidate` table — a
# verbatim projection of exactly the rows `_backfill_state_classifications` used
# to read directly off `variable_instance`. Shared as a single source of truth so
# the byte-identical-gated filter (`classification_id IS NOT NULL AND variable_id
# IS NOT NULL`) cannot drift between the build feed and its test.
_CLASSIFICATION_CANDIDATE_FEED_SQL = (
    "INSERT INTO classification_candidate "
    "(variable_id, value_set_id, classification_id) "
    "SELECT variable_id, value_set_id, classification_id FROM variable_instance "
    "WHERE classification_id IS NOT NULL AND variable_id IS NOT NULL"
)


def _feed_classification_candidates(
    conn: sqlite3.Connection,
    candidates: list[tuple[int, int | None, str]],
) -> int:
    """A4.4e PR2: feed adapter-supplied classification candidates into the
    provider-blind `classification_candidate` table.

    Drains the `(variable_id, value_set_id, short_name)` candidates that ANY
    adapter contributes — SOS resolves each variable's free-text
    `external_classification` (it carries no `value_set_version_label`) to a
    seeded classification short_name; curated thin providers (#446) name an
    existing catalog classification's short_name directly (value_set_id is None,
    no codes). This resolves short_name → classification_id against the populated
    `classification` table and INSERTs the same `(variable_id, value_set_id,
    classification_id)` shape the SCB feed produces, so
    `_backfill_state_classifications` stays provider-blind (it reads only the
    candidate table). Candidates whose short_name is absent (e.g. a typo, since
    every declared classification is seeded) are dropped — no row, no error.
    Returns the number of candidate rows inserted.
    """
    if not candidates:
        return 0
    id_by_short = {
        short_name: cls_id
        for cls_id, short_name in conn.execute(
            "SELECT id, short_name FROM classification"
        )
    }
    rows = [
        (variable_id, value_set_id, id_by_short[short_name])
        for variable_id, value_set_id, short_name in candidates
        if short_name in id_by_short
    ]
    conn.executemany(
        "INSERT INTO classification_candidate "
        "(variable_id, value_set_id, classification_id) VALUES (?, ?, ?)",
        rows,
    )
    return len(rows)


def _backfill_state_classifications(conn: sqlite3.Connection) -> None:
    """A4.4e: tag `variable_state.classification_id` from the PROVIDER-BLIND
    `classification_candidate` table, folding candidates to their OWNING split
    sibling's state.

    `classification_candidate(variable_id, value_set_id, classification_id)` is a
    build-time-only table every adapter feeds (SCB projects `variable_instance`
    verbatim; SOS resolves `external_classification`; curated thin providers name
    a catalog short_name directly — see the STEP 2 feed in `materialize`). The
    backfill reads ONLY this table, so it no longer knows which provider supplied
    a candidate — the GAP-1 close-out. It runs after the feeds and is one of the
    last passes before the scratch tables drop.

    Split-sibling attribution: post-A2.2 a `var_id` can be NON-unique (split
    siblings share one `provider_key`). Each candidate row carries its OWNING
    `variable_id` (the SCB feed projects `variable_instance.variable_id`, stamped
    by `_coalesce_variable_states` from the triage's ground-truth
    assignment), so only that sibling's state gets the classification — no
    fan-out, no column-tie heuristic, no skip. (Contrast the old
    `(register_id, provider_key)` join, which fanned every cvid's classification
    onto EVERY sibling, discriminated only by `value_set_id IS …`; `IS` matches
    shared NULLs, so a code-less sibling could adopt a classification a different
    sibling owned.)

    Correlation key within a sibling: `(variable_id, value_set_id)`. A state's
    `value_set_id` is part of the coalescer's group key, so every cvid folded
    into that state shares it. `value_set_id` is preferred over
    `value_set_version_label` (the design's secondary signal) because the fold
    logic overwrites the label with a synthetic column-suffix token for states
    that never carried a classification; the value-set link is immune to that.

    The candidate set is NOT necessarily single-valued: multiple
    `classification_candidate` rows can share a `(variable_id, value_set_id)` but
    carry DIFFERENT classifications (on the SCB feed this happens when cvids share
    a state key but carry distinct `value_set_version_label`s — see the STEP 2
    feed comment). The lowest-id classification wins (`min`) — a deterministic
    tie-break, not a claim of correctness when the candidates genuinely disagree.

    Code-less states (NULL `value_set_id`) are NOT guaranteed to stay NULL: they
    key on `(variable_id, None)`, so a code-less state still adopts a
    classification if a candidate row carries one for its OWNING `variable_id`.
    They're left NULL only when no candidate matched.
    """
    _progress("Backfilling variable_state.classification_id...")

    # (variable_id, value_set_id) -> lowest candidate classification_id, keyed
    # off each candidate's OWNING `variable_id`, so a sibling never inherits
    # another's classification. `value_set_id` may be None (a code-less state
    # keys on (variable_id, None)). Reads the provider-blind candidate table;
    # the SCB feed already filtered to non-NULL variable_id/classification_id.
    cls_by_state_key: dict[tuple[int, int | None], int] = {}
    for variable_id, value_set_id, classification_id in conn.execute(
        "SELECT variable_id, value_set_id, classification_id FROM classification_candidate"
    ):
        skey = (variable_id, value_set_id)
        prev = cls_by_state_key.get(skey)
        if prev is None or classification_id < prev:
            cls_by_state_key[skey] = classification_id

    # `value_set_id IS ?` so the None key matches a code-less state's NULL.
    conn.executemany(
        "UPDATE variable_state SET classification_id = ? "
        "WHERE variable_id = ? AND value_set_id IS ?",
        [
            (classification_id, variable_id, value_set_id)
            for (
                variable_id,
                value_set_id,
            ), classification_id in cls_by_state_key.items()
        ],
    )
    n = conn.execute(
        "SELECT COUNT(*) FROM variable_state WHERE classification_id IS NOT NULL"
    ).fetchone()[0]
    _progress(f"  {n:,} variable_state rows tagged with a classification")


def dump_classification_linkage(
    conn: sqlite3.Connection,
) -> list[tuple[int, int | None, int]]:
    """Stable, ordered dump of the shipped variable→classification linkage.

    Returns every `(variable_id, value_set_id, classification_id)` from
    `variable_state` where a classification was tagged, ordered deterministically.
    Reusable by the full-corpus byte-identity gate (A4.4e re-point) and by the
    in-repo regression test as a CI proxy for it.
    """
    return [
        (row[0], row[1], row[2])
        for row in conn.execute(
            "SELECT variable_id, value_set_id, classification_id FROM variable_state "
            "WHERE classification_id IS NOT NULL "
            "ORDER BY variable_id, value_set_id, classification_id"
        )
    ]


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
    (the "same_as on the source side").

    Two seed nodes, so all three equivalence kinds are covered:
      - the SOURCE-side identity node `(source_provider, source_register,
        variable_slug)` — the slug-equality identity match (LISA `kon` → RTB
        `kon`, which needs no curated edge) plus any within-source rename
        reachable from it (RTB `kon` ↔ `kon-v2`);
      - the CONSUMER node `(consumer_provider, consumer_register,
        variable_slug)` — any curated cross-register / cross-provider `same_as`
        edge whose endpoints have *different* slugs (LISA `foo` ↔ RTB `bar`),
        plus renames transitively reachable from the matched source node.

    The earlier single-seed form (source node only) silently missed the
    mismatched-slug cross-register edge — a gap that stayed latent while
    `variable_same_as` was empty, fixed here per the A2.4 review. The COMMON
    case is no `same_as` edge at all → the result is just `{variable_slug}` (the
    identity match).

    Edges are stored both directions (per `relations.materialize_same_as`), so a
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
    """Materialize state-pair interval-overlap lineage edges.

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
        # `bar`). The no-rename common case yields just {consumer_slug}.
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
# Provider-blind materializer (A4.1)
# ---------------------------------------------------------------------------

# Sentinels the universal DDL applies as NOT NULL DEFAULTs; the IR contract
# carries None / open-ended, so the materializer reconciles them at the insert
# site ("None-to-sentinel reconciliation").
_VALID_TO_SENTINEL = "9999-12-31"  # variable_state.valid_to open-ended
_VALID_FROM_UNKNOWN = "0001-01-01"  # variable_state.valid_from start unknown


def _reinsert_core_graph_from_ir(
    conn: sqlite3.Connection,
    *,
    registers: list[IRRegister],
    variants: list[IRVariant],
    variables: list[IRVariable],
    states: list[IRVariableState],
    aliases: list[IRVariableAlias],
) -> None:
    """A4.3a provider-blindness flip: make the materializer the SOLE WRITER of
    the shipped provider-shaped core graph by re-inserting it from the IR.

    The adapter wrote these rows during emit() to derive SCB's exact legacy IDs
    (strategy 2). This DELETEs the adapter-written rows and re-INSERTs them from
    the collected IR with EXPLICIT PKs, so there is exactly one final writer and
    no parallel old+new path. The re-inserted rows are content-identical to the
    adapter's (the IR mirror carried the exact IDs), so the universal DB stays
    byte-identical to the pre-A4 baseline (`sqlite_sequence` re-seed is excluded
    from the dbdiff content comparison).

    Scope: `register`, `register_variant`, `variable`, `variable_state`,
    `variable_alias` — the genuinely provider-shaped tables. `value_set` /
    `value_code` / `value_set_member` are NOT re-inserted here: they are
    content-addressed (member_hash) / counter-derived and PROVIDER-SHARED BY
    CONTENT (an identical SOS code list collapses onto the same row, A4.3b), and
    the year-projection can leave orphan `value_code` rows that belong to no
    `value_set_member`, which the member-derived IR stream cannot reproduce. The
    adapter stays their writer; they carry no provider-specific shape.

    Slugs are inserted NULL: the IR's slug field is ignored for the core graph;
    `populate_slugs` / `populate_variable_slugs` UPDATE them in place afterwards
    (strategy B) — which is why the A4.1 inert-mirror NULL→"" slug caveat
    disappears (the mirror is gone; insert-then-UPDATE, no read-back).
    """
    _progress("A4.3a: re-inserting core graph from IR (materializer sole-writer)...")

    # Delete adapter-written rows (FK-child → parent; build runs foreign_keys=OFF
    # so order is not load-bearing, but keep it FK-safe for clarity).
    for table in (
        "variable_alias",
        "variable_state",
        "variable",
        "register_variant",
        "register",
    ):
        conn.execute(f"DELETE FROM {table}")

    conn.executemany(
        "INSERT INTO register (register_id, provider_id, name, purpose, slug) "
        "VALUES (:register_id, :provider_id, :name, :purpose, NULL)",
        [
            {
                "register_id": r.register_id,
                "provider_id": _provider_id_for(r.provider),
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
        " description, source_register_text, measurement_unit, source_register_id, "
        " source_label, is_sensitive, is_identifier) "
        "VALUES (:variable_id, :register_id, :provider_key, NULL, :name, "
        " :definition, :description, :source_register_text, :measurement_unit, "
        " :source_register_id, :source_label, :is_sensitive, :is_identifier)",
        [
            {
                "variable_id": v.variable_id,
                "register_id": v.register_id,
                "provider_key": v.provider_key,
                "name": v.name,
                "definition": v.definition,
                "description": v.description,
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
        " data_type, data_length, delivery_column_name, value_set_id, "
        " value_set_version_label, classification_id) "
        "VALUES (:state_id, :variable_id, :register_variant_id, :valid_from, "
        " :valid_to, :data_type, :data_length, :delivery_column_name, "
        " :value_set_id, :value_set_version_label, NULL)",
        [
            {
                "state_id": s.state_id,
                "variable_id": s.variable_id,
                "register_variant_id": s.register_variant_id,
                "valid_from": s.valid_from,
                "valid_to": s.valid_to
                if s.valid_to is not None
                else _VALID_TO_SENTINEL,
                "data_type": s.data_type,
                "data_length": s.data_length,
                "delivery_column_name": s.delivery_column_name,
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

    _progress(
        f"  re-inserted {len(registers):,} register / {len(variants):,} variant / "
        f"{len(variables):,} variable / {len(states):,} state / "
        f"{len(aliases):,} alias row(s) from IR"
    )


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


def materialize(
    conn: sqlite3.Connection,
    adapters: list[tuple[Any, Path]],
    *,
    seed_path: Path | None,
    cls_dir: Path,
    skip_classifications: bool,
    slug_dir: Path | None,
    skip_slugs: bool,
) -> dict[str, Any]:
    """Consume EACH adapter's IR stream and run the provider-blind derivation
    post-passes ONCE over the combined graph, writing the universal catalog.

    A4.3b — multi-adapter loop: ``adapters`` is a list of ``(adapter,
    source_dir)`` pairs (SCB then SOS). The PER-ADAPTER work (emit -> buffer ->
    value-table writes) runs in a loop, accumulating into combined IR buffers;
    `_reinsert_core_graph_from_ir` then writes the combined core graph ONCE, and
    the SHARED post-passes (classifications, slugs, same_as, replaced_by,
    lineage, code_variable_map, classification backfill, scratch DROPs, FTS) run
    ONCE over both providers' rows. Each adapter's
    `row_counts`/`source_checksums`/`related_edges`/`fold_slug_hints` and the
    provenance sinks are MERGED into combined structures the post-passes consume
    once. SCB-only stats (`projection_stats`, `coalesce_stats`) come from the SCB
    adapter; the SCB `code_variable_map` coverage guard reads SCB scratch
    (`variable_instance`) and stays SCB-only (SOS coverage is policed in
    `validate.py`). The scratch DROPs run ONCE after both adapters reinsert + the
    shared post-passes — never double-dropped (a naive second `materialize()`
    call would re-DROP already-dropped tables; the loop avoids that).

    Provider-blind boundary (A4.3a — the provider-blindness flip): the adapter
    derives SCB's exact legacy IDs by writing the core graph + its SCB-named
    build-scratch (`variable_instance`, `variable_alias_build`,
    `register_version`, ...) + SCB-reference tables into ``conn`` during
    ``emit()``, and the IR mirror reads the IDs back. This materializer is now
    the SOLE WRITER of the shipped provider-shaped core graph: it DELETEs the
    adapter-written `register` / `register_variant` / `variable` /
    `variable_state` / `variable_alias` rows and re-INSERTs them from the IR with
    explicit PKs (`_reinsert_core_graph_from_ir`) — one writer per table, no
    parallel old+new path. (`value_set` / `value_code` / `value_set_member` stay
    adapter-written; see `_reinsert_core_graph_from_ir`.) It then runs the
    derivations that read the scratch: classifications, slugs, same_as,
    register/variant/variable replaced_by (G3), state-pair lineage,
    code_variable_map (now from `variable_state ⨝ value_set_member`),
    `variable_state.classification_id` backfill, FTS. It drops the scratch before
    ship. `IRWarning` / `IRDeliveryProvenance` go to the sibling provenance DB.

    Returns the manifest inputs (checksums, row counts, projection/coalesce/
    replaced_by stats) + the provenance payload.
    """
    # A4.3a — provider-blindness flip. The materializer is now the SOLE WRITER
    # of the shipped core-graph tables (register, register_variant, variable,
    # variable_state, value_set, value_code, value_set_member, variable_alias):
    # it INSERTs them from the IR stream with EXPLICIT PKs. The adapter still
    # derives SCB's exact legacy IDs by writing those rows to `conn` during
    # emit() (strategy 2 — reuse the proven A4.1 ID-derivation verbatim,
    # only move WHERE the final rows land), and the IR mirror reads them back, so
    # the IR carries the exact IDs/content. `_reinsert_core_graph_from_ir` below
    # DELETEs the adapter-written core-graph rows and re-INSERTs them from the
    # collected IR — making the materializer the final writer with no parallel
    # path. Byte-identity holds because the re-inserted rows are content-identical
    # (sqlite_sequence is excluded from the dbdiff content comparison, so the
    # autoincrement re-seed is invisible).
    #
    # Slug columns are written NULL at insert (the IR's slug is ignored for the
    # core graph); the existing populate_slugs / populate_variable_slugs UPDATE
    # passes fill them in place exactly as today (strategy B) — closing the
    # A4.1 inert-mirror NULL→"" caveat, since the mirror is gone.
    adapter_warnings: list[tuple[str, str, int, str, str | None]] = []
    delivery_approvals: list[
        tuple[
            int,
            int,
            str,
            str | None,
            str | None,
            str | None,
            str | None,
            str | None,
            str | None,
        ]
    ] = []
    scb_register_id_map: list[tuple[int, str]] = []
    # The core-graph IR is BUFFERED into these lists because the DELETE-then-
    # reinsert flip (strategy 2) cannot reinsert until emit() has fully drained —
    # the adapter is still writing those same tables during iteration. Bounded +
    # small: ~240k objects on the real corpus (42k variables + 118k states + 80k
    # aliases, low-hundreds of MB), DWARFED by the build's dominant memory (the
    # 102M-row Vardemangder import + 515k-cvid coalesce). The genuinely huge
    # stream — value_set/value_code/value_set_member — is NOT buffered (the
    # IRValueSet branch below is a no-op; the adapter streams it). The real 102M
    # build completes without OOM. (Codex P2 — buffering is inherent to strategy
    # 2; streaming/disk-staging would be a redesign for a non-issue here.)
    registers: list[IRRegister] = []
    variants: list[IRVariant] = []
    variables: list[IRVariable] = []
    states: list[IRVariableState] = []
    aliases: list[IRVariableAlias] = []
    # Combined manifest inputs + the side channels the post-passes consume once.
    row_counts: dict[str, Any] = {}
    source_checksums: dict[str, str] = {}
    related_edges: list[tuple[int, int, str]] = []
    # A4.4e PR2: provider-blind classification linkage from the SOS adapter
    # (variable_id, value_set_id, short_name). Merged per-adapter like
    # `related_edges`; fed into `classification_candidate` after the SCB feed.
    classification_candidates: list[tuple[int, int | None, str]] = []
    fold_slug_hints: dict[int, str] = {}
    # SCB-only stats (the SOS adapter has no projection/coalesce passes). The SCB
    # adapter is the only one that populates these; default to empty / zeroed for
    # an SCB-excluded (`--providers=sos`) build.
    state_stats: dict[str, Any] = {}
    projection_stats: dict[str, int] = {
        "n_value_sets": 0,
        "cvids_with_set": 0,
        "cvids_empty_after_projection": 0,
    }
    # Whether the SCB adapter is in this build. Only SCB writes build-scratch
    # (`variable_instance`, `variable_alias_build`, `register_version`, ...); the
    # SCB-only coverage guard + the scratch DROPs run only when SCB ran (an
    # SCB-excluded `--providers=sos` build has no scratch to read or drop).
    scb_ran = any(a.provider == "scb" for a, _ in adapters)

    # A4.3b multi-adapter loop. Per adapter: drain its IR stream into the COMBINED
    # buffers (the DELETE-then-reinsert flip can't reinsert until every adapter's
    # emit() has drained — SCB is still writing those tables during iteration;
    # SOS writes none), and MERGE its row_counts/source_checksums/related_edges/
    # fold_slug_hints. The reinsert + shared post-passes then run ONCE below.
    for adapter, source_dir in adapters:
        _adapter_t0 = time.perf_counter()
        for obj in adapter.emit(source_dir):
            if isinstance(obj, IRRegister):
                registers.append(obj)
                # Provenance source-ID linkage rides on the IR (no re-query of
                # the universal `register` table). SCB only: the provenance DB's
                # `scb_register_id_map` records the SCB-native `Registernamn`.
                if obj.provider == "scb":
                    scb_register_id_map.append((obj.register_id, obj.name))
            elif isinstance(obj, IRVariant):
                variants.append(obj)
            elif isinstance(obj, IRVariable):
                variables.append(obj)
            elif isinstance(obj, IRVariableState):
                states.append(obj)
            elif isinstance(obj, IRValueSet):
                # value_set / value_code / value_set_member are adapter-written
                # (content-addressed, PROVIDER-SHARED BY CONTENT). SCB writes
                # them directly; SOS writes them via INSERT OR IGNORE + read-back
                # so an identical code list collapses onto SCB's row. Nothing to
                # collect here.
                pass
            elif isinstance(obj, IRVariableAlias):
                aliases.append(obj)
            elif isinstance(obj, IRWarning):
                adapter_warnings.append(
                    (
                        adapter.provider,
                        obj.entity_kind,
                        obj.entity_id,
                        obj.code,
                        obj.detail,
                    )
                )
            elif isinstance(obj, IRDeliveryProvenance):
                # One delivery_approval row per (variant, period_token); union
                # the first/last-approval token sets. When that union is empty,
                # the adapter's `emit_when_no_tokens` flag decides whether to
                # still record one bare-token row: SCB leaves it False (empty
                # dicts → zero rows, the A4.2 behavior), SOS sets it True so the
                # variant's delivery metadata survives. The decision lives at the
                # IR boundary, keeping this loop provider-blind.
                first = obj.first_approval_dates or {}
                last = obj.last_approval_dates or {}
                delivery_date = (
                    obj.delivery_date.isoformat() if obj.delivery_date else None
                )
                tokens = sorted(set(first) | set(last))
                if not tokens and obj.emit_when_no_tokens:
                    tokens = [""]
                for token in tokens:
                    delivery_approvals.append(
                        (
                            obj.register_id,
                            obj.register_variant_id,
                            token,
                            first.get(token),
                            last.get(token),
                            obj.source_file,
                            obj.delivery_version,
                            delivery_date,
                            obj.template_version,
                        )
                    )
        # Merge this adapter's manifest inputs + side channels.
        row_counts.update(adapter.row_counts)
        source_checksums.update(adapter.source_checksums)
        related_edges.extend(adapter.related_edges)
        # SOS and curated thin providers (#446) populate classification
        # candidates; SCB instead tags classification on `variable_instance` and
        # feeds the candidate table via SQL (the SCB feed below). getattr keeps
        # this loop blind to which adapter carries the attribute.
        classification_candidates.extend(
            getattr(adapter, "classification_candidates", ())
        )
        fold_slug_hints.update(adapter.fold_slug_hints)
        # SCB-machine-build stats come only from the real `SCBAdapter`. We can't key
        # on `provider == "scb"`: `CanonicalScbAdapter` (#444) is also a `scb`-provider
        # adapter (and runs AFTER SCB, so it would clobber the real stats), and we
        # can't key on `coalesce_stats` either — `SOSAdapter` has that too. The
        # SCB-only marker is `projection_stats`. Same hasattr idiom as
        # classification_candidates above.
        if hasattr(adapter, "projection_stats"):
            state_stats = adapter.coalesce_stats
            projection_stats = {
                "n_value_sets": adapter.projection_stats.n_value_sets,
                "cvids_with_set": adapter.projection_stats.cvids_with_set,
                "cvids_empty_after_projection": (
                    adapter.projection_stats.cvids_empty_after_projection
                ),
            }
        _emit_timing(f"adapter.emit[{adapter.provider}]", _adapter_t0)

    _progress(
        f"  IR stream: collected {len(delivery_approvals):,} delivery-approval + "
        f"{len(adapter_warnings):,} warning row(s) for the provenance DB"
    )

    # The flip: re-insert the combined core graph from the IR (sole-writer).
    _t = time.perf_counter()
    _reinsert_core_graph_from_ir(
        conn,
        registers=registers,
        variants=variants,
        variables=variables,
        states=states,
        aliases=aliases,
    )
    _emit_timing("reinsert_core_graph", _t)

    # Provider gate shared by the curated passes below: classifications and
    # concept-group families whose provider isn't in this build are skipped,
    # not errors (a `--providers=sos` build must not fail on an scb entry).
    active_providers = frozenset(a.provider for a, _ in adapters)

    # Curated pairwise relations (#522): one typed `[[edge]]` surface loaded
    # ONCE here, then materialized into its three table groups — `related_to`
    # below (after variable slugs), `same_as` / `replaced_by` further down (after
    # all slugs). Empty when the file is absent (synthetic builds, wheel
    # installs).
    relations = load_relations(repo_relations_path())

    # Classifications — maintainer-curated normalized code systems. Every
    # classification is seeded regardless of `--providers` (shared standards with
    # git-tracked canonical-code CSVs), so there are no provider-skipped entries
    # to thread anywhere.
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
        # Every classification is seeded; `built_providers` only scopes the #597
        # per-classification seed-drift demotion (a label-source provider that
        # isn't built relaxes its classification's unmatched-string drift).
        n_classifications = populate_classifications(
            conn,
            seed,
            valid_codes_dir=valid_codes_dir,
            built_providers=active_providers,
        )
        row_counts["classifications.toml"] = n_classifications

    # Slug TOMLs: populate slug columns on register / register_variant /
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

        # Period column-family merges (#319) — fold each curated family's period
        # (today, month) columns into ONE variable BEFORE variable slugs are
        # assigned, so the survivor is slugged as the family stem (registered into
        # `fold_slug_hints`, the same side channel the A2.2 fold uses) and the
        # merged-away siblings never get a slug. Runs after the IR reinsert
        # (variable_state / variable_alias exist) and after populate_slugs
        # (register/variant slugs). Member columns are identified by
        # `delivery_column_name` (slugs don't exist yet). A dangling/incoherent
        # family fails the build (EXIT_CONFIG). Empty without
        # curation/period_family_merges.toml.
        fm_counts = materialize_period_family_merges(
            conn,
            load_period_family_merges(repo_period_family_merges_path()),
            providers=active_providers,
            fold_slug_hints=fold_slug_hints,
            progress=_progress,
        )
        # Manifest row-count key deliberately kept as the pre-rename
        # `monthly_family_merges` (the surface is now `period_family_merges`): the
        # whole `row_counts` dict is serialized into the dbdiff-compared
        # `import_manifest`, so renaming this label would break the byte-identity of
        # an otherwise pure relocation (#518/#523) and owe a release for no content
        # change. It's an internal metric label with no runtime consumer; rename it
        # in a future build that already owes a manifest delta.
        row_counts["monthly_family_merges"] = fm_counts["families"]
        row_counts["variable_alias_windows"] = fm_counts["windows"]

        # Variable grafts (#365 PR1d) — mint catalog variables reg_meta lacks but
        # a steward delivers, onto an existing (register, variant). Runs AFTER
        # populate_slugs (register/variant slugs resolve the target) and BEFORE
        # populate_variable_slugs (the minted variable's NULL slug auto-derives
        # from its delivery column, like any other). Gap-fill only; banded ids.
        from .variable_grafts import (
            load_variable_grafts,
            materialize_grafts,
            repo_variable_grafts_path,
        )

        graft_counts = materialize_grafts(
            conn,
            load_variable_grafts(repo_variable_grafts_path()),
            providers=active_providers,
            warn=_progress,
        )
        row_counts["variable_grafts"] = graft_counts["minted"]
        _progress(
            f"  {graft_counts['minted']:,} variable grafts "
            f"({graft_counts['skipped']:,} already present, "
            f"{graft_counts['unresolved']:,} unresolved)"
        )

        # Canonical-SCB attach (#400 PR2) — the RICH analog of grafts: mint
        # canonical-SCB variables (LISA columns absent from SCB's machine export)
        # onto an existing (register, variant), with canonical-SCB-banded ids, a
        # closed validity window, and an optional classification link. Same
        # slug-guarded block + ordering rationale as grafts (after populate_slugs
        # resolves the target, before populate_variable_slugs auto-derives the new
        # slug). Its classification candidates join the SAME list fed to
        # `_feed_classification_candidates` below, so the backfill tags them.
        from .canonical_attach import (
            canonical_attach_path,
            load_canonical_attach,
            materialize_canonical_attach,
        )

        # The canonical-attach seed (`lisa_canonical.toml`) is read from the SAME
        # `--input-dir/scb_canonical/` the `CanonicalScbAdapter` reads its
        # `scb_canonical.toml` from — found here as that adapter's paired source
        # dir (basename `scb_canonical`). None when this build has no such adapter
        # (synthetic / SCB-only / SOS-only builds) → the load no-ops.
        canonical_dir = next(
            (d for _a, d in adapters if d.name == _CANONICAL_SCB_DIRNAME), None
        )
        attach_seed_path = canonical_attach_path(canonical_dir)
        # Provenance: record the attach seed alongside the other canonical-SCB
        # inputs (scb_canonical.toml + CSVs, keyed by basename via `_file_sha256`).
        # Only when the seed actually resolved (None ⇒ no canonical adapter, a
        # legitimate no-op — matching the no-canonical-dir case; a stale-but-present
        # dir already failed loud in `canonical_attach_path`). Without this, a build
        # audit sees scb_canonical.toml recorded but NOT the seed that minted the
        # LISA rows.
        if attach_seed_path is not None:
            source_checksums[attach_seed_path.name] = _file_sha256(attach_seed_path)
        attach_counts = materialize_canonical_attach(
            conn,
            load_canonical_attach(
                attach_seed_path,
                classification_seed_path=seed_path,
            ),
            providers=active_providers,
            classification_candidates=classification_candidates,
            warn=_progress,
        )
        row_counts["canonical_attach"] = attach_counts["minted"]
        _progress(
            f"  {attach_counts['minted']:,} canonical-SCB attaches "
            f"({attach_counts['unresolved']:,} unresolved)"
        )

        # Stored `variable.slug`. Runs after populate_slugs
        # (register/variant slugs feed collision messages) and after
        # _coalesce_variable_states (reads variable_state.delivery_column_name),
        # but before the curated relation passes (related_to / replaced_by
        # resolve endpoints off the stored `variable.slug`). Curated `[variable]`
        # overrides in scb.toml win; the rest auto-derive into scb.auto.toml.
        # `fold_slugs` is the adapter's R8 build-only side channel (NOT an IR
        # object).
        var_slug_counts = populate_variable_slugs(
            conn, slug_root, fold_slugs=fold_slug_hints
        )
        row_counts["variable_slugs_curated"] = var_slug_counts["curated"]
        row_counts["variable_slugs_auto"] = (
            var_slug_counts["auto_existing"] + var_slug_counts["auto_new"]
        )

        # Non-foldable split-sibling edges. Runs after variable slugs so each
        # sibling variable_id resolves to its FQID slug; the triage emitted the
        # (variable_id, variable_id, kind) pairs during coalescing. #591: the
        # foldable `same_def` kind is NOT persisted here — it feeds the concept-
        # group edge pass below via `edge_siblings`; only code/label-pair and
        # import-bug-suspect edges land in the table.
        n_related = _materialize_variable_related_to(conn, related_edges)
        row_counts["variable_related_to"] = n_related
        _progress(f"  {n_related:,} variable_related_to edges (auto:triage)")

        # Curated cross-register "see also" edges (#353, now from the typed
        # `relations.toml`). Runs right after the auto:triage pass (both write
        # `variable_related_to`, on disjoint relation-kind vocabularies) so it
        # shares the --skip-slugs guard; the curated kinds are NON-foldable, so
        # the concept-group edge pass below ignores them. `providers` gates each
        # edge to this build's providers.
        n_curated_related = materialize_related_to(
            conn,
            relations.related_to,
            providers=active_providers,
        )
        row_counts["variable_related_to_curated"] = n_curated_related
        _progress(f"  {n_curated_related:,} curated variable_related_to edges")

        # Derived concept groups (#303) — presentation-only browse folding.
        # Ordering: after variable slugs (the edge pass resolves the in-build
        # sibling variable_ids to slugs) and after populate_classifications +
        # populate_slugs (classification rows + slugs, both above). Skipped
        # with the rest of the slug-dependent passes under --skip-slugs (every
        # slug is NULL then — month stems and edge endpoints don't exist).
        # Dimension 0 folds the in-build `same_def` split-sibling subset
        # (`edge_siblings`) — #591: those pairs are no longer persisted to
        # `variable_related_to`, so the fold reads them straight off the triage's
        # list. Dimension 2 is opt-in over the generated auto catalog (#496):
        # custom `[[variable_group]]` families fold unconditionally (and take
        # PRECEDENCE over the edge fold — a claimed FQID is excluded from its
        # edge component); an auto family folds only when an `[[accept]]` in
        # concept_groups.toml references it.
        edge_siblings = [
            (a, b) for a, b, kind in related_edges if kind == EDGE_RELATION_KIND
        ]
        cg_counts = materialize_concept_groups(
            conn,
            load_concept_groups(repo_concept_groups_path()),
            auto=load_concept_groups(repo_concept_groups_auto_path()),
            accepts=load_concept_group_accepts(repo_concept_groups_path()),
            classification_groups=load_classification_groups(
                repo_concept_groups_path()
            ),
            edge_siblings=edge_siblings,
            providers=active_providers,
            warn=_progress,
        )
        row_counts["concept_groups"] = (
            cg_counts["edge_groups"]
            + cg_counts["month_groups"]
            + cg_counts["curated_groups"]
            + cg_counts["classification_curated_groups"]
        )
        _progress(
            f"  {row_counts['concept_groups']:,} concept groups "
            f"({cg_counts['edge_groups']:,} edge / "
            f"{cg_counts['month_groups']:,} month / "
            f"{cg_counts['curated_groups']:,} curated / "
            f"{cg_counts['classification_curated_groups']:,} curated classification; "
            f"{cg_counts['grouped_variables']:,} variables + "
            f"{cg_counts['grouped_classifications']:,} classifications grouped)"
        )

        # Classification EDITION succession (#571) — adjacent-vintage chain
        # edges into `classification_replaced_by` (ssyk1996→ssyk2012,
        # lkf1980…lkf2026, …), NOT a presentation group. Same slug-dependent
        # block: it reads `classification.slug` (populated by populate_slugs
        # above; every slug is NULL under --skip-slugs).
        n_succession = derive_classification_succession(conn)
        row_counts["classification_replaced_by"] = n_succession
        _progress(f"  {n_succession:,} classification succession edges")

        # Delivery-list description backfill (#365 PR1a) — fill empty
        # variable.description from curated delivery-list prose. Runs after
        # populate_variable_slugs (resolves (register, variable) off stored
        # slugs); gap-fill only (never overwrites an official description).
        # Slug-dependent, so it lives in this --skip-slugs-guarded block.
        de_counts = apply_delivery_enrichment(
            conn,
            load_delivery_enrichment(repo_delivery_enrichment_path()),
            providers=active_providers,
            warn=_progress,
        )
        row_counts["description_backfills"] = de_counts["applied"]
        row_counts["delivery_aliases"] = de_counts["alias_applied"]
        _progress(
            f"  {de_counts['applied']:,} description backfills "
            f"({de_counts['skipped']:,} already set, "
            f"{de_counts['unresolved']:,} unresolved)"
        )
        _progress(
            f"  {de_counts['alias_applied']:,} delivery aliases "
            f"({de_counts['alias_skipped']:,} already present, "
            f"{de_counts['alias_unresolved']:,} unresolved)"
        )

        # Curated cross-register thematic tags (#311) — discovery overlay. Runs
        # after populate_variable_slugs (member FQIDs resolve off stored
        # register/variable slugs), same slug-dependent block as concept groups /
        # delivery enrichment. Tables ship EMPTY until curation content lands; a
        # dangling member reference fails the build LOUD (EXIT_CONFIG).
        tag_counts = materialize_tags(
            conn,
            load_tags(repo_tags_path()),
            providers=active_providers,
            progress=_progress,
        )
        row_counts["tags"] = tag_counts["tags"]
        row_counts["tag_members"] = tag_counts["members"]

    # same_as edges (curated `relations.toml`). Runs *after* populate_slugs so
    # register / classification slug columns are populated — the materializer
    # validates endpoints against them. Skip-slugs takes the honest-failure
    # stance shared by the slug-keyed linkers below (replaced_by + lineage): skip
    # cleanly rather than emit zero edges silently from NULL slug columns.
    if skip_slugs:
        _progress("Skipping same_as edges (skip_slugs=True)")
    else:
        sa_counts = materialize_same_as(
            conn,
            relations.same_as,
            providers=active_providers,
        )
        # #522: same_as curated counts now reach the manifest (previously
        # missing) — both grains, mirroring the related_to / replaced_by curated
        # keys. Emitted ONLY when non-zero: a build with no same_as edge for a
        # grain (e.g. classification today, or any partial `--providers` build
        # that gates out the #508 variable batch) would otherwise carry an
        # always-present `…: 0` pair that changes the manifest `row_counts` JSON
        # blob vs the released DB, tripping the dbdiff byte-identity gate for a
        # count that carries no information. The key appears (and dbdiff
        # legitimately moves) the first build a same_as edge of that grain lands
        # — exactly when the count becomes informative.
        if sa_counts["variable"]:
            row_counts["variable_same_as_curated"] = sa_counts["variable"]
        if sa_counts["classification"]:
            row_counts["classification_same_as_curated"] = sa_counts["classification"]
        _progress(
            f"  {sa_counts['variable']:,} variable same_as edges, "
            f"{sa_counts['classification']:,} classification same_as edges "
            "(curated)"
        )

    # replaced_by edges. Runs *after* populate_variable_slugs
    # (above) — every grain resolves off a stored slug column, and the
    # variable grain reads `variable.slug`. Under `--skip-slugs` those
    # columns are NULL, so the materializer would emit zero edges silently;
    # mirror the same_as honest-failure stance and skip cleanly with zeroed
    # stats instead. The curated `replaced_by` edges (`relations.replaced_by`)
    # are threaded in to share the event pass's seen-PK dedup + cycle check.
    if skip_slugs:
        _progress("Skipping replaced_by edges (skip_slugs=True)")
        replaced_by_stats: dict[str, int] = _empty_replaced_by_stats()
    else:
        replaced_by_stats = _materialize_replaced_by_edges(
            conn, relations.replaced_by, providers=active_providers
        )
        # #579: curated classification edges land in `classification_replaced_by`
        # AFTER `derive_classification_succession` set the row count to the auto
        # total — fold the curated additions back in so the manifest record of the
        # table's size stays accurate (the auto+curated rows now coexist there).
        if "classification_replaced_by" in row_counts:
            row_counts["classification_replaced_by"] += replaced_by_stats[
                "n_curated_classification_replaced_by"
            ]

        # #579: project `classification.supersedes_id` from the now-complete
        # `classification_replaced_by` (auto year-tail editions + the curated
        # `relations.toml` `class/<slug>` edges just materialized). The edge table
        # is the single canonical succession surface; `supersedes_id` is a derived
        # back-pointer onto it (see `derive_supersedes_from_edges`). Slug-anchored,
        # so it shares the `--skip-slugs` honest-failure stance (every slug is NULL
        # then — the projection would no-op). Runs BEFORE
        # `link_value_set_classifications` below, whose `_chain_root` recursive CTE
        # walks `supersedes_id`.
        n_supersedes = derive_supersedes_from_edges(conn)
        row_counts["classification_supersedes_derived"] = n_supersedes

    # Lineage edges. Runs *after* populate_variable_slugs so
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
        # State-pair interval-overlap lineage. Ordering: after
        # populate_variable_slugs (reads variable.slug on both sides),
        # materialize_same_as (the BFS reads variable_same_as), and
        # _coalesce_variable_states (reads the finished variable_state rows
        # it joins) — so it must be among the last passes. Shares the
        # skip_slugs guard: every slug is NULL under --skip-slugs, so the
        # linker would silently emit zero edges instead of an honest
        # incompleteness signal. `slug_root` is in scope from the slug
        # branch above.
        lineage_counts = link_variable_state_lineage(conn, slug_root)
        row_counts["variable_state_lineage"] = lineage_counts["edges"]
        row_counts["variable_state_lineage_warnings"] = (
            lineage_counts["warnings_ambiguous"] + lineage_counts["warnings_no_source"]
        )

    # Populate code_variable_map from year-projected value_set_member rows
    # joined through variable_instance.value_set_id. A code only appears for
    # the variables whose value set contained it at some cvid year.
    # VARIABLE-grained via the coalescer's `variable_instance.variable_id`
    # stamp (#150): an A2.2 split makes siblings share `var_id`, so keying on
    # `(register, var_id)` would over-attribute a code to every sibling; the
    # cvid's stamped `variable_id` names the one owning sibling. A cvid with
    # no stamp (raise-on-collision residual) carries NULL — skipped, since it
    # has no resolved owner to attribute its codes to (and NOT NULL would
    # reject it anyway).
    # A4.3a: re-pointed off the (now scratch) `variable_instance` onto the
    # universal `variable_state` — both are the materializer's own IR-inserted
    # rows. `variable_state.value_set_id` carries the cvid's year-projected set;
    # joining it to `value_set_member` yields the same (code_id, variable_id)
    # pairs the `variable_instance`-based derivation produced (the #152 grain
    # parity: every code-bearing cvid yields a state, and the coalescer stamped
    # each cvid's owning `variable_id` onto its state — so the skip-sets coincide).
    # Verified row-identical on the classification fixture; the FULL dbdiff on
    # real data is the gate. A state with NULL value_set_id contributes nothing
    # (the WHERE guard mirrors the old `value_set_id IS NOT NULL` skip).
    _progress("Building code_variable_map...")
    _t = time.perf_counter()
    conn.execute(
        "INSERT INTO code_variable_map (code_id, variable_id) "
        "SELECT DISTINCT vsm.code_id, vs.variable_id "
        "FROM variable_state vs "
        "JOIN value_set_member vsm ON vs.value_set_id = vsm.value_set_id "
        "WHERE vs.value_set_id IS NOT NULL"
    )
    # SCB cvid-scratch top-up — makes code→variable search coverage STRUCTURAL.
    # `code_variable_map` is a code→variable SEARCH index (no period dimension), so
    # it must list every code a variable EVER delivered. Two effects drop codes the
    # variable_state-derived map above needs: (1) the per-(variable, variant, year)
    # co-delivery cascade (`sources/scb.py`) drops superseded value sets from
    # `variable_state` (a preliminary/old coding beaten by the final one emits no
    # state); (2) a `_collapse_residual`'d cvid carries a stamped variable_id but no
    # `variable_state` row at all. A code unique to either is absent above. This
    # top-up restores them from the cvid scratch (`variable_instance`), which holds
    # the complete code set per cvid — so the index is COMPLETE BY CONSTRUCTION.
    # (This supersedes the former A4.3a fail-loudly coverage guard: that guard
    # asserted `variable_instance ⨝ value_set_member EXCEPT code_variable_map` was
    # empty, i.e. exactly the rows this INSERT supplies — tautological once the
    # top-up runs, so it was removed. validate.py keeps the SOS coverage guard the
    # dbdiff cannot police.) Authoritative per-period codings still live only in
    # `variable_state`. Provider-blind base + SCB top-up (SOS emits states directly,
    # so the base derivation already covers it). Guarded on `scb_ran` —
    # `variable_instance` is only populated when the SCB adapter ran.
    if scb_ran:
        conn.execute(
            "INSERT OR IGNORE INTO code_variable_map (code_id, variable_id) "
            "SELECT DISTINCT vsm.code_id, vi.variable_id "
            "FROM variable_instance vi "
            "JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id "
            "WHERE vi.variable_id IS NOT NULL"
        )
    cvm_count = conn.execute("SELECT COUNT(*) FROM code_variable_map").fetchone()[0]
    _progress(f"  {cvm_count:,} code×variable mappings")
    _emit_timing("code_variable_map", _t)

    # value_code.mapping_count (#352): precompute per-(code,label) variable count
    # from the now-complete code_variable_map (base derivation + SCB top-up). Used
    # by search(type="value") to downweight common labels — a generic enum shared
    # by many variables is less discriminative than a rare one. Computed here (not
    # in the FTS table) so it's JOINed at query time, never aggregated over the
    # 4.1M-row map. Codes with no mapping keep the DEFAULT 0.
    _progress("Computing value_code.mapping_count...")
    conn.execute(
        "UPDATE value_code SET mapping_count = ("
        "SELECT COUNT(*) FROM code_variable_map WHERE code_id = value_code.code_id)"
    )

    # A4.4e: SCB feed of the provider-blind `classification_candidate` table.
    # `populate_classifications` tags `variable_instance.classification_id` by
    # `value_set_version_label`, AFTER the SCB adapter emits (so the adapter
    # cannot carry it on IRValueSet) — `variable_instance` is the home of SCB
    # classification linkage. Each cvid carries its OWNING `variable_id` (stamped
    # by `_coalesce_variable_states` from the triage's ground truth), so the
    # projection below attributes each candidate to the right split sibling — no
    # column-tie heuristic, no fan-out. This is the VERBATIM SELECT the backfill
    # used to read directly off `variable_instance`; A4.4e moved it here so the
    # backfill becomes provider-blind (reads only `classification_candidate`). The
    # per-state-key min() tie-break is LOAD-BEARING: value_set→classification is
    # NOT 1:1 (≈5,161 value_sets span >1 classification on the real corpus), so
    # the candidate-level linkage cannot be replaced by a value_set-derived map.
    # This feeds ONLY the SCB rows; SOS + curated candidates are fed separately
    # below via `_feed_classification_candidates`. Guarded on
    # `scb_ran`: `variable_instance` is only POPULATED when the SCB adapter ran (it
    # is in the BASE DDL, so it always exists but is empty otherwise) — the guard
    # makes the intent explicit and leaves `classification_candidate` empty in an
    # SCB-excluded build, so the backfill is a safe no-op. `classification_candidate`
    # is likewise in the BASE DDL and drops below with the other scratch.
    if scb_ran:
        conn.execute(_CLASSIFICATION_CANDIDATE_FEED_SQL)

    # A4.4e PR2 (+ #446): adapter feed of the same provider-blind candidate
    # table. Runs after `populate_classifications` (so `classification.id` exists
    # for short_name resolution) AND after the SCB feed (both write into one table
    # the backfill reads). SOS resolved `external_classification` → short_name per
    # state; curated thin providers name an existing classification's short_name
    # directly (value_set_id None). This resolves short_name → classification_id
    # and INSERTs the SCB-shaped rows. Guarded on candidates being present: an
    # SCB-only build produces none, leaving the table byte-identical to the
    # SCB-only state.
    n_candidates_fed = _feed_classification_candidates(conn, classification_candidates)
    if classification_candidates:
        _progress(
            f"  {n_candidates_fed:,} classification candidates fed "
            f"(of {len(classification_candidates):,} resolved states)"
        )
        for adapter, _ in adapters:
            summary = getattr(adapter, "classification_summary", None)
            if summary is not None:
                _progress(f"  {summary()}")

    # #416: two more producers of `classification_candidate`, both AFTER the
    # SCB/SOS/#446 feeds and BEFORE `_backfill_state_classifications`.
    #
    # Order is LOAD-BEARING: curated links FIRST (delete-then-insert → curated
    # wins), then the auto code-set-containment detector, whose additive guard
    # (NOT EXISTS for the state key) then skips every key the feeds or the curated
    # links already claimed. So precedence is name-map/SCB/SOS/#446 + curated >
    # auto.
    #
    # The curated loader resolves FQIDs off stored slugs, so it shares the
    # `--skip-slugs` guard (every slug is NULL then). The auto detector needs no
    # slugs — only `value_set_id` / `variable_id` — so it runs UNCONDITIONALLY.
    if skip_slugs:
        _progress("Skipping curated classification links (skip_slugs=True)")
    else:
        n_curated_links = materialize_classification_links(
            conn,
            load_classification_links(repo_classification_links_path()),
            providers=active_providers,
        )
        row_counts["classification_links_curated"] = n_curated_links
        _progress(f"  {n_curated_links:,} curated classification links")

    cls_link_counts = link_value_set_classifications(conn)
    # Auto classification links = the confident tier PLUS the vintage-period reclaim
    # (#494); counting only the confident tier under-reports by the vintage population.
    row_counts["classification_links_auto"] = (
        cls_link_counts["value_sets_linked"]
        + cls_link_counts["vintage_value_sets_linked"]
    )

    # A4.3a: the `variable_alias` re-parent is GONE — `_reinsert_core_graph_from_ir`
    # already wrote `variable_alias` from IRVariableAlias (the FULL historical
    # column set the old `_reparent_variable_alias` projected from the
    # `variable_alias_build ⨝ variable_instance` scratch). `variable_state` is
    # now IR-inserted; `_backfill_state_classifications` tags its
    # `classification_id` from the provider-blind `classification_candidate` table
    # (fed just above). It UPDATEs the IR-inserted `variable_state` rows unchanged.
    _backfill_state_classifications(conn)

    # Variable vintage succession (#584) — lift `classification_replaced_by`
    # edition edges (#571) to the variable grain through value-set bindings,
    # clean tier only (a same-name family that maps 1:1 to editions). Its join
    # filters `variable_state.classification_id IS NOT NULL`, so it MUST run
    # after `_backfill_state_classifications` (just above), which tags that
    # column — running it earlier (e.g. beside `_materialize_replaced_by_edges`)
    # silently returns zero rows on the real corpus and trips the corpus-gated
    # floor `_MIN_VARIABLE_VINTAGE_LIFT_EDGES`. The other three predecessors are
    # all earlier in this function: `populate_variable_slugs` (edge endpoints
    # read `variable.slug`), `derive_classification_succession` (reads
    # `classification_replaced_by`), and `_materialize_replaced_by_edges`
    # (dedups against the curated #375/#440 + auto timeseries_event rows it just
    # inserted — those WIN on a PK collision via INSERT OR IGNORE). It reads
    # `variable_state` / `classification` / `classification_replaced_by` /
    # `variable_replaced_by`, none dropped below. Its count gets its OWN
    # `row_counts` key rather than folding into `_REPLACED_BY_STAT_KEYS` (pinned
    # by test_replaced_by_stats_in_manifest) — these rows ARE in
    # `variable_replaced_by` but on a distinct
    # `derived:classification_vintage_lift` provenance. Guarded by `skip_slugs`
    # like the other slug-keyed linkers: under --skip-slugs `variable.slug` is
    # NULL and `classification_replaced_by` is empty, so it no-ops anyway, but
    # the explicit guard matches convention.
    #
    # NOTE: the corpus-gated floor `_MIN_VARIABLE_VINTAGE_LIFT_EDGES` is the
    # regression guard for this ordering — synthetic (corpus=False) builds don't
    # exercise the floor, so the unit tests in test_relations.py (which bypass
    # `materialize`) can't catch a re-introduced ordering bug.
    if skip_slugs:
        _progress("Skipping variable vintage succession lift (skip_slugs=True)")
    else:
        row_counts["variable_replaced_by_vintage_lift"] = (
            derive_variable_vintage_succession(conn, progress=_progress)
        )

    # A2.7 / A4.4e: drop `variable_instance` + its cvid-grained alias staging +
    # the provider-blind `classification_candidate` before ship. Every build-time
    # reader has run: `_coalesce_variable_states` (→ `variable_state`),
    # `populate_classifications` (tags `classification_id`), value-set projection
    # (`value_set_id`), `code_variable_map` (above), the SCB candidate feed +
    # `_backfill_state_classifications` (just above). The shipped query layer
    # reads `variable_state` / `variable` / re-parented `variable_alias`.
    # `variable_alias_build` FKs `variable_instance(cvid)`, so it must drop FIRST
    # (child before parent) or `PRAGMA foreign_key_check` (below) flags the
    # dangling cvids. `classification_candidate` has NO FK, so its drop order is
    # free. `variable_alias` (shipped) FKs `variable`/`register_variant`, not the
    # dropped tables, so it survives clean. (`variable_context` was dropped from
    # the DDL outright in A2.7 — a write-only debug table with no consumer that
    # would have orphaned on this drop.)
    conn.execute("DROP TABLE variable_alias_build")
    conn.execute("DROP TABLE variable_instance")
    conn.execute("DROP TABLE classification_candidate")
    _progress(
        "Dropped variable_instance + variable_alias_build + "
        "classification_candidate (A2.7/A4.4e)."
    )

    # A2.6: drop the build-only register-edition tables before ship (mirrors
    # the `unika_summary` drop above). `register_version` fed the coalescer's
    # valid_from/to year fallback and the lineage linkers (`*_replaced_by`,
    # `link_variable_state_lineage`), all of which ran above; `population` /
    # `object_type` are write-only debug tables nothing in the shipped
    # catalog reads. The FQID grammar no longer has a version segment,
    # so none of these belong in the shipped DB. Drop order is FK-safe:
    # children (`population`, `object_type` FK `register_version`) first,
    # then the parent — `PRAGMA foreign_key_check` (below) flags children of
    # a dropped parent, so leaving them would fail the build.
    conn.execute("DROP TABLE population")
    conn.execute("DROP TABLE object_type")
    conn.execute("DROP TABLE register_version")
    _progress("Dropped register_version + population + object_type (A2.6).")

    _t = time.perf_counter()
    _populate_fts(conn)
    _emit_timing("populate_fts", _t)

    # Match the legacy `import_manifest.row_counts` key order (byte-identity,
    # dbdiff R5): the SCB-reference table counts ran physically LAST in the
    # pre-A4 build (their imports were the final pass before FTS), so their
    # keys sorted last in the dict. A4.1 moved those imports into the adapter's
    # `emit()`, which records the counts earlier — so re-append them here to
    # restore the baseline ordering. Order matches the baseline: Tabell first,
    # then ID-kolumner.
    for _ref_key in ("Tabelldefinitioner.sql", "ID-kolumner.xlsx"):
        if _ref_key in row_counts:
            row_counts[_ref_key] = row_counts.pop(_ref_key)

    # Provenance: per-provider source-ID linkage. SCB's universal register_id IS
    # the source RegisterId; record the native `Registernamn` alongside it for
    # the maintainer-only provenance DB. A4.3a: collected from the IR stream
    # (register_id, IRRegister.name) above — NOT re-queried from the universal
    # `register` table, so the materializer never reads a provider-native column
    # back. Sorted by register_id for byte-stable provenance writes.
    scb_register_id_map.sort()

    return {
        "source_checksums": source_checksums,
        "row_counts": row_counts,
        "projection_stats": projection_stats,
        "coalesce_stats": state_stats,
        "replaced_by_stats": replaced_by_stats,
        # A4.2 provenance payload (sibling-DB only; never touches the universal
        # schema). `build_db` writes these to reg_meta.provenance.db after the
        # universal swap.
        "provenance": {
            "adapter_warnings": adapter_warnings,
            "delivery_approvals": delivery_approvals,
            "scb_register_id_map": scb_register_id_map,
        },
    }


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
    providers: tuple[str, ...] = ("scb",),
    pre_rename_hook: Callable[[Path], None] | None = None,
    provenance_pre_rename_hook: Callable[[Path], None] | None = None,
) -> dict[str, Any]:
    """Build the reg_meta database from the selected providers' source exports.

    ``input_dir`` must contain:
      - ``<input_dir>/SCB/*.csv``             — SCB metadata CSV exports
      - ``<input_dir>/Socialstyrelsen/*.xlsx``— SOS register workbooks (A4.3b;
        required only when ``"sos"`` is in ``providers``)
      - ``<input_dir>/classifications/*.csv`` — canonical classification CSVs
        (optional; required only for seed entries that set ``valid_codes_file``)

    ``providers`` selects which adapters run. Both the PROGRAMMATIC default and
    the CLI ``--providers`` flag default to ``("scb",)`` so existing fixture
    callers stay behavior-identical and the bare default build stays green while
    SOS lacks curated slugs (sos.toml is A4.4); A4.5 flips the CLI default to
    ``scb,sos``. A4.3b: SOS is purely additive (minted ids in band
    ``[2^62, 2^63)``, content-shared value_sets), so ``providers=("scb",)``
    reproduces the byte-identical pre-SOS SCB-only DB — the `--providers=scb`
    dbdiff gate.

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
    sos_dir = input_dir / "Socialstyrelsen"
    cls_dir = input_dir / "classifications"

    known = {slug for _pid, slug, _name in _PROVIDER_SEED}
    unknown = [p for p in providers if p not in known]
    if not providers or unknown:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="unknown_provider",
            error_class="configuration",
            message=(
                f"--providers must be a non-empty subset of {sorted(known)}; "
                f"got {list(providers)} (unknown: {unknown})."
            ),
            remediation=f"Pass a comma-list of known providers, e.g. {','.join(sorted(known))}.",
        )

    if not input_dir.is_dir():
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="input_dir_not_found",
            error_class="configuration",
            message=f"Input directory not found: {input_dir}",
            remediation="Provide a directory containing SCB/ and classifications/ subdirectories.",
        )

    if "scb" in providers:
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

    if "sos" in providers and not sos_dir.is_dir():
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="sos_dir_not_found",
            error_class="configuration",
            message=f"Socialstyrelsen subdirectory not found: {sos_dir}",
            remediation="Place SOS register workbooks under <input_dir>/Socialstyrelsen/.",
        )

    for prov_slug, dirname in _CURATED_PROVIDERS:
        if prov_slug in providers and not (input_dir / dirname).is_dir():
            raise RegMetaError(
                exit_code=EXIT_CONFIG,
                code=f"{prov_slug}_dir_not_found",
                error_class="configuration",
                message=(
                    f"{dirname} subdirectory not found: {input_dir / dirname} "
                    f"(provider {prov_slug!r} is in the build set)."
                ),
                remediation=(
                    f"The curated {prov_slug}.toml is a small committed seed under "
                    f"<input_dir>/{dirname}/. If --input-dir points at a separate seed "
                    f"checkout, it likely predates this provider's onboarding — update "
                    f"it (e.g. `git -C <seed-checkout> pull`). Otherwise drop "
                    f"`--providers {prov_slug}` to skip it."
                ),
            )

    # Stale-seed preflight (#556): the conditional CanonicalScbAdapter guard below
    # (~3900) SILENTLY skips when scb_canonical/ is absent — correct for genuinely
    # canonical-free synthetic builds. But when the curated scb.toml pins canonical-
    # band register slugs (#444), a missing seed means those registers never mint
    # and populate_slugs later raises a MISLEADING slug_unknown_source_id "mark
    # deprecated". A missing committed seed almost always means a stale --input-dir
    # checkout, so fail fast here with the staleness hint instead.
    if "scb" in providers and not skip_slugs:
        slug_root = slug_dir or repo_slug_dir()
        if slug_root is not None and slug_dir_curates_canonical_scb(slug_root):
            from .sources.curated import CanonicalScbAdapter

            canonical_seed = (
                input_dir / _CANONICAL_SCB_DIRNAME / CanonicalScbAdapter.SOURCE_FILE
            )
            if not canonical_seed.is_file():
                raise RegMetaError(
                    exit_code=EXIT_CONFIG,
                    code="scb_canonical_seed_missing",
                    error_class="configuration",
                    message=(
                        f"Canonical-SCB seed not found: {canonical_seed}. The curated "
                        f"scb.toml pins canonical-SCB register slugs (#444) this build "
                        f"must mint from it."
                    ),
                    remediation=(
                        "input_data/scb_canonical/ is a small committed seed (#444); if "
                        "--input-dir points at a separate seed checkout it likely "
                        "predates that content — update it (e.g. "
                        "`git -C <seed-checkout> pull`). The misleading downstream "
                        "`slug_unknown_source_id` 'mark deprecated' you'd otherwise hit "
                        "is this stale seed, not a bad slug entry."
                    ),
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
    # The build writes to a temp file and atomically renames on success,
    # unlinking it on ANY failure (see `finally` below) — there is nothing to
    # crash-recover, so journaling + fsync buy the artifact nothing. journal_mode
    # OFF + synchronous OFF drop both (the bulk of the ~10% wall win is removed
    # fsync/journal I/O); the page-cache bump keeps the heavy index maintenance
    # and DISTINCT/ORDER-BY sorts off disk. Safe ONLY because of temp-then-rename
    # — do not copy this config to a connection that opens the published DB.
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute(f"PRAGMA cache_size={_BUILD_PAGE_CACHE_KIB}")  # ~2 GiB main page cache
    conn.execute("PRAGMA temp_store=MEMORY")  # classification build uses temp tables
    conn.execute("PRAGMA foreign_keys=OFF")  # Enable after import for speed
    build_failed = True
    try:
        conn.executescript(DDL)
        seed_providers(conn)

        # ATTACH the sibling staging DB (the SCB adapter creates and fills its
        # per-build `_build_cvid_pair` projection table inside it). FK
        # declarations don't work across attached DBs — fine, no main-DB rows
        # reference the staging table. Path is bound (not interpolated) so
        # quotes/specials in the parent dir can't break or inject SQL.
        conn.execute("ATTACH DATABASE ? AS staging", (str(staging_path),))
        # journal_mode/synchronous/cache_size are PER-DATABASE and do NOT
        # propagate to a database attached AFTER they were set on `main`. The
        # 102M-row WITHOUT ROWID staging B-tree — the dominant insert — lives
        # here, so it needs its own OFF/OFF + large cache or it keeps paying
        # rollback-journal + fsync cost. journal_mode can only change in
        # autocommit mode, but seed_providers() left an implicit txn open — commit
        # it first or the change is silently refused (DELETE stays). (Persisting
        # the seed rows early is safe: build-failure unlinks the temp file.)
        conn.commit()
        conn.execute("PRAGMA staging.journal_mode=OFF")
        conn.execute("PRAGMA staging.synchronous=OFF")
        conn.execute(f"PRAGMA staging.cache_size={_BUILD_PAGE_CACHE_KIB}")  # ~2 GiB

        # Provider-blind materialization. Each selected adapter parses its native
        # exports and emits the IR stream; `materialize` loops over them and runs
        # the shared derivation post-passes ONCE. Adapters imported function-local
        # to break the db ↔ sources.* import cycle (those modules import shared
        # infra from this one). ORDER IS LOAD-BEARING: SCB runs before SOS so SOS
        # value_sets content-collapse onto SCB's already-written rows (R2 hybrid).
        from .codelivery import load_codelivery, repo_codelivery_path
        from .source_column_repairs import (
            load_column_merges,
            load_fold_overrides,
            repo_source_column_repairs_path,
        )
        from .sources.curated import CanonicalScbAdapter, CuratedAdapter
        from .sources.scb import SCBAdapter
        from .sources.sos import SOSAdapter

        adapters: list[tuple[Any, Path]] = []
        if "scb" in providers:
            # Co-delivery curation (maintainer artifact, like the slug TOMLs):
            # resolves genuine one-off same-column re-codings the coalescer cascade
            # leaves. SCB-only — loaded INSIDE this branch so a malformed/invalid
            # codelivery.toml can't fail an SOS-only build that never reads it.
            # Empty when the file is absent (wheel installs, synthetic builds).
            codelivery = load_codelivery(repo_codelivery_path())
            # SCB source-column repairs (#196 / #261), same maintainer-artifact
            # shape — two sibling sections in ONE SCB-scoped file
            # (`curation/scb/source_column_repairs.toml`), loaded once and read
            # twice. Fold-overrides fold disjoint-stem CONTESTED columns the
            # triage stem rule would split; column-merges unify never-co-occurring
            # era-rename column twins in the coalescer's rule-2 union-find. Empty
            # file ⇒ no behavioral change (triage/connectivity unchanged).
            repairs_path = repo_source_column_repairs_path()
            fold_overrides = load_fold_overrides(repairs_path)
            column_merges = load_column_merges(repairs_path)
            adapters.append(
                (SCBAdapter(conn, codelivery, fold_overrides, column_merges), scb_dir)
            )
        if "sos" in providers:
            adapters.append((SOSAdapter(conn), sos_dir))
        # Thin curated providers (#422): one shared adapter per agency, each
        # reading its committed `<provider>.toml`. Additive like SOS — minted
        # high-band ids, no scratch.
        for prov_slug, dirname in _CURATED_PROVIDERS:
            if prov_slug in providers:
                adapters.append(
                    (
                        CuratedAdapter(prov_slug, classification_seed_path=seed_path),
                        input_dir / dirname,
                    )
                )
        # Canonical-SCB curated content (#444): SCB registers SWECOV holds but that
        # SCB's machine export lacks (Utrikeshandel med tjänster, …). Attributed to
        # the `scb` provider with low-band ids, and it interns real value sets — so it
        # MUST run after the SCB adapter (value_code AUTOINCREMENT high-water mark);
        # appended last. Guarded on the committed source file so synthetic SCB-only
        # builds (no scb_canonical/ dir) skip it.
        if "scb" in providers:
            canonical_dir = input_dir / _CANONICAL_SCB_DIRNAME
            if (canonical_dir / CanonicalScbAdapter.SOURCE_FILE).is_file():
                adapters.append(
                    (
                        CanonicalScbAdapter(conn, classification_seed_path=seed_path),
                        canonical_dir,
                    )
                )

        _t_mat = time.perf_counter()
        mat = materialize(
            conn,
            adapters,
            seed_path=seed_path,
            cls_dir=cls_dir,
            skip_classifications=skip_classifications,
            slug_dir=slug_dir,
            skip_slugs=skip_slugs,
        )
        _emit_timing("materialize (total)", _t_mat)
        source_checksums = mat["source_checksums"]
        row_counts = mat["row_counts"]
        provenance_payload = mat["provenance"]

        # Write manifest.
        #
        # A4.2 DEFERRAL (resolved fork #3): `source_checksums` + `row_counts`
        # are ALSO written to the sibling provenance DB (see write_provenance_db
        # below), but they STAY in the shipped import_manifest here so the
        # universal DB is byte-identical to the pre-A4 baseline and dbdiff stays
        # exit-0. The provenance-DB design moves them OUT of import_manifest; that
        # REMOVAL is deferred to A4.4+ (where the baseline stops being the gate).
        # Do not drop these two keys here without re-baselining.
        manifest_data = {
            "schema_version": SCHEMA_VERSION,
            "import_date": utc_now(),
            "input_dir": str(input_dir),
            "source_checksums": source_checksums,
            "row_counts": row_counts,
            "projection_stats": mat["projection_stats"],
            # A2.1 coalescer stats — let maintainers eyeball the empirical
            # 5× shrink and unika-vs-fallback split without re-running.
            "coalesce_stats": mat["coalesce_stats"],
            # A2.3 replaced_by stats — fan-out per entity grain plus the
            # skipped-row counters. Lets maintainers verify the inverse-
            # direction collapse worked and spot regressions in the
            # unresolved/ambiguous-id rate without re-running the build.
            "replaced_by_stats": mat["replaced_by_stats"],
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
        _t = time.perf_counter()
        violations = list(conn.execute("PRAGMA foreign_key_check"))
        _emit_timing("foreign_key_check", _t)
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
        # `variable_alias_build` (A2.7) + `classification_candidate` (A4.4e) —
        # so the shipped DB doesn't carry
        # a fat freelist. `validate.py` flags
        # >= 1% freelist as staging-bloat; on the synthetic fixture the drop
        # alone leaves ~2.7%. VACUUM must run outside a transaction — the
        # preceding commit ensures it does. ATTACH-staging was already
        # detached implicitly when the staging path was passed (or stays
        # attached harmlessly; VACUUM only touches `main`).
        _t = time.perf_counter()
        conn.execute("VACUUM")
        _emit_timing("VACUUM", _t)

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
            _unlink_wal_sidecars(tmp_path)
            raise

    # The build connection's clean close deletes the WAL `-wal`/`-shm` sidecars,
    # but the post-build validator re-opens the tmp DB read-only and SQLite
    # re-creates them — a read-only close then leaves them on disk. The atomic
    # rename below moves only the base file, so without this they orphan in the
    # DB dir as `<db>.tmp-wal`/`<db>.tmp-shm`. (Drop them whether or not a hook
    # ran; it's a no-op when none did.)
    _unlink_wal_sidecars(tmp_path)

    # Rotate the prior universal DB aside before the atomic replace
    # (single-generation `.prev`, no auto-cleanup).
    rotate_db_to_prev(final_path)
    tmp_path.rename(final_path)

    # Sibling provenance DB population (A4.2). Runs AFTER the universal swap so
    # build_manifest can carry the FINALIZED universal-DB sha256. Mirrors the
    # universal DB's atomicity: populate a tmp file → rotate the prior live
    # provenance DB aside → rename the tmp into place. The provenance DB is a
    # SEPARATE file dbdiff never opens, so none of this touches the gate.
    #
    # Wrapped in try/except: this runs AFTER the universal DB has already been
    # swapped in, so any failure here (disk full, perms, a provenance write
    # bug) must NOT flip the build's exit code — the primary artifact already
    # succeeded. Surface as a warning instead; the provenance DB is cheap to
    # recreate by re-running the build. (Known limitation: on a REBUILD, a
    # failure before the rotate leaves the PRIOR live provenance DB in place,
    # now stale vs the new universal generation, until the re-run — acceptable
    # for maintainer-only debug data; A4.4+ may harden the failure path.)
    provenance_path = db_dir / PROVENANCE_DB_FILENAME
    provenance_tmp = provenance_path.with_suffix(".db.tmp")
    try:
        # Build the payload INSIDE the try: `_file_sha256(final_path)` reads the
        # just-renamed universal DB, and even that (near-impossible) failure must
        # stay non-fatal — the universal artifact already succeeded.
        payload = {
            "schema_version": SCHEMA_VERSION,
            "universal_db_path": str(final_path),
            # sha256 of the FINALIZED universal DB now sitting at final_path.
            "universal_db_sha256": _file_sha256(final_path),
            "build_date": manifest_data["import_date"],
            "adapter_warnings": provenance_payload["adapter_warnings"],
            "delivery_approvals": provenance_payload["delivery_approvals"],
            "scb_register_id_map": provenance_payload["scb_register_id_map"],
        }
        provenance_tmp.unlink(missing_ok=True)
        write_provenance_db(provenance_tmp, payload)
        # Test/maintenance seam: lets a caller inject a failure AFTER the tmp is
        # written but BEFORE the live file is replaced, to verify a provenance
        # failure never poisons the already-swapped universal DB.
        if provenance_pre_rename_hook is not None:
            provenance_pre_rename_hook(provenance_tmp)
        rotate_db_to_prev(provenance_path)
        provenance_tmp.rename(provenance_path)
    except Exception as e:  # noqa: BLE001 — provenance is non-fatal; the
        # universal DB is already swapped in. Catch BROADLY (not just
        # OSError/RegMetaError): a provenance write bug must never flip the
        # build's exit code or poison the primary artifact.
        provenance_tmp.unlink(missing_ok=True)
        _progress(
            f"  WARNING: provenance DB population failed ({type(e).__name__}: {e}); "
            f"universal DB was written successfully — re-run the build to restore "
            f"{provenance_path.name}."
        )
    _progress(f"Database written to {final_path}")

    return {
        "db_path": str(final_path),
        "schema_version": SCHEMA_VERSION,
        "import_date": manifest_data["import_date"],
        "source_checksums": source_checksums,
        "row_counts": row_counts,
    }
