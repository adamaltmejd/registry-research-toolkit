"""SCB provider adapter for `reg_meta_build`.

Parses Statistics Sweden (SCB) microdata-catalog CSV/SQL/xlsx exports and
runs the build-time triage (see DESIGN.md → Build-time triage (SCB)), value-set
projection, and state coalescer. `SCBAdapter.emit()` yields the provider-neutral
IR stream (`reg_meta_build.ir.*`) consumed by the provider-blind materializer in
`reg_meta_build.db`. See DESIGN.md → IR + adapter architecture.

A4.1 is a pure byte-identical refactor: the SCB ingest functions below moved
out of `db.py` VERBATIM (only their module home changed). The adapter runs
the legacy pipeline against the working connection — writing the universal
catalog tables plus its SCB-named build-scratch (`variable_instance`,
`variable_alias_build`, `unika_summary`, `register_version`, ...) and
SCB-reference tables (`identifier_semantics`, `source_column_type`,
`source_join_key`) — and emits the IR mirror. The materializer runs the
provider-blind derivation post-passes that read the scratch (alias reparent,
classification backfill, code_variable_map, register/variant replaced_by) and
then drops it before ship. Each table has exactly one writer; there is no
parallel old+new path (CLAUDE.md "no shims").

`IRWarning` / `IRDeliveryProvenance` are EMITTED here (so A4.2 only has to
wire them to the provenance DB) but the materializer DISCARDS them in A4.1 —
A4.1 does not populate the provenance DB (resolved fork #1).
"""

from __future__ import annotations

import re
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from itertools import combinations, groupby
from typing import TYPE_CHECKING, Any

from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.fqid import derive_variable_slug, period_token_to_bounds
from reg_meta.queries import extract_year

from reg_meta_build._curation import fold_column

# Shared SCB-CSV / hashing / progress infra + the Vardemangder sentinel
# allowlists + the SCB provider id stay in `db.py` (used by the materializer
# too); import them rather than duplicate.
from reg_meta_build.db import (
    _VALID_FROM_UNKNOWN,
    _VALID_TO_SENTINEL,
    _VARDEMANGDER_REAL_SHAPED,
    _VARDEMANGDER_SENTINELS,
    PROVIDER_ID_SCB,
    _file_sha256,
    _open_scb_csv,
    _progress,
    _value_set_hash,
)
from reg_meta_build.ir import (
    IRDeliveryProvenance,
    IRRegister,
    IRRelatedToEdge,
    IRValueCode,
    IRValueSet,
    IRVariable,
    IRVariableAlias,
    IRVariableState,
    IRVariant,
    IRWarning,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

    from reg_meta_build.codelivery import CodeliveryMap
    from reg_meta_build.column_merges import ColumnMergeMap
    from reg_meta_build.fold_overrides import FoldOverrideMap
    from reg_meta_build.sources import IRObject


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
                    # `Registerrubrik` is dropped (universal-vocabulary rename;
                    # see reg_meta/DESIGN.md → Glossary and Swedish↔English crosswalk).
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
                    # are dropped.
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
                    # The operational definition merges into
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
        # Operational definition folds into description when distinct
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
    # A2.7: cvid-grained alias staging; projected onto the shipped
    # `variable_alias` (variable_id-keyed) via `_emit_variable_aliases` → IR (A4.3a).
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
        # A2.7: these rows land in the cvid-grained `variable_alias_build` staging
        # (projected onto `variable_alias` later via `_emit_variable_aliases` → IR),
        # NOT the shipped `variable_alias` — label by the table actually written.
        "variable_alias_build": len(aliases),
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
    for the same `(register_id, var_id)` carries the truthy value. The SCB
    export encodes these columns as the numeric strings `'1'` (yes) / `'0'`
    (no) — NOT the Swedish `'Ja'`/`'Nej'` the column-NAME might suggest. We
    match `'1'` AND `'Ja'` (the latter defensively, against an older/assumed
    form — matching only one literal is exactly the bug that left these flags
    silently empty for the whole corpus pre-A4); any other value (including
    `'0'` / empty) is falsy. This flag feeds the MONA PII scanner, so a
    false-negative is a real miss — robustness over purity. `kanslig_variabel` and
    `kanslig_variabel_ibland` both fold into `is_sensitive` — the ~22 rows
    flagged only-sometimes don't justify a third column. Returns the number of variable rows whose flags were refreshed
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
        "        MAX(CASE WHEN us.kanslig_variabel IN ('1', 'Ja') "
        "                  OR us.kanslig_variabel_ibland IN ('1', 'Ja') "
        "                 THEN 1 ELSE 0 END) AS is_sensitive, "
        "        MAX(CASE WHEN us.identitetsvariabel IN ('1', 'Ja') "
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

    # Union the authoritative declared-identifier list (Identifierare.csv →
    # identifier_semantics) into is_identifier. The two sources are
    # COMPLEMENTARY: `unika.identitetsvariabel` (above) misses identifiers that
    # Identifierare declares (per-column match gaps), and Identifierare misses a
    # few that unika flags — so OR maximizes recall (the A1.2 lift was
    # unika-only and silently dropped ~7 register-level declared identifiers).
    # Keyed on the source var_id (provider_key), so it lands on the one
    # pre-triage variable per var_id; the triage split then copies it to siblings
    # (`_inherited_flags`). Orphan declared var_ids (no matching variable) no-op.
    # Scoped to SCB registers: identifier_semantics + the numeric `provider_key`
    # are SCB-native (a non-SCB/SOS text provider_key would `CAST` to 0), so the
    # explicit provider scope keeps this correct independent of build order
    # rather than relying on no SOS rows being present yet.
    id_cur = conn.execute(
        "UPDATE variable SET is_identifier = 1 "
        "WHERE is_identifier = 0 "
        f"  AND register_id IN (SELECT register_id FROM register "
        f"WHERE provider_id = {PROVIDER_ID_SCB}) "
        "  AND CAST(provider_key AS INTEGER) IN (SELECT var_id FROM identifier_semantics)"
    )
    declared = id_cur.rowcount or 0
    _progress(f"  {declared:,} additional rows flagged from Identifierare.csv")
    return refreshed


# A2.1: SCB ships VersionForsta/VersionSista as plain year strings ("2020").
# `variable_state.valid_from`/`valid_to` must be full YYYY-MM-DD.
# Expansion rules are deterministic — year N → first day Jan / last day Dec.
# Open-ended (no upper bound observable) → the universal variable_state.valid_to
# DDL sentinel (single source of truth: db._VALID_TO_SENTINEL).
# `_VALID_FROM_UNKNOWN` (db) is the symmetric lower-bound sentinel, used when no
# year is derivable from any signal (yearless cvids like "Person-År").
_VALID_TO_OPEN_SENTINEL = _VALID_TO_SENTINEL


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


# Edition AUTHORITY: when two SCB deliveries map to the same reference year with
# DIFFERENT value sets (the co-delivery root cause), the authoritative one for the
# catalog is determined by SCB's own `registerversionnamn` qualifier. Ranking
# (high wins): a FINAL ("slutlig") annual supersedes a PRELIMINARY ("preliminär")
# one for the same year; a full-year annual supersedes a SUB-ANNUAL slice
# (term/quarter/half/month — these collide at year granularity but are partial);
# an explicit "_old" historical re-coding is the lowest (superseded by anything).
# The per-year timeline (`_coalesce_variable_states`) picks the highest-authority
# value set per (variable, variant, year); equal top authority is a genuine tie
# the deterministic rule can't break → curation (see CODELIVERY_PLAN.md).
_AUTH_FINAL = 4
_AUTH_PLAIN = 3
_AUTH_PRELIM = 2
_AUTH_SUBANNUAL = 1
_AUTH_OLD = 0

# Sub-annual qualifier markers (ascii-folded, lowercased substrings) — terms,
# quarters, half-years, school years, seasons, and Swedish month names. A version
# name carrying any of these delivers a slice of a year, not the full year.
_SUBANNUAL_MARKERS = (
    "termin",
    "kvartal",
    " kv ",
    " kv2",
    "halvar",
    "lasar",
    "hosten",
    "varen",
    "sommar",
    "vt ",
    "ht ",
)

# Compact / bare term forms the string markers above miss: `HT2018` / `VT2018`
# (no trailing space) and a bare `HT` / `VT` token. Mirrors `_LABEL_HT_RE` /
# `_LABEL_VT_RE` used in label-freshness ranking — a contested column must rank
# these sub-annual too, else a compact-term coding ties a full-year annual at
# `_AUTH_PLAIN` (and the later freshness step could even prefer the term).
_SUBANNUAL_TERM_RE = re.compile(r"\bht\d|\bvt\d|\bht\b|\bvt\b")

# Swedish month names — WORD-BOUNDARY matched (ascii-folded) so a token like
# `juni` isn't matched inside `junior`, `maj` inside `majoritet`, etc. (the bare
# substrings these replace mis-ranked such names sub-annual).
_SUBANNUAL_MONTH_RE = re.compile(
    r"\b(?:januari|februari|mars|april|maj|juni|juli|augusti|september"
    r"|oktober|november|december)\b"
)


def _edition_authority(versionname: str | None) -> int:
    """Rank a `registerversionnamn` by catalog authority (see the ranking note
    above). Substring-matched on the ascii-folded, lowercased name so å/ä/ö and
    casing don't matter. Order is load-bearing: `_old` and finality qualifiers
    are checked before the sub-annual markers (a name like 'YYYY, slutlig
    version' must rank FINAL, not fall through)."""
    s = _ascii_fold_lower(versionname)
    if not s:
        return _AUTH_PLAIN
    if "_old" in s:
        return _AUTH_OLD
    if "slutlig" in s or "slutgiltig" in s:
        return _AUTH_FINAL
    if "preliminar" in s or "prel." in s or s.endswith(" prel"):
        return _AUTH_PRELIM
    if (
        any(m in s for m in _SUBANNUAL_MARKERS)
        or _SUBANNUAL_TERM_RE.search(s)
        or _SUBANNUAL_MONTH_RE.search(s)
    ):
        return _AUTH_SUBANNUAL
    return _AUTH_PLAIN


# Sub-annual delivery-window narrowing (#219). `_edition_authority` above answers
# "is this a partial-year slice?" for value-set authority ranking; this answers the
# adjacent but distinct question "what inclusive ISO window did this edition cover?"
# so the materializer can clamp a state's lifetime START/END to the real delivery
# window instead of over-claiming the whole boundary year (the HT-start / VT-end
# over-claim — see DESIGN.md → Sub-annual boundary clamp).
#
# We narrow ONLY the academic-term / quarter / half-year forms, where the slice is
# unambiguous. Everything else — bare year, dated annual, prelim/final, month names,
# seasons (Hösten/Våren/Sommar), läsår, Sommarterminen — expands to the FULL year:
# their sub-year span is ambiguous (a season is not cleanly H1/H2; a läsår spans two
# calendar years) and over-narrowing would drop real coverage. So this is a CURATED
# SUBSET of `_SUBANNUAL_MARKERS` on purpose — authority ranks ANY partial slice down,
# but bounds narrows only the forms whose window is well-defined.
#
# Token → ISO expansion is delegated to reg_meta's `period_token_to_bounds` so a
# query (`HT2024`) and the emitted state bound agree byte-for-byte (identical
# month/day arithmetic, incl. the intentional Feb-29 over-count).

# Term phrase → HT/VT prefix, year on either side. `\bhosttermin(?:en)?\b` covers
# `Höstterminen`/`Hösttermin` (NFKD-folded); `\bht` the compact `HT 2024`/`Ht 2003`/
# `HT2024`. We scan with `finditer` (not a single match) so a multi-term name
# (`Höstterminen 2020 - Vårterminen 2021`, `Komvux HT 1988 - VT 2024`) surfaces every
# term; `_edition_bounds` then keeps only those matching the row's edition year (see
# there). Mirrors reg_meta.fqid._TERMIN_EXTRACT_PATTERNS (höst→HT, vår→VT) but needs
# ALL matches; `_SUBANNUAL_TERM_RE` can't be reused — it carries no year group.
_TERM_BOUND_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bhosttermin(?:en)?\s+(\d{4})\b"), "HT"),
    (re.compile(r"\b(\d{4})\s+hosttermin(?:en)?\b"), "HT"),
    (re.compile(r"\bht\s*(\d{4})\b"), "HT"),
    (re.compile(r"\bvartermin(?:en)?\s+(\d{4})\b"), "VT"),
    (re.compile(r"\b(\d{4})\s+vartermin(?:en)?\b"), "VT"),
    (re.compile(r"\bvt\s*(\d{4})\b"), "VT"),
)

# Quarter: `kvartal N` / `kv N` / `kvN`, optionally a range `N-M` / `N- kv M`
# (`2005 kvartal 2-4`, `2007 kv 2-kv 4`). The quarter token carries no year of its own
# (unlike HT/VT), so it is expanded against the row's edition year. A bare `kvartal`
# with no digit (`2010 Kvartal`) matches nothing → full year (all quarters). Both
# range endpoints are emitted as separate tokens so the union picks up the whole range.
_QUARTER_BOUND_RE = re.compile(r"\bkv(?:artal)?\s*([1-4])(?:\s*-\s*(?:kv\s*)?([1-4]))?")

# Half-year: `Första/Andra halvåret YYYY` → H1/H2 (NFKD-folded `forsta`/`andra` +
# `halvar`). A bare `halvår` with no ordinal can't pick a half → full year.
_HALF_BOUND_RE = re.compile(r"\b(forsta|andra)\s+halvar(?:et)?\s+(\d{4})\b")


def _edition_bounds(
    versionname: str | None, year: int | None
) -> tuple[str, str] | None:
    """Inclusive ISO ``(lo, hi)`` delivery window for a ``registerversionnamn``.

    ``year`` is the row's edition year (`extract_year(registerversionnamn)`). Only
    sub-annual markers whose own year EQUALS ``year`` are narrowed; a marker naming a
    DIFFERENT year is ignored. This ties the window to the edition year, so the result
    is always a subset of ``[year-01-01, year-12-31]`` — it can never invert or escape
    the edition's `[regver_min, regver_max]` band. Two corpus-constructible traps this
    closes:

      - a collection note like ``"Insamling 2019 avseende höstterminen 2020"``
        (`year=2019`) → the 2020 term is dropped → full-year 2019, NOT an inverted
        2020-07-01..2019-12-31 window;
      - a stray out-of-range term like ``"HT 1850, version 2024"`` (`year=2024`) → the
        1850 term is dropped → full-year 2024, NOT a `period_token_to_bounds("HT1850")`
        `FqidError` crash (every retained marker reuses ``year``, which `extract_year`
        already validated to 1900-2099, so the expansion can't raise).

    Quarter markers carry no year and are expanded against ``year`` directly. With no
    matching marker, returns full-year ``(year-01-01, year-12-31)``; with ``year``
    None (yearless row), returns ``None`` so the caller's year/unika fallback fires.
    See the narrowing-scope note above for why seasons/läsår/months stay full-year."""
    if year is None:
        return None
    s = _ascii_fold_lower(versionname)
    if not s:
        return None
    ystr = f"{year:04d}"
    bounds: list[tuple[str, str]] = []
    for pat, prefix in _TERM_BOUND_PATTERNS:
        for m in pat.finditer(s):
            if m.group(1) == ystr:  # only the edition year's term
                bounds.append(period_token_to_bounds(f"{prefix}{ystr}"))
    for m in _QUARTER_BOUND_RE.finditer(s):
        for q in (m.group(1), m.group(2)):
            if q:
                bounds.append(period_token_to_bounds(f"{ystr}-Q{q}"))
    for m in _HALF_BOUND_RE.finditer(s):
        if m.group(2) == ystr:
            half = "1" if m.group(1) == "forsta" else "2"
            bounds.append(period_token_to_bounds(f"{ystr}-H{half}"))
    if bounds:
        return min(lo for lo, _ in bounds), max(hi for _, hi in bounds)
    return f"{ystr}-01-01", f"{ystr}-12-31"


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
    triage below can read it). The 9-component group key lives in the
    `groups` dict; the accumulator carries the year-range signals plus the
    latest-era delivery column for triage."""

    register_id: int
    register_variant_id: int
    var_id: int
    data_type: str | None
    data_length: str | None
    value_set_id: int | None
    value_set_version_label: str | None
    # grain is part of the *group key* (gkey position 7), not stored here.
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
    # #219: inclusive sub-annual ISO envelope, parallel to the year-int
    # regver_min/regver_max above. `from_iso` is the earliest actual delivery START
    # and `to_iso` the latest delivery END across the group's editions (a full-year
    # edition contributes YYYY-01-01/YYYY-12-31, so a sub-annual edition only narrows
    # a boundary when NO full-year edition shares that boundary year). The
    # materializer reads these ONLY at a state's lifetime start/end, to avoid
    # over-claiming the boundary year (see `_edition_bounds`). None when no edition
    # carried a parseable year (the yearless/unika fallback fires instead).
    from_iso: str | None = None
    to_iso: str | None = None
    # Latest-era alias: highest regver_id, ties broken by lexically smallest
    # delivery_column_name. regver_id alone orders alias selection; the row's
    # year only updates regver_min/max.
    latest_alias: str | None = None
    latest_alias_regver: int | None = None
    # The set of register_version (edition) ids this group was observed in. The
    # The contested-column gate buckets by *edition*, not calendar year, so a
    # sub-annual variant (e.g. one with both HT2018 and VT2018) doesn't treat a
    # term-to-term column rename as a same-year co-delivery (Codex #139).
    regvers: set[int] = field(default_factory=set)
    # The set of REFERENCE years this group was observed in (parsed from
    # `registerversionnamn`). Distinct from `regver_min/max`: this carries the
    # GAPS, so the materializer can partition a shape's window into contiguous
    # runs instead of collapsing to `[min, max]` and paving over years the shape
    # wasn't delivered (the co-delivery root cause — see CODELIVERY_PLAN.md).
    regyears: set[int] = field(default_factory=set)
    # year → highest edition AUTHORITY (`_edition_authority`) observed for that
    # year in this group. When two groups (distinct value sets) compete for a
    # year, the per-year timeline picks the higher-authority one (final > plain
    # > preliminary > sub-annual > old); equal top authority is a genuine tie.
    year_authority: dict[int, int] = field(default_factory=dict)
    # year → latest edition approval date (`registerversion_senastgodkanddatum`,
    # ISO so lexical == chronological) observed for that year. The RECENCY
    # tiebreak after authority: when same-authority value sets compete for a year,
    # the one delivered by the most-recently-approved edition supersedes (the old
    # coding lingering in a newer edition loses). Equal date = same delivery = a
    # genuine parallel co-delivery the rule can't break.
    year_approval: dict[int, str] = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────
# Build-time triage (see DESIGN.md → Build-time triage (SCB))
# ─────────────────────────────────────────────────────────────────────────
# The coalescer groups variable_instance rows into pre-triage states (one per
# shape/grain/column 9-tuple). A single source `var_id` can carry several
# states that collide on the universal invariant key
# (variable_id, register_variant_id, valid_from, value_set_version_label).
# Triage resolves every such collision three ways so the uniqueness
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
# — the column stem carries the concept boundary. The discriminator that routes
# a column to its sibling is build-time in-memory only (the per-column
# assignment here) — never a shipped table.

# Rule 2 grain patterns → fold label token (the token discriminates the
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
# adjudicates the fuzzy boundary (curation backlog).
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
    # Delegates to the shared curation fold so the curated column keys
    # (fold_overrides / column_merges / codelivery) canonicalize EXACTLY like
    # the coalescer's rule-2 node-col — one definition, no drift.
    return fold_column(s) if s else ""


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
    """The group's lower-bound YEAR — observed `regver_min`, else `unika_min`.
    `_collapse_residual` buckets pass 1 on this year. Since #219 the emitted
    `valid_from` may be sub-annual (e.g. `YYYY-07-01`), but year-bucketing is a safe
    COARSENING of the uniqueness index: a sub-annual `valid_from` always shares the
    year of its full-year form, so same emitted `valid_from` ⟹ same year ⟹ same
    bucket. The coarser key only ever groups MORE rows together, so it can never
    under-protect the `(variable_id, register_variant_id, valid_from, label)` index."""
    return grp.regver_min if grp.regver_min is not None else grp.unika_min


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
    # gkey → clamped valid_to YEAR. Pass 2 of `_collapse_residual` caps a group
    # whose `[regver_min, regver_max]` span is superseded by a later same-column
    # state; the materializer's fast path emits this as valid_to (overriding the
    # open sentinel). NEVER touches `regver_min`/valid_from, so the unique index
    # keys stay stable. (No default: built positionally alongside `dropped`.)
    clamped_to: dict[tuple, int]
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
# Modest by design; the fuzzy boundary is the curation backlog.
_REP_SUFFIX_TOKENS = frozenset(
    {"red", "old", "ny", "grov", "detalj", "alfa", "alpha", "huvud", "avd", "under"}
)


def _is_representation_suffix(suffix: str) -> bool:
    """True when a column's suffix-past-the-shared-stem is a representation
    axis (empty base, pure digits, or a known coding/grain token) rather than a
    distinct-concept word (`hem` / `skol`)."""
    s = suffix.strip("-_").lower()
    return not s or s.isdigit() or s in _REP_SUFFIX_TOKENS


def _decide_fold_or_split(folded_cols: list[str]) -> str:
    """Decide *fold* vs *split* for one source var_id delivered under ≥2
    distinct columns (rule 3), purely on the column stem: a shared stem ≥
    `_FOLD_MIN_STEM` AND every column's differing suffix being a representation
    token → fold (`Ssyk3`/`Ssyk5`, `BCIV`/`BCIVRED`). A concept-word suffix
    (`Hemkommun`/`Skolkommun`) or no shared stem → split."""
    prefix = _common_prefix_len(folded_cols)
    if prefix >= _FOLD_MIN_STEM and all(
        _is_representation_suffix(c[prefix:]) for c in folded_cols
    ):
        return "fold"
    return "split"


def _cluster_contested(
    contested_cols: list[str],
    *,
    forced_same: list[frozenset[str]] | None = None,
) -> list[list[str]]:
    """Partition one split container's contested columns into fold-clusters
    (#223). Each returned cluster is the column set that folds into ONE variable;
    a singleton cluster is a column that splits into its own variable.

    Two columns share a cluster iff they are pairwise stem-foldable
    (`_decide_fold_or_split([a, b])` == "fold" — shared stem ≥
    `_FOLD_MIN_STEM` and rep-only differing suffixes), grown into connected
    components and then VERIFIED as a whole: a component that does not fold as a
    unit degrades to singletons, so the partition NEVER folds a set the stem rule
    wouldn't. The classification family is intentionally NOT consulted — the
    column stem is the concept boundary (the family signal over-folds distinct
    concepts sharing a code system).

    `forced_same` pre-seeds the union-find with curator-asserted equivalences
    (columns that ARE one concept regardless of stem); those components fold by
    fiat, bypassing the stem verify. It is populated by the curated fold-override
    surface (#261), threaded in from `_triage_groups`."""
    folded = {c: _ascii_fold_lower(c) for c in contested_cols}
    parent = {c: c for c in contested_cols}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)  # deterministic root (lex-min)

    # Curator fiat first: a forced group is one concept regardless of stem.
    # Membership is probed on the folded form — `forced_same` groups are
    # case-folded at load, while a contested component can be raw when the #196
    # co-delivery guard kept its case-twin spellings apart.
    forced_cols: set[str] = set()
    for group in forced_same or ():
        members = [c for c in contested_cols if _ascii_fold_lower(c) in group]
        for a, b in combinations(members, 2):
            union(a, b)
        forced_cols.update(members)

    # Stem-foldable pairwise unions (sorted for deterministic component roots).
    for a, b in combinations(sorted(contested_cols), 2):
        if _decide_fold_or_split([folded[a], folded[b]]) == "fold":
            union(a, b)

    comps: dict[str, list[str]] = defaultdict(list)
    for c in contested_cols:
        comps[find(c)].append(c)

    clusters: list[list[str]] = []
    for members in comps.values():
        members_sorted = sorted(members)
        forced = any(c in forced_cols for c in members_sorted)
        whole_folds = (
            _decide_fold_or_split([folded[c] for c in members_sorted]) == "fold"
        )
        if len(members_sorted) > 1 and (forced or whole_folds):
            clusters.append(members_sorted)  # fold cluster
        else:
            clusters.extend([c] for c in members_sorted)  # singletons → split
    return clusters


def _triage_groups(
    conn: sqlite3.Connection,
    groups: dict[tuple, _StateGroup],
    vid_map: dict[tuple[int, int], int],
    fold_overrides: FoldOverrideMap | None = None,
) -> _TriageResult:
    """Resolve pre-triage state collisions. Mutates the DB (mints
    split-sibling `variable` rows) and returns the per-gkey routing the
    coalescer applies when it materializes `variable_state`.

    `fold_overrides` (#261) is the curated `(register_id, var_id) → fold groups`
    surface: maintainer-asserted equivalences that fold disjoint-stem columns the
    stem rule would split. EMPTY MAP ⇒ byte-identical to the pre-#261 path —
    `forced_same` stays `[]` for every var, so no non-curated var changes."""
    res = _TriageResult({}, {}, set(), {}, {}, [], Counter())
    fold_overrides = fold_overrides or {}
    # Every fold-override key must match a real split container; track which do so
    # an unknown/stale entry fails the build after the loop (not a silent no-op).
    consumed: set[tuple[int, int]] = set()

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

        # Triage acts ONLY on a genuine same-(variant, year) collision
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
        # CO-DELIVERED column pairs: two columns that share at least one edition
        # bucket. `contested` is a UNION across buckets, so contested membership
        # does NOT imply two columns ever co-occurred — the per-pair split kind
        # must gate on this precise pairwise relation, not on `contested`.
        codelivered_pairs: set[frozenset[str]] = set()
        for cols in bucket_cols.values():
            if len(cols) > 1:
                contested |= cols
                for col_a, col_b in combinations(cols - {""}, 2):
                    codelivered_pairs.add(frozenset((col_a, col_b)))
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

        # Curated fold-override (#261): validate + mark consumed HERE, at the
        # container gate, so it's branch-INDEPENDENT — every column a maintainer
        # named must be a contested column of THIS var (a non-contested column
        # can't be force-folded). The `forced_same` groups are only FED into
        # `_cluster_contested` in the split branch below (the one place they
        # change the partition); a var whose whole contested set already folds is
        # still "consumed" here (its columns ARE contested) — redundant, not an
        # error. An unconsumed key (unknown var / never-co-occurring columns) is
        # caught after the loop.
        override_groups = fold_overrides.get((register_id, var_id))
        if override_groups is not None:
            override_cols = set().union(*override_groups)
            # Override columns are case-folded at load; contested components are
            # folded too EXCEPT a co-delivered case-twin group the #196 guard
            # left raw — compare on the folded form.
            unknown_cols = override_cols - {_ascii_fold_lower(c) for c in contested}
            if unknown_cols:
                raise RegMetaError(
                    exit_code=EXIT_CONFIG,
                    code="fold_override_unknown_column",
                    error_class="configuration",
                    message=(
                        f"fold-override for register_id={register_id} "
                        f"var_id={var_id} names column(s) {sorted(unknown_cols)} "
                        f"that are not contested columns of this variable "
                        f"(contested: {sorted(contested)})."
                    ),
                    remediation=(
                        "Only contested (same-edition co-delivered) columns can "
                        "be folded. Fix the column name(s) in "
                        "reg_meta_build/fold_overrides.toml or drop the entry."
                    ),
                )
            consumed.add((register_id, var_id))

        # The fold/split DECISION is made on the *contested* columns only (the
        # real collision); the assignment then covers all columns.
        folded = [_ascii_fold_lower(c) for c in contested_cols]
        if _decide_fold_or_split(folded) == "fold":
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
            # The whole contested set doesn't fold. Sub-cluster by shared stem +
            # rep-only suffix and decide PER CLUSTER, so a var_id mixing foldable
            # + disjoint columns folds each stem-family and splits the rest —
            # instead of over-splitting all of them (#223). Non-contested columns
            # never co-occur, so they're always singletons (own variable), as the
            # legacy `_apply_split(all_cols)` already gave them.
            # Curated fold-overrides (#261) resolve here, keyed by
            # (register_id, var_id): they fold disjoint-stem columns the stem rule
            # would split. Empty (the common case) ⇒ byte-identical to pre-#261.
            forced_same = fold_overrides.get((register_id, var_id), [])
            clusters = _cluster_contested(contested_cols, forced_same=forced_same)
            clusters += [[c] for c in non_contested_cols]
            if any(len(c) > 1 for c in clusters):
                _apply_clustered(
                    conn,
                    groups,
                    by_col,
                    clusters,
                    codelivered_pairs,
                    register_id,
                    var_id,
                    orig_vid,
                    res,
                )
                # Outcome-by-variable counters + a per-var_id `clustered` tally so
                # the build log shows how often the new path fires (#223).
                res.stats["folds"] += sum(1 for c in clusters if len(c) > 1)
                res.stats["splits"] += sum(1 for c in clusters if len(c) == 1)
                res.stats["clustered"] += 1
            else:
                # No foldable sub-cluster → byte-identical to the legacy split-all.
                _apply_split(
                    conn,
                    groups,
                    by_col,
                    all_cols,
                    codelivered_pairs,
                    register_id,
                    var_id,
                    orig_vid,
                    res,
                )
                res.stats["splits"] += 1

    # Every fold-override whose REGISTER is present in this build must have matched
    # a real split container (#261). Scoping to the live registers (not just the
    # exact var) is the synthetic-/partial-build escape, mirroring a codelivery pin
    # for an absent register: a `--providers=sos` or fixture build that never loads
    # register 195 must not fail on its override. But once the register IS built,
    # the override must bind — an unconsumed key then names a typo'd var, or a var
    # whose named columns never co-occur in one edition (so it is not a contested
    # split container). The maintainer's full-corpus build validates every shipped
    # override this way; the column-contested check at the gate caught the rest.
    live_registers = {reg for reg, _ in vid_map}
    unconsumed = {key for key in fold_overrides if key[0] in live_registers} - consumed
    if unconsumed:
        keys = ", ".join(
            f"(register_id={r}, var_id={v})" for r, v in sorted(unconsumed)
        )
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="fold_override_unused",
            error_class="configuration",
            message=(
                f"{len(unconsumed)} fold-override(s) name a variable that is not a "
                f"contested split container in this build: {keys}. The variable's "
                f"named columns never co-occur in one edition, so there is nothing "
                f"to fold."
            ),
            remediation=(
                "Remove the stale entry from reg_meta_build/fold_overrides.toml, "
                "or correct its register_id / var_id / columns."
            ),
        )

    # Universal residual collapse: after fold/split assigned variable_ids and
    # fold labels, guarantee the state-uniqueness invariant holds by making
    # every (variable_id, register_variant_id, valid_from-year) scope carry
    # distinct labels. Catches single-column shape drift AND split-sibling
    # within-column drift (a split sibling can still carry same-year drift that
    # _apply_split alone wouldn't resolve).
    _collapse_residual(groups, res)
    return res


def _spans_overlap(groups: dict[tuple, _StateGroup], gkeys: list[tuple]) -> bool:
    """True iff this (variable_id, register_variant_id) partition needs the
    materializer's per-year TIMELINE: two year-bearing groups carry DISTINCT
    value sets over overlapping `[regver_min, regver_max]` windows, OR a yearless
    group sits on a column carrying >1 distinct value set (its open span would
    overlap the column's other coding). A FALSE partition stays on the fast
    `[min, max]` span path — so there `[min, max]` subsumption is real emitted
    coverage, which is why `_collapse_residual`'s overlap pass only acts on it.
    Shared by the materializer and that pass, so the two agree on the routing."""
    spans: list[tuple[int, int, int]] = []
    col_vs: dict[str, set[int]] = defaultdict(set)
    col_yearless: dict[str, bool] = defaultdict(bool)
    for gk in gkeys:
        g = groups[gk]
        if g.value_set_id is None:
            continue
        col_vs[gk[8]].add(g.value_set_id)
        if not g.regyears:
            col_yearless[gk[8]] = True
        elif g.regver_min is not None and g.regver_max is not None:
            spans.append((g.regver_min, g.regver_max, g.value_set_id))
    # (a) two year-bearing distinct-value-set groups with overlapping windows.
    for i in range(len(spans)):
        lo_i, hi_i, vs_i = spans[i]
        for j in range(i + 1, len(spans)):
            lo_j, hi_j, vs_j = spans[j]
            if vs_i != vs_j and max(lo_i, lo_j) <= min(hi_i, hi_j):
                return True
    # (b) a YEARLESS group on a column carrying >1 distinct value set — it emits
    # an open span (fast path) that would overlap the column's other coding;
    # route the cluster through the timeline so it resolves.
    for col, yearless in col_yearless.items():
        if yearless and len(col_vs[col]) > 1:
            return True
    return False


def _preferred_label(gk: tuple, grp: _StateGroup, res: _TriageResult) -> str:
    """The label `_collapse_residual` PASS 1 prefers when deduping a `valid_from`
    scope: an existing triage override, else a grain token, else the group's
    `value_set_version_label`. Pass 1 writes the winner into `res.labels`. Pass 2
    must NOT use this — it keys on the materializer's EMITTED label
    (`res.labels.get(gk, value_set_version_label)`), which never adds the grain
    token. The two diverge for a grain-bearing group pass 1 left untouched (a scope
    singleton, so no `res.labels` entry): here the grain fallback is picked but
    never emitted, so keying pass 2 on it would collapse distinct vintages."""
    return (
        res.labels.get(gk)
        or _fold_token_from_grain(gk[7])
        or (grp.value_set_version_label or "")
    )


def _collapse_residual(groups: dict[tuple, _StateGroup], res: _TriageResult) -> None:
    """Rule 4 — final collision resolution, in two passes.

    PASS 1 (index-key label dedup): every materializing group is scoped by its
    FINAL uniqueness coordinate (assigned variable_id, register_variant_id,
    valid_from-year); within a scope each surviving group must carry a distinct
    `value_set_version_label` or the unique index fails. Per group the preferred
    label is its fold label (if triage set one), else a grain token, else its own
    value_set_version_label. Processing latest-era first, a free label is kept; a
    *meaningful* label already taken is disambiguated (`-N`); an empty/
    uninformative collision is pure shape/value drift and the group is dropped.
    This preserves multi-vintage (distinct labels) and multi-grain (distinct
    grain tokens) while collapsing drift — and, running after fold/split, also
    resolves split-sibling within-column drift.

    PASS 2 (same-column cross-year overlap): pass 1 keys on `valid_from`-year, so
    two SAME-column, SAME-value-set, SAME-label groups with DIFFERENT lower
    bounds but overlapping `[regver_min, regver_max]` spans (a temporal
    supersession the index can't see) slip through. Only the materializer's FAST
    path emits a contiguous `[min, max]` span, so there `[min, max]` subsumption
    is real emitted overlap; timeline partitions (distinct value sets) de-overlap
    per-year already and are left to the materializer (`_spans_overlap` gates
    this). Within each fast-path partition, same-(column, value set, label)
    groups are swept ascending by lower bound against a running "container": a
    group fully inside the container is DROPPED (its coverage is redundant); a
    group that starts inside it but extends past range-CLAMPS the container
    (`res.clamped_to`) to end the year before this group begins, then becomes the
    new container — never touching `regver_min`/valid_from, so the index keys
    stay stable. Distinct value sets are the timeline/validator's domain and are
    never reconciled here."""
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
            preferred = _preferred_label(gk, grp, res)
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

    # ── Pass 2: same-column cross-year overlap reconciliation ───────────────
    # Partition the post-pass-1 survivors by (assigned vid, register_variant_id);
    # act only on fast-path partitions, where each group emits a contiguous span.
    fp_partitions: dict[tuple[int, int], list[tuple]] = defaultdict(list)
    for gkey, grp in groups.items():
        if gkey in res.dropped:
            continue
        vid = res.assignments.get(gkey)
        if vid is None:
            continue
        fp_partitions[(vid, grp.register_variant_id)].append(gkey)

    for part_gkeys in fp_partitions.values():
        if len(part_gkeys) <= 1 or _spans_overlap(groups, part_gkeys):
            continue  # singleton, or a timeline partition the materializer owns
        # Sub-group by (delivery column, value set, EMITTED label): only the SAME
        # column carrying the SAME coding under the SAME emitted label can be
        # temporal drift. `latest_alias` is the emitted `delivery_column_name`
        # (None matches None). The label MUST be the materializer's emitted formula
        # (`_append_state`: `res.labels.get(gk, value_set_version_label)`), NOT pass
        # 1's grain-aware `_preferred_label`: the materializer never applies the
        # grain token, so keying on it would wrongly merge distinct
        # `value_set_version_label` vintages that share codes + grain (e.g.
        # "SSYK2012"/"SSYK2012rev") and collapse one the materializer ships intact.
        subgroups: dict[tuple, list[tuple]] = defaultdict(list)
        for gk in part_gkeys:
            grp = groups[gk]
            emitted_label = res.labels.get(gk, grp.value_set_version_label or "")
            subgroups[(grp.latest_alias, grp.value_set_id, emitted_label)].append(gk)

        for sub_gkeys in subgroups.values():
            if len(sub_gkeys) <= 1:
                continue
            # Need both bounds on every member to compare spans; the narrowed
            # (lo, hi, gkey) list also satisfies the type checker for the sweep.
            bounds: list[tuple[int, int, tuple]] = []
            for gk in sub_gkeys:
                lo, hi = groups[gk].regver_min, groups[gk].regver_max
                if lo is None or hi is None:
                    break
                bounds.append((lo, hi, gk))
            if len(bounds) != len(sub_gkeys):
                continue
            # Sweep ascending, merging overlaps: drop a fully-subsumed group,
            # clamp a crossing one to end the year before the next begins.
            bounds.sort(key=lambda b: (b[0], b[1]))
            container_lo, covered_to, container_gk = bounds[0]
            for lo, hi, gk in bounds[1:]:
                if lo > covered_to:  # gap → this group opens a fresh container
                    container_lo, covered_to, container_gk = lo, hi, gk
                elif hi <= covered_to:  # subsumed by the container → drop
                    res.dropped.add(gk)
                else:  # crossing: clamp the container, then advance to this group
                    # Pass 1 already deduped same-valid_from collisions, so lower
                    # bounds are distinct (lo > container_lo) and the clamp can
                    # never empty the container's span.
                    assert lo - 1 >= container_lo
                    res.clamped_to[container_gk] = lo - 1
                    container_lo, covered_to, container_gk = lo, hi, gk


def _inherited_flags(
    conn: sqlite3.Connection, orig_vid: int
) -> tuple[str | None, int, int]:
    """The `(name, is_sensitive, is_identifier)` a split sibling inherits from
    its origin variable. The A1.2 sensitivity/identity flags are lifted
    PRE-triage (`_populate_sensitivity_flags`), so siblings minted during the
    split MUST copy them — otherwise the INSERT defaults both to 0 and the flag
    survives only on the lex-first column."""
    row = conn.execute(
        "SELECT name, is_sensitive, is_identifier FROM variable WHERE variable_id = ?",
        (orig_vid,),
    ).fetchone()
    return (row[0], row[1], row[2]) if row else (None, 0, 0)


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
    # name/provider_key chain instead of emitting a slug that fails the
    # grammar. (Split siblings already route through derive_variable_slug; only
    # this fold-stem path bypassed it.)
    stem_raw = _ascii_fold_lower(named_cols[0])[:stem_len].strip("-_")
    hint = derive_variable_slug(stem_raw) or derive_variable_slug(named_cols[0])
    if hint:
        res.fold_slug_hints[orig_vid] = hint


# ── split relation_kind heuristics (#218) ─────────────────────────────────
# A triage SPLIT links its sibling variables with `variable_related_to` edges.
# The kind is decided PER CO-DELIVERED PAIR — only two columns that actually
# shared an edition bucket may receive a specific kind, because a code/label
# pairing or a mis-typed-delivery suspicion is a claim about TWO columns SEEN
# TOGETHER. A pair that never co-occurred (a temporal/renamed sibling, or two
# `contested` columns that — `contested` being a union across buckets — never
# shared one bucket) stays generic; its pairwise code/datatype signals are
# meaningless across editions. Precedence for a co-delivered pair, most
# specific first:
#   1. code_vs_label_pair — name-based, high confidence
#   2. import_bug_suspect — data_type/shape mismatch, lower confidence
#   3. same_definition_different_column — generic fallback
# NEVER `same_concept_different_grain`: that is a FOLD-only kind and folds emit
# no edges at all (DESIGN.md → Build-time triage (SCB)).

# A label column carries the Swedish `namn` (name) suffix; its partner code
# column is either the bare stem (`Kommun`/`Kommunnamn`) or carries a `kod`/`id`
# code suffix (`Lid`/`LNamn`, `Sun2000Kod`/`Sun2000Namn`).
_CODE_SUFFIXES = ("kod", "id")
_LABEL_SUFFIX = "namn"


def _strip_suffix(folded: str, suffixes: tuple[str, ...]) -> str | None:
    """The non-empty stem when `folded` ends with one of `suffixes`, else None."""
    for suf in suffixes:
        if folded.endswith(suf) and len(folded) > len(suf):
            return folded[: -len(suf)]
    return None


def _is_code_then_label(code: str, label: str) -> bool:
    """True when `code` is a code column and `label` its matching label column:
    `label` is `<stem>namn` and `code` is either the bare `<stem>` or
    `<stem>kod`/`<stem>id` on the SAME stem."""
    stem = _strip_suffix(label, (_LABEL_SUFFIX,))
    if stem is None:
        return False
    if code == stem:  # bare-stem code paired with its `<stem>namn` label
        return True
    return _strip_suffix(code, _CODE_SUFFIXES) == stem


def _looks_like_code_label_pair(col_a: str, col_b: str) -> bool:
    """A code column paired with its label column, in either order. Name-based
    only (the old #132 heuristic, re-derived to current conventions)."""
    a, b = _ascii_fold_lower(col_a), _ascii_fold_lower(col_b)
    if not a or not b:
        return False
    return _is_code_then_label(a, b) or _is_code_then_label(b, a)


# data_type marker substrings. SCB ships SQL-ish lowercased types (`int`,
# `text`); SOS-style Swedish labels (`Heltal`, `Sträng (text)`, `Datum`) reach
# the same column. Substring match is safe — the field only ever holds a type
# name — and the two marker sets are disjoint across known types, so order
# doesn't matter.
_NUMERIC_TYPE_MARKERS = ("int", "tal", "num", "dec", "float", "real", "double")
_TEXT_TYPE_MARKERS = ("text", "char", "strang", "string", "varchar")


def _data_type_class(dt: str | None) -> str:
    """Coarse `numeric` / `text` / `other` class for a data_type. `other` covers
    dates and anything unrecognized (never claimed numeric or text)."""
    s = _ascii_fold_lower(dt)
    if any(m in s for m in _TEXT_TYPE_MARKERS):
        return "text"
    if any(m in s for m in _NUMERIC_TYPE_MARKERS):
        return "numeric"
    return "other"


def _representative_group(
    gkeys: list[tuple], groups: dict[tuple, _StateGroup]
) -> _StateGroup | None:
    """The latest-era group for a column — highest `latest_alias_regver` (the
    edition that set the surviving delivery alias), deterministic stringified-
    gkey tiebreak. Mirrors the `latest_alias` rule so the compared shape is the
    column's latest delivered shape. None when no gkey resolves to a group
    (defensive: the kind then falls back to generic)."""
    present = [(gk, groups[gk]) for gk in gkeys if gk in groups]
    if not present:
        return None
    _, grp = max(
        present,
        key=lambda item: (
            item[1].latest_alias_regver
            if item[1].latest_alias_regver is not None
            else -1,
            tuple("" if x is None else str(x) for x in item[0]),
        ),
    )
    return grp


def _import_bug_suspect(a: _StateGroup | None, b: _StateGroup | None) -> bool:
    """Lower-confidence shape heuristic: the two split siblings disagree on
    physical SHAPE in a way that suggests one delivery was mis-imported. Primary
    signal — a numeric-vs-text `data_type` mismatch (the canonical SCB/SOS import
    bug: one lumped delivery shipped as a number, the other as text). Fallback —
    when `data_type` can't be classified on at least one side, a present-on-both
    `data_length` disagreement is the only remaining shape evidence. A SAME-class
    length difference is deliberately NOT flagged: on genuinely-distinct split
    siblings differing widths are normal and would fire this 'suspect' label on
    nearly every split, diluting the taxonomy."""
    if a is None or b is None:
        return False
    class_a, class_b = _data_type_class(a.data_type), _data_type_class(b.data_type)
    if {class_a, class_b} == {"numeric", "text"}:
        return True
    if "other" in (class_a, class_b) and a.data_length and b.data_length:
        return a.data_length != b.data_length
    return False


def _split_relation_kind(
    col_a: str,
    col_b: str,
    groups: dict[tuple, _StateGroup],
    by_col: dict[str, list[tuple]],
    codelivered_pairs: set[frozenset[str]],
) -> str:
    """relation_kind for ONE split sibling pair, decided from the pair's two
    delivery columns (precedence in the section note above). A pair that is not
    CO-DELIVERED (never shared an edition bucket) short-circuits to the generic
    kind BEFORE the heuristics run — across editions the pairwise code/datatype
    signals are meaningless."""
    if frozenset((col_a, col_b)) not in codelivered_pairs:
        return "same_definition_different_column"
    if _looks_like_code_label_pair(col_a, col_b):
        return "code_vs_label_pair"
    if _import_bug_suspect(
        _representative_group(by_col[col_a], groups),
        _representative_group(by_col[col_b], groups),
    ):
        return "import_bug_suspect"
    return "same_definition_different_column"


def _apply_split(
    conn: sqlite3.Connection,
    groups: dict[tuple, _StateGroup],
    by_col: dict[str, list[tuple]],
    named_cols: list[str],
    codelivered_pairs: set[frozenset[str]],
    register_id: int,
    var_id: int,
    orig_vid: int,
    res: _TriageResult,
) -> None:
    """SPLIT: each distinct column becomes its own sibling `variable` (sharing
    the source provider_key); link siblings with variable_related_to edges.
    Sibling slugs derive later from each sibling's own reassigned column.
    `codelivered_pairs` is the set of unordered column pairs that actually
    shared an edition bucket — only those pairs get a specific relation_kind."""
    # First column (lexically) keeps the original variable; the rest mint new
    # sibling variables. Name + sensitivity/identity flags are shared: siblings
    # are column-variants of ONE source var_id (same concept family), so a flag
    # set on the pre-split variable applies to every sibling. `is_sensitive` /
    # `is_identifier` are lifted PRE-triage (`_populate_sensitivity_flags`), so
    # an INSERT that omitted them would default both to 0 and silently drop the
    # flag on all but the lex-first column — the source of ~201 false-negative
    # identifiers across the corpus (flags vs split ordering).
    shared_name, shared_sensitive, shared_identifier = _inherited_flags(conn, orig_vid)
    sibling_vids = [orig_vid]
    for col in named_cols[1:]:
        cur = conn.execute(
            "INSERT INTO variable "
            "(register_id, provider_key, name, is_sensitive, is_identifier) "
            "VALUES (?, CAST(? AS TEXT), ?, ?, ?)",
            (register_id, var_id, shared_name, shared_sensitive, shared_identifier),
        )
        new_vid = cur.lastrowid
        assert new_vid is not None  # lastrowid is set after an INSERT
        sibling_vids.append(new_vid)
        for gk in by_col[col]:
            res.assignments[gk] = new_vid
    # (N choose 2) edges between siblings (both directions are emitted at
    # materialization). The relation_kind is computed PER CO-DELIVERED PAIR from
    # the pair's two delivery columns — code_vs_label_pair / import_bug_suspect
    # are claims about two columns seen together, not the whole split; a pair
    # that never shared an edition bucket stays generic. sibling_vids[k] ⟷
    # named_cols[k] by construction (sibling_vids[0] = orig_vid ⟷ named_cols[0],
    # then named_cols[1:] append in order), so zip re-pairs each vid with its
    # column. EVERY pair still emits an edge in the same order — identity (the
    # vid pairs and their order) is unchanged; only the kind label is refined.
    for i, (vid_a, col_a) in enumerate(zip(sibling_vids, named_cols)):
        for vid_b, col_b in zip(sibling_vids[i + 1 :], named_cols[i + 1 :]):
            kind = _split_relation_kind(col_a, col_b, groups, by_col, codelivered_pairs)
            res.related_edges.append((vid_a, vid_b, kind))


def _apply_clustered(
    conn: sqlite3.Connection,
    groups: dict[tuple, _StateGroup],
    by_col: dict[str, list[tuple]],
    clusters: list[list[str]],
    codelivered_pairs: set[frozenset[str]],
    register_id: int,
    var_id: int,
    orig_vid: int,
    res: _TriageResult,
) -> None:
    """Per-cluster triage (#223): each cluster becomes ONE sibling `variable`
    (sharing the source provider_key). A multi-column cluster FOLDS its columns
    into one variable (`_apply_fold` — value-set-version labels + shared-stem slug
    hint); a singleton cluster is a plain split sibling. This generalizes
    `_apply_split`, which is the all-singletons special case.

    The lexically-first cluster keeps `orig_vid`; the rest mint siblings with the
    inherited name/sensitivity/identity flags (see `_apply_split` for why the
    flags must be copied). Sibling edges link the cluster variables; the
    relation_kind is decided from each cluster's REPRESENTATIVE (lex-min) column
    via PR1's `_split_relation_kind` — so a specific kind (`code_vs_label_pair` /
    `import_bug_suspect`) needs the two REPS to have co-delivered, and a non-rep
    cross-member co-delivery degrades to the generic kind (an acceptable
    precision tradeoff on an informational label)."""
    clusters_sorted = sorted(clusters, key=lambda c: min(c))
    shared_name, shared_sensitive, shared_identifier = _inherited_flags(conn, orig_vid)
    cluster_vids: list[int] = []
    for i, cluster_cols in enumerate(clusters_sorted):
        if i == 0:
            vid = orig_vid  # lex-first cluster keeps the original variable
        else:
            cur = conn.execute(
                "INSERT INTO variable "
                "(register_id, provider_key, name, is_sensitive, is_identifier) "
                "VALUES (?, CAST(? AS TEXT), ?, ?, ?)",
                (register_id, var_id, shared_name, shared_sensitive, shared_identifier),
            )
            vid = cur.lastrowid
            assert vid is not None  # lastrowid is set after an INSERT
        cluster_vids.append(vid)
        for col in cluster_cols:
            for gk in by_col[col]:
                res.assignments[gk] = vid
        if len(cluster_cols) > 1:
            _apply_fold(
                groups,
                by_col,
                cluster_cols,
                [_ascii_fold_lower(c) for c in cluster_cols],
                vid,
                res,
            )
    # (N_clusters choose 2) edges, kind from each cluster's representative column.
    reps = [min(c) for c in clusters_sorted]
    for i, vid_a in enumerate(cluster_vids):
        for j in range(i + 1, len(cluster_vids)):
            kind = _split_relation_kind(
                reps[i], reps[j], groups, by_col, codelivered_pairs
            )
            res.related_edges.append((vid_a, cluster_vids[j], kind))


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
    # Inherit name + sensitivity/identity flags from the origin (see _apply_split).
    shared_name, shared_sensitive, shared_identifier = _inherited_flags(conn, orig_vid)
    vids = [orig_vid]
    for col in non_contested_cols:
        cur = conn.execute(
            "INSERT INTO variable "
            "(register_id, provider_key, name, is_sensitive, is_identifier) "
            "VALUES (?, CAST(? AS TEXT), ?, ?, ?)",
            (register_id, var_id, shared_name, shared_sensitive, shared_identifier),
        )
        nvid = cur.lastrowid
        assert nvid is not None
        vids.append(nvid)
        for gk in by_col[col]:
            res.assignments[gk] = nvid
    # Generic kind by design (NOT the per-pair `_apply_split` heuristics): a
    # non-contested column never co-occurs with another in one edition, so these
    # siblings are temporal/rename variants, not parallel code/label or mis-typed
    # co-deliveries — the pairwise code/datatype signals are meaningless here.
    for i, a in enumerate(vids):
        for b in vids[i + 1 :]:
            res.related_edges.append((a, b, "same_definition_different_column"))


# ─────────────────────────────────────────────────────────────────────────
# Per-(variable, variant) year timeline — co-delivery resolution
# ─────────────────────────────────────────────────────────────────────────
# The materializer below emits one state per group spanning `[regver_min,
# regver_max]`. That single span CANNOT express gaps: a value set delivered
# 1998-2009 + 2011-2025 (with another value set in 2010) still emits 1998-2025,
# overlapping the 2010 state — the co-delivery root cause (CODELIVERY_PLAN.md).
# When a `(variable, variant)` has OVERLAPPING distinct-value-set groups, the
# functions here rebuild its states from per-year occupancy of the OBSERVED
# editions (`_StateGroup.regyears`), resolving each contested year via a
# deterministic cascade (authority → recency → cosmetic) and re-deriving each
# group's window as the contiguous RUNS of the years it actually won. Years a
# group lost (a competitor superseded it) carve out of its window. A residual
# year still held by ≥2 distinct value sets is a GENUINE co-delivery that no
# deterministic signal resolves — those states stay overlapping so the
# `validate.py` invariant FAILS the build until curated.

_COSMETIC_MAX_SYM = 2  # symmetric code-count diff treated as cosmetic drift


def _rle_runs(years: list[int]) -> list[tuple[int, int]]:
    """Run-length-encode a sorted year list into maximal contiguous
    `(start, end)` runs (a gap > 1 year starts a new run)."""
    runs: list[tuple[int, int]] = []
    for y in years:
        if runs and y == runs[-1][1] + 1:
            runs[-1] = (runs[-1][0], y)
        else:
            runs.append((y, y))
    return runs


# Grain markers (ascii-folded substrings) flagging a HISTORICAL re-coding —
# `Kommun historisk`, `Län historisk`, `… tidigare`. SCB co-delivers these
# alongside the current coding in a re-issue edition (e.g. the 2010 LKF re-issue);
# on one column the current coding supersedes the historical one. A column-period
# physically holds ONE coding, so this is a VINTAGE choice, not a representation.
_HISTORICAL_GRAIN_MARKERS = ("historisk", "tidigare")


def _is_historical_grain(grain: str | None) -> bool:
    s = _ascii_fold_lower(grain)
    return any(m in s for m in _HISTORICAL_GRAIN_MARKERS)


_LABEL_DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")
_LABEL_ACADEMIC_RE = re.compile(r"\d{4}\s*/\s*\d{4}")
# `\bht\d` also matches the no-space form `HT1986` (no word boundary between the
# `t` and the digit, so a bare `\bht\b` would miss it).
_LABEL_HT_RE = re.compile(r"\bht\d|\bht\b|hosttermin|hosten")
_LABEL_VT_RE = re.compile(r"\bvt\d|\bvt\b|vartermin|varen")


def _label_resolution_rank(
    label: str | None,
) -> tuple[int, int, tuple[int, int, int], int]:
    """A sortable 'freshness' key for SCB's RECURRING co-delivery families, read
    from the value-set version label (higher wins). These are SCB delivery
    conventions — they live in this adapter, not the provider-blind layer:
      - FINALITY: a `slutlig`/plain coding beats a `preliminär` one.
      - CALENDAR over ACADEMIC: `Kurskod 2001` beats `Kurskod 2001/2002`.
      - DATED snapshot: the later `YYYY-MM-DD` wins (`2015-10-15` > `2015-05-15`).
      - TERM: autumn (`HT`) after spring (`VT`).
    One-off genuine re-codings (`Br92-kod`/`Br07-kod`, `Ja nej 1`/`Ja nej 3`) carry
    none of these markers → equal rank → they fall through to curation."""
    s = _ascii_fold_lower(label)
    finality = 0 if "preliminar" in s else 1
    academic = -1 if _LABEL_ACADEMIC_RE.search(s) else 0  # calendar (0) > academic (-1)
    dm = _LABEL_DATE_RE.search(s)
    date_key = (int(dm[1]), int(dm[2]), int(dm[3])) if dm else (0, 0, 0)
    term = 2 if _LABEL_HT_RE.search(s) else (1 if _LABEL_VT_RE.search(s) else 0)
    return (finality, academic, date_key, term)


def _resolve_year_winners(
    cands: list[tuple],
    groups: dict[tuple, _StateGroup],
    year: int,
    codes_fn: Callable[[int], frozenset[str]],
    codelivery: CodeliveryMap | None = None,
    labels: dict[tuple, str] | None = None,
    code_labels_fn: Callable[[int], dict[str, str]] | None = None,
) -> tuple[list[tuple], bool]:
    """Pick the winning group(s) for one contested `year`, COLUMN-AWARE.

    A delivery column (gkey[8]) is one representation of the concept; distinct
    columns are parallel representations (SSYK 3/5-digit, age 5/10-yr brackets)
    that legitimately CO-EXIST under one FQID — they are NOT a conflict. So
    candidates are partitioned by column and each column resolves INDEPENDENTLY to
    one winner; the winners across columns all survive. `is_genuine` is True only
    when a SINGLE column still holds >1 distinct value set after its cascade (a
    real same-column conflict the build can't resolve → curation).

    `codelivery` is the curation map (`(register_id, var_id, column) → keep label`)
    consulted for genuine one-off conflicts before they're flagged. `labels` is
    the triage EMITTED labels (`triage.labels`) so a curation pin matches the label
    that lands in `variable_state` (what the maintainer drafts from), not the raw
    source `value_set_version_label` (which a fold/collapse may relabel).
    """
    by_col: dict[str, list[tuple]] = defaultdict(list)
    for gk in cands:
        by_col[gk[8]].append(gk)
    winners: list[tuple] = []
    any_genuine = False
    for col_cands in by_col.values():
        w, genuine = _resolve_column_year(
            col_cands, groups, year, codes_fn, codelivery, labels, code_labels_fn
        )
        winners.extend(w)
        any_genuine = any_genuine or genuine
    return winners, any_genuine


def _resolve_column_year(
    cands: list[tuple],
    groups: dict[tuple, _StateGroup],
    year: int,
    codes_fn: Callable[[int], frozenset[str]],
    codelivery: CodeliveryMap | None = None,
    labels: dict[tuple, str] | None = None,
    code_labels_fn: Callable[[int], dict[str, str]] | None = None,
) -> tuple[list[tuple], bool]:
    """Resolve ONE column's groups for a contested year to a single winner. The
    within-column cascade:
      1. AUTHORITY — keep groups at the highest `year_authority`.
      2. RECENCY   — keep groups at the latest `year_approval`.
      3. CURRENT   — drop HISTORICAL-grain groups if a non-historical one survives.
      4. value-set fold — survivors sharing one value set (or all code-less) → one rep.
      5. SUPERSESSION — sequential vintages: the latest-INTRODUCED value set (max
         min observed year) wins the transition year; equal introduction continues.
      6. SAME-LABEL drift — distinct value sets sharing one source label (`-N`
         collapse near-dups) → keep the largest.
      7. LABEL FRESHNESS — SCB recurring families (`_label_resolution_rank`):
         final>preliminary, calendar>academic year, latest dated snapshot, HT>VT.
      8. CURATION — a `codelivery` entry pinning the kept label for this
         (register, var, column) wins (the one-off escape hatch).
      9. EXTENDS-LATER — introduction tied at SUPERSESSION, but one coding's
         timeline reaches a strictly later period (max `regver_max`) → it's the
         live/newer coding of a sequential re-coding; keep it. Co-extensive codings
         tie and fall through.
     10. COSMETIC  — distinct value sets within `_COSMETIC_MAX_SYM` symmetric codes
         → keep the largest. GATED label-aware: a small symmetric diff that hides a
         RELABELED shared code (same code, different label) is a genuine re-coding,
         not cosmetic drift, so it does NOT collapse here — it falls to GENUINE.
     11. GENUINE   — distinct, non-cosmetic value sets on ONE column → all reps,
         `is_genuine` (overlapping → the invariant flags it for curation).
    """
    if len(cands) == 1:
        return cands, False
    top_auth = max(groups[gk].year_authority.get(year, _AUTH_PLAIN) for gk in cands)
    cands = [
        gk
        for gk in cands
        if groups[gk].year_authority.get(year, _AUTH_PLAIN) == top_auth
    ]
    top_appr = max(groups[gk].year_approval.get(year, "") for gk in cands)
    cands = [gk for gk in cands if groups[gk].year_approval.get(year, "") == top_appr]
    non_hist = [gk for gk in cands if not _is_historical_grain(gk[7])]
    if non_hist and len(non_hist) < len(cands):
        cands = non_hist

    by_vs: dict[int | None, list[tuple]] = defaultdict(list)
    for gk in cands:
        by_vs[groups[gk].value_set_id].append(gk)
    nonnull = {vs: gks for vs, gks in by_vs.items() if vs is not None}
    if len(nonnull) <= 1:
        # One real value set (code-less groups are exempt from the invariant);
        # pick the representative of the value set if present, else any rep.
        pool = next(iter(nonnull.values())) if nonnull else cands
        return [_pick_state_rep(pool, groups)], False

    # SUPERSESSION: a physical column holds one coding per period, so two distinct
    # value sets overlapping ON ONE COLUMN are sequential vintages whose transition
    # year is claimed by both (SNI2002→SNI2007 at 2008, RTB-2019→RTB-2020 at 2020).
    # The most-recently-INTRODUCED coding (latest min observed year) supersedes at
    # the boundary; the predecessor carves to end before it. Fires only when one
    # value set is strictly later — EQUAL introduction (same-span re-codings on one
    # column: Br92/Br07, Ja-nej-1/Ja-nej-3) stays a genuine conflict → curation.
    vs_min: dict[int, int] = {}
    for vs, gks in nonnull.items():
        vs_min[vs] = min(
            (groups[gk].regver_min for gk in gks if groups[gk].regver_min is not None),
            default=1 << 30,
        )
    latest = max(vs_min.values())
    if sum(1 for m in vs_min.values() if m == latest) == 1:
        win_vs = max(vs_min, key=lambda v: vs_min[v])
        return [_pick_state_rep(nonnull[win_vs], groups)], False

    # SAME-LABEL DRIFT: distinct value sets on one column carrying the SAME source
    # version label are re-coding drift of ONE concept — triage disambiguated them
    # with a `-N` suffix (`SEI_PSU`/`SEI_PSU-1`, `4pos`/`4pos-1`). Keep the largest
    # (most complete). Genuinely different codings carry DIFFERENT labels
    # (`Br92-kod`/`Br07-kod`, prelim/final, sub-annual dates) and fall through to
    # curation. Provider-agnostic: any same-label coding drift collapses here.
    orig_labels = {
        groups[gk].value_set_version_label or ""
        for gk in cands
        if groups[gk].value_set_id is not None
    }
    if len(orig_labels) == 1:
        win_vs = max(nonnull, key=lambda v: (len(codes_fn(v)), v))
        return [_pick_state_rep(nonnull[win_vs], groups)], False

    # LABEL FRESHNESS (SCB recurring families): final>preliminary, calendar>academic
    # year, latest dated snapshot, autumn>spring term. One value set strictly fresher
    # → it wins; equal (one-off genuine re-codings) falls through to curation.
    vs_fresh = {
        vs: max(
            _label_resolution_rank(groups[gk].value_set_version_label) for gk in gks
        )
        for vs, gks in nonnull.items()
    }
    top_fresh = max(vs_fresh.values())
    if sum(1 for f in vs_fresh.values() if f == top_fresh) == 1:
        win_vs = max(vs_fresh, key=lambda v: vs_fresh[v])
        return [_pick_state_rep(nonnull[win_vs], groups)], False

    # CURATION (one-off escape hatch): an explicit pin for this (register, var,
    # column) keeps the named value-set label. register/var/column are constant
    # across `cands` (partitioned by column, one variable's timeline). A pin that
    # doesn't match THIS year's survivors falls through to GENUINE, so the
    # coalescer FAILS the build (`coalesce_unresolved_codelivery`) naming the
    # column + candidate labels: a stale pin, a typo, or a pin covering only some
    # years of a multi-year column all surface as an actionable build error rather
    # than a silently shipped ambiguous column.
    if codelivery:
        reg_id, var_id, column = cands[0][0], cands[0][2], cands[0][8]
        # Pin keys are case-folded at load; fold the component too — it can be
        # raw when the #196 co-delivery guard kept case-twin spellings apart
        # (the folded key then pins ALL spellings of the header, by design).
        entry = codelivery.get((reg_id, var_id, _ascii_fold_lower(column) or column))
        if entry is not None:
            keep_label, keep_rule = entry
            if keep_label is not None:
                # Match the EMITTED label (what lands in variable_state, what the
                # maintainer drafts the pin from) — a fold/collapse can relabel
                # the raw source `value_set_version_label`.
                target = keep_label.strip()

                def _emitted(gk: tuple) -> str:
                    if labels is not None and gk in labels:
                        return labels[gk]
                    return groups[gk].value_set_version_label or ""

                for gks in nonnull.values():
                    if any(_emitted(gk).strip() == target for gk in gks):
                        return [_pick_state_rep(gks, groups)], False
            elif keep_rule == "latest_year":
                # Recurring per-year vintages: keep the coding whose label embeds
                # the latest 4-digit year. Unique max → resolved; ties fall through.
                vs_year: dict[int, int] = {}
                for vs, gks in nonnull.items():
                    yrs = [
                        int(m)
                        for gk in gks
                        for m in re.findall(
                            r"(?:19|20)\d{2}", groups[gk].value_set_version_label or ""
                        )
                    ]
                    vs_year[vs] = max(yrs) if yrs else -1
                top = max(vs_year.values())
                if top >= 0 and sum(1 for y in vs_year.values() if y == top) == 1:
                    win = max(vs_year, key=lambda v: vs_year[v])
                    return [_pick_state_rep(nonnull[win], groups)], False

    # EXTENDS-LATER (more modern): nothing above separated these and there is no
    # curation pin, but one coding's timeline reaches a STRICTLY LATER period than
    # the rest — it is the live/newer coding of a sequential re-coding whose
    # INTRODUCTION year tied at SUPERSESSION (the plain→'…2009'→'…2014' bloc series;
    # a FoB75 coding that carries forward open-ended). Keep the latest-extending
    # one. Co-extensive codings (same last period: UpplForm Hh/Lgh, Br92/Br07,
    # the genuinely parallel re-codings) tie here and fall through to cosmetic /
    # genuine, where a relabel is surfaced for curation.
    vs_max: dict[int, int] = {}
    for vs, gks in nonnull.items():
        vs_max[vs] = max(
            (groups[gk].regver_max for gk in gks if groups[gk].regver_max is not None),
            default=-1,
        )
    top_max = max(vs_max.values())
    if top_max >= 0 and sum(1 for m in vs_max.values() if m == top_max) == 1:
        win_vs = max(vs_max, key=lambda v: vs_max[v])
        return [_pick_state_rep(nonnull[win_vs], groups)], False

    vss = sorted(nonnull)
    max_sym = max(
        len(codes_fn(vss[i]) ^ codes_fn(vss[j]))
        for i in range(len(vss))
        for j in range(i + 1, len(vss))
    )
    if max_sym <= _COSMETIC_MAX_SYM and not _shared_code_relabeled(vss, code_labels_fn):
        win_vs = max(nonnull, key=lambda v: (len(codes_fn(v)), v))
        return [_pick_state_rep(nonnull[win_vs], groups)], False

    return [_pick_state_rep(gks, groups) for gks in nonnull.values()], True


def _shared_code_relabeled(
    vss: list[int], code_labels_fn: Callable[[int], dict[str, str]] | None
) -> bool:
    """True if any code appears in ≥2 of these value sets with a MEANINGFULLY
    different label. Such a re-coding (same code, new meaning — e.g. a 1-code
    Br92/Br07-style clash, or FoB75 'i huset' vs 'i lägenheten') is NOT cosmetic
    drift even when the symmetric code-count diff is tiny, so the cosmetic collapse
    must skip it and let the conflict fall through to curation.

    Labels are compared ASCII-folded/lowercased with whitespace collapsed, so a
    pure case/diacritic/spacing difference ('Makedonien' vs 'MAKEDONIEN',
    'STOCKHOLMS LÄNS' vs 'Stockholms läns') stays cosmetic and still collapses.
    No label source (older callers / tests) → fall back to code-only behavior."""
    if code_labels_fn is None:
        return False

    def _norm(label: str) -> str:
        return " ".join(_ascii_fold_lower(label).split())

    seen: dict[str, str] = {}
    for vs in vss:
        for code, label in code_labels_fn(vs).items():
            norm = _norm(label)
            prior = seen.get(code)
            if prior is not None and prior != norm:
                return True
            seen[code] = norm
    return False


def _pick_state_rep(gkeys: list[tuple], groups: dict[tuple, _StateGroup]) -> tuple:
    """Deterministic representative among gkeys sharing a value set: the
    latest-era group (highest `regver_max`), ties broken by the stringified gkey
    (a raw gkey carries `value_set_id: int | None`, so a None-vs-int compare would
    raise; stringify each element)."""
    return max(
        gkeys,
        key=lambda gk: (
            groups[gk].regver_max if groups[gk].regver_max is not None else -1,
            tuple("" if x is None else str(x) for x in gk),
        ),
    )


def _coalesce_variable_states(
    conn: sqlite3.Connection,
    codelivery: CodeliveryMap | None = None,
    fold_overrides: FoldOverrideMap | None = None,
    column_merges: ColumnMergeMap | None = None,
) -> dict[str, Any]:
    """Coalesce `variable_instance` rows into `variable_state` (see reg_meta/DESIGN.md → Two-level variable model).

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
        "       vi.variabelnamn, va.delivery_column_name, "
        "       rv.registerversionnamn, "
        "       rv.registerversion_senastgodkanddatum "
        "FROM variable_instance vi "
        "LEFT JOIN variable_alias_build va ON va.cvid = vi.cvid "
        "JOIN register_version rv ON rv.regver_id = vi.regver_id"
    ).fetchall()

    # Rule 2 — kolumnnamn connectivity per (register, variant, var_id).
    # Two delivery columns are one concept-candidate iff some cvid carries both
    # as aliases (set *intersection*); union them. The component representative
    # (lex-smallest column) enters the group key below, so the coalescer keeps
    # genuinely-disjoint columns (a split candidate) as distinct pre-triage
    # groups. Columns that never co-occur form separate components — except:
    #
    #   - AUTO CASE-FOLD (#196): the node-col is `_ascii_fold_lower(column)`,
    #     so case/diacritic header twins delivered under separate cvids
    #     (`PersonNr`/`Personnr`, `Kon`/`Kön`) are ONE node — without it, a
    #     split-container var shards each casing into its own sibling variable
    #     (~543 fragments across the corpus). Raw casing survives where it
    #     matters: `delivery_column_name` comes from `latest_alias` below, and
    #     the unika lookup keys stay raw. GUARD: the fold targets era-rename
    #     twins that NEVER co-occur. When two distinct spellings of one folded
    #     header share an edition of a variant (81 groups in the corpus; e.g.
    #     HRE ships parallel `Niva` + `Nivå` columns carrying a 3-group and a
    #     2-group coding for 25 years), they are genuinely parallel columns —
    #     folding them would put both codings on ONE column and the co-delivery
    #     invariant would have to drop one. Those keep their raw node-cols; the
    #     triage still folds them into one variable (identical folded stems)
    #     with label-discriminated states — the pre-#196 handling.
    #   - CURATED COLUMN-MERGE (#196, `column_merges.toml`): a maintainer-
    #     asserted era-rename twin set (`PNR` ≡ `PersonNr`) normalizes to one
    #     node-col (the lex-min folded member) by fiat. The triage's
    #     fold-override surface cannot express this — it acts on CONTESTED
    #     (same-edition co-delivered) columns only, which an era-rename twin
    #     never is. Validated after the sweep: every named column must be
    #     observed for its (register, var), scoped to built registers.
    column_merges = column_merges or {}
    merge_canon: dict[tuple[int, int, str], str] = {
        (reg, var, member): min(group)
        for (reg, var), groups_list in column_merges.items()
        for group in groups_list
        for member in group
    }
    # Folded delivery columns actually seen per curated key, for the build-time
    # stale-entry check (only curated keys are tracked — the dict stays tiny).
    # Populated in the pre-pass below, the one place the row sweep records
    # observations; `_node_col` stays a pure lookup.
    merge_observed: dict[tuple[int, int], set[str]] = {}

    # Co-delivered case-twin guard: per (register, variant, var, folded-col),
    # the raw spellings and the editions each was seen in. A folded group whose
    # two distinct spellings share an edition is left UNfolded (see the guard
    # note above).
    spell_eds: dict[tuple[int, int, int, str], dict[str, set[int]]] = {}
    for row in rows:
        col = row["delivery_column_name"]
        if not col:
            continue
        # A column that folds to "" (no ASCII content) keeps its raw spelling —
        # "" is the no-alias stub component and must not absorb a real column.
        fcol = _ascii_fold_lower(col) or col
        if (row["register_id"], row["var_id"]) in column_merges:
            merge_observed.setdefault((row["register_id"], row["var_id"]), set()).add(
                fcol
            )
        spells = spell_eds.setdefault(
            (row["register_id"], row["register_variant_id"], row["var_id"], fcol), {}
        )
        spells.setdefault(col, set()).add(row["regver_id"])
    guarded: set[tuple[int, int, int, str]] = set()
    for skey, spells in spell_eds.items():
        if len(spells) < 2:
            continue
        eds = list(spells.values())
        if any(
            eds[i] & eds[j] for i in range(len(eds)) for j in range(i + 1, len(eds))
        ):
            guarded.add(skey)

    def _node_col(register_id: int, variant_id: int, var_id: int, col: str) -> str:
        fcol = _ascii_fold_lower(col) or col
        canon = merge_canon.get((register_id, var_id, fcol))
        if canon is not None:
            return canon  # curated fiat outranks the co-delivery guard
        if (register_id, variant_id, var_id, fcol) in guarded:
            return col
        return fcol

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
        node = (
            row["register_id"],
            row["register_variant_id"],
            row["var_id"],
            _node_col(
                row["register_id"], row["register_variant_id"], row["var_id"], col
            ),
        )
        col_parent.setdefault(node, node)
        anchor = cvid_anchor.get(row["cvid"])
        if anchor is None:
            cvid_anchor[row["cvid"]] = node
        else:
            _col_union(anchor, node)

    # Group accumulator: key → mutable `_StateGroup` (module scope; the
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

    # cvid → its group key. A cvid belongs to exactly ONE group: every gkey
    # field is a cvid-level constant (register/variant/var_id/shape/value-set/
    # label/grain) EXCEPT the trailing column component, and rule-2
    # connectivity (the `cvid_anchor` union above) merges ALL of a cvid's
    # columns into one component — so every row a cvid contributes carries the
    # same gkey. After triage assigns each gkey a `variable_id`, this lets the
    # coalescer stamp `variable_instance.variable_id` with no guessing.
    cvid_gkey: dict[int, tuple] = {}

    for row in rows:
        grain = row["grain"] or ""
        col = row["delivery_column_name"]
        # Column component (rule 2): disjoint columns get distinct
        # components → distinct groups → triage can fold/split them. A cvid
        # with no alias contributes the "" component (a stub group). The
        # component is the case-folded (+ merge-normalized) node-col — the
        # form the curated fold-override / codelivery column keys match.
        component = (
            _col_find(
                (
                    row["register_id"],
                    row["register_variant_id"],
                    row["var_id"],
                    _node_col(
                        row["register_id"],
                        row["register_variant_id"],
                        row["var_id"],
                        col,
                    ),
                )
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
        cvid_gkey[row["cvid"]] = gkey  # idempotent: one cvid → one gkey
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

        # Editions this group was delivered in — the contested gate buckets
        # by edition, not year (regver_id is NOT NULL on variable_instance).
        grp.regvers.add(row["regver_id"])

        # Track register_version year per cvid (fallback signal) on the
        # group, and also on the per-variable max so the materializer can
        # identify the latest-era group when clamping unika ranges.
        rver_year = extract_year(row["registerversionnamn"] or "")
        if rver_year is not None:
            grp.regyears.add(rver_year)
            _auth = _edition_authority(row["registerversionnamn"])
            if _auth > grp.year_authority.get(rver_year, -1):
                grp.year_authority[rver_year] = _auth
            _appr = row["registerversion_senastgodkanddatum"] or ""
            if _appr > grp.year_approval.get(rver_year, ""):
                grp.year_approval[rver_year] = _appr
            grp.regver_min = (
                rver_year if grp.regver_min is None else min(grp.regver_min, rver_year)
            )
            grp.regver_max = (
                rver_year if grp.regver_max is None else max(grp.regver_max, rver_year)
            )
            # #219: accumulate the sub-annual ISO envelope alongside the year ints.
            # String min/max is chronological for ISO dates. Bounds are tied to this
            # edition's year (`rver_year`), so each edition contributes within its own
            # year: a full-year edition gives YYYY-01-01/YYYY-12-31 (a min-year that
            # ALSO has a full-year/spring edition keeps -01-01 — no spurious narrowing),
            # and `from_iso`/`to_iso` can never escape `[regver_min, regver_max]`.
            ed = _edition_bounds(row["registerversionnamn"], rver_year)
            if ed is not None:
                grp.from_iso = (
                    ed[0] if grp.from_iso is None else min(grp.from_iso, ed[0])
                )
                grp.to_iso = ed[1] if grp.to_iso is None else max(grp.to_iso, ed[1])
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

    # Curated column-merge build-time validation (#196): every column a
    # maintainer named must be an OBSERVED delivery column of its
    # (register, var). Scoping to the registers present in this build is the
    # synthetic-/partial-build escape, mirroring fold_overrides: a
    # `--providers=sos` or fixture build that never loads the register must not
    # fail on its merge. Once the register IS built, a stale/typo'd entry FAILS
    # the build — never a silent never-matching no-op.
    live_registers = {reg for reg, _ in vid_map}
    for (m_reg, m_var), m_groups in sorted(column_merges.items()):
        if m_reg not in live_registers:
            continue
        observed = merge_observed.get((m_reg, m_var), set())
        missing = sorted(set().union(*m_groups) - observed)
        if missing:
            raise RegMetaError(
                exit_code=EXIT_CONFIG,
                code="column_merge_unknown_column",
                error_class="configuration",
                message=(
                    f"column-merge for register_id={m_reg} var_id={m_var} names "
                    f"column(s) {missing} never observed as delivery columns of "
                    f"this variable in this build "
                    f"(observed, case-folded: {sorted(observed)})."
                ),
                remediation=(
                    "Fix the column name(s) in reg_meta_build/column_merges.toml "
                    "or drop the entry. Columns are matched case-folded "
                    "(lowercase, diacritics stripped)."
                ),
            )

    # Triage: resolve pre-triage collisions (fold/split/collapse) before
    # materializing. Mints split-sibling `variable` rows (so vid_map above is
    # stale for them — triage.assignments carries the per-gkey target) and
    # routes each group to its variable_id + (folded) value_set_version_label.
    triage = _triage_groups(conn, groups, vid_map, fold_overrides)

    # Stamp each cvid's OWNING `variable_id` now that triage has assigned every
    # group (including the split siblings it just minted). This is the GROUND
    # TRUTH `_emit_variable_aliases` / `_backfill_state_classifications` read
    # instead of a post-hoc column-tie: each cvid maps to exactly one gkey (see
    # `cvid_gkey`), so `assignments[gkey]` is its unambiguous sibling — no
    # guessing, no skip. A None assignment is a missing-parent invariant break
    # the materializer below raises on; the walrus filter skips it here so that
    # raise wins (and `variable_id` stays NULL, which the readers tolerate).
    # Coverage is the coalescer-visible cvid set (`cvid_gkey`, built from `rows`,
    # which INNER-JOINs `register_version`): a cvid absent from `rows` produced no
    # `variable_state` either, so an unstamped cvid is consistent with
    # materialization — never a dropped state. The real corpus stamps every cvid
    # (0 unattributed); the importer writes a `register_version` per edition.
    stamps = [
        (vid, cvid)
        for cvid, gkey in cvid_gkey.items()
        if (vid := triage.assignments.get(gkey)) is not None
    ]
    conn.executemany(
        "UPDATE variable_instance SET variable_id = ? WHERE cvid = ?", stamps
    )
    n_unattributed = len(cvid_gkey) - len(stamps)
    _progress(
        f"  stamped {len(stamps):,} cvids with their owning variable_id"
        + (f" ({n_unattributed:,} unattributed)" if n_unattributed else "")
    )

    batch: list[tuple] = []
    sentinel_count = 0
    fallback_only_count = 0
    open_top_from_unika = 0
    disambig_count = 0

    # Cosmetic-diff needs value-set code lists, and the label-aware cosmetic gate
    # needs each code's label; cache the code→label map per value_set_id (the
    # contested set is a small fraction of all value sets, so lazy is cheap).
    _vs_code_labels_cache: dict[int, dict[str, str]] = {}

    def _vs_code_labels(vsid: int) -> dict[str, str]:
        cached = _vs_code_labels_cache.get(vsid)
        if cached is None:
            cached = {
                r[0]: (r[1] or "")
                for r in conn.execute(
                    "SELECT vc.code, vc.label FROM value_set_member vsm "
                    "JOIN value_code vc ON vc.code_id = vsm.code_id "
                    "WHERE vsm.value_set_id = ?",
                    (vsid,),
                )
            }
            _vs_code_labels_cache[vsid] = cached
        return cached

    def _vs_codes(vsid: int) -> frozenset[str]:
        return frozenset(_vs_code_labels(vsid))

    def _resolve_variable_id(gkey: tuple, grp: _StateGroup) -> int:
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
        return variable_id

    def _group_open_to(grp: _StateGroup) -> int | None:
        """Upper-bound year for a group's LATEST run, or None → open sentinel.
        Latest-era group with unika-open → sentinel; else its observed
        `regver_max`; else the unika upper (yearless fallback)."""
        vkey = (grp.register_id, grp.register_variant_id, grp.var_id)
        is_latest_era = grp.regver_max == var_max_regver.get(vkey)
        if (
            is_latest_era
            and grp.unika_matched
            and (grp.unika_max is None or grp.unika_has_open_top)
        ):
            return None
        if grp.regver_max is not None:
            return grp.regver_max
        return grp.unika_max

    # (variable_id, register_variant_id, valid_from, label) keys already emitted —
    # the uniqueness index. The fast path NEVER collides (a collision there is
    # a triage bug → let the INSERT raise loudly). The timeline path CAN legitimately
    # collide on CROSS-COLUMN co-delivery: two distinct delivery columns (parallel
    # representations — agrupp/agrupp2, SSYK 3/5-digit) each win the same year with
    # an empty label, so they share `(vid, rv, valid_from, '')`. Those are real
    # distinct codings on distinct columns, so disambiguate the label with the
    # DELIVERY-COLUMN NAME (self-documenting + stable) so both ship. SAME-column
    # conflicts never reach the INSERT: they're collected as
    # `genuine` and the coalescer RAISES before materializing (see below).
    # Disambiguation is counted; a flood would signal a bug.
    _used_index_keys: set[tuple[int, int, str, str]] = set()

    def _append_state(
        grp: _StateGroup,
        gkey: tuple,
        vid: int,
        vf: str,
        vt: str,
        disambig: bool = False,
    ) -> None:
        nonlocal sentinel_count, fallback_only_count, disambig_count
        # #219 fail-fast: a non-sentinel inverted window (valid_from > valid_to) is a
        # bounds-derivation bug (e.g. an edition/term year disagreement). Neither the
        # DDL nor validate.py asserts valid_from <= valid_to, so catch it loudly here
        # — covering both the fast and timeline paths — rather than ship it silently.
        # The open sentinel (vt 9999-12-31) and UNKNOWN start (vf 0001-01-01) are
        # extremes that can't invert; exempting them keeps the check exact.
        if vf != _VALID_FROM_UNKNOWN and vt != _VALID_TO_OPEN_SENTINEL and vf > vt:
            raise RegMetaError(
                exit_code=EXIT_CONFIG,
                code="coalesce_inverted_state_window",
                error_class="configuration",
                message=(
                    f"variable_state window inverted (valid_from {vf} > valid_to {vt})"
                    f" for variable_id={vid}, "
                    f"register_variant_id={grp.register_variant_id}."
                ),
                remediation=(
                    "A sub-annual bound derivation produced valid_from > valid_to. "
                    "Rebuild from source with `reg-meta-build build-db`; if it "
                    "persists, the `_edition_bounds` registerversionnamn parse "
                    "(reg_meta_build/sources/scb.py) needs a fix."
                ),
            )
        if vt == _VALID_TO_OPEN_SENTINEL or vf == _VALID_FROM_UNKNOWN:
            sentinel_count += 1
        if not grp.unika_matched:
            fallback_only_count += 1
        # Fold token from triage wins; else the group's own label (NOT NULL
        # DEFAULT '' — coalesce NULL→'' so the index bites for single-version).
        label = triage.labels.get(gkey, grp.value_set_version_label or "")
        if disambig:
            base = label
            # Disambiguate a cross-column co-delivery collision with the delivery-
            # column name (self-documenting + order-stable) rather than an opaque
            # counter; the first-emitted column keeps the base label. The numeric
            # fallback only fires if the column name ALSO collides (it shouldn't —
            # same-column conflicts are caught as `genuine` before emit).
            col = grp.latest_alias or gkey[8] or "cd"
            n = 0
            while (vid, grp.register_variant_id, vf, label) in _used_index_keys:
                n += 1
                suffix = col if n == 1 else f"{col}{n}"
                label = f"{base}-{suffix}" if base else suffix
            if label != base:
                disambig_count += 1
        _used_index_keys.add((vid, grp.register_variant_id, vf, label))
        batch.append(
            (
                vid,
                grp.register_variant_id,
                vf,
                vt,
                grp.data_type,
                grp.data_length,
                grp.latest_alias,
                grp.value_set_id,
                label,
            )
        )

    def _emit_span(gkey: tuple, grp: _StateGroup) -> None:
        """Fast path / yearless fallback: one state over the group's
        `[from, to]` span (the pre-timeline behavior)."""
        nonlocal open_top_from_unika
        vid = _resolve_variable_id(gkey, grp)
        from_year = _group_from_year(grp)
        # A triage clamp (residual same-column supersession, _collapse_residual
        # pass 2) caps valid_to the year before the superseding group begins and
        # overrides the open sentinel — a superseded group is no longer active.
        # Clamps only land on fast-path groups, so honoring it here suffices.
        clamp = triage.clamped_to.get(gkey)
        to_year = clamp if clamp is not None else _group_open_to(grp)
        if to_year is None:
            open_top_from_unika += 1
        # #219: clamp the state's lifetime START/END to the sub-annual delivery
        # window (from_iso/to_iso) instead of the boundary year. `_edition_bounds`
        # ties every edition's window to its OWN year, so from_iso lands in the
        # regver_min year and to_iso in the regver_max year BY CONSTRUCTION — they can
        # only narrow within those boundary years, never cross one (a cross-year span
        # would risk a same-column overlap with a distinct value set in the adjacent
        # year). So no within-year guard is needed here; `_append_state` additionally
        # fail-fast-asserts valid_from <= valid_to. The `or` chains preserve the
        # yearless/unika fallback (from_iso/to_iso are None there); a year clamp and
        # the open sentinel both ignore to_iso (else branch) — a superseded group ends
        # at the year clamp, a still-active one stays open.
        vf = grp.from_iso or _year_to_iso_from(from_year) or _VALID_FROM_UNKNOWN
        if clamp is None and to_year is not None:
            vt = grp.to_iso or _year_to_iso_to(to_year) or _VALID_TO_OPEN_SENTINEL
        else:
            vt = _year_to_iso_to(to_year) or _VALID_TO_OPEN_SENTINEL
        _append_state(grp, gkey, vid, vf, vt)

    # Partition surviving groups by (variable_id, register_variant_id). A
    # `(variable, variant)` needs the per-year TIMELINE iff two of its groups
    # carry DISTINCT non-null value sets with OVERLAPPING `[regver_min,
    # regver_max]` spans (the same condition `validate.py` flags). Everything
    # else keeps the fast `[min,max]` span path — byte-identical to before, and
    # benign single-edition gaps stay covered (no needless fragmentation).
    by_vv: dict[tuple[int, int], list[tuple]] = defaultdict(list)
    for gkey, grp in groups.items():
        if gkey in triage.dropped:
            continue  # collapsed into a sibling state (rule 4 drift)
        vid = _resolve_variable_id(gkey, grp)
        by_vv[(vid, grp.register_variant_id)].append(gkey)

    # Genuine same-column conflicts: a delivery column resolves a period to >1
    # distinct value set the cascade + curation couldn't reduce. Keyed by
    # (register_id, var_id, column) → contested period descriptors (a year, or
    # "(yearless)" for an open-span conflict) + candidate (value_set_id, emitted
    # label) pairs, so the build-time failure names exactly which curation pin to
    # add. Any entry here fails the build before materializing.
    genuine_years: dict[tuple[int, int, str], set[str]] = defaultdict(set)
    genuine_cands: dict[tuple[int, int, str], set[tuple[int, str]]] = defaultdict(set)
    timeline_vv = 0

    def _emitted_label(gk: tuple) -> str:
        """The label that lands in `variable_state` (triage fold token wins, else
        the group's own label) — what a curation `keep` pin matches against."""
        return (triage.labels.get(gk, groups[gk].value_set_version_label or "")).strip()

    def _col_value_sets(gks: list[tuple]) -> set[int]:
        return {vs for gk in gks if (vs := groups[gk].value_set_id) is not None}

    for (vid, rv), gkeys in by_vv.items():
        if not _spans_overlap(groups, gkeys):
            for gk in gkeys:  # fast path
                _emit_span(gk, groups[gk])
            continue
        timeline_vv += 1
        year_bearing = [gk for gk in gkeys if groups[gk].regyears]
        yearless = [gk for gk in gkeys if not groups[gk].regyears]
        # A YEARLESS group has no per-year placement: it emits a single OPEN span
        # over the variable's lifetime, so on a column carrying >1 DISTINCT value
        # set it overlaps every other coding there. Resolve each yearless-bearing
        # column up front, deciding which yearless to EMIT and which year-bearing
        # rivals to DROP from the per-year timeline below:
        #   - a curation `keep` pin selects the kept coding (yearless OR
        #     year-bearing); every other coding on that column is dropped;
        #   - else a year-bearing coding wins (it carries real years) and the
        #     yearless ones are dropped;
        #   - else (ALL-yearless, no pin) the column is an unresolvable open-span
        #     co-delivery → recorded GENUINE so the build fails (not shipped).
        # Non-conflicting yearless groups (≤1 distinct value set on the column)
        # always emit.
        yearless_emit: set[tuple] = set()
        yb_drop: set[tuple] = set()
        for col in {gk[8] for gk in yearless}:
            col_yl = [gk for gk in yearless if gk[8] == col]
            col_yb = [gk for gk in year_bearing if gk[8] == col]
            if len(_col_value_sets(col_yl + col_yb)) <= 1:
                yearless_emit.update(col_yl)  # no conflict on this column
                continue
            pin = None
            if codelivery:
                # Folded lookup, mirroring the year-winner cascade's pin lookup.
                _entry = codelivery.get(
                    (col_yl[0][0], col_yl[0][2], _ascii_fold_lower(col) or col)
                )
                if _entry is not None and _entry[0] is not None:
                    pin = _entry[0].strip()
            pinned_yl = [gk for gk in col_yl if pin and _emitted_label(gk) == pin]
            pinned_yb = [gk for gk in col_yb if pin and _emitted_label(gk) == pin]
            if pinned_yl:
                # A yearless coding is pinned: keep it, drop every rival on the
                # column — other yearless AND all year-bearing.
                yearless_emit.add(pinned_yl[0])
                yb_drop.update(col_yb)
            elif pin is not None and not pinned_yb:
                # A `keep` pin is set for this column but matches NEITHER a yearless
                # nor a year-bearing coding here (stale/typo). Honor the curation
                # file's guarantee that a non-matching pin fails the build: record
                # GENUINE rather than silently shipping the year-bearing default —
                # with a single year-bearing rival the year loop returns before
                # consulting curation, so the bad pin would otherwise go unreported.
                ckey = (col_yl[0][0], col_yl[0][2], col)
                genuine_years[ckey].add("(yearless)")
                for gk in col_yl + col_yb:
                    if (vs := groups[gk].value_set_id) is not None:
                        genuine_cands[ckey].add((vs, _emitted_label(gk)))
                yb_drop.update(col_yb)  # don't also process them in the year loop
            elif col_yb:
                # A year-bearing coding wins (no pin, or the pin matches a
                # year-bearing coding the year loop keeps): drop all yearless here.
                continue
            else:
                # All-yearless, no pin → unresolvable open-span conflict.
                ckey = (col_yl[0][0], col_yl[0][2], col)
                genuine_years[ckey].add("(yearless)")
                for gk in col_yl:
                    if (vs := groups[gk].value_set_id) is not None:
                        genuine_cands[ckey].add((vs, _emitted_label(gk)))
        for gk in yearless:
            if gk in yearless_emit:
                _emit_span(gk, groups[gk])
        # A pinned yearless coding beat these year-bearing rivals — drop them so
        # the timeline doesn't re-emit an overlapping coding on the same column.
        year_bearing = [gk for gk in year_bearing if gk not in yb_drop]
        owned: dict[tuple, set[int]] = defaultdict(set)
        all_years: set[int] = set()
        for gk in year_bearing:
            all_years |= groups[gk].regyears
        for y in sorted(all_years):
            cands = [gk for gk in year_bearing if y in groups[gk].regyears]
            winners, genuine = _resolve_year_winners(
                cands, groups, y, _vs_codes, codelivery, triage.labels, _vs_code_labels
            )
            for gk in winners:
                owned[gk].add(y)
            if genuine:
                # Pin the conflict to the offending column(s): winners on ONE
                # column carrying >1 distinct value set (cross-column winners are a
                # legitimate co-delivery, not a conflict). Record each contested
                # year + its candidate (value_set_id, emitted label) pairs.
                col_winners: dict[str, list[tuple]] = defaultdict(list)
                for gk in winners:
                    if groups[gk].value_set_id is not None:
                        col_winners[gk[8]].append(gk)
                for col, gks in col_winners.items():
                    if len({groups[gk].value_set_id for gk in gks}) <= 1:
                        continue
                    ckey = (gks[0][0], gks[0][2], col)
                    genuine_years[ckey].add(str(y))
                    for gk in gks:
                        vs = groups[gk].value_set_id
                        if vs is None:
                            continue
                        lbl = triage.labels.get(
                            gk, groups[gk].value_set_version_label or ""
                        )
                        genuine_cands[ckey].add((vs, lbl))
        # Each owning group's window = the contiguous RUNS of years it won
        # (gaps where a competitor superseded it carve out). The open-ended
        # sentinel applies only to the final run that still ends at the group's
        # latest observed edition (it didn't lose its latest era).
        for gk, yrs in owned.items():
            grp = groups[gk]
            runs = _rle_runs(sorted(yrs))
            open_to = _group_open_to(grp)
            for idx, (run_lo, run_hi) in enumerate(runs):
                is_first = idx == 0
                is_last = idx == len(runs) - 1
                # #219: the sub-annual envelope narrows ONLY the group's lifetime
                # START (first run, at regver_min) and END (last run, at regver_max);
                # INTERIOR run boundaries are competitor handoffs and stay
                # year-aligned. to_iso/from_iso are within the regver_max/regver_min
                # years BY CONSTRUCTION (`_edition_bounds` ties each edition's window
                # to its own year), so the `run_hi == regver_max` / `run_lo ==
                # regver_min` gates already keep the clamp inside the run's year — a
                # run that lost its boundary era stays year-granular, and no cross-year
                # extension into a rival value set's year is possible.
                if is_last and open_to is None and run_hi == grp.regver_max:
                    vt = _VALID_TO_OPEN_SENTINEL
                    open_top_from_unika += 1
                elif is_last and run_hi == grp.regver_max and grp.to_iso:
                    vt = grp.to_iso
                else:
                    vt = _year_to_iso_to(run_hi) or _VALID_TO_OPEN_SENTINEL
                if is_first and run_lo == grp.regver_min and grp.from_iso:
                    vf = grp.from_iso
                else:
                    vf = _year_to_iso_from(run_lo) or _VALID_FROM_UNKNOWN
                _append_state(grp, gk, vid, vf, vt, disambig=True)

    if timeline_vv:
        _progress(
            f"  partitioned {timeline_vv:,} overlapping (variable,variant) into "
            f"per-year value-set timelines "
            f"({disambig_count:,} cross-column co-delivery labels disambiguated)"
        )
    # Fail BEFORE materializing: a same-column conflict that survived the cascade
    # AND curation is an unresolvable co-delivery. This is the build-time half of
    # the `(variable, variant, period, column) → one value set` invariant —
    # `validate.py` re-checks the shipped DB as a redundant backstop, but the build
    # must never ship an ambiguous column, even on a default (non-`--validate`) run.
    if genuine_years:
        lines = []
        for ckey in sorted(genuine_years):
            reg, var, col = ckey
            yrs = ", ".join(sorted(genuine_years[ckey]))
            cands_str = ", ".join(
                f"vs{vs} {lbl!r}" for vs, lbl in sorted(genuine_cands[ckey])
            )
            lines.append(
                f"  register_id={reg} var_id={var} "
                f"column={col or '(no column)'} period(s) {yrs}: {cands_str}"
            )
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="coalesce_unresolved_codelivery",
            error_class="configuration",
            message=(
                f"{len(genuine_years)} delivery column(s) resolve a period to >1 "
                "value set after the deterministic cascade — an unresolvable "
                "same-column co-delivery:\n" + "\n".join(lines)
            ),
            remediation=(
                "Add a curation pin to reg_meta_build/codelivery.toml keyed on "
                '(register_id, var_id, column), with `keep = "<emitted label>"` '
                'naming the coding to keep (or `keep_rule = "latest_year"` for '
                "recurring dated vintages). See the file header."
            ),
        )

    conn.executemany(
        "INSERT INTO variable_state (variable_id, register_variant_id, "
        "    valid_from, valid_to, data_type, data_length, delivery_column_name, "
        "    value_set_id, value_set_version_label) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        batch,
    )

    # State-uniqueness index — UNIQUE(variable_id, register_variant_id,
    # valid_from, value_set_version_label). A4.3b moved its CREATE into the
    # universal DDL (db.py), so it already exists when this post-triage INSERT
    # runs: an INSERT collision raises here (the same loud failure the old
    # in-place CREATE provided), surfacing a residual triage collision as a build
    # bug rather than shipping it. The index is provider-blind now — SOS's
    # `_reinsert_core_graph_from_ir` gets the same guarantee with no per-adapter
    # CREATE.
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
        f"{len(triage.dropped):,} states collapsed, "
        f"{triage.stats.get('clustered', 0):,} clustered, "
        f"{len(triage.clamped_to):,} clamped"
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
        "n_triage_clamped": len(triage.clamped_to),
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


def _load_validity_map(path: Path) -> tuple[dict[int, list[tuple[int, int]]], int]:
    """Load VardemangderValidDates.csv into an in-memory ItemId → year-windows map.

    Returns (validity_map, row_count). validity_map[item_id] is a list of
    (year_from, year_to) tuples — one per validity row for that ItemId.
    NULL valid_from → 1, NULL valid_to → 9999 (per SCB rule: NULL means
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


# ---------------------------------------------------------------------------
# SCBAdapter — the IRAdapter entry point
# ---------------------------------------------------------------------------

# Enrichment CSVs consumed by the adapter, in the order the legacy build read
# them (the order is load-bearing: Vardemangder year-projection runs last and
# joins the variable_instance rows the earlier files populated).
_ENRICHMENT_FILES = [
    "UnikaRegisterOchVariabler.csv",
    "Identifierare.csv",
    "Timeseries.csv",
    "Vardemangder.csv",
]


class SCBAdapter:
    """Statistics Sweden source adapter (IRAdapter).

    `emit(source_dir)` parses SCB's native exports, runs the triage and
    value-set projection against the working connection, and yields the IR
    stream. The connection is bound via the constructor (the adapter writes its
    SCB-named build-scratch + SCB-reference tables there, and reads the
    universal rows back to mirror them as IR). The materializer in
    `reg_meta_build.db` consumes the stream and runs the provider-blind
    derivation post-passes.

    Build-orchestration outputs the materializer/manifest need are exposed as
    attributes after `emit()` drains:
      - `source_checksums`, `row_counts` — manifest inputs.
      - `coalesce_stats`, `projection_stats` — manifest inputs.
      - `fold_slug_hints` — side channel to `populate_variable_slugs`
        (R8: not an IR object; a build-only `{variable_id: slug_stem}` dict).
      - `related_edges` — split-sibling (variable_id, variable_id, kind) triples
        (also surfaced in-stream as `IRRelatedToEdge`; the materializer
        resolves variable_id → slug at write time, after slugs exist).
    """

    provider = "scb"

    def __init__(
        self,
        conn: sqlite3.Connection,
        codelivery: CodeliveryMap | None = None,
        fold_overrides: FoldOverrideMap | None = None,
        column_merges: ColumnMergeMap | None = None,
    ) -> None:
        # The adapter writes its scratch/reference tables into the working conn
        # and reads the universal rows back to emit IR (strategy B).
        self.conn = conn
        # Co-delivery curation (register_id, var_id, column) → kept label,
        # consulted by the coalescer for genuine one-off same-column conflicts.
        self.codelivery = codelivery or {}
        # Fold-override curation (register_id, var_id) → fold groups, consulted by
        # the triage to fold disjoint-stem columns the stem rule would split.
        self.fold_overrides = fold_overrides or {}
        # Column-merge curation (register_id, var_id) → merge groups, consulted by
        # the coalescer's rule-2 union-find to unify never-co-occurring
        # era-rename column twins (#196).
        self.column_merges = column_merges or {}
        self.source_checksums: dict[str, str] = {}
        self.row_counts: dict[str, int] = {}
        self.coalesce_stats: dict[str, Any] = {}
        self.projection_stats: _ProjectionStats = _ProjectionStats()
        self.fold_slug_hints: dict[int, str] = {}
        self.related_edges: list[tuple[int, int, str]] = []

    def emit(self, source_dir: Path) -> Iterator[IRObject]:
        """Parse SCB exports under ``source_dir`` and emit IR objects.

        ``source_dir`` is the ``SCB/`` directory (containing
        Registerinformation.csv and the enrichment files). Yields in
        FK-topological order: registers → variants → value_sets (+codes) →
        variables → variable_states → related-to edges → provenance/warnings.
        (No IRClassification / IRLineageEdge / IRReplacedByEdge: in A4.1 those
        stay materializer-derived; the adapter emits the subset above.)

        A4.3a — the IR is now CONSUMED: ``materialize()`` re-inserts the core
        graph (`register` / `register_variant` / `variable` / `variable_state` /
        `variable_alias`) from this stream with explicit PKs, making the
        materializer the sole writer. The A4.1 inert-mirror slug caveat is closed
        STRUCTURALLY rather than by re-sequencing: the slug columns are NULL at
        emit time (``populate_slugs`` / ``populate_variable_slugs`` run later), and
        the materializer IGNORES the IR slug for the core graph — it inserts NULL,
        then those UPDATE passes fill the slug (and ``display_group``) IN PLACE on
        the re-inserted rows. So the mirror's ``""`` slug placeholder never
        reaches disk. ``valid_to`` / ``value_set_version_label`` are read back as
        the stored sentinels (`9999-12-31` / `''`); the materializer's
        None→sentinel reconciliation is idempotent on them. ``IRVariableState``
        now carries ``delivery_column_name``; the full historical column set rides
        on ``IRVariableAlias`` (``_emit_variable_aliases``). ``IRValueSet`` /
        ``IRValueCode`` are emitted faithfully but the value tables stay
        adapter-written in A4.3a (content-shared; see ``_reinsert_core_graph_from_ir``).
        ``IRValueSet.classification_id`` stays None — classifications run in
        ``materialize()`` AFTER emit, so the adapter cannot know it; the
        ``variable_state.classification_id`` backfill reads the post-classification
        ``variable_instance`` scratch instead.
        """
        conn = self.conn
        scb_dir = source_dir

        ri_path = scb_dir / "Registerinformation.csv"

        # SCB-private value-set-projection scratch: the (cvid, code_id, item_id)
        # triples. PRECONDITION: the caller must ATTACH a `staging` database
        # before calling emit() — build_db owns the ATTACH + file lifecycle
        # (db.py); the adapter only CREATEs its table inside it. (The legacy
        # build did both back-to-back in build_db.)
        conn.execute(
            "CREATE TABLE staging._build_cvid_pair ("
            "cvid INTEGER NOT NULL,"
            "code_id INTEGER NOT NULL,"
            "item_id INTEGER NOT NULL,"  # 0 = empty ItemId in the source CSV
            "PRIMARY KEY (cvid, code_id, item_id)"
            ") WITHOUT ROWID"
        )

        # Core backbone: Registerinformation.csv (required).
        self.source_checksums["Registerinformation.csv"] = _file_sha256(ri_path)
        ri_count, unika_join, known_cvids = _import_registerinformation(conn, ri_path)
        self.row_counts["Registerinformation.csv"] = ri_count

        # Pre-load validity windows (consumed by Vardemangder year-projection).
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
            self.source_checksums["VardemangderValidDates.csv"] = _file_sha256(vvd_path)
            validity_map, validity_row_count = _load_validity_map(vvd_path)
            self.row_counts["VardemangderValidDates.csv"] = validity_row_count

        # Enrichment files (optional). VardemangderValidDates.csv handled above.
        for filename in _ENRICHMENT_FILES:
            path = scb_dir / filename
            if not path.exists():
                _progress(f"Skipping {filename} (not found)")
                continue
            self.source_checksums[filename] = _file_sha256(path)

            if filename == "UnikaRegisterOchVariabler.csv":
                self.row_counts[filename] = _import_unika(conn, path, unika_join)
            elif filename == "Identifierare.csv":
                self.row_counts[filename] = _import_identifierare(conn, path)
            elif filename == "Timeseries.csv":
                self.row_counts[filename] = _import_timeseries(conn, path)
            elif filename == "Vardemangder.csv":
                vm_count, cvid_vs_info = _import_vardemangder(conn, path, known_cvids)
                self.row_counts[filename] = vm_count
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
                self.projection_stats = _project_and_mint_value_sets(conn, validity_map)

        # A1.2: lift sensitivity / identifier flags from unika_summary into the
        # variable table. Runs after the enrichment loop so both source and
        # target tables are populated.
        _populate_sensitivity_flags(conn)

        # A2.1: coalesce variable_instance rows into variable_state. Reads
        # `unika_summary` and `register_version`; must run before the
        # unika_summary DROP below.
        self.coalesce_stats = _coalesce_variable_states(
            conn, self.codelivery, self.fold_overrides, self.column_merges
        )
        self.row_counts["variable_state"] = self.coalesce_stats["n_variable_states"]
        # R8 side channels (NOT manifest values): consumed by the materializer's
        # slug + related-to post-passes.
        self.fold_slug_hints = self.coalesce_stats["_fold_slug_hints"]
        self.related_edges = self.coalesce_stats["_related_edges"]

        # A2.1: drop the now-unused unika_summary table (both consumers ran).
        conn.execute("DROP TABLE unika_summary")
        _progress("Dropped unika_summary (consumed by A1.2 + A2.1).")

        # SCB-reference tables (R3): no IR carrier; the adapter writes them
        # directly to the working conn as a side effect of emit().
        sql_path = scb_dir / "Tabelldefinitioner.sql"
        if sql_path.exists():
            self.row_counts["Tabelldefinitioner.sql"] = _import_tabelldefinitioner(
                conn, sql_path
            )
        else:
            _progress("Skipping Tabelldefinitioner.sql (not found)")

        xlsx_path = scb_dir / "ID-kolumner.xlsx"
        if xlsx_path.exists():
            self.row_counts["ID-kolumner.xlsx"] = _import_id_kolumner(conn, xlsx_path)
        else:
            _progress("Skipping ID-kolumner.xlsx (not found)")

        # ---- Emit the IR mirror (FK-topological order). ------------------
        # The universal rows are already written verbatim above; reading them
        # back guarantees the emitted IR matches the materialized catalog.
        yield from self._emit_registers()
        yield from self._emit_variants()
        yield from self._emit_value_sets()
        yield from self._emit_variables()
        yield from self._emit_variable_states()
        yield from self._emit_variable_aliases()
        yield from self._emit_related_edges()
        yield from self._emit_replaced_by_edges()
        yield from self._emit_provenance()
        yield from self._emit_warnings()

    # -- IR emit helpers (read-back of the verbatim-written universal rows) --

    def _emit_registers(self) -> Iterator[IRObject]:
        # register.slug is NULL at emit time (populate_slugs runs later in the
        # materializer post-passes) → emit "" placeholder; A4.3 re-sequences the
        # mirror after population so this same query returns the real slug. The
        # provider string comes from self.provider — no provider join, and the
        # register's OWN slug must be read, not provider.slug.
        for row in self.conn.execute(
            "SELECT register_id, slug, name, purpose FROM register"
        ):
            yield IRRegister(
                register_id=row[0],
                provider=self.provider,
                slug=row[1] or "",
                name=row[2],
                description=None,
                purpose=row[3],
            )

    def _emit_variants(self) -> Iterator[IRObject]:
        # register_variant.slug is NULL at emit time (curated later by
        # populate_slugs) → "" placeholder; A4.3 re-reads after population. (Do
        # NOT hardcode "_default" — real variant slugs differ per variant.)
        for row in self.conn.execute(
            "SELECT register_variant_id, register_id, slug, name, description "
            "FROM register_variant"
        ):
            yield IRVariant(
                register_variant_id=row[0],
                register_id=row[1],
                slug=row[2] or "",
                name=row[3],
                description=row[4],
            )

    def _emit_value_sets(self) -> Iterator[IRObject]:
        # Lockstep over two value_set_id-ordered cursors so the whole member
        # corpus (~millions of rows on a real build) is never held in memory:
        # group the members on the fly and advance them in step with the
        # value_set cursor. Both queries ORDER BY value_set_id, so one ordered
        # pass over each suffices. (The legacy build minted value_sets via
        # on-disk staging; a dict read-back would reintroduce a ~GB spike.)
        member_groups = groupby(
            self.conn.execute(
                "SELECT vsm.value_set_id, vsm.code_id, vc.code, vc.label "
                "FROM value_set_member vsm "
                "JOIN value_code vc ON vc.code_id = vsm.code_id "
                "ORDER BY vsm.value_set_id, vsm.code_id"
            ),
            key=lambda r: r[0],
        )
        pending = next(member_groups, None)
        for vsid, member_hash in self.conn.execute(
            "SELECT value_set_id, member_hash FROM value_set ORDER BY value_set_id"
        ):
            codes: tuple[IRValueCode, ...] = ()
            if pending is not None and pending[0] == vsid:
                # Consume this group fully BEFORE advancing the groupby iterator
                # — groupby invalidates the sub-iterator on the next() below.
                codes = tuple(
                    IRValueCode(
                        code_id=r[1],
                        value_set_id=vsid,
                        code=r[2],
                        label=r[3],
                        valid_from=None,
                        valid_to=None,
                    )
                    for r in pending[1]
                )
                pending = next(member_groups, None)
            yield IRValueSet(
                value_set_id=vsid,
                member_hash=member_hash,
                classification_id=None,
                codes=codes,
            )

    def _emit_variables(self) -> Iterator[IRObject]:
        # variable.slug is NULL at emit time (populate_variable_slugs runs later).
        # Post-A4.3a-flip the materializer IGNORES the IR slug: it inserts
        # variable.slug NULL and populate_variable_slugs UPDATEs it in place (no
        # read-back). The "" here is a harmless placeholder for that ignored field.
        # Do NOT use `name` as the slug — they diverge (slug is a folded ASCII
        # stem). Stream the cursor (the variable table is large on real builds).
        for row in self.conn.execute(
            "SELECT variable_id, register_id, provider_key, name, definition, "
            "       description, measurement_unit, is_sensitive, is_identifier, "
            "       source_register_id, source_register_text, slug, source_label "
            "FROM variable ORDER BY variable_id"
        ):
            yield IRVariable(
                variable_id=row[0],
                register_id=row[1],
                provider_key=row[2],
                slug=row[11] or "",
                name=row[3],
                definition=row[4],
                description=row[5],
                measurement_unit=row[6],
                is_sensitive=bool(row[7]),
                is_identifier=bool(row[8]),
                source_register_id=row[9],
                source_register_text=row[10],
                source_label=row[12],
            )

    def _emit_variable_states(self) -> Iterator[IRObject]:
        # Stream the cursor (variable_state is large on real builds). A4.3a
        # carries delivery_column_name (the LATEST-era column) so the
        # materializer is the sole writer of variable_state; the FULL historical
        # column set rides on IRVariableAlias (_emit_variable_aliases). valid_to /
        # value_set_version_label are read back as the STORED sentinels here
        # ('9999-12-31' / ''); the materializer's None→sentinel reconciliation is
        # idempotent on them, so the round-trip stays byte-identical.
        for row in self.conn.execute(
            "SELECT state_id, variable_id, register_variant_id, valid_from, "
            "       valid_to, data_type, data_length, delivery_column_name, "
            "       value_set_id, value_set_version_label "
            "FROM variable_state ORDER BY state_id"
        ):
            yield IRVariableState(
                state_id=row[0],
                variable_id=row[1],
                register_variant_id=row[2],
                valid_from=row[3],
                valid_to=row[4],
                data_type=row[5],
                data_length=row[6],
                delivery_column_name=row[7],
                value_set_id=row[8],
                value_set_version_label=row[9],
            )

    def _emit_variable_aliases(self) -> Iterator[IRObject]:
        # The FULL delivery-column history (one row per historical column),
        # keyed by the cvid's OWNING variable_id + delivering variant. Read from
        # the cvid-grained `variable_alias_build` staging joined through the
        # coalescer's ground-truth `variable_instance.variable_id` stamp — the
        # exact projection `_reparent_variable_alias` performed in A4.1/A4.2, now
        # carried as IR so the materializer writes `variable_alias`. DISTINCT +
        # ORDER BY for deterministic emit. (`variable_instance` /
        # `variable_alias_build` still exist here — dropped later in materialize.)
        for variable_id, register_variant_id, column in self.conn.execute(
            "SELECT DISTINCT vi.variable_id, vi.register_variant_id, "
            "       vab.delivery_column_name "
            "FROM variable_alias_build vab "
            "JOIN variable_instance vi ON vi.cvid = vab.cvid "
            "WHERE vi.variable_id IS NOT NULL "
            "ORDER BY vi.variable_id, vi.register_variant_id, vab.delivery_column_name"
        ):
            yield IRVariableAlias(
                variable_id=variable_id,
                register_variant_id=register_variant_id,
                delivery_column_name=column,
            )

    def _emit_related_edges(self) -> Iterator[IRObject]:
        # Variable-grain split-sibling edges. The materializer resolves
        # variable_id → slug at write time (after slugs exist), so these are
        # emitted from the build-only routing list, not the shipped table.
        for a, b, kind in self.related_edges:
            yield IRRelatedToEdge(
                a_variable_id=a, b_variable_id=b, relation_kind=kind, note="auto:triage"
            )

    def _emit_replaced_by_edges(self) -> Iterator[IRObject]:
        # Variable-grain succession is derived by the materializer from the
        # adapter-written `timeseries_event`; A4.1 has no variable-grain
        # replaced_by IR carrier wired through, so nothing is emitted here yet
        # (G3 — register/variant/variable replaced_by stays materializer-side).
        return iter(())

    def _emit_provenance(self) -> Iterator[IRObject]:
        # One IRDeliveryProvenance per register_variant, carrying the
        # Registerversion approval dates (period_token → first/last-approved
        # date). SCB has no per-register delivery-version/date field, so those
        # stay None. EMITTED for A4.2; the materializer now WRITES these to the
        # provenance DB (A4.2; resolved fork #1).
        #
        # A4.2 keying fix (resolved fork (c)): approvals key on
        # (register_variant_id, period_token), NOT register_id. `register_version`
        # is grained by register_variant_id (db.py register_version DDL); the old
        # per-register key collapsed two variants sharing a `registerversionnamn`
        # token into one slot — last-writer-wins, and with no ORDER BY it was
        # also non-deterministic. The ORDER BY below pins emit order for
        # byte-stable provenance writes.
        #
        # Lifecycle note: `register_version` is dropped in materialize() AFTER
        # the emit() drain, so it still exists here (resolved fork (c)). Any
        # future move of this read into a post-emit materializer pass must run
        # before that DROP or capture the data first.
        first_by_variant: dict[int, dict[str, str]] = {}
        last_by_variant: dict[int, dict[str, str]] = {}
        for (
            variant_id,
            version_name,
            first_approved,
            last_approved,
        ) in self.conn.execute(
            "SELECT rver.register_variant_id, rver.registerversionnamn, "
            "       rver.registerversion_forstagodkannandedatum, "
            "       rver.registerversion_senastgodkanddatum "
            "FROM register_version rver "
            "ORDER BY rver.register_variant_id, rver.registerversionnamn, "
            "         rver.regver_id"
        ).fetchall():
            token = version_name or ""
            if first_approved:
                first_by_variant.setdefault(variant_id, {})[token] = first_approved
            if last_approved:
                last_by_variant.setdefault(variant_id, {})[token] = last_approved
        for variant_id, register_id in self.conn.execute(
            "SELECT register_variant_id, register_id FROM register_variant "
            "ORDER BY register_variant_id"
        ).fetchall():
            yield IRDeliveryProvenance(
                register_id=register_id,
                register_variant_id=variant_id,
                source_file="Registerinformation.csv",
                delivery_version=None,
                delivery_date=None,
                template_version=None,
                first_approval_dates=first_by_variant.get(variant_id) or None,
                last_approval_dates=last_by_variant.get(variant_id) or None,
            )

    def _emit_warnings(self) -> Iterator[IRObject]:
        # Triage-skip / projection-empty warnings. EMITTED for A4.2; the
        # materializer now WRITES these to the provenance DB (A4.2; resolved
        # fork #1).
        stats = self.coalesce_stats
        n_collapsed = stats.get("n_triage_collapsed", 0)
        if n_collapsed:
            yield IRWarning(
                entity_kind="variable_state",
                entity_id=0,
                code="triage_collapsed_states",
                detail=f"{n_collapsed} state(s) collapsed as residual drift",
            )
        n_empty = self.projection_stats.cvids_empty_after_projection
        if n_empty:
            yield IRWarning(
                entity_kind="value_set",
                entity_id=0,
                code="cvids_empty_after_projection",
                detail=f"{n_empty} cvid(s) had an empty value set after projection",
            )
