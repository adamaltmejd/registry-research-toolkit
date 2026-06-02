"""Database connection management and schema-compat checks for reg_meta.

The build pipeline (DDL, CSV import, ``build_db``) lives in
``reg_meta_build.db``; this module exposes only the query-side surface:
DB-path resolution, read-only ``open_db``, manifest reads, and the
shared ``SCHEMA_VERSION`` / ``DB_FILENAME`` constants that the build
side imports back.
"""

from __future__ import annotations

import os
import sqlite3
import sys
from datetime import UTC, datetime
from pathlib import Path

from .errors import EXIT_CONFIG, RegMetaError

# SCHEMA_VERSION bump history (Model A migration). Each entry pins the
# refactor stage that justified the bump and what becomes
# incompatible. `_check_schema_compat` rejects DBs with a different
# major OR an older minor — additive table/column changes are minor
# bumps, drops or renames are major. **Pre-v1 exception:** the A2.1.5
# two-level restructure (renames, a re-parent, column moves) rides the
# 4.x line by policy — regenerate-not-migrate means every tester rebuilds
# from source, so there is no in-place upgrade to break; the one
# consumer-visible major break is deferred to A2.7 (see below).
#
# - 4.0.0 (A1.1): §5.11 renamed ~21 columns across the universal schema
#   and dropped the SCB-Swedish names. Pre-4.x DBs reference columns
#   that no longer exist — hard break. A1.2's additive sensitivity
#   columns (`is_sensitive`, `is_identifier`) rode on the same major
#   bump and didn't require their own version step.
# - 4.1.0 (A2.1): added `variable_state` and dropped the
#   build-only `unika_summary` table. The drop is benign for the query
#   layer (nothing in `reg_meta` reads `unika_summary`); the addition
#   matters because the upcoming A2.5 resolver flip needs
#   `variable_state` to be populated. Old 4.0.0 DBs without that table
#   are rejected via the minor-version gate.
# - 4.2.0 (A2.1.5 restructure): two-level restructure (rides 4.x per the
#   pre-v1 exception above; lands across several commits on the a2.1.5
#   branch). `register_variant.regvar_id` renamed to `register_variant_id`
#   schema-wide; `variable` promoted (synthetic `variable_id` PK +
#   register-unique `slug`; `var_id` → `provider_key` join hint);
#   `variable_state` re-parented onto `variable_id` + a
#   `register_variant_id` coordinate; `variable_same_as` demoted to
#   variable grain. The first commit (the `regvar_id` rename) sets 4.2.0;
#   the structural commits stay on it.
# - 4.3.0 (A2.1.5 stored variable slug): `populate_variable_slugs`
#   fills `variable.slug` and the resolver now READS it instead of deriving the
#   variable slug from `delivery_column_name` at query time. A 4.2.0 DB (slug
#   column present but NULL — never populated) resolves nothing under the flip,
#   so it's rejected via the minor-version gate. Additive within the 4.x line;
#   no DDL change (the column shipped in 4.2.0).
# - 4.4.0 (A2.2 build-time triage + interim resolver flip): adds the
#   `variable_related_to` table, the `variable_state` state-uniqueness index
#   (UNIQUE(variable_id, register_variant_id, valid_from, value_set_version_label),
#   created post-triage by the coalescer), and triaged `variable_state` rows
#   (folds discriminated by `value_set_version_label`; splits under distinct
#   sibling `variable` rows). The interim binding resolver now reads
#   `variable_state` (keyed by `variable_id`), not the `variable_instance`
#   `provider_key` join. A 4.3.0 DB has un-triaged states (same-year multi-shape
#   rows violating the new index) and no `variable_related_to`, so it's rejected
#   via the minor-version gate. Additive within the 4.x line.
# - 4.5.0 (A2.3, current): added three directional succession edge tables —
#   `register_replaced_by`, `variant_replaced_by`, `variable_replaced_by` —
#   auto-derived from `timeseries_event` rows with `handelse IN ('Ersatt av',
#   'Ersätter')`. `variable_replaced_by` is variable grain (3-part
#   `provider/register/variable` endpoints; no variant — the two-level model
#   put the variant out of the binding). Additive tables only → minor bump;
#   old 4.4.0 DBs without them are rejected via the minor-version gate. A2.5's
#   resolver consumes these for `predecessors()` / `successors()`.
# - 4.6.0 (A2.4, current): added `variable_state_lineage` +
#   `variable_state_lineage_warning` — state-pair interval-overlap consumer→source
#   lineage (§5.6), materialized by `link_variable_state_lineage` from
#   `variable.source_register_id` + variable-grain `variable_same_as`. Replaces
#   v0.11's per-cvid `variable_instance.via_source_id` edges, but via_source_id
#   is KEPT in parallel through A2.6 (catalog.py's interim resolver still reads
#   it); both drop with `variable_instance` in A2.7. Additive tables only →
#   minor bump; 4.5.0 DBs without them are rejected via the minor-version gate.
# - 4.7.0 (A2.5 + #142, current): adds a `beskrivning` column to the three
#   `*_replaced_by` tables (the human transition reason from
#   `timeseries_event.beskrivning`, carried alongside the `auto:timeseries_event`
#   provenance marker in `note`), populates `effective_year` for the
#   AktuellVariabel variable grain (the successor edition's year; bare
#   Variabel/Register/RegisterVariant grains stay NULL — no edition), and adds
#   `idx_variable_replaced_by_successor` for the new `.predecessors()` accessor.
#   `Catalog.resolve()` flips its binding arm to the longitudinal
#   `ResolvedVariable` and grows resolve_at/states/predecessors/successors/
#   related/lineage/lineage_warnings (query-side only — no DDL impact beyond the
#   above). A 4.6.0 DB lacks the `beskrivning` column + successor index, so it's
#   rejected via the minor-version gate. Additive within the 4.x line.
# - 4.8.0 (A2.6): the FQID grammar flip (§5.2). The binding FQID drops
#   to 3 segments (`provider/register/slug`) and the variant / register_version
#   FQID kinds are gone — variant + period are delivery coordinates, not
#   identity. Shipped-DB shape change: the build-only `register_version`,
#   `population`, and `object_type` tables are DROPped before ship (like
#   `unika_summary` at 4.1.0) — `register_version` is consumed build-time (the
#   coalescer's valid_from/to year fallback + the lineage linkers) then dropped;
#   `population`/`object_type` are unread write-only debug tables (their content
#   moves to the provenance DB, A4.2). The `register_version.slug` column +
#   ~1,264 curated version-slug TOML entries are removed (version slugs leave the
#   model entirely). A 4.7.0 DB still carrying those tables resolves nothing
#   under the 3-seg grammar, so it's rejected via the minor-version gate.
#   Query-side only beyond the drops; no break to the 4.x line (5.0.0 stays
#   reserved for A2.7's `variable_instance` drop).
# - 4.9.0 (A2.6.1, current): 2-seg classification grammar (§5.2). The
#   classification FQID folds the vintage into the slug (`class/<slug>/<version>`
#   → `class/<slug>`; 'sun2020', 'lkf2007'). `classification.slug` becomes UNIQUE
#   and the redundant `classification.version` column is DROPped (the vintage
#   lives in slug + name + valid_from/to). A 4.8.0 DB carries the version column
#   + non-baked slugs ('sun' not 'sun2020'), so it resolves nothing under the
#   2-seg grammar and is rejected via the minor-version gate. Additive/drop
#   within the 4.x line (5.0.0 stays reserved for A2.7's `variable_instance` drop).
# - 5.0.0 (A2.7, current): drops `variable_instance` from the shipped DB now
#   that the resolver + every query reads `variable_state` / `variable` /
#   re-parented `variable_alias`. `variable_instance` is BUILT (coalescer,
#   classification tagging, value-set projection, code_variable_map) then
#   DROPped before ship, mirroring `register_version`/`unika_summary`. Also:
#   `variable_alias` re-parented onto `variable_id` + `register_variant_id`
#   (was cvid; full delivery-column history survives for `get_datacolumns`);
#   `variable_context` dropped from the DDL outright (write-only debug, no
#   consumer); `variable_instance.via_source_id` + `link_consumer_side_bindings`
#   removed (superseded by `variable_state_lineage`, 4.6.0); NEW
#   `variable_state.classification_id` (backfilled from instances) so
#   classification queries sibling-isolate off `variable_state` (which has
#   `variable_id`) — resolving the A2.6 `classifications_for_variable`
#   limitation. A 4.x DB still carries `variable_instance` + a cvid-keyed
#   `variable_alias` and no `variable_state.classification_id`, so it's rejected
#   via the MAJOR-version gate (4 != 5) — rebuild with `reg-meta-build build-db`.
# - 5.1.0 (post-A2.7 value→variable precision fix): re-grains the shipped
#   `code_variable_map` from `(code_id, register_id, var_id)` to
#   `(code_id, variable_id)`. An A2.2 triage split makes sibling variables share
#   one source `var_id`, so the old `(register_id, var_id)` key fanned each code
#   across EVERY sibling — including ones whose value set excluded it (false
#   positives in `search --value`; ~2.8M over-attributed `(code, variable)` pairs
#   on the real corpus). The populating cvid belongs to exactly one sibling,
#   carried via the coalescer's ground-truth `variable_instance.variable_id`
#   stamp (#150); `_search_values` now joins `code_variable_map.variable_id` →
#   `variable` instead of `(register_id, provider_key)`. `register_id`/`var_id`
#   are DROPped from the table (recoverable through the variable join). The drop
#   rides the 5.x line by the pre-v1 regenerate-not-migrate policy — the same
#   exception the A2.1.5 restructure took on 4.x: no in-place upgrade exists to
#   break, every tester rebuilds from source. A stale pre-5.1.0 DB carries
#   `cvm.register_id`/`cvm.var_id` but no `variable_id`, so the new
#   `_search_values` can't query it — the minor gate rejects it (5.0.0 < 5.1.0)
#   and `reg-meta update` refuses to install a 5.0.0 asset over a working DB
#   (`incompatible_db_asset`). Rebuild with `reg-meta-build build-db`.
# - 5.2.0 (A4.4c): additive panel_entity_key/panel_time_key/panel_time_grain
#   on register_variant — nullable panel-shape columns curated via TOML
#   (`populate_slugs`); most variants stay NULL (curation is a later seam).
#   `Catalog.list_variants` exposes them read-only. Additive within the 5.x
#   line; a 5.1.0 DB lacks the columns, so it's rejected via the minor gate.
SCHEMA_VERSION = "5.2.0"
DB_FILENAME = "reg_meta.db"


def default_db_dir() -> Path:
    """Default directory for the reg_meta database.

    Resolution: $REG_META_DB > $XDG_DATA_HOME/reg_meta > platform default.
    """
    if env := os.environ.get("REG_META_DB"):
        return Path(env).expanduser()
    if xdg := os.environ.get("XDG_DATA_HOME"):
        return Path(xdg) / "reg_meta"
    if sys.platform == "win32":
        return Path(os.environ.get("LOCALAPPDATA", "~/AppData/Local")) / "reg_meta"
    return Path.home() / ".local" / "share" / "reg_meta"


def db_path_from_args(db_arg: str | None, filename: str = DB_FILENAME) -> Path:
    if db_arg:
        return Path(db_arg).expanduser().resolve() / filename
    return default_db_dir().resolve() / filename


def _check_schema_compat(conn: sqlite3.Connection, db_path: Path) -> None:
    """Raise if the database schema is incompatible with the installed code.

    Code with ``SCHEMA_VERSION = M.m.p`` requires a DB whose manifest records a
    schema version with the same major M and minor >= m. A lower minor means
    the code may reference columns that don't exist in the DB; different majors
    are hard breaks. Patch differences are ignored.

    Missing or unparseable ``schema_version`` is treated as incompatible — the
    ``check_schema=False`` escape hatch exists for legitimate bypasses
    (e.g. ``reg-meta info``, doc DB).
    """
    fix = "Run `reg-meta update` to get a compatible database."

    try:
        manifest = get_manifest(conn)
    except sqlite3.OperationalError as exc:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="schema_incompatible",
            error_class="configuration",
            message=(
                f"Database manifest is missing or unreadable in {db_path}. "
                f"Expected schema v{SCHEMA_VERSION} metadata."
            ),
            remediation=fix,
        ) from exc

    db_ver = manifest.get("schema_version")
    try:
        if not db_ver:
            raise ValueError("missing schema_version")
        db_parts = db_ver.split(".")
        db_major, db_minor = int(db_parts[0]), int(db_parts[1])
        code_parts = SCHEMA_VERSION.split(".")
        code_major, code_minor = int(code_parts[0]), int(code_parts[1])
    except (ValueError, IndexError) as exc:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="schema_incompatible",
            error_class="configuration",
            message=(
                f"Database schema version is missing or invalid in {db_path}: "
                f"{db_ver!r}. This version of reg_meta expects schema v{SCHEMA_VERSION}."
            ),
            remediation=fix,
        ) from exc

    if db_major != code_major or db_minor < code_minor:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="schema_incompatible",
            error_class="configuration",
            message=(
                f"Database schema v{db_ver} ({db_path}) is incompatible "
                f"with this version of reg_meta (expects schema v{SCHEMA_VERSION})."
            ),
            remediation=fix,
        )


def open_db(
    db_path: Path,
    *,
    check_schema: bool = True,
    error_code: str = "db_not_found",
    remediation: str = (
        "Run `reg-meta update` to fetch the pre-built DB, "
        "or `reg-meta-build build-db --input-dir <path>` to build from CSV exports."
    ),
) -> sqlite3.Connection:
    if not db_path.exists():
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code=error_code,
            error_class="configuration",
            message=f"Database not found: {db_path}",
            remediation=remediation,
        )
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    if check_schema:
        try:
            _check_schema_compat(conn, db_path)
        except RegMetaError:
            conn.close()
            raise
    return conn


def get_manifest(conn: sqlite3.Connection) -> dict[str, str]:
    rows = conn.execute("SELECT key, value FROM import_manifest").fetchall()
    return {row["key"]: row["value"] for row in rows}


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
