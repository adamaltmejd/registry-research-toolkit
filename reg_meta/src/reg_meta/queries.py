"""Query functions for reg_meta.

Pure query logic against an open sqlite3.Connection. No CLI concerns
(argument parsing, output formatting, envelopes, timing). These are
the functions that library consumers (e.g. mock_data_wizard) import.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from .errors import EXIT_NOT_FOUND, EXIT_USAGE, RegMetaError
from .fqid import Fqid, try_emit

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Iterable

_YEAR_RE = re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)")


def _try_int(value: str) -> int | str:
    """Convert to int if the string is numeric, otherwise return as-is."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return value


# ---------------------------------------------------------------------------
# Register lookup
# ---------------------------------------------------------------------------


def resolve_register_ids(conn: sqlite3.Connection, value: str) -> list[int]:
    """Resolve a register name or ID to a list of register_ids.

    Tries: exact ID → case-insensitive name → substring match.
    Returns empty list if nothing found.
    """
    # IDs are INTEGER — convert for exact match
    row = conn.execute(
        "SELECT register_id FROM register WHERE register_id = ?", (_try_int(value),)
    ).fetchone()
    if row:
        return [row["register_id"]]

    rows = conn.execute(
        # `register.name` is the §5.11 rename of `registernamn`; values
        # are still provider-native (e.g. "LISA").
        "SELECT register_id FROM register WHERE LOWER(name) = LOWER(?)",
        (value,),
    ).fetchall()
    if rows:
        return [r["register_id"] for r in rows]

    rows = conn.execute(
        "SELECT register_id FROM register WHERE LOWER(name) LIKE '%' || LOWER(?) || '%'",
        (value,),
    ).fetchall()
    return [r["register_id"] for r in rows]


def require_register_ids(conn: sqlite3.Connection, value: str) -> list[int]:
    """Like resolve_register_ids but raises NOT_FOUND if empty."""
    ids = resolve_register_ids(conn, value)
    if not ids:
        raise RegMetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"No register matching '{value}'.",
            remediation="Use `reg-meta search` to find valid register names or IDs.",
        )
    return ids


# ---------------------------------------------------------------------------
# Year helpers
# ---------------------------------------------------------------------------


def parse_year_range(spec: str) -> tuple[int | None, int | None]:
    """Parse '2010', '2010-2015', '2010-', '-2015' into (lo, hi) bounds."""
    if "-" in spec:
        parts = spec.split("-", 1)
        lo = int(parts[0]) if parts[0] else None
        hi = int(parts[1]) if parts[1] else None
        return lo, hi
    return int(spec), int(spec)


def extract_year(version_name: str) -> int | None:
    """Extract a 1900-2099 year from a version name. Rejects 4-digit runs
    embedded in longer digit sequences (so "v19999" → None, not 1999) and
    out-of-range numbers (so "Komvux 1234-poäng" → None, not 1234)."""
    m = _YEAR_RE.search(version_name)
    return int(m.group()) if m else None


def _years_in_range(lo_iso: str, hi_iso: str) -> list[int]:
    """A2.6: the calendar years a `variable_state` validity window
    (`valid_from`..`valid_to`, ISO `YYYY-MM-DD`) spans, for DISPLAY enumeration
    only (availability year lists, lineage year ranges). The shipped DB has no
    `register_version` to read an edition year from; the per-state validity
    window is the year source now. The open-ended sentinel `9999-12-31` is
    capped at the start year so a still-active state contributes only its own
    opening year, not a 7000-year run.

    NOT for requested-year FILTERING: capping the open end here would wrongly
    drop a still-active state from any year past its opening. Year filters route
    through `_state_covers_year` / `_state_overlaps_years` instead, which read
    the sentinels with `<=`/`>=` and keep open-ended/multi-year windows."""
    lo = int(lo_iso[:4])
    hi = int(hi_iso[:4])
    if hi >= 9999:
        return [lo]
    return list(range(lo, hi + 1))


def _state_covers_year(valid_from: str, valid_to: str, year: int) -> bool:
    """True when a `variable_state` validity window (`valid_from`..`valid_to`,
    ISO `YYYY-MM-DD`) covers the calendar `year`.

    A2.6 overlap semantics for requested-year FILTERS: a window with year bounds
    `[from_year, to_year]` covers `year` iff `from_year <= year <= to_year`. The
    `9999` (open-ended) and `0001` (yearless-fallback) sentinels read naturally
    under `<=`/`>=`, so a multi-year, still-active, or yearless window matches
    any year it actually spans — not just its opening year."""
    return int(valid_from[:4]) <= year <= int(valid_to[:4])


def _state_overlaps_years(
    valid_from: str, valid_to: str, lo: int | None, hi: int | None
) -> bool:
    """True when a `variable_state` validity window overlaps the requested year
    range `[lo, hi]` (either bound may be ``None`` for open-ended).

    A2.6 overlap semantics: window `[from_year, to_year]` overlaps `[lo, hi]` iff
    `from_year <= hi AND to_year >= lo`. Missing bounds widen to the sentinels
    (`hi=None` → 9999, `lo=None` → 0) so an open-ended request matches every
    window, and the `9999`/`0001` window sentinels match correctly too."""
    from_year = int(valid_from[:4])
    to_year = int(valid_to[:4])
    return from_year <= (hi if hi is not None else 9999) and to_year >= (
        lo if lo is not None else 0
    )


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


SEARCH_FIELDS = frozenset({"datacolumn", "varname", "description", "value", "all"})


def _filter_search_by_years(
    conn: sqlite3.Connection,
    results: list[dict[str, Any]],
    years: str,
) -> list[dict[str, Any]]:
    """Filter search results to those with versions in the given year range."""
    year_lo, year_hi = parse_year_range(years)
    if not results:
        return results

    # Collect unique register_ids and (register_id, var_id) pairs to check
    var_pairs: set[tuple[int, int]] = set()
    reg_only_ids: set[int] = set()
    for r in results:
        rid = r.get("register_id")
        vid = r.get("var_id")
        if rid is not None and vid is not None:
            var_pairs.add((rid, vid))
        elif rid is not None:
            reg_only_ids.add(rid)

    # A2.6: edition years come from `variable_state` validity windows now (the
    # register_version table is dropped before ship). A (register_id, var_id)
    # pair is in-range if any of its states' validity window overlaps the year
    # range; `var_id` is the variable's `provider_key`.
    valid_var_pairs: set[tuple[int, int]] = set()
    if var_pairs:
        all_reg_ids = {p[0] for p in var_pairs}
        placeholders = ",".join("?" * len(all_reg_ids))
        rows = conn.execute(
            "SELECT DISTINCT v.register_id, CAST(v.provider_key AS INTEGER) AS var_id, "
            "vs.valid_from, vs.valid_to "
            "FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            f"WHERE v.register_id IN ({placeholders})",
            list(all_reg_ids),
        ).fetchall()
        for row in rows:
            pair = (row["register_id"], row["var_id"])
            if pair not in var_pairs:
                continue
            if _state_overlaps_years(
                row["valid_from"], row["valid_to"], year_lo, year_hi
            ):
                valid_var_pairs.add(pair)

    # For register-type results: check if register has any state in range.
    valid_reg_ids: set[int] = set()
    if reg_only_ids:
        placeholders = ",".join("?" * len(reg_only_ids))
        rows = conn.execute(
            "SELECT DISTINCT v.register_id, vs.valid_from, vs.valid_to "
            "FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            f"WHERE v.register_id IN ({placeholders})",
            list(reg_only_ids),
        ).fetchall()
        for row in rows:
            if _state_overlaps_years(
                row["valid_from"], row["valid_to"], year_lo, year_hi
            ):
                valid_reg_ids.add(row["register_id"])

    filtered = []
    for r in results:
        rid = r.get("register_id")
        vid = r.get("var_id")
        if rid is not None and vid is not None:
            if (rid, vid) in valid_var_pairs:
                filtered.append(r)
        elif rid is not None:
            if rid in valid_reg_ids:
                filtered.append(r)
        else:
            filtered.append(r)
    return filtered


def search(
    conn: sqlite3.Connection,
    query: str,
    *,
    field: str = "all",
    type: str = "all",
    register: str | None = None,
    years: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """Search across registers and variables.

    field controls what is searched:
      - "datacolumn": column aliases (LIKE pattern match)
      - "varname": canonical variable names (LIKE pattern match)
      - "description": FTS on variable/register descriptions
      - "value": value codes and labels (LIKE pattern match)
      - "all": all of the above (default)

    Returns {"total_count": int, "results": [...]}.
    Doc results are NOT included here — the CLI layer merges them separately.
    """
    if field not in SEARCH_FIELDS:
        raise RegMetaError(
            exit_code=EXIT_USAGE,
            code="usage_error",
            error_class="usage",
            message=f"Invalid search field '{field}'. Valid: {sorted(SEARCH_FIELDS)}",
            remediation="Use --datacolumn, --varname, --description, --value, or --all-fields.",
        )

    reg_ids: set[int] | None = None
    if register:
        ids = resolve_register_ids(conn, register)
        if not ids:
            return {"total_count": 0, "results": []}
        reg_ids = set(ids)

    _REGISTER_TYPES = {"register"}
    _VARIABLE_TYPES = {"variable", "varname", "datacolumn", "value"}

    all_results: list[dict[str, Any]] = []
    like_pattern = f"%{query}%"

    if field in ("datacolumn", "all"):
        all_results.extend(_search_datacolumns(conn, like_pattern, reg_ids))

    if field in ("varname", "all"):
        all_results.extend(_search_varnames(conn, like_pattern, reg_ids))

    if field in ("description", "all"):
        if type in ("register", "all"):
            all_results.extend(_search_description_registers(conn, query, reg_ids))
        if type in ("variable", "all"):
            all_results.extend(_search_description_variables(conn, query, reg_ids))

    if field in ("value", "all"):
        all_results.extend(_search_values(conn, like_pattern, reg_ids))

    if type == "register":
        all_results = [r for r in all_results if r["type"] in _REGISTER_TYPES]
    elif type == "variable":
        all_results = [r for r in all_results if r["type"] in _VARIABLE_TYPES]

    if years:
        all_results = _filter_search_by_years(conn, all_results, years)

    all_results.sort(key=lambda x: x.get("fts_rank", 0))
    total_count = len(all_results)
    results = all_results[offset : offset + limit]

    return {"total_count": total_count, "results": results}


def _search_datacolumns(
    conn: sqlite3.Connection, like_pattern: str, reg_ids: set[int] | None
) -> list[dict[str, Any]]:
    # Aliased SELECT so both `variable.name` and `register.name` land under
    # distinct row keys after the §5.11 rename collapsed them to a single
    # column name.
    # A2.7: `variable_alias` is variable_id-keyed now (was cvid-keyed). Join
    # straight to `variable` via `variable_id`; `var_id` is the variable's
    # `provider_key`.
    rows = conn.execute(
        "SELECT DISTINCT va.delivery_column_name, v.register_id, "
        "CAST(v.provider_key AS INTEGER) AS var_id, "
        "v.name AS variable_name, r.name AS register_name "
        "FROM variable_alias va "
        "JOIN variable v ON va.variable_id = v.variable_id "
        "JOIN register r ON v.register_id = r.register_id "
        "WHERE va.delivery_column_name LIKE ? "
        "ORDER BY va.delivery_column_name, v.register_id",
        (like_pattern,),
    ).fetchall()
    results = []
    for r in rows:
        if reg_ids and r["register_id"] not in reg_ids:
            continue
        results.append(
            {
                "type": "datacolumn",
                "datacolumn": r["delivery_column_name"],
                "register_id": r["register_id"],
                "register_name": r["register_name"],
                "var_id": r["var_id"],
                "variable_name": r["variable_name"],
                "fts_rank": 0,
            }
        )
    return results


def _search_varnames(
    conn: sqlite3.Connection, like_pattern: str, reg_ids: set[int] | None
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT v.register_id, CAST(v.provider_key AS INTEGER) AS var_id, "
        "v.name AS variable_name, r.name AS register_name "
        "FROM variable v "
        "JOIN register r ON v.register_id = r.register_id "
        "WHERE v.name LIKE ? "
        "ORDER BY v.name, v.register_id",
        (like_pattern,),
    ).fetchall()
    results = []
    for r in rows:
        if reg_ids and r["register_id"] not in reg_ids:
            continue
        results.append(
            {
                "type": "varname",
                "register_id": r["register_id"],
                "register_name": r["register_name"],
                "var_id": r["var_id"],
                "variable_name": r["variable_name"],
                "fts_rank": 0,
            }
        )
    return results


def _search_description_registers(
    conn: sqlite3.Connection, query: str, reg_ids: set[int] | None
) -> list[dict[str, Any]]:
    # register_fts now mirrors the renamed columns: `name` + `purpose`.
    # `registerrubrik` was dropped per §5.11.
    rows = conn.execute(
        "SELECT register_id, name, purpose, rank "
        "FROM register_fts WHERE register_fts MATCH ? "
        "ORDER BY rank",
        (query,),
    ).fetchall()
    results = []
    for r in rows:
        if reg_ids and r["register_id"] not in reg_ids:
            continue
        results.append(
            {
                "type": "register",
                "register_id": r["register_id"],
                "register_name": r["name"],
                "register_purpose": r["purpose"],
                "fts_rank": r["rank"],
            }
        )
    return results


def _search_description_variables(
    conn: sqlite3.Connection, query: str, reg_ids: set[int] | None
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT vf.register_id, CAST(vf.provider_key AS INTEGER) AS var_id, "
        "vf.name AS variable_name, vf.definition AS variable_definition, "
        "vf.description AS variable_description, vf.rank, "
        "r.name AS register_name, r.purpose AS register_purpose "
        "FROM variable_fts vf "
        "JOIN register r ON vf.register_id = r.register_id "
        "WHERE variable_fts MATCH ? "
        "ORDER BY vf.rank",
        (query,),
    ).fetchall()
    results = []
    for r in rows:
        if reg_ids and r["register_id"] not in reg_ids:
            continue
        results.append(
            {
                "type": "variable",
                "register_id": r["register_id"],
                "register_name": r["register_name"],
                "register_purpose": r["register_purpose"],
                "var_id": r["var_id"],
                "variable_name": r["variable_name"],
                "variable_definition": r["variable_definition"],
                "fts_rank": r["rank"],
            }
        )
    return results


def _search_values(
    conn: sqlite3.Connection, like_pattern: str, reg_ids: set[int] | None
) -> list[dict[str, Any]]:
    # `code_variable_map` is variable_id-grained, so a code resolves to its exact
    # owning sibling(s) — NOT every variable sharing a `provider_key`. Post-A2.2
    # a split makes siblings share one source `var_id`, so the old
    # `(register_id, provider_key)` join fanned a code across all of them, even
    # siblings whose value set excluded it (false positives). `register_id` /
    # `var_id` come off the joined `variable` row (the map no longer stores them).
    # No DISTINCT: the grain is one row per (code_id, variable_id) — `value_code`
    # and `code_variable_map`'s PKs, then PK joins to `variable` / `register` —
    # and each is already a distinct output tuple (code ≡ code_id via
    # UNIQUE(code, label); slug ≡ variable_id via UNIQUE(register_id, slug),
    # non-NULL in any slugged/query-serving build). Unlike `_search_datacolumns`,
    # there is no variant fan-out to dedup.
    rows = conn.execute(
        "SELECT vc.code, vc.label, "
        "v.register_id, CAST(v.provider_key AS INTEGER) AS var_id, "
        "v.slug AS variable_slug, "
        "v.name AS variable_name, r.name AS register_name "
        "FROM value_code vc "
        "JOIN code_variable_map cvm ON vc.code_id = cvm.code_id "
        "JOIN variable v ON cvm.variable_id = v.variable_id "
        "JOIN register r ON v.register_id = r.register_id "
        "WHERE vc.code LIKE ? OR vc.label LIKE ? "
        "ORDER BY vc.code "
        "LIMIT 500",
        (like_pattern, like_pattern),
    ).fetchall()
    results = []
    for r in rows:
        if reg_ids and r["register_id"] not in reg_ids:
            continue
        results.append(
            {
                "type": "value",
                # §5.11: SCB `vardekod`/`vardebenamning` are exposed in the
                # JSON envelope under the universal English `code`/`label`.
                "code": r["code"],
                "label": r["label"],
                "register_id": r["register_id"],
                "register_name": r["register_name"],
                "var_id": r["var_id"],
                # The specific owning variable. Split siblings share var_id and
                # `name` but have distinct slugs, so the slug is what names the
                # exact sibling whose value set contains this code.
                "variable_slug": r["variable_slug"],
                "variable_name": r["variable_name"],
                "fts_rank": 0,
            }
        )
    return results


# ---------------------------------------------------------------------------
# Get register
# ---------------------------------------------------------------------------


def get_register(
    conn: sqlite3.Connection,
    register: str,
) -> list[dict[str, Any]]:
    """Get register(s) by name or ID with variants.

    Returns a list of register dicts, each with a "variants" key.
    """
    reg_ids = require_register_ids(conn, register)

    registers = []
    for rid in reg_ids:
        # SELECT * picks up the renamed `name` and `purpose` columns; the
        # row dict surfaces them under those keys (consumers updated).
        reg = conn.execute(
            "SELECT r.*, p.slug AS provider_slug "
            "FROM register r JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE r.register_id = ?",
            (rid,),
        ).fetchone()
        entry = dict(reg)
        provider_slug = entry.pop("provider_slug")
        register_fqid = try_emit(Fqid.register_fqid, provider_slug, entry["slug"])
        entry["fqid"] = register_fqid
        variants = conn.execute(
            "SELECT * FROM register_variant WHERE register_id = ? ORDER BY register_variant_id",
            (rid,),
        ).fetchall()
        variant_dicts: list[dict[str, Any]] = []
        for v in variants:
            vd = dict(v)
            # A2.6: a variant is a navigational sub-resource of a register, not a
            # slash-path FQID (§5.2 DECISION POINT 2). It carries the parent
            # register FQID + its own slug (the `?variant=` browse coordinate),
            # not an addressable variant FQID.
            vd["register_fqid"] = register_fqid
            variant_dicts.append(vd)
        entry["variants"] = variant_dicts
        registers.append(entry)
    return registers


# ---------------------------------------------------------------------------
# Get schema
# ---------------------------------------------------------------------------


def _in_placeholders(ids: Iterable[object]) -> str:
    return ",".join("?" for _ in ids)


def get_schema(
    conn: sqlite3.Connection,
    *,
    register_variant_id: str | None = None,
    register: str | None = None,
    years: str | None = None,
    columns_like: str | None = None,
) -> dict[str, Any]:
    """Get column listing organized by variant → version → columns.

    Requires either register_variant_id or register. Returns {"variants": [...]}.
    """
    variant_select = (
        "SELECT rv.*, p.slug AS provider_slug, r.slug AS register_slug "
        "FROM register_variant rv "
        "JOIN register r ON rv.register_id = r.register_id "
        "JOIN provider p ON r.provider_id = p.provider_id "
    )
    if register_variant_id:
        rv = conn.execute(
            variant_select + "WHERE rv.register_variant_id = ?",
            (_try_int(register_variant_id),),
        ).fetchone()
        if not rv:
            raise RegMetaError(
                exit_code=EXIT_NOT_FOUND,
                code="not_found",
                error_class="query",
                message=f"Register variant {register_variant_id} not found.",
                remediation="Use `reg-meta get register <name>` to list variants.",
            )
        variant_rows = [rv]
    elif register:
        reg_ids = require_register_ids(conn, register)
        variant_rows = conn.execute(
            variant_select + f"WHERE rv.register_id IN ({_in_placeholders(reg_ids)}) "
            "ORDER BY rv.register_id, rv.register_variant_id",
            reg_ids,
        ).fetchall()
        if not variant_rows:
            raise RegMetaError(
                exit_code=EXIT_NOT_FOUND,
                code="not_found",
                error_class="query",
                message=f"No variants found for register '{register}'.",
                remediation="Use `reg-meta get register <name>` to verify.",
            )
    else:
        raise RegMetaError(
            exit_code=EXIT_USAGE,
            code="usage_error",
            error_class="usage",
            message="Provide either a register_variant_id or register.",
            remediation="Usage: get_schema(conn, register_variant_id=...) or get_schema(conn, register=...)",
        )

    year_lo, year_hi = None, None
    if years:
        year_lo, year_hi = parse_year_range(years)

    variants_out: list[dict[str, Any]] = []
    for rv in variant_rows:
        rvid = rv["register_variant_id"]
        provider_slug = rv["provider_slug"]
        register_slug = rv["register_slug"]
        # A2.6: schema is organized by `variable_state` editions now (validity
        # windows), not the dropped `register_version`. One "version" per
        # distinct (valid_from, valid_to) DELIVERY WINDOW the variant delivered;
        # its columns are every state in that window. `value_set_version_label`
        # is a PER-COLUMN attribute (a §5.7 folded multi-vintage variable carries
        # two states in the SAME window with labels like `sni92`/`sni2007` while
        # ordinary columns carry ''), so it must NOT be part of the edition key —
        # keying by it would shard one delivered schema into partial pseudo-
        # versions. The binding FQID is 3-seg (provider/register/variable_slug).
        state_rows = conn.execute(
            "SELECT vs.valid_from, vs.valid_to, vs.value_set_version_label, "
            "vs.data_type, vs.data_length, vs.delivery_column_name, "
            "CAST(v.provider_key AS INTEGER) AS var_id, v.slug AS variable_slug, "
            "v.name AS variable_name, COALESCE(v.source_label, '') AS source "
            "FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "WHERE vs.register_variant_id = ? "
            "ORDER BY vs.valid_from, vs.valid_to, vs.value_set_version_label, v.slug",
            (rvid,),
        ).fetchall()

        # Group states into editions keyed by the DELIVERY WINDOW only,
        # preserving first-seen order for determinism. One edition per window
        # holds ALL columns delivered then — including every vintage-state of a
        # folded variable; the label rides on each column below.
        editions: dict[tuple[str, str], list[sqlite3.Row]] = {}
        for s in state_rows:
            editions.setdefault((s["valid_from"], s["valid_to"]), []).append(s)

        versions_out: list[dict[str, Any]] = []
        for (valid_from, valid_to), states in editions.items():
            # A2.6: filter by validity-window OVERLAP against the requested
            # years, not the opening year alone — a multi-year or open-ended
            # edition must survive a filter for any year it spans. `year` below
            # stays the opening year for display.
            if years and not _state_overlaps_years(
                valid_from, valid_to, year_lo, year_hi
            ):
                continue
            year = int(valid_from[:4])

            col_dicts: list[dict[str, Any]] = []
            for s in states:
                col_dicts.append(
                    {
                        "var_id": s["var_id"],
                        "data_type": s["data_type"],
                        "data_length": s["data_length"],
                        "variable_name": s["variable_name"],
                        "source": s["source"],
                        # Per-column §5.7 vintage discriminator: '' for ordinary
                        # columns, e.g. `sni92`/`sni2007` for the two states of a
                        # folded multi-vintage variable sharing this window.
                        "value_set_version_label": s["value_set_version_label"],
                        # The state's denormalized latest alias (§5.1) is the
                        # display column; emit it under `aliases` for the
                        # table/flat renderers and `compare()` flattening.
                        "aliases": s["delivery_column_name"] or "",
                        "fqid": try_emit(
                            Fqid.binding_fqid,
                            provider_slug,
                            register_slug,
                            s["variable_slug"],
                        ),
                    }
                )
            if columns_like:
                pattern = re.compile(columns_like, re.IGNORECASE)
                col_dicts = [
                    c
                    for c in col_dicts
                    if pattern.search(c.get("aliases") or "")
                    or pattern.search(c.get("variable_name") or "")
                ]

            versions_out.append(
                {
                    "valid_from": valid_from,
                    "valid_to": valid_to,
                    "year": year,
                    "columns": col_dicts,
                }
            )

        if versions_out:
            variants_out.append(
                {
                    "register_variant_id": rvid,
                    "register_id": rv["register_id"],
                    # §5.11: the variant's name + description (was Swedish
                    # `registervariantnamn` / `registervariantbeskrivning`).
                    "variant_name": rv["name"],
                    "variant_description": rv["description"],
                    # A2.6: a variant has no slash-path FQID; it carries the
                    # parent register FQID + its browse slug.
                    "register_fqid": try_emit(
                        Fqid.register_fqid, provider_slug, register_slug
                    ),
                    "variant": rv["slug"],
                    "versions": versions_out,
                }
            )

    return {"variants": variants_out}


# ---------------------------------------------------------------------------
# Get varinfo
# ---------------------------------------------------------------------------


def get_varinfo(
    conn: sqlite3.Connection,
    variable: str,
    *,
    register: str | None = None,
) -> list[dict[str, Any]]:
    """Get detailed variable information.

    Returns a list of variable dicts, each with an "instances" key.
    """
    reg_ids: list[int] | None = None
    if register:
        reg_ids = require_register_ids(conn, register)

    # Match variable by var_id first, fall back to name
    int_variable = _try_int(variable)
    # SELECT v.* surfaces the renamed `name`/`definition`/`description`
    # columns directly. `r.name AS register_name` disambiguates the join.
    if reg_ids:
        ph = _in_placeholders(reg_ids)
        vars_by_id = conn.execute(
            f"SELECT v.*, CAST(v.provider_key AS INTEGER) AS var_id, r.name AS register_name FROM variable v "
            f"JOIN register r ON v.register_id = r.register_id "
            f"WHERE v.provider_key = CAST(? AS TEXT) AND v.register_id IN ({ph})",
            [int_variable, *reg_ids],
        ).fetchall()
        vars_by_name = conn.execute(
            f"SELECT v.*, CAST(v.provider_key AS INTEGER) AS var_id, r.name AS register_name FROM variable v "
            f"JOIN register r ON v.register_id = r.register_id "
            f"WHERE LOWER(v.name) = LOWER(?) AND v.register_id IN ({ph})",
            [variable, *reg_ids],
        ).fetchall()
    else:
        vars_by_id = conn.execute(
            "SELECT v.*, CAST(v.provider_key AS INTEGER) AS var_id, r.name AS register_name FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "WHERE v.provider_key = CAST(? AS TEXT)",
            (int_variable,),
        ).fetchall()
        vars_by_name = conn.execute(
            "SELECT v.*, CAST(v.provider_key AS INTEGER) AS var_id, r.name AS register_name FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "WHERE LOWER(v.name) = LOWER(?)",
            (variable,),
        ).fetchall()

    matched_vars = vars_by_id or vars_by_name

    # Fall back to alias (column name) lookup
    if not matched_vars:
        # A2.7: `variable_alias` is variable_id-keyed; join straight to `variable`.
        alias_sql = (
            "SELECT DISTINCT v.*, CAST(v.provider_key AS INTEGER) AS var_id, r.name AS register_name FROM variable_alias a "
            "JOIN variable v ON a.variable_id = v.variable_id "
            "JOIN register r ON v.register_id = r.register_id "
            "WHERE LOWER(a.delivery_column_name) = LOWER(?)"
        )
        if reg_ids:
            ph = _in_placeholders(reg_ids)
            alias_sql += f" AND v.register_id IN ({ph})"
            matched_vars = conn.execute(alias_sql, [variable, *reg_ids]).fetchall()
        else:
            matched_vars = conn.execute(alias_sql, (variable,)).fetchall()

    if not matched_vars:
        raise RegMetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"No variable matching '{variable}'"
            + (f" in register '{register}'" if register else "")
            + ".",
            remediation="Use `reg-meta search --query <term>` to find variables.",
        )

    variables_out: list[dict[str, Any]] = []
    for var in matched_vars:
        rid, vid = var["register_id"], var["var_id"]
        variable_id = var["variable_id"]

        # A2.6: "instances" are `variable_state` rows now (per-delivery shape),
        # not per-cvid `variable_instance` × `register_version` rows. Each state
        # carries its variant + validity window + value-set version; the year
        # comes from `valid_from`. The 3-seg binding FQID is built from the
        # state's own variable slug — split siblings each surface their own slug,
        # not a shared `(register_id, var_id)` pick.
        #
        # Select states by the matched row's `variable_id`, NOT by
        # `(register_id, provider_key)`: `provider_key` is NON-unique after an
        # A2.2 split (siblings share one source key), so a provider_key filter
        # would fan in every sibling's states under this one matched variable.
        states = conn.execute(
            "SELECT vs.state_id, vs.register_variant_id, vs.valid_from, vs.valid_to, "
            "vs.value_set_version_label, vs.data_type, vs.data_length, "
            "vs.delivery_column_name, vs.value_set_id, "
            "rv.name AS variant_name, "
            "p.slug AS provider_slug, r.slug AS register_slug, v.slug AS variable_slug "
            "FROM variable_state vs "
            "JOIN variable v ON vs.variable_id = v.variable_id "
            "JOIN register_variant rv ON vs.register_variant_id = rv.register_variant_id "
            "JOIN register r ON v.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE vs.variable_id = ? "
            "ORDER BY vs.valid_from, vs.valid_to, vs.value_set_version_label, "
            "vs.register_variant_id, vs.state_id",
            (variable_id,),
        ).fetchall()

        # Value-set member counts per value_set_id (None when the state has no
        # codes). Batched so a wide variable doesn't fan out N+1 queries.
        vs_ids = {s["value_set_id"] for s in states if s["value_set_id"] is not None}
        value_counts: dict[int, int] = dict.fromkeys(vs_ids, 0)
        if vs_ids:
            vs_ph = _in_placeholders(vs_ids)
            for row in conn.execute(
                f"SELECT value_set_id, COUNT(*) AS cnt FROM value_set_member "
                f"WHERE value_set_id IN ({vs_ph}) GROUP BY value_set_id",
                list(vs_ids),
            ):
                value_counts[row["value_set_id"]] = row["cnt"]

        instances_out: list[dict[str, Any]] = []
        for s in states:
            col = s["delivery_column_name"]
            inst_dict: dict[str, Any] = {
                "state_id": s["state_id"],
                "register_variant_id": s["register_variant_id"],
                "variant_name": s["variant_name"],
                "valid_from": s["valid_from"],
                "valid_to": s["valid_to"],
                "value_set_version_label": s["value_set_version_label"],
                "year": int(s["valid_from"][:4]),
                "data_type": s["data_type"],
                "data_length": s["data_length"],
                # The state's denormalized latest alias (§5.1); list-shaped for
                # the CLI renderer's `", ".join(...)`.
                "aliases": [col] if col else [],
                "value_set_count": (
                    value_counts.get(s["value_set_id"], 0)
                    if s["value_set_id"] is not None
                    else 0
                ),
                "fqid": try_emit(
                    Fqid.binding_fqid,
                    s["provider_slug"],
                    s["register_slug"],
                    s["variable_slug"],
                ),
            }
            instances_out.append(inst_dict)

        var_classifications = classifications_for_variable(conn, variable_id)

        variables_out.append(
            {
                "register_id": rid,
                "register_name": var["register_name"],
                "var_id": vid,
                # §5.11 keys: SCB Swedish columns surface here as the
                # universal English names. Dropped columns
                # (variabelreferenstid, variabelhamtadfran,
                # variabelextern_kommentar, variabeloperationell_definition)
                # no longer appear; their values (where meaningful) were
                # folded into `description` at ingest.
                "name": var["name"],
                "definition": var["definition"],
                "description": var["description"],
                "source_register_text": var["source_register_text"],
                "measurement_unit": var["measurement_unit"],
                "classifications": var_classifications,
                "instances": instances_out,
            }
        )

    return variables_out


# ---------------------------------------------------------------------------
# Get availability
# ---------------------------------------------------------------------------


def get_availability(
    conn: sqlite3.Connection,
    target: str,
    *,
    register: str | None = None,
) -> dict[str, Any]:
    """Return temporal availability summary for a variable or register.

    Auto-detects target type: tries variable first, falls back to register.
    """
    result = _get_availability_variable(conn, target, register=register)
    if result is not None:
        return result

    result = _get_availability_register(conn, target)
    if result is not None:
        return result

    raise RegMetaError(
        exit_code=EXIT_NOT_FOUND,
        code="not_found",
        error_class="query",
        message=f"No variable or register matching '{target}'.",
        remediation="Use `reg-meta search` to find valid names or IDs.",
    )


def _get_availability_variable(
    conn: sqlite3.Connection,
    variable: str,
    *,
    register: str | None = None,
) -> dict[str, Any] | None:
    """Availability for a variable across registers and years."""
    int_variable = _try_int(variable)

    reg_filter = ""
    params: list = [int_variable, variable]
    if register:
        ids = resolve_register_ids(conn, register)
        if not ids:
            return None
        ph = _in_placeholders(ids)
        reg_filter = f" AND v.register_id IN ({ph})"
        params.extend(ids)

    var_rows = conn.execute(
        "SELECT v.variable_id, v.register_id, CAST(v.provider_key AS INTEGER) AS var_id, v.name AS variable_name, "
        "r.name AS register_name FROM variable v "
        "JOIN register r ON v.register_id = r.register_id "
        f"WHERE (v.provider_key = CAST(? AS TEXT) OR LOWER(v.name) = LOWER(?)){reg_filter}",
        params,
    ).fetchall()

    if not var_rows:
        return None

    # Gather all version years and aliases per (register, year)
    all_years: set[int] = set()
    registers_out: list[dict[str, Any]] = []

    for var in var_rows:
        rid = var["register_id"]
        vid = var["var_id"]
        variable_id = var["variable_id"]

        # A2.6: year coverage comes from `variable_state` validity windows
        # (register_version is dropped before ship). Each state contributes the
        # calendar years its window spans; its delivery column is the per-year
        # alias.
        #
        # Select by the matched `variable_id`, NOT `(register_id, provider_key)`:
        # `provider_key` is NON-unique after an A2.2 split, so a provider_key
        # filter would credit one sibling with every sibling's year coverage.
        rows = conn.execute(
            "SELECT vs.valid_from, vs.valid_to, vs.delivery_column_name "
            "FROM variable_state vs "
            "WHERE vs.variable_id = ? "
            "ORDER BY vs.valid_from, vs.valid_to",
            (variable_id,),
        ).fetchall()

        reg_years: list[int] = []
        aliases_by_year: dict[str, list[str]] = {}
        for row in rows:
            col = row["delivery_column_name"]
            for year in _years_in_range(row["valid_from"], row["valid_to"]):
                reg_years.append(year)
                all_years.add(year)
                bucket = aliases_by_year.setdefault(str(year), [])
                if col and col not in bucket:
                    bucket.append(col)

        if not reg_years:
            continue

        reg_years_sorted = sorted(set(reg_years))
        min_y, max_y = reg_years_sorted[0], reg_years_sorted[-1]
        expected = set(range(min_y, max_y + 1))
        gaps = sorted(expected - set(reg_years_sorted))

        registers_out.append(
            {
                "register_id": rid,
                "register_name": var["register_name"],
                "var_id": vid,
                "min_year": min_y,
                "max_year": max_y,
                "years": reg_years_sorted,
                "gaps": gaps,
                "aliases_by_year": aliases_by_year,
            }
        )

    if not registers_out:
        return None

    all_years_sorted = sorted(all_years)
    min_y = all_years_sorted[0]
    max_y = all_years_sorted[-1]
    expected = set(range(min_y, max_y + 1))
    gaps = sorted(expected - all_years)

    return {
        "target": variable,
        "target_type": "variable",
        "variable_name": var_rows[0]["variable_name"],
        "min_year": min_y,
        "max_year": max_y,
        "years": all_years_sorted,
        "gaps": gaps,
        "register_count": len(registers_out),
        "registers": registers_out,
    }


def _get_availability_register(
    conn: sqlite3.Connection,
    register: str,
) -> dict[str, Any] | None:
    """Availability for a register across years."""
    ids = resolve_register_ids(conn, register)
    if not ids:
        return None

    # Use first match
    reg_id = ids[0]
    reg = conn.execute(
        "SELECT name FROM register WHERE register_id = ?", (reg_id,)
    ).fetchone()

    # A2.6: year coverage per variant comes from `variable_state` validity
    # windows (register_version is dropped before ship).
    rows = conn.execute(
        "SELECT rvar.register_variant_id, rvar.name AS variant_name, "
        "vs.valid_from, vs.valid_to "
        "FROM register_variant rvar "
        "JOIN variable_state vs ON vs.register_variant_id = rvar.register_variant_id "
        "WHERE rvar.register_id = ? "
        "ORDER BY rvar.register_variant_id, vs.valid_from",
        (reg_id,),
    ).fetchall()

    all_years: set[int] = set()
    variants: dict[int, dict[str, Any]] = {}

    for row in rows:
        rvid = row["register_variant_id"]
        for year in _years_in_range(row["valid_from"], row["valid_to"]):
            all_years.add(year)
            if rvid not in variants:
                variants[rvid] = {
                    "register_variant_id": rvid,
                    "variant_name": row["variant_name"],
                    "years": [],
                }
            variants[rvid]["years"].append(year)

    for v in variants.values():
        v["years"] = sorted(set(v["years"]))

    if not all_years:
        return None

    all_years_sorted = sorted(all_years)
    min_y, max_y = all_years_sorted[0], all_years_sorted[-1]
    expected = set(range(min_y, max_y + 1))
    gaps = sorted(expected - all_years)

    return {
        "target": register,
        "target_type": "register",
        "register_id": reg_id,
        "register_name": reg["name"],
        "min_year": min_y,
        "max_year": max_y,
        "years": all_years_sorted,
        "gaps": gaps,
        "variant_count": len(variants),
        "variants": list(variants.values()),
    }


# ---------------------------------------------------------------------------
# Get values
# ---------------------------------------------------------------------------


def get_values_by_variable(
    conn: sqlite3.Connection,
    variable: str,
    *,
    register: str | None = None,
    year: int | None = None,
) -> dict[str, Any]:
    """Resolve a variable to its states and return year-correct codes per state.

    A2.6: each "instance" is one `variable_state` → year-correct value list (the
    state's `value_set_id`). Filter via ``register`` and/or ``year``. Returns
    ``{input, variable_name, instances: [{state_id, register_id, register_name,
    register_variant_id, variant_name, valid_from, valid_to, year, values}]}``.
    Resolution mirrors ``get_varinfo``: var_id → variable name → alias.
    Keys follow the §5.11 rename (`variabelnamn` → `variable_name`).
    """
    reg_ids: list[int] | None = None
    if register:
        reg_ids = require_register_ids(conn, register)

    int_variable: int | None
    raw_int = _try_int(variable)
    int_variable = raw_int if isinstance(raw_int, int) else None

    # Carry `variable_id` from the match: it's the unique per-variable key the
    # state query filters by. `provider_key` (= var_id) is NON-unique after an
    # A2.2 split, so selecting states by it would merge sibling value sets.
    rows_by_id: list[Any] = []
    if reg_ids:
        ph = _in_placeholders(reg_ids)
        if int_variable is not None:
            rows_by_id = conn.execute(
                f"SELECT variable_id, register_id, CAST(provider_key AS INTEGER) AS var_id, name FROM variable "
                f"WHERE provider_key = CAST(? AS TEXT) AND register_id IN ({ph})",
                [int_variable, *reg_ids],
            ).fetchall()
        rows_by_name = conn.execute(
            f"SELECT variable_id, register_id, CAST(provider_key AS INTEGER) AS var_id, name FROM variable "
            f"WHERE LOWER(name) = LOWER(?) AND register_id IN ({ph})",
            [variable, *reg_ids],
        ).fetchall()
    else:
        if int_variable is not None:
            rows_by_id = conn.execute(
                "SELECT variable_id, register_id, CAST(provider_key AS INTEGER) AS var_id, name FROM variable WHERE provider_key = CAST(? AS TEXT)",
                (int_variable,),
            ).fetchall()
        rows_by_name = conn.execute(
            "SELECT variable_id, register_id, CAST(provider_key AS INTEGER) AS var_id, name FROM variable "
            "WHERE LOWER(name) = LOWER(?)",
            (variable,),
        ).fetchall()

    matched = rows_by_id or rows_by_name

    if not matched:
        # A2.7: `variable_alias` is variable_id-keyed; join straight to `variable`.
        alias_sql = (
            "SELECT DISTINCT v.variable_id, v.register_id, CAST(v.provider_key AS INTEGER) AS var_id, v.name "
            "FROM variable_alias a "
            "JOIN variable v ON a.variable_id = v.variable_id "
            "WHERE LOWER(a.delivery_column_name) = LOWER(?)"
        )
        if reg_ids:
            ph = _in_placeholders(reg_ids)
            alias_sql += f" AND v.register_id IN ({ph})"
            matched = conn.execute(alias_sql, [variable, *reg_ids]).fetchall()
        else:
            matched = conn.execute(alias_sql, (variable,)).fetchall()

        # Generic column aliases (e.g. "Rad", "Kolumn1", "OBS_VALUE") map to
        # many unrelated variables. Refuse to silently merge their value sets
        # under one name — surface the spread so the caller can pick.
        distinct_names = {m["name"] for m in matched}
        if len(distinct_names) > 1:
            sample = ", ".join(sorted(distinct_names)[:5])
            more = (
                f" (+{len(distinct_names) - 5} more)" if len(distinct_names) > 5 else ""
            )
            raise RegMetaError(
                exit_code=EXIT_USAGE,
                code="ambiguous_alias",
                error_class="usage",
                message=(
                    f"Column alias '{variable}' maps to "
                    f"{len(distinct_names)} distinct variables: {sample}{more}."
                ),
                remediation=(
                    f"Run `reg-meta get datacolumns {variable}` to see the spread, "
                    "then call `reg-meta get values <name> --register R`."
                ),
            )

    if not matched:
        raise RegMetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"No variable matching '{variable}'"
            + (f" in register '{register}'" if register else "")
            + ".",
            remediation="Use `reg-meta search --query <term>` to find variables.",
        )

    variable_name = matched[0]["name"]

    # A2.6: "instances" are `variable_state` rows now (year from validity
    # window), not per-cvid `variable_instance` × `register_version` rows. Codes
    # come from the state's `value_set_id` (the same year-projected set the
    # coalescer assigned). Batched across all matched `variable_id`s to avoid the
    # N+1 pattern when a variable spans dozens of registers.
    #
    # Filter by `variable_id`, NOT `(register_id, provider_key)`: `provider_key`
    # is NON-unique after an A2.2 split (siblings share one source key), so a
    # provider_key filter would merge every sibling's value sets under one name.
    variable_ids = [var["variable_id"] for var in matched]
    vid_ph = _in_placeholders(variable_ids)

    state_rows = conn.execute(
        f"SELECT vs.state_id, vs.value_set_id, vs.valid_from, vs.valid_to, "
        f"vs.variable_id, v.slug AS variable_slug, "
        f"v.register_id, CAST(v.provider_key AS INTEGER) AS var_id, "
        f"vs.register_variant_id, r.name AS register_name, rv.name AS variant_name "
        f"FROM variable_state vs "
        f"JOIN variable v ON vs.variable_id = v.variable_id "
        f"JOIN register r ON v.register_id = r.register_id "
        f"JOIN register_variant rv ON vs.register_variant_id = rv.register_variant_id "
        f"WHERE vs.variable_id IN ({vid_ph})",
        variable_ids,
    ).fetchall()

    instances: list[dict[str, Any]] = []
    # Group code rows by value_set_id; a state's `values` is its set's codes.
    by_value_set: dict[int, list[dict[str, Any]]] = {}
    for row in state_rows:
        # A2.6: a state matches the requested `year` when its validity window
        # COVERS that year (overlap), not only when the window opens in it — a
        # coalesced multi-year state (e.g. 2020-01-01..2021-12-31) must answer a
        # `--year 2021` query. `inst_year` (the opening year) stays for display.
        if year is not None and not _state_covers_year(
            row["valid_from"], row["valid_to"], year
        ):
            continue
        inst_year = int(row["valid_from"][:4])
        inst = {
            "state_id": row["state_id"],
            # A2.7: attribute each instance to its owning variable. A numeric
            # var_id can map to >1 A2.2 split sibling (same provider_key, distinct
            # variable_id/slug) with differing value sets — carrying the slug lets
            # the caller tell them apart instead of seeing them merged under one
            # name (Codex P2 #149).
            "variable_id": row["variable_id"],
            "variable_slug": row["variable_slug"],
            "register_id": row["register_id"],
            "register_name": row["register_name"],
            "register_variant_id": row["register_variant_id"],
            "variant_name": row["variant_name"],
            "valid_from": row["valid_from"],
            "valid_to": row["valid_to"],
            "year": inst_year,
            "values": [],
        }
        instances.append(inst)
        if row["value_set_id"] is not None:
            by_value_set.setdefault(row["value_set_id"], []).append(inst)

    if by_value_set:
        vs_ph = _in_placeholders(list(by_value_set))
        codes_by_set: dict[int, list[dict[str, Any]]] = {}
        for row in conn.execute(
            f"SELECT vsm.value_set_id, vc.code, vc.label "
            f"FROM value_set_member vsm "
            f"JOIN value_code vc ON vsm.code_id = vc.code_id "
            f"WHERE vsm.value_set_id IN ({vs_ph}) "
            f"ORDER BY vsm.value_set_id, vc.code",
            list(by_value_set),
        ):
            codes_by_set.setdefault(row["value_set_id"], []).append(
                {"code": row["code"], "label": row["label"]}
            )
        for vsid, insts in by_value_set.items():
            for inst in insts:
                inst["values"] = list(codes_by_set.get(vsid, []))

    instances.sort(
        key=lambda i: (i["register_name"] or "", i["year"] or 0, i["state_id"])
    )

    return {
        "input": variable,
        # §5.11: surface as `variable_name` (variable.name with the entity
        # qualifier so consumers don't confuse it with register.name).
        "variable_name": variable_name,
        "instances": instances,
    }


# ---------------------------------------------------------------------------
# Resolve
# ---------------------------------------------------------------------------


def get_datacolumns(
    conn: sqlite3.Connection,
    variable: str,
    *,
    register: str | None = None,
) -> list[dict[str, Any]]:
    """Get all delivery-column aliases for a variable.

    A2.7: sourced from `variable_alias` — the FULL delivery-column history,
    re-parented onto `variable_id` (was cvid-keyed through A2.6) and joined
    straight to `variable`. The full history is the right source here; the
    coalesced `variable_state.delivery_column_name` keeps only the denormalized
    latest era. Returns a list of dicts with "delivery_column_name",
    "register_id", "register_name", "register_variant_id". Keys follow the §5.11
    rename (`kolumnnamn` → `delivery_column_name`).

    Filters the alias rows by the matched `variable_id` (NOT the non-unique
    `(register_id, provider_key)`): an A2.2 split sibling has its own
    `variable_id`, so each sibling surfaces only its own columns.
    """
    reg_ids: list[int] | None = None
    if register:
        reg_ids = require_register_ids(conn, register)

    # Match by var_id or variable name (§5.11: was `variabelnamn`). Carry
    # `variable_id` — the unique key the re-parented `variable_alias` filters by.
    int_variable = _try_int(variable)
    if reg_ids:
        ph = _in_placeholders(reg_ids)
        var_rows = conn.execute(
            f"SELECT variable_id, register_id, CAST(provider_key AS INTEGER) AS var_id FROM variable "
            f"WHERE (provider_key = CAST(? AS TEXT) OR LOWER(name) = LOWER(?)) "
            f"AND register_id IN ({ph})",
            [int_variable, variable, *reg_ids],
        ).fetchall()
    else:
        var_rows = conn.execute(
            "SELECT variable_id, register_id, CAST(provider_key AS INTEGER) AS var_id FROM variable "
            "WHERE provider_key = CAST(? AS TEXT) OR LOWER(name) = LOWER(?)",
            (int_variable, variable),
        ).fetchall()

    if not var_rows:
        raise RegMetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"No variable matching '{variable}'"
            + (f" in register '{register}'" if register else "")
            + ".",
            remediation="Use `reg-meta search` to find variable names or IDs.",
        )

    results: list[dict[str, Any]] = []
    seen: set[str] = set()
    for vr in var_rows:
        rows = conn.execute(
            "SELECT DISTINCT va.delivery_column_name, "
            "v.register_id, va.register_variant_id, r.name AS register_name "
            "FROM variable_alias va "
            "JOIN variable v ON va.variable_id = v.variable_id "
            "JOIN register r ON v.register_id = r.register_id "
            "WHERE va.variable_id = ? "
            "ORDER BY va.delivery_column_name, va.register_variant_id",
            (vr["variable_id"],),
        ).fetchall()
        for r in rows:
            key = (
                f"{r['delivery_column_name']}:{r['register_id']}:"
                f"{r['register_variant_id']}"
            )
            if key in seen:
                continue
            seen.add(key)
            results.append(
                {
                    # §5.11: SCB `kolumnnamn` → universal
                    # `delivery_column_name`.
                    "delivery_column_name": r["delivery_column_name"],
                    "register_id": r["register_id"],
                    "register_name": r["register_name"],
                    "register_variant_id": r["register_variant_id"],
                }
            )

    return results


# ---------------------------------------------------------------------------
# Get diff
# ---------------------------------------------------------------------------


def _columns_at_year(
    conn: sqlite3.Connection, register_variant_id: int, year: int
) -> dict[int, dict[str, Any]]:
    """A2.6: the variant's columns active at a calendar `year`, keyed by var_id.

    Sourced from `variable_state` validity windows (register_version is dropped
    before ship): a state is active at `year` when `valid_from`..`valid_to`
    overlaps that calendar year. Returns an empty dict when the variant has no
    state covering the year (the caller treats that like "version absent").

    Keyed by `var_id` (NOT `variable_id`) on purpose: `get_diff` is a
    GROUP-level schema diff whose public contract diffs a register's columns by
    `var_id` (from/to var_id set intersection), so this collapses to one row per
    var_id. After an A2.2 split several siblings share a var_id; if more than one
    is active in the same (variant, year) we take the lexically-smallest delivery
    column for determinism — the diff under-reports the extra siblings as one
    column rather than leaking them onto an unrelated single variable. That is an
    accepted imprecision of var_id-grained diffing, distinct from the
    matched-variable sibling leak the get_varinfo / get_values fixes address.
    """
    iso_lo = f"{year:04d}-12-31"  # any state starting on/before year-end ...
    iso_hi = f"{year:04d}-01-01"  # ... and ending on/after year-start overlaps
    rows = conn.execute(
        "SELECT CAST(v.provider_key AS INTEGER) AS var_id, vs.data_type, "
        "vs.data_length, v.name AS variable_name, vs.delivery_column_name "
        "FROM variable_state vs "
        "JOIN variable v ON vs.variable_id = v.variable_id "
        "WHERE vs.register_variant_id = ? "
        "AND vs.valid_from <= ? AND vs.valid_to >= ? "
        "ORDER BY v.provider_key, vs.delivery_column_name",
        (register_variant_id, iso_lo, iso_hi),
    ).fetchall()
    result: dict[int, dict[str, Any]] = {}
    for r in rows:
        if r["var_id"] in result:
            continue  # first (lex-smallest column) wins per var_id
        col = r["delivery_column_name"]
        result[r["var_id"]] = {
            "var_id": r["var_id"],
            "variable_name": r["variable_name"],
            "data_type": r["data_type"],
            "data_length": r["data_length"],
            "aliases": [col] if col else [],
        }
    return result


def get_diff(
    conn: sqlite3.Connection,
    *,
    register: str,
    from_year: int,
    to_year: int,
    variant: str | None = None,
    variables: list[str] | None = None,
) -> dict[str, Any]:
    """Compare a register's schema between two years."""
    reg_ids = require_register_ids(conn, register)

    reg = conn.execute(
        "SELECT register_id, name FROM register WHERE register_id = ?",
        (reg_ids[0],),
    ).fetchone()

    if variant:
        variant_rows = conn.execute(
            f"SELECT * FROM register_variant WHERE register_id IN ({_in_placeholders(reg_ids)}) "
            "AND register_variant_id = ?",
            [*reg_ids, _try_int(variant)],
        ).fetchall()
    else:
        variant_rows = conn.execute(
            f"SELECT * FROM register_variant WHERE register_id IN ({_in_placeholders(reg_ids)}) "
            "ORDER BY register_variant_id",
            reg_ids,
        ).fetchall()

    # Resolve each variable input to var_ids, tracking name mapping
    filter_var_ids: set[int] | None = None
    var_id_to_name: dict[int, str] = {}
    var_id_to_input: dict[int, str] = {}
    if variables:
        filter_var_ids = set()
        ph = _in_placeholders(reg_ids)
        for v in variables:
            rows = conn.execute(
                f"SELECT CAST(provider_key AS INTEGER) AS var_id, name FROM variable "
                f"WHERE (provider_key = CAST(? AS TEXT) OR LOWER(name) = LOWER(?)) "
                f"AND register_id IN ({ph})",
                [_try_int(v), v, *reg_ids],
            ).fetchall()
            if not rows:
                # A2.7: `variable_alias` is variable_id-keyed; join straight to
                # `variable`. `var_id` is the variable's `provider_key`.
                rows = conn.execute(
                    f"SELECT DISTINCT CAST(var.provider_key AS INTEGER) AS var_id, var.name "
                    f"FROM variable_alias va "
                    f"JOIN variable var ON va.variable_id = var.variable_id "
                    f"WHERE LOWER(va.delivery_column_name) = LOWER(?) AND var.register_id IN ({ph})",
                    [v, *reg_ids],
                ).fetchall()
            for r in rows:
                filter_var_ids.add(r["var_id"])
                var_id_to_name[r["var_id"]] = r["name"]
                var_id_to_input[r["var_id"]] = v

        if not filter_var_ids:
            names = ", ".join(f"'{v}'" for v in variables)
            raise RegMetaError(
                exit_code=EXIT_NOT_FOUND,
                code="not_found",
                error_class="query",
                message=f"No variables matching {names} in register '{register}'.",
                remediation="Use `reg-meta search --query <term>` to find variables.",
            )

    variants_out: list[dict[str, Any]] = []
    unchanged_by_var: dict[int, list[str]] = {}
    changed_any_variant: set[int] = set()
    any_versions_found = False

    for rv in variant_rows:
        rvid = rv["register_variant_id"]
        # A2.6: the columns active in each year come from `variable_state`
        # validity windows (register_version is dropped before ship). A variant
        # with no state covering a year contributes nothing — same skip as the
        # old "version absent" branch.
        from_cols = _columns_at_year(conn, rvid, from_year)
        to_cols = _columns_at_year(conn, rvid, to_year)
        if not from_cols or not to_cols:
            continue
        any_versions_found = True

        from_ids = set(from_cols)
        to_ids = set(to_cols)

        added_ids = to_ids - from_ids
        removed_ids = from_ids - to_ids
        common_ids = from_ids & to_ids

        added = [to_cols[vid] for vid in sorted(added_ids)]
        removed = [from_cols[vid] for vid in sorted(removed_ids)]
        changed: list[dict[str, Any]] = []
        unchanged_count = 0

        for vid in sorted(common_ids):
            fc, tc = from_cols[vid], to_cols[vid]
            changes: list[dict[str, Any]] = []
            for field in ("data_type", "data_length", "aliases"):
                if fc[field] != tc[field]:
                    changes.append({"field": field, "from": fc[field], "to": tc[field]})
            if changes:
                changed.append(
                    {
                        "var_id": vid,
                        "variable_name": tc["variable_name"],
                        "changes": changes,
                    }
                )
            else:
                unchanged_count += 1

        if filter_var_ids is not None:
            changed_var_ids = (
                {a["var_id"] for a in added}
                | {r["var_id"] for r in removed}
                | {c["var_id"] for c in changed}
            ) & filter_var_ids
            changed_any_variant.update(changed_var_ids)
            for vid in filter_var_ids - changed_var_ids:
                if vid in from_ids or vid in to_ids:
                    # `name` is the §5.11 rename of
                    # `registervariantnamn` on register_variant.
                    unchanged_by_var.setdefault(vid, []).append(rv["name"])

            added = [a for a in added if a["var_id"] in filter_var_ids]
            removed = [r for r in removed if r["var_id"] in filter_var_ids]
            changed = [c for c in changed if c["var_id"] in filter_var_ids]

        if not added and not removed and not changed:
            continue

        variants_out.append(
            {
                "register_variant_id": rvid,
                "variant_name": rv["name"],
                # A2.6: the diff is year-keyed (no register_version rows to name).
                "from_year": from_year,
                "to_year": to_year,
                "summary": {
                    "added": len(added),
                    "removed": len(removed),
                    "changed": len(changed),
                    "unchanged": unchanged_count,
                },
                "added": added,
                "removed": removed,
                "changed": changed,
            }
        )

    if not any_versions_found:
        raise RegMetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"No versions found for register '{register}' between years {from_year} and {to_year}.",
            remediation="Use `reg-meta get schema --register <name>` to see available versions.",
        )

    result: dict[str, Any] = {
        "register_id": reg["register_id"],
        "register_name": reg["name"],
        "from_year": from_year,
        "to_year": to_year,
        "variants": variants_out,
    }
    if var_id_to_input:
        result["resolved_variables"] = [
            {
                "input": var_id_to_input[vid],
                "variable_name": var_id_to_name[vid],
                "var_id": vid,
            }
            for vid in sorted(var_id_to_name)
        ]
    fully_unchanged = [
        var_id_to_name[vid]
        for vid in sorted(unchanged_by_var)
        if vid not in changed_any_variant
    ]
    if fully_unchanged:
        result["unchanged"] = fully_unchanged
    return result


# ---------------------------------------------------------------------------
# Get lineage
# ---------------------------------------------------------------------------


def get_lineage(
    conn: sqlite3.Connection,
    variable: str,
    *,
    register: str | None = None,
) -> dict[str, Any]:
    """Show cross-register variable provenance."""
    reg_ids: list[int] | None = None
    if register:
        reg_ids = require_register_ids(conn, register)

    int_variable = _try_int(variable)
    if reg_ids:
        ph = _in_placeholders(reg_ids)
        matched = conn.execute(
            f"SELECT v.*, CAST(v.provider_key AS INTEGER) AS var_id, r.name AS register_name FROM variable v "
            f"JOIN register r ON v.register_id = r.register_id "
            f"WHERE (v.provider_key = CAST(? AS TEXT) OR LOWER(v.name) = LOWER(?)) "
            f"AND v.register_id IN ({ph})",
            [int_variable, variable, *reg_ids],
        ).fetchall()
    else:
        matched = conn.execute(
            "SELECT v.*, CAST(v.provider_key AS INTEGER) AS var_id, r.name AS register_name FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "WHERE v.provider_key = CAST(? AS TEXT) OR LOWER(v.name) = LOWER(?)",
            (int_variable, variable),
        ).fetchall()

    if not matched:
        raise RegMetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"No variable matching '{variable}'"
            + (f" in register '{register}'" if register else "")
            + ".",
            remediation="Use `reg-meta search --query <term>` to find variables.",
        )

    registers_out: list[dict[str, Any]] = []
    total_instances = 0
    with_source = 0

    for var in matched:
        rid, vid = var["register_id"], var["var_id"]
        variable_id = var["variable_id"]
        # §5.11 drop: `variabelhamtadfran` is no longer ingested. Lineage
        # role detection now keys solely on `source_register_text` (the
        # renamed `variabelregister_kalla`); the auxiliary `hamtad` text
        # carried no orthogonal signal in practice and its disposition is
        # "(dropped)" per §5.11.
        kalla = (var["source_register_text"] or "").strip()
        source_register_id = var["source_register_id"]

        # Classify role
        if not kalla:
            role = "unknown"
        elif source_register_id != rid:
            role = "consumer"
        else:
            role = "source"

        # A2.6: state count + year range from `variable_state` (register_version
        # is dropped before ship). `instance_count` counts states now (the
        # per-delivery shape), not per-cvid `variable_instance` rows.
        #
        # Select by the matched `variable_id`, NOT `(register_id, provider_key)`:
        # `matched` already yields one row per split sibling (each with its own
        # `variable_id` / role), so the state count must be per-sibling — a
        # provider_key filter is NON-unique post-split and would sum siblings.
        states = conn.execute(
            "SELECT vs.valid_from, vs.valid_to FROM variable_state vs "
            "WHERE vs.variable_id = ?",
            (variable_id,),
        ).fetchall()

        instance_count = len(states)
        years = [
            y for s in states for y in _years_in_range(s["valid_from"], s["valid_to"])
        ]
        year_range = [min(years), max(years)] if years else []

        total_instances += instance_count
        if kalla:
            with_source += instance_count

        registers_out.append(
            {
                "register_id": rid,
                "register_name": var["register_name"],
                "var_id": vid,
                "role": role,
                # §5.11 rename: surface SCB's raw attribution under the universal
                # English key. `variabelhamtadfran` was dropped at §5.11; lineage
                # signal collapsed onto `source_register_text` alone.
                "source_register_text": kalla,
                "source_register_id": source_register_id,
                "instance_count": instance_count,
                "year_range": year_range,
            }
        )

    var_name = matched[0]["name"]

    return {
        "variable_name": var_name,
        "occurrences": total_instances,
        "registers": registers_out,
        "provenance_coverage": {
            "total": total_instances,
            "with_source": with_source,
            "without_source": total_instances - with_source,
        },
    }


# ---------------------------------------------------------------------------
# Resolve
# ---------------------------------------------------------------------------


def get_coded_variables(
    conn: sqlite3.Connection,
    *,
    min_codes: int = 1,
    min_registers: int = 1,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Find variables that have value sets, ranked by usage.

    Returns a list of dicts with "variable_name", "n_distinct_codes",
    "n_registers", "n_instances".

    A2.7: sourced from `variable_state` (was per-cvid `variable_instance`).
    `n_instances` counts distinct states now — the per-era shape is the unit the
    shipped DB carries.
    """
    rows = conn.execute(
        "SELECT v.name AS variable_name, "
        "COUNT(DISTINCT vc.code) as n_distinct_codes, "
        "COUNT(DISTINCT v.register_id) as n_registers, "
        "COUNT(DISTINCT vs.state_id) as n_instances "
        "FROM variable v "
        "JOIN variable_state vs ON vs.variable_id = v.variable_id "
        "JOIN value_set_member vsm ON vs.value_set_id = vsm.value_set_id "
        "JOIN value_code vc ON vsm.code_id = vc.code_id "
        "GROUP BY v.name "
        "HAVING n_distinct_codes >= ? AND n_registers >= ? "
        "ORDER BY n_registers DESC, n_distinct_codes DESC "
        "LIMIT ?",
        (min_codes, min_registers, limit),
    ).fetchall()
    return [
        {
            "variable_name": r["variable_name"],
            "n_distinct_codes": r["n_distinct_codes"],
            "n_registers": r["n_registers"],
            "n_instances": r["n_instances"],
        }
        for r in rows
    ]


def resolve(
    conn: sqlite3.Connection,
    columns: list[str],
    *,
    register: str | None = None,
) -> list[dict[str, Any]]:
    """Resolve column names to variables via exact alias lookup.

    Returns a list of dicts, one per input column, each with
    "column_name", "status", and "matches" keys.
    """
    reg_ids: list[int] | None = None
    if register:
        reg_ids = require_register_ids(conn, register)

    results: list[dict[str, Any]] = []

    for col in columns:
        col_lower = col.lower()

        # A2.7: `variable_alias` is variable_id-keyed; join straight to
        # `variable`. `var_id` is the variable's `provider_key`.
        if reg_ids:
            ph = _in_placeholders(reg_ids)
            exact_rows = conn.execute(
                f"SELECT va.delivery_column_name, v.register_id, "
                f"CAST(v.provider_key AS INTEGER) AS var_id, v.name AS variable_name "
                f"FROM variable_alias va "
                f"JOIN variable v ON va.variable_id = v.variable_id "
                f"WHERE LOWER(va.delivery_column_name) = ? AND v.register_id IN ({ph}) "
                f"GROUP BY v.register_id, v.provider_key "
                f"ORDER BY v.register_id, v.provider_key",
                [col_lower, *reg_ids],
            ).fetchall()
        else:
            exact_rows = conn.execute(
                "SELECT va.delivery_column_name, v.register_id, "
                "CAST(v.provider_key AS INTEGER) AS var_id, v.name AS variable_name "
                "FROM variable_alias va "
                "JOIN variable v ON va.variable_id = v.variable_id "
                "WHERE LOWER(va.delivery_column_name) = ? "
                "GROUP BY v.register_id, v.provider_key "
                "ORDER BY v.register_id, v.provider_key",
                (col_lower,),
            ).fetchall()

        matches = [
            {
                "var_id": r["var_id"],
                "variable_name": r["variable_name"],
                "matched_column": r["delivery_column_name"],
                "register_id": r["register_id"],
            }
            for r in exact_rows
        ]

        results.append(
            {
                "column_name": col,
                "status": "matched" if matches else "no_match",
                "matches": matches,
            }
        )

    return results


# ---------------------------------------------------------------------------
# Compare
# ---------------------------------------------------------------------------


def compare(
    conn: sqlite3.Connection,
    *,
    columns_by_file: dict[str, list[str]],
    register_hints: dict[str, int | None] | None = None,
    year_hints: dict[str, int | None] | None = None,
) -> dict[str, Any]:
    """Compare local file columns against registry metadata.

    For each file (keyed by label), resolves the register (from hint or
    explicit), retrieves the registry schema, and classifies columns as
    matched, extra_local, or missing_from_registry.
    """
    register_hints = register_hints or {}
    year_hints = year_hints or {}

    files_out: list[dict[str, Any]] = []

    for file_label, local_columns in columns_by_file.items():
        reg_hint = register_hints.get(file_label)
        year_hint = year_hints.get(file_label)

        if reg_hint is None:
            files_out.append(
                {
                    "file": file_label,
                    "register_id": None,
                    "register_name": None,
                    "register_status": "no_hint",
                    "year_hint": year_hint,
                    "matched": [],
                    "extra_local": local_columns,
                    "missing_from_registry": [],
                    "summary": {
                        "matched": 0,
                        "extra_local": len(local_columns),
                        "missing_from_registry": 0,
                    },
                }
            )
            continue

        # Resolve register_id to register name
        reg_row = conn.execute(
            "SELECT name FROM register WHERE register_id = ?", (reg_hint,)
        ).fetchone()
        if not reg_row:
            files_out.append(
                {
                    "file": file_label,
                    "register_id": reg_hint,
                    "register_name": None,
                    "register_status": "not_found",
                    "year_hint": year_hint,
                    "matched": [],
                    "extra_local": local_columns,
                    "missing_from_registry": [],
                    "summary": {
                        "matched": 0,
                        "extra_local": len(local_columns),
                        "missing_from_registry": 0,
                    },
                }
            )
            continue

        register_name = reg_row["name"]

        # Get schema for this register, optionally filtered by year
        years_arg = str(year_hint) if year_hint else None
        try:
            schema = get_schema(conn, register=str(reg_hint), years=years_arg)
        except RegMetaError:
            files_out.append(
                {
                    "file": file_label,
                    "register_id": reg_hint,
                    "register_name": register_name,
                    "register_status": "no_schema",
                    "year_hint": year_hint,
                    "matched": [],
                    "extra_local": local_columns,
                    "missing_from_registry": [],
                    "summary": {
                        "matched": 0,
                        "extra_local": len(local_columns),
                        "missing_from_registry": 0,
                    },
                }
            )
            continue

        # Flatten schema: build alias→variable mapping
        alias_to_var: dict[str, dict[str, Any]] = {}
        all_registry_vars: dict[int, dict[str, Any]] = {}
        for variant in schema.get("variants", []):
            for version in variant.get("versions", []):
                for col in version.get("columns", []):
                    vid = col["var_id"]
                    vname = col["variable_name"]
                    aliases_str = col.get("aliases") or ""
                    aliases = [a.strip() for a in aliases_str.split(",") if a.strip()]

                    var_info = {
                        "var_id": vid,
                        "variable_name": vname,
                        "aliases": aliases,
                    }
                    all_registry_vars[vid] = var_info

                    for alias in aliases:
                        alias_to_var[alias.lower()] = var_info
                    alias_to_var[vname.lower()] = var_info

        # Classify local columns
        matched = []
        extra_local = []
        matched_var_ids: set[int] = set()
        local_lower = set()

        for col in local_columns:
            col_lower = col.lower()
            local_lower.add(col_lower)
            var_info = alias_to_var.get(col_lower)
            if var_info:
                matched.append(
                    {
                        "column": col,
                        "var_id": var_info["var_id"],
                        "variable_name": var_info["variable_name"],
                    }
                )
                matched_var_ids.add(var_info["var_id"])
            else:
                extra_local.append(col)

        # Registry variables not in local columns
        missing_from_registry = []
        for vid, var_info in sorted(all_registry_vars.items()):
            if vid in matched_var_ids:
                continue
            if any(a.lower() in local_lower for a in var_info["aliases"]):
                continue
            if var_info["variable_name"].lower() in local_lower:
                continue
            missing_from_registry.append(
                {
                    "var_id": var_info["var_id"],
                    "variable_name": var_info["variable_name"],
                    "aliases": var_info["aliases"],
                }
            )

        files_out.append(
            {
                "file": file_label,
                "register_id": reg_hint,
                "register_name": register_name,
                "register_status": "resolved",
                "year_hint": year_hint,
                "matched": matched,
                "extra_local": extra_local,
                "missing_from_registry": missing_from_registry,
                "summary": {
                    "matched": len(matched),
                    "extra_local": len(extra_local),
                    "missing_from_registry": len(missing_from_registry),
                },
            }
        )

    return {"files": files_out}


# ---------------------------------------------------------------------------
# Classifications
# ---------------------------------------------------------------------------


def _classification_row(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    fqid = try_emit(Fqid.classification_fqid, d.get("slug"))
    # Drop NULL fields to keep JSON output lean.
    out = {k: v for k, v in d.items() if v is not None}
    if fqid:
        out["fqid"] = fqid
    return out


def list_classifications(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Enumerate all classifications with a superseded_by back-pointer.

    superseded_by uses a scalar GROUP_CONCAT subquery rather than a LEFT JOIN
    on supersedes_id: a classification can be superseded by more than one
    successor (the schema doesn't enforce 1:1), and a JOIN would multiply
    the parent row. The result is comma-separated when there are multiple.
    """
    rows = conn.execute(
        """
        SELECT c.id, c.short_name, c.slug, c.name, c.name_en, c.publisher,
               c.valid_from, c.valid_to, c.description, c.url, c.code_count,
               c.valid_code_count,
               s.short_name AS supersedes,
               (SELECT GROUP_CONCAT(short_name, ',')
                FROM (SELECT short_name FROM classification
                      WHERE supersedes_id = c.id ORDER BY short_name)) AS superseded_by
        FROM classification c
        LEFT JOIN classification s ON c.supersedes_id = s.id
        ORDER BY c.short_name
        """
    ).fetchall()
    return [_classification_row(r) for r in rows]


def _resolve_classification_id(conn: sqlite3.Connection, value: str) -> int:
    """Resolve a classification by id, short_name (case-insensitive), or substring."""
    int_value = _try_int(value)
    if isinstance(int_value, int):
        row = conn.execute(
            "SELECT id FROM classification WHERE id = ?", (int_value,)
        ).fetchone()
        if row:
            return row["id"]

    row = conn.execute(
        "SELECT id FROM classification WHERE LOWER(short_name) = LOWER(?)",
        (value,),
    ).fetchone()
    if row:
        return row["id"]

    rows = conn.execute(
        "SELECT id, short_name FROM classification "
        "WHERE LOWER(short_name) LIKE '%' || LOWER(?) || '%' "
        "   OR LOWER(name) LIKE '%' || LOWER(?) || '%' "
        "ORDER BY short_name",
        (value, value),
    ).fetchall()
    if len(rows) == 1:
        return rows[0]["id"]
    if len(rows) > 1:
        candidates = ", ".join(r["short_name"] for r in rows)
        raise RegMetaError(
            exit_code=EXIT_NOT_FOUND,
            code="ambiguous",
            error_class="query",
            message=(f"Classification {value!r} is ambiguous: matches {candidates}."),
            remediation="Use the exact short_name.",
        )
    raise RegMetaError(
        exit_code=EXIT_NOT_FOUND,
        code="not_found",
        error_class="query",
        message=f"No classification matching '{value}'.",
        remediation="Use `reg-meta get classification --list` to see available classifications.",
    )


def _classification_by_id(conn: sqlite3.Connection, cls_id: int) -> dict[str, Any]:
    # Scalar GROUP_CONCAT for superseded_by — see list_classifications for why.
    row = conn.execute(
        """
        SELECT c.*, s.short_name AS supersedes,
               (SELECT GROUP_CONCAT(short_name, ',')
                FROM (SELECT short_name FROM classification
                      WHERE supersedes_id = c.id ORDER BY short_name)) AS superseded_by
        FROM classification c
        LEFT JOIN classification s ON c.supersedes_id = s.id
        WHERE c.id = ?
        """,
        (cls_id,),
    ).fetchone()
    data = _classification_row(row)
    # supersedes_id is an internal FK; supersedes short_name is the useful form.
    data.pop("supersedes_id", None)
    return data


def get_classification(conn: sqlite3.Connection, identifier: str) -> dict[str, Any]:
    """Return one classification's metadata (no codes)."""
    return _classification_by_id(conn, _resolve_classification_id(conn, identifier))


def get_classification_codes(
    conn: sqlite3.Connection,
    identifier: str,
    *,
    level: int | None = None,
    only_valid: bool = False,
) -> dict[str, Any]:
    """Return a classification plus its full code list (optionally filtered).

    With ``only_valid=True`` the result only includes codes flagged as
    canonical (``is_valid=1``). Classifications without a canonical CSV have
    ``is_valid=NULL`` everywhere; ``only_valid`` will return zero codes for
    them, which is the correct semantics ("no canonical list available").
    """
    cls_id = _resolve_classification_id(conn, identifier)
    meta = _classification_by_id(conn, cls_id)

    sql = (
        "SELECT vc.code, vc.label, cc.level, cc.is_valid "
        "FROM classification_code cc "
        "JOIN value_code vc ON cc.code_id = vc.code_id "
        "WHERE cc.classification_id = ?"
    )
    params: list[Any] = [cls_id]
    if level is not None:
        sql += " AND cc.level = ?"
        params.append(level)
    if only_valid:
        sql += " AND cc.is_valid = 1"
    sql += " ORDER BY vc.code"

    # Strip is_valid when NULL so classifications without a canonical CSV
    # don't carry a meaningless field on every code.
    codes = []
    for r in conn.execute(sql, params).fetchall():
        row = dict(r)
        if row["is_valid"] is None:
            del row["is_valid"]
        codes.append(row)
    meta["codes"] = codes
    return meta


def search_variables_by_classification(
    conn: sqlite3.Connection,
    identifier: str,
    *,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """List variables with at least one state tagged with this classification.

    A2.7: re-sourced off `variable_state.classification_id` (was per-instance
    `variable_instance.classification_id`). `variable_state` carries
    `variable_id`, so the join is direct and sibling-isolated.
    """
    cls_id = _resolve_classification_id(conn, identifier)
    rows = conn.execute(
        """
        SELECT DISTINCT v.register_id, r.name AS register_name,
               CAST(v.provider_key AS INTEGER) AS var_id, v.name AS variable_name
        FROM variable_state vs
        JOIN variable v ON vs.variable_id = v.variable_id
        JOIN register r ON v.register_id = r.register_id
        WHERE vs.classification_id = ?
        ORDER BY r.name, v.name
        LIMIT ? OFFSET ?
        """,
        (cls_id, limit, offset),
    ).fetchall()
    return [dict(r) for r in rows]


def classifications_for_variable(
    conn: sqlite3.Connection, variable_id: int
) -> list[dict[str, Any]]:
    """Return the distinct classifications a variable's states use.

    A single variable can span multiple classifications across its lifetime
    (e.g. SUN2000 → SUN2020), so this returns a list, not a scalar.

    A2.7: re-sourced off `variable_state.classification_id` and keyed by
    `variable_id` (the unique per-variable key). This SIBLING-ISOLATES — the
    A2.6 limitation (where `variable_instance` had no `variable_id`, so an A2.2
    split sibling's classifications aggregated across every sibling sharing the
    `var_id`) is resolved. `instance_count` counts distinct states now.
    """
    rows = conn.execute(
        """
        SELECT c.id, c.short_name, c.name, c.publisher,
               COUNT(DISTINCT vs.state_id) AS instance_count
        FROM variable_state vs
        JOIN classification c ON vs.classification_id = c.id
        WHERE vs.variable_id = ?
        GROUP BY c.id
        ORDER BY c.short_name
        """,
        (variable_id,),
    ).fetchall()
    return [dict(r) for r in rows]
