"""Query functions for reg_meta.

Pure query logic against an open sqlite3.Connection. No CLI concerns
(argument parsing, output formatting, envelopes, timing). These are
the functions that library consumers (e.g. mock_data_wizard) import.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from .errors import EXIT_NOT_FOUND, EXIT_USAGE, RegMetaError
from .fqid import Fqid, derive_variable_slug, try_emit

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


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


SEARCH_FIELDS = frozenset({"datacolumn", "varname", "description", "value", "all"})


def _version_years_for_register(
    conn: sqlite3.Connection, register_id: int
) -> list[int]:
    """Return all version years for a register."""
    rows = conn.execute(
        # `registerversionnamn` stays Swedish — A2.6 drops the table.
        "SELECT rv.registerversionnamn "
        "FROM register_version rv "
        "JOIN register_variant rvar ON rv.register_variant_id = rvar.register_variant_id "
        "WHERE rvar.register_id = ?",
        (register_id,),
    ).fetchall()
    years = []
    for row in rows:
        y = extract_year(row["registerversionnamn"] or "")
        if y is not None:
            years.append(y)
    return years


def _year_in_range(year: int, lo: int | None, hi: int | None) -> bool:
    if lo is not None and year < lo:
        return False
    return hi is None or year <= hi


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

    # For variable-type results: check which (register_id, var_id) pairs
    # have a variable_instance in a version within the year range
    valid_var_pairs: set[tuple[int, int]] = set()
    if var_pairs:
        all_reg_ids = {p[0] for p in var_pairs}
        placeholders = ",".join("?" * len(all_reg_ids))
        rows = conn.execute(
            "SELECT DISTINCT vi.register_id, vi.var_id, rv.registerversionnamn "
            "FROM variable_instance vi "
            "JOIN register_version rv ON vi.regver_id = rv.regver_id "
            f"WHERE vi.register_id IN ({placeholders})",
            list(all_reg_ids),
        ).fetchall()
        for row in rows:
            pair = (row["register_id"], row["var_id"])
            if pair not in var_pairs:
                continue
            year = extract_year(row["registerversionnamn"] or "")
            if year is not None and _year_in_range(year, year_lo, year_hi):
                valid_var_pairs.add(pair)

    # For register-type results: check if register has any version in range
    valid_reg_ids: set[int] = set()
    if reg_only_ids:
        placeholders = ",".join("?" * len(reg_only_ids))
        rows = conn.execute(
            "SELECT DISTINCT rvar.register_id, rv.registerversionnamn "
            "FROM register_version rv "
            "JOIN register_variant rvar ON rv.register_variant_id = rvar.register_variant_id "
            f"WHERE rvar.register_id IN ({placeholders})",
            list(reg_only_ids),
        ).fetchall()
        for row in rows:
            year = extract_year(row["registerversionnamn"] or "")
            if year is not None and _year_in_range(year, year_lo, year_hi):
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
    rows = conn.execute(
        "SELECT DISTINCT va.delivery_column_name, vi.register_id, vi.var_id, "
        "v.name AS variable_name, r.name AS register_name "
        "FROM variable_alias va "
        "JOIN variable_instance vi ON va.cvid = vi.cvid "
        "JOIN variable v ON vi.register_id = v.register_id AND CAST(vi.var_id AS TEXT) = v.provider_key "
        "JOIN register r ON vi.register_id = r.register_id "
        "WHERE va.delivery_column_name LIKE ? "
        "ORDER BY va.delivery_column_name, vi.register_id",
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
    rows = conn.execute(
        "SELECT DISTINCT vc.code, vc.label, "
        "cvm.register_id, cvm.var_id, "
        "v.name AS variable_name, r.name AS register_name "
        "FROM value_code vc "
        "JOIN code_variable_map cvm ON vc.code_id = cvm.code_id "
        "JOIN variable v ON cvm.register_id = v.register_id AND CAST(cvm.var_id AS TEXT) = v.provider_key "
        "JOIN register r ON cvm.register_id = r.register_id "
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
        entry["fqid"] = try_emit(Fqid.register_fqid, provider_slug, entry["slug"])
        variants = conn.execute(
            "SELECT * FROM register_variant WHERE register_id = ? ORDER BY register_variant_id",
            (rid,),
        ).fetchall()
        variant_dicts: list[dict[str, Any]] = []
        for v in variants:
            vd = dict(v)
            vd["fqid"] = try_emit(
                Fqid.register_variant_fqid,
                provider_slug,
                entry["slug"],
                vd["slug"],
            )
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
        variant_slug = rv["slug"]
        versions = conn.execute(
            "SELECT * FROM register_version WHERE register_variant_id = ? ORDER BY regver_id",
            (rvid,),
        ).fetchall()

        versions_out: list[dict[str, Any]] = []
        for ver in versions:
            year = extract_year(ver["registerversionnamn"] or "")
            if year_lo is not None and year is not None and year < year_lo:
                continue
            if year_hi is not None and year is not None and year > year_hi:
                continue

            columns = conn.execute(
                "SELECT vi.cvid, vi.var_id, vi.data_type, vi.data_length, "
                "v.name AS variable_name, "
                "COALESCE(v.source_label, '') as source, "
                "MIN(va.delivery_column_name) as first_alias, "
                "GROUP_CONCAT(va.delivery_column_name, ', ') as aliases "
                "FROM variable_instance vi "
                "JOIN variable v ON vi.register_id = v.register_id AND CAST(vi.var_id AS TEXT) = v.provider_key "
                "LEFT JOIN variable_alias va ON vi.cvid = va.cvid "
                "WHERE vi.regver_id = ? "
                "GROUP BY vi.cvid ORDER BY vi.var_id, vi.cvid",
                (ver["regver_id"],),
            ).fetchall()

            # `register_version.slug` (§5.2) is the canonical version-slot
            # token — auto-derived period for periodized rows, curated slug
            # for unperiodized aux rows. Both reach Catalog via the same
            # column, so emitting from it keeps the round-trip honest.
            period = ver["slug"]
            col_dicts: list[dict[str, Any]] = []
            for c in columns:
                cd = dict(c)
                # `first_alias` from a SQL MIN — deterministic across runs;
                # the comma-joined `aliases` is still emitted for display.
                variable_slug = derive_variable_slug(cd.pop("first_alias", None))
                cd["fqid"] = try_emit(
                    Fqid.binding_fqid,
                    provider_slug,
                    register_slug,
                    variant_slug,
                    period,
                    variable_slug,
                )
                col_dicts.append(cd)
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
                    "regver_id": ver["regver_id"],
                    "version_name": ver["registerversionnamn"],
                    "year": year,
                    "fqid": try_emit(
                        Fqid.register_version_fqid,
                        provider_slug,
                        register_slug,
                        variant_slug,
                        period,
                    ),
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
                    "fqid": try_emit(
                        Fqid.register_variant_fqid,
                        provider_slug,
                        register_slug,
                        variant_slug,
                    ),
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
        alias_sql = (
            "SELECT DISTINCT v.*, CAST(v.provider_key AS INTEGER) AS var_id, r.name AS register_name FROM variable_alias a "
            "JOIN variable_instance vi ON a.cvid = vi.cvid "
            "JOIN variable v ON vi.register_id = v.register_id AND CAST(vi.var_id AS TEXT) = v.provider_key "
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

        instances = conn.execute(
            "SELECT vi.cvid, vi.register_variant_id, vi.regver_id, "
            "vi.data_type, vi.data_length, "
            "vi.value_set_version_label, vi.classification_id, "
            "c.short_name AS classification, "
            "rv.name AS variant_name, rver.registerversionnamn, "
            "p.slug AS provider_slug, r.slug AS register_slug, "
            "rv.slug AS variant_slug, rver.slug AS version_slug "
            "FROM variable_instance vi "
            "LEFT JOIN classification c ON vi.classification_id = c.id "
            "JOIN register_variant rv ON vi.register_variant_id = rv.register_variant_id "
            "JOIN register_version rver ON vi.regver_id = rver.regver_id "
            "JOIN register r ON vi.register_id = r.register_id "
            "JOIN provider p ON r.provider_id = p.provider_id "
            "WHERE vi.register_id = ? AND vi.var_id = ? "
            "ORDER BY rver.registerversionnamn, vi.cvid",
            (rid, vid),
        ).fetchall()

        cvids = [inst["cvid"] for inst in instances]

        # Batch-fetch aliases and value counts for all instances
        aliases_map: dict[str, list[str]] = {c: [] for c in cvids}
        value_counts: dict[str, int] = dict.fromkeys(cvids, 0)
        if cvids:
            cvid_ph = _in_placeholders(cvids)
            for row in conn.execute(
                f"SELECT cvid, delivery_column_name FROM variable_alias "
                f"WHERE cvid IN ({cvid_ph}) ORDER BY cvid, delivery_column_name",
                cvids,
            ):
                aliases_map[row["cvid"]].append(row["delivery_column_name"])
            for row in conn.execute(
                f"SELECT vi.cvid, COUNT(*) as cnt "
                f"FROM variable_instance vi "
                f"JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id "
                f"WHERE vi.cvid IN ({cvid_ph}) "
                f"GROUP BY vi.cvid",
                cvids,
            ):
                value_counts[row["cvid"]] = row["cnt"]

        instances_out: list[dict[str, Any]] = []
        for inst in instances:
            cvid = inst["cvid"]
            inst_aliases = aliases_map[cvid]
            # First alias is the lexically-smallest delivery_column_name —
            # `aliases_map` is sorted by ``ORDER BY cvid, delivery_column_name``
            # in the fetch above (§5.11 rename from `kolumnnamn`).
            first_alias = inst_aliases[0] if inst_aliases else None
            variable_slug = derive_variable_slug(first_alias)
            inst_dict: dict[str, Any] = {
                "cvid": cvid,
                "register_variant_id": inst["register_variant_id"],
                # `variant_name` already aliased in the SELECT.
                "variant_name": inst["variant_name"],
                "regver_id": inst["regver_id"],
                "version_name": inst["registerversionnamn"],
                "year": extract_year(inst["registerversionnamn"] or ""),
                "data_type": inst["data_type"],
                "data_length": inst["data_length"],
                "aliases": inst_aliases,
                "value_set_count": value_counts[cvid],
                "fqid": try_emit(
                    Fqid.binding_fqid,
                    inst["provider_slug"],
                    inst["register_slug"],
                    inst["variant_slug"],
                    inst["version_slug"],
                    variable_slug,
                ),
            }
            if inst["classification"]:
                inst_dict["classification"] = inst["classification"]
            instances_out.append(inst_dict)

        var_classifications = classifications_for_variable(conn, rid, vid)

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
        "SELECT v.register_id, CAST(v.provider_key AS INTEGER) AS var_id, v.name AS variable_name, "
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

        rows = conn.execute(
            "SELECT rv.registerversionnamn, "
            "GROUP_CONCAT(DISTINCT va.delivery_column_name) as aliases "
            "FROM variable_instance vi "
            "JOIN register_version rv ON vi.regver_id = rv.regver_id "
            "LEFT JOIN variable_alias va ON vi.cvid = va.cvid "
            "WHERE vi.register_id = ? AND vi.var_id = ? "
            "GROUP BY rv.regver_id "
            "ORDER BY rv.registerversionnamn",
            (rid, vid),
        ).fetchall()

        reg_years: list[int] = []
        aliases_by_year: dict[str, list[str]] = {}
        for row in rows:
            year = extract_year(row["registerversionnamn"] or "")
            if year is None:
                continue
            reg_years.append(year)
            all_years.add(year)
            aliases = (row["aliases"] or "").split(",")
            aliases = [a.strip() for a in aliases if a.strip()]
            aliases_by_year[str(year)] = aliases

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

    rows = conn.execute(
        "SELECT rvar.register_variant_id, rvar.name AS variant_name, "
        "rv.registerversionnamn "
        "FROM register_variant rvar "
        "JOIN register_version rv ON rvar.register_variant_id = rv.register_variant_id "
        "WHERE rvar.register_id = ? "
        "ORDER BY rv.registerversionnamn",
        (reg_id,),
    ).fetchall()

    all_years: set[int] = set()
    variants: dict[int, dict[str, Any]] = {}

    for row in rows:
        year = extract_year(row["registerversionnamn"] or "")
        if year is None:
            continue
        all_years.add(year)
        rvid = row["register_variant_id"]
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


def get_values(conn: sqlite3.Connection, cvid: str) -> list[dict[str, Any]]:
    """Get value-set members for a CVID.

    Returns the year-correct code list — codes valid at the cvid's regver
    year per SCB validity windows. Year projection happens at build time
    (see ``reg_meta.db._project_and_mint_value_sets``); this query is a plain
    3-table read with no temporal logic.
    """
    int_cvid = _try_int(cvid)
    inst = conn.execute(
        "SELECT 1 FROM variable_instance WHERE cvid = ?", (int_cvid,)
    ).fetchone()
    if not inst:
        raise RegMetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"Variable instance (CVID) {cvid} not found.",
            remediation="Use `reg-meta get schema` to find valid CVIDs.",
        )

    values = conn.execute(
        # vardemangdsniva stays Swedish (scope guard); the other three
        # columns ride the §5.11 rename.
        "SELECT vc.code, vc.label, "
        "vi.value_set_version_label, vi.vardemangdsniva "
        "FROM variable_instance vi "
        "JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id "
        "JOIN value_code vc ON vsm.code_id = vc.code_id "
        "WHERE vi.cvid = ? "
        "ORDER BY vc.code",
        (int_cvid,),
    ).fetchall()
    return [dict(v) for v in values]


def get_values_by_variable(
    conn: sqlite3.Connection,
    variable: str,
    *,
    register: str | None = None,
    year: int | None = None,
) -> dict[str, Any]:
    """Resolve a variable to its instances and return year-correct codes per instance.

    Each instance is one cvid → year-correct value list. Filter via
    ``register`` and/or ``year``. Returns
    ``{input, variable_name, instances: [{cvid, register_id, register_name,
    register_variant_id, variant_name, regver_id, version_name, year, values}]}``.
    Resolution mirrors ``get_varinfo``: var_id → variable name → alias.
    Keys follow the §5.11 rename (`variabelnamn` → `variable_name`).
    """
    reg_ids: list[int] | None = None
    if register:
        reg_ids = require_register_ids(conn, register)

    int_variable: int | None
    raw_int = _try_int(variable)
    int_variable = raw_int if isinstance(raw_int, int) else None

    rows_by_id: list[Any] = []
    if reg_ids:
        ph = _in_placeholders(reg_ids)
        if int_variable is not None:
            rows_by_id = conn.execute(
                f"SELECT register_id, CAST(provider_key AS INTEGER) AS var_id, name FROM variable "
                f"WHERE provider_key = CAST(? AS TEXT) AND register_id IN ({ph})",
                [int_variable, *reg_ids],
            ).fetchall()
        rows_by_name = conn.execute(
            f"SELECT register_id, CAST(provider_key AS INTEGER) AS var_id, name FROM variable "
            f"WHERE LOWER(name) = LOWER(?) AND register_id IN ({ph})",
            [variable, *reg_ids],
        ).fetchall()
    else:
        if int_variable is not None:
            rows_by_id = conn.execute(
                "SELECT register_id, CAST(provider_key AS INTEGER) AS var_id, name FROM variable WHERE provider_key = CAST(? AS TEXT)",
                (int_variable,),
            ).fetchall()
        rows_by_name = conn.execute(
            "SELECT register_id, CAST(provider_key AS INTEGER) AS var_id, name FROM variable "
            "WHERE LOWER(name) = LOWER(?)",
            (variable,),
        ).fetchall()

    matched = rows_by_id or rows_by_name

    if not matched:
        alias_sql = (
            "SELECT DISTINCT v.register_id, CAST(v.provider_key AS INTEGER) AS var_id, v.name "
            "FROM variable_alias a "
            "JOIN variable_instance vi ON a.cvid = vi.cvid "
            "JOIN variable v ON vi.register_id = v.register_id AND CAST(vi.var_id AS TEXT) = v.provider_key "
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

    # Batch-fetch instances for all (register_id, var_id) pairs in one query,
    # then batch-fetch all value codes in a second query keyed on cvid. Avoids
    # the N+1 pattern when a variable spans dozens of registers.
    pair_clauses = " OR ".join(
        ["(vi.register_id = ? AND vi.var_id = ?)"] * len(matched)
    )
    pair_params: list[Any] = []
    for var in matched:
        pair_params.extend([var["register_id"], var["var_id"]])

    inst_rows = conn.execute(
        f"SELECT vi.cvid, vi.register_id, vi.var_id, vi.register_variant_id, vi.regver_id, "
        f"r.name AS register_name, rv.name AS variant_name, "
        f"rver.registerversionnamn "
        f"FROM variable_instance vi "
        f"JOIN register r ON vi.register_id = r.register_id "
        f"JOIN register_variant rv ON vi.register_variant_id = rv.register_variant_id "
        f"JOIN register_version rver ON vi.regver_id = rver.regver_id "
        f"WHERE {pair_clauses}",
        pair_params,
    ).fetchall()

    instances: list[dict[str, Any]] = []
    cvid_index: dict[Any, dict[str, Any]] = {}
    for row in inst_rows:
        inst_year = extract_year(row["registerversionnamn"] or "")
        if year is not None and inst_year != year:
            continue
        inst = {
            "cvid": row["cvid"],
            "register_id": row["register_id"],
            "register_name": row["register_name"],
            "register_variant_id": row["register_variant_id"],
            "variant_name": row["variant_name"],
            "regver_id": row["regver_id"],
            "version_name": row["registerversionnamn"],
            "year": inst_year,
            "values": [],
        }
        instances.append(inst)
        cvid_index[row["cvid"]] = inst

    if cvid_index:
        cvid_ph = _in_placeholders(list(cvid_index))
        for row in conn.execute(
            f"SELECT vi.cvid, vc.code, vc.label "
            f"FROM variable_instance vi "
            f"JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id "
            f"JOIN value_code vc ON vsm.code_id = vc.code_id "
            f"WHERE vi.cvid IN ({cvid_ph}) "
            f"ORDER BY vi.cvid, vc.code",
            list(cvid_index),
        ):
            cvid_index[row["cvid"]]["values"].append(
                {"code": row["code"], "label": row["label"]}
            )

    instances.sort(key=lambda i: (i["register_name"] or "", i["year"] or 0, i["cvid"]))

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
    """Get all column aliases for a variable.

    Returns a list of dicts with "delivery_column_name", "register_id",
    "register_name", "register_variant_id", "regver_id", "version_name". Keys follow
    the §5.11 rename (`kolumnnamn` → `delivery_column_name`).
    """
    reg_ids: list[int] | None = None
    if register:
        reg_ids = require_register_ids(conn, register)

    # Match by var_id or variable name (§5.11: was `variabelnamn`)
    int_variable = _try_int(variable)
    if reg_ids:
        ph = _in_placeholders(reg_ids)
        var_rows = conn.execute(
            f"SELECT register_id, CAST(provider_key AS INTEGER) AS var_id FROM variable "
            f"WHERE (provider_key = CAST(? AS TEXT) OR LOWER(name) = LOWER(?)) "
            f"AND register_id IN ({ph})",
            [int_variable, variable, *reg_ids],
        ).fetchall()
    else:
        var_rows = conn.execute(
            "SELECT register_id, CAST(provider_key AS INTEGER) AS var_id FROM variable "
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
            "vi.register_id, vi.register_variant_id, vi.regver_id, "
            "r.name AS register_name, rver.registerversionnamn "
            "FROM variable_alias va "
            "JOIN variable_instance vi ON va.cvid = vi.cvid "
            "JOIN register r ON vi.register_id = r.register_id "
            "JOIN register_version rver ON vi.regver_id = rver.regver_id "
            "WHERE vi.register_id = ? AND vi.var_id = ? "
            "ORDER BY va.delivery_column_name, r.name",
            (vr["register_id"], vr["var_id"]),
        ).fetchall()
        for r in rows:
            key = f"{r['delivery_column_name']}:{r['register_id']}:{r['regver_id']}"
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
                    "regver_id": r["regver_id"],
                    "version_name": r["registerversionnamn"],
                }
            )

    return results


# ---------------------------------------------------------------------------
# Get diff
# ---------------------------------------------------------------------------


def _find_version_for_year(
    conn: sqlite3.Connection, register_variant_id: int, year: int
) -> dict[str, Any] | None:
    """Find the version matching a year: exact first, then latest ≤ year."""
    versions = conn.execute(
        "SELECT regver_id, registerversionnamn FROM register_version "
        "WHERE register_variant_id = ? ORDER BY regver_id",
        (register_variant_id,),
    ).fetchall()

    best: dict[str, Any] | None = None
    best_year: int | None = None
    for v in versions:
        vy = extract_year(v["registerversionnamn"] or "")
        if vy is None:
            continue
        if vy == year:
            return {
                "regver_id": v["regver_id"],
                "version_name": v["registerversionnamn"],
                "year": vy,
            }
        if vy <= year and (best_year is None or vy > best_year):
            best = {
                "regver_id": v["regver_id"],
                "version_name": v["registerversionnamn"],
                "year": vy,
            }
            best_year = vy
    return best


def _fetch_columns_for_version(
    conn: sqlite3.Connection, regver_id: int
) -> dict[int, dict[str, Any]]:
    """Fetch columns for a version, keyed by var_id."""
    rows = conn.execute(
        "SELECT vi.var_id, vi.data_type, vi.data_length, "
        "v.name AS variable_name, "
        "GROUP_CONCAT(va.delivery_column_name, ', ') as aliases "
        "FROM variable_instance vi "
        "JOIN variable v ON vi.register_id = v.register_id AND CAST(vi.var_id AS TEXT) = v.provider_key "
        "LEFT JOIN variable_alias va ON vi.cvid = va.cvid "
        "WHERE vi.regver_id = ? "
        "GROUP BY vi.var_id ORDER BY vi.var_id",
        (regver_id,),
    ).fetchall()
    result: dict[int, dict[str, Any]] = {}
    for r in rows:
        aliases = sorted(r["aliases"].split(", ")) if r["aliases"] else []
        result[r["var_id"]] = {
            "var_id": r["var_id"],
            "variable_name": r["variable_name"],
            "data_type": r["data_type"],
            "data_length": r["data_length"],
            "aliases": aliases,
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
                rows = conn.execute(
                    f"SELECT DISTINCT vi.var_id, var.name "
                    f"FROM variable_alias va "
                    f"JOIN variable_instance vi ON va.cvid = vi.cvid "
                    f"JOIN variable var ON vi.register_id = var.register_id AND CAST(vi.var_id AS TEXT) = var.provider_key "
                    f"WHERE LOWER(va.delivery_column_name) = LOWER(?) AND vi.register_id IN ({ph})",
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
        from_ver = _find_version_for_year(conn, rvid, from_year)
        to_ver = _find_version_for_year(conn, rvid, to_year)
        if not from_ver or not to_ver:
            continue
        any_versions_found = True

        from_cols = _fetch_columns_for_version(conn, from_ver["regver_id"])
        to_cols = _fetch_columns_for_version(conn, to_ver["regver_id"])

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
                "from_version": from_ver,
                "to_version": to_ver,
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

        # Instance count and year range
        instances = conn.execute(
            "SELECT vi.cvid, rver.registerversionnamn "
            "FROM variable_instance vi "
            "JOIN register_version rver ON vi.regver_id = rver.regver_id "
            "WHERE vi.register_id = ? AND vi.var_id = ?",
            (rid, vid),
        ).fetchall()

        instance_count = len(instances)
        years = [extract_year(i["registerversionnamn"] or "") for i in instances]
        years = [y for y in years if y is not None]
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
    """
    rows = conn.execute(
        "SELECT v.name AS variable_name, "
        "COUNT(DISTINCT vc.code) as n_distinct_codes, "
        "COUNT(DISTINCT v.register_id) as n_registers, "
        "COUNT(DISTINCT vi.cvid) as n_instances "
        "FROM variable v "
        "JOIN variable_instance vi ON v.register_id = vi.register_id AND v.provider_key = CAST(vi.var_id AS TEXT) "
        "JOIN value_set_member vsm ON vi.value_set_id = vsm.value_set_id "
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

        if reg_ids:
            ph = _in_placeholders(reg_ids)
            exact_rows = conn.execute(
                f"SELECT va.delivery_column_name, vi.register_id, vi.var_id, "
                f"v.name AS variable_name "
                f"FROM variable_alias va "
                f"JOIN variable_instance vi ON va.cvid = vi.cvid "
                f"JOIN variable v ON vi.register_id = v.register_id AND CAST(vi.var_id AS TEXT) = v.provider_key "
                f"WHERE LOWER(va.delivery_column_name) = ? AND vi.register_id IN ({ph}) "
                f"GROUP BY vi.register_id, vi.var_id "
                f"ORDER BY vi.register_id, vi.var_id",
                [col_lower, *reg_ids],
            ).fetchall()
        else:
            exact_rows = conn.execute(
                "SELECT va.delivery_column_name, vi.register_id, vi.var_id, "
                "v.name AS variable_name "
                "FROM variable_alias va "
                "JOIN variable_instance vi ON va.cvid = vi.cvid "
                "JOIN variable v ON vi.register_id = v.register_id AND CAST(vi.var_id AS TEXT) = v.provider_key "
                "WHERE LOWER(va.delivery_column_name) = ? "
                "GROUP BY vi.register_id, vi.var_id "
                "ORDER BY vi.register_id, vi.var_id",
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
    fqid = try_emit(Fqid.classification_fqid, d.get("slug"), d.get("version"))
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
        SELECT c.id, c.short_name, c.name, c.name_en, c.publisher, c.version,
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
    """List variables that have at least one instance tagged with this classification."""
    cls_id = _resolve_classification_id(conn, identifier)
    rows = conn.execute(
        """
        SELECT DISTINCT v.register_id, r.name AS register_name,
               CAST(v.provider_key AS INTEGER) AS var_id, v.name AS variable_name
        FROM variable_instance vi
        JOIN variable v ON vi.register_id = v.register_id AND CAST(vi.var_id AS TEXT) = v.provider_key
        JOIN register r ON v.register_id = r.register_id
        WHERE vi.classification_id = ?
        ORDER BY r.name, v.name
        LIMIT ? OFFSET ?
        """,
        (cls_id, limit, offset),
    ).fetchall()
    return [dict(r) for r in rows]


def classifications_for_variable(
    conn: sqlite3.Connection, register_id: int, var_id: int
) -> list[dict[str, Any]]:
    """Return the distinct classifications a variable's instances use.

    A single variable can span multiple classifications across its lifetime
    (e.g. SUN2000 → SUN2020), so this returns a list, not a scalar.
    """
    rows = conn.execute(
        """
        SELECT c.id, c.short_name, c.name, c.publisher, c.version,
               COUNT(DISTINCT vi.cvid) AS instance_count
        FROM variable_instance vi
        JOIN classification c ON vi.classification_id = c.id
        WHERE vi.register_id = ? AND vi.var_id = ?
        GROUP BY c.id
        ORDER BY c.short_name
        """,
        (register_id, var_id),
    ).fetchall()
    return [dict(r) for r in rows]
