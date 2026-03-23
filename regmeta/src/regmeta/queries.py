"""Query functions for regmeta.

Pure query logic against an open sqlite3.Connection. No CLI concerns
(argument parsing, output formatting, envelopes, timing). These are
the functions that library consumers (e.g. mock_data_wizard) import.
"""

from __future__ import annotations

import re
import sqlite3
from typing import Any

from .errors import EXIT_NOT_FOUND, EXIT_USAGE, RegmetaError

_YEAR_RE = re.compile(r"\d{4}")


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
        "SELECT register_id FROM register WHERE LOWER(registernamn) = LOWER(?)",
        (value,),
    ).fetchall()
    if rows:
        return [r["register_id"] for r in rows]

    rows = conn.execute(
        "SELECT register_id FROM register WHERE LOWER(registernamn) LIKE '%' || LOWER(?) || '%'",
        (value,),
    ).fetchall()
    return [r["register_id"] for r in rows]


def require_register_ids(conn: sqlite3.Connection, value: str) -> list[str]:
    """Like resolve_register_ids but raises NOT_FOUND if empty."""
    ids = resolve_register_ids(conn, value)
    if not ids:
        raise RegmetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"No register matching '{value}'.",
            remediation="Use `regmeta search` to find valid register names or IDs.",
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
    """Extract the first 4-digit year from a version name string."""
    m = _YEAR_RE.search(version_name)
    return int(m.group()) if m else None


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


SEARCH_FIELDS = frozenset({"datacolumn", "varname", "description", "value", "all"})


def search(
    conn: sqlite3.Connection,
    query: str,
    *,
    field: str = "all",
    type: str = "all",
    register: str | None = None,
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
    """
    if field not in SEARCH_FIELDS:
        raise RegmetaError(
            exit_code=EXIT_USAGE,
            code="usage_error",
            error_class="usage",
            message=f"Invalid search field '{field}'. Valid: {sorted(SEARCH_FIELDS)}",
            remediation="Use --datacolumn, --varname, --description, --value, or --all-fields.",
        )

    reg_ids: set[str] | None = None
    if register:
        ids = resolve_register_ids(conn, register)
        if not ids:
            return {"total_count": 0, "results": []}
        reg_ids = set(ids)

    # type filter: "register" types vs "variable" types
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

    all_results.sort(key=lambda x: x.get("fts_rank", 0))
    total_count = len(all_results)
    results = all_results[offset : offset + limit]
    return {"total_count": total_count, "results": results}


def _search_datacolumns(
    conn: sqlite3.Connection, like_pattern: str, reg_ids: set[str] | None
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT DISTINCT va.kolumnnamn, vi.register_id, vi.var_id, "
        "v.variabelnamn, r.registernamn "
        "FROM variable_alias va "
        "JOIN variable_instance vi ON va.cvid = vi.cvid "
        "JOIN variable v ON vi.register_id = v.register_id AND vi.var_id = v.var_id "
        "JOIN register r ON vi.register_id = r.register_id "
        "WHERE va.kolumnnamn LIKE ? "
        "ORDER BY va.kolumnnamn, vi.register_id",
        (like_pattern,),
    ).fetchall()
    results = []
    for r in rows:
        if reg_ids and r["register_id"] not in reg_ids:
            continue
        results.append(
            {
                "type": "datacolumn",
                "datacolumn": r["kolumnnamn"],
                "register_id": r["register_id"],
                "register_name": r["registernamn"],
                "var_id": r["var_id"],
                "variable_name": r["variabelnamn"],
                "fts_rank": 0,
            }
        )
    return results


def _search_varnames(
    conn: sqlite3.Connection, like_pattern: str, reg_ids: set[str] | None
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT v.register_id, v.var_id, v.variabelnamn, r.registernamn "
        "FROM variable v "
        "JOIN register r ON v.register_id = r.register_id "
        "WHERE v.variabelnamn LIKE ? "
        "ORDER BY v.variabelnamn, v.register_id",
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
                "register_name": r["registernamn"],
                "var_id": r["var_id"],
                "variable_name": r["variabelnamn"],
                "fts_rank": 0,
            }
        )
    return results


def _search_description_registers(
    conn: sqlite3.Connection, query: str, reg_ids: set[str] | None
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT register_id, registernamn, registerrubrik, rank "
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
                "register_name": r["registernamn"],
                "register_rubrik": r["registerrubrik"],
                "fts_rank": r["rank"],
            }
        )
    return results


def _search_description_variables(
    conn: sqlite3.Connection, query: str, reg_ids: set[str] | None
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT vf.register_id, vf.var_id, vf.variabelnamn, vf.variabeldefinition, "
        "vf.variabelbeskrivning, vf.rank, r.registernamn, r.registerrubrik "
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
                "register_name": r["registernamn"],
                "register_rubrik": r["registerrubrik"],
                "var_id": r["var_id"],
                "variable_name": r["variabelnamn"],
                "variable_definition": r["variabeldefinition"],
                "fts_rank": r["rank"],
            }
        )
    return results


def _search_values(
    conn: sqlite3.Connection, like_pattern: str, reg_ids: set[str] | None
) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT DISTINCT vc.vardekod, vc.vardebenamning, "
        "cvm.register_id, cvm.var_id, v.variabelnamn, r.registernamn "
        "FROM value_code vc "
        "JOIN code_variable_map cvm ON vc.code_id = cvm.code_id "
        "JOIN variable v ON cvm.register_id = v.register_id AND cvm.var_id = v.var_id "
        "JOIN register r ON cvm.register_id = r.register_id "
        "WHERE vc.vardekod LIKE ? OR vc.vardebenamning LIKE ? "
        "ORDER BY vc.vardekod "
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
                "vardekod": r["vardekod"],
                "vardebenamning": r["vardebenamning"],
                "register_id": r["register_id"],
                "register_name": r["registernamn"],
                "var_id": r["var_id"],
                "variable_name": r["variabelnamn"],
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
        reg = conn.execute(
            "SELECT * FROM register WHERE register_id = ?", (rid,)
        ).fetchone()
        entry = dict(reg)
        variants = conn.execute(
            "SELECT * FROM register_variant WHERE register_id = ? ORDER BY regvar_id",
            (rid,),
        ).fetchall()
        entry["variants"] = [dict(v) for v in variants]
        registers.append(entry)
    return registers


# ---------------------------------------------------------------------------
# Get schema
# ---------------------------------------------------------------------------


def _in_placeholders(ids: list[str]) -> str:
    return ",".join("?" for _ in ids)


def get_schema(
    conn: sqlite3.Connection,
    *,
    regvar_id: str | None = None,
    register: str | None = None,
    years: str | None = None,
) -> dict[str, Any]:
    """Get column listing organized by variant → version → columns.

    Requires either regvar_id or register. Returns {"variants": [...]}.
    """
    if regvar_id:
        rv = conn.execute(
            "SELECT * FROM register_variant WHERE regvar_id = ?",
            (_try_int(regvar_id),),
        ).fetchone()
        if not rv:
            raise RegmetaError(
                exit_code=EXIT_NOT_FOUND,
                code="not_found",
                error_class="query",
                message=f"Register variant {regvar_id} not found.",
                remediation="Use `regmeta get register <name>` to list variants.",
            )
        variant_rows = [rv]
    elif register:
        reg_ids = require_register_ids(conn, register)
        variant_rows = conn.execute(
            f"SELECT * FROM register_variant WHERE register_id IN ({_in_placeholders(reg_ids)}) "
            "ORDER BY register_id, regvar_id",
            reg_ids,
        ).fetchall()
        if not variant_rows:
            raise RegmetaError(
                exit_code=EXIT_NOT_FOUND,
                code="not_found",
                error_class="query",
                message=f"No variants found for register '{register}'.",
                remediation="Use `regmeta get register <name>` to verify.",
            )
    else:
        raise RegmetaError(
            exit_code=EXIT_USAGE,
            code="usage_error",
            error_class="usage",
            message="Provide either a regvar_id or register.",
            remediation="Usage: get_schema(conn, regvar_id=...) or get_schema(conn, register=...)",
        )

    year_lo, year_hi = None, None
    if years:
        year_lo, year_hi = parse_year_range(years)

    variants_out: list[dict[str, Any]] = []
    for rv in variant_rows:
        rvid = rv["regvar_id"]
        versions = conn.execute(
            "SELECT * FROM register_version WHERE regvar_id = ? ORDER BY regver_id",
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
                "SELECT vi.cvid, vi.var_id, vi.datatyp, vi.datalangd, "
                "v.variabelnamn, "
                "GROUP_CONCAT(va.kolumnnamn, ', ') as aliases "
                "FROM variable_instance vi "
                "JOIN variable v ON vi.register_id = v.register_id AND vi.var_id = v.var_id "
                "LEFT JOIN variable_alias va ON vi.cvid = va.cvid "
                "WHERE vi.regver_id = ? "
                "GROUP BY vi.cvid ORDER BY vi.var_id, vi.cvid",
                (ver["regver_id"],),
            ).fetchall()

            versions_out.append(
                {
                    "regver_id": ver["regver_id"],
                    "version_name": ver["registerversionnamn"],
                    "year": year,
                    "columns": [dict(c) for c in columns],
                }
            )

        if versions_out:
            variants_out.append(
                {
                    "regvar_id": rvid,
                    "register_id": rv["register_id"],
                    "registervariantnamn": rv["registervariantnamn"],
                    "registervariantrubrik": rv["registervariantrubrik"],
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
    reg_ids: list[str] | None = None
    if register:
        reg_ids = require_register_ids(conn, register)

    # Match variable by var_id first, fall back to name
    int_variable = _try_int(variable)
    if reg_ids:
        ph = _in_placeholders(reg_ids)
        vars_by_id = conn.execute(
            f"SELECT v.*, r.registernamn FROM variable v "
            f"JOIN register r ON v.register_id = r.register_id "
            f"WHERE v.var_id = ? AND v.register_id IN ({ph})",
            [int_variable, *reg_ids],
        ).fetchall()
        vars_by_name = conn.execute(
            f"SELECT v.*, r.registernamn FROM variable v "
            f"JOIN register r ON v.register_id = r.register_id "
            f"WHERE LOWER(v.variabelnamn) = LOWER(?) AND v.register_id IN ({ph})",
            [variable, *reg_ids],
        ).fetchall()
    else:
        vars_by_id = conn.execute(
            "SELECT v.*, r.registernamn FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "WHERE v.var_id = ?",
            (int_variable,),
        ).fetchall()
        vars_by_name = conn.execute(
            "SELECT v.*, r.registernamn FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "WHERE LOWER(v.variabelnamn) = LOWER(?)",
            (variable,),
        ).fetchall()

    matched_vars = vars_by_id if vars_by_id else vars_by_name

    if not matched_vars:
        raise RegmetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"No variable matching '{variable}'"
            + (f" in register '{register}'" if register else "")
            + ".",
            remediation="Use `regmeta search --query <term>` to find variables.",
        )

    variables_out: list[dict[str, Any]] = []
    for var in matched_vars:
        rid, vid = var["register_id"], var["var_id"]

        instances = conn.execute(
            "SELECT vi.cvid, vi.regvar_id, vi.regver_id, vi.datatyp, vi.datalangd, "
            "rv.registervariantnamn, rver.registerversionnamn "
            "FROM variable_instance vi "
            "JOIN register_variant rv ON vi.regvar_id = rv.regvar_id "
            "JOIN register_version rver ON vi.regver_id = rver.regver_id "
            "WHERE vi.register_id = ? AND vi.var_id = ? "
            "ORDER BY rver.registerversionnamn, vi.cvid",
            (rid, vid),
        ).fetchall()

        cvids = [inst["cvid"] for inst in instances]

        # Batch-fetch aliases and value counts for all instances
        aliases_map: dict[str, list[str]] = {c: [] for c in cvids}
        value_counts: dict[str, int] = {c: 0 for c in cvids}
        if cvids:
            cvid_ph = _in_placeholders(cvids)
            for row in conn.execute(
                f"SELECT cvid, kolumnnamn FROM variable_alias "
                f"WHERE cvid IN ({cvid_ph}) ORDER BY cvid, kolumnnamn",
                cvids,
            ):
                aliases_map[row["cvid"]].append(row["kolumnnamn"])
            for row in conn.execute(
                f"SELECT cvid, COUNT(*) as cnt FROM cvid_value_code "
                f"WHERE cvid IN ({cvid_ph}) GROUP BY cvid",
                cvids,
            ):
                value_counts[row["cvid"]] = row["cnt"]

        instances_out: list[dict[str, Any]] = []
        for inst in instances:
            cvid = inst["cvid"]
            instances_out.append(
                {
                    "cvid": cvid,
                    "regvar_id": inst["regvar_id"],
                    "variant_name": inst["registervariantnamn"],
                    "regver_id": inst["regver_id"],
                    "version_name": inst["registerversionnamn"],
                    "year": extract_year(inst["registerversionnamn"] or ""),
                    "datatyp": inst["datatyp"],
                    "datalangd": inst["datalangd"],
                    "aliases": aliases_map[cvid],
                    "value_set_count": value_counts[cvid],
                }
            )

        variables_out.append(
            {
                "register_id": rid,
                "register_name": var["registernamn"],
                "var_id": vid,
                "variabelnamn": var["variabelnamn"],
                "variabeldefinition": var["variabeldefinition"],
                "variabelbeskrivning": var["variabelbeskrivning"],
                "variabeloperationell_definition": var[
                    "variabeloperationell_definition"
                ],
                "variabelreferenstid": var["variabelreferenstid"],
                "variabelhamtadfran": var["variabelhamtadfran"],
                "variabelregister_kalla": var["variabelregister_kalla"],
                "mattenhet": var["mattenhet"],
                "instances": instances_out,
            }
        )

    return variables_out


# ---------------------------------------------------------------------------
# Get values
# ---------------------------------------------------------------------------


def get_values(
    conn: sqlite3.Connection, cvid: str, *, valid_at: str | None = None
) -> list[dict[str, Any]]:
    """Get value-set members for a CVID.

    If valid_at is an ISO date (YYYY-MM-DD), only return values whose
    validity period includes that date.  Codes with no value_item
    entries (no temporal tracking) are treated as always valid.
    """
    int_cvid = _try_int(cvid)
    inst = conn.execute(
        "SELECT * FROM variable_instance WHERE cvid = ?", (int_cvid,)
    ).fetchone()
    if not inst:
        raise RegmetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"Variable instance (CVID) {cvid} not found.",
            remediation="Use `regmeta get schema` to find valid CVIDs.",
        )

    if valid_at is None:
        values = conn.execute(
            "SELECT vc.vardekod, vc.vardebenamning, "
            "vi.vardemangdsversion, vi.vardemangdsniva "
            "FROM cvid_value_code cvc "
            "JOIN value_code vc ON cvc.code_id = vc.code_id "
            "JOIN variable_instance vi ON cvc.cvid = vi.cvid "
            "WHERE cvc.cvid = ? ORDER BY vc.vardekod",
            (int_cvid,),
        ).fetchall()
    else:
        # A code is valid at a date if it has no value_item entries (no
        # temporal tracking), or at least one of its items has a validity
        # range covering the date.
        values = conn.execute(
            "SELECT vc.vardekod, vc.vardebenamning, "
            "vi.vardemangdsversion, vi.vardemangdsniva "
            "FROM cvid_value_code cvc "
            "JOIN value_code vc ON cvc.code_id = vc.code_id "
            "JOIN variable_instance vi ON cvc.cvid = vi.cvid "
            "WHERE cvc.cvid = ? AND ("
            "  NOT EXISTS ("
            "    SELECT 1 FROM value_item vit"
            "    WHERE vit.cvid = cvc.cvid AND vit.code_id = cvc.code_id"
            "  )"
            "  OR EXISTS ("
            "    SELECT 1 FROM value_item vit"
            "    JOIN value_item_validity viv ON vit.item_id = viv.item_id"
            "    WHERE vit.cvid = cvc.cvid AND vit.code_id = cvc.code_id"
            "    AND (viv.valid_from IS NULL OR viv.valid_from <= ?)"
            "    AND (viv.valid_to IS NULL OR viv.valid_to >= ?)"
            "  )"
            ") ORDER BY vc.vardekod",
            (int_cvid, valid_at, valid_at),
        ).fetchall()
    return [dict(v) for v in values]


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

    Returns a list of dicts with "kolumnnamn", "register_id", "register_name",
    "regvar_id", "regver_id", "version_name".
    """
    reg_ids: list[str] | None = None
    if register:
        reg_ids = require_register_ids(conn, register)

    # Match by var_id or variabelnamn
    int_variable = _try_int(variable)
    if reg_ids:
        ph = _in_placeholders(reg_ids)
        var_rows = conn.execute(
            f"SELECT register_id, var_id FROM variable "
            f"WHERE (var_id = ? OR LOWER(variabelnamn) = LOWER(?)) "
            f"AND register_id IN ({ph})",
            [int_variable, variable, *reg_ids],
        ).fetchall()
    else:
        var_rows = conn.execute(
            "SELECT register_id, var_id FROM variable "
            "WHERE var_id = ? OR LOWER(variabelnamn) = LOWER(?)",
            (int_variable, variable),
        ).fetchall()

    if not var_rows:
        raise RegmetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"No variable matching '{variable}'"
            + (f" in register '{register}'" if register else "")
            + ".",
            remediation="Use `regmeta search` to find variable names or IDs.",
        )

    results: list[dict[str, Any]] = []
    seen: set[str] = set()
    for vr in var_rows:
        rows = conn.execute(
            "SELECT DISTINCT va.kolumnnamn, vi.register_id, vi.regvar_id, vi.regver_id, "
            "r.registernamn, rver.registerversionnamn "
            "FROM variable_alias va "
            "JOIN variable_instance vi ON va.cvid = vi.cvid "
            "JOIN register r ON vi.register_id = r.register_id "
            "JOIN register_version rver ON vi.regver_id = rver.regver_id "
            "WHERE vi.register_id = ? AND vi.var_id = ? "
            "ORDER BY va.kolumnnamn, r.registernamn",
            (vr["register_id"], vr["var_id"]),
        ).fetchall()
        for r in rows:
            key = f"{r['kolumnnamn']}:{r['register_id']}:{r['regver_id']}"
            if key in seen:
                continue
            seen.add(key)
            results.append(
                {
                    "kolumnnamn": r["kolumnnamn"],
                    "register_id": r["register_id"],
                    "register_name": r["registernamn"],
                    "regvar_id": r["regvar_id"],
                    "regver_id": r["regver_id"],
                    "version_name": r["registerversionnamn"],
                }
            )

    return results


# ---------------------------------------------------------------------------
# Get diff
# ---------------------------------------------------------------------------


def _find_version_for_year(
    conn: sqlite3.Connection, regvar_id: str, year: int
) -> dict[str, Any] | None:
    """Find the version matching a year: exact first, then latest ≤ year."""
    versions = conn.execute(
        "SELECT regver_id, registerversionnamn FROM register_version "
        "WHERE regvar_id = ? ORDER BY regver_id",
        (regvar_id,),
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
    conn: sqlite3.Connection, regver_id: str
) -> dict[str, dict[str, Any]]:
    """Fetch columns for a version, keyed by var_id."""
    rows = conn.execute(
        "SELECT vi.var_id, vi.datatyp, vi.datalangd, v.variabelnamn, "
        "GROUP_CONCAT(va.kolumnnamn, ', ') as aliases "
        "FROM variable_instance vi "
        "JOIN variable v ON vi.register_id = v.register_id AND vi.var_id = v.var_id "
        "LEFT JOIN variable_alias va ON vi.cvid = va.cvid "
        "WHERE vi.regver_id = ? "
        "GROUP BY vi.var_id ORDER BY vi.var_id",
        (regver_id,),
    ).fetchall()
    result: dict[str, dict[str, Any]] = {}
    for r in rows:
        aliases = sorted(r["aliases"].split(", ")) if r["aliases"] else []
        result[r["var_id"]] = {
            "var_id": r["var_id"],
            "variabelnamn": r["variabelnamn"],
            "datatyp": r["datatyp"],
            "datalangd": r["datalangd"],
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
        "SELECT register_id, registernamn FROM register WHERE register_id = ?",
        (reg_ids[0],),
    ).fetchone()

    if variant:
        variant_rows = conn.execute(
            f"SELECT * FROM register_variant WHERE register_id IN ({_in_placeholders(reg_ids)}) "
            "AND regvar_id = ?",
            [*reg_ids, _try_int(variant)],
        ).fetchall()
    else:
        variant_rows = conn.execute(
            f"SELECT * FROM register_variant WHERE register_id IN ({_in_placeholders(reg_ids)}) "
            "ORDER BY regvar_id",
            reg_ids,
        ).fetchall()

    # Resolve each variable input to var_ids, tracking name mapping
    filter_var_ids: set[str] | None = None
    var_id_to_name: dict[str, str] = {}
    var_id_to_input: dict[str, str] = {}
    if variables:
        filter_var_ids = set()
        ph = _in_placeholders(reg_ids)
        for v in variables:
            rows = conn.execute(
                f"SELECT var_id, variabelnamn FROM variable "
                f"WHERE (var_id = ? OR LOWER(variabelnamn) = LOWER(?)) "
                f"AND register_id IN ({ph})",
                [_try_int(v), v, *reg_ids],
            ).fetchall()
            if not rows:
                rows = conn.execute(
                    f"SELECT DISTINCT vi.var_id, var.variabelnamn "
                    f"FROM variable_alias va "
                    f"JOIN variable_instance vi ON va.cvid = vi.cvid "
                    f"JOIN variable var ON vi.register_id = var.register_id AND vi.var_id = var.var_id "
                    f"WHERE LOWER(va.kolumnnamn) = LOWER(?) AND vi.register_id IN ({ph})",
                    [v, *reg_ids],
                ).fetchall()
            for r in rows:
                filter_var_ids.add(r["var_id"])
                var_id_to_name[r["var_id"]] = r["variabelnamn"]
                var_id_to_input[r["var_id"]] = v

        if not filter_var_ids:
            names = ", ".join(f"'{v}'" for v in variables)
            raise RegmetaError(
                exit_code=EXIT_NOT_FOUND,
                code="not_found",
                error_class="query",
                message=f"No variables matching {names} in register '{register}'.",
                remediation="Use `regmeta search --query <term>` to find variables.",
            )

    variants_out: list[dict[str, Any]] = []
    unchanged_by_var: dict[str, list[str]] = {}
    changed_any_variant: set[str] = set()
    any_versions_found = False

    for rv in variant_rows:
        rvid = rv["regvar_id"]
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
            for field in ("datatyp", "datalangd", "aliases"):
                if fc[field] != tc[field]:
                    changes.append({"field": field, "from": fc[field], "to": tc[field]})
            if changes:
                changed.append(
                    {
                        "var_id": vid,
                        "variabelnamn": tc["variabelnamn"],
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
                    unchanged_by_var.setdefault(vid, []).append(
                        rv["registervariantnamn"]
                    )

            added = [a for a in added if a["var_id"] in filter_var_ids]
            removed = [r for r in removed if r["var_id"] in filter_var_ids]
            changed = [c for c in changed if c["var_id"] in filter_var_ids]

        if not added and not removed and not changed:
            continue

        variants_out.append(
            {
                "regvar_id": rvid,
                "variant_name": rv["registervariantnamn"],
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
        raise RegmetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"No versions found for register '{register}' between years {from_year} and {to_year}.",
            remediation="Use `regmeta get schema --register <name>` to see available versions.",
        )

    result: dict[str, Any] = {
        "register_id": reg["register_id"],
        "register_name": reg["registernamn"],
        "from_year": from_year,
        "to_year": to_year,
        "variants": variants_out,
    }
    if var_id_to_input:
        result["resolved_variables"] = [
            {
                "input": var_id_to_input[vid],
                "variabelnamn": var_id_to_name[vid],
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
    reg_ids: list[str] | None = None
    if register:
        reg_ids = require_register_ids(conn, register)

    int_variable = _try_int(variable)
    if reg_ids:
        ph = _in_placeholders(reg_ids)
        matched = conn.execute(
            f"SELECT v.*, r.registernamn FROM variable v "
            f"JOIN register r ON v.register_id = r.register_id "
            f"WHERE (v.var_id = ? OR LOWER(v.variabelnamn) = LOWER(?)) "
            f"AND v.register_id IN ({ph})",
            [int_variable, variable, *reg_ids],
        ).fetchall()
    else:
        matched = conn.execute(
            "SELECT v.*, r.registernamn FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "WHERE v.var_id = ? OR LOWER(v.variabelnamn) = LOWER(?)",
            (int_variable, variable),
        ).fetchall()

    if not matched:
        raise RegmetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"No variable matching '{variable}'"
            + (f" in register '{register}'" if register else "")
            + ".",
            remediation="Use `regmeta search --query <term>` to find variables.",
        )

    registers_out: list[dict[str, Any]] = []
    total_instances = 0
    with_source = 0

    for var in matched:
        rid, vid = var["register_id"], var["var_id"]
        hamtad = (var["variabelhamtadfran"] or "").strip()
        kalla = (var["variabelregister_kalla"] or "").strip()

        # Resolve source register
        source_register_id: str | None = None
        if kalla:
            resolved = resolve_register_ids(conn, kalla)
            source_register_id = resolved[0] if resolved else None

        # Classify role
        if not kalla and not hamtad:
            role = "unknown"
        elif not kalla or source_register_id == rid:
            role = "source"
        else:
            role = "consumer"

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
        if kalla or hamtad:
            with_source += instance_count

        registers_out.append(
            {
                "register_id": rid,
                "register_name": var["registernamn"],
                "var_id": vid,
                "role": role,
                "variabelhamtadfran": hamtad,
                "variabelregister_kalla": kalla,
                "source_register_id": source_register_id,
                "instance_count": instance_count,
                "year_range": year_range,
            }
        )

    var_name = matched[0]["variabelnamn"]

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
        "SELECT v.variabelnamn, "
        "COUNT(DISTINCT vc.vardekod) as n_distinct_codes, "
        "COUNT(DISTINCT v.register_id) as n_registers, "
        "COUNT(DISTINCT vi.cvid) as n_instances "
        "FROM variable v "
        "JOIN variable_instance vi ON v.register_id = vi.register_id AND v.var_id = vi.var_id "
        "JOIN cvid_value_code cvc ON vi.cvid = cvc.cvid "
        "JOIN value_code vc ON cvc.code_id = vc.code_id "
        "GROUP BY v.variabelnamn "
        "HAVING n_distinct_codes >= ? AND n_registers >= ? "
        "ORDER BY n_registers DESC, n_distinct_codes DESC "
        "LIMIT ?",
        (min_codes, min_registers, limit),
    ).fetchall()
    return [
        {
            "variable_name": r["variabelnamn"],
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
    reg_ids: list[str] | None = None
    if register:
        reg_ids = require_register_ids(conn, register)

    results: list[dict[str, Any]] = []

    for col in columns:
        col_lower = col.lower()

        if reg_ids:
            ph = _in_placeholders(reg_ids)
            exact_rows = conn.execute(
                f"SELECT va.kolumnnamn, vi.register_id, vi.var_id, v.variabelnamn "
                f"FROM variable_alias va "
                f"JOIN variable_instance vi ON va.cvid = vi.cvid "
                f"JOIN variable v ON vi.register_id = v.register_id AND vi.var_id = v.var_id "
                f"WHERE LOWER(va.kolumnnamn) = ? AND vi.register_id IN ({ph}) "
                f"GROUP BY vi.register_id, vi.var_id "
                f"ORDER BY vi.register_id, vi.var_id",
                [col_lower, *reg_ids],
            ).fetchall()
        else:
            exact_rows = conn.execute(
                "SELECT va.kolumnnamn, vi.register_id, vi.var_id, v.variabelnamn "
                "FROM variable_alias va "
                "JOIN variable_instance vi ON va.cvid = vi.cvid "
                "JOIN variable v ON vi.register_id = v.register_id AND vi.var_id = v.var_id "
                "WHERE LOWER(va.kolumnnamn) = ? "
                "GROUP BY vi.register_id, vi.var_id "
                "ORDER BY vi.register_id, vi.var_id",
                (col_lower,),
            ).fetchall()

        matches = [
            {
                "var_id": r["var_id"],
                "variable_name": r["variabelnamn"],
                "matched_column": r["kolumnnamn"],
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
