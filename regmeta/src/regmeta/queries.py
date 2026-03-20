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


# ---------------------------------------------------------------------------
# Register lookup
# ---------------------------------------------------------------------------


def resolve_register_ids(conn: sqlite3.Connection, value: str) -> list[str]:
    """Resolve a register name or ID to a list of register_ids.

    Tries: exact ID → case-insensitive name → substring match.
    Returns empty list if nothing found.
    """
    row = conn.execute(
        "SELECT register_id FROM register WHERE register_id = ?", (value,)
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
        "SELECT DISTINCT val.vardekod, val.vardebenamning, "
        "vi.register_id, vi.var_id, v.variabelnamn, r.registernamn "
        "FROM value_item val "
        "JOIN variable_instance vi ON val.cvid = vi.cvid "
        "JOIN variable v ON vi.register_id = v.register_id AND vi.var_id = v.var_id "
        "JOIN register r ON vi.register_id = r.register_id "
        "WHERE val.vardekod LIKE ? OR val.vardebenamning LIKE ? "
        "ORDER BY val.vardekod "
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
            (regvar_id,),
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

    if reg_ids:
        ph = _in_placeholders(reg_ids)
        vars_by_id = conn.execute(
            f"SELECT * FROM variable WHERE var_id = ? AND register_id IN ({ph})",
            [variable, *reg_ids],
        ).fetchall()
        vars_by_name = conn.execute(
            f"SELECT * FROM variable WHERE LOWER(variabelnamn) = LOWER(?) AND register_id IN ({ph})",
            [variable, *reg_ids],
        ).fetchall()
    else:
        vars_by_id = conn.execute(
            "SELECT * FROM variable WHERE var_id = ?", (variable,)
        ).fetchall()
        vars_by_name = conn.execute(
            "SELECT * FROM variable WHERE LOWER(variabelnamn) = LOWER(?)",
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

        reg = conn.execute(
            "SELECT registernamn FROM register WHERE register_id = ?", (rid,)
        ).fetchone()

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

        instances_out: list[dict[str, Any]] = []
        for inst in instances:
            aliases = conn.execute(
                "SELECT kolumnnamn FROM variable_alias WHERE cvid = ? ORDER BY kolumnnamn",
                (inst["cvid"],),
            ).fetchall()
            value_count = conn.execute(
                "SELECT COUNT(*) FROM value_item WHERE cvid = ?",
                (inst["cvid"],),
            ).fetchone()[0]

            instances_out.append(
                {
                    "cvid": inst["cvid"],
                    "regvar_id": inst["regvar_id"],
                    "variant_name": inst["registervariantnamn"],
                    "regver_id": inst["regver_id"],
                    "version_name": inst["registerversionnamn"],
                    "year": extract_year(inst["registerversionnamn"] or ""),
                    "datatyp": inst["datatyp"],
                    "datalangd": inst["datalangd"],
                    "aliases": [a["kolumnnamn"] for a in aliases],
                    "value_set_count": value_count,
                }
            )

        variables_out.append(
            {
                "register_id": rid,
                "register_name": reg["registernamn"] if reg else None,
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


def get_values(conn: sqlite3.Connection, cvid: str) -> list[dict[str, Any]]:
    """Get value-set members for a CVID."""
    inst = conn.execute(
        "SELECT * FROM variable_instance WHERE cvid = ?", (cvid,)
    ).fetchone()
    if not inst:
        raise RegmetaError(
            exit_code=EXIT_NOT_FOUND,
            code="not_found",
            error_class="query",
            message=f"Variable instance (CVID) {cvid} not found.",
            remediation="Use `regmeta get schema` to find valid CVIDs.",
        )

    values = conn.execute(
        "SELECT vardekod, vardebenamning, vardemangdsversion, vardemangdsniva "
        "FROM value_item WHERE cvid = ? ORDER BY vardekod",
        (cvid,),
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
    if reg_ids:
        ph = _in_placeholders(reg_ids)
        var_rows = conn.execute(
            f"SELECT register_id, var_id FROM variable "
            f"WHERE (var_id = ? OR LOWER(variabelnamn) = LOWER(?)) "
            f"AND register_id IN ({ph})",
            [variable, variable, *reg_ids],
        ).fetchall()
    else:
        var_rows = conn.execute(
            "SELECT register_id, var_id FROM variable "
            "WHERE var_id = ? OR LOWER(variabelnamn) = LOWER(?)",
            (variable, variable),
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
        "COUNT(DISTINCT val.vardekod) as n_distinct_codes, "
        "COUNT(DISTINCT v.register_id) as n_registers, "
        "COUNT(DISTINCT vi.cvid) as n_instances "
        "FROM variable v "
        "JOIN variable_instance vi ON v.register_id = vi.register_id AND v.var_id = vi.var_id "
        "JOIN value_item val ON vi.cvid = val.cvid "
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
