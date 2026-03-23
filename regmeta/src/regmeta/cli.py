"""CLI entry point for regmeta."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import time
from pathlib import Path
from typing import Any

from .db import (
    SCHEMA_VERSION,
    utc_now,
    build_db,
    db_path_from_args,
    get_manifest,
    open_db,
)
from .errors import EXIT_INTERNAL, EXIT_USAGE, RegmetaError
from .queries import (
    get_coded_variables,
    get_datacolumns,
    get_diff,
    get_lineage,
    get_register,
    get_schema,
    get_values,
    get_varinfo,
    resolve,
    search,
)

CONTRACT_VERSION = "3.0.0"


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def _success_envelope(
    *,
    command: str,
    args_payload: dict[str, Any],
    db_info: dict[str, str] | None,
    data: Any,
    duration_ms: int,
) -> dict[str, Any]:
    envelope: dict[str, Any] = {
        "contract_version": CONTRACT_VERSION,
        "generated_at": utc_now(),
        "request": {"command": command, "args": args_payload},
    }
    if db_info:
        envelope["database"] = db_info
    envelope["data"] = data
    envelope["run"] = {"duration_ms": duration_ms}
    return envelope


_MAX_DISPLAY_ROWS = 100


def _write_to(content: str, output_path: str | None, *, truncate: bool = False) -> None:
    if output_path:
        target = Path(output_path).expanduser().resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w" if truncate else "a", encoding="utf-8") as f:
            f.write(content)
    else:
        sys.stdout.write(content)


def _write_json(payload: dict[str, Any], output_path: str | None) -> None:
    content = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if output_path:
        tmp = Path(output_path).expanduser().resolve()
        tmp_file = tmp.with_suffix(tmp.suffix + ".tmp")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        tmp_file.write_text(content, encoding="utf-8")
        tmp_file.replace(tmp)
    else:
        sys.stdout.write(content)


def _terminal_width(output_path: str | None) -> int:
    if output_path:
        return 10_000
    return shutil.get_terminal_size().columns


def _render_table(rows: list[dict[str, Any]], columns: list[str]) -> tuple[str, int]:
    widths = {c: len(c) for c in columns}
    str_rows = []
    for row in rows:
        str_row = {c: str(row.get(c, "")) for c in columns}
        for c in columns:
            widths[c] = max(widths[c], len(str_row[c]))
        str_rows.append(str_row)

    table_width = sum(widths.values()) + 2 * (len(columns) - 1)
    header = "  ".join(c.ljust(widths[c]) for c in columns)
    sep = "  ".join("-" * widths[c] for c in columns)
    lines = [header, sep]
    for sr in str_rows:
        lines.append("  ".join(sr[c].ljust(widths[c]) for c in columns))
    return "\n".join(lines) + "\n", table_width


def _render_list(rows: list[dict[str, Any]], columns: list[str]) -> str:
    key_width = max(len(c) for c in columns)
    lines: list[str] = []
    for i, row in enumerate(rows):
        if i > 0:
            lines.append("")
        for c in columns:
            lines.append(f"  {c.ljust(key_width)}  {row.get(c, '')}")
    return "\n".join(lines) + "\n"


def _write_formatted(
    rows: list[dict[str, Any]],
    columns: list[str],
    output_path: str | None,
    *,
    fmt: str = "table",
) -> None:
    if not rows:
        _write_to("(no results)\n", output_path)
        return

    truncated = 0
    if len(rows) > _MAX_DISPLAY_ROWS:
        truncated = len(rows) - _MAX_DISPLAY_ROWS
        rows = rows[:_MAX_DISPLAY_ROWS]

    if fmt == "list":
        content = _render_list(rows, columns)
    else:
        table_content, table_width = _render_table(rows, columns)
        if table_width > _terminal_width(output_path):
            content = _render_list(rows, columns)
        else:
            content = table_content

    if truncated:
        content += f"\n({truncated} more rows — use --format json for full output)\n"

    _write_to(content, output_path)


def _db_info(conn):
    manifest = get_manifest(conn)
    return {
        "schema_version": manifest.get("schema_version", "unknown"),
        "import_date": manifest.get("import_date", "unknown"),
    }


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


class _NoRepeatParser(argparse.ArgumentParser):
    """ArgumentParser that rejects repeated optional flags."""

    def parse_known_args(self, args=None, namespace=None):
        if args is None:
            args = sys.argv[1:]
        seen: dict[str, str] = {}
        for token in args:
            if token.startswith("-") and "=" not in token:
                if token in seen:
                    self.error(f"{token} may only be specified once")
                seen[token] = token
        return super().parse_known_args(args, namespace)


def _build_parser() -> argparse.ArgumentParser:
    parser = _NoRepeatParser(
        prog="regmeta",
        description=(
            "Search and query SCB registry metadata.\n\n"
            "Requires a database built with: regmeta maintain build-db --csv-dir <path>\n"
            "Default database: ~/.local/share/regmeta/ (override with --db)"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Database directory (default: ~/.local/share/regmeta/)",
    )
    parser.add_argument(
        "--format",
        default="table",
        choices=["table", "list", "json"],
        help="Output format: table (default, auto-switches to list if too wide), list (record blocks), json (machine-readable).",
    )
    parser.add_argument(
        "--output", default=None, help="Write output to file instead of stdout."
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Include envelope metadata (contract version, timing, db info).",
    )

    sub = parser.add_subparsers(dest="command")

    search_p = sub.add_parser(
        "search",
        help="Search registers, variables, columns, and values.",
        description=(
            "Search across metadata fields. By default searches all fields.\n"
            "Use a field flag to narrow: --datacolumn, --varname, --description, --value.\n\n"
            "Examples:\n"
            "  regmeta search --query kommun                  # all fields\n"
            "  regmeta search --query kommun --datacolumn     # column headers only\n"
            "  regmeta search --query 0180 --value            # value codes/labels only"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    search_p.add_argument(
        "--query",
        required=True,
        help="Search term (substring match; FTS for --description).",
    )
    search_field = search_p.add_mutually_exclusive_group()
    search_field.add_argument(
        "--datacolumn",
        dest="field",
        action="store_const",
        const="datacolumn",
        help="Search column headers/aliases in data files only.",
    )
    search_field.add_argument(
        "--varname",
        dest="field",
        action="store_const",
        const="varname",
        help="Search canonical SCB variable names only.",
    )
    search_field.add_argument(
        "--description",
        dest="field",
        action="store_const",
        const="description",
        help="Search variable/register descriptions only (full-text).",
    )
    search_field.add_argument(
        "--value",
        dest="field",
        action="store_const",
        const="value",
        help="Search value codes and labels only.",
    )
    search_field.add_argument(
        "--all-fields",
        dest="field",
        action="store_const",
        const="all",
        help="Search all fields (default when no field flag given).",
    )
    search_p.set_defaults(field="all")
    search_p.add_argument(
        "--type",
        default="all",
        choices=["register", "variable", "all"],
        help="Filter results by entity type: register or variable (default: all).",
    )
    search_p.add_argument(
        "--register", default=None, help="Filter by register (name or ID)."
    )
    search_p.add_argument(
        "--limit", type=int, default=50, help="Max results (default: 50)."
    )
    search_p.add_argument(
        "--offset", type=int, default=0, help="Skip first N results (default: 0)."
    )

    get_p = sub.add_parser(
        "get",
        help="Retrieve records by type: register, schema, varinfo, values, datacolumns, coded-variables.",
    )
    get_sub = get_p.add_subparsers(dest="get_command")

    get_reg = get_sub.add_parser(
        "register", help="Get register overview with variants."
    )
    get_reg.add_argument("register", help="Register name or numeric ID.")

    get_schema_p = get_sub.add_parser(
        "schema",
        help="Get column listing per version. Provide regvar_id or --register.",
    )
    get_schema_p.add_argument(
        "regvar_id", nargs="?", default=None, help="Register variant ID."
    )
    get_schema_p.add_argument(
        "--register",
        default=None,
        help="Register name or ID (alternative to regvar_id).",
    )
    get_schema_p.add_argument(
        "--years",
        default=None,
        help="Year range filter (e.g. 2010, 2010-2015, 2010-, -2015).",
    )

    get_varinfo_p = get_sub.add_parser(
        "varinfo", help="Get variable details with instance history."
    )
    get_varinfo_p.add_argument("variable", help="Variable name or var_id.")
    get_varinfo_p.add_argument(
        "--register", default=None, help="Filter by register (name or ID)."
    )

    get_values_p = get_sub.add_parser(
        "values", help="Get value-set members (code + label) for a CVID."
    )
    get_values_p.add_argument(
        "cvid", help="CVID (find via: regmeta get varinfo <variable>)."
    )
    get_values_p.add_argument(
        "--valid-at",
        default=None,
        help="ISO date (YYYY-MM-DD). Only return values valid at this date.",
    )

    get_datacols_p = get_sub.add_parser(
        "datacolumns",
        help="Get all column aliases (data file headers) for a variable.",
        description=(
            "List every column name a variable appears under across registers and versions.\n\n"
            "Examples:\n"
            "  regmeta get datacolumns Kommun\n"
            '  regmeta get datacolumns "Kön" --register LISA'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    get_datacols_p.add_argument("variable", help="Variable name or var_id.")
    get_datacols_p.add_argument(
        "--register", default=None, help="Filter by register (name or ID)."
    )

    get_coded_p = get_sub.add_parser(
        "coded-variables",
        help="List variables that have value sets, ranked by usage.",
        description=(
            "Find categorical variables with coded value sets in the database.\n\n"
            "Examples:\n"
            "  regmeta get coded-variables --min-registers 5\n"
            "  regmeta get coded-variables --min-codes 50 --min-registers 10"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    get_coded_p.add_argument(
        "--min-codes",
        type=int,
        default=1,
        help="Minimum distinct value codes (default: 1).",
    )
    get_coded_p.add_argument(
        "--min-registers",
        type=int,
        default=1,
        help="Minimum registers using this variable (default: 1).",
    )
    get_coded_p.add_argument(
        "--limit", type=int, default=100, help="Max results (default: 100)."
    )

    get_diff_p = get_sub.add_parser(
        "diff",
        help="Compare a register's schema between two years.",
        description=(
            "Show added, removed, and changed variables between two years.\n\n"
            "Examples:\n"
            "  regmeta get diff --register LISA --from 2015 --to 2020\n"
            "  regmeta get diff --register LISA --from 2015 --to 2020 --variable Kon"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    get_diff_p.add_argument(
        "--register", required=True, help="Register name or numeric ID."
    )
    get_diff_p.add_argument(
        "--from",
        dest="from_year",
        type=int,
        required=True,
        help="Start year (4-digit).",
    )
    get_diff_p.add_argument(
        "--to", dest="to_year", type=int, required=True, help="End year (4-digit)."
    )
    get_diff_p.add_argument(
        "--variant", default=None, help="Filter by register variant ID (regvar_id)."
    )
    get_diff_p.add_argument(
        "--variable",
        nargs="+",
        default=None,
        help="Filter to one or more variables (name, var_id, or alias).",
    )

    get_lineage_p = get_sub.add_parser(
        "lineage",
        help="Show cross-register variable provenance.",
        description=(
            "Show where a variable originates and which registers consume it.\n\n"
            "Examples:\n"
            "  regmeta get lineage Kon\n"
            "  regmeta get lineage Kon --register LISA"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    get_lineage_p.add_argument("variable", help="Variable name or var_id.")
    get_lineage_p.add_argument(
        "--register", default=None, help="Filter by register (name or ID)."
    )

    resolve_p = sub.add_parser(
        "resolve", help="Resolve column names to variables (exact alias lookup)."
    )
    resolve_p.add_argument(
        "--columns",
        default=None,
        help="Comma-separated column names. If omitted, reads JSON array from stdin.",
    )
    resolve_p.add_argument(
        "--register", default=None, help="Filter by register (name or ID)."
    )
    resolve_p.add_argument(
        "--require-match",
        action="store_true",
        help="Fail (exit 17) if any column has no matches.",
    )

    maintain_p = sub.add_parser("maintain", help="Setup and maintenance commands.")
    maintain_sub = maintain_p.add_subparsers(dest="maintain_command")

    build_p = maintain_sub.add_parser(
        "build-db", help="Build database from SCB CSV exports."
    )
    build_p.add_argument(
        "--csv-dir", required=True, help="Directory containing SCB CSV exports."
    )

    maintain_sub.add_parser("info", help="Database stats and import metadata.")

    return parser


# ---------------------------------------------------------------------------
# Command handlers (thin wrappers around queries.py)
# ---------------------------------------------------------------------------


def _cmd_maintain_build_db(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db_dir = Path(args.db) if args.db else Path("~/.local/share/regmeta")
    result = build_db(csv_dir=Path(args.csv_dir), db_dir=db_dir)
    duration_ms = int((time.perf_counter() - start) * 1000)
    return _success_envelope(
        command="maintain build-db",
        args_payload={"csv_dir": args.csv_dir},
        db_info={
            "schema_version": SCHEMA_VERSION,
            "import_date": result["import_date"],
        },
        data=result,
        duration_ms=duration_ms,
    ), 0


def _cmd_maintain_info(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db = db_path_from_args(args.db)
    conn = open_db(db)
    try:
        manifest = get_manifest(conn)
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%_fts%' "
                "ORDER BY name"
            ).fetchall()
        ]
        table_counts = {}
        for t in tables:
            count = conn.execute(f'SELECT MAX(rowid) FROM "{t}"').fetchone()[0]  # noqa: S608
            table_counts[t] = count or 0
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    return _success_envelope(
        command="maintain info",
        args_payload={},
        db_info={
            "schema_version": manifest.get("schema_version", "unknown"),
            "import_date": manifest.get("import_date", "unknown"),
        },
        data={"manifest": manifest, "table_counts": table_counts, "db_path": str(db)},
        duration_ms=duration_ms,
    ), 0


def _cmd_search(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db = db_path_from_args(args.db)
    conn = open_db(db)
    try:
        info = _db_info(conn)
        data = search(
            conn,
            args.query,
            field=args.field,
            type=args.type,
            register=args.register,
            limit=args.limit,
            offset=args.offset,
        )
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    return _success_envelope(
        command="search",
        args_payload={
            "query": args.query,
            "field": args.field,
            "type": args.type,
            "register": args.register,
            "limit": args.limit,
            "offset": args.offset,
        },
        db_info=info,
        data=data,
        duration_ms=duration_ms,
    ), 0


def _cmd_get_register(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db = db_path_from_args(args.db)
    conn = open_db(db)
    try:
        info = _db_info(conn)
        registers = get_register(conn, args.register)
        data = registers[0] if len(registers) == 1 else {"registers": registers}
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    return _success_envelope(
        command="get register",
        args_payload={"register": args.register},
        db_info=info,
        data=data,
        duration_ms=duration_ms,
    ), 0


def _cmd_get_schema(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db = db_path_from_args(args.db)
    conn = open_db(db)
    try:
        info = _db_info(conn)
        data = get_schema(
            conn,
            regvar_id=args.regvar_id,
            register=args.register,
            years=args.years,
        )
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    args_out: dict[str, Any] = {}
    if args.regvar_id:
        args_out["regvar_id"] = args.regvar_id
    if args.register:
        args_out["register"] = args.register
    if args.years:
        args_out["years"] = args.years
    return _success_envelope(
        command="get schema",
        args_payload=args_out,
        db_info=info,
        data=data,
        duration_ms=duration_ms,
    ), 0


def _cmd_get_varinfo(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db = db_path_from_args(args.db)
    conn = open_db(db)
    try:
        info = _db_info(conn)
        variables = get_varinfo(conn, args.variable, register=args.register)
        data = variables[0] if len(variables) == 1 else {"variables": variables}
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    return _success_envelope(
        command="get varinfo",
        args_payload={"variable": args.variable, "register": args.register},
        db_info=info,
        data=data,
        duration_ms=duration_ms,
    ), 0


_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _cmd_get_values(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    if args.valid_at and not _ISO_DATE_RE.match(args.valid_at):
        raise RegmetaError(
            exit_code=EXIT_USAGE,
            code="bad_date",
            error_class="usage",
            message=f"Invalid date format: {args.valid_at!r}",
            remediation="Use ISO format: YYYY-MM-DD (e.g. 2020-01-15).",
        )
    start = time.perf_counter()
    db = db_path_from_args(args.db)
    conn = open_db(db)
    try:
        info = _db_info(conn)
        data = get_values(conn, args.cvid, valid_at=args.valid_at)
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    args_payload: dict[str, Any] = {"cvid": args.cvid}
    if args.valid_at:
        args_payload["valid_at"] = args.valid_at
    return _success_envelope(
        command="get values",
        args_payload=args_payload,
        db_info=info,
        data=data,
        duration_ms=duration_ms,
    ), 0


def _cmd_get_datacolumns(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db = db_path_from_args(args.db)
    conn = open_db(db)
    try:
        info = _db_info(conn)
        data = get_datacolumns(conn, args.variable, register=args.register)
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    return _success_envelope(
        command="get datacolumns",
        args_payload={"variable": args.variable, "register": args.register},
        db_info=info,
        data=data,
        duration_ms=duration_ms,
    ), 0


def _cmd_get_coded_variables(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db = db_path_from_args(args.db)
    conn = open_db(db)
    try:
        info = _db_info(conn)
        data = get_coded_variables(
            conn,
            min_codes=args.min_codes,
            min_registers=args.min_registers,
            limit=args.limit,
        )
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    return _success_envelope(
        command="get coded-variables",
        args_payload={
            "min_codes": args.min_codes,
            "min_registers": args.min_registers,
            "limit": args.limit,
        },
        db_info=info,
        data=data,
        duration_ms=duration_ms,
    ), 0


def _cmd_get_diff(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    if args.from_year >= args.to_year:
        raise RegmetaError(
            exit_code=EXIT_USAGE,
            code="usage_error",
            error_class="usage",
            message=f"--from ({args.from_year}) must be less than --to ({args.to_year}).",
            remediation="Swap the year values.",
        )
    start = time.perf_counter()
    db = db_path_from_args(args.db)
    conn = open_db(db)
    try:
        info = _db_info(conn)
        data = get_diff(
            conn,
            register=args.register,
            from_year=args.from_year,
            to_year=args.to_year,
            variant=args.variant,
            variables=args.variable,
        )
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    args_payload: dict[str, Any] = {
        "register": args.register,
        "from_year": args.from_year,
        "to_year": args.to_year,
    }
    if args.variant:
        args_payload["variant"] = args.variant
    if args.variable:
        args_payload["variable"] = args.variable
    return _success_envelope(
        command="get diff",
        args_payload=args_payload,
        db_info=info,
        data=data,
        duration_ms=duration_ms,
    ), 0


def _cmd_get_lineage(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db = db_path_from_args(args.db)
    conn = open_db(db)
    try:
        info = _db_info(conn)
        data = get_lineage(conn, args.variable, register=args.register)
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    args_payload: dict[str, Any] = {"variable": args.variable}
    if args.register:
        args_payload["register"] = args.register
    return _success_envelope(
        command="get lineage",
        args_payload=args_payload,
        db_info=info,
        data=data,
        duration_ms=duration_ms,
    ), 0


def _cmd_resolve(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    from .errors import EXIT_NO_MATCH

    start = time.perf_counter()

    columns: list[str] = []
    if args.columns:
        columns = [c.strip() for c in args.columns.split(",") if c.strip()]
    else:
        raw = sys.stdin.read().strip()
        if raw:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                columns = [item for item in parsed if isinstance(item, str)]

    if not columns:
        raise RegmetaError(
            exit_code=EXIT_USAGE,
            code="usage_error",
            error_class="usage",
            message="No columns provided.",
            remediation="Use --columns or pass JSON array of strings on stdin.",
        )

    db = db_path_from_args(args.db)
    conn = open_db(db)
    try:
        info = _db_info(conn)
        results = resolve(conn, columns, register=args.register)
    finally:
        conn.close()

    if args.require_match and any(r["status"] == "no_match" for r in results):
        raise RegmetaError(
            exit_code=EXIT_NO_MATCH,
            code="no_match",
            error_class="validation",
            message="One or more columns had no matches.",
            remediation="Check column names or provide --register.",
        )

    duration_ms = int((time.perf_counter() - start) * 1000)
    return _success_envelope(
        command="resolve",
        args_payload={"columns": columns, "register": args.register},
        db_info=info,
        data={"columns": results},
        duration_ms=duration_ms,
    ), 0


# ---------------------------------------------------------------------------
# Table output
# ---------------------------------------------------------------------------


def _write_payload(
    key: tuple[str, str | None],
    payload: dict[str, Any],
    output_path: str | None,
    *,
    fmt: str = "table",
) -> None:
    # Truncate output file so multi-section commands (diff, lineage) append correctly
    _write_to("", output_path, truncate=True)
    data = payload.get("data", {})

    # Pick columns based on what result types are in the payload
    if key == ("search", None):
        results = data.get("results", [])
        types = {r.get("type") for r in results}
        if types == {"datacolumn"}:
            cols = [
                "datacolumn",
                "register_id",
                "register_name",
                "var_id",
                "variable_name",
            ]
        elif types == {"value"}:
            cols = [
                "vardekod",
                "vardebenamning",
                "register_id",
                "var_id",
                "variable_name",
            ]
        elif types == {"varname"}:
            cols = ["variable_name", "register_id", "register_name", "var_id"]
        else:
            cols = ["type", "register_id", "register_name", "var_id", "variable_name"]
        _write_formatted(results, cols, output_path, fmt=fmt)
    elif key == ("get", "register"):
        regs = data.get("registers", [data]) if "registers" in data else [data]
        rows = []
        for r in regs:
            for v in r.get("variants", []):
                rows.append(
                    {
                        "register_id": r["register_id"],
                        "register_name": r["registernamn"],
                        "regvar_id": v["regvar_id"],
                        "variant_name": v.get("registervariantnamn", ""),
                    }
                )
        _write_formatted(
            rows,
            ["register_id", "register_name", "regvar_id", "variant_name"],
            output_path,
            fmt=fmt,
        )
    elif key == ("get", "schema"):
        rows = []
        for v in data.get("variants", []):
            for ver in v.get("versions", []):
                for col in ver.get("columns", []):
                    rows.append(
                        {
                            "version": ver.get("version_name", ""),
                            "var_id": col.get("var_id", ""),
                            "variabelnamn": col.get("variabelnamn", ""),
                            "datatyp": col.get("datatyp", ""),
                            "aliases": col.get("aliases", ""),
                            "cvid": col.get("cvid", ""),
                        }
                    )
        _write_formatted(
            rows,
            ["version", "var_id", "variabelnamn", "datatyp", "aliases", "cvid"],
            output_path,
            fmt=fmt,
        )
    elif key == ("get", "varinfo"):
        variables = data.get("variables", [data]) if "variables" in data else [data]
        rows = []
        for v in variables:
            for inst in v.get("instances", []):
                rows.append(
                    {
                        "register_id": v.get("register_id", ""),
                        "var_id": v.get("var_id", ""),
                        "variabelnamn": v.get("variabelnamn", ""),
                        "version": inst.get("version_name", ""),
                        "cvid": inst.get("cvid", ""),
                        "datatyp": inst.get("datatyp", ""),
                        "aliases": ", ".join(inst.get("aliases", [])),
                        "values": inst.get("value_set_count", 0),
                    }
                )
        _write_formatted(
            rows,
            [
                "register_id",
                "var_id",
                "variabelnamn",
                "version",
                "cvid",
                "datatyp",
                "aliases",
                "values",
            ],
            output_path,
            fmt=fmt,
        )
    elif key == ("get", "values"):
        _write_formatted(
            data if isinstance(data, list) else [],
            ["vardekod", "vardebenamning"],
            output_path,
            fmt=fmt,
        )
    elif key == ("get", "datacolumns"):
        _write_formatted(
            data if isinstance(data, list) else [],
            ["kolumnnamn", "register_id", "register_name", "version_name"],
            output_path,
            fmt=fmt,
        )
    elif key == ("get", "coded-variables"):
        _write_formatted(
            data if isinstance(data, list) else [],
            ["variable_name", "n_distinct_codes", "n_registers", "n_instances"],
            output_path,
            fmt=fmt,
        )
    elif key == ("get", "diff"):
        rows = []
        for v in data.get("variants", []):
            for item in v.get("added", []):
                rows.append(
                    {
                        "variant": v.get("variant_name", ""),
                        "change": "+",
                        "var_id": item["var_id"],
                        "variabelnamn": item["variabelnamn"],
                        "detail": f"{item['datatyp']}  {item.get('aliases', [])}",
                    }
                )
            for item in v.get("removed", []):
                rows.append(
                    {
                        "variant": v.get("variant_name", ""),
                        "change": "-",
                        "var_id": item["var_id"],
                        "variabelnamn": item["variabelnamn"],
                        "detail": f"{item['datatyp']}  {item.get('aliases', [])}",
                    }
                )
            for item in v.get("changed", []):
                details = "; ".join(
                    f"{c['field']}: {c['from']} → {c['to']}" for c in item["changes"]
                )
                rows.append(
                    {
                        "variant": v.get("variant_name", ""),
                        "change": "~",
                        "var_id": item["var_id"],
                        "variabelnamn": item["variabelnamn"],
                        "detail": details,
                    }
                )
        resolved = data.get("resolved_variables", [])
        if resolved:
            lines = ["Resolved variables:"]
            for rv in resolved:
                if rv["input"].lower() != rv["variabelnamn"].lower():
                    lines.append(
                        f"  {rv['input']} → {rv['variabelnamn']} (var_id {rv['var_id']})"
                    )
                else:
                    lines.append(f"  {rv['variabelnamn']} (var_id {rv['var_id']})")
            _write_to("\n".join(lines) + "\n\n", output_path)
        _write_formatted(
            rows,
            ["variant", "change", "var_id", "variabelnamn", "detail"],
            output_path,
            fmt=fmt,
        )
        unchanged = data.get("unchanged", [])
        if unchanged:
            _write_to(f"\nUnchanged: {', '.join(unchanged)}\n", output_path)
    elif key == ("get", "lineage"):
        rows = []
        for r in data.get("registers", []):
            source_info = ""
            if r["role"] == "consumer" and r.get("variabelregister_kalla"):
                source_info = f"← {r['variabelregister_kalla']}"
            yr = r.get("year_range", [])
            year_str = f"{yr[0]}-{yr[1]}" if len(yr) == 2 else ""
            rows.append(
                {
                    "register": f"{r['register_name']} ({r['register_id']})",
                    "var_id": r["var_id"],
                    "role": r["role"],
                    "instances": str(r["instance_count"]),
                    "years": year_str,
                    "source": source_info,
                }
            )
        _write_formatted(
            rows,
            ["register", "var_id", "role", "instances", "years", "source"],
            output_path,
            fmt=fmt,
        )
        cov = data.get("provenance_coverage", {})
        if cov.get("total"):
            pct = round(100 * cov["with_source"] / cov["total"])
            _write_to(
                f"\nProvenance: {cov['with_source']}/{cov['total']} ({pct}%)\n",
                output_path,
            )
    elif key == ("resolve", None):
        rows = []
        for col in data.get("columns", []):
            if col["matches"]:
                for m in col["matches"]:
                    rows.append(
                        {
                            "column": col["column_name"],
                            "status": col["status"],
                            "register_id": m.get("register_id", ""),
                            "var_id": m.get("var_id", ""),
                            "variable_name": m.get("variable_name", ""),
                        }
                    )
            else:
                rows.append(
                    {
                        "column": col["column_name"],
                        "status": col["status"],
                        "register_id": "",
                        "var_id": "",
                        "variable_name": "",
                    }
                )
        _write_formatted(
            rows,
            ["column", "status", "register_id", "var_id", "variable_name"],
            output_path,
            fmt=fmt,
        )
    else:
        _write_json(payload, output_path)


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


COMMAND_DISPATCH = {
    ("maintain", "build-db"): _cmd_maintain_build_db,
    ("maintain", "info"): _cmd_maintain_info,
    ("search", None): _cmd_search,
    ("get", "register"): _cmd_get_register,
    ("get", "schema"): _cmd_get_schema,
    ("get", "varinfo"): _cmd_get_varinfo,
    ("get", "values"): _cmd_get_values,
    ("get", "datacolumns"): _cmd_get_datacolumns,
    ("get", "coded-variables"): _cmd_get_coded_variables,
    ("get", "diff"): _cmd_get_diff,
    ("get", "lineage"): _cmd_get_lineage,
    ("resolve", None): _cmd_resolve,
}


def run(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else EXIT_USAGE

    if not args.command:
        parser.print_help(sys.stderr)
        return EXIT_USAGE

    sub_command = None
    if args.command == "maintain":
        sub_command = getattr(args, "maintain_command", None)
        if not sub_command:
            parser.parse_args(["maintain", "--help"])
            return EXIT_USAGE
    elif args.command == "get":
        sub_command = getattr(args, "get_command", None)
        if not sub_command:
            parser.parse_args(["get", "--help"])
            return EXIT_USAGE

    key = (args.command, sub_command)
    handler = COMMAND_DISPATCH.get(key)
    if not handler:
        sys.stderr.write(f"Unknown command: {args.command} {sub_command or ''}\n")
        return EXIT_USAGE

    fmt = getattr(args, "format", "table")
    verbose = getattr(args, "verbose", False)
    output_path = getattr(args, "output", None)

    try:
        payload, exit_code = handler(args)
        if fmt == "json":
            if verbose:
                _write_json(payload, output_path)
            else:
                _write_json(payload.get("data", payload), output_path)
        else:
            _write_payload(key, payload, output_path, fmt=fmt)
        return exit_code
    except RegmetaError as exc:
        _write_json({"error": exc.to_dict()}, getattr(args, "output", None))
        return exc.exit_code
    except Exception as exc:
        error_payload = {
            "error": {
                "code": "internal_error",
                "class": "internal",
                "message": str(exc),
                "remediation": "Report this error to maintainers.",
            }
        }
        try:
            _write_json(error_payload, getattr(args, "output", None))
        except Exception:
            sys.stderr.write(json.dumps(error_payload) + "\n")
        return EXIT_INTERNAL


def main() -> int:
    return run()
