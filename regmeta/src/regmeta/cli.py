"""CLI entry point for regmeta."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

from .db import (
    SCHEMA_VERSION,
    default_db_dir,
    utc_now,
    build_db,
    db_path_from_args,
    get_manifest,
    open_db,
)
from .errors import EXIT_CONFIG, EXIT_INTERNAL, EXIT_NOT_FOUND, EXIT_USAGE, RegmetaError
from .queries import (
    get_availability,
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
_MAX_HINTS = 3


def _hint_add(hints: list[str] | None, msg: str) -> None:
    if hints is not None and len(hints) < _MAX_HINTS:
        hints.append(msg)


def _emit_hints(hints: list[str]) -> None:
    sys.stderr.write("\n")
    for h in hints:
        sys.stderr.write(f"  hint: {h}\n")


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


def _render_table(
    rows: list[dict[str, Any]],
    columns: list[str],
    *,
    max_width: int | None = None,
) -> tuple[str, int]:
    widths = {c: len(c) for c in columns}
    str_rows = []
    for row in rows:
        str_row = {c: str(row.get(c, "")) for c in columns}
        for c in columns:
            widths[c] = max(widths[c], len(str_row[c]))
        str_rows.append(str_row)

    separators = 2 * (len(columns) - 1)
    table_width = sum(widths.values()) + separators

    # Shrink widest columns to fit terminal when max_width is set
    if max_width and table_width > max_width:
        budget = max_width - separators
        min_col = max(8, max(len(c) for c in columns))
        while sum(widths.values()) > budget:
            widest = max(columns, key=lambda c: widths[c])
            if widths[widest] <= min_col:
                break
            widths[widest] = max(
                min_col, budget - sum(w for c, w in widths.items() if c != widest)
            )
        table_width = sum(widths.values()) + separators
        # Truncate cell values that exceed their column width
        for sr in str_rows:
            for c in columns:
                if len(sr[c]) > widths[c]:
                    sr[c] = sr[c][: widths[c] - 1] + "…"

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


def format_rows(
    rows: list[dict[str, Any]],
    columns: list[str],
    *,
    max_width: int | None = None,
) -> str:
    """Render rows as a table or list string.

    Auto-selects list format for ≤5 rows. Truncates wide columns to fit
    max_width when given. Importable by other packages (e.g. mock-data-wizard).
    """
    if not rows:
        return "(no results)\n"
    if len(rows) <= 5:
        return _render_list(rows, columns)
    content, width = _render_table(rows, columns)
    if max_width and width > max_width:
        content, _ = _render_table(rows, columns, max_width=max_width)
    return content


def _write_formatted(
    rows: list[dict[str, Any]],
    columns: list[str],
    output_path: str | None,
    *,
    fmt: str = "table",
    fmt_explicit: bool = False,
    hints: list[str] | None = None,
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
    elif not fmt_explicit and len(rows) <= 5:
        # Few results — list is more readable
        content = _render_list(rows, columns)
    else:
        term_w = _terminal_width(output_path)
        table_content, table_width = _render_table(rows, columns)
        if table_width > term_w:
            table_content, _ = _render_table(rows, columns, max_width=term_w)
            _hint_add(hints, "Long values truncated (--format list for full text)")
            content = table_content
        else:
            content = table_content

    if truncated:
        _hint_add(
            hints,
            f"Table view truncated {truncated} rows (--format json for full output)",
        )

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


_GLOBAL_FLAGS = {
    "--db",
    "--format",
    "--output",
    "-v",
    "--verbose",
    "-q",
    "--quiet",
    "--version",
}
_GLOBAL_FLAGS_WITH_VALUE = {"--db", "--format", "--output"}


def _reorder_global_flags(argv: list[str]) -> list[str]:
    """Move global flags before the subcommand so argparse handles them.

    Handles both ``--flag value`` and ``--flag=value`` syntax.
    """
    front: list[str] = []
    rest: list[str] = []
    i = 0
    while i < len(argv):
        token = argv[i]
        # Handle --flag=value for global flags
        eq_name = token.split("=", 1)[0] if "=" in token else None
        if token in _GLOBAL_FLAGS:
            front.append(token)
            if token in _GLOBAL_FLAGS_WITH_VALUE and i + 1 < len(argv):
                i += 1
                front.append(argv[i])
        elif eq_name in _GLOBAL_FLAGS_WITH_VALUE:
            front.append(token)
        else:
            rest.append(token)
        i += 1
    return front + rest


def _clean_leaf_help(parser: argparse.ArgumentParser) -> None:
    """Hide -h/--help from output, rename 'positional arguments', add epilog."""
    for action in parser._actions:
        if isinstance(action, argparse._HelpAction):
            action.help = argparse.SUPPRESS
            break
    for group in parser._action_groups:
        if group.title == "positional arguments":
            group.title = "Arguments"
            break
    if not parser.epilog:
        cmd = parser.prog.removeprefix("regmeta ")
        parser.epilog = f"Run `regmeta {cmd} --examples` for usage examples."


def _build_parser() -> argparse.ArgumentParser:
    parser = _NoRepeatParser(
        prog="regmeta",
        description="Search and query SCB registry metadata.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )
    parser.add_argument(
        "--db",
        default=None,
        help=f"Database directory (default: {default_db_dir()}).",
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
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        default=False,
        help="Suppress contextual hints on stderr.",
    )
    parser.add_argument(
        "-h", "--help", action="store_true", default=False, help=argparse.SUPPRESS
    )
    parser.add_argument(
        "--version", action="store_true", default=False, help=argparse.SUPPRESS
    )

    sub = parser.add_subparsers(dest="command")

    search_p = sub.add_parser(
        "search",
        help="Search registers, variables, columns, and value codes.",
        description=(
            "Search across metadata. By default searches all fields.\n"
            "Use --field to narrow. Doc results are included and hinted at the bottom.\n"
            "For full documentation search, use: regmeta docs search <query>\n\n"
            "Note: --type and --register do different things:\n"
            "  --type register    Filter results to only show registers (not variables)\n"
            "  --register LISA    Restrict search scope to a specific register\n\n"
            "Examples:\n"
            "  regmeta search --query kommun                        # all fields\n"
            "  regmeta search --query kommun --field datacolumn     # column headers only\n"
            "  regmeta search --query 0180 --field value            # value codes/labels\n"
            "  regmeta search --query utbildning --type register    # find registers\n"
            "  regmeta search --query kommun --register LISA        # within LISA only"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    search_p.add_argument(
        "--query",
        required=True,
        help="Search term (substring match; FTS for --field description).",
    )
    search_p.add_argument(
        "--field",
        default="all",
        choices=["datacolumn", "varname", "description", "value", "all"],
        help="Which fields to search (default: all).",
    )
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
        "--years",
        default=None,
        help="Filter to entries with versions in year range (e.g. 2015, 2015-2024, 2015-, -2024).",
    )
    search_p.add_argument(
        "--limit", type=int, default=50, help="Max results (default: 50)."
    )
    search_p.add_argument(
        "--offset", type=int, default=0, help="Skip first N results (default: 0)."
    )

    get_p = sub.add_parser(
        "get",
        help="Look up registers, schemas, variables, values, lineage, and more.",
    )
    get_sub = get_p.add_subparsers(dest="get_command")

    get_reg = get_sub.add_parser(
        "register",
        help="Get register overview with variants.",
        description=(
            "Show register metadata including all variants (sub-tables),\n"
            "each with regvar_id, name, description, and secrecy level.\n\n"
            "Examples:\n"
            "  regmeta get register LISA\n"
            "  regmeta get register 34"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    get_reg.add_argument(
        "register", metavar="REGISTER", help="Register name or numeric ID."
    )

    get_schema_p = get_sub.add_parser(
        "schema",
        help="Get column listing per version. Provide regvar_id or --register.",
        description=(
            "List columns (aliases, variable names, data types, CVIDs) per\n"
            "register version. Can be verbose for large registers — use\n"
            "--years, --columns-like, --summary, or --flat to narrow.\n\n"
            "Examples:\n"
            "  regmeta get schema --register LISA --years 2022\n"
            "  regmeta get schema 153 --years 2022            # by regvar_id\n"
            '  regmeta get schema --register LISA --columns-like "Merit|Betyg"\n'
            "  regmeta get schema --register LISA --summary    # one row per variant\n"
            "  regmeta get schema --register LISA --flat       # one row per alias"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
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
    get_schema_p.add_argument(
        "--columns-like",
        default=None,
        help="Regex filter on column aliases or variable names (case-insensitive).",
    )
    schema_mode = get_schema_p.add_mutually_exclusive_group()
    schema_mode.add_argument(
        "--summary",
        action="store_true",
        help="Condensed output: one row per variant with year range and column count.",
    )
    schema_mode.add_argument(
        "--flat",
        action="store_true",
        help="Flat output: one row per (year, alias, variable_name, regvar_id).",
    )

    get_varinfo_p = get_sub.add_parser(
        "varinfo",
        help="Get variable details with instance history.",
        description=(
            "Show variable definition, description, and every register version\n"
            "where it appears — with CVIDs, data types, aliases, and value counts.\n\n"
            "Examples:\n"
            '  regmeta get varinfo "Kön"\n'
            "  regmeta get varinfo 44               # by var_id\n"
            '  regmeta get varinfo "Kön" --register LISA'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    get_varinfo_p.add_argument("variable", help="Variable name or var_id.")
    get_varinfo_p.add_argument(
        "--register", default=None, help="Filter by register (name or ID)."
    )

    get_values_p = get_sub.add_parser(
        "values",
        help="Get value-set members (code + label) for a CVID.",
        description=(
            "Show code/label pairs for a categorical variable's value set.\n"
            "Requires a CVID — find it via `regmeta get varinfo <variable>`.\n"
            "Value sets are historical unions; use --valid-at for date filtering.\n\n"
            "Examples:\n"
            "  regmeta get values 1001\n"
            "  regmeta get values 1001 --valid-at 2020-01-01"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
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

    get_avail_p = get_sub.add_parser(
        "availability",
        help="Show temporal availability (years, gaps, aliases) for a variable or register.",
        description=(
            "Show when a variable or register is available across years.\n\n"
            "Auto-detects whether the target is a variable or register.\n\n"
            "Examples:\n"
            '  regmeta get availability "Kön"\n'
            "  regmeta get availability LISA\n"
            '  regmeta get availability "Kön" --register LISA'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    get_avail_p.add_argument("target", help="Variable name/var_id or register name/ID.")
    get_avail_p.add_argument(
        "--register", default=None, help="Scope to a specific register (for variables)."
    )

    resolve_p = sub.add_parser(
        "resolve",
        help="Resolve column names to variables (exact alias lookup).",
        description=(
            "Map data-file column names to official variable definitions.\n"
            "Each column gets status 'matched' or 'no_match'. Matches include\n"
            "var_id, variable_name, and register_id.\n\n"
            "Uses exact alias lookup only — no fuzzy matching. For discovery,\n"
            "use `search --field datacolumn` instead.\n\n"
            "Examples:\n"
            '  regmeta resolve --columns "Kon,FodelseAr,Kommun" --register LISA\n'
            '  echo \'["Kon","FodelseAr"]\' | regmeta resolve --register LISA'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
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

    update_p = maintain_sub.add_parser(
        "update",
        help="Update regmeta package and database to the latest version.",
        description=(
            "Download the latest regmeta package and pre-built database from\n"
            "GitHub Releases. Safe to run repeatedly — skips if already current.\n\n"
            "Examples:\n"
            "  regmeta maintain update            # interactive confirmation\n"
            "  regmeta maintain update --yes      # skip confirmation\n"
            "  regmeta maintain update --force    # re-download even if current"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    update_p.add_argument(
        "--tag", default="latest", help="Target release tag (default: latest)."
    )
    update_p.add_argument(
        "--force", action="store_true", help="Re-download database even if up to date."
    )
    update_p.add_argument(
        "-y", "--yes", action="store_true", help="Skip confirmation prompt."
    )

    maintain_sub.add_parser(
        "info",
        help="Database stats and import metadata.",
        description=(
            "Show database path, schema version, import timestamp, and row\n"
            "counts per table.\n\n"
            "Examples:\n"
            "  regmeta maintain info"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    build_p = maintain_sub.add_parser(
        "build-db",
        help="Build database from SCB CSV exports (maintainer-only).",
        description=(
            "Build the metadata database from raw SCB CSV exports. This\n"
            "replaces the database entirely (not incremental). Most users\n"
            "should use `maintain update` instead.\n\n"
            "Examples:\n"
            "  regmeta maintain build-db --csv-dir /path/to/SCB-data/"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    build_p.add_argument(
        "--csv-dir", required=True, help="Directory containing SCB CSV exports."
    )

    build_docs_p = maintain_sub.add_parser(
        "build-docs",
        help="Rebuild the doc DB from markdown files (maintainer-only).",
        description=(
            "Rebuild the documentation FTS index from markdown files.\n"
            "End users receive the doc DB via `maintain update`; this command\n"
            "is for maintainers rebuilding from a repo checkout before upload.\n\n"
            "Examples:\n"
            "  regmeta maintain build-docs\n"
            "  regmeta maintain build-docs --docs-dir /path/to/docs/"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    build_docs_p.add_argument(
        "--docs-dir",
        default=None,
        help=(
            "Directory containing register doc subdirectories "
            "(default: regmeta/docs/ if run from a repo checkout)."
        ),
    )

    # --- doc command family ---
    doc_p = sub.add_parser(
        "docs",
        help="Search and browse curated register documentation.",
    )
    doc_sub = doc_p.add_subparsers(dest="doc_command")

    doc_search_p = doc_sub.add_parser(
        "search",
        help="Full-text search over documentation.",
        description=(
            "Search curated register documentation (parsed from SCB PDFs).\n"
            "Returns titles, types, topics, and relevance scores.\n\n"
            "Examples:\n"
            "  regmeta docs search inkomst\n"
            "  regmeta docs search sysselsättning --register lisa --type variable"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    doc_search_p.add_argument("query", help="Search query.")
    doc_search_p.add_argument(
        "--type",
        default=None,
        dest="doc_type",
        help="Filter by type tag (variable, methodology, appendix, changelog, overview).",
    )
    doc_search_p.add_argument(
        "--topic",
        default=None,
        help="Filter by topic tag (income, employment, demographic, etc.).",
    )
    doc_search_p.add_argument(
        "--register", default=None, help="Filter by register (e.g. lisa)."
    )
    doc_search_p.add_argument(
        "--limit", type=int, default=20, help="Max results (default: 20)."
    )
    doc_search_p.add_argument(
        "--offset", type=int, default=0, help="Skip first N results."
    )

    doc_get_p = doc_sub.add_parser(
        "get",
        help="Retrieve full documentation for a variable or topic.",
        description=(
            "Show the full markdown content of a documentation entry.\n\n"
            "Examples:\n"
            "  regmeta docs get SyssStat\n"
            "  regmeta docs get _overview"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    doc_get_p.add_argument(
        "identifier", help="Variable name or doc filename (e.g. SyssStat, _overview)."
    )

    doc_list_p = doc_sub.add_parser(
        "list",
        help="Browse available documentation.",
        description=(
            "List available documentation entries. Use filters to narrow.\n\n"
            "Examples:\n"
            "  regmeta docs list\n"
            "  regmeta docs list --register lisa\n"
            "  regmeta docs list --type variable --topic income"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    doc_list_p.add_argument(
        "--type",
        default=None,
        dest="doc_type",
        help="Filter by type tag.",
    )
    doc_list_p.add_argument("--topic", default=None, help="Filter by topic tag.")
    doc_list_p.add_argument("--register", default=None, help="Filter by register.")

    # Clean up help display on all leaf subcommands
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            for sub_p in action.choices.values():
                sub_actions = [
                    a
                    for a in sub_p._actions
                    if isinstance(a, argparse._SubParsersAction)
                ]
                if sub_actions:
                    for leaf_p in sub_actions[0].choices.values():
                        _clean_leaf_help(leaf_p)
                else:
                    _clean_leaf_help(sub_p)

    return parser


# ---------------------------------------------------------------------------
# Command handlers (thin wrappers around queries.py)
# ---------------------------------------------------------------------------


def _cmd_maintain_build_db(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db_dir = Path(args.db) if args.db else default_db_dir()
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
    conn = open_db(db, check_schema=False)
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
            count = conn.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0]  # noqa: S608
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


def _cmd_maintain_update(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    from .update import run_update

    start = time.perf_counter()
    db_dir = Path(args.db) if args.db else None
    result = run_update(db_dir=db_dir, tag=args.tag, force=args.force, yes=args.yes)
    duration_ms = int((time.perf_counter() - start) * 1000)
    return _success_envelope(
        command="maintain update",
        args_payload={"tag": args.tag, "force": args.force},
        db_info=None,
        data=result,
        duration_ms=duration_ms,
    ), 0


# ---------------------------------------------------------------------------
# Doc command handlers
# ---------------------------------------------------------------------------


def _cmd_maintain_build_docs(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    from .doc_db import build_doc_db, repo_docs_dir

    if args.docs_dir:
        docs_dir = Path(args.docs_dir).resolve()
    else:
        docs_dir = repo_docs_dir()
        if docs_dir is None:
            raise RegmetaError(
                exit_code=EXIT_CONFIG,
                code="no_docs_dir",
                error_class="configuration",
                message=(
                    "No --docs-dir specified and no in-repo docs found. "
                    "This command is for maintainers rebuilding the doc DB from a repo checkout."
                ),
                remediation=(
                    "Run from a regmeta checkout with `regmeta/docs/` present, "
                    "or pass --docs-dir pointing to a directory with register doc subdirectories."
                ),
            )
    db_dir = Path(args.db).resolve() if args.db else None
    if db_dir is None:
        from .db import default_db_dir

        db_dir = default_db_dir().resolve()
    db_path = build_doc_db(docs_dir, db_dir)
    return {
        "data": {"db_path": str(db_path), "docs_dir": str(docs_dir)},
    }, 0


def _cmd_doc_search(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    from .doc_db import ensure_doc_db
    from .doc_queries import doc_search

    start = time.perf_counter()
    conn = ensure_doc_db(args.db)
    try:
        data = doc_search(
            conn,
            args.query,
            type_tag=args.doc_type,
            topic_tag=args.topic,
            register=args.register,
            limit=args.limit,
            offset=args.offset,
        )
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    return _success_envelope(
        command="doc search",
        args_payload={
            "query": args.query,
            "type": args.doc_type,
            "topic": args.topic,
            "register": args.register,
            "limit": args.limit,
            "offset": args.offset,
        },
        db_info=None,
        data=data,
        duration_ms=duration_ms,
    ), 0


def _cmd_doc_get(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    from .doc_db import ensure_doc_db
    from .doc_queries import doc_get

    start = time.perf_counter()
    conn = ensure_doc_db(args.db)
    try:
        data = doc_get(conn, args.identifier)
    finally:
        conn.close()
    if data is None:
        raise RegmetaError(
            exit_code=EXIT_NOT_FOUND,
            code="doc_not_found",
            error_class="not_found",
            message=f"No documentation found for: {args.identifier!r}",
            remediation="Use `regmeta docs list` to see available docs, or `regmeta docs search <query>` to search.",
        )
    duration_ms = int((time.perf_counter() - start) * 1000)
    return _success_envelope(
        command="doc get",
        args_payload={"identifier": args.identifier},
        db_info=None,
        data=data,
        duration_ms=duration_ms,
    ), 0


def _cmd_doc_list(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    from .doc_db import ensure_doc_db
    from .doc_queries import doc_list

    start = time.perf_counter()
    conn = ensure_doc_db(args.db)
    try:
        data = doc_list(
            conn,
            type_tag=args.doc_type,
            topic_tag=args.topic,
            register=args.register,
        )
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    return _success_envelope(
        command="doc list",
        args_payload={
            "type": args.doc_type,
            "topic": args.topic,
            "register": args.register,
        },
        db_info=None,
        data=data,
        duration_ms=duration_ms,
    ), 0


# ---------------------------------------------------------------------------
# Search and get handlers
# ---------------------------------------------------------------------------


def _search_docs(query: str, db_arg: str | None = None) -> list[dict[str, Any]]:
    """Search the doc index for matching documentation.

    Returns lightweight hint results (no full body). Exact variable name
    matches get a boosted rank so they surface near the top of mixed
    search results. Raises ``RegmetaError`` if the doc DB is missing or
    incompatible — query commands require docs to be installed.
    """
    from .doc_db import ensure_doc_db
    from .doc_queries import doc_search

    conn = ensure_doc_db(db_arg)
    try:
        data = doc_search(conn, query, limit=10)
        results = []
        for r in data.get("results", []):
            rank = r.get("fts_rank", 0)
            var = r.get("variable") or ""
            if var.lower() == query.lower():
                rank = -100.0
            results.append(
                {
                    "type": "doc",
                    "register_id": "",
                    "register_name": r.get("register", ""),
                    "var_id": "",
                    "variable_name": var or r["filename"],
                    "display_name": r["display_name"],
                    "fts_rank": rank,
                }
            )
        return results
    finally:
        conn.close()


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
            years=args.years,
            limit=args.limit,
            offset=args.offset,
        )
    finally:
        conn.close()

    # Merge doc results (always included regardless of --type filter)
    doc_results = _search_docs(args.query, db_arg=args.db)
    all_results = data["results"] + doc_results
    all_results.sort(key=lambda x: x.get("fts_rank", 0))
    total_count = data["total_count"] + len(doc_results)
    results = all_results[: args.limit]

    doc_total = sum(1 for r in all_results if r.get("type") == "doc")
    doc_shown = sum(1 for r in results if r.get("type") == "doc")
    doc_hidden = doc_total - doc_shown

    out: dict[str, Any] = {"total_count": total_count, "results": results}
    if doc_hidden > 0:
        out["doc_hint"] = (
            f"{doc_hidden} documentation match{'es' if doc_hidden != 1 else ''} "
            f"not shown (try: regmeta docs search <query>)"
        )

    duration_ms = int((time.perf_counter() - start) * 1000)
    return _success_envelope(
        command="search",
        args_payload={
            "query": args.query,
            "field": args.field,
            "type": args.type,
            "register": args.register,
            "years": args.years,
            "limit": args.limit,
            "offset": args.offset,
        },
        db_info=info,
        data=out,
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
            columns_like=args.columns_like,
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
    if args.columns_like:
        args_out["columns_like"] = args.columns_like
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

    # Annotate results with doc availability hint
    try:
        from .doc_db import ensure_doc_db
        from .doc_queries import doc_exists

        doc_conn = ensure_doc_db(args.db)
        try:
            has_doc = doc_exists(doc_conn, args.variable)
            if has_doc:
                if isinstance(data, dict) and "variables" not in data:
                    data["doc_available"] = True
                elif isinstance(data, dict) and "variables" in data:
                    for v in data["variables"]:
                        v["doc_available"] = True
        finally:
            doc_conn.close()
    except (RegmetaError, sqlite3.Error):
        pass

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


def _cmd_get_availability(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db = db_path_from_args(args.db)
    conn = open_db(db)
    try:
        info = _db_info(conn)
        data = get_availability(conn, args.target, register=args.register)
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    args_payload: dict[str, Any] = {"target": args.target}
    if args.register:
        args_payload["register"] = args.register
    return _success_envelope(
        command="get availability",
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
    fmt_explicit: bool = False,
    args: argparse.Namespace | None = None,
    hints: list[str] | None = None,
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
        elif types == {"doc"}:
            cols = ["variable_name", "display_name"]
        else:
            cols = ["type", "register_id", "register_name", "var_id", "variable_name"]
        _write_formatted(
            results, cols, output_path, fmt=fmt, fmt_explicit=fmt_explicit, hints=hints
        )
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
            fmt_explicit=fmt_explicit,
            hints=hints,
        )
    elif key == ("get", "schema"):
        schema_summary = getattr(args, "summary", False) if args else False
        schema_flat = getattr(args, "flat", False) if args else False

        if schema_summary:
            rows = []
            for v in data.get("variants", []):
                ver_years = [
                    ver.get("year")
                    for ver in v.get("versions", [])
                    if ver.get("year") is not None
                ]
                year_range = f"{min(ver_years)}-{max(ver_years)}" if ver_years else "-"
                total_cols = max(
                    (len(ver.get("columns", [])) for ver in v.get("versions", [])),
                    default=0,
                )
                rows.append(
                    {
                        "regvar_id": v.get("regvar_id", ""),
                        "variant": v.get("registervariantnamn", ""),
                        "years": year_range,
                        "versions": len(v.get("versions", [])),
                        "columns": total_cols,
                    }
                )
            _write_formatted(
                rows,
                ["regvar_id", "variant", "years", "versions", "columns"],
                output_path,
                fmt=fmt,
                fmt_explicit=fmt_explicit,
                hints=hints,
            )
        elif schema_flat:
            rows = []
            for v in data.get("variants", []):
                for ver in v.get("versions", []):
                    for col in ver.get("columns", []):
                        aliases = (col.get("aliases") or "").split(", ")
                        for alias in aliases:
                            if not alias:
                                continue
                            rows.append(
                                {
                                    "regvar_id": v.get("regvar_id", ""),
                                    "year": ver.get("year", ""),
                                    "alias": alias,
                                    "variabelnamn": col.get("variabelnamn", ""),
                                    "source": col.get("source", ""),
                                    "var_id": col.get("var_id", ""),
                                }
                            )
            _write_formatted(
                rows,
                ["regvar_id", "year", "alias", "variabelnamn", "source", "var_id"],
                output_path,
                fmt=fmt,
                fmt_explicit=fmt_explicit,
                hints=hints,
            )
        else:
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
                                "source": col.get("source", ""),
                                "cvid": col.get("cvid", ""),
                            }
                        )
            _write_formatted(
                rows,
                [
                    "version",
                    "var_id",
                    "variabelnamn",
                    "datatyp",
                    "aliases",
                    "source",
                    "cvid",
                ],
                output_path,
                fmt=fmt,
                fmt_explicit=fmt_explicit,
                hints=hints,
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
            fmt_explicit=fmt_explicit,
            hints=hints,
        )
    elif key == ("get", "values"):
        _write_formatted(
            data if isinstance(data, list) else [],
            ["vardekod", "vardebenamning"],
            output_path,
            fmt=fmt,
            fmt_explicit=fmt_explicit,
            hints=hints,
        )
    elif key == ("get", "datacolumns"):
        _write_formatted(
            data if isinstance(data, list) else [],
            ["kolumnnamn", "register_id", "register_name", "version_name"],
            output_path,
            fmt=fmt,
            fmt_explicit=fmt_explicit,
            hints=hints,
        )
    elif key == ("get", "coded-variables"):
        _write_formatted(
            data if isinstance(data, list) else [],
            ["variable_name", "n_distinct_codes", "n_registers", "n_instances"],
            output_path,
            fmt=fmt,
            fmt_explicit=fmt_explicit,
            hints=hints,
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
            fmt_explicit=fmt_explicit,
            hints=hints,
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
            fmt_explicit=fmt_explicit,
            hints=hints,
        )
        cov = data.get("provenance_coverage", {})
        if cov.get("total"):
            pct = round(100 * cov["with_source"] / cov["total"])
            _write_to(
                f"\nProvenance: {cov['with_source']}/{cov['total']} ({pct}%)\n",
                output_path,
            )
    elif key == ("get", "availability"):
        target_type = data.get("target_type", "")
        if target_type == "variable":
            rows = []
            for r in data.get("registers", []):
                yr = r.get("years", [])
                year_str = f"{yr[0]}-{yr[-1]}" if yr else ""
                gaps_str = ", ".join(str(g) for g in r.get("gaps", []))
                rows.append(
                    {
                        "register": f"{r['register_name']} ({r['register_id']})",
                        "var_id": r["var_id"],
                        "years": year_str,
                        "gaps": gaps_str or "-",
                    }
                )
            _write_formatted(
                rows,
                ["register", "var_id", "years", "gaps"],
                output_path,
                fmt=fmt,
                fmt_explicit=fmt_explicit,
                hints=hints,
            )
        else:
            rows = []
            for v in data.get("variants", []):
                yr = v.get("years", [])
                year_str = f"{yr[0]}-{yr[-1]}" if yr else ""
                rows.append(
                    {
                        "regvar_id": v["regvar_id"],
                        "variant_name": v["variant_name"],
                        "years": year_str,
                        "version_count": len(yr),
                    }
                )
            _write_formatted(
                rows,
                ["regvar_id", "variant_name", "years", "version_count"],
                output_path,
                fmt=fmt,
                fmt_explicit=fmt_explicit,
                hints=hints,
            )
        all_gaps = data.get("gaps", [])
        if all_gaps:
            _write_to(f"\nGaps: {', '.join(str(g) for g in all_gaps)}\n", output_path)
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
            fmt_explicit=fmt_explicit,
            hints=hints,
        )
    elif key == ("docs", "search"):
        results = data.get("results", [])
        rows = [
            {
                "variable": r.get("variable") or r["filename"],
                "display_name": r["display_name"],
                "filename": r["filename"],
                "snippet": (r.get("snippet") or "")[:80],
            }
            for r in results
        ]
        _write_formatted(
            rows,
            ["variable", "display_name", "filename", "snippet"],
            output_path,
            fmt=fmt,
            fmt_explicit=fmt_explicit,
            hints=hints,
        )
    elif key == ("docs", "get"):
        header = []
        if data.get("variable"):
            header.append(f"  variable:     {data['variable']}")
        header.append(f"  display_name: {data['display_name']}")
        if data.get("tags"):
            header.append(f"  tags:         {', '.join(data['tags'])}")
        if data.get("source"):
            header.append(f"  source:       {data['source']}")
        _write_to("\n".join(header) + "\n\n", output_path)
        _write_to(data.get("body", "") + "\n", output_path)
    elif key == ("docs", "list"):
        if data.get("results") is not None:
            rows = [
                {
                    "filename": r["filename"],
                    "display_name": r["display_name"],
                    "variable": r.get("variable") or "",
                }
                for r in data["results"]
            ]
            _write_formatted(
                rows,
                ["filename", "display_name", "variable"],
                output_path,
                fmt=fmt,
                fmt_explicit=fmt_explicit,
                hints=hints,
            )
        else:
            lines = [f"  total: {data.get('total_count', 0)}"]
            lines.append("")
            lines.append("  registers:")
            for reg, n in data.get("registers", {}).items():
                lines.append(f"    {reg}: {n}")
            lines.append("")
            lines.append("  types:")
            for tag, n in data.get("types", {}).items():
                lines.append(f"    {tag}: {n}")
            lines.append("")
            lines.append("  topics:")
            for tag, n in data.get("topics", {}).items():
                lines.append(f"    {tag}: {n}")
            _write_to("\n".join(lines) + "\n", output_path)
    elif key == ("maintain", "update"):
        pass  # status messages already emitted on stderr by run_update()
    elif key == ("maintain", "info"):
        lines = [f"  database: {data.get('db_path', 'unknown')}"]
        manifest = data.get("manifest", {})
        if manifest.get("schema_version"):
            lines.append(f"  schema:   {manifest['schema_version']}")
        if manifest.get("import_date"):
            lines.append(f"  imported: {manifest['import_date']}")
        if manifest.get("source_tag"):
            lines.append(f"  release:  {manifest['source_tag']}")
        table_counts = data.get("table_counts", {})
        if table_counts:
            lines.append("")
            lines.append("  tables:")
            for t, n in table_counts.items():
                lines.append(f"    {t}: {n:,}")
        _write_to("\n".join(lines) + "\n", output_path)
    elif key == ("maintain", "build-db"):
        _write_to(f"Database built: {data.get('db_path', 'unknown')}\n", output_path)
    elif key == ("maintain", "build-docs"):
        _write_to(f"Built doc index: {data.get('db_path')}\n", output_path)
    else:
        _write_json(payload, output_path)


# ---------------------------------------------------------------------------
# Hints
# ---------------------------------------------------------------------------


def _collect_hints(
    key: tuple[str, str | None],
    data: dict[str, Any],
    args: argparse.Namespace,
    hints: list[str],
) -> None:
    """Populate command-specific contextual hints."""
    if key == ("search", None):
        if getattr(args, "field", "all") == "all":
            _hint_add(hints, "Searching all fields (--field to narrow)")
        total = data.get("total_count", 0)
        results = data.get("results", [])
        if total > len(results):
            _hint_add(
                hints,
                f"Showing {len(results)} of {total} matches (--limit/--offset to page)",
            )
        doc_hint = data.pop("doc_hint", None)
        if doc_hint:
            _hint_add(hints, doc_hint)
        if total == 0 and not results:
            _hint_add(hints, "No results (try broader --field or regmeta docs search)")

    elif key == ("get", "schema"):
        if not getattr(args, "summary", False) and not getattr(args, "flat", False):
            _hint_add(
                hints, "Full schema view (--summary for overview, --flat for export)"
            )

    elif key == ("get", "varinfo"):
        variables = data.get("variables", [data]) if "variables" in data else [data]
        n_regs = len({v.get("register_id") for v in variables})
        n_vars = len({v.get("var_id") for v in variables})
        if n_vars > 1:
            _hint_add(
                hints,
                f"Alias maps to {n_vars} variable definitions across {n_regs} register(s) (--register to narrow)",
            )
        elif n_regs > 1:
            _hint_add(hints, f"Found in {n_regs} registers (--register to narrow)")
        if any(v.get("doc_available") for v in variables):
            _hint_add(
                hints,
                f"Docs available (run: regmeta docs get {getattr(args, 'variable', '')})",
            )

    elif key == ("get", "values"):
        if not getattr(args, "valid_at", None) and data:
            values = data if isinstance(data, list) else []
            if values:
                _hint_add(
                    hints,
                    "Some values have date ranges (--valid-at YYYY-MM-DD to filter)",
                )


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


COMMAND_DISPATCH = {
    ("maintain", "build-db"): _cmd_maintain_build_db,
    ("maintain", "info"): _cmd_maintain_info,
    ("maintain", "update"): _cmd_maintain_update,
    ("search", None): _cmd_search,
    ("get", "register"): _cmd_get_register,
    ("get", "schema"): _cmd_get_schema,
    ("get", "varinfo"): _cmd_get_varinfo,
    ("get", "values"): _cmd_get_values,
    ("get", "datacolumns"): _cmd_get_datacolumns,
    ("get", "coded-variables"): _cmd_get_coded_variables,
    ("get", "diff"): _cmd_get_diff,
    ("get", "lineage"): _cmd_get_lineage,
    ("get", "availability"): _cmd_get_availability,
    ("resolve", None): _cmd_resolve,
    ("maintain", "build-docs"): _cmd_maintain_build_docs,
    ("docs", "search"): _cmd_doc_search,
    ("docs", "get"): _cmd_doc_get,
    ("docs", "list"): _cmd_doc_list,
}


# ---------------------------------------------------------------------------
# Usage / version display
# ---------------------------------------------------------------------------

_KEY_CONCEPTS = [
    ("register", "A statistical register (e.g. LISA, RTB). Has a numeric register_id."),
    (
        "variant",
        "A sub-table within a register (e.g. LISA/Individer). Has a regvar_id.",
    ),
    (
        "variable",
        'A logical concept (e.g. "Kön"). Has a var_id. Shared across registers.',
    ),
    ("alias", "Column header in a data file. May differ across registers/versions."),
    ("CVID", "Links a variable instance to its value set. Use with `get values`."),
    (
        "value set",
        "Valid coded values for a categorical variable (e.g. 1=Man, 2=Kvinna).",
    ),
]

# (command_syntax, description) for the top-level overview. None = blank separator.
_COMMAND_OVERVIEW: list[tuple[str, str] | None] = [
    (
        "search --query TERM [--field F] [--type T] [--register R] [--years Y]",
        "Search registers, variables, columns, and value codes.",
    ),
    (
        "resolve [--columns COL,...] [--register R] [--require-match]",
        "Map data-file column names to variable definitions (exact match).",
    ),
    None,
    ("get register NAME", "Register overview with variants."),
    (
        "get schema [REGVAR_ID] [--register R] [--years Y] [--columns-like PAT] [--summary|--flat]",
        "Column listing per version.",
    ),
    ("get varinfo VARIABLE [--register R]", "Variable details with instance history."),
    ("get values CVID [--valid-at DATE]", "Value codes and labels for a CVID."),
    (
        "get datacolumns VARIABLE [--register R]",
        "All column aliases for a variable across registers.",
    ),
    (
        "get coded-variables [--min-codes N] [--min-registers N]",
        "Categorical variables ranked by usage.",
    ),
    (
        "get diff --register R --from YEAR --to YEAR [--variant ID] [--variable V...]",
        "Schema changes between two years.",
    ),
    ("get lineage VARIABLE [--register R]", "Cross-register variable provenance."),
    (
        "get availability TARGET [--register R]",
        "Temporal availability (years, gaps, aliases) for a variable or register.",
    ),
    None,
    (
        "docs search QUERY [--type T] [--topic T] [--register R]",
        "Full-text search over curated documentation.",
    ),
    ("docs get IDENTIFIER", "Full documentation for a variable or topic."),
    (
        "docs list [--type T] [--topic T] [--register R]",
        "Browse available documentation.",
    ),
    None,
    ("maintain update [--tag TAG] [--force] [--yes]", "Update package and database."),
    ("maintain info", "Database stats and import metadata."),
    (
        "maintain build-db --csv-dir DIR",
        "Build database from SCB CSV exports (maintainer-only).",
    ),
    (
        "maintain build-docs [--docs-dir DIR]",
        "Rebuild the doc DB from markdown (maintainer-only).",
    ),
]


def _version_line(db_arg: str | None = None) -> str:
    from . import __version__

    db_path = db_path_from_args(db_arg)
    db_status = str(db_path) if db_path.exists() else "not installed"
    return f"regmeta v{__version__}  ·  db: {db_status}"


def _print_usage(db_arg: str | None = None) -> None:
    """Brief overview (bare `regmeta` with no args)."""
    w = sys.stderr.write
    w(f"{_version_line(db_arg)}\n")
    db_path = db_path_from_args(db_arg)
    if not db_path.exists():
        w("\n  No database installed. Run `regmeta maintain update` to get started.\n")
    w("\nCommands:\n")
    info = _get_subcommand_info(_build_parser())
    col_w = max(len(name) for name, _, _ in info) + 2
    for name, _, help_text in info:
        w(f"  {name:<{col_w}} {help_text}\n")
    w(
        "\nRun `regmeta --help` for full reference, `regmeta --examples` for usage examples.\n"
    )


def _print_help(db_arg: str | None = None) -> None:
    """Full help (regmeta --help)."""
    w = sys.stderr.write
    w(f"{_version_line(db_arg)}\n")
    db_path = db_path_from_args(db_arg)
    if not db_path.exists():
        w("\n  No database installed. Run `regmeta maintain update` to get started.\n")

    w("\nKey concepts:\n")
    name_w = max(len(name) for name, _ in _KEY_CONCEPTS) + 2
    for name, desc in _KEY_CONCEPTS:
        w(f"  {name:<{name_w}} {desc}\n")

    w("\nGlobal flags (place before subcommand):\n")
    w("  --format {table,list,json}   Output format (default: table)\n")
    w("  --output FILE                Write output to file\n")
    w("  -v, --verbose                Include envelope metadata\n")
    w("  -q, --quiet                  Suppress hints on stderr\n")

    w("\nCommands:\n")
    for entry in _COMMAND_OVERVIEW:
        if entry is None:
            w("\n")
        else:
            syntax, desc = entry
            w(f"  {syntax}\n")
            w(f"      {desc}\n")

    w("\nRun `regmeta <command> --help` for detailed help.\n")
    w("Run `regmeta --examples` for usage examples and workflows.\n")


def _get_subcommand_info(
    parser: argparse.ArgumentParser,
) -> list[tuple[str, argparse.ArgumentParser, str]]:
    """Get [(name, subparser, help_text)] for a parser's subcommands."""
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            help_map = {ca.dest: ca.help or "" for ca in action._choices_actions}
            return [
                (name, sub_p, help_map.get(name, ""))
                for name, sub_p in action.choices.items()
            ]
    return []


def _print_group_brief(parser: argparse.ArgumentParser, group_name: str) -> None:
    """Brief subcommand listing (shown when no subcommand is given)."""
    w = sys.stderr.write
    group_p = None
    group_help = ""
    for name, p, h in _get_subcommand_info(parser):
        if name == group_name:
            group_p, group_help = p, h
            break
    if not group_p:
        return

    w(f"\nregmeta {group_name} — {group_help}\n\n")
    w("Subcommands:\n")
    sub_info = _get_subcommand_info(group_p)
    col_w = max(len(n) for n, _, _ in sub_info) + 2
    for name, _, help_text in sub_info:
        w(f"  {name:<{col_w}} {help_text}\n")
    w(
        f"\nRun `regmeta {group_name} <command> --help`"
        " for detailed help with examples.\n"
    )


def _print_group_detailed(parser: argparse.ArgumentParser, group_name: str) -> None:
    """Full help for all subcommands in a group (shown with --help)."""
    w = sys.stderr.write
    group_p = None
    group_help = ""
    for name, p, h in _get_subcommand_info(parser):
        if name == group_name:
            group_p, group_help = p, h
            break
    if not group_p:
        return

    w(f"\nregmeta {group_name} — {group_help}\n")
    for name, sub_p, _ in _get_subcommand_info(group_p):
        w(f"\n{'─' * 60}\n")
        w(f"  {group_name} {name}\n")
        w(f"{'─' * 60}\n\n")
        w(sub_p.format_help())


def _strip_global_flags(reordered: list[str]) -> list[str]:
    """Remove global flags from reordered argv, leaving command args only."""
    result: list[str] = []
    skip_next = False
    for arg in reordered:
        if skip_next:
            skip_next = False
            continue
        if arg in _GLOBAL_FLAGS:
            if arg in _GLOBAL_FLAGS_WITH_VALUE:
                skip_next = True
            continue
        eq_name = arg.split("=", 1)[0] if "=" in arg else None
        if eq_name in _GLOBAL_FLAGS_WITH_VALUE:
            continue
        result.append(arg)
    return result


# ---------------------------------------------------------------------------
# Examples (agent-oriented: question → command → what to expect)
# ---------------------------------------------------------------------------

# Keys: "command" for top-level, ("group", "sub") for subcommands.
# Printed by --examples flag. Order matters — it's the display order.
_EXAMPLES: dict[str | tuple[str, str], str] = {
    "search": """\
search — Finding registers, variables, and values
──────────────────────────────────────────────────

  "What registers deal with education?"
    regmeta search --query utbildning --type register

  "Find income-related variables available after 2015"
    regmeta search --query inkomst --years 2015-

  "Which columns in data files contain kommun?"
    regmeta search --query kommun --field datacolumn

  "Find variables within LISA mentioning kommun"
    regmeta search --query kommun --register LISA

  "What value codes include 0180?"
    regmeta search --query 0180 --field value
""",
    "resolve": """\
resolve — Mapping column headers to official definitions
────────────────────────────────────────────────────────

  "I have a CSV with columns Kon, FodelseAr, AstKommun — what are they?"
    regmeta resolve --columns "Kon,FodelseAr,AstKommun" --register LISA

  "Resolve columns from a JSON list"
    echo '["Kon","FodelseAr"]' | regmeta resolve --register LISA

  resolve is exact match only. If a column shows no_match, try:
    regmeta search --query AstKommun --field datacolumn
""",
    ("get", "register"): """\
get register — Register overview
────────────────────────────────

  "Tell me about LISA"
    regmeta get register LISA

  "What register has ID 34?"
    regmeta get register 34

  The output lists all variants (sub-tables) with their regvar_id.
  Use the regvar_id with `get schema` for column details.
""",
    ("get", "schema"): """\
get schema — What columns does a register have?
────────────────────────────────────────────────

  "What variables are in LISA?"
    regmeta get schema --register LISA --summary

  "What columns does LISA 2022 have?"
    regmeta get schema --register LISA --years 2022

  "Show education-related columns in register 340"
    regmeta get schema --register 340 --columns-like "Merit|Betyg|Prov"

  "One row per column for easy scanning"
    regmeta get schema --register LISA --flat --years 2022

  For large registers, always narrow with --years, --columns-like,
  --summary, or --flat. Unfiltered output can be very long.
""",
    ("get", "varinfo"): """\
get varinfo — Variable details and history
──────────────────────────────────────────

  "What is the variable Kön?"
    regmeta get varinfo "Kön"

  "Where does variable 44 appear?"
    regmeta get varinfo 44

  "Show Kön only within LISA"
    regmeta get varinfo "Kön" --register LISA

  The output includes CVIDs — use those with `get values` to see
  the actual code/label pairs.
""",
    ("get", "values"): """\
get values — What do the coded values mean?
───────────────────────────────────────────

  "What are the valid values for CVID 1001?"
    regmeta get values 1001

  "What values were valid in 2020?"
    regmeta get values 1001 --valid-at 2020-01-01

  You need a CVID, not a variable name. Get it from `get varinfo`:
    regmeta get varinfo "Kommun" --register LISA   → find the CVID
    regmeta get values <cvid>                      → see the codes
""",
    ("get", "datacolumns"): """\
get datacolumns — What column names does a variable appear under?
────────────────────────────────────────────────────────────────

  "What column headers does Kommun use across registers?"
    regmeta get datacolumns "Kommun"

  "What aliases does Kön have in LISA specifically?"
    regmeta get datacolumns "Kön" --register LISA
""",
    ("get", "coded-variables"): """\
get coded-variables — Which variables have value sets?
──────────────────────────────────────────────────────

  "What are the most widely used categorical variables?"
    regmeta get coded-variables --min-registers 5

  "Find variables with many value codes"
    regmeta get coded-variables --min-codes 50 --min-registers 10
""",
    ("get", "diff"): """\
get diff — How has a register changed?
──────────────────────────────────────

  "What changed in LISA between 2015 and 2020?"
    regmeta get diff --register LISA --from 2015 --to 2020

  "Did Kon change between 2015 and 2020 in LISA?"
    regmeta get diff --register LISA --from 2015 --to 2020 --variable Kon
""",
    ("get", "lineage"): """\
get lineage — Where does a variable come from?
──────────────────────────────────────────────

  "Which register is the source of Kön, and who consumes it?"
    regmeta get lineage "Kön"

  "Where does LISA get Kön from?"
    regmeta get lineage "Kön" --register LISA
""",
    ("get", "availability"): """\
get availability — When is something available?
───────────────────────────────────────────────

  "Is Kön available from 2015 to 2024?"
    regmeta get availability "Kön"

  "What years does LISA cover?"
    regmeta get availability LISA

  "When is Kön available in LISA specifically?"
    regmeta get availability "Kön" --register LISA
""",
    ("docs", "search"): """\
docs search — Search curated documentation
──────────────────────────────────────────

  "What does the documentation say about income?"
    regmeta docs search inkomst

  "Find documentation about SyssStat in LISA"
    regmeta docs search SyssStat --register lisa --type variable
""",
    ("docs", "get"): """\
docs get — Read full documentation
──────────────────────────────────

  "Show me the full documentation for SyssStat"
    regmeta docs get SyssStat

  "Show the LISA overview"
    regmeta docs get _overview
""",
    ("docs", "list"): """\
docs list — Browse available documentation
──────────────────────────────────────────

  "What documentation is available?"
    regmeta docs list

  "What LISA documentation exists?"
    regmeta docs list --register lisa

  "Show all variable documentation about income"
    regmeta docs list --type variable --topic income
""",
    ("maintain", "update"): """\
maintain update — Install or update the database
─────────────────────────────────────────────────

  "Set up regmeta for the first time"
    regmeta maintain update --yes

  "Update to the latest database"
    regmeta maintain update

  "Force re-download even if already current"
    regmeta maintain update --force --yes
""",
    ("maintain", "info"): """\
maintain info — What database am I using?
─────────────────────────────────────────

  "Show database version, schema, and import stats"
    regmeta maintain info
""",
    ("maintain", "build-db"): """\
maintain build-db — Build database from raw CSVs
─────────────────────────────────────────────────

  "Build the database from SCB CSV exports"
    regmeta maintain build-db --csv-dir /path/to/SCB-data/

  Most users should use `maintain update` to download a pre-built
  database instead.
""",
    ("maintain", "build-docs"): """\
maintain build-docs — Rebuild documentation index
──────────────────────────────────────────────────

  "Rebuild the docs search index from markdown files"
    regmeta maintain build-docs

  "Use custom documentation directory"
    regmeta maintain build-docs --docs-dir /path/to/docs/
""",
}

_WORKFLOW_EXAMPLES = """\
Common workflows
────────────────

  "What are the valid values for Kommun in LISA?"
    regmeta get varinfo "Kommun" --register LISA   → note the CVID
    regmeta get values <cvid>                      → code/label pairs

  "I have a data file — what do the columns mean?"
    regmeta resolve --columns "Kon,FodelseAr,AstKommun" --register LISA
    (for no_match columns, try search:)
    regmeta search --query AstKommun --field datacolumn

  "Get structured output for programmatic use"
    regmeta --format json get schema --register LISA --years 2022

  "How has my register changed since I last looked?"
    regmeta get diff --register LISA --from 2018 --to 2023

  "What SCB data exists but isn't in my local mock data?"
    mock-data-wizard compare mock_data/manifest.json
    (requires the mock-data-wizard package)
"""

# Display order for --examples (all)
_EXAMPLES_ORDER: list[str | tuple[str, str]] = [
    "search",
    "resolve",
    ("get", "register"),
    ("get", "schema"),
    ("get", "varinfo"),
    ("get", "values"),
    ("get", "datacolumns"),
    ("get", "coded-variables"),
    ("get", "diff"),
    ("get", "lineage"),
    ("get", "availability"),
    ("docs", "search"),
    ("docs", "get"),
    ("docs", "list"),
    ("maintain", "update"),
    ("maintain", "info"),
    ("maintain", "build-db"),
    ("maintain", "build-docs"),
]


def _print_examples(cmd_args: list[str]) -> None:
    """Print examples for the given command path, or all if empty."""
    w = sys.stderr.write

    if not cmd_args:
        # All examples
        for key in _EXAMPLES_ORDER:
            w(_EXAMPLES[key])
            w("\n")
        w(_WORKFLOW_EXAMPLES)
        return

    cmd = cmd_args[0]

    if len(cmd_args) == 1:
        if cmd in _EXAMPLES:
            # Leaf command (search, resolve)
            w(_EXAMPLES[cmd])
        else:
            # Group command (get, docs, maintain) — show all sub-examples
            for key in _EXAMPLES_ORDER:
                if isinstance(key, tuple) and key[0] == cmd:
                    w(_EXAMPLES[key])
                    w("\n")
        return

    # Sub-subcommand (get schema, docs search, etc.)
    key = (cmd_args[0], cmd_args[1])
    if key in _EXAMPLES:
        w(_EXAMPLES[key])


def _print_version(db_arg: str | None = None) -> None:
    from .update import UpdateChecker
    from . import __version__

    sys.stderr.write(f"{_version_line(db_arg)}\n")
    sys.stderr.write("Checking for updates...\n")
    try:
        checker = UpdateChecker(http_timeout=10)
        newer = checker.get_newer_version(timeout=10)
        if newer:
            sys.stderr.write(
                f"Update available: v{__version__} → v{newer}"
                "  —  run `regmeta maintain update`\n"
            )
        elif checker.completed:
            sys.stderr.write("Up to date.\n")
        else:
            sys.stderr.write("Could not check for updates.\n")
    except Exception:
        sys.stderr.write("Could not check for updates.\n")


def _prompt_first_run_download(
    args: argparse.Namespace, fmt: str, *, needs_main: bool
) -> None:
    """Offer an interactive download when a query command finds artifacts missing.

    *needs_main* is True for commands that open the main metadata DB
    (``search``, ``get``, ``resolve``) and False for ``docs/*`` which
    only read the doc DB. Prompting for the ~400 MB main DB on a
    docs-only command would be wasteful and can fail the command even
    when the user has a usable doc DB. In non-interactive contexts
    (pipes, ``--format json``) this is a no-op — the subsequent handler
    call will raise ``db_not_found`` / ``doc_db_not_found`` which
    surface as the standard structured error.
    """
    from .doc_db import DOC_DB_FILENAME

    db_path = db_path_from_args(args.db)
    docs_path = db_path.parent / DOC_DB_FILENAME
    missing_main = needs_main and not db_path.exists()
    missing_docs = not docs_path.exists()
    if not (missing_main or missing_docs):
        return
    if fmt == "json" or not sys.stdin.isatty():
        return

    parts: list[str] = []
    if missing_main:
        parts.append("main database (~400 MB compressed, ~1.6 GB on disk)")
    if missing_docs:
        parts.append("doc DB (~600 KB compressed, ~3 MB on disk)")
    header = (
        "Query commands require both the main DB and the doc DB."
        if needs_main
        else "Docs commands require the doc DB."
    )
    sys.stderr.write(
        f"{header}\nMissing: " + ", ".join(parts) + ".\nDownload now? [y/N] "
    )
    sys.stderr.flush()
    if input().strip().lower() not in ("y", "yes"):
        return

    from .download import download_db, download_docs_db

    if missing_main:
        download_db(db_dir=db_path.parent, yes=True)
    if missing_docs:
        download_docs_db(db_dir=docs_path.parent)
    sys.stderr.write("\n")


def run(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    effective = argv if argv is not None else sys.argv[1:]
    reordered = _reorder_global_flags(effective)

    # Intercept --examples and group --help before argparse processes them
    cmd_args = _strip_global_flags(reordered)

    if "--examples" in cmd_args:
        _print_examples([a for a in cmd_args if a != "--examples"])
        return 0

    if (
        len(cmd_args) == 2
        and cmd_args[1] in ("-h", "--help")
        and cmd_args[0] in ("get", "docs", "maintain")
    ):
        _print_group_detailed(parser, cmd_args[0])
        return 0

    try:
        args = parser.parse_args(reordered)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else EXIT_USAGE

    if getattr(args, "version", False):
        _print_version(args.db)
        return 0

    if getattr(args, "help", False):
        _print_help(args.db)
        return 0
    if not args.command:
        _print_usage(args.db)
        return EXIT_USAGE

    sub_command = None
    if args.command == "maintain":
        sub_command = getattr(args, "maintain_command", None)
        if not sub_command:
            _print_group_brief(parser, "maintain")
            return EXIT_USAGE
    elif args.command == "get":
        sub_command = getattr(args, "get_command", None)
        if not sub_command:
            _print_group_brief(parser, "get")
            return EXIT_USAGE
    elif args.command == "docs":
        sub_command = getattr(args, "doc_command", None)
        if not sub_command:
            _print_group_brief(parser, "docs")
            return EXIT_USAGE

    key = (args.command, sub_command)
    handler = COMMAND_DISPATCH.get(key)
    if not handler:
        sys.stderr.write(f"Unknown command: {args.command} {sub_command or ''}\n")
        return EXIT_USAGE

    fmt = getattr(args, "format", "table")
    fmt_explicit = any(a == "--format" or a.startswith("--format=") for a in effective)
    verbose = getattr(args, "verbose", False)
    output_path = getattr(args, "output", None)
    quiet = getattr(args, "quiet", False) or os.environ.get("REGMETA_QUIET") == "1"
    hints: list[str] = []

    # Kick off background update check early so it runs in parallel with the
    # actual command.  We collect the result (with a short timeout) just before
    # returning so the user never waits for it.
    update_checker = None
    if not quiet and fmt != "json" and key != ("maintain", "update"):
        try:
            from .update import UpdateChecker

            update_checker = UpdateChecker()
        except Exception:
            pass

    try:
        # Auto-download artifacts on first use (interactive only). Only
        # bootstrap the artifacts each command actually needs: search/get/
        # resolve open the main DB and the doc DB; docs/* only open the
        # doc DB and must not trigger the ~400 MB main-DB download.
        needs_main = args.command in ("search", "get", "resolve")
        if needs_main or args.command == "docs":
            _prompt_first_run_download(args, fmt, needs_main=needs_main)
            # Enforce doc-DB presence for non-docs query commands up front
            # so they fail fast and consistently before doing main-DB query
            # work. docs/* handlers call ensure_doc_db themselves.
            if needs_main:
                from .doc_db import ensure_doc_db

                ensure_doc_db(args.db).close()
        payload, exit_code = handler(args)
        if not quiet and fmt != "json":
            _collect_hints(key, payload.get("data", {}), args, hints)
        if fmt == "json":
            if verbose:
                _write_json(payload, output_path)
            else:
                _write_json(payload.get("data", payload), output_path)
        else:
            _write_payload(
                key,
                payload,
                output_path,
                fmt=fmt,
                fmt_explicit=fmt_explicit,
                args=args,
                hints=hints if not quiet else None,
            )
        if hints and not quiet:
            sys.stdout.flush()
            _emit_hints(hints)
        if update_checker is not None and sys.stderr.isatty():
            try:
                new_ver = update_checker.get_newer_version()
                if not new_ver and not update_checker.completed:
                    # Background check timed out — fall back to persistent flag
                    from . import __version__
                    from .update import _parse_version, read_pending_update

                    flagged = read_pending_update()
                    if flagged and _parse_version(flagged) > _parse_version(
                        __version__
                    ):
                        new_ver = flagged
                if new_ver:
                    from . import __version__

                    sys.stderr.write(
                        f"\n  Update available: v{__version__} → v{new_ver}"
                        "  —  run `regmeta maintain update`\n"
                    )
            except Exception:
                pass
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
