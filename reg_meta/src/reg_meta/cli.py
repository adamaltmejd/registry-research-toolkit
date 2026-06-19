"""CLI entry point for reg_meta."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any

from .cli_common import (
    GLOBAL_FLAGS,
    GLOBAL_FLAGS_WITH_VALUE,
    NoRepeatParser,
    apply_leaf_help,
    emit_hints,
    get_db_info,
    handle_cli_exception,
    hint_add,
    reorder_global_flags,
    success_envelope,
    write_formatted,
    write_json,
    write_to,
)
from .db import (
    db_path_from_args,
    default_db_dir,
    get_manifest,
    open_db,
)
from .errors import (
    EXIT_NOT_FOUND,
    EXIT_USAGE,
    RegMetaError,
)
from .fqid import (
    period_token_for_bounds,
)
from .queries import (
    get_availability,
    get_classification,
    get_classification_codes,
    get_classification_concept_groups,
    get_coded_variables,
    get_concept_groups,
    get_datacolumns,
    get_diff,
    get_lineage,
    get_register,
    get_schema,
    get_values_by_variable,
    get_varinfo,
    list_classifications,
    resolve,
    search,
    search_variables_by_classification,
)


def _write_groups_payload(data: dict[str, Any], output_path: str | None) -> None:
    """Render the `get values` disagreement payload (groups by value-set)."""
    n_groups = data.get("value_set_count", len(data.get("groups", [])))
    n_inst = data.get("instance_count", 0)
    n_regs = data.get("register_count", 0)
    year = data.get("year")
    name = data.get("variable_name") or data.get("input") or ""

    parts = [f"Variable '{name}'"]
    if year is not None:
        parts.append(f"year {year}")
    parts.append(
        f"{n_groups} distinct value set(s) across {n_inst} instance(s) "
        f"in {n_regs} register(s)"
    )
    lines = [" — ".join(parts), ""]

    cap = 10
    for i, group in enumerate(data.get("groups", []), 1):
        ic = group.get("instance_count", 0)
        rc = group.get("register_count", 0)
        lines.append(f"[Group {i}] {ic} instance(s) across {rc} register(s)")
        for v in group.get("values", []):
            kod = v.get("code") or "(empty)"
            label = v.get("label", "")
            lines.append(f"  {kod:<8}  {label}")
        regs = group.get("registers", [])
        shown = regs[:cap]
        more = f" (+{len(regs) - cap} more)" if len(regs) > cap else ""
        lines.append(f"  Registers: {', '.join(shown)}{more}")
        lines.append("")

    write_to("\n".join(lines), output_path)


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = NoRepeatParser(
        prog="reg-meta",
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
            "For full documentation search, use: reg-meta docs search <query>\n\n"
            "Hits on members of a concept group (a folded variable family, e.g. a\n"
            "month-suffixed series) collapse into one group row; group labels match\n"
            "too. Use --no-fold for flat member rows, `get groups` for a family's\n"
            "full member/facet listing.\n\n"
            "Note: --type and --register do different things:\n"
            "  --type register    Filter results to only show registers (not variables)\n"
            "  --register LISA    Restrict search scope to a specific register\n\n"
            "Examples:\n"
            "  reg-meta search --query kommun                        # all fields\n"
            "  reg-meta search --query kommun --field datacolumn     # column headers only\n"
            "  reg-meta search --query 0180 --field value            # value codes/labels\n"
            "  reg-meta search --query utbildning --type register    # find registers\n"
            "  reg-meta search --query kommun --register LISA        # within LISA only"
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
        choices=["register", "variable", "classification", "value", "all"],
        help=(
            "Filter results by entity type: register, variable, classification, "
            "or value (codes/labels, #352) (default: all)."
        ),
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
    search_p.add_argument(
        "--no-fold",
        action="store_true",
        help="Disable concept-group folding (one row per member hit).",
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
            "each with register_variant_id, name, description, and secrecy level.\n\n"
            "Examples:\n"
            "  reg-meta get register LISA\n"
            "  reg-meta get register 34"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    get_reg.add_argument(
        "register", metavar="REGISTER", help="Register name or numeric ID."
    )

    get_schema_p = get_sub.add_parser(
        "schema",
        help="Get column listing per version. Provide register_variant_id or --register.",
        description=(
            "List columns (aliases, variable names, data types, CVIDs) per\n"
            "register version. Can be verbose for large registers — use\n"
            "--years, --columns-like, --summary, or --flat to narrow.\n\n"
            "Examples:\n"
            "  reg-meta get schema --register LISA --years 2022\n"
            "  reg-meta get schema 153 --years 2022            # by register_variant_id\n"
            '  reg-meta get schema --register LISA --columns-like "Merit|Betyg"\n'
            "  reg-meta get schema --register LISA --summary    # one row per variant\n"
            "  reg-meta get schema --register LISA --flat       # one row per alias"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    get_schema_p.add_argument(
        "register_variant_id", nargs="?", default=None, help="Register variant ID."
    )
    get_schema_p.add_argument(
        "--register",
        default=None,
        help="Register name or ID (alternative to register_variant_id).",
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
        help="Flat output: one row per (year, alias, variable_name, register_variant_id).",
    )

    get_groups_p = get_sub.add_parser(
        "groups",
        help="List concept groups (folded variable families) with member facets.",
        description=(
            "Show a register's derived concept groups: families of near-identical\n"
            "variables (month-suffixed series, split siblings, curated facet\n"
            "families) folded into one labeled group, each member carrying its\n"
            "facets (month/rank/...). Groups are presentation-only — members keep\n"
            "their own FQIDs and metadata.\n\n"
            "Examples:\n"
            "  reg-meta get groups LISA\n"
            "  reg-meta get groups --classifications   # curated umbrella groups (e.g. SUN)\n"
            "  reg-meta --format json get groups LISA  # member FQIDs + facets"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    get_groups_p.add_argument(
        "register",
        nargs="?",
        default=None,
        metavar="REGISTER",
        help="Register name or numeric ID.",
    )
    get_groups_p.add_argument(
        "--classifications",
        action="store_true",
        help=(
            "List classification umbrella groups (catalog-wide) instead of a "
            "register's variable groups."
        ),
    )

    get_varinfo_p = get_sub.add_parser(
        "varinfo",
        help="Get variable details with instance history.",
        description=(
            "Show variable definition, description, and every register version\n"
            "where it appears — with CVIDs, data types, aliases, and value counts.\n\n"
            "Examples:\n"
            '  reg-meta get varinfo "Kön"\n'
            "  reg-meta get varinfo 44               # by var_id\n"
            '  reg-meta get varinfo "Kön" --register LISA'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    get_varinfo_p.add_argument("variable", help="Variable name or var_id.")
    get_varinfo_p.add_argument(
        "--register", default=None, help="Filter by register (name or ID)."
    )

    get_values_p = get_sub.add_parser(
        "values",
        help="Get value-set members (code + label) for a variable.",
        description=(
            "Show code/label pairs for a categorical variable's value set,\n"
            "broken out per state (era × variant) and optionally one year.\n"
            "Codes are projected to each era's year via SCB validity windows\n"
            "at build time, so the result is the year-correct set.\n\n"
            "TARGET is a variable name, column alias, or var_id (a numeric arg\n"
            "resolves as a var_id).\n\n"
            "Examples:\n"
            '  reg-meta get values "ArbSokNov"                            # all states × codes\n'
            '  reg-meta get values "ArbSokNov" --register LISA            # year × codes table\n'
            '  reg-meta get values "ArbSokNov" --register LISA --year 2015  # codes for one year'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    get_values_p.add_argument(
        "target",
        help="Variable name, column alias, or var_id.",
    )
    get_values_p.add_argument(
        "--register",
        default=None,
        help="Filter by register.",
    )
    get_values_p.add_argument(
        "--year",
        type=int,
        default=None,
        help="Filter to a single year.",
    )

    get_datacols_p = get_sub.add_parser(
        "datacolumns",
        help="Get all column aliases (data file headers) for a variable.",
        description=(
            "List every column name a variable appears under across registers and versions.\n\n"
            "Examples:\n"
            "  reg-meta get datacolumns Kommun\n"
            '  reg-meta get datacolumns "Kön" --register LISA'
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
            "  reg-meta get coded-variables --min-registers 5\n"
            "  reg-meta get coded-variables --min-codes 50 --min-registers 10"
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
            "  reg-meta get diff --register LISA --from 2015 --to 2020\n"
            "  reg-meta get diff --register LISA --from 2015 --to 2020 --variable Kon"
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
        "--variant",
        default=None,
        help="Filter by register variant ID (register_variant_id).",
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
            "  reg-meta get lineage Kon\n"
            "  reg-meta get lineage Kon --register LISA"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    get_lineage_p.add_argument("variable", help="Variable name or var_id.")
    get_lineage_p.add_argument(
        "--register", default=None, help="Filter by register (name or ID)."
    )

    get_cls_p = get_sub.add_parser(
        "classification",
        help="Show normalized code systems (SUN2000, SSYK, SNI, LKF, ...).",
        description=(
            "List classifications, show metadata, or dump the full code list.\n\n"
            "Classifications are normalized code systems (SUN, SSYK, SNI, ...)\n"
            "that aggregate the value codes produced by many variable instances.\n\n"
            "Examples:\n"
            "  reg-meta get classification --list\n"
            "  reg-meta get classification SUN2000\n"
            "  reg-meta get classification SUN2000 --codes\n"
            "  reg-meta get classification SUN2000 --codes --level 3"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    get_cls_p.add_argument(
        "classification",
        nargs="?",
        default=None,
        help="Classification short_name or id (e.g. SUN2000).",
    )
    cls_mode = get_cls_p.add_mutually_exclusive_group()
    cls_mode.add_argument(
        "--list",
        dest="list_all",
        action="store_true",
        help="Enumerate all classifications.",
    )
    cls_mode.add_argument(
        "--codes",
        action="store_true",
        help="Include the full code list.",
    )
    cls_mode.add_argument(
        "--variables",
        action="store_true",
        help="List variables tagged with this classification.",
    )
    get_cls_p.add_argument(
        "--level",
        type=int,
        default=None,
        help="With --codes: only include codes at this hierarchical level.",
    )
    get_cls_p.add_argument(
        "--only-valid",
        dest="only_valid",
        action="store_true",
        help=(
            "With --codes: only include canonical codes (is_valid=1). "
            "Empty for classifications without a canonical CSV."
        ),
    )
    get_cls_p.add_argument(
        "--limit",
        type=int,
        default=100,
        help="With --variables: max results (default 100).",
    )
    get_cls_p.add_argument(
        "--offset",
        type=int,
        default=0,
        help="With --variables: pagination offset (default 0).",
    )

    get_avail_p = get_sub.add_parser(
        "availability",
        help="Show temporal availability (years, gaps, aliases) for a variable or register.",
        description=(
            "Show when a variable or register is available across years.\n\n"
            "Auto-detects whether the target is a variable or register.\n\n"
            "Examples:\n"
            '  reg-meta get availability "Kön"\n'
            "  reg-meta get availability LISA\n"
            '  reg-meta get availability "Kön" --register LISA'
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
            '  reg-meta resolve --columns "Kon,FodelseAr,Kommun" --register LISA\n'
            '  echo \'["Kon","FodelseAr"]\' | reg-meta resolve --register LISA'
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

    update_p = sub.add_parser(
        "update",
        help="Update reg_meta package and database to the latest version.",
        description=(
            "Download the latest reg_meta package and pre-built database from\n"
            "GitHub Releases. Safe to run repeatedly — skips if already current.\n\n"
            "Examples:\n"
            "  reg-meta update            # interactive confirmation\n"
            "  reg-meta update --yes      # skip confirmation\n"
            "  reg-meta update --force    # re-download even if current"
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

    sub.add_parser(
        "info",
        help="Database stats and import metadata.",
        description=(
            "Show database path, schema version, import timestamp, and row\n"
            "counts per table.\n\n"
            "Examples:\n"
            "  reg-meta info"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
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
            "  reg-meta docs search inkomst\n"
            "  reg-meta docs search sysselsättning --register lisa --type variable"
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
            "  reg-meta docs get SyssStat\n"
            "  reg-meta docs get _overview"
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
            "  reg-meta docs list\n"
            "  reg-meta docs list --register lisa\n"
            "  reg-meta docs list --type variable --topic income"
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

    apply_leaf_help(parser)
    return parser


# ---------------------------------------------------------------------------
# Command handlers (thin wrappers around queries.py)
# ---------------------------------------------------------------------------


def _cmd_info(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
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
    return success_envelope(
        command="info",
        args_payload={},
        db_info={
            "schema_version": manifest.get("schema_version", "unknown"),
            "import_date": manifest.get("import_date", "unknown"),
        },
        data={"manifest": manifest, "table_counts": table_counts, "db_path": str(db)},
        duration_ms=duration_ms,
    ), 0


def _cmd_update(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    from .update import run_update

    start = time.perf_counter()
    db_dir = Path(args.db) if args.db else None
    result = run_update(db_dir=db_dir, tag=args.tag, force=args.force, yes=args.yes)
    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
        command="update",
        args_payload={"tag": args.tag, "force": args.force},
        db_info=None,
        data=result,
        duration_ms=duration_ms,
    ), 0


# ---------------------------------------------------------------------------
# Doc command handlers
# ---------------------------------------------------------------------------


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
    return success_envelope(
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
        raise RegMetaError(
            exit_code=EXIT_NOT_FOUND,
            code="doc_not_found",
            error_class="not_found",
            message=f"No documentation found for: {args.identifier!r}",
            remediation="Use `reg-meta docs list` to see available docs, or `reg-meta docs search <query>` to search.",
        )
    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
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
    return success_envelope(
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
    search results. Raises ``RegMetaError`` if the doc DB is missing or
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
        info = get_db_info(conn)
        data = search(
            conn,
            args.query,
            field=args.field,
            type=args.type,
            register=args.register,
            years=args.years,
            limit=args.limit,
            offset=args.offset,
            fold_groups=not args.no_fold,
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
            f"not shown (try: reg-meta docs search <query>)"
        )

    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
        command="search",
        args_payload={
            "query": args.query,
            "field": args.field,
            "type": args.type,
            "register": args.register,
            "years": args.years,
            "limit": args.limit,
            "offset": args.offset,
            "no_fold": args.no_fold,
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
        info = get_db_info(conn)
        registers = get_register(conn, args.register)
        data = registers[0] if len(registers) == 1 else {"registers": registers}
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
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
        info = get_db_info(conn)
        data = get_schema(
            conn,
            register_variant_id=args.register_variant_id,
            register=args.register,
            years=args.years,
            columns_like=args.columns_like,
        )
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    args_out: dict[str, Any] = {}
    if args.register_variant_id:
        args_out["register_variant_id"] = args.register_variant_id
    if args.register:
        args_out["register"] = args.register
    if args.years:
        args_out["years"] = args.years
    if args.columns_like:
        args_out["columns_like"] = args.columns_like
    return success_envelope(
        command="get schema",
        args_payload=args_out,
        db_info=info,
        data=data,
        duration_ms=duration_ms,
    ), 0


def _cmd_get_groups(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    if bool(args.register) == bool(args.classifications):
        raise RegMetaError(
            exit_code=EXIT_USAGE,
            code="usage_error",
            error_class="usage",
            message="Provide either a REGISTER or --classifications (not both).",
            remediation=(
                "Usage: reg-meta get groups <REGISTER> | "
                "reg-meta get groups --classifications"
            ),
        )
    start = time.perf_counter()
    db = db_path_from_args(args.db)
    conn = open_db(db)
    try:
        info = get_db_info(conn)
        if args.classifications:
            data = get_classification_concept_groups(conn)
        else:
            data = get_concept_groups(conn, args.register)
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
        command="get groups",
        args_payload={
            "register": args.register,
            "classifications": args.classifications,
        },
        db_info=info,
        data=data,
        duration_ms=duration_ms,
    ), 0


def _cmd_get_varinfo(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db = db_path_from_args(args.db)
    conn = open_db(db)
    try:
        info = get_db_info(conn)
        variables = get_varinfo(conn, args.variable, register=args.register)
        data: dict[str, Any] = (
            variables[0] if len(variables) == 1 else {"variables": variables}
        )
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
    except (RegMetaError, sqlite3.Error):
        pass

    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
        command="get varinfo",
        args_payload={"variable": args.variable, "register": args.register},
        db_info=info,
        data=data,
        duration_ms=duration_ms,
    ), 0


def _group_instances_by_codes(
    instances: list[dict[str, Any]],
    *,
    input_value: str,
    variable_name: str,
    year: int | None,
) -> dict[str, Any]:
    """Bucket instances by their (code, label) set so callers don't have to
    scroll through dozens of rows of identical codes. Used when a (variable,
    year) lookup hits multiple distinct value sets. Keys follow the glossary rename (see DESIGN.md → Glossary and Swedish↔English
    crosswalk) from `(vardekod, vardebenamning)`.
    """
    buckets: dict[tuple, list[dict[str, Any]]] = {}
    for inst in instances:
        key = tuple(sorted((v["code"], v["label"]) for v in inst["values"]))
        buckets.setdefault(key, []).append(inst)

    # Largest group first; tie-break by the value-set key for determinism.
    sorted_keys = sorted(buckets.keys(), key=lambda k: (-len(buckets[k]), k))

    groups_out: list[dict[str, Any]] = []
    for key in sorted_keys:
        members = buckets[key]
        registers = sorted({m["register_name"] for m in members})
        # Distinct owning variables in this code-group. When a numeric var_id
        # hit several A2.2 split siblings whose code sets differ, each lands in
        # its own group — surface the slugs so the caller sees which variable a
        # group belongs to (the var_id alone no longer identifies one).
        variable_slugs = sorted(
            {m["variable_slug"] for m in members if m.get("variable_slug")}
        )
        groups_out.append(
            {
                "values": [{"code": code, "label": label} for code, label in key],
                "instance_count": len(members),
                "register_count": len(registers),
                "registers": registers,
                "variable_slugs": variable_slugs,
                "instances": [
                    {
                        # A2.6: instances are variable_state rows now.
                        "state_id": m["state_id"],
                        "register_id": m["register_id"],
                        "register_name": m["register_name"],
                        "register_variant_id": m["register_variant_id"],
                        "variant_name": m["variant_name"],
                        "valid_from": m["valid_from"],
                        "valid_to": m["valid_to"],
                        "year": m["year"],
                    }
                    for m in members
                ],
            }
        )

    return {
        "input": input_value,
        "variable_name": variable_name,
        "year": year,
        "value_set_count": len(groups_out),
        "instance_count": len(instances),
        "register_count": len({i["register_name"] for i in instances}),
        "groups": groups_out,
    }


def _cmd_get_values(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db = db_path_from_args(args.db)
    conn = open_db(db)

    target = args.target
    args._collapsed_instances = 0
    args._collapsed_registers = 0

    try:
        info = get_db_info(conn)
        # A2.7: the by-CVID path is gone — the FQID is variable-grained and a
        # raw CVID is an internal build artifact with no consumer. A numeric
        # target resolves as a var_id (the variable's `provider_key`) inside
        # `get_values_by_variable`, like a variable name.
        multi = get_values_by_variable(
            conn, target, register=args.register, year=args.year
        )
        instances = multi["instances"]
        data: list[dict[str, Any]] | dict[str, Any]

        if args.year is not None:
            if not instances:
                raise RegMetaError(
                    exit_code=EXIT_NOT_FOUND,
                    code="not_found",
                    error_class="query",
                    message=(
                        f"No instance of '{target}' for year {args.year}"
                        + (f" in register '{args.register}'" if args.register else "")
                        + "."
                    ),
                    remediation=(
                        f"Run `reg-meta get availability {target}` to see "
                        "covered years."
                    ),
                )
            # >1 instance for one (variable, year) is common in SCB:
            # a variable like "Kön" can carry the same {1=Man, 2=Kvinna}
            # codes across dozens of registers and variants. The
            # *answer* (the codes) isn't ambiguous — only the
            # provenance is. Collapse on identical (vardekod,
            # vardebenamning) sets regardless of register; only fall
            # back to the multi-instance shape when codes truly
            # disagree.
            value_keys = {
                tuple(sorted((v["code"], v["label"]) for v in i["values"]))
                for i in instances
            }
            regs = sorted({i["register_name"] for i in instances})
            if len(value_keys) == 1:
                args._collapsed_instances = len(instances)
                args._collapsed_registers = len(regs)
                data = instances[0]["values"]
            else:
                data = _group_instances_by_codes(
                    instances,
                    input_value=multi["input"],
                    variable_name=multi["variable_name"],
                    year=args.year,
                )
        else:
            data = multi
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
        command="get values",
        args_payload={
            "target": target,
            "register": args.register,
            "year": args.year,
        },
        db_info=info,
        data=data,
        duration_ms=duration_ms,
    ), 0


def _cmd_get_datacolumns(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db = db_path_from_args(args.db)
    conn = open_db(db)
    try:
        info = get_db_info(conn)
        data = get_datacolumns(conn, args.variable, register=args.register)
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
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
        info = get_db_info(conn)
        data = get_coded_variables(
            conn,
            min_codes=args.min_codes,
            min_registers=args.min_registers,
            limit=args.limit,
        )
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
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
        raise RegMetaError(
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
        info = get_db_info(conn)
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
    return success_envelope(
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
        info = get_db_info(conn)
        data = get_lineage(conn, args.variable, register=args.register)
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    args_payload: dict[str, Any] = {"variable": args.variable}
    if args.register:
        args_payload["register"] = args.register
    return success_envelope(
        command="get lineage",
        args_payload=args_payload,
        db_info=info,
        data=data,
        duration_ms=duration_ms,
    ), 0


def _cmd_get_classification(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    if args.list_all and args.classification:
        raise RegMetaError(
            exit_code=EXIT_USAGE,
            code="usage_error",
            error_class="usage",
            message="--list does not take a positional argument.",
            remediation="Run `reg-meta get classification --list` (no name).",
        )
    if not args.list_all and not args.classification:
        raise RegMetaError(
            exit_code=EXIT_USAGE,
            code="usage_error",
            error_class="usage",
            message="Provide a classification short_name or use --list.",
            remediation="Try `reg-meta get classification --list`.",
        )
    if args.level is not None and not args.codes:
        raise RegMetaError(
            exit_code=EXIT_USAGE,
            code="usage_error",
            error_class="usage",
            message="--level requires --codes.",
            remediation="Add --codes to filter the code list by level.",
        )
    if args.only_valid and not args.codes:
        raise RegMetaError(
            exit_code=EXIT_USAGE,
            code="usage_error",
            error_class="usage",
            message="--only-valid requires --codes.",
            remediation="Add --codes to filter the code list by validity.",
        )

    start = time.perf_counter()
    db = db_path_from_args(args.db)
    conn = open_db(db)
    try:
        info = get_db_info(conn)
        if args.list_all:
            data: Any = {"classifications": list_classifications(conn)}
            args_payload: dict[str, Any] = {"list": True}
        elif args.codes:
            data = get_classification_codes(
                conn,
                args.classification,
                level=args.level,
                only_valid=args.only_valid,
            )
            args_payload = {"classification": args.classification}
            if args.level is not None:
                args_payload["level"] = args.level
            if args.only_valid:
                args_payload["only_valid"] = True
        elif args.variables:
            variables = search_variables_by_classification(
                conn,
                args.classification,
                limit=args.limit,
                offset=args.offset,
            )
            data = {"variables": variables, "count": len(variables)}
            args_payload = {
                "classification": args.classification,
                "limit": args.limit,
                "offset": args.offset,
            }
        else:
            data = get_classification(conn, args.classification)
            args_payload = {"classification": args.classification}
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
        command="get classification",
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
        info = get_db_info(conn)
        data = get_availability(conn, args.target, register=args.register)
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    args_payload: dict[str, Any] = {"target": args.target}
    if args.register:
        args_payload["register"] = args.register
    return success_envelope(
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
        raise RegMetaError(
            exit_code=EXIT_USAGE,
            code="usage_error",
            error_class="usage",
            message="No columns provided.",
            remediation="Use --columns or pass JSON array of strings on stdin.",
        )

    db = db_path_from_args(args.db)
    conn = open_db(db)
    try:
        info = get_db_info(conn)
        results = resolve(conn, columns, register=args.register)
    finally:
        conn.close()

    if args.require_match and any(r["status"] == "no_match" for r in results):
        raise RegMetaError(
            exit_code=EXIT_NO_MATCH,
            code="no_match",
            error_class="validation",
            message="One or more columns had no matches.",
            remediation="Check column names or provide --register.",
        )

    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
        command="resolve",
        args_payload={"columns": columns, "register": args.register},
        db_info=info,
        data={"columns": results},
        duration_ms=duration_ms,
    ), 0


# ---------------------------------------------------------------------------
# Table output
# ---------------------------------------------------------------------------


def _search_display_row(r: dict[str, Any]) -> dict[str, Any]:
    """Project one search result onto the table/list renderer's keys.

    `type: "group"` rows (#322) carry nested `members`/`matched` lists the
    renderer can't show — flatten them to counts and fill the generic columns
    (`variable_name` shows the group label) so a group row reads sensibly in
    both the pure-group and the mixed-type column sets. Leaf rows pass through,
    with the `concept_group` annotation re-keyed to a short `group` column.

    Classification rows (#350) carry `short_name`/`classification_name`/`fqid`,
    none of which are in the mixed-type fallback columns — so in a `--type all`
    table they'd render blank. Project their identity onto the generic columns
    (mirroring the group-row treatment) so they read sensibly there too; the
    pure-classification column set selects the native keys directly.

    `classification_succession` rows (#571) collapse an edition chain and carry
    the same `short_name`/`classification_name`/`fqid` plus an `editions` list —
    share the classification projection, and append a folded-family hint to
    `variable_name` (mirroring the group row's "(N/M members matched)" idiom) so
    the collapse reads clearly in a mixed table. A scalar `n_editions` count is
    also stamped (distinct from the raw `editions` list, which feeds --format
    json) for the classification column set's trailing `n_editions` column."""
    if r.get("type") == "code":
        # Code/value hits (#352) carry their owning variables/classifications as
        # nested lists the table renderer can't show — flatten to a representative
        # owner + the full counts (the full owner lists live in --format json).
        r = dict(r)
        first_var = (r.get("variables") or [{}])[0]
        first_cls = (r.get("classifications") or [{}])[0]
        r["variable_name"] = first_var.get("name", "")
        r["register_name"] = first_var.get("register") or first_cls.get(
            "short_name", ""
        )
        return r
    if r.get("type") != "group":
        is_classification = r.get("type") in (
            "classification",
            "classification_succession",
        )
        if is_classification or r.get("concept_group"):
            r = dict(r)
        if is_classification:
            r.setdefault("register_name", r.get("short_name", ""))
            r.setdefault("variable_name", r.get("classification_name", ""))
        if r.get("type") == "classification_succession":
            editions = len(r.get("editions") or [])
            r["variable_name"] = (
                f"{r.get('classification_name', '')} ({editions} editions)"
            )
            # Distinct from the raw `editions` list key (which feeds --format
            # json): a scalar count for the classification column set's trailing
            # `n_editions` column.
            r["n_editions"] = editions
        if r.get("concept_group"):
            r["group"] = r["concept_group"]
        return r
    matched = len(r.get("matched") or [])
    total = r.get("member_count") or 0
    return {
        "type": "group",
        "group_key": r.get("group_key", ""),
        "group_label": r.get("group_label", ""),
        "group": r.get("group_key", ""),
        "source": r.get("group_source", ""),
        "register_id": r.get("register_id") or "",
        "register_name": r.get("register_name") or "",
        "var_id": "",
        "variable_name": f"{r.get('group_label', '')} ({matched}/{total} members matched)",
        "matched": matched,
        "members": total,
    }


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
    write_to("", output_path, truncate=True)
    data = payload.get("data", {})

    # Pick columns based on what result types are in the payload
    if key == ("search", None):
        results = [_search_display_row(r) for r in data.get("results", [])]
        types = {r.get("type") for r in results}
        if types == {"datacolumn"}:
            cols = [
                "datacolumn",
                "register_id",
                "register_name",
                "var_id",
                "variable_name",
            ]
        elif types == {"code"}:
            # #352: one representative owner + the full counts; the complete
            # owning-variable / classification lists are in --format json.
            cols = [
                "code",
                "label",
                "variable_name",
                "register_name",
                "variable_count",
                "classification_count",
            ]
        elif types == {"varname"}:
            cols = ["variable_name", "register_id", "register_name", "var_id"]
        elif types == {"doc"}:
            cols = ["variable_name", "display_name"]
        elif types and types <= {"classification", "classification_succession"}:
            # Pure classification, pure succession-fold (#571), or a mix — all
            # carry the classification-native columns; `fqid` is the navigable
            # current-edition target a succession row exists to surface.
            cols = ["short_name", "classification_name", "fqid"]
        elif types == {"group"}:
            # Pure group fold (#322) — e.g. every hit was one month family.
            cols = [
                "group_key",
                "group_label",
                "source",
                "register_name",
                "matched",
                "members",
            ]
        else:
            cols = ["type", "register_id", "register_name", "var_id", "variable_name"]
        # Lone member hits carry a `concept_group` annotation (#322): surface
        # it as a trailing column only when at least one row has it.
        if types != {"group"} and any(r.get("group") for r in results):
            cols.append("group")
        # Succession folds (#571) carry an edition count; the classification
        # column set drops `variable_name` (where the "(N editions)" hint lives),
        # so surface the count as a trailing column when any row is a fold.
        if any(r.get("type") == "classification_succession" for r in results):
            cols.append("n_editions")
        write_formatted(
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
                        # `get_register` returns the raw `register.name` /
                        # `register_variant.name` columns from `SELECT *`; we
                        # re-key them under the entity-qualified names the
                        # CLI table renderer uses.
                        "register_name": r["name"],
                        "register_variant_id": v["register_variant_id"],
                        "variant_name": v.get("name", ""),
                    }
                )
        write_formatted(
            rows,
            ["register_id", "register_name", "register_variant_id", "variant_name"],
            output_path,
            fmt=fmt,
            fmt_explicit=fmt_explicit,
            hints=hints,
        )
    elif key == ("get", "groups"):
        # Variable-group payload nests groups under registers; the
        # --classifications payload is a flat group list. One row per group;
        # member FQIDs/facets live in --format json (hinted).
        rows = []
        if "registers" in data:
            for reg in data.get("registers", []):
                for g in reg.get("groups", []):
                    rows.append(
                        {
                            "register": reg.get("register_name", ""),
                            "group_key": g.get("key", ""),
                            "label": g.get("label", ""),
                            "source": g.get("source", ""),
                            "axes": ", ".join(g.get("axes", [])),
                            "members": g.get("member_count", 0),
                        }
                    )
            cols = ["register", "group_key", "label", "source", "axes", "members"]
        else:
            for g in data.get("groups", []):
                rows.append(
                    {
                        "group_key": g.get("key", ""),
                        "label": g.get("label", ""),
                        "source": g.get("source", ""),
                        "axes": ", ".join(g.get("axes", [])),
                        "members": g.get("member_count", 0),
                    }
                )
            cols = ["group_key", "label", "source", "axes", "members"]
        write_formatted(
            rows, cols, output_path, fmt=fmt, fmt_explicit=fmt_explicit, hints=hints
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
                        "register_variant_id": v.get("register_variant_id", ""),
                        "variant": v.get("variant_name", ""),
                        "years": year_range,
                        "versions": len(v.get("versions", [])),
                        "columns": total_cols,
                    }
                )
            write_formatted(
                rows,
                ["register_variant_id", "variant", "years", "versions", "columns"],
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
                                    "register_variant_id": v.get(
                                        "register_variant_id", ""
                                    ),
                                    "year": ver.get("year", ""),
                                    "alias": alias,
                                    "name": col.get("variable_name", ""),
                                    "source": col.get("source", ""),
                                    "var_id": col.get("var_id", ""),
                                    "group": col.get("concept_group") or "",
                                }
                            )
            cols = ["register_variant_id", "year", "alias", "name", "source", "var_id"]
            # #325: show the concept-group fold inline, but only when the
            # register actually has grouped columns — no dead column otherwise.
            if any(r["group"] for r in rows):
                cols.append("group")
            write_formatted(
                rows,
                cols,
                output_path,
                fmt=fmt,
                fmt_explicit=fmt_explicit,
                hints=hints,
            )
        else:
            rows = []
            for v in data.get("variants", []):
                for ver in v.get("versions", []):
                    # A2.6: editions are validity windows now. #321: render the
                    # window at its COARSEST exact period token (never round a
                    # sub-annual span down to a bare year) — raw `lo..hi` only
                    # when the window falls outside the period grammar.
                    period = period_token_for_bounds(
                        ver.get("valid_from", ""), ver.get("valid_to", "")
                    )
                    for col in ver.get("columns", []):
                        rows.append(
                            {
                                "period": period,
                                "var_id": col.get("var_id", ""),
                                "name": col.get("variable_name", ""),
                                "data_type": col.get("data_type", ""),
                                "aliases": col.get("aliases", ""),
                                "source": col.get("source", ""),
                                # A2.6: per-column vintage label, see reg_meta_build/DESIGN.md → Build-time triage (SCB) ('' for
                                # ordinary columns; e.g. sni92/sni2007 for a
                                # folded variable's two states in one window).
                                "vintage": col.get("value_set_version_label", ""),
                                "group": col.get("concept_group") or "",
                            }
                        )
            cols = [
                "period",
                "var_id",
                "name",
                "data_type",
                "aliases",
                "source",
                "vintage",
            ]
            # #325: show the concept-group fold inline, but only when the
            # register actually has grouped columns — no dead column otherwise.
            if any(r["group"] for r in rows):
                cols.append("group")
            write_formatted(
                rows,
                cols,
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
                        "name": v.get("name", ""),
                        # A2.6: per-state validity window instead of a
                        # register_version name + cvid.
                        "variant": inst.get("variant_name", ""),
                        "period": period_token_for_bounds(
                            inst.get("valid_from", ""), inst.get("valid_to", "")
                        ),
                        "data_type": inst.get("data_type", ""),
                        "aliases": ", ".join(inst.get("aliases", [])),
                        "values": inst.get("value_set_count", 0),
                    }
                )
        write_formatted(
            rows,
            [
                "register_id",
                "var_id",
                "name",
                "variant",
                "period",
                "data_type",
                "aliases",
                "values",
            ],
            output_path,
            fmt=fmt,
            fmt_explicit=fmt_explicit,
            hints=hints,
        )
    elif key == ("get", "values"):
        # Three payload shapes:
        #   list                       — flat value list (variable+year whose
        #                                instances collapsed to one code set)
        #   {instances: [...]}         — multi-year (no --year given)
        #   {groups: [...]}            — variable+year disagreement, grouped
        #                                by distinct value set
        if isinstance(data, list):
            write_formatted(
                data,
                ["code", "label"],
                output_path,
                fmt=fmt,
                fmt_explicit=fmt_explicit,
                hints=hints,
            )
        elif isinstance(data, dict) and "groups" in data:
            _write_groups_payload(data, output_path)
        elif isinstance(data, dict):
            instances = data.get("instances", [])
            # Show variant column only when it actually disambiguates rows —
            # otherwise it's noise. Detected by any (register, year) carrying
            # more than one register_variant_id.
            seen: dict[tuple[Any, Any], set[Any]] = {}
            for inst in instances:
                key = (inst.get("register_name"), inst.get("year"))
                seen.setdefault(key, set()).add(inst.get("register_variant_id"))
            show_variant = any(len(v) > 1 for v in seen.values())
            rows: list[dict[str, Any]] = []
            for inst in instances:
                values = inst.get("values", [])
                if not values:
                    continue
                for v in values:
                    row = {
                        "register": inst.get("register_name", ""),
                        "year": inst.get("year", ""),
                        # A2.6: per-state id (instances are variable_state rows).
                        "state_id": inst.get("state_id", ""),
                        "code": v.get("code", ""),
                        "label": v.get("label", ""),
                    }
                    if show_variant:
                        row["variant"] = inst.get("variant_name", "") or ""
                    rows.append(row)
            cols = ["register", "year"]
            if show_variant:
                cols.append("variant")
            cols += ["state_id", "code", "label"]
            write_formatted(
                rows,
                cols,
                output_path,
                fmt=fmt,
                fmt_explicit=fmt_explicit,
                hints=hints,
            )
    elif key == ("get", "datacolumns"):
        write_formatted(
            data if isinstance(data, list) else [],
            # A2.6: full alias list from variable_alias; the register_version
            # coordinate is gone, so the variant id stands in for grouping.
            [
                "delivery_column_name",
                "register_id",
                "register_name",
                "register_variant_id",
            ],
            output_path,
            fmt=fmt,
            fmt_explicit=fmt_explicit,
            hints=hints,
        )
    elif key == ("get", "coded-variables"):
        write_formatted(
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
                        "name": item["variable_name"],
                        "detail": f"{item['data_type']}  {item.get('aliases', [])}",
                    }
                )
            for item in v.get("removed", []):
                rows.append(
                    {
                        "variant": v.get("variant_name", ""),
                        "change": "-",
                        "var_id": item["var_id"],
                        "name": item["variable_name"],
                        "detail": f"{item['data_type']}  {item.get('aliases', [])}",
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
                        "name": item["variable_name"],
                        "detail": details,
                    }
                )
        resolved = data.get("resolved_variables", [])
        if resolved:
            lines = ["Resolved variables:"]
            for rv in resolved:
                # var_id is None for non-SCB providers (#466) — omit the suffix
                # rather than print "(var_id None)"; SOS/curated vars carry no
                # numeric var_id.
                suffix = f" (var_id {rv['var_id']})" if rv["var_id"] is not None else ""
                if rv["input"].lower() != rv["variable_name"].lower():
                    lines.append(f"  {rv['input']} → {rv['variable_name']}{suffix}")
                else:
                    lines.append(f"  {rv['variable_name']}{suffix}")
            write_to("\n".join(lines) + "\n\n", output_path)
        write_formatted(
            rows,
            ["variant", "change", "var_id", "name", "detail"],
            output_path,
            fmt=fmt,
            fmt_explicit=fmt_explicit,
            hints=hints,
        )
        unchanged = data.get("unchanged", [])
        if unchanged:
            write_to(f"\nUnchanged: {', '.join(unchanged)}\n", output_path)
    elif key == ("get", "lineage"):
        rows = []
        for r in data.get("registers", []):
            source_info = ""
            if r["role"] == "consumer" and r.get("source_register_text"):
                source_info = f"← {r['source_register_text']}"
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
        write_formatted(
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
            write_to(
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
            write_formatted(
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
                        "register_variant_id": v["register_variant_id"],
                        "variant_name": v["variant_name"],
                        "years": year_str,
                        "version_count": len(yr),
                    }
                )
            write_formatted(
                rows,
                ["register_variant_id", "variant_name", "years", "version_count"],
                output_path,
                fmt=fmt,
                fmt_explicit=fmt_explicit,
                hints=hints,
            )
        all_gaps = data.get("gaps", [])
        if all_gaps:
            write_to(f"\nGaps: {', '.join(str(g) for g in all_gaps)}\n", output_path)
    elif key == ("get", "classification"):
        if "classifications" in data:
            rows = [
                {
                    "short_name": c.get("short_name", ""),
                    "name": c.get("name", ""),
                    # A2.6.1: the vintage now lives in slug/short_name; surface
                    # the 2-seg `class/<slug>` FQID instead of the dropped
                    # `version` column.
                    "fqid": c.get("fqid", ""),
                    "publisher": c.get("publisher", ""),
                    "code_count": c.get("code_count", 0),
                    "supersedes": c.get("supersedes", ""),
                }
                for c in data.get("classifications", [])
            ]
            write_formatted(
                rows,
                [
                    "short_name",
                    "name",
                    "fqid",
                    "publisher",
                    "code_count",
                    "supersedes",
                ],
                output_path,
                fmt=fmt,
                fmt_explicit=fmt_explicit,
                hints=hints,
            )
        elif "codes" in data:
            header = (
                f"{data.get('short_name', '')} — {data.get('name', '')}\n"
                f"{data.get('code_count', len(data.get('codes', [])))} codes\n\n"
            )
            write_to(header, output_path)
            write_formatted(
                data.get("codes", []),
                ["code", "label", "level"],
                output_path,
                fmt=fmt,
                fmt_explicit=fmt_explicit,
                hints=hints,
            )
        elif "variables" in data:
            write_formatted(
                data.get("variables", []),
                ["register_id", "register_name", "var_id", "variable_name"],
                output_path,
                fmt=fmt,
                fmt_explicit=fmt_explicit,
                hints=hints,
            )
        else:
            rows = [
                {
                    "short_name": data.get("short_name", ""),
                    "name": data.get("name", ""),
                    "publisher": data.get("publisher", ""),
                    # A2.6.1: vintage is in slug/short_name + valid_from/valid_to;
                    # show the 2-seg `class/<slug>` FQID, not a `version` column.
                    "fqid": data.get("fqid", ""),
                    "valid_from": data.get("valid_from", ""),
                    "valid_to": data.get("valid_to", ""),
                    "code_count": data.get("code_count", 0),
                    "supersedes": data.get("supersedes", ""),
                    "superseded_by": data.get("superseded_by", ""),
                    "url": data.get("url", ""),
                }
            ]
            write_formatted(
                rows,
                [
                    "short_name",
                    "name",
                    "publisher",
                    "fqid",
                    "valid_from",
                    "valid_to",
                    "code_count",
                    "supersedes",
                    "superseded_by",
                    "url",
                ],
                output_path,
                fmt="list" if fmt == "table" else fmt,
                fmt_explicit=fmt_explicit,
                hints=hints,
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
        write_formatted(
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
        write_formatted(
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
        write_to("\n".join(header) + "\n\n", output_path)
        write_to(data.get("body", "") + "\n", output_path)
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
            write_formatted(
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
            write_to("\n".join(lines) + "\n", output_path)
    elif key == ("update", None):
        pass  # status messages already emitted on stderr by run_update()
    elif key == ("info", None):
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
        write_to("\n".join(lines) + "\n", output_path)
    else:
        write_json(payload, output_path)


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
        total = data.get("total_count", 0)
        results = data.get("results", [])
        group_rows = [r for r in results if r.get("type") == "group"]
        folded = sum(len(r.get("matched") or []) for r in group_rows)
        if folded:
            hint_add(
                hints,
                f"{folded} hit(s) folded into {len(group_rows)} concept group(s) "
                "(--no-fold to flatten; members in --format json)",
            )
        if getattr(args, "field", "all") == "all":
            hint_add(hints, "Searching all fields (--field to narrow)")
        if total > len(results):
            hint_add(
                hints,
                f"Showing {len(results)} of {total} matches (--limit/--offset to page)",
            )
        doc_hint = data.pop("doc_hint", None)
        if doc_hint:
            hint_add(hints, doc_hint)
        if total == 0 and not results:
            hint_add(hints, "No results (try broader --field or reg-meta docs search)")

    elif key == ("get", "groups"):
        n_groups = sum(
            len(reg.get("groups", [])) for reg in data.get("registers", [])
        ) + len(data.get("groups", []))
        if n_groups:
            hint_add(hints, "Member FQIDs, names, and facets in --format json")

    elif key == ("get", "schema"):
        if not getattr(args, "summary", False) and not getattr(args, "flat", False):
            hint_add(
                hints, "Full schema view (--summary for overview, --flat for export)"
            )

    elif key == ("get", "varinfo"):
        variables = data.get("variables", [data]) if "variables" in data else [data]
        n_regs = len({v.get("register_id") for v in variables})
        n_vars = len({v.get("var_id") for v in variables})
        if n_vars > 1:
            hint_add(
                hints,
                f"Alias maps to {n_vars} variable definitions across {n_regs} register(s) (--register to narrow)",
            )
        elif n_regs > 1:
            hint_add(hints, f"Found in {n_regs} registers (--register to narrow)")
        if any(v.get("doc_available") for v in variables):
            hint_add(
                hints,
                f"Docs available (run: reg-meta docs get {getattr(args, 'variable', '')})",
            )

    elif key == ("get", "values"):
        collapsed = getattr(args, "_collapsed_instances", 0)
        collapsed_regs = getattr(args, "_collapsed_registers", 0)
        if collapsed > 1:
            scope = (
                f"{collapsed_regs} register(s)"
                if collapsed_regs > 1
                else "different variants in one register"
            )
            hint_add(
                hints,
                f"Codes are identical across {collapsed} instance(s) "
                f"spanning {scope} — collapsed to one list.",
            )
        # Groups view (disagreement case) carries its own header summary —
        # no extra hint needed.
        if isinstance(data, dict) and "instances" in data:
            instances = data.get("instances", [])
            n_with = sum(1 for i in instances if i.get("values"))
            n_without = len(instances) - n_with
            n_regs = len({i.get("register_name") for i in instances})
            if n_without and n_with:
                hint_add(
                    hints,
                    f"{n_without}/{len(instances)} instance(s) had no value set "
                    "(elided from table; see JSON for full picture).",
                )
            elif n_without and not n_with:
                hint_add(
                    hints,
                    "No state carries a value set — variable may be "
                    "numeric/text, or year-projection emptied every state.",
                )
            if n_regs > 1 and not getattr(args, "register", None):
                hint_add(
                    hints,
                    f"Variable spans {n_regs} register(s); use --register to narrow.",
                )


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


COMMAND_DISPATCH = {
    ("info", None): _cmd_info,
    ("update", None): _cmd_update,
    ("search", None): _cmd_search,
    ("get", "register"): _cmd_get_register,
    ("get", "schema"): _cmd_get_schema,
    ("get", "groups"): _cmd_get_groups,
    ("get", "varinfo"): _cmd_get_varinfo,
    ("get", "values"): _cmd_get_values,
    ("get", "datacolumns"): _cmd_get_datacolumns,
    ("get", "coded-variables"): _cmd_get_coded_variables,
    ("get", "diff"): _cmd_get_diff,
    ("get", "lineage"): _cmd_get_lineage,
    ("get", "availability"): _cmd_get_availability,
    ("get", "classification"): _cmd_get_classification,
    ("resolve", None): _cmd_resolve,
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
        "A sub-table within a register (e.g. LISA/Individer). Has a register_variant_id.",
    ),
    (
        "variable",
        'A logical concept (e.g. "Kön"). Has a var_id. Shared across registers.',
    ),
    ("alias", "Column header in a data file. May differ across registers/versions."),
    (
        "state",
        "One era × variant shape of a variable (validity window + value set). "
        "Surfaced by `get varinfo` / `get values`.",
    ),
    (
        "value set",
        "Valid coded values for a categorical variable (e.g. 1=Man, 2=Kvinna).",
    ),
    (
        "group",
        "A derived concept group: a family of near-identical variables (month "
        "series, split siblings, vintages) folded for browse. Presentation-only "
        "— members keep their own FQIDs. Surfaced by `get groups` and search.",
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
    (
        "get groups REGISTER | get groups --classifications",
        "Concept groups (folded variable families) with member facets.",
    ),
    ("get varinfo VARIABLE [--register R]", "Variable details with instance history."),
    (
        "get values TARGET [--register R] [--year Y]",
        "Value codes for a variable — per state, or a year × codes view.",
    ),
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
    ("update [--tag TAG] [--force] [--yes]", "Update package and database."),
    ("info", "Database stats and import metadata."),
    None,
    (
        "(maintainer build commands moved to `reg-meta-build`:",
        "  build-db, build-docs, seed-slugs, precheck-slugs, parse-sos)",
    ),
]


def _version_line(db_arg: str | None = None) -> str:
    from . import __version__

    db_path = db_path_from_args(db_arg)
    db_status = str(db_path) if db_path.exists() else "not installed"
    return f"reg_meta v{__version__}  ·  db: {db_status}"


def _print_usage(db_arg: str | None = None) -> None:
    """Brief overview (bare `reg_meta` with no args)."""
    w = sys.stderr.write
    w(f"{_version_line(db_arg)}\n")
    db_path = db_path_from_args(db_arg)
    if not db_path.exists():
        w("\n  No database installed. Run `reg-meta update` to get started.\n")
    w("\nCommands:\n")
    info = _get_subcommand_info(_build_parser())
    col_w = max(len(name) for name, _, _ in info) + 2
    for name, _, help_text in info:
        w(f"  {name:<{col_w}} {help_text}\n")
    w(
        "\nRun `reg-meta --help` for full reference, `reg_meta --examples` for usage examples.\n"
    )


def _print_help(db_arg: str | None = None) -> None:
    """Full help (reg-meta --help)."""
    w = sys.stderr.write
    w(f"{_version_line(db_arg)}\n")
    db_path = db_path_from_args(db_arg)
    if not db_path.exists():
        w("\n  No database installed. Run `reg-meta update` to get started.\n")

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

    w("\nRun `reg_meta <command> --help` for detailed help.\n")
    w("Run `reg_meta --examples` for usage examples and workflows.\n")


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

    w(f"\nreg_meta {group_name} — {group_help}\n\n")
    w("Subcommands:\n")
    sub_info = _get_subcommand_info(group_p)
    col_w = max(len(n) for n, _, _ in sub_info) + 2
    for name, _, help_text in sub_info:
        w(f"  {name:<{col_w}} {help_text}\n")
    w(
        f"\nRun `reg_meta {group_name} <command> --help`"
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

    w(f"\nreg_meta {group_name} — {group_help}\n")
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
        if arg in GLOBAL_FLAGS:
            if arg in GLOBAL_FLAGS_WITH_VALUE:
                skip_next = True
            continue
        eq_name = arg.split("=", 1)[0] if "=" in arg else None
        if eq_name in GLOBAL_FLAGS_WITH_VALUE:
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
    reg-meta search --query utbildning --type register

  "Find income-related variables available after 2015"
    reg-meta search --query inkomst --years 2015-

  "Which columns in data files contain kommun?"
    reg-meta search --query kommun --field datacolumn

  "Find variables within LISA mentioning kommun"
    reg-meta search --query kommun --register LISA

  "What value codes include 0180?"
    reg-meta search --query 0180 --field value

  Hits on members of a concept group (e.g. a per-month variable family)
  collapse into one group row — its label matches too. Flatten with:
    reg-meta search --query lonfink --no-fold
""",
    "resolve": """\
resolve — Mapping column headers to official definitions
────────────────────────────────────────────────────────

  "I have a CSV with columns Kon, FodelseAr, AstKommun — what are they?"
    reg-meta resolve --columns "Kon,FodelseAr,AstKommun" --register LISA

  "Resolve columns from a JSON list"
    echo '["Kon","FodelseAr"]' | reg-meta resolve --register LISA

  resolve is exact match only. If a column shows no_match, try:
    reg-meta search --query AstKommun --field datacolumn
""",
    ("get", "register"): """\
get register — Register overview
────────────────────────────────

  "Tell me about LISA"
    reg-meta get register LISA

  "What register has ID 34?"
    reg-meta get register 34

  The output lists all variants (sub-tables) with their register_variant_id.
  Use the register_variant_id with `get schema` for column details.
""",
    ("get", "schema"): """\
get schema — What columns does a register have?
────────────────────────────────────────────────

  "What variables are in LISA?"
    reg-meta get schema --register LISA --summary

  "What columns does LISA 2022 have?"
    reg-meta get schema --register LISA --years 2022

  "Show education-related columns in register 340"
    reg-meta get schema --register 340 --columns-like "Merit|Betyg|Prov"

  "One row per column for easy scanning"
    reg-meta get schema --register LISA --flat --years 2022

  For large registers, always narrow with --years, --columns-like,
  --summary, or --flat. Unfiltered output can be very long.
""",
    ("get", "groups"): """\
get groups — Folded variable families (concept groups)
──────────────────────────────────────────────────────

  "Which variable families does LISA fold?"
    reg-meta get groups LISA

  "Show the member FQIDs and facets (month/rank/...) of each family"
    reg-meta --format json get groups LISA

  "Which curated classification umbrella groups exist?"
    reg-meta get groups --classifications

  Groups are presentation-only: members keep their own FQIDs and
  metadata. Search folds member hits into these same groups.
""",
    ("get", "varinfo"): """\
get varinfo — Variable details and history
──────────────────────────────────────────

  "What is the variable Kön?"
    reg-meta get varinfo "Kön"

  "Where does variable 44 appear?"
    reg-meta get varinfo 44

  "Show Kön only within LISA"
    reg-meta get varinfo "Kön" --register LISA

  The output lists each state's value-set count — use `get values`
  on the variable to see the actual code/label pairs.
""",
    ("get", "values"): """\
get values — What do the coded values mean?
───────────────────────────────────────────

  "How did ArbSokNov's codes evolve across LISA years?"
    reg-meta get values "ArbSokNov" --register LISA
        → year × codes table (one row per state, codes inline)

  "What codes were valid for ArbSokNov in 2015 specifically?"
    reg-meta get values "ArbSokNov" --register LISA --year 2015

  "What are the valid values for var_id 44?"
    reg-meta get values 44

  TARGET is a variable name, column alias, or var_id (numeric input
  resolves as a var_id). var_id is SCB-only — the legacy numeric id;
  SOS and curated variables have no var_id (it shows blank), so address
  them by name or column alias. Codes are year-projected through SCB
  validity windows at build time, so each state carries the year-correct set.

  When --year is given, instances sharing the same code/label set
  collapse to a single flat list (the answer is unambiguous even when
  the variable spans many registers); when value sets genuinely
  disagree, the result is bucketed by distinct value set instead.
  Use --register to narrow provenance to one register.
""",
    ("get", "datacolumns"): """\
get datacolumns — What column names does a variable appear under?
────────────────────────────────────────────────────────────────

  "What column headers does Kommun use across registers?"
    reg-meta get datacolumns "Kommun"

  "What aliases does Kön have in LISA specifically?"
    reg-meta get datacolumns "Kön" --register LISA
""",
    ("get", "coded-variables"): """\
get coded-variables — Which variables have value sets?
──────────────────────────────────────────────────────

  "What are the most widely used categorical variables?"
    reg-meta get coded-variables --min-registers 5

  "Find variables with many value codes"
    reg-meta get coded-variables --min-codes 50 --min-registers 10
""",
    ("get", "diff"): """\
get diff — How has a register changed?
──────────────────────────────────────

  "What changed in LISA between 2015 and 2020?"
    reg-meta get diff --register LISA --from 2015 --to 2020

  "Did Kon change between 2015 and 2020 in LISA?"
    reg-meta get diff --register LISA --from 2015 --to 2020 --variable Kon
""",
    ("get", "lineage"): """\
get lineage — Where does a variable come from?
──────────────────────────────────────────────

  "Which register is the source of Kön, and who consumes it?"
    reg-meta get lineage "Kön"

  "Where does LISA get Kön from?"
    reg-meta get lineage "Kön" --register LISA
""",
    ("get", "availability"): """\
get availability — When is something available?
───────────────────────────────────────────────

  "Is Kön available from 2015 to 2024?"
    reg-meta get availability "Kön"

  "What years does LISA cover?"
    reg-meta get availability LISA

  "When is Kön available in LISA specifically?"
    reg-meta get availability "Kön" --register LISA
""",
    ("get", "classification"): """\
get classification — Normalized code systems
────────────────────────────────────────────

  "What classifications exist?"
    reg-meta get classification --list

  "Show metadata for SUN2000"
    reg-meta get classification SUN2000

  "List every code in SUN2000"
    reg-meta get classification SUN2000 --codes

  "Top-level SSYK codes only"
    reg-meta get classification SSYK2012 --codes --level 1

  "Which variables use SUN2020?"
    reg-meta get classification SUN2020 --variables
""",
    ("docs", "search"): """\
docs search — Search curated documentation
──────────────────────────────────────────

  "What does the documentation say about income?"
    reg-meta docs search inkomst

  "Find documentation about SyssStat in LISA"
    reg-meta docs search SyssStat --register lisa --type variable
""",
    ("docs", "get"): """\
docs get — Read full documentation
──────────────────────────────────

  "Show me the full documentation for SyssStat"
    reg-meta docs get SyssStat

  "Show the LISA overview"
    reg-meta docs get _overview
""",
    ("docs", "list"): """\
docs list — Browse available documentation
──────────────────────────────────────────

  "What documentation is available?"
    reg-meta docs list

  "What LISA documentation exists?"
    reg-meta docs list --register lisa

  "Show all variable documentation about income"
    reg-meta docs list --type variable --topic income
""",
    "update": """\
update — Install or update the database
───────────────────────────────────────

  "Set up reg_meta for the first time"
    reg-meta update --yes

  "Update to the latest database"
    reg-meta update

  "Force re-download even if already current"
    reg-meta update --force --yes
""",
    "info": """\
info — What database am I using?
────────────────────────────────

  "Show database version, schema, and import stats"
    reg-meta info
""",
}

_WORKFLOW_EXAMPLES = """\
Common workflows
────────────────

  "What are the valid values for Kommun in LISA?"
    reg-meta get values "Kommun" --register LISA              → year × codes
    reg-meta get values "Kommun" --register LISA --year 2022  → one year's codes

  "I have a data file — what do the columns mean?"
    reg-meta resolve --columns "Kon,FodelseAr,AstKommun" --register LISA
    (for no_match columns, try search:)
    reg-meta search --query AstKommun --field datacolumn

  "Get structured output for programmatic use"
    reg-meta --format json get schema --register LISA --years 2022

  "How has my register changed since I last looked?"
    reg-meta get diff --register LISA --from 2018 --to 2023

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
    ("get", "groups"),
    ("get", "varinfo"),
    ("get", "values"),
    ("get", "datacolumns"),
    ("get", "coded-variables"),
    ("get", "diff"),
    ("get", "lineage"),
    ("get", "availability"),
    ("get", "classification"),
    ("docs", "search"),
    ("docs", "get"),
    ("docs", "list"),
    "update",
    "info",
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
    from . import __version__
    from .update import UpdateChecker

    sys.stderr.write(f"{_version_line(db_arg)}\n")
    sys.stderr.write("Checking for updates...\n")
    try:
        checker = UpdateChecker(http_timeout=10)
        newer = checker.get_newer_version(timeout=10)
        if newer:
            sys.stderr.write(
                f"Update available: v{__version__} → v{newer}"
                "  —  run `reg-meta update`\n"
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
    reordered = reorder_global_flags(effective)

    # Intercept --examples and group --help before argparse processes them
    cmd_args = _strip_global_flags(reordered)

    if "--examples" in cmd_args:
        _print_examples([a for a in cmd_args if a != "--examples"])
        return 0

    if (
        len(cmd_args) == 2
        and cmd_args[1] in ("-h", "--help")
        and cmd_args[0] in ("get", "docs")
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
    if args.command == "get":
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
    quiet = getattr(args, "quiet", False) or os.environ.get("REG_META_QUIET") == "1"
    hints: list[str] = []

    # Kick off background update check early so it runs in parallel with the
    # actual command.  We collect the result (with a short timeout) just before
    # returning so the user never waits for it.
    update_checker = None
    if not quiet and fmt != "json" and key != ("update", None):
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
                write_json(payload, output_path)
            else:
                write_json(payload.get("data", payload), output_path)
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
            emit_hints(hints)
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
                        "  —  run `reg-meta update`\n"
                    )
            except Exception:
                pass
        return exit_code
    except Exception as exc:
        return handle_cli_exception(exc, getattr(args, "output", None))


def main() -> int:
    return run()
