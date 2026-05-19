"""CLI entry point for regmeta."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Callable

from .cli_common import (
    GLOBAL_FLAGS,
    GLOBAL_FLAGS_WITH_VALUE,
    NoRepeatParser,
    clean_leaf_help,
    db_info,
    emit_hints,
    hint_add,
    reorder_global_flags,
    success_envelope,
    write_formatted,
    write_json,
    write_to,
)
from .db import (
    SCHEMA_VERSION,
    default_db_dir,
    build_db,
    db_path_from_args,
    get_manifest,
    open_db,
)
from .errors import EXIT_CONFIG, EXIT_INTERNAL, EXIT_NOT_FOUND, EXIT_USAGE, RegmetaError
from .validate import validate_built_db
from .queries import (
    get_availability,
    get_classification,
    get_classification_codes,
    get_coded_variables,
    get_datacolumns,
    get_diff,
    get_lineage,
    get_register,
    get_schema,
    get_values,
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
    name = data.get("variabelnamn") or data.get("input") or ""

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
            kod = v.get("vardekod") or "(empty)"
            label = v.get("vardebenamning", "")
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
        help="Get value-set members (code + label) by variable or CVID.",
        description=(
            "Show code/label pairs for a categorical variable's value set.\n"
            "Codes are projected to each cvid's regver year via SCB validity\n"
            "windows at build time, so the result is the year-correct set.\n\n"
            "TARGET dispatch: a fully numeric arg is treated as a CVID; any\n"
            "other arg is treated as a variable name (or column alias).\n\n"
            "Examples:\n"
            "  regmeta get values 1001                                  # by CVID\n"
            '  regmeta get values "ArbSokNov" --register LISA            # year × codes table\n'
            '  regmeta get values "ArbSokNov" --register LISA --year 2015  # codes for one year'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    get_values_p.add_argument(
        "target",
        help="Variable name, column alias, or CVID. Numeric input is treated as a CVID.",
    )
    get_values_p.add_argument(
        "--register",
        default=None,
        help="Filter by register (only used with a variable target).",
    )
    get_values_p.add_argument(
        "--year",
        type=int,
        default=None,
        help="Filter to a single year (only used with a variable target).",
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

    get_cls_p = get_sub.add_parser(
        "classification",
        help="Show normalized code systems (SUN2000, SSYK, SNI, LKF, ...).",
        description=(
            "List classifications, show metadata, or dump the full code list.\n\n"
            "Classifications are normalized code systems (SUN, SSYK, SNI, ...)\n"
            "that aggregate the value codes produced by many variable instances.\n\n"
            "Examples:\n"
            "  regmeta get classification --list\n"
            "  regmeta get classification SUN2000\n"
            "  regmeta get classification SUN2000 --codes\n"
            "  regmeta get classification SUN2000 --codes --level 3"
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
            "The input directory must contain:\n"
            "  <input-dir>/SCB/*.csv             — SCB metadata exports\n"
            "  <input-dir>/classifications/*.csv — canonical classification CSVs (optional)\n\n"
            "Examples:\n"
            "  regmeta maintain build-db --input-dir regmeta/input_data/\n"
            "  regmeta maintain build-db --input-dir regmeta/input_data/ --skip-slugs"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    build_p.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing SCB/ and classifications/ subdirectories.",
    )
    build_p.add_argument(
        "--slug-dir",
        default=None,
        help=(
            "Directory of curated slug TOMLs (default: regmeta/fqid_slugs/ "
            "when run from a repo checkout)."
        ),
    )
    build_p.add_argument(
        "--skip-slugs",
        action="store_true",
        help=(
            "Skip slug TOML loading and the strict-coverage check. Used to "
            "bootstrap the DB so `maintain seed-slugs` has something to read "
            "from before the slug TOMLs exist (REFACTOR_SPEC §5.4 Activation). "
            "Implies `--slug-dir` is ignored; the resulting DB has empty slug "
            "columns and is intended only as input to `seed-slugs`, not for "
            "downstream queries that depend on FQIDs."
        ),
    )
    build_p.add_argument(
        "--validate",
        action="store_true",
        help=(
            "Run post-build invariant checks (value-set dedup, year-projection "
            "anchors, FK integrity, freelist ceiling) against the freshly-built "
            "DB. Fails with EXIT_CONFIG on any violation. Equivalent to running "
            "`scripts/validate_valueset_dedup.py` after the build."
        ),
    )

    seed_slugs_p = maintain_sub.add_parser(
        "seed-slugs",
        help="Emit starter slug TOMLs from the current DB (maintainer-only).",
        description=(
            "Generate hand-review starter TOMLs at <out-dir>/<provider>.toml\n"
            "and <out-dir>/classifications.toml, mirroring REFACTOR_SPEC §5.3.\n"
            "Slugs are auto-derived from registernamn / variantnamn / short_name\n"
            "and need maintainer review before commit.\n\n"
            "Examples:\n"
            "  regmeta maintain seed-slugs\n"
            "  regmeta maintain seed-slugs --out-dir /tmp/slugs/"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    seed_slugs_p.add_argument(
        "--out-dir",
        default=None,
        help=(
            "Where to write the TOMLs (default: regmeta/fqid_slugs/ in a repo "
            "checkout, else CWD/fqid_slugs/)."
        ),
    )
    seed_slugs_p.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing TOMLs in --out-dir.",
    )
    seed_slugs_p.add_argument(
        "--all-hints",
        action="store_true",
        help=(
            "Show every `_default` candidate in the stderr hint block instead "
            "of the default ~5-row preview. Pass the global -q/--quiet to "
            "suppress the hint block entirely."
        ),
    )

    precheck_p = maintain_sub.add_parser(
        "precheck-slugs",
        help="Validate slug TOMLs and list source IDs missing a slug entry.",
        description=(
            "Verify the slug TOMLs match the current DB. Reports:\n"
            "  - TOML parse / validation errors\n"
            "  - register / register_variant / classification rows with no slug\n"
            "  - non-additive changes vs. the committed snapshot (§5.4)\n\n"
            "Exits 10 if any check fails (cleaner failure mode than a build).\n\n"
            "Examples:\n"
            "  regmeta maintain precheck-slugs\n"
            "  regmeta maintain precheck-slugs --update-snapshot"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    precheck_p.add_argument(
        "--slug-dir",
        default=None,
        help="Directory of slug TOMLs (default: regmeta/fqid_slugs/).",
    )
    precheck_p.add_argument(
        "--update-snapshot",
        action="store_true",
        help=(
            "Rewrite the snapshot file to match the current TOMLs. Skips the "
            "snapshot diff but still exits non-zero on parse errors / missing "
            "slugs so a broken state isn't snapshot-frozen."
        ),
    )

    parse_sos_p = maintain_sub.add_parser(
        "parse-sos",
        help="Parse Socialstyrelsen metadata Excel deliveries (maintainer-only).",
        description=(
            "Parse one Socialstyrelsen register .xlsx (or a directory of them)\n"
            "into structured JSON. Useful for inspecting upstream deliveries\n"
            "before build-db. Does not modify the database.\n\n"
            "Examples:\n"
            "  regmeta maintain parse-sos input_data/Socialstyrelsen/\n"
            "  regmeta maintain parse-sos input_data/Socialstyrelsen/PAR.xlsx"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parse_sos_p.add_argument(
        "path",
        help="Path to an .xlsx file or a directory containing them.",
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
                        clean_leaf_help(leaf_p)
                else:
                    clean_leaf_help(sub_p)

    return parser


# ---------------------------------------------------------------------------
# Command handlers (thin wrappers around queries.py)
# ---------------------------------------------------------------------------


def _build_validate_hook() -> Callable[[Path], None]:
    """Return a build_db pre_rename_hook that runs the value-set dedup
    validator against the staging DB and raises on failure. Defined as a
    helper so the closure stays narrowly scoped (issue #92, Copilot review)."""

    def hook(staging_db: Path) -> None:
        validation = validate_built_db(staging_db)
        sys.stderr.write(validation.format_report() + "\n")
        sys.stderr.flush()
        if validation.failures:
            raise RegmetaError(
                exit_code=EXIT_CONFIG,
                code="validation_failed",
                error_class="configuration",
                message=(
                    f"Post-build validation failed: {len(validation.failures)} "
                    f"check(s) — {'; '.join(validation.failures)}"
                ),
                remediation=(
                    "Inspect the [FAIL] lines above. The staging DB has been "
                    "discarded and the previously-installed DB is unchanged. "
                    "Fix the underlying build issue and rerun "
                    "`regmeta maintain build-db --validate`."
                ),
            )

    return hook


def _cmd_maintain_build_db(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db_dir = Path(args.db) if args.db else default_db_dir()
    slug_dir = Path(args.slug_dir).expanduser().resolve() if args.slug_dir else None

    pre_rename_hook = _build_validate_hook() if args.validate else None
    result = build_db(
        input_dir=Path(args.input_dir),
        db_dir=db_dir,
        slug_dir=slug_dir,
        skip_slugs=args.skip_slugs,
        pre_rename_hook=pre_rename_hook,
    )
    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
        command="maintain build-db",
        args_payload={
            "input_dir": args.input_dir,
            "skip_slugs": args.skip_slugs,
            "validate": args.validate,
        },
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
    return success_envelope(
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
    return success_envelope(
        command="maintain update",
        args_payload={"tag": args.tag, "force": args.force},
        db_info=None,
        data=result,
        duration_ms=duration_ms,
    ), 0


def _resolve_slug_dir(slug_arg: str | None) -> Path:
    from .fqid_slugs import repo_slug_dir

    if slug_arg is not None:
        return Path(slug_arg).expanduser().resolve()
    resolved = repo_slug_dir()
    if resolved is None:
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="slug_dir_not_found",
            error_class="configuration",
            message=(
                "Slug TOMLs not found. Pass --slug-dir or run from a regmeta "
                "checkout containing regmeta/fqid_slugs/."
            ),
            remediation=(
                "Run from a repo checkout, or `regmeta maintain seed-slugs` "
                "to bootstrap a new slug directory."
            ),
        )
    return resolved


def _cmd_maintain_seed_slugs(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    from .fqid_slugs import (
        format_default_slug_hints,
        iter_default_slug_candidates,
        repo_slug_dir,
        seed_all,
    )

    start = time.perf_counter()
    db = db_path_from_args(args.db)
    if args.out_dir:
        out_dir = Path(args.out_dir).expanduser().resolve()
    else:
        out_dir = repo_slug_dir() or (Path.cwd() / "fqid_slugs").resolve()
    if out_dir.exists() and any(out_dir.glob("*.toml")) and not args.force:
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="slug_seed_would_overwrite",
            error_class="configuration",
            message=f"{out_dir} already contains TOMLs; refusing to overwrite.",
            remediation=(
                "Pass --force to overwrite, or point --out-dir at an empty "
                "directory for hand-review."
            ),
        )
    # `seed_provider_toml` reads `register_version.slug` (3.3+), so a stale
    # pre-3.3 DB would otherwise fall out as a raw `OperationalError: no such
    # column: rver.slug`. Schema-compat gives the user the right remediation.
    conn = open_db(db)
    try:
        written = seed_all(conn, out_dir)
        suppress_hints = (
            args.quiet
            or os.environ.get("REGMETA_QUIET") == "1"
            or getattr(args, "format", "table") == "json"
        )
        if not suppress_hints:
            hint = format_default_slug_hints(
                list(iter_default_slug_candidates(conn)),
                all_hints=args.all_hints,
            )
            if hint is not None:
                sys.stderr.write(hint)
    finally:
        conn.close()
    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
        command="maintain seed-slugs",
        args_payload={
            "out_dir": str(out_dir),
            "force": args.force,
            "all_hints": args.all_hints,
            "quiet": args.quiet,
        },
        db_info=None,
        data={
            "out_dir": str(out_dir),
            "files": sorted(written.keys()),
        },
        duration_ms=duration_ms,
    ), 0


def _cmd_maintain_precheck_slugs(
    args: argparse.Namespace,
) -> tuple[dict[str, Any], int]:
    from .fqid_slugs import (
        SNAPSHOT_FILENAME,
        diff_snapshot,
        is_unfrozen,
        precheck_slugs,
        read_snapshot,
        snapshot_payload,
        write_snapshot,
    )

    start = time.perf_counter()
    slug_dir = _resolve_slug_dir(args.slug_dir)
    snapshot_path = slug_dir / SNAPSHOT_FILENAME
    unfrozen = is_unfrozen(slug_dir)
    db = db_path_from_args(args.db)
    conn = open_db(db, check_schema=False)
    try:
        result = precheck_slugs(conn, slug_dir)
    finally:
        conn.close()

    snapshot_status: dict[str, Any] = {"path": str(snapshot_path), "unfrozen": unfrozen}
    exit_code = EXIT_CONFIG if not result.ok else 0
    current = snapshot_payload(list(result.entries))
    if args.update_snapshot:
        # Refuse to overwrite when TOMLs failed to parse — `result.entries`
        # is the truncated set up to the first error, so writing would wipe
        # the prior baseline and surface phantom `removed` diffs on the next
        # run.
        if result.parse_errors:
            snapshot_status["updated"] = False
            snapshot_status["update_skipped_reason"] = "parse_errors"
        else:
            # §5.4 grow-only enforcement: `--update-snapshot` must NOT bless
            # a removal or a slug rename — that's how committed FQIDs rot in
            # researcher project_data.json files. The `UNFROZEN` sentinel in
            # the slug dir lifts the refusal pre-v1 so curators can iterate
            # freely; diffs are still reported so drift stays visible. At v1
            # release the sentinel is deleted and refusal becomes active.
            previous = read_snapshot(snapshot_path)
            diff = diff_snapshot(previous, current)
            non_additive = bool(diff["removed"] or diff["renamed"])
            if non_additive and not unfrozen:
                snapshot_status["updated"] = False
                snapshot_status["update_skipped_reason"] = "non_additive_change"
                snapshot_status["removed"] = diff["removed"]
                snapshot_status["renamed"] = diff["renamed"]
                exit_code = EXIT_CONFIG
            else:
                write_snapshot(snapshot_path, current)
                snapshot_status["updated"] = True
                snapshot_status["added"] = diff["added"]
                if non_additive:
                    # Pre-v1 write-through: surface what drifted so reviewers
                    # see the rename/removal explicitly in the envelope.
                    snapshot_status["removed"] = diff["removed"]
                    snapshot_status["renamed"] = diff["renamed"]
    else:
        previous = read_snapshot(snapshot_path)
        diff = diff_snapshot(previous, current)
        snapshot_status["added"] = diff["added"]
        snapshot_status["removed"] = diff["removed"]
        snapshot_status["renamed"] = diff["renamed"]
        # `added` is non-fatal in spirit but must fail CI so a maintainer
        # doesn't merge new slugs without refreshing .snapshot.json; mirrors
        # test_slug_snapshot.test_snapshot_covers_committed_additions. The
        # pre-v1 `UNFROZEN` sentinel doesn't relax this — drift must still
        # round-trip through `--update-snapshot` to commit cleanly.
        if diff["removed"] or diff["renamed"] or diff["added"]:
            exit_code = EXIT_CONFIG

    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
        command="maintain precheck-slugs",
        args_payload={
            "slug_dir": str(slug_dir),
            "update_snapshot": args.update_snapshot,
        },
        db_info=None,
        data={
            "slug_dir": str(slug_dir),
            "missing_registers": [
                {"provider": p, "source_id": sid, "registernamn": name}
                for (p, sid, name) in result.missing_registers
            ],
            "missing_variants": [
                {"provider": p, "source_id": sid, "name": name}
                for (p, sid, name) in result.missing_variants
            ],
            "missing_versions": [
                {"provider": p, "source_id": sid, "name": name}
                for (p, sid, name) in result.missing_versions
            ],
            "missing_classifications": list(result.missing_classifications),
            "parse_errors": list(result.parse_errors),
            "stale_registers": [
                {"provider": p, "source_id": sid} for (p, sid) in result.stale_registers
            ],
            "stale_variants": [
                {"provider": p, "source_id": sid} for (p, sid) in result.stale_variants
            ],
            "stale_versions": [
                {"provider": p, "source_id": sid} for (p, sid) in result.stale_versions
            ],
            "stale_classifications": list(result.stale_classifications),
            "colliding_versions": [
                {"provider": p, "source_id": sid, "name": name, "would_be_slug": slug}
                for (p, sid, name, slug) in result.colliding_versions
            ],
            "snapshot": snapshot_status,
        },
        duration_ms=duration_ms,
    ), exit_code


def _cmd_maintain_parse_sos(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    import dataclasses
    from datetime import date

    from .sources.sos import SosParseError, parse_directory, parse_register_file

    start = time.perf_counter()
    path = Path(args.path).expanduser().resolve()

    try:
        if path.is_dir():
            results = parse_directory(path)
        elif path.is_file():
            results = [parse_register_file(path)]
        else:
            raise RegmetaError(
                exit_code=EXIT_NOT_FOUND,
                code="path_not_found",
                error_class="input",
                message=f"{path} is neither a file nor a directory",
                remediation="Pass a .xlsx file or a directory containing them.",
            )
    except SosParseError as exc:
        raise RegmetaError(
            exit_code=EXIT_CONFIG,
            code="sos_parse_error",
            error_class="input",
            message=str(exc),
            remediation="Verify the file is a valid Socialstyrelsen metadata workbook.",
        ) from exc

    def _to_plain(obj: Any) -> Any:
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            return _to_plain(dataclasses.asdict(obj))
        if isinstance(obj, dict):
            return {k: _to_plain(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [_to_plain(v) for v in obj]
        if isinstance(obj, Path):
            return str(obj)
        if isinstance(obj, date):
            return obj.isoformat()
        return obj

    data = {
        "registers": [_to_plain(r) for r in results],
        "register_count": len(results),
    }
    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
        command="maintain parse-sos",
        args_payload={"path": str(path)},
        db_info=None,
        data=data,
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
        raise RegmetaError(
            exit_code=EXIT_NOT_FOUND,
            code="doc_not_found",
            error_class="not_found",
            message=f"No documentation found for: {args.identifier!r}",
            remediation="Use `regmeta docs list` to see available docs, or `regmeta docs search <query>` to search.",
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
        info = db_info(conn)
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
        info = db_info(conn)
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
        info = db_info(conn)
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
    return success_envelope(
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
        info = db_info(conn)
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
    variabelnamn: str,
    year: int | None,
) -> dict[str, Any]:
    """Bucket instances by their (vardekod, vardebenamning) set so callers
    don't have to scroll through dozens of rows of identical codes. Used when
    a (variable, year) lookup hits multiple distinct value sets.
    """
    buckets: dict[tuple, list[dict[str, Any]]] = {}
    for inst in instances:
        key = tuple(
            sorted((v["vardekod"], v["vardebenamning"]) for v in inst["values"])
        )
        buckets.setdefault(key, []).append(inst)

    # Largest group first; tie-break by the value-set key for determinism.
    sorted_keys = sorted(buckets.keys(), key=lambda k: (-len(buckets[k]), k))

    groups_out: list[dict[str, Any]] = []
    for key in sorted_keys:
        members = buckets[key]
        registers = sorted({m["register_name"] for m in members})
        groups_out.append(
            {
                "values": [
                    {"vardekod": code, "vardebenamning": label} for code, label in key
                ],
                "instance_count": len(members),
                "register_count": len(registers),
                "registers": registers,
                "instances": [
                    {
                        "cvid": m["cvid"],
                        "register_id": m["register_id"],
                        "register_name": m["register_name"],
                        "regvar_id": m["regvar_id"],
                        "variant_name": m["variant_name"],
                        "regver_id": m["regver_id"],
                        "version_name": m["version_name"],
                        "year": m["year"],
                    }
                    for m in members
                ],
            }
        )

    return {
        "input": input_value,
        "variabelnamn": variabelnamn,
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
    is_cvid = target.isdigit()
    args._is_cvid = is_cvid
    args._projection_emptied = False
    args._collapsed_instances = 0
    args._collapsed_registers = 0

    try:
        info = db_info(conn)
        if is_cvid:
            if args.register is not None or args.year is not None:
                raise RegmetaError(
                    exit_code=EXIT_USAGE,
                    code="usage_error",
                    error_class="usage",
                    message=(
                        "--register and --year are not valid with a CVID; "
                        "a CVID already pins one register/year."
                    ),
                    remediation=(
                        "Drop --register/--year, or pass a variable name "
                        "instead of a CVID."
                    ),
                )
            data: list[dict[str, Any]] | dict[str, Any] = get_values(conn, target)
            # Discriminate empty results: a cvid with `vardemangdsversion IS NOT NULL`
            # had real Vardemangder rows but year-projection excluded every code, so
            # the empty list signals an SCB validity gap rather than a numeric/text
            # variable. Read alongside the data so the hint layer can surface it.
            if not data:
                args._projection_emptied = bool(
                    conn.execute(
                        "SELECT 1 FROM variable_instance "
                        "WHERE cvid = ? AND value_set_id IS NULL "
                        "AND vardemangdsversion IS NOT NULL",
                        (target,),
                    ).fetchone()
                )
        else:
            multi = get_values_by_variable(
                conn, target, register=args.register, year=args.year
            )
            instances = multi["instances"]

            if args.year is not None:
                if not instances:
                    raise RegmetaError(
                        exit_code=EXIT_NOT_FOUND,
                        code="not_found",
                        error_class="query",
                        message=(
                            f"No instance of '{target}' for year {args.year}"
                            + (
                                f" in register '{args.register}'"
                                if args.register
                                else ""
                            )
                            + "."
                        ),
                        remediation=(
                            f"Run `regmeta get availability {target}` to see "
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
                    tuple(
                        sorted(
                            (v["vardekod"], v["vardebenamning"]) for v in i["values"]
                        )
                    )
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
                        variabelnamn=multi["variabelnamn"],
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
        info = db_info(conn)
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
        info = db_info(conn)
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
        info = db_info(conn)
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
        info = db_info(conn)
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
        raise RegmetaError(
            exit_code=EXIT_USAGE,
            code="usage_error",
            error_class="usage",
            message="--list does not take a positional argument.",
            remediation="Run `regmeta get classification --list` (no name).",
        )
    if not args.list_all and not args.classification:
        raise RegmetaError(
            exit_code=EXIT_USAGE,
            code="usage_error",
            error_class="usage",
            message="Provide a classification short_name or use --list.",
            remediation="Try `regmeta get classification --list`.",
        )
    if args.level is not None and not args.codes:
        raise RegmetaError(
            exit_code=EXIT_USAGE,
            code="usage_error",
            error_class="usage",
            message="--level requires --codes.",
            remediation="Add --codes to filter the code list by level.",
        )
    if args.only_valid and not args.codes:
        raise RegmetaError(
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
        info = db_info(conn)
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
        info = db_info(conn)
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
        info = db_info(conn)
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
                        "register_name": r["registernamn"],
                        "regvar_id": v["regvar_id"],
                        "variant_name": v.get("registervariantnamn", ""),
                    }
                )
        write_formatted(
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
            write_formatted(
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
            write_formatted(
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
            write_formatted(
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
        write_formatted(
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
        # Three payload shapes:
        #   list                       — flat value list (cvid path, or
        #                                variable+year that collapsed)
        #   {instances: [...]}         — multi-year (no --year given)
        #   {groups: [...]}            — variable+year disagreement, grouped
        #                                by distinct value set
        if isinstance(data, list):
            write_formatted(
                data,
                ["vardekod", "vardebenamning"],
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
            # more than one regvar_id.
            seen: dict[tuple[Any, Any], set[Any]] = {}
            for inst in instances:
                key = (inst.get("register_name"), inst.get("year"))
                seen.setdefault(key, set()).add(inst.get("regvar_id"))
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
                        "cvid": inst.get("cvid", ""),
                        "vardekod": v.get("vardekod", ""),
                        "vardebenamning": v.get("vardebenamning", ""),
                    }
                    if show_variant:
                        row["variant"] = inst.get("variant_name", "") or ""
                    rows.append(row)
            cols = ["register", "year"]
            if show_variant:
                cols.append("variant")
            cols += ["cvid", "vardekod", "vardebenamning"]
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
            ["kolumnnamn", "register_id", "register_name", "version_name"],
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
            write_to("\n".join(lines) + "\n\n", output_path)
        write_formatted(
            rows,
            ["variant", "change", "var_id", "variabelnamn", "detail"],
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
                        "regvar_id": v["regvar_id"],
                        "variant_name": v["variant_name"],
                        "years": year_str,
                        "version_count": len(yr),
                    }
                )
            write_formatted(
                rows,
                ["regvar_id", "variant_name", "years", "version_count"],
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
                    "version": c.get("version", ""),
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
                    "version",
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
                ["vardekod", "vardebenamning", "level"],
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
                    "version": data.get("version", ""),
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
                    "version",
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
        write_to("\n".join(lines) + "\n", output_path)
    elif key == ("maintain", "build-db"):
        write_to(f"Database built: {data.get('db_path', 'unknown')}\n", output_path)
    elif key == ("maintain", "seed-slugs"):
        lines = [f"Seeded slug TOMLs into {data.get('out_dir', '?')}:"]
        for name in data.get("files", []):
            lines.append(f"  {name}")
        lines.append("")
        lines.append("Hand-review every slug, then commit.")
        write_to("\n".join(lines) + "\n", output_path)
    elif key == ("maintain", "precheck-slugs"):
        lines: list[str] = []
        parse_errors = data.get("parse_errors", [])
        if parse_errors:
            lines.append("TOML errors:")
            for err in parse_errors:
                lines.append(f"  {err}")
            lines.append("")
        missing_regs = data.get("missing_registers", [])
        if missing_regs:
            lines.append(f"Missing register slugs ({len(missing_regs)}):")
            for m in missing_regs[:20]:
                lines.append(f"  {m['provider']}/{m['source_id']}  {m['registernamn']}")
            if len(missing_regs) > 20:
                lines.append(f"  ... and {len(missing_regs) - 20} more")
            lines.append("")
        missing_vars = data.get("missing_variants", [])
        if missing_vars:
            lines.append(f"Missing variant slugs ({len(missing_vars)}):")
            for m in missing_vars[:20]:
                lines.append(f"  {m['provider']}/{m['source_id']}  {m['name']}")
            if len(missing_vars) > 20:
                lines.append(f"  ... and {len(missing_vars) - 20} more")
            lines.append("")
        missing_vers = data.get("missing_versions", [])
        if missing_vers:
            lines.append(f"Missing unperiodized version slugs ({len(missing_vers)}):")
            for m in missing_vers[:20]:
                lines.append(f"  {m['provider']}/{m['source_id']}  {m['name']}")
            if len(missing_vers) > 20:
                lines.append(f"  ... and {len(missing_vers) - 20} more")
            lines.append("")
        missing_cls = data.get("missing_classifications", [])
        if missing_cls:
            lines.append(f"Missing classification slugs ({len(missing_cls)}):")
            for short in missing_cls[:20]:
                lines.append(f"  {short}")
            if len(missing_cls) > 20:
                lines.append(f"  ... and {len(missing_cls) - 20} more")
            lines.append("")
        stale_regs = data.get("stale_registers", [])
        if stale_regs:
            lines.append(f"Stale register slugs ({len(stale_regs)}):")
            for m in stale_regs[:20]:
                lines.append(f"  {m['provider']}/{m['source_id']}")
            if len(stale_regs) > 20:
                lines.append(f"  ... and {len(stale_regs) - 20} more")
            lines.append("")
        stale_vars = data.get("stale_variants", [])
        if stale_vars:
            lines.append(f"Stale variant slugs ({len(stale_vars)}):")
            for m in stale_vars[:20]:
                lines.append(f"  {m['provider']}/{m['source_id']}")
            if len(stale_vars) > 20:
                lines.append(f"  ... and {len(stale_vars) - 20} more")
            lines.append("")
        stale_vers = data.get("stale_versions", [])
        if stale_vers:
            lines.append(f"Stale version slugs ({len(stale_vers)}):")
            for m in stale_vers[:20]:
                lines.append(f"  {m['provider']}/{m['source_id']}")
            if len(stale_vers) > 20:
                lines.append(f"  ... and {len(stale_vers) - 20} more")
            lines.append("")
        stale_cls = data.get("stale_classifications", [])
        if stale_cls:
            lines.append(f"Stale classification slugs ({len(stale_cls)}):")
            for short in stale_cls[:20]:
                lines.append(f"  {short}")
            if len(stale_cls) > 20:
                lines.append(f"  ... and {len(stale_cls) - 20} more")
            lines.append("")
        if stale_regs or stale_vars or stale_vers or stale_cls:
            lines.append("Drop these entries or mark them `deprecated = true`.")
            lines.append("")
        colliding_vers = data.get("colliding_versions", [])
        if colliding_vers:
            # Group by (provider, parent variant, would_be_slug). The
            # UNIQUE constraint scopes per (regvar_id, slug), so two rows
            # collide only if they share a parent variant — parent variant
            # is the leading "<reg>.<var>" of the dotted source_id. Grouping
            # by provider+slug alone would merge unrelated collisions under
            # different variants into one misleading bullet.
            groups: dict[tuple[str, str, str], list[dict[str, str]]] = {}
            for m in colliding_vers:
                parent_variant = m["source_id"].rsplit(".", 1)[0]
                groups.setdefault(
                    (m["provider"], parent_variant, m["would_be_slug"]), []
                ).append(m)
            lines.append(
                f"Periodized version slug collisions "
                f"({len(colliding_vers)} rows in {len(groups)} groups):"
            )
            for (provider, parent, slug), rows in list(groups.items())[:20]:
                lines.append(f"  {provider}/{parent} → slug {slug!r}:")
                for m in rows:
                    lines.append(f"    {m['source_id']}  {m['name']}")
            if len(groups) > 20:
                lines.append(f"  ... and {len(groups) - 20} more groups")
            lines.append(
                'Add a curated `[register_version."<RegisterId>.<RegVarID>.<RegVerID>"]` '
                "entry on one or more siblings to disambiguate."
            )
            lines.append("")
        snap = data.get("snapshot") or {}
        if snap.get("updated"):
            lines.append(f"Snapshot rewritten: {snap.get('path')}")
        elif snap.get("update_skipped_reason") == "parse_errors":
            lines.append(
                f"Snapshot NOT rewritten ({snap.get('path')}): fix the TOML "
                "parse errors above first."
            )
        elif snap.get("update_skipped_reason") == "non_additive_change":
            lines.append(
                f"Snapshot NOT rewritten ({snap.get('path')}): §5.4 grow-only "
                "violations present (slugs can only be added, never removed "
                "or renamed). Restore the entry, or mark the old row "
                "deprecated=true and add a replaced_by link."
            )
            for r in snap.get("removed") or []:
                lines.append(f"  removed: {r}")
            for r in snap.get("renamed") or []:
                lines.append(f"  renamed: {r}")
        else:
            removed = snap.get("removed") or []
            renamed = snap.get("renamed") or []
            added = snap.get("added") or []
            if removed or renamed:
                lines.append("Snapshot violations (§5.4 grow-only):")
                for r in removed:
                    lines.append(f"  removed: {r}")
                for r in renamed:
                    lines.append(f"  renamed: {r}")
                lines.append(
                    "  remediation: restore the entry, or mark the old row "
                    "deprecated and add a replaced_by link."
                )
                lines.append("")
            if added:
                lines.append(f"New entries since last snapshot ({len(added)}):")
                for a in added[:10]:
                    lines.append(f"  {a}")
                if len(added) > 10:
                    lines.append(f"  ... and {len(added) - 10} more")
                lines.append(
                    "  run `regmeta maintain precheck-slugs --update-snapshot` "
                    "after review."
                )
        if not lines:
            lines.append("OK — all live source IDs have curated slugs.")
        write_to("\n".join(lines) + "\n", output_path)
    elif key == ("maintain", "build-docs"):
        write_to(f"Built doc index: {data.get('db_path')}\n", output_path)
    elif key == ("maintain", "parse-sos"):
        count = data.get("register_count", 0)
        write_to(
            f"Parsed {count} register(s). Use --format json for full detail.\n",
            output_path,
        )
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
        if getattr(args, "field", "all") == "all":
            hint_add(hints, "Searching all fields (--field to narrow)")
        total = data.get("total_count", 0)
        results = data.get("results", [])
        if total > len(results):
            hint_add(
                hints,
                f"Showing {len(results)} of {total} matches (--limit/--offset to page)",
            )
        doc_hint = data.pop("doc_hint", None)
        if doc_hint:
            hint_add(hints, doc_hint)
        if total == 0 and not results:
            hint_add(hints, "No results (try broader --field or regmeta docs search)")

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
                f"Docs available (run: regmeta docs get {getattr(args, 'variable', '')})",
            )

    elif key == ("get", "values"):
        if getattr(args, "_projection_emptied", False):
            hint_add(
                hints,
                "cvid had value codes in Vardemangder.csv but every code was "
                "excluded by year-projection — likely an SCB validity gap for "
                "this cvid's regver year. Compare with neighbouring years via "
                "`regmeta get values <variable>` to see when codes appear.",
            )
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
                    "No instance carries a value set — variable may be "
                    "numeric/text, or year-projection emptied every cvid.",
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
    ("get", "classification"): _cmd_get_classification,
    ("resolve", None): _cmd_resolve,
    ("maintain", "build-docs"): _cmd_maintain_build_docs,
    ("maintain", "parse-sos"): _cmd_maintain_parse_sos,
    ("maintain", "seed-slugs"): _cmd_maintain_seed_slugs,
    ("maintain", "precheck-slugs"): _cmd_maintain_precheck_slugs,
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
    (
        "get values TARGET [--register R] [--year Y]",
        "Value codes by CVID, or year × codes view by variable.",
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
    ("maintain update [--tag TAG] [--force] [--yes]", "Update package and database."),
    ("maintain info", "Database stats and import metadata."),
    (
        "maintain build-db --input-dir DIR",
        "Build database from SCB CSV exports (maintainer-only).",
    ),
    (
        "maintain build-docs [--docs-dir DIR]",
        "Rebuild the doc DB from markdown (maintainer-only).",
    ),
    (
        "maintain parse-sos PATH",
        "Parse Socialstyrelsen metadata Excel files; emit JSON (maintainer-only).",
    ),
    (
        "maintain seed-slugs [--out-dir DIR] [--force] [--all-hints]",
        "Emit starter slug TOMLs from the current DB (maintainer-only).",
    ),
    (
        "maintain precheck-slugs [--slug-dir DIR] [--update-snapshot]",
        "Validate slug TOMLs and list source IDs missing a slug entry.",
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

  "How did ArbSokNov's codes evolve across LISA years?"
    regmeta get values "ArbSokNov" --register LISA
        → year × codes table (one row per cvid, codes inline)

  "What codes were valid for ArbSokNov in 2015 specifically?"
    regmeta get values "ArbSokNov" --register LISA --year 2015

  "What are the valid values for CVID 1001?"
    regmeta get values 1001

  Numeric input is treated as a CVID; anything else as a variable name
  (or column alias). Codes are year-projected through SCB validity
  windows at build time, so each cvid carries the year-correct set.

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
    ("get", "classification"): """\
get classification — Normalized code systems
────────────────────────────────────────────

  "What classifications exist?"
    regmeta get classification --list

  "Show metadata for SUN2000"
    regmeta get classification SUN2000

  "List every code in SUN2000"
    regmeta get classification SUN2000 --codes

  "Top-level SSYK codes only"
    regmeta get classification SSYK2012 --codes --level 1

  "Which variables use SUN2020?"
    regmeta get classification SUN2020 --variables
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
    regmeta maintain build-db --input-dir regmeta/input_data/

  "Bootstrap the DB without populated slug TOMLs (pre-v1 only)"
    regmeta maintain build-db --input-dir regmeta/input_data/ --skip-slugs

  Most users should use `maintain update` to download a pre-built
  database instead.
""",
    ("maintain", "parse-sos"): """\
maintain parse-sos — Parse Socialstyrelsen metadata Excel files (maintainer-only)

Examples:
    regmeta maintain parse-sos regmeta/input_data/Socialstyrelsen/
    regmeta maintain parse-sos path/to/PAR.xlsx --format json
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
    regmeta get values "Kommun" --register LISA              → year × codes
    regmeta get values "Kommun" --register LISA --year 2022  → one year's codes

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
    ("get", "classification"),
    ("docs", "search"),
    ("docs", "get"),
    ("docs", "list"),
    ("maintain", "update"),
    ("maintain", "info"),
    ("maintain", "build-db"),
    ("maintain", "build-docs"),
    ("maintain", "parse-sos"),
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
    reordered = reorder_global_flags(effective)

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
                        "  —  run `regmeta maintain update`\n"
                    )
            except Exception:
                pass
        return exit_code
    except RegmetaError as exc:
        write_json({"error": exc.to_dict()}, getattr(args, "output", None))
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
            write_json(error_payload, getattr(args, "output", None))
        except Exception:
            sys.stderr.write(json.dumps(error_payload) + "\n")
        return EXIT_INTERNAL


def main() -> int:
    return run()
