"""CLI entrypoint for mock-data-wizard."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


PROJECT_PATH_TEMPLATE = "\\\\micro.intra\\projekt\\P{num}$\\P{num}_Data"

DESCRIPTION = """\
Generate mock CSV data from MONA project metadata, without exporting any
personal data. This is a two-step process:

  Step 1: Generate an R script and run it on MONA.
          The script reads your project's data files and exports only
          aggregate statistics (counts, means, frequencies — no individual
          records) to a stats.json file.

    mock-data-wizard generate-script -p P1405
    # Upload the R script to MONA and run it in the Batch client.
    # Download the resulting stats.json to your local machine.
    # IMPORTANT: verify that stats.json does not contain any personal
    # data. The script censors cells with 5 or fewer individuals, but
    # you should verify yourself that no personal data is leaking.

  Step 2: Generate mock CSV files from the stats.

    mock-data-wizard generate

  The mock files are written to mock_data/ in the current directory.
  They have the same column names, types, and distributions as the
  real data, but contain only synthetic values.
"""

GENERATE_SCRIPT_HELP = """\
Generate an R script that extracts aggregate statistics from your MONA
project data. The script is designed to be run on MONA with Rscript.

  1. Run this command locally to create the R script.
  2. Upload the script to your MONA project directory.
  3. Run it on MONA in the Batch client:  Rscript extract_stats_P1405.R
  4. Download the resulting stats.json to your local machine.

The R script only exports aggregate statistics — no individual-level data
leaves MONA.
"""

COMPARE_HELP = """\
Compare columns in local data files against SCB registry metadata.

Input modes (mutually exclusive):
  mock-data-wizard compare manifest.json                              # wizard manifest v2
  mock-data-wizard compare --files mock_data/*.csv --register LISA    # read CSV headers
  mock-data-wizard compare --columns "Kon,FodelseAr" --register 189  # explicit

CSV and --columns modes require --register.
"""

GENERATE_HELP = """\
Generate mock CSV files from a stats.json produced by the R script.

By default, uses the regmeta database to enrich categorical columns with
registry metadata (value codes, variable names). If the regmeta database
is not available, use --no-regmeta to skip enrichment.

Examples:
  mock-data-wizard generate
  mock-data-wizard generate --sample-pct 0.1 --seed 42
  mock-data-wizard generate --stats path/to/stats.json --no-regmeta
"""


def _confirm() -> bool:
    """Read a single keypress; return True if 'y' or 'Y'."""
    if not sys.stdin.isatty():
        return True
    try:
        import termios
        import tty

        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)
        print()
        return ch in ("y", "Y")
    except ImportError:
        # Windows — fall back to line input
        try:
            return input().strip().lower() == "y"
        except (KeyboardInterrupt, EOFError):
            return False


def _parse_project_number(value: str) -> str:
    """Extract numeric project ID from 'P1405', 'p1405', or '1405'."""
    stripped = value.strip().upper()
    if stripped.startswith("P"):
        stripped = stripped[1:]
    if not stripped.isdigit():
        raise ValueError(
            f"Invalid project number: {value!r} (expected e.g. '1405' or 'P1405')"
        )
    return stripped


def _cmd_generate_script(args: argparse.Namespace) -> int:
    from .script_gen import generate_script

    paths: list[str] = []
    project_num: str | None = None
    if args.project:
        project_num = _parse_project_number(args.project)
        paths.append(PROJECT_PATH_TEMPLATE.format(num=project_num))
    if args.project_dir:
        paths.extend(p.strip() for p in args.project_dir if p.strip())
    if not paths:
        print(
            "Error: provide --project or --project-dir\n"
            "  Example: mock-data-wizard generate-script -p P1405",
            file=sys.stderr,
        )
        return 1

    if args.output:
        output = Path(args.output)
    elif project_num:
        output = Path(f"extract_stats_P{project_num}.R")
    else:
        output = Path("extract_stats.R")
    result = generate_script(paths, output)
    print(f"R script written to: {result}")
    return 0


def _cmd_compare(args: argparse.Namespace) -> int:
    import csv as csv_mod
    import json

    from regmeta import compare, open_db, resolve_register_ids
    from regmeta.db import db_path_from_args

    from ._util import strip_project_prefix

    columns_by_file: dict[str, list[str]] = {}
    register_hints: dict[str, int | None] = {}
    year_hints: dict[str, int | None] = {}

    if args.manifest:
        manifest_path = Path(args.manifest)
        if not manifest_path.exists():
            print(f"Error: manifest file not found: {args.manifest}", file=sys.stderr)
            return 1
        manifest_data = json.loads(manifest_path.read_text(encoding="utf-8"))
        sv = manifest_data.get("schema_version")
        if sv != "2":
            print(
                f"Error: unsupported manifest schema_version '{sv}'. Expected '2'.\n"
                "Regenerate with mock-data-wizard >= v0.2.0.",
                file=sys.stderr,
            )
            return 1
        for f in manifest_data.get("files", []):
            label = f["file_name"]
            columns_by_file[label] = f.get("columns", [])
            register_hints[label] = f.get("register_hint")
            year_hints[label] = f.get("year_hint")

    elif args.files:
        if not args.register:
            print("Error: --register is required when using --files.", file=sys.stderr)
            return 1
        for fpath_str in args.files:
            fpath = Path(fpath_str)
            if not fpath.exists():
                print(f"Error: file not found: {fpath_str}", file=sys.stderr)
                return 1
            with fpath.open(encoding="utf-8") as fh:
                reader = csv_mod.reader(fh)
                headers = next(reader, [])
            columns_by_file[fpath.name] = headers

    elif args.columns:
        if not args.register:
            print("Error: --register is required when using --columns.", file=sys.stderr)
            return 1
        cols = [c.strip() for c in args.columns.split(",") if c.strip()]
        columns_by_file["(columns)"] = cols

    else:
        print(
            "Error: no input provided.\n"
            "Provide a manifest path, --files, or --columns.",
            file=sys.stderr,
        )
        return 1

    db = db_path_from_args(args.db if hasattr(args, "db") else None)
    conn = open_db(db)
    try:
        if args.register:
            reg_ids = resolve_register_ids(conn, args.register)
            if not reg_ids:
                print(f"Error: register '{args.register}' not found.", file=sys.stderr)
                return 1
            reg_id = reg_ids[0]
            for label in columns_by_file:
                if register_hints.get(label) is None:
                    register_hints[label] = reg_id

        # Strip MONA project prefixes (P1105_LopNr → LopNr) before matching
        stripped_by_file = {
            label: [strip_project_prefix(c) for c in cols]
            for label, cols in columns_by_file.items()
        }

        data = compare(
            conn,
            columns_by_file=stripped_by_file,
            register_hints=register_hints,
            year_hints=year_hints,
        )
    finally:
        conn.close()

    if args.format == "json":
        print(json.dumps(data, indent=2, ensure_ascii=False))
    else:
        _print_compare_table(data)
    return 0


def _print_compare_table(data: dict) -> None:
    for f in data.get("files", []):
        status = f.get("register_status", "")
        reg_name = f.get("register_name") or "?"
        reg_id = f.get("register_id") or "?"
        header = f"── {f['file']}  [{reg_name} ({reg_id})] {status}"
        if f.get("year_hint"):
            header += f"  year={f['year_hint']}"
        print(header)

        if status != "resolved":
            print(f"  (skipped: {status})\n")
            continue

        s = f.get("summary", {})
        print(
            f"  matched: {s.get('matched', 0)}  "
            f"extra_local: {s.get('extra_local', 0)}  "
            f"missing_from_registry: {s.get('missing_from_registry', 0)}"
        )

        for m in f.get("matched", []):
            print(f"    {m['column']:30s}  matched    var_id={m.get('var_id', '')}  {m.get('variable_name', '')}")
        for col in f.get("extra_local", []):
            print(f"    {col:30s}  extra_local")

        missing = f.get("missing_from_registry", [])
        if missing:
            print("\n  Missing from local:")
            for m in missing:
                aliases = ", ".join(m.get("aliases", []))
                print(f"    var_id={m['var_id']}  {m['variable_name']}  ({aliases})")
        print()


def _cmd_generate(args: argparse.Namespace) -> int:
    from .enrich import enrich
    from .generate import generate
    from .stats import StatsValidationError, parse_stats

    stats_path = Path(args.stats)
    if not stats_path.exists():
        print(f"Error: stats file not found: {stats_path}", file=sys.stderr)
        return 1

    try:
        stats = parse_stats(stats_path)
    except StatsValidationError as exc:
        print(f"Stats validation error: {exc}", file=sys.stderr)
        return 1

    output_dir = Path(args.output_dir) if args.output_dir else Path("mock_data")
    n_files = len(stats.files)
    sample_label = f" at {args.sample_pct:.0%}" if args.sample_pct < 1.0 else ""

    # Check for existing output directory with files
    existing_files = (
        sorted(p.name for p in output_dir.iterdir() if p.is_file())
        if output_dir.is_dir()
        else []
    )
    if existing_files:
        if args.yes and not args.force:
            print(
                f"Error: output directory {output_dir}/ already contains "
                f"{len(existing_files)} file(s).\n"
                f"Use --force to overwrite (stale files will be removed).",
                file=sys.stderr,
            )
            return 1
        if not args.force:
            print(
                f"WARNING: {output_dir}/ already contains {len(existing_files)} "
                f"file(s) from a previous run.\n"
                f"Continuing will overwrite matching files and remove stale ones.\n"
                f"Press Y to continue or any other key to abort.",
                flush=True,
            )
            if not _confirm():
                print("Aborted.", file=sys.stderr)
                return 1

    if not (args.yes or existing_files):
        print(
            f"Will generate {n_files} mock CSV files{sample_label} "
            f"from {stats_path} into {output_dir}/\n"
            f"This may take a while. Press Y to continue or any other key to abort.",
            flush=True,
        )
        if not _confirm():
            print("Aborted.", file=sys.stderr)
            return 1

    if args.no_regmeta:
        db_path = None
    elif args.db:
        db_path = Path(args.db)
    else:
        from regmeta.db import db_path_from_args

        db_path = db_path_from_args(None)

    try:
        enriched = enrich(stats, register=args.register, db_path=db_path)
    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        if not args.no_regmeta:
            print(
                "Hint: use --no-regmeta to generate without registry metadata.",
                file=sys.stderr,
            )
        return 1

    try:
        manifest = generate(
            stats,
            enriched,
            seed=args.seed,
            sample_pct=args.sample_pct,
            output_dir=output_dir,
            verbose=args.verbose,
        )
    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        return 1

    print(f"Generated {len(manifest.files)} file(s) in {manifest.output_dir}")
    for f in manifest.files:
        print(f"  {f.file_name}: {f.row_count} rows (sha256: {f.sha256[:12]}...)")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mock-data-wizard",
        description=DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command")

    # generate-script
    gs = sub.add_parser(
        "generate-script",
        help="Step 1: Generate an R script to run on MONA",
        description=GENERATE_SCRIPT_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    gs.add_argument(
        "--project",
        "-p",
        help="SCB project number (e.g. P1405 or 1405). Builds the standard MONA data path automatically.",
    )
    gs.add_argument(
        "--project-dir",
        nargs="+",
        help="Custom data path(s) to scan. Combinable with --project.",
    )
    gs.add_argument(
        "--output",
        "-o",
        help="Output path for the R script (default: extract_stats_P<num>.R)",
    )

    # compare
    cmp = sub.add_parser(
        "compare",
        help="Compare local file columns against registry metadata",
        description=COMPARE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    cmp_input = cmp.add_mutually_exclusive_group()
    cmp.add_argument(
        "manifest",
        nargs="?",
        default=None,
        help="Path to wizard manifest.json (schema_version 2).",
    )
    cmp_input.add_argument(
        "--files",
        nargs="+",
        default=None,
        help="CSV file paths to compare (reads first row as headers).",
    )
    cmp_input.add_argument(
        "--columns",
        default=None,
        help="Comma-separated column names to compare.",
    )
    cmp.add_argument(
        "--register",
        default=None,
        help="Register name or ID (required for --files and --columns modes).",
    )
    cmp.add_argument(
        "--db",
        default=None,
        help="Path to regmeta database directory.",
    )
    cmp.add_argument(
        "--format",
        choices=["table", "json"],
        default="table",
        help="Output format (default: table).",
    )

    # generate
    gen = sub.add_parser(
        "generate",
        help="Step 2: Generate mock CSV files from stats.json",
        description=GENERATE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    gen.add_argument(
        "--stats",
        default="stats.json",
        help="Path to stats.json (default: stats.json in current directory)",
    )
    gen.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible output (default: 42)",
    )
    gen.add_argument(
        "--sample-pct",
        type=float,
        default=1.0,
        help="Fraction of rows to generate, e.g. 0.1 for 10%% (default: 1.0)",
    )
    gen.add_argument(
        "--output-dir",
        help="Directory for generated CSV files (default: mock_data)",
    )
    gen.add_argument(
        "--db",
        help="Path to regmeta database directory (override $REGMETA_DB or $XDG_DATA_HOME).",
    )
    gen.add_argument(
        "--no-regmeta",
        action="store_true",
        help="Skip regmeta enrichment (by default, the regmeta DB is required)",
    )
    gen.add_argument(
        "--register",
        help="Filter regmeta matches to a specific register",
    )
    gen.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Skip confirmation prompt (does NOT override --force for existing output)",
    )
    gen.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing output directory (stale files are removed)",
    )
    gen.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show per-file timing breakdown",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        return 0

    if args.command == "generate-script":
        return _cmd_generate_script(args)
    if args.command == "compare":
        return _cmd_compare(args)
    if args.command == "generate":
        return _cmd_generate(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
