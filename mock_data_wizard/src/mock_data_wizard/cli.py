"""CLI entrypoint for mock-data-wizard."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


DESCRIPTION = """\
Generate mock CSV data from MONA project metadata, without exporting any
personal data. The workflow has two steps:

  Step 1: Build the MONA extract bundle and run it on MONA.
          The bundle reads your project's data files and exports only
          aggregate statistics (counts, means, frequencies — no individual
          records) to a stats.json file.

    mock-data-wizard build-bundle
    # Upload mock_data_wizard_extract.py to MONA, edit its configure()
    # block, and run `python mock_data_wizard_extract.py` in the Batch
    # client.
    # Download the resulting stats.json to your local machine.
    # IMPORTANT: verify that stats.json does not contain any personal
    # data. The bundle censors cells with 5 or fewer individuals, but
    # you should verify yourself that no personal data is leaking.

  Step 2: Generate mock CSV files from the stats.

    mock-data-wizard generate

  The mock files are written to mock_data/ in the current directory.
  They have the same column names, types, and distributions as the
  real data, but contain only synthetic values.
"""

BUILD_BUNDLE_HELP = """\
Build the single-file Python bundle that runs the extract step on MONA.

  1. Run this command locally to create mock_data_wizard_extract.py.
  2. Upload the bundle to your MONA project directory.
  3. Edit the configure() block at the top to declare your data sources.
  4. Run on MONA in the Batch client:  python mock_data_wizard_extract.py
  5. Download the resulting stats.json to your local machine.

The bundle only exports aggregate statistics — no individual-level data
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
Generate mock CSV files from a stats.json produced by the MONA extract bundle.

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
        if sv != "3":
            print(
                f"Error: unsupported manifest schema_version '{sv}'. Expected '3'.\n"
                "Regenerate with mock-data-wizard >= v0.3.0.",
                file=sys.stderr,
            )
            return 1
        for f in manifest_data.get("files", []):
            label = f["source_name"]
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
            print(
                "Error: --register is required when using --columns.", file=sys.stderr
            )
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

    db = db_path_from_args(args.db)
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
    import shutil

    from regmeta.cli import format_rows

    term_w = shutil.get_terminal_size().columns

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

        rows = []
        for m in f.get("matched", []):
            rows.append(
                {
                    "column": m["column"],
                    "status": "matched",
                    "var_id": str(m.get("var_id", "")),
                    "variable_name": m.get("variable_name", ""),
                }
            )
        for col in f.get("extra_local", []):
            rows.append(
                {
                    "column": col,
                    "status": "extra_local",
                    "var_id": "",
                    "variable_name": "",
                }
            )
        if rows:
            cols = ["column", "status", "var_id", "variable_name"]
            print(format_rows(rows, cols, max_width=term_w), end="")

        missing = f.get("missing_from_registry", [])
        if missing:
            print("\n  Missing from local:")
            miss_rows = [
                {
                    "var_id": str(m["var_id"]),
                    "variable_name": m["variable_name"],
                    "aliases": ", ".join(m.get("aliases", [])),
                }
                for m in missing
            ]
            print(
                format_rows(
                    miss_rows, ["var_id", "variable_name", "aliases"], max_width=term_w
                ),
                end="",
            )
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
    n_sources = len(stats.sources)
    sample_label = f" at {args.sample_pct:.0%}" if args.sample_pct < 1.0 else ""

    # Check for existing output directory with files
    existing_files = (
        sorted(p.name for p in output_dir.iterdir() if p.is_file())
        if output_dir.is_dir()
        else []
    )
    if existing_files and not args.yes and not args.force:
        print(
            f"WARNING: {output_dir}/ already contains {len(existing_files)} "
            f"file(s) from a previous run.\n"
            f"Continuing will overwrite matching files; stale files (not "
            f"produced by this run) will be left in place with a warning.\n"
            f"Pass --force to delete stale files instead.\n"
            f"Press Y to continue or any other key to abort.",
            flush=True,
        )
        if not _confirm():
            print("Aborted.", file=sys.stderr)
            return 1

    if not (args.yes or existing_files):
        print(
            f"Will generate {n_sources} mock CSV files{sample_label} "
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
            force=args.force,
        )
    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        return 1

    print(f"Generated {len(manifest.files)} file(s) in {manifest.output_dir}")
    for f in manifest.files:
        print(f"  {f.source_name}: {f.row_count} rows (sha256: {f.sha256[:12]}...)")
    return 0


def _cmd_build_bundle(args: argparse.Namespace) -> int:
    """Amalgamate the runtime modules into a single .py for MONA upload."""
    from . import _bundle

    output = Path(args.output) if args.output else Path(_bundle.DEFAULT_OUTPUT_NAME)
    out = _bundle.build_bundle(output)
    print(f"Built {out} ({out.stat().st_size:,} bytes)")
    return 0


def _cmd_update(_args: argparse.Namespace) -> int:
    from .update import run_update

    return run_update()


def _print_version() -> None:
    from . import __version__
    from .update import UpdateChecker

    sys.stderr.write(f"mock-data-wizard v{__version__}\n")
    sys.stderr.write("Checking for updates...\n")
    try:
        checker = UpdateChecker(http_timeout=10)
        newer = checker.get_newer_version(timeout=10)
        if newer:
            sys.stderr.write(
                f"Update available: v{__version__} → v{newer}"
                "  —  run `mock-data-wizard update`\n"
            )
        elif checker.completed:
            sys.stderr.write("Up to date.\n")
        else:
            sys.stderr.write("Could not check for updates.\n")
    except Exception:
        sys.stderr.write("Could not check for updates.\n")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mock-data-wizard",
        description=DESCRIPTION,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--version",
        action="store_true",
        default=False,
        help="Show version and check for updates.",
    )
    sub = parser.add_subparsers(dest="command")

    # build-bundle
    bb = sub.add_parser(
        "build-bundle",
        help="Step 1: Build the MONA extract bundle to upload",
        description=BUILD_BUNDLE_HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    bb.add_argument(
        "--output",
        "-o",
        help="Output path (default: mock_data_wizard_extract.py in cwd)",
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
        help="Skip confirmation prompts. Stale files are still kept-and-warned unless --force.",
    )
    gen.add_argument(
        "--force",
        action="store_true",
        help="Delete stale files (those in the output directory that aren't produced by this run). "
        "Default is warn-and-keep: safer when SOURCES shrinks between runs.",
    )
    gen.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show per-file timing breakdown",
    )

    sub.add_parser(
        "update",
        help="Update mock-data-wizard to the latest version on PyPI",
        description=(
            "Check PyPI for a newer version of mock-data-wizard and run "
            "`uv tool upgrade mock-data-wizard` if one is available."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.version:
        _print_version()
        return 0

    if args.command is None:
        parser.print_help()
        return 0

    if args.command == "update":
        return _cmd_update(args)

    # Kick off the PyPI update check in the background so it runs in parallel
    # with the user's actual work. Skipped for `update` (just ran the check)
    # and in non-interactive contexts where a trailing notice would clutter
    # piped output.
    update_checker = None
    if sys.stderr.isatty():
        try:
            from .update import UpdateChecker

            update_checker = UpdateChecker()
        except Exception:
            pass

    try:
        if args.command == "build-bundle":
            rc = _cmd_build_bundle(args)
        elif args.command == "compare":
            rc = _cmd_compare(args)
        elif args.command == "generate":
            rc = _cmd_generate(args)
        else:
            parser.print_help()
            return 1
    finally:
        if update_checker is not None:
            try:
                newer = update_checker.get_newer_version()
                if newer:
                    from . import __version__

                    sys.stderr.write(
                        f"\n  Update available: v{__version__} → v{newer}"
                        "  —  run `mock-data-wizard update`\n"
                    )
            except Exception:
                pass

    return rc


if __name__ == "__main__":
    sys.exit(main())
