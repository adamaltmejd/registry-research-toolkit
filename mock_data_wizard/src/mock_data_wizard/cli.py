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
    sample_label = (
        f" at {args.sample_pct:.0%}" if args.sample_pct < 1.0 else ""
    )

    if not args.yes:
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
        help="Path to regmeta database directory (default: ~/.local/share/regmeta or $REGMETA_DB)",
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
        help="Skip confirmation prompt",
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
    if args.command == "generate":
        return _cmd_generate(args)

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
