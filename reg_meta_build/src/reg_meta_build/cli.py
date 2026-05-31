"""reg-meta-build CLI entrypoint.

Build pipeline for the reg_meta SQLite databases (main + docs) plus the
slug TOML maintenance subcommands. The query CLI lives in `reg_meta`;
this binary is the maintainer-side tool that produces the artifacts
those queries read from.
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import sys
import time
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any

from reg_meta.cli_common import (
    NoRepeatParser,
    apply_leaf_help,
    handle_cli_exception,
    reorder_global_flags,
    success_envelope,
    write_json,
)
from reg_meta.db import (
    SCHEMA_VERSION,
    db_path_from_args,
    default_db_dir,
    open_db,
)
from reg_meta.errors import (
    EXIT_CONFIG,
    EXIT_NOT_FOUND,
    EXIT_USAGE,
    RegMetaError,
)

from .db import build_db
from .doc_db import build_doc_db, repo_docs_dir
from .fqid_slugs import (
    SNAPSHOT_FILENAME,
    diff_snapshot,
    format_default_slug_hints,
    is_unfrozen,
    iter_default_slug_candidates,
    precheck_slugs,
    read_snapshot,
    repo_slug_dir,
    seed_all,
    snapshot_payload,
    write_snapshot,
)
from .sources.sos import SosParseError, parse_directory, parse_register_file
from .validate import validate_built_db

if TYPE_CHECKING:
    from collections.abc import Callable

# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = NoRepeatParser(
        prog="reg-meta-build",
        description="Build pipeline for the reg_meta SQLite databases.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )
    parser.add_argument(
        "--db",
        default=None,
        help=f"Database directory (default: {default_db_dir()}).",
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

    build_p = sub.add_parser(
        "build-db",
        help="Build the metadata DB from SCB CSV exports (maintainer-only).",
        description=(
            "Build the metadata database from raw SCB CSV exports. This\n"
            "replaces the database entirely (not incremental). End users\n"
            "should use `reg-meta update` to fetch the pre-built DB instead.\n\n"
            "The input directory must contain:\n"
            "  <input-dir>/SCB/*.csv             — SCB metadata exports\n"
            "  <input-dir>/classifications/*.csv — canonical classification CSVs (optional)\n\n"
            "Examples:\n"
            "  reg-meta-build build-db --input-dir reg_meta_build/input_data/\n"
            "  reg-meta-build build-db --input-dir reg_meta_build/input_data/ --skip-slugs"
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
            "Directory of curated slug TOMLs (default: reg_meta_build/fqid_slugs/ "
            "when run from a repo checkout)."
        ),
    )
    build_p.add_argument(
        "--skip-slugs",
        action="store_true",
        help=(
            "Skip slug TOML loading and the strict-coverage check. Used to "
            "bootstrap the DB so `seed-slugs` has something to read from "
            "before the slug TOMLs exist (REFACTOR_SPEC §5.4 Activation). "
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
            "DB. Fails with EXIT_CONFIG on any violation."
        ),
    )

    build_docs_p = sub.add_parser(
        "build-docs",
        help="Rebuild the doc DB from markdown files (maintainer-only).",
        description=(
            "Rebuild the documentation FTS index from markdown files.\n"
            "End users receive the doc DB via `reg-meta update`; this command\n"
            "is for maintainers rebuilding from a repo checkout before upload.\n\n"
            "Examples:\n"
            "  reg-meta-build build-docs\n"
            "  reg-meta-build build-docs --docs-dir /path/to/docs/"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    build_docs_p.add_argument(
        "--docs-dir",
        default=None,
        help=(
            "Directory containing register doc subdirectories "
            "(default: reg_meta_build/docs/ if run from a repo checkout)."
        ),
    )

    seed_slugs_p = sub.add_parser(
        "seed-slugs",
        help="Emit starter slug TOMLs from the current DB (maintainer-only).",
        description=(
            "Generate hand-review starter TOMLs at <out-dir>/<provider>.toml\n"
            "and <out-dir>/classifications.toml, mirroring REFACTOR_SPEC §5.3.\n"
            "Slugs are auto-derived from register.name / register_variant.name / short_name\n"
            "and need maintainer review before commit.\n\n"
            "Examples:\n"
            "  reg-meta-build seed-slugs\n"
            "  reg-meta-build seed-slugs --out-dir /tmp/slugs/"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    seed_slugs_p.add_argument(
        "--out-dir",
        default=None,
        help=(
            "Where to write the TOMLs (default: reg_meta_build/fqid_slugs/ in a repo "
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

    precheck_p = sub.add_parser(
        "precheck-slugs",
        help="Validate slug TOMLs and list source IDs missing a slug entry.",
        description=(
            "Verify the slug TOMLs match the current DB. Reports:\n"
            "  - TOML parse / validation errors\n"
            "  - register / register_variant / classification rows with no slug\n"
            "  - non-additive changes vs. the committed snapshot (§5.4)\n\n"
            "Exits 10 if any check fails (cleaner failure mode than a build).\n\n"
            "Examples:\n"
            "  reg-meta-build precheck-slugs\n"
            "  reg-meta-build precheck-slugs --update-snapshot"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    precheck_p.add_argument(
        "--slug-dir",
        default=None,
        help="Directory of slug TOMLs (default: reg_meta_build/fqid_slugs/).",
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

    parse_sos_p = sub.add_parser(
        "parse-sos",
        help="Parse Socialstyrelsen metadata Excel deliveries (maintainer-only).",
        description=(
            "Parse one Socialstyrelsen register .xlsx (or a directory of them)\n"
            "into structured JSON. Useful for inspecting upstream deliveries\n"
            "before build-db. Does not modify the database.\n\n"
            "Examples:\n"
            "  reg-meta-build parse-sos input_data/Socialstyrelsen/\n"
            "  reg-meta-build parse-sos input_data/Socialstyrelsen/PAR.xlsx"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parse_sos_p.add_argument(
        "path",
        help="Path to an .xlsx file or a directory containing them.",
    )

    # `reg-meta-build` has no `--examples` handler (the query CLI's `--examples`
    # interceptor lives in `reg_meta.cli.run`); suppress the epilog so each
    # subcommand's --help doesn't point at an unrecognized flag.
    apply_leaf_help(parser, examples_epilog=False)
    return parser


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------


def _build_validate_hook() -> Callable[[Path], None]:
    """Return a build_db pre_rename_hook that runs the value-set dedup
    validator against the staging DB and raises on failure. Defined as a
    helper so the closure stays narrowly scoped."""

    def hook(staging_db: Path) -> None:
        validation = validate_built_db(staging_db)
        sys.stderr.write(validation.format_report() + "\n")
        sys.stderr.flush()
        if validation.failures:
            raise RegMetaError(
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
                    "`reg-meta-build build-db --validate`."
                ),
            )

    return hook


def _cmd_build_db(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
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
        command="build-db",
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


def _cmd_build_docs(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    if args.docs_dir:
        docs_dir = Path(args.docs_dir).resolve()
    else:
        docs_dir = repo_docs_dir()
        if docs_dir is None:
            raise RegMetaError(
                exit_code=EXIT_CONFIG,
                code="no_docs_dir",
                error_class="configuration",
                message=(
                    "No --docs-dir specified and no in-repo docs found. "
                    "This command is for maintainers rebuilding the doc DB from a repo checkout."
                ),
                remediation=(
                    "Run from a reg_meta_build checkout with `reg_meta_build/docs/` present, "
                    "or pass --docs-dir pointing to a directory with register doc subdirectories."
                ),
            )
    db_dir = Path(args.db).resolve() if args.db else default_db_dir().resolve()
    db_path = build_doc_db(docs_dir, db_dir)
    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
        command="build-docs",
        args_payload={"docs_dir": str(docs_dir)},
        db_info=None,
        data={"db_path": str(db_path), "docs_dir": str(docs_dir)},
        duration_ms=duration_ms,
    ), 0


def _resolve_slug_dir(slug_arg: str | None) -> Path:
    if slug_arg is not None:
        return Path(slug_arg).expanduser().resolve()
    resolved = repo_slug_dir()
    if resolved is None:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="slug_dir_not_found",
            error_class="configuration",
            message=(
                "Slug TOMLs not found. Pass --slug-dir or run from a reg_meta "
                "checkout containing reg_meta_build/fqid_slugs/."
            ),
            remediation=(
                "Run from a repo checkout, or `reg-meta-build seed-slugs` "
                "to bootstrap a new slug directory."
            ),
        )
    return resolved


def _cmd_seed_slugs(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db = db_path_from_args(args.db)
    if args.out_dir:
        out_dir = Path(args.out_dir).expanduser().resolve()
    else:
        out_dir = repo_slug_dir() or (Path.cwd() / "fqid_slugs").resolve()
    if out_dir.exists() and any(out_dir.glob("*.toml")) and not args.force:
        raise RegMetaError(
            exit_code=EXIT_CONFIG,
            code="slug_seed_would_overwrite",
            error_class="configuration",
            message=f"{out_dir} already contains TOMLs; refusing to overwrite.",
            remediation=(
                "Pass --force to overwrite, or point --out-dir at an empty "
                "directory for hand-review."
            ),
        )
    # Schema-compat (open_db) rejects a stale DB up front, so seed reads run
    # against the current shape and give the user the right remediation rather
    # than a raw `OperationalError`.
    conn = open_db(db)
    try:
        written = seed_all(conn, out_dir)
        # reg-meta-build always emits JSON on stdout, so hints (stderr) are
        # independent of format and only suppressed by --quiet / env.
        suppress_hints = args.quiet or os.environ.get("REG_META_QUIET") == "1"
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
        command="seed-slugs",
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


def _cmd_precheck_slugs(
    args: argparse.Namespace,
) -> tuple[dict[str, Any], int]:
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
        command="precheck-slugs",
        args_payload={
            "slug_dir": str(slug_dir),
            "update_snapshot": args.update_snapshot,
        },
        db_info=None,
        data={
            "slug_dir": str(slug_dir),
            "missing_registers": [
                # `name` mirrors the renamed `register.name` column (was
                # SCB `registernamn`); the JSON envelope uses the universal
                # English key like the other entity arrays.
                {"provider": p, "source_id": sid, "name": name}
                for (p, sid, name) in result.missing_registers
            ],
            "missing_variants": [
                {"provider": p, "source_id": sid, "name": name}
                for (p, sid, name) in result.missing_variants
            ],
            # A2.6: register_version left the FQID grammar — no version slug
            # missing/stale/collision arrays in the precheck payload.
            "missing_classifications": list(result.missing_classifications),
            "parse_errors": list(result.parse_errors),
            "stale_registers": [
                {"provider": p, "source_id": sid} for (p, sid) in result.stale_registers
            ],
            "stale_variants": [
                {"provider": p, "source_id": sid} for (p, sid) in result.stale_variants
            ],
            "stale_classifications": list(result.stale_classifications),
            # Advisory only (§5.3/#143) — never affects `ok`/exit. Variables
            # whose delivery column drifts across editions, auto-slugged from a
            # stable basis; a curator scans this for the pre-v1 slug freeze.
            "drifting_variables": [
                {
                    "provider": prov,
                    "register_id": reg_id,
                    "provider_key": provider_key,
                    "slug": slug,
                    "name": name,
                    "columns": list(columns),
                }
                for (prov, reg_id, provider_key, slug, name, columns) in (
                    result.drifting_variables
                )
            ],
            "snapshot": snapshot_status,
        },
        duration_ms=duration_ms,
    ), exit_code


def _cmd_parse_sos(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    path = Path(args.path).expanduser().resolve()

    try:
        if path.is_dir():
            results = parse_directory(path)
        elif path.is_file():
            results = [parse_register_file(path)]
        else:
            raise RegMetaError(
                exit_code=EXIT_NOT_FOUND,
                code="path_not_found",
                error_class="input",
                message=f"{path} is neither a file nor a directory",
                remediation="Pass a .xlsx file or a directory containing them.",
            )
    except SosParseError as exc:
        raise RegMetaError(
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
        command="parse-sos",
        args_payload={"path": str(path)},
        db_info=None,
        data=data,
        duration_ms=duration_ms,
    ), 0


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------


COMMAND_DISPATCH: dict[
    str, Callable[[argparse.Namespace], tuple[dict[str, Any], int]]
] = {
    "build-db": _cmd_build_db,
    "build-docs": _cmd_build_docs,
    "seed-slugs": _cmd_seed_slugs,
    "precheck-slugs": _cmd_precheck_slugs,
    "parse-sos": _cmd_parse_sos,
}


# ---------------------------------------------------------------------------
# Usage / version display
# ---------------------------------------------------------------------------


_COMMAND_OVERVIEW: list[tuple[str, str]] = [
    (
        "build-db --input-dir DIR",
        "Build the metadata DB from SCB CSV exports.",
    ),
    (
        "build-docs [--docs-dir DIR]",
        "Rebuild the doc DB from markdown files.",
    ),
    (
        "seed-slugs [--out-dir DIR] [--force] [--all-hints]",
        "Emit starter slug TOMLs from the current DB.",
    ),
    (
        "precheck-slugs [--slug-dir DIR] [--update-snapshot]",
        "Validate slug TOMLs and list source IDs missing a slug entry.",
    ),
    (
        "parse-sos PATH",
        "Parse Socialstyrelsen metadata Excel files; emit JSON.",
    ),
]


def _version_line() -> str:
    from . import __version__ as build_version

    return f"reg-meta-build v{build_version}"


def _print_usage() -> None:
    w = sys.stderr.write
    w(f"{_version_line()}\n\n")
    w("Build pipeline for the reg_meta SQLite databases (maintainer-only).\n\n")
    w("Commands:\n")
    col_w = max(len(syntax) for syntax, _ in _COMMAND_OVERVIEW) + 2
    for syntax, desc in _COMMAND_OVERVIEW:
        w(f"  {syntax:<{col_w}} {desc}\n")
    w("\nRun `reg-meta-build <command> --help` for detailed help.\n")


def _print_help() -> None:
    w = sys.stderr.write
    w(f"{_version_line()}\n\n")
    w("Global flags (place before subcommand):\n")
    w("  --db DIR                     Database directory\n")
    w("  --output FILE                Write output to file\n")
    w("  -v, --verbose                Include envelope metadata\n")
    w("  -q, --quiet                  Suppress hints on stderr\n\n")
    w("Commands:\n")
    for syntax, desc in _COMMAND_OVERVIEW:
        w(f"  {syntax}\n      {desc}\n")
    w("\nRun `reg-meta-build <command> --help` for detailed help.\n")


def run(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    effective = argv if argv is not None else sys.argv[1:]
    reordered = reorder_global_flags(effective)

    try:
        args = parser.parse_args(reordered)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else EXIT_USAGE

    if getattr(args, "version", False):
        sys.stderr.write(f"{_version_line()}\n")
        return 0
    if getattr(args, "help", False):
        _print_help()
        return 0
    if not args.command:
        _print_usage()
        return EXIT_USAGE

    handler = COMMAND_DISPATCH.get(args.command)
    if not handler:
        sys.stderr.write(f"Unknown command: {args.command}\n")
        return EXIT_USAGE

    output_path = getattr(args, "output", None)
    verbose = getattr(args, "verbose", False)

    try:
        payload, exit_code = handler(args)
        if verbose:
            write_json(payload, output_path)
        else:
            write_json(payload.get("data", payload), output_path)
        return exit_code
    except Exception as exc:
        return handle_cli_exception(exc, output_path)


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
