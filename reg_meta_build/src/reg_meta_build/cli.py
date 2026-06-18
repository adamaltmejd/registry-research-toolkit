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

from .concept_group_candidates import (
    infer_concept_group_candidates,
    render_candidates_toml as render_concept_candidates_toml,
)
from .concept_groups import (
    load_concept_group_accepts,
    repo_concept_groups_path,
)
from .db import build_db
from .doc_db import build_doc_db, repo_docs_dir
from .extend_db import extend_db
from .fqid_slugs import (
    SNAPSHOT_FILENAME,
    diff_snapshot,
    format_default_slug_hints,
    frozen_zones,
    infer_entity_key_pins,
    iter_default_slug_candidates,
    load_freeze_states,
    precheck_slugs,
    read_snapshot,
    render_entity_key_pins_toml,
    repo_slug_dir,
    seed_all,
    snapshot_payload,
    write_entity_key_pins,
    write_snapshot,
)
from .sources.sos import SosParseError, parse_directory, parse_register_file
from .validate import validate_built_db
from .variable_same_as import (
    infer_same_as_candidates,
    render_candidates_toml,
)

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
            "before the slug TOMLs exist (see DESIGN.md → Slug immutability). "
            "Implies `--slug-dir` is ignored; the resulting DB has empty slug "
            "columns and is intended only as input to `seed-slugs`, not for "
            "downstream queries that depend on FQIDs."
        ),
    )
    build_p.add_argument(
        "--no-validate",
        action="store_true",
        help=(
            "Skip post-build invariant checks. By default build-db validates the "
            "freshly-built DB (schema shape, value-set dedup, year-projection "
            "anchors, FK integrity, minted-id bands, freelist ceiling, and — as a "
            "real maintainer build — the SOS corpus-volume gate) and fails with "
            "EXIT_CONFIG on any violation. Maintainer escape hatch only."
        ),
    )
    build_p.add_argument(
        "--providers",
        default="scb,sos,fohm,fk,lakemedelsverket,pliktverket,riksarkivet,umu",
        help=(
            "Comma-separated provider adapters to build (default: "
            "scb,sos,fohm,fk,lakemedelsverket,pliktverket,riksarkivet,umu). Pass "
            "`--providers scb` for the "
            "SCB-only DB that reproduces the byte-identical A4.3b dbdiff gate. "
            "Non-SCB providers are purely additive: they add rows in a disjoint id "
            "band (>= 2^62), never alter SCB's. `fohm`/`fk` (#422) and "
            "`lakemedelsverket`/`pliktverket`/`riksarkivet`/`umu` (#443) are thin "
            "curated providers whose committed TOMLs ship with the repo. (The `build_db()` function default stays "
            "`('scb',)` so synthetic SCB-only test fixtures need no extra inputs.)"
        ),
    )
    build_p.add_argument(
        "--timing",
        action="store_true",
        help=(
            "Emit per-stage `[timing] <stage>: <s>` lines to stderr (equivalent to "
            "REG_META_BUILD_TIMING=1). Off by default; a profiler-free way to see "
            "where build time goes."
        ),
    )

    extend_db_p = sub.add_parser(
        "extend-db",
        help="Overlay a steward inventory onto a released global DB (maintainer-only).",
        description=(
            "Build a steward-FLAVORED metadata DB (#365 PR2): an insert-only\n"
            "overlay of steward-ONLY content (the steward's own providers,\n"
            "registers, and variables) onto a RELEASED global reg_meta.db. The\n"
            "base DB is a read-only input — never mutated; the result is written\n"
            "to the --db output directory. Enrichment of existing global entities\n"
            "(descriptions, aliases, shared-column grafts) is global-build work.\n\n"
            "Examples:\n"
            "  reg-meta-build --db /tmp/swecov extend-db \\\n"
            "      --base-db ~/.reg_meta/reg_meta.db \\\n"
            "      --inventory input_data/swecov/inventory.json"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    extend_db_p.add_argument(
        "--base-db",
        required=True,
        help="Path to the released global reg_meta.db to overlay onto (read-only).",
    )
    extend_db_p.add_argument(
        "--inventory",
        required=True,
        help="Path to the steward inventory JSON (see extend_db.py for the contract).",
    )
    extend_db_p.add_argument(
        "--steward",
        default="swecov",
        help="Steward slug (default: swecov). Must match the inventory's `steward`.",
    )
    extend_db_p.add_argument(
        "--slug-dir",
        default=None,
        help=(
            "Directory of the steward's curated slug TOMLs "
            "(default: reg_meta_build/fqid_slugs/<steward>/ from a repo checkout)."
        ),
    )
    extend_db_p.add_argument(
        "--skip-slugs",
        action="store_true",
        help="Skip steward slug population (the overlaid rows keep NULL slugs).",
    )
    extend_db_p.add_argument(
        "--no-validate",
        action="store_true",
        help=(
            "Skip the post-overlay flavored validation. By default extend-db runs "
            "the full structural suite plus the tightened non-SCB minted-id band "
            "check and fails with EXIT_CONFIG on any violation."
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
            "and <out-dir>/classifications.toml, mirroring DESIGN.md → Slug curation.\n"
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
    seed_slugs_p.add_argument(
        "--propose-panel",
        action="store_true",
        help=(
            "Also emit proposed panel-shape starter lines (panel_entity_key / "
            "panel_time_key / panel_time_grain) on each register_variant (A4.4c-ii). "
            "Entity-key proposals come from variable.is_identifier (falling back to "
            "ID-kolumner join keys); time key/grain default to the delivery-aligned "
            "majority. These are starter hints — a curator reviews them in A4.4d."
        ),
    )

    precheck_p = sub.add_parser(
        "precheck-slugs",
        help="Validate slug TOMLs and list source IDs missing a slug entry.",
        description=(
            "Verify the slug TOMLs match the current DB. Reports:\n"
            "  - TOML parse / validation errors\n"
            "  - register / register_variant / classification rows with no slug\n"
            "  - non-additive changes vs. the committed snapshot (see DESIGN.md → Slug immutability)\n\n"
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

    same_as_p = sub.add_parser(
        "same-as-candidates",
        help="Generate variable_same_as candidate pairs (maintainer review worklist).",
        description=(
            "Infer cross-register `same_as` identity candidates from a BUILT DB\n"
            "(shared classification, shared value set, name agreement) and emit a\n"
            'tiered `[[edge]] type = "same_as"` TOML review worklist. NOTHING is\n'
            "materialized — same_as is resolver-load-bearing, so a maintainer reviews\n"
            "each pair and copies ONLY confirmed identities into\n"
            "reg_meta_build/curation/relations.toml. Reads a built DB; never mutates\n"
            "it.\n\n"
            "Tiers (strongest first): 1 = classification + value set + name; 2 =\n"
            "classification + name; 3 = classification + value set; 4 = a shared\n"
            "classification-NULL value set with >= --min-value-set-codes codes.\n"
            "A shared value set corroborates a tier only when it meets that code\n"
            "floor (a generic 2-code hub never lifts a pair's tier).\n\n"
            "Hub suppression (--max-signal-fanout): a signal spanning more than N\n"
            "registers is a hub and generates O(N^2) cross-register pairs; its pairs\n"
            "are dropped UNLESS the two variables' names agree (name-corroborated\n"
            "pairs are kept). The dropped count is reported, never silently truncated.\n\n"
            "Examples:\n"
            "  reg-meta-build same-as-candidates --output-toml /tmp/candidates.toml\n"
            "  reg-meta-build same-as-candidates --max-tier 2\n"
            "  reg-meta-build same-as-candidates --max-signal-fanout 0  # uncapped"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    same_as_p.add_argument(
        "-o",
        "--output-toml",
        default=None,
        help=(
            "Write the candidate TOML worklist to this path. Without it the JSON "
            "counts summary still prints; the TOML is included in the payload."
        ),
    )
    same_as_p.add_argument(
        "--max-tier",
        type=int,
        default=4,
        help="Highest tier to emit (1=strongest .. 4=value-set-only). Default 4.",
    )
    same_as_p.add_argument(
        "--min-value-set-codes",
        type=int,
        default=15,
        help=(
            "A shared value set must carry at least this many codes to corroborate "
            "a pair at ANY tier (excludes generic Ja/Nej hubs). Default 15."
        ),
    )
    same_as_p.add_argument(
        "--max-signal-fanout",
        type=int,
        default=12,
        help=(
            "Suppress pairs generated only by a signal spanning more than N "
            "registers (a hub); name-agreeing pairs are exempt. 0 disables. "
            "Default 12."
        ),
    )

    entity_key_pins_p = sub.add_parser(
        "entity-key-pins",
        help="Generate panel entity-key slug pins (mandatory curation, #546).",
        description=(
            "Emit `[variable]` slug pins for every panel entity-key variable on a\n"
            "BUILT DB, across ALL global providers (#554). A\n"
            "`register_variant.panel_entity_key` ref binds to a variable.slug, which\n"
            "CHURNS every build (the default freeze zone re-derives it); a reslug then\n"
            "silently dangles the ref. A curated pin (precedence 1 in slug population)\n"
            "freezes that slug. The build-side curation gate makes the pin MANDATORY,\n"
            "so a new entity-key variable can't ship un-pinned.\n\n"
            "Scope = ALL global providers (#554): any provider's entity-key slug can\n"
            "churn and dangle a panel ref, so every provider present in a build-db DB\n"
            "is emitted. Steward/swecov providers appear only in extend-db (whose\n"
            "flavored validate passes no slug_dir → the gate self-skips), so they're\n"
            "never reached.\n\n"
            "Idempotent: variables already carrying a hand-curated `[variable]` slug\n"
            "(the existing #539 pins) are SKIPPED, so re-running after the pins are\n"
            "committed emits nothing. Reads a built DB; never mutates it. The\n"
            "emitted block is dbdiff-identical (each pin reproduces the slug the\n"
            "variable already carries).\n\n"
            "Output: --out-dir writes one DIR/<provider>.toml per provider (the\n"
            "curation shape — fold each into fqid_slugs/<provider>.toml); --output-toml\n"
            "writes ALL providers' pins to a single file (for inspection). The two are\n"
            "mutually exclusive. With neither, the JSON payload carries the combined\n"
            "TOML and per-provider counts.\n\n"
            "Curation flow: run with --out-dir, then fold each NON-duplicate\n"
            "<provider>.toml block into reg_meta_build/fqid_slugs/<provider>.toml.\n"
            "(Chicken-and-egg: the first gated build of a new entity-key var fails —\n"
            "generate via a --no-validate build, commit the pins, then rebuild with\n"
            "validation.)\n\n"
            "The curated slug dir (--slug-dir; default: the repo's fqid_slugs/) is\n"
            "read to skip already-pinned variables.\n\n"
            "Examples:\n"
            "  reg-meta-build --db <built-db> entity-key-pins --out-dir /tmp/pins/\n"
            "  reg-meta-build --db <built-db> entity-key-pins \\\n"
            "    --output-toml /tmp/entity_key_pins.toml\n"
            "  reg-meta-build --db <built-db> entity-key-pins  # counts only"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    entity_key_pins_p.add_argument(
        "--out-dir",
        default=None,
        help=(
            "Write one <provider>.toml pin block per provider into this directory "
            "(the curation shape). Mutually exclusive with --output-toml."
        ),
    )
    entity_key_pins_p.add_argument(
        "-o",
        "--output-toml",
        default=None,
        help=(
            "Write the combined (all-providers) pin TOML block to this single path "
            "(for inspection). Mutually exclusive with --out-dir. Without either the "
            "JSON count summary still prints; the TOML is included in the payload."
        ),
    )
    entity_key_pins_p.add_argument(
        "--force",
        action="store_true",
        help="overwrite existing *.toml in --out-dir",
    )
    entity_key_pins_p.add_argument(
        "--slug-dir",
        default=None,
        help=(
            "Directory of curated slug TOMLs to read for already-pinned variables "
            "(default: reg_meta_build/fqid_slugs/ when run from a repo checkout)."
        ),
    )

    concept_group_p = sub.add_parser(
        "concept-group-candidates",
        help="Generate concept-group fold candidates (maintainer review worklist).",
        description=(
            "Scan a BUILT DB (the `--db` global) for ungrouped digit-suffixed slug\n"
            "families (sun-niva2000…, morsak1/2/3, the fasit yearly series) and\n"
            "regenerate the committed, machine-owned candidate catalog\n"
            "reg_meta_build/concept_groups.auto.toml. NOTHING is materialized;\n"
            "concept groups are presentation-only and folding is OPT-IN: a maintainer\n"
            "reviews each family and folds the confirmed ones by adding an\n"
            "`[[accept]]` (register + key) in reg_meta_build/concept_groups.toml.\n"
            "Reads a built DB; never mutates it, and never hand-edit the generated\n"
            "output.\n\n"
            "Without --output-toml the catalog is NOT written — only the JSON counts\n"
            "summary prints (the TOML is in the payload). To regenerate the committed\n"
            "file, point --output-toml at it (see the Examples).\n\n"
            "A family folds only past a label-agreement gate: its common\n"
            "case-insensitive name prefix must be >= --min-label-prefix chars AND\n"
            "the prefix-to-mean-name-length ratio >= --min-agreement. Families that\n"
            "share a stem but not a meaning (BATTERIES, e.g. ULF's 2-char survey\n"
            "items) fail the gate and are excluded; their count is reported, never\n"
            "silently dropped.\n\n"
            "The proposed `axis` (vintage / ordinal / numeric) and each member's\n"
            "facet `label` are EVIDENCE — the maintainer overrides them in the\n"
            "`[[accept]]` entry.\n\n"
            "Examples:\n"
            "  # Regenerate the committed catalog (the canonical invocation):\n"
            "  reg-meta-build --db <built-db-dir> concept-group-candidates \\\n"
            "    --output-toml reg_meta_build/concept_groups.auto.toml\n"
            "  # Preview counts only (writes nothing):\n"
            "  reg-meta-build --db <built-db-dir> concept-group-candidates\n"
            "  reg-meta-build --db <built-db-dir> concept-group-candidates "
            "--min-agreement 0.7"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    concept_group_p.add_argument(
        "-o",
        "--output-toml",
        default=None,
        help=(
            "Write the candidate catalog TOML to this path — point it at "
            "reg_meta_build/concept_groups.auto.toml to regenerate the committed "
            "file. Without it the JSON counts summary still prints; the TOML is "
            "included in the payload."
        ),
    )
    concept_group_p.add_argument(
        "--min-siblings",
        type=int,
        default=2,
        help=(
            "A family needs at least this many distinct digit suffixes to fold. "
            "Default 2."
        ),
    )
    concept_group_p.add_argument(
        "--min-label-prefix",
        type=int,
        default=8,
        help=(
            "Minimum common case-insensitive name-prefix length for a family to "
            "fold (shorter = a battery). Default 8."
        ),
    )
    concept_group_p.add_argument(
        "--min-agreement",
        type=float,
        default=0.5,
        help=(
            "Minimum common-prefix / mean-name-length ratio for a family to fold "
            "(lower = a battery). Default 0.5."
        ),
    )

    # `reg-meta-build` has no `--examples` handler (the query CLI's `--examples`
    # interceptor lives in `reg_meta.cli.run`); suppress the epilog so each
    # subcommand's --help doesn't point at an unrecognized flag.
    apply_leaf_help(parser, examples_epilog=False)
    return parser


# ---------------------------------------------------------------------------
# Command handlers
# ---------------------------------------------------------------------------


def _build_validate_hook(slug_dir: Path | None) -> Callable[[Path], None]:
    """Return a build_db pre_rename_hook that runs the post-build validator
    against the staging DB and raises on failure. Defined as a helper so the
    closure stays narrowly scoped.

    Runs with ``corpus=True``: build-db is the real maintainer build, so the
    provider-specific corpus-volume gates apply here (synthetic CI uses
    ``corpus=False``). ``slug_dir`` is the SAME resolved curation dir the build
    loaded, threaded through so the mandatory entity-key curation gate (#546) can
    read the curated ``[variable]`` pins."""

    def hook(staging_db: Path) -> None:
        validation = validate_built_db(staging_db, corpus=True, slug_dir=slug_dir)
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
                    "Fix the underlying build issue and rerun `reg-meta-build "
                    "build-db` (pass `--no-validate` to skip these checks)."
                ),
            )

    return hook


def _cmd_build_db(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    # `--timing` is surfaced to the build internals (db.py `_timing_enabled`) via
    # the env var so the deep helpers need no extra plumbing.
    if args.timing:
        os.environ["REG_META_BUILD_TIMING"] = "1"
    db_dir = Path(args.db) if args.db else default_db_dir()
    # Resolve the curation dir ONCE and feed the SAME value to both `build_db`
    # and the validate hook so the entity-key curation gate (#546) reads the
    # exact `[variable]` pins the build loaded. `build_db` itself falls back to
    # `repo_slug_dir()` when slug_dir is None (db.py: `slug_dir or
    # repo_slug_dir()`); mirror that here so the gate isn't handed None on the
    # default invocation (which would silently skip it).
    slug_dir = (
        Path(args.slug_dir).expanduser().resolve() if args.slug_dir else repo_slug_dir()
    )

    providers = tuple(p.strip() for p in args.providers.split(",") if p.strip())

    pre_rename_hook = None if args.no_validate else _build_validate_hook(slug_dir)
    result = build_db(
        input_dir=Path(args.input_dir),
        db_dir=db_dir,
        slug_dir=slug_dir,
        skip_slugs=args.skip_slugs,
        providers=providers,
        pre_rename_hook=pre_rename_hook,
    )
    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
        command="build-db",
        args_payload={
            "input_dir": args.input_dir,
            "skip_slugs": args.skip_slugs,
            "validate": not args.no_validate,
            "providers": list(providers),
        },
        db_info={
            "schema_version": SCHEMA_VERSION,
            "import_date": result["import_date"],
        },
        data=result,
        duration_ms=duration_ms,
    ), 0


def _flavored_validate_hook() -> Callable[[Path], None]:
    """Return an extend_db pre_rename_hook running the FLAVORED validator against
    the staging DB. Same fail-on-failures shape as ``_build_validate_hook``, but
    ``flavored=True`` (the tightened non-SCB minted-id band check) and
    ``corpus=False`` (a flavor adds a steward tail, not the SCB/SOS bulk, so the
    real-corpus volume floors don't apply)."""

    def hook(staging_db: Path) -> None:
        validation = validate_built_db(staging_db, flavored=True)
        sys.stderr.write(validation.format_report() + "\n")
        sys.stderr.flush()
        if validation.failures:
            raise RegMetaError(
                exit_code=EXIT_CONFIG,
                code="validation_failed",
                error_class="configuration",
                message=(
                    f"Post-overlay flavored validation failed: "
                    f"{len(validation.failures)} check(s) — "
                    f"{'; '.join(validation.failures)}"
                ),
                remediation=(
                    "Inspect the [FAIL] lines above. The staging DB has been "
                    "discarded and any previously-installed flavored DB is "
                    "unchanged. Fix the inventory and rerun `reg-meta-build "
                    "extend-db` (pass `--no-validate` to skip these checks)."
                ),
            )

    return hook


def _cmd_extend_db(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db_dir = Path(args.db) if args.db else default_db_dir()
    slug_dir = Path(args.slug_dir).expanduser().resolve() if args.slug_dir else None

    pre_rename_hook = None if args.no_validate else _flavored_validate_hook()
    result = extend_db(
        base_db=Path(args.base_db),
        inventory_path=Path(args.inventory),
        db_dir=db_dir,
        steward=args.steward,
        slug_dir=slug_dir,
        skip_slugs=args.skip_slugs,
        pre_rename_hook=pre_rename_hook,
    )
    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
        command="extend-db",
        args_payload={
            "base_db": args.base_db,
            "inventory": args.inventory,
            "steward": args.steward,
            "skip_slugs": args.skip_slugs,
            "validate": not args.no_validate,
        },
        db_info={"schema_version": SCHEMA_VERSION},
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
        written = seed_all(conn, out_dir, propose_panel=args.propose_panel)
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
            "propose_panel": args.propose_panel,
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
    # Per-provider slug-freeze model (#470): only `frozen` zones gate
    # rename/removal; `churning`/`curating` zones still write through.
    states = load_freeze_states(slug_dir)
    fz = frozen_zones(states)
    db = db_path_from_args(args.db)
    conn = open_db(db, check_schema=False)
    try:
        result = precheck_slugs(conn, slug_dir)
    finally:
        conn.close()

    snapshot_status: dict[str, Any] = {
        "path": str(snapshot_path),
        "freeze_states": states,
    }
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
            # Grow-only enforcement (see DESIGN.md → Slug immutability):
            # `--update-snapshot` must NOT bless a removal or a slug rename in a
            # `frozen` zone — that's how committed FQIDs rot in researcher
            # project_data.json files. Per-provider (#470): only `frozen` zones
            # refuse (`diff["blocked"]`); `churning`/`curating` zones write
            # rename/removal diffs through so curators can still iterate, with
            # the diffs reported so drift stays visible. Real providers advance
            # to `frozen` deliberately at v1.
            previous = read_snapshot(snapshot_path)
            diff = diff_snapshot(previous, current, frozen_zones=fz)
            non_additive = bool(diff["removed"] or diff["renamed"])
            if diff["blocked"]:
                snapshot_status["updated"] = False
                snapshot_status["update_skipped_reason"] = "frozen_zone_violation"
                snapshot_status["removed"] = diff["removed"]
                snapshot_status["renamed"] = diff["renamed"]
                snapshot_status["blocked"] = diff["blocked"]
                exit_code = EXIT_CONFIG
            else:
                write_snapshot(snapshot_path, current)
                snapshot_status["updated"] = True
                snapshot_status["added"] = diff["added"]
                if non_additive:
                    # Churning/curating write-through: surface what drifted so
                    # reviewers see the rename/removal explicitly in the envelope.
                    snapshot_status["removed"] = diff["removed"]
                    snapshot_status["renamed"] = diff["renamed"]
    else:
        previous = read_snapshot(snapshot_path)
        diff = diff_snapshot(previous, current, frozen_zones=fz)
        snapshot_status["added"] = diff["added"]
        snapshot_status["removed"] = diff["removed"]
        snapshot_status["renamed"] = diff["renamed"]
        # The read-only branch is a snapshot-FRESHNESS check, freeze-agnostic:
        # ANY added/removed/renamed fails CI so a maintainer can't merge slug
        # drift without round-tripping through `--update-snapshot` to commit a
        # refreshed .snapshot.json (mirrors
        # test_slug_snapshot.test_snapshot_covers_committed_additions). Zone
        # freeze state doesn't relax this — it only gates the write side above.
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
            # Advisory only (#143) — never affects `ok`/exit. Variables
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
            # Advisory only (A4.4a) — never affects `ok`/exit. The name-fallback
            # curation backlog: auto-slugged variables whose slug came from the
            # variable name / a `-N` disambiguator / the `v<provider_key>` last
            # resort (per the `# source:` markers in `<provider>.auto.toml`). A
            # curator works through these toward a canonical slug before the slug
            # freeze. Mirrors `drifting_variables` (informational, ungated).
            "name_fallback_variables": [
                {
                    "provider": prov,
                    "source_id": source_id,
                    "slug": slug,
                    "derivation": derivation,
                }
                for (prov, source_id, slug, derivation) in (
                    result.name_fallback_variables
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


def _cmd_same_as_candidates(
    args: argparse.Namespace,
) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db = db_path_from_args(args.db)
    # Schema-checked open: the generator reads current-schema tables
    # (variable_same_as, variable_state.classification_id), so a stale DB should
    # fail fast with the standard actionable schema-mismatch error, not crash
    # deep in a query.
    conn = open_db(db)
    # CLI contract: <=0 means DISABLED; the API takes None for ∞.
    fanout = args.max_signal_fanout if args.max_signal_fanout > 0 else None
    try:
        result = infer_same_as_candidates(
            conn,
            max_tier=args.max_tier,
            min_value_set_codes=args.min_value_set_codes,
            max_signal_fanout=fanout,
        )
    finally:
        conn.close()
    candidates = result.candidates

    counts_by_tier: dict[int, int] = {}
    for c in candidates:
        counts_by_tier[c.tier] = counts_by_tier.get(c.tier, 0) + 1
    toml = render_candidates_toml(
        candidates,
        counts_by_tier=counts_by_tier,
        max_signal_fanout=fanout,
        hub_suppressed=result.hub_suppressed,
    )

    data: dict[str, Any] = {
        "total": len(candidates),
        # Sorted-key dict so the JSON counts read tier-ascending.
        "counts_by_tier": {str(t): counts_by_tier[t] for t in sorted(counts_by_tier)},
        "max_tier": args.max_tier,
        "min_value_set_codes": args.min_value_set_codes,
        # `None` (disabled) serializes to JSON null; the raw CLI arg is in args_payload.
        "max_signal_fanout": fanout,
        "hub_suppressed": result.hub_suppressed,
    }
    if args.output_toml:
        out_path = Path(args.output_toml).expanduser().resolve()
        out_path.write_text(toml, encoding="utf-8")
        data["output_toml"] = str(out_path)
    else:
        # No file target — carry the TOML in the payload so the worklist isn't lost.
        data["toml"] = toml

    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
        command="same-as-candidates",
        args_payload={
            "max_tier": args.max_tier,
            "min_value_set_codes": args.min_value_set_codes,
            "max_signal_fanout": args.max_signal_fanout,
            "output_toml": args.output_toml,
        },
        db_info=None,
        data=data,
        duration_ms=duration_ms,
    ), 0


def _cmd_entity_key_pins(
    args: argparse.Namespace,
) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db = db_path_from_args(args.db)
    # --out-dir (per-provider files, the curation shape) and --output-toml
    # (single combined inspection file) are two output targets for the same
    # pins; picking both is a usage error rather than a silent precedence.
    if args.out_dir and args.output_toml:
        raise RegMetaError(
            exit_code=EXIT_USAGE,
            code="entity_key_pins_output_conflict",
            error_class="usage",
            message="--out-dir and --output-toml are mutually exclusive.",
            remediation=(
                "Pass --out-dir for per-provider files, or --output-toml for a "
                "single combined file — not both."
            ),
        )
    # The curation dir to skip already-pinned variables. Mirrors the build's
    # resolution (`--slug-dir` override, else the repo's fqid_slugs/). Missing
    # (wheel install, no checkout) is a usage error here — the generator MUST
    # read the curated pins to stay idempotent.
    slug_dir = (
        Path(args.slug_dir).expanduser().resolve() if args.slug_dir else repo_slug_dir()
    )
    if slug_dir is None:
        raise RegMetaError(
            exit_code=EXIT_USAGE,
            code="slug_dir_not_found",
            error_class="usage",
            message="No slug directory: not in a repo checkout and --slug-dir unset.",
            remediation="Run from the repo (ships fqid_slugs/) or pass --slug-dir.",
        )
    # Schema-checked open: the generator reads current-schema tables
    # (register_variant.panel_entity_key, variable.slug/provider_key), so a stale
    # DB should fail fast with the standard schema-mismatch error.
    conn = open_db(db)
    try:
        pins = infer_entity_key_pins(conn, slug_dir)
    finally:
        conn.close()

    # Per-provider pin counts are part of the JSON summary for ALL modes (the
    # help advertises "counts" for --out-dir, --output-toml, and no-target), so
    # compute them once before branching on the output target.
    counts: dict[str, int] = {}
    for pin in pins:
        counts[pin.provider_slug] = counts.get(pin.provider_slug, 0) + 1

    data: dict[str, Any] = {
        "count": len(pins),
        "counts": counts,
        "slug_dir": str(slug_dir),
    }
    if args.out_dir:
        out_dir = Path(args.out_dir).expanduser().resolve()
        written = write_entity_key_pins(pins, out_dir, force=args.force)
        data["out_dir"] = str(out_dir)
        data["files"] = written
    elif args.output_toml:
        # Single combined file, all providers — for inspection only.
        out_path = Path(args.output_toml).expanduser().resolve()
        out_path.write_text(render_entity_key_pins_toml(pins), encoding="utf-8")
        data["output_toml"] = str(out_path)
    else:
        # No file target — carry the combined TOML in the payload so the pins
        # aren't lost.
        data["toml"] = render_entity_key_pins_toml(pins)

    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
        command="entity-key-pins",
        args_payload={
            "slug_dir": args.slug_dir,
            "out_dir": args.out_dir,
            "output_toml": args.output_toml,
            "force": args.force,
        },
        db_info=None,
        data=data,
        duration_ms=duration_ms,
    ), 0


def _cmd_concept_group_candidates(
    args: argparse.Namespace,
) -> tuple[dict[str, Any], int]:
    start = time.perf_counter()
    db = db_path_from_args(args.db)
    # Schema-checked open: the generator reads current-schema tables
    # (variable.slug, concept_group_variable), so a stale DB should fail fast with
    # the standard actionable schema-mismatch error, not crash deep in a query.
    conn = open_db(db)
    # Accept-aware regeneration: an `[[accept]]`-ed auto family is materialized as a
    # `curated` group at build time, which a naive rescan would drop. Feed the
    # currently-accepted scopes so those families re-emit (idempotent catalog).
    # `repo_concept_groups_path()` is None outside a checkout → no accepts → empty
    # scopes (the empty path is byte-identical to the unaware scan).
    accepts = load_concept_group_accepts(repo_concept_groups_path())
    accepted_scopes = frozenset((a.provider, a.register, a.key) for a in accepts)
    try:
        result = infer_concept_group_candidates(
            conn,
            min_siblings=args.min_siblings,
            min_label_prefix=args.min_label_prefix,
            min_agreement=args.min_agreement,
            accepted_scopes=accepted_scopes,
        )
    finally:
        conn.close()

    toml = render_concept_candidates_toml(
        result,
        min_siblings=args.min_siblings,
        min_label_prefix=args.min_label_prefix,
        min_agreement=args.min_agreement,
    )

    data: dict[str, Any] = {
        "foldable": len(result.candidates),
        "excluded_batteries": result.excluded_batteries,
        "skipped_existing_key": result.skipped_existing_key,
        "min_siblings": args.min_siblings,
        "min_label_prefix": args.min_label_prefix,
        "min_agreement": args.min_agreement,
    }
    if args.output_toml:
        out_path = Path(args.output_toml).expanduser().resolve()
        out_path.write_text(toml, encoding="utf-8")
        data["output_toml"] = str(out_path)
    else:
        # No file target — carry the TOML in the payload so the worklist isn't lost.
        data["toml"] = toml

    duration_ms = int((time.perf_counter() - start) * 1000)
    return success_envelope(
        command="concept-group-candidates",
        args_payload={
            "min_siblings": args.min_siblings,
            "min_label_prefix": args.min_label_prefix,
            "min_agreement": args.min_agreement,
            "output_toml": args.output_toml,
        },
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
    "extend-db": _cmd_extend_db,
    "build-docs": _cmd_build_docs,
    "seed-slugs": _cmd_seed_slugs,
    "precheck-slugs": _cmd_precheck_slugs,
    "parse-sos": _cmd_parse_sos,
    "same-as-candidates": _cmd_same_as_candidates,
    "entity-key-pins": _cmd_entity_key_pins,
    "concept-group-candidates": _cmd_concept_group_candidates,
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
        "extend-db --base-db DB --inventory JSON [--steward S]",
        "Overlay a steward inventory onto a released global DB (flavored DB).",
    ),
    (
        "build-docs [--docs-dir DIR]",
        "Rebuild the doc DB from markdown files.",
    ),
    (
        "seed-slugs [--out-dir DIR] [--force] [--all-hints] [--propose-panel]",
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
    (
        "same-as-candidates [-o TOML] [--max-tier N] [--min-value-set-codes N] "
        "[--max-signal-fanout N]",
        "Infer variable_same_as candidate pairs (maintainer review worklist).",
    ),
    (
        "entity-key-pins [--out-dir DIR | -o TOML] [--slug-dir DIR]",
        "Generate panel entity-key slug pins, all global providers "
        "(mandatory curation, #546).",
    ),
    (
        "concept-group-candidates [-o TOML] [--min-siblings N] "
        "[--min-label-prefix N] [--min-agreement F]",
        "Infer concept-group fold candidates (maintainer review worklist).",
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
