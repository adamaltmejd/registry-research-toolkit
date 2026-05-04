"""Interactive default flow: stage detection + dispatch.

Detects which pipeline artifact is present in cwd and either runs the
local action with prompts or prints instructions for the MONA-side
action that the CLI cannot perform itself.
"""

from __future__ import annotations

import enum
import re
import sys
from pathlib import Path

from . import _bundle
from .configure import CONFIG_FILENAME
from .extract import DISCOVER_FILENAME, STATS_FILENAME
from .generate import MOCK_DATA_DIRNAME

BUNDLE_FILENAME = _bundle.DEFAULT_OUTPUT_NAME


class Stage(enum.Enum):
    BUILD = "build"
    DISCOVER_INSTRUCTIONS = "discover_instructions"
    CONFIGURE = "configure"
    EXTRACT_INSTRUCTIONS = "extract_instructions"
    GENERATE = "generate"
    DONE = "done"


def _detect_stage(cwd: Path) -> Stage:
    if (cwd / STATS_FILENAME).exists():
        mock_dir = cwd / MOCK_DATA_DIRNAME
        if mock_dir.is_dir() and any(p.is_file() for p in mock_dir.iterdir()):
            return Stage.DONE
        return Stage.GENERATE
    if (cwd / CONFIG_FILENAME).exists():
        return Stage.EXTRACT_INSTRUCTIONS
    if (cwd / DISCOVER_FILENAME).exists():
        return Stage.CONFIGURE
    if (cwd / BUNDLE_FILENAME).exists():
        return Stage.DISCOVER_INSTRUCTIONS
    return Stage.BUILD


def _prompt(message: str, *, default: str | None = None) -> str:
    suffix = f" [{default}]" if default else ""
    try:
        raw = input(f"{message}{suffix}: ").strip()
    except EOFError:
        return default or ""
    return raw or (default or "")


def _yes_no(message: str, *, default: bool) -> bool:
    suffix = "[Y/n]" if default else "[y/N]"
    while True:
        try:
            raw = input(f"{message} {suffix} ").strip().lower()
        except EOFError:
            return default
        if not raw:
            return default
        if raw in ("y", "yes"):
            return True
        if raw in ("n", "no"):
            return False
        print("Please answer y or n.", file=sys.stderr)


def _yes_no_custom(message: str, *, default: str) -> str:
    """Three-way prompt: returns ``'y'``, ``'n'``, or ``'c'`` (custom)."""
    suffix_map = {"y": "[Y/n/c]", "n": "[y/N/c]"}
    suffix = suffix_map[default]
    while True:
        try:
            raw = input(f"{message} {suffix} ").strip().lower()
        except EOFError:
            return default
        if not raw:
            return default
        if raw in ("y", "yes"):
            return "y"
        if raw in ("n", "no"):
            return "n"
        if raw in ("c", "custom"):
            return "c"
        print('Please answer y, n, or c (for "custom").', file=sys.stderr)


_PROJECT_RE = re.compile(r"^P?(\d{4})$", re.IGNORECASE)


def _normalize_project_number(raw: str) -> str | None:
    """Accept ``P1405``/``p1405``/``1405`` → ``"P1405"``; else ``None``."""
    m = _PROJECT_RE.match(raw.strip())
    return f"P{m.group(1)}" if m else None


def _prompt_project_number() -> str:
    while True:
        raw = _prompt("Project number (e.g. P1405)")
        if not raw:
            print("Project number is required.", file=sys.stderr)
            continue
        normalized = _normalize_project_number(raw)
        if not normalized:
            print(
                "Expected 4 digits (e.g. 1405) or P-prefixed (e.g. P1405).",
                file=sys.stderr,
            )
            continue
        return normalized


def _render_configure_body(
    *, dsn: str | None = None, file_paths: list[str] | None = None
) -> str:
    """Render a ``configure()`` body from the user's Stage 1 answers.

    Uses ``repr()`` on the user-supplied strings so UNC paths
    (``\\\\micro.intra\\projekt\\P1105$\\P1105_Data``) and DSN names
    round-trip safely as Python literals.
    """
    paths = file_paths or []
    if not dsn and not paths:
        raise ValueError("at least one of dsn or file_paths must be supplied")
    items: list[str] = []
    if dsn:
        items.append(f"sql_source(dsn={dsn!r})")
    for fp in paths:
        items.append(f"file_source(path={fp!r})")
    body = ",\n        ".join(items)
    return f"def configure():\n    return [\n        {body},\n    ]"


def _print_discover_instructions() -> None:
    print(
        f"Next:\n"
        f"  1. Upload {BUNDLE_FILENAME} to MONA.\n"
        f"  2. On MONA: run {BUNDLE_FILENAME} with python on the batch client\n"
        f"     -> writes {DISCOVER_FILENAME} next to the script.\n"
        f"  3. Copy {DISCOVER_FILENAME} back into THIS directory.\n"
        f"  4. Re-run mock-data-wizard."
    )


def _print_extract_instructions() -> None:
    print(
        f"Next:\n"
        f"  1. Upload {CONFIG_FILENAME} next to {BUNDLE_FILENAME} on MONA.\n"
        f'  2. In the bundle, set MODE = "extract".\n'
        f"  3. On MONA: re-run {BUNDLE_FILENAME} with python on the batch\n"
        f"     client -> writes {STATS_FILENAME}.\n"
        f"  4. Sanity check (locally): mock-data-wizard scan {STATS_FILENAME}\n"
        f"  5. Copy {STATS_FILENAME} back into THIS directory.\n"
        f"  6. Re-run mock-data-wizard."
    )


def _stage1_build(cwd: Path, *, force: bool = False) -> int:
    print("Welcome to mock-data-wizard.")
    print("I'm assuming this directory is your project workspace:")
    print(f"  {cwd}")
    print(
        f"All artifacts ({BUNDLE_FILENAME}, {DISCOVER_FILENAME},\n"
        f"{CONFIG_FILENAME}, {STATS_FILENAME}, {MOCK_DATA_DIRNAME}/) will live here.\n"
    )

    project = _prompt_project_number()

    sql_choice = _yes_no_custom(
        f"Do you have a SQL/ODBC source on MONA? (Y = DSN '{project}'; c = custom DSN)",
        default="y",
    )
    if sql_choice == "y":
        dsn = project
    elif sql_choice == "c":
        dsn = _prompt("Custom DSN name").strip()
        if not dsn:
            print("DSN cannot be empty.", file=sys.stderr)
            return 1
    else:
        dsn = ""

    default_unc = rf"\\micro.intra\projekt\{project}$\{project}_Data"
    file_choice = _yes_no_custom(
        f"Do you have file-based data (CSV/TXT on a UNC share)? "
        f"(y = '{default_unc}'; c = custom path)",
        default="n",
    )
    file_paths: list[str] = []
    if file_choice == "y":
        file_paths.append(default_unc)
    elif file_choice == "c":
        first = _prompt("Path").strip()
        if not first:
            print("Path cannot be empty.", file=sys.stderr)
            return 1
        file_paths.append(first)

    while file_paths and _yes_no("Add another file source path?", default=False):
        extra = _prompt("Path").strip()
        if not extra:
            print("Path cannot be empty; skipping.", file=sys.stderr)
            continue
        file_paths.append(extra)

    if not dsn and not file_paths:
        print("Need at least one source. Aborting.", file=sys.stderr)
        return 1

    bundle_path = cwd / BUNDLE_FILENAME
    if bundle_path.exists() and not force:
        if not _yes_no(
            f"{BUNDLE_FILENAME} already exists. Rebuild? "
            "Any hand-edits to configure() will be lost.",
            default=False,
        ):
            print("Aborted.", file=sys.stderr)
            return 1

    body = _render_configure_body(dsn=dsn or None, file_paths=file_paths)
    out = _bundle.build_bundle(bundle_path, configure_body=body)
    print(f"\nBuilt {out} ({out.stat().st_size:,} bytes)\n")
    _print_discover_instructions()
    return 0


def _stage2_instructions(cwd: Path, *, force: bool = False) -> int:
    print(f"I see {BUNDLE_FILENAME} but no {DISCOVER_FILENAME} yet.\n")
    _print_discover_instructions()
    print()
    if _yes_no("Want to rebuild the bundle (e.g. add a source)?", default=False):
        return _stage1_build(cwd, force=force)
    return 0


def _stage3_configure(cwd: Path, *, force: bool = False) -> int:
    from .configure import run_configure_from_discover

    discover_path = cwd / DISCOVER_FILENAME
    config_path = cwd / CONFIG_FILENAME

    print(f"I see {DISCOVER_FILENAME}.\n")
    register_in = _prompt(
        "Which register is this project mostly built around? Press enter to skip\n"
        "(LISA, SCB-RAMS, ... — used by regmeta to pre-classify categorical "
        "columns)"
    )
    register = register_in or None

    if config_path.exists() and not force:
        if not _yes_no(f"{CONFIG_FILENAME} already exists. Overwrite?", default=False):
            print("Aborted.", file=sys.stderr)
            return 1

    rc = run_configure_from_discover(
        discover_path,
        output_path=config_path,
        overwrite=True,
        register=register,
        regmeta_skip_hint="leave register blank to skip regmeta lookup entirely",
    )
    if rc != 0:
        return rc

    print(
        f"\nReview {CONFIG_FILENAME} before uploading. Common edits:\n"
        "  - flip suspicious columns to high_cardinality (the safe type)\n"
        "  - add a panels: [...] block if your project has panel structure\n"
        "    (DESIGN.md § Panels)\n"
        "  - raise suppress_k for sensitive columns (DESIGN.md § PII safety)\n"
    )
    _print_extract_instructions()
    return 0


def _stage4_instructions(cwd: Path, *, force: bool = False) -> int:
    print(f"I see {CONFIG_FILENAME} but no {STATS_FILENAME} yet.\n")
    _print_extract_instructions()
    return 0


def _stage5_generate(cwd: Path, *, force: bool = False) -> int:
    from argparse import Namespace

    from .cli import _cmd_generate

    print(
        f"I see {STATS_FILENAME} — generating mock CSVs with default settings.\n"
        "(Phase 3 will let you customise seed, sample fraction, etc.)\n"
    )

    args = Namespace(
        stats=str(cwd / STATS_FILENAME),
        seed=42,
        sample_pct=1.0,
        output_dir=str(cwd / MOCK_DATA_DIRNAME),
        db=None,
        no_regmeta=False,
        register=None,
        yes=force,
        force=force,
        verbose=False,
    )
    return _cmd_generate(args)


def _done(cwd: Path, *, force: bool = False) -> int:
    mock_dir = cwd / MOCK_DATA_DIRNAME
    n_files = sum(1 for p in mock_dir.iterdir() if p.is_file())
    present = [
        name
        for name in (
            BUNDLE_FILENAME,
            DISCOVER_FILENAME,
            CONFIG_FILENAME,
            STATS_FILENAME,
        )
        if (cwd / name).exists()
    ]
    present.append(f"{MOCK_DATA_DIRNAME}/ ({n_files} files)")
    print("This project looks complete:\n  " + " / ".join(present) + "\n")
    print(
        f"What now?\n"
        f"  [r] regenerate mock CSVs from {STATS_FILENAME}\n"
        f"  [c] re-run configure (re-author {CONFIG_FILENAME})\n"
        f"  [b] rebuild the bundle\n"
        f"  [q] quit"
    )
    choice = _prompt("Choice", default="q").lower()
    if choice == "r":
        return _stage5_generate(cwd, force=force)
    if choice == "c":
        return _stage3_configure(cwd, force=force)
    if choice == "b":
        return _stage1_build(cwd, force=force)
    return 0


_DISPATCH = {
    Stage.BUILD: _stage1_build,
    Stage.DISCOVER_INSTRUCTIONS: _stage2_instructions,
    Stage.CONFIGURE: _stage3_configure,
    Stage.EXTRACT_INSTRUCTIONS: _stage4_instructions,
    Stage.GENERATE: _stage5_generate,
    Stage.DONE: _done,
}


def run(cwd: Path, *, force: bool = False) -> int:
    try:
        return _DISPATCH[_detect_stage(cwd)](cwd, force=force)
    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        return 130
