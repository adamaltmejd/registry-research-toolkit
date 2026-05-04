"""Interactive default flow: stage detection + dispatch.

Detects which pipeline artifact is present in cwd and either runs the
local action with prompts or prints instructions for the MONA-side
action that the CLI cannot perform itself.
"""

from __future__ import annotations

import enum
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


def _render_configure_body(
    *, dsn: str | None = None, file_path: str | None = None
) -> str:
    """Render a ``configure()`` body from the user's Stage 1 answers.

    Uses ``repr()`` on the user-supplied strings so UNC paths
    (``\\\\micro.intra\\projekt\\P1105$\\P1105_Data``) and DSN names
    round-trip safely as Python literals.
    """
    if not dsn and not file_path:
        raise ValueError("at least one of dsn or file_path must be supplied")
    items: list[str] = []
    if dsn:
        items.append(f"sql_source(dsn={dsn!r})")
    if file_path:
        items.append(f"file_source(path={file_path!r})")
    body = ",\n        ".join(items)
    return f"def configure():\n    return [\n        {body},\n    ]"


def _print_discover_instructions() -> None:
    print(
        f"Next:\n"
        f"  1. Upload {BUNDLE_FILENAME} to MONA (10 MB cap; .py is accepted).\n"
        f'  2. Leave MODE = "discover" (the default).\n'
        f"  3. Run on MONA: python {BUNDLE_FILENAME}\n"
        f"     -> writes {DISCOVER_FILENAME} next to the script.\n"
        f"  4. Copy {DISCOVER_FILENAME} back into THIS directory.\n"
        f"  5. Re-run mock-data-wizard."
    )


def _print_extract_instructions() -> None:
    print(
        f"Next:\n"
        f"  1. Upload {CONFIG_FILENAME} next to {BUNDLE_FILENAME} on MONA.\n"
        f'  2. In the bundle, set MODE = "extract".\n'
        f"  3. Run: python {BUNDLE_FILENAME} -> writes {STATS_FILENAME}.\n"
        f"  4. Sanity check (locally): mock-data-wizard scan {STATS_FILENAME}\n"
        f"  5. Copy {STATS_FILENAME} back into THIS directory.\n"
        f"  6. Re-run mock-data-wizard."
    )


def _stage1_build(cwd: Path) -> int:
    print("Welcome to mock-data-wizard.")
    print("I'm assuming this directory is your project workspace:")
    print(f"  {cwd}")
    print(
        "All artifacts (bundle, discover.json, mdw_config.json, stats.json,\n"
        f"{MOCK_DATA_DIRNAME}/) will live here.\n"
    )

    has_sql = _yes_no("Do you have a SQL/ODBC source on MONA?", default=True)
    dsn = _prompt("DSN name (usually the project ID, e.g. P1105)") if has_sql else ""

    has_file = _yes_no(
        "Do you have file-based data (CSV/TXT on a UNC share)?", default=False
    )
    file_path = _prompt("Path") if has_file else ""

    if not dsn and not file_path:
        print("Need at least one source. Aborting.", file=sys.stderr)
        return 1

    bundle_path = cwd / BUNDLE_FILENAME
    if bundle_path.exists():
        if not _yes_no(
            f"{BUNDLE_FILENAME} already exists. Rebuild? "
            "Any hand-edits to configure() will be lost.",
            default=False,
        ):
            print("Aborted.", file=sys.stderr)
            return 1

    body = _render_configure_body(dsn=dsn or None, file_path=file_path or None)
    out = _bundle.build_bundle(bundle_path, configure_body=body)
    print(f"\nBuilt {out} ({out.stat().st_size:,} bytes)\n")
    _print_discover_instructions()
    return 0


def _stage2_instructions(cwd: Path) -> int:
    print(f"I see {BUNDLE_FILENAME} but no {DISCOVER_FILENAME} yet.\n")
    _print_discover_instructions()
    print()
    if _yes_no("Want to rebuild the bundle (e.g. add a source)?", default=False):
        return _stage1_build(cwd)
    return 0


def _stage3_configure(cwd: Path) -> int:
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

    if config_path.exists():
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


def _stage4_instructions(cwd: Path) -> int:
    print(f"I see {CONFIG_FILENAME} but no {STATS_FILENAME} yet.\n")
    _print_extract_instructions()
    return 0


def _stage5_generate(cwd: Path) -> int:
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
        yes=False,
        force=False,
        verbose=False,
    )
    return _cmd_generate(args)


def _done(cwd: Path) -> int:
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
        "What now?\n"
        "  [r] regenerate mock CSVs from stats.json\n"
        "  [c] re-run configure (re-author mdw_config.json)\n"
        "  [b] rebuild the bundle\n"
        "  [q] quit"
    )
    choice = _prompt("Choice", default="q").lower()
    if choice == "r":
        return _stage5_generate(cwd)
    if choice == "c":
        return _stage3_configure(cwd)
    if choice == "b":
        return _stage1_build(cwd)
    return 0


_DISPATCH = {
    Stage.BUILD: _stage1_build,
    Stage.DISCOVER_INSTRUCTIONS: _stage2_instructions,
    Stage.CONFIGURE: _stage3_configure,
    Stage.EXTRACT_INSTRUCTIONS: _stage4_instructions,
    Stage.GENERATE: _stage5_generate,
    Stage.DONE: _done,
}


def run(cwd: Path) -> int:
    try:
        return _DISPATCH[_detect_stage(cwd)](cwd)
    except KeyboardInterrupt:
        print("\nAborted.", file=sys.stderr)
        return 130
