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

    File sources are emitted with an explicit ``encoding='latin-1'``
    because the bundle is built for MONA, where SCB CSVs are Windows
    cp1252 and ``locale.getpreferredencoding()`` is ``cp1252`` (probed;
    see DESIGN.md § MONA upload). ``file_source``'s own default is
    ``utf-8`` -- right for general code, wrong for MONA -- so the
    wizard pins the MONA-correct value here. Users with UTF-8 files
    edit the literal in the generated bundle.
    """
    paths = file_paths or []
    if not dsn and not paths:
        raise ValueError("at least one of dsn or file_paths must be supplied")
    items: list[str] = []
    if dsn:
        items.append(f"sql_source(dsn={dsn!r})")
    for fp in paths:
        items.append(f"file_source(path={fp!r}, encoding='latin-1')")
    body = ",\n        ".join(items)
    return f"def configure():\n    return [\n        {body},\n    ]"


def _print_discover_instructions() -> None:
    print(
        f"Next:\n"
        f"  1. Upload {BUNDLE_FILENAME} to MONA.\n"
        f"  2. On MONA's batch client, run {BUNDLE_FILENAME} with python\n"
        f"     -> writes {DISCOVER_FILENAME} next to the script.\n"
        f"  3. Copy {DISCOVER_FILENAME} back into THIS directory.\n"
        f"  4. Re-run mock-data-wizard."
    )


def _print_extract_instructions() -> None:
    print(
        f"Next:\n"
        f"  1. Upload {CONFIG_FILENAME} next to {BUNDLE_FILENAME} on MONA.\n"
        f'  2. In the bundle, set MODE = "extract".\n'
        f"  3. On MONA's batch client, re-run {BUNDLE_FILENAME} with python\n"
        f"     -> writes {STATS_FILENAME}.\n"
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


# -- Phase 2 helpers (configure interview) ---------------------------------

# Strip the SQL-server `dbo.` schema prefix and SCB's own `scb_` prefix
# before matching the leading register stem. `lisa_2018` → `lisa`,
# `dbo.scb_rams_2020` → `rams`. Names starting with a digit don't match
# (the regex requires letters); the suggester returns None for those.
_REGISTER_PREFIX_RE = re.compile(r"^(?:dbo\.)?(?:scb_)?([a-z]+)")
# Only emit a suggestion when each source carries at least one strong
# SCB-style marker: `dbo.` schema prefix, `scb_` namespace prefix, or a
# 4-digit year suffix. A bare `registry_main` looks like a register
# stem to the regex but isn't one — without this gate the wizard would
# happily push REGISTRY at the user and let regmeta error out.
_REGISTER_MARKER_RE = re.compile(r"^(?:dbo\.|scb_)|\d{4}$")
# Trailing 4-digit year, optionally separated by `_` or `-`. Used to
# cluster `lisa_2018` / `lisa_2019` / `lisa_2020` into one panel.
_YEAR_SUFFIX_RE = re.compile(r"^(.+?)[_-]?(\d{4})$")
# Column names that look like a panel time-key. Lowercase comparison.
_TIME_KEY_NAMES = frozenset({"ar", "indatum", "year", "period"})
# A high_cardinality column with one of these suffixes is almost
# always a miscategorised code/type column. `_kod` and `_typ` are SCB
# conventions; trailing digits cover `Yrke3`, `SNI2007Niva3`, etc.
_AMBIGUOUS_SUFFIX_RE = re.compile(r"(_kod|_typ|\d+)$", re.IGNORECASE)
# Cap on per-column ambiguity prompts to avoid death-by-prompts on
# wide tables. The remainder is reported once at the end and left for
# the user to hand-edit.
_AMBIGUOUS_REVIEW_CAP = 10


def _suggest_register(discover: dict) -> str | None:
    """Heuristic register name from source-name prefixes.

    All source names sharing a single SCB-style prefix → uppercase that
    prefix (e.g. ``lisa_2018`` + ``lisa_2019`` → ``"LISA"``;
    ``dbo.scb_rams_2020`` → ``"RAMS"``). Returns ``None`` when:

    - a name doesn't match the prefix regex (e.g. starts with a digit),
    - sources span multiple prefixes,
    - or no source carries an SCB-style marker (``dbo.`` / ``scb_`` /
      year suffix). The marker gate is what stops a generic name like
      ``registry_main`` from generating a noisy ``REGISTRY`` suggestion
      that regmeta will then reject.
    """
    sources = discover.get("sources", [])
    if not sources:
        return None
    prefixes: set[str] = set()
    has_marker = False
    for src in sources:
        name = src.get("source_name", "").lower()
        m = _REGISTER_PREFIX_RE.match(name)
        if not m:
            return None
        prefixes.add(m.group(1))
        if _REGISTER_MARKER_RE.search(name):
            has_marker = True
    if not has_marker or len(prefixes) != 1:
        return None
    return prefixes.pop().upper()


def _detect_separate_file_panels(discover: dict) -> list[dict]:
    """Cluster sources by ``<prefix>_<4-digit year>`` suffix.

    Returns clusters of size ≥ 2 only:
        ``[{"prefix": "lisa", "members": [{"source": ..., "period": int}, ...]}]``
    Members are sorted by period ascending so the user sees a stable
    chronological list.
    """
    clusters: dict[str, list[dict]] = {}
    for src in discover.get("sources", []):
        name = src.get("source_name", "")
        m = _YEAR_SUFFIX_RE.match(name)
        if not m:
            continue
        prefix = m.group(1).rstrip("_-")
        if not prefix:
            continue
        year = int(m.group(2))
        clusters.setdefault(prefix, []).append({"source": name, "period": year})
    return [
        {"prefix": p, "members": sorted(ms, key=lambda d: d["period"])}
        for p, ms in clusters.items()
        if len(ms) >= 2
    ]


def _find_time_key_in_source(src: dict) -> str | None:
    """Return the source's first ``AR``/``INDATUM``/``year``/``period``
    column name (matched case-insensitively), else ``None``."""
    for col in src.get("columns", []):
        name = col.get("name", "")
        if name.lower() in _TIME_KEY_NAMES:
            return name
    return None


def _shared_id_column(member_sources: list[dict]) -> str | None:
    """Unique id-typed column name present in every member, else None.

    Uses ``is_known_id`` (the same name pattern the configurer uses) so
    a panel_key default lines up with whatever ``build_config`` would
    classify as ``id`` for these columns.
    """
    from .classify import is_known_id

    sets: list[set[str]] = []
    for src in member_sources:
        sets.append(
            {c["name"] for c in src.get("columns", []) if is_known_id(c["name"])}
        )
    if not sets:
        return None
    shared = set.intersection(*sets)
    return next(iter(shared)) if len(shared) == 1 else None


def _ambiguous_columns(payload: dict) -> list[tuple[str, str]]:
    """``[(source, column)]`` for ``high_cardinality`` columns whose
    names suggest they might be categorical/numeric instead.
    """
    out: list[tuple[str, str]] = []
    for source, cols in payload.get("column_types", {}).items():
        for col, entry in cols.items():
            if entry.get("type") == "high_cardinality" and _AMBIGUOUS_SUFFIX_RE.search(
                col
            ):
                out.append((source, col))
    return out


def _interview_panels(discover: dict, payload: dict) -> None:
    """Surface candidate panels and append confirmed ones to ``payload['panels']``.

    Two passes: separate-files clusters first (so the merged-table pass
    can skip already-claimed sources). Each candidate is presented as a
    yes/no prompt; declining leaves the schema untouched.
    """
    panels: list[dict] = list(payload.get("panels", []))
    used_sources: set[str] = set()
    sources_by_name = {s["source_name"]: s for s in discover.get("sources", [])}

    for cluster in _detect_separate_file_panels(discover):
        members = cluster["members"]
        members_str = ", ".join(m["source"] for m in members)
        print(f"\nThese {len(members)} sources look like a panel: {members_str}")
        if not _yes_no("Treat them as one panel?", default=True):
            continue
        member_srcs = [sources_by_name[m["source"]] for m in members]
        shared_id = _shared_id_column(member_srcs)
        panel_id = (
            _prompt("  panel_id", default=cluster["prefix"]).strip()
            or cluster["prefix"]
        )
        if shared_id:
            panel_key = _prompt("  panel_key", default=shared_id).strip() or shared_id
        else:
            panel_key = _prompt("  panel_key (no shared id-typed column found)").strip()
            if not panel_key:
                print("  Skipping panel (no panel_key supplied).", file=sys.stderr)
                continue
        panels.append(
            {
                "panel_id": panel_id,
                "layout": "separate_files",
                "panel_key": panel_key,
                "members": members,
            }
        )
        used_sources.update(m["source"] for m in members)

    for src in discover.get("sources", []):
        name = src["source_name"]
        if name in used_sources:
            continue
        time_key = _find_time_key_in_source(src)
        if not time_key:
            continue
        print(f"\nSource {name!r} has a time-key-like column {time_key!r}.")
        if not _yes_no("Set up a merged_table panel?", default=False):
            continue
        from .classify import is_known_id

        ids = [c["name"] for c in src.get("columns", []) if is_known_id(c["name"])]
        default_pk = ids[0] if len(ids) == 1 else None
        if default_pk:
            panel_key = _prompt("  panel_key", default=default_pk).strip() or default_pk
        else:
            panel_key = _prompt("  panel_key").strip()
            if not panel_key:
                print("  Skipping panel (no panel_key supplied).", file=sys.stderr)
                continue
        panel_id = _prompt("  panel_id", default=name).strip() or name
        panels.append(
            {
                "panel_id": panel_id,
                "layout": "merged_table",
                "panel_key": panel_key,
                "source": name,
                "time_key": time_key,
            }
        )
        used_sources.add(name)

    if panels:
        payload["panels"] = panels


def _interview_ambiguous(payload: dict) -> None:
    """Walk the user through suspicious ``high_cardinality`` columns.

    Per-column three-way prompt: keep / flip-categorical / flip-numeric.
    Capped at ``_AMBIGUOUS_REVIEW_CAP`` to avoid death-by-prompts; the
    overflow is reported as a "review by hand" hint at the end.
    """
    candidates = _ambiguous_columns(payload)
    if not candidates:
        return
    truncated = candidates[_AMBIGUOUS_REVIEW_CAP:]
    candidates = candidates[:_AMBIGUOUS_REVIEW_CAP]
    print(
        f"\n{len(candidates)} `high_cardinality` column(s) have suspicious "
        f"names (`*_kod`, `*_typ`, trailing digits)."
    )
    for source, col in candidates:
        choice = (
            _prompt(
                f"  {source}.{col} — [k]eep / [c]ategorical / [n]umeric",
                default="k",
            )
            .strip()
            .lower()
        )
        if choice in ("c", "categorical"):
            payload["column_types"][source][col] = {"type": "categorical"}
        elif choice in ("n", "numeric"):
            payload["column_types"][source][col] = {"type": "numeric"}
        # anything else (incl. 'k') → keep as high_cardinality
    if truncated:
        print(
            f"  …and {len(truncated)} more — review by hand in {CONFIG_FILENAME}.",
            file=sys.stderr,
        )


def _interview_suppress_k(payload: dict) -> None:
    """Optional walkthrough for raising ``suppress_k`` on sensitive columns.

    Skippable by default. Each entry is a ``<source-glob>:<column>``
    pair plus an integer k. The schema enforces k ≥ project-wide
    SUPPRESS_K (10); we additionally require k > 10 here because k=10
    matches the default and adding an "override" of the default is
    almost certainly a typo.
    """
    if not _yes_no(
        "\nAre there sensitive columns that should require k>10?",
        default=False,
    ):
        return
    options: dict[str, dict[str, dict[str, int]]] = {
        glob: {col: dict(opts) for col, opts in cols.items()}
        for glob, cols in payload.get("column_options", {}).items()
    }
    while True:
        spec = _prompt(
            "  suppress_k entry as `<source-glob>:<column>` (blank to finish)"
        ).strip()
        if not spec:
            break
        if ":" not in spec:
            print(
                "  Expected `<glob>:<column>` (e.g. `lisa_*:Diagnos`).",
                file=sys.stderr,
            )
            continue
        glob, col = (s.strip() for s in spec.split(":", 1))
        if not glob or not col:
            print("  Both glob and column are required.", file=sys.stderr)
            continue
        k_raw = _prompt("  k", default="20").strip() or "20"
        try:
            k = int(k_raw)
        except ValueError:
            print(f"  Not an integer: {k_raw!r}.", file=sys.stderr)
            continue
        if k <= 10:
            print("  Must be > 10 (the project minimum). Skipping.", file=sys.stderr)
            continue
        options.setdefault(glob, {}).setdefault(col, {})["suppress_k"] = k
    if options:
        payload["column_options"] = options


def _stage3_configure(cwd: Path, *, force: bool = False) -> int:
    import json as _json

    from regmeta.errors import RegmetaError

    from .configure import (
        _summary_counts,
        _validate_discover_payload,
        build_config,
        write_config,
    )

    discover_path = cwd / DISCOVER_FILENAME
    config_path = cwd / CONFIG_FILENAME

    print(f"I see {DISCOVER_FILENAME}.\n")

    if config_path.exists() and not force:
        if not _yes_no(f"{CONFIG_FILENAME} already exists. Overwrite?", default=False):
            print("Aborted.", file=sys.stderr)
            return 1

    try:
        payload = _json.loads(discover_path.read_text(encoding="utf-8"))
        _validate_discover_payload(payload, str(discover_path))
    except (_json.JSONDecodeError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    if not payload["sources"]:
        print(
            f"Error: {discover_path} has no sources -- nothing to configure.",
            file=sys.stderr,
        )
        return 1

    suggested = _suggest_register(payload)
    register_in = _prompt(
        "Which register is this project mostly built around?\n"
        "(LISA, RAMS, ... — pre-classifies categorical columns via regmeta; "
        "`-` skips regmeta)",
        default=suggested or "",
    ).strip()
    register = None if register_in == "-" else (register_in or None)

    try:
        config = build_config(payload, register=register)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    except RegmetaError as exc:
        print(f"Error: regmeta lookup failed: {exc.message}", file=sys.stderr)
        if exc.remediation:
            print(f"  {exc.remediation}", file=sys.stderr)
        print(
            "  (use `-` at the register prompt to skip regmeta entirely)",
            file=sys.stderr,
        )
        return 1

    _interview_panels(payload, config)
    _interview_ambiguous(config)
    _interview_suppress_k(config)

    write_config(config_path, config)
    counts = _summary_counts(config)
    summary = ", ".join(f"{t}={n}" for t, n in sorted(counts.items()))
    n_sources = len(config["column_types"])
    n_cols = sum(counts.values())
    print(
        f"\nWrote {config_path} ({n_sources} source(s), {n_cols} column(s)): {summary}"
    )

    print(f"\nReview {CONFIG_FILENAME} before uploading.")
    _print_extract_instructions()
    return 0


def _stage4_instructions(cwd: Path, *, force: bool = False) -> int:
    print(f"I see {CONFIG_FILENAME} but no {STATS_FILENAME} yet.\n")
    _print_extract_instructions()
    return 0


def _prompt_int(message: str, *, default: int) -> int:
    """Prompt for an integer; re-prompt on bad input."""
    while True:
        raw = _prompt(message, default=str(default)).strip() or str(default)
        try:
            return int(raw)
        except ValueError:
            print(f"  Must be an integer (got {raw!r}).", file=sys.stderr)


def _prompt_sample_pct(default: float = 1.0) -> float:
    """Prompt for a sample fraction in (0, 1]; re-prompt on bad input."""
    while True:
        raw = _prompt(
            "Sample fraction (0 < x ≤ 1; try 0.1 for fast iteration)",
            default=str(default),
        ).strip() or str(default)
        try:
            value = float(raw)
        except ValueError:
            print(f"  Must be a number (got {raw!r}).", file=sys.stderr)
            continue
        if not (0.0 < value <= 1.0):
            print(f"  Must be in (0, 1] (got {value}).", file=sys.stderr)
            continue
        return value


def _stage5_generate(cwd: Path, *, force: bool = False) -> int:
    from argparse import Namespace

    from .cli import _cmd_generate

    stats_path = cwd / STATS_FILENAME
    default_output_dir = cwd / MOCK_DATA_DIRNAME

    if force:
        # `--force` in the interactive flow = accept all defaults, no
        # prompts. `yes=True` skips _cmd_generate's own confirmations
        # since we're already in an auto-confirmed wizard.
        args = Namespace(
            stats=str(stats_path),
            seed=42,
            sample_pct=1.0,
            output_dir=str(default_output_dir),
            db=None,
            no_regmeta=False,
            register=None,
            yes=True,
            force=False,
            verbose=False,
        )
        return _cmd_generate(args)

    print(f"I see {STATS_FILENAME}.\n")

    seed = _prompt_int("Random seed", default=42)
    sample_pct = _prompt_sample_pct()
    use_regmeta = _yes_no(
        "Enrich with regmeta (variable names + value codes)?", default=True
    )
    register: str | None = None
    if use_regmeta:
        register_in = _prompt(
            "Filter regmeta to a single register? Press enter to skip"
        ).strip()
        register = register_in or None

    output_dir_in = (
        _prompt("Output directory", default=MOCK_DATA_DIRNAME).strip()
        or MOCK_DATA_DIRNAME
    )
    output_path = Path(output_dir_in)
    if not output_path.is_absolute():
        output_path = cwd / output_path

    delete_stale = False
    if output_path.is_dir() and any(p.is_file() for p in output_path.iterdir()):
        delete_stale = _yes_no(
            f"{output_path}/ already contains files. Delete stale "
            "(those not produced by this run)?",
            default=False,
        )

    args = Namespace(
        stats=str(stats_path),
        seed=seed,
        sample_pct=sample_pct,
        output_dir=str(output_path),
        db=None,
        no_regmeta=not use_regmeta,
        register=register,
        # The wizard already collected confirmations upstream; suppress
        # _cmd_generate's own prompts so the user isn't asked twice.
        yes=True,
        force=delete_stale,
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
