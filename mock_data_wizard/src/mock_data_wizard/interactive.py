"""Interactive default flow: stage detection + dispatch.

Detects which pipeline artifact is present in cwd and either runs the
local action with prompts or prints instructions for the MONA-side
action that the CLI cannot perform itself.
"""

from __future__ import annotations

import enum
import re
import shutil
import sys
import textwrap
from pathlib import Path
from typing import Any

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

# Trailing 4-digit year. The negative lookbehind blocks a match when
# the 4 digits are preceded by another digit — that's how
# `..._20241231` (a YYYYMMDD date) avoids being parsed as year=1231,
# while still letting `Kursprov_gymn_HT2011` (no underscore separator
# but a non-digit boundary) match cleanly.
_YEAR_SUFFIX_RE = re.compile(r"^(.+?)(?<!\d)(\d{4})$")
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
        # Strip any single dot-extension before matching the year
        # suffix; otherwise `Äp9_2003.csv` never clusters with its
        # siblings because `.csv` isn't 4 digits.
        m = _YEAR_SUFFIX_RE.match(_strip_extension(name))
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


_CONFIDENCE_GLYPH = {"high": "✓", "partial": "⚠", "none": "✗"}
_CONFIDENCE_RANK = {"high": 0, "partial": 1, "none": 2}
_TYPE_KEY_MAP = {
    "c": "categorical",
    "n": "numeric",
    "d": "date",
    "i": "id",
    "h": "high_cardinality",
}
_VALID_TYPES = frozenset({"id", "categorical", "numeric", "high_cardinality", "date"})

# Below this width we drop the trailing "(X/Y matched)" tail and tighten
# the source/column count column. Above it we render the full row.
_NARROW_WIDTH_THRESHOLD = 90
# Minimum columns we'll render at; below this we just print without
# pretty alignment (rare — most terminals are 80+).
_MIN_RENDER_WIDTH = 60


def _terminal_width(default: int = 100) -> int:
    """Best-effort terminal width. Falls back to ``default`` on failure."""
    try:
        w = shutil.get_terminal_size().columns
    except (OSError, ValueError):
        return default
    return w if w > 0 else default


def _truncate(s: str, width: int) -> str:
    """Truncate ``s`` to ``width`` with a single-char ellipsis when over."""
    if width <= 0:
        return ""
    if len(s) <= width:
        return s
    if width == 1:
        return "…"
    return s[: width - 1] + "…"


def _print_wrapped(prefix: str, body: str, width: int) -> None:
    """Print ``prefix + body`` wrapped to ``width``, hanging-indented to
    align continuation lines under ``body``.

    Used for inspector group/section headers where ``body`` is a
    comma-separated source list that may exceed terminal width.
    """
    indent = " " * len(prefix)
    wrapped = textwrap.wrap(
        prefix + body,
        width=max(width, len(prefix) + 8),
        subsequent_indent=indent,
        break_long_words=False,
        break_on_hyphens=False,
    )
    for line in wrapped or [prefix]:
        print(line)


def _strip_extension(name: str) -> str:
    """Strip a single trailing dot-extension (``foo.csv`` → ``foo``)."""
    return name.rsplit(".", 1)[0] if "." in name else name


def _collapse_year_ranges(years: list[int]) -> str:
    """``[2003, 2004, 2005, 2008, 2009]`` → ``"2003–2005, 2008–2009"``.

    Contiguous runs collapse; gaps split into separate ranges so things
    like the COVID-cancelled national-test years (2020/2021 missing)
    render as ``2012–2019, 2022–2024`` rather than papering over the
    gap as ``2012–2024``.
    """
    if not years:
        return ""
    sorted_years = sorted(set(years))
    ranges: list[tuple[int, int]] = []
    start = prev = sorted_years[0]
    for y in sorted_years[1:]:
        if y == prev + 1:
            prev = y
        else:
            ranges.append((start, prev))
            start = prev = y
    ranges.append((start, prev))
    return ", ".join(f"{s}–{e}" if s != e else str(s) for s, e in ranges)


def _format_source_list(sources: list[str]) -> str:
    """Render a list of source filenames for the inspector / menu.

    Files matching ``<stem>_YYYY[.ext]`` and sharing the same stem
    collapse into one entry with their year range
    (``Individ_2018.csv`` … ``Individ_2024.csv`` →
    ``Individ_2018–2024``). Other files — and singletons — appear with
    their full filename. Ordering follows the input list so menu and
    inspector text match the discovery order the user sees elsewhere.
    """
    if not sources:
        return ""
    if len(sources) == 1:
        return sources[0]
    grouped: dict[tuple[str, str], list[tuple[str, int | None]]] = {}
    order: list[tuple[str, str]] = []
    for s in sources:
        base = _strip_extension(s)
        m = _YEAR_SUFFIX_RE.match(base)
        if m:
            key = ("yr", m.group(1).rstrip("_-"))
            year: int | None = int(m.group(2))
        else:
            key = ("lit", s)
            year = None
        if key not in grouped:
            grouped[key] = []
            order.append(key)
        grouped[key].append((s, year))
    parts: list[str] = []
    for key in order:
        items = grouped[key]
        # Only collapse when ≥2 distinct years share a stem; a lone
        # `Foo_2024.csv` is more honest as the literal filename than
        # as `Foo_2024`.
        if key[0] == "yr" and len({y for _, y in items if y is not None}) >= 2:
            years = [y for _, y in items if y is not None]
            parts.append(f"{key[1]}_{_collapse_year_ranges(years)}")
        else:
            for name, _ in items:
                parts.append(name)
    return ", ".join(parts)


# -- Register groups -------------------------------------------------------
# A register group bundles every schema family that shares a register
# guess. The user reviews one row per register (~10 rows for a typical
# multi-register study, vs. one per schema family before — which can be
# 50+ when annual deliveries drift in column shape). Schema variation
# within a group is surfaced inside the inspector by splitting columns
# into "shared by all" and "only in <subset>" sections.


from dataclasses import dataclass, field  # noqa: E402 — local to this section


@dataclass
class RegisterGroup:
    """All sources sharing one auto-detected register, regardless of schema variant.

    Members may have different schemas (annual deliveries with column
    drift). The inspector groups columns by which subset of members
    contains each one, so the user can read off the stable core vs. the
    variant-specific columns without first picking a year.
    """

    group_id: str
    register_id: int | None
    register_name: str | None
    register_str: str
    confidence: str
    sources: list[str] = field(default_factory=list)
    columns_by_source: dict[str, list[tuple[str, str | None]]] = field(
        default_factory=dict
    )
    # column name → classification short_name (or None when regmeta sees
    # the column as categorical text but with no shared classification —
    # e.g. ALKod, FamStF). Absent when regmeta has no entry. Used by the
    # inspector to render the trailing "(SUN2020-GRUPP)" / "(regmeta)" tag.
    regmeta_tags: dict[str, str | None] = field(default_factory=dict)
    schema_variants: int = 0


def _worst_confidence(a: str, b: str) -> str:
    """Pick the higher-rank (worse) of two confidence labels.

    A register group is only as confident as its weakest schema variant
    — if any annual delivery has mostly-unmatched columns under the
    chosen register, the group surfaces ``⚠`` so the user notices.
    """
    return max(a, b, key=_CONFIDENCE_RANK.__getitem__)


def _register_display(register_id: int | None, register_name: str | None) -> str:
    if register_name:
        return f"{register_name} (id={register_id})"
    if register_id is not None:
        return f"id={register_id}"
    return "—"


def group_by_register(
    families: dict[str, list[dict]],
    guesses: dict[str, Any],
) -> dict[str, RegisterGroup]:
    """Bucket schema families by register_id; build per-group metadata.

    No-register families (custom CSVs, key-mapping files, all-id
    schemas with no regmeta hits) each get their own group rather than
    being merged into a single "no register" bucket — they're
    heterogeneous and the user needs to decide on each one separately.
    """
    groups: dict[str, RegisterGroup] = {}
    for fid, family_sources in families.items():
        guess = guesses[fid]
        key = (
            f"reg-{guess.register_id}"
            if guess.register_id is not None
            else f"noreg-{fid}"
        )
        if key not in groups:
            groups[key] = RegisterGroup(
                group_id=key,
                register_id=guess.register_id,
                register_name=guess.register_name,
                register_str=_register_display(guess.register_id, guess.register_name),
                confidence=guess.confidence,
            )
        grp = groups[key]
        grp.schema_variants += 1
        grp.confidence = _worst_confidence(grp.confidence, guess.confidence)
        # Merge regmeta tags across schema variants of the same register.
        # If a column appears in multiple variants with conflicting
        # classifications (rare; typically only the schema changes year
        # to year, not the variable's classification), the first non-None
        # short_name wins so the user still sees a specific tag.
        for col_name, sn in guess.regmeta_tags.items():
            existing = grp.regmeta_tags.get(col_name, ...)
            if existing is ... or (existing is None and sn is not None):
                grp.regmeta_tags[col_name] = sn
        for src in family_sources:
            name = src["source_name"]
            grp.sources.append(name)
            grp.columns_by_source[name] = [
                (c["name"], c.get("sql_type")) for c in src.get("columns", [])
            ]
    return groups


def _columns_by_availability(
    group: RegisterGroup,
) -> list[tuple[list[str], list[str]]]:
    """Return ``[(source_subset, sorted_col_names), ...]`` for the inspector.

    First entry is always the columns shared by all sources (when any
    exist). Subsequent entries are subsets ordered by descending size,
    ties broken by alphabetic source list. Source subsets are returned
    in original discovery order so year-range labels render naturally.
    """
    col_to_sources: dict[str, set[str]] = {}
    for src_name, cols in group.columns_by_source.items():
        for col_name, _ in cols:
            col_to_sources.setdefault(col_name, set()).add(src_name)
    by_set: dict[frozenset[str], list[str]] = {}
    for col_name, srcs in col_to_sources.items():
        by_set.setdefault(frozenset(srcs), []).append(col_name)

    ordered_unique = list(dict.fromkeys(group.sources))
    all_set = frozenset(ordered_unique)
    sections: list[tuple[list[str], list[str]]] = []
    if all_set in by_set:
        sections.append((ordered_unique, sorted(by_set.pop(all_set))))
    for src_set, cols in sorted(
        by_set.items(),
        key=lambda kv: (-len(kv[0]), sorted(kv[0])),
    ):
        ordered = [s for s in ordered_unique if s in src_set]
        sections.append((ordered, sorted(cols)))
    return sections


def _render_register_menu(groups: dict[str, RegisterGroup]) -> list[str]:
    """Print the register-group menu. Returns group ids in display order."""
    gids = list(groups.keys())
    width = _terminal_width()

    n_idx_w = max(2, len(str(len(gids))))
    fixed_left = 2 + 1 + n_idx_w + 2 + 1
    fixed_glyph = 3
    counts_w = len(" 999 src, 99–999 cols, 99 schemas")
    overhead = fixed_left + fixed_glyph + counts_w + 2

    budget = max(width - overhead, _MIN_RENDER_WIDTH - overhead)
    label_w = max(16, int(budget * 0.40))
    register_w = max(20, budget - label_w)
    cont_indent = " " * (2 + 1 + n_idx_w + 2 + label_w + 1 + 2)

    print("Register groups:\n")
    for i, gid in enumerate(gids, 1):
        grp = groups[gid]
        glyph = _CONFIDENCE_GLYPH[grp.confidence]
        unique_sources = list(dict.fromkeys(grp.sources))
        n_src = len(unique_sources)
        col_counts = [len(cols) for cols in grp.columns_by_source.values()]
        if grp.schema_variants > 1:
            lo, hi = min(col_counts), max(col_counts)
            counts_part = (
                f"{n_src:>3} src, {lo}–{hi} cols, {grp.schema_variants} schemas"
            )
        else:
            counts_part = f"{n_src:>3} src, {col_counts[0]:>4} cols"
        label = _truncate(_format_source_list(unique_sources), label_w)
        reg_chunks = textwrap.wrap(grp.register_str, width=register_w) or [""]
        first_line = (
            f"  [{i:>{n_idx_w}}] {label:<{label_w}} {glyph} "
            f"{reg_chunks[0]:<{register_w}} {counts_part}"
        )
        print(first_line)
        for chunk in reg_chunks[1:]:
            print(cont_indent + chunk)
    print()
    return gids


def _resolve_column_type(
    col_name: str,
    sources: list[str],
    columns_by_source: dict[str, list[tuple[str, str | None]]],
    column_overrides: dict[tuple[str, str], str],
    config: dict,
) -> str:
    """Look up the current effective type for a column in the group.

    Walks group sources in order and returns the first match: an
    explicit override beats the auto-classified type from ``config``.
    Returns ``"?"`` if no source carries the column (shouldn't happen
    for columns surfaced by ``_columns_by_availability``).
    """
    for src_name in sources:
        if (src_name, col_name) in column_overrides:
            return column_overrides[(src_name, col_name)]
        cols = config.get("column_types", {}).get(src_name, {})
        if col_name in cols:
            return cols[col_name]["type"]
    return "?"


def _is_column_overridden(
    col_name: str,
    sources: list[str],
    column_overrides: dict[tuple[str, str], str],
) -> bool:
    """Whether the user has manually changed this column's type in any
    of the group's sources during this inspector session."""
    return any((src_name, col_name) in column_overrides for src_name in sources)


def _inspect_register_group(
    gid: str,
    groups: dict[str, RegisterGroup],
    register_per_source: dict[str, str | None],
    column_overrides: dict[tuple[str, str], str],
    config: dict,
    payload: dict,
) -> dict:
    """Inspect / edit a register group. Returns the (possibly rebuilt) config."""
    from .configure import build_config, resolve_register_to_id_and_name

    while True:
        grp = groups[gid]
        unique_sources = list(dict.fromkeys(grp.sources))
        n_unique = len(unique_sources)
        width = _terminal_width()
        print()
        _print_wrapped("Group: ", _format_source_list(unique_sources), width)
        print(f"  Register: {grp.register_str}")
        if grp.schema_variants > 1:
            print(
                f"  Sources: {n_unique} files in {grp.schema_variants} schema variants"
            )
        else:
            print(f"  Sources: {n_unique} file(s)")

        sections = _columns_by_availability(grp)
        # Flat ordered list of (col_name, current_type) so the user's
        # numeric choice indexes uniformly across all sections.
        type_rows: list[tuple[str, str]] = []

        total_cols = sum(len(cols) for _, cols in sections)
        n_idx_w = max(2, len(str(total_cols)))
        # Width fits "high_cardinality" (16) plus a trailing "*" marker
        # for manually-overridden rows.
        type_w = 17
        # Size the name column to actual content (longest column name + 1
        # space gap), not the full terminal width — otherwise short names
        # leave a giant whitespace gap that pushes long classification
        # tags off the right edge.
        all_col_names = [c for _, cols in sections for c in cols]
        longest_name = max((len(c) for c in all_col_names), default=12)
        prefix_w = 4 + 1 + n_idx_w + 2  # "    [NN] "
        name_w = max(12, min(longest_name + 1, max(12, width - prefix_w - type_w - 20)))
        # Regmeta column: "✓" for in-regmeta, classification short_name
        # when one exists. Header reads "regmeta" so the bare ✓ rows stay
        # interpretable.
        regmeta_w = max(
            [len("regmeta")] + [len(sn) for sn in grp.regmeta_tags.values() if sn]
        )

        # Header row: aligned with the data columns below it.
        header_prefix = " " * prefix_w
        header_name = "name".ljust(name_w)
        header_type = "type".ljust(type_w)
        print()
        print(f"{header_prefix}{header_name} {header_type} regmeta")

        for src_subset, col_names in sections:
            print()
            if len(src_subset) == n_unique:
                print(f"  Columns shared by all {n_unique} sources ({len(col_names)}):")
            else:
                suffix = (
                    f" ({len(src_subset)}/{n_unique} sources, {len(col_names)} cols):"
                )
                _print_wrapped(
                    "  Only in ",
                    _format_source_list(src_subset) + suffix,
                    width,
                )
            for col_name in col_names:
                t = _resolve_column_type(
                    col_name,
                    src_subset,
                    grp.columns_by_source,
                    column_overrides,
                    config,
                )
                # "*" marks rows whose type was changed manually in this
                # inspector session — distinguishes user judgement from
                # the auto-classifier's guess.
                t_disp = (
                    f"{t}*"
                    if _is_column_overridden(col_name, src_subset, column_overrides)
                    else t
                )
                if col_name in grp.regmeta_tags:
                    sn = grp.regmeta_tags[col_name]
                    regmeta_cell = sn if sn else "✓"
                else:
                    regmeta_cell = ""
                idx = len(type_rows) + 1
                name_disp = _truncate(col_name, name_w)
                line = (
                    f"    [{idx:>{n_idx_w}}] {name_disp:<{name_w}} "
                    f"{t_disp:<{type_w}} {regmeta_cell:<{regmeta_w}}"
                )
                print(line.rstrip())
                type_rows.append((col_name, t))

        print("\n  [r] change register / [number] change column type / [enter] back")
        # Only show the legend when at least one override exists in
        # this group — keeps the help line out of the way when nothing
        # is starred.
        if any(
            src_name in {s for s, _ in column_overrides} for src_name in grp.sources
        ):
            print("  ('*' after type = manually overridden in this session)")
        choice = _prompt("  Choice", default="").strip().lower()
        if choice == "":
            return config
        if choice == "r":
            new_reg = _prompt(
                "  New register (name or id; blank to skip regmeta for this group)"
            ).strip()
            if not new_reg:
                grp.register_id = None
                grp.register_name = None
                grp.register_str = "—"
                grp.confidence = "none"
                grp.regmeta_tags = {}
                for src_name in grp.sources:
                    register_per_source[src_name] = None
            else:
                resolved = resolve_register_to_id_and_name(new_reg)
                if resolved is None:
                    print(
                        f"  Register {new_reg!r} not found in regmeta.",
                        file=sys.stderr,
                    )
                    continue
                reg_id, reg_name = resolved
                grp.register_id = reg_id
                grp.register_name = reg_name
                grp.register_str = _register_display(reg_id, reg_name)
                # User-asserted register — coverage rate unknown until
                # next regmeta query; treat as "partial" until proven.
                grp.confidence = "partial"
                for src_name in grp.sources:
                    register_per_source[src_name] = reg_name
            try:
                config = build_config(payload, register_per_source=register_per_source)
            except Exception as exc:
                print(f"  Error rebuilding config: {exc}", file=sys.stderr)
                continue
            continue
        try:
            i = int(choice)
        except ValueError:
            print(f"  Unknown choice: {choice!r}", file=sys.stderr)
            continue
        if not 1 <= i <= len(type_rows):
            print(
                f"  Column number out of range (1..{len(type_rows)}).",
                file=sys.stderr,
            )
            continue
        col_name, current = type_rows[i - 1]
        new_raw = (
            _prompt(
                f"  New type for {col_name} (current: {current}) "
                "— [c]ategorical/[n]umeric/[d]ate/[i]d/[h]igh_cardinality, blank=keep"
            )
            .strip()
            .lower()
        )
        if not new_raw:
            continue
        new_type = _TYPE_KEY_MAP.get(new_raw, new_raw)
        if new_type not in _VALID_TYPES:
            print(f"  Unknown type {new_raw!r}.", file=sys.stderr)
            continue
        # Apply the override only to sources that actually have this
        # column — otherwise an unused override would dangle on a source
        # whose schema doesn't include it.
        for src_name in grp.sources:
            if any(c[0] == col_name for c in grp.columns_by_source.get(src_name, [])):
                column_overrides[(src_name, col_name)] = new_type


def _interview_register_groups(
    payload: dict,
    groups: dict[str, RegisterGroup],
    register_per_source: dict[str, str | None],
    column_overrides: dict[tuple[str, str], str],
    config: dict,
) -> dict:
    """Register-group review loop. Returns the (possibly rebuilt) config."""
    gids = _render_register_menu(groups)
    print("Press [enter] or [a] to accept all, [number] to inspect/edit, [q] to abort.")
    while True:
        choice = _prompt("Choice", default="").strip().lower()
        if choice in ("", "a"):
            return config
        if choice == "q":
            print("Aborted.", file=sys.stderr)
            raise SystemExit(1)
        try:
            idx = int(choice)
        except ValueError:
            print(f"  Unknown choice: {choice!r}", file=sys.stderr)
            continue
        if not 1 <= idx <= len(gids):
            print(f"  Group number out of range (1..{len(gids)}).", file=sys.stderr)
            continue
        gid = gids[idx - 1]
        config = _inspect_register_group(
            gid,
            groups,
            register_per_source,
            column_overrides,
            config,
            payload,
        )
        gids = _render_register_menu(groups)
        print(
            "Press [enter] or [a] to accept all, [number] to inspect/edit, "
            "[q] to abort."
        )


def _stage3_configure(cwd: Path, *, force: bool = False) -> int:
    import json as _json

    from regmeta.errors import RegmetaError

    from .configure import (
        _summary_counts,
        _validate_discover_payload,
        build_config,
        group_schema_families,
        guess_register_per_family,
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

    families = group_schema_families(payload)
    n_sources = len(payload["sources"])

    try:
        guesses = guess_register_per_family(families)
    except RegmetaError as exc:
        print(
            f"  (regmeta lookup failed: {exc.message}; "
            f"continuing without auto-classification)",
            file=sys.stderr,
        )
        from .classify import is_known_id
        from .configure import FamilyGuess

        guesses = {}
        for fid, sources in families.items():
            first = sources[0]
            cols = first.get("columns", [])
            nonid = [c["name"] for c in cols if not is_known_id(c["name"])]
            guesses[fid] = FamilyGuess(
                family_id=fid,
                sources=[s["source_name"] for s in sources],
                columns=[(c["name"], c.get("sql_type")) for c in cols],
                register_id=None,
                register_name=None,
                confidence="none",
                match_count=0,
                nonid_count=len(nonid),
            )

    groups = group_by_register(families, guesses)
    print(
        f"Auto-classifying {n_sources} source(s) into {len(groups)} register "
        f"group{'' if len(groups) == 1 else 's'}..."
    )

    register_per_source: dict[str, str | None] = {}
    for grp in groups.values():
        chosen = grp.register_name if grp.register_id else None
        for src_name in grp.sources:
            register_per_source[src_name] = chosen

    try:
        config = build_config(payload, register_per_source=register_per_source)
    except (ValueError, RegmetaError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    print()

    column_overrides: dict[tuple[str, str], str] = {}
    if not force:
        try:
            config = _interview_register_groups(
                payload,
                groups,
                register_per_source,
                column_overrides,
                config,
            )
        except SystemExit as exc:
            return int(exc.code or 1)

    for (source, col), new_type in column_overrides.items():
        if source in config["column_types"] and col in config["column_types"][source]:
            config["column_types"][source][col] = {"type": new_type}

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
