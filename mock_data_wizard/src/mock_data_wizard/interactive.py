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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from . import _bundle
from .configure import (
    CONFIG_FILENAME,
    Confidence,
    RegmetaSignal,
    regmeta_implied_type,
)
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
# Trailing 4-digit year. The negative lookbehind blocks a match when
# the 4 digits are preceded by another digit — that's how
# `..._20241231` (a YYYYMMDD date) avoids being parsed as year=1231,
# while still letting `Kursprov_gymn_HT2011` (no underscore separator
# but a non-digit boundary) match cleanly.
_YEAR_SUFFIX_RE = re.compile(r"^(.+?)(?<!\d)(\d{4})$")
# Column names that look like a panel time-key. Lowercase comparison.
_TIME_KEY_NAMES = frozenset({"ar", "indatum", "year", "period"})
# An opaque column with one of these suffixes is almost
# always a miscategorised code/type column. `_kod` and `_typ` are SCB
# conventions; trailing digits cover `Yrke3`, `SNI2007Niva3`, etc.
_AMBIGUOUS_SUFFIX_RE = re.compile(r"(_kod|_typ|\d+)$", re.IGNORECASE)
# Cap on per-column ambiguity prompts to avoid death-by-prompts on
# wide tables. The remainder is reported once at the end and left for
# the user to hand-edit.
_AMBIGUOUS_REVIEW_CAP = 10


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


def _longest_common_prefix(strs: list[str]) -> str:
    """Longest character-wise prefix across ``strs``.

    Used to suggest a ``panel_id`` from a panel's source names —
    ``[Kursprov_gymn_HT_2011, Kursprov_gymn_VT_2012]`` → ``Kursprov_gymn_``.
    """
    if not strs:
        return ""
    s_min = min(strs)
    s_max = max(strs)
    for i, c in enumerate(s_min):
        if c != s_max[i]:
            return s_min[:i]
    return s_min


def _suggest_panel_id(grp: "RegisterGroup", sources: list[str]) -> str:
    """Default panel_id for a register group's auto-detected panel.

    Prefers the longest common stem (year suffix stripped) across
    sources — ``Kursprov_gymn_HT_2011`` / ``Kursprov_gymn_VT_2012`` →
    ``Kursprov_gymn``. Falls back to the register name (the panel
    *is* the register, so it's a sensible label) and finally to the
    group_id when no name is available.
    """
    stems: list[str] = []
    for n in sources:
        m = _match_year_suffix(n)
        stems.append(m.group(1).rstrip("_-") if m else n)
    common = _longest_common_prefix(stems).rstrip("_-")
    if common:
        return common
    if grp.register_name:
        return grp.register_name
    return grp.group_id


def _build_panel_members(sources: list[str], years: dict[str, int]) -> list[dict]:
    """Build panel members with unique integer periods.

    When every source has a distinct year, ``period`` is the year
    itself — most readable. When sources share a year (e.g. HT/VT
    intra-year shards), disambiguate with an alphabetic rank within
    that year encoded as ``year * M + rank``, where ``M`` is the
    smallest power of ten that fits the densest year's shard count
    (so 100+ same-year shards never collide with the next year). Sorted
    ascending so the JSON reads chronologically.
    """
    if len(set(years.values())) == len(sources):
        members = [{"source": n, "period": years[n]} for n in sources]
    else:
        same_year: dict[int, list[str]] = {}
        for n in sources:
            same_year.setdefault(years[n], []).append(n)
        max_per_year = max(len(names) for names in same_year.values())
        multiplier = 100
        while multiplier <= max_per_year:
            multiplier *= 10
        rank: dict[str, int] = {}
        for _, names in same_year.items():
            for i, n in enumerate(sorted(names), start=1):
                rank[n] = i
        members = [
            {"source": n, "period": years[n] * multiplier + rank[n]} for n in sources
        ]
    members.sort(key=lambda d: d["period"])
    return members


def _find_time_key_in_source(src: dict) -> str | None:
    """Return the source's first ``AR``/``INDATUM``/``year``/``period``
    column name (matched case-insensitively), else ``None``."""
    for col in src.get("columns", []):
        name = col.get("name", "")
        if name.lower() in _TIME_KEY_NAMES:
            return name
    return None


def _shared_id_column(member_sources: list[dict]) -> str | None:
    """Best-guess id column shared by every member, else None.

    Uses ``is_known_id`` (the same name pattern the configurer uses) so
    the panel_key default lines up with whatever ``build_config`` would
    classify as ``id``. When several id columns are shared (e.g.
    ``LopNr`` + ``LopNr_PersonNr``), prefer the personnr-derived one —
    it's the actual person identifier, while ``LopNr`` is often a
    record-level surrogate that doesn't span the panel.
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
    if not shared:
        return None
    if len(shared) == 1:
        return next(iter(shared))
    # Prefer personnr-derived names. Order encodes preference: explicit
    # composite key first, then bare PersonNr/PersNr, then fallback to
    # an alphabetic pick so the result is deterministic across runs.
    for needle in ("lopnr_personnr", "lopnr_persnr", "personnr", "persnr", "personid"):
        for cand in shared:
            if cand.lower() == needle:
                return cand
    return sorted(shared)[0]


def _ambiguous_columns(payload: dict) -> list[tuple[str, str]]:
    """``[(source, column)]`` for ``opaque`` columns whose
    names suggest they might be categorical/numeric instead.
    """
    out: list[tuple[str, str]] = []
    for source, cols in payload.get("column_types", {}).items():
        for col, entry in cols.items():
            if entry.get("type") == "opaque" and _AMBIGUOUS_SUFFIX_RE.search(col):
                out.append((source, col))
    return out


def _interview_ambiguous(payload: dict) -> None:
    """Walk the user through suspicious ``opaque`` columns.

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
        f"\n{len(candidates)} `opaque` column(s) have suspicious "
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
        # anything else (incl. 'k') → keep as opaque
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
    "o": "opaque",
}
_VALID_TYPES = frozenset({"id", "categorical", "numeric", "opaque", "date"})

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


def _match_year_suffix(name: str) -> re.Match[str] | None:
    """Match ``<stem><year>`` against ``name``, falling back to stripped form.

    Raw name first preserves SQL table names like ``dbo.scb_rams_2018``
    where the dot is a schema separator; the stripped fallback handles
    filename sources like ``Äp9_2003.csv``.
    """
    return _YEAR_SUFFIX_RE.match(name) or _YEAR_SUFFIX_RE.match(_strip_extension(name))


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
        m = _match_year_suffix(s)
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
        # A lone `Foo_2024.csv` reads more honestly as the literal
        # filename than as `Foo_2024`, so only collapse when ≥2 years.
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


@dataclass
class PanelCandidate:
    """Auto-detected panel shape for a register group.

    ``members`` is a list of dicts, each with ``source`` and either
    ``period`` (a literal int — file-member, one period per source) or
    ``time_key`` (a column name — column-member, periods materialise at
    extract time). Members are sorted: file-members ascending by
    period, column-members at the end in source order.
    """

    members: list[dict] = field(default_factory=list)
    suggested_panel_id: str | None = None
    suggested_panel_key: str | None = None


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
    confidence: Confidence
    sources: list[str] = field(default_factory=list)
    columns_by_source: dict[str, list[tuple[str, str | None]]] = field(
        default_factory=dict
    )
    regmeta_signals: dict[str, RegmetaSignal] = field(default_factory=dict)
    schema_variants: int = 0
    panel_candidate: PanelCandidate | None = None

    @property
    def register_str(self) -> str:
        return _register_display(self.register_id, self.register_name)


def _worst_confidence(a: Confidence, b: Confidence) -> Confidence:
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
    sources_by_name: dict[str, dict] = {}
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
                confidence=guess.confidence,
            )
        grp = groups[key]
        grp.schema_variants += 1
        grp.confidence = _worst_confidence(grp.confidence, guess.confidence)
        # Merge regmeta signals across schema variants of the same
        # register. The first non-None classification short_name wins
        # (in practice this rarely matters: schemas drift year to year
        # but a variable's classification doesn't).
        for col_name, sig in guess.regmeta_signals.items():
            existing = grp.regmeta_signals.get(col_name)
            if existing is None or (
                existing.classification_short_name is None
                and sig.classification_short_name is not None
            ):
                grp.regmeta_signals[col_name] = sig
        for src in family_sources:
            name = src["source_name"]
            grp.sources.append(name)
            grp.columns_by_source[name] = [
                (c["name"], c.get("sql_type")) for c in src.get("columns", [])
            ]
            sources_by_name[name] = src
    for grp in groups.values():
        grp.panel_candidate = _detect_panel_candidate(grp, sources_by_name)
    return groups


def _detect_panel_candidate(
    grp: RegisterGroup,
    sources_by_name: dict[str, dict],
) -> PanelCandidate | None:
    """Return the panel shape implied by the group's sources, or None.

    Two member-emission paths:

    - Multi-source group: every source must match ``...<YYYY>``. Each
      source becomes a file-member with the parsed year as ``period``.
      The year suffix is the gate that distinguishes annual snapshots
      of one register from sibling tables of one register (e.g.
      RTB main/address/kommun, no year tags). Schema drift across
      periods is tolerated — the group already merges schema variants.
    - Singleton group: the lone source must carry an
      ``AR`` / ``INDATUM`` / ``year`` / ``period`` column; it becomes
      a column-member with that column as ``time_key``.

    Anything else (no year suffix, no time key) returns None — the
    user can still hand-edit ``panels`` in the JSON.
    """
    unique_sources = list(dict.fromkeys(grp.sources))
    if len(unique_sources) >= 2:
        years_per_source: dict[str, int] = {}
        for name in unique_sources:
            m = _match_year_suffix(name)
            if not m:
                return None
            years_per_source[name] = int(m.group(2))
        member_srcs = [sources_by_name[name] for name in unique_sources]
        suggested_key = _shared_id_column(member_srcs)
        members = _build_panel_members(unique_sources, years_per_source)
        return PanelCandidate(
            members=members,
            suggested_panel_id=_suggest_panel_id(grp, unique_sources),
            suggested_panel_key=suggested_key,
        )
    if len(unique_sources) == 1:
        from .classify import is_known_id

        name = unique_sources[0]
        src = sources_by_name.get(name)
        if src is None:
            return None
        time_key = _find_time_key_in_source(src)
        if time_key is None:
            return None
        ids = [c["name"] for c in src.get("columns", []) if is_known_id(c["name"])]
        suggested_key = ids[0] if len(ids) == 1 else None
        return PanelCandidate(
            members=[{"source": name, "time_key": time_key}],
            suggested_panel_id=name,
            suggested_panel_key=suggested_key,
        )
    return None


def _summarise_panel_members(members: list[dict]) -> str:
    """Short one-liner describing a member list.

    File-members surface as ``N periods YYYY–YYYY``; column-members
    surface as ``N column-members on <src>.<time_key>``. Mixed panels
    show both halves comma-joined.
    """
    file_periods = [m["period"] for m in members if m.get("period") is not None]
    col_members = [m for m in members if m.get("time_key") is not None]
    parts: list[str] = []
    if file_periods:
        ps = sorted(file_periods)
        if len(ps) == 1:
            parts.append(f"1 period {ps[0]}")
        else:
            parts.append(f"{len(ps)} periods {ps[0]}–{ps[-1]}")
    if col_members:
        if len(col_members) == 1:
            cm = col_members[0]
            parts.append(f"on {cm['source']}.{cm['time_key']}")
        else:
            parts.append(f"{len(col_members)} time_key sources")
    return ", ".join(parts) or f"{len(members)} members"


def _render_panel_line(grp: RegisterGroup, configured: dict | None) -> str | None:
    """One-line panel summary for the inspector header.

    States:

    - configured: ``panel: <id> by <key> (...)``
    - candidate w/ unambiguous suggested key, not configured (e.g. user
      removed an auto-applied panel): show the suggested key
    - candidate w/ no suggested key: ``panel: candidate (..., no panel_key set)``
    - no candidate: ``None`` (line omitted)
    """
    if configured is not None:
        pid = configured.get("panel_id", "?")
        pkey = configured.get("panel_key", "?")
        members = configured.get("members", [])
        return (
            f"panel: {pid} by {pkey} ({_summarise_panel_members(members)})  [p to edit]"
        )
    cand = grp.panel_candidate
    if cand is None:
        return None
    summary = _summarise_panel_members(cand.members)
    if cand.suggested_panel_key:
        return (
            f"panel: candidate ({summary}, suggested panel_key="
            f"{cand.suggested_panel_key})  [p to configure]"
        )
    return f"panel: candidate ({summary}, no panel_key set)  [p to configure]"


def _auto_apply_panel_candidates(
    groups: dict[str, RegisterGroup],
) -> dict[str, dict]:
    """Pre-populate panels for groups whose candidate has an unambiguous key.

    Mirrors how column types are auto-classified: the wizard makes the
    obvious call up front and the user overrides via the inspector.
    Auto-apply requires ``suggested_panel_key`` to be set — for
    file-member groups that means a single shared id-typed column
    across all members; for a column-member singleton it means exactly
    one id column on the source. Anything else stays as a "candidate"
    hint until the user hits ``[p]`` to pick the key explicitly.

    Skips candidates whose ``panel_id``, ``panel_key``, or member source
    would collide with an already-applied panel — ``parse_config``
    rejects such configs, so silently writing them out would surface as
    a downstream extract failure. The colliding group keeps its
    ``panel_candidate`` and the user can resolve it via ``[p]``.
    """
    out: dict[str, dict] = {}
    seen_ids: set[str] = set()
    seen_keys: set[str] = set()
    seen_sources: set[str] = set()
    for gid, grp in groups.items():
        cand = grp.panel_candidate
        if cand is None or cand.suggested_panel_key is None:
            continue
        panel_id = cand.suggested_panel_id or gid
        panel_key = cand.suggested_panel_key
        member_sources = [m["source"] for m in cand.members]
        if (
            panel_id in seen_ids
            or panel_key in seen_keys
            or any(s in seen_sources for s in member_sources)
        ):
            continue
        out[gid] = {
            "panel_id": panel_id,
            "panel_key": panel_key,
            "members": cand.members,
        }
        seen_ids.add(panel_id)
        seen_keys.add(panel_key)
        seen_sources.update(member_sources)
    return out


def _apply_panels_to_config(config: dict, panels_by_gid: dict[str, dict]) -> None:
    """Mirror ``panels_by_gid`` onto ``config['panels']``.

    Drops the key entirely when empty so the written JSON stays minimal —
    ``parse_config`` treats missing/empty equivalently, but a stray
    ``"panels": []`` would mislead readers into thinking the user
    deliberately authored an empty panel list.
    """
    if panels_by_gid:
        config["panels"] = list(panels_by_gid.values())
    else:
        config.pop("panels", None)


def _format_member(m: dict) -> str:
    """Compact per-member label for the inspector member picker."""
    if m.get("period") is not None:
        return f"{m['source']} (period={m['period']})"
    return f"{m['source']} (time_key={m['time_key']})"


def _toggle_panel_members(
    panel_members: list[dict], grp: RegisterGroup
) -> list[dict] | None:
    """Interactive add/remove member loop. Returns ``None`` on cancel.

    Sources currently in the panel are listed first with ``-`` to
    remove. Group sources outside the panel are listed next with ``+``
    to add. Adding a source asks whether to attach it via period
    (year-suffix derived) or time_key (column name).
    """
    members = [dict(m) for m in panel_members]
    while True:
        print()
        print("  Members:")
        in_panel_sources = {m["source"] for m in members}
        for i, m in enumerate(members, 1):
            print(f"    [- {i}] {_format_member(m)}")
        unique_sources = list(dict.fromkeys(grp.sources))
        outside = [s for s in unique_sources if s not in in_panel_sources]
        for j, s in enumerate(outside, len(members) + 1):
            print(f"    [+ {j}] {s}")
        if not members and not outside:
            print("    (no group sources)")
        choice = _prompt("  Toggle by number, [enter] to finish", default="").strip()
        if not choice:
            return members
        try:
            idx = int(choice)
        except ValueError:
            print(f"  Unknown choice {choice!r}.", file=sys.stderr)
            continue
        if 1 <= idx <= len(members):
            members.pop(idx - 1)
            continue
        out_idx = idx - len(members) - 1
        if not 0 <= out_idx < len(outside):
            print(f"  Number out of range: {idx}", file=sys.stderr)
            continue
        new_src = outside[out_idx]
        new_member = _prompt_member_kind(new_src, grp)
        if new_member is not None:
            members.append(new_member)


def _prompt_member_kind(src: str, grp: RegisterGroup) -> dict | None:
    """Ask whether the new member is a file-member or column-member.

    File-members default the period from the year-suffix in the source
    name. Column-members default the time_key from any AR / INDATUM /
    year / period column on the source.
    """
    cols = [c for c, _ in grp.columns_by_source.get(src, [])]
    time_key_default: str | None = next(
        (c for c in cols if c.lower() in _TIME_KEY_NAMES),
        None,
    )
    year_match = _match_year_suffix(src)
    period_default = int(year_match.group(2)) if year_match else None
    options = []
    if period_default is not None:
        options.append("p")
    if time_key_default is not None:
        options.append("t")
    if not options:
        # Neither auto-default; prompt for explicit period int or
        # time_key column name.
        print(
            f"  No year suffix in {src!r} and no AR/INDATUM/year/period column — "
            "supply a period or time_key explicitly."
        )
        options = ["p", "t"]
    kinds = "/".join(f"[{o}]" for o in options)
    raw = (
        _prompt(f"  Add {src!r} as {kinds}: [p]eriod / [t]ime_key", default=options[0])
        .strip()
        .lower()
    )
    if raw in ("p", "period"):
        if period_default is not None:
            period_str = _prompt(
                "    period", default=str(period_default)
            ).strip() or str(period_default)
        else:
            period_str = _prompt("    period").strip()
        try:
            period = int(period_str)
        except ValueError:
            print(f"  Invalid period {period_str!r}.", file=sys.stderr)
            return None
        return {"source": src, "period": period}
    if raw in ("t", "time_key"):
        if time_key_default is not None:
            tk = (
                _prompt("    time_key", default=time_key_default).strip()
                or time_key_default
            )
        else:
            tk = _prompt("    time_key (column name on this source)").strip()
        if not tk:
            print("  No time_key supplied; skipping.", file=sys.stderr)
            return None
        return {"source": src, "time_key": tk}
    print(f"  Unknown choice {raw!r}.", file=sys.stderr)
    return None


def _edit_panel_for_group(
    gid: str, grp: RegisterGroup, panels_by_gid: dict[str, dict]
) -> None:
    """Run the [p] sub-prompt for one group, mutating ``panels_by_gid``.

    Three branches:

    - Existing panel: ``Keep this panel? [Y/n]`` → ``n`` removes it,
      ``y`` lets the user re-edit panel_key and members.
    - No existing, has candidate: ``Is this a panel? [Y/n]`` → ``y``
      configures from the candidate (with member-edit pass).
    - No existing, no candidate: lets the user start with an empty
      member list and add from group sources.
    """
    cand = grp.panel_candidate
    existing = panels_by_gid.get(gid)

    if existing is not None:
        if not _yes_no("  Keep this panel?", default=True):
            panels_by_gid.pop(gid, None)
            return
        members: list[dict] = [dict(m) for m in existing.get("members", [])]
        default_key = existing["panel_key"]
        panel_id = existing["panel_id"]
    else:
        if cand is not None:
            if not _yes_no("  Is this a panel?", default=True):
                return
            members = [dict(m) for m in cand.members]
            default_key = cand.suggested_panel_key
            panel_id = cand.suggested_panel_id or gid
        else:
            if not _yes_no("  Set up a panel for this group?", default=False):
                return
            members = []
            default_key = None
            panel_id = grp.register_name or gid

    edited = _toggle_panel_members(members, grp)
    if edited is None or not edited:
        if existing is not None:
            panels_by_gid.pop(gid, None)
        return

    # panel_id is auto-derived; user edits the JSON to rename. The only
    # meaningful question on the inspector path is panel_key.
    if default_key:
        panel_key = _prompt("  panel_key", default=default_key).strip() or default_key
    else:
        panel_key = _prompt("  panel_key").strip()
        if not panel_key:
            print("  Skipping panel (no panel_key supplied).", file=sys.stderr)
            return
    panels_by_gid[gid] = {
        "panel_id": panel_id,
        "panel_key": panel_key,
        "members": edited,
    }


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


def _render_register_menu(
    groups: dict[str, RegisterGroup],
    panels_by_gid: dict[str, dict],
) -> list[str]:
    """Print the register-group menu. Returns group ids in display order.

    The trailing column is a panel indicator (``✓ panel`` when one is
    configured; ``⚠ candidate`` when the auto-detector found a shape
    but couldn't pick a panel_key — i.e. genuinely ambiguous and needs
    user input; blank otherwise) — replaces the earlier source / column
    / schema counts since per-group counts are already visible in the
    inspector header. A candidate with an unambiguous suggested key
    that is *not* configured (e.g. user explicitly removed it) leaves
    the column blank rather than nagging.
    """
    gids = list(groups.keys())
    width = _terminal_width()

    n_idx_w = max(2, len(str(len(gids))))
    fixed_left = 2 + 1 + n_idx_w + 2 + 1
    fixed_glyph = 3
    panel_w = len("⚠ candidate")
    overhead = fixed_left + fixed_glyph + panel_w + 2

    budget = max(width - overhead, _MIN_RENDER_WIDTH - overhead)
    label_w = max(16, int(budget * 0.40))
    register_w = max(20, budget - label_w)
    cont_indent = " " * (2 + 1 + n_idx_w + 2 + label_w + 1 + 2)

    print("Register groups:\n")
    for i, gid in enumerate(gids, 1):
        grp = groups[gid]
        glyph = _CONFIDENCE_GLYPH[grp.confidence]
        unique_sources = list(dict.fromkeys(grp.sources))
        if gid in panels_by_gid:
            panel_part = "✓ panel"
        elif (
            grp.panel_candidate is not None
            and grp.panel_candidate.suggested_panel_key is None
        ):
            panel_part = "⚠ candidate"
        else:
            panel_part = ""
        label = _truncate(_format_source_list(unique_sources), label_w)
        reg_chunks = textwrap.wrap(grp.register_str, width=register_w) or [""]
        first_line = (
            f"  [{i:>{n_idx_w}}] {label:<{label_w}} {glyph} "
            f"{reg_chunks[0]:<{register_w}} {panel_part}"
        )
        print(first_line.rstrip())
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


def _regmeta_cell(
    signal: RegmetaSignal | None,
    effective_type: str,
    is_overridden: bool,
) -> str:
    """Render the regmeta column for one row in the inspector.

    - Empty when regmeta has no entry for the column.
    - Plain ✓ or classification short_name when regmeta agrees (or has
      no opinion) with the column's effective type.
    - ⚠-prefixed when the user's manual override contradicts what
      regmeta implies. The short_name (or bare ⚠) lets the user see
      both the warning and what regmeta actually said about the column.
    """
    if signal is None:
        return ""
    label = signal.classification_short_name or "✓"
    if not is_overridden:
        return label
    implied = regmeta_implied_type(signal)
    if implied is None or implied == effective_type:
        return label
    return (
        f"⚠ {signal.classification_short_name}"
        if signal.classification_short_name
        else "⚠"
    )


def _collect_precomputed_signals(
    groups: dict[str, "RegisterGroup"],
) -> dict[str, dict[str, RegmetaSignal]]:
    """Index already-fetched regmeta signals by the register string
    that ``build_config`` will look them up under.

    Outer key matches whatever ``register_per_source`` carries for the
    group's sources — preferring the registernamn, falling back to a
    stringified register_id when the name lookup didn't populate. Inner
    key is lowercased to match ``_regmeta_lookup``'s output convention
    (``RegisterGroup.regmeta_signals`` uses original-case keys for
    inspector display).

    Multiple groups can share a register key (e.g. after a user
    re-points one group to match another). Their per-column signals
    are merged so ``build_config``'s cache short-circuit doesn't drop
    columns that only one of the groups had fetched evidence for.
    """
    out: dict[str, dict[str, RegmetaSignal]] = {}
    for grp in groups.values():
        if grp.register_id is None:
            continue
        key = grp.register_name or str(grp.register_id)
        bucket = out.setdefault(key, {})
        for n, sig in grp.regmeta_signals.items():
            bucket[n.lower()] = sig
    return out


def _refresh_regmeta_signals(grp: "RegisterGroup", reg_id: int) -> None:
    """Re-query regmeta for the group's columns under ``reg_id`` and
    overwrite ``grp.regmeta_signals`` with the result.

    Called after the user changes a group's register so the inspector
    reflects coverage under the *new* register rather than stale signals
    from the auto-guess.
    """
    from regmeta import open_db
    from regmeta.db import db_path_from_args

    from ._util import lookup_with_prefix_fallback
    from .configure import _regmeta_lookup

    col_names: set[str] = set()
    for cols in grp.columns_by_source.values():
        for name, _sql in cols:
            col_names.add(name)
    if not col_names:
        grp.regmeta_signals = {}
        return

    conn = open_db(db_path_from_args(None))
    try:
        signals = _regmeta_lookup(conn, col_names, [reg_id])
    finally:
        conn.close()

    refreshed: dict[str, RegmetaSignal] = {}
    for name in col_names:
        sig = lookup_with_prefix_fallback(signals, name)
        if sig is not None:
            refreshed[name] = sig
    grp.regmeta_signals = refreshed


def _inspect_register_group(
    gid: str,
    groups: dict[str, RegisterGroup],
    register_per_source: dict[str, str | None],
    column_overrides: dict[tuple[str, str], str],
    panels_by_gid: dict[str, dict],
    config: dict,
    payload: dict,
) -> dict:
    """Inspect / edit a register group. Returns the (possibly rebuilt) config."""
    from regmeta.errors import RegmetaError

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
        panel_line = _render_panel_line(grp, panels_by_gid.get(gid))
        if panel_line is not None:
            print(f"  {panel_line}")

        sections = _columns_by_availability(grp)
        # Flat ordered list of (col_name, current_type) so the user's
        # numeric choice indexes uniformly across all sections.
        type_rows: list[tuple[str, str]] = []

        total_cols = sum(len(cols) for _, cols in sections)
        n_idx_w = max(2, len(str(total_cols)))
        # Width fits "categorical" (11, the longest type name) plus a
        # trailing "*" marker for manually-overridden rows.
        type_w = 12
        # Size the name column to actual content (longest column name + 1
        # space gap), not the full terminal width — otherwise short names
        # leave a giant whitespace gap that pushes long classification
        # tags off the right edge.
        all_col_names = [c for _, cols in sections for c in cols]
        longest_name = max((len(c) for c in all_col_names), default=12)
        prefix_w = 4 + 1 + n_idx_w + 2  # "    [NN] "
        name_w = max(12, min(longest_name + 1, width - prefix_w - type_w - 20))
        # Regmeta column: classification short_name when one exists,
        # bare ✓ when regmeta knows the column with no classification,
        # ⚠ (or "⚠ short_name") when the manually-overridden type
        # contradicts what regmeta implies. Header reads "regmeta" so
        # the bare ✓ rows stay interpretable.
        regmeta_w = max(
            [len("regmeta")]
            + [
                # 2 = "⚠ " prefix; reserve enough space even if no
                # conflict actually fires this render.
                len(sig.classification_short_name or "✓") + 2
                for sig in grp.regmeta_signals.values()
            ]
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
                is_overridden = _is_column_overridden(
                    col_name, src_subset, column_overrides
                )
                t_disp = f"{t}*" if is_overridden else t
                regmeta_cell = _regmeta_cell(
                    grp.regmeta_signals.get(col_name), t, is_overridden
                )
                idx = len(type_rows) + 1
                name_disp = _truncate(col_name, name_w)
                line = (
                    f"    [{idx:>{n_idx_w}}] {name_disp:<{name_w}} "
                    f"{t_disp:<{type_w}} {regmeta_cell:<{regmeta_w}}"
                )
                print(line.rstrip())
                type_rows.append((col_name, t))

        print(
            "\n  [r] change register / [p] panel / [number] change column type / "
            "[enter] back"
        )
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
        if choice == "p":
            _edit_panel_for_group(gid, grp, panels_by_gid)
            _apply_panels_to_config(config, panels_by_gid)
            continue
        if choice == "r":
            new_reg = _prompt(
                "  New register (name or id; blank to skip regmeta for this group)"
            ).strip()
            if not new_reg:
                grp.register_id = None
                grp.register_name = None
                grp.confidence = "none"
                grp.regmeta_signals = {}
                for src_name in grp.sources:
                    register_per_source[src_name] = None
            else:
                try:
                    resolved = resolve_register_to_id_and_name(new_reg)
                except ValueError as exc:
                    print(f"  {exc}", file=sys.stderr)
                    continue
                if resolved is None:
                    print(
                        f"  Register {new_reg!r} not found in regmeta.",
                        file=sys.stderr,
                    )
                    continue
                reg_id, reg_name = resolved
                grp.register_id = reg_id
                grp.register_name = reg_name
                # User-asserted register — coverage rate unknown until
                # next regmeta query; treat as "partial" until proven.
                grp.confidence = "partial"
                for src_name in grp.sources:
                    register_per_source[src_name] = reg_name or str(reg_id)
                # Refresh regmeta signals so the inspector reflects
                # coverage under the new register, not the auto-guess.
                try:
                    _refresh_regmeta_signals(grp, reg_id)
                except RegmetaError as exc:
                    print(
                        f"  Warning: could not refresh regmeta signals: {exc}",
                        file=sys.stderr,
                    )
            try:
                config = build_config(
                    payload,
                    register_per_source=register_per_source,
                    precomputed_signals=_collect_precomputed_signals(groups),
                )
            except (ValueError, RegmetaError) as exc:
                print(f"  Error rebuilding config: {exc}", file=sys.stderr)
                continue
            # build_config rewrites the dict from scratch — restore the
            # user's in-progress panel edits.
            _apply_panels_to_config(config, panels_by_gid)
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
                "— [c]ategorical/[n]umeric/[d]ate/[i]d/[o]paque, blank=keep"
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
    panels_by_gid: dict[str, dict],
    config: dict,
) -> dict:
    """Register-group review loop. Returns the (possibly rebuilt) config."""
    gids = _render_register_menu(groups, panels_by_gid)
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
            panels_by_gid,
            config,
            payload,
        )
        gids = _render_register_menu(groups, panels_by_gid)
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
        # Prefer the registernamn so the config carries a human-readable
        # register tag; fall back to the numeric id when the name lookup
        # didn't populate (e.g. the register table query returned no row).
        if grp.register_id is None:
            chosen: str | None = None
        else:
            chosen = grp.register_name or str(grp.register_id)
        for src_name in grp.sources:
            register_per_source[src_name] = chosen

    try:
        config = build_config(
            payload,
            register_per_source=register_per_source,
            precomputed_signals=_collect_precomputed_signals(groups),
        )
    except (ValueError, RegmetaError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    print()

    column_overrides: dict[tuple[str, str], str] = {}
    panels_by_gid: dict[str, dict] = _auto_apply_panel_candidates(groups)
    _apply_panels_to_config(config, panels_by_gid)
    if panels_by_gid:
        print(
            f"Auto-detected {len(panels_by_gid)} panel(s); inspect a group "
            "and press [p] to edit or remove."
        )
    if not force:
        try:
            config = _interview_register_groups(
                payload,
                groups,
                register_per_source,
                column_overrides,
                panels_by_gid,
                config,
            )
        except SystemExit as exc:
            return int(exc.code or 1)

    for (source, col), new_type in column_overrides.items():
        if source in config["column_types"] and col in config["column_types"][source]:
            config["column_types"][source][col] = {"type": new_type}

    _apply_panels_to_config(config, panels_by_gid)

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
