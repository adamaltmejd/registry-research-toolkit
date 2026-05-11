"""Panel-shape detection and year/date-token helpers.

Pure functions used by the editor (and by ``extract.py`` for year
inference). The editor calls these to surface panel candidates per
register-group via ``RegisterGroupView.panel_candidate``; the user
either accepts via the UI or hand-edits the resulting JSON.

Two member kinds:

* **file-member** — one source contributes a single period; period is
  encoded from the date token in the source name.
* **column-member** — one source carries a time-key column whose values
  are the period; periods materialise at extract time.

Why the date logic is so finicky: SCB delivery shapes vary wildly.
Annual snapshots use trailing year (``Individ_2018.csv``); embedded
date stamps use ``YYYYMM`` (``Arb_AGIIndivid201907_Def``); within-year
shards use term tags (HT/VT/Q1–Q4). The regex tries to recognise all
three without misreading a YYYYMMDD timestamp as ``year=2024,month=12``
or eating extra characters from the suffix.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal

from .classify import is_known_id

# -- Year + date tokens ----------------------------------------------------

# Naive 4-digit year search. Matches `regmeta.queries.extract_year` so
# discover-time year detection on MONA agrees with local resolution.
_YEAR_RE = re.compile(r"\d{4}")


def detect_year_from_source_name(source_name: str) -> int | None:
    """Return the first 4-digit year embedded in ``source_name``, or None.

    Naive: returns the first 4-digit run regardless of context. Editor
    callers can override via the per-source ``year`` field; callers that
    need richer date-token semantics (panel detection) use
    ``_match_date_token`` instead.
    """
    m = _YEAR_RE.search(source_name)
    return int(m.group(0)) if m else None


# Locates a date token (YYYY, YYYYMM, YYYY[_-]MM) anywhere in a name.
# Stem (variable, non-greedy) and suffix (constant across siblings)
# bracket the date. Negative lookbehind/lookahead block the digit
# before/after the token from being eaten — that's how `_20241231`
# (a YYYYMMDD stamp) avoids being parsed as year=1231 or
# year=2024+month=12+day=31, since the trailing `31` would force the
# suffix to start with a digit.
_DATE_TOKEN_RE = re.compile(
    r"""^
    (?P<stem>.+?)
    (?<!\d)
    (?P<year>\d{4})
    (?:(?P<sep>[_-])?(?P<month>0[1-9]|1[0-2]))?
    (?P<suffix>(?!\d).*)
    $""",
    re.VERBOSE,
)
# Intra-year shard tags → first month of the term (1–12). Used as a
# fallback month source when a filename carries a term marker rather
# than an explicit YYYYMM. VT (vårtermin/spring) → January; HT
# (hösttermin/autumn) → August. Q1–Q4 follow calendar quarters.
_INTRA_YEAR_TAG_MONTH = {
    "VT": 1,
    "HT": 8,
    "Q1": 1,
    "Q2": 4,
    "Q3": 7,
    "Q4": 10,
}
_TAG_TOKEN_SPLIT_RE = re.compile(r"[_\-]")
# Column names that look like a panel time-key. Lowercase comparison.
_TIME_KEY_NAMES = frozenset({"ar", "indatum", "year", "period"})


def _strip_extension(name: str) -> str:
    """Strip a single trailing dot-extension (``foo.csv`` → ``foo``)."""
    return name.rsplit(".", 1)[0] if "." in name else name


@dataclass(frozen=True, slots=True)
class _DateToken:
    """A date marker located within a filename.

    ``stem`` and ``suffix`` retain the original separators; ``shape_key``
    derives a separator-tolerant identity so siblings that differ only
    in date can be recognised as one panel even if one writes
    ``foo_2019_def`` and another ``foo-2020-def``.
    """

    stem: str
    year: int
    month: int | None
    suffix: str

    @property
    def shape_key(self) -> tuple[str, str]:
        """Identity used to decide whether two tokens belong to the same panel.

        Strips a recognised intra-year tag (HT/VT/Q1–Q4) from the trailing
        position of the stem so ``..._HT_2011`` and ``..._VT_2012`` share
        a key — they differ only in the tag, which encodes the period,
        not the panel identity.
        """
        stem = self.stem.rstrip("_-")
        if stem:
            last = _TAG_TOKEN_SPLIT_RE.split(stem)[-1].upper()
            if last in _INTRA_YEAR_TAG_MONTH:
                stem = stem[: -len(last)].rstrip("_-")
        return (stem, self.suffix.strip("_-"))


def _match_date_token(name: str) -> _DateToken | None:
    """Locate a date token in ``name``.

    Tries the extension-stripped form first so filename suffixes like
    ``.csv`` don't leak into the captured ``suffix`` group. Falls back
    to the raw name for SQL-table sources like ``dbo.scb_rams_2018``,
    where ``_strip_extension`` would over-shorten the input to ``dbo``.
    Returns ``None`` when no valid date token is present (e.g. a
    YYYYMMDD timestamp where the trailing day digits force the
    suffix-starts-with-non-digit lookahead to fail).
    """
    for s in (_strip_extension(name), name):
        m = _DATE_TOKEN_RE.match(s)
        if m:
            return _DateToken(
                stem=m["stem"],
                year=int(m["year"]),
                month=int(m["month"]) if m["month"] else None,
                suffix=m["suffix"],
            )
    return None


def _resolve_period_month(token: _DateToken) -> int | None:
    """Return the month-of-term-start for ``token``, or ``None``.

    Prefers an explicit YYYYMM month from the filename, then falls back
    to a recognised HT/VT/Q1–Q4 tag at the trailing ``_-``-separated
    token of the stem.
    """
    if token.month is not None:
        return token.month
    rstripped = token.stem.rstrip("_-")
    if not rstripped:
        return None
    last = _TAG_TOKEN_SPLIT_RE.split(rstripped)[-1].upper()
    return _INTRA_YEAR_TAG_MONTH.get(last)


# -- Panel id / key suggestion --------------------------------------------


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


def suggest_panel_id(
    sources: list[str], *, register_name: str | None = None, fallback: str = ""
) -> str:
    """Default panel_id for a multi-source panel.

    Prefers the longest common stem (date token stripped) across
    sources — ``Kursprov_gymn_HT_2011`` / ``Kursprov_gymn_VT_2012`` →
    ``Kursprov_gymn``. Appends a constant suffix when present so
    embedded-date families like ``Arb_AGIIndivid201907_Def`` /
    ``Arb_AGIIndivid202302_Def`` collapse to ``Arb_AGIIndivid_Def``.
    Falls back to the register name and then to ``fallback``.
    """
    stems: list[str] = []
    suffixes: list[str] = []
    for n in sources:
        token = _match_date_token(n)
        if token is not None:
            stems.append(token.stem.rstrip("_-"))
            suffixes.append(token.suffix.strip("_-"))
        else:
            stems.append(n)
            suffixes.append("")
    common_stem = _longest_common_prefix(stems).rstrip("_-")
    common_suffix = suffixes[0] if len(set(suffixes)) == 1 else ""
    parts = [p for p in (common_stem, common_suffix) if p]
    if parts:
        return "_".join(parts)
    if register_name:
        return register_name
    return fallback


def _shared_id_column(member_sources: list[dict]) -> str | None:
    """Best-guess id column shared by every member, else None.

    Uses ``is_known_id`` (the same name pattern the classifier uses) so
    the ``entity_key`` default lines up with whatever the classifier
    would assign as ``id``. When several id columns are shared (e.g.
    ``LopNr`` + ``LopNr_PersonNr``), prefer the personnr-derived one —
    it's the actual person identifier, while ``LopNr`` is often a
    record-level surrogate that doesn't span the panel.
    """
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
    for needle in (
        "lopnr_personnr",
        "lopnr_persnr",
        "personnr",
        "persnr",
        "personid",
    ):
        for cand in shared:
            if cand.lower() == needle:
                return cand
    return sorted(shared)[0]


def _build_panel_members(
    sources: list[str],
    years: dict[str, int],
    months: dict[str, int | None],
) -> list[dict]:
    """Build panel members with unique integer ``time_key`` literals.

    Three encoding strategies, in order of preference:

    1. **Year-month** — when every source has a month (explicit YYYYMM
       or a recognised intra-year tag like HT/VT/Q1–Q4), encode
       ``time_key = year * 100 + month``. Reads chronologically
       (VT2012=201201 < HT2012=201208). Falls through to (3) only if
       encodings collide (e.g. two ``HT_2012`` shards).
    2. **Year-as-period** — when every source has a distinct year and
       no month information, ``time_key`` is the year itself.
    3. **Alphabetic-rank fallback** — same-year siblings without a
       month get ``year * M + rank``, where ``M`` is the smallest
       power of ten that fits the densest year's shard count (so 100+
       same-year shards never collide with the next year).

    Members are sorted by ``time_key`` so the JSON reads chronologically.
    """
    if all(months.get(n) is not None for n in sources):
        encoded = {n: years[n] * 100 + months[n] for n in sources}  # type: ignore[operator]
        if len(set(encoded.values())) == len(sources):
            members = [{"source": n, "time_key": encoded[n]} for n in sources]
            members.sort(key=lambda d: d["time_key"])
            return members
    if len(set(years.values())) == len(sources):
        members = [{"source": n, "time_key": years[n]} for n in sources]
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
            {"source": n, "time_key": years[n] * multiplier + rank[n]} for n in sources
        ]
    members.sort(key=lambda d: d["time_key"])
    return members


# -- Time-key detection ---------------------------------------------------


def _find_time_key_in_source(columns: list[dict]) -> str | None:
    """Return the first ``AR``/``INDATUM``/``year``/``period`` column
    name (case-insensitive) from ``columns``, else None."""
    for col in columns:
        name = col.get("name", "")
        if name.lower() in _TIME_KEY_NAMES:
            return name
    return None


# -- Per-source panel-member suggestion -----------------------------------


@dataclass(frozen=True, slots=True)
class PanelMemberHints:
    """Independent per-source seeds used by the manual panel editor.

    Unlike ``PanelMemberSuggestion`` (which picks one shape, file beats
    column), both hints are reported so the editor can pre-fill the
    "other mode" field as a convenience when the user switches modes.

    The editor was previously reimplementing this detection client-side
    against a naïve ``\\d{4}`` regex — wrong for HT/VT/Q tags and
    embedded ``YYYYMM`` tokens. Shipping the hints from the server
    eliminates the duplication and the divergence.
    """

    year_from_name: int | None
    time_key_column: str | None


def detect_panel_member_hints(
    source_name: str, columns: tuple[str, ...]
) -> PanelMemberHints:
    """Return the per-source seeds for the manual panel editor.

    ``year_from_name`` is the date-token year (or ``year*100+month``
    when a month is present) extracted by ``_match_date_token``.
    ``time_key_column`` is the first recognised time-key column name.
    Both signals are computed independently; the editor decides which
    to surface.
    """
    token = _match_date_token(source_name)
    if token is not None:
        month = _resolve_period_month(token)
        year_from_name = token.year * 100 + month if month is not None else token.year
    else:
        year_from_name = None
    time_key_column = _find_time_key_in_source([{"name": c} for c in columns])
    return PanelMemberHints(
        year_from_name=year_from_name, time_key_column=time_key_column
    )


PanelMemberKind = Literal["file", "column"]


@dataclass(frozen=True, slots=True)
class PanelMemberSuggestion:
    """Single-source panel-member shape inferred from a name + columns.

    ``time_key`` is polymorphic by JSON type:

    * ``kind == "file"`` — the source carries a date token; ``time_key``
      is the encoded year (int, or year×100+month) and the source
      contributes a single period.
    * ``kind == "column"`` — the source carries a time-key column;
      ``time_key`` is that column's name (str) and the source contributes
      many periods.
    * ``kind is None`` — neither shape detected; ``time_key`` is None.

    ``suggested_entity_key`` is the best-guess id column on this source
    (``is_known_id`` match). Multi-source aggregation (cross-source key
    selection, common-stem panel_id) is the editor's job; the helper
    only returns per-source signal.
    """

    kind: PanelMemberKind | None
    time_key: int | str | None = None
    suggested_entity_key: str | None = None


def detect_panel_member_kind(
    source_name: str, columns: tuple[str, ...]
) -> PanelMemberSuggestion:
    """Detect the panel-member shape implied by one source.

    File-member detection: a date token in ``source_name`` (trailing
    year, embedded ``YYYYMM`` / ``YYYY[_-]MM``, or year + HT/VT/Q tag).
    Column-member detection: any column matching ``_TIME_KEY_NAMES``.
    File takes precedence when both apply — file-members carry their
    period in the name, which is the more disambiguating signal.

    ``columns`` is a tuple of column names (no sql_type needed for this
    helper). The id-column suggestion uses ``is_known_id`` against those
    names.
    """
    cols_as_dicts = [{"name": c} for c in columns]
    ids = [c for c in columns if is_known_id(c)]
    suggested_key = ids[0] if len(ids) == 1 else None

    token = _match_date_token(source_name)
    if token is not None:
        month = _resolve_period_month(token)
        period = token.year * 100 + month if month is not None else token.year
        return PanelMemberSuggestion(
            kind="file", time_key=period, suggested_entity_key=suggested_key
        )

    time_key = _find_time_key_in_source(cols_as_dicts)
    if time_key is not None:
        return PanelMemberSuggestion(
            kind="column", time_key=time_key, suggested_entity_key=suggested_key
        )

    return PanelMemberSuggestion(kind=None, suggested_entity_key=suggested_key)


# -- Multi-source panel-shape detection -----------------------------------


@dataclass(frozen=True, slots=True)
class PanelCandidate:
    """Auto-detected panel shape for a register group.

    ``members`` is a list of dicts, each with ``source`` and a
    polymorphic ``time_key``: int for a literal period (file-member),
    str for a column name (column-member). Members are sorted by
    time_key when integer.
    """

    members: tuple[dict, ...]
    suggested_panel_id: str | None = None
    suggested_entity_key: str | None = None


def detect_panel_candidate(
    sources: list[str],
    sources_by_name: dict[str, dict[str, Any]],
    *,
    register_name: str | None = None,
) -> PanelCandidate | None:
    """Return the panel shape implied by the group's sources, or None.

    Two member-emission paths:

    * Multi-source: every source must carry a date token; all sources
      must share the same stem + suffix and the same date granularity
      (all year-only or all year+month). Mixed shapes signal
      heterogeneous tables.
    * Singleton: the lone source must carry a time-key column. Becomes
      a column-member with that column as ``time_key``.

    Anything else returns None — the user can still hand-edit ``panels``.
    """
    unique_sources = list(dict.fromkeys(sources))
    if len(unique_sources) >= 2:
        tokens: dict[str, _DateToken] = {}
        for name in unique_sources:
            t = _match_date_token(name)
            if t is None:
                return None
            tokens[name] = t
        if len({t.shape_key for t in tokens.values()}) > 1:
            return None
        months_per_source: dict[str, int | None] = {
            name: _resolve_period_month(t) for name, t in tokens.items()
        }
        if len({m is None for m in months_per_source.values()}) > 1:
            return None
        years_per_source = {name: t.year for name, t in tokens.items()}
        member_srcs = [sources_by_name[name] for name in unique_sources]
        suggested_key = _shared_id_column(member_srcs)
        members = _build_panel_members(
            unique_sources, years_per_source, months_per_source
        )
        return PanelCandidate(
            members=tuple(members),
            suggested_panel_id=suggest_panel_id(
                unique_sources, register_name=register_name
            ),
            suggested_entity_key=suggested_key,
        )
    if len(unique_sources) == 1:
        name = unique_sources[0]
        src = sources_by_name.get(name)
        if src is None:
            return None
        time_key = _find_time_key_in_source(src.get("columns", []))
        if time_key is None:
            return None
        ids = [c["name"] for c in src.get("columns", []) if is_known_id(c["name"])]
        suggested_key = ids[0] if len(ids) == 1 else None
        return PanelCandidate(
            members=({"source": name, "time_key": time_key},),
            suggested_panel_id=name,
            suggested_entity_key=suggested_key,
        )
    return None
