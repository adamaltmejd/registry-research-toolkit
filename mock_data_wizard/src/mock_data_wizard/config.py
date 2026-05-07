"""User-supplied configuration for extract.

Reads ``mdw_step2_config.json`` next to ``mdw_step3_stats.json``. Three concerns:

- ``column_types``: per-column type overrides for the classifier.
  Each entry is keyed by table-glob -> column-name -> override dict
  ``{"type": ..., "id_subtype": ..., "numeric_subtype": ...,
  "date_format": ...}``. Inline subtype/format hints let the bundle
  skip the sample query for that column entirely.
- ``column_options``: per-column option overrides for downstream
  consumers (``suppress_k`` on the disclosure-control side). Validated
  here, consumed in ``summarize.py``. Each option key is checked
  against ``VALID_OPTION_KEYS`` and the option's own invariants;
  ``suppress_k`` in particular is floored at the global ``SUPPRESS_K``
  so an override can only *raise* the disclosure-control threshold
  for a column, never lower it (a typo'd ``0`` would otherwise turn
  the override into a fail-open path).
- ``sources``: per-source metadata, keyed by exact source name.
  Currently carries ``year`` for year-aware CVID selection in
  ``enrich.py``. The configurer seeds entries from a 4-digit name
  regex; users can correct mis-detections by editing the file.

The three namespaces are separate on purpose: type, option, and
source-level metadata are independent concerns and mixing them in
one entry would cross-pollute the schema.

Order matters in both namespaces: when multiple table-globs match a
source, **last match wins**. List broad globs first (``lisa_*``) and
specific overrides below them (``lisa_2018``).

Strict by design: unknown types, duplicate keys (same string twice in
one JSON object -- a typo footgun), and schema-version mismatches
raise. Better to fail fast than silently swallow user intent.
"""

from __future__ import annotations

import fnmatch
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .classify import COLUMN_TYPES
from .summarize import SUPPRESS_K

CONFIG_FILENAME = "mdw_step2_config.json"
SCHEMA_VERSION = "mdw-config-2.0.0"

VALID_ID_SUBTYPES = ("integer", "string")
VALID_NUMERIC_SUBTYPES = ("integer", "double")
INLINE_HINT_KEYS: dict[str, tuple[str, ...]] = {
    "id": ("id_subtype",),
    "numeric": ("numeric_subtype",),
    "date": ("date_format",),
    "categorical": (),
    "opaque": (),
}
assert set(INLINE_HINT_KEYS) == set(COLUMN_TYPES)

# Per-column option keys recognised in `column_options`. Strict: anything
# not in this set raises at parse time so a typo can't silently no-op.
VALID_OPTION_KEYS: tuple[str, ...] = ("suppress_k",)

# Per-source metadata keys recognised in `sources`. Strict: anything not
# in this set raises at parse time so a typo can't silently no-op.
VALID_SOURCE_KEYS: tuple[str, ...] = ("year",)


@dataclass(frozen=True)
class ColumnTypeOverride:
    """A typed override for one column. ``type`` is required; the other
    fields are inline subtype/format hints. When *any* inline hint is
    supplied, the extractor skips the per-column sample query."""

    type: str
    id_subtype: str | None = None
    numeric_subtype: str | None = None
    date_format: str | None = None

    def has_inline_hint(self) -> bool:
        return any(getattr(self, k) is not None for k in INLINE_HINT_KEYS[self.type])


@dataclass(frozen=True)
class PanelMember:
    """One source contributing to a panel.

    Exactly one of ``period`` and ``time_key`` is set:

    - ``period``: a literal integer period for a one-period-per-file
      delivery (the source contributes a single period's rows).
    - ``time_key``: a column name on the source whose values carry the
      period for each row. The source can contribute many periods.

    Mixing the two within the same panel is allowed — e.g. a long
    history in one merged file with a ``year`` column, plus the most
    recent year as a separate file.
    """

    source: str
    period: int | None = None
    time_key: str | None = None

    def __post_init__(self) -> None:
        if (self.period is None) == (self.time_key is None):
            raise ValueError(
                f"PanelMember(source={self.source!r}): exactly one of "
                f"'period' or 'time_key' must be set"
            )


@dataclass(frozen=True)
class Panel:
    """A panel declaration on the configure side.

    A panel is a unit of analysis where the same entities (identified
    by ``panel_key``) recur across multiple periods. Each member
    declares how its rows map to a period — either via the filename
    (``period`` literal) or via a column on the source (``time_key``).
    """

    panel_id: str
    panel_key: str
    members: tuple[PanelMember, ...] = ()


@dataclass(frozen=True)
class MDWConfig:
    contract_version: str
    # table-glob -> column-name -> override
    column_types: dict[str, dict[str, ColumnTypeOverride]] = field(default_factory=dict)
    # table-glob -> column-name -> {option: value, ...} (consumed by #17)
    column_options: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)
    # exact source-name -> {key: value, ...} (year, ...). Exact-match only:
    # year is a per-source fact, not a class-of-source rule, so a glob
    # would invite confusion with the column_types/column_options globs.
    sources: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Panel declarations (#23): explicit panel_id + (panel_key, time_key
    # or member list) per panel. Out of scope for auto-detection -- the
    # configurer surfaces candidates, the user confirms.
    panels: tuple[Panel, ...] = ()

    def source_year(self, source_name: str) -> tuple[bool, int | None]:
        """Return ``(configured, year)`` for ``source_name``.

        - ``(False, None)`` -- the source has no entry in the ``sources``
          block; the caller may fall back to a name-regex guess.
        - ``(True, year)`` -- explicit user-supplied year; authoritative.
        - ``(True, None)`` -- explicit JSON null; suppress the regex
          fallback (the user is asserting "no year for this source").
        """
        entry = self.sources.get(source_name)
        if entry is None:
            return (False, None)
        # Type already validated by ``_parse_source_entry``: int or None.
        return (True, entry.get("year"))

    def lookup_type(
        self, source_name: str, column_name: str
    ) -> ColumnTypeOverride | None:
        """Return the override for ``(source_name, column_name)`` or None.

        Multiple table-globs may match. JSON insertion order decides;
        last match wins -- the entire override record is replaced (a
        column has exactly one type, so merging fields across globs
        doesn't make sense). List broad globs first and specific
        overrides below them. Within a matching glob, the column name
        is matched exactly (case-sensitive).
        """
        match: ColumnTypeOverride | None = None
        for glob, cols in self.column_types.items():
            if fnmatch.fnmatchcase(source_name, glob) and column_name in cols:
                match = cols[column_name]
        return match

    def lookup_options(self, source_name: str, column_name: str) -> dict[str, Any]:
        """Return merged options for ``(source_name, column_name)``.

        Same last-glob-wins ordering as ``lookup_type``, but per-key:
        non-conflicting keys from earlier matches persist, and only
        keys present in a later matching glob are overridden. Options
        are independent concerns (``suppress_k`` doesn't preclude any
        future option), so merging is the natural fit -- in contrast
        to ``lookup_type`` where the type is atomic.
        """
        merged: dict[str, Any] = {}
        for glob, cols in self.column_options.items():
            if fnmatch.fnmatchcase(source_name, glob) and column_name in cols:
                merged.update(cols[column_name])
        return merged


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """object_pairs_hook that raises on duplicate keys instead of
    silently keeping the last value (the json default)."""
    seen: dict[str, Any] = {}
    for k, v in pairs:
        if k in seen:
            raise ValueError(f"duplicate key {k!r} in {CONFIG_FILENAME}")
        seen[k] = v
    return seen


def _parse_override(table_glob: str, col: str, raw: Any) -> ColumnTypeOverride:
    if not isinstance(raw, dict):
        raise ValueError(
            f"column_types[{table_glob!r}][{col!r}] must be an object, "
            f"got {type(raw).__name__}"
        )
    if "type" not in raw:
        raise ValueError(
            f"column_types[{table_glob!r}][{col!r}] is missing required key 'type'"
        )
    typ = raw["type"]
    if typ not in COLUMN_TYPES:
        raise ValueError(
            f"column_types[{table_glob!r}][{col!r}].type={typ!r}, "
            f"expected one of {COLUMN_TYPES}"
        )

    allowed = {"type"} | set(INLINE_HINT_KEYS[typ])
    extra = set(raw) - allowed
    if extra:
        raise ValueError(
            f"column_types[{table_glob!r}][{col!r}] has key(s) {sorted(extra)} "
            f"not valid for type={typ!r} (allowed: {sorted(allowed)})"
        )

    id_subtype = raw.get("id_subtype")
    if id_subtype is not None and id_subtype not in VALID_ID_SUBTYPES:
        raise ValueError(
            f"column_types[{table_glob!r}][{col!r}].id_subtype={id_subtype!r}, "
            f"expected one of {VALID_ID_SUBTYPES}"
        )
    numeric_subtype = raw.get("numeric_subtype")
    if numeric_subtype is not None and numeric_subtype not in VALID_NUMERIC_SUBTYPES:
        raise ValueError(
            f"column_types[{table_glob!r}][{col!r}].numeric_subtype="
            f"{numeric_subtype!r}, expected one of {VALID_NUMERIC_SUBTYPES}"
        )
    return ColumnTypeOverride(
        type=typ,
        id_subtype=id_subtype,
        numeric_subtype=numeric_subtype,
        date_format=raw.get("date_format"),
    )


def _parse_options(table_glob: str, col: str, raw: Any) -> dict[str, Any]:
    """Validate one column's options dict.

    Strict on unknown keys (typo guard). Per-key validation enforces the
    invariants each option needs:

    - ``suppress_k``: a positive int (not bool -- ``bool`` is an ``int``
      subclass in Python and would slip past a naive isinstance check).
      Floored at the global ``SUPPRESS_K`` so an override can only
      *raise* the disclosure-control threshold for a specific column,
      never lower it below the project-wide minimum. A typo'd ``0`` or
      ``-1`` would otherwise turn the override into a fail-open path.
    """
    if not isinstance(raw, dict):
        raise ValueError(
            f"column_options[{table_glob!r}][{col!r}] must be an object, "
            f"got {type(raw).__name__}"
        )
    out: dict[str, Any] = {}
    for key, val in raw.items():
        if key not in VALID_OPTION_KEYS:
            raise ValueError(
                f"column_options[{table_glob!r}][{col!r}] has unknown option "
                f"{key!r} (allowed: {sorted(VALID_OPTION_KEYS)})"
            )
        if key == "suppress_k":
            if isinstance(val, bool) or not isinstance(val, int):
                raise ValueError(
                    f"column_options[{table_glob!r}][{col!r}].suppress_k must be "
                    f"an int, got {type(val).__name__} ({val!r})"
                )
            if val < SUPPRESS_K:
                raise ValueError(
                    f"column_options[{table_glob!r}][{col!r}].suppress_k={val} "
                    f"is below the global minimum SUPPRESS_K={SUPPRESS_K}; "
                    f"overrides may only raise the threshold, not lower it"
                )
        out[key] = val
    return out


_TOP_LEVEL_KEYS = frozenset(
    {"contract_version", "column_types", "column_options", "sources", "panels"}
)


def _parse_panel(raw: Any, idx: int) -> Panel:
    """Validate one panel declaration.

    Required: ``panel_id`` (str), ``panel_key`` (str), ``members``
    (non-empty list). Each member is ``{"source": str, "period": int}``
    OR ``{"source": str, "time_key": str}`` — exactly one of period /
    time_key. Source uniqueness within the panel is enforced; period
    uniqueness across file-members is enforced (column-member periods
    materialise at extract time and are validated there).
    """
    if not isinstance(raw, dict):
        raise ValueError(f"panels[{idx}] must be an object, got {type(raw).__name__}")
    required = {"panel_id", "panel_key", "members"}
    allowed = required
    extra = set(raw) - allowed
    if extra:
        raise ValueError(
            f"panels[{idx}] has unknown key(s) {sorted(extra)} "
            f"(allowed: {sorted(allowed)})"
        )
    missing = required - set(raw)
    if missing:
        raise ValueError(f"panels[{idx}] missing required key(s) {sorted(missing)}")
    panel_id = raw["panel_id"]
    if not isinstance(panel_id, str) or not panel_id:
        raise ValueError(f"panels[{idx}].panel_id must be a non-empty string")
    panel_key = raw["panel_key"]
    if not isinstance(panel_key, str) or not panel_key:
        raise ValueError(f"panels[{idx}].panel_key must be a non-empty string")
    members_raw = raw["members"]
    if not isinstance(members_raw, list) or not members_raw:
        raise ValueError(f"panels[{idx}].members must be a non-empty list")

    members: list[PanelMember] = []
    seen_sources: set[str] = set()
    seen_periods: set[int] = set()
    for j, m in enumerate(members_raw):
        if not isinstance(m, dict):
            raise ValueError(
                f"panels[{idx}].members[{j}] must be an object, got {type(m).__name__}"
            )
        member_extra = set(m) - {"source", "period", "time_key"}
        if member_extra:
            raise ValueError(
                f"panels[{idx}].members[{j}] has unknown key(s) "
                f"{sorted(member_extra)} (allowed: ['period', 'source', 'time_key'])"
            )
        src = m.get("source")
        if not isinstance(src, str) or not src:
            raise ValueError(
                f"panels[{idx}].members[{j}].source must be a non-empty string"
            )
        if src in seen_sources:
            raise ValueError(f"panels[{idx}].members has duplicate source {src!r}")
        seen_sources.add(src)
        has_period = "period" in m
        has_time_key = "time_key" in m
        if has_period == has_time_key:
            raise ValueError(
                f"panels[{idx}].members[{j}] (source={src!r}): exactly one of "
                f"'period' or 'time_key' must be set"
            )
        if has_period:
            period = m["period"]
            if isinstance(period, bool) or not isinstance(period, int):
                raise ValueError(
                    f"panels[{idx}].members[{j}].period must be an int, "
                    f"got {type(period).__name__}"
                )
            if period in seen_periods:
                raise ValueError(f"panels[{idx}].members has duplicate period {period}")
            seen_periods.add(period)
            members.append(PanelMember(source=src, period=period))
        else:
            time_key = m["time_key"]
            if not isinstance(time_key, str) or not time_key:
                raise ValueError(
                    f"panels[{idx}].members[{j}].time_key must be a non-empty string"
                )
            members.append(PanelMember(source=src, time_key=time_key))
    return Panel(
        panel_id=panel_id,
        panel_key=panel_key,
        members=tuple(members),
    )


def _parse_source_entry(source_name: str, raw: Any) -> dict[str, Any]:
    """Validate one source's metadata dict.

    Strict on unknown keys (typo guard). Per-key validation:

    - ``year``: int (or null to mean "no year"). Rejects bool because
      ``bool`` is an ``int`` subclass in Python.
    """
    if not isinstance(raw, dict):
        raise ValueError(
            f"sources[{source_name!r}] must be an object, got {type(raw).__name__}"
        )
    out: dict[str, Any] = {}
    for key, val in raw.items():
        if key not in VALID_SOURCE_KEYS:
            raise ValueError(
                f"sources[{source_name!r}] has unknown key {key!r} "
                f"(allowed: {sorted(VALID_SOURCE_KEYS)})"
            )
        if key == "year":
            if val is None:
                out["year"] = None
                continue
            if isinstance(val, bool) or not isinstance(val, int):
                raise ValueError(
                    f"sources[{source_name!r}].year must be an int or null, "
                    f"got {type(val).__name__} ({val!r})"
                )
            out["year"] = val
    return out


def parse_config(payload: dict[str, Any]) -> MDWConfig:
    if "contract_version" not in payload:
        raise ValueError(
            f"{CONFIG_FILENAME}: missing required key 'contract_version' "
            f"(expected {SCHEMA_VERSION!r})"
        )
    contract_version = payload["contract_version"]
    if contract_version != SCHEMA_VERSION:
        raise ValueError(
            f"{CONFIG_FILENAME}: unsupported contract_version {contract_version!r} "
            f"(this build supports {SCHEMA_VERSION!r})"
        )
    extra = set(payload) - _TOP_LEVEL_KEYS
    if extra:
        raise ValueError(
            f"{CONFIG_FILENAME}: unknown top-level key(s) {sorted(extra)} "
            f"(allowed: {sorted(_TOP_LEVEL_KEYS)})"
        )

    raw_types = payload.get("column_types", {})
    if not isinstance(raw_types, dict):
        raise ValueError(f"{CONFIG_FILENAME}: column_types must be an object")
    column_types: dict[str, dict[str, ColumnTypeOverride]] = {}
    for table_glob, cols in raw_types.items():
        if not isinstance(cols, dict):
            raise ValueError(
                f"column_types[{table_glob!r}] must be an object, "
                f"got {type(cols).__name__}"
            )
        column_types[table_glob] = {
            col: _parse_override(table_glob, col, raw) for col, raw in cols.items()
        }

    raw_options = payload.get("column_options", {})
    if not isinstance(raw_options, dict):
        raise ValueError(f"{CONFIG_FILENAME}: column_options must be an object")
    column_options: dict[str, dict[str, dict[str, Any]]] = {}
    for table_glob, cols in raw_options.items():
        if not isinstance(cols, dict):
            raise ValueError(
                f"column_options[{table_glob!r}] must be an object, "
                f"got {type(cols).__name__}"
            )
        column_options[table_glob] = {
            col: _parse_options(table_glob, col, opts) for col, opts in cols.items()
        }

    raw_sources = payload.get("sources", {})
    if not isinstance(raw_sources, dict):
        raise ValueError(f"{CONFIG_FILENAME}: sources must be an object")
    sources: dict[str, dict[str, Any]] = {
        name: _parse_source_entry(name, entry) for name, entry in raw_sources.items()
    }

    raw_panels = payload.get("panels", [])
    if not isinstance(raw_panels, list):
        raise ValueError(f"{CONFIG_FILENAME}: panels must be an array")
    panels: list[Panel] = []
    seen_panel_ids: set[str] = set()
    # source -> first panel_id that owns it. ``panel_by_source`` in
    # generate.py is a flat dict keyed by source, so any collision
    # silently drops the second panel's behavior at generation time.
    seen_panel_sources: dict[str, str] = {}
    for i, raw in enumerate(raw_panels):
        panel = _parse_panel(raw, i)
        if panel.panel_id in seen_panel_ids:
            raise ValueError(f"panels: duplicate panel_id {panel.panel_id!r}")
        seen_panel_ids.add(panel.panel_id)
        # Two panels CAN share a panel_key — in SCB data nearly every
        # register is keyed on the same person id (e.g.
        # ``P1105_LopNr_PersonNr``). They share one id universe by
        # design; generate.py builds one pool per panel_key sized to
        # the largest contributor.
        # A source can only belong to one panel (multi-key panels are
        # out of scope). Each member's source is claimed regardless of
        # whether it contributes via period or time_key.
        for src in (m.source for m in panel.members):
            prior = seen_panel_sources.get(src)
            if prior is not None:
                raise ValueError(
                    f"panels: {panel.panel_id!r} and {prior!r} both reference "
                    f"source {src!r}; a source may participate in at most one "
                    f"panel (merge the panels or split the source)"
                )
            seen_panel_sources[src] = panel.panel_id
        panels.append(panel)

    return MDWConfig(
        contract_version=contract_version,
        column_types=column_types,
        column_options=column_options,
        sources=sources,
        panels=tuple(panels),
    )


def load_config(directory: Path) -> MDWConfig | None:
    """Load ``mdw_step2_config.json`` from ``directory`` if present.

    Returns None if no file is there. Raises on any structural problem
    -- the configurer is meant to flag typos at extract time, not
    silently ignore them.
    """
    path = Path(directory) / CONFIG_FILENAME
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as fp:
        payload = json.load(fp, object_pairs_hook=_reject_duplicate_keys)
    if not isinstance(payload, dict):
        raise ValueError(f"{CONFIG_FILENAME}: top-level value must be an object")
    return parse_config(payload)
