"""User-supplied configuration for extract.

Reads ``mock_data_config.json`` next to ``mock_data_stats.json``. Schema
version 3.0.0. Five concerns:

- ``column_types``: per-column type overrides for the classifier. Keyed
  by exact ``source_name`` -> column-name -> override dict
  ``{"type": ..., "id_subtype": ..., "numeric_subtype": ...,
  "date_format": ...}``. Inline subtype/format hints let the bundle
  skip the per-column sample query at extract time and serve as a
  manual-override hatch when sampling would be ambiguous (e.g.
  ``01-02-2018`` could parse two ways) or impossible (all-NULL
  columns).
- ``column_options``: per-column option overrides for downstream
  consumers (``suppress_k`` on the disclosure-control side). Validated
  here, consumed in ``summarize.py``. ``suppress_k`` is floored at the
  global ``SUPPRESS_K`` so an override can only *raise* the
  disclosure-control threshold for a column, never lower it (a typo'd
  ``0`` would otherwise turn the override into a fail-open path).
- ``sources``: per-source metadata, keyed by exact source name. Carries
  ``year`` (year-aware CVID selection in ``enrich.py``) and
  ``register`` (which register's regmeta evidence drove this source's
  classification — persisted so reopening the editor restores
  context).
- ``manual_columns``: top-level array of ``[source, column]`` pairs the
  user explicitly overrode. Re-classification operations (e.g.
  changing a group's register) skip these by default. Side namespace
  rather than a per-column ``provenance`` field so the bundle's
  strict parser doesn't need to know about it; the bundle ignores
  ``manual_columns`` entirely.
- ``discover_hash``: SHA-256 of the discover payload's
  ``(source_name, [(col, sql_type), ...])`` tuples (sorted on both
  axes for determinism). The editor recomputes the hash on every
  read and surfaces a ``discover_drift`` warning when it differs.

Version 3.0.0 dropped ``fnmatchcase`` glob keys in favour of exact
``source_name`` matches. With the editor producing N exact entries on
every save, globs were redundant noise. **No backwards compatibility**
— pre-3.0.0 files raise on read with a regenerate hint.

Strict by design: unknown types, unknown options, unknown source keys,
duplicate JSON keys (a typo footgun), and schema-version mismatches all
raise. Better to fail fast than silently swallow user intent.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .classify import COLUMN_TYPES
from .summarize import SUPPRESS_K

CONFIG_FILENAME = "mock_data_config.json"
SCHEMA_VERSION = "mdw-config-3.0.0"

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
VALID_SOURCE_KEYS: tuple[str, ...] = ("year", "register")


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

    ``time_key`` is polymorphic by JSON type:

    - ``int``: a literal period for a one-period-per-file delivery (the
      source contributes a single period's rows). Typically the year
      derived from the source name.
    - ``str``: a column name on the source whose values carry the
      period for each row. The source can contribute many periods.

    Mixing the two within the same panel is allowed — e.g. a long
    history in one merged file with a ``year`` column, plus the most
    recent year as a separate file.
    """

    source: str
    time_key: int | str

    def __post_init__(self) -> None:
        if isinstance(self.time_key, bool) or not isinstance(self.time_key, (int, str)):
            raise ValueError(
                f"PanelMember(source={self.source!r}): time_key must be int or str, "
                f"got {type(self.time_key).__name__}"
            )
        if isinstance(self.time_key, str) and not self.time_key:
            raise ValueError(
                f"PanelMember(source={self.source!r}): time_key must be non-empty"
            )


@dataclass(frozen=True)
class Panel:
    """A panel declaration on the configure side.

    A panel is a unit of analysis where the same entities (identified
    by ``entity_key``) recur across multiple periods. Each member
    declares how its rows map to a period via ``time_key`` — either an
    integer literal (file-member) or a column name (column-member).
    """

    panel_id: str
    entity_key: str
    members: tuple[PanelMember, ...] = ()


@dataclass(frozen=True)
class MDWConfig:
    contract_version: str
    # exact source-name -> column-name -> override
    column_types: dict[str, dict[str, ColumnTypeOverride]] = field(default_factory=dict)
    # exact source-name -> column-name -> {option: value, ...}
    column_options: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)
    # exact source-name -> {key: value, ...}. Carries `year` and `register`.
    sources: dict[str, dict[str, Any]] = field(default_factory=dict)
    # Panel declarations: explicit panel_id + entity_key + member list.
    # Each member has a polymorphic time_key (int = literal period;
    # str = column name on the source).
    panels: tuple[Panel, ...] = ()
    # Top-level provenance: (source, column) pairs the user explicitly
    # overrode. Re-classification skips these by default.
    manual_columns: tuple[tuple[str, str], ...] = ()
    # SHA-256 of the discover payload's (source, [(col, sql_type)])
    # tuples (sorted both axes). The editor recomputes on read and
    # warns on drift. None when the config was written without a
    # discover payload (rare; mostly hand-edited test fixtures).
    discover_hash: str | None = None

    def source_year(self, source_name: str) -> tuple[bool, int | None]:
        """Return ``(configured, year)`` for ``source_name``.

        - ``(False, None)`` -- the source has no entry in the ``sources``
          block; the caller may fall back to a name-regex guess.
        - ``(True, year)`` -- explicit user-supplied year; authoritative.
        - ``(True, None)`` -- explicit JSON null; suppress the regex
          fallback (the user is asserting "no year for this source").
        """
        entry = self.sources.get(source_name)
        if entry is None or "year" not in entry:
            return (False, None)
        return (True, entry.get("year"))

    def lookup_type(
        self, source_name: str, column_name: str
    ) -> ColumnTypeOverride | None:
        """Return the override for ``(source_name, column_name)`` or None.

        Exact ``source_name`` match (case-sensitive); the column name is
        also matched exactly. With 3.0.0's exact-name keying the lookup
        is a single dict access.
        """
        return self.column_types.get(source_name, {}).get(column_name)

    def lookup_options(self, source_name: str, column_name: str) -> dict[str, Any]:
        """Return options for ``(source_name, column_name)``.

        Exact-name lookup, returning a copy so callers can't mutate the
        stored dict. Empty when no entry exists.
        """
        return dict(self.column_options.get(source_name, {}).get(column_name, {}))


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """object_pairs_hook that raises on duplicate keys instead of
    silently keeping the last value (the json default)."""
    seen: dict[str, Any] = {}
    for k, v in pairs:
        if k in seen:
            raise ValueError(f"duplicate key {k!r} in {CONFIG_FILENAME}")
        seen[k] = v
    return seen


def _parse_override(source_name: str, col: str, raw: Any) -> ColumnTypeOverride:
    if not isinstance(raw, dict):
        raise ValueError(
            f"column_types[{source_name!r}][{col!r}] must be an object, "
            f"got {type(raw).__name__}"
        )
    if "type" not in raw:
        raise ValueError(
            f"column_types[{source_name!r}][{col!r}] is missing required key 'type'"
        )
    typ = raw["type"]
    if typ not in COLUMN_TYPES:
        raise ValueError(
            f"column_types[{source_name!r}][{col!r}].type={typ!r}, "
            f"expected one of {COLUMN_TYPES}"
        )

    allowed = {"type"} | set(INLINE_HINT_KEYS[typ])
    extra = set(raw) - allowed
    if extra:
        raise ValueError(
            f"column_types[{source_name!r}][{col!r}] has key(s) {sorted(extra)} "
            f"not valid for type={typ!r} (allowed: {sorted(allowed)})"
        )

    id_subtype = raw.get("id_subtype")
    if id_subtype is not None and id_subtype not in VALID_ID_SUBTYPES:
        raise ValueError(
            f"column_types[{source_name!r}][{col!r}].id_subtype={id_subtype!r}, "
            f"expected one of {VALID_ID_SUBTYPES}"
        )
    numeric_subtype = raw.get("numeric_subtype")
    if numeric_subtype is not None and numeric_subtype not in VALID_NUMERIC_SUBTYPES:
        raise ValueError(
            f"column_types[{source_name!r}][{col!r}].numeric_subtype="
            f"{numeric_subtype!r}, expected one of {VALID_NUMERIC_SUBTYPES}"
        )
    return ColumnTypeOverride(
        type=typ,
        id_subtype=id_subtype,
        numeric_subtype=numeric_subtype,
        date_format=raw.get("date_format"),
    )


def _parse_options(source_name: str, col: str, raw: Any) -> dict[str, Any]:
    """Validate one column's options dict.

    Strict on unknown keys (typo guard). Per-key validation enforces the
    invariants each option needs. ``suppress_k`` is floored at the
    global ``SUPPRESS_K`` so an override can only raise the threshold,
    never lower it below the project-wide minimum.
    """
    if not isinstance(raw, dict):
        raise ValueError(
            f"column_options[{source_name!r}][{col!r}] must be an object, "
            f"got {type(raw).__name__}"
        )
    out: dict[str, Any] = {}
    for key, val in raw.items():
        if key not in VALID_OPTION_KEYS:
            raise ValueError(
                f"column_options[{source_name!r}][{col!r}] has unknown option "
                f"{key!r} (allowed: {sorted(VALID_OPTION_KEYS)})"
            )
        if key == "suppress_k":
            if isinstance(val, bool) or not isinstance(val, int):
                raise ValueError(
                    f"column_options[{source_name!r}][{col!r}].suppress_k must be "
                    f"an int, got {type(val).__name__} ({val!r})"
                )
            if val < SUPPRESS_K:
                raise ValueError(
                    f"column_options[{source_name!r}][{col!r}].suppress_k={val} "
                    f"is below the global minimum SUPPRESS_K={SUPPRESS_K}; "
                    f"overrides may only raise the threshold, not lower it"
                )
        out[key] = val
    return out


_TOP_LEVEL_KEYS = frozenset(
    {
        "contract_version",
        "discover_hash",
        "column_types",
        "column_options",
        "sources",
        "panels",
        "manual_columns",
    }
)


def _parse_panel(raw: Any, idx: int) -> Panel:
    """Validate one panel declaration."""
    if not isinstance(raw, dict):
        raise ValueError(f"panels[{idx}] must be an object, got {type(raw).__name__}")
    required = {"panel_id", "entity_key", "members"}
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
    entity_key = raw["entity_key"]
    if not isinstance(entity_key, str) or not entity_key:
        raise ValueError(f"panels[{idx}].entity_key must be a non-empty string")
    members_raw = raw["members"]
    if not isinstance(members_raw, list) or not members_raw:
        raise ValueError(f"panels[{idx}].members must be a non-empty list")

    members: list[PanelMember] = []
    seen_sources: set[str] = set()
    seen_int_time_keys: set[int] = set()
    for j, m in enumerate(members_raw):
        if not isinstance(m, dict):
            raise ValueError(
                f"panels[{idx}].members[{j}] must be an object, got {type(m).__name__}"
            )
        member_extra = set(m) - {"source", "time_key"}
        if member_extra:
            raise ValueError(
                f"panels[{idx}].members[{j}] has unknown key(s) "
                f"{sorted(member_extra)} (allowed: ['source', 'time_key'])"
            )
        src = m.get("source")
        if not isinstance(src, str) or not src:
            raise ValueError(
                f"panels[{idx}].members[{j}].source must be a non-empty string"
            )
        if src in seen_sources:
            raise ValueError(f"panels[{idx}].members has duplicate source {src!r}")
        seen_sources.add(src)
        if "time_key" not in m:
            raise ValueError(
                f"panels[{idx}].members[{j}] (source={src!r}): missing required "
                f"key 'time_key' (int for literal period, str for column name)"
            )
        time_key = m["time_key"]
        # bool is an int subclass in Python; reject explicitly.
        if isinstance(time_key, bool):
            raise ValueError(
                f"panels[{idx}].members[{j}].time_key must be int or non-empty "
                f"string, got bool"
            )
        if isinstance(time_key, int):
            if time_key in seen_int_time_keys:
                raise ValueError(
                    f"panels[{idx}].members has duplicate literal time_key {time_key}"
                )
            seen_int_time_keys.add(time_key)
        elif isinstance(time_key, str):
            if not time_key:
                raise ValueError(
                    f"panels[{idx}].members[{j}].time_key must be a non-empty string"
                )
        else:
            raise ValueError(
                f"panels[{idx}].members[{j}].time_key must be int or non-empty "
                f"string, got {type(time_key).__name__}"
            )
        members.append(PanelMember(source=src, time_key=time_key))
    return Panel(
        panel_id=panel_id,
        entity_key=entity_key,
        members=tuple(members),
    )


def _parse_source_entry(source_name: str, raw: Any) -> dict[str, Any]:
    """Validate one source's metadata dict.

    Strict on unknown keys (typo guard). Per-key validation:

    - ``year``: int (or null to mean "no year"). Rejects bool because
      ``bool`` is an ``int`` subclass in Python.
    - ``register``: str (or null) — name or numeric id of the register
      whose regmeta evidence drove classification for this source.
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
        elif key == "register":
            if val is None:
                out["register"] = None
                continue
            if not isinstance(val, str):
                raise ValueError(
                    f"sources[{source_name!r}].register must be a string or null, "
                    f"got {type(val).__name__} ({val!r})"
                )
            out["register"] = val
    return out


def _parse_manual_columns(raw: Any) -> tuple[tuple[str, str], ...]:
    """Validate ``[[source, column], ...]`` pairs.

    Order is preserved (informational only; semantics are set membership).
    Duplicates are rejected — a duplicate signals confusion, not idempotence.
    """
    if not isinstance(raw, list):
        raise ValueError(
            f"{CONFIG_FILENAME}: manual_columns must be an array, "
            f"got {type(raw).__name__}"
        )
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []
    for i, pair in enumerate(raw):
        if not isinstance(pair, list) or len(pair) != 2:
            raise ValueError(
                f"manual_columns[{i}] must be a 2-element list "
                f"[source, column], got {pair!r}"
            )
        src, col = pair
        if not isinstance(src, str) or not src:
            raise ValueError(f"manual_columns[{i}][0] must be a non-empty string")
        if not isinstance(col, str) or not col:
            raise ValueError(f"manual_columns[{i}][1] must be a non-empty string")
        key = (src, col)
        if key in seen:
            raise ValueError(f"manual_columns: duplicate entry {list(key)}")
        seen.add(key)
        out.append(key)
    return tuple(out)


def parse_config(payload: dict[str, Any]) -> MDWConfig:
    if "contract_version" not in payload:
        raise ValueError(
            f"{CONFIG_FILENAME}: missing required key 'contract_version' "
            f"(expected {SCHEMA_VERSION!r})"
        )
    contract_version = payload["contract_version"]
    if contract_version != SCHEMA_VERSION:
        # Pre-3.0.0 (1.0.0 / 2.0.0) carried glob-keyed column_types and
        # no manual_columns / discover_hash. Editor produces 3.0.0 from
        # discover; tell the user to regenerate rather than migrate.
        raise ValueError(
            f"{CONFIG_FILENAME}: unsupported contract_version "
            f"{contract_version!r} (this build supports {SCHEMA_VERSION!r}). "
            f"Regenerate the config from a fresh discover payload via "
            f"the editor's init_if_missing(); pre-3.0.0 schemas are not "
            f"migrated."
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
    for source_name, cols in raw_types.items():
        if not isinstance(cols, dict):
            raise ValueError(
                f"column_types[{source_name!r}] must be an object, "
                f"got {type(cols).__name__}"
            )
        column_types[source_name] = {
            col: _parse_override(source_name, col, raw) for col, raw in cols.items()
        }

    raw_options = payload.get("column_options", {})
    if not isinstance(raw_options, dict):
        raise ValueError(f"{CONFIG_FILENAME}: column_options must be an object")
    column_options: dict[str, dict[str, dict[str, Any]]] = {}
    for source_name, cols in raw_options.items():
        if not isinstance(cols, dict):
            raise ValueError(
                f"column_options[{source_name!r}] must be an object, "
                f"got {type(cols).__name__}"
            )
        column_options[source_name] = {
            col: _parse_options(source_name, col, opts) for col, opts in cols.items()
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
    seen_panel_sources: dict[str, str] = {}
    for i, raw in enumerate(raw_panels):
        panel = _parse_panel(raw, i)
        if panel.panel_id in seen_panel_ids:
            raise ValueError(f"panels: duplicate panel_id {panel.panel_id!r}")
        seen_panel_ids.add(panel.panel_id)
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

    manual_columns = _parse_manual_columns(payload.get("manual_columns", []))

    discover_hash = payload.get("discover_hash")
    if discover_hash is not None and not isinstance(discover_hash, str):
        raise ValueError(
            f"{CONFIG_FILENAME}: discover_hash must be a string or absent, "
            f"got {type(discover_hash).__name__}"
        )

    return MDWConfig(
        contract_version=contract_version,
        column_types=column_types,
        column_options=column_options,
        sources=sources,
        panels=tuple(panels),
        manual_columns=manual_columns,
        discover_hash=discover_hash,
    )


def load_config(directory: Path) -> MDWConfig | None:
    """Load ``mock_data_config.json`` from ``directory`` if present.

    Returns None if no file is there. Raises on any structural problem
    -- the editor flags typos at extract time, not silently ignores them.
    """
    path = Path(directory) / CONFIG_FILENAME
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as fp:
        payload = json.load(fp, object_pairs_hook=_reject_duplicate_keys)
    if not isinstance(payload, dict):
        raise ValueError(f"{CONFIG_FILENAME}: top-level value must be an object")
    return parse_config(payload)
