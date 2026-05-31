"""Runtime adapter over ``reg_schema.ProjectData``.

Reads ``project_data.json`` (REFACTOR_SPEC.md §6), validates it
structurally via ``reg_schema.validate_structural``, then exposes the
``(source_name, display_name)``-keyed lookup surface that extract /
sources / summarize consume.

``reg_schema.ProjectData`` is the on-disk shape — frozen, FQID-keyed
columns. ``LoadedSpec`` is the runtime adapter — it indexes the spec
for O(1) lookups by SQL column header (``display_name``) and owns the
per-source mutable override cache that
``sources._probe_and_promote_opaque`` writes into. ``ProjectData`` is
frozen and cannot host that mutation; the cache is the only mutation
path.

Step 4 boundaries (REFACTOR_SPEC.md §15):

- Composite ``entity_key`` / ``time_key`` (tuple-shaped) and
  ``LiteralPeriod`` (``{"period": ...}``) are rejected at load with a
  clear "step 10b" message — the schema accepts them but the
  extract/generate runtime is scalar-only until then.
- Panel-level ``time_key`` defaults and per-member ``entity_key``
  overrides are rejected the same way — the old runtime supported
  neither, and adding the resolver here would bleed scope.
- Every column must carry a ``display_name`` at load. The schema marks
  it optional because the webapp materializes defaults from reg_meta
  at bundle-build time (§6.3 + §7); that pipeline lands in §15 step 6.
- The ``reg_monabundle`` namespaced block is validated by
  ``reg_monabundle.validate_block`` (§6.8.2). The cross-block
  referential checks (orphan FQID, suppress_k-on-non-categorical) stay
  here because they need the resolved column dataclasses.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from reg_monabundle import validate_block
from reg_schema import (
    Binding,
    Panel,
    PanelMember,
    ProjectData,
    Source,
    validate_structural,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

from .classify import COLUMN_TYPES

PROJECT_DATA_FILENAME = "project_data.json"

# Per-type inline hint keys. Mirrors mdw's COLUMN_TYPES exactly —
# reg_schema's ``datetime`` is rejected at load (see ``_build_column``)
# because the mdw extract/summarize/generate stack has no datetime
# branch; until end-to-end datetime support lands, accepting it here
# would surface as a late ``ValueError`` from ``sql_emit``.
INLINE_HINT_KEYS: dict[str, tuple[str, ...]] = {
    "id": ("id_subtype",),
    "numeric": ("numeric_subtype",),
    "date": ("date_format",),
    "categorical": (),
    "opaque": (),
}
assert set(INLINE_HINT_KEYS) == set(COLUMN_TYPES)


# -- Runtime convenience dataclass ----------------------------------------


@dataclass(frozen=True)
class ColumnTypeOverride:
    """A per-binding type assignment with optional inline subtype/format hints.

    Built from a ``reg_schema.Binding`` at load time. Lives in ``spec.py``
    rather than ``reg_schema`` because it's a runtime artifact —
    ``sources._probe_and_promote_opaque`` mutates a per-source copy when
    promoting opaque columns, and ``reg_schema`` is frozen by design.

    When *any* inline hint is supplied, ``extract.process_handle`` skips
    the per-column sample query: the hint pins subtype/format without
    needing data inspection.
    """

    type: str
    id_subtype: str | None = None
    numeric_subtype: str | None = None
    date_format: str | None = None

    def has_inline_hint(self) -> bool:
        return any(getattr(self, k) is not None for k in INLINE_HINT_KEYS[self.type])

    @classmethod
    def from_column(cls, binding: Binding) -> ColumnTypeOverride:
        # ``datetime_format`` is intentionally dropped: ``_build_column``
        # rejects ``type == "datetime"`` before any Binding with that
        # type or ``datetime_format`` set can reach this constructor.
        return cls(
            type=binding.type,
            id_subtype=binding.id_subtype,
            numeric_subtype=binding.numeric_subtype,
            date_format=binding.date_format,
        )


# -- LoadedSpec adapter ---------------------------------------------------


class LoadedSpec:
    """Runtime adapter over a validated ``reg_schema.ProjectData``.

    Indexes sources by name and columns by ``(source_name, display_name)``.
    Exposes the lookup surface ``extract`` / ``sources`` / ``summarize``
    consume, so the migration off ``MDWConfig`` doesn't bleed into
    every caller.

    Owns the per-source mutable override cache (``_type_cache``) so
    ``sources._probe_and_promote_opaque`` has a place to write
    promotions — ``ProjectData`` itself is frozen.
    """

    def __init__(self, project_data: ProjectData) -> None:
        self.project_data = project_data
        self._sources_by_name: dict[str, Source] = {
            s.name: s for s in project_data.sources
        }
        self._columns_by_display: dict[tuple[str, str], Binding] = {}
        for source in project_data.sources:
            for binding in source.bindings:
                # display_name is required at load (asserted in
                # _build_column). The assert silences the type checker
                # about the schema's Optional[str].
                assert binding.display_name is not None
                self._columns_by_display[(source.name, binding.display_name)] = binding
        self._type_cache: dict[str, dict[str, ColumnTypeOverride]] = {}
        block = project_data.reg_monabundle or {}
        raw_options = block.get("column_options") or {}
        # Cast through Mapping for callers; the validator pinned the
        # inner shape, so a plain dict access is enough.
        self._column_options: Mapping[str, Mapping[str, Any]] = raw_options  # ty: ignore[invalid-assignment]

    @property
    def panels(self) -> tuple[Panel, ...]:
        return self.project_data.panels

    def column_types_for_source(
        self, source_name: str
    ) -> dict[str, ColumnTypeOverride]:
        """Return the per-source override dict — lazy, cached, and mutable.

        ``sources._probe_and_promote_opaque`` writes promoted overrides
        into the returned dict; ``lookup_type`` reads from the same
        cache, so the promotion propagates to
        ``extract.process_handle`` without extra plumbing. The cache
        key is ``display_name`` (the SQL header) — the runtime never
        sees FQIDs.
        """
        if source_name not in self._type_cache:
            source = self._sources_by_name.get(source_name)
            if source is None:
                self._type_cache[source_name] = {}
            else:
                self._type_cache[source_name] = {
                    binding.display_name: ColumnTypeOverride.from_column(binding)
                    for binding in source.bindings
                    if binding.display_name is not None
                }
        return self._type_cache[source_name]

    def lookup_type(
        self, source_name: str, display_name: str
    ) -> ColumnTypeOverride | None:
        return self.column_types_for_source(source_name).get(display_name)

    def lookup_options(self, source_name: str, display_name: str) -> dict[str, Any]:
        """Return ``reg_monabundle.column_options`` for one column.

        Resolves through the column's binding FQID: the on-disk block
        is FQID-keyed (single source of truth) but the runtime lookup
        is SQL-header-keyed (what extract has). Returns a fresh copy
        so callers can't mutate the spec.
        """
        binding = self._columns_by_display.get((source_name, display_name))
        if binding is None:
            return {}
        return dict(self._column_options.get(binding.variable, {}))


# -- JSON loader ----------------------------------------------------------


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """``object_pairs_hook`` that raises on duplicate JSON keys.

    The json default keeps the last value silently; for a hand-edited
    file on MONA the typo footgun matters.
    """
    seen: dict[str, Any] = {}
    for k, v in pairs:
        if k in seen:
            raise ValueError(f"duplicate key {k!r} in {PROJECT_DATA_FILENAME}")
        seen[k] = v
    return seen


# -- Dataclass construction (post-validation) -----------------------------


def _build_column(data: Mapping[str, Any], *, source_name: str, idx: int) -> Binding:
    """Build a ``reg_schema.Binding`` from a JSON dict, requiring display_name.

    The schema marks ``display_name`` optional because the webapp
    pre-resolves defaults from reg_meta at bundle-build time
    (REFACTOR_SPEC.md §6.3 + §7). ``mock_data_wizard`` runs without
    reg_meta in the extract loop, so every binding must already carry a
    ``display_name``. The bundle pre-resolve pipeline lands at §15
    step 6; until then, hand-write ``display_name`` on every binding.

    Also rejects ``type == "datetime"``: reg_schema accepts datetime
    bindings but the mdw extract/summarize/generate stack has no
    datetime branch (``classify.COLUMN_TYPES`` and
    ``sql_emit.queries_for_column``). Rejecting at load surfaces the
    error here instead of as a late ``ValueError`` deep in extract.
    """
    column_type = data.get("type")
    if column_type == "datetime":
        raise ValueError(
            f"sources[{source_name!r}].bindings[{idx}].type='datetime' is "
            f"not supported by mock_data_wizard (extract/summarize/generate "
            f"only handle {sorted(COLUMN_TYPES)!r}). Use type='date' for "
            f"date-only columns or split timestamps into separate date + "
            f"time columns. End-to-end datetime support is a separate "
            f"workstream from §15 step 4."
        )
    display_name = data.get("display_name")
    if not display_name:
        raise ValueError(
            f"sources[{source_name!r}].bindings[{idx}] is missing "
            f"display_name; every binding must carry display_name in "
            f"step 4 (reg_meta resolution of defaults lands in §15 step 6)"
        )
    return Binding(
        variable=data["variable"],
        type=data["type"],
        display_name=display_name,
        id_subtype=data.get("id_subtype"),
        numeric_subtype=data.get("numeric_subtype"),
        date_format=data.get("date_format"),
        value_set=data.get("value_set"),
    )


def _build_source(data: Mapping[str, Any], *, idx: int) -> Source:
    name = data["name"]
    bindings = tuple(
        _build_column(c, source_name=name, idx=i)
        for i, c in enumerate(data.get("bindings", []))
    )
    return Source(
        name=name,
        register_variant=data["register_variant"],
        period=data["period"],
        bindings=bindings,
    )


def _reject_composite(
    panel_id: str, member_idx: int | None, field_name: str, value: object
) -> None:
    """Reject list/tuple-shaped composite keys with a clear step-10b message."""
    if isinstance(value, (list, tuple)):
        member = "" if member_idx is None else f".members[{member_idx}]"
        raise ValueError(
            f"panels[{panel_id!r}]{member}.{field_name} is composite; "
            f"runtime support for composite entity_key / time_key lands "
            f"in §15 step 10b. Step 4 supports scalar keys only."
        )


def _reject_literal_period(
    panel_id: str, member_idx: int | None, value: object
) -> None:
    """Reject the ``{"period": ...}`` time_key form (LiteralPeriod)."""
    if isinstance(value, dict):
        member = "" if member_idx is None else f".members[{member_idx}]"
        raise ValueError(
            f"panels[{panel_id!r}]{member}.time_key uses the "
            f"{{'period': ...}} literal form; LiteralPeriod runtime "
            f"support lands in §15 step 10b. Step 4 accepts int or "
            f"str (column ref) only."
        )


def _build_panel_member(
    member: Mapping[str, Any] | str, *, panel_id: str, idx: int
) -> PanelMember:
    # §6.4 bare-string shorthand: a string member names the source and relies
    # on panel-level / variant-inherited key defaults. reg_schema's structural
    # layer now accepts it (effective-key *presence* is a reg_meta concern,
    # §6.8.1), so it can reach here; the step-4 runtime doesn't resolve
    # inheritance, so normalize to the object form and let the time_key check
    # below raise the actionable ValueError — not an AttributeError on a str.
    data: Mapping[str, Any] = {"source": member} if isinstance(member, str) else member
    entity_key = data.get("entity_key")
    time_key = data.get("time_key")
    _reject_composite(panel_id, idx, "entity_key", entity_key)
    _reject_composite(panel_id, idx, "time_key", time_key)
    _reject_literal_period(panel_id, idx, time_key)
    if entity_key is not None:
        # The new schema allows per-member entity_key overrides; the old
        # runtime didn't, and adding the resolver here would bleed step
        # 4 scope. Reject with a clear deferral pointer.
        raise ValueError(
            f"panels[{panel_id!r}].members[{idx}].entity_key override is "
            f"not supported in step 4; set entity_key at the panel level. "
            f"Per-member overrides land in §15 step 10b."
        )
    if time_key is None:
        # Old runtime required every member to carry time_key; panel-level
        # defaults aren't resolved at this layer until step 10b.
        raise ValueError(
            f"panels[{panel_id!r}].members[{idx}] is missing time_key; "
            f"panel-level time_key defaults are not resolved by the "
            f"step-4 runtime — set time_key on each member."
        )
    return PanelMember(source=data["source"], time_key=time_key)


def _build_panel(data: Mapping[str, Any]) -> Panel:
    panel_id = data["panel_id"]
    entity_key = data.get("entity_key")
    time_key = data.get("time_key")
    _reject_composite(panel_id, None, "entity_key", entity_key)
    _reject_composite(panel_id, None, "time_key", time_key)
    _reject_literal_period(panel_id, None, time_key)
    if not isinstance(entity_key, str) or not entity_key:
        raise ValueError(
            f"panels[{panel_id!r}].entity_key must be a non-empty string "
            f"in step 4 (composite entity_key support lands in §15 step 10b)"
        )
    if time_key is not None:
        # Panel-level time_key default isn't honored by the step-4
        # runtime; require members to set their own.
        raise ValueError(
            f"panels[{panel_id!r}].time_key (panel-level default) is "
            f"not honored by the step-4 runtime; set time_key on each "
            f"member instead. Panel-level defaults land in §15 step 10b."
        )
    members = tuple(
        _build_panel_member(m, panel_id=panel_id, idx=i)
        for i, m in enumerate(data.get("members", []))
    )
    return Panel(
        panel_id=panel_id,
        entity_key=entity_key,
        members=members,
        comment=data.get("comment"),
    )


def _build_project_data(payload: Mapping[str, Any]) -> ProjectData:
    sources = tuple(
        _build_source(s, idx=i) for i, s in enumerate(payload.get("sources", []))
    )
    panels = tuple(_build_panel(p) for p in payload.get("panels", []))
    return ProjectData(
        schema_version=payload["schema_version"],
        steward=payload["steward"],
        reg_meta_version=payload["reg_meta_version"],
        name=payload["name"],
        sources=sources,
        panels=panels,
        reg_monabundle=payload.get("reg_monabundle"),
    )


def _validate_column_options_against_columns(
    block: object, project_data: ProjectData
) -> None:
    """Cross-check ``column_options`` keys against actual columns.

    Two checks, both requiring access to the resolved column dataclasses:

    1. **Orphan keys.** Well-formedness (5-segment, non-class,
       ``[A-Za-z0-9_-]+``) is checked in
       ``reg_monabundle.validate_block``; that catches typos that
       mangle the shape but not typos where the shape survives and
       the key just doesn't match any column. Without this check, a
       misspelled FQID silently no-ops at lookup time.

    2. **Per-option type compatibility.** ``suppress_k`` only feeds
       ``_suppress_below_k`` (categorical frequency cutoff) in
       ``summarize_column``; the id / numeric / date / opaque
       branches ignore it. Accepting it on those types would silently
       no-op the same way an orphan FQID would. Future panel-level
       k-anonymity tunability lives at ``panels[*].suppress_k`` (not
       yet implemented), not here.
    """
    if not isinstance(block, dict):
        return
    block_obj = cast("Mapping[str, Any]", block)
    options = block_obj.get("column_options")
    if not isinstance(options, dict):
        return
    columns_by_fqid = {
        binding.variable: binding
        for source in project_data.sources
        for binding in source.bindings
    }
    orphans = sorted(set(options) - set(columns_by_fqid))
    if orphans:
        raise ValueError(
            f"reg_monabundle.column_options has key(s) that don't match "
            f"any binding FQID in sources: {orphans}. Check for typos "
            f"against the binding FQIDs declared in sources[*].bindings[*].variable."
        )
    for fqid, opts in options.items():
        binding = columns_by_fqid[fqid]
        if "suppress_k" in opts and binding.type != "categorical":
            raise ValueError(
                f"reg_monabundle.column_options[{fqid!r}].suppress_k is "
                f"only honored on categorical columns (this binding has "
                f"type={binding.type!r}). The runtime applies suppress_k "
                f"to the categorical frequency cutoff only — setting it "
                f"on id/numeric/date/opaque is a no-op. For panel-level "
                f"k-anonymity tunability see panels[*].suppress_k (not "
                f"yet implemented)."
            )


def parse_project_data(payload: Mapping[str, Any]) -> LoadedSpec:
    """Validate + construct a ``LoadedSpec`` from a parsed JSON dict."""
    result = validate_structural(payload)
    if not result.ok:
        errors = [
            f"{issue.code} @ {issue.path}: {issue.message}"
            for issue in result.issues
            if issue.level == "error"
        ]
        raise ValueError(
            f"{PROJECT_DATA_FILENAME} failed structural validation:\n  - "
            + "\n  - ".join(errors)
        )
    validate_block(payload.get("reg_monabundle"))
    project_data = _build_project_data(payload)
    _validate_column_options_against_columns(
        payload.get("reg_monabundle"), project_data
    )
    return LoadedSpec(project_data)


def load_project_data(directory: Path) -> LoadedSpec | None:
    """Load ``project_data.json`` from ``directory`` if present.

    Returns ``None`` when the file is absent. Raises on duplicate JSON
    keys, structural validation failures, or namespaced-block violations.
    """
    path = Path(directory) / PROJECT_DATA_FILENAME
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as fp:
        payload = json.load(fp, object_pairs_hook=_reject_duplicate_keys)
    if not isinstance(payload, dict):
        raise ValueError(
            f"{PROJECT_DATA_FILENAME}: top-level value must be an object, "
            f"got {type(payload).__name__}"
        )
    return parse_project_data(payload)
