"""Runtime deserializer: ``project_data.json`` dict -> ``LoadedSpec``.

This module is the **bundle-runtime** side of the §9.6 boundary. It is
amalgamated into the MONA bundle, so it carries **no Pydantic and no
``reg_schema``** — only stdlib + the sibling runtime modules. It reads
an already-validated ``project_data.json`` dict (REFACTOR_SPEC.md §6),
deserializes it into stdlib ``@dataclass`` instances, and exposes the
``(source_name, display_name)``-keyed lookup surface that extract /
sources / summarize consume.

Structural validation does **not** run here. Per §6.8.1 + §9.6, the
bundle on MONA trusts its embedded (or sidecar) JSON: the full Pydantic
``reg_schema`` structural validator runs once at **bundle-build time**
(``reg_monabundle.build.spec_loader.validate_project_data``), which is
the gate that refuses to amalgamate a structurally broken spec. The
bundle is a build artifact, not an authoring surface — if a researcher
hand-edits the embedded JSON on MONA in a way that breaks dataclass
deserialization, it errors at load with a stdlib exception, by design.

``ProjectData`` here is the runtime adapter shape — a minimal frozen
dataclass tree carrying only the fields the on-MONA pipeline reads.
``LoadedSpec`` indexes it for O(1) lookups by SQL column header
(``display_name``) and owns the per-source mutable override cache that
``sources._probe_and_promote_opaque`` writes into. The frozen dataclass
tree cannot host that mutation; the cache is the only mutation path.

Step 4 capability gates (REFACTOR_SPEC.md §15) — these are *runtime*
rejections, NOT structural validation:

- Composite ``entity_key`` / ``time_key`` (tuple-shaped) and
  ``LiteralPeriod`` (``{"period": ...}``) are rejected at deserialize
  with a clear "step 10b" message — the schema accepts them but the
  extract/generate runtime is scalar-only until then.
- Panel-level ``time_key`` defaults and per-member ``entity_key``
  overrides are rejected the same way — the old runtime supported
  neither, and adding the resolver here would bleed scope.
- Every column must carry a ``display_name``. The schema marks it
  optional because the webapp materializes defaults from reg_meta at
  bundle-build time (§6.3 + §7); that pipeline lands in §15 step 6.

The cross-block referential checks (orphan FQID,
suppress_k-on-non-categorical) are **not** here — they need FQID-typed
bindings and run at bundle-build time in
``reg_monabundle.build.spec_loader``. The bundle trusts the embedded
JSON and does not re-check them (§9.6).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

from reg_monabundle import validate_block

from .classify import COLUMN_TYPES

PROJECT_DATA_FILENAME = "project_data.json"

# Per-type inline hint keys. Mirrors mdw's COLUMN_TYPES exactly —
# reg_schema's ``datetime`` is rejected at deserialize (see
# ``_build_column``) because the mdw extract/summarize/generate stack
# has no datetime branch; until end-to-end datetime support lands,
# accepting it here would surface as a late ``ValueError`` from
# ``sql_emit``.
INLINE_HINT_KEYS: dict[str, tuple[str, ...]] = {
    "id": ("id_subtype",),
    "numeric": ("numeric_subtype",),
    "date": ("date_format",),
    "categorical": (),
    "opaque": (),
}
assert set(INLINE_HINT_KEYS) == set(COLUMN_TYPES)


# -- Runtime spec dataclasses ---------------------------------------------
#
# Minimal frozen dataclasses carrying ONLY the fields the on-MONA
# pipeline reads. Deliberately leaner than ``reg_schema`` (no
# register_variant / period / value_set / datetime_format /
# PanelMember.entity_key): those are authoring-side fields the runtime
# never touches, and the bundle's 1 MB budget rules out carrying them.


@dataclass(frozen=True)
class Binding:
    """A bound delivery column: the runtime fields of a ``reg_schema.Binding``.

    ``display_name`` is the SQL header the runtime keys on; ``variable``
    is the binding FQID used to resolve ``reg_monabundle.column_options``.
    ``value_set`` / ``datetime_format`` are dropped — unused at runtime
    (datetime is rejected at deserialize, so no ``datetime_format``
    binding can reach here).
    """

    variable: str
    type: str
    display_name: str | None = None
    id_subtype: str | None = None
    numeric_subtype: str | None = None
    date_format: str | None = None


@dataclass(frozen=True)
class Source:
    name: str
    bindings: tuple[Binding, ...] = ()


@dataclass(frozen=True)
class PanelMember:
    source: str
    time_key: object = None


@dataclass(frozen=True)
class Panel:
    panel_id: str
    entity_key: str
    members: tuple[PanelMember, ...] = ()


@dataclass(frozen=True)
class ProjectData:
    sources: tuple[Source, ...] = ()
    panels: tuple[Panel, ...] = ()
    reg_monabundle: Mapping[str, Any] | None = None


# -- Runtime convenience dataclass ----------------------------------------


@dataclass(frozen=True)
class ColumnTypeOverride:
    """A per-binding type assignment with optional inline subtype/format hints.

    Built from a runtime ``Binding`` at load time. Lives in ``spec.py``
    rather than ``reg_schema`` because it's a runtime artifact —
    ``sources._probe_and_promote_opaque`` mutates a per-source copy when
    promoting opaque columns, and the spec dataclass tree is frozen by
    design.

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
        return cls(
            type=binding.type,
            id_subtype=binding.id_subtype,
            numeric_subtype=binding.numeric_subtype,
            date_format=binding.date_format,
        )


# -- LoadedSpec adapter ---------------------------------------------------


class LoadedSpec:
    """Runtime adapter over a deserialized ``ProjectData``.

    Indexes sources by name and columns by ``(source_name, display_name)``.
    Exposes the lookup surface ``extract`` / ``sources`` / ``summarize``
    consume, so the migration off ``MDWConfig`` doesn't bleed into
    every caller.

    Owns the per-source mutable override cache (``_type_cache``) so
    ``sources._probe_and_promote_opaque`` has a place to write
    promotions — the spec dataclass tree itself is frozen.
    """

    def __init__(self, project_data: ProjectData) -> None:
        self.project_data = project_data
        self._sources_by_name: dict[str, Source] = {
            s.name: s for s in project_data.sources
        }
        self._columns_by_display: dict[tuple[str, str], Binding] = {}
        for source in project_data.sources:
            for binding in source.bindings:
                # display_name is required at deserialize (asserted in
                # _build_column). The assert silences the type checker
                # about the dataclass's Optional[str].
                assert binding.display_name is not None
                self._columns_by_display[(source.name, binding.display_name)] = binding
        self._type_cache: dict[str, dict[str, ColumnTypeOverride]] = {}
        block = project_data.reg_monabundle or {}
        raw_options = block.get("column_options") or {}
        # The build-time validator pinned the inner shape, so a plain
        # dict access is enough at runtime.
        self._column_options: Mapping[str, Mapping[str, Any]] = raw_options

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


# -- Dataclass deserialization (trusts validated input) -------------------


def _build_column(data: Mapping[str, Any], *, source_name: str, idx: int) -> Binding:
    """Deserialize a runtime ``Binding`` from a JSON dict, requiring display_name.

    The schema marks ``display_name`` optional because the webapp
    pre-resolves defaults from reg_meta at bundle-build time
    (REFACTOR_SPEC.md §6.3 + §7). ``mock_data_wizard`` runs without
    reg_meta in the extract loop, so every binding must already carry a
    ``display_name``. The bundle pre-resolve pipeline lands at §15
    step 6; until then, hand-write ``display_name`` on every binding.

    Also rejects ``type == "datetime"``: reg_schema accepts datetime
    bindings but the mdw extract/summarize/generate stack has no
    datetime branch (``classify.COLUMN_TYPES`` and
    ``sql_emit.queries_for_column``). Rejecting at deserialize surfaces
    the error here instead of as a late ``ValueError`` deep in extract.
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
    )


def _build_source(data: Mapping[str, Any], *, idx: int) -> Source:
    name = data["name"]
    bindings = tuple(
        _build_column(c, source_name=name, idx=i)
        for i, c in enumerate(data.get("bindings", []))
    )
    return Source(name=name, bindings=bindings)


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
    # layer accepts it (effective-key *presence* is a reg_meta concern,
    # §6.8.1), and the runtime builds from the raw JSON dict where members may
    # still be bare strings, so normalize to the object form and let the
    # time_key check below raise the actionable ValueError — not an
    # AttributeError on a str.
    data: Mapping[str, Any] = {"source": member} if isinstance(member, str) else member
    entity_key = data.get("entity_key")
    time_key = data.get("time_key")
    _reject_composite(panel_id, idx, "entity_key", entity_key)
    _reject_composite(panel_id, idx, "time_key", time_key)
    _reject_literal_period(panel_id, idx, time_key)
    if entity_key is not None:
        # The schema allows per-member entity_key overrides; the old
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
    )


def _build_project_data(payload: Mapping[str, Any]) -> ProjectData:
    sources = tuple(
        _build_source(s, idx=i) for i, s in enumerate(payload.get("sources", []))
    )
    panels = tuple(_build_panel(p) for p in payload.get("panels", []))
    return ProjectData(
        sources=sources,
        panels=panels,
        reg_monabundle=payload.get("reg_monabundle"),
    )


def loadedspec_from_dict(payload: Mapping[str, Any]) -> LoadedSpec:
    """Deserialize a ``LoadedSpec`` from a parsed JSON dict (bundle-load path).

    No **structural** validation runs here (§6.8.1 / §9.6): the bundle
    trusts its embedded / sidecar JSON because bundle-build already ran the
    Pydantic structural gate via
    ``reg_monabundle.build.spec_loader.validate_project_data``. But the
    **§6.8.2 namespaced-block validator** (``validate_block`` — option keys +
    the suppress_k floor) IS pure-stdlib and runs at bundle LOAD time on MONA
    too (same code, amalgamated), so it is re-checked here. The step-4 runtime
    capability gates in ``_build_*`` likewise still raise on shapes the on-MONA
    pipeline can't execute.
    """
    validate_block(payload.get("reg_monabundle"))
    return LoadedSpec(_build_project_data(payload))


def load_project_data(directory: Path) -> LoadedSpec | None:
    """Load ``project_data.json`` from ``directory`` if present.

    Returns ``None`` when the file is absent. Raises on duplicate JSON
    keys or runtime-capability rejections (composite keys, datetime,
    missing display_name, …). Does **not** structurally re-validate:
    the sidecar file is trusted input on MONA (§9.6) — structural
    validation is the bundle-build gate.
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
    return loadedspec_from_dict(payload)
