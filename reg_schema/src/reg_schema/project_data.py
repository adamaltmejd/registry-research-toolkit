"""project_data.json schema dataclasses (REFACTOR_SPEC.md §6.1-§6.4).

Pure shape definitions, not validators. Frozen + tuple-backed so
consumers (mock_data_wizard, reg_webapp, reg_monabundle, the SPA via
TS codegen) can hash, share, and pass instances freely.

JSON deserialization and structural validation are deliberately
separate concerns:

- Deserialization belongs at boundaries (API ingress, bundle load,
  config read). reg_schema does not ship a loader; consumers
  construct dataclasses from already-parsed dicts.
- Structural rules (§6.8.1 — type/subtype consistency, FQID
  well-formedness, panel ordering, etc.) land in a follow-up phase
  as ``validate_structural()`` returning a ``ValidationResult``.

Sequence fields are coerced from list to tuple in ``__post_init__``
so the frozen + hashable contract holds regardless of how callers
construct values. Element types are not checked here — that is the
structural validator's job; see ``DESIGN.md`` for why defensive
isinstance checks are intentionally absent.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

# Top-level enums (§6.1, §6.3). Mirrored at runtime by the structural
# validator using ``get_args`` — same drift-protection pattern as
# ``IssueLevel`` in ``validation.py``.
Steward = Literal["global", "ifau", "swecov"]
ColumnType = Literal["id", "categorical", "numeric", "date", "datetime", "opaque"]
IdSubtype = Literal["integer", "string"]
NumericSubtype = Literal["integer", "double"]


@dataclass(frozen=True)
class Column:
    """A column on a Source (§6.3).

    ``name`` is the binding FQID (5 segments,
    ``<provider>/<register>/<variant>/<period>/<variable>``); its
    first four segments must equal the source's ``register_version``.
    Cross-field rules of that kind are enforced by the structural
    validator, not at construction.

    ``display_name`` is optional: when absent, reg_meta-backed
    consumers resolve the default from ``variable_alias.kolumnnamn``
    for the binding. Reg_meta-free consumers (bundle on MONA, kit
    runs) never see unresolved ``display_name`` — bundle build and
    kit build materialize defaults before emitting their artifacts.
    """

    name: str
    type: ColumnType
    display_name: str | None = None
    id_subtype: IdSubtype | None = None
    numeric_subtype: NumericSubtype | None = None
    date_format: str | None = None
    datetime_format: str | None = None
    value_set: str | None = None


@dataclass(frozen=True)
class Source:
    """A data source / table in the spec (§6.2).

    ``register_version`` is the 4-segment register_version FQID
    (``<provider>/<register>/<variant>/<period>``). ``name`` is the
    internal source handle referenced by panel members; it is not an
    FQID.
    """

    name: str
    register_version: str
    columns: tuple[Column, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.columns, tuple):
            object.__setattr__(self, "columns", tuple(self.columns))


# §6.4 Panel ---------------------------------------------------------------
#
# Type aliases follow REFACTOR_SPEC.md §6.4:
#
#   EntityKey = string | string[]                      // always column refs
#   TimePoint = int | string | LiteralPeriod           // string is column ref
#   TimeKey   = TimePoint | TimePoint[]
#
# Bare strings in panel keys are *always* column refs against a
# source's column ``display_name`` values; literal string-shaped
# periods (e.g. ``"2018-01"``, ``"HT2018"``) must use the
# ``LiteralPeriod`` object form. Integer literals stay as plain ints.


@dataclass(frozen=True)
class LiteralPeriod:
    """The ``{"period": int | string}`` time_key form (§6.4).

    The only way to express a string-shaped literal period at the
    schema level. Disambiguates ``"2018"`` (column ref) from
    ``{"period": "2018-01"}`` (literal period).
    """

    period: int | str


TimePoint = int | str | LiteralPeriod
TimeKey = TimePoint | tuple[TimePoint, ...]
EntityKey = str | tuple[str, ...]


def _coerce_seq(value: object) -> object:
    """Coerce a list to tuple; pass other shapes through untouched.

    Keeps EntityKey / TimeKey fields hashable when callers supply
    list-shaped composites. Non-list values (scalar key, ``None``,
    already-a-tuple) pass through so the type alias unions hold.
    """

    return tuple(value) if isinstance(value, list) else value


@dataclass(frozen=True)
class PanelMember:
    """A member of a Panel (§6.4).

    ``source`` is the source ``name`` (the panel layer joins on
    delivered-data column headers, not FQIDs). ``entity_key`` /
    ``time_key`` override panel-level defaults; when both panel and
    member leave a key unset the structural validator reports the
    missing effective key.
    """

    source: str
    entity_key: EntityKey | None = None
    time_key: TimeKey | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "entity_key", _coerce_seq(self.entity_key))
        object.__setattr__(self, "time_key", _coerce_seq(self.time_key))


@dataclass(frozen=True)
class Panel:
    """A panel definition over sources (§6.4).

    Source-collision rule (each source belongs to at most one panel)
    and composite ordering / homogeneity rules are enforced by the
    structural validator, not at construction.
    """

    panel_id: str
    members: tuple[PanelMember, ...]
    entity_key: EntityKey | None = None
    time_key: TimeKey | None = None
    comment: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.members, tuple):
            object.__setattr__(self, "members", tuple(self.members))
        object.__setattr__(self, "entity_key", _coerce_seq(self.entity_key))
        object.__setattr__(self, "time_key", _coerce_seq(self.time_key))


# §6.1 Top-level shape -----------------------------------------------------


@dataclass(frozen=True)
class ProjectData:
    """The top-level ``project_data.json`` shape (§6.1).

    ``reg_monabundle`` (and any other namespaced block added later)
    is intentionally typed as an opaque ``Mapping``: §6.8.2 delegates
    validation of namespaced blocks to their owning package, and
    reg_schema's frozen-ness does not deep-freeze nested mappings.
    That is by design — namespaced consumers own their payload
    lifecycle.

    Steward-specific blocks beyond ``reg_monabundle`` (e.g.
    ``swecov``) are not modeled as fields here in v1; they ride
    through deserialization on the dict side and are handled by the
    owning package. If a field is wanted for one, we add it
    deliberately rather than growing an ``extras`` dict.
    """

    schema_version: str
    steward: Steward
    reg_meta_version: str
    name: str
    sources: tuple[Source, ...]
    panels: tuple[Panel, ...] = ()
    reg_monabundle: Mapping[str, object] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.sources, tuple):
            object.__setattr__(self, "sources", tuple(self.sources))
        if not isinstance(self.panels, tuple):
            object.__setattr__(self, "panels", tuple(self.panels))
