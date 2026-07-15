"""project_data.json schema models (see DESIGN.md → Two layers: models vs. validator).

Pure shape definitions, not validators. Pydantic v2 ``BaseModel`` —
``reg_schema`` is the deliberate exception to the workspace no-Pydantic
rule (``CLAUDE.md`` stack §): these models are the canonical project_data
shape, FastAPI response models in ``reg_webapp``, and the source of the
SPA's TypeScript types via ``model_json_schema()``.

Models are ``frozen`` + tuple-backed so consumers (reg_webapp and the SPA
via TS codegen) can hash, share, and pass instances freely. Pydantic
coerces list → tuple for ``tuple[...]`` fields automatically, so callers
may construct from list-shaped composites without losing the frozen +
hashable contract.

JSON deserialization and structural validation are deliberately
separate concerns:

- Structural rules (see DESIGN.md → Structural rules and issue codes — type/subtype consistency, FQID
  well-formedness, panel ordering, period grammar, etc.) live in
  ``validate_structural()`` and run on the **raw dict** before any
  model is constructed. They accumulate every issue into a
  ``ValidationResult`` rather than raising, so every runtime that shares
  the contract (SPA via TS mirror, webapp via direct import) sees the full
  issue list. The models intentionally do NOT re-encode those rules as
  raising field validators — that would replace the issue-accumulating
  contract with a fail-fast one.
- Models are constructed at boundaries (API ingress) from data that
  already passed ``validate_structural``. A Pydantic raise at that point
  signals validator/model drift, not user error.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# Top-level enums (see DESIGN.md → Two layers: models vs. validator). Mirrored at runtime by the structural
# validator using ``get_args`` — same drift-protection pattern as
# ``IssueLevel`` in ``validation.py``.
Steward = Literal["global", "ifau", "swecov"]
ColumnType = Literal["id", "categorical", "numeric", "date", "datetime", "opaque"]
IdSubtype = Literal["integer", "string"]
NumericSubtype = Literal["integer", "double"]


class _Model(BaseModel):
    """Shared config: frozen (immutable + hashable) and extra-forbidding.

    ``extra="forbid"`` makes a typo in a constructed model fail loudly
    instead of dropping into defaults — the same drift guard the IR
    models use (``reg_meta_build.ir``).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")


# Period -------------------------------------------------------------


class PeriodRange(_Model):
    """The ``{"from": ..., "to": ...}`` range form of ``Source.period``.

    Endpoints follow the same int / period-token-string forms as a bare
    period. ``from`` is a Python keyword, so the field is ``from_`` with a
    ``"from"`` alias; ``populate_by_name`` lets callers use either.

    This bare object is **only** legal as a ``Source.period`` value; a
    ``TimePoint`` range uses the discriminated ``TimeRange`` wrapper
    (``{"range": {...}}``) so ``TimeKey``'s union stays unambiguous.

    ``serialize_by_alias=True`` so ``model_dump()`` emits ``"from"`` (not the
    Python-safe ``"from_"``) without every caller having to pass
    ``by_alias=True`` — the un-aliased key would fail re-validation
    (``_is_period_range_obj`` requires exactly ``{"from", "to"}``).
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        populate_by_name=True,
        serialize_by_alias=True,
    )

    from_: int | str = Field(alias="from")
    to: int | str


# One contiguous piece of a ``Source.period``: bare year, period-token string,
# or explicit range. The ``"_default"`` snapshot sentinel is a plain string at
# the top level only — it is NOT a legal list member (structural rule).
PeriodSegment = int | str | PeriodRange

# ``Source.period``: a single segment, the ``"_default"`` sentinel (rides the
# ``str`` arm), or a LIST of segments — an interrupted series (#307, e.g.
# ``[{"from": 2005, "to": 2010}, {"from": 2015, "to": 2020}]``). The list form
# keeps one source = one register extraction (panel keys / binding sets are not
# duplicated across pseudo-sources). Structural rules for the list: non-empty,
# members are segments (no ``_default``, no nesting), sorted ascending and
# non-overlapping (adjacency allowed — the wire form stays canonical). Always
# required.
Period = PeriodSegment | tuple[PeriodSegment, ...]


# Binding ------------------------------------------------------------


class Binding(_Model):
    """A binding on a Source — one variable to include in the extract.

    ``variable`` is the binding FQID: ``<provider>/<register>/<slug>`` (3
    segments, see reg_meta/DESIGN.md → FQID grammar). Its ``provider/register`` prefix (first 2 segments) must
    equal the source's ``register_variant`` prefix — the variant is NOT
    repeated here, it lives once on the Source. That cross-field rule
    is enforced by the structural validator. There is no ``@version`` pin —
    that grammar is retired.

    A FQID names one CONCEPT. The reg_meta build enforces one value set per
    ``(variable, variant, period, delivery_column)``, but a concept may carry
    several co-existing delivery columns — parallel REPRESENTATIONS of it (SSYK
    3/4/5-digit, age 5/10-yr brackets). ``representation`` selects which one (by
    its ``variable_alias.delivery_column_name``); it is required only when the
    concept resolves to >1 column at the source's ``(variant, period)`` — the
    semantic validator (see reg_webapp/DESIGN.md → Semantic validation (semantic.py)) flags an ambiguous binding that omits it, and the
    SPA offers a chooser. A single-representation concept leaves it ``None``.

    ``display_name`` is optional: when absent, reg_meta-backed consumers
    resolve the default from ``variable_alias.delivery_column_name`` for
    the binding's state at the source's ``(register_variant, period)``. A
    reg_meta-free consumer that materializes data artifacts must resolve
    the default itself before emitting them — it never carries an
    unresolved ``display_name`` into its output.
    """

    variable: str
    type: ColumnType
    display_name: str | None = None
    id_subtype: IdSubtype | None = None
    numeric_subtype: NumericSubtype | None = None
    date_format: str | None = None
    datetime_format: str | None = None
    value_set: str | None = None
    representation: str | None = None


class Source(_Model):
    """A data source / table in the spec.

    ``register_variant`` is the 3-part variant **coordinate**
    (``<provider>/<register>/<variant>``) — not an FQID kind (see reg_meta/DESIGN.md → FQID grammar), but
    the same 3-part grammar. ``period`` is always required and polymorphic
    (``Period``). Together ``(register_variant's variant, period)`` selects
    each binding variable's ``variable_state``. ``name`` is the internal
    source handle referenced by panel members; it is not an FQID.
    """

    name: str
    register_variant: str
    period: Period
    bindings: tuple[Binding, ...]


# Panel ---------------------------------------------------------------
#
# Type aliases:
#
#   EntityKey = string | string[]                          // always column refs
#   TimePoint = int | string | LiteralPeriod | TimeRange   // string is column ref
#   TimeKey   = TimePoint | TimePoint[]
#
# Bare strings in panel keys are *always* column refs against a source's
# binding ``display_name`` values; literal string-shaped periods (e.g.
# ``"2018-01"``, ``"HT2018"``) must use the ``LiteralPeriod`` object form,
# and ranges the ``TimeRange`` wrapper. Integer literals stay as plain ints.


class LiteralPeriod(_Model):
    """The ``{"period": int | string}`` time_key form.

    The only way to express a string-shaped literal period at the
    schema level. Disambiguates ``"2018"`` (column ref) from
    ``{"period": "2018-01"}`` (literal period).
    """

    period: int | str


class TimeRange(_Model):
    """The ``{"range": {"from": ..., "to": ...}}`` time_key form.

    The discriminated wrapper for a period range in ``TimeKey`` position —
    distinct from the bare ``{"from", "to"}`` object, which is legal only
    as a ``Source.period`` (``PeriodRange``). The wrapper keeps the
    ``TimePoint`` union unambiguous.
    """

    range: PeriodRange


TimePoint = int | str | LiteralPeriod | TimeRange
TimeKey = TimePoint | tuple[TimePoint, ...]
EntityKey = str | tuple[str, ...]


class PanelMember(_Model):
    """A member of a Panel.

    ``source`` is the source ``name`` (the panel layer joins on
    delivered-data column headers, not FQIDs). ``entity_key`` /
    ``time_key`` override panel-level defaults; when both panel and member
    leave a key unset, it is inherited from the member's variant's
    ``panel_template`` at kit/bundle-build time — the structural
    validator does not flag the absence (it has no reg_meta).
    """

    source: str
    entity_key: EntityKey | None = None
    time_key: TimeKey | None = None


class Panel(_Model):
    """A panel definition over sources.

    Members are stored uniformly as ``PanelMember``. The bare-string
    shorthand (a source name with panel-level key defaults) is normalized
    to ``PanelMember(source=<name>)`` by the ``members`` validator, so
    consumers never branch on ``str | PanelMember``. Source-collision (each
    source belongs to at most one panel) and composite ordering /
    homogeneity rules are enforced by the structural validator, not here.
    """

    panel_id: str
    members: tuple[PanelMember, ...]
    entity_key: EntityKey | None = None
    time_key: TimeKey | None = None
    comment: str | None = None

    @field_validator(
        "members",
        mode="before",
        json_schema_input_type=tuple[str | PanelMember, ...],
    )
    @classmethod
    def _normalize_member_shorthand(cls, value: object) -> object:
        # A bare-string member is the source name. Expand it to the
        # object form before per-element validation; dicts and already-built
        # PanelMember instances pass through untouched.
        if isinstance(value, (list, tuple)):
            return [{"source": m} if isinstance(m, str) else m for m in value]
        return value


# Study window --------------------------------------------------------


class StudyWindow(_Model):
    """The optional ``{"from": <year>, "to": <year>}`` project study window.

    The global "project window" the redesigned subject page defaults each
    page's period picker to (see issue #611 → Period model). Deliberately
    NOT the full ``Period`` / ``PeriodRange`` grammar: it is a plain
    year-int pair, matching the year-granular header slider. Per-page
    deviation (months/quarters/terms, interrupted segments) keeps the rich
    grammar via ``?period``; this window only seeds the default.

    ``from`` is a Python keyword, so the field is ``from_`` with a ``"from"``
    alias — mirroring ``PeriodRange``. ``serialize_by_alias=True`` so
    ``model_dump()`` emits ``"from"`` (not ``"from_"``) without callers
    passing ``by_alias=True``.

    Endpoints are plain ``int`` years (not the ``int | str`` period-token
    forms of ``PeriodRange``): the window is year-granular by design. The
    only invariant is ``to >= from`` — a same-year window (``from == to``)
    is valid; a window can't end before it starts.
    """

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        populate_by_name=True,
        serialize_by_alias=True,
    )

    from_: int = Field(alias="from")
    to: int

    @model_validator(mode="after")
    def _check_order(self) -> StudyWindow:
        if self.to < self.from_:
            raise ValueError(
                f"study window 'to' ({self.to}) must be >= 'from' ({self.from_})"
            )
        return self


# Top-level shape -----------------------------------------------------


class ProjectData(_Model):
    """The top-level ``project_data.json`` shape.

    The root is closed like every nested ``_Model``: an unknown field is a
    model-construction error. API boundaries still run the accumulating
    structural validator first so callers receive one stable
    ``unexpected_field`` issue per unknown key rather than a fail-fast Pydantic
    error. A future extension needs an explicit modeled container; arbitrary
    namespaced root blocks are not part of the v1 contract.
    """

    schema_version: str
    steward: Steward
    reg_meta_version: str
    name: str
    sources: tuple[Source, ...]
    panels: tuple[Panel, ...] = ()
    # Optional global study window (see issue #611 → Period model). Absent =
    # full history; existing specs validate unchanged (additive surface).
    window: StudyWindow | None = None
