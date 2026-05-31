"""project_data.json schema models (REFACTOR_SPEC.md §6.1-§6.4).

Pure shape definitions, not validators. Pydantic v2 ``BaseModel`` —
``reg_schema`` is the deliberate exception to the workspace no-Pydantic
rule (``CLAUDE.md`` stack §): these models are the canonical project_data
shape, FastAPI response models in ``reg_webapp``, and the source of the
SPA's TypeScript types via ``model_json_schema()``.

Models are ``frozen`` + tuple-backed so consumers (mock_data_wizard,
reg_webapp, reg_monabundle, the SPA via TS codegen) can hash, share, and
pass instances freely. Pydantic coerces list → tuple for ``tuple[...]``
fields automatically, so callers may construct from list-shaped
composites without losing the frozen + hashable contract.

JSON deserialization and structural validation are deliberately
separate concerns:

- Structural rules (§6.8.1 — type/subtype consistency, FQID
  well-formedness, panel ordering, period grammar, etc.) live in
  ``validate_structural()`` and run on the **raw dict** before any
  model is constructed. They accumulate every issue into a
  ``ValidationResult`` rather than raising, so the three runtimes that
  share the contract (SPA, MONA bundle, webapp) all see the full issue
  list. The models intentionally do NOT re-encode those rules as
  raising field validators — that would replace the issue-accumulating
  contract with a fail-fast one.
- Models are constructed at boundaries (API ingress, bundle build) from
  data that already passed ``validate_structural``. A Pydantic raise at
  that point signals validator/model drift, not user error.

On MONA the models are NOT used: bundle-build converts a validated
``Source`` into a stdlib-dataclass ``LoadedSpec`` (reg_monabundle, A3.4)
that the bundle amalgamates instead — Pydantic never ships to MONA
(REFACTOR_SPEC §9.6).
"""

from __future__ import annotations

# Runtime import (not TYPE_CHECKING): Pydantic resolves field annotations at
# model-build time, so `Mapping` must be in the module namespace.
from collections.abc import Mapping  # noqa: TC003
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Top-level enums (§6.1, §6.3). Mirrored at runtime by the structural
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
    models use (``reg_meta_build.ir``). ``ProjectData`` relaxes this for
    namespaced blocks; see its docstring.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")


# §6.2 Period -------------------------------------------------------------


class PeriodRange(_Model):
    """The ``{"from": ..., "to": ...}`` range form of ``Source.period`` (§6.2).

    Endpoints follow the same int / period-token-string forms as a bare
    period. ``from`` is a Python keyword, so the field is ``from_`` with a
    ``"from"`` alias; ``populate_by_name`` lets callers use either.

    This bare object is **only** legal as a ``Source.period`` value; a
    ``TimePoint`` range uses the discriminated ``TimeRange`` wrapper
    (``{"range": {...}}``) so ``TimeKey``'s union stays unambiguous (§6.2).
    """

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)

    from_: int | str = Field(alias="from")
    to: int | str


# ``Source.period`` (§6.2): bare year, period-token string, explicit range,
# or the ``"_default"`` snapshot sentinel (a plain string). Always required.
Period = int | str | PeriodRange


# §6.3 Binding ------------------------------------------------------------


class Binding(_Model):
    """A binding on a Source (§6.3) — one variable to include in the extract.

    ``variable`` is the binding FQID: ``<provider>/<register>/<slug>`` (3
    segments, §5.2), optionally suffixed ``@<value-set-version>`` to pin
    one of several value-set versions co-delivered in the bound period
    (e.g. ``scb/lisa/naringsgren@sni2007``). Its ``provider/register``
    prefix (first 2 segments) must equal the source's ``register_variant``
    prefix — the variant is NOT repeated here, it lives once on the Source
    (§6.2). That cross-field rule is enforced by the structural validator.

    ``display_name`` is optional: when absent, reg_meta-backed consumers
    resolve the default from ``variable_alias.delivery_column_name`` for
    the binding's state at the source's ``(register_variant, period)``.
    Reg_meta-free consumers (bundle on MONA, kit runs) never see an
    unresolved ``display_name`` — bundle build and kit build materialize
    defaults before emitting their artifacts.
    """

    variable: str
    type: ColumnType
    display_name: str | None = None
    id_subtype: IdSubtype | None = None
    numeric_subtype: NumericSubtype | None = None
    date_format: str | None = None
    datetime_format: str | None = None
    value_set: str | None = None


class Source(_Model):
    """A data source / table in the spec (§6.2).

    ``register_variant`` is the 3-part variant **coordinate**
    (``<provider>/<register>/<variant>``) — not an FQID kind (§5.2), but
    the same 3-part grammar. ``period`` is always required and polymorphic
    (``Period``). Together ``(register_variant's variant, period)`` selects
    each binding variable's ``variable_state``. ``name`` is the internal
    source handle referenced by panel members; it is not an FQID.
    """

    name: str
    register_variant: str
    period: Period
    bindings: tuple[Binding, ...]


# §6.4 Panel ---------------------------------------------------------------
#
# Type aliases follow REFACTOR_SPEC.md §6.4:
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
    """The ``{"period": int | string}`` time_key form (§6.4).

    The only way to express a string-shaped literal period at the
    schema level. Disambiguates ``"2018"`` (column ref) from
    ``{"period": "2018-01"}`` (literal period).
    """

    period: int | str


class TimeRange(_Model):
    """The ``{"range": {"from": ..., "to": ...}}`` time_key form (§6.4).

    The discriminated wrapper for a period range in ``TimeKey`` position —
    distinct from the bare ``{"from", "to"}`` object, which is legal only
    as a ``Source.period`` (``PeriodRange``). The wrapper keeps the
    ``TimePoint`` union unambiguous (§6.2).
    """

    range: PeriodRange


TimePoint = int | str | LiteralPeriod | TimeRange
TimeKey = TimePoint | tuple[TimePoint, ...]
EntityKey = str | tuple[str, ...]


class PanelMember(_Model):
    """A member of a Panel (§6.4).

    ``source`` is the source ``name`` (the panel layer joins on
    delivered-data column headers, not FQIDs). ``entity_key`` /
    ``time_key`` override panel-level defaults; when both panel and member
    leave a key unset, it is inherited from the member's variant's
    ``panel_template`` (§6.4) at kit/bundle-build time — the structural
    validator does not flag the absence (it has no reg_meta).
    """

    source: str
    entity_key: EntityKey | None = None
    time_key: TimeKey | None = None


class Panel(_Model):
    """A panel definition over sources (§6.4).

    Members are stored uniformly as ``PanelMember``. The §6.4 bare-string
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

    @field_validator("members", mode="before")
    @classmethod
    def _normalize_member_shorthand(cls, value: object) -> object:
        # §6.4: a bare-string member is the source name. Expand it to the
        # object form before per-element validation; dicts and already-built
        # PanelMember instances pass through untouched.
        if isinstance(value, (list, tuple)):
            return [{"source": m} if isinstance(m, str) else m for m in value]
        return value


# §6.1 Top-level shape -----------------------------------------------------


class ProjectData(_Model):
    """The top-level ``project_data.json`` shape (§6.1).

    ``reg_monabundle`` (and any other namespaced block added later) is
    intentionally typed as an opaque ``Mapping``: §6.8.2 delegates
    validation of namespaced blocks to their owning package, and
    reg_schema's frozen-ness does not deep-freeze nested mappings. That is
    by design — namespaced consumers own their payload lifecycle.

    ``extra="ignore"`` (overriding ``_Model``'s ``forbid``) tolerates
    additional steward-namespaced blocks (``swecov``, ``reg_mockdata``,
    …) without modeling them as fields: they ride through on the dict side
    and are handled by the owning package (§6.8.2), exactly as the v0.x
    dataclass did. If a field is wanted for one, we add it deliberately
    rather than growing an ``extras`` dict.

    Because the ``reg_monabundle`` block is an opaque (typically
    unhashable) ``Mapping``, an instance carrying one is unhashable on
    demand. No consumer hashes ``ProjectData`` (the bundle wraps it in a
    ``LoadedSpec``; the webapp serializes it), so ``frozen=True`` is kept
    for value-immutability and ``__eq__`` without a custom ``__hash__``.
    """

    model_config = ConfigDict(frozen=True, extra="ignore")

    schema_version: str
    steward: Steward
    reg_meta_version: str
    name: str
    sources: tuple[Source, ...]
    panels: tuple[Panel, ...] = ()
    reg_monabundle: Mapping[str, object] | None = None
