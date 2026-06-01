"""§16 ``?period`` / ``?variant`` query allow-list for the catalog catch-all.

The second §16 chokepoint (alongside ``catalog_fqid.validate_fqid_path``): a
thin SYNTACTIC allow-list that parses the raw ``?period=`` / ``?variant=`` query
values into the polymorphic ``reg_meta.catalog.Period`` type **before** any
reg_meta lookup — so a malformed value (SQLi probe, traversal, NUL,
percent-encoded slash) returns 422 with ZERO SQL executed and ZERO connections
opened (§16 "Server-side input-validation gates"). reg_meta's ``resolve_at`` /
``_period_bounds`` is the SEMANTIC backstop; this layer is purely about refusing
non-grammar input before the DB is ever touched.

Single source of truth: the period grammar is ``reg_meta.fqid.is_period`` /
``period_token_to_bounds`` — we do NOT re-encode it. The wire format (§9.5):

    ?period=2020              int year      → Period int 2020
    ?period=HT2020            period token  → Period str "HT2020"
    ?period=2020-Q3           period token  → Period str "2020-Q3"
    ?period=2020-08           period token  → Period str "2020-08"
    ?period=2018-12-31        period token  → Period str "2018-12-31"
    ?period=2018..2020        range         → Period dict {"from": 2018, "to": 2020}
    ?period=2020-Q1..2020-Q4  range         → Period dict {"from": "...", "to": "..."}
    ?period=_default          snapshot      → Period str "_default"

A bare 4-digit year is mapped to ``int`` (matches ``resolve_at``'s int-year
arm); every other token stays a ``str``. The range ``..`` separator yields the
``{"from","to"}`` dict ``_period_bounds`` accepts; each endpoint is itself a
bare year (→ int) or a token (→ str).

The module is FastAPI-free so it's unit-testable in isolation; the router wires
it as a pre-open ``Depends`` that maps ``PeriodParamError`` → 422.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from reg_meta.fqid import (
    DEFAULT_VARIANT_SLUG,
    FqidError,
    is_period,
    validate_slug,
)

if TYPE_CHECKING:
    from reg_meta.catalog import Period

RANGE_SEP = ".."

# A bare year token (`2020`) is the one period form `resolve_at` wants as an
# `int` rather than a `str` (both `_period_bounds` arms produce identical bounds,
# but the int arm is the documented year case). Every other token stays a string.
_YEAR_LEN = 4


class PeriodParamError(ValueError):
    """Raised when a raw ``?period=`` value fails the §16 syntactic allow-list.

    The catalog router maps this to HTTP 422 before any Catalog call, so a
    rejection means zero SQL was executed and zero connections opened.
    """


class VariantParamError(ValueError):
    """Raised when a raw ``?variant=`` value fails the slug allow-list."""


class ValueSetVersionParamError(ValueError):
    """Raised when a raw ``?value_set_version=`` value fails the slug allow-list."""


def _parse_endpoint(raw: str) -> int | str:
    """One range endpoint: a bare year (→ int) or a period token (→ str).

    Rejects anything the period grammar doesn't accept — the SQLi / traversal /
    NUL probes never reach reg_meta because they aren't period tokens."""
    if not is_period(raw):
        raise PeriodParamError(
            f"invalid period range endpoint: {raw!r} "
            "(grammar: YYYY, YYYY-MM, YYYY-MM-DD, HTYYYY/VTYYYY, "
            "YYYY-Q[1-4], YYYY-H[12])"
        )
    if len(raw) == _YEAR_LEN and raw.isdigit():
        return int(raw)
    return raw


def parse_period(raw: str) -> Period:
    """Parse a raw ``?period=`` wire value into a ``reg_meta.catalog.Period``.

    A pure SYNTACTIC allow-list (§16): the result is one of the polymorphic
    ``Period`` forms ``resolve_at`` accepts — ``int`` year, period-token ``str``,
    ``{"from","to"}`` range ``dict``, or the ``_default`` sentinel ``str``.
    Raises ``PeriodParamError`` (→ 422, zero SQL) on anything else, BEFORE
    reg_meta's semantic ``_period_bounds`` runs. reg_meta is still the semantic
    backstop (e.g. it rejects ``from`` after ``to``); this only guarantees the
    value is grammar-shaped before the DB is touched.
    """
    if not raw:
        raise PeriodParamError("empty period")
    if raw == DEFAULT_VARIANT_SLUG:  # the "_default" snapshot sentinel
        return DEFAULT_VARIANT_SLUG
    if RANGE_SEP in raw:
        lo_raw, sep, hi_raw = raw.partition(RANGE_SEP)
        # `partition` splits on the FIRST `..`; a second `..` lands in `hi_raw`
        # and would fail `is_period`, but reject explicitly for a clearer error.
        if RANGE_SEP in hi_raw:
            raise PeriodParamError(
                f"period range admits exactly one {RANGE_SEP!r} separator: {raw!r}"
            )
        if not lo_raw or not hi_raw:
            raise PeriodParamError(
                f"period range needs both endpoints (<from>{RANGE_SEP}<to>): {raw!r}"
            )
        assert sep == RANGE_SEP
        return {"from": _parse_endpoint(lo_raw), "to": _parse_endpoint(hi_raw)}
    # A single token: bare year → int, every other period form → str.
    return _parse_endpoint(raw)


def parse_variant(raw: str) -> str:
    """Validate a raw ``?variant=`` value as a register_variant slug.

    UNLIKE the FQID path guard, this ADMITS ``_default`` — it is a real
    ``register_variant`` slug (§5.1, the synthesized variant for LSS/BU/SOL etc.;
    108 in the real DB), and ``?variant=`` is the register-sub-resource browse
    coordinate, not a path segment. So the value passes if it is the ``_default``
    literal OR a valid slug. Raises ``VariantParamError`` (→ 422, zero SQL) on
    anything else — delegating the slug grammar to ``reg_meta.fqid.validate_slug``
    (single source of truth, ``allow_default=True``)."""
    if not raw:
        raise VariantParamError("empty variant")
    try:
        validate_slug(raw, "variant", allow_default=True)
    except FqidError as exc:
        raise VariantParamError(str(exc)) from exc
    return raw


def parse_value_set_version(raw: str) -> str:
    """Validate a raw ``?value_set_version=`` value as a value-set-version label.

    Per §5.2 the version part is the classification-slug / ``value_set_version_label``
    grammar — which is the plain slug grammar (same as the binding-leaf ``@version``
    pin the path guard validates). Does NOT admit ``_default`` (a version label is
    not a variant coordinate). Raises ``ValueSetVersionParamError`` (→ 422, zero
    SQL) on anything else, delegating to ``reg_meta.fqid.validate_slug`` (single
    source of truth)."""
    if not raw:
        raise ValueSetVersionParamError("empty value_set_version")
    try:
        validate_slug(raw, "value_set_version")
    except FqidError as exc:
        raise ValueSetVersionParamError(str(exc)) from exc
    return raw
