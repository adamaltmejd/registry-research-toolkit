"""``?period`` / ``?variant`` query allow-list for the catalog catch-all.

See DESIGN.md → query allow-list (period_param.py). The second chokepoint
(alongside ``catalog_fqid.validate_fqid_path``): a
thin SYNTACTIC allow-list that parses the raw ``?period=`` / ``?variant=`` query
values into the polymorphic ``reg_meta.catalog.Period`` type **before** any
reg_meta lookup — so a malformed value (SQLi probe, traversal, NUL,
percent-encoded slash) returns 422 with ZERO SQL executed and ZERO connections
opened. reg_meta's ``resolve_at`` /
``_period_bounds`` is the SEMANTIC backstop; this layer is purely about refusing
non-grammar input before the DB is ever touched.

Single source of truth: the period grammar is ``reg_meta.fqid.is_period`` /
``period_token_to_bounds`` — we do NOT re-encode it. The wire format:

    ?period=2020              int year      → Period int 2020
    ?period=HT2020            period token  → Period str "HT2020"
    ?period=2020-Q3           period token  → Period str "2020-Q3"
    ?period=2020-08           period token  → Period str "2020-08"
    ?period=2018-12-31        period token  → Period str "2018-12-31"
    ?period=2018..2020        range         → Period dict {"from": 2018, "to": 2020}
    ?period=2020-Q1..2020-Q4  range         → Period dict {"from": "...", "to": "..."}
    ?period=_default          snapshot      → Period str "_default"
    ?period=2005..2010,2015..2020  list     → one Period per comma member (#307/#340)

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
LIST_SEP = ","

# A bare year token (`2020`) is the one period form `resolve_at` wants as an
# `int` rather than a `str` (both `_period_bounds` arms produce identical bounds,
# but the int arm is the documented year case). Every other token stays a string.
_YEAR_LEN = 4

# A value-set-version label is a short human string (e.g. "SUN 2000 -
# Utbildningsnivå"); cap the sanity gate well above any real label.
_MAX_VALUE_SET_VERSION_LEN = 200

# Sentinel `?value_set_version` for "the empty/default label" (a state with
# `value_set_version_label = ''`). The empty string can't ride in the query (it's
# indistinguishable from absent), so a multi-vintage set that mixes a labeled and
# an empty-default state uses this sentinel to select the empty one; the catalog
# handler maps it to `''` before `resolve_at`. Starts with `_` so it can't collide
# with a real label (labels are SCB display text, never `_`-prefixed). It passes
# the gate below as ordinary non-empty text.
VALUE_SET_VERSION_NONE = "_none"


class PeriodParamError(ValueError):
    """Raised when a raw ``?period=`` value fails the syntactic allow-list.

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

    A pure SYNTACTIC allow-list: the result is one of the polymorphic
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


def parse_period_query(raw: str) -> list[Period]:
    """Parse a raw ``?period=`` wire value into resolve SEGMENTS: the #307
    comma-joined list form (``2005..2010,2015..2020``) yields one ``Period``
    per member, a scalar value a one-segment list. The catalog route resolves
    per segment and unions the states deduped by ``state_id`` —
    ``Catalog.resolve_at`` never sees the list form, mirroring the semantic
    validator's per-segment iteration (#340; also keeps the list grammar out
    of the separately-released reg_meta).

    Member rules mirror reg_schema's list rules SYNTACTICALLY: no empty
    members, and ``_default`` is whole-value-only (the full-history sentinel
    is not one piece of a series). Order/overlap are NOT gated — the union is
    order-insensitive, and a browse query is ephemeral, unlike an authored
    ``Source.period`` (where structural enforces sorted/disjoint).
    """
    if LIST_SEP not in raw:
        return [parse_period(raw)]
    members = raw.split(LIST_SEP)
    if any(not member for member in members):
        raise PeriodParamError(f"period list admits no empty members: {raw!r}")
    if DEFAULT_VARIANT_SLUG in members:
        raise PeriodParamError(
            f"{DEFAULT_VARIANT_SLUG!r} cannot be a period list member "
            f"(whole-value only): {raw!r}"
        )
    return [parse_period(member) for member in members]


def parse_variant(raw: str) -> str:
    """Validate a raw ``?variant=`` value as a register_variant slug.

    UNLIKE the FQID path guard, this ADMITS ``_default`` — it is a real
    ``register_variant`` slug (see reg_meta/DESIGN.md → Two-level variable model;
    the synthesized variant for LSS/BU/SOL etc.; 108 in the real DB), and
    ``?variant=`` is the register-sub-resource browse
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
    """Validate a raw ``?value_set_version=`` value as a value-set-version LABEL.

    ``?value_set_version`` is matched against the free-text ``value_set_version_label``
    (e.g. ``"SUN 1996, 5 positioner, brutto"``) via a Python ``==`` filter in
    ``Catalog.resolve_at`` — NOT a SQL predicate — so there is no injection surface
    and the slug grammar is the WRONG validator (real labels carry spaces, commas,
    parentheses, mixed case and non-ASCII). This gate is therefore a SANITY
    check only: non-empty, length-capped, and no NUL/control characters (the
    classic smuggling / log-injection vectors, never part of a real label). The
    SEMANTIC match (does this label exist for the variable at this period) is
    reg_meta's job. Raises ``ValueSetVersionParamError`` (→ 422) on a malformed
    value.

    [A5.3b] This replaced an over-strict ``validate_slug`` gate (A5.2a-ii) that
    422'd every real label — the version picker had no working consumer before
    A5.3b, so the mis-spec was latent and the slug-shaped test fixtures masked it.
    """
    if not raw.strip():
        # Empty OR whitespace-only — never a real label (validate, don't mutate:
        # real labels can carry trailing spaces, e.g. "Utbildningsnivå (SUN 2000)  ").
        raise ValueSetVersionParamError("empty value_set_version")
    if len(raw) > _MAX_VALUE_SET_VERSION_LEN:
        raise ValueSetVersionParamError(
            f"value_set_version too long (max {_MAX_VALUE_SET_VERSION_LEN} chars)"
        )
    # C0 controls (incl. NUL / tab / newline), DEL, and C1 controls are never part
    # of a real label; reject them (smuggling / log-injection defense in depth).
    if any(ord(c) < 0x20 or 0x7F <= ord(c) <= 0x9F for c in raw):
        raise ValueSetVersionParamError("value_set_version contains control characters")
    return raw
