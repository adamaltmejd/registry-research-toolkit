"""FQID parser/emitter for reg_meta (see DESIGN.md → FQID grammar).

Forms:

    scb                                    1 seg   provider
    scb/lisa                               2 segs  register
    scb/lisa/kon                           3 segs  variable binding (the variable)
    class/sun2020                          2 segs w/ `class/`  classification

A2.6 grammar flip: the binding FQID names the **variable** directly and is
3-segment (`provider/register/slug`). The variant and the period are
**delivery coordinates** (carried on `variable_state` / passed to
`resolve_at`), NOT FQID segments — so the variant FQID kind and the
register_version FQID kind are gone. With the variant
slot removed, a 3-segment string like `scb/lisa/individer-15plus` is
unambiguously a binding (variable slug `individer-15plus`); there is no
3-segment variant address to collide with.

The leading ``class/`` discriminates classification FQIDs from the binding
form; ``class`` is reserved everywhere else. A2.6.1 folded the classification
vintage into the slug, so the classification FQID is the 2-segment
`class/<slug>` (`class/sun2020`, `class/lkf2007`; see DESIGN.md → FQID grammar) — there is no separate
version segment, and the slug alone is the global uniqueness key.

The period grammar (``is_period`` / ``period_token_to_bounds`` /
``derive_period``) survives: ``resolve_at`` expands a polymorphic period to ISO
bounds, and the build-time coalescer derives per-edition periods from
``registerversionnamn``. It is no longer an FQID segment.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from pydantic import GetCoreSchemaHandler
    from pydantic_core import CoreSchema


class FqidKind(StrEnum):
    PROVIDER = "provider"
    REGISTER = "register"
    VARIABLE_BINDING = "variable_binding"
    CLASSIFICATION = "classification"


CLASSIFICATION_PREFIX = "class"
DEFAULT_VARIANT_SLUG = "_default"

# Reserved HTTP-suffix slugs (see DESIGN.md → FQID grammar). reg_webapp's catalog router declares
# sub-resource routes that greedy-shadow variable canonical paths; a slug equal
# to one of these would mint an FQID whose URL is permanently captured by the
# route, so they're reserved in the slot(s) whose canonical URL position the
# route occupies. Source of truth for the route list:
# `reg_webapp/backend/src/reg_webapp/routes/catalog.py` (pinned in
# `reg_webapp/backend/tests/test_boot.py` as `_ROUTES_BEFORE_CATCH_ALL`).
#
# The binding-suffix routes `/catalog/{fqid:path}/<suffix>` greedy-match ANY
# fqid path, so `<suffix>` shadows a 3-seg variable leaf (`scb/lisa/states`), a
# 2-seg register (`scb/states`), AND a classification (`class/states`) — reserved
# in all three slots. (`lineage_warnings` carries an underscore, so the slug
# grammar already rejects it before this check runs; it's listed anyway to keep
# the set a faithful mirror of the route list and to stay correct if the grammar
# ever loosened.) `dimensions` (#489) is the concept-group membership
# sub-resource. (#571's classification succession is no longer a sub-resource: the
# FULL edition chain is embedded on the classification node as `edition_chain`, so
# the immediate-neighbor routes were retired.)
RESERVED_HTTP_SUFFIX_SLUGS: frozenset[str] = frozenset(
    {
        "states",
        "predecessors",
        "successors",
        "related",
        "lineage",
        "lineage_warnings",
        "dimensions",
    }
)
# The literal `/catalog/{provider}/{register}/variants` register sub-resource
# shadows ONLY a 3-seg variable leaf (`scb/lisa/variants`); a 2-seg register
# (`scb/variants`) is a clean register path. So `variants` is reserved in the
# VARIABLE slot only. (The register_variant slug rides a `?variant=` query value,
# never a path segment, so it carries no reservation either.)
RESERVED_VARIANTS_SLUG = "variants"

# The literal `/catalog/group/{provider}/{register}/{key}` concept-group SUBJECT
# route (#617) puts `group` at a NON-leading position: `{provider}` is its second
# segment, not the first. A provider named `group` would mint a binding-suffix URL
# `/catalog/group/<register>/<variable>/states` (5 segments) that this earlier-declared
# 5-seg group route captures (provider=<register>, register=<variable>, key=`states`)
# → a wrong 404 instead of the binding's `/states`. So `group` is reserved in the
# PROVIDER slot — the FIRST token to carry a reservation in the provider slot,
# correcting the prior assumption that the provider slot (always a leading segment)
# could never be a colliding URL position. No provider is named `group` today, so this
# breaks no data; it's a forward-looking guard + correctness fix.
RESERVED_GROUP_SLUG = "group"

# The FQID-grammar prose (see DESIGN.md → FQID grammar) pairs the regex `^[a-z][a-z0-9-]*[a-z0-9]$` with "single hyphens
# only"; the form below enforces both in one expression. Anchored with `\Z`, NOT
# `$`: Python's `$` also matches just before a single trailing newline, so `$`
# would accept a slug like `kon\n` — a hole the reg_webapp path guard (see reg_webapp/DESIGN.md → FQID path guard (catalog_fqid.py)) (which
# delegates here) and build-time slug validation both rely on this rejecting.
_SLUG_RE = re.compile(r"^[a-z](?:-?[a-z0-9])*\Z")
_SLUG_NONALNUM = re.compile(r"[^a-z0-9]+")

# Period grammar building blocks. Year is 1900-2099, month 01-12, day 01-31.
# Shared by the anchored validators and the substring extractors so `is_period`
# and `derive_period` agree on what counts as a period. The day bound here is
# only the syntactic 01-31 envelope; an author-supplied `YYYY-MM-DD` is ALSO
# calendar-validated in `is_period` (real leap years etc. — `2019-02-29` is
# rejected), and `derive_period_with_span` routes its extractor matches through
# `is_period` so the agree-invariant holds for calendar-impossible dates too (a
# `2019-02-29` substring degrades to `2019-02`). The year/month bounds still don't
# enforce SCB coverage, and the SYNTHESIZED month/quarter/half upper bound in
# `period_token_to_bounds` intentionally over-counts Feb→29 for interval overlap
# (see `_MONTH_LAST_DAY`).
_YEAR = r"(?:19|20)\d{2}"
_MONTH = r"(?:0[1-9]|1[0-2])"
_DAY = r"(?:0[1-9]|[12]\d|3[01])"

# `\Z` not `$` (same footgun fixed for `_SLUG_RE` above): Python's `$` also matches
# just before a single trailing newline, so `^{_YEAR}$` would accept `"2020\n"`.
# `is_period` is the `?period` allow-list reg_webapp delegates to (see reg_webapp/DESIGN.md → query allow-list (period_param.py)), so a
# trailing-newline period must be rejected here, not opened-and-queried downstream.
_PERIOD_PATTERNS = (
    re.compile(rf"^{_YEAR}\Z"),
    re.compile(rf"^{_YEAR}-{_MONTH}\Z"),
    re.compile(rf"^{_YEAR}-{_MONTH}-{_DAY}\Z"),
    re.compile(rf"^[HV]T{_YEAR}\Z"),
    re.compile(rf"^{_YEAR}-Q[1-4]\Z"),
    re.compile(rf"^{_YEAR}-H[12]\Z"),
)

# Most-specific-first so "LISA HT2020" yields "HT2020", not "2020". Word
# boundaries on term/quarter forms reject embedded matches like "XHT2020";
# trailing `(?!\d)` on the month pattern stops range forms like "2018-2020"
# matching as `2018-20`. Year pattern anchors against longer digit runs the
# same way `queries.extract_year` does (rejects "v19999").
#
# ISO date is in the extract list because SCB source names occasionally
# contain literal `YYYY-MM-DD` (e.g. `'2014-12-31'`); the half-year pattern
# is curated-only (Swedish source forms like `Första halvåret 1995` don't
# carry the bare `1995-H1` substring, same as `maj-2011`/`kv1-2011` — see
# reg_meta_build/DESIGN.md → Slug curation for the canonical-form convention). `derive_period_with_span`
# checks each match against `is_period`, so a calendar-impossible ISO date
# (`2019-02-29`) skips the full-date pattern and degrades to the next, most-specific
# VALID token (`2019-02`) — keeping extractor output and `is_period` in agreement.
_PERIOD_EXTRACT_PATTERNS = (
    re.compile(rf"(?<![A-Za-z0-9])[HV]T{_YEAR}(?!\d)"),
    re.compile(rf"(?<!\d){_YEAR}-Q[1-4](?![A-Za-z0-9])"),
    re.compile(rf"(?<!\d){_YEAR}-{_MONTH}-{_DAY}(?!\d)"),
    re.compile(rf"(?<!\d){_YEAR}-{_MONTH}(?!\d)"),
    re.compile(rf"(?<!\d){_YEAR}(?!\d)"),
)

# Swedish termin tokens map to HT/VT prefix forms. SCB version names like
# `1980 höstterminen` and `1980 vårterminen` would otherwise both derive
# to bare `1980` and collide under the same variant (220 such groups in
# the current SCB DB). Patterns intentionally match before the bare-year
# extractor in `derive_period`. The optional `en` suffix is the Swedish
# definite article (termin vs terminen). The cross-term form
# `Höstterminen YYYY - Vårterminen YYYY` resolves to HT<first-year>
# because the höst pattern is checked first — same as how
# `_PERIOD_EXTRACT_PATTERNS` favors most-specific tokens.
#
# Collision assumption: this resolves cleanly only when a cross-term row
# like `Höstterminen 2018 - Vårterminen 2019` does NOT coexist with a
# `Höstterminen 2018` sibling in the same variant (both would derive to
# `HT2018`). Verified clean against current SCB data; future deliveries
# should re-run `reg-meta-build precheck-slugs` to catch a new collision
# before it trips `UNIQUE(register_variant_id, slug)` at build time.
_TERMIN_EXTRACT_PATTERNS = (
    (re.compile(rf"\bhöst(?:termin|terminen)\s+({_YEAR})\b", re.IGNORECASE), "HT"),
    (re.compile(rf"\bvår(?:termin|terminen)\s+({_YEAR})\b", re.IGNORECASE), "VT"),
    (re.compile(rf"\b({_YEAR})\s+höst(?:termin|terminen)\b", re.IGNORECASE), "HT"),
    (re.compile(rf"\b({_YEAR})\s+vår(?:termin|terminen)\b", re.IGNORECASE), "VT"),
)


class FqidError(ValueError):
    """Raised when an FQID string fails grammar validation."""


def is_slug(value: str) -> bool:
    return bool(_SLUG_RE.match(value))


def is_period(value: str) -> bool:
    if not any(p.match(value) for p in _PERIOD_PATTERNS):
        return False
    # The `YYYY-MM-DD` form is the only 10-char form and the only one carrying an
    # author-supplied day; calendar-validate it so an impossible day (`2019-02-29`
    # in a non-leap year, `2018-02-30`) is rejected, not just bounded 01-31 by the
    # regex. Other forms carry no author day, so the regex match alone is enough.
    if len(value) == 10:
        try:
            date.fromisoformat(value)
        except ValueError:
            return False
    return True


# Quarter / half-year → (start_month, end_month). Used by `period_token_to_bounds`
# to expand a token into an ISO date interval the same way the ingest coalescer
# does (period tokens expand to ISO ranges — see DESIGN.md → FQID grammar).
_QUARTER_MONTHS = {
    "1": ("01", "03"),
    "2": ("04", "06"),
    "3": ("07", "09"),
    "4": ("10", "12"),
}
_HALF_MONTHS = {"1": ("01", "06"), "2": ("07", "12")}
# Last day per month (non-leap). This is the SYNTHESIZED upper bound for the
# month/quarter/half/term forms, NOT an author-supplied day. February uses 29 so
# a `YYYY-02` month token still expands to `..-02-29` even in a non-leap year:
# over-counting Feb to 29 is intentional and harmless for interval overlap (the
# resolver only intersects, never round-trips this through `date.fromisoformat`).
# This is deliberately DIFFERENT from the author-supplied `YYYY-MM-DD` day, which
# `is_period` now calendar-validates (a real `2019-02-29` is rejected). The two
# must not be conflated: rejecting an impossible author day is correct; rejecting
# the synthesized Feb-29 bound would break valid `YYYY-02` month tokens.
_MONTH_LAST_DAY = {
    "01": "31", "02": "29", "03": "31", "04": "30", "05": "31", "06": "30",
    "07": "31", "08": "31", "09": "30", "10": "31", "11": "30", "12": "31",
}  # fmt: skip


def period_token_to_bounds(token: str) -> tuple[str, str]:
    """Expand a period token to an inclusive ISO date interval ``(lo, hi)``.

    The full date-range expansion the resolver's `resolve_at` needs to intersect
    against `variable_state` validity ranges (which are stored as full ISO dates,
    see DESIGN.md → Two-level variable model). Mirrors the ingest-side expansion so a `HT2020` query and a `HT2020`
    state agree on bounds. Accepts the six period forms `is_period` accepts:

        2018          → 2018-01-01 .. 2018-12-31
        2018-03       → 2018-03-01 .. 2018-03-31
        2018-12-31    → 2018-12-31 .. 2018-12-31  (single day)
        HT2020 / VT2020 → autumn (Jul-Dec) / spring (Jan-Jun) term
        2018-Q3       → 2018-07-01 .. 2018-09-30
        2018-H1       → 2018-01-01 .. 2018-06-30

    Raises ``FqidError`` for anything that isn't a period token (fail-fast — the
    caller validates user input here).
    """
    if not is_period(token):
        raise FqidError(
            f"not a period token: {token!r} "
            "(grammar: YYYY, YYYY-MM, YYYY-MM-DD, HTYYYY/VTYYYY, "
            "YYYY-Q[1-4], YYYY-H[12])"
        )
    if token[:2] in ("HT", "VT"):
        year = token[2:]
        lo_m, hi_m = ("07", "12") if token[0] == "H" else ("01", "06")
        return f"{year}-{lo_m}-01", f"{year}-{hi_m}-{_MONTH_LAST_DAY[hi_m]}"
    if "-Q" in token:
        year, q = token.split("-Q")
        lo_m, hi_m = _QUARTER_MONTHS[q]
        return f"{year}-{lo_m}-01", f"{year}-{hi_m}-{_MONTH_LAST_DAY[hi_m]}"
    if "-H" in token:
        year, h = token.split("-H")
        lo_m, hi_m = _HALF_MONTHS[h]
        return f"{year}-{lo_m}-01", f"{year}-{hi_m}-{_MONTH_LAST_DAY[hi_m]}"
    parts = token.split("-")
    if len(parts) == 1:  # YYYY
        return f"{parts[0]}-01-01", f"{parts[0]}-12-31"
    if len(parts) == 2:  # YYYY-MM
        return f"{token}-01", f"{token}-{_MONTH_LAST_DAY[parts[1]]}"
    return token, token  # YYYY-MM-DD single day


def period_token_for_bounds(lo: str, hi: str) -> str:
    """Render an inclusive ISO interval as the COARSEST period token that
    `period_token_to_bounds` expands back to exactly ``(lo, hi)`` — the
    display/diagnostic inverse (#271).

    A window outside the grammar renders as the explicit range ``"lo..hi"`` —
    NEVER rounded to a containing year: two sub-annual sibling states both
    reading "2009" would re-create exactly the ambiguity the interval resolver
    removes. Month windows use the same synthesized `_MONTH_LAST_DAY` ends as
    the forward expansion (incl. the intentional Feb-29 over-count), so the two
    directions stay byte-agreed. Tie-break: ``VTYYYY``/``HTYYYY`` share bounds
    with ``YYYY-H1``/``-H2``; the term form wins (these windows arise from
    SCB's term registers, and the curated period grammar prefers the term
    spelling) — the ``-H`` forms are accepted on input but never emitted."""
    year, ylo_m = lo[:4], lo[5:7]
    if hi[:4] == year:
        if (lo, hi) == (f"{year}-01-01", f"{year}-12-31"):
            return year
        if lo[8:] == "01" and lo == f"{year}-{ylo_m}-01":
            if (ylo_m, hi) == ("01", f"{year}-06-30"):
                return f"VT{year}"
            if (ylo_m, hi) == ("07", f"{year}-12-31"):
                return f"HT{year}"
            for q, (q_lo, q_hi) in _QUARTER_MONTHS.items():
                if ylo_m == q_lo and hi == f"{year}-{q_hi}-{_MONTH_LAST_DAY[q_hi]}":
                    return f"{year}-Q{q}"
            if hi == f"{year}-{ylo_m}-{_MONTH_LAST_DAY[ylo_m]}":
                return f"{year}-{ylo_m}"
    if lo == hi and is_period(lo):
        return lo
    return f"{lo}..{hi}"


def validate_slug(
    value: str,
    slot: FqidKind | str,
    *,
    allow_default: bool = False,
) -> None:
    slot_name = slot.value if isinstance(slot, FqidKind) else slot
    if value == DEFAULT_VARIANT_SLUG:
        if allow_default:
            return
        raise FqidError(
            f"`_default` is reserved for the register_variant slug "
            f"(a delivery coordinate); got it in {slot_name}"
        )
    if value == CLASSIFICATION_PREFIX:
        raise FqidError(
            f"`{CLASSIFICATION_PREFIX}` is reserved and may not appear as a slug "
            f"in {slot_name}"
        )
    # A2.6.1: period-shaped slugs are rejected everywhere. The classification
    # version was the only `allow_period` caller; folding the vintage into the
    # slug (`sun2020`, leading alpha stem) removed it — baked slugs pass the
    # normal grammar, so no period exemption remains.
    if is_period(value):
        raise FqidError(
            f"slug in {slot_name} matches the period grammar: {value!r} "
            f"(period-shaped slugs are rejected outside the period slot for legibility)"
        )
    if not _SLUG_RE.match(value):
        raise FqidError(
            f"invalid slug in {slot_name}: {value!r} "
            f"(grammar: ^[a-z][a-z0-9-]*[a-z0-9]$ or single ^[a-z]$, single hyphens only)"
        )
    # Reserved HTTP-suffix slugs (see DESIGN.md → FQID grammar): a grammar-valid slug equal to one of these
    # would shadow a live reg_webapp catalog sub-resource route (see
    # `RESERVED_HTTP_SUFFIX_SLUGS`). Keyed on the slot — a token only collides in
    # the slot(s) whose canonical URL position the route can occupy. The
    # register_variant slot carries no reservation (it rides a `?variant=` query
    # value, never a path segment); the provider slot carries the SINGLE
    # `RESERVED_GROUP_SLUG` reservation below, but no HTTP-suffix reservation.
    if slot_name in ("variable", "register", "classification") and (
        value in RESERVED_HTTP_SUFFIX_SLUGS
    ):
        raise FqidError(
            f"slug in {slot_name} is a reserved HTTP-suffix: {value!r} "
            f"(it would shadow the `/catalog/{{fqid:path}}/{value}` catalog route — "
            f"see reg_webapp routes/catalog.py)"
        )
    if slot_name == "variable" and value == RESERVED_VARIANTS_SLUG:
        raise FqidError(
            f"slug in {slot_name} is reserved: {value!r} (it would shadow the "
            f"`/catalog/{{provider}}/{{register}}/variants` register sub-resource — "
            f"see reg_webapp routes/catalog.py)"
        )
    # `group` is reserved in the PROVIDER slot ONLY: the 5-seg group SUBJECT route
    # `/catalog/group/{provider}/{register}/{key}` (#617) is declared above the
    # binding-suffix routes, so a provider named `group` would have its binding's
    # suffix URL (`/catalog/group/<register>/<variable>/states`, also 5 segments)
    # captured by the group route → wrong 404. A register/variable/classification
    # named `group` is fine — only the provider position lands at that literal.
    if slot_name == FqidKind.PROVIDER.value and value == RESERVED_GROUP_SLUG:
        raise FqidError(
            f"slug in {slot_name} is reserved: {value!r} (it would shadow the "
            f"`/catalog/group/{{provider}}/{{register}}/{{key}}` concept-group "
            f"subject route — see reg_webapp routes/catalog.py)"
        )


def _validate_period(value: str) -> None:
    if not is_period(value):
        raise FqidError(
            f"invalid period: {value!r} "
            "(grammar: YYYY, YYYY-MM, YYYY-MM-DD, HTYYYY/VTYYYY, "
            "YYYY-Q[1-4], YYYY-H[12])"
        )


@dataclass(frozen=True)
class Fqid:
    """Parsed FQID. Exactly one set of fields is populated per kind.

    A2.6: the binding FQID names the variable (`provider/register/variable`);
    `variant` and `period` are no longer FQID fields (they are delivery
    coordinates carried elsewhere — see DESIGN.md → FQID grammar / Two-level variable model).
    """

    kind: FqidKind
    provider: str | None = None
    register: str | None = None
    variable: str | None = None
    classification: str | None = None

    def __str__(self) -> str:
        """Canonical FQID string."""
        if self.kind is FqidKind.CLASSIFICATION:
            return f"{CLASSIFICATION_PREFIX}/{self.classification}"
        parts = [self.provider]
        for v in (self.register, self.variable):
            if v is None:
                break
            parts.append(v)
        return "/".join(p for p in parts if p is not None)

    @classmethod
    def provider_fqid(cls, provider: str) -> Fqid:
        validate_slug(provider, FqidKind.PROVIDER)
        return cls(kind=FqidKind.PROVIDER, provider=provider)

    @classmethod
    def register_fqid(cls, provider: str, register: str) -> Fqid:
        validate_slug(provider, FqidKind.PROVIDER)
        validate_slug(register, FqidKind.REGISTER)
        return cls(kind=FqidKind.REGISTER, provider=provider, register=register)

    @classmethod
    def binding_fqid(cls, provider: str, register: str, variable: str) -> Fqid:
        """A2.6: the 3-segment binding FQID names the variable directly
        (`provider/register/variable`). The variant and period are delivery
        coordinates resolved via `resolve_at`, not FQID segments (see DESIGN.md → FQID grammar)."""
        validate_slug(provider, FqidKind.PROVIDER)
        validate_slug(register, FqidKind.REGISTER)
        validate_slug(variable, "variable")
        return cls(
            kind=FqidKind.VARIABLE_BINDING,
            provider=provider,
            register=register,
            variable=variable,
        )

    @classmethod
    def classification_fqid(cls, classification: str) -> Fqid:
        """A2.6.1: the 2-segment classification FQID names the version-baked
        slug directly (`class/<slug>`, e.g. `class/sun2020`). The vintage lives
        in the slug, not a separate segment (see DESIGN.md → FQID grammar)."""
        validate_slug(classification, FqidKind.CLASSIFICATION)
        return cls(kind=FqidKind.CLASSIFICATION, classification=classification)

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        """Pydantic v2 hook so a `BaseModel` with an `Fqid` field treats it as a
        STRING on the wire (#681): serialize via the canonical `str(fqid)`,
        validate by accepting an existing `Fqid` as-is or parsing a `str` via
        `parse`, and emit `type: string` in the JSON/OpenAPI schema (not an
        object). `core_schema` is imported lazily so the eager `import reg_meta`
        stays cheap (mirrors the `__init__.py` lazy-import note)."""
        from pydantic_core import core_schema

        from_str = core_schema.no_info_after_validator_function(
            parse, core_schema.str_schema()
        )
        return core_schema.json_or_python_schema(
            json_schema=from_str,
            python_schema=core_schema.union_schema(
                [core_schema.is_instance_schema(cls), from_str]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(
                str, when_used="always"
            ),
        )


def parse(value: str) -> Fqid:
    """Parse and validate an FQID string. Raises ``FqidError`` on any violation.

    A2.6 grammar (see DESIGN.md → FQID grammar): kind is determined purely by segment count + the
    `class/` discriminator. 1 = provider, 2 = register, 3 = variable binding
    (the FQID names the variable). The `class/` prefix marks a classification;
    A2.6.1 made it 2-segment (`class/<slug>`, vintage baked into the slug). There
    is no variant or period segment — both are delivery coordinates resolved via
    `resolve_at`, not part of identity.
    """
    if not isinstance(value, str):
        raise FqidError(f"FQID must be a string, got {type(value).__name__}")
    if not value:
        raise FqidError("empty FQID")
    if value.startswith("/") or value.endswith("/") or "//" in value:
        raise FqidError(f"FQID contains an empty segment: {value!r}")

    segs = value.split("/")

    if segs[0] == CLASSIFICATION_PREFIX:
        if len(segs) != 2:
            raise FqidError(
                f"classification FQID needs 2 segments (class/<slug>): {value!r}"
            )
        return Fqid.classification_fqid(segs[1])

    n = len(segs)
    if n == 1:
        return Fqid.provider_fqid(segs[0])
    if n == 2:
        return Fqid.register_fqid(segs[0], segs[1])
    if n == 3:
        return Fqid.binding_fqid(segs[0], segs[1], segs[2])
    raise FqidError(
        f"FQID has {n} segments; grammar accepts 1-3 (or 2 with `class/` "
        f"prefix for a classification): {value!r}"
    )


def try_emit(factory: Callable[..., Fqid], *parts: str | None) -> str | None:
    """Build and stringify an FQID, returning ``None`` if any part is missing
    or fails validation.

    Used by query commands so a row with NULL slug columns surfaces as
    ``fqid: None`` alongside legacy fields rather than crashing the response.
    """
    if any(p is None or p == "" for p in parts):
        return None
    try:
        return str(factory(*parts))
    except FqidError:
        return None


def derive_period_with_span(
    version_name: str | None,
) -> tuple[str, int, int] | None:
    """Like :func:`derive_period` but also returns the `(start, end)` span the
    match consumed in ``version_name``. Lets audit tooling (the seed-slugs rule callout, see reg_meta_build/DESIGN.md → Slug
    curation) recover the residual — the source-name text outside the
    matched period — so a curator can see what auto-derive would discard.
    """
    if not version_name:
        return None
    for pat, prefix in _TERMIN_EXTRACT_PATTERNS:
        m = pat.search(version_name)
        if m:
            return (f"{prefix}{m.group(1)}", m.start(), m.end())
    # Validate each match against `is_period` and, on failure, fall through to the
    # next (less-specific) pattern — preserving the extractors-and-`is_period`-agree
    # invariant now that `is_period` calendar-validates the author day. Only the
    # full-date pattern can produce a calendar-invalid token (the HT/VT/Q/half/
    # YYYY-MM/YYYY forms carry no author day), so e.g. `2019-02-29` degrades to the
    # most-specific VALID token `2019-02` instead of returning a token the
    # downstream coalescer's `period_token_to_bounds` would crash on.
    for pat in _PERIOD_EXTRACT_PATTERNS:
        m = pat.search(version_name)
        if m and is_period(m.group(0)):
            return (m.group(0), m.start(), m.end())
    return None


def derive_period(version_name: str | None) -> str | None:
    """Extract the most-specific period token from a register-version name.

    Returns the matched period substring (`HT2020`, `2020-Q1`, `2020-01`,
    `2020`) so distinct sub-year versions don't collapse to the same FQID.
    Swedish termin tokens (`höstterminen 1980`, `1980 vårterminen`) map to
    `HT1980` / `VT1980` so terms under the same year stay distinguishable.

    Match order is termin patterns first, then the period patterns in
    most-specific-first order. Reordering would let `1980 höstterminen`
    collapse to bare `1980` and re-trip the uniqueness rule (see reg_meta_build/DESIGN.md → Slug curation) it exists
    to prevent.
    """
    match = derive_period_with_span(version_name)
    return match[0] if match is not None else None


def derive_variable_slug(delivery_column_name: str | None) -> str | None:
    """Derive a variable slug from a SCB delivery column name (auto-slug
    rule; see reg_meta_build/DESIGN.md → Slug curation). The input is `variable_alias.delivery_column_name` — the glossary
    rename (see DESIGN.md → Glossary and Swedish↔English crosswalk) of what SCB ships as `kolumnnamn` (e.g. `Kon`, `PersonNr`).

    Lowercases, strips diacritics via NFKD ASCII fold, replaces runs of
    non-alphanumerics with single hyphens. Returns ``None`` when the result
    is empty, fails the slug grammar, is period-shaped, or lands on a reserved
    variable-slot token (so a column literally named e.g. "States"/"Variants"
    degrades to the name/last-resort fallback rather than minting a slug that
    would shadow a catalog route — see `validate_slug`).
    """
    if not delivery_column_name:
        return None
    folded = (
        unicodedata.normalize("NFKD", delivery_column_name)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    candidate = _SLUG_NONALNUM.sub("-", folded).strip("-")
    # A derived slug must clear the exact same bar an authored variable slug does,
    # so delegate to `validate_slug` (grammar, period, the `_default`/`class`
    # reservations, and the HTTP-suffix tokens; see DESIGN.md → FQID grammar) rather than re-checking them
    # here — the two can never drift, and it's the same build-via-validating-factory
    # idiom as `try_emit`. A column literally named e.g. "States"/"Variants" thus
    # degrades to None (caller falls back to the name/last-resort slug) instead of
    # minting a slug that would shadow a catalog route.
    try:
        validate_slug(candidate, "variable")
    except FqidError:
        return None
    return candidate
