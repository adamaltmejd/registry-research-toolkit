"""FQID parser/emitter for reg_meta (REFACTOR_SPEC.md §5.2).

Forms:

    scb                                    1 seg   provider
    scb/lisa                               2 segs  register
    scb/lisa/individer-15plus              3 segs  register_variant
    scb/lisa/individer-15plus/2018         4 segs  register_version
    scb/lisa/individer-15plus/2018/kon     5 segs  variable binding
    class/sun/2020                         3 segs  classification

The leading ``class/`` discriminates classification FQIDs from the
3-segment register_variant form; ``class`` is reserved everywhere else.
"""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum


class FqidKind(str, Enum):
    PROVIDER = "provider"
    REGISTER = "register"
    REGISTER_VARIANT = "register_variant"
    REGISTER_VERSION = "register_version"
    VARIABLE_BINDING = "variable_binding"
    CLASSIFICATION = "classification"


CLASSIFICATION_PREFIX = "class"
DEFAULT_VARIANT_SLUG = "_default"
RESERVED_SLUGS: frozenset[str] = frozenset(
    {DEFAULT_VARIANT_SLUG, CLASSIFICATION_PREFIX}
)

# §5.2 prose pairs the regex `^[a-z][a-z0-9-]*[a-z0-9]$` with "single hyphens
# only"; the form below enforces both in one expression.
_SLUG_RE = re.compile(r"^[a-z](?:-?[a-z0-9])*$")
_SLUG_NONALNUM = re.compile(r"[^a-z0-9]+")

# Period grammar building blocks. Year is 1900-2099, month 01-12, day 01-31.
# Shared by the anchored validators and the substring extractors so `is_period`
# and `derive_period` agree on what counts as a period. Day bound is purely
# syntactic — Feb 30 passes the grammar; calendar validity is the curator's
# responsibility (same as how the year/month bounds don't enforce SCB
# coverage).
_YEAR = r"(?:19|20)\d{2}"
_MONTH = r"(?:0[1-9]|1[0-2])"
_DAY = r"(?:0[1-9]|[12]\d|3[01])"

_PERIOD_PATTERNS = (
    re.compile(rf"^{_YEAR}$"),
    re.compile(rf"^{_YEAR}-{_MONTH}$"),
    re.compile(rf"^{_YEAR}-{_MONTH}-{_DAY}$"),
    re.compile(rf"^[HV]T{_YEAR}$"),
    re.compile(rf"^{_YEAR}-Q[1-4]$"),
    re.compile(rf"^{_YEAR}-H[12]$"),
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
# REFACTOR_SPEC.md §5.3 for the canonical-form convention).
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
# before it trips `UNIQUE(regvar_id, slug)` at build time.
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
    return any(p.match(value) for p in _PERIOD_PATTERNS)


def validate_slug(
    value: str,
    slot: FqidKind | str,
    *,
    allow_default: bool = False,
    allow_period: bool = False,
) -> None:
    slot_name = slot.value if isinstance(slot, FqidKind) else slot
    if value == DEFAULT_VARIANT_SLUG:
        if allow_default:
            return
        raise FqidError(
            f"`_default` is reserved for the register_variant and "
            f"register_version slots; got it in {slot_name}"
        )
    if value == CLASSIFICATION_PREFIX:
        raise FqidError(
            f"`{CLASSIFICATION_PREFIX}` is reserved and may not appear as a slug "
            f"in {slot_name}"
        )
    if is_period(value):
        if allow_period:
            return
        raise FqidError(
            f"slug in {slot_name} matches the period grammar: {value!r} "
            f"(period-shaped slugs are rejected outside the period slot for legibility)"
        )
    if not _SLUG_RE.match(value):
        raise FqidError(
            f"invalid slug in {slot_name}: {value!r} "
            f"(grammar: ^[a-z][a-z0-9-]*[a-z0-9]$ or single ^[a-z]$, single hyphens only)"
        )


def _validate_version_slot(value: str) -> None:
    """§5.2: the version slot accepts either a period token (`2018`, `HT2020`,
    `2018-Q3`, `2018-H1`, `2018-01`, `2018-01-15`) or a curated slug
    (`ackumulerat-register`, `_default`).
    """
    validate_slug(value, "register_version", allow_default=True, allow_period=True)


def _validate_period(value: str) -> None:
    if not is_period(value):
        raise FqidError(
            f"invalid period: {value!r} "
            "(grammar: YYYY, YYYY-MM, YYYY-MM-DD, HTYYYY/VTYYYY, "
            "YYYY-Q[1-4], YYYY-H[12])"
        )


@dataclass(frozen=True)
class Fqid:
    """Parsed FQID. Exactly one set of fields is populated per kind."""

    kind: FqidKind
    provider: str | None = None
    register: str | None = None
    variant: str | None = None
    period: str | None = None
    variable: str | None = None
    classification: str | None = None
    version: str | None = None

    def __str__(self) -> str:
        """Canonical FQID string. Stored FQIDs never elide (§5.2)."""
        if self.kind is FqidKind.CLASSIFICATION:
            return f"{CLASSIFICATION_PREFIX}/{self.classification}/{self.version}"
        parts = [self.provider]
        for v in (self.register, self.variant, self.period, self.variable):
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
    def register_variant_fqid(cls, provider: str, register: str, variant: str) -> Fqid:
        validate_slug(provider, FqidKind.PROVIDER)
        validate_slug(register, FqidKind.REGISTER)
        validate_slug(variant, FqidKind.REGISTER_VARIANT, allow_default=True)
        return cls(
            kind=FqidKind.REGISTER_VARIANT,
            provider=provider,
            register=register,
            variant=variant,
        )

    @classmethod
    def register_version_fqid(
        cls, provider: str, register: str, variant: str, period: str
    ) -> Fqid:
        validate_slug(provider, FqidKind.PROVIDER)
        validate_slug(register, FqidKind.REGISTER)
        validate_slug(variant, FqidKind.REGISTER_VARIANT, allow_default=True)
        _validate_version_slot(period)
        return cls(
            kind=FqidKind.REGISTER_VERSION,
            provider=provider,
            register=register,
            variant=variant,
            period=period,
        )

    @classmethod
    def binding_fqid(
        cls,
        provider: str,
        register: str,
        variant: str,
        period: str,
        variable: str,
    ) -> Fqid:
        validate_slug(provider, FqidKind.PROVIDER)
        validate_slug(register, FqidKind.REGISTER)
        validate_slug(variant, FqidKind.REGISTER_VARIANT, allow_default=True)
        _validate_version_slot(period)
        validate_slug(variable, "variable")
        return cls(
            kind=FqidKind.VARIABLE_BINDING,
            provider=provider,
            register=register,
            variant=variant,
            period=period,
            variable=variable,
        )

    @classmethod
    def classification_fqid(cls, classification: str, version: str) -> Fqid:
        validate_slug(classification, FqidKind.CLASSIFICATION)
        validate_slug(version, "classification version", allow_period=True)
        return cls(
            kind=FqidKind.CLASSIFICATION,
            classification=classification,
            version=version,
        )


def parse(value: str) -> Fqid:
    """Parse and validate an FQID string. Raises ``FqidError`` on any violation.

    §5.2 accepts elided variant slot when the position is unambiguous: a period
    in slot 3 (after provider/register) signals an omitted `_default` variant,
    so `scb/r/2022` parses as `scb/r/_default/2022` and `scb/r/2022/kon` as
    `scb/r/_default/2022/kon`. The period-slug ban guarantees this never
    collides with a real variant slug. Canonical (non-elided) form is what
    ``__str__`` and stored FQIDs use.
    """
    if not isinstance(value, str):
        raise FqidError(f"FQID must be a string, got {type(value).__name__}")
    if not value:
        raise FqidError("empty FQID")
    if value.startswith("/") or value.endswith("/") or "//" in value:
        raise FqidError(f"FQID contains an empty segment: {value!r}")

    segs = value.split("/")

    if segs[0] == CLASSIFICATION_PREFIX:
        if len(segs) != 3:
            raise FqidError(
                f"classification FQID needs 3 segments "
                f"(class/<slug>/<version>): {value!r}"
            )
        return Fqid.classification_fqid(segs[1], segs[2])

    n = len(segs)
    if n == 1:
        return Fqid.provider_fqid(segs[0])
    if n == 2:
        return Fqid.register_fqid(segs[0], segs[1])
    if n == 3:
        if is_period(segs[2]):
            return Fqid.register_version_fqid(
                segs[0], segs[1], DEFAULT_VARIANT_SLUG, segs[2]
            )
        return Fqid.register_variant_fqid(segs[0], segs[1], segs[2])
    if n == 4:
        if is_period(segs[2]):
            return Fqid.binding_fqid(
                segs[0], segs[1], DEFAULT_VARIANT_SLUG, segs[2], segs[3]
            )
        return Fqid.register_version_fqid(segs[0], segs[1], segs[2], segs[3])
    if n == 5:
        return Fqid.binding_fqid(segs[0], segs[1], segs[2], segs[3], segs[4])
    raise FqidError(
        f"FQID has {n} segments; grammar accepts 1-5 (or 3 with `class/` prefix): "
        f"{value!r}"
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


def derive_period(version_name: str | None) -> str | None:
    """Extract the most-specific period token from a register-version name.

    Returns the matched period substring (`HT2020`, `2020-Q1`, `2020-01`,
    `2020`) so distinct sub-year versions don't collapse to the same FQID.
    Swedish termin tokens (`höstterminen 1980`, `1980 vårterminen`) map to
    `HT1980` / `VT1980` so terms under the same year stay distinguishable.

    Match order is termin patterns first, then the period patterns in
    most-specific-first order. Reordering would let `1980 höstterminen`
    collapse to bare `1980` and re-trip the §5.3 uniqueness rule it exists
    to prevent.
    """
    if not version_name:
        return None
    for pat, prefix in _TERMIN_EXTRACT_PATTERNS:
        m = pat.search(version_name)
        if m:
            return f"{prefix}{m.group(1)}"
    for pat in _PERIOD_EXTRACT_PATTERNS:
        m = pat.search(version_name)
        if m:
            return m.group(0)
    return None


def derive_variable_slug(kolumnnamn: str | None) -> str | None:
    """Derive a variable slug from a SCB kolumnnamn (§5.3 auto-slug rule).

    Lowercases, strips diacritics via NFKD ASCII fold, replaces runs of
    non-alphanumerics with single hyphens. Returns ``None`` when the result
    is empty or fails the slug grammar.
    """
    if not kolumnnamn:
        return None
    folded = (
        unicodedata.normalize("NFKD", kolumnnamn)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    candidate = _SLUG_NONALNUM.sub("-", folded).strip("-")
    if not candidate or not _SLUG_RE.match(candidate):
        return None
    if candidate in RESERVED_SLUGS or is_period(candidate):
        return None
    return candidate
