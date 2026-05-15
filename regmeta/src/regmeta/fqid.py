"""FQID parser/emitter for regmeta (REFACTOR_SPEC.md §5.2).

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
_PERIOD_PATTERNS = (
    re.compile(r"^\d{4}$"),
    re.compile(r"^\d{4}-\d{2}$"),
    re.compile(r"^[HV]T\d{4}$"),
    re.compile(r"^\d{4}-Q[1-4]$"),
)
_SLUG_NONALNUM = re.compile(r"[^a-z0-9]+")


class FqidError(ValueError):
    """Raised when an FQID string fails grammar validation."""


def is_slug(value: str) -> bool:
    return bool(_SLUG_RE.match(value))


def is_period(value: str) -> bool:
    return any(p.match(value) for p in _PERIOD_PATTERNS)


def _validate_slug(
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
            f"`_default` is reserved for the register_variant slot; got it in {slot_name}"
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


def _validate_period(value: str) -> None:
    if not is_period(value):
        raise FqidError(
            f"invalid period: {value!r} "
            "(grammar: YYYY, YYYY-MM, HTYYYY/VTYYYY, YYYY-Q[1-4])"
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
        _validate_slug(provider, FqidKind.PROVIDER)
        return cls(kind=FqidKind.PROVIDER, provider=provider)

    @classmethod
    def register_fqid(cls, provider: str, register: str) -> Fqid:
        _validate_slug(provider, FqidKind.PROVIDER)
        _validate_slug(register, FqidKind.REGISTER)
        return cls(kind=FqidKind.REGISTER, provider=provider, register=register)

    @classmethod
    def register_variant_fqid(cls, provider: str, register: str, variant: str) -> Fqid:
        _validate_slug(provider, FqidKind.PROVIDER)
        _validate_slug(register, FqidKind.REGISTER)
        _validate_slug(variant, FqidKind.REGISTER_VARIANT, allow_default=True)
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
        _validate_slug(provider, FqidKind.PROVIDER)
        _validate_slug(register, FqidKind.REGISTER)
        _validate_slug(variant, FqidKind.REGISTER_VARIANT, allow_default=True)
        _validate_period(period)
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
        _validate_slug(provider, FqidKind.PROVIDER)
        _validate_slug(register, FqidKind.REGISTER)
        _validate_slug(variant, FqidKind.REGISTER_VARIANT, allow_default=True)
        _validate_period(period)
        _validate_slug(variable, "variable")
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
        _validate_slug(classification, FqidKind.CLASSIFICATION)
        _validate_slug(version, "classification version", allow_period=True)
        return cls(
            kind=FqidKind.CLASSIFICATION,
            classification=classification,
            version=version,
        )


def parse(value: str) -> Fqid:
    """Parse and validate an FQID string. Raises ``FqidError`` on any violation.

    Stored FQIDs never elide (§5.2): `sos/lss/_default/2022` is accepted, but
    the elided display form `sos/lss/2022` is rejected — the period-slug ban
    makes `2022` invalid in the variant slot.
    """
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
        return Fqid.register_variant_fqid(segs[0], segs[1], segs[2])
    if n == 4:
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
