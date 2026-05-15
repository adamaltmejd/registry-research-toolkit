"""FQID parser/emitter for regmeta.

Pure functions implementing REFACTOR_SPEC.md §5.2's FQID grammar. No DB
dependency: parsing, validation, and slug derivation happen in-memory.
``Catalog`` (catalog.py) layers DB resolution on top of these primitives.

Forms (§5.2):

  scb                                    1 segment   provider
  scb/lisa                               2 segments  register
  scb/lisa/individer-15plus              3 segments  register_variant
  scb/lisa/individer-15plus/2018         4 segments  register_version
  scb/lisa/individer-15plus/2018/kon     5 segments  variable binding
  class/sun/2020                         3 segments  classification

The leading ``class/`` token discriminates classification FQIDs from the
3-segment register_variant form; ``class`` is reserved everywhere else
to keep the discriminator unambiguous.
"""

from __future__ import annotations

import re
import sqlite3
import unicodedata
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

# Slugs and periods (§5.2). The §5.2 regex `^[a-z][a-z0-9-]*[a-z0-9]$` is paired
# with the prose rule "single hyphens only"; the form below enforces the latter
# (no double hyphens, no leading/trailing hyphens) directly.
_SLUG_RE = re.compile(r"^[a-z](?:-?[a-z0-9])*$")
_PERIOD_PATTERNS = (
    re.compile(r"^\d{4}$"),
    re.compile(r"^\d{4}-\d{2}$"),
    re.compile(r"^[HV]T\d{4}$"),
    re.compile(r"^\d{4}-Q[1-4]$"),
)


class FqidError(ValueError):
    """Raised when an FQID string fails grammar validation."""


def is_slug(value: str) -> bool:
    return bool(_SLUG_RE.match(value))


def is_period(value: str) -> bool:
    return any(p.match(value) for p in _PERIOD_PATTERNS)


def _validate_slug(value: str, slot: str, *, allow_default: bool = False) -> None:
    if value == DEFAULT_VARIANT_SLUG:
        if allow_default:
            return
        raise FqidError(
            f"`_default` is reserved for the register_variant slot; got it in {slot}"
        )
    if value == CLASSIFICATION_PREFIX:
        raise FqidError(
            f"`{CLASSIFICATION_PREFIX}` is reserved and may not appear as a slug "
            f"in {slot}"
        )
    # Period check first: gives the more specific error and catches digit-led
    # tokens (e.g. `2018`) that would otherwise be reported as a generic slug
    # grammar miss.
    if is_period(value):
        raise FqidError(
            f"slug in {slot} matches the period grammar: {value!r} "
            f"(period-shaped slugs are rejected outside the period slot for legibility)"
        )
    if not _SLUG_RE.match(value):
        raise FqidError(
            f"invalid slug in {slot}: {value!r} "
            f"(grammar: ^[a-z][a-z0-9-]*[a-z0-9]$ or single ^[a-z]$, single hyphens only)"
        )


def _validate_period(value: str) -> None:
    if not is_period(value):
        raise FqidError(
            f"invalid period: {value!r} "
            "(grammar: YYYY, YYYY-MM, HTYYYY/VTYYYY, YYYY-Q[1-4])"
        )


def _validate_classification_version(value: str) -> None:
    # Examples in §5.3 use bare years; the grammar isn't tightly constrained.
    # Accept either the period grammar (e.g. `2020`) or the slug grammar
    # (e.g. `v1`, `2-0`), and explicitly reject the reserved `class` token.
    if value == CLASSIFICATION_PREFIX:
        raise FqidError(
            f"`{CLASSIFICATION_PREFIX}` is reserved and may not appear as a "
            "classification version"
        )
    if value == DEFAULT_VARIANT_SLUG:
        raise FqidError(
            "`_default` is reserved for the register_variant slot, not "
            "classification version"
        )
    if is_period(value) or _SLUG_RE.match(value):
        return
    raise FqidError(
        f"invalid classification version: {value!r} "
        "(accepts period grammar or slug grammar)"
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

    def emit(self) -> str:
        """Serialize to canonical FQID string. Stored FQIDs never elide (§5.2)."""
        if self.kind is FqidKind.CLASSIFICATION:
            return f"{CLASSIFICATION_PREFIX}/{self.classification}/{self.version}"
        parts = [self.provider]
        for v in (self.register, self.variant, self.period, self.variable):
            if v is None:
                break
            parts.append(v)
        return "/".join(p for p in parts if p is not None)

    def __str__(self) -> str:
        return self.emit()

    # Factory constructors that re-validate. Useful where callers have raw
    # slug strings (e.g. emitting an FQID from DB rows) — bad input fails
    # fast rather than producing a malformed string.

    @classmethod
    def provider_fqid(cls, provider: str) -> Fqid:
        _validate_slug(provider, "provider")
        return cls(kind=FqidKind.PROVIDER, provider=provider)

    @classmethod
    def register_fqid(cls, provider: str, register: str) -> Fqid:
        _validate_slug(provider, "provider")
        _validate_slug(register, "register")
        return cls(kind=FqidKind.REGISTER, provider=provider, register=register)

    @classmethod
    def register_variant_fqid(cls, provider: str, register: str, variant: str) -> Fqid:
        _validate_slug(provider, "provider")
        _validate_slug(register, "register")
        _validate_slug(variant, "register_variant", allow_default=True)
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
        _validate_slug(provider, "provider")
        _validate_slug(register, "register")
        _validate_slug(variant, "register_variant", allow_default=True)
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
        _validate_slug(provider, "provider")
        _validate_slug(register, "register")
        _validate_slug(variant, "register_variant", allow_default=True)
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
        _validate_slug(classification, "classification")
        _validate_classification_version(version)
        return cls(
            kind=FqidKind.CLASSIFICATION,
            classification=classification,
            version=version,
        )


def parse(value: str) -> Fqid:
    """Parse and validate an FQID string. Raises ``FqidError`` on any violation.

    Stored FQIDs never elide (§5.2): `sos/lss/_default/2022` is accepted, but
    the elided display form `sos/lss/2022` is rejected here — the period-slug
    ban makes `2022` invalid in the variant slot.
    """
    if not isinstance(value, str) or not value:
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


# ---------------------------------------------------------------------------
# Variable slug derivation
# ---------------------------------------------------------------------------
#
# Variables are auto-slugged from kolumnnamn (§5.3). Build-time
# materialization lands with 1e; until then, query commands and Catalog
# resolve derive the slug on the fly with this helper.

_SLUG_NONALNUM = re.compile(r"[^a-z0-9]+")


def derive_variable_slug(kolumnnamn: str | None) -> str | None:
    """Derive a variable slug from a SCB kolumnnamn (column header).

    Lowercases, strips diacritics via NFKD decomposition, replaces runs of
    non-alphanumerics with single hyphens, and trims edge hyphens. Returns
    ``None`` if the input is empty or the result fails the slug grammar
    (e.g. starts with a digit).
    """
    if not kolumnnamn:
        return None
    decomposed = unicodedata.normalize("NFKD", kolumnnamn)
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    candidate = _SLUG_NONALNUM.sub("-", stripped.lower()).strip("-")
    if not candidate or not _SLUG_RE.match(candidate):
        return None
    if candidate in (DEFAULT_VARIANT_SLUG, CLASSIFICATION_PREFIX):
        return None
    if is_period(candidate):
        return None
    return candidate


# ---------------------------------------------------------------------------
# Build-time helper for query commands
# ---------------------------------------------------------------------------
#
# Variable bindings need a kolumnnamn → slug map at query time. The mapping
# is keyed on cvid because §5.3 says variables are slugged from the *latest*
# alias of a variable concept; that's a per-concept choice, but for now —
# until 1e materializes binding rows — query commands pick the alias that
# happens to be attached to the cvid they're emitting. Tested-fixture
# friendly.


def cvid_to_variable_slug(conn: sqlite3.Connection, cvid: int) -> str | None:
    """Pick a variable slug for a single cvid by hashing its alias rows.

    Returns the derived slug from the lexically-first non-empty kolumnnamn
    on the cvid. ``None`` if the cvid has no alias rows or every alias fails
    slug derivation. Pure read, no caching — small fan-out (typically 1-2
    aliases per cvid).
    """
    rows = conn.execute(
        "SELECT kolumnnamn FROM variable_alias WHERE cvid = ? ORDER BY kolumnnamn",
        (cvid,),
    ).fetchall()
    for row in rows:
        slug = derive_variable_slug(row[0])
        if slug is not None:
            return slug
    return None
