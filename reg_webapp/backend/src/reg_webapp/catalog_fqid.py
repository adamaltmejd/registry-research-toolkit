"""§16 per-segment FQID slug-grammar allow-list for the catalog catch-all.

This is the single chokepoint guarding ``/api/catalog/{fqid:path}`` (§9.5
"Path-traversal rejection", §16 "Server-side input-validation gates"). It runs
**before** any reg_meta lookup, so a malformed or traversal-shaped FQID returns
422 with ZERO SQL executed — the guard, not the resolver, rejects it.

Starlette URL-decodes the ``{fqid:path}`` before the handler sees it, so
percent-encoded probes (``%2e%2e``, ``%2f``, ``%00``) arrive already decoded and
fail the per-segment slug check like any other non-slug character.

Single source of truth: each segment is validated by **delegating** to
``reg_meta.fqid.validate_slug`` — we do NOT re-encode the slug regex here (a
second copy would drift from the grammar reg_meta enforces at build time). The
only literal this module admits beyond the slug grammar is ``class`` (§5.2's
classification-root sentinel, which ``validate_slug`` rejects); ``_default`` (the
variant coordinate) is NOT a catalog path segment — variants are a register
sub-resource (§9.5), never a ``/api/catalog/{fqid}`` segment — so it is rejected
like any other reserved token. A binding leaf is a bare slug: the ``@version``
value-set-version pin is **retired** (the value set is determined by the resolved
``(variable, variant, period)``, not pinned on the FQID), so ``@`` is just a
non-slug character that 422s like any other. (The catalog browse still offers a
separate, query-string ``?value_set_version`` label filter — that is unrelated to
the path grammar guarded here.)

The module is intentionally reg_meta-only and FastAPI-free so it's unit-testable
in isolation and reusable by A5.2's suffixed routes (``/states`` etc.).
"""

from __future__ import annotations

from dataclasses import dataclass

from reg_meta.fqid import (
    CLASSIFICATION_PREFIX,
    FqidError,
    validate_slug,
)

# `class` is the one §5.2 literal that is a valid catalog PATH segment but which
# `validate_slug` rejects as a slug — and ONLY as the LEADING classification
# prefix (`class` root or `class/<slug>`). In any other slot (provider, register,
# variable) `class` is a reserved token: it must 422 at the guard, NOT be admitted
# into a slot reg_meta later rejects — e.g. `class/<x>/variants` would otherwise
# construct `Fqid.register_fqid('class', …)` → FqidError → an HTTP 500 instead of a
# clean 422. So `_validate_segment` admits the literal only at the prefix position
# (`is_prefix`), never unconditionally. `_default` (the variant coordinate) is
# NEVER a catalog path segment — variants are a register sub-resource (§9.5) — so
# it always fails the slug grammar (422).


class FqidPathError(ValueError):
    """Raised when a raw ``{fqid:path}`` fails the §16 per-segment allow-list.

    The catalog router maps this to HTTP 422 (before any Catalog call), so a
    rejection means zero SQL was executed.
    """


@dataclass(frozen=True)
class ValidatedFqidPath:
    """A path that passed the §16 allow-list.

    ``fqid`` is the validated FQID string to hand to ``reg_meta.fqid.parse``. A
    binding leaf is a bare slug — there is no ``@version`` pin to strip (retired).
    """

    fqid: str


def _validate_segment(segment: str, *, slot: str, is_prefix: bool = False) -> None:
    """Validate one path segment against the §5.2 allow-list (slug grammar, plus
    the ``class`` classification-prefix literal). Raises ``FqidPathError`` on any
    violation — including an empty segment (``//`` or a leading/trailing ``/``).

    ``is_prefix`` (the leading segment) admits the ``class`` literal; everywhere
    else ``class`` is a reserved token ``validate_slug`` rejects (→ 422), so it
    can never reach a provider/register/variable/version slot that reg_meta would
    reject downstream with a 500."""
    if not segment:
        raise FqidPathError("FQID contains an empty segment")
    if is_prefix and segment == CLASSIFICATION_PREFIX:
        return
    try:
        validate_slug(segment, slot)
    except FqidError as exc:
        raise FqidPathError(str(exc)) from exc


def validate_fqid_path(raw_path: str) -> ValidatedFqidPath:
    """§16 chokepoint: validate every segment of ``raw_path`` before resolution.

    Splits on the structural ``/`` and validates each segment against the slug
    grammar (via ``reg_meta.fqid.validate_slug``) or the leading ``class``
    classification-prefix literal. A binding leaf is a bare slug — the ``@version``
    pin is retired, so ``@`` is just a non-slug character (the value set is
    resolved from ``(variable, variant, period)``, not pinned on the FQID).

    Returns a ``ValidatedFqidPath`` carrying the validated FQID. Raises
    ``FqidPathError`` (→ 422, zero SQL) on anything else: empty segment, ``.``,
    ``..``, ``%``, ``\\``, NUL, ``@``, or any other non-slug character.
    """
    if not raw_path:
        raise FqidPathError("empty FQID path")
    # A leading/trailing slash or `//` yields an empty segment — caught below.
    segments = raw_path.split("/")
    leaf_index = len(segments) - 1
    for index, segment in enumerate(segments):
        slot = "variable" if index == leaf_index and len(segments) == 3 else "segment"
        _validate_segment(segment, slot=slot, is_prefix=(index == 0))

    return ValidatedFqidPath(fqid="/".join(segments))
