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
like any other reserved token. Plus the ONE binding-leaf carve-out: a leaf of the
form ``slug@version`` is split on the
single ``@`` and each half validated separately (slug grammar both sides; the
version part is the classification-slug / ``value_set_version_label`` grammar,
which is the slug grammar). ``@`` is the only non-slug character admitted, and
only as the single leaf delimiter — a second ``@`` (or ``@`` anywhere but the
leaf) is rejected.

A5.1b-ii validates ``@version`` but does NOT yet narrow on it: ``parse`` is
fed the bare 3-segment FQID and ``@version`` narrowing is A5.2's
``?value_set_version`` (documented, deferred).

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

# The one §5.2 literal that is a valid catalog PATH segment but which
# `validate_slug` rejects as a slug: `class` (the classification-root sentinel /
# classification-prefix segment). `_default` (the variant coordinate) is NOT a
# catalog path segment — variants are a register sub-resource (§9.5), never a
# `/api/catalog/{fqid}` segment — so it is not admitted; a `_default` segment
# fails the slug grammar like any other reserved token (422 at the guard, before
# any DB access). Anything else must pass the slug grammar.
_ALLOWED_LITERALS = frozenset({CLASSIFICATION_PREFIX})

# Only the binding LEAF (3rd segment) may carry the value-set-version pin.
_LEAF_VERSION_DELIM = "@"


class FqidPathError(ValueError):
    """Raised when a raw ``{fqid:path}`` fails the §16 per-segment allow-list.

    The catalog router maps this to HTTP 422 (before any Catalog call), so a
    rejection means zero SQL was executed.
    """


@dataclass(frozen=True)
class ValidatedFqidPath:
    """A path that passed the §16 allow-list.

    ``fqid`` is the bare FQID string to hand to ``reg_meta.fqid.parse`` — for a
    binding leaf carrying ``@version`` this is the slug part only (the
    ``@version`` pin is stripped; ``value_set_version`` carries it forward for
    A5.2 narrowing). ``value_set_version`` is the stripped version (None when no
    ``@`` was present).
    """

    fqid: str
    value_set_version: str | None


def _validate_segment(segment: str, *, slot: str) -> None:
    """Validate one path segment against the §5.2 allow-list (slug grammar +
    the reserved literals). Raises ``FqidPathError`` on any violation —
    including an empty segment (``//`` or a leading/trailing ``/``)."""
    if not segment:
        raise FqidPathError("FQID contains an empty segment")
    if segment in _ALLOWED_LITERALS:
        return
    try:
        validate_slug(segment, slot)
    except FqidError as exc:
        raise FqidPathError(str(exc)) from exc


def validate_fqid_path(raw_path: str) -> ValidatedFqidPath:
    """§16 chokepoint: validate every segment of ``raw_path`` before resolution.

    Splits on the structural ``/`` and validates each segment against the slug
    grammar (via ``reg_meta.fqid.validate_slug``) or the ``class`` / ``_default``
    literals. The single carve-out: the binding LEAF (a 3-segment path) may carry
    ``slug@version`` — split on the one ``@``, both halves validated as slugs.

    Returns a ``ValidatedFqidPath`` carrying the bare FQID (``@version`` stripped)
    plus the stripped ``value_set_version`` (None when absent). Raises
    ``FqidPathError`` (→ 422, zero SQL) on anything else: empty segment, ``.``,
    ``..``, ``%``, ``\\``, NUL, a second ``@``, or ``@`` outside the leaf.
    """
    if not raw_path:
        raise FqidPathError("empty FQID path")
    # A leading/trailing slash or `//` yields an empty segment — caught below.
    segments = raw_path.split("/")

    value_set_version: str | None = None
    leaf_index = len(segments) - 1
    # The `@version` carve-out applies ONLY to a 3-segment binding leaf (§5.2:
    # version is value-set-bindable only on the variable). Restricting it to the
    # leaf of a 3-seg path keeps `@` out of provider/register/classification
    # segments and out of shorter/longer paths (a stray `@` there is a reject).
    bare_leaf = segments[leaf_index]
    if _LEAF_VERSION_DELIM in bare_leaf and len(segments) == 3:
        slug_part, _, version_part = bare_leaf.partition(_LEAF_VERSION_DELIM)
        # `partition` splits on the FIRST `@`; a second `@` lands in
        # `version_part`, so reject it explicitly (only ONE `@`, as the single
        # leaf delimiter, is admitted).
        if _LEAF_VERSION_DELIM in version_part:
            raise FqidPathError(
                f"binding leaf admits at most one '{_LEAF_VERSION_DELIM}' "
                f"(value-set-version delimiter): {bare_leaf!r}"
            )
        _validate_segment(version_part, slot="value_set_version")
        value_set_version = version_part
        segments = [*segments[:leaf_index], slug_part]
    elif _LEAF_VERSION_DELIM in raw_path:
        # `@` anywhere other than a 3-seg binding leaf is not a legal segment
        # char — reject before validate_slug would (clearer error, same 422).
        raise FqidPathError(
            f"'{_LEAF_VERSION_DELIM}' is only legal on a binding leaf "
            f"(value-set-version pin): {raw_path!r}"
        )

    for index, segment in enumerate(segments):
        slot = "variable" if index == leaf_index and len(segments) == 3 else "segment"
        _validate_segment(segment, slot=slot)

    return ValidatedFqidPath(
        fqid="/".join(segments), value_set_version=value_set_version
    )
