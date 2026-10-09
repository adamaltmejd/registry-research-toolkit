"""The build's slug rules: reg-core's slug grammar plus the slot reservations.

The grammar is `reg_core_py.check_slug` (`RUST_RUNTIME_SPEC.md` section 5); what this
module adds is build policy with no Rust consumer: `_default` is the variant-less
register_variant coordinate, and two slugs are reserved in one slot each because an
SPA route would capture them (`reg_webapp/frontend/src/lib/router.svelte.ts`).
"""

from __future__ import annotations

import re
import unicodedata

from reg_core_py import GrammarError, check_slug

DEFAULT_VARIANT_SLUG = "_default"
# `/catalog/<provider>/<register>/variants` is the SPA's register variants page, so
# no variable may take the slug `variants`.
_RESERVED_VARIABLE_SLUG = "variants"
# `/catalog/group/<provider>/<register>/<key>` is the SPA's concept-group page and
# `group/...` the group ref, so no provider may take the slug `group`.
_RESERVED_PROVIDER_SLUG = "group"

_SLUG_NONALNUM = re.compile(r"[^a-z0-9]+")


def validate_slug(value: str, slot: str, *, allow_default: bool = False) -> None:
    """Refuse `value` as a slug in `slot` with a `GrammarError` naming the slot.

    `allow_default` admits `_default`, the register_variant coordinate.
    """
    if value == DEFAULT_VARIANT_SLUG:
        if allow_default:
            return
        raise GrammarError(
            f"`{DEFAULT_VARIANT_SLUG}` is reserved for the register_variant slug "
            f"(a delivery coordinate); got it in {slot}"
        )
    try:
        check_slug(value)
    except GrammarError as exc:
        raise GrammarError(f"invalid slug in {slot}: {exc}") from None
    if (slot, value) in (
        ("variable", _RESERVED_VARIABLE_SLUG),
        ("provider", _RESERVED_PROVIDER_SLUG),
    ):
        raise GrammarError(
            f"slug in {slot} is reserved: {value!r} (an SPA route captures it)"
        )


def derive_variable_slug(delivery_column_name: str | None) -> str | None:
    """The auto-slug of a delivery column name (reg_meta_build/DESIGN.md → Slug
    curation): NFKD ASCII fold, lowercase, runs of non-alphanumerics to one hyphen.

    `None` when the result is not a valid variable slug (empty, outside the grammar,
    or reserved), so the caller falls back to the name or last-resort slug.
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
    try:
        validate_slug(candidate, "variable")
    except GrammarError:
        return None
    return candidate
