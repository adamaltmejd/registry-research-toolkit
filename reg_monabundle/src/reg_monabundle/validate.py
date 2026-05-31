"""``reg_monabundle`` namespaced-block validator (§6.8.2).

Per REFACTOR_SPEC.md §6.8.2, each namespaced block under
``project_data.json`` is validated by its owning package. This module
owns the ``reg_monabundle`` block: shape, allowed keys, ``column_options``
binding-FQID well-formedness, and the ``suppress_k`` floor check against
``SUPPRESS_K``.

What this validator does NOT do:

- Resolve ``column_options`` keys against the spec's ``sources[*].bindings``
  (the orphan-FQID check). That's a cross-block referential rule the
  loader runs after building the dataclass tree — it requires the
  resolved column dataclasses, which this layer doesn't accept.
- Cross-check ``suppress_k`` against the column's declared ``type``
  (it only makes sense on categorical columns). Same reason — needs
  the resolved column tree. Lives in
  ``reg_monabundle.runtime.spec._validate_column_options_against_columns``
  alongside the rest of the runtime spec adapter.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, cast

from .constants import SUPPRESS_K

if TYPE_CHECKING:
    from collections.abc import Mapping

# Per-column option keys recognised in ``reg_monabundle.column_options``.
# Strict: anything not in this set raises at parse time (typo guard).
VALID_OPTION_KEYS: tuple[str, ...] = ("suppress_k",)

# Binding-FQID well-formedness for ``reg_monabundle.column_options``
# keys. Mirrors ``reg_schema.structural._is_binding_fqid`` so a typo
# (display_name, whitespace, empty segment, ``class/...``) raises
# loudly instead of silently no-opping at lookup time. The duplication
# is deliberate per reg_schema/DESIGN.md "Why no FQID parser dependency".
_FQID_TOKEN: re.Pattern[str] = re.compile(r"^[A-Za-z0-9_-]+$")


def _binding_leaf_parts(leaf: str) -> list[str] | None:
    """Split a binding-FQID leaf ``slug[@version]`` into its tokens.

    Mirrors ``reg_schema.structural._binding_leaf_parts``: returns
    ``[slug]`` or ``[slug, version]``, or ``None`` when malformed (empty
    slug, empty version, stray second ``@``). The ``@`` is split off
    before the per-segment regex so it never reaches the slug grammar.
    """
    if "@" not in leaf:
        return [leaf] if leaf else None
    slug, _, version = leaf.partition("@")
    if not slug or not version or "@" in version:
        return None
    return [slug, version]


def _is_binding_fqid(value: object) -> bool:
    if not isinstance(value, str):
        return False
    segs = value.split("/")
    if len(segs) != 3 or segs[0] == "class":
        return False
    leaf_parts = _binding_leaf_parts(segs[2])
    if leaf_parts is None:
        return False
    return all(bool(_FQID_TOKEN.match(s)) for s in [segs[0], segs[1], *leaf_parts])


def validate_block(block: object) -> None:
    """Validate the ``reg_monabundle`` namespaced block.

    Raises ``ValueError`` with a concrete, message-stable error on the
    first violation. ``block`` may be ``None`` (no namespaced block),
    a dict (validated), or any other type (rejected).
    """
    if block is None:
        return
    if not isinstance(block, dict):
        raise ValueError(
            f"reg_monabundle block must be an object, got {type(block).__name__}"
        )
    block_obj = cast("Mapping[str, Any]", block)
    allowed = {"column_options"}
    extra = set(block_obj) - allowed
    if extra:
        raise ValueError(
            f"reg_monabundle has unknown key(s) {sorted(extra)} "
            f"(allowed: {sorted(allowed)})"
        )
    options = block_obj.get("column_options")
    if options is None:
        return
    if not isinstance(options, dict):
        raise ValueError(
            f"reg_monabundle.column_options must be an object, "
            f"got {type(options).__name__}"
        )
    for fqid, opts in options.items():
        # Binding FQIDs are 3-segment slash-separated identifiers
        # (``<provider>/<register>/<slug>``, optional ``@<version>`` on
        # the slug) with per-segment ``[A-Za-z0-9_-]+`` tokens and a
        # non-``class`` provider. The structural validator (§6.8.1)
        # checks well-formedness on ``binding.variable``;
        # reg_monabundle.column_options keys are opaque to reg_schema.
        # Mirror the same rule here so a typo (display_name, whitespace,
        # empty segment, ``class/...``) raises loudly instead of
        # silently no-opping at lookup time.
        if not _is_binding_fqid(fqid):
            raise ValueError(
                f"reg_monabundle.column_options key {fqid!r} is not a "
                f"well-formed binding FQID (expected 3 slash-separated "
                f"segments of [A-Za-z0-9_-]+ with an optional @<version> "
                f"on the slug, non-'class' provider); keys are binding "
                f"FQIDs. Update your project_data.json."
            )
        if not isinstance(opts, dict):
            raise ValueError(
                f"reg_monabundle.column_options[{fqid!r}] must be an object, "
                f"got {type(opts).__name__}"
            )
        for key, val in opts.items():
            if key not in VALID_OPTION_KEYS:
                raise ValueError(
                    f"reg_monabundle.column_options[{fqid!r}] has unknown "
                    f"option {key!r} (allowed: {sorted(VALID_OPTION_KEYS)})"
                )
            if key == "suppress_k":
                if isinstance(val, bool) or not isinstance(val, int):
                    raise ValueError(
                        f"reg_monabundle.column_options[{fqid!r}].suppress_k "
                        f"must be an int, got {type(val).__name__} ({val!r})"
                    )
                if val < SUPPRESS_K:
                    raise ValueError(
                        f"reg_monabundle.column_options[{fqid!r}].suppress_k="
                        f"{val} is below the global minimum SUPPRESS_K="
                        f"{SUPPRESS_K}; overrides may only raise the "
                        f"threshold, not lower it"
                    )
