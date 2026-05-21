"""``reg_monabundle`` namespaced-block validator (§6.8.2).

Per REFACTOR_SPEC.md §6.8.2, each namespaced block under
``project_data.json`` is validated by its owning package. This module
owns the ``reg_monabundle`` block: shape, allowed keys, ``column_options``
binding-FQID well-formedness, and the ``suppress_k`` floor check against
``SUPPRESS_K``.

What this validator does NOT do:

- Resolve ``column_options`` keys against the spec's ``sources[*].columns``
  (the orphan-FQID check). That's a cross-block referential rule the
  loader runs after building the dataclass tree — it requires the
  resolved column dataclasses, which this layer doesn't accept.
- Cross-check ``suppress_k`` against the column's declared ``type``
  (it only makes sense on categorical columns). Same reason — needs
  the resolved column tree. Lives in
  ``mock_data_wizard.spec._validate_column_options_against_columns``
  (and will follow the validator to its owner in a later phase).
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


def _is_binding_fqid(value: object) -> bool:
    if not isinstance(value, str):
        return False
    segs = value.split("/")
    return (
        len(segs) == 5
        and segs[0] != "class"
        and all(bool(_FQID_TOKEN.match(s)) for s in segs)
    )


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
        # Binding FQIDs are 5-segment slash-separated identifiers with
        # per-segment ``[A-Za-z0-9_-]+`` tokens and a non-``class``
        # provider. The structural validator (§6.8.1) checks
        # well-formedness on ``column.name``; reg_monabundle.column_options
        # keys are opaque to reg_schema. Mirror the same rule here so a
        # typo (display_name, whitespace, empty segment, ``class/...``)
        # raises loudly instead of silently no-opping at lookup time.
        if not _is_binding_fqid(fqid):
            raise ValueError(
                f"reg_monabundle.column_options key {fqid!r} is not a "
                f"well-formed binding FQID (expected 5 slash-separated "
                f"segments of [A-Za-z0-9_-]+, non-'class' provider); "
                f"keys changed from (source, column) pairs to binding "
                f"FQIDs in step 4. Update your project_data.json."
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
