"""``reg_monabundle`` namespaced-block validator.

Each namespaced block under ``project_data.json`` is validated by its
owning package (see DESIGN.md → The two halves). This module
owns the ``reg_monabundle`` block: shape, allowed keys, ``binding_options``
binding-FQID well-formedness, and the ``suppress_k`` floor check against
``SUPPRESS_K``.

What this validator does NOT do:

- Resolve ``binding_options`` keys against the spec's ``sources[*].bindings``
  (the orphan-FQID check). That's a cross-block referential rule the
  loader runs after building the dataclass tree — it requires the
  resolved column dataclasses, which this layer doesn't accept.
- Cross-check ``suppress_k`` against the column's declared ``type``
  (it only makes sense on categorical columns). Same reason — needs
  the resolved column tree. Lives in
  ``reg_monabundle.build.spec_loader._validate_binding_options_against_columns``
  (build-time gate; see DESIGN.md → The two halves — the bundle runtime
  trusts the embedded JSON).
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, cast

from .constants import SUPPRESS_K

if TYPE_CHECKING:
    from collections.abc import Mapping

# Per-column option keys recognised in ``reg_monabundle.binding_options``.
# Strict: anything not in this set raises at parse time (typo guard).
VALID_OPTION_KEYS: tuple[str, ...] = ("suppress_k",)

# Binding-FQID well-formedness for ``reg_monabundle.binding_options``
# keys. Mirrors ``reg_schema.structural._is_binding_fqid`` so a typo
# (display_name, whitespace, empty segment, ``class/...``) raises
# loudly instead of silently no-opping at lookup time. The duplication
# is deliberate per reg_schema/DESIGN.md "Why no FQID parser dependency".
_FQID_TOKEN: re.Pattern[str] = re.compile(r"^[A-Za-z0-9_-]+$")


def _is_binding_fqid(value: object) -> bool:
    """A 3-segment ``<provider>/<register>/<slug>`` binding FQID (non-``class``
    provider, per-segment ``[A-Za-z0-9_-]+``).

    Mirrors ``reg_schema.structural._is_binding_fqid``: the value set is
    resolved from ``(variable, variant, period)``, not pinned on the
    FQID, so a binding leaf is a bare slug with no ``@version`` suffix.
    """
    if not isinstance(value, str):
        return False
    segs = value.split("/")
    if len(segs) != 3 or segs[0] == "class":
        return False
    return all(bool(_FQID_TOKEN.match(s)) for s in segs)


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
    allowed = {"binding_options"}
    extra = set(block_obj) - allowed
    if extra:
        raise ValueError(
            f"reg_monabundle has unknown key(s) {sorted(extra)} "
            f"(allowed: {sorted(allowed)})"
        )
    options = block_obj.get("binding_options")
    if options is None:
        return
    if not isinstance(options, dict):
        raise ValueError(
            f"reg_monabundle.binding_options must be an object, "
            f"got {type(options).__name__}"
        )
    for fqid, opts in options.items():
        # Binding FQIDs are 3-segment slash-separated identifiers
        # (``<provider>/<register>/<slug>``) with per-segment
        # ``[A-Za-z0-9_-]+`` tokens and a non-``class`` provider. The
        # structural validator (see reg_schema/DESIGN.md → Structural rules
        # and issue codes) checks well-formedness on
        # ``binding.variable``; reg_monabundle.binding_options keys are opaque
        # to reg_schema. Mirror the same rule here so a typo (display_name,
        # whitespace, empty segment, ``class/...``) raises loudly instead of
        # silently no-opping at lookup time.
        if not _is_binding_fqid(fqid):
            raise ValueError(
                f"reg_monabundle.binding_options key {fqid!r} is not a "
                f"well-formed binding FQID (expected 3 slash-separated "
                f"segments of [A-Za-z0-9_-]+, non-'class' provider); keys are "
                f"binding FQIDs. Update your project_data.json."
            )
        if not isinstance(opts, dict):
            raise ValueError(
                f"reg_monabundle.binding_options[{fqid!r}] must be an object, "
                f"got {type(opts).__name__}"
            )
        for key, val in opts.items():
            if key not in VALID_OPTION_KEYS:
                raise ValueError(
                    f"reg_monabundle.binding_options[{fqid!r}] has unknown "
                    f"option {key!r} (allowed: {sorted(VALID_OPTION_KEYS)})"
                )
            if key == "suppress_k":
                if isinstance(val, bool) or not isinstance(val, int):
                    raise ValueError(
                        f"reg_monabundle.binding_options[{fqid!r}].suppress_k "
                        f"must be an int, got {type(val).__name__} ({val!r})"
                    )
                if val < SUPPRESS_K:
                    raise ValueError(
                        f"reg_monabundle.binding_options[{fqid!r}].suppress_k="
                        f"{val} is below the global minimum SUPPRESS_K="
                        f"{SUPPRESS_K}; overrides may only raise the "
                        f"threshold, not lower it"
                    )
