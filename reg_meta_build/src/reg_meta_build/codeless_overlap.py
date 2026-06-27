"""Curated resolution for the residual code-less ↔ code-bearing overlap class
(epic #858, residual characterized in #866).

After the automatic full-cover removal (`_drop_fullcover_codeless_states`, #867)
deletes the code-less `variable_state` rows (`value_set_id IS NULL`) FULLY covered
by code-bearing siblings on the same `(variable_id, register_variant_id,
delivery_column_name)`, a PARTIAL-coverage tail remains: a code-less span that
extends into years with no coded state (an "edge" = code-less past one end; an
"interior" = a coded window strictly inside, leaving ≥2 gap spans). The machine
cannot decide whether to extend the coding across the gap, accept the code-less
span, or close it at a coded boundary — it's a per-column maintainer call landed
here. See `db.py:_resolve_curated_codeless_overlaps` for the materialization.

A curation entry is keyed on `(register_slug, variable_slug, column)` — the same
human-readable coordinates the #866 worklist names (`naringsgren`/`SNI`,
`skollan`/`SkolLan`). Slugs are register-scoped-unique and resolvable at the pass
site (it runs after `populate_variable_slugs`); `column` matches a state's
`delivery_column_name` after case-folding (`fold_column`), so TOML casing is
cosmetic. A state can have `delivery_column_name IS NULL` (no delivery alias);
to curate such a residual, OMIT the `column` field — its key column component is
a `None` sentinel matching the NULL row. (A present-but-empty `column = ""` is
rejected as ambiguous: absent means NULL, not empty-string.) It resolves the
residual one of three ways:
  - `resolution = "cap"`  — trim the code-less state to the interval-complement of
    the union of overlapping code-bearing windows (delete if nothing residual,
    split into a code-less twin per residual span if an interior gap leaves ≥2).
  - `resolution = "drop"` — delete the code-less state outright.
  - `resolution = "extend"` — absorb the code-less span into the named coded
    vintage's window: the coded state on the SAME key whose
    `value_set_version_label` matches `extend = "<value_set_version_label>"` grows
    to cover the code-less span, and the code-less state is deleted. A coded vintage
    can carry an EMPTY/whitespace `value_set_version_label` (e.g. a binary `[0,1]`
    flag SCB delivered without a named value set — `par/typ-av-diagnos` HDIA,
    `par/atc-komplement-atgardskod` ATCO); target it with `extend = ""`, which
    matches the (necessarily unique) empty-label coded vintage on the key.

The `extend` KEY is required iff `resolution == "extend"` (a missing `extend` on an
`extend` entry stays an error — the typo guard), forbidden otherwise. Its VALUE may
be the empty string `""` (explicitly targeting the empty-label coded vintage); a
non-empty value names that label.
"""

from __future__ import annotations

import functools
from pathlib import Path

from ._curation import (
    curation_error,
    fold_column,
    load_curation_entries,
    require_str,
)

# Bind this loader's error code / prefix / file once (the shared leaf convention).
_require_str = functools.partial(
    require_str,
    code="codeless_overlap_invalid",
    prefix="codeless_overlap",
    file_name="codeless_overlap.toml",
)

# (register_slug, variable_slug, folded-delivery-column) — the human-authorable
# worklist coordinates. The column is folded to the same rule-2 connectivity key
# the SCB coalescer uses (`fold_column`), so a raw-cased pin still matches the
# stored `variable_state.delivery_column_name`. The column component is `None`
# (not `""`) when the entry OMITS `column`, matching a state whose
# `delivery_column_name IS NULL`.
CodelessOverlapKey = tuple[str, str, str | None]
# (resolution, extend_label) — `extend_label` is non-None iff resolution=="extend"
# (it can be `""`, explicitly targeting the empty-label coded vintage; None still
# means "no extend", used by cap/drop).
CodelessOverlapRule = tuple[str, str | None]
CodelessOverlapMap = dict[CodelessOverlapKey, CodelessOverlapRule]

_RESOLUTIONS = frozenset({"cap", "drop", "extend"})


def repo_codeless_overlap_path() -> Path | None:
    """`reg_meta_build/codeless_overlap.toml` from a repo checkout, or None (wheel
    installs don't ship curation — it's a maintainer artifact like the slug TOMLs
    and `codelivery.toml`)."""
    candidate = Path(__file__).resolve().parent.parent.parent / "codeless_overlap.toml"
    return candidate if candidate.is_file() else None


def load_codeless_overlap(path: Path | None) -> CodelessOverlapMap:
    """Parse the curation TOML into `{(register_slug, variable_slug, column):
    (resolution, extend_label)}`. Empty when no file (synthetic test builds, wheel
    installs).

    Load-time validation (all EXIT_CONFIG, actionable — mirrors `codelivery.py`):
      - Only `[[resolve]]` top-level (a misspelled `[[resolves]]` is a loud error,
        not a silent no-op that disables ALL curation); `resolve` is an array of
        tables (a scalar / single `[resolve]` table is rejected before the loop).
      - `register` / `variable` present non-empty strings; a missing/blank/
        non-string key part is curation drift, not a silent default. `column` is
        OPTIONAL: omit it to match a state with `delivery_column_name IS NULL`
        (key column component `None`); when present it must be a non-empty string
        (a `column = ""` is rejected — absent means NULL, not empty-string).
      - `resolution` is one of `cap` / `drop` / `extend` (an unknown directive is
        rejected, not silently ignored — every residual must be decided).
      - The `extend` KEY is present iff `resolution == "extend"`, forbidden
        otherwise — a `cap`/`drop` carrying a stray `extend`, or an `extend` entry
        with no `extend` key at all, is a malformed entry, not a no-op. The value
        must be a string but MAY be the empty string `""`: `extend = ""` explicitly
        targets the (unique) empty/whitespace-labelled coded vintage on the key (a
        binary flag SCB delivered without a named value set). Distinguish ABSENT
        (`entry.get("extend")` is None → error on an `extend` entry) from
        PRESENT-EMPTY (`""` → valid, empty-label target)."""
    entries = load_curation_entries(
        path,
        entry_key="resolve",
        label="code-less overlap",
        prefix="codeless_overlap",
        code_base="codeless_overlap",
        file_name="codeless_overlap.toml",
        entry_fields="register / variable / column / resolution",
    )
    out: CodelessOverlapMap = {}
    for entry in entries:
        # Slug key parts: register-scoped-unique, human-readable. A non-string or
        # blank value is curation drift → load-time error, not an inert pin
        # (shared `require_str` returns the narrowed, stripped string).
        ctx = "[[resolve]] entry"
        register = _require_str(entry, "register", ctx)
        variable = _require_str(entry, "variable", ctx)
        # `column` is OPTIONAL: ABSENT → `None` sentinel (matches a state with
        # `delivery_column_name IS NULL`); PRESENT → must be a non-empty string,
        # folded to the coalescer's rule-2 key so TOML casing is cosmetic (the
        # stored `delivery_column_name` is folded the same way for matching). A
        # present-but-empty `column = ""` is rejected as ambiguous — absent already
        # means NULL, so empty-string has no meaning.
        raw_column = entry.get("column")
        column: str | None
        if raw_column is None:
            column = None
        else:
            column = fold_column(_require_str(entry, "column", ctx))
        key: CodelessOverlapKey = (register, variable, column)
        if key in out:
            raise curation_error(
                "codeless_overlap_invalid",
                f"codeless_overlap has duplicate [[resolve]] entries for key {key}.",
                "Give exactly one [[resolve]] entry per (register, variable, column) "
                "in reg_meta_build/codeless_overlap.toml.",
            )
        resolution = entry.get("resolution")
        if not isinstance(resolution, str) or resolution not in _RESOLUTIONS:
            raise curation_error(
                "codeless_overlap_invalid",
                f"codeless_overlap entry {key} has unknown resolution "
                f"{resolution!r} (known: {sorted(_RESOLUTIONS)}).",
                f"Use a supported resolution: {sorted(_RESOLUTIONS)}.",
            )
        # `entry.get("extend")` returns None for an ABSENT key and `""` for a
        # PRESENT-but-empty `extend = ""` — the distinction the empty-label target
        # turns on. The KEY must be present iff resolution == "extend"; its VALUE may
        # be `""` (explicitly targets the unique empty-label coded vintage).
        extend_present = "extend" in entry
        extend = entry.get("extend")
        if resolution == "extend":
            if not extend_present:
                raise curation_error(
                    "codeless_overlap_invalid",
                    f'codeless_overlap entry {key} has `resolution = "extend"` but '
                    "no `extend` key.",
                    'Give `extend = "<value_set_version_label>"` naming a coded '
                    'vintage on the same key (use `extend = ""` to target the '
                    "empty-label coded vintage).",
                )
            if not isinstance(extend, str):
                raise curation_error(
                    "codeless_overlap_invalid",
                    f"codeless_overlap entry {key} has a non-string `extend`, got "
                    f"{extend!r}.",
                    'Give `extend = "<value_set_version_label>"` (a string; `""` '
                    "targets the empty-label coded vintage).",
                )
            # `extend = ""` keeps `extend_label == ""` (empty-label target); a
            # non-empty value is stripped to match the coded label modulo whitespace.
            extend_label: str | None = extend.strip()
        else:
            if extend_present:
                raise curation_error(
                    "codeless_overlap_invalid",
                    f"codeless_overlap entry {key} sets `extend` but its resolution "
                    f'is {resolution!r}, not "extend".',
                    'Drop the `extend` field, or set `resolution = "extend"`.',
                )
            extend_label = None
        out[key] = (resolution, extend_label)
    return out
