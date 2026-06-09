"""Fold-override curation (see DESIGN.md → Build-time triage (SCB)): force
same-concept delivery columns that DON'T share a stem to FOLD into one variable.

The SCB triage (`sources/scb.py`) partitions a split container's contested
columns into fold-clusters PURELY on the column stem (`_cluster_contested`): a
shared stem + representation-only suffix folds (`Ssyk3`/`Ssyk5`), a disjoint stem
splits. That is right for the vast majority of cases — but a register sometimes
delivers ONE concept under columns that share no stem (e.g. näringsgren as
`Ksjusni` / `NG1` / `bransch` / `sni2`). The stem rule cannot recover that; the
maintainer asserts it here, and the override pre-seeds the union-find so those
columns fold by fiat (`forced_same`, bypassing the stem verify).

An entry is keyed on `(register_id, var_id)` — the SCB source ids the triage
carries (RegisterId + VarId == `variable.provider_key`, both numeric for SCB),
identical to codelivery's first two key parts. Each entry lists the disjoint-stem
columns that ARE one concept. One `[[fold]]` is ONE fold group for one
`(register_id, var_id)`; a var with two independent fold groups gets two `[[fold]]`
entries with that same key. Keying per-(register, var) makes a fold group spanning
MULTIPLE variables unrepresentable by construction.

This surface is the curated escape hatch the #223 per-cluster splitter left open;
contrast `codelivery.py`, which resolves a different conflict (two value-set
codings on ONE column in one period). Both are maintainer artifacts like the slug
TOMLs — absent in wheel installs and synthetic test builds (empty map).
"""

from __future__ import annotations

import tomllib
from pathlib import Path

from reg_meta.errors import EXIT_CONFIG, RegMetaError

# (register_id, var_id) — the same coordinates the triage's per-var split
# container carries (`register.register_id`, `variable.provider_key`).
FoldOverrideKey = tuple[int, int]
# Each var's curated fold groups, ready to hand to `_cluster_contested`'s
# `forced_same`: every frozenset is one column set that folds into one variable.
FoldOverrideMap = dict[FoldOverrideKey, list[frozenset[str]]]

# The only legal top-level table is `[[fold]]`. Anything else (a misspelled
# `[[folds]]`, a stray key) is a typo that would otherwise silently disable ALL
# curation — reject it loudly, mirroring fqid_slugs' strict top-level typo check.
_ALLOWED_TOPLEVEL_KEYS = frozenset({"fold"})


def _fold_override_error(code: str, message: str, remediation: str) -> RegMetaError:
    """A configuration-class error (EXIT_CONFIG). `fold_overrides.toml` is
    maintainer-edited curation data like the slug / codelivery TOMLs, so a syntax
    typo or a malformed entry is a config failure with actionable remediation —
    not an internal build bug (which is how a raw tomllib/ValueError would surface
    through the CLI's generic handler)."""
    return RegMetaError(
        exit_code=EXIT_CONFIG,
        code=code,
        error_class="configuration",
        message=message,
        remediation=remediation,
    )


def repo_fold_overrides_path() -> Path | None:
    """`reg_meta_build/fold_overrides.toml` from a repo checkout, or None (wheel
    installs don't ship curation — it's a maintainer artifact like the slug
    TOMLs). Sits at the package root, NOT under `fqid_slugs/` (that dir is
    glob-loaded as provider-slug TOMLs; a file there would break the build)."""
    candidate = Path(__file__).resolve().parent.parent.parent / "fold_overrides.toml"
    return candidate if candidate.is_file() else None


def _canonical_int(value: object) -> int | None:
    """Coerce a TOML `register_id` / `var_id` value to its canonical int, or None
    if it isn't one. A TOML integer is already canonical (the format forbids
    leading zeros); a string is accepted only in canonical form — no leading
    zeros, so `"01"` can't alias `1` (mirrors fqid_slugs `_parse_canonical_int`).
    A bool (TOML true/false, a Python int subclass) and a float are rejected."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str):
        if not value or not value.isdigit():
            return None
        if len(value) > 1 and value[0] == "0":
            return None
        return int(value)
    return None


def load_fold_overrides(path: Path | None) -> FoldOverrideMap:
    """Parse the curation TOML into `{(register_id, var_id): [frozenset(cols), …]}`.
    Empty when no file (synthetic test builds, wheel installs).

    Load-time validation (all EXIT_CONFIG, actionable):
      - Only `[[fold]]` top-level (a misspelled `[[folds]]` is a loud error, not a
        silent no-op); `fold` is an array of tables.
      - `register_id` / `var_id` present and canonical int (no leading zeros).
      - `columns` is a list of ≥2 non-empty strings (a singleton fold is a no-op).
      - No column repeats within a group, or across groups of the SAME
        `(register_id, var_id)` (duplicate / overlapping group).

    The build-time half (every named column must be a CONTESTED column of a real
    split container, and every key must be consumed) lives in `_triage_groups`,
    where the contested set is known.
    """
    if path is None or not path.is_file():
        return {}
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise _fold_override_error(
            "fold_override_toml_unreadable",
            f"Could not parse fold-override curation TOML {path}: {exc}",
            "Fix the TOML syntax in reg_meta_build/fold_overrides.toml.",
        ) from exc
    unknown_top = set(data) - _ALLOWED_TOPLEVEL_KEYS
    if unknown_top:
        raise _fold_override_error(
            "fold_override_invalid",
            f"fold-override TOML has unknown top-level key(s): {sorted(unknown_top)}.",
            "The only legal table is `[[fold]]` — check for a typo like "
            "`[[folds]]` in reg_meta_build/fold_overrides.toml.",
        )
    fold_entries = data.get("fold", [])
    if not isinstance(fold_entries, list):
        raise _fold_override_error(
            "fold_override_invalid",
            f"fold-override `fold` must be an array of tables (`[[fold]]`), got "
            f"{type(fold_entries).__name__}.",
            "Use `[[fold]]` table entries in reg_meta_build/fold_overrides.toml, "
            "not `fold = …` or a single `[fold]` table.",
        )
    out: FoldOverrideMap = {}
    seen_cols: dict[FoldOverrideKey, set[str]] = {}
    for entry in fold_entries:
        if not isinstance(entry, dict):
            raise _fold_override_error(
                "fold_override_invalid",
                f"fold-override entry {entry!r} must be a `[[fold]]` table.",
                "Each entry is a `[[fold]]` table with register_id / var_id / columns.",
            )
        reg = _canonical_int(entry.get("register_id"))
        var = _canonical_int(entry.get("var_id"))
        if reg is None or var is None:
            raise _fold_override_error(
                "fold_override_invalid",
                f"fold-override entry {entry!r} needs `register_id` and `var_id` "
                f"as canonical integers (no leading zeros).",
                "Each [[fold]] entry needs integer `register_id` and `var_id`.",
            )
        key: FoldOverrideKey = (reg, var)
        columns = entry.get("columns")
        if (
            not isinstance(columns, list)
            or len(columns) < 2
            or not all(isinstance(c, str) and c for c in columns)
        ):
            raise _fold_override_error(
                "fold_override_invalid",
                f"fold-override entry {key} `columns` must be a list of ≥2 "
                f"non-empty strings (a singleton fold is a no-op).",
                'Give `columns = ["ColA", "ColB", …]` with at least two columns.',
            )
        # `str(c)` is a no-op past the guard above, but it gives the frozenset a
        # concrete `str` element type (tomllib values are `Any`).
        group: frozenset[str] = frozenset(str(c) for c in columns)
        if len(group) != len(columns):
            raise _fold_override_error(
                "fold_override_invalid",
                f"fold-override entry {key} repeats a column within its group: "
                f"{columns}.",
                "List each column once per [[fold]] group.",
            )
        prior = seen_cols.setdefault(key, set())
        overlap = group & prior
        if overlap:
            raise _fold_override_error(
                "fold_override_invalid",
                f"fold-override key {key} has column(s) {sorted(overlap)} in more "
                f"than one [[fold]] group.",
                "Each column belongs to exactly one fold group per "
                "(register, var); merge the groups or remove the duplicate.",
            )
        prior |= group
        out.setdefault(key, []).append(group)
    return out
