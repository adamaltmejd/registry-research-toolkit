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

from pathlib import Path

from ._curation import canonical_int, curation_error, fold_column, load_curation_entries

# (register_id, var_id) — the same coordinates the triage's per-var split
# container carries (`register.register_id`, `variable.provider_key`).
FoldOverrideKey = tuple[int, int]
# Each var's curated fold groups, ready to hand to `_cluster_contested`'s
# `forced_same`: every frozenset is one CASE-FOLDED column set that folds into
# one variable. Folded at load because the triage's contested columns are the
# coalescer's case-folded rule-2 components (`fold_column`) — a raw-cased entry
# would silently never match.
FoldOverrideMap = dict[FoldOverrideKey, list[frozenset[str]]]


def repo_fold_overrides_path() -> Path | None:
    """`reg_meta_build/fold_overrides.toml` from a repo checkout, or None (wheel
    installs don't ship curation — it's a maintainer artifact like the slug
    TOMLs). Sits at the package root, NOT under `fqid_slugs/` (that dir is
    glob-loaded as provider-slug TOMLs; a file there would break the build)."""
    candidate = Path(__file__).resolve().parent.parent.parent / "fold_overrides.toml"
    return candidate if candidate.is_file() else None


def load_fold_overrides(path: Path | None) -> FoldOverrideMap:
    """Parse the curation TOML into `{(register_id, var_id): [frozenset(cols), …]}`
    with case-folded columns. Empty when no file (synthetic builds, wheels).

    Load-time validation (all EXIT_CONFIG, actionable):
      - Only `[[fold]]` top-level (a misspelled `[[folds]]` is a loud error, not a
        silent no-op); `fold` is an array of tables.
      - `register_id` / `var_id` present and canonical int (no leading zeros).
      - `columns` is a list of ≥2 non-empty strings (a singleton fold is a no-op).
      - No column repeats within a group, or across groups of the SAME
        `(register_id, var_id)` (duplicate / overlapping group) — compared on the
        case-folded form (`_curation.fold_column`).

    The build-time half (every named column must be a CONTESTED column of a real
    split container, and every key must be consumed) lives in `_triage_groups`,
    where the contested set is known.
    """
    # Shared scaffold (parse + top-level typo guard + array-of-tables +
    # per-entry table check) — see `_curation.load_curation_entries`.
    entries = load_curation_entries(
        path,
        entry_key="fold",
        label="fold-override",
        prefix="fold-override",
        code_base="fold_override",
        file_name="fold_overrides.toml",
        entry_fields="register_id / var_id / columns",
    )
    out: FoldOverrideMap = {}
    seen_cols: dict[FoldOverrideKey, set[str]] = {}
    for entry in entries:
        reg = canonical_int(entry.get("register_id"))
        var = canonical_int(entry.get("var_id"))
        if reg is None or var is None:
            raise curation_error(
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
            raise curation_error(
                "fold_override_invalid",
                f"fold-override entry {key} `columns` must be a list of ≥2 "
                f"non-empty strings (a singleton fold is a no-op).",
                'Give `columns = ["ColA", "ColB", …]` with at least two columns.',
            )
        # Case-folded to the rule-2 connectivity key so the group matches the
        # triage's (folded) contested components; TOML casing is cosmetic.
        group: frozenset[str] = frozenset(fold_column(c) for c in columns)
        if len(group) != len(columns):
            raise curation_error(
                "fold_override_invalid",
                f"fold-override entry {key} repeats a column within its group "
                f"(after case-folding): {columns}.",
                "List each column once per [[fold]] group; case/diacritic twins "
                "collapse automatically and must not be spelled out.",
            )
        prior = seen_cols.setdefault(key, set())
        overlap = group & prior
        if overlap:
            raise curation_error(
                "fold_override_invalid",
                f"fold-override key {key} has column(s) {sorted(overlap)} in more "
                f"than one [[fold]] group.",
                "Each column belongs to exactly one fold group per "
                "(register, var); merge the groups or remove the duplicate.",
            )
        prior |= group
        out.setdefault(key, []).append(group)
    return out
