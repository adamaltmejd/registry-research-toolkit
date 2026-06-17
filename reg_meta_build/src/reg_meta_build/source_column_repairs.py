"""SCB pre-state source-column repairs (see DESIGN.md → Build-time triage (SCB)):
two maintainer surfaces that repair a var_id's SCB delivery columns BEFORE the
coalescer/triage emits states. Both ship in one SCB-scoped file
(`curation/scb/source_column_repairs.toml`), both key on `(register_id, var_id)`
— the SCB source ids the coalescer/triage carry (RegisterId + VarId ==
`variable.provider_key`) — and both case-fold their columns to the rule-2
connectivity key at load (`_curation.fold_column`), so TOML casing is cosmetic.

* `[[column_merge]]` (#196) — assert that NEVER-co-occurring delivery columns of
  one var_id are the SAME concept, so the coalescer's rule-2 union-find treats
  them as ONE node-col from the start, UPSTREAM of triage. An era-rename twin
  pair (`PNR` → `PersonNr`) never co-occurs, so the two columns form separate
  union-find components — and once the var_id is a split container (some OTHER
  columns co-deliver), each component shards into its own sibling variable,
  splitting one identity's history across fragments. The triage cannot recover
  this: its fold-override surface acts on CONTESTED (same-edition co-delivered)
  columns only, which a never-co-occurring twin by definition is not. The
  maintainer asserts the equivalence here and the coalescer normalizes the twins
  to one union-find node-col by fiat.

* `[[fold_override]]` (#261) — force same-concept CONTESTED columns that DON'T
  share a stem to FOLD into one variable. The triage partitions a split
  container's contested columns PURELY on the column stem (`_cluster_contested`):
  a shared stem + representation-only suffix folds (`Ssyk3`/`Ssyk5`), a disjoint
  stem splits. That is right for the vast majority of cases — but a register
  sometimes delivers ONE concept under columns that share no stem (e.g.
  näringsgren as `Ksjusni` / `NG1` / `bransch`). The stem rule cannot recover
  that; the maintainer asserts it here, and the override pre-seeds the union-find
  so those columns fold by fiat (`forced_same`, bypassing the stem verify).

The two are complementary, not interchangeable — the distinction is exactly
co-occurrence (a column_merge acts on columns that NEVER share an edition; a
fold_override acts on CONTESTED same-edition columns). Each `[[entry]]` is ONE
group for one `(register_id, var_id)`; a var with two independent groups gets two
entries with that same key. Keying per-(register, var) makes a group spanning
MULTIPLE var_ids unrepresentable by construction — cross-var_id column SHARING
(#197) is a different shape and NOT these surfaces. Contrast `codelivery.py`
(two value-set codings on ONE column in one period). All are maintainer artifacts
like the slug TOMLs — absent in wheel installs and synthetic test builds (empty
map).
"""

from __future__ import annotations

from pathlib import Path

from ._curation import load_column_groups, load_curation_entries

# (register_id, var_id) — the same coordinates the coalescer's rule-2 union-find
# carries (`register.register_id`, `variable.provider_key`). Each value is the
# var's curated groups: every frozenset is one CASE-FOLDED column set. The two
# aliases are semantically distinct (merge groups become a single union-find
# node-col in `_coalesce_variable_states`; fold groups are handed to
# `_cluster_contested`'s `forced_same`) and `sources/scb.py` imports each.
ColumnMergeMap = dict[tuple[int, int], list[frozenset[str]]]
FoldOverrideMap = dict[tuple[int, int], list[frozenset[str]]]

_FILE_NAME = "curation/scb/source_column_repairs.toml"


def repo_source_column_repairs_path() -> Path | None:
    """`reg_meta_build/curation/scb/source_column_repairs.toml` from a repo
    checkout, or None (wheel installs don't ship curation — it's a maintainer
    artifact like the slug TOMLs). Sits under `curation/scb/`, NOT under
    `fqid_slugs/` (that dir is glob-loaded as provider-slug TOMLs; a file there
    would break the build)."""
    package_root = Path(__file__).resolve().parent.parent.parent
    candidate = package_root / "curation" / "scb" / "source_column_repairs.toml"
    return candidate if candidate.is_file() else None


def load_column_merges(path: Path | None) -> ColumnMergeMap:
    """Parse the `[[column_merge]]` section into
    `{(register_id, var_id): [frozenset(cols), …]}` with case-folded columns.
    Empty when no file (synthetic builds, wheels).

    Load-time validation (all EXIT_CONFIG, actionable):
      - Only `[[column_merge]]` / `[[fold_override]]` top-level (a misspelled
        `[[column_merges]]` is a loud error, not a silent no-op); `column_merge`
        is an array of tables.
      - `register_id` / `var_id` present and canonical int (no leading zeros).
      - `columns` is a list of ≥2 non-empty strings that stay ≥2 DISTINCT after
        case-folding (case twins collapse automatically — a group that survives
        only on case spelling is a no-op the auto case-fold already covers), and
        no column folds to "".
      - No column repeats within a group, or across groups of the SAME
        `(register_id, var_id)` — compared on the folded form.

    The build-time half (every named column must be an OBSERVED delivery column
    of the var, scoped to the registers present in the build) lives in
    `_coalesce_variable_states`, where the observed column set is known.
    """
    # Shared scaffold (parse + top-level typo guard + array-of-tables +
    # per-entry table check) — see `_curation.load_curation_entries`.
    entries = load_curation_entries(
        path,
        entry_key="column_merge",
        label="column-merge",
        prefix="column-merge",
        code_base="column_merge",
        file_name=_FILE_NAME,
        entry_fields="register_id / var_id / columns",
        sibling_keys=frozenset({"fold_override"}),
    )
    return load_column_groups(
        entries,
        code="column_merge_invalid",
        prefix="column-merge",
        entry_key="column_merge",
        noun="merge",
    )


def load_fold_overrides(path: Path | None) -> FoldOverrideMap:
    """Parse the `[[fold_override]]` section into
    `{(register_id, var_id): [frozenset(cols), …]}` with case-folded columns.
    Empty when no file (synthetic builds, wheels). Columns are folded at load
    because the triage's contested columns are the coalescer's case-folded rule-2
    components (`fold_column`) — a raw-cased entry would silently never match.

    Load-time validation (all EXIT_CONFIG, actionable):
      - Only `[[fold_override]]` / `[[column_merge]]` top-level (a misspelled
        `[[fold_overrides]]` is a loud error, not a silent no-op); `fold_override`
        is an array of tables.
      - `register_id` / `var_id` present and canonical int (no leading zeros).
      - `columns` is a list of ≥2 non-empty strings that stay ≥2 DISTINCT after
        case-folding, and no column folds to "".
      - No column repeats within a group, or across groups of the SAME
        `(register_id, var_id)` — compared on the case-folded form.

    The build-time half (every named column must be a CONTESTED column of a real
    split container, and every key must be consumed) lives in `_triage_groups`,
    where the contested set is known.
    """
    entries = load_curation_entries(
        path,
        entry_key="fold_override",
        label="fold-override",
        prefix="fold-override",
        code_base="fold_override",
        file_name=_FILE_NAME,
        entry_fields="register_id / var_id / columns",
        sibling_keys=frozenset({"column_merge"}),
    )
    return load_column_groups(
        entries,
        code="fold_override_invalid",
        prefix="fold-override",
        entry_key="fold_override",
        noun="fold",
    )
