"""Column-merge curation (see DESIGN.md → Build-time triage (SCB)): assert that
NEVER-co-occurring delivery columns of one `var_id` are the SAME concept, so the
coalescer treats them as one column from the start.

The SCB rule-2 connectivity (`_coalesce_variable_states`, `sources/scb.py`)
unions two delivery columns only when some cvid carries both as aliases. An
era-rename twin pair (`PNR` → `PersonNr`) never co-occurs, so the two columns
form separate union-find components — and once the var_id is a split container
(some OTHER columns co-deliver), every component becomes its own sibling
variable, sharding the identity's history across fragments. The triage cannot
recover this: its fold-override surface (`fold_overrides.py`) acts on CONTESTED
(same-edition co-delivered) columns only, which a never-co-occurring twin by
definition is not. The maintainer asserts the equivalence here and the coalescer
normalizes the twins to ONE union-find node-col by fiat, upstream of triage.

An entry is keyed on `(register_id, var_id)` — the same SCB source ids the
coalescer carries (RegisterId + VarId == `variable.provider_key`), identical to
the fold-override key. Each entry lists the columns that ARE one concept; they
are case-folded to the rule-2 connectivity key at load (`_curation.fold_column`),
so case/diacritic spelling in the TOML is cosmetic. One `[[merge]]` is ONE merge
group for one `(register_id, var_id)`; a var with two independent merge groups
gets two `[[merge]]` entries with that same key. Keying per-(register, var) makes
a merge spanning MULTIPLE var_ids unrepresentable by construction — cross-var_id
column SHARING (#197) is a different shape and NOT this surface.

This is the curated counterpart of the automatic case-fold (both normalize the
union-find node-col; the case-fold needs no curation because case identity is
mechanical). Contrast `fold_overrides.py` (folds contested columns the stem rule
splits) and `codelivery.py` (two value-set codings on ONE column in one period).
All are maintainer artifacts like the slug TOMLs — absent in wheel installs and
synthetic test builds (empty map).
"""

from __future__ import annotations

from pathlib import Path

from ._curation import canonical_int, curation_error, fold_column, load_curation_entries

# (register_id, var_id) — the same coordinates the coalescer's rule-2 union-find
# carries (`register.register_id`, `variable.provider_key`).
ColumnMergeKey = tuple[int, int]
# Each var's curated merge groups. Every frozenset is one CASE-FOLDED column set
# that becomes a single union-find node-col in `_coalesce_variable_states`.
ColumnMergeMap = dict[ColumnMergeKey, list[frozenset[str]]]


def repo_column_merges_path() -> Path | None:
    """`reg_meta_build/column_merges.toml` from a repo checkout, or None (wheel
    installs don't ship curation — it's a maintainer artifact like the slug
    TOMLs). Sits at the package root, NOT under `fqid_slugs/` (that dir is
    glob-loaded as provider-slug TOMLs; a file there would break the build)."""
    candidate = Path(__file__).resolve().parent.parent.parent / "column_merges.toml"
    return candidate if candidate.is_file() else None


def load_column_merges(path: Path | None) -> ColumnMergeMap:
    """Parse the curation TOML into `{(register_id, var_id): [frozenset(cols), …]}`
    with case-folded columns. Empty when no file (synthetic builds, wheels).

    Load-time validation (all EXIT_CONFIG, actionable):
      - Only `[[merge]]` top-level (a misspelled `[[merges]]` is a loud error, not
        a silent no-op); `merge` is an array of tables.
      - `register_id` / `var_id` present and canonical int (no leading zeros).
      - `columns` is a list of ≥2 non-empty strings that stay ≥2 DISTINCT after
        case-folding (case twins collapse automatically — a group that survives
        only on case spelling is a no-op the auto case-fold already covers).
      - No column repeats within a group, or across groups of the SAME
        `(register_id, var_id)` (duplicate / overlapping group) — compared on the
        folded form.

    The build-time half (every named column must be an OBSERVED delivery column
    of the var, scoped to the registers present in the build) lives in
    `_coalesce_variable_states`, where the observed column set is known.
    """
    # Shared scaffold (parse + top-level typo guard + array-of-tables +
    # per-entry table check) — see `_curation.load_curation_entries`.
    entries = load_curation_entries(
        path,
        entry_key="merge",
        label="column-merge",
        prefix="column-merge",
        code_base="column_merge",
        file_name="column_merges.toml",
        entry_fields="register_id / var_id / columns",
    )
    out: ColumnMergeMap = {}
    seen_cols: dict[ColumnMergeKey, set[str]] = {}
    for entry in entries:
        reg = canonical_int(entry.get("register_id"))
        var = canonical_int(entry.get("var_id"))
        if reg is None or var is None:
            raise curation_error(
                "column_merge_invalid",
                f"column-merge entry {entry!r} needs `register_id` and `var_id` "
                f"as canonical integers (no leading zeros).",
                "Each [[merge]] entry needs integer `register_id` and `var_id`.",
            )
        key: ColumnMergeKey = (reg, var)
        columns = entry.get("columns")
        if (
            not isinstance(columns, list)
            or len(columns) < 2
            or not all(isinstance(c, str) and c for c in columns)
        ):
            raise curation_error(
                "column_merge_invalid",
                f"column-merge entry {key} `columns` must be a list of ≥2 "
                f"non-empty strings (a singleton merge is a no-op).",
                'Give `columns = ["ColA", "ColB", …]` with at least two columns.',
            )
        group: frozenset[str] = frozenset(fold_column(c) for c in columns)
        if "" in group:
            # A column of only non-ASCII characters folds to "" — that can never
            # match a rule-2 node-col (the coalescer keeps such a column raw).
            raise curation_error(
                "column_merge_invalid",
                f"column-merge entry {key} has a column that case-folds to an "
                f"empty string: {columns}.",
                "Name real ASCII-foldable delivery columns in each [[merge]] group.",
            )
        if len(group) != len(columns):
            raise curation_error(
                "column_merge_invalid",
                f"column-merge entry {key} repeats a column within its group "
                f"(after case-folding): {columns}.",
                "List each column once per [[merge]] group; case/diacritic twins "
                "collapse automatically and must not be spelled out.",
            )
        prior = seen_cols.setdefault(key, set())
        overlap = group & prior
        if overlap:
            raise curation_error(
                "column_merge_invalid",
                f"column-merge key {key} has column(s) {sorted(overlap)} in more "
                f"than one [[merge]] group.",
                "Each column belongs to exactly one merge group per "
                "(register, var); merge the groups or remove the duplicate.",
            )
        prior |= group
        out.setdefault(key, []).append(group)
    return out
