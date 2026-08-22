"""#474: diff / compare / search were keyed on the SCB-only displayed `var_id`,
which is NULL for EVERY non-SCB variable (SOS, curated, FOHM, steward). A
non-SCB register with ≥2 variables therefore collapsed all of them under one
None key, so the three consumers each dropped or mis-handled the siblings:

  - `get_diff` (`_columns_at_year`): keyed the per-year column dict on `var_id`,
    so two changed non-SCB columns collapsed to one — only the first survived.
  - `compare`: keyed `all_registry_vars` / `matched_var_ids` on `var_id`, so
    matching ONE local column to a non-SCB registry column marked the whole
    None-keyed bucket "matched", suppressing the siblings from
    `missing_from_registry`.
  - `search --years` (`_filter_search_by_years`): a None `var_id` fell through to
    the register-wide branch (kept if the REGISTER had any state in range), so a
    non-SCB variable NOT covering the filtered year was wrongly kept on a
    sibling's state.

The fix re-keys all three on the always-present, unique `variable_id`; the
displayed `var_id` rides along for output only. These tests build a non-SCB
register with two variables (both displayed `var_id` = None) and assert the
siblings no longer collapse. Each was verified to FAIL on the pre-#474
var_id-keyed code (origin/main): diff reported only the first column, compare's
`missing_from_registry` came back empty, and search kept the out-of-range
sibling — all three opposite to the assertions below.

Non-SCB variables are placed in the minted-id HIGH band (`variable_id >= 2^62`)
so the band guard resolves their displayed `var_id` to None, matching a real
validated build (SQLite autoincrement would hand out low ids that read as SCB).
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

from reg_meta.queries import _SCB_ID_CEILING, compare, get_diff, search

sys.path.insert(
    0, str(Path(__file__).resolve().parents[2] / "reg_meta_build" / "tests")
)

from _slugged_db import (
    add_register,
    add_state,
    add_variant,
    build_slugged_db,
)

if TYPE_CHECKING:
    import sqlite3

# Minted-id band base = the production `_SCB_ID_CEILING` (reg_meta.queries), itself
# kept in sync with `reg_meta_build.id._MINT_BIT` by `test_band_constant_in_sync_with_build`
# in the sibling test_var_id_nonnumeric.py. SCB variable_id < ceiling, non-SCB >=
# ceiling; the displayed var_id is None for the high band — the collapse surface #474 fixes.

# Curated (non-SCB) register: provider_id 3, two variables with column-token
# provider_keys (non-numeric) and HIGH-band ids → both displayed var_id = None.
_CUR_REGISTER_ID = 3
_CUR_VARIANT_ID = 30
_VAR_A_ID = _SCB_ID_CEILING + 1
_VAR_B_ID = _SCB_ID_CEILING + 2


def _insert_nonscb_variable(
    conn: sqlite3.Connection,
    *,
    variable_id: int,
    register_id: int,
    provider_key: str,
    name: str,
    slug: str,
) -> None:
    """Insert a non-SCB `variable` with an explicit HIGH-band `variable_id` and a
    non-numeric `provider_key` — the shape that collapsed under #474's var_id key.
    `add_variable` (the int-typed helper) can't express either, so go raw."""
    conn.execute(
        "INSERT INTO variable (variable_id, register_id, provider_key, name, slug) "
        "VALUES (?, ?, ?, ?, ?)",
        (variable_id, register_id, provider_key, name, slug),
    )


def _curated_register(conn: sqlite3.Connection) -> None:
    """Add a curated (non-SCB) register + one variant to a slugged DB."""
    add_register(
        conn,
        register_id=_CUR_REGISTER_ID,
        slug="curreg",
        name="Curated Register",
        provider_id=3,
    )
    add_variant(
        conn,
        register_variant_id=_CUR_VARIANT_ID,
        register_id=_CUR_REGISTER_ID,
        slug="curvar",
        name="Curated",
    )


# ---------------------------------------------------------------------------
# diff
# ---------------------------------------------------------------------------


def test_diff_reports_both_changed_nonscb_columns() -> None:
    """Two non-SCB columns BOTH change data_type 2020→2022. The old var_id-keyed
    `_columns_at_year` collapsed them under one None key (first column wins), so
    the diff reported only one. Keyed on `variable_id`, both surface as changed."""
    conn = build_slugged_db(variable=None)
    _curated_register(conn)

    for variable_id, pkey, name, slug in (
        (_VAR_A_ID, "cola", "Column A", "col-a"),
        (_VAR_B_ID, "colb", "Column B", "col-b"),
    ):
        _insert_nonscb_variable(
            conn,
            variable_id=variable_id,
            register_id=_CUR_REGISTER_ID,
            provider_key=pkey,
            name=name,
            slug=slug,
        )
        # 2020 era: int; 2022 era: varchar — a data_type change for BOTH columns.
        add_state(
            conn,
            register_id=_CUR_REGISTER_ID,
            variable_slug=slug,
            register_variant_id=_CUR_VARIANT_ID,
            valid_from="2020-01-01",
            valid_to="2020-12-31",
            data_type="int",
            delivery_column_name=name,
        )
        add_state(
            conn,
            register_id=_CUR_REGISTER_ID,
            variable_slug=slug,
            register_variant_id=_CUR_VARIANT_ID,
            valid_from="2022-01-01",
            valid_to="2022-12-31",
            data_type="varchar",
            delivery_column_name=name,
        )
    conn.commit()

    out = get_diff(conn, register="Curated Register", from_year=2020, to_year=2022)
    variants = out["variants"]
    assert len(variants) == 1
    changed_names = {c["variable_name"] for c in variants[0]["changed"]}
    # BOTH columns must be reported, not just the lexically-first one.
    assert changed_names == {"Column A", "Column B"}
    # Both carry a blank (None) displayed var_id but distinct variable_ids.
    changed_var_ids = {c["var_id"] for c in variants[0]["changed"]}
    assert changed_var_ids == {None}
    changed_variable_ids = {c["variable_id"] for c in variants[0]["changed"]}
    assert changed_variable_ids == {_VAR_A_ID, _VAR_B_ID}


# ---------------------------------------------------------------------------
# compare
# ---------------------------------------------------------------------------


def test_compare_does_not_suppress_sibling_nonscb_columns() -> None:
    """Comparing a local schema with ONE of two non-SCB registry columns. The old
    var_id-keyed `compare` marked the whole None bucket "matched" once any sibling
    matched, hiding the others from `missing_from_registry`. Keyed on
    `variable_id`, the unmatched sibling is correctly reported as missing."""
    conn = build_slugged_db(variable=None)
    _curated_register(conn)

    for variable_id, pkey, name, slug in (
        (_VAR_A_ID, "cola", "Column A", "col-a"),
        (_VAR_B_ID, "colb", "Column B", "col-b"),
    ):
        _insert_nonscb_variable(
            conn,
            variable_id=variable_id,
            register_id=_CUR_REGISTER_ID,
            provider_key=pkey,
            name=name,
            slug=slug,
        )
        add_state(
            conn,
            register_id=_CUR_REGISTER_ID,
            variable_slug=slug,
            register_variant_id=_CUR_VARIANT_ID,
            valid_from="2020-01-01",
            valid_to="9999-12-31",
            data_type="int",
            delivery_column_name=name,
        )
    conn.commit()

    # Local file carries ONLY Column A's delivery column.
    out = compare(
        conn,
        columns_by_file={"local.csv": ["Column A"]},
        register_hints={"local.csv": _CUR_REGISTER_ID},
    )
    file_result = out["files"][0]
    matched_names = {m["variable_name"] for m in file_result["matched"]}
    missing_names = {m["variable_name"] for m in file_result["missing_from_registry"]}
    assert matched_names == {"Column A"}
    # Column B is NOT in the local file → must surface as missing, not be
    # suppressed by Column A's match (the collapse the old None-keying caused).
    assert "Column B" in missing_names


# ---------------------------------------------------------------------------
# search --years
# ---------------------------------------------------------------------------


def test_search_years_filters_each_nonscb_variable_by_its_own_states() -> None:
    """Two non-SCB variables, only ONE covering the filtered year. The old code
    keyed year-filtering on `(register_id, var_id)`; with var_id None for both,
    the pair branch was skipped and both fell to the register-wide branch (kept if
    the REGISTER had any state in range), so the out-of-range sibling leaked.
    Keyed on `_variable_id`, only the in-range variable survives."""
    conn = build_slugged_db(variable=None)
    _curated_register(conn)

    # Column A covers 2020; Column B covers only 2099 (open-ended from 2099).
    specs = (
        (_VAR_A_ID, "cola", "Findable A", "find-a", "2020-01-01", "2020-12-31"),
        (_VAR_B_ID, "colb", "Findable B", "find-b", "2099-01-01", "9999-12-31"),
    )
    for variable_id, pkey, name, slug, vfrom, vto in specs:
        _insert_nonscb_variable(
            conn,
            variable_id=variable_id,
            register_id=_CUR_REGISTER_ID,
            provider_key=pkey,
            name=name,
            slug=slug,
        )
        add_state(
            conn,
            register_id=_CUR_REGISTER_ID,
            variable_slug=slug,
            register_variant_id=_CUR_VARIANT_ID,
            valid_from=vfrom,
            valid_to=vto,
            data_type="int",
            delivery_column_name=name,
        )
    conn.commit()

    # field="varname" is the LIKE-over-name path; "Findable" matches both rows
    # pre-filter. The 2020 filter must keep ONLY Findable A.
    out = search(conn, "Findable", field="varname", years="2020")
    names = {r.name for r in out.results}
    assert names == {"Findable A"}
