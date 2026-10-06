"""Shared-definition sibling grouping guards at the build-catalog boundary.

Synthetic fixture: one SCB register (native id 1) delivers eight source variables,
each as two columns of the same edition, and the curation tree splits every column
into its own catalog variable. Co-delivered siblings of one source definition are
grouped into one variable edge group unless the build's guards keep them apart
(``reg_meta_build/_curation.py`` comments and ``source_siblings`` docstring):

- a code column and its label column (``<stem>namn`` paired with the bare
  ``<stem>`` or with ``<stem>kod`` / ``<stem>id``, in either order) are a
  representation pair, never grouped;
- a numeric/text type mismatch is a shape conflict; Swedish type labels classify by
  their ASCII fold (``Heltal`` numeric, ``Sträng`` text);
- when a type is unclassifiable (a date, or no documented type), differing
  ``data_length`` values are a shape conflict.

Only the two label-like columns ``FORNAMN``/``EFTERNAMN`` (two ``namn`` columns,
not a code/label pair, same shape) remain eligible, so they form the only group.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

from _csv_fixtures import var_row
from _curation_toml_boundary_support import SCB_SAMPLE_REGISTER, build_scb_catalog

if TYPE_CHECKING:
    from pathlib import Path

# (var_id, column, Datatyp, Datalängd, curated slug). The SUN2000 slugs sort the
# label column first, so the code/label check must accept either column order.
SIBLINGS = [
    (100, "LID", "int", "1", "lid"),
    (100, "LNAMN", "int", "1", "lnamn"),
    (200, "KOMMUN", "int", "1", "kommun"),
    (200, "KOMMUNNAMN", "int", "1", "kommunnamn"),
    (300, "FORNAMN", "int", "1", "fornamn"),
    (300, "EFTERNAMN", "int", "1", "efternamn"),
    (400, "BELOPP", "Heltal", "1", "belopp"),
    (400, "BELOPPTXT", "Sträng (text)", "1", "belopptxt"),
    (500, "DAG", "Datum", "8", "dag"),
    (500, "DAGTXT", "Datum", "10", "dagtxt"),
    (600, "SUN2000KOD", "int", "1", "sun-kod"),
    (600, "SUN2000NAMN", "int", "1", "namn-sun"),
    (700, "ANTAL", "Heltal", "1", "antal"),
    (700, "ANTALTXT", "Sträng", "1", "antaltxt"),
    (800, "X", "", "1", "x"),
    (800, "XTXT", "int", "2", "xtxt"),
]


def test_only_same_shape_non_code_label_siblings_form_a_group(
    tmp_path: Path,
) -> None:
    built = build_scb_catalog(
        tmp_path,
        curation={
            "registers/scb/sample.toml": SCB_SAMPLE_REGISTER
            + "".join(
                f'[[variable]]\nnative_id = "1.{var_id}.{column.lower()}"\n'
                f'slug = "{slug}"\n'
                for var_id, column, _, _, slug in SIBLINGS
            )
        },
        registerinformation_rows=[
            var_row(
                cvid=1000 + index,
                var_id=var_id,
                colname=column,
                varname=f"Var{var_id}",
                data_type=data_type,
                data_length=length,
            )
            for index, (var_id, column, data_type, length, _) in enumerate(SIBLINGS)
        ],
        unika_rows=[
            f"TESTREG|Testregistret|Individer|Individer|Var{var_id}|{column}"
            "|2020|2020|0|0|0"
            for var_id, column, _, _, _ in SIBLINGS
        ],
    )
    assert built.result["status"] == "diagnostic_complete"
    with sqlite3.connect(built.db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM variable").fetchone() == (
            len(SIBLINGS),
        )
        groups = conn.execute(
            "SELECT group_concat(v.slug, ',') FROM ("
            "  SELECT gv.group_id, v.slug FROM concept_group_variable gv "
            "  JOIN variable v USING (variable_id) ORDER BY gv.group_id, v.slug"
            ") v GROUP BY v.group_id"
        ).fetchall()
    assert groups == [("efternamn,fornamn",)]
