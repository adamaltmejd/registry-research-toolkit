"""Authored thin-provider registers at the build-catalog boundary.

The fixture is synthetic: one SCB register (native id 1, column VALUE) so the
catalog has an SCB scope, plus one authored Försäkringskassan TOML whose register
declares no dates, one pooled variant (2020-01-01..2021-12-31) and one variable
(HEADER) restricted to 2020-06-01..2021-06-30 that carries a ``data_warning``.
The register, variant and variable TOML is the one the deleted
``test_curated_source_records.py::test_pooled_delivery_remains_pooled_through_checked_thin_compilation``
read; the naming file pins its authored native ids.
"""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

from _csv_fixtures import var_row
from _curation_toml_boundary_support import SCB_SAMPLE_REGISTER, build_scb_catalog
from _pipeline_catalog_support import report_issues
from reg_meta_build.source_naming import authored_naming_id

if TYPE_CHECKING:
    from pathlib import Path

WARNING = "Header-only metadata; response codes unavailable."
POOLED_THIN_REGISTER = f"""
[[register]]
key = "r"
name = "Register with unknown inception"
[[register.variant]]
key = "table"
name = "Pooled table"
valid_from = "2020-01-01"
valid_to = "2021-12-31"
period_scope = "pooled"
[[register.variable]]
name = "Literal header"
column = "HEADER"
data_type = "text"
data_warning = "{WARNING}"
variants = ["table"]
valid_from = "2020-06-01"
valid_to = "2021-06-30"
"""


def _thin_naming() -> str:
    register = authored_naming_id("register", provider="fk", register_key="r")
    variant = authored_naming_id(
        "register_variant", provider="fk", register_key="r", member_key="table"
    )
    variable = authored_naming_id(
        "variable", provider="fk", register_key="r", member_key="HEADER"
    )
    return (
        f'[register]\nprovider = "fk"\nslug = "r"\nnative_id = "{register}"\n'
        f'[[variant]]\nslug = "table"\nnative_id = "{variant}"\n'
        f'[[variable]]\nslug = "header"\nnative_id = "{variable}"\n'
    )


def test_pooled_thin_variant_keeps_one_pooled_state_with_its_warning(
    tmp_path: Path,
) -> None:
    """A pooled authored variant yields one pooled HEADER state whose window is the
    variable's own dates inside the variant's, plus the declared data warning on
    the name, definition and coding fields."""
    built = build_scb_catalog(
        tmp_path,
        curation={
            "registers/scb/sample.toml": SCB_SAMPLE_REGISTER
            + '[[variable]]\nnative_id = "1.101"\nslug = "value"\n',
            "registers/fk/r.toml": _thin_naming(),
        },
        registerinformation_rows=[
            var_row(cvid=1001, var_id=101, colname="VALUE", data_type="int")
        ],
        unika_rows=[
            "TESTREG|Testregistret|Individer|Individer|GenericVar|VALUE|2020|2020|0|0|0"
        ],
        fk_toml=POOLED_THIN_REGISTER,
    )
    assert built.result["status"] == "diagnostic_complete"
    assert report_issues(built.report) == []
    with sqlite3.connect(built.db) as conn:
        assert conn.execute(
            "SELECT v.slug, rv.slug, s.valid_from, s.valid_to, s.pooled "
            "FROM variable_state s JOIN variable v USING (variable_id) "
            "JOIN register_variant rv ON rv.register_variant_id = s.register_variant_id "
            "WHERE s.delivery_column_name = 'HEADER'"
        ).fetchall() == [("header", "table", "2020-06-01", "2021-06-30", 1)]
        warnings = [
            json.loads(raw)
            for (raw,) in conn.execute(
                "SELECT warning_json FROM data_warning WHERE delivery_column_name = "
                "'HEADER'"
            )
        ]
    assert [
        (
            w["variable_fqid"],
            w["variant"],
            w["summary"],
            w["fields"],
            w["valid_from"],
            w["valid_to"],
        )
        for w in warnings
    ] == [
        (
            "fk/r/header",
            "table",
            WARNING,
            ["name", "definition", "coding"],
            "2020-06-01",
            "2021-06-30",
        )
    ]
