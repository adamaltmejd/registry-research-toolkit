"""Authored thin-provider coverage windows and declared code lists at the build boundary.

Each case writes a synthetic thin-provider TOML (the maintained `[[register]]` layout
read by `sources.curated_records`), prepares and accepts it with the SCB sample, and
builds it with an authored curation tree. Expected values follow the documented rule
for authored thin records: a variable's delivery period is the intersection of its own
bounds (else its register's) with its variant's; an empty or inverted intersection
fails the build; a variable that names its own code list carries that list's members.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _curation_sos_boundary_support import prepare, thin_register_toml

if TYPE_CHECKING:
    from pathlib import Path


def _fk_source(variant_bounds: str):
    def write(source: Path) -> None:
        directory = source / "Forsakringskassan"
        directory.mkdir()
        (directory / "fk.toml").write_text(
            '[[register]]\nkey = "r"\nname = "R"\n'
            'valid_from = "2000-01-01"\nvalid_to = "2020-12-31"\n'
            '[[register.variant]]\nkey = "v"\nname = "V"\n'
            f"{variant_bounds}"
            '[[register.variable]]\nname = "Col"\ncolumn = "COL"\ndata_type = "int"\n'
            'variants = ["v"]\nvalid_from = "2005-01-01"\nvalid_to = "2019-12-31"\n',
            encoding="utf-8",
        )

    return write


FK_CURATION = {
    "fk/r.toml": thin_register_toml("fk", "r", variants=("v",), variables=("COL",))
}


def test_thin_delivery_period_intersects_register_variant_and_variable_bounds(
    tmp_path: Path,
) -> None:
    """Register 2000-2020, variant 2010-2018 and variable 2005-2019 give 2010-2018."""
    build = prepare(
        tmp_path, _fk_source('valid_from = "2010-01-01"\nvalid_to = "2018-12-31"\n')
    ).build(tmp_path, "build", FK_CURATION)
    assert build.states("r") == [("col", "v", "int", "2010-01-01", "2018-12-31")]
    assert build.case_status() == {
        "accepted-authored:Forsakringskassan/fk.toml:r:COL": "applicable"
    }


def test_thin_inverted_coverage_window_fails_the_build(tmp_path: Path) -> None:
    """An open variant from 2021 starts after the variable's 2019 end: no window."""
    prepared = prepare(tmp_path, _fk_source('valid_from = "2021-01-01"\n'))
    with pytest.raises(
        ValueError,
        match=r"accepted-authored:Forsakringskassan/fk\.toml:r:COL: "
        r"empty or inverted thin coverage window for 'v'",
    ):
        prepared.build(tmp_path, "build", FK_CURATION)


def _canonical_source(source: Path) -> None:
    directory = source / "scb_canonical"
    directory.mkdir()
    (directory / "scb_canonical.toml").write_text(
        '[[register]]\nkey = "r"\nname = "R"\nvalid_from = "2010-01-01"\n'
        '[[register.variable]]\nname = "Col"\ncolumn = "COL"\ndata_type = "text"\n'
        'value_set = "own-list"\n',
        encoding="utf-8",
    )
    (directory / "own-list.csv").write_text(
        "code,label\n01,First\n02,Second\n", encoding="utf-8"
    )


def test_thin_variable_carries_the_members_of_its_own_declared_list(
    tmp_path: Path,
) -> None:
    """The open register window from 2010 bounds the state; its value set is the
    declared list's two members."""
    build = prepare(tmp_path, _canonical_source).build(
        tmp_path,
        "build",
        {
            "scb/r.toml": thin_register_toml(
                "scb",
                "r",
                variants=(("_default", "default"),),
                variables=("COL",),
                canonical_scb=True,
            )
        },
    )
    assert build.states("r") == [("col", "default", "text", "2010-01-01", "9999-12-31")]
    assert build.rows(
        "SELECT c.code, c.label FROM variable_state s "
        "JOIN variable v USING (variable_id) "
        "JOIN value_set_member m ON m.value_set_id = s.value_set_id "
        "JOIN value_code c USING (code_id) WHERE v.slug = 'col' ORDER BY c.code"
    ) == [("01", "First"), ("02", "Second")]
