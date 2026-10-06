"""Relations into a curated period family, in full and register-slice builds."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import var_row, write_input_bundle, write_scb_input
from _pipeline_catalog_support import report_issues
from _prepared_fixtures import accept_prepared
from reg_meta_build.pipeline import build_catalog
from reg_meta_build.prepared_catalog import prepare_catalog_sources

if TYPE_CHECKING:
    from pathlib import Path

_MONTHS = ("Jan", "Feb", "Mar", "Apr", "Maj", "Jun")
_MONTHS += ("Jul", "Aug", "Sep", "Okt", "Nov", "Dec")


@dataclass(frozen=True)
class FamilyCatalog:
    prepared: Path
    commit: str
    digest: str
    curation: Path

    def build(self, output: Path, report: Path, **kwargs):
        return build_catalog(
            self.prepared,
            self.commit,
            self.digest,
            output,
            report,
            curation_dir=self.curation,
            **kwargs,
        )


@pytest.fixture
def family_catalog(tmp_path: Path) -> FamilyCatalog:
    """Register 1 holds one variable; register 2 holds twelve month columns.

    The month columns of register 2 are curated as one period family whose
    variable slug, `calendar`, exists only through that curation.
    """
    other = ("OTHERREG", 2, 20)
    records = [var_row(cvid=1001, var_id=101, colname="VALUE")]
    summaries = [
        "TESTREG|Testregistret|Individer|Individer|GenericVar|VALUE|2020|2020|0|0|0"
    ]
    for index, month in enumerate(_MONTHS, 1):
        records.append(
            var_row(
                cvid=2100 + index,
                var_id=210 + index,
                colname=f"Calendar{month}",
                varname=f"Calendar{month}",
                register=other,
            )
        )
        summaries.append(
            f"OTHERREG|Testregistret|Individer|Individer|Calendar{month}|"
            f"Calendar{month}|2020|2020|0|0|0"
        )
    source = tmp_path / "source"
    write_scb_input(
        source,
        registerinformation_rows=records,
        unika_rows=summaries,
        include=("registerinformation", "unika"),
    )
    bundle = write_input_bundle(tmp_path / "inputs", source)
    prepared = tmp_path / "prepared" / "catalog"
    manifest = prepare_catalog_sources(bundle, prepared)
    commit = accept_prepared(prepared)
    curation = tmp_path / "curation"
    registers = curation / "registers" / "scb"
    registers.mkdir(parents=True)
    (curation / "classifications").mkdir()
    (registers / "sample.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
        '[[variant]]\nnative_id = "1.10"\nslug = "people"\n'
        '[[variable]]\nnative_id = "1.101"\nslug = "value"\n',
        encoding="utf-8",
    )
    (registers / "other.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "other"\nnative_id = "2"\n'
        '[[variant]]\nnative_id = "2.20"\nslug = "people"\n'
        "[[representation.period_family]]\n"
        'register = "scb/other"\nfamily_stem = "calendar"\n'
        'label = "Calendar per month"\nslug = "calendar"\n',
        encoding="utf-8",
    )
    (curation / "relations.toml").write_text(
        '[[edge]]\ntype = "same_as"\na = "scb/sample/value"\n'
        'b = "scb/other/calendar"\n',
        encoding="utf-8",
    )
    return FamilyCatalog(prepared, commit, manifest.sha256, curation)


def test_relation_resolves_to_curated_period_family_variable(
    family_catalog: FamilyCatalog, tmp_path: Path
) -> None:
    output, report = tmp_path / "full.db", tmp_path / "report"
    result = family_catalog.build(output, report, diagnostic=True)
    assert result["counts"].get("error", 0) == 0
    with sqlite3.connect(output) as conn:
        edges = conn.execute(
            "SELECT a_provider, a_register, a_variable, "
            "b_provider, b_register, b_variable FROM variable_same_as"
        ).fetchall()
    assert sorted(edges) == [
        ("scb", "other", "calendar", "scb", "sample", "value"),
        ("scb", "sample", "value", "scb", "other", "calendar"),
    ]


def test_slice_defers_relation_into_unselected_period_family(
    family_catalog: FamilyCatalog, tmp_path: Path
) -> None:
    output, report = tmp_path / "slice.db", tmp_path / "report"
    result = family_catalog.build(output, report, registers=("1",), diagnostic=True)
    assert result["counts"].get("error", 0) == 0
    assert result["counts"]["deferred_references"] == 1
    assert [(i["code"], i["severity"]) for i in report_issues(report)] == [
        ("deferred_out_of_slice_reference", "warning")
    ]
    with sqlite3.connect(output) as conn:
        assert conn.execute("SELECT COUNT(*) FROM variable_same_as").fetchone() == (0,)
