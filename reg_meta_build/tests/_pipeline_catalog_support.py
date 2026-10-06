"""Shared prepared-catalog fixture and report readers for the build-db pipeline tests."""

from __future__ import annotations

import gzip
import json
import sqlite3
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import var_row as _var_row, write_input_bundle, write_scb_input
from _prepared_fixtures import accept_prepared
from _sos_fixtures import DEFAULT_REGISTERS, write_sos_input
from reg_meta_build.pipeline import (
    build_catalog,
    check_curation,
)
from reg_meta_build.prepared_catalog import (
    prepare_catalog_sources,
)
from reg_meta_build.source_naming import authored_naming_id

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class CatalogFixture:
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

    def check(self, report: Path, **kwargs):
        registers = kwargs.pop("registers", ("1",))
        return check_curation(
            self.prepared,
            self.commit,
            self.digest,
            report,
            curation_dir=self.curation,
            registers=registers,
            **kwargs,
        )


@pytest.fixture
def catalog(tmp_path: Path, request) -> CatalogFixture:
    mode = getattr(request, "param", False)
    second = mode is True or mode == "unknown_support"
    thin = mode in {"thin", "thin_two"}
    source = tmp_path / "source"
    records = [_var_row(cvid=1001, var_id=101, colname="VALUE", data_type="int")]
    summaries = [
        "TESTREG|Testregistret|Individer|Individer|GenericVar|VALUE|2020|2020|0|0|0"
    ]
    if second:
        records.append(
            _var_row(
                cvid=2001,
                var_id=201,
                colname="OTHER",
                varname="OtherVar",
                data_type="int",
                register=("OTHERREG", 2, 20),
            )
        )
        summaries.append(
            "OTHERREG|Testregistret|Individer|Individer|OtherVar|OTHER|2020|2020|0|0|0"
        )
    if mode == "unknown_support":
        summaries = [
            "|".join((*row.split("|")[:5], "", *row.split("|")[6:]))
            for row in summaries
        ]
    write_scb_input(
        source,
        registerinformation_rows=records,
        unika_rows=summaries,
        include=("registerinformation", "unika"),
    )
    if thin:
        thin_source = source / "Forsakringskassan"
        thin_source.mkdir()
        (thin_source / "fk.toml").write_text(
            '[[register]]\nkey = "remote"\nname = "Remote"\n'
            'valid_from = "2020-01-01"\nvalid_to = "2020-12-31"\n'
            '[[register.variable]]\nname = "Amount"\ncolumn = "AMOUNT"\n'
            'data_type = "int"\n'
            + (
                '[[register]]\nkey = "aktivitetsstod"\nname = "Aktivitetsstöd"\n'
                'valid_from = "2020-01-01"\nvalid_to = "2020-12-31"\n'
                '[[register.variable]]\nname = "Benefit"\ncolumn = "BENEFIT"\n'
                'data_type = "int"\n'
                if mode == "thin_two"
                else ""
            ),
            encoding="utf-8",
        )
    if mode == "sos_whole":
        from openpyxl import load_workbook

        sos_dir = write_sos_input(source, registers=DEFAULT_REGISTERS[1:2])
        workbook_path = next(sos_dir.glob("*.xlsx"))
        workbook = load_workbook(workbook_path)
        workbook["Generell information"]["C4"] = None
        workbook.save(workbook_path)
        workbook.close()
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
    if second:
        (registers / "other.toml").write_text(
            '[register]\nprovider = "scb"\nslug = "other"\nnative_id = "2"\n'
            '[[variant]]\nnative_id = "2.20"\nslug = "people"\n'
            '[[variable]]\nnative_id = "2.201"\nslug = "value"\n',
            encoding="utf-8",
        )
    if thin:
        thin_curation = curation / "registers" / "fk"
        thin_curation.mkdir()
        register_id = authored_naming_id(
            "register", provider="fk", register_key="remote"
        )
        variant_id = authored_naming_id(
            "register_variant",
            provider="fk",
            register_key="remote",
            member_key="_default",
        )
        variable_id = authored_naming_id(
            "variable", provider="fk", register_key="remote", member_key="AMOUNT"
        )
        (thin_curation / "remote.toml").write_text(
            '[register]\nprovider = "fk"\nslug = "remote"\n'
            f'native_id = "{register_id}"\n'
            '[[variant]]\nslug = "default"\n'
            f'native_id = "{variant_id}"\n'
            '[[variable]]\nslug = "amount"\n'
            f'native_id = "{variable_id}"\n',
            encoding="utf-8",
        )
        if mode == "thin_two":
            register_id = authored_naming_id(
                "register", provider="fk", register_key="aktivitetsstod"
            )
            variant_id = authored_naming_id(
                "register_variant",
                provider="fk",
                register_key="aktivitetsstod",
                member_key="_default",
            )
            variable_id = authored_naming_id(
                "variable",
                provider="fk",
                register_key="aktivitetsstod",
                member_key="BENEFIT",
            )
            (thin_curation / "aktivitetsstod.toml").write_text(
                '[register]\nprovider = "fk"\nslug = "aktivitetsstod"\n'
                f'native_id = "{register_id}"\n'
                '[[variant]]\nslug = "default"\n'
                f'native_id = "{variant_id}"\n'
                '[[variable]]\nslug = "benefit"\n'
                f'native_id = "{variable_id}"\n',
                encoding="utf-8",
            )
    return CatalogFixture(prepared, commit, manifest.sha256, curation)


def prepare_accepted(tmp_path: Path, source: Path) -> tuple[Path, str, str]:
    """Bundle, prepare and accept ``source``; return (prepared dir, commit, digest)."""
    bundle = write_input_bundle(tmp_path / "inputs", source)
    prepared = tmp_path / "prepared" / "catalog"
    manifest = prepare_catalog_sources(bundle, prepared)
    return prepared, accept_prepared(prepared), manifest.sha256


def write_curation_tree(root: Path, files: dict[str, str]) -> Path:
    """Write an authored curation tree; ``files`` maps root-relative paths to text."""
    (root / "classifications").mkdir(parents=True)
    for relative, text in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return root


def report_events(report: Path) -> list[dict]:
    """Every event of a build or check report ledger, decoded, in ledger order."""
    with gzip.open(report / "events.jsonl.gz", "rt", encoding="utf-8") as stream:
        return [json.loads(line) for line in stream]


def report_issues(report: Path) -> list[dict]:
    return [row for row in report_events(report) if row["kind"] == "issue"]


def import_manifest(path: Path) -> dict[str, str]:
    with sqlite3.connect(path) as conn:
        return dict(conn.execute("SELECT key, value FROM import_manifest"))
