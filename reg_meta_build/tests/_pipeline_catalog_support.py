"""Shared prepared-catalog fixture and report readers for the build-db pipeline tests."""

from __future__ import annotations

import gzip
import json
import shutil
import sqlite3
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import write_input_bundle
from _prepared_fixtures import accept_prepared
from reg_meta_build.pipeline import (
    build_catalog,
    check_curation,
)
from reg_meta_build.prepared_catalog import (
    prepare_catalog_sources,
)

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

    def private(self, root: Path) -> CatalogFixture:
        """This catalog over its own copy of the shared prepared repository.

        For a test that checks a guard against writing into the prepared input: a
        regressed guard then writes the copy, never the session's shared cache.
        """
        copy = root / "private-prepared"
        shutil.copytree(self.prepared.parent, copy, symlinks=True)
        return replace(self, prepared=copy / self.prepared.name)

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


# The `catalog` fixture's source rows (`_csv_fixtures.var_row` keyword arguments).
_VALUE = {"cvid": 1001, "var_id": 101, "colname": "VALUE", "data_type": "int"}


def _other(var_id: int) -> dict:
    return {
        "cvid": 2001,
        "var_id": var_id,
        "colname": "OTHER",
        "varname": "OtherVar",
        "data_type": "int",
        "register": ["OTHERREG", 2, 20],
    }


# An FK thin register keyed like SCB register 1 (`_csv_fixtures.var_row`'s default).
_THIN_TWIN = [
    "[[register]]",
    'key = "1"',
    'name = "Remote"',
    'valid_from = "2020-01-01"',
    'valid_to = "2020-12-31"',
    "[[register.variable]]",
    'name = "Amount"',
    'column = "AMOUNT"',
    'data_type = "int"',
]


@pytest.fixture
def catalog(tmp_path: Path, request, prepared_cache) -> CatalogFixture:
    """An accepted synthetic SCB source plus a per-test curation tree.

    The prepared input comes from the session's `cases/build` cache, shared
    read-only across tests; each test writes its own curation under ``tmp_path``.
    ``True`` adds a second register; ``"shared_var"`` makes its variable reuse the
    first's native variable id, so the two differ only by their register's native id.
    ``"thin_twin"`` adds an uncurated FK thin register keyed ``1``, so two sources
    expose the bare register id ``1``.
    """
    mode = getattr(request, "param", False)
    second = mode in (True, "shared_var")
    other_var = 101 if mode == "shared_var" else 201
    rows = [_VALUE] + ([_other(other_var)] if second else [])
    spec: dict = {
        "description": "The build-db `catalog` fixture: register 1 delivers VALUE"
        + ("; register 2 delivers OTHER." if second else ".")
        + ("; FK thin register 1 delivers AMOUNT." if mode == "thin_twin" else ""),
        "scb": {"registerinformation": rows},
    }
    if mode == "thin_twin":
        spec["files"] = {"Forsakringskassan/fk.toml": _THIN_TWIN}
    prepared = prepared_cache.get(spec)
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
            f'[[variable]]\nnative_id = "2.{other_var}"\nslug = "value"\n',
            encoding="utf-8",
        )
    return CatalogFixture(prepared.prepared, prepared.commit, prepared.digest, curation)


def built_db_dir(
    catalog: CatalogFixture, tmp_path: Path, registers: tuple[str, ...] = ("1",)
) -> Path:
    """The ``--db`` dir of a complete catalog built from the synthetic source."""
    output = tmp_path / "db" / "reg_meta.db"
    output.parent.mkdir()
    result = catalog.build(output, tmp_path / "report", registers=registers)
    assert result["status"] == "complete"
    return output.parent


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
