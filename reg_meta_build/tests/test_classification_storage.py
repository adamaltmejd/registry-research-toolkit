"""Classification CSV loading, written classification storage and the reader CLI."""

from __future__ import annotations

import json
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest
from catalog_manifest import synthetic_manifest
from reg_meta.errors import RegMetaError
from reg_meta_build.classifications import load_valid_codes

if TYPE_CHECKING:
    from pathlib import Path


class TestLoadValidCodes:
    def _csv(self, tmp_path: Path, body: str) -> Path:
        path = tmp_path / "codes.csv"
        path.write_text(body, encoding="utf-8")
        return path

    def test_loads_simple(self, tmp_path: Path):
        path = self._csv(tmp_path, "vardekod,vardebenamning\nA,Alpha\nB,Bravo\n")
        assert load_valid_codes(path) == {"A": "Alpha", "B": "Bravo"}

    def test_strips_whitespace(self, tmp_path: Path):
        path = self._csv(tmp_path, "vardekod,vardebenamning\n  A  ,  Alpha label  \n")
        assert load_valid_codes(path) == {"A": "Alpha label"}

    def test_skips_blank_lines(self, tmp_path: Path):
        path = self._csv(tmp_path, "vardekod,vardebenamning\nA,Alpha\n\n,\nB,Bravo\n")
        assert load_valid_codes(path) == {"A": "Alpha", "B": "Bravo"}

    def test_bad_header(self, tmp_path: Path):
        path = self._csv(tmp_path, "foo,bar\nA,Alpha\n")
        with pytest.raises(RegMetaError) as ei:
            load_valid_codes(path)
        assert ei.value.code == "classification_csv_invalid"

    def test_universal_header_accepted(self, tmp_path: Path):
        # The SOS CSVs ship `code,label` (+ extra trailing columns we ignore).
        path = self._csv(
            tmp_path,
            "code,label,label_en,parent_code\nA,Alpha,Alpha-en,\nB,Bravo,,A\n",
        )
        assert load_valid_codes(path) == {"A": "Alpha", "B": "Bravo"}

    def test_duplicate_code(self, tmp_path: Path):
        path = self._csv(tmp_path, "vardekod,vardebenamning\nA,Alpha\nA,Apple\n")
        with pytest.raises(RegMetaError) as ei:
            load_valid_codes(path)
        assert "duplicate" in ei.value.message.lower()

    def test_empty_data(self, tmp_path: Path):
        path = self._csv(tmp_path, "vardekod,vardebenamning\n")
        with pytest.raises(RegMetaError):
            load_valid_codes(path)


def _run_json(db_dir: Path, args: list[str]) -> tuple[dict, int]:
    proc = subprocess.run(
        [
            sys.executable,
            "-m",
            "reg_meta",
            "--db",
            str(db_dir),
            "--format",
            "json",
            *args,
        ],
        capture_output=True,
        text=True,
        check=False,  # tests assert on returncode; nonzero exits are expected
    )
    out = proc.stdout.strip()
    # JSON errors still produce JSON on stdout; just parse.
    return json.loads(out), proc.returncode


@pytest.fixture(scope="module")
def classification_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    from reg_meta_build.resolved_catalog import (
        ResolvedClassification,
        ResolvedClassificationCode,
        ResolvedClassificationLink,
        ResolvedCodeSet,
        ResolvedRegister,
        ResolvedState,
        ResolvedVariable,
        ResolvedVariant,
        write_resolved_catalog,
    )

    tmp = tmp_path_factory.mktemp("cls")
    books = tuple(
        ResolvedClassification(
            slug=short_name.lower(),
            short_name=short_name,
            name=name,
            codes=tuple(
                ResolvedClassificationCode(code=code, label=label, level=len(code))
                for code, label in members
            ),
        )
        for short_name, name, members in (
            (
                "TESTKON",
                "Test classification for gender codes",
                (("1", "Man"), ("2", "Kvinna")),
            ),
            (
                "TESTKON2",
                "Successor",
                (("10", "Female"), ("20", "Male"), ("30", "Other")),
            ),
        )
    )
    variables = tuple(
        ResolvedVariable(
            register=ResolvedRegister(
                provider="scb", slug=register, name=register.upper()
            ),
            provider_key="44",
            slug="kon",
            name="Kön",
            definition=None,
            description=None,
            operational_definition=None,
            measurement_unit=None,
            is_identifier=False,
            is_sensitive=False,
            states=tuple(
                ResolvedState(
                    variant=ResolvedVariant(slug="individer", name="Individer"),
                    valid_from=f"{year}-01-01",
                    valid_to=f"{year}-12-31",
                    delivery_column_name="Kon",
                    data_type="integer",
                    data_length="2",
                    operational_definition=None,
                    provenance=None,
                    classification_links=(
                        ResolvedClassificationLink(classification=book.slug),
                    ),
                    value_set=ResolvedCodeSet(
                        members=tuple((c.code, c.label) for c in book.codes)
                    ),
                )
                for year, book in ((2020, books[0]), (2022, books[1]))
                if register == "testreg" or year == 2020
            ),
        )
        for register in ("testreg", "otherreg")
    )
    db_dir = tmp / "db"
    write_resolved_catalog(
        variables,
        db_dir / "reg_meta.db",
        manifest=synthetic_manifest(),
        classifications=books,
    )

    # Query commands require a doc DB alongside.
    from reg_meta_build.doc_db import build_doc_db

    docs_src = tmp / "docs" / "stub"
    docs_src.mkdir(parents=True)
    (docs_src / "Stub.md").write_text(
        "---\nvariable: Stub\ndisplay_name: Stub\ntags:\n  - type/variable\n---\n\nBody.\n",
        encoding="utf-8",
    )
    build_doc_db(tmp / "docs", db_dir)
    return db_dir


class TestClassificationStorage:
    def test_books_codes_and_state_associations(self, classification_db: Path):
        from contextlib import closing

        from reg_meta_build.db import open_built_db

        with closing(open_built_db(classification_db / "reg_meta.db")) as conn:
            assert {
                tuple(row)
                for row in conn.execute(
                    "SELECT short_name, code_count FROM classification"
                )
            } == {("TESTKON", 2), ("TESTKON2", 3)}
            assert {
                tuple(row)
                for row in conn.execute(
                    "SELECT c.short_name, v.code, cc.level, cc.is_valid FROM classification_code cc JOIN classification c ON c.id=cc.classification_id JOIN value_code v USING(code_id)"
                )
            } == {
                ("TESTKON", "1", 1, 1),
                ("TESTKON", "2", 1, 1),
                ("TESTKON2", "10", 2, 1),
                ("TESTKON2", "20", 2, 1),
                ("TESTKON2", "30", 2, 1),
            }
            assert {
                tuple(row)
                for row in conn.execute(
                    "SELECT r.slug, s.valid_from, c.short_name FROM state_classification sc JOIN classification c ON c.id=sc.classification_id JOIN variable_state s USING(state_id) JOIN variable v USING(variable_id) JOIN register r USING(register_id)"
                )
            } == {
                ("testreg", "2020-01-01", "TESTKON"),
                ("testreg", "2022-01-01", "TESTKON2"),
                ("otherreg", "2020-01-01", "TESTKON"),
            }
            assert conn.execute("PRAGMA foreign_key_check").fetchall() == []

    def test_reader_cli_accepts_current_producer_schema(self, classification_db: Path):
        data, code = _run_json(classification_db, ["get", "classification", "--list"])
        assert code == 0
        assert {item["short_name"] for item in data["classifications"]} == {
            "TESTKON",
            "TESTKON2",
        }
