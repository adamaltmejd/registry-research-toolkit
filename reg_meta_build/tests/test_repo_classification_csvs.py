"""The committed classification books and their code CSVs must load.

Reads the repository's curated classification TOMLs and the tracked code CSVs
they name, through the same public loaders the build uses.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest
from reg_meta_build.classifications import load_valid_codes
from reg_meta_build.curation_tree import ClassificationMetadata, load_classifications

# reg_meta_build/ package root (tests/ sits beside curation/ and input_data/).
_ROOT = Path(__file__).resolve().parent.parent
_BOOKS = _ROOT / "input_data" / "classifications"


@pytest.fixture(scope="module")
def books() -> dict[str, ClassificationMetadata]:
    return {
        entry.classification.short_name: entry.classification
        for entry in load_classifications(_ROOT / "curation")
    }


def test_every_committed_book_csv_loads(
    books: dict[str, ClassificationMetadata],
) -> None:
    for book in books.values():
        assert load_valid_codes(_BOOKS / book.codes_file), book.short_name


def test_icd11_book_loads_its_labelled_hierarchical_codes(
    books: dict[str, ClassificationMetadata],
) -> None:
    assert books["ICD-11-SE"].codes_file == "sos/icd-11-se.csv"
    assert books["ICD-11-SE"].valid_from == 2027
    assert books["ICD-10-SE"].valid_to is None
    icd11 = load_valid_codes(_BOOKS / "sos" / "icd-11-se.csv")
    assert icd11["1A00"] == "Kolera"
    assert all(icd11.values())
    with (_BOOKS / "sos" / "icd-11-se.csv").open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert sum(1 for row in rows if row["parent_code"]) > 0


def test_sni2025_book_loads_its_labelled_codes(
    books: dict[str, ClassificationMetadata],
) -> None:
    assert books["SNI2025"].codes_file == "sni2025.csv"
    assert books["SNI2025"].valid_from is None
    assert books["SNI2007"].valid_to is None
    sni2025 = load_valid_codes(_BOOKS / "sni2025.csv")
    assert sni2025["A"] == "Jordbruk, skogsbruk och fiske"
    assert sni2025["01110"] == (
        "Odling av spannmål (utom ris), baljväxter och oljeväxter"
    )
