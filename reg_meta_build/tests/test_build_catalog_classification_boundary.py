"""Classification references and label bindings at the build-catalog boundary.

The fixture is one SCB register (native id 1) plus one Försäkringskassan thin
register whose AMOUNT column (valid 2020) may declare a classification
reference. Curated classification files live under
`curation/classifications/<SHORT>.toml`; each book's code list is a prepared
`classifications/<slug>.csv` input.
"""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import var_row, write_input_bundle, write_scb_input
from _pipeline_catalog_support import report_issues
from _prepared_fixtures import accept_prepared
from reg_meta.errors import RegMetaError
from reg_meta.queries import search
from reg_meta_build.db import open_built_db
from reg_meta_build.pipeline import build_catalog, check_curation
from reg_meta_build.prepared_catalog import prepare_catalog_sources
from reg_meta_build.source_naming import authored_naming_id

if TYPE_CHECKING:
    from pathlib import Path

Books = dict[str, tuple[str, str]]  # short_name -> (slug, extra TOML lines)
STALE_REF = "classifications/A.toml#/binding/value_set_labels/1"


def write_catalog(
    tmp_path: Path, books: Books, declared: str | None = None
) -> tuple[Path, str, str, Path]:
    source = tmp_path / "source"
    write_scb_input(
        source,
        registerinformation_rows=[
            var_row(cvid=1001, var_id=101, colname="VALUE", data_type="int")
        ],
        unika_rows=[
            "TESTREG|Testregistret|Individer|Individer|GenericVar|VALUE|2020|2020|0|0|0"
        ],
        include=("registerinformation", "unika"),
    )
    fk = source / "Forsakringskassan"
    fk.mkdir()
    (fk / "fk.toml").write_text(
        '[[register]]\nkey = "remote"\nname = "Remote"\n'
        'valid_from = "2020-01-01"\nvalid_to = "2020-12-31"\n'
        '[[register.variable]]\nname = "Amount"\ncolumn = "AMOUNT"\n'
        'data_type = "int"\n'
        + (f"classification = {json.dumps(declared)}\n" if declared else ""),
        encoding="utf-8",
    )
    codes = source / "classifications"
    codes.mkdir()
    curation = tmp_path / "curation"
    (curation / "classifications").mkdir(parents=True)
    for short_name, (slug, extra) in books.items():
        (curation / "classifications" / f"{short_name}.toml").write_text(
            f'[classification]\nshort_name = "{short_name}"\nslug = "{slug}"\n'
            f'name = "{short_name}"\ncodes_file = "{slug}.csv"\n{extra}',
            encoding="utf-8",
        )
        (codes / f"{slug}.csv").write_text(
            "code,label\n1,One\n2,Two\n", encoding="utf-8"
        )
    bundle = write_input_bundle(tmp_path / "inputs", source)
    prepared = tmp_path / "prepared" / "catalog"
    manifest = prepare_catalog_sources(bundle, prepared)
    commit = accept_prepared(prepared)
    scb = curation / "registers" / "scb"
    scb.mkdir(parents=True)
    (scb / "sample.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
        '[[variant]]\nnative_id = "1.10"\nslug = "people"\n'
        '[[variable]]\nnative_id = "1.101"\nslug = "value"\n',
        encoding="utf-8",
    )
    thin = curation / "registers" / "fk"
    thin.mkdir()
    register_id = authored_naming_id("register", provider="fk", register_key="remote")
    variant_id = authored_naming_id(
        "register_variant", provider="fk", register_key="remote", member_key="_default"
    )
    variable_id = authored_naming_id(
        "variable", provider="fk", register_key="remote", member_key="AMOUNT"
    )
    (thin / "remote.toml").write_text(
        '[register]\nprovider = "fk"\nslug = "remote"\n'
        f'native_id = "{register_id}"\n'
        f'[[variant]]\nslug = "default"\nnative_id = "{variant_id}"\n'
        f'[[variable]]\nslug = "amount"\nnative_id = "{variable_id}"\n',
        encoding="utf-8",
    )
    return prepared, commit, manifest.sha256, curation


def diagnostic_build(tmp_path: Path, books: Books, declared: str | None = None):
    prepared, commit, digest, curation = write_catalog(tmp_path, books, declared)
    return build_catalog(
        prepared,
        commit,
        digest,
        tmp_path / "out.db",
        tmp_path / "report",
        curation_dir=curation,
        diagnostic=True,
    )


def amount_classifications(db: Path) -> list[str]:
    with sqlite3.connect(db) as conn:
        return [
            slug
            for (slug,) in conn.execute(
                "SELECT c.slug FROM state_classification sc "
                "JOIN classification c ON c.id = sc.classification_id "
                "JOIN variable_state s ON s.state_id = sc.state_id "
                "WHERE s.delivery_column_name = 'AMOUNT' ORDER BY c.slug"
            )
        ]


def issue_codes(report: Path) -> list[tuple[str, str, str]]:
    return [(i["code"], i["severity"], i["subject"]) for i in report_issues(report)]


@pytest.mark.parametrize("declared", ["A", "Source A"])
def test_declared_short_name_or_alias_binds_state_to_its_classification(
    tmp_path: Path, declared: str
) -> None:
    books = {"A": ("a", 'aliases = ["Source A"]\n'), "B": ("b", "")}
    result = diagnostic_build(tmp_path, books, declared)
    assert result["status"] == "diagnostic_complete"
    assert amount_classifications(tmp_path / "out.db") == ["a"]
    assert issue_codes(tmp_path / "report") == []


def test_declared_family_alias_binds_state_to_the_covering_edition(
    tmp_path: Path,
) -> None:
    books = {
        "OLD": (
            "old",
            'family = "pair"\nfamily_aliases = ["Source pair"]\nvalid_to = 2019\n',
        ),
        "NEW": ("new", 'family = "pair"\nvalid_from = 2020\n'),
    }
    diagnostic_build(tmp_path, books, "Source pair")
    assert amount_classifications(tmp_path / "out.db") == ["new"]
    assert issue_codes(tmp_path / "report") == []


def test_classification_is_found_by_name_search_on_the_built_artifact(
    tmp_path: Path,
) -> None:
    books = {"ALPHA": ("alpha", 'name_en = "Distinctive nomenclature"\n')}
    diagnostic_build(tmp_path, books)
    conn = open_built_db(tmp_path / "out.db")
    try:
        by_name = search(conn, "alpha", field="description", type="classification")
        by_name_en = search(
            conn, "nomenclature", field="description", type="classification"
        )
    finally:
        conn.close()
    assert [str(row.fqid) for row in by_name.results] == ["class/alpha"]
    assert [str(row.fqid) for row in by_name_en.results] == ["class/alpha"]


@pytest.mark.parametrize(
    ("books", "message"),
    [
        pytest.param(
            {"A": ("a", 'aliases = ["B"]\n'), "B": ("b", "")},
            "classifications/B.toml: classification reference 'B' is also "
            "declared in classifications/A.toml.",
            id="alias-repeats-short-name",
        ),
        pytest.param(
            {
                "A": ("a", 'aliases = ["Shared"]\n'),
                "B": ("b", 'aliases = ["Shared"]\n'),
            },
            "classifications/B.toml: classification reference 'Shared' is also "
            "declared in classifications/A.toml.",
            id="alias-repeated-across-books",
        ),
        pytest.param(
            {
                "A": ("a", 'family = "pair"\nfamily_aliases = ["B"]\n'),
                "B": ("b", 'family = "pair"\n'),
            },
            "classifications/B.toml: classification reference 'B' is also "
            "declared in classifications/A.toml.",
            id="family-alias-repeats-short-name",
        ),
        pytest.param(
            {
                "A": ("a", 'family = "first"\nfamily_aliases = ["Shared"]\n'),
                "B": ("b", 'family = "first"\n'),
                "C": ("c", 'family = "second"\nfamily_aliases = ["Shared"]\n'),
                "D": ("d", 'family = "second"\n'),
            },
            "classifications/C.toml: classification reference 'Shared' is also "
            "declared in classifications/A.toml.",
            id="family-alias-repeated-across-families",
        ),
    ],
)
def test_ambiguous_classification_reference_spelling_fails_at_its_file(
    tmp_path: Path, books: Books, message: str
) -> None:
    with pytest.raises(RegMetaError) as caught:
        diagnostic_build(tmp_path, books)
    assert caught.value.code == "classification_curation_invalid"
    assert caught.value.message == message


def test_full_build_reports_value_set_label_matching_no_descriptor(
    tmp_path: Path,
) -> None:
    books = {"A": ("a", '[binding]\nvalue_set_labels = ["No such label"]\n')}
    diagnostic_build(tmp_path, books)
    assert issue_codes(tmp_path / "report") == [
        ("stale_curation_entry", "error", STALE_REF)
    ]


def test_check_curation_defers_unmatched_value_set_label_to_full_build(
    tmp_path: Path,
) -> None:
    books = {"A": ("a", '[binding]\nvalue_set_labels = ["No such label"]\n')}
    prepared, commit, digest, curation = write_catalog(tmp_path, books)
    result = check_curation(
        prepared,
        commit,
        digest,
        tmp_path / "report",
        curation_dir=curation,
        registers=("1",),
        dump_decisions=tmp_path / "decisions",
    )
    assert result["status"] == "curation_check_complete"
    assert result["passed"] is True
    assert issue_codes(tmp_path / "report") == []
    report = json.loads((tmp_path / "decisions/compile-report.json").read_text())
    assert report["_classifications"]["not_evaluated_in_subset"] == [STALE_REF]
