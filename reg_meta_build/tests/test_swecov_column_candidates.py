"""SWECOV generator: the register-file `[[errata.column]]` candidates (`cmd_grafts`).

Gapfill columns the `globals` stage found are placed on a catalog variant and
rendered as whole `[[errata.column]]` entries in `derived/column_candidates.toml`;
what the scan cannot place rides as a comment. Fixture provenance and the
generator's boundary: `_swecov_fixtures`.
"""

from __future__ import annotations

import argparse
import json
import tomllib
from typing import TYPE_CHECKING

import pytest
from _curation_fixtures import write_lisa_errata
from _swecov_fixtures import (
    build_catalog,
    flavored_db_fixture,  # noqa: F401
)
from reg_meta.errors import RegMetaError

if TYPE_CHECKING:
    from pathlib import Path


def _grafts_text(
    tmp_path: Path,
    db: Path,
    gapfill: list[dict],
    enriched: dict[str, dict],
    doc_columns: tuple[str, ...] = (),
) -> str:
    """Run the subcommand over synthetic `globals` output; read back the worklist.

    The CSV is header-only: with no physical holding to consult, a `mapped`
    holding's single coordinate places every column, which is the path under test.
    `doc_columns` seeds the ingested-docs tree the scan cross-references — its root
    is two levels above the CSV, the repo layout `reg_meta_build/docs/<register>/`.
    """
    base = tmp_path / "input_data" / "swecov"
    (base / "derived").mkdir(parents=True)
    csv_path = base / "SWECOV_variables_2025-12-11.csv"
    csv_path.write_text("Category,Detail,Table\n", encoding="utf-8")
    (base / "derived" / "holdings_enriched.json").write_text(
        json.dumps(enriched), encoding="utf-8"
    )
    (base / "derived" / "global_enrichment.json").write_text(
        json.dumps({"gapfill": gapfill}), encoding="utf-8"
    )
    for column in doc_columns:
        page = tmp_path / "docs" / "lisa" / f"{column}.md"
        page.parent.mkdir(parents=True, exist_ok=True)
        page.write_text(f"# {column}\n", encoding="utf-8")

    build_catalog.cmd_grafts(argparse.Namespace(csv=csv_path, db=db))
    return (base / "derived" / "column_candidates.toml").read_text(encoding="utf-8")


def _gapfill(register: str, column: str, **extra: str) -> dict:
    return {
        "register": register,
        "column": column,
        "description": f"{column} enligt SCB",
        **extra,
    }


def _lisa_holding(*columns: str) -> dict[str, dict]:
    """One `mapped` LISA holding documenting `columns` — the single-coordinate
    mapping every placed candidate below resolves through."""
    return {
        "LISA / Individer": {
            "columns": [{"name": column} for column in columns],
            "mapping": {"status": "mapped", "to": "scb/lisa/individer-15plus"},
        }
    }


def test_a_placed_gapfill_column_is_a_complete_column_entry(
    tmp_path: Path, flavored_db: Path
) -> None:
    """The candidate is a whole `[[column]]`: the variant the mapping placed it on,
    the description as both `name` and `definition`, `all_versions = true` because a
    holdings list dates nothing — and the curator's two fields as TODO placeholders,
    `noted` in the form `load_scb_errata` refuses. No `data_type`: the delivery
    spells `varchar`, which is not one of the four the loader accepts."""
    text = _grafts_text(
        tmp_path,
        flavored_db,
        [_gapfill("scb/lisa", "FastBet", data_type="varchar", kalla="LISA")],
        _lisa_holding("FastBet"),
    )

    assert tomllib.loads(text)["errata"]["column"] == [
        {
            "variant": "individer-15plus",
            "column": "FastBet",
            "name": "FastBet enligt SCB",
            "definition": "FastBet enligt SCB",
            "all_versions": True,
            "source": "steward-holdings",
            "evidence": "TODO: the delivery list holding this column, and that SCB's "
            "export carries no row for it on any version of this variant",
            "noted": "TODO: YYYY-MM-DD",
        }
    ]
    # The scan's own evidence rides as a comment, for the curator to write
    # `evidence` from — not as a key the loader would refuse.
    assert "# scb/lisa/individer-15plus FastBet; källa LISA; delivery type varchar" in (
        text.splitlines()
    )


def test_an_scb_documented_column_is_the_same_entry_under_the_other_source(
    tmp_path: Path, flavored_db: Path
) -> None:
    """A column reg_meta's ingested SCB docs describe is no longer routed away from
    the scan: since Y-116 it is the SAME entry kind, separated only by `source`, so
    both land in one file and the maintainer curates one grammar."""
    text = _grafts_text(
        tmp_path,
        flavored_db,
        [_gapfill("scb/lisa", "Ssyk4_J16"), _gapfill("scb/lisa", "FastBet")],
        _lisa_holding("Ssyk4_J16", "FastBet"),
        doc_columns=("Ssyk4_J16",),
    )

    assert {
        entry["column"]: entry["source"]
        for entry in tomllib.loads(text)["errata"]["column"]
    } == {"Ssyk4_J16": "scb-docs", "FastBet": "steward-holdings"}
    # Sectioned by source, each section saying what still has to be curated in it.
    assert "# ── source = scb-docs: 1 column(s)" in text
    assert "narrow `all_versions` to the years its doc page carries" in text


@pytest.mark.parametrize(
    ("gapfill", "enriched", "section"),
    [
        pytest.param(
            _gapfill("scb/lisa", "Okand"),
            {},
            "no variant:",
            id="unplaced",
        ),
        pytest.param(
            _gapfill("scb/gdb", "Ruta250"),
            {
                "Geografidatabasen / Individer": {
                    "columns": [{"name": "Ruta250"}],
                    "mapping": {"status": "flavor", "graft": "scb/gdb"},
                }
            },
            "steward flavor:",
            id="flavor-register",
        ),
        pytest.param(
            _gapfill("scb/lisa", "SyssStat_klartext"),
            _lisa_holding("SyssStat_klartext"),
            "content-dropped:",
            id="klartext",
        ),
    ],
)
def test_a_column_the_scan_cannot_place_is_a_comment_not_an_entry(
    tmp_path: Path,
    flavored_db: Path,
    gapfill: dict,
    enriched: dict[str, dict],
    section: str,
) -> None:
    """Nothing the scan did not place reaches the grammar. A column with no variant
    has nothing to mint onto, a steward-flavor column is not SCB's export at all,
    and a klartext label column is #373's — each rides as a comment in its own
    section, so the file the maintainer reads parses to no entries."""
    text = _grafts_text(tmp_path, flavored_db, [gapfill], enriched)

    assert tomllib.loads(text) == {}
    assert f"# ── {section} 1 column(s)" in text
    assert any(
        line.startswith("# ") and gapfill["column"] in line
        for line in text.splitlines()
    )


def test_the_emitted_candidates_load_as_scb_errata(
    tmp_path: Path, flavored_db: Path
) -> None:
    """The output IS the repair candidate, so its `[[errata.column]]` table parses,
    and `load_scb_errata` accepts its shape against the repo's curated SCB slugs.

    `noted` is the one field a placeholder cannot satisfy — the loader demands a
    canonical `YYYY-MM-DD`, which is exactly what stops an uncurated paste from
    reaching a build — so the proof is the same one `inventory_coverage`'s worklist
    carries: refused while undated, loads once dated, every other key already what
    the loader wants. Real repo slugs, because that is what the stanzas name.
    """
    from reg_meta_build.scb_errata import load_scb_errata

    from reg_meta_build.fqid_slugs import repo_slug_dir

    text = _grafts_text(
        tmp_path,
        flavored_db,
        [_gapfill("scb/lisa", "Ssyk4_J16"), _gapfill("scb/lisa", "FastBet")],
        _lisa_holding("Ssyk4_J16", "FastBet"),
        doc_columns=("Ssyk4_J16",),
    )
    root = write_lisa_errata(tmp_path / "curation", text)

    with pytest.raises(RegMetaError) as exc:
        load_scb_errata(root, repo_slug_dir())
    assert "`noted`" in exc.value.message

    root = write_lisa_errata(
        root,
        text.replace('noted = "TODO: YYYY-MM-DD"', 'noted = "2026-09-12"'),
    )
    errata = load_scb_errata(root, repo_slug_dir())
    assert {(column.column, column.source) for column in errata.columns} == {
        ("Ssyk4_J16", "scb-docs"),
        ("FastBet", "steward-holdings"),
    }
    # `all_versions` — the generator dates nothing, and neither source does.
    assert all(column.versions is None for column in errata.columns)
