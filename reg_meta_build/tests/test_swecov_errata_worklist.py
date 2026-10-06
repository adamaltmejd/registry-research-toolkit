"""SWECOV generator: the register-file errata candidate worklist (`cmd_errata`).

Columns the generated inventory holds in editions the flavored DB has no window for
become `[[errata.version]]` / `[[errata.delivered]]` candidates for SCB, inspection
comments for errata-created columns, and curated-window comments for other
providers. Assertions read `derived/errata_worklist.toml` and the CLI summary.
Fixture provenance and the generator's boundary: `_swecov_fixtures`.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import tomllib
from typing import TYPE_CHECKING

import pytest
from _swecov_fixtures import (
    build_catalog,
    flavored_db_fixture,  # noqa: F401
)
from reg_meta.errors import RegMetaError
from reg_meta.inventory import edition_bounds
from reg_meta_build.edition_bounds import edition_claims

if TYPE_CHECKING:
    from pathlib import Path


_VARIABLE_OF = {
    "Covid-19 antikroppar": "covid-19-antikroppar",
    "Covid_19_antikroppar": "covid-19-antikroppar",
    "T_kolumn": "t-kolumn",
    "Errata_utan_variant": "errata-utan-variant",
}

_INERA = "inera/bestallda-prover/_default"


def _held(
    steward_dir: Path, holdings: dict[int | str, tuple[str, ...]], variant: str
) -> None:
    """A committed-inventory stand-in: one `[[table]]` per edition holding those
    columns of `Beställda prover`, each mapped to its own representation — the
    shape `cmd_inventory` emits. `variant` is the coordinate they map onto, so a
    holdings statement can name the register under another provider."""
    register = variant.rpartition("/")[0]
    lines = ["version = 1", 'steward = "swecov"']
    for edition, columns in holdings.items():
        edition_value = (
            str(edition)
            if isinstance(edition, int) or edition.startswith(("{", "["))
            else json.dumps(edition)
        )
        lines += [
            "",
            "[[table]]",
            f"id = {json.dumps(f'T_{edition}')}",
            f"edition = {edition_value}",
        ]
        for column in columns:
            lines += [
                "",
                "[[table.column]]",
                f'name = "{column}"',
                "",
                "[[table.column.mapping]]",
                f'register_variant = "{variant}"',
                f'variable = "{register}/{_VARIABLE_OF[column]}"',
                f'representation = "{column}"',
            ]
    (steward_dir / "inventory.toml").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def _errata_text(
    tmp_path: Path,
    db: Path,
    holdings: dict[int | str, tuple[str, ...]],
    variant: str = _INERA,
) -> str:
    """Write the holdings, run the subcommand, read back the worklist it leaves
    under `derived/`. `tomllib.loads` of the result is the maintainer's own read:
    comment-only sections carry no entries, so an all-commented worklist is `{}`."""
    steward_dir = tmp_path / "steward"
    steward_dir.mkdir(exist_ok=True)
    _held(steward_dir, holdings, variant)
    csv_path = tmp_path / "SWECOV_variables_2025-12-11.csv"
    build_catalog.cmd_errata(argparse.Namespace(csv=csv_path, db=db, out=steward_dir))
    return (csv_path.parent / "derived" / "errata_worklist.toml").read_text(
        encoding="utf-8"
    )


def test_errata_worklist_is_empty_when_every_holding_has_a_window(
    tmp_path: Path, flavored_db: Path
) -> None:
    """The fixture's states are open-ended, so a holding of any edition is
    covered — the worklist carries no candidate entries."""
    worklist = _errata_text(
        tmp_path, flavored_db, {2021: ("Covid-19 antikroppar", "T_kolumn")}
    )
    assert tomllib.loads(worklist) == {}


def test_errata_rejects_incompatible_catalog_schema(
    tmp_path: Path, flavored_db: Path
) -> None:
    db = tmp_path / "old.db"
    db.write_bytes(flavored_db.read_bytes())
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE import_manifest SET value = '0.1.0' WHERE key = 'schema_version'"
        )
    with pytest.raises(RegMetaError) as exc_info:
        _errata_text(tmp_path, db, {2021: ("T_kolumn",)})
    assert exc_info.value.code == "schema_incompatible"
    assert not (tmp_path / "derived/errata_worklist.toml").exists()


def test_errata_worklist_splits_version_missing_from_column_missing(
    tmp_path: Path, flavored_db: Path
) -> None:
    """With `T_kolumn` narrowed to 2019 and the variant documenting exactly one
    register version (2020), a 2020 holding of `T_kolumn` is column-missing — the
    omitted row names that version verbatim — and a 2021 holding is version-missing
    too, since the catalog knows no version covering it. So the worklist carries one
    `[[errata.version]]` (2021) and ONE `[[errata.delivered]]` naming both editions (two entries
    for one column would be the duplicate `scb_errata` refuses).

    The register moves to the `scb` provider on the copy, because stanzas are what
    SCB register files accept these tables and no other provider: a miss on the fixture's
    own flavor provider is the curated-window case below, not this one.
    """
    db = tmp_path / "narrowed.db"
    db.write_bytes(flavored_db.read_bytes())
    conn = sqlite3.connect(db)
    conn.execute("UPDATE provider SET slug = 'scb' WHERE provider_id = 900")
    conn.execute(
        "UPDATE variable_state SET valid_from = '2019-01-01', valid_to = '2019-12-31' "
        "WHERE delivery_column_name = 'T_kolumn'"
    )
    conn.execute(
        "INSERT INTO register_version "
        "(regver_id, register_variant_id, registerversionnamn) "
        "VALUES (905, 902, '2020')"
    )
    conn.commit()
    conn.close()

    worklist = tomllib.loads(
        _errata_text(
            tmp_path,
            db,
            {2020: ("T_kolumn",), 2021: ("T_kolumn",)},
            variant="scb/bestallda-prover/_default",
        )
    )

    # Complete entries, not fragments: every key `load_scb_errata` requires is
    # present, with the curator's own two as TODO placeholders.
    todo = {
        "evidence": "TODO: the evidence that SCB delivered this row",
        "noted": "TODO: YYYY-MM-DD",
    }
    assert worklist["errata"]["version"] == [
        {
            "variant": "_default",
            "name": "2021",
            **todo,
        }
    ]
    assert worklist["errata"]["delivered"] == [
        {
            "variant": "_default",
            "column": "T_kolumn",
            "versions": ["2020", "2021"],
            **todo,
        }
    ]


def test_errata_column_misses_are_inspection_items_not_delivered_candidates(
    tmp_path: Path, flavored_db: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A `[[column]]`-minted variable has no real SCB row for `[[delivered]]`.

    The finite 2019 state models an explicitly version-limited entry; the second
    variable resolves at register scope but has no state on the target variant,
    so the DB cannot distinguish a missing entry from a routing/materialization
    problem. A real undocumented 2021 edition still earns its independent
    `[[version]]` candidate. The range table proves it contributes to none of the
    candidate or assessed counts.
    """
    db = tmp_path / "errata-columns.db"
    db.write_bytes(flavored_db.read_bytes())
    conn = sqlite3.connect(db)
    conn.execute("UPDATE provider SET slug = 'scb' WHERE provider_id = 900")
    conn.execute(
        "UPDATE variable SET source_label = 'scb-errata' WHERE variable_id = 904"
    )
    conn.execute(
        "UPDATE variable_state SET valid_from = '2019-01-01', "
        "valid_to = '2019-12-31' WHERE variable_id = 904"
    )
    conn.execute(
        "INSERT INTO variable "
        "(variable_id, register_id, provider_key, slug, name, source_label) "
        "VALUES (906, 901, 'Errata_utan_variant', 'errata-utan-variant', "
        "'Errata utan variant', 'scb-errata')"
    )
    conn.execute(
        "INSERT INTO register_version "
        "(regver_id, register_variant_id, registerversionnamn) "
        "VALUES (905, 902, '2020')"
    )
    conn.commit()
    conn.close()

    text = _errata_text(
        tmp_path,
        db,
        {
            2020: ("T_kolumn", "Errata_utan_variant"),
            2021: ("T_kolumn",),
            "{ from = 2022, to = 2023 }": (
                "T_kolumn",
                "Errata_utan_variant",
            ),
        },
        variant="scb/bestallda-prover/_default",
    )

    worklist = tomllib.loads(text)
    assert [entry["name"] for entry in worklist["errata"]["version"]] == ["2021"]
    assert "delivered" not in worklist
    assert "column-missing: the omitted column rows themselves, 0" in text
    assert (
        "errata-created columns: 2 group(s); no [[errata.delivered]] candidate" in text
    )
    assert "scb/bestallda-prover/_default T_kolumn: held 2020..2021" in text
    assert "scb/bestallda-prover/_default Errata_utan_variant: held 2020" in text
    assert "target-variant [[errata.column]] entry and inventory" in text
    assert "does not reveal `all_versions` versus explicit `versions`" in text
    assert "only when its matching" in text
    assert "uses `all_versions = true`" in text
    assert "routing/materialization mismatch" in text
    assert "1 multi-period table(s) not assessed" in text
    assert 'name = "2022"' not in text
    assert 'name = "2023"' not in text

    stdout = capsys.readouterr().out
    assert "held column × edition pairs: 3 assessed, 3 with no catalog window" in stdout
    assert "1 multi-period table(s) not assessed" in stdout
    assert "version-missing: 1 [[errata.version]] candidate(s)" in stdout
    assert "column-missing: 0 [[errata.delivered]] candidate(s)" in stdout
    assert "errata-column: 2 miss(es) to inspect" in stdout


def test_school_year_version_candidate_round_trips_through_scb_claims(
    tmp_path: Path, flavored_db: Path
) -> None:
    """The holdings token stays `LA2020`, while the suggested SCB version name
    uses the loader's native `2020/2021` spelling and claims those exact bounds.
    A multi-period control remains entirely outside inference.
    """
    db = tmp_path / "school-year.db"
    db.write_bytes(flavored_db.read_bytes())
    conn = sqlite3.connect(db)
    conn.execute("UPDATE provider SET slug = 'scb' WHERE provider_id = 900")
    conn.execute(
        "UPDATE variable_state SET valid_from = '2019-01-01', "
        "valid_to = '2019-12-31' WHERE variable_id = 904"
    )
    conn.commit()
    conn.close()

    text = _errata_text(
        tmp_path,
        db,
        {"LA2020": ("T_kolumn",), "[2022, 2023]": ("T_kolumn",)},
        variant="scb/bestallda-prover/_default",
    )
    worklist = tomllib.loads(text)

    assert [entry["name"] for entry in worklist["errata"]["version"]] == ["2020/2021"]
    assert worklist["errata"]["delivered"][0]["versions"] == ["2020/2021"]
    claims = edition_claims(worklist["errata"]["version"][0]["name"])
    assert claims == (
        (2020, "2020-07-01", "2020-12-31"),
        (2021, "2021-01-01", "2021-06-30"),
    )
    assert (claims[0][1], claims[-1][2]) == edition_bounds("LA2020")[0]
    assert "held LA2020, catalog windows 2019" in text
    assert "1 multi-period table(s) not assessed" in text
    assert 'name = "2022"' not in text
    assert 'name = "2023"' not in text


def test_errata_worklist_lists_a_non_scb_miss_as_a_curated_window(
    tmp_path: Path, flavored_db: Path
) -> None:
    """`Beställda prover` sits on the steward's own `inera` provider, and
    errata entries belong in the SCB register files alone — so the same narrowed holding
    yields NO stanza at all. It rides in the third section as a comment naming the
    held editions, the catalog's window and the surface that carries it: for a
    flavor provider, the curated-provider TOML `extend-db` overlaid. A
    `[[errata.delivered]]` here would send the maintainer to a file
    whose loader refuses the entry."""
    db = tmp_path / "narrowed.db"
    db.write_bytes(flavored_db.read_bytes())
    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE variable_state SET valid_from = '2019-01-01', valid_to = '2019-12-31' "
        "WHERE delivery_column_name = 'T_kolumn'"
    )
    conn.commit()
    conn.close()

    text = _errata_text(tmp_path, db, {2020: ("T_kolumn",)})

    # Not one entry of either kind — the whole finding is a comment, so no line
    # opens a stanza and the file parses to nothing.
    assert not [ln for ln in text.splitlines() if ln.startswith("[[")]
    assert tomllib.loads(text) == {}
    assert "curated windows: 1 group(s)" in text
    (line,) = [ln for ln in text.splitlines() if "T_kolumn:" in ln]
    assert "held 2020, catalog windows 2019" in line
    assert "not errata (provider `inera`, not `scb`)" in line
    assert "the curated-provider TOML extend-db overlays" in line


@pytest.mark.parametrize("variant", [_INERA, "scb/bestallda-prover/_default"])
def test_errata_worklist_excludes_every_multi_period_suggestion(
    tmp_path: Path, flavored_db: Path, variant: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Partial, absent and disjoint range/list holdings are not availability
    evidence. They produce no version, delivered-column or curated-window
    suggestion regardless of provider, and the worklist/CLI report an honest
    zero assessed denominator plus the skipped-table count."""
    db = tmp_path / "narrowed.db"
    db.write_bytes(flavored_db.read_bytes())
    conn = sqlite3.connect(db)
    if variant.startswith("scb/"):
        conn.execute("UPDATE provider SET slug = 'scb' WHERE provider_id = 900")
    conn.execute(
        "UPDATE variable_state SET valid_from = '2019-01-01', "
        "valid_to = '2019-12-31' WHERE delivery_column_name = 'T_kolumn'"
    )
    conn.commit()
    conn.close()

    text = _errata_text(
        tmp_path,
        db,
        {
            "{ from = 2018, to = 2020 }": ("T_kolumn",),
            "{ from = 2021, to = 2022 }": ("T_kolumn",),
            "[2016, 2017]": ("T_kolumn",),
        },
        variant=variant,
    )

    assert tomllib.loads(text) == {}
    assert "out of 0 assessed" in text
    assert "3 multi-period table(s) not assessed" in text
    assert "version-missing: 0" in text
    assert "column-missing: the omitted column rows themselves, 0" in text
    assert "errata-created columns: 0" in text
    assert "curated windows: 0" in text
    stdout = capsys.readouterr().out
    assert "held column × edition pairs: 0 assessed, 0 with no catalog window" in stdout
    assert "3 multi-period table(s) not assessed" in stdout
    assert "version-missing: 0 [[errata.version]] candidate(s)" in stdout
    assert "column-missing: 0 [[errata.delivered]] candidate(s)" in stdout
    assert "errata-column: 0 miss(es) to inspect" in stdout
    assert "curated-window: 0 non-scb miss(es)" in stdout
