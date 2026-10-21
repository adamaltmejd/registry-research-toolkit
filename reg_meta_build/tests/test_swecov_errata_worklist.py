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
from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from _swecov_fixtures import (
    build_catalog,
    flavored_db_fixture,  # noqa: F401
    on_scb,
    owner_block,
    t_kolumn_window,
    variant_block,
    write_flavored_db,
)
from reg_meta_build.edition_bounds import edition_claims
from reg_meta_build.errors import RegMetaError
from reg_meta_build.id import mint
from reg_meta_build.inventory import edition_bounds

if TYPE_CHECKING:
    from pathlib import Path


_VARIABLE_OF = {
    "Covid-19 antikroppar": "covid-19-antikroppar",
    "Covid_19_antikroppar": "covid-19-antikroppar",
    "T_kolumn": "t-kolumn",
    "Errata_utan_variant": "errata-utan-variant",
}

# The fixture's own variant coordinate — on `inera`, the steward's flavor provider.
_INERA = "inera/bestallda-prover/_default"


def _held(
    steward_dir: Path,
    holdings: dict[int | str, tuple[str, ...]],
    variant: str,
    narrowing: str | None = None,
) -> None:
    """A committed-inventory stand-in: one `[[table]]` per edition holding those
    columns of `Beställda prover`, each mapped to its own representation — the
    shape `cmd_inventory` emits. `variant` is the coordinate they map onto, so a
    holdings statement can name the register under another provider.
    `narrowing` is a TOML line added to every mapping: a column window (#1645) or
    a year or month member (#1495)."""
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
            if narrowing is not None:
                lines.append(narrowing)
    (steward_dir / "inventory.toml").write_text(
        "\n".join(lines) + "\n", encoding="utf-8"
    )


def _scb_db(directory: Path, provider_toml: str) -> Path:
    """The flavored DB with `Beställda prover` authored under `scb`: errata
    stanzas are what SCB register files accept, and no other provider's."""
    return write_flavored_db(directory, on_scb(provider_toml), provider="scb")


def _scb_rows(db: Path, *, versions: tuple[str, ...] = (), **sql: str) -> None:
    """SCB-side rows the steward overlay never writes, stated directly: the
    documented `register_version`s of the variant, and each `sql` statement
    (the register's id bound as `:register`, `T_kolumn`'s as `:t_kolumn`)."""
    register = mint("register", "scb", "bestallda-prover")
    variant = mint("variant", "scb", "bestallda-prover", "_default")
    with closing(sqlite3.connect(db)) as conn:
        conn.executemany(
            "INSERT INTO register_version "
            "(regver_id, register_variant_id, registerversionnamn) VALUES (?, ?, ?)",
            [(905 + index, variant, name) for index, name in enumerate(versions)],
        )
        for statement in sql.values():
            conn.execute(
                statement,
                {
                    "register": register,
                    "t_kolumn": mint("variable", "scb", "bestallda-prover", "t-kolumn"),
                },
            )
        conn.commit()


def _errata_text(
    tmp_path: Path,
    db: Path,
    holdings: dict[int | str, tuple[str, ...]],
    variant: str = _INERA,
    narrowing: str | None = None,
) -> str:
    """Write the holdings, run the subcommand, read back the worklist it leaves
    under `derived/`. `tomllib.loads` of the result is the maintainer's own read:
    comment-only sections carry no entries, so an all-commented worklist is `{}`."""
    steward_dir = tmp_path / "steward"
    steward_dir.mkdir(exist_ok=True)
    _held(steward_dir, holdings, variant, narrowing)
    csv_path = tmp_path / "SWECOV_variables_2025-12-11.csv"
    build_catalog.cmd_errata(argparse.Namespace(csv=csv_path, db=db, out=steward_dir))
    return (steward_dir / "derived" / "errata_worklist.toml").read_text(
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
    # Closed, not only committed: the reader opens `immutable=1` and ignores a WAL.
    with closing(sqlite3.connect(db)) as conn:
        conn.execute(
            "UPDATE import_manifest SET value = '0.1.0' WHERE key = 'schema_version'"
        )
        conn.commit()
    with pytest.raises(RegMetaError) as exc_info:
        _errata_text(tmp_path, db, {2021: ("T_kolumn",)})
    assert exc_info.value.code == "schema_incompatible"
    assert not (tmp_path / "steward/derived/errata_worklist.toml").exists()


def test_errata_worklist_splits_version_missing_from_column_missing(
    tmp_path: Path,
) -> None:
    """With `T_kolumn` narrowed to 2019 and the variant documenting exactly one
    register version (2020), a 2020 holding of `T_kolumn` is column-missing — the
    omitted row names that version verbatim — and a 2021 holding is version-missing
    too, since the catalog knows no version covering it. So the worklist carries one
    `[[errata.version]]` (2021) and ONE `[[errata.delivered]]` naming both editions (two entries
    for one column would be the duplicate `scb_errata` refuses).

    The register sits on the `scb` provider, because stanzas are what SCB
    register files accept and no other provider: a miss on the fixture's own
    flavor provider is the curated-window case below, not this one.
    """
    db = _scb_db(tmp_path / "narrowed", t_kolumn_window("2019-01-01", "2019-12-31"))
    _scb_rows(db, versions=("2020",))

    worklist = tomllib.loads(
        _errata_text(
            tmp_path,
            db,
            {2020: ("T_kolumn",), 2021: ("T_kolumn",)},
            variant="scb/bestallda-prover/_default",
        )
    )

    # Complete entries, not fragments: every key `resolve_scb_errata` requires is
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
            "expected_evidence_sha256": "TODO: acknowledgement_evidence_sha256 of "
            "the documented column and target rows",
            **todo,
        }
    ]


def test_errata_column_misses_are_inspection_items_not_delivered_candidates(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A `[[column]]`-minted variable has no real SCB row for `[[delivered]]`.

    The finite 2019 state models an explicitly version-limited entry; the second
    variable resolves at register scope but has no state on the target variant,
    so the DB cannot distinguish a missing entry from a routing/materialization
    problem. A real undocumented 2021 edition still earns its independent
    `[[version]]` candidate. The range table proves it contributes to none of the
    candidate or assessed counts.
    """
    db = _scb_db(
        tmp_path / "errata-columns", t_kolumn_window("2019-01-01", "2019-12-31")
    )
    # `[[column]]`-minted variables carry the SCB errata source label, which only
    # the global SCB build writes; the second has no state at all.
    _scb_rows(
        db,
        versions=("2020",),
        label="UPDATE variable SET source_label = 'scb-errata' "
        "WHERE variable_id = :t_kolumn",
        stateless="INSERT INTO variable "
        "(variable_id, register_id, provider_key, slug, name, source_name, source_label) "
        "VALUES (906, :register, 'Errata_utan_variant', 'errata-utan-variant', "
        "'Errata utan variant', 'Errata utan variant', 'scb-errata')",
    )

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
    tmp_path: Path,
) -> None:
    """The holdings token stays `LA2020`, while the suggested SCB version name
    uses the loader's native `2020/2021` spelling and claims those exact bounds.
    A multi-period control remains entirely outside inference.
    """
    db = _scb_db(tmp_path / "school-year", t_kolumn_window("2019-01-01", "2019-12-31"))

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
    tmp_path: Path,
) -> None:
    """`Beställda prover` sits on the steward's own `inera` provider, and
    errata entries belong in the SCB register files alone — so the same narrowed holding
    yields NO stanza at all. It rides in the third section as a comment naming the
    held editions, the catalog's window and the surface that carries it: for a
    flavor provider, the curated-provider TOML `extend-db` overlaid. A
    `[[errata.delivered]]` here would send the maintainer to a file
    whose loader refuses the entry."""
    db = write_flavored_db(
        tmp_path / "narrowed", t_kolumn_window("2019-01-01", "2019-12-31")
    )

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


@pytest.mark.parametrize(
    ("narrowing", "served"),
    [
        ('window = "2018-Q1"', ("2018-01-01", "2018-03-31")),
        ('member = { month = "2018-03" }', ("2018-03-01", "2018-03-31")),
    ],
)
def test_errata_worklist_judges_a_narrowed_column_on_its_own_period(
    tmp_path: Path, narrowing: str, served: tuple[str, str]
) -> None:
    """A column window (#1645) or a month member (#1495) is the column's own
    delivered period, so a 2018 table holding `T_kolumn` only there is covered by a
    catalog window of exactly that period. Fails if the diagnostic judges the
    mapping on the table's whole 2018 edition, which lists a false curated-window
    miss."""
    db = write_flavored_db(tmp_path / "narrowed", t_kolumn_window(*served))

    text = _errata_text(tmp_path, db, {2018: ("T_kolumn",)}, narrowing=narrowing)

    assert "T_kolumn:" not in text


@pytest.mark.parametrize("old_first", [True, False])
def test_errata_worklist_judges_each_windowed_variant_of_a_column_alone(
    tmp_path: Path, old_first: bool
) -> None:
    """One 2018 column mapped by two variants over disjoint windows (#1645): the
    old variant in Q1, which the catalog covers, and a new variant in Q4, which the
    catalog delivers only from November. October is a real gap of the new variant,
    reported once in either mapping order. Fails if the diagnostic judges the
    column by one mapping only, or lets the covered old window cover the new one."""
    db = write_flavored_db(
        tmp_path / "two-variants",
        t_kolumn_window("2018-01-01", "2018-03-31", variants=("_default",))
        + variant_block("ny")
        + owner_block(
            "t-kolumn",
            "T_kolumn",
            ("2018-11-01", None),
            variants=("ny",),
            name="T_kolumn",
        ),
    )
    register = _INERA.rpartition("/")[0]
    mappings = [
        f'register_variant = "{variant}"\nvariable = "{register}/t-kolumn"\n'
        f'representation = "T_kolumn"\nwindow = "{window}"'
        for variant, window in ((_INERA, "2018-Q1"), (f"{register}/ny", "2018-Q4"))
    ]
    steward_dir = tmp_path / "steward"
    steward_dir.mkdir()
    (steward_dir / "inventory.toml").write_text(
        'version = 1\nsteward = "swecov"\n\n[[table]]\nid = "T_2018"\n'
        'edition = 2018\n\n[[table.column]]\nname = "T_kolumn"\n'
        + "".join(
            f"\n[[table.column.mapping]]\n{mapping}\n"
            for mapping in (mappings if old_first else mappings[::-1])
        ),
        encoding="utf-8",
    )
    csv_path = tmp_path / "SWECOV_variables_2025-12-11.csv"
    build_catalog.cmd_errata(argparse.Namespace(csv=csv_path, db=db, out=steward_dir))
    text = (steward_dir / "derived" / "errata_worklist.toml").read_text(
        encoding="utf-8"
    )

    (line,) = [ln for ln in text.splitlines() if "T_kolumn:" in ln]
    assert line.startswith(f"# {register}/ny T_kolumn:")
    assert "held 2018-Q4, catalog windows 2018-11-01.." in line


@pytest.mark.parametrize("variant", [_INERA, "scb/bestallda-prover/_default"])
def test_errata_worklist_excludes_every_multi_period_suggestion(
    tmp_path: Path, variant: str, capsys: pytest.CaptureFixture[str]
) -> None:
    """Partial, absent and disjoint range/list holdings are not availability
    evidence. They produce no version, delivered-column or curated-window
    suggestion regardless of provider, and the worklist/CLI report an honest
    zero assessed denominator plus the skipped-table count."""
    narrowed = t_kolumn_window("2019-01-01", "2019-12-31")
    db = (
        _scb_db(tmp_path / "narrowed", narrowed)
        if variant.startswith("scb/")
        else write_flavored_db(tmp_path / "narrowed", narrowed)
    )

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
