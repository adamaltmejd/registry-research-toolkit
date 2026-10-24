"""The curated-provider steward overlay, driven through `extend_db` on a session base.

`reg-meta-build extend-db` reaches only the strict accepted-holdings path
(`test_holdings_steward_boundary.py`, `cases/holdings/cli`); the metadata-only
diagnostic overlay these cases pin is a library boundary: `extend_db`'s returned
counts, the artifact it writes, and its located `EXIT_CONFIG` refusals. The
provider-TOML refusals are in `test_extend_db_provider_refusals.py`.
"""

from __future__ import annotations

import shutil
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from _extend_db_support import (
    BANK as _BANK,
    BASE_BOOK_TERM as _BASE_BOOK_TERM,
    BASE_TOML as _BASE_TOML,
    STEWARD as _STEWARD,
    base_db_fixture,  # noqa: F401
    provider_ids as _ids,
    run as _run,
)
from reg_meta_build.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.extend_db import (
    extend_db,
)
from reg_meta_build.validate import validate_built_db


def test_core_graph_metadata_flags_open_dates_and_source_label(
    tmp_path: Path, base_db: Path
) -> None:
    # Fails if the overlay drops or renames a provider, register, variant or variable
    # field the provider TOML states, loses is_identifier/is_sensitive (disclosure),
    # or stops opening an undated state to 0001-01-01..9999-12-31.
    counts, out = _run(tmp_path, base_db)
    assert {
        key: counts[key]
        for key in ("providers", "registers", "variants", "variables", "states")
    } == {
        "providers": 1,
        "registers": 2,
        "variants": 3,
        "variables": 3,
        "states": 4,
    }
    conn = sqlite3.connect(out)
    ids = _ids()
    assert conn.execute(
        "SELECT provider_id, name FROM provider WHERE slug = ?", (_BANK,)
    ).fetchone() == (ids["provider"], "Swedbank AB")
    assert conn.execute(
        "SELECT name, purpose FROM register WHERE register_id = ?", (ids["register"],)
    ).fetchone() == ("Transaktioner", "Bank delivery")
    assert conn.execute(
        "SELECT name, description FROM register_variant WHERE register_variant_id = ?",
        (ids["variant"],),
    ).fetchone() == ("Transaktioner", "One table")
    assert conn.execute(
        "SELECT name, description, source_label FROM variable WHERE variable_id = ?",
        (ids["belopp"],),
    ).fetchone() == ("Belopp", "Transaktionsbelopp i SEK.", "swecov-inventory-test")
    assert conn.execute(
        "SELECT is_identifier, is_sensitive FROM variable WHERE variable_id = ?",
        (ids["kontonr"],),
    ).fetchone() == (1, 1)
    assert conn.execute(
        "SELECT valid_from, valid_to, data_type FROM variable_state WHERE variable_id = ?",
        (ids["belopp"],),
    ).fetchone() == ("0001-01-01", "9999-12-31", "float")
    assert conn.execute(
        "SELECT valid_from, valid_to FROM variable_state WHERE variable_id = ?",
        (ids["kontonr"],),
    ).fetchone() == ("2018-01-01", "9999-12-31")
    conn.close()


def test_register_scoped_variable_is_pooled_across_variants(
    tmp_path: Path, base_db: Path
) -> None:
    # Fails if one key declared per variant becomes one variable per variant
    # instead of one register variable with a state in each variant.
    _, out = _run(tmp_path, base_db)
    conn = sqlite3.connect(out)
    ids = _ids()
    assert conn.execute(
        "SELECT COUNT(*) FROM variable WHERE register_id = ?", (ids["pooled_register"],)
    ).fetchone() == (1,)
    assert set(
        conn.execute(
            "SELECT register_variant_id, delivery_column_name FROM variable_state "
            "WHERE variable_id = ?",
            (ids["personnr"],),
        )
    ) == {(ids["privat"], "PersonNr"), (ids["foretag"], "PersonNr")}
    conn.close()


def test_multistate_rename_preserves_one_variable(
    tmp_path: Path, base_db: Path
) -> None:
    # Fails if a column rename between states splits the variable or loses a
    # state's window.
    text = _BASE_TOML.replace(
        '    column = "BELOPP"\n    data_type = "float"',
        '    column = "BELOPP"\n    data_type = "float"\n'
        '    valid_from = "2018"\n    valid_to = "2020"\n\n'
        '    [[register.variable.state]]\n    column = "BELOPP_SEK"\n'
        '    data_type = "float"\n    valid_from = "2021"',
        1,
    )
    counts, out = _run(tmp_path, base_db, text)
    assert counts["variables"] == 3 and counts["states"] == 5
    conn = sqlite3.connect(out)
    assert conn.execute(
        "SELECT delivery_column_name, valid_from, valid_to FROM variable_state "
        "WHERE variable_id = ? ORDER BY valid_from",
        (_ids()["belopp"],),
    ).fetchall() == [
        ("BELOPP", "2018-01-01", "2020-12-31"),
        ("BELOPP_SEK", "2021-01-01", "9999-12-31"),
    ]
    conn.close()


def test_co_delivered_aliases_get_orderable_windows(
    tmp_path: Path, base_db: Path
) -> None:
    # Fails if a state's `aliases` stop becoming search aliases with the state's
    # own (open) window.
    text = _BASE_TOML.replace(
        '    data_type = "float"',
        '    data_type = "float"\n    aliases = ["BELOPP_SEK", "Belopp-SEK"]',
        1,
    )
    _, out = _run(tmp_path, base_db, text)
    conn = sqlite3.connect(out)
    var_id = _ids()["belopp"]
    assert {
        row[0]
        for row in conn.execute(
            "SELECT delivery_column_name FROM variable_alias WHERE variable_id = ?",
            (var_id,),
        )
    } == {"BELOPP", "BELOPP_SEK", "Belopp-SEK"}
    assert set(
        conn.execute(
            "SELECT delivery_column_name, valid_from, valid_to FROM variable_alias_window "
            "WHERE variable_id = ?",
            (var_id,),
        )
    ) == {
        ("BELOPP", "0001-01-01", "9999-12-31"),
        ("BELOPP_SEK", "0001-01-01", "9999-12-31"),
        ("Belopp-SEK", "0001-01-01", "9999-12-31"),
    }
    conn.close()


def test_steward_identity_inputs_and_slug_pins_bind(
    tmp_path: Path, base_db: Path
) -> None:
    # Fails if steward ids stop being minted from (provider, register, key) or the
    # pinned register and variant slugs stop binding to those ids.
    _, out = _run(tmp_path, base_db)
    conn = sqlite3.connect(out)
    ids = _ids()
    assert conn.execute(
        "SELECT slug FROM register WHERE register_id = ?", (ids["register"],)
    ).fetchone() == ("transaktioner",)
    assert conn.execute(
        "SELECT slug FROM register_variant WHERE register_variant_id = ?",
        (ids["variant"],),
    ).fetchone() == ("transaktioner-default",)
    assert conn.execute(
        "SELECT slug FROM variable WHERE variable_id = ?", (ids["belopp"],)
    ).fetchone() == ("belopp",)
    conn.close()


@pytest.mark.parametrize("steward_end", ["belopp", "saldo"])
def test_curated_same_as_onto_a_steward_variable_resolves_in_the_flavor(
    tmp_path: Path, base_db: Path, steward_end: str
) -> None:
    """A relations.toml same_as edge into a register the steward's slug dir pins
    (#1549) joins its variable to the base's `scb/utbildning/kon`, stored both
    ways; the global build skips such an edge. Its twin names a steward variable
    the flavor lacks and is refused as a missing catalog dependency. Fails if
    extend-db stops applying the steward's edges, or writes one whose endpoint
    does not resolve."""
    relations = tmp_path / "relations.toml"
    relations.write_text(
        '[[edge]]\ntype = "same_as"\n'
        f'a = "swedbank/transaktioner/{steward_end}"\nb = "scb/utbildning/kon"\n',
        encoding="utf-8",
    )
    if steward_end == "saldo":
        with pytest.raises(RegMetaError) as refused:
            _run(tmp_path, base_db, relations=relations)
        assert refused.value.code == "catalog_dependency_missing"
        assert "swedbank/transaktioner/saldo" in refused.value.message
        assert not (tmp_path / "out" / "reg_meta.db").exists()
        return
    result, out = _run(tmp_path, base_db, relations=relations)
    assert result["same_as_edges"] == 1
    with closing(sqlite3.connect(out)) as conn:
        assert sorted(conn.execute("SELECT * FROM variable_same_as")) == [
            ("scb", "utbildning", "kon", "swedbank", "transaktioner", "belopp"),
            ("swedbank", "transaktioner", "belopp", "scb", "utbildning", "kon"),
        ]


def test_new_provider_content_is_searchable(tmp_path: Path, base_db: Path) -> None:
    # Fails if the overlay stops indexing its own variables for search.
    _, out = _run(tmp_path, base_db)
    conn = sqlite3.connect(out)
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM variable_fts WHERE variable_fts MATCH ?",
            ('"Transaktionsbelopp"',),
        ).fetchone()[0]
        >= 1
    )
    conn.close()


def test_base_rows_and_base_file_are_not_clobbered(
    tmp_path: Path, base_db: Path
) -> None:
    # Fails if the overlay rewrites or deletes a base row, or writes the base file.
    # The base is read-only (mode 0444), as a released artifact often is, so this
    # also fails if the staging copy keeps the base's mode (`shutil.copy2`) and
    # SQLite cannot open it for writing.
    released = tmp_path / "released"
    released.mkdir()
    base_db = Path(shutil.copyfile(base_db, released / base_db.name))
    base_db.chmod(0o444)
    before_bytes = base_db.read_bytes()
    tables = (
        "provider",
        "register",
        "register_variant",
        "variable",
        "variable_state",
        "variable_alias",
        "variable_alias_window",
    )
    base_conn = sqlite3.connect(base_db)
    before = {
        table: set(base_conn.execute(f"SELECT * FROM {table}")) for table in tables
    }
    base_conn.close()
    # A killed earlier run leaves its staging tmp and an uncheckpointed WAL. Fails
    # if extend-db removes only the stale tmp: SQLite replays the leftover WAL onto
    # the fresh, digest-checked base copy, rewriting a base row (#1697).
    stale = tmp_path / "out" / "reg_meta.db.tmp"
    stale.parent.mkdir()
    shutil.copyfile(base_db, stale)
    captured_wal = tmp_path / "captured-wal"
    with closing(sqlite3.connect(stale)) as stale_conn:
        stale_conn.execute("PRAGMA journal_mode=WAL")
        stale_conn.execute("PRAGMA wal_autocheckpoint=0")
        stale_conn.execute("UPDATE provider SET name = name || ' (stale)'")
        stale_conn.commit()
        shutil.copyfile(f"{stale}-wal", captured_wal)
    shutil.copyfile(captured_wal, f"{stale}-wal")
    _, out = _run(tmp_path, base_db)
    flavored_conn = sqlite3.connect(out)
    after = {
        table: set(flavored_conn.execute(f"SELECT * FROM {table}")) for table in tables
    }
    flavored_conn.close()
    assert all(before[table] <= after[table] for table in tables)
    assert base_db.read_bytes() == before_bytes


def test_overlay_keeps_base_classifications_searchable_and_reanalyzes(
    tmp_path: Path, base_db: Path
) -> None:
    # Fails if the overlay drops the base's classification index or ships the
    # base's statistics (no ANALYZE after the overlay rows).
    _, out = _run(tmp_path, base_db)
    with closing(sqlite3.connect(out)) as conn:
        hits = conn.execute(
            "SELECT COUNT(*) FROM classification_fts WHERE classification_fts MATCH ?",
            (f'"{_BASE_BOOK_TERM}"',),
        ).fetchone()[0]
        (variables,) = conn.execute("SELECT COUNT(*) FROM variable").fetchone()
        stats = [
            int(stat.split()[0])
            for (stat,) in conn.execute(
                "SELECT stat FROM sqlite_stat1 WHERE tbl = 'variable'"
            )
        ]
    assert hits == 1
    # The overlay's statistics count its own rows, not the base's.
    assert stats and set(stats) == {variables}
    assert validate_built_db(out, flavored=True, corpus=False).passed


def _seeded_provider_run(tmp_path: Path, base_db: Path, name: str) -> dict:
    return _run(
        tmp_path,
        base_db,
        _BASE_TOML.replace('name = "Swedbank AB"', f'name = "{name}"', 1),
        provider="fk",
    )[0]


def test_provider_matching_base_name_is_reused_through_extend_db(
    tmp_path: Path, base_db: Path
) -> None:
    # Fails if a provider the base already holds under the same name is inserted
    # again or its base row changes.
    with closing(sqlite3.connect(base_db)) as conn:
        before = conn.execute("SELECT * FROM provider ORDER BY provider_id").fetchall()
    result = _seeded_provider_run(tmp_path, base_db, "Försäkringskassan")
    assert result["providers"] == 0
    with closing(sqlite3.connect(tmp_path / "out" / "reg_meta.db")) as conn:
        assert (
            conn.execute("SELECT * FROM provider ORDER BY provider_id").fetchall()
            == before
        )


def test_provider_name_conflicting_with_base_is_rejected_through_extend_db(
    tmp_path: Path, base_db: Path
) -> None:
    # Fails if a provider TOML naming a base provider differently is accepted.
    with pytest.raises(RegMetaError) as exc:
        _seeded_provider_run(tmp_path, base_db, "Wrong")
    assert exc.value.exit_code == EXIT_CONFIG
    assert exc.value.code == "extend_providers_invalid"
    assert "'fk' already has name 'Försäkringskassan'" in exc.value.message
    assert "'Wrong'" in exc.value.message
    assert not (tmp_path / "out" / "reg_meta.db").exists()
    assert not (tmp_path / "out" / "reg_meta.db.tmp").exists()


def test_missing_or_empty_provider_directory_is_exit_config_through_extend_db(
    tmp_path: Path, base_db: Path
) -> None:
    # Fails if a missing or empty providers directory builds an overlay, or the
    # refusal stops naming the directory.
    empty = tmp_path / "empty"
    empty.mkdir()
    for providers, code, locator in (
        (tmp_path / "missing", "extend_providers_dir_not_found", "not found"),
        (empty, "extend_providers_invalid", "has no provider TOMLs"),
    ):
        with pytest.raises(RegMetaError) as exc:
            extend_db(
                base_db=base_db,
                providers_dir=providers,
                db_dir=tmp_path / "out",
                steward=_STEWARD,
                diagnostic=True,
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == code
        assert locator in exc.value.message
        assert str(providers) in exc.value.message
        assert not (tmp_path / "out" / "reg_meta.db").exists()


def test_diagnostic_extension_names_its_base_generation_and_claims_none(
    tmp_path: Path, base_db: Path
) -> None:
    # Fails if a diagnostic overlay keeps the base's generation_id or
    # builder_commit (passing for a publishable generation), or stops naming the
    # generation of the base it extends.
    with closing(sqlite3.connect(base_db)) as conn:
        base = dict(conn.execute("SELECT key, value FROM import_manifest"))
    assert {"generation_id", "builder_commit"} <= base.keys()
    _, output = _run(tmp_path, base_db)
    with closing(sqlite3.connect(output)) as conn:
        manifest = dict(conn.execute("SELECT key, value FROM import_manifest"))
    assert {
        key: manifest.get(key)
        for key in (
            "catalog_artifact_kind",
            "catalog_publishable",
            "catalog_completeness",
            "base_generation_id",
        )
    } == {
        "catalog_artifact_kind": "diagnostic",
        "catalog_publishable": "false",
        "catalog_completeness": "incomplete",
        "base_generation_id": base["generation_id"],
    }
    assert {"generation_id", "builder_commit"}.isdisjoint(manifest)
