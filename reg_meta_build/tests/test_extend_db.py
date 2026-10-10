"""The curated-provider steward overlay, driven through `extend_db` on a session base.

`reg-meta-build extend-db` reaches only the strict accepted-holdings path
(`test_holdings_steward_boundary.py`, `cases/holdings/cli`); the metadata-only
diagnostic overlay these cases pin is a library boundary: `extend_db`'s returned
counts, the artifact it writes, and its located `EXIT_CONFIG` refusals.
"""

from __future__ import annotations

import shutil
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from reader_artifacts import FIXTURE_IMPORT_DATE, build_reader_artifact
from reg_meta_build.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.extend_db import (
    extend_db,
)
from reg_meta_build.id import mint
from reg_meta_build.validate import validate_built_db

_STEWARD = "swecov"
_BANK = "swedbank"

_BASE_TOML = """\
[provider]
name = "Swedbank AB"
source_label = "swecov-inventory-test"

[[register]]
key = "transaktioner"
name = "Transaktioner"
purpose = "Bank delivery"
description = "Bankkontotransaktioner."

  [[register.variant]]
  key = "_default"
  name = "Transaktioner"
  description = "One table"

  [[register.variable]]
  key = "belopp"
  name = "Belopp"
  description = "Transaktionsbelopp i SEK."
  variants = ["_default"]

    [[register.variable.state]]
    column = "BELOPP"
    data_type = "float"

  [[register.variable]]
  key = "kontonr"
  name = "Kontonummer"
  is_identifier = true
  is_sensitive = true
  variants = ["_default"]

    [[register.variable.state]]
    column = "KONTO"
    data_type = "varchar"
    valid_from = "2018"

[[register]]
key = "kunder"
name = "Kunder"

  [[register.variant]]
  key = "privat"
  name = "Privatkunder"

  [[register.variant]]
  key = "foretag"
  name = "Företagskunder"

  [[register.variable]]
  key = "personnr"
  name = "Personnummer"
  is_identifier = true
  is_sensitive = true
  variants = ["privat"]

    [[register.variable.state]]
    column = "PersonNr"

  [[register.variable]]
  key = "personnr"
  name = "Personnummer"
  is_identifier = true
  is_sensitive = true
  variants = ["foretag"]

    [[register.variable.state]]
    column = "PersonNr"
"""


def _ids() -> dict[str, int]:
    return {
        "provider": mint("provider", _BANK),
        "register": mint("register", _BANK, "transaktioner"),
        "variant": mint("variant", _BANK, "transaktioner", "_default"),
        "belopp": mint("variable", _BANK, "transaktioner", "belopp"),
        "kontonr": mint("variable", _BANK, "transaktioner", "kontonr"),
        "pooled_register": mint("register", _BANK, "kunder"),
        "privat": mint("variant", _BANK, "kunder", "privat"),
        "foretag": mint("variant", _BANK, "kunder", "foretag"),
        "personnr": mint("variable", _BANK, "kunder", "personnr"),
    }


def _providers(
    tmp_path: Path, text: str = _BASE_TOML, *, name: str = "providers"
) -> Path:
    directory = tmp_path / name
    directory.mkdir()
    (directory / f"{_BANK}.toml").write_text(text, encoding="utf-8")
    return directory


def _write_slug_dir(path: Path) -> None:
    ids = _ids()
    path.mkdir()
    (path / ".snapshot.json").write_text("{}\n", encoding="utf-8")
    (path / f"{_BANK}.toml").write_text(
        f'[register."{ids["register"]}"]\nslug = "transaktioner"\n'
        f'[register_variant."{ids["register"]}.{ids["variant"]}"]\n'
        'slug = "transaktioner-default"\n'
        f'[register."{ids["pooled_register"]}"]\nslug = "kunder"\n'
        f'[register_variant."{ids["pooled_register"]}.{ids["privat"]}"]\n'
        'slug = "kunder-privat"\n'
        f'[register_variant."{ids["pooled_register"]}.{ids["foretag"]}"]\n'
        'slug = "kunder-foretag"\n',
        encoding="utf-8",
    )


# The session base: a published-shape catalog from the conformance reader fixtures,
# one that carries a classification book (`classifications.json` there: SUN2020,
# "Svensk utbildningsnomenklatur"). The cache stores it read-only; each worker
# overlays a private copy so no test can touch the cached bytes.
_BASE_FIXTURE = "reader/classification-editions"
# A word of exactly one base book's name (SUN2020's, in the fixture's
# classifications.json).
_BASE_BOOK_TERM = "utbildningsnomenklatur"


@pytest.fixture(scope="session")
def base_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return build_reader_artifact(
        tmp_path_factory.mktemp("base"),
        _BASE_FIXTURE,
        "catalog",
        identity_overrides={"import_date": FIXTURE_IMPORT_DATE},
    )


def _run(
    tmp_path: Path,
    base_db: Path,
    text: str = _BASE_TOML,
    *,
    skip_slugs: bool = False,
) -> tuple[dict, Path]:
    providers_dir = _providers(tmp_path, text, name="out-providers")
    slug_dir = None
    if not skip_slugs:
        slug_dir = tmp_path / "out-slugs"
        _write_slug_dir(slug_dir)
    out = tmp_path / "out"
    result = extend_db(
        base_db=base_db,
        providers_dir=providers_dir,
        db_dir=out,
        steward=_STEWARD,
        slug_dir=slug_dir,
        skip_slugs=skip_slugs,
        diagnostic=True,
    )
    return result, out / "reg_meta.db"


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


def _assert_rejected_without_output(
    tmp_path: Path, base_db: Path, text: str, *, code: str = "curated_toml_invalid"
) -> RegMetaError:
    with pytest.raises(RegMetaError) as exc:
        _run(tmp_path, base_db, text)
    assert exc.value.exit_code == EXIT_CONFIG
    assert exc.value.code == code
    assert not (tmp_path / "out" / "reg_meta.db").exists()
    assert not (tmp_path / "out" / "reg_meta.db.tmp").exists()
    return exc.value


@pytest.mark.parametrize(
    "old, new, locator",
    [
        (
            'purpose = "Bank delivery"',
            'purpose = "Bank delivery"\nunexpected = true',
            "register 'transaktioner': unknown key(s) ['unexpected']",
        ),
        (
            'valid_from = "2018"',
            'valid_from = "not-a-date"',
            "state[0] valid_from: 'not-a-date' is not a valid ISO period",
        ),
        (
            'column = "BELOPP"',
            'column = "BELOPP"\naliases = ["BELOPP"]',
            "state[0] repeats a delivery column",
        ),
    ],
)
def test_malformed_provider_toml_is_exit_config_through_extend_db(
    tmp_path: Path, base_db: Path, old: str, new: str, locator: str
) -> None:
    # Fails if a malformed provider TOML is accepted, refused under another code,
    # or leaves an output or staging file.
    error = _assert_rejected_without_output(
        tmp_path, base_db, _BASE_TOML.replace(old, new, 1)
    )
    assert locator in error.message


_PRIVATE = '[provider]\nname = "Private"\nsource_label = "test"\n\n'


@pytest.mark.parametrize(
    "text, locator",
    [
        ("register = [1]\n\n" + _PRIVATE, "[[register]]"),
        (
            _PRIVATE + '[[register]]\nkey = "r"\nname = "R"\nvariant = [1]\n'
            '[[register.variable]]\nkey = "v"\nname = "V"\n'
            '[[register.variable.state]]\ncolumn = "C"\n',
            "variant",
        ),
        (
            _PRIVATE + '[[register]]\nkey = "r"\nname = "R"\nvariable = [1]\n',
            "[[register.variable]]",
        ),
    ],
    ids=["register", "variant", "variable"],
)
def test_table_array_of_non_tables_is_exit_config_through_extend_db(
    tmp_path: Path, base_db: Path, text: str, locator: str
) -> None:
    # Fails if a `register`, `variant` or `variable` array whose element is not a
    # table is accepted, crashes, or is refused under another code or without
    # naming the table array. Read from test_curated_adapter.py::
    # test_steward_table_arrays_reject_non_table_elements.
    error = _assert_rejected_without_output(tmp_path, base_db, text)
    assert locator in error.message


@pytest.mark.parametrize(
    "book, code, fragment",
    [
        (
            "ATC",
            "curated_toml_invalid",
            "classification 'ATC' is not a declared classification",
        ),
        ("SUN2020", "extend_providers_invalid", "declares classification linkage"),
    ],
    ids=["checkout-book", "base-book"],
)
def test_classification_reference_resolves_against_the_base_books(
    tmp_path: Path, base_db: Path, book: str, code: str, fragment: str
) -> None:
    # A steward variable's `classification` resolves against the books of the
    # selected base, never this checkout's: ATC is a checkout book
    # (curation/classifications/ATC.toml) the base lacks, SUN2020 is the base's
    # book. The base book passes resolution, and the overlay then refuses the
    # linkage it does not materialize (extend_db.py, `_load_provider_ir`), so no
    # build reaches the old adapter test's accepted reference. Fails if references
    # resolve against the checkout's books (ATC is refused as linkage instead), or
    # not against the base's (SUN2020 is refused as undeclared), or if the overlay
    # writes the linkage. Read from test_curated_adapter.py::
    # test_classification_reference_uses_supplied_books.
    text = _BASE_TOML.replace(
        '  variants = ["_default"]\n',
        f'  classification = "{book}"\n  variants = ["_default"]\n',
        1,
    )
    error = _assert_rejected_without_output(tmp_path, base_db, text, code=code)
    assert fragment in error.message


def test_duplicate_state_key_is_rejected_through_extend_db(
    tmp_path: Path, base_db: Path
) -> None:
    # Fails if two states of one variable starting the same period are accepted.
    text = _BASE_TOML.replace(
        '    column = "BELOPP"\n    data_type = "float"',
        '    column = "BELOPP"\n    data_type = "float"\n    valid_from = "2020"\n\n'
        '    [[register.variable.state]]\n    column = "BELOPP_SEK"\n'
        '    data_type = "float"\n    valid_from = "2020"',
        1,
    )
    error = _assert_rejected_without_output(tmp_path, base_db, text)
    assert "duplicate state key" in error.message


@pytest.mark.parametrize("explicit_start", ["0001", "0001-01", "0001-01-01"])
def test_year_one_state_start_is_rejected_through_extend_db(
    tmp_path: Path, base_db: Path, explicit_start: str
) -> None:
    # Fails if an explicit year-one start, which reads as the open-start sentinel,
    # is accepted in any of its spellings.
    text = _BASE_TOML.replace(
        '    column = "BELOPP"\n    data_type = "float"',
        '    column = "BELOPP"\n    data_type = "float"\n\n'
        '    [[register.variable.state]]\n    column = "BELOPP_SEK"\n'
        f'    data_type = "float"\n    valid_from = "{explicit_start}"',
        1,
    )
    error = _assert_rejected_without_output(tmp_path, base_db, text)
    assert f"valid_from: {explicit_start!r} is not a valid ISO period" in error.message


def test_inverted_register_window_is_rejected_through_extend_db(
    tmp_path: Path, base_db: Path
) -> None:
    # Fails if an inverted register window is accepted because a valid variable
    # window overrides it.
    text = _BASE_TOML.replace(
        'purpose = "Bank delivery"',
        'purpose = "Bank delivery"\nvalid_from = "2022"\nvalid_to = "2020"',
        1,
    ).replace(
        '  variants = ["_default"]',
        '  variants = ["_default"]\n  valid_from = "2010"\n  valid_to = "2030"',
        1,
    )
    error = _assert_rejected_without_output(tmp_path, base_db, text)
    assert "register 'transaktioner' has an inverted validity window" in error.message
    assert "2022-01-01 > 2020-12-31" in error.message


def test_inverted_variable_window_is_rejected_through_extend_db(
    tmp_path: Path, base_db: Path
) -> None:
    # Fails if an inverted variable window is accepted because a valid state
    # window overrides it.
    text = _BASE_TOML.replace(
        '  variants = ["_default"]',
        '  variants = ["_default"]\n  valid_from = "2022"\n  valid_to = "2020"',
        1,
    ).replace(
        '    column = "BELOPP"\n    data_type = "float"',
        '    column = "BELOPP"\n    data_type = "float"\n'
        '    valid_from = "2010"\n    valid_to = "2030"',
        1,
    )
    error = _assert_rejected_without_output(tmp_path, base_db, text)
    assert "variable 'Belopp' has an inverted validity window" in error.message
    assert "2022-01-01 > 2020-12-31" in error.message


def test_valid_child_window_overrides_register_window_through_extend_db(
    tmp_path: Path, base_db: Path
) -> None:
    # Fails if a valid variable window stops overriding the register's (the
    # allowed twin of the inverted-register refusal).
    text = _BASE_TOML.replace(
        'purpose = "Bank delivery"',
        'purpose = "Bank delivery"\nvalid_from = "2018"\nvalid_to = "2020"',
        1,
    ).replace(
        '  variants = ["_default"]',
        '  variants = ["_default"]\n  valid_from = "2010"\n  valid_to = "2030"',
        1,
    )
    _, out = _run(tmp_path, base_db, text)
    with closing(sqlite3.connect(out)) as conn:
        assert conn.execute(
            "SELECT valid_from, valid_to FROM variable_state "
            "WHERE delivery_column_name = 'BELOPP'"
        ).fetchall() == [("2010-01-01", "2030-12-31")]


def test_inverted_state_window_is_rejected_through_extend_db(
    tmp_path: Path, base_db: Path
) -> None:
    # Fails if a state whose start follows its end is accepted.
    text = _BASE_TOML.replace(
        '    column = "BELOPP"\n    data_type = "float"',
        '    column = "BELOPP"\n    data_type = "float"\n'
        '    valid_from = "2021"\n    valid_to = "2020"',
        1,
    )
    error = _assert_rejected_without_output(tmp_path, base_db, text)
    assert "state[0] has an inverted validity window" in error.message


def test_variable_key_cannot_repeat_within_one_variant_through_extend_db(
    tmp_path: Path, base_db: Path
) -> None:
    # Fails if a variable key declared twice for one variant is accepted.
    needle = """\
    [[register.variable.state]]
    column = "BELOPP"
    data_type = "float"
"""
    repeated = (
        needle
        + """\

  [[register.variable]]
  key = "belopp"
  name = "Belopp"
  description = "Transaktionsbelopp i SEK."
  variants = ["_default"]

    [[register.variable.state]]
    column = "BELOPP_SEK"
    valid_from = "2021"
"""
    )
    error = _assert_rejected_without_output(
        tmp_path, base_db, _BASE_TOML.replace(needle, repeated)
    )
    assert "repeats variable" in error.message


def test_pooled_variable_metadata_disagreement_is_rejected_through_extend_db(
    tmp_path: Path, base_db: Path
) -> None:
    # Fails if per-variant declarations of one pooled variable that disagree on
    # `name` are accepted (one of them silently wins).
    head, separator, tail = _BASE_TOML.rpartition('name = "Personnummer"')
    text = head + separator.replace("Personnummer", "Other") + tail
    error = _assert_rejected_without_output(tmp_path, base_db, text)
    assert "different `name`" in error.message


def _seeded_provider_run(tmp_path: Path, base_db: Path, name: str) -> dict:
    providers = tmp_path / "fk-providers"
    providers.mkdir()
    (providers / "fk.toml").write_text(
        _BASE_TOML.replace('name = "Swedbank AB"', f'name = "{name}"', 1),
        encoding="utf-8",
    )
    return extend_db(
        base_db=base_db,
        providers_dir=providers,
        db_dir=tmp_path / "out",
        steward=_STEWARD,
        skip_slugs=True,
        diagnostic=True,
    )


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
                skip_slugs=True,
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
