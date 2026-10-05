"""Synthetic proof for the curated-provider steward overlay."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest
from catalog_manifest import synthetic_manifest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.extend_db import (
    _insert_providers,
    _load_provider_ir,
    extend_db,
    resolve_steward_providers_dir,
)
from reg_meta_build.id import _MINT_BIT, mint
from reg_meta_build.resolved_catalog import (
    ResolvedCodeSet,
    ResolvedRegister,
    ResolvedState,
    ResolvedVariable,
    ResolvedVariant,
    write_resolved_catalog,
)
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


@pytest.fixture()
def global_db(tmp_path: Path) -> Path:
    output = tmp_path / "global" / "reg_meta.db"
    write_resolved_catalog(
        (
            ResolvedVariable(
                register=ResolvedRegister(
                    provider="scb", slug="testreg", name="Test register"
                ),
                slug="category",
                provider_key="44",
                name="Category",
                definition=None,
                description=None,
                operational_definition=None,
                measurement_unit=None,
                is_identifier=False,
                is_sensitive=False,
                states=(
                    ResolvedState(
                        variant=ResolvedVariant(slug="individuals", name="Individuals"),
                        valid_from="2020-01-01",
                        valid_to="2020-12-31",
                        delivery_column_name="Category",
                        data_type="integer",
                        data_length="1",
                        operational_definition=None,
                        provenance=None,
                        value_set=ResolvedCodeSet(members=(("1", "One"), ("2", "Two"))),
                    ),
                ),
            ),
        ),
        output,
        manifest=synthetic_manifest(),
    )
    return output


def _run(
    tmp_path: Path,
    base_db: Path,
    text: str = _BASE_TOML,
    *,
    name: str = "out",
    skip_slugs: bool = False,
    pre_rename_hook=None,
) -> tuple[dict, Path]:
    providers_dir = _providers(tmp_path, text, name=f"{name}-providers")
    slug_dir = None
    if not skip_slugs:
        slug_dir = tmp_path / f"{name}-slugs"
        _write_slug_dir(slug_dir)
    out = tmp_path / name
    result = extend_db(
        base_db=base_db,
        providers_dir=providers_dir,
        db_dir=out,
        steward=_STEWARD,
        slug_dir=slug_dir,
        skip_slugs=skip_slugs,
        pre_rename_hook=pre_rename_hook,
        diagnostic=True,
    )
    return result, out / "reg_meta.db"


def test_core_graph_metadata_flags_open_dates_and_source_label(
    tmp_path: Path, global_db: Path
) -> None:
    counts, out = _run(tmp_path, global_db)
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
    tmp_path: Path, global_db: Path
) -> None:
    _, out = _run(tmp_path, global_db)
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
    tmp_path: Path, global_db: Path
) -> None:
    text = _BASE_TOML.replace(
        '    column = "BELOPP"\n    data_type = "float"',
        '    column = "BELOPP"\n    data_type = "float"\n'
        '    valid_from = "2018"\n    valid_to = "2020"\n\n'
        '    [[register.variable.state]]\n    column = "BELOPP_SEK"\n'
        '    data_type = "float"\n    valid_from = "2021"',
        1,
    )
    counts, out = _run(tmp_path, global_db, text)
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
    tmp_path: Path, global_db: Path
) -> None:
    text = _BASE_TOML.replace(
        '    data_type = "float"',
        '    data_type = "float"\n    aliases = ["BELOPP_SEK", "Belopp-SEK"]',
        1,
    )
    _, out = _run(tmp_path, global_db, text)
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


@pytest.mark.parametrize(
    "old, new",
    [
        ('purpose = "Bank delivery"', 'purpose = "Bank delivery"\nunexpected = true'),
        ('valid_from = "2018"', 'valid_from = "not-a-date"'),
        ('column = "BELOPP"', 'column = "BELOPP"\naliases = ["BELOPP"]'),
    ],
)
def test_malformed_provider_toml_is_exit_config(
    tmp_path: Path, old: str, new: str
) -> None:
    text = _BASE_TOML.replace(old, new, 1)
    with pytest.raises(RegMetaError) as exc:
        _load_provider_ir(_providers(tmp_path, text), _STEWARD)
    assert exc.value.exit_code == EXIT_CONFIG


def test_duplicate_state_key_is_rejected(tmp_path: Path) -> None:
    text = _BASE_TOML.replace(
        '    column = "BELOPP"\n    data_type = "float"',
        '    column = "BELOPP"\n    data_type = "float"\n    valid_from = "2020"\n\n'
        '    [[register.variable.state]]\n    column = "BELOPP_SEK"\n'
        '    data_type = "float"\n    valid_from = "2020"',
        1,
    )
    with pytest.raises(RegMetaError) as exc:
        _load_provider_ir(_providers(tmp_path, text), _STEWARD)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "duplicate state key" in exc.value.message


@pytest.mark.parametrize("explicit_start", ["0001", "0001-01", "0001-01-01"])
def test_year_one_state_start_is_rejected_at_boundary(
    tmp_path: Path, explicit_start: str
) -> None:
    text = _BASE_TOML.replace(
        '    column = "BELOPP"\n    data_type = "float"',
        '    column = "BELOPP"\n    data_type = "float"\n\n'
        '    [[register.variable.state]]\n    column = "BELOPP_SEK"\n'
        f'    data_type = "float"\n    valid_from = "{explicit_start}"',
        1,
    )
    with pytest.raises(RegMetaError) as exc:
        _load_provider_ir(_providers(tmp_path, text), _STEWARD)
    assert exc.value.exit_code == EXIT_CONFIG
    assert (
        f"valid_from: {explicit_start!r} is not a valid ISO period" in exc.value.message
    )


def test_inverted_register_window_is_rejected_before_variable_override(
    tmp_path: Path,
) -> None:
    text = _BASE_TOML.replace(
        'purpose = "Bank delivery"',
        'purpose = "Bank delivery"\nvalid_from = "2022"\nvalid_to = "2020"',
        1,
    ).replace(
        '  variants = ["_default"]',
        '  variants = ["_default"]\n  valid_from = "2010"\n  valid_to = "2030"',
        1,
    )
    with pytest.raises(RegMetaError) as exc:
        _load_provider_ir(_providers(tmp_path, text), _STEWARD)
    assert exc.value.exit_code == EXIT_CONFIG
    assert (
        "register 'transaktioner' has an inverted validity window" in exc.value.message
    )
    assert "2022-01-01 > 2020-12-31" in exc.value.message


def test_inverted_variable_window_is_rejected_before_state_override(
    tmp_path: Path,
) -> None:
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
    with pytest.raises(RegMetaError) as exc:
        _load_provider_ir(_providers(tmp_path, text), _STEWARD)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "variable 'Belopp' has an inverted validity window" in exc.value.message
    assert "2022-01-01 > 2020-12-31" in exc.value.message


def test_valid_child_window_still_overrides_register_window(tmp_path: Path) -> None:
    text = _BASE_TOML.replace(
        'purpose = "Bank delivery"',
        'purpose = "Bank delivery"\nvalid_from = "2018"\nvalid_to = "2020"',
        1,
    ).replace(
        '  variants = ["_default"]',
        '  variants = ["_default"]\n  valid_from = "2010"\n  valid_to = "2030"',
        1,
    )
    graph = _load_provider_ir(_providers(tmp_path, text), _STEWARD)
    state = next(s for s in graph.states if s.delivery_column_name == "BELOPP")
    assert (state.valid_from, state.valid_to) == ("2010-01-01", "2030-12-31")


def test_inverted_state_window_is_rejected(tmp_path: Path) -> None:
    text = _BASE_TOML.replace(
        '    column = "BELOPP"\n    data_type = "float"',
        '    column = "BELOPP"\n    data_type = "float"\n'
        '    valid_from = "2021"\n    valid_to = "2020"',
        1,
    )
    with pytest.raises(RegMetaError) as exc:
        _load_provider_ir(_providers(tmp_path, text), _STEWARD)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "state[0] has an inverted validity window" in exc.value.message


def test_variable_key_cannot_repeat_within_one_variant(tmp_path: Path) -> None:
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
    with pytest.raises(RegMetaError) as exc:
        _load_provider_ir(
            _providers(tmp_path, _BASE_TOML.replace(needle, repeated)), _STEWARD
        )
    assert exc.value.exit_code == EXIT_CONFIG
    assert "repeats variable" in exc.value.message


def test_pooled_variable_metadata_disagreement_is_rejected(tmp_path: Path) -> None:
    head, separator, tail = _BASE_TOML.rpartition('name = "Personnummer"')
    text = head + separator.replace("Personnummer", "Other") + tail
    with pytest.raises(RegMetaError) as exc:
        _load_provider_ir(_providers(tmp_path, text), _STEWARD)
    assert exc.value.exit_code == EXIT_CONFIG
    assert "different `name`" in exc.value.message


def test_steward_identity_inputs_and_slug_pins_bind(
    tmp_path: Path, global_db: Path
) -> None:
    _, out = _run(tmp_path, global_db)
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
    assert all(value >= _MINT_BIT for value in ids.values())
    conn.close()


def test_new_provider_content_is_searchable(tmp_path: Path, global_db: Path) -> None:
    _, out = _run(tmp_path, global_db)
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
    tmp_path: Path, global_db: Path
) -> None:
    before_bytes = global_db.read_bytes()
    tables = (
        "provider",
        "register",
        "register_variant",
        "variable",
        "variable_state",
        "variable_alias",
        "variable_alias_window",
    )
    base_conn = sqlite3.connect(global_db)
    before = {
        table: set(base_conn.execute(f"SELECT * FROM {table}")) for table in tables
    }
    base_conn.close()
    _, out = _run(tmp_path, global_db)
    flavored_conn = sqlite3.connect(out)
    after = {
        table: set(flavored_conn.execute(f"SELECT * FROM {table}")) for table in tables
    }
    flavored_conn.close()
    assert all(before[table] <= after[table] for table in tables)
    assert global_db.read_bytes() == before_bytes


def test_fresh_runs_are_deterministic(tmp_path: Path, global_db: Path) -> None:
    first, first_db = _run(tmp_path, global_db, name="first")
    second, second_db = _run(tmp_path, global_db, name="second")
    assert {key: value for key, value in first.items() if key != "db_path"} == {
        key: value for key, value in second.items() if key != "db_path"
    }
    query = (
        "SELECT v.register_id, vs.register_variant_id, v.variable_id, vs.state_id "
        "FROM variable_state vs JOIN variable v USING (variable_id) "
        "JOIN register_variant rv USING (register_variant_id) "
        "WHERE v.variable_id >= ? ORDER BY vs.state_id"
    )
    assert (
        sqlite3.connect(first_db).execute(query, (_MINT_BIT,)).fetchall()
        == sqlite3.connect(second_db).execute(query, (_MINT_BIT,)).fetchall()
    )


def test_flavored_db_validates(tmp_path: Path, global_db: Path) -> None:
    _, out = _run(tmp_path, global_db)
    result = validate_built_db(out, flavored=True, corpus=False)
    assert not result.failures, result.failures


def test_provider_insert_is_idempotent_and_name_safe() -> None:
    from reg_meta_build.db import DDL, seed_providers

    conn = sqlite3.connect(":memory:")
    conn.executescript(DDL)
    seed_providers(conn)
    name = conn.execute("SELECT name FROM provider WHERE slug = 'scb'").fetchone()[0]
    assert _insert_providers(conn, (("scb", name),)) == 0
    with pytest.raises(RegMetaError):
        _insert_providers(conn, (("scb", "Wrong"),))


def test_missing_or_empty_provider_directory_is_exit_config(tmp_path: Path) -> None:
    with pytest.raises(RegMetaError) as exc:
        resolve_steward_providers_dir(tmp_path / "missing", _STEWARD)
    assert exc.value.exit_code == EXIT_CONFIG
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(RegMetaError) as exc:
        _load_provider_ir(empty, _STEWARD)
    assert exc.value.exit_code == EXIT_CONFIG


def test_hook_failure_discards_staging_db(tmp_path: Path, global_db: Path) -> None:
    class HookError(RuntimeError):
        pass

    def fail(_path: Path) -> None:
        raise HookError

    with pytest.raises(HookError):
        _run(tmp_path, global_db, skip_slugs=True, pre_rename_hook=fail)
    assert not (tmp_path / "out" / "reg_meta.db").exists()
    assert not (tmp_path / "out" / "reg_meta.db.tmp").exists()


def test_extension_writes_register_warnings_using_actual_private_ids(
    tmp_path: Path, global_db: Path
) -> None:
    from reg_meta_build.db import open_built_db

    warning = _fixture_warning()
    providers = _providers(tmp_path, _BASE_TOML)
    slugs = tmp_path / "slugs"
    _write_slug_dir(slugs)
    out = tmp_path / "out"
    extend_db(
        global_db,
        providers,
        out,
        steward=_STEWARD,
        slug_dir=slugs,
        data_warnings=(warning,),
        diagnostic=True,
    )
    conn = open_built_db(out / "reg_meta.db")
    try:
        row = conn.execute(
            "SELECT register_id, variable_id, register_variant_id, delivery_column_name, valid_from, valid_to FROM data_warning"
        ).fetchone()
        assert tuple(row) == (_ids()["register"], None, None, None, None, None)
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    finally:
        conn.close()


def test_warning_writer_rejects_missing_register_and_demotes_only_explicitly(
    tmp_path: Path, global_db: Path
) -> None:
    import json

    from reg_meta.catalog import DataWarning
    from reg_meta.source_evidence import canonical_sha256
    from reg_meta_build.data_warnings import write_data_warnings

    warning = _fixture_warning()
    conn = sqlite3.connect(global_db)
    try:
        with pytest.raises(ValueError, match="register is not written"):
            write_data_warnings(conn, (warning,))
        provider, register = conn.execute(
            "SELECT p.slug,r.slug FROM register r JOIN provider p USING(provider_id) LIMIT 1"
        ).fetchone()
        payload = warning.model_dump(mode="json", exclude={"warning_id"})
        payload.update(
            register_fqid=f"{provider}/{register}",
            variable_fqid=f"{provider}/{register}/unwritten",
            variant="_default",
            delivery_column_name="X",
        )
        missing = DataWarning.model_validate_json(
            json.dumps({"warning_id": canonical_sha256(payload), **payload})
        )
        with pytest.raises(ValueError, match="variable is not written"):
            write_data_warnings(conn, (missing,))
        write_data_warnings(conn, (missing,), demote_missing_variables=True)
        raw = conn.execute("SELECT warning_json FROM data_warning").fetchone()[0]
        demoted = DataWarning.model_validate_json(raw)
        assert demoted.variable_fqid is None and demoted.variant is None
        assert demoted.warning_id != missing.warning_id
    finally:
        conn.close()


def _fixture_warning():
    from pathlib import Path

    from reg_meta.catalog import DataWarning

    return DataWarning.model_validate_json(
        (Path(__file__).parent / "cases/holdings/warning/warning.json").read_text()
    )


def test_extension_preserves_base_generation_provenance(
    tmp_path: Path, global_db: Path
) -> None:
    case = Path(__file__).parent / "cases/holdings/extension-manifest"
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
    _, output = _run(tmp_path, global_db)
    with sqlite3.connect(output) as conn:
        manifest = dict(conn.execute("SELECT key,value FROM import_manifest"))
    assert {key: manifest[key] for key in request["fields"]} == expected
    assert set(request["absent_fields"]).isdisjoint(manifest)
