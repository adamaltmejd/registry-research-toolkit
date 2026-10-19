"""Provider-TOML refusals of the steward overlay, driven through `extend_db` on a session base.

Each malformed or contradictory provider TOML is refused with a located
`EXIT_CONFIG` error and leaves no output or staging file; the accepted twins sit
beside their refusals. Shared provider TOML, base and run entry:
`_extend_db_support`.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from _extend_db_support import (
    BASE_TOML as _BASE_TOML,
    base_db_fixture,  # noqa: F401
    run as _run,
)
from reg_meta_build.errors import EXIT_CONFIG, RegMetaError

if TYPE_CHECKING:
    from pathlib import Path


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


@pytest.mark.parametrize(
    "old, new, refusal",
    [
        (
            '  is_sensitive = true\n  variants = ["_default"]',
            '  is_sensitive = true\n  variants = ["_default"]\n  valid_from = "2018"',
            "state 'KONTO' in year-independent variant '_default' carries a "
            "validity bound.",
        ),
        (
            '    column = "BELOPP"',
            '    column = "BELOPP"\n    aliases = ["BELOPP_SEK"]',
            "state 'BELOPP' in year-independent variant '_default' carries "
            "co-delivered aliases.",
        ),
    ],
)
def test_year_independent_variant_refuses_a_window_through_extend_db(
    tmp_path: Path, base_db: Path, old: str, new: str, refusal: str
) -> None:
    # Fails if a year-independent variant's state is written with a calendar
    # window, either inherited from its variable (here) or through an alias's
    # dated window table, instead of being refused. Its accepted twin is
    # `test_swecov_inventory.py`'s year-independent flavor table.
    text = (
        _BASE_TOML.replace(
            '  description = "One table"',
            '  description = "One table"\n  period_scope = "year_independent"',
            1,
        )
        .replace(
            '    data_type = "varchar"\n    valid_from = "2018"',
            '    data_type = "varchar"',
            1,
        )
        .replace(old, new, 1)
    )
    error = _assert_rejected_without_output(tmp_path, base_db, text)
    assert refusal in error.message


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
