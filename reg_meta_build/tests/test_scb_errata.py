"""`scb_errata.toml` loader and focused apply-time tests.

Structural validation only (EXIT_CONFIG, every arm with a remediation): what a
`[[column]]` must say for the build to mint a variable from it. The
materialization — minting at source grain, the existence guard, windows, slugs,
flags, ids — is `test_scb_adapter.py::TestScbErrataColumn`, where a real build
runs. `TestDeliveredApplication` isolates the source-grain decision that must
preserve an existing target cvid before coalescing discards it. Folds in the
loader halves of the retired `test_variable_grafts.py` and
`test_canonical_attach.py`.
"""

from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.scb_errata import (
    ErrataDelivered,
    ScbErrata,
    apply_scb_errata,
    load_scb_errata,
)

if TYPE_CHECKING:
    from pathlib import Path


_ENTRY = {
    "register": '"scb/lisa"',
    "variant": '"individer-15plus"',
    "column": '"Ssyk4_J16"',
    "name": '"Yrke enligt SSYK 96"',
    "definition": '"Yrkeskod pa 4-siffernivan."',
    "versions": '["2010", "2011"]',
    "source": '"scb-docs"',
    "evidence": '"SCB doc library lisa-bakgrundsfakta-1990-2017."',
    "noted": '"2026-09-12"',
}


def _toml(**overrides: str | None) -> str:
    """The canonical `[[column]]` entry with `overrides` applied; a None value
    DROPS the key (so a required-key case is one call)."""
    fields = {**_ENTRY, **overrides}
    return "[[column]]\n" + "".join(
        f"{k} = {v}\n" for k, v in fields.items() if v is not None
    )


def _version(name: str) -> str:
    return (
        "[[version]]\n"
        'register = "scb/lisa"\n'
        'variant = "individer-15plus"\n'
        f'name = "{name}"\n'
        'evidence = "the steward holds this edition"\n'
        'noted = "2026-09-12"\n'
    )


@pytest.fixture
def slug_dir(tmp_path: Path) -> Path:
    """A curated `scb.toml` carrying the two LISA individual-frame variants the
    entries resolve against (`register` 34, variants 153 / 1335)."""
    d = tmp_path / "fqid_slugs"
    d.mkdir()
    (d / "scb.toml").write_text(
        '[register."34"]\nslug = "lisa"\n'
        '[register_variant."34.153"]\nslug = "individer-15plus"\n'
        '[register_variant."34.1335"]\nslug = "individer-16plus"\n',
        encoding="utf-8",
    )
    return d


@pytest.fixture
def seed(tmp_path: Path) -> Path:
    path = tmp_path / "classifications.toml"
    path.write_text(
        '[[classification]]\nshort_name = "SSYK96"\nname = "SSYK 96"\n'
        'valid_codes_file = "ssyk96.csv"\nversions = []\n',
        encoding="utf-8",
    )
    return path


def _load(tmp_path: Path, slug_dir: Path, body: str, seed: Path | None = None):
    path = tmp_path / "scb_errata.toml"
    path.write_text(body, encoding="utf-8")
    return load_scb_errata(path, slug_dir, classification_seed_path=seed)


def _refused(tmp_path: Path, slug_dir: Path, body: str, seed: Path | None = None):
    with pytest.raises(RegMetaError) as exc:
        _load(tmp_path, slug_dir, body, seed)
    assert exc.value.exit_code == EXIT_CONFIG
    assert exc.value.remediation
    return exc.value


def _application_db(
    targets: list[tuple[int, str | None]],
) -> sqlite3.Connection:
    """Minimal apply-time schema: one named 2022 source plus 2021 targets.

    Each target is `(cvid, column)` for source VarId 931; `None` models SCB's
    blank Kolumnnamn, which has no `variable_alias_build` row.
    """
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE register_variant (register_variant_id INTEGER PRIMARY KEY);
        CREATE TABLE register_version (
            regver_id INTEGER PRIMARY KEY,
            register_variant_id INTEGER NOT NULL,
            registerversionnamn TEXT
        );
        CREATE TABLE variable_instance (
            cvid INTEGER PRIMARY KEY,
            register_id INTEGER NOT NULL,
            register_variant_id INTEGER NOT NULL,
            regver_id INTEGER NOT NULL,
            var_id INTEGER NOT NULL,
            variabelnamn TEXT,
            data_type TEXT,
            data_length TEXT,
            value_set_version_label TEXT,
            vardemangdsniva TEXT,
            operational_definition TEXT,
            source_register_text TEXT,
            value_set_id INTEGER
        );
        CREATE TABLE variable_alias_build (
            cvid INTEGER NOT NULL,
            delivery_column_name TEXT NOT NULL,
            PRIMARY KEY (cvid, delivery_column_name)
        );
        INSERT INTO register_variant VALUES (10);
        INSERT INTO register_version VALUES (101, 10, '2021');
        INSERT INTO register_version VALUES (102, 10, '2022');
        INSERT INTO variable_instance VALUES (
            9310, 1, 10, 102, 931, 'Source name', 'varchar', '10',
            'source coding', '1', 'source operation', 'source register', 77
        );
        INSERT INTO variable_alias_build VALUES (9310, 'DispCol');
        """
    )
    for cvid, column in targets:
        conn.execute(
            "INSERT INTO variable_instance VALUES "
            "(?, 1, 10, 101, 931, 'Target name', 'int', '2', "
            "'target coding', '2', 'target operation', 'target register', 88)",
            (cvid,),
        )
        if column is not None:
            conn.execute(
                "INSERT INTO variable_alias_build VALUES (?, ?)", (cvid, column)
            )
    return conn


def _apply_delivered(conn: sqlite3.Connection) -> dict[str, int]:
    return apply_scb_errata(
        conn,
        ScbErrata(
            delivered=(
                ErrataDelivered(
                    register_id=1,
                    register_variant_id=10,
                    column="DispCol",
                    versions=("2021",),
                ),
            )
        ),
        [],
    )


class TestDeliveredApplication:
    def test_unique_blank_target_is_named_without_replacing_its_instance(self) -> None:
        conn = _application_db([(9311, None)])
        before = conn.execute(
            "SELECT * FROM variable_instance WHERE cvid = 9311"
        ).fetchone()

        assert _apply_delivered(conn)["rows"] == 1

        assert (
            conn.execute("SELECT * FROM variable_instance WHERE cvid = 9311").fetchone()
            == before
        )
        assert conn.execute(
            "SELECT cvid, delivery_column_name FROM variable_alias_build "
            "WHERE cvid = 9311"
        ).fetchall() == [(9311, "DispCol")]
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM variable_instance WHERE regver_id = 101"
            ).fetchone()[0]
            == 1
        )
        conn.close()

    def test_other_column_is_refused_and_named(self) -> None:
        conn = _application_db([(9311, "OtherCol")])
        with pytest.raises(RegMetaError) as exc:
            _apply_delivered(conn)
        assert exc.value.code == "scb_errata_delivered_under_other_column"
        assert "OtherCol" in exc.value.message
        assert "matrix columns" in exc.value.remediation
        conn.close()

    def test_multiple_blank_targets_are_ambiguous(self) -> None:
        conn = _application_db([(9311, None), (9312, None)])
        with pytest.raises(RegMetaError) as exc:
            _apply_delivered(conn)
        assert exc.value.code == "scb_errata_delivered_ambiguous"
        assert "cvid 9311" in exc.value.message
        assert "cvid 9312" in exc.value.message
        conn.close()

    def test_mixed_blank_and_named_targets_are_ambiguous(self) -> None:
        conn = _application_db([(9311, None), (9312, "MatrixCol")])
        with pytest.raises(RegMetaError) as exc:
            _apply_delivered(conn)
        assert exc.value.code == "scb_errata_delivered_ambiguous"
        assert "blank Kolumnnamn" in exc.value.message
        assert "MatrixCol" in exc.value.message
        conn.close()

    def test_competing_entries_cannot_claim_one_blank_target(self) -> None:
        conn = _application_db([(9311, None)])
        conn.execute(
            "INSERT INTO variable_instance VALUES "
            "(9312, 1, 10, 102, 931, 'Source name', 'varchar', '10', "
            "'source coding', '1', 'source operation', 'source register', 77)"
        )
        conn.execute("INSERT INTO variable_alias_build VALUES (9312, 'RivalCol')")
        errata = ScbErrata(
            delivered=tuple(
                ErrataDelivered(
                    register_id=1,
                    register_variant_id=10,
                    column=column,
                    versions=("2021",),
                )
                for column in ("DispCol", "RivalCol")
            )
        )

        with pytest.raises(RegMetaError) as exc:
            apply_scb_errata(conn, errata, [])

        assert exc.value.code == "scb_errata_delivered_under_other_column"
        assert "RivalCol" in exc.value.message
        assert "DispCol" in exc.value.message
        conn.close()


class TestVersionEntry:
    def test_historical_year_parses(self, tmp_path: Path, slug_dir: Path) -> None:
        (entry,) = _load(tmp_path, slug_dir, _version("2001")).versions
        assert entry.name == "2001"

    def test_name_without_claimed_year_fails(
        self, tmp_path: Path, slug_dir: Path
    ) -> None:
        err = _refused(tmp_path, slug_dir, _version("Äldre leverans"))
        assert err.code == "scb_errata_version_year_unknown"
        assert "four-digit year" in err.remediation


class TestColumnEntry:
    def test_absent_file_is_empty(self, slug_dir: Path) -> None:
        errata = load_scb_errata(None, slug_dir)
        assert not errata
        assert errata.columns == ()

    def test_full_entry_parses(self, tmp_path: Path, slug_dir: Path) -> None:
        (entry,) = _load(tmp_path, slug_dir, _toml()).columns
        assert (entry.register_id, entry.register_variant_id) == (34, 153)
        assert entry.column == "Ssyk4_J16"
        assert entry.name == "Yrke enligt SSYK 96"
        assert entry.definition == "Yrkeskod pa 4-siffernivan."
        assert entry.versions == ("2010", "2011")
        assert entry.source == "scb-docs"
        assert entry.key == (34, "ssyk4_j16")
        # Optional identity: absent data_type/classification means "not stated",
        # never a guessed fact; the PII flags default to off.
        assert entry.data_type is None
        assert entry.classification is None
        assert (entry.is_identifier, entry.is_sensitive) == (False, False)

    def test_all_versions_parses_as_none(self, tmp_path: Path, slug_dir: Path) -> None:
        (entry,) = _load(
            tmp_path, slug_dir, _toml(versions=None, all_versions="true")
        ).columns
        assert entry.versions is None

    def test_optional_identity_parses(self, tmp_path: Path, slug_dir: Path) -> None:
        (entry,) = _load(
            tmp_path,
            slug_dir,
            _toml(data_type='"text"', is_identifier="true", is_sensitive="true"),
        ).columns
        assert entry.data_type == "text"
        assert entry.is_identifier and entry.is_sensitive

    @pytest.mark.parametrize(
        "key",
        [
            "register",
            "variant",
            "column",
            "name",
            "definition",
            "source",
            "evidence",
            "noted",
        ],
    )
    def test_required_key_missing_fails(
        self, tmp_path: Path, slug_dir: Path, key: str
    ) -> None:
        assert _refused(tmp_path, slug_dir, _toml(**{key: None})).code

    def test_unknown_key_fails(self, tmp_path: Path, slug_dir: Path) -> None:
        err = _refused(tmp_path, slug_dir, _toml(valid_from='"2010-01-01"'))
        assert "valid_from" in err.message

    def test_versions_and_all_versions_together_fail(
        self, tmp_path: Path, slug_dir: Path
    ) -> None:
        err = _refused(tmp_path, slug_dir, _toml(all_versions="true"))
        assert "both" in err.message

    def test_neither_versions_nor_all_versions_fails(
        self, tmp_path: Path, slug_dir: Path
    ) -> None:
        err = _refused(tmp_path, slug_dir, _toml(versions=None))
        assert "neither" in err.message

    def test_repeated_version_fails(self, tmp_path: Path, slug_dir: Path) -> None:
        err = _refused(tmp_path, slug_dir, _toml(versions='["2010", "2010"]'))
        assert "repeats" in err.message

    def test_unknown_source_fails(self, tmp_path: Path, slug_dir: Path) -> None:
        err = _refused(tmp_path, slug_dir, _toml(source='"a-hunch"'))
        assert "a-hunch" in err.message

    def test_unknown_data_type_fails(self, tmp_path: Path, slug_dir: Path) -> None:
        err = _refused(tmp_path, slug_dir, _toml(data_type='"txt"'))
        assert "txt" in err.message

    def test_non_scb_register_fails(self, tmp_path: Path, slug_dir: Path) -> None:
        err = _refused(tmp_path, slug_dir, _toml(register='"sos/lisa"'))
        assert err.code == "scb_errata_unknown_variant"

    def test_uncurated_variant_fails(self, tmp_path: Path, slug_dir: Path) -> None:
        err = _refused(tmp_path, slug_dir, _toml(variant='"foretag"'))
        assert err.code == "scb_errata_unknown_variant"

    def test_non_bool_flag_fails(self, tmp_path: Path, slug_dir: Path) -> None:
        assert _refused(tmp_path, slug_dir, _toml(is_identifier='"yes"')).code

    def test_declared_classification_passes(
        self, tmp_path: Path, slug_dir: Path, seed: Path
    ) -> None:
        (entry,) = _load(
            tmp_path, slug_dir, _toml(classification='"SSYK96"'), seed
        ).columns
        assert entry.classification == "SSYK96"

    def test_undeclared_classification_fails(
        self, tmp_path: Path, slug_dir: Path, seed: Path
    ) -> None:
        # The candidate feed drops an unknown short_name with no row and no
        # error, so a typo would ship an untagged state.
        err = _refused(tmp_path, slug_dir, _toml(classification='"SSYK69"'), seed)
        assert "SSYK69" in err.message

    def test_duplicate_column_on_one_variant_fails(
        self, tmp_path: Path, slug_dir: Path
    ) -> None:
        err = _refused(tmp_path, slug_dir, _toml() + "\n" + _toml())
        assert "duplicate" in err.message

    def test_a_column_is_delivered_or_minted_never_both(
        self, tmp_path: Path, slug_dir: Path
    ) -> None:
        delivered = (
            '[[delivered]]\nregister = "scb/lisa"\nvariant = "individer-15plus"\n'
            'column = "Ssyk4_J16"\nversions = ["2010"]\n'
            'evidence = "holdings"\nnoted = "2026-09-12"\n'
        )
        err = _refused(tmp_path, slug_dir, delivered + "\n" + _toml())
        assert "[[delivered]]" in err.remediation

    def test_same_column_on_two_variants_is_one_variable(
        self, tmp_path: Path, slug_dir: Path
    ) -> None:
        both = _toml() + "\n" + _toml(variant='"individer-16plus"')
        first, second = _load(tmp_path, slug_dir, both).columns
        assert first.key == second.key
        assert first.register_variant_id != second.register_variant_id

    def test_two_variants_disagreeing_about_the_variable_fail(
        self, tmp_path: Path, slug_dir: Path
    ) -> None:
        both = (
            _toml()
            + "\n"
            + _toml(variant='"individer-16plus"', name='"Something else"')
        )
        err = _refused(tmp_path, slug_dir, both)
        assert "different variable" in err.message
