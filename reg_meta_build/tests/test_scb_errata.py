"""`scb_errata.toml` loader tests — the `[[column]]` entry kind (Y-116).

Structural validation only (EXIT_CONFIG, every arm with a remediation): what a
`[[column]]` must say for the build to mint a variable from it. The
materialization — minting at source grain, the existence guard, windows, slugs,
flags, ids — is `test_scb_adapter.py::TestScbErrataColumn`, where a real build
runs. Folds in the loader halves of the retired `test_variable_grafts.py` and
`test_canonical_attach.py`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.scb_errata import load_scb_errata

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
