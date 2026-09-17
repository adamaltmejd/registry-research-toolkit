"""Validation of existing lineage configuration during conversion."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError

from reg_meta_build.fqid_slugs import load_lineage_config

if TYPE_CHECKING:
    from pathlib import Path

# Source register RTB-style, consumer LISA-style. RTB owns Kön; LISA's Kön is
# sourced from it. IDs are arbitrary but stable across the helpers below.
_SOURCE_REGISTER_ID = 1
_CONSUMER_REGISTER_ID = 2
_SCB_PROVIDERS = frozenset({"scb"})
_SCB_SOS_PROVIDERS = frozenset({"scb", "sos"})


class TestLoadLineageConfig:
    def test_parses_defaults_and_overrides(self, tmp_path: Path):
        path = tmp_path / "lineage.toml"
        path.write_text(
            '[lineage_defaults]\n"scb/rtb" = "folkbokforda-personer"\n'
            '"scb/iot" = "bostadshushall"\n'
            '[lineage."scb/lisa/inkomst_pension"]\n'
            'source_register = "rams"\nsource_variant = "individregister"\n',
            encoding="utf-8",
        )
        cfg = load_lineage_config(path)
        assert cfg.defaults == {
            ("scb", "rtb"): "folkbokforda-personer",
            ("scb", "iot"): "bostadshushall",
        }
        assert cfg.overrides == {
            ("scb", "lisa", "inkomst_pension"): ("rams", "individregister")
        }

    def test_unknown_top_level_table_fails(self, tmp_path: Path):
        path = tmp_path / "lineage.toml"
        path.write_text('[classification."SUN2020"]\nslug = "sun"\n', encoding="utf-8")
        with pytest.raises(RegMetaError) as exc:
            load_lineage_config(path)
        assert exc.value.code == "lineage_invalid"

    def test_missing_source_variant_in_override_fails(self, tmp_path: Path):
        path = tmp_path / "lineage.toml"
        path.write_text(
            '[lineage."scb/lisa/kon"]\nsource_register = "rtb"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_lineage_config(path)
        assert exc.value.code == "lineage_override_incomplete"

    def test_same_register_slug_across_providers_coexist(self, tmp_path: Path):
        """register.slug is not globally unique: an `rtb` default under scb and
        an `rtb` default under sos are DISTINCT (provider-keyed), not a
        duplicate. The linker resolves each against its own source provider
        (Codex P2 on #145)."""
        path = tmp_path / "lineage.toml"
        path.write_text(
            '[lineage_defaults]\n"scb/rtb" = "folkbokforda-personer"\n'
            '"sos/rtb" = "grund-bosattning"\n',
            encoding="utf-8",
        )
        cfg = load_lineage_config(path)
        assert cfg.defaults == {
            ("scb", "rtb"): "folkbokforda-personer",
            ("sos", "rtb"): "grund-bosattning",
        }

    def test_duplicate_default_same_provider_fails(self, tmp_path: Path):
        """TOML rejects duplicate FQID keys before they can fight at apply time."""
        path = tmp_path / "lineage.toml"
        path.write_text(
            '[lineage_defaults]\n"scb/rtb" = "folkbokforda-personer"\n'
            '"scb/rtb" = "grund-bosattning"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_lineage_config(path)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_missing_file_yields_empty_config(self, tmp_path: Path):
        cfg = load_lineage_config(tmp_path / "absent.toml")
        assert cfg.defaults == {}
        assert cfg.overrides == {}
