"""Coverage for the curated residual code-less ↔ code-bearing overlap mechanism
(#868): the `codeless_overlap.py` loader and the `db.py` materialization pass
`_resolve_curated_codeless_overlaps` + its mandatory-curation gate.

Three layers:
  - Loader validation — mirrors `TestLoadCodelivery` in `test_triage.py`: bad
    directive, missing/forbidden `extend`, blank key parts, duplicate key, malformed
    TOML → EXIT_CONFIG.
  - Materialization — in-memory DDL with FKs off (no provider/register parents
    needed) and states inserted directly, mirroring `TestDropFullcoverCodelessStates`
    in `test_triage.py`. Exercises cap-edge, cap-interior split, drop, extend, the
    unresolvable-extend failure, and the post-resolution mandatory-curation gate —
    both buckets: uncurated keys and a curated entry whose resolution leaves a
    residual overlap.
  - Build integration — a reversed-id, mixed-case SCB edition sequence exercises
    claimed-year alias selection before the literal-column overlap gate, then proves
    an exact `cap` keeps both uncoded tails and leaves the coded years unchanged.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.codeless_overlap import load_codeless_overlap

if TYPE_CHECKING:
    from pathlib import Path


# ── loader validation ───────────────────────────────────────────────────────


class TestLoadCodelessOverlap:
    def test_parses_each_resolution(self, tmp_path: Path) -> None:
        toml = tmp_path / "codeless_overlap.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="naringsgren"\nvariable="sni"\n'
            'column="SNI"\nresolution="cap"\n\n'
            '[[resolve]]\nprovider="scb"\nregister="skollan"\nvariable="skollan"\n'
            'column="SkolLan"\nresolution="drop"\n\n'
            '[[resolve]]\nprovider="scb"\nregister="bef"\nvariable="fodelseland"\n'
            'column="FodelseLand"\nresolution="extend"\nextend="2010 coding"\n',
            encoding="utf-8",
        )
        cmap = load_codeless_overlap(toml)
        # Column folded to the rule-2 connectivity key (#196) — casing is cosmetic.
        # Key is provider-scoped: (provider, register, variable, folded-column).
        assert cmap[("scb", "naringsgren", "sni", "sni")] == ("cap", None)
        assert cmap[("scb", "skollan", "skollan", "skollan")] == ("drop", None)
        assert cmap[("scb", "bef", "fodelseland", "fodelseland")] == (
            "extend",
            "2010 coding",
        )

    def test_omitted_column_is_none_sentinel(self, tmp_path: Path) -> None:
        # A state with `delivery_column_name IS NULL` is curated by OMITTING the
        # `column` field → the key's column component is the `None` sentinel (NOT
        # `""`), so it matches the NULL row at materialization time.
        toml = tmp_path / "codeless_overlap.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="skyddad-natur"\n'
            'variable="naturtyp-skyddade-omraden-areal"\nresolution="cap"\n',
            encoding="utf-8",
        )
        cmap = load_codeless_overlap(toml)
        assert cmap[
            ("scb", "skyddad-natur", "naturtyp-skyddade-omraden-areal", None)
        ] == (
            "cap",
            None,
        )

    def test_missing_provider_rejected(self, tmp_path: Path) -> None:
        # `provider` is a required key part (the register slug is unique only per
        # provider) — a missing one is curation drift, not a silent default.
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolve]]\nregister="r"\nvariable="v"\ncolumn="c"\nresolution="drop"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(toml)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codeless_overlap_invalid"
        assert "`provider`" in exc.value.message

    def test_empty_column_rejected(self, tmp_path: Path) -> None:
        # A present-but-empty `column = ""` is ambiguous (absent already means NULL)
        # → rejected, not folded to "".
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="r"\nvariable="v"\ncolumn=""\n'
            'resolution="drop"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(toml)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codeless_overlap_invalid"
        assert "`column`" in exc.value.message

    def test_unknown_resolution_rejected(self, tmp_path: Path) -> None:
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="r"\nvariable="v"\ncolumn="c"\n'
            'resolution="trim"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(toml)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codeless_overlap_invalid"
        assert "unknown resolution" in exc.value.message

    def test_extend_missing_label_rejected(self, tmp_path: Path) -> None:
        # An `extend` entry with NO `extend` key at all stays an error (the typo
        # guard) — distinct from a present-but-empty `extend = ""`, which is valid.
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="r"\nvariable="v"\ncolumn="c"\n'
            'resolution="extend"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(toml)
        assert exc.value.exit_code == EXIT_CONFIG
        assert "no `extend` key" in exc.value.message

    def test_extend_empty_label_loads(self, tmp_path: Path) -> None:
        # `extend = ""` is the empty-label target: the KEY is present (so the typo
        # guard passes) and the stored `extend_label` is `""` (NOT None — None means
        # "no extend", reserved for cap/drop). It names the unique empty/whitespace-
        # labelled coded vintage on the key (HDIA/ATCO shape).
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="par"\nvariable="typ-av-diagnos"\n'
            'column="HDIA"\nresolution="extend"\nextend=""\n',
            encoding="utf-8",
        )
        cmap = load_codeless_overlap(toml)
        assert cmap[("scb", "par", "typ-av-diagnos", "hdia")] == ("extend", "")

    def test_extend_empty_forbidden_on_cap(self, tmp_path: Path) -> None:
        # `extend = ""` on a `cap` entry is still a forbidden stray `extend` — the
        # present-empty key is detected by membership, not truthiness.
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="r"\nvariable="v"\ncolumn="c"\n'
            'resolution="cap"\nextend=""\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(toml)
        assert exc.value.exit_code == EXIT_CONFIG
        assert "sets `extend`" in exc.value.message

    def test_extend_forbidden_on_cap(self, tmp_path: Path) -> None:
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="r"\nvariable="v"\ncolumn="c"\n'
            'resolution="cap"\nextend="2010"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(toml)
        assert exc.value.exit_code == EXIT_CONFIG
        assert "sets `extend`" in exc.value.message

    def test_blank_key_part_rejected(self, tmp_path: Path) -> None:
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="r"\nvariable=""\ncolumn="c"\n'
            'resolution="drop"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(toml)
        assert exc.value.exit_code == EXIT_CONFIG
        assert "`variable`" in exc.value.message

    def test_duplicate_key_rejected(self, tmp_path: Path) -> None:
        # Two entries that fold to the same (register, variable, column) key.
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolve]]\nprovider="scb"\nregister="r"\nvariable="v"\ncolumn="Col"\n'
            'resolution="drop"\n'
            '[[resolve]]\nprovider="scb"\nregister="r"\nvariable="v"\ncolumn="COL"\n'
            'resolution="cap"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(toml)
        assert exc.value.exit_code == EXIT_CONFIG
        assert "duplicate" in exc.value.message

    def test_unknown_top_level_key_rejected(self, tmp_path: Path) -> None:
        # A misspelled `[[resolves]]` must be a loud error, not a silent no-op that
        # disables ALL curation (shared scaffold guarantee).
        toml = tmp_path / "x.toml"
        toml.write_text(
            '[[resolves]]\nregister="r"\nvariable="v"\ncolumn="c"\nresolution="drop"\n',
            encoding="utf-8",
        )
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(toml)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codeless_overlap_invalid"

    def test_malformed_toml_is_config_error(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.toml"
        bad.write_text('[[resolve]]\nregister = = "r"\n', encoding="utf-8")
        with pytest.raises(RegMetaError) as exc:
            load_codeless_overlap(bad)
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "codeless_overlap_toml_unreadable"

    def test_missing_file_is_empty(self, tmp_path: Path) -> None:
        assert load_codeless_overlap(tmp_path / "nope.toml") == {}
        assert load_codeless_overlap(None) == {}


# ── materialization pass ─────────────────────────────────────────────────────
