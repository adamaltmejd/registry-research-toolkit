"""Tests for variable grafts (#365 PR1d; `variable_grafts.py`).

Covers the TOML loader (structural validation, EXIT_CONFIG) and the
materialization pass: minting variable+state+alias onto an existing
(register, variant), gap-fill skip, lenient unresolved, the provider gate,
typeless→NULL data_type, and SCB minted-id banding."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import build_slugged_db
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.id import _MINT_BIT
from reg_meta_build.variable_grafts import (
    CuratedGraft,
    load_variable_grafts,
    materialize_grafts,
)

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

_SCB = frozenset({"scb"})


def _g(
    column: str,
    description: str = "Some delivered column",
    *,
    register: str = "lisa",
    variant: str = "individer-15plus",
    data_type: str = "",
    is_identifier: bool = False,
    is_sensitive: bool = False,
) -> CuratedGraft:
    return CuratedGraft(
        provider="scb",
        register=register,
        variant=variant,
        column=column,
        description=description,
        data_type=data_type,
        is_identifier=is_identifier,
        is_sensitive=is_sensitive,
    )


def _variable(conn: sqlite3.Connection, provider_key: str):
    return conn.execute(
        "SELECT variable_id, name, description, source_label, slug FROM variable "
        "WHERE provider_key = ?",
        (provider_key,),
    ).fetchone()


def _write(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


# ── loader ───────────────────────────────────────────────────────────────────


class TestLoader:
    def test_none_path_is_empty(self) -> None:
        assert load_variable_grafts(None) == ()

    def test_valid_entries_parse(self, tmp_path: Path) -> None:
        toml = _write(
            tmp_path / "variable_grafts.toml",
            '[[graft]]\nregister = "scb/agi"\nvariant = "individuppgifter-agi"\n'
            'column = "DIST_HU_SAKNAS"\ndescription = "Saknas-flagga"\n\n'
            '[[graft]]\nregister = "scb/par"\nvariant = "v1"\ncolumn = "C"\n'
            'description = "D"\ndata_type = "char"\nis_identifier = true\n',
        )
        grafts = load_variable_grafts(toml)
        assert [
            (g.register, g.variant, g.column, g.data_type, g.is_identifier)
            for g in grafts
        ] == [
            ("agi", "individuppgifter-agi", "DIST_HU_SAKNAS", "", False),
            ("par", "v1", "C", "char", True),
        ]

    @pytest.mark.parametrize(
        "body",
        [
            '[[graft]]\nvariant = "v"\ncolumn = "C"\ndescription = "D"\n',  # no register
            '[[graft]]\nregister = "scb/agi"\ncolumn = "C"\ndescription = "D"\n',  # no variant
            '[[graft]]\nregister = "scb/agi"\nvariant = "v"\ndescription = "D"\n',  # no column
            '[[graft]]\nregister = "scb/agi"\nvariant = "v"\ncolumn = "C"\n',  # no description
            '[[graft]]\nregister = "agi"\nvariant = "v"\ncolumn = "C"\ndescription = "D"\n',  # 1-seg
            '[[graft]]\nregister = "scb/agi"\nvariant = "v"\ncolumn = "C"\n'
            'description = "D"\ndata_type = 7\n',  # non-string data_type
            '[[graft]]\nregister = "scb/agi"\nvariant = "v"\ncolumn = "C"\n'
            'description = "D"\nis_identifier = "false"\n',  # quoted bool
        ],
    )
    def test_malformed_fails(self, tmp_path: Path, body: str) -> None:
        toml = _write(tmp_path / "variable_grafts.toml", body)
        with pytest.raises(RegMetaError) as exc:
            load_variable_grafts(toml)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_duplicate_triple_fails(self, tmp_path: Path) -> None:
        toml = _write(
            tmp_path / "variable_grafts.toml",
            '[[graft]]\nregister = "scb/agi"\nvariant = "v"\ncolumn = "Col"\n'
            'description = "A"\n\n'
            '[[graft]]\nregister = "scb/agi"\nvariant = "v"\ncolumn = "col"\n'
            'description = "B"\n',  # case-folds to the same column
        )
        with pytest.raises(RegMetaError) as exc:
            load_variable_grafts(toml)
        assert exc.value.exit_code == EXIT_CONFIG


# ── materialize ────────────────────────────────────────────────────────────────


class TestMaterialize:
    def test_mints_variable_state_alias(self) -> None:
        conn = build_slugged_db()  # scb/lisa/individer-15plus (variant 10), var kon/Kon
        counts = materialize_grafts(
            conn, (_g("Bilersattning", "Bilförmån"),), providers=_SCB
        )
        assert counts == {"minted": 1, "skipped": 0, "unresolved": 0}
        row = _variable(conn, "graft:Bilersattning")
        assert row is not None
        vid, name, desc, source_label, slug = row
        assert name == "Bilförmån" and desc == "Bilförmån"
        assert source_label == "swecov-graft"
        assert slug is None  # populate_variable_slugs auto-derives it later
        assert vid < _MINT_BIT
        state = conn.execute(
            "SELECT register_variant_id, valid_from, valid_to, delivery_column_name "
            "FROM variable_state WHERE variable_id = ?",
            (vid,),
        ).fetchone()
        assert tuple(state) == (10, "0001-01-01", "9999-12-31", "Bilersattning")
        assert conn.execute(
            "SELECT 1 FROM variable_alias WHERE variable_id = ? "
            "AND delivery_column_name = ?",
            (vid, "Bilersattning"),
        ).fetchone()

    def test_gap_fill_skips_existing_column(self) -> None:
        conn = build_slugged_db()  # kon already delivers column 'Kon' in variant 10
        counts = materialize_grafts(conn, (_g("Kon"),), providers=_SCB)
        assert counts == {"minted": 0, "skipped": 1, "unresolved": 0}

    def test_gap_fill_skip_is_case_insensitive(self) -> None:
        conn = build_slugged_db()
        counts = materialize_grafts(conn, (_g("kON"),), providers=_SCB)
        assert counts["skipped"] == 1 and counts["minted"] == 0

    def test_gap_fill_skip_folds_swedish_case(self) -> None:
        # The existing state delivers column "Ägare" (uppercase Swedish Ä); a graft
        # for "ägare" must fold to the same column and be SKIPPED, not minted as a
        # duplicate. ASCII LOWER() leaves Ä/ä distinct, so it would MINT here —
        # this asserts the Unicode-aware py_lower fold (refs #853).
        conn = build_slugged_db(delivery_column_name="Ägare")
        counts = materialize_grafts(conn, (_g("ägare"),), providers=_SCB)
        assert counts["skipped"] == 1 and counts["minted"] == 0

    def test_unresolved_variant_counted(self) -> None:
        conn = build_slugged_db()
        warnings: list[str] = []
        counts = materialize_grafts(
            conn,
            (_g("X", variant="no-such-variant"),),
            providers=_SCB,
            warn=warnings.append,
        )
        assert counts == {"minted": 0, "skipped": 0, "unresolved": 1}
        assert any("did not resolve" in w for w in warnings)

    def test_provider_gate_skips_inactive(self) -> None:
        conn = build_slugged_db()
        counts = materialize_grafts(conn, (_g("X"),), providers=frozenset({"sos"}))
        assert counts == {"minted": 0, "skipped": 0, "unresolved": 0}
        assert _variable(conn, "graft:X") is None

    def test_typeless_data_type_is_null(self) -> None:
        conn = build_slugged_db()
        materialize_grafts(conn, (_g("X", data_type=""),), providers=_SCB)
        dt = conn.execute(
            "SELECT data_type FROM variable_state WHERE delivery_column_name = 'X'"
        ).fetchone()[0]
        assert dt is None

    def test_typed_data_type_kept(self) -> None:
        conn = build_slugged_db()
        materialize_grafts(conn, (_g("X", data_type="char"),), providers=_SCB)
        dt = conn.execute(
            "SELECT data_type FROM variable_state WHERE delivery_column_name = 'X'"
        ).fetchone()[0]
        assert dt == "char"

    def test_identifier_sensitive_flags_kept(self) -> None:
        conn = build_slugged_db()
        materialize_grafts(
            conn,
            (_g("PeOrgNrHe", is_identifier=True, is_sensitive=True),),
            providers=_SCB,
        )
        row = conn.execute(
            "SELECT is_identifier, is_sensitive FROM variable "
            "WHERE provider_key = 'graft:PeOrgNrHe'"
        ).fetchone()
        assert tuple(row) == (1, 1)

    def test_mints_in_scb_band_despite_high_ids(self) -> None:
        conn = build_slugged_db()
        # a SOS-minted variable holds a high id; AUTOINCREMENT would jump past it,
        # but the graft must stay in the SCB band (< 2^62).
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key) "
            "VALUES (?, 1, 'sos-minted')",
            (_MINT_BIT + 5,),
        )
        materialize_grafts(conn, (_g("X"),), providers=_SCB)
        vid = _variable(conn, "graft:X")[0]
        assert vid < _MINT_BIT

    def test_multiple_grafts_get_distinct_ids(self) -> None:
        conn = build_slugged_db()
        counts = materialize_grafts(conn, (_g("A"), _g("B"), _g("C")), providers=_SCB)
        assert counts["minted"] == 3
        ids = [r[0] for r in conn.execute("SELECT state_id FROM variable_state")]
        assert len(ids) == len(set(ids))  # no state_id collision
