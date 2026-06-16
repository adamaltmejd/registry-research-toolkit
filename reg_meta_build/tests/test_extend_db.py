"""Tests for the steward-flavored DB overlay (#365 PR2; `extend_db.py`).

Synthetic-only: a small global DB is built from the SCB CSV fixtures, then
`extend_db` overlays a steward inventory onto a COPY. Minted ids are computed
via `id.mint(...)` (deterministic) so a temp steward slug dir can be keyed on
them. Covers the two steward-only operation kinds (providers +
registers/variants/variables/states), the no-clobber guarantee, base-DB
immutability, incremental slugging, FTS rebuild, flavored validation, and
deterministic re-run.

The `_no_repo_curation` autouse fixture (session-scoped, in `_shared_fixtures`)
is in scope, so the global build runs with empty curation maps.
"""

from __future__ import annotations

import ast
import inspect
import json
import re
import sqlite3
from typing import TYPE_CHECKING

import pytest
from _csv_fixtures import write_scb_input
from _shared_fixtures import _write_fixture_slug_dir
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.db import build_db
from reg_meta_build.extend_db import (
    InvProvider,
    _expand_window,
    _insert_providers,
    extend_db,
    load_inventory,
)
from reg_meta_build.id import _MINT_BIT, mint
from reg_meta_build.validate import validate_built_db

from reg_meta_build.fqid_slugs import populate_variable_slugs

if TYPE_CHECKING:
    from pathlib import Path

# Steward + provider used across the inventory fixtures.
_STEWARD = "swecov"
_BANK = "swedbank"


# ── inventory fixtures ─────────────────────────────────────────────────────────


def _base_inventory() -> dict:
    """A steward-only inventory: one new provider with a register/variant and
    two variables. No global-entity enrichment (grafts/aliases) — that is
    global-build work, not flavor content."""
    return {
        "steward": _STEWARD,
        "source_label": "swecov-inventory-test",
        "providers": [{"slug": _BANK, "name": "Swedbank AB"}],
        "registers": [
            {
                "provider": _BANK,
                "key": "transaktioner",
                "name": "Transaktioner",
                "purpose": None,
                "description": "Bankkontotransaktioner.",
                "variants": [
                    {
                        "key": "_default",
                        "name": "Transaktioner",
                        "description": None,
                        "variables": [
                            {
                                "key": "belopp",
                                "name": "Belopp",
                                "definition": None,
                                "description": "Transaktionsbelopp i SEK.",
                                "column": "BELOPP",
                                "data_type": "float",
                                "is_identifier": False,
                                "is_sensitive": False,
                                "valid_from": None,
                                "valid_to": None,
                            },
                            {
                                "key": "kontonr",
                                "name": "Kontonummer",
                                "definition": None,
                                "description": None,
                                "column": "KONTO",
                                "data_type": "varchar",
                                "is_identifier": True,
                                "is_sensitive": True,
                                "valid_from": "2018",
                                "valid_to": None,
                            },
                        ],
                    }
                ],
            }
        ],
    }


def _reg_stub(provider: str, key: str) -> dict:
    """A minimal structurally-valid register dict (one variant, one variable) —
    used to exercise the loader's duplicate-(provider, key) guard."""
    return {
        "provider": provider,
        "key": key,
        "name": "N",
        "variants": [
            {
                "key": "v",
                "name": "V",
                "variables": [{"key": "x", "name": "X", "column": "C"}],
            }
        ],
    }


def _steward_register_ids() -> dict[str, int]:
    """Deterministic minted ids for the base inventory's steward graph."""
    return {
        "provider": mint("provider", _BANK),
        "register": mint("register", _BANK, "transaktioner"),
        "variant": mint("variant", _BANK, "transaktioner", "_default"),
        "var_belopp": mint("variable", _BANK, "transaktioner", "_default", "belopp"),
        "var_kontonr": mint("variable", _BANK, "transaktioner", "_default", "kontonr"),
    }


def _write_steward_slug_dir(slug_dir: Path) -> None:
    """Author a steward slug dir keyed on the minted ids: a `swedbank.toml`
    register/variant slug entry + an UNFROZEN sentinel + empty snapshot. No
    variable entries — the new variables auto-slug incrementally."""
    ids = _steward_register_ids()
    (slug_dir / "UNFROZEN").write_text("test steward unfrozen\n", encoding="utf-8")
    (slug_dir / ".snapshot.json").write_text("{}\n", encoding="utf-8")
    (slug_dir / f"{_BANK}.toml").write_text(
        f'[register."{ids["register"]}"]\nslug = "transaktioner"\n'
        f'[register_variant."{ids["register"]}.{ids["variant"]}"]\n'
        'slug = "transaktioner-default"\n',
        encoding="utf-8",
    )


# ── build helpers ──────────────────────────────────────────────────────────────


@pytest.fixture()
def global_db(tmp_path: Path) -> Path:
    """Build a synthetic global DB from the SCB CSV fixtures; return its path."""
    inp, dbdir, slug = tmp_path / "in", tmp_path / "globaldb", tmp_path / "gslug"
    for d in (inp, dbdir, slug):
        d.mkdir()
    write_scb_input(inp)
    _write_fixture_slug_dir(slug)
    build_db(input_dir=inp, db_dir=dbdir, skip_classifications=True, slug_dir=slug)
    return dbdir / "reg_meta.db"


def _run_extend(
    tmp_path: Path,
    base_db: Path,
    inventory: dict,
    *,
    out_name: str = "out",
    validate: bool = False,
) -> tuple[dict, Path]:
    """Write the inventory + a steward slug dir, run extend_db, return
    (counts, output_db_path)."""
    inv_path = tmp_path / f"{out_name}-inventory.json"
    inv_path.write_text(json.dumps(inventory), encoding="utf-8")
    slug_dir = tmp_path / f"{out_name}-sslug"
    slug_dir.mkdir()
    _write_steward_slug_dir(slug_dir)
    out_dir = tmp_path / out_name
    out_dir.mkdir()
    hook = None
    if validate:

        def hook(staging: Path) -> None:
            result = validate_built_db(staging, flavored=True)
            if result.failures:
                raise AssertionError(f"flavored validation failed: {result.failures}")

    counts = extend_db(
        base_db=base_db,
        inventory_path=inv_path,
        db_dir=out_dir,
        steward=_STEWARD,
        slug_dir=slug_dir,
        pre_rename_hook=hook,
    )
    return counts, out_dir / "reg_meta.db"


# ── _expand_window ─────────────────────────────────────────────────────────────


class TestExpandWindow:
    def test_month_from_open_to(self) -> None:
        # YYYY-MM lower bound expands to the month's first day; None upper → open.
        assert _expand_window("2018-06", None) == ("2018-06-01", "9999-12-31")

    def test_open_from_month_to(self) -> None:
        # None lower → open sentinel; YYYY-MM upper expands to month-last-day.
        assert _expand_window(None, "2020-12") == ("0001-01-01", "2020-12-31")

    def test_malformed_token_is_config_error(self) -> None:
        with pytest.raises(RegMetaError) as exc:
            _expand_window("not-a-date", None)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_inverted_window_is_config_error(self) -> None:
        # valid_from after valid_to would violate the variable_state DDL CHECK
        # (valid_to >= valid_from) as an opaque IntegrityError at INSERT — caught
        # here as a clean structural defect instead.
        with pytest.raises(RegMetaError) as exc:
            _expand_window("2020", "2018")
        assert exc.value.exit_code == EXIT_CONFIG


# ── overlay inserts ──────────────────────────────────────────────────────────────


class TestOverlayInserts:
    def test_new_core_graph_rows_present_with_source_label(
        self, tmp_path: Path, global_db: Path
    ) -> None:
        counts, out = _run_extend(tmp_path, global_db, _base_inventory())
        assert counts["providers"] == 1
        assert counts["registers"] == 1
        assert counts["variants"] == 1
        assert counts["variables"] == 2
        assert counts["states"] == 2
        conn = sqlite3.connect(out)
        ids = _steward_register_ids()

        prov = conn.execute(
            "SELECT provider_id, name FROM provider WHERE slug = ?", (_BANK,)
        ).fetchone()
        assert prov == (ids["provider"], "Swedbank AB")

        reg = conn.execute(
            "SELECT register_id, provider_id, name FROM register WHERE register_id = ?",
            (ids["register"],),
        ).fetchone()
        assert reg == (ids["register"], ids["provider"], "Transaktioner")

        # Both steward variables carry the inventory source_label.
        labels = {
            r[0]
            for r in conn.execute(
                "SELECT DISTINCT source_label FROM variable WHERE register_id = ?",
                (ids["register"],),
            )
        }
        assert labels == {"swecov-inventory-test"}

        # Each state's delivery column has a variable_alias row.
        for var_id, column in (
            (ids["var_belopp"], "BELOPP"),
            (ids["var_kontonr"], "KONTO"),
        ):
            state = conn.execute(
                "SELECT delivery_column_name FROM variable_state WHERE variable_id = ?",
                (var_id,),
            ).fetchone()
            assert state == (column,)
            alias = conn.execute(
                "SELECT 1 FROM variable_alias WHERE variable_id = ? "
                "AND delivery_column_name = ?",
                (var_id, column),
            ).fetchone()
            assert alias is not None

    def test_flags_and_validity_window_round_trip(
        self, tmp_path: Path, global_db: Path
    ) -> None:
        _, out = _run_extend(tmp_path, global_db, _base_inventory())
        conn = sqlite3.connect(out)
        ids = _steward_register_ids()
        # kontonr: is_identifier/is_sensitive set, valid_from "2018" expanded.
        flags = conn.execute(
            "SELECT is_identifier, is_sensitive FROM variable WHERE variable_id = ?",
            (ids["var_kontonr"],),
        ).fetchone()
        assert flags == (1, 1)
        window = conn.execute(
            "SELECT valid_from, valid_to FROM variable_state WHERE variable_id = ?",
            (ids["var_kontonr"],),
        ).fetchone()
        # valid_from "2018" expands to full ISO bounds (the DDL CHECKs
        # length=10); valid_to (None) falls back to the open sentinel.
        assert window == ("2018-01-01", "9999-12-31")
        # belopp: open-range fallback on both ends.
        belopp_window = conn.execute(
            "SELECT valid_from, valid_to FROM variable_state WHERE variable_id = ?",
            (ids["var_belopp"],),
        ).fetchone()
        assert belopp_window == ("0001-01-01", "9999-12-31")

    def test_steward_provider_id_in_high_band(
        self, tmp_path: Path, global_db: Path
    ) -> None:
        _run_extend(tmp_path, global_db, _base_inventory())
        ids = _steward_register_ids()
        for key in ("provider", "register", "variant", "var_belopp", "var_kontonr"):
            assert ids[key] >= _MINT_BIT


# ── no-clobber + base-DB immutability ────────────────────────────────────────


class TestNoClobber:
    def test_global_core_rows_and_slugs_byte_identical(
        self, tmp_path: Path, global_db: Path
    ) -> None:
        before = _global_snapshot(sqlite3.connect(global_db))
        _, out = _run_extend(tmp_path, global_db, _base_inventory())
        after = _global_snapshot(sqlite3.connect(out))
        assert before == after

    def test_base_db_not_mutated(self, tmp_path: Path, global_db: Path) -> None:
        size_before = global_db.stat().st_size
        mtime_before = global_db.stat().st_mtime_ns
        digest_before = _global_snapshot(sqlite3.connect(global_db))
        _run_extend(tmp_path, global_db, _base_inventory())
        assert global_db.stat().st_size == size_before
        assert global_db.stat().st_mtime_ns == mtime_before
        assert _global_snapshot(sqlite3.connect(global_db)) == digest_before


def _global_snapshot(conn: sqlite3.Connection) -> dict:
    """Snapshot the pre-existing SCB global core graph (provider_id 1) for the
    no-clobber comparison: every register/variant/variable row + its slug."""
    return {
        "registers": conn.execute(
            "SELECT register_id, slug, name FROM register WHERE provider_id = 1 "
            "ORDER BY register_id"
        ).fetchall(),
        "variants": conn.execute(
            "SELECT rv.register_variant_id, rv.slug, rv.name FROM register_variant rv "
            "JOIN register r ON rv.register_id = r.register_id "
            "WHERE r.provider_id = 1 ORDER BY rv.register_variant_id"
        ).fetchall(),
        # Every provider_id=1 variable is a pre-existing global row (the overlay
        # inserts ONLY steward-provider rows, never onto SCB), so its slug must
        # be byte-identical after the overlay — the no-clobber guarantee.
        "variables": conn.execute(
            "SELECT v.variable_id, v.slug, v.name FROM variable v "
            "JOIN register r ON v.register_id = r.register_id "
            "WHERE r.provider_id = 1 "
            "ORDER BY v.variable_id"
        ).fetchall(),
    }


# ── slugs ────────────────────────────────────────────────────────────────────


class TestSlugs:
    def test_steward_register_variant_slugged_from_toml(
        self, tmp_path: Path, global_db: Path
    ) -> None:
        _, out = _run_extend(tmp_path, global_db, _base_inventory())
        conn = sqlite3.connect(out)
        ids = _steward_register_ids()
        assert (
            conn.execute(
                "SELECT slug FROM register WHERE register_id = ?", (ids["register"],)
            ).fetchone()[0]
            == "transaktioner"
        )
        assert (
            conn.execute(
                "SELECT slug FROM register_variant WHERE register_variant_id = ?",
                (ids["variant"],),
            ).fetchone()[0]
            == "transaktioner-default"
        )

    def test_new_variables_auto_slugged(self, tmp_path: Path, global_db: Path) -> None:
        _, out = _run_extend(tmp_path, global_db, _base_inventory())
        conn = sqlite3.connect(out)
        ids = _steward_register_ids()
        slugs = {
            r[0]
            for r in conn.execute(
                "SELECT slug FROM variable WHERE register_id = ?", (ids["register"],)
            )
        }
        assert None not in slugs
        assert len(slugs) == 2  # belopp, kontonr both got distinct slugs

    def test_steward_auto_toml_written(self, tmp_path: Path, global_db: Path) -> None:
        inv_path = tmp_path / "inventory.json"
        inv_path.write_text(json.dumps(_base_inventory()), encoding="utf-8")
        slug_dir = tmp_path / "sslug"
        slug_dir.mkdir()
        _write_steward_slug_dir(slug_dir)
        out_dir = tmp_path / "out"
        out_dir.mkdir()
        extend_db(
            base_db=global_db,
            inventory_path=inv_path,
            db_dir=out_dir,
            steward=_STEWARD,
            slug_dir=slug_dir,
        )
        auto_path = slug_dir / f"{_BANK}.auto.toml"
        assert auto_path.is_file()
        assert "[variable" in auto_path.read_text(encoding="utf-8")


# ── FTS rebuild ──────────────────────────────────────────────────────────────


class TestFts:
    def test_new_register_and_variable_searchable(
        self, tmp_path: Path, global_db: Path
    ) -> None:
        _, out = _run_extend(tmp_path, global_db, _base_inventory())
        conn = sqlite3.connect(out)
        # New register name "Transaktioner" indexed.
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM register_fts WHERE register_fts MATCH 'Transaktioner'"
            ).fetchone()[0]
            >= 1
        )
        # New variable name "Belopp" indexed.
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM variable_fts WHERE variable_fts MATCH 'Belopp'"
            ).fetchone()[0]
            >= 1
        )

    def test_no_duplicate_fts_rows(self, tmp_path: Path, global_db: Path) -> None:
        _, out = _run_extend(tmp_path, global_db, _base_inventory())
        conn = sqlite3.connect(out)
        # External-content FTS rows must match the base-table row count exactly
        # (a full rebuild, not a double-insert).
        for tbl, fts in (("register", "register_fts"), ("variable", "variable_fts")):
            assert (
                conn.execute(f"SELECT COUNT(*) FROM {fts}").fetchone()[0]
                == conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            )

    def test_value_code_fts_unchanged(self, tmp_path: Path, global_db: Path) -> None:
        """The overlay never inserts value_code rows, so extend_db SKIPS the
        value_code_fts rebuild — the copied index is already in sync. Assert the
        honest indexed-row count (the `_docsize` shadow table — COUNT(*) on the
        external-content FTS reads `value_code` and can't see a double-insert)
        equals the base DB's, guarding against a regression that re-inserts it."""
        base_n = (
            sqlite3.connect(global_db)
            .execute("SELECT COUNT(*) FROM value_code_fts_docsize")
            .fetchone()[0]
        )
        _, out = _run_extend(tmp_path, global_db, _base_inventory())
        out_n = (
            sqlite3.connect(out)
            .execute("SELECT COUNT(*) FROM value_code_fts_docsize")
            .fetchone()[0]
        )
        assert out_n == base_n


# ── flavored validation ──────────────────────────────────────────────────────


class TestFlavoredValidation:
    def test_overlaid_db_passes_flavored_validation(
        self, tmp_path: Path, global_db: Path
    ) -> None:
        # validate=True runs the flavored validator as the pre_rename_hook; it
        # raises on failure, so reaching here means it passed.
        counts, out = _run_extend(tmp_path, global_db, _base_inventory(), validate=True)
        assert counts["variables"] == 2
        result = validate_built_db(out, flavored=True)
        assert not result.failures, result.failures

    def test_un_minted_steward_id_caught_by_band_check(
        self, tmp_path: Path, global_db: Path
    ) -> None:
        _, out = _run_extend(tmp_path, global_db, _base_inventory())
        conn = sqlite3.connect(out)
        ids = _steward_register_ids()
        # Deliberately corrupt a steward register id into the LOW band — the
        # flavored band check must catch it (non-SCB id < 2^62).
        conn.execute(
            "UPDATE register SET register_id = 5 WHERE register_id = ?",
            (ids["register"],),
        )
        conn.execute(
            "UPDATE register_variant SET register_id = 5 WHERE register_id = ?",
            (ids["register"],),
        )
        conn.execute(
            "UPDATE variable SET register_id = 5 WHERE register_id = ?",
            (ids["register"],),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(out, flavored=True)
        assert any("below the minted band" in f for f in result.failures), (
            result.failures
        )

    def test_low_band_steward_id_passes_non_flavored(
        self, tmp_path: Path, global_db: Path
    ) -> None:
        # #422: the GLOBAL (non-flavored) band check enforces high-band for every
        # SEEDED non-SCB provider (sos + fohm; scb is excluded). A steward
        # provider is dynamically minted, NOT in `_PROVIDER_SEED`, so it stays
        # out of scope of the non-flavored check — its low-band id passes here
        # and is caught only by `flavored=True` (the test above).
        _, out = _run_extend(tmp_path, global_db, _base_inventory())
        conn = sqlite3.connect(out)
        ids = _steward_register_ids()
        conn.execute(
            "UPDATE register SET register_id = 5 WHERE register_id = ?",
            (ids["register"],),
        )
        conn.execute(
            "UPDATE register_variant SET register_id = 5 WHERE register_id = ?",
            (ids["register"],),
        )
        conn.execute(
            "UPDATE variable SET register_id = 5 WHERE register_id = ?",
            (ids["register"],),
        )
        conn.commit()
        conn.close()
        result = validate_built_db(out, flavored=False)
        assert not any("below the minted band" in f for f in result.failures)


# ── idempotent re-run ────────────────────────────────────────────────────────


class TestIdempotentReRun:
    def test_two_runs_onto_fresh_copies_give_identical_counts_and_ids(
        self, tmp_path: Path, global_db: Path
    ) -> None:
        counts_a, out_a = _run_extend(
            tmp_path, global_db, _base_inventory(), out_name="a"
        )
        counts_b, out_b = _run_extend(
            tmp_path, global_db, _base_inventory(), out_name="b"
        )
        # `db_path` is the per-run output dir; compare only the integer counts.
        del counts_a["db_path"], counts_b["db_path"]
        assert counts_a == counts_b
        ids_a = _minted_overlay_ids(sqlite3.connect(out_a))
        ids_b = _minted_overlay_ids(sqlite3.connect(out_b))
        assert ids_a == ids_b


def _minted_overlay_ids(conn: sqlite3.Connection) -> dict:
    """All overlay-inserted ids — steward-only, so every one is high-band
    (provider_id != SCB / variable_id >= 2^62). Deterministic across runs."""
    return {
        "registers": conn.execute(
            "SELECT register_id FROM register WHERE provider_id != 1 ORDER BY register_id"
        ).fetchall(),
        "variables": conn.execute(
            "SELECT variable_id FROM variable WHERE variable_id >= ? "
            "ORDER BY variable_id",
            (_MINT_BIT,),
        ).fetchall(),
    }


# ── populate_variable_slugs(incremental=False) unchanged ─────────────────────


class TestIncrementalFlagDefault:
    def test_non_incremental_processes_all_variables(self, tmp_path: Path) -> None:
        """incremental=False (the global build path) must derive a slug for EVERY
        variable, including ones whose slug is already set — proving the
        `AND v.slug IS NULL` filter is gated behind the flag and the global build
        path is untouched."""
        from reg_meta_build.db import DDL, seed_providers

        conn = sqlite3.connect(":memory:")
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name, slug) "
            "VALUES (1, 1, 'R', 'r')"
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, name, slug) "
            "VALUES (10, 1, 'V', 'v')"
        )
        # Two variables: one already-slugged, one NULL.
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, name, slug) "
            "VALUES (1, 1, '100', 'Alpha', 'preexisting')"
        )
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, name) "
            "VALUES (2, 1, '200', 'Beta')"
        )
        for vid in (1, 2):
            conn.execute(
                "INSERT INTO variable_state (variable_id, register_variant_id, "
                "valid_from, valid_to, data_type, delivery_column_name) "
                "VALUES (?, 10, '2018-01-01', '9999-12-31', 'int', ?)",
                (vid, f"COL{vid}"),
            )
        conn.commit()

        slug_dir = tmp_path / "slug"
        slug_dir.mkdir()
        # Non-incremental rewrites the already-slugged variable too (its prior
        # slug isn't in any auto.toml, so it re-derives → 2 auto_new).
        counts = populate_variable_slugs(conn, slug_dir, incremental=False)
        assert counts["auto_new"] == 2

    def test_incremental_processes_only_null_slug_variables(
        self, tmp_path: Path
    ) -> None:
        from reg_meta_build.db import DDL, seed_providers

        conn = sqlite3.connect(":memory:")
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name, slug) "
            "VALUES (1, 1, 'R', 'r')"
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, name, slug) "
            "VALUES (10, 1, 'V', 'v')"
        )
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, name, slug) "
            "VALUES (1, 1, '100', 'Alpha', 'preexisting')"
        )
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, name) "
            "VALUES (2, 1, '200', 'Beta')"
        )
        for vid in (1, 2):
            conn.execute(
                "INSERT INTO variable_state (variable_id, register_variant_id, "
                "valid_from, valid_to, data_type, delivery_column_name) "
                "VALUES (?, 10, '2018-01-01', '9999-12-31', 'int', ?)",
                (vid, f"COL{vid}"),
            )
        conn.commit()

        slug_dir = tmp_path / "slug"
        slug_dir.mkdir()
        counts = populate_variable_slugs(conn, slug_dir, incremental=True)
        # Only the NULL-slug variable (id 2) is processed.
        assert counts["auto_new"] == 1
        assert (
            conn.execute("SELECT slug FROM variable WHERE variable_id = 1").fetchone()[
                0
            ]
            == "preexisting"  # untouched
        )
        assert (
            conn.execute("SELECT slug FROM variable WHERE variable_id = 2").fetchone()[
                0
            ]
            is not None
        )

    def test_incremental_skips_providers_with_no_null_slug_variables(
        self, tmp_path: Path
    ) -> None:
        # #365 PR2 perf: under incremental=True, a provider whose variables are
        # ALL already slugged must not be processed at all. Observable via its
        # `<provider>.auto.toml` NOT being written (only a processed provider
        # with first-sight slugs writes it) and its slugs staying untouched.
        from reg_meta_build.db import DDL, seed_providers

        conn = sqlite3.connect(":memory:")
        conn.executescript(DDL)
        seed_providers(conn)
        # SCB (provider 1): one variable, already slugged → no NULL rows.
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name, slug) "
            "VALUES (1, 1, 'R', 'r')"
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, name, slug) "
            "VALUES (10, 1, 'V', 'v')"
        )
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, name, slug) "
            "VALUES (1, 1, '100', 'Alpha', 'preexisting')"
        )
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, "
            "valid_from, valid_to, data_type, delivery_column_name) "
            "VALUES (1, 10, '2018-01-01', '9999-12-31', 'int', 'COL1')"
        )
        # A steward provider (high-band id) with one NULL-slug variable.
        bank_pid = mint("provider", "bankx")
        conn.execute(
            "INSERT INTO provider (provider_id, slug, name) VALUES (?, 'bankx', 'Bank X')",
            (bank_pid,),
        )
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name, slug) "
            "VALUES (2000, ?, 'BR', 'br')",
            (bank_pid,),
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, name, slug) "
            "VALUES (2010, 2000, 'BV', 'bv')"
        )
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, name) "
            "VALUES (2001, 2000, 'belopp', 'Belopp')"
        )
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, "
            "valid_from, valid_to, data_type, delivery_column_name) "
            "VALUES (2001, 2010, '2018-01-01', '9999-12-31', 'float', 'BELOPP')"
        )
        conn.commit()

        slug_dir = tmp_path / "slug"
        slug_dir.mkdir()
        counts = populate_variable_slugs(conn, slug_dir, incremental=True)
        # Only the steward NULL-slug variable was processed.
        assert counts["auto_new"] == 1
        # SCB (no NULL rows) was skipped → its auto.toml is never written.
        assert not (slug_dir / "scb.auto.toml").exists()
        assert (slug_dir / "bankx.auto.toml").exists()
        # SCB's published slug is untouched.
        assert (
            conn.execute("SELECT slug FROM variable WHERE variable_id = 1").fetchone()[
                0
            ]
            == "preexisting"
        )

    def test_incremental_uniquifies_against_published_slug(
        self, tmp_path: Path
    ) -> None:
        """A new variable whose auto-derived slug collides with a PUBLISHED global
        slug in the same register must get a `-N` suffix, not raise on
        UNIQUE(register_id, slug)."""
        from reg_meta.fqid import derive_variable_slug
        from reg_meta_build.db import DDL, seed_providers

        conn = sqlite3.connect(":memory:")
        conn.executescript(DDL)
        seed_providers(conn)
        conn.execute(
            "INSERT INTO register (register_id, provider_id, name, slug) "
            "VALUES (1, 1, 'R', 'r')"
        )
        conn.execute(
            "INSERT INTO register_variant (register_variant_id, register_id, name, slug) "
            "VALUES (10, 1, 'V', 'v')"
        )
        published = derive_variable_slug("KON")
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, name, slug) "
            "VALUES (1, 1, '100', 'Alpha', ?)",
            (published,),
        )
        # New variable delivers the SAME column KON AND has a name that slugs to
        # the same base, so both the kolumnnamn and name fallback arms collide
        # with the published slug — forcing the `-N` suffix that proves the
        # `used` set was seeded with the published global slug.
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, name) "
            "VALUES (2, 1, '200', 'Kön')"
        )
        conn.execute(
            "INSERT INTO variable_state (variable_id, register_variant_id, "
            "valid_from, valid_to, data_type, delivery_column_name) "
            "VALUES (2, 10, '2018-01-01', '9999-12-31', 'int', 'KON')"
        )
        conn.commit()

        slug_dir = tmp_path / "slug"
        slug_dir.mkdir()
        # Without the `used` seeding this would raise UNIQUE(register_id, slug).
        populate_variable_slugs(conn, slug_dir, incremental=True)
        new_slug = conn.execute(
            "SELECT slug FROM variable WHERE variable_id = 2"
        ).fetchone()[0]
        assert new_slug != published
        assert new_slug.startswith(published)  # `-N` suffix variant
        # The published slug on the pre-existing variable is untouched.
        assert (
            conn.execute("SELECT slug FROM variable WHERE variable_id = 1").fetchone()[
                0
            ]
            == published
        )


# ── loader strictness ────────────────────────────────────────────────────────


class TestLoader:
    def test_minimal_inventory_parses(self, tmp_path: Path) -> None:
        inv = {"steward": "swecov", "source_label": "x"}
        path = tmp_path / "inv.json"
        path.write_text(json.dumps(inv), encoding="utf-8")
        parsed = load_inventory(path)
        assert parsed.steward == "swecov"
        assert parsed.registers == ()

    @pytest.mark.parametrize(
        "mutate",
        [
            lambda d: d.pop("steward"),
            lambda d: d.pop("source_label"),
            lambda d: d.update(unexpected_key=1),
            # grafts/aliases are now UNKNOWN top-level keys (trimmed to global
            # build) — the strict top-level check must reject them.
            lambda d: d.update(grafts=[]),
            lambda d: d.update(aliases=[]),
            lambda d: d.update(
                registers=[{"provider": "p", "key": "k", "name": "N", "variants": []}]
            ),  # empty variants
            lambda d: d.update(
                registers=[
                    {
                        "provider": "p",
                        "key": "k",
                        "name": "N",
                        "variants": [{"key": "v", "name": "V", "variables": []}],
                    }
                ]
            ),  # empty variables
            lambda d: d.update(providers=[{"slug": "p"}]),  # missing name
            # A non-bool `is_identifier` on a variable must be rejected (the
            # field is INTEGER-flag-backed; a stray int is a generator bug).
            lambda d: d.update(
                registers=[
                    {
                        "provider": "p",
                        "key": "k",
                        "name": "N",
                        "variants": [
                            {
                                "key": "v",
                                "name": "V",
                                "variables": [
                                    {
                                        "key": "x",
                                        "name": "X",
                                        "column": "C",
                                        "is_identifier": 1,
                                    }
                                ],
                            }
                        ],
                    }
                ]
            ),  # non-bool is_identifier
            # Duplicate provider slug.
            lambda d: d.update(
                providers=[
                    {"slug": "p", "name": "P"},
                    {"slug": "p", "name": "P2"},
                ]
            ),
            # Duplicate (provider, register key).
            lambda d: d.update(
                registers=[
                    _reg_stub("p", "k"),
                    _reg_stub("p", "k"),
                ]
            ),
        ],
    )
    def test_structural_defects_fail(self, tmp_path: Path, mutate) -> None:
        inv = {"steward": "swecov", "source_label": "x"}
        mutate(inv)
        path = tmp_path / "inv.json"
        path.write_text(json.dumps(inv), encoding="utf-8")
        with pytest.raises(RegMetaError) as exc:
            load_inventory(path)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_non_object_root_is_config_error(self, tmp_path: Path) -> None:
        # A JSON array (or any non-object) root must fail strict load.
        path = tmp_path / "inv.json"
        path.write_bytes(b"[]")
        with pytest.raises(RegMetaError) as exc:
            load_inventory(path)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_dotted_variable_key_is_config_error(self, tmp_path: Path) -> None:
        # A variable key with a '.' becomes a provider_key the slug source-ID
        # grammar would mis-parse — rejected at load, not deep in slugging.
        reg = _reg_stub("swedbank", "tx")
        reg["variants"][0]["variables"][0]["key"] = "a.b"
        inv = {"steward": "swecov", "source_label": "x", "registers": [reg]}
        path = tmp_path / "inv.json"
        path.write_text(json.dumps(inv), encoding="utf-8")
        with pytest.raises(RegMetaError) as exc:
            load_inventory(path)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_steward_mismatch_fails(self, tmp_path: Path, global_db: Path) -> None:
        inv = {"steward": "other", "source_label": "x"}
        path = tmp_path / "inv.json"
        path.write_text(json.dumps(inv), encoding="utf-8")
        out = tmp_path / "out"
        out.mkdir()
        with pytest.raises(RegMetaError) as exc:
            extend_db(
                base_db=global_db,
                inventory_path=path,
                db_dir=out,
                steward="swecov",
                skip_slugs=True,
            )
        assert exc.value.exit_code == EXIT_CONFIG


# ── failure / edge paths ─────────────────────────────────────────────────────


def _run_extend_skip_slugs(
    tmp_path: Path, base_db: Path, inventory: dict, *, out_name: str = "out"
) -> tuple[dict, Path]:
    """`extend_db` with `skip_slugs=True` (no slug dir needed) — for failure/edge
    tests that don't exercise slugging."""
    inv_path = tmp_path / f"{out_name}-inventory.json"
    inv_path.write_text(json.dumps(inventory), encoding="utf-8")
    out_dir = tmp_path / out_name
    out_dir.mkdir()
    counts = extend_db(
        base_db=base_db,
        inventory_path=inv_path,
        db_dir=out_dir,
        steward=_STEWARD,
        skip_slugs=True,
    )
    return counts, out_dir / "reg_meta.db"


class TestFailurePaths:
    def test_missing_base_db(self, tmp_path: Path) -> None:
        inv_path = tmp_path / "inv.json"
        inv_path.write_text(json.dumps(_base_inventory()), encoding="utf-8")
        out = tmp_path / "out"
        out.mkdir()
        with pytest.raises(RegMetaError) as exc:
            extend_db(
                base_db=tmp_path / "nonexistent.db",
                inventory_path=inv_path,
                db_dir=out,
                steward=_STEWARD,
                skip_slugs=True,
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "extend_base_db_not_found"

    def test_base_db_equals_output_path(self, tmp_path: Path, global_db: Path) -> None:
        # If --base-db resolves to <db_dir>/reg_meta.db, the end-of-run rotate
        # would move the "read-only" base aside — reject up front.
        import shutil

        out = tmp_path / "out"
        out.mkdir()
        base_in_out = out / "reg_meta.db"
        shutil.copy2(global_db, base_in_out)
        inv_path = tmp_path / "inv.json"
        inv_path.write_text(json.dumps(_base_inventory()), encoding="utf-8")
        with pytest.raises(RegMetaError) as exc:
            extend_db(
                base_db=base_in_out,
                inventory_path=inv_path,
                db_dir=out,
                steward=_STEWARD,
                skip_slugs=True,
            )
        assert exc.value.exit_code == EXIT_CONFIG
        assert exc.value.code == "extend_base_db_is_output"

    def test_register_names_undeclared_provider(
        self, tmp_path: Path, global_db: Path
    ) -> None:
        # A register whose provider is neither declared in `providers[]` nor live
        # in the DB is a hard structural error in `_insert_core_graph`.
        inv = _base_inventory()
        inv["providers"] = []  # drop the declaration; `swedbank` is not live
        with pytest.raises(RegMetaError) as exc:
            _run_extend_skip_slugs(tmp_path, global_db, inv)
        assert exc.value.exit_code == EXIT_CONFIG

    def test_pre_rename_hook_failure_cleans_up(
        self, tmp_path: Path, global_db: Path
    ) -> None:
        inv_path = tmp_path / "inv.json"
        inv_path.write_text(json.dumps(_base_inventory()), encoding="utf-8")
        out = tmp_path / "out"
        out.mkdir()

        class _HookError(RuntimeError):
            pass

        def hook(_staging: Path) -> None:
            raise _HookError("validation refused the overlay")

        with pytest.raises(_HookError):
            extend_db(
                base_db=global_db,
                inventory_path=inv_path,
                db_dir=out,
                steward=_STEWARD,
                skip_slugs=True,
                pre_rename_hook=hook,
            )
        # The staging tmp is removed and no final DB was written (nothing to
        # rotate — this is a first overlay into a fresh dir).
        assert not (out / "reg_meta.db.tmp").exists()
        assert not (out / "reg_meta.db").exists()

    def test_skip_slugs_leaves_null_slugs(
        self, tmp_path: Path, global_db: Path
    ) -> None:
        _, out = _run_extend_skip_slugs(tmp_path, global_db, _base_inventory())
        conn = sqlite3.connect(out)
        ids = _steward_register_ids()
        null_slugs = conn.execute(
            "SELECT COUNT(*) FROM variable WHERE register_id = ? AND slug IS NULL",
            (ids["register"],),
        ).fetchone()[0]
        assert null_slugs == 2  # belopp + kontonr both unslugged

    def test_empty_inventory_is_zero_counts_and_no_change(
        self, tmp_path: Path, global_db: Path
    ) -> None:
        before = _global_snapshot(sqlite3.connect(global_db))
        inv = {"steward": _STEWARD, "source_label": "swecov-empty"}
        counts, out = _run_extend_skip_slugs(tmp_path, global_db, inv)
        for key in ("providers", "registers", "variants", "variables", "states"):
            assert counts[key] == 0
        # Core graph unchanged vs base, and the result validates clean.
        assert _global_snapshot(sqlite3.connect(out)) == before
        assert not validate_built_db(out, flavored=True).failures

    def test_no_wal_sidecars_after_run(self, tmp_path: Path, global_db: Path) -> None:
        # #365 PR2: extend_db reuses db._unlink_wal_sidecars (no local dupe). A
        # successful run must leave no orphaned `-wal`/`-shm` next to the output.
        _, out = _run_extend_skip_slugs(tmp_path, global_db, _base_inventory())
        assert out.exists()
        assert not out.with_name(out.name + "-wal").exists()
        assert not out.with_name(out.name + "-shm").exists()

    def test_unlink_wal_sidecars_is_db_function(self) -> None:
        # The local definition was deleted in favor of importing from .db; assert
        # the symbol used by extend_db IS the db.py one (no silent re-divergence).
        import reg_meta_build.db as _db
        import reg_meta_build.extend_db as _ext

        assert not hasattr(_ext, "_unlink_wal_sidecars")
        assert hasattr(_db, "_unlink_wal_sidecars")

    def test_steward_register_missing_slug_fails(
        self, tmp_path: Path, global_db: Path
    ) -> None:
        # A steward slug dir that slugs the VARIANT but omits the REGISTER entry:
        # populate_slugs(strict=False) leaves the steward register NULL-slug, and
        # the scoped guard must catch it as a clean EXIT_CONFIG (otherwise an
        # unaddressable steward FQID would ship).
        ids = _steward_register_ids()
        slug_dir = tmp_path / "sslug"
        slug_dir.mkdir()
        (slug_dir / "UNFROZEN").write_text("u\n", encoding="utf-8")
        (slug_dir / ".snapshot.json").write_text("{}\n", encoding="utf-8")
        # Variant entry present, register entry DELIBERATELY absent.
        (slug_dir / f"{_BANK}.toml").write_text(
            f'[register_variant."{ids["register"]}.{ids["variant"]}"]\n'
            'slug = "transaktioner-default"\n',
            encoding="utf-8",
        )
        inv_path = tmp_path / "inv.json"
        inv_path.write_text(json.dumps(_base_inventory()), encoding="utf-8")
        out = tmp_path / "out"
        out.mkdir()
        with pytest.raises(RegMetaError) as exc:
            extend_db(
                base_db=global_db,
                inventory_path=inv_path,
                db_dir=out,
                steward=_STEWARD,
                slug_dir=slug_dir,
            )
        assert exc.value.exit_code == EXIT_CONFIG


# ── provider idempotency (_insert_providers) ─────────────────────────────────


class TestProviderIdempotency:
    def test_existing_slug_name_mismatch_fails(self) -> None:
        from reg_meta_build.db import DDL, seed_providers

        conn = sqlite3.connect(":memory:")
        conn.executescript(DDL)
        seed_providers(conn)
        # `scb` is seeded; re-declaring it with a DIFFERENT name must fail.
        with pytest.raises(RegMetaError) as exc:
            _insert_providers(conn, (InvProvider(slug="scb", name="Wrong Name"),))
        assert exc.value.exit_code == EXIT_CONFIG

    def test_existing_slug_matching_name_is_noop(self) -> None:
        from reg_meta_build.db import DDL, seed_providers

        conn = sqlite3.connect(":memory:")
        conn.executescript(DDL)
        seed_providers(conn)
        # Query the real seeded SCB name rather than hardcoding it.
        seeded_name = conn.execute(
            "SELECT name FROM provider WHERE slug = 'scb'"
        ).fetchone()[0]
        before = conn.execute("SELECT COUNT(*) FROM provider").fetchone()[0]
        inserted = _insert_providers(conn, (InvProvider(slug="scb", name=seeded_name),))
        assert inserted == 0  # matching name → skip
        after = conn.execute("SELECT COUNT(*) FROM provider").fetchone()[0]
        assert after == before  # no duplicate row


# ── CLI handler ──────────────────────────────────────────────────────────────


# ── core-graph INSERT column parity (#425) ───────────────────────────────────

# Two writers hand-roll the shipped core-graph INSERTs and their column lists
# must stay identical: `extend_db._insert_core_graph` (steward overlay, per-row
# `conn.execute`) and `db._reinsert_core_graph_from_ir` (materializer sole-writer,
# `conn.executemany`). FOOTGUN: a future *nullable* column added to one writer but
# not the other compiles, runs, and silently leaves the other writer's rows unset
# (no NOT NULL to trip). No shared helper joins them — the two call sites differ
# structurally (per-row positional vs bulk named binds), so a shared writer would
# be an invasive refactor for a parity concern. This test is the chosen lock.

_CORE_GRAPH_TABLES = frozenset(
    {"register", "register_variant", "variable", "variable_state", "variable_alias"}
)

# `variable_alias` uses `INSERT OR IGNORE`, hence the optional clause.
_INSERT_RE = re.compile(
    r"INSERT(?:\s+OR\s+IGNORE)?\s+INTO\s+(\w+)\s*\(([^)]*)\)",
    re.IGNORECASE,
)


def _insert_columns_by_table(func) -> dict[str, frozenset[str]]:
    """Walk ``func``'s source for ``conn.execute``/``conn.executemany`` calls whose
    first positional arg is a string constant, and from each ``INSERT INTO`` string
    extract ``{table: frozenset(columns)}`` for the core-graph tables.

    Both writers spell the SQL as implicitly-concatenated multi-line string
    literals (``"INSERT INTO variable " "(...) "``); Python's parser folds those
    into a single ``ast.Constant``, so the whole INSERT statement reaches us as one
    string — no manual re-joining needed."""
    tree = ast.parse(inspect.getsource(func))
    columns: dict[str, frozenset[str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        callee = node.func
        if not (
            isinstance(callee, ast.Attribute)
            and callee.attr in ("execute", "executemany")
        ):
            continue
        if not node.args:
            continue
        first = node.args[0]
        if not (isinstance(first, ast.Constant) and isinstance(first.value, str)):
            continue
        match = _INSERT_RE.search(first.value)
        if match is None:
            continue
        table = match.group(1)
        if table not in _CORE_GRAPH_TABLES:
            continue
        columns[table] = frozenset(
            col.strip() for col in match.group(2).split(",") if col.strip()
        )
    return columns


class TestCoreGraphInsertParity:
    def test_overlay_and_materializer_insert_same_columns(self) -> None:
        from reg_meta_build.db import _reinsert_core_graph_from_ir
        from reg_meta_build.extend_db import _insert_core_graph

        overlay = _insert_columns_by_table(_insert_core_graph)
        materializer = _insert_columns_by_table(_reinsert_core_graph_from_ir)

        # Both writers must INSERT into every core-graph table (else the parse
        # missed one and the per-table check below would vacuously pass).
        assert set(overlay) == _CORE_GRAPH_TABLES, overlay
        assert set(materializer) == _CORE_GRAPH_TABLES, materializer

        for table in sorted(_CORE_GRAPH_TABLES):
            assert overlay[table] == materializer[table], (
                f"{table}: _insert_core_graph and _reinsert_core_graph_from_ir "
                f"INSERT different columns; symmetric difference "
                f"{set(overlay[table] ^ materializer[table])}"
            )


# ── CLI handler ──────────────────────────────────────────────────────────────


class TestCli:
    def test_cmd_extend_db_envelope(self, tmp_path: Path, global_db: Path) -> None:
        import argparse

        from reg_meta_build.cli import _cmd_extend_db

        inv_path = tmp_path / "inv.json"
        inv_path.write_text(json.dumps(_base_inventory()), encoding="utf-8")
        slug_dir = tmp_path / "sslug"
        slug_dir.mkdir()
        _write_steward_slug_dir(slug_dir)
        out_dir = tmp_path / "out"
        out_dir.mkdir()

        args = argparse.Namespace(
            db=str(out_dir),
            base_db=str(global_db),
            inventory=str(inv_path),
            steward=_STEWARD,
            slug_dir=str(slug_dir),
            skip_slugs=False,
            no_validate=False,
        )
        envelope, exit_code = _cmd_extend_db(args)
        assert exit_code == 0
        data = envelope["data"]
        for key in ("providers", "registers", "variants", "variables", "states"):
            assert key in data
        assert data["variables"] == 2
        assert "db_path" in data
        assert data["db_path"] == str(out_dir / "reg_meta.db")
