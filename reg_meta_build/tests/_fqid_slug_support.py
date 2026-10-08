"""Shared helpers for the test_fqid_slugs_*.py modules."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from _slugged_db import (
    build_slugged_db,
)
from reg_meta_build.cli import run

from reg_meta_build.fqid_slugs import (
    FREEZE_STATE_FILE,
)

if TYPE_CHECKING:
    import sqlite3
    from pathlib import Path

    import pytest


def write_text_file(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


def write_register_file(
    root: Path, slug: str, native_id: str, *, provider: str = "scb", body: str = ""
) -> Path:
    """Author a register file under ``root``; seed-slugs reads the register's
    native id from it."""
    path = root / "registers" / provider / f"{slug}.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f'[register]\nprovider = "{provider}"\nslug = "{slug}"\n'
        f'native_id = "{native_id}"\n{body}',
        encoding="utf-8",
    )
    return path


def run_precheck(
    db_dir: Path,
    slug_dir: Path,
    capsys: pytest.CaptureFixture[str],
    *,
    update: bool = True,
) -> tuple[int, dict]:
    """Run ``precheck-slugs`` (``--update-snapshot`` by default); return the exit
    code and the JSON payload."""
    capsys.readouterr()
    args = ["--db", str(db_dir), "precheck-slugs", "--slug-dir", str(slug_dir)]
    code = run([*args, "--update-snapshot"] if update else args)
    return code, json.loads(capsys.readouterr().out)


def assert_precheck_clean(code: int, data: dict) -> None:
    """Exit 0, and every catalog row is pinned by an entry that names a row."""
    assert {
        key: data[key]
        for key in (
            "missing_registers",
            "missing_variants",
            "stale_registers",
            "stale_variants",
        )
    } == {
        "missing_registers": [],
        "missing_variants": [],
        "stale_registers": [],
        "stale_variants": [],
    }
    assert code == 0


class PopulateVariableSlugsHelpers:
    """DB, slug-dir and stored-slug helpers shared by the two halves of
    `TestPopulateVariableSlugs` (variable slugs and drift markers)."""

    @staticmethod
    def _db(
        *, slug: str | None = None, kol: str = "Kon", name: str = "Kön"
    ) -> sqlite3.Connection:
        # build_slugged_db seeds variable.slug + a variable_state era carrying
        # `kol`; NULL the stored slug so the population function does the work.
        conn = build_slugged_db(variable=(name, 44, 1001, kol), variable_slug=slug)
        if slug is None:
            conn.execute("UPDATE variable SET slug = NULL")
            conn.commit()
        return conn

    @staticmethod
    def _slug_dir(tmp_path: Path, scb_body: str = "", *, scb_freeze: str = "") -> Path:
        (tmp_path / "scb.toml").write_text(scb_body, encoding="utf-8")
        # #470: by default no freeze.toml ⇒ the `scb` zone is churning (auto
        # slugs regenerate each build). A test exercising the pinned-auto
        # behavior (curating/frozen) passes `scb_freeze=`.
        if scb_freeze:
            (tmp_path / FREEZE_STATE_FILE).write_text(
                f'scb = "{scb_freeze}"\n', encoding="utf-8"
            )
        return tmp_path

    def _stored_slug(self, conn: sqlite3.Connection, var_id: int) -> str | None:
        row = conn.execute(
            "SELECT slug FROM variable WHERE provider_key = CAST(? AS TEXT)",
            (var_id,),
        ).fetchone()
        return row[0] if row else None


def add_single_variant_register(
    conn: sqlite3.Connection,
    *,
    register_id: int,
    register_slug: str,
    register_variant_id: int,
    name: str,
    variant_name: str,
    variant_slug: str | None,
    provider_id: int = 1,
) -> None:
    """Insert a register with exactly one variant for iter-candidate tests."""
    conn.execute(
        "INSERT INTO register (register_id, provider_id, slug, name) "
        "VALUES (?, ?, ?, ?)",
        (register_id, provider_id, register_slug, name),
    )
    conn.execute(
        "INSERT INTO register_variant "
        "(register_variant_id, register_id, slug, name) "
        "VALUES (?, ?, ?, ?)",
        (register_variant_id, register_id, variant_slug, variant_name),
    )
