"""`reg-meta-build seed-slugs` driven through `cli.run` against a file DB: exit code,
stdout JSON, the `_default` hint block on stderr, and the generated pin files."""

from __future__ import annotations

import json
import sqlite3
from typing import TYPE_CHECKING

from _slugged_db import add_register, add_variant, build_slugged_db
from reg_meta_build.cli import run
from reg_meta_build.db import SCHEMA_VERSION

if TYPE_CHECKING:
    from pathlib import Path

    import pytest

_AUTO = ("registers", "scb", "lisa.auto.toml")


def _db_dir(tmp_path: Path) -> Path:
    """LISA plus one single-variant register whose variant name mirrors the
    register name (a `_default` candidate), stamped with the builder schema and
    copied to `<dir>/reg_meta.db` (the sqlite backup API stalls on an open write
    transaction, so commit first)."""
    conn = build_slugged_db()
    add_register(conn, register_id=42, slug="komvux", name="Nybörjare i Komvux")
    add_variant(
        conn,
        register_variant_id=124,
        register_id=42,
        slug="nyborjare",
        name="Nybörjare i Komvux",
    )
    conn.execute(
        "INSERT INTO import_manifest (key, value) VALUES ('schema_version', ?)",
        (SCHEMA_VERSION,),
    )
    conn.commit()
    db_dir = tmp_path / "db"
    db_dir.mkdir()
    dest = sqlite3.connect(db_dir / "reg_meta.db")
    conn.backup(dest)
    dest.close()
    conn.close()
    return db_dir


def _seed(
    capsys: pytest.CaptureFixture[str], db_dir: Path, out: Path, *flags: str
) -> tuple[int, dict, str]:
    """Run seed-slugs; `flags` go after the subcommand (the global `--quiet` is
    reordered by the CLI)."""
    code = run(["--db", str(db_dir), "seed-slugs", "--out-dir", str(out), *flags])
    captured = capsys.readouterr()
    return code, json.loads(captured.out), captured.err


def test_hint_names_default_candidate_on_stderr(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out = tmp_path / "out"
    code, data, err = _seed(capsys, _db_dir(tmp_path), out)
    assert code == 0
    assert data["files"] == ["registers/scb/komvux.auto.toml", "/".join(_AUTO)]
    assert "single-variant register(s)" in err
    assert "_default" in err
    assert "scb/42.124" in err
    # The hint is advice only: it never reaches the generated pins.
    body = out.joinpath(*_AUTO).read_text(encoding="utf-8")
    assert "Hint:" not in body
    assert "_default" not in body


def test_quiet_flag_suppresses_hint(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    code, _data, err = _seed(capsys, _db_dir(tmp_path), tmp_path / "out", "--quiet")
    assert code == 0
    assert err == ""


def test_quiet_env_suppresses_hint(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("REG_META_QUIET", "1")
    code, _data, err = _seed(capsys, _db_dir(tmp_path), tmp_path / "out")
    assert code == 0
    assert err == ""


def test_pins_byte_identical_with_or_without_hint(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db_dir = _db_dir(tmp_path)
    _seed(capsys, db_dir, tmp_path / "quiet", "--quiet")
    _code, _data, err = _seed(capsys, db_dir, tmp_path / "loud", "--all-hints")
    assert "scb/42.124" in err
    quiet = (tmp_path / "quiet").joinpath(*_AUTO).read_bytes()
    assert (tmp_path / "loud").joinpath(*_AUTO).read_bytes() == quiet
