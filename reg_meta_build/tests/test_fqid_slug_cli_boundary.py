"""`reg-meta-build seed-slugs` driven through `cli.run` against a file DB: exit code,
stdout JSON, the `_default` hint block on stderr, and the generated pin files. The
pin keys are proven on a pipeline-built catalog, whose register ids are surrogates."""

from __future__ import annotations

import json
import sqlite3
import tomllib
from typing import TYPE_CHECKING

from _csv_fixtures import var_row, write_scb_input
from _fqid_slug_support import write_register_file
from _pipeline_catalog_support import (
    CatalogFixture,
    built_db_dir,
    prepare_accepted,
    write_curation_tree,
)
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
    """Run seed-slugs into `out`, which holds both registers' files; `flags` go
    after the subcommand (the global `--quiet` is reordered by the CLI)."""
    write_register_file(out, "lisa", "1")
    write_register_file(out, "komvux", "42")
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
    # The register is named by its slug, never by the catalog's surrogate
    # `<register_id>.<variant_id>`, which no curation file carries.
    assert "scb/komvux" in err
    assert "42.124" not in err
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
    assert "scb/komvux" in err
    quiet = (tmp_path / "quiet").joinpath(*_AUTO).read_bytes()
    assert (tmp_path / "loud").joinpath(*_AUTO).read_bytes() == quiet


def test_pipeline_catalog_pins_key_on_native_register_id(
    catalog: CatalogFixture, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The built catalog keeps no native register id, so the pin prefix comes from
    the register file. Fails if seed-slugs keys pins by the catalog's surrogate
    ``register_id`` (#1215): the build then rejects the pin as not belonging to
    register ``1``."""
    db_dir = built_db_dir(catalog, tmp_path)
    register = catalog.curation / "registers" / "scb" / "sample.toml"
    # Drop the authored variable pin so its slug is left to the generated file.
    register.write_text(
        register.read_text(encoding="utf-8").split("[[variable]]", 1)[0],
        encoding="utf-8",
    )

    code = run(["--db", str(db_dir), "seed-slugs", "--out-dir", str(catalog.curation)])

    assert code == 0
    pins = register.with_name("sample.auto.toml").read_text(encoding="utf-8")
    assert tomllib.loads(pins) == {
        "variable": [{"native_id": "1.101", "slug": "value"}]
    }


def test_pipeline_catalog_partition_owners_key_on_their_split_ids(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A partition owner's provider_key carries its split id (`5.first`), so its
    pin key is its authored native id `1.5.first`: both owners are already pinned
    and seed-slugs generates nothing. Fails if the shared source-id derivation
    refuses a dotted provider_key (as it did before #1222) or re-derives a split
    discriminator, which would emit pins under ids no register entry carries."""
    source = tmp_path / "source"
    columns = ("FIRST", "SECOND")
    write_scb_input(
        source,
        registerinformation_rows=[
            var_row(cvid=cvid, var_id=5, colname=column, register=("TEST", 1, 2))
            for cvid, column in enumerate(columns, 10)
        ],
        unika_rows=[
            f"TEST|Testregistret|Individer|Individer|GenericVar|{column}|2020|2020|0|0|0"
            for column in columns
        ],
        include=("registerinformation", "unika"),
    )
    curation = write_curation_tree(
        tmp_path / "curation",
        {
            "registers/scb/sample.toml": (
                '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
                '[[variant]]\nnative_id = "1.2"\nslug = "people"\n'
                '[[variable]]\nnative_id = "1.5.first"\nslug = "first"\n'
                '[[variable]]\nnative_id = "1.5.second"\nslug = "second"\n'
                '[[identity.partition]]\nvariable = "1.5"\n'
                'columns = { FIRST = "1.5.first", SECOND = "1.5.second" }\n'
                'columns_ref = "fixture"\n'
            )
        },
    )
    fixture = CatalogFixture(*prepare_accepted(tmp_path, source), curation)
    db_dir = built_db_dir(fixture, tmp_path)

    code = run(["--db", str(db_dir), "seed-slugs", "--out-dir", str(curation)])

    assert code == 0, capsys.readouterr().out
    pins = curation / "registers" / "scb" / "sample.auto.toml"
    assert tomllib.loads(pins.read_text(encoding="utf-8")) == {}


def test_pipeline_catalog_register_without_file_refused(
    catalog: CatalogFixture, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """With no register file to name its native id, a register cannot be pinned:
    seed-slugs refuses and writes nothing. Fails if it falls back to the
    surrogate id or skips the register silently."""
    db_dir = built_db_dir(catalog, tmp_path)
    out = tmp_path / "empty"

    code = run(["--db", str(db_dir), "seed-slugs", "--out-dir", str(out)])

    assert code == 10
    error = json.loads(capsys.readouterr().out)["error"]
    assert error["code"] == "slug_seed_register_unknown"
    assert "scb/sample" in error["message"]
    assert not list(out.rglob("*.auto.toml"))
