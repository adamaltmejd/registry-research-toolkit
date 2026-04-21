"""Tests for the update module (version parsing and release resolution)."""

import sqlite3
from pathlib import Path

import pytest
import zstandard

from regmeta import download
from regmeta.db import DB_FILENAME, SCHEMA_VERSION
from regmeta.download import _is_regmeta_release, _pick_release, version_from_tag
from regmeta.errors import RegmetaError
from regmeta.update import (
    _clear_pending_update,
    _parse_version,
    _set_pending_update,
    read_pending_update,
)


class TestParseVersion:
    """_parse_version produces comparable tuples for X.Y.Z, X.Y.ZaN, X.Y.Z.devN."""

    def test_final_release(self):
        assert _parse_version("0.4.0") == (0, 4, 0, 0, 0)

    def test_alpha(self):
        assert _parse_version("0.5.0a1") == (0, 5, 0, -1, 1)

    def test_dev(self):
        assert _parse_version("0.5.0.dev3") == (0, 5, 0, -2, 3)

    def test_strips_v_prefix(self):
        assert _parse_version("v1.2.3") == (1, 2, 3, 0, 0)

    def test_unparseable_returns_lowest(self):
        assert _parse_version("garbage") == (0, 0, 0, -99, 0)

    @pytest.mark.parametrize(
        "older, newer",
        [
            ("0.4.0.dev1", "0.4.0a1"),
            ("0.4.0a1", "0.4.0"),
            ("0.4.0", "0.5.0"),
            ("0.4.0.dev1", "0.4.0"),
            ("0.4.0a1", "0.4.0a2"),
            ("0.4.0.dev1", "0.4.0.dev2"),
            ("0.4.0", "1.0.0"),
        ],
    )
    def test_ordering(self, older: str, newer: str):
        assert _parse_version(older) < _parse_version(newer)


class TestVersionFromTag:
    def test_prefixed_tag(self):
        assert version_from_tag("regmeta/v0.5.0") == "0.5.0"

    def test_legacy_bare_tag(self):
        assert version_from_tag("v0.4.0") == "0.4.0"

    def test_no_v_prefix(self):
        assert version_from_tag("regmeta/0.5.0") == "0.5.0"


class TestIsRegmetaRelease:
    def test_prefixed_tag(self):
        assert _is_regmeta_release({"tag_name": "regmeta/v0.5.0"})

    def test_legacy_bare_tag(self):
        assert _is_regmeta_release({"tag_name": "v0.4.0"})

    def test_other_package_tag(self):
        assert not _is_regmeta_release({"tag_name": "mock-data-wizard/v0.4.0"})

    def test_non_semver_v_tag(self):
        assert not _is_regmeta_release({"tag_name": "vNext"})


def _release(tag: str, has_db: bool = False) -> dict:
    """Build a minimal GitHub release dict for testing."""
    assets = [{"name": "regmeta.db.zst"}] if has_db else []
    return {"tag_name": tag, "assets": assets}


class TestPickRelease:
    def test_picks_latest_prefixed(self):
        releases = [
            _release("regmeta/v0.6.0"),
            _release("regmeta/v0.5.0", has_db=True),
        ]
        tag, version, db_tag = _pick_release(releases)
        assert tag == "regmeta/v0.6.0"
        assert version == "0.6.0"
        assert db_tag == "regmeta/v0.5.0"

    def test_db_on_latest(self):
        releases = [_release("regmeta/v0.5.0", has_db=True)]
        tag, version, db_tag = _pick_release(releases)
        assert tag == "regmeta/v0.5.0"
        assert db_tag == "regmeta/v0.5.0"

    def test_no_db_in_any_release(self):
        releases = [_release("regmeta/v0.6.0"), _release("regmeta/v0.5.0")]
        tag, _version, db_tag = _pick_release(releases)
        assert tag == "regmeta/v0.6.0"
        assert db_tag is None

    def test_ignores_other_package_tags(self):
        releases = [
            _release("mock-data-wizard/v1.0.0", has_db=True),
            _release("regmeta/v0.5.0"),
        ]
        tag, version, db_tag = _pick_release(releases)
        assert tag == "regmeta/v0.5.0"
        assert version == "0.5.0"
        assert db_tag is None

    def test_legacy_bare_tags(self):
        releases = [_release("v0.4.0", has_db=True)]
        tag, version, db_tag = _pick_release(releases)
        assert tag == "v0.4.0"
        assert version == "0.4.0"
        assert db_tag == "v0.4.0"

    def test_prefers_prefixed_over_legacy(self):
        releases = [
            _release("regmeta/v0.6.0"),
            _release("v0.5.0", has_db=True),
        ]
        tag, version, db_tag = _pick_release(releases)
        assert tag == "regmeta/v0.6.0"
        assert version == "0.6.0"
        assert db_tag == "v0.5.0"

    def test_empty_raises(self):
        with pytest.raises(RegmetaError) as exc_info:
            _pick_release([])
        assert exc_info.value.code == "no_releases"


class TestPendingUpdate:
    """Persistent update-available flag read/write/clear."""

    @pytest.fixture(autouse=True)
    def _isolate_flag(self, monkeypatch, tmp_path):
        flag = tmp_path / ".update_available"
        monkeypatch.setattr("regmeta.update._update_available_path", lambda: flag)
        self.flag_path = flag

    def test_roundtrip(self):
        assert read_pending_update() is None
        _set_pending_update("0.7.0")
        assert read_pending_update() == "0.7.0"

    def test_clear(self):
        _set_pending_update("0.7.0")
        _clear_pending_update()
        assert read_pending_update() is None

    def test_clear_when_missing(self):
        _clear_pending_update()  # should not raise


def _write_fake_db_zst(dest_zst: Path, schema_version: str) -> None:
    """Build a minimal sqlite DB with the given schema_version and zstd it to dest."""
    db_path = dest_zst.with_suffix(".db.source")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE import_manifest (key TEXT PRIMARY KEY, value TEXT)")
    conn.execute(
        "INSERT INTO import_manifest VALUES ('schema_version', ?)", (schema_version,)
    )
    conn.commit()
    conn.close()
    cctx = zstandard.ZstdCompressor()
    with db_path.open("rb") as src, dest_zst.open("wb") as out:
        cctx.copy_stream(src, out)
    db_path.unlink()


class TestDownloadDbSchemaGuard:
    """download_db refuses to overwrite an existing DB with an incompatible asset."""

    def _patch_download(
        self, monkeypatch: pytest.MonkeyPatch, schema_version: str
    ) -> None:
        """Replace the network download with a local zstd-ed DB having *schema_version*."""

        def fake_download(url: str, dest: Path) -> None:
            _write_fake_db_zst(dest, schema_version)

        monkeypatch.setattr(download, "_download_file", fake_download)

    def test_incompatible_asset_aborts_without_overwriting(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Stale DB asset (old minor) must not replace a working local DB."""
        db_dir = tmp_path / "share"
        db_dir.mkdir()
        existing = db_dir / DB_FILENAME
        existing.write_bytes(b"existing-db-sentinel")

        major, minor = (int(x) for x in SCHEMA_VERSION.split(".")[:2])
        if minor == 0:
            pytest.skip("requires a non-zero minor in SCHEMA_VERSION")
        self._patch_download(monkeypatch, f"{major}.{minor - 1}.0")

        with pytest.raises(RegmetaError) as exc_info:
            download.download_db(
                db_dir=db_dir, tag="regmeta/vX.Y.Z", force=True, yes=True
            )
        assert exc_info.value.code == "incompatible_db_asset"
        # Existing DB left untouched.
        assert existing.read_bytes() == b"existing-db-sentinel"
        # No tmp files leftover.
        assert not (db_dir / "regmeta.db.tmp").exists()
        assert not (db_dir / "regmeta.db.zst.tmp").exists()

    def test_compatible_asset_replaces_existing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Matching schema version is installed normally."""
        db_dir = tmp_path / "share"
        db_dir.mkdir()
        existing = db_dir / DB_FILENAME
        existing.write_bytes(b"existing-db-sentinel")

        self._patch_download(monkeypatch, SCHEMA_VERSION)

        result = download.download_db(
            db_dir=db_dir, tag="regmeta/vX.Y.Z", force=True, yes=True
        )
        assert result["tag"] == "regmeta/vX.Y.Z"
        assert existing.exists()
        assert existing.read_bytes() != b"existing-db-sentinel"
