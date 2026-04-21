"""Tests for the update module (version parsing and release resolution)."""

import sqlite3
from pathlib import Path

import pytest
import zstandard

from regmeta import download
from regmeta.db import DB_FILENAME, SCHEMA_VERSION
from regmeta.doc_db import (
    DOC_DB_FILENAME,
    DOC_SCHEMA_VERSION,
)
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


def _release(tag: str, *, has_db: bool = False, has_docs: bool = False) -> dict:
    """Build a minimal GitHub release dict for testing."""
    assets: list[dict] = []
    if has_db:
        assets.append({"name": "regmeta.db.zst"})
    if has_docs:
        assets.append({"name": "regmeta_docs.db.zst"})
    return {"tag_name": tag, "assets": assets}


class TestPickRelease:
    def test_picks_latest_prefixed(self):
        releases = [
            _release("regmeta/v0.6.0"),
            _release("regmeta/v0.5.0", has_db=True, has_docs=True),
        ]
        resolution = _pick_release(releases)
        assert resolution.release_tag == "regmeta/v0.6.0"
        assert resolution.version == "0.6.0"
        assert resolution.db_tag == "regmeta/v0.5.0"
        assert resolution.docs_tag == "regmeta/v0.5.0"

    def test_db_on_latest(self):
        releases = [_release("regmeta/v0.5.0", has_db=True, has_docs=True)]
        resolution = _pick_release(releases)
        assert resolution.release_tag == "regmeta/v0.5.0"
        assert resolution.db_tag == "regmeta/v0.5.0"
        assert resolution.docs_tag == "regmeta/v0.5.0"

    def test_no_db_in_any_release(self):
        releases = [_release("regmeta/v0.6.0"), _release("regmeta/v0.5.0")]
        resolution = _pick_release(releases)
        assert resolution.release_tag == "regmeta/v0.6.0"
        assert resolution.db_tag is None
        assert resolution.docs_tag is None

    def test_ignores_other_package_tags(self):
        releases = [
            _release("mock-data-wizard/v1.0.0", has_db=True, has_docs=True),
            _release("regmeta/v0.5.0"),
        ]
        resolution = _pick_release(releases)
        assert resolution.release_tag == "regmeta/v0.5.0"
        assert resolution.version == "0.5.0"
        assert resolution.db_tag is None
        assert resolution.docs_tag is None

    def test_legacy_bare_tags(self):
        releases = [_release("v0.4.0", has_db=True, has_docs=True)]
        resolution = _pick_release(releases)
        assert resolution.release_tag == "v0.4.0"
        assert resolution.version == "0.4.0"
        assert resolution.db_tag == "v0.4.0"
        assert resolution.docs_tag == "v0.4.0"

    def test_prefers_prefixed_over_legacy(self):
        releases = [
            _release("regmeta/v0.6.0"),
            _release("v0.5.0", has_db=True, has_docs=True),
        ]
        resolution = _pick_release(releases)
        assert resolution.release_tag == "regmeta/v0.6.0"
        assert resolution.version == "0.6.0"
        assert resolution.db_tag == "v0.5.0"
        assert resolution.docs_tag == "v0.5.0"

    def test_db_and_docs_tracked_independently(self):
        """Walker picks the newest release per asset — asset presence varies by release."""
        releases = [
            _release("regmeta/v0.8.0"),  # no assets
            _release("regmeta/v0.7.0", has_docs=True),  # docs-only refresh
            _release("regmeta/v0.6.0", has_db=True),  # main-db-only (schema bump)
        ]
        resolution = _pick_release(releases)
        assert resolution.release_tag == "regmeta/v0.8.0"
        assert resolution.db_tag == "regmeta/v0.6.0"
        assert resolution.docs_tag == "regmeta/v0.7.0"

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


def _write_fake_docs_db_zst(dest_zst: Path, schema_version: str) -> None:
    """Build a minimal doc DB with given schema_version and zstd it to dest."""
    from regmeta.doc_db import DOC_DDL

    db_path = dest_zst.with_suffix(".db.source")
    conn = sqlite3.connect(db_path)
    conn.executescript(DOC_DDL)
    conn.execute(
        "INSERT INTO doc_meta (key, value) VALUES ('schema_version', ?)",
        (schema_version,),
    )
    conn.commit()
    conn.close()
    cctx = zstandard.ZstdCompressor()
    with db_path.open("rb") as src, dest_zst.open("wb") as out:
        cctx.copy_stream(src, out)
    db_path.unlink()


class TestDownloadDocsDbSchemaGuard:
    """download_docs_db refuses to overwrite an existing doc DB with an incompatible asset."""

    def _patch_download(
        self, monkeypatch: pytest.MonkeyPatch, schema_version: str
    ) -> None:
        def fake_download(url: str, dest: Path) -> None:
            _write_fake_docs_db_zst(dest, schema_version)

        monkeypatch.setattr(download, "_download_file", fake_download)

    def test_incompatible_docs_asset_aborts_without_overwriting(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        db_dir = tmp_path / "share"
        db_dir.mkdir()
        existing = db_dir / DOC_DB_FILENAME
        existing.write_bytes(b"existing-docs-sentinel")

        major, minor = (int(x) for x in DOC_SCHEMA_VERSION.split(".")[:2])
        # Force incompatibility by claiming a different major version.
        self._patch_download(monkeypatch, f"{major + 1}.0.0")

        with pytest.raises(RegmetaError) as exc_info:
            download.download_docs_db(db_dir=db_dir, tag="regmeta/vX.Y.Z", force=True)
        assert exc_info.value.code == "incompatible_docs_asset"
        assert existing.read_bytes() == b"existing-docs-sentinel"
        assert not (db_dir / "regmeta_docs.db.tmp").exists()
        assert not (db_dir / "regmeta_docs.db.zst.tmp").exists()

    def test_compatible_docs_asset_replaces_existing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        db_dir = tmp_path / "share"
        db_dir.mkdir()
        existing = db_dir / DOC_DB_FILENAME
        existing.write_bytes(b"existing-docs-sentinel")

        self._patch_download(monkeypatch, DOC_SCHEMA_VERSION)

        result = download.download_docs_db(
            db_dir=db_dir, tag="regmeta/vX.Y.Z", force=True
        )
        assert result["tag"] == "regmeta/vX.Y.Z"
        assert existing.exists()
        assert existing.read_bytes() != b"existing-docs-sentinel"
        # .docs_source written so the walker can detect future updates.
        assert (db_dir / ".docs_source").exists()

    def test_no_docs_asset_in_release_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """tag=latest when walker finds no doc asset raises no_docs_in_release."""
        from regmeta.download import ReleaseResolution

        def fake_resolve(*, timeout: float = 15) -> ReleaseResolution:
            return ReleaseResolution(
                release_tag="regmeta/v0.7.0",
                version="0.7.0",
                db_tag="regmeta/v0.7.0",
                docs_tag=None,
            )

        monkeypatch.setattr(download, "resolve_latest_release", fake_resolve)
        db_dir = tmp_path / "share"
        db_dir.mkdir()

        with pytest.raises(RegmetaError) as exc_info:
            download.download_docs_db(db_dir=db_dir, tag="latest")
        assert exc_info.value.code == "no_docs_in_release"


class TestRunUpdateFailFast:
    """run_update must not leave the install in a broken state.

    If the walker can't resolve an asset the user doesn't already have,
    maintain update raises rather than reporting success — otherwise
    query commands would fail with db_not_found/doc_db_not_found on the
    very next invocation while `maintain update` claimed to succeed.
    """

    def _fake_resolve(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        db_tag: str | None,
        docs_tag: str | None,
    ) -> None:
        # Match __version__ so run_update skips the package-upgrade branch
        # entirely — we're testing asset-resolution behaviour, not uv.
        # (CI doesn't have regmeta installed as a uv tool, so invoking
        # `uv tool upgrade regmeta` would fail before any assertion.)
        from regmeta import __version__, update
        from regmeta.download import ReleaseResolution

        def fake_resolve(*, timeout: float = 15) -> ReleaseResolution:
            return ReleaseResolution(
                release_tag=f"regmeta/v{__version__}",
                version=__version__,
                db_tag=db_tag,
                docs_tag=docs_tag,
            )

        monkeypatch.setattr(update, "resolve_latest_release", fake_resolve)
        monkeypatch.setattr(
            update, "fetch_pypi_latest_version", lambda *, timeout=15: __version__
        )

    def test_missing_main_asset_and_no_local_db_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        from regmeta.update import run_update

        self._fake_resolve(monkeypatch, db_tag=None, docs_tag=None)
        with pytest.raises(RegmetaError) as exc_info:
            run_update(db_dir=tmp_path, yes=True)
        assert exc_info.value.code == "no_db_in_release"

    def test_missing_docs_asset_and_no_local_docs_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Main DB present locally, doc asset missing from walker: still raise."""
        from regmeta.update import run_update

        # Simulate an already-installed main DB so the main-DB branch is
        # 'up_to_date'; the raise must come from the doc-DB branch only.
        (tmp_path / DB_FILENAME).write_bytes(b"placeholder")
        (tmp_path / ".db_source").write_text('{"tag": "regmeta/v0.7.0"}')

        self._fake_resolve(monkeypatch, db_tag="regmeta/v0.7.0", docs_tag=None)
        with pytest.raises(RegmetaError) as exc_info:
            run_update(db_dir=tmp_path, yes=True)
        assert exc_info.value.code == "no_docs_in_release"

    def test_missing_asset_but_local_copy_present_reports_no_in_release(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """If user already has both artifacts locally, missing assets are OK."""
        from regmeta.update import run_update

        (tmp_path / DB_FILENAME).write_bytes(b"main-db-placeholder")
        (tmp_path / ".db_source").write_text('{"tag": "regmeta/v0.7.0"}')
        (tmp_path / DOC_DB_FILENAME).write_bytes(b"doc-db-placeholder")
        (tmp_path / ".docs_source").write_text('{"tag": "regmeta/v0.7.0"}')

        self._fake_resolve(monkeypatch, db_tag=None, docs_tag=None)
        # Should not raise: user has working local copies; up-to-date.
        result = run_update(db_dir=tmp_path, yes=True)
        assert result["database"] == "no_db_in_release"
        assert result["docs"] == "no_docs_in_release"


class TestRunUpdatePypiBehind:
    """GitHub tag can land before the gated PyPI publish. PyPI is the
    source of truth for "what's installable"; GitHub drives asset tags."""

    def test_pypi_behind_github_no_upgrade_offered(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """PyPI says installed == latest; GitHub advertises a newer tag.
        The package upgrade is skipped and existing local assets matching
        the GitHub tag are treated as up-to-date."""
        from regmeta import __version__, update
        from regmeta.download import ReleaseResolution
        from regmeta.update import run_update

        newer_tag = "regmeta/v99.99.99"
        (tmp_path / DB_FILENAME).write_bytes(b"db-placeholder")
        (tmp_path / ".db_source").write_text(f'{{"tag": "{newer_tag}"}}')
        (tmp_path / DOC_DB_FILENAME).write_bytes(b"docs-placeholder")
        (tmp_path / ".docs_source").write_text(f'{{"tag": "{newer_tag}"}}')

        monkeypatch.setattr(
            update,
            "resolve_latest_release",
            lambda *, timeout=15: ReleaseResolution(
                release_tag=newer_tag,
                version="99.99.99",
                db_tag=newer_tag,
                docs_tag=newer_tag,
            ),
        )
        monkeypatch.setattr(
            update, "fetch_pypi_latest_version", lambda *, timeout=15: __version__
        )

        result = run_update(db_dir=tmp_path, yes=True)
        assert result["package"] == "up_to_date"
        assert result["database"] == "up_to_date"
        assert result["docs"] == "up_to_date"

    def test_uv_nothing_to_upgrade_reports_no_upgrade(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """Belt-and-braces: if PyPI says newer but `uv tool upgrade` reports
        'Nothing to upgrade' (e.g. uv's index cache lags), don't lie about
        a successful upgrade."""
        import subprocess as _subprocess

        from regmeta import update
        from regmeta.download import ReleaseResolution
        from regmeta.update import run_update

        target_tag = "regmeta/v99.99.99"
        (tmp_path / DB_FILENAME).write_bytes(b"db-placeholder")
        (tmp_path / ".db_source").write_text(f'{{"tag": "{target_tag}"}}')
        (tmp_path / DOC_DB_FILENAME).write_bytes(b"docs-placeholder")
        (tmp_path / ".docs_source").write_text(f'{{"tag": "{target_tag}"}}')

        monkeypatch.setattr(
            update,
            "resolve_latest_release",
            lambda *, timeout=15: ReleaseResolution(
                release_tag=target_tag,
                version="99.99.99",
                db_tag=target_tag,
                docs_tag=target_tag,
            ),
        )
        monkeypatch.setattr(
            update, "fetch_pypi_latest_version", lambda *, timeout=15: "99.99.99"
        )

        def fake_run(cmd, capture_output, text):
            return _subprocess.CompletedProcess(
                cmd, returncode=0, stdout="", stderr="Nothing to upgrade\n"
            )

        monkeypatch.setattr(update.subprocess, "run", fake_run)

        result = run_update(db_dir=tmp_path, yes=True)
        assert result["package"] == "no_upgrade"


class TestUpdateCheckerUsesPypi:
    """UpdateChecker must compare installed version against PyPI, not GitHub."""

    def test_checker_queries_pypi(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        from regmeta import update

        # Isolate filesystem writes.
        monkeypatch.setattr(update, "default_db_dir", lambda: tmp_path)

        calls = {"pypi": 0, "github": 0}

        def fake_pypi(*, timeout=15):
            calls["pypi"] += 1
            return "99.99.99"

        def fake_github(*, timeout=15):
            calls["github"] += 1
            raise AssertionError("UpdateChecker must not hit GitHub for version")

        monkeypatch.setattr(update, "fetch_pypi_latest_version", fake_pypi)
        monkeypatch.setattr(update, "resolve_latest_release", fake_github)

        checker = update.UpdateChecker(http_timeout=5)
        newer = checker.get_newer_version(timeout=5)

        assert newer == "99.99.99"
        assert calls["pypi"] == 1
        assert calls["github"] == 0
