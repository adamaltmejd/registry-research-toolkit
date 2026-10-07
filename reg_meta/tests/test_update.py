"""Update decisions at the public boundary: network at urlopen, uv at subprocess.run.

The fake network serves a configurable GitHub releases list, a PyPI version and
zstd assets built from readable sources. Version cases are relative to the
installed ``reg_meta.__version__`` and assume it is a final ``X.Y.Z`` release.
"""

from __future__ import annotations

import io
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from email.message import Message
from typing import TYPE_CHECKING

import pytest
import zstandard
from reader_artifacts import CASES, build_reader_artifact
from reg_meta.cli import run
from reg_meta.doc_db import DOC_SCHEMA_VERSION
from reg_meta.download import version_from_tag
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.update import UpdateChecker, read_pending_update, run_update
from reg_meta_build.doc_db import build_doc_db

from reg_meta import __version__

if TYPE_CHECKING:
    from pathlib import Path

DB = "reg_meta.db.zst"
DOCS = "reg_meta_docs.db.zst"
UV_TOOL_DIR = "/x/uv/tools"
UV_TOOL_PREFIX = "/x/uv/tools/reg-meta"
VENV_PREFIX = "/app/.venv"
UPGRADE = ["uv", "tool", "upgrade", "reg_meta"]


def _installed() -> tuple[int, int, int]:
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", __version__)
    assert match, f"version cases assume a final installed version, got {__version__}"
    major, minor, patch = (int(part) for part in match.groups())
    return major, minor, patch


MAJOR, MINOR, PATCH = _installed()
SAME = f"{MAJOR}.{MINOR}.{PATCH}"
NEXT_PATCH = f"{MAJOR}.{MINOR}.{PATCH + 1}"


def release(tag: str, *assets: str) -> dict:
    return {"tag_name": tag, "assets": [{"name": name} for name in assets]}


class Response(io.BytesIO):
    @property
    def headers(self):
        return {"Content-Length": str(len(self.getvalue()))}


@pytest.fixture(scope="module")
def payloads(tmp_path_factory) -> dict[str, bytes]:
    root = tmp_path_factory.mktemp("assets")
    catalog = build_reader_artifact(root / "catalog", "annual-series", "catalog")
    docs = build_doc_db(CASES / "selection/docs", root / "docs")
    incompatible = build_doc_db(CASES / "selection/docs", root / "incompatible")
    major = int(DOC_SCHEMA_VERSION.split(".")[0])
    with sqlite3.connect(incompatible) as conn:
        conn.execute(
            "UPDATE doc_meta SET value=? WHERE key='schema_version'",
            (f"{major + 1}.0.0",),
        )
    compress = zstandard.ZstdCompressor().compress
    return {
        DB: compress(catalog.read_bytes()),
        DOCS: compress(docs.read_bytes()),
        "incompatible-docs": compress(incompatible.read_bytes()),
    }


class FakeNetwork:
    """GitHub releases, PyPI JSON and release downloads behind urlopen."""

    def __init__(self, payloads: dict[str, bytes]) -> None:
        self.payloads = payloads
        self.releases: list[dict] = []
        self.pypi: str | None = SAME  # None: PyPI unreachable
        self.docs_payload = DOCS
        self.urls: list[str] = []

    def __call__(self, request, **kwargs):
        url = request.full_url
        self.urls.append(url)
        if "api.github.com" in url:
            return Response(json.dumps(self.releases).encode())
        if "pypi.org" in url:
            if self.pypi is None:
                raise urllib.error.URLError("offline")
            return Response(json.dumps({"info": {"version": self.pypi}}).encode())
        tag, name = url.split("/releases/download/", 1)[1].rsplit("/", 1)
        carried = [
            asset["name"]
            for item in self.releases
            if item["tag_name"] == tag
            for asset in item["assets"]
        ]
        if name not in carried:
            raise urllib.error.HTTPError(url, 404, "Not Found", Message(), None)
        return Response(self.payloads[self.docs_payload if name == DOCS else name])

    @property
    def hosts(self) -> list[str]:
        return [urllib.parse.urlsplit(url).hostname or "" for url in self.urls]

    @property
    def downloads(self) -> list[str]:
        return [url for url in self.urls if "/releases/download/" in url]


class FakeUv:
    """`uv tool dir` and `uv tool upgrade` behind subprocess.run."""

    def __init__(self) -> None:
        self.tool_dir = UV_TOOL_DIR + "\n"
        self.returncode = 0
        self.missing = False
        self.upgrade_output = ("Upgraded reg-meta\n", "")
        self.calls: list[list[str]] = []

    def __call__(self, cmd, **kwargs):
        self.calls.append(cmd)
        if self.missing:
            raise FileNotFoundError("uv")
        if cmd == ["uv", "tool", "dir"]:
            return subprocess.CompletedProcess(cmd, self.returncode, self.tool_dir, "")
        if cmd == UPGRADE:
            stdout, stderr = self.upgrade_output
            return subprocess.CompletedProcess(cmd, 0, stdout, stderr)
        raise AssertionError(f"unexpected subprocess: {cmd!r}")


@pytest.fixture
def net(payloads, tmp_path: Path, monkeypatch) -> FakeNetwork:
    network = FakeNetwork(payloads)
    monkeypatch.setattr(urllib.request, "urlopen", network)
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.delenv("REG_META_DB", raising=False)
    monkeypatch.setattr(sys, "prefix", VENV_PREFIX)
    return network


@pytest.fixture
def uv(monkeypatch) -> FakeUv:
    fake = FakeUv()
    monkeypatch.setattr(subprocess, "run", fake)
    return fake


def install(net: FakeNetwork, tag: str) -> None:
    """Install both assets through a real update from a release carrying them."""
    pypi = net.pypi
    net.releases, net.pypi = [release(tag, DB, DOCS)], SAME
    run_update(yes=True)
    net.pypi = pypi


def check() -> str | None:
    return UpdateChecker(http_timeout=5).get_newer_version(timeout=5)


# --- version ordering through the upgrade decision ------------------------


@pytest.mark.parametrize(
    "candidate, offered",
    [
        pytest.param(SAME, False, id="same"),
        pytest.param(f"v{SAME}", False, id="v-same"),
        pytest.param(NEXT_PATCH, True, id="next-patch"),
        pytest.param(f"v{NEXT_PATCH}", True, id="v-next-patch"),
        pytest.param(f"{SAME}a1", False, id="alpha-of-same"),
        pytest.param(f"{SAME}.dev1", False, id="dev-of-same"),
        pytest.param(f"{NEXT_PATCH}a1", True, id="alpha-of-next"),
        pytest.param(f"{NEXT_PATCH}.dev1", True, id="dev-of-next"),
        pytest.param(f"{MAJOR}.{MINOR + 1}.0", True, id="next-minor"),
        pytest.param(f"{MAJOR + 1}.0.0", True, id="next-major"),
        pytest.param("garbage", False, id="unparseable"),
    ],
)
def test_checker_offers_only_a_newer_pypi_version(net, uv, candidate, offered):
    net.pypi = candidate
    assert check() == (candidate if offered else None)


def test_checker_asks_pypi_not_github_for_the_version(net, uv):
    net.pypi = NEXT_PATCH
    assert check() == NEXT_PATCH
    assert net.hosts == ["pypi.org"]


class TestVersionFromTag:
    def test_prefixed_tag(self):
        assert version_from_tag("reg_meta/v0.5.0") == "0.5.0"

    def test_legacy_bare_tag(self):
        assert version_from_tag("v0.4.0") == "0.4.0"

    def test_no_v_prefix(self):
        assert version_from_tag("reg_meta/0.5.0") == "0.5.0"


# --- pending-update flag ---------------------------------------------------


def test_newer_check_records_pending_update(net, uv):
    assert read_pending_update() is None
    net.pypi = NEXT_PATCH
    check()
    assert read_pending_update() == NEXT_PATCH


def test_non_newer_check_clears_pending_update(net, uv, monkeypatch):
    net.pypi = NEXT_PATCH
    check()
    real_time = time.time
    monkeypatch.setattr(time, "time", lambda: real_time() + 8 * 24 * 3600)
    net.pypi = SAME
    assert check() is None
    assert read_pending_update() is None


def test_run_update_clears_pending_update(net, uv):
    net.pypi = NEXT_PATCH
    check()
    install(net, "reg_meta/v99.0.0")
    assert read_pending_update() is None


# --- release walk ------------------------------------------------------------


@pytest.mark.parametrize(
    "releases, expected",
    [
        pytest.param(
            [release("reg_meta/v99.2.0"), release("reg_meta/v99.1.0", DB, DOCS)],
            ("99.2.0", "reg_meta/v99.1.0", "reg_meta/v99.1.0"),
            id="latest-sets-version-assets-from-older",
        ),
        pytest.param(
            [release("v98.0.0", DB, DOCS)],
            ("98.0.0", "v98.0.0", "v98.0.0"),
            id="legacy-bare-tags",
        ),
        pytest.param(
            [release("reg_meta/v99.0.0"), release("v98.0.0", DB, DOCS)],
            ("99.0.0", "v98.0.0", "v98.0.0"),
            id="prefixed-version-legacy-assets",
        ),
        pytest.param(
            [
                release("reg_meta_build/v100.0.0", DB, DOCS),
                release("reg_meta/v99.0.0"),
                release("reg_meta/v98.0.0", DB, DOCS),
            ],
            ("99.0.0", "reg_meta/v98.0.0", "reg_meta/v98.0.0"),
            id="foreign-package-ignored",
        ),
        pytest.param(
            [release("vNext", DB, DOCS), release("reg_meta/v99.0.0", DB, DOCS)],
            ("99.0.0", "reg_meta/v99.0.0", "reg_meta/v99.0.0"),
            id="non-semver-v-tag-ignored",
        ),
        pytest.param(
            [
                release("reg_meta/v99.2.0"),
                release("reg_meta/v99.1.0", DOCS),
                release("reg_meta/v99.0.0", DB),
            ],
            ("99.2.0", "reg_meta/v99.0.0", "reg_meta/v99.1.0"),
            id="db-and-docs-walked-independently",
        ),
    ],
)
def test_release_walk_resolves_version_and_asset_tags(
    net, uv, monkeypatch, releases, expected
):
    # PyPI offline: the walked release version is the upgrade target.
    monkeypatch.setattr(sys, "prefix", UV_TOOL_PREFIX)
    net.releases, net.pypi = releases, None
    result = run_update(yes=True)
    actual = (
        result["package"]["new_version"],
        result["database"]["tag"],
        result["docs"]["tag"],
    )
    assert actual == expected


@pytest.mark.parametrize(
    "releases",
    [[], [release("reg_meta_build/v1.0.0", DB, DOCS)]],
    ids=["empty", "foreign-only"],
)
def test_no_reg_meta_release_fails(net, uv, releases):
    net.releases = releases
    with pytest.raises(RegMetaError) as exc_info:
        run_update(yes=True)
    assert exc_info.value.code == "no_releases"


def test_no_asset_anywhere_without_local_catalog_fails(net, uv):
    net.releases = [release("reg_meta/v99.0.0"), release("reg_meta/v98.0.0")]
    with pytest.raises(RegMetaError) as exc_info:
        run_update(yes=True)
    assert exc_info.value.code == "no_db_in_release"


def test_no_asset_anywhere_keeps_admitted_local_copies(net, uv):
    install(net, "reg_meta/v98.0.0")
    net.releases = [release("reg_meta/v99.0.0")]
    result = run_update(yes=True)
    assert (result["database"], result["docs"]) == (
        "no_db_in_release",
        "no_docs_in_release",
    )


# --- docs asset admission -----------------------------------------------------


def test_incompatible_docs_asset_is_refused_without_overwriting(net, uv, tmp_path):
    install(net, "reg_meta/v98.0.0")
    data = tmp_path / "data/reg_meta"
    before = (data / "reg_meta_docs.db").read_bytes()
    net.releases = [release("reg_meta/v99.0.0", DOCS), release("reg_meta/v98.0.0", DB)]
    net.docs_payload = "incompatible-docs"
    with pytest.raises(RegMetaError) as exc_info:
        run_update(yes=True)
    assert exc_info.value.code == "incompatible_docs_asset"
    assert (data / "reg_meta_docs.db").read_bytes() == before
    assert sorted(path.name for path in data.glob("*.tmp")) == []


def test_compatible_docs_asset_replaces_and_records_its_tag(net, uv):
    install(net, "reg_meta/v98.0.0")
    net.releases = [release("reg_meta/v99.0.0", DOCS), release("reg_meta/v98.0.0", DB)]
    assert run_update(yes=True)["docs"]["tag"] == "reg_meta/v99.0.0"
    downloads = len(net.downloads)
    assert run_update(yes=True)["docs"] == "up_to_date"
    assert len(net.downloads) == downloads


def test_first_run_docs_download_reports_missing_docs_asset(
    net, uv, monkeypatch, capsys
):
    # The interactive bootstrap is the route that downloads docs at tag "latest".
    net.releases = [release("reg_meta/v99.0.0", DB)]
    assert run_update(yes=True)["docs"] == "no_docs_in_release"

    class TtyInput(io.StringIO):
        def isatty(self) -> bool:
            return True

    monkeypatch.setattr(sys, "stdin", TtyInput("y\n"))
    monkeypatch.setenv("REG_META_QUIET", "1")
    capsys.readouterr()
    code = run(["docs", "list"])
    error = json.loads(capsys.readouterr().out)["error"]
    assert (code, error["code"]) == (EXIT_CONFIG, "no_docs_in_release")
    # End users cannot run the maintainer-only `reg-meta-build build-docs`.
    assert error["remediation"] == (
        "Metadata commands work without the doc DB. Pass a release that carries "
        "one to `reg-meta update --tag`, or report the missing asset at "
        "https://github.com/adamaltmejd/registry-research-toolkit/issues."
    )


# --- package upgrade decision -------------------------------------------------


def test_pypi_behind_github_offers_no_upgrade(net, uv, monkeypatch):
    monkeypatch.setattr(sys, "prefix", UV_TOOL_PREFIX)
    install(net, "reg_meta/v99.0.0")
    result = run_update(yes=True)
    assert result == {
        "package": "up_to_date",
        "database": "up_to_date",
        "docs": "up_to_date",
    }
    assert UPGRADE not in uv.calls


def test_uv_nothing_to_upgrade_reports_no_upgrade(net, uv, monkeypatch):
    monkeypatch.setattr(sys, "prefix", UV_TOOL_PREFIX)
    install(net, "reg_meta/v99.0.0")
    net.pypi = NEXT_PATCH
    uv.upgrade_output = ("", "Nothing to upgrade\n")
    assert run_update(yes=True)["package"] == "no_upgrade"


@pytest.mark.parametrize(
    "tool_dir, returncode, missing, prefix, attempted",
    [
        pytest.param(UV_TOOL_DIR, 0, False, UV_TOOL_PREFIX, True, id="under-tool-dir"),
        pytest.param(UV_TOOL_DIR, 0, False, UV_TOOL_DIR, True, id="is-tool-dir"),
        pytest.param(UV_TOOL_DIR, 0, False, VENV_PREFIX, False, id="outside"),
        pytest.param(
            UV_TOOL_DIR, 0, False, UV_TOOL_DIR + "-evil", False, id="sibling-prefix"
        ),
        pytest.param(UV_TOOL_DIR, 1, False, UV_TOOL_PREFIX, False, id="nonzero-exit"),
        # realpath("") is the working directory; empty output must not match it.
        pytest.param("", 0, False, "<cwd>/reg-meta", False, id="empty-stdout"),
        pytest.param(UV_TOOL_DIR, 0, True, UV_TOOL_PREFIX, False, id="uv-missing"),
    ],
)
def test_upgrade_runs_only_for_a_uv_tool_install(
    net, uv, monkeypatch, tmp_path, tool_dir, returncode, missing, prefix, attempted
):
    monkeypatch.chdir(tmp_path)
    prefix = prefix.replace("<cwd>", os.path.realpath(tmp_path))
    install(net, "reg_meta/v99.0.0")
    net.pypi = NEXT_PATCH
    uv.tool_dir, uv.returncode, uv.missing = tool_dir + "\n", returncode, missing
    monkeypatch.setattr(sys, "prefix", prefix)
    expected = (
        {"old_version": __version__, "new_version": NEXT_PATCH}
        if attempted
        else "skipped_not_uv_tool"
    )
    assert run_update(yes=True)["package"] == expected


def test_non_uv_tool_install_still_fetches_assets(net, uv):
    net.pypi = NEXT_PATCH
    net.releases = [release("reg_meta/v99.0.0", DB, DOCS)]
    result = run_update(yes=True)
    actual = (result["package"], result["database"]["tag"], result["docs"]["tag"])
    assert actual == ("skipped_not_uv_tool", "reg_meta/v99.0.0", "reg_meta/v99.0.0")
    assert UPGRADE not in uv.calls


def test_uv_tool_install_runs_uv_tool_upgrade(net, uv, monkeypatch):
    install(net, "reg_meta/v99.0.0")
    net.pypi = NEXT_PATCH
    monkeypatch.setattr(sys, "prefix", UV_TOOL_PREFIX)
    result = run_update(yes=True)
    assert result["package"] == {"old_version": __version__, "new_version": NEXT_PATCH}
    assert uv.calls[-1] == UPGRADE
