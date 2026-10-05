"""Named selection and updates at CLI and downloaded-artifact boundaries."""

from __future__ import annotations

import io
import json
import sqlite3
import urllib.error
import urllib.request
from email.message import Message
from typing import TYPE_CHECKING

import pytest
import zstandard
from reader_artifacts import CASES, build_reader_artifact
from reg_meta.cli import run
from reg_meta.db import get_manifest, open_db
from reg_meta.errors import RegMetaError
from reg_meta.update import run_update
from reg_meta_build.doc_db import build_doc_db

if TYPE_CHECKING:
    from pathlib import Path

SELECTION = CASES / "selection"


@pytest.mark.parametrize(
    "case", sorted(SELECTION.glob("*/request.json")), ids=lambda p: p.parent.name
)
def test_selection_case(case: Path, tmp_path: Path, monkeypatch, capsys):
    request = json.loads(case.read_text())
    expected = json.loads(case.with_name("expected.json").read_text())
    data = tmp_path / "data/reg_meta"
    explicit = tmp_path / "explicit"
    directory = {"global": data, "explicit": explicit}.get(
        request["install"], data / request["install"]
    )
    monkeypatch.setenv("XDG_DATA_HOME", str(data.parent))
    monkeypatch.delenv("REG_META_DB", raising=False)
    if request["kind"]:
        path = build_reader_artifact(directory, "annual-series", request["kind"])
        if request.get("manifest_patch"):
            with sqlite3.connect(path) as conn:
                conn.executemany(
                    "UPDATE import_manifest SET value=? WHERE key=?",
                    [(value, key) for key, value in request["manifest_patch"].items()],
                )
    if request.get("env"):
        monkeypatch.setenv(
            "REG_META_DB",
            str(explicit if request["env"] == "explicit" else tmp_path / "empty"),
        )
    if request.get("global_docs"):
        build_doc_db(SELECTION / "docs", data)
    if request.get("local_docs"):
        build_doc_db(SELECTION / "docs", directory)
    argv = [
        str(explicit) if token == "$EXPLICIT" else token for token in request["argv"]
    ]
    argv += request.get("command", ["get", "register", "scb/example"])

    def no_network(*args, **kwargs):
        raise AssertionError(
            "Selection must fail or serve locally before any network access."
        )

    monkeypatch.setattr(urllib.request, "urlopen", no_network)
    code = run([*argv, "--format", "json", "--verbose"])
    output = json.loads(capsys.readouterr().out)
    actual = {"exit": code}
    if code:
        actual["error"] = output["error"]
        expected["error"]["message"] = (
            expected["error"]["message"]
            .replace("$DATA", str(data))
            .replace("$EXPLICIT", str(explicit))
        )
    else:
        actual.update(
            kind=output["database"]["artifact_kind"],
            steward=output["database"]["steward"],
            scope=output["database"]["scope"],
        )
        conn = open_db(directory / "reg_meta.db")
        try:
            assert (
                output["database"]["generation_id"]
                == get_manifest(conn)["generation_id"]
            )
        finally:
            conn.close()
    assert actual == expected
    if request["kind"] is None:
        assert not (directory / "reg_meta.db").exists()
        if not request.get("local_docs"):
            assert not directory.exists()


class NetworkResponse(io.BytesIO):
    @property
    def headers(self):
        return {"Content-Length": str(len(self.getvalue()))}


def network_fixture(
    tmp_path: Path, monkeypatch, request=None
) -> tuple[Path, list[str]]:
    request = request or {}
    source = tmp_path / "source"
    source_db = build_reader_artifact(
        source, "annual-series", request.get("asset_kind", "steward")
    )
    docs = build_doc_db(SELECTION / "docs", source)
    payloads = {
        "reg_meta_swecov.db.zst": zstandard.ZstdCompressor().compress(
            source_db.read_bytes()
        ),
        "reg_meta_docs.db.zst": zstandard.ZstdCompressor().compress(docs.read_bytes()),
    }
    urls = []

    def network(transport_request, **kwargs):
        url = transport_request.full_url
        if "api.github.com" in url:
            releases = json.loads((SELECTION / "releases.json").read_text())
            if not request.get("docs_asset", True):
                releases[0]["assets"] = [
                    asset
                    for asset in releases[0]["assets"]
                    if asset["name"] != "reg_meta_docs.db.zst"
                ]
            return NetworkResponse(json.dumps(releases).encode())
        if "pypi.org" in url:
            return NetworkResponse(b'{"info":{"version":"0.0.0"}}')
        name = url.rsplit("/", 1)[-1]
        urls.append(name)
        if name == "reg_meta_docs.db.zst" and not request.get("docs_asset", True):
            raise urllib.error.HTTPError(url, 404, "Not Found", Message(), None)
        return NetworkResponse(payloads[name])

    monkeypatch.setattr(urllib.request, "urlopen", network)
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.delenv("REG_META_DB", raising=False)
    return source_db, urls


@pytest.mark.parametrize("selection", ["named", "path", "env"])
def test_update_preserves_selected_identity(selection, tmp_path: Path, monkeypatch):
    source, urls = network_fixture(tmp_path, monkeypatch)
    target = (
        tmp_path / "data/reg_meta/swecov"
        if selection == "named"
        else tmp_path / "explicit"
    )
    if selection != "named":
        target.mkdir()
        (target / "reg_meta.db").write_bytes(source.read_bytes())
    if selection == "env":
        monkeypatch.setenv("REG_META_DB", str(target))
    result = run_update(
        catalog="swecov" if selection == "named" else None,
        db_dir=target if selection == "path" else None,
        yes=True,
    )
    expected = json.loads((SELECTION / "update-expected.json").read_text())
    actual = {
        "package": result["package"],
        "database_tag": result["database"]["tag"],
        "docs_tag": result["docs"]["tag"],
        "urls": urls,
    }
    assert actual == expected
    conn = open_db(target / "reg_meta.db", catalog="swecov")
    try:
        assert get_manifest(conn)["catalog_artifact_kind"] == "steward"
    finally:
        conn.close()
    assert (target / "reg_meta_docs.db").exists()
    if selection == "named":
        assert not (target.parent / "reg_meta.db").exists()


@pytest.mark.parametrize(
    "case",
    sorted((CASES / "update").glob("*/request.json")),
    ids=lambda p: p.parent.name,
)
def test_update_admission_case(case: Path, tmp_path: Path, monkeypatch):
    request = json.loads(case.read_text())
    expected = json.loads(case.with_name("expected.json").read_text())
    _source, urls = network_fixture(tmp_path, monkeypatch, request)
    directory = (
        tmp_path / "data/reg_meta/swecov"
        if request["selection"] == "named"
        else tmp_path / "explicit"
    )
    old_bytes = None
    if request.get("initial_schema"):
        path = build_reader_artifact(directory, "annual-series", "steward")
        with sqlite3.connect(path) as conn:
            conn.execute(
                "UPDATE import_manifest SET value=? WHERE key='schema_version'",
                (request["initial_schema"],),
            )
        old_bytes = path.read_bytes()
        if request.get("initial_source_tag"):
            (directory / ".db_source").write_text(
                json.dumps({"tag": request["initial_source_tag"]})
            )
    try:
        result = run_update(
            catalog="swecov" if request["selection"] == "named" else None,
            db_dir=directory if request["selection"] == "path" else None,
            tag=request.get("tag", "latest"),
            yes=True,
        )
    except RegMetaError as exc:
        actual = {
            "error": exc.to_dict(),
            "old_bytes_preserved": (directory / "reg_meta.db").read_bytes()
            == old_bytes,
            "temporary_files": [path.name for path in directory.glob("*.tmp")],
        }
        if request["selection"] == "path":
            assert urls == []
    else:
        actual = {
            "package": result["package"],
            "database_tag": result["database"]["tag"],
            "docs_tag": result["docs"]["tag"]
            if isinstance(result["docs"], dict)
            else None,
            "urls": urls,
        }
        open_db(directory / "reg_meta.db", catalog="swecov").close()
    assert actual == expected
