"""Conformance admission and session-built artifacts; package marker meanings stay intact."""

from __future__ import annotations

from pathlib import Path

import pytest
from http_cases import ServerPool
from reader_artifacts import FIXTURE_IMPORT_DATE, cached_reader_artifact
from reg_meta.db import open_db
from reg_meta.errors import RegMetaError


def pytest_configure(config):
    directory = config.getoption("--artifact-dir")
    holdings_input = config.getoption("--holdings-input")
    if holdings_input is not None and (
        directory is None or not config.getoption("--run-release")
    ):
        raise pytest.UsageError(
            "--holdings-input requires --run-release and --artifact-dir"
        )
    if directory is None:
        return
    if not config.getoption("--run-release"):
        raise pytest.UsageError("--artifact-dir requires --run-release")
    # The reader owns admission; never substitute fixtures for an invalid directory.
    try:
        open_db(Path(directory).expanduser().resolve() / "reg_meta.db").close()
    except RegMetaError as exc:
        raise pytest.UsageError(f"{exc.code}: {exc.message}") from exc
    if holdings_input is not None:
        from holdings_accounting import admit_holdings_input

        try:
            admit_holdings_input(Path(holdings_input), Path(directory))
        except ValueError as exc:
            raise pytest.UsageError(str(exc)) from exc


def pytest_generate_tests(metafunc):
    if "artifact_dir" not in metafunc.fixturenames:
        return
    directory = metafunc.config.getoption("--artifact-dir")
    values = (
        [pytest.param(directory, id="selected", marks=pytest.mark.release)]
        if directory is not None
        else [pytest.param(kind, id=kind) for kind in ("catalog", "steward")]
    )
    metafunc.parametrize("artifact_dir", values, indirect=True, scope="session")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--holdings-input") is not None:
        return
    # Private acceptance is an explicit maintainer boundary, never inferred from disk.
    deselected = [
        item
        for item in items
        if item.name.startswith("test_accepted_input_census_matches_compiled_artifact")
    ]
    if deselected:
        items[:] = [item for item in items if item not in deselected]
        config.hook.pytest_deselected(items=deselected)


@pytest.fixture(scope="session")
def artifact_dir(request):
    if request.config.getoption("--artifact-dir") is not None:
        return Path(request.param).expanduser().resolve()
    if request.config.getoption("--run-release"):
        raise pytest.UsageError("conformance tier 3 requires --artifact-dir")
    return cached_reader_artifact(
        "reader",
        request.param,
        identity_overrides={"import_date": FIXTURE_IMPORT_DATE},
    ).parent


@pytest.fixture(scope="session")
def http_servers(request, tmp_path_factory):
    """Server processes for `--server-cmd`, one per artifact; None runs in-process.

    Every server is stopped at session end, whether or not the cases passed."""
    template = request.config.getoption("--server-cmd")
    if template is None:
        yield None
        return
    pool = ServerPool(template, tmp_path_factory.mktemp("servers"))
    try:
        yield pool
    finally:
        pool.close()


@pytest.fixture
def artifact_client(artifact_dir, monkeypatch):
    from fastapi.testclient import TestClient
    from reg_meta.db import get_manifest
    from reg_webapp.app import create_app

    with open_db(artifact_dir / "reg_meta.db") as conn:
        manifest = get_manifest(conn)
    monkeypatch.setenv("REG_META_DB", str(artifact_dir))
    monkeypatch.setenv("REG_WEBAPP_STEWARD", manifest.get("steward", "global"))
    monkeypatch.setenv(
        "REG_WEBAPP_STEWARDS_DIR",
        str(Path(__file__).resolve().parents[1] / "reg_webapp/stewards"),
    )
    with TestClient(create_app(rate_limit_per_minute=1000)) as client:
        yield client
