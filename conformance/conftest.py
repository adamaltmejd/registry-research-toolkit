"""Conformance admission and session-built artifacts, served by the Rust server."""

from __future__ import annotations

from pathlib import Path

import pytest
from http_cases import PARKED, ServerPool
from reader_artifacts import cached_reader_artifact


def pytest_addoption(parser):
    parser.addoption(
        "--run-release",
        action="store_true",
        default=False,
        help="admit a real artifact (tier 3); requires --artifact-dir",
    )
    parser.addoption(
        "--artifact-dir",
        default=None,
        help="conformance artifact directory (requires --run-release)",
    )
    parser.addoption(
        "--server-cmd",
        default=None,
        help="the Rust server's command template ({db}, {catalog}, {port}) every "
        "HTTP and MCP case runs against (required); see conformance/README.md",
    )
    parser.addoption(
        "--mcp-cmd",
        default=None,
        help="run the MCP stdio session against a process started from this "
        "command template ({db}, {catalog}); see conformance/README.md",
    )


def pytest_configure(config):
    # The suite's boundary is the Rust server; there is no in-process adapter.
    if config.getoption("--server-cmd") is None:
        raise pytest.UsageError(
            "conformance runs against the Rust server: pass --server-cmd "
            "(scripts/gate.py rust)"
        )
    if config.getoption("--artifact-dir") is not None and not config.getoption(
        "--run-release"
    ):
        raise pytest.UsageError("--artifact-dir requires --run-release")


def pytest_generate_tests(metafunc):
    if "artifact_dir" not in metafunc.fixturenames:
        return
    directory = metafunc.config.getoption("--artifact-dir")
    values = [
        pytest.param(directory, id="selected", marks=pytest.mark.release)
        if directory is not None
        else pytest.param("base", id="base")
    ]
    metafunc.parametrize("artifact_dir", values, indirect=True, scope="session")


@pytest.fixture(scope="session")
def artifact_dir(request):
    if request.config.getoption("--artifact-dir") is not None:
        return Path(request.param).expanduser().resolve()
    if request.config.getoption("--run-release"):
        raise pytest.UsageError("conformance tier 3 requires --artifact-dir")
    return cached_reader_artifact(request.param).parent


@pytest.fixture(scope="session")
def server_cmd(request):
    """The `--server-cmd` template, for a test that starts the server."""
    if PARKED["server"]:
        pytest.skip("parked: the server does not admit the 4 schema (PARKED.toml)")
    return request.config.getoption("--server-cmd")


@pytest.fixture(scope="session")
def http_servers(server_cmd, tmp_path_factory):
    """Server processes for `--server-cmd`, one per artifact.

    Every server is stopped at session end, whether or not the cases passed."""
    pool = ServerPool(server_cmd, tmp_path_factory.mktemp("servers"))
    try:
        yield pool
    finally:
        pool.close()
