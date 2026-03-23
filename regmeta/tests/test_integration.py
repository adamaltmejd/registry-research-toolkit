"""Integration test: full install-and-query pipeline in a Docker container.

Not run by default. Requires Docker and a published GitHub release.

    pytest -m integration regmeta/tests/test_integration.py
"""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import textwrap
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]

DOCKERFILE = textwrap.dedent("""\
    FROM python:3.12-slim

    COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

    WORKDIR /src
    COPY . .

    RUN uv venv /opt/venv
    ENV VIRTUAL_ENV=/opt/venv
    ENV PATH="/opt/venv/bin:$PATH"
    RUN uv pip install "./regmeta"
""")

IMAGE_TAG = "regmeta-integration-test"


@pytest.fixture(scope="module")
def docker() -> str:
    path = shutil.which("docker")
    if not path:
        pytest.skip("Docker not available")
    result = subprocess.run([path, "info"], capture_output=True, timeout=10)
    if result.returncode != 0:
        pytest.skip("Docker daemon not running")
    return path


@pytest.fixture(scope="module")
def image(docker: str) -> str:
    """Build a Docker image with regmeta installed from local source."""
    with tempfile.TemporaryDirectory() as ctx_str:
        ctx = Path(ctx_str)

        # Minimal build context: only regmeta package + workspace root config
        shutil.copytree(
            REPO_ROOT / "regmeta",
            ctx / "regmeta",
            ignore=shutil.ignore_patterns(
                "__pycache__", "*.pyc", ".pytest_cache", "*.egg-info"
            ),
        )
        shutil.copy2(REPO_ROOT / "pyproject.toml", ctx)
        (ctx / "Dockerfile").write_text(DOCKERFILE)

        result = subprocess.run(
            [docker, "build", "-t", IMAGE_TAG, "."],
            cwd=ctx,
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert result.returncode == 0, f"Docker build failed:\n{result.stderr}"

    yield IMAGE_TAG

    subprocess.run([docker, "rmi", IMAGE_TAG], capture_output=True, timeout=30)


def _docker_run(
    docker: str, image: str, cmd: str, *, timeout: int = 60
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [docker, "run", "--rm", image, "sh", "-c", cmd],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_install_and_cli_help(docker: str, image: str):
    """Package installs cleanly and CLI is functional."""
    result = _docker_run(docker, image, "regmeta --help")
    assert result.returncode == 0
    assert "search" in result.stdout
    assert "maintain" in result.stdout


def test_version_importable(docker: str, image: str):
    """Package version is importable."""
    result = _docker_run(
        docker, image, "python -c 'import regmeta; print(regmeta.__version__)'"
    )
    assert result.returncode == 0
    assert result.stdout.strip()


def test_download_and_query(docker: str, image: str):
    """Full pipeline: download DB from GitHub Releases and run a query."""
    cmd = (
        "regmeta maintain download --yes > /dev/null"
        " && regmeta --format json search --query kommun --datacolumn"
    )
    result = _docker_run(docker, image, cmd, timeout=600)
    assert result.returncode == 0, (
        f"Pipeline failed (exit {result.returncode}):\n{result.stderr}"
    )

    payload = json.loads(result.stdout)
    results = payload.get("results", payload.get("data", {}).get("results", []))
    assert len(results) > 0, "Expected search results for 'kommun'"


def test_download_refuses_overwrite(docker: str, image: str):
    """Second download without --force should fail."""
    cmd = (
        "regmeta maintain download --yes"
        " && regmeta maintain download --yes 2>&1; echo EXIT:$?"
    )
    result = _docker_run(docker, image, cmd, timeout=600)
    assert "EXIT:10" in result.stdout or "db_exists" in result.stdout
