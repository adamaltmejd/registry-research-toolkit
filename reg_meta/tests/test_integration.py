"""Integration test: full install-and-query pipeline in a Docker container.

Not run by default. Requires Docker and a published GitHub release.

    pytest -m integration reg_meta/tests/test_integration.py
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import textwrap
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]

# Both external images carry a readable exact tag PLUS its immutable multi-arch
# manifest digest, matching the workspace baseline (reg_webapp/Dockerfile,
# .yard/Dockerfile): python 3.14.7 and uv 0.12.6. Floating `python:3.14-slim` /
# `uv:latest` made this test drift with whatever uv shipped that week, which is
# how the workspace-source rejection below landed as a surprise failure; bump
# both halves deliberately, with the rest of the baseline.
#
# `--no-sources` makes uv ignore the root pyproject's `[tool.uv.sources]` and
# resolve reg_meta's dependencies from the registry instead. That IS this test's
# boundary: the local reg_meta source must install the way a published-package
# consumer gets it. The root pyproject stays in the context precisely so the
# install is proven to hold in the presence of the workspace config — the answer
# to `reg-schema = { workspace = true }` is to ignore the source, not to copy
# reg_schema in, which would install the local tree and hide whether reg_meta's
# published metadata resolves at all.
DOCKERFILE = textwrap.dedent("""\
    FROM python:3.14.7-slim-bookworm@sha256:9ab8d9c8514b44f90cf0029dd42fdd7e9e211e639c8b995304cc04568dee900f

    COPY --from=ghcr.io/astral-sh/uv:0.12.6@sha256:88bc6eb1ccd4b82efd0e1b530caffabddf50dc2bf612e66c14ea25b8ee8a4d3d /uv /usr/local/bin/uv

    WORKDIR /src
    COPY . .

    RUN uv venv /opt/venv
    ENV VIRTUAL_ENV=/opt/venv
    ENV PATH="/opt/venv/bin:$PATH"
    RUN uv pip install --no-sources "./reg_meta"
""")

IMAGE_TAG = "reg-meta-integration-test"


@pytest.fixture(scope="module")
def docker() -> str:
    """Resolve a working Docker daemon, or FAIL the test.

    This fixture only runs when integration tests are opted into via
    ``--run-integration`` (the root conftest skips them at collection otherwise),
    so opting in IS the assertion that Docker is available: a missing binary or a
    stopped daemon is a hard failure here, not a skip. The pre-push hook
    (.pre-commit-config.yaml) passes ``--run-integration``, which is what makes a
    running Docker daemon a required pre-push gate — start Docker and push again
    rather than bypassing with ``--no-verify``.
    """
    path = shutil.which("docker")
    if not path:
        pytest.fail("Docker not available (binary not found on PATH)", pytrace=False)
    result = subprocess.run(
        [path, "info"], capture_output=True, timeout=10, check=False
    )
    if result.returncode != 0:
        pytest.fail("Docker daemon not running — start Docker and retry", pytrace=False)
    return path


@pytest.fixture(scope="module")
def image(docker: str) -> str:
    """Build a Docker image with reg_meta installed from local source.

    The tag is per-xdist-worker (PYTEST_XDIST_WORKER) so a parallel `-n auto` run
    — what the pre-push hook uses — can't race on a shared tag: without this, two
    workers each get their own module-scoped instance of this fixture, and one's
    teardown `rmi` can delete the image out from under the other's `docker run`.
    Docker's layer cache makes the extra per-worker build effectively free. A
    serial run (CI, plain pytest) has no worker id and uses the bare tag.
    """
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    tag = f"{IMAGE_TAG}-{worker}" if worker else IMAGE_TAG
    with tempfile.TemporaryDirectory() as ctx_str:
        ctx = Path(ctx_str)

        # Minimal build context: only reg_meta package + workspace root config
        shutil.copytree(
            REPO_ROOT / "reg_meta",
            ctx / "reg_meta",
            ignore=shutil.ignore_patterns(
                "__pycache__", "*.pyc", ".pytest_cache", "*.egg-info"
            ),
        )
        shutil.copy2(REPO_ROOT / "pyproject.toml", ctx)
        (ctx / "Dockerfile").write_text(DOCKERFILE)

        result = subprocess.run(
            [docker, "build", "-t", tag, "."],
            cwd=ctx,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )
        assert result.returncode == 0, f"Docker build failed:\n{result.stderr}"

    yield tag

    subprocess.run([docker, "rmi", tag], capture_output=True, timeout=30, check=False)


def _docker_run(
    docker: str, image: str, cmd: str, *, timeout: int = 60
) -> subprocess.CompletedProcess[str]:
    # Forward a GitHub token into the container when the host has one (CI sets
    # GITHUB_TOKEN), so the in-container `reg-meta update` authenticates its
    # GitHub Releases API call — the unauthenticated 60/hr-per-IP limit is
    # easily exhausted from shared CI runner IPs. `-e VAR` (no value) forwards
    # the host value without leaking it into argv. No token set (the usual local
    # case) → no -e flags, behavior unchanged.
    env_flags = [
        flag
        for var in ("GITHUB_TOKEN", "GH_TOKEN")
        if os.environ.get(var)
        for flag in ("-e", var)
    ]
    return subprocess.run(
        [docker, "run", "--rm", *env_flags, image, "sh", "-c", cmd],
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def test_install_and_cli_help(docker: str, image: str):
    """Package installs cleanly and CLI is functional."""
    result = _docker_run(docker, image, "reg-meta --help")
    assert result.returncode == 0
    # reg_meta's CLI prints its custom help to stderr (cli.py `_print_help`, with
    # `add_help=False` on the parser), so assert over the combined stream rather
    # than pinning stdout — robust whichever way help is routed.
    help_text = result.stdout + result.stderr
    assert "search" in help_text
    assert "update" in help_text


def test_version_importable(docker: str, image: str):
    """Package version is importable."""
    result = _docker_run(
        docker, image, "python -c 'import reg_meta; print(reg_meta.__version__)'"
    )
    assert result.returncode == 0
    assert result.stdout.strip()


@pytest.mark.release
def test_update_and_query(docker: str, image: str):
    """Full pipeline: update (downloads DB) from GitHub Releases and run a query.

    Carries the `release` marker on top of the module-level `integration` mark, so
    it needs BOTH --run-integration AND --run-release. The pre-push hook passes
    only the former (so this is skipped — a push isn't blocked when a release is
    merely owed); a post-release / scheduled CI job passes both and runs it as a
    hard gate, where a compatible published asset is guaranteed to exist."""
    cmd = (
        "reg-meta update --yes > /dev/null"
        " && reg-meta --format json search --query kommun --field datacolumn"
    )
    result = _docker_run(docker, image, cmd, timeout=600)
    assert result.returncode == 0, (
        f"Pipeline failed (exit {result.returncode}):\n{result.stderr}"
    )

    payload = json.loads(result.stdout)
    results = payload.get("results", payload.get("data", {}).get("results", []))
    assert len(results) > 0, "Expected search results for 'kommun'"
