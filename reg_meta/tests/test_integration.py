"""Integration test: full install-and-query pipeline in a Docker container.

Not run by default. Requires Docker, and `--install-mode registry` (the default)
additionally requires reg_meta's declared dependencies to be resolvable from
PyPI. The `release`-marked test also needs a published GitHub release asset.

    pytest --run-integration reg_meta/tests/test_integration.py
    pytest --run-integration --install-mode workspace reg_meta/tests/test_integration.py

The two modes answer two different questions and neither substitutes for the
other:

  - `registry` is the PUBLICATION boundary. reg_schema is kept out of the build
    context and reg_meta installs with `--no-sources`, so a green run means the
    `reg-schema` floor in reg_meta's published metadata actually resolved from
    the index. It is red for as long as the sibling release is owed, which is
    exactly what it is for — a reg_meta wheel must not reach PyPI before it.
  - `workspace` is the SOURCE-COHERENCE boundary. It builds this checkout's
    reg_schema and reg_meta wheels in the same pinned container and installs
    both, so main can carry a coherent unpublished sibling bump and still get a
    hard packaging gate on every push. A green run is NOT evidence that
    reg-schema is on PyPI.
"""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import tempfile
import textwrap
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[2]

# Both external images below carry a readable exact tag PLUS its immutable
# multi-arch manifest digest, matching the workspace baseline
# (reg_webapp/Dockerfile, .yard/Dockerfile). Floating `python:3.14-slim` /
# `uv:latest` made this test drift with whatever uv shipped that week, which is
# how uv's rejection of the workspace source (see INSTALL_STEP's `registry`
# note) landed as a surprise failure; bump both halves deliberately, with the
# rest of the baseline.
DOCKERFILE_PREAMBLE = textwrap.dedent("""\
    FROM python:3.14.7-slim-bookworm@sha256:9ab8d9c8514b44f90cf0029dd42fdd7e9e211e639c8b995304cc04568dee900f

    COPY --from=ghcr.io/astral-sh/uv:0.12.11@sha256:79c6f4776b851471cc73b7d21d0cc834bb94383c292e83640d27eff512864df7 /uv /usr/local/bin/uv

    WORKDIR /src
    COPY . .

    RUN uv venv /opt/venv
    ENV VIRTUAL_ENV=/opt/venv
    ENV PATH="/opt/venv/bin:$PATH"
""")

# The install step is the whole difference between the modes.
#
# registry: `--no-sources` makes uv ignore the root pyproject's
# `[tool.uv.sources]` and resolve reg_meta's dependencies from the registry
# instead. The root pyproject stays in the context precisely so the install is
# proven to hold in the presence of the workspace config — the answer to
# `reg-schema = { workspace = true }` is to ignore the source, not to copy
# reg_schema in, which would install the local tree and hide whether reg_meta's
# published metadata resolves at all. There is deliberately no local-wheel or
# alternate-index fallback here: a fallback would turn this gate green while the
# published wheel stayed uninstallable.
#
# workspace: both wheels are built from this checkout and installed together in
# one resolution, so reg_meta's `reg-schema` floor is satisfied by the local
# 3.x wheel while pydantic/zstandard still come from the index — a normal
# resolution, not `--no-deps`.
INSTALL_STEP: dict[str, str] = {
    "registry": textwrap.dedent("""\
        RUN uv pip install --no-sources "./reg_meta"
    """),
    "workspace": textwrap.dedent("""\
        RUN uv build --wheel --out-dir /wheels ./reg_schema \\
            && uv build --wheel --out-dir /wheels ./reg_meta \\
            && uv pip install /wheels/reg_schema-*.whl /wheels/reg_meta-*.whl
    """),
}

# Minimal per-mode build context, copied into the temp context dir verbatim
# (directories with copytree, files with copy2). registry pairs reg_meta with
# the workspace root pyproject for the reason above; workspace ships neither it
# nor the other members, so uv sees two standalone projects rather than a
# partial workspace. `tests`/`test_corpus` are excluded from the copy on top of
# the usual build droppings: neither reaches the wheel (both packages are
# src-layout), and letting them in busts the `COPY` layer — and so re-runs the
# whole install — on the test-only edits this gate fires for most often.
CONTEXT_PATHS: dict[str, tuple[str, ...]] = {
    "registry": ("reg_meta", "pyproject.toml"),
    "workspace": ("reg_meta", "reg_schema"),
}
CONTEXT_IGNORE = shutil.ignore_patterns(
    "__pycache__", "*.pyc", ".pytest_cache", "*.egg-info", "tests", "test_corpus"
)

IMAGE_TAG = "reg-meta-integration-test"

# PEP 610: pip/uv write `direct_url.json` into the .dist-info of a distribution
# installed from a local path or URL, and omit it for one resolved by NAME. It
# is therefore installed-tree evidence of HOW reg_schema got there — unlike the
# install command's text, which proves only what we asked for.
SCHEMA_PROVENANCE_PY = (
    "import importlib.metadata as m; "
    'print(m.distribution("reg-schema").read_text("direct_url.json") or "")'
)
VERSIONS_PY = (
    "import reg_meta, reg_schema; print(reg_meta.__version__, reg_schema.__version__)"
)


@pytest.fixture(scope="module")
def install_mode(request: pytest.FixtureRequest) -> str:
    """Which installation boundary this run exercises (root conftest option)."""
    return request.config.getoption("--install-mode")


@pytest.fixture(scope="module")
def docker() -> str:
    """Resolve a working Docker daemon, or FAIL the test.

    This fixture only runs when integration tests are opted into via
    ``--run-integration`` (the root conftest skips them at collection otherwise),
    so opting in IS the assertion that Docker is available: a missing binary or a
    stopped daemon is a hard failure here, not a skip. The pre-push hook
    (.pre-commit-config.yaml) passes ``--run-integration`` (with
    ``--install-mode workspace``), which is what makes a running Docker daemon a
    required pre-push gate — start Docker and push again rather than bypassing
    with ``--no-verify``.
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
def image(docker: str, install_mode: str) -> str:
    """Build a Docker image with reg_meta installed per ``--install-mode``.

    The tag carries the mode AND the xdist worker (PYTEST_XDIST_WORKER). The
    worker half stops a parallel `-n auto` run — what the pre-push hook uses —
    from racing on a shared tag: without it, two workers each get their own
    module-scoped instance of this fixture, and one's teardown `rmi` can delete
    the image out from under the other's `docker run`. The mode half stops two
    differently-installed images from colliding on one tag (and keeps each
    mode's layer cache intact). Docker's layer cache makes the extra builds
    effectively free. A serial run (CI, plain pytest) has no worker id.
    """
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    tag = f"{IMAGE_TAG}-{install_mode}"
    if worker:
        tag = f"{tag}-{worker}"
    with tempfile.TemporaryDirectory() as ctx_str:
        ctx = Path(ctx_str)

        for name in CONTEXT_PATHS[install_mode]:
            src = REPO_ROOT / name
            if src.is_dir():
                shutil.copytree(src, ctx / name, ignore=CONTEXT_IGNORE)
            else:
                shutil.copy2(src, ctx / name)
        (ctx / "Dockerfile").write_text(
            DOCKERFILE_PREAMBLE + INSTALL_STEP[install_mode]
        )

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
    """Both installed packages import and report their version."""
    result = _docker_run(docker, image, f"python -c {shlex.quote(VERSIONS_PY)}")
    assert result.returncode == 0
    assert result.stdout.strip()
    assert len(result.stdout.split()) == 2, result.stdout


def test_installed_dependencies_are_satisfied(docker: str, image: str):
    """`uv pip check`: every installed distribution's requirements are met.

    In registry mode this is the publication guarantee in its observable form —
    reg_meta's `reg-schema` floor was met by a distribution uv actually
    resolved from the index. In workspace mode it is the compatibility check
    between the two wheels this checkout just built.
    """
    result = _docker_run(docker, image, "uv pip check")
    assert result.returncode == 0, (
        f"uv pip check failed:\n{result.stdout}{result.stderr}"
    )


def test_installed_schema_provenance(docker: str, image: str, install_mode: str):
    """The installed reg_schema was installed the way its mode requires.

    Workspace mode's has a `direct_url.json` naming the local wheel built in
    the container; registry mode's has none, because uv resolved `reg-schema`
    by NAME. A named resolution is all PEP 610 records — not which index served
    it: an extra index, or `--no-index --find-links`, would leave no
    `direct_url.json` either. "The registry" means PyPI here because of the
    setup — clean container, default index, no index configuration in the image
    or the install step — not because of this assertion.

    What this does catch is a direct local-wheel or URL install of reg_schema
    sneaking into registry mode, which would leave the install command's text
    unchanged while making the mode's guarantee vacuous.
    """
    result = _docker_run(
        docker, image, f"python -c {shlex.quote(SCHEMA_PROVENANCE_PY)}"
    )
    assert result.returncode == 0, result.stderr
    direct_url = result.stdout.strip()
    if install_mode == "workspace":
        url = json.loads(direct_url)["url"]
        assert url.startswith("file://") and url.endswith(".whl"), direct_url
    else:
        assert not direct_url, (
            f"reg-schema was installed directly, not resolved by name: {direct_url}"
        )


@pytest.mark.release
def test_update_and_query(docker: str, image: str):
    """Full pipeline: update (downloads DB) from GitHub Releases and run a query.

    Carries the `release` marker on top of the module-level `integration` mark, so
    it needs BOTH --run-integration AND --run-release. The pre-push hook passes
    only the former (so this is skipped — a push isn't blocked when a release is
    merely owed); a post-release / scheduled CI job passes both and runs it as a
    hard gate, where a compatible published asset is guaranteed to exist. That job
    runs in `registry` mode, so the container it queries from is the published
    consumer's install, not a locally built one."""
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
