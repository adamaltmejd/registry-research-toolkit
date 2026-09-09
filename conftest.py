"""Root conftest: opt-in gates for expensive test markers.

Add new markers to OPTIONAL_MARKERS to gate them behind --run-<name> flags.
Tests decorated with these markers are skipped unless explicitly opted in.

    pytest                          # unit tests only
    pytest --run-integration        # include integration tests

--install-mode picks which installation the Docker integration module builds.
It is registered HERE, not in reg_meta/tests/conftest.py, because the pre-push
hook invokes the WHOLE suite in one process (`pytest -n auto --run-integration
--install-mode workspace`): an option registered under a subdirectory conftest
is unknown at argument-parse time and would abort that run.
"""

from __future__ import annotations

import pytest

# The two installation boundaries reg_meta/tests/test_integration.py can build.
# What each one proves — and why `registry` is the default — is documented there,
# next to the Dockerfile steps that implement them.
INSTALL_MODES = ("registry", "workspace")

# marker name -> CLI flag description
OPTIONAL_MARKERS: dict[str, str] = {
    "integration": "run Docker-based integration tests",
    # Tests that need a PUBLISHED release asset: either downloaded from GitHub
    # (a subset of the Docker integration tests) or already fetched and pointed
    # at by the runner (the §12 inventory ↔ flavored-DB gate, which needs no
    # Docker). Gated separately so the pre-push hook (which opts into
    # `integration` as a hard Docker gate) does NOT block a push when a release
    # is merely owed — these belong in a post-release / scheduled CI job.
    "release": "run tests that need a published release asset",
}


def pytest_addoption(parser: pytest.Parser) -> None:
    for name, help_text in OPTIONAL_MARKERS.items():
        parser.addoption(
            f"--run-{name}",
            action="store_true",
            default=False,
            help=help_text,
        )
    parser.addoption(
        "--install-mode",
        choices=INSTALL_MODES,
        default="registry",
        help="installation the Docker integration module builds "
        "(registry: reg_meta alone, --no-sources, deps from PyPI; "
        "workspace: this checkout's reg_schema + reg_meta wheels)",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    for name in OPTIONAL_MARKERS:
        if config.getoption(f"--run-{name}"):
            continue
        skip = pytest.mark.skip(reason=f"needs --run-{name} to run")
        for item in items:
            if name in item.keywords:
                item.add_marker(skip)
