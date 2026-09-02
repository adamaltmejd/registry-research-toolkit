"""Root conftest: opt-in gates for expensive test markers.

Add new markers to OPTIONAL_MARKERS to gate them behind --run-<name> flags.
Tests decorated with these markers are skipped unless explicitly opted in.

    pytest                          # unit tests only
    pytest --run-integration        # include integration tests
"""

from __future__ import annotations

import pytest

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
