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
    "integration": "run native container integration tests",
    # Tests that need a PUBLISHED release asset: either downloaded from GitHub
    # (a subset of the container integration tests) or already fetched and selected
    # with --artifact-dir for conformance checks (which need no container).
    # Gated separately so a run that opts into `integration` as a hard native
    # container gate (CI's package-integration job) does NOT fail when a release
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
        "--artifact-dir",
        type=str,
        default=None,
        help="conformance artifact directory (requires --run-release)",
    )
    parser.addoption(
        "--holdings-input",
        type=str,
        default=None,
        help="accepted holdings input directory (requires --run-release and --artifact-dir)",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    for name in OPTIONAL_MARKERS:
        if config.getoption(f"--run-{name}"):
            continue
        skip = pytest.mark.skip(reason=f"needs --run-{name} to run")
        for item in items:
            # Not `name in item.keywords`: keywords also hold directory, module and
            # parametrize-id names, so a checkout under `.../integration/` skipped
            # the whole suite while reporting green.
            if item.get_closest_marker(name) is not None:
                item.add_marker(skip)
