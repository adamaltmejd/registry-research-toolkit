"""Build one conformance fixture into the fixture cache; print its path.

    uv run python conformance/fixture_cache.py base

FIXTURE is a directory name under `conformance/cases/fixtures`. The artifact is built
by `target/debug/reg-build`, which must be compiled. This prints the entry the suite
reads, building it only on a miss; conformance builds in-process, this is for
consumers outside pytest. The path stays valid for 6 hours after its last lookup, so
look it up again rather than keeping it.
"""

from __future__ import annotations

import argparse
import sys

from reader_artifacts import cached_reader_artifact


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        epilog="The printed path stays valid for 6 hours after its last lookup.",
    )
    parser.add_argument("fixture")
    args = parser.parse_args()
    print(cached_reader_artifact(args.fixture))
    print("valid for 6 hours after this lookup", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
