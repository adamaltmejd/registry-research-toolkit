"""Build one synthetic artifact into the fixture cache; print its path.

    uv run python conformance/fixture_cache.py reader catalog

FIXTURE is `reader`, `reader/<case>` or a reg_meta_build holdings case name. The
artifact gets the same identity as conformance's, so this prints the entry the
suite reads, building it only on a miss. Conformance builds in-process; this is
for consumers outside pytest. The path stays valid for 6 hours after its last
lookup, so look it up again rather than keeping it.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "reg_meta/tests"))

from reader_artifacts import FIXTURE_IMPORT_DATE, cached_reader_artifact


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        epilog="The printed path stays valid for 6 hours after its last lookup.",
    )
    parser.add_argument("fixture")
    parser.add_argument("kind", choices=("catalog", "steward"))
    args = parser.parse_args()
    print(
        cached_reader_artifact(
            args.fixture,
            args.kind,
            identity_overrides={"import_date": FIXTURE_IMPORT_DATE},
        )
    )
    print("valid for 6 hours after this lookup", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
