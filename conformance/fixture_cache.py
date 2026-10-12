"""Build one synthetic artifact into the fixture cache; print its path.

    uv run python conformance/fixture_cache.py reader catalog
    uv run python conformance/fixture_cache.py fixtures/webapp-dev catalog \\
        --docs fixtures/webapp-dev/docs

FIXTURE is `reader`, `reader/<case>`, `fixtures/<case>` or a reg_meta_build holdings
case name. `--docs` names a readable docs source (relative to `conformance/cases`)
whose `reg_meta_docs.db` is built beside the catalog. The artifact gets the same
identity as conformance's, so this prints the entry the suite reads, building it only
on a miss. Conformance builds in-process; this is for consumers outside pytest. The
path stays valid for 6 hours after its last lookup, so look it up again rather than
keeping it.
"""

from __future__ import annotations

import argparse
import sys

from reader_artifacts import CASES, FIXTURE_IMPORT_DATE, cached_reader_artifact


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        epilog="The printed path stays valid for 6 hours after its last lookup.",
    )
    parser.add_argument("fixture")
    parser.add_argument("kind", choices=("catalog", "steward"))
    parser.add_argument("--docs", help="docs source, relative to conformance/cases")
    args = parser.parse_args()
    print(
        cached_reader_artifact(
            args.fixture,
            args.kind,
            identity_overrides={"import_date": FIXTURE_IMPORT_DATE},
            docs=CASES / args.docs if args.docs else None,
        )
    )
    print("valid for 6 hours after this lookup", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
