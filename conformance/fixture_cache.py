"""Build one synthetic artifact into the fixture cache; print its path.

    uv run python conformance/fixture_cache.py --fixture reader catalog
    uv run python conformance/fixture_cache.py --source DIR steward --identity K=V

Conformance and the reader tests build through the same cache in-process; this
script is for consumers outside pytest (and for pre-warming). It prints the
read-only `reg_meta.db` path, building it only on a miss. The path stays valid for
6 hours after its last lookup; after that a new build generation may prune it, so
look it up again rather than keeping it. The cache directory is
`$REG_FIXTURE_CACHE`, else `registry-research-toolkit-fixtures` in the system temp
directory.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "reg_meta/tests"))

from reader_artifacts import FIXTURE_CACHE_RETENTION_SECONDS, cached_reader_artifact

VALIDITY_HOURS = FIXTURE_CACHE_RETENTION_SECONDS // 3600


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        epilog=f"The printed path stays valid for {VALIDITY_HOURS} hours after its "
        "last lookup; look it up again rather than keeping it.",
    )
    fixture = parser.add_mutually_exclusive_group(required=True)
    fixture.add_argument(
        "--fixture",
        help="`reader`, `reader/<case>` or a reg_meta_build holdings case name",
    )
    fixture.add_argument("--source", type=Path, help="a readable source directory")
    parser.add_argument("kind", choices=("catalog", "steward"))
    parser.add_argument(
        "--identity",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="manifest identity override (repeatable)",
    )
    args = parser.parse_args(argv)
    if any("=" not in item for item in args.identity):
        parser.error("--identity takes KEY=VALUE")
    path = cached_reader_artifact(
        args.source.resolve() if args.source else args.fixture,
        args.kind,
        identity_overrides=dict(item.split("=", 1) for item in args.identity),
    )
    print(path)
    print(
        f"valid for {VALIDITY_HOURS} hours after this lookup; look it up again "
        "rather than keeping it",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
