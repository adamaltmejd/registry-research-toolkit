"""Build one synthetic artifact into the per-user fixture cache; print its path.

    uv run python conformance/fixture_cache.py --fixture reader catalog
    uv run python conformance/fixture_cache.py --source DIR steward --identity K=V

Conformance and the reader tests build through the same cache in-process; this
script is for consumers outside pytest (and for pre-warming). It prints the
read-only `reg_meta.db` path, building it only on a miss. The cache directory is
`$REG_FIXTURE_CACHE`, else `$XDG_CACHE_HOME` (or `~/.cache`) under
`registry-research-toolkit/fixture-artifacts`.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "reg_meta/tests"))

from reader_artifacts import cached_reader_artifact


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
