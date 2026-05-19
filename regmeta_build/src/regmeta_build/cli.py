"""regmeta-build CLI entrypoint.

Build pipeline for the regmeta SQLite databases. The query side lives in
`regmeta`; this package owns the artifacts those queries read from.
"""

from __future__ import annotations

import sys


def main(argv: list[str] | None = None) -> int:
    # Subcommands are wired in subsequent commits as we carve out
    # `regmeta maintain build-db|seed-slugs|precheck-slugs|parse-sos|build-docs`
    # from regmeta/src/regmeta/cli.py.
    print("regmeta-build: CLI under construction", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
