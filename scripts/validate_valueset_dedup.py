"""Validate the value-set dedup + year-projection rebuild.

Thin wrapper around ``regmeta_build.validate.validate_built_db`` — kept
so maintainers can re-validate an existing DB without rebuilding it.
The build-time path lives in `regmeta-build build-db --validate` (issue
#92); both call the same module so checks stay in one place.

Usage:
    uv run python scripts/validate_valueset_dedup.py [DB_PATH]
"""

from __future__ import annotations

import sys
from pathlib import Path

from regmeta.errors import RegmetaError
from regmeta_build.validate import validate_built_db

DEFAULT_DB = Path("/tmp/regmeta-rebuild-test/regmeta.db")


def main() -> None:
    db_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DB
    try:
        result = validate_built_db(db_path)
    except FileNotFoundError as exc:
        sys.exit(str(exc))
    except RegmetaError as exc:
        # `open_db` raises RegmetaError on a missing or unreadable DB;
        # surface its message rather than letting a traceback escape.
        sys.exit(f"{exc.code}: {exc.message}")
    print(result.format_report())
    print()
    if result.failures:
        print(f"FAIL: {len(result.failures)} check(s) failed")
        sys.exit(1)
    print("PASS")


if __name__ == "__main__":
    main()
