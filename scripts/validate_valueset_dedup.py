"""Validate the value-set dedup + year-projection rebuild.

Thin wrapper around ``reg_meta_build.validate.validate_built_db`` — kept
so maintainers can re-validate an existing DB without rebuilding it.
The build-time path runs the same checks automatically (`reg-meta-build
build-db` validates by default; opt out with `--no-validate`, issue #92).
Runs with ``corpus=True`` like the real build, so it expects the full SOS
volume — point it at a real shipped DB, not a synthetic/partial one.

Usage:
    uv run python scripts/validate_valueset_dedup.py [DB_PATH]
"""

from __future__ import annotations

import sys
from pathlib import Path

from reg_meta.errors import RegMetaError
from reg_meta_build.validate import validate_built_db

DEFAULT_DB = Path("/tmp/reg-meta-rebuild-test/reg_meta.db")


def main() -> None:
    db_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_DB
    try:
        result = validate_built_db(db_path, corpus=True)
    except FileNotFoundError as exc:
        sys.exit(str(exc))
    except RegMetaError as exc:
        # `open_db` raises RegMetaError on a missing or unreadable DB;
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
