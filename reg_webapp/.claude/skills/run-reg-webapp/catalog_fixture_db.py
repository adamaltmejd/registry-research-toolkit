#!/usr/bin/env python3
"""Write the backend's synthetic catalog DB into a directory, for REG_META_DB.

The browser flows need a real backend over real catalog data, and the repo
already builds exactly that: ``backend/scripts/fixture_db.py`` (a slugged
``scb/lisa`` + ``scb/rams`` DB with the boot manifest stamped) is the builder the
``/api/catalog`` and ``/api/project/*`` suites resolve against, and the one
``dev.sh --fixture-db`` runs. It is imported from that script rather than rebuilt
here, so the flows' catalog and the suite's can never drift.

    python3 reg_webapp/.claude/skills/run-reg-webapp/catalog_fixture_db.py <dir>
    REG_META_DB=<dir> bash reg_webapp/.claude/skills/run-reg-webapp/dev.sh flows <out>
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import reg_meta.db

# Load the builder script by spec under a UNIQUE module name, the way
# test_openapi_snapshot.py loads its sibling script: it lives in `scripts/`, not
# on the import path, and a bare-name import would need sys.path injection. The
# script does its own sys.path work for the bare-name `_slugged_db` builder it
# wraps.
# reg_webapp/.claude/skills/run-reg-webapp/ → reg_webapp/.
_FIXTURE_DB_PATH = (
    Path(__file__).resolve().parents[3] / "backend" / "scripts" / "fixture_db.py"
)
_spec = importlib.util.spec_from_file_location(
    "reg_webapp_fixture_db_flows", _FIXTURE_DB_PATH
)
assert _spec and _spec.loader
fixture_db = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fixture_db)


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: catalog_fixture_db.py <db-dir>", file=sys.stderr)
        return 2
    db_dir = Path(sys.argv[1])
    db_dir.mkdir(parents=True, exist_ok=True)

    db_path = db_dir / reg_meta.db.DB_FILENAME
    fixture_db.build_catalog_fixture_db(db_path)
    print(f"catalog fixture DB: {db_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
