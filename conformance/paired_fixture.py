"""Build a synthetic paired catalog into OUT_DIR; print its catalog selector.

    uv run python conformance/paired_fixture.py /tmp/paired-fixture

Requires git and this checkout's compiled target/debug/reg-build. Inputs come
from conformance/cases/holdings; no private steward repository is used.
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path

from holdings_artifacts import prepare_holdings, run


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("out_dir", type=Path)
    out = parser.parse_args().out_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="reg-webapp-paired-") as tmp:
        base, _, command = prepare_holdings(Path(tmp))
        result = run([*command, "--out", out / "reg_meta_fixture.db"])
        if result.returncode:
            print(result.stderr, file=sys.stderr, end="")
            return result.returncode
        shutil.copyfile(base, out / "reg_meta.db")
    print("fixture")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
