"""`scripts/real_seed_cache.py`'s contract: what it reuses, and what it never stores.

The tool runs as its CLI. A stub `reg-meta-build` (`$REG_REAL_SEED_BUILDER`) stands
in for the real-seed builder at the process boundary and logs each call, so a hit is
a run that never reached it. The keys' code, curation and runtime facts are the
checkout's real ones.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

TOOL = Path(__file__).resolve().parents[1] / "real_seed_cache.py"
STUB = """
import gzip, json, os, sys
from pathlib import Path
from reg_meta_build.curation_compile import tree_sha256

argv = sys.argv[1:]
with open(os.environ["STUB_LOG"], "a") as log:
    log.write(json.dumps(argv) + "\\n")

def option(name):
    return argv[argv.index(name) + 1]

report = Path(option("--report-dir"))
report.mkdir()
Path(option("--diagnostic-db-path")).write_bytes(b"catalog")
with gzip.open(report / "events.jsonl.gz", "wt") as events:
    events.write('{"kind": "issue"}\\n')
failed = os.environ.get("STUB_FAIL") == "1"
(report / "summary.json").write_text(json.dumps({
    "status": "engineering_failure" if failed else "diagnostic_complete",
    "curation_tree_sha256": tree_sha256(Path(option("--curation-dir"))),
}))
print("{}")
sys.exit(10)
"""


def _tool(tmp_path: Path, *args: str, fail: bool = False) -> tuple[int, dict]:
    stub = tmp_path / "stub.py"
    stub.write_text(STUB)
    env = {
        **os.environ,
        "XDG_CACHE_HOME": str(tmp_path / "xdg"),
        "REG_REAL_SEED_CACHE": str(tmp_path / "cache"),
        "REG_REAL_SEED_BUILDER": f"{sys.executable} {stub}",
        "STUB_LOG": str(tmp_path / "calls.jsonl"),
        "STUB_FAIL": "1" if fail else "0",
    }
    proc = subprocess.run(
        [sys.executable, str(TOOL), *args],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    return proc.returncode, json.loads(proc.stdout) if proc.stdout else {}


def _calls(tmp_path: Path) -> int:
    log = tmp_path / "calls.jsonl"
    return len(log.read_text().splitlines()) if log.exists() else 0


def test_build_stores_only_completed_runs_and_hits_only_their_key(
    tmp_path: Path,
) -> None:
    # Fails if a failed run is stored (the next build hits it), if a lookup ignores
    # the stored entry (the repeat runs again), or if the key leaves out the curation
    # tree (the edited tree hits the old entry).
    curation = tmp_path / "curation"
    (curation / "registers").mkdir(parents=True)
    (curation / "registers/a.toml").write_text("[register]\n")

    def build(**kw) -> tuple[int, dict]:
        return _tool(
            tmp_path,
            "build",
            "--prepared",
            str(tmp_path / "prepared"),
            "--input-commit",
            "a" * 40,
            "--input-manifest-sha256",
            "b" * 64,
            "--diagnostic",
            "--curation-dir",
            str(curation),
            **kw,
        )

    code, failed = build(fail=True)
    assert (code != 0, failed["stored"]) == (True, False)
    assert Path(failed["run_dir"], "report/summary.json").is_file()

    assert build()[1]["hit"] is False
    code, again = build()
    assert (code, again["hit"], _calls(tmp_path)) == (0, True, 2)
    assert Path(again["database"]).read_bytes() == b"catalog"

    (curation / "registers/a.toml").write_text("[register]\nname = 'edited'\n")
    code, edited = build()
    assert (code, edited["hit"], _calls(tmp_path)) == (0, False, 3)
    assert edited["key"] != again["key"]


def test_prepare_key_covers_preparation_code_but_not_resolution(
    tmp_path: Path,
) -> None:
    # Fails if the import walk loses the preparation entry point (a prepare change
    # would hit a stale record) or grows to the whole builder (every resolution
    # change would repeat a 1-2 h preparation).
    bundle = tmp_path / "repo/bundle"
    bundle.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(tmp_path / "repo")], check=True)
    code, key = _tool(
        tmp_path,
        "prepare",
        "--input-bundle",
        str(bundle),
        "--input-commit",
        "c" * 40,
        "--input-manifest-sha256",
        "d" * 64,
        "--key",
    )
    files = key["fields"]["code"]
    package = "reg_meta_build/src/reg_meta_build"
    assert (code, key["fields"]["bundle_path"]) == (0, "bundle/")
    assert f"{package}/prepared_catalog.py" in files
    assert f"{package}/pipeline.py" not in files
