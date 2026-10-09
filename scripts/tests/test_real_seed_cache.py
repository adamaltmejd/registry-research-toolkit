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
PREPARED_COMMIT = "a" * 40
PREPARED_SHA256 = "b" * 64
STUB = """
import gzip, hashlib, json, os, sys
from pathlib import Path

argv = sys.argv[1:]
with open(os.environ["STUB_LOG"], "a") as log:
    log.write(json.dumps(argv) + "\\n")

def option(name):
    return argv[argv.index(name) + 1] if name in argv else None

if "prepare-sources" in argv:
    output = Path(option("--output-dir"))
    output.mkdir(parents=True)
    manifest = Path(option("--input-bundle"), "catalog-bundle.json").read_bytes()
    (output / "manifest.json").write_bytes(manifest)
    print(json.dumps({
        "status": "prepared",
        "prepared_manifest_sha256": hashlib.sha256(manifest).hexdigest(),
    }))
    sys.exit(0)

from reg_meta_build.curation_compile import tree_sha256

report = Path(option("--report-dir"))
report.mkdir()
Path(option("--diagnostic-db-path")).write_bytes(b"catalog")
with gzip.open(report / "events.jsonl.gz", "wt") as events:
    events.write('{"kind": "issue"}\\n')
failed = os.environ.get("STUB_FAIL") == "1"
(report / "summary.json").write_text(json.dumps({
    "status": "engineering_failure" if failed else "diagnostic_complete",
    "curation_tree_sha256": tree_sha256(Path(option("--curation-dir")).resolve()),
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


def _diagnostic_build(tmp_path: Path, curation: Path, **kw) -> tuple[int, dict]:
    return _tool(
        tmp_path,
        "build",
        "--prepared",
        str(tmp_path / "prepared"),
        "--input-commit",
        PREPARED_COMMIT,
        "--input-manifest-sha256",
        PREPARED_SHA256,
        "--diagnostic",
        "--curation-dir",
        str(curation),
        **kw,
    )


def _curation(tmp_path: Path) -> Path:
    curation = tmp_path / "curation"
    (curation / "registers").mkdir(parents=True)
    (curation / "registers/a.toml").write_text("[register]\n")
    return curation


def test_build_is_reused_only_for_the_same_keyed_inputs(tmp_path: Path) -> None:
    # Fails if a lookup ignores the stored entry (the second build runs again) or if
    # the key leaves out the curation tree (the edited tree hits the old entry).
    curation = _curation(tmp_path)
    assert _diagnostic_build(tmp_path, curation)[1]["hit"] is False
    code, again = _diagnostic_build(tmp_path, curation)
    assert (code, again["hit"], _calls(tmp_path)) == (0, True, 1)
    assert Path(again["database"]).read_bytes() == b"catalog"

    (curation / "registers/a.toml").write_text("[register]\nname = 'edited'\n")
    code, edited = _diagnostic_build(tmp_path, curation)
    assert (code, edited["hit"], _calls(tmp_path)) == (0, False, 2)
    assert edited["key"] != again["key"]


def test_failed_build_is_never_stored(tmp_path: Path) -> None:
    # Fails if a run whose summary is not `diagnostic_complete` is stored: the next
    # build would then hit the failed output instead of running.
    curation = _curation(tmp_path)
    code, failed = _diagnostic_build(tmp_path, curation, fail=True)
    assert (code != 0, failed["stored"]) == (True, False)
    assert Path(failed["run_dir"], "report/summary.json").is_file()
    assert _diagnostic_build(tmp_path, curation)[1]["hit"] is False
    assert _calls(tmp_path) == 2


def test_prepare_record_that_no_longer_holds_is_a_miss(tmp_path: Path) -> None:
    # Fails if a lookup trusts the index record without re-checking the prepared
    # tree: the changed manifest would be returned as a hit.
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "catalog-bundle.json").write_text('{"bundle": 1}')

    def prepare(output: str) -> tuple[int, dict]:
        return _tool(
            tmp_path,
            "prepare",
            "--input-bundle",
            str(bundle),
            "--input-commit",
            "c" * 40,
            "--input-manifest-sha256",
            "d" * 64,
            "--output-dir",
            str(tmp_path / output),
        )

    assert prepare("first")[1]["hit"] is False
    code, again = prepare("unused")
    assert (code, again["hit"], _calls(tmp_path)) == (0, True, 1)

    (tmp_path / "first/manifest.json").write_text('{"changed": true}')
    code, rerun = prepare("second")
    assert (code, rerun["hit"], rerun["prepared_path"]) == (
        0,
        False,
        str((tmp_path / "second").resolve()),
    )
    assert _calls(tmp_path) == 2
