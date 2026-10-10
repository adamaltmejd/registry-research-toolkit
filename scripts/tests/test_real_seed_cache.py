"""`scripts/real_seed_cache.py`'s contract: what it reuses, and what it never stores.

The tool runs as its CLI. Its process boundaries are stubbed: the real-seed
`reg-meta-build` (`$REG_REAL_SEED_BUILDER`, which logs each call, so a hit is a run that
never reached it; it writes a minimal resolved bundle for `build-db --resolved-out` and
places one with `materialize-db`), the project-environment probe
(`$REG_REAL_SEED_PYTHON`, which reports a content digest of the curation tree, a fixed
resolve-code fingerprint and, on request, a failed admission) and `rustc` (on `PATH`).
The keyed code is real source.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
REPO = SCRIPTS.parent
CURATION_SHA = """
def curation_sha(root):
    import hashlib
    from pathlib import Path
    files = sorted(p for p in Path(root).rglob("*") if p.is_file())
    return hashlib.sha256(b"".join(p.read_bytes() for p in files)).hexdigest()
"""
BUILDER = (
    CURATION_SHA
    + """
import gzip, hashlib, json, os, sys
from pathlib import Path

argv = sys.argv[1:]
with open(os.environ["STUB_LOG"], "a") as log:
    log.write(json.dumps(argv) + "\\n")

def option(name):
    return argv[argv.index(name) + 1]

# A full build writes one ledger member; materialize-db appends its tail to the
# bundle's member, so the phased ledger is two members with the same text.
RESOLVED, TAIL = b'{"kind": "issue"}\\n', b'{"kind": "corpus_validation"}\\n'
report = Path(option("--report-dir"))
if argv[0] == "materialize-db":
    bundle = Path(option("--resolved"))
    record = json.loads((bundle / "bundle.json").read_text())
    events = (bundle / "events.jsonl.gz").read_bytes()
    if hashlib.sha256(events).hexdigest() != record["events_sha256"]:
        print(json.dumps({"error": {"code": "resolved_bundle_digest_mismatch"}}))
        sys.exit(10)
    curation = record["curation_tree_sha256"]
    report.mkdir()
    (report / "events.jsonl.gz").write_bytes(events + gzip.compress(TAIL, mtime=0))
else:
    curation = curation_sha(option("--curation-dir"))
    report.mkdir()
    (report / "events.jsonl.gz").write_bytes(gzip.compress(RESOLVED + TAIL, mtime=0))
    if "--resolved-out" in argv:
        bundle = Path(option("--resolved-out"))
        bundle.mkdir()
        events = gzip.compress(RESOLVED, mtime=0)
        (bundle / "events.jsonl.gz").write_bytes(events)
        (bundle / "bundle.json").write_text(json.dumps({
            "resolve_code_sha256": "stub-resolve",
            "mode": "diagnostic",
            "registers": sorted(set(option("--registers").split(","))),
            "prepared_commit": option("--input-commit"),
            "prepared_manifest_sha256": option("--input-manifest-sha256"),
            "curation_tree_sha256": curation,
            "events_sha256": hashlib.sha256(events).hexdigest(),
        }))
Path(option("--diagnostic-db-path")).write_bytes(
    b"drifted" if os.environ.get("STUB_DRIFT") == "1" else b"catalog"
)
failed = os.environ.get("STUB_FAIL") == "1"
(report / "summary.json").write_text(json.dumps({
    "status": "engineering_failure" if failed else "diagnostic_complete",
    "curation_tree_sha256": curation,
    "database": option("--diagnostic-db-path"),
}))
print("{}")
sys.exit(10)
"""
)
PROBE = (
    CURATION_SHA
    + """
import json, os, sys
request = json.loads(sys.argv[-1])
facts = {"python": "stub", "sqlite": "stub"}
if request.get("curation"):
    facts["curation_tree_sha256"] = curation_sha(request["curation"])
if request.get("resolve_code"):
    facts["resolve_code_sha256"] = "stub-resolve"
if request.get("admit") and os.environ.get("STUB_ADMIT_FAIL") == "1":
    facts["admission_error"] = {"code": "prepared_input_mismatch", "message": "stub"}
print(json.dumps(facts))
"""
)


def _tool(
    tmp_path: Path, *args: str, tool: Path = SCRIPTS / "real_seed_cache.py", **flags
) -> tuple[int, dict]:
    (tmp_path / "builder.py").write_text(BUILDER)
    (tmp_path / "probe.py").write_text(PROBE)
    rustc = tmp_path / "bin/rustc"
    rustc.parent.mkdir(exist_ok=True)
    rustc.write_text("#!/bin/sh\necho 'rustc stub'\n")
    rustc.chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{rustc.parent}{os.pathsep}{os.environ['PATH']}",
        "XDG_CACHE_HOME": str(tmp_path / "xdg"),
        "REG_REAL_SEED_CACHE": str(tmp_path / "cache"),
        "REG_REAL_SEED_BUILDER": f"{sys.executable} {tmp_path / 'builder.py'}",
        "REG_REAL_SEED_PYTHON": f"{sys.executable} {tmp_path / 'probe.py'}",
        "STUB_LOG": str(tmp_path / "calls.jsonl"),
        **{f"STUB_{name.upper()}": "1" for name, on in flags.items() if on},
    }
    proc = subprocess.run(
        [sys.executable, str(tool), *args],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    return proc.returncode, json.loads(proc.stdout) if proc.stdout else {
        "stderr": proc.stderr
    }


def _calls(tmp_path: Path) -> list[str]:
    """The builder subcommands run so far, in order."""
    log = tmp_path / "calls.jsonl"
    lines = log.read_text().splitlines() if log.exists() else []
    return [json.loads(line)[0] for line in lines]


def test_build_stores_only_completed_runs_and_hits_only_admitted_keys(
    tmp_path: Path,
) -> None:
    # Fails if a failed run is stored (the next build hits it), if a lookup ignores
    # the stored entry or keys `--registers` in the order given (the reordered repeat
    # runs again), if the stored report still names the
    # staging directory, if a hit skips the build's admission checks (a changed
    # prepared checkout returns the old entry), if a hit skips the ledger (a truncated
    # one is returned), if a failed `--verify` leaves its entry or its resolve entry
    # reachable (the next build hits or rematerializes it), or if the key leaves out
    # the curation tree (the edited tree hits the old entry).
    curation = tmp_path / "curation"
    (curation / "registers").mkdir(parents=True)
    (curation / "registers/a.toml").write_text("[register]\n")

    def build(*extra: str, registers: str = "SCB:B,SCB:A", **flags) -> tuple[int, dict]:
        return _tool(
            tmp_path,
            "build",
            *extra,
            "--prepared",
            str(tmp_path / "prepared"),
            "--input-commit",
            "a" * 40,
            "--input-manifest-sha256",
            "b" * 64,
            "--diagnostic",
            "--curation-dir",
            str(curation),
            "--registers",
            registers,
            **flags,
        )

    code, failed = build(fail=True)
    assert (code != 0, failed["stored"]) == (True, False)
    assert Path(failed["run_dir"], "report/summary.json").is_file()
    # A failed run's bundle is never stored: it would place a failed resolution.
    resolve = tmp_path / "cache/resolve"
    assert not [p for p in resolve.iterdir() if not p.name.startswith(".")]

    built = build()[1]
    assert (built["hit"], built["phased"]) == (False, False)
    code, again = build(registers="SCB:A,SCB:B")
    assert (code, again["hit"], len(_calls(tmp_path))) == (0, True, 2)
    assert Path(again["database"]).read_bytes() == b"catalog"
    summary = json.loads(Path(again["report"], "summary.json").read_text())
    assert summary["database"] == again["database"]

    code, refused = build(admit_fail=True)
    assert (code, refused["error"]["code"], len(_calls(tmp_path))) == (
        10,
        "prepared_input_mismatch",
        2,
    )

    # Fails if a build miss whose resolve key hits reruns build-db instead of
    # placing the stored bundle with materialize-db, or if the phased entry's report
    # still names its staging directory.
    ledger = Path(again["report"], "events.jsonl.gz")
    ledger.write_bytes(ledger.read_bytes()[:-1])
    code, phased = build()
    assert (code, phased["hit"], phased["phased"]) == (0, False, True)
    assert _calls(tmp_path)[2:] == ["materialize-db"]
    summary = json.loads(Path(phased["report"], "summary.json").read_text())
    assert summary["database"] == phased["database"]

    # Fails if --verify compares compressed ledgers: the phased entry's two gzip
    # members hold the same events as the full rebuild's one.
    code, verified = build("--verify")
    assert (code, verified["identical"]) == (0, True)

    # Fails if a bundle materialize-db refuses is an error, or stays stored (every
    # later miss would retry it), instead of a full build that stores a fresh one.
    bundle = next(p for p in resolve.iterdir() if not p.name.startswith("."))
    (bundle / "bundle/events.jsonl.gz").write_bytes(b"damaged")
    ledger.write_bytes(ledger.read_bytes()[:-1])
    code, rebuilt = build()
    assert (code, rebuilt["hit"], rebuilt["phased"]) == (0, False, False)
    assert _calls(tmp_path)[4:] == ["materialize-db", "build-db"]

    # Fails if a failed --verify leaves the resolve entry reusable: the next build
    # would rematerialize the rejected resolution instead of building in full.
    code, verified = build("--verify", drift=True)
    assert (code, Path(verified["quarantined_entry"]).is_dir()) == (1, True)
    assert build()[1]["phased"] is False
    assert _calls(tmp_path)[6:] == ["build-db", "build-db"]

    # Fails if the resolve key leaves out the curation tree: the edited tree would
    # place the old bundle.
    (curation / "registers/a.toml").write_text("[register]\nname = 'edited'\n")
    code, edited = build()
    assert (code, edited["hit"], edited["phased"]) == (0, False, False)
    assert _calls(tmp_path)[8:] == ["build-db"]
    assert edited["key"] != again["key"]


def test_prepare_key_moves_with_preparation_code_only(tmp_path: Path) -> None:
    # Fails if the prepare key stops covering code prepare runs (an edit to a source
    # adapter, or to a new module not yet committed, would hit a stale preparation),
    # grows to cover resolution code (every resolution change would repeat a 1-2 h
    # preparation) or ignored junk beside a module, or if the walk silently skips a
    # workspace package it does not key. Edits a copied, committed tree.
    tree = tmp_path / "tree"
    ignore = shutil.ignore_patterns("__pycache__", "target")
    for path in ("reg_meta_build/src", "crates/reg-core", "crates/reg-core-py"):
        shutil.copytree(REPO / path, tree / path, ignore=ignore)
    for path in ("uv.lock", "Cargo.toml", "Cargo.lock", ".gitignore"):
        shutil.copy2(REPO / path, tree / path)
    for name in ("real_seed_cache.py", "keyed_cache.py", "gate.py"):
        (tree / "scripts").mkdir(exist_ok=True)
        shutil.copy2(SCRIPTS / name, tree / "scripts" / name)
    (tree / "pyproject.toml").write_text(
        '[tool.uv.workspace]\nmembers = ["reg_meta_build", "reg_extra"]\n'
    )
    (tree / "reg_extra/src/reg_extra").mkdir(parents=True)
    (tree / "reg_extra/src/reg_extra/__init__.py").touch()
    subprocess.run(["git", "init", "-q", str(tree)], check=True)
    subprocess.run(["git", "-C", str(tree), "add", "-A"], check=True)
    bundle = tmp_path / "repo/bundle"
    bundle.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(tmp_path / "repo")], check=True)

    def key() -> str:
        code, out = keyed()
        assert code == 0, out
        return out["key"]

    def keyed() -> tuple[int, dict]:
        return _tool(
            tmp_path,
            "prepare",
            "--input-bundle",
            str(bundle),
            "--input-commit",
            "c" * 40,
            "--input-manifest-sha256",
            "d" * 64,
            "--key",
            tool=tree / "scripts/real_seed_cache.py",
        )

    package = tree / "reg_meta_build/src/reg_meta_build"
    before = key()
    (package / "sources/_untracked.py").write_text("X = 1\n")
    with (package / "sources/sos.py").open("a") as source:
        source.write("\nfrom reg_meta_build.sources import _untracked\n")
    adapter_edited = key()
    (package / "sources/_untracked.py").write_text("X = 2\n")
    untracked_edited = key()
    with (package / "pipeline.py").open("a") as source:
        source.write("\n# edited\n")
    (package / ".DS_Store").write_bytes(b"junk")
    assert (
        adapter_edited != before,
        untracked_edited != adapter_edited,
        key() == untracked_edited,
    ) == (True, True, True)

    with (package / "sources/sos.py").open("a") as source:
        source.write("\nimport reg_extra\n")
    code, refused = keyed()
    assert (code, "reg_extra" in refused["stderr"]) == (1, True)
