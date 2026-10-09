"""The G1 cache's candidate copies: one directory per derive source tree.

G1 itself proves a warm hit end to end; this guards what a single run cannot see,
two checkouts with different builders preparing the shared cache at once.
"""

from __future__ import annotations

import shutil
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor

from reader_artifacts import FIXTURE_IMPORT_DATE, cached_reader_artifact

from conformance.differential import cache
from conformance.http_cases import CASES

CATALOGS = ("global", "swecov")
# The derive and docs-index subprocesses' stand-in: derive copies its base and
# holds long enough that both preparations are in flight together; the docs index
# step leaves its copy as is. The real steps refuse an uncommitted builder tree.
INTERPRETER = """\
import shutil, sys, time
args = sys.argv[1:]
if "derive" in args:
    time.sleep(1)
    shutil.copyfile(args[args.index("--base") + 1], args[args.index("--out") + 1])
    print("{}")
"""


def _builder_commit(directory) -> str:
    conn = sqlite3.connect(f"file:{directory / cache.DB_FILENAME}?mode=ro", uri=True)
    try:
        (commit,) = conn.execute(
            "SELECT value FROM import_manifest WHERE key = 'builder_commit'"
        ).fetchone()
    finally:
        conn.close()
    return commit


def test_concurrent_preparations_from_two_source_trees_keep_their_own_copies(
    tmp_path, monkeypatch
):
    # Fails if two source trees share a derived path (one re-derives over or
    # deletes the other's copy, or trips on its layout), if either preparation
    # fails or returns a key without each catalog's copy and docs copy, or if a
    # warm key re-derives.
    monkeypatch.setenv("REG_META_G1_CACHE", str(tmp_path / "cache"))
    interpreter = tmp_path / "python"
    interpreter.write_text(f"#!{sys.executable}\n{INTERPRETER}")
    interpreter.chmod(0o755)
    monkeypatch.setattr(sys, "executable", str(interpreter))
    artifact = cached_reader_artifact(
        CASES / "fixtures" / "compiled",
        "steward",
        identity_overrides={"import_date": FIXTURE_IMPORT_DATE},
        docs=CASES / "fixtures" / "docs",
    ).parent
    originals = {}
    for catalog in CATALOGS:
        directory = tmp_path / "originals" / catalog
        directory.mkdir(parents=True)
        shutil.copyfile(artifact / cache.DB_FILENAME, directory / cache.DB_FILENAME)
        originals[catalog] = directory
    shutil.copyfile(
        artifact / cache.DOC_DB_FILENAME,
        originals["global"] / cache.DOC_DB_FILENAME,
    )
    pins = cache.Pins(
        baseline_commit="0" * 40,
        tag="reg_meta/v0.0.0",
        catalogs={c: cache.Asset(f"{c}.db.zst", f"{c}-digest") for c in CATALOGS},
        docs=cache.Asset("docs.db.zst", "docs-digest"),
    )

    with ThreadPoolExecutor(2) as pool:
        first, second = pool.map(
            lambda source: cache.ensure_derived(pins, originals, source),
            ("tree-a", "tree-b"),
        )

    assert first["global"].parent != second["global"].parent
    for derived in (first, second):
        assert sorted(derived) == sorted(CATALOGS)
        for directory in derived.values():
            assert _builder_commit(directory) == "0" * 40
            assert (directory / cache.DOC_DB_FILENAME).read_bytes() == (
                artifact / cache.DOC_DB_FILENAME
            ).read_bytes()
    # A second run from the first tree reads its copy instead of re-deriving.
    copy = first["global"] / cache.DB_FILENAME
    inode = copy.stat().st_ino
    assert cache.ensure_derived(pins, originals, "tree-a") == first
    assert copy.stat().st_ino == inode
