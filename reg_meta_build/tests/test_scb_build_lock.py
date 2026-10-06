"""Converter provenance and build locks through the prototype CLI in a builder copy."""

from __future__ import annotations

import json
import re
import shutil
import sqlite3
from typing import TYPE_CHECKING

from _csv_fixtures import sparsify_scb_values, write_scb_input, write_scb_snapshot
from _prepared_fixtures import accept_prepared
from _snapshot_fixtures import (
    copied_builder_checkout,
    git,
    run_prototype,
    scb_inventory,
)
from reg_meta_build.input_snapshot import load_manifest, prepare_snapshot

if TYPE_CHECKING:
    import subprocess
    from pathlib import Path


def _write_catalog_db(
    path: Path,
    *,
    import_date: str,
    input_dir: str,
    source_checksums: str = "same normalized inputs",
    fact: str = "same catalog fact",
) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            "CREATE TABLE import_manifest (key TEXT PRIMARY KEY, value TEXT NOT NULL);"
            "CREATE TABLE catalog_fact (id INTEGER PRIMARY KEY, value TEXT NOT NULL);"
        )
        conn.executemany(
            "INSERT INTO import_manifest (key, value) VALUES (?, ?)",
            (
                ("import_date", import_date),
                ("input_dir", input_dir),
                ("source_checksums", source_checksums),
            ),
        )
        conn.execute("INSERT INTO catalog_fact VALUES (1, ?)", (fact,))
        conn.commit()
    finally:
        conn.close()


def _update_db(path: Path, sql: str, value: str) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(sql, (value,))
        conn.commit()
    finally:
        conn.close()


def _aux_args(auxiliary: Path) -> tuple[str, ...]:
    return (
        "--aux",
        f"Tabelldefinitioner.sql={auxiliary}",
        "--aux",
        "ID-kolumner.xlsx=-",
    )


def _assert_rejected(result: subprocess.CompletedProcess[str], message: str) -> None:
    assert result.returncode == 2, result.stdout + result.stderr
    assert re.search(message, result.stderr), result.stderr


def test_converter_commit_requires_same_clean_tracked_checkout(
    tmp_path: Path,
) -> None:
    checkout = copied_builder_checkout(tmp_path)
    script = checkout / "scripts/prototype_scb_inputs.py"
    inventory = scb_inventory(
        tmp_path / "inventory", write_scb_input(tmp_path / "source")
    )

    clean = run_prototype(checkout, "prepare", inventory, tmp_path / "snapshot-a")
    assert clean.returncode == 0, clean.stderr
    manifest = json.loads(
        (tmp_path / "snapshot-a/manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["converter_commit"] == git(checkout, "rev-parse", "HEAD")

    converter = checkout / "reg_meta_build/src/reg_meta_build/input_snapshot.py"
    original = converter.read_bytes()
    converter.write_bytes(original + b"\n")
    _assert_rejected(
        run_prototype(checkout, "prepare", inventory, tmp_path / "snapshot-b"),
        "must be clean",
    )
    converter.write_bytes(original)

    ignored = checkout / "ignored/prototype.py"
    ignored.parent.mkdir()
    shutil.copyfile(script, ignored)
    _assert_rejected(
        run_prototype(
            checkout, "prepare", inventory, tmp_path / "snapshot-c", script=ignored
        ),
        "not a tracked HEAD blob",
    )

    unrelated = tmp_path / "unrelated-repo" / "prototype.py"
    unrelated.parent.mkdir()
    shutil.copyfile(script, unrelated)
    accept_prepared(unrelated)
    _assert_rejected(
        run_prototype(
            checkout, "prepare", inventory, tmp_path / "snapshot-d", script=unrelated
        ),
        "same Git checkout",
    )
    assert not any((tmp_path / f"snapshot-{name}").exists() for name in "bcd")


def test_build_lock_refuses_sparse_values_before_streaming_committed_blobs(
    tmp_path: Path,
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    selection = write_scb_snapshot(tmp_path / "accepted", source_dir)
    checkout = copied_builder_checkout(tmp_path)
    recorded_db = tmp_path / "recorded.db"
    _write_catalog_db(
        recorded_db,
        import_date="2026-09-15T10:00:00Z",
        input_dir="/tmp/complete",
    )
    lock_path = tmp_path / "build-lock.json"
    pin = (
        "pin-build",
        selection.path,
        recorded_db,
        "--providers",
        "scb",
        "--option",
        "validate=true",
    )
    first = run_prototype(checkout, *pin[:3], lock_path, *pin[3:])
    assert first.returncode == 0, first.stderr
    assert json.loads(first.stdout)["builder_commit"] == git(
        checkout, "rev-parse", "HEAD"
    )
    sparsify_scb_values(selection)

    # GIT_TRACE shows the refusal precedes any committed blob stream.
    trace = tmp_path / "git-trace.log"
    second = run_prototype(
        checkout,
        *pin[:3],
        tmp_path / "second-lock.json",
        *pin[3:],
        env={"GIT_TRACE": str(trace)},
    )
    _assert_rejected(second, "sparse-checkout add --stdin")
    assert "cat-file --batch" not in trace.read_text(encoding="utf-8")
    verify = run_prototype(
        checkout,
        "verify-lock",
        lock_path,
        selection.path,
        env={"GIT_TRACE": str(trace)},
    )
    _assert_rejected(verify, "sparse-checkout add --stdin")
    assert "cat-file --batch" not in trace.read_text(encoding="utf-8")
    assert not (tmp_path / "second-lock.json").exists()


def test_build_lock_pins_clean_repositories_auxiliary_inputs_and_result(
    tmp_path: Path,
) -> None:
    source_dir = write_scb_input(tmp_path / "source")
    input_repo = tmp_path / "input-repo"
    input_repo.mkdir()
    snapshot = input_repo / "snapshots" / "fixture"
    prepare_snapshot(
        scb_inventory(tmp_path, source_dir), snapshot, converter_commit="e" * 40
    )
    git(input_repo, "init", "-q")
    git(input_repo, "config", "user.email", "test@example.invalid")
    git(input_repo, "config", "user.name", "Test")
    git(input_repo, "add", ".")
    git(input_repo, "commit", "-q", "-m", "snapshot")

    builder_repo = copied_builder_checkout(tmp_path)

    auxiliary = tmp_path / "Tabelldefinitioner.sql"
    auxiliary.write_text("CREATE TABLE x (id int);\n", encoding="utf-8")
    aux = _aux_args(auxiliary)
    recorded_db = tmp_path / "recorded.db"
    replay_db = tmp_path / "replay.db"
    _write_catalog_db(
        recorded_db,
        import_date="2026-09-14T10:00:00Z",
        input_dir="/tmp/first-reconstruction",
    )
    _write_catalog_db(
        replay_db,
        import_date="2026-09-14T11:00:00Z",
        input_dir="/tmp/second-reconstruction",
    )
    lock_path = tmp_path / "build-lock.json"
    created = run_prototype(
        builder_repo,
        "pin-build",
        snapshot,
        recorded_db,
        lock_path,
        "--providers",
        "scb",
        "--option",
        "validate=true",
        "--option",
        "prestage=false",
        *aux,
    )
    assert created.returncode == 0, created.stderr
    lock = json.loads(created.stdout)
    assert lock["snapshot_path"] == "snapshots/fixture"
    assert (
        load_manifest(snapshot).converter_version == lock["snapshot_converter_version"]
    )

    def verify_lock(lock: Path, *extra: object) -> subprocess.CompletedProcess[str]:
        return run_prototype(builder_repo, "verify-lock", lock, snapshot, *aux, *extra)

    verified = verify_lock(lock_path)
    assert verified.returncode == 0, verified.stderr
    replayed = verify_lock(
        lock_path, "--recorded-db", recorded_db, "--replay-db", replay_db
    )
    assert replayed.returncode == 0, replayed.stderr
    _assert_rejected(
        verify_lock(lock_path, "--replay-db", replay_db),
        "requires the recorded result DB",
    )

    _update_db(
        replay_db,
        "UPDATE import_manifest SET value = ? WHERE key = 'source_checksums'",
        "changed normalized inputs",
    )
    _assert_rejected(
        verify_lock(lock_path, "--recorded-db", recorded_db, "--replay-db", replay_db),
        "replay result DB differs",
    )

    recorded_bytes = recorded_db.read_bytes()
    recorded_db.write_bytes(b"tampered recorded artifact")
    _assert_rejected(
        verify_lock(lock_path, "--recorded-db", recorded_db),
        "recorded result DB hash",
    )
    recorded_db.write_bytes(recorded_bytes)
    _update_db(
        replay_db,
        "UPDATE import_manifest SET value = ? WHERE key = 'source_checksums'",
        "same normalized inputs",
    )
    _update_db(
        replay_db,
        "UPDATE catalog_fact SET value = ? WHERE id = 1",
        "changed catalog fact",
    )
    _assert_rejected(
        verify_lock(lock_path, "--recorded-db", recorded_db, "--replay-db", replay_db),
        "replay result DB differs",
    )

    for field, value, message in (
        ("snapshot_schema_version", 999, "snapshot schema version"),
        ("snapshot_converter_version", -999, "snapshot converter version"),
    ):
        invalid_lock = json.loads(lock_path.read_text(encoding="utf-8"))
        invalid_lock[field] = value
        invalid_lock_path = tmp_path / f"invalid-{field}.json"
        invalid_lock_path.write_text(json.dumps(invalid_lock), encoding="utf-8")
        _assert_rejected(verify_lock(invalid_lock_path), f"{message} pin mismatch")

    auxiliary.write_text("changed\n", encoding="utf-8")
    _assert_rejected(verify_lock(lock_path), "hash/size mismatch")
    auxiliary.write_text("CREATE TABLE x (id int);\n", encoding="utf-8")

    (input_repo / ".gitignore").write_text(
        "/snapshots/fixture/files/\n", encoding="utf-8"
    )
    git(input_repo, "rm", "-q", "-r", "--cached", "snapshots/fixture/files")
    git(input_repo, "add", ".gitignore")
    git(input_repo, "commit", "-q", "-m", "omit normalized snapshot files")
    unbacked_commit = git(input_repo, "rev-parse", "HEAD")

    unbacked_lock_path = tmp_path / "unbacked-lock.json"
    _assert_rejected(
        run_prototype(
            builder_repo,
            "pin-build",
            snapshot,
            recorded_db,
            unbacked_lock_path,
            "--providers",
            "scb",
            "--option",
            "validate=true",
            "--option",
            "prestage=false",
            *aux,
        ),
        "missing from pinned commit",
    )
    assert not unbacked_lock_path.exists()

    forged_lock = json.loads(lock_path.read_text(encoding="utf-8"))
    forged_lock["input_repository_commit"] = unbacked_commit
    forged_lock_path = tmp_path / "forged-lock.json"
    forged_lock_path.write_text(json.dumps(forged_lock), encoding="utf-8")
    _assert_rejected(verify_lock(forged_lock_path), "missing from pinned commit")

    (builder_repo / ".gitignore").write_text(
        "/reg_meta_build/src/reg_meta_build/cli.py\n", encoding="utf-8"
    )
    git(
        builder_repo, "rm", "-q", "--cached", "reg_meta_build/src/reg_meta_build/cli.py"
    )
    git(builder_repo, "add", ".gitignore")
    git(builder_repo, "commit", "-q", "-m", "omit builder entry point")
    _assert_rejected(
        verify_lock(lock_path), "builder source is not a tracked HEAD blob"
    )
