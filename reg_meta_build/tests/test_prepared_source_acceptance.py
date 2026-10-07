"""Prepared records open only from a clean Git acceptance; failures are atomic."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
from pathlib import Path

import pytest
from _prepared_fixtures import (
    accept_prepared,
    prepare_records as _prepare,
    prepared_record as _record,
    prepared_revision as _revision,
)
from reg_meta_build.prepared_sources import (
    PreparedSourceError,
    PreparedSourceRecords,
    open_prepared_source_records,
    prepare_source_records,
    prepared_source_paths,
)


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def test_prepare_validates_identity_revision_membership_and_cleans_failure(
    tmp_path: Path,
) -> None:
    revision = _revision("source-a", "a")
    record = _record(revision, row=2, member="First", raw_value="First")
    output = tmp_path / "records"
    with pytest.raises(PreparedSourceError, match="revision absent"):
        prepare_source_records(
            output, records=(record,), revisions=(), scope="missing revision"
        )
    assert not output.exists()
    assert list(tmp_path.iterdir()) == []
    invalid = record.model_copy(update={"record_id": "tampered"})
    with pytest.raises(PreparedSourceError, match="SourceRecord contract"):
        prepare_source_records(
            output, records=(invalid,), revisions=(revision,), scope="invalid identity"
        )
    assert list(tmp_path.iterdir()) == []
    with pytest.raises(PreparedSourceError, match="duplicate source revision"):
        prepare_source_records(
            output,
            records=(record,),
            revisions=(revision, revision),
            scope="duplicate revision",
        )


def test_existing_candidate_is_immutable_and_atomic_failure_leaves_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "existing"
    _prepare(output)
    before = tuple(path.read_bytes() for path in prepared_source_paths(output))
    with pytest.raises(PreparedSourceError, match="already exists"):
        _prepare(output)
    assert tuple(path.read_bytes() for path in prepared_source_paths(output)) == before

    def fail_replace(*_args: object, **_kwargs: object) -> None:
        raise OSError("injected replace failure")

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(OSError, match="injected replace failure"):
        _prepare(tmp_path / "new")
    assert sorted(path.name for path in tmp_path.iterdir()) == ["existing"]


def test_open_requires_accepted_exact_commit_and_manifest(tmp_path: Path) -> None:
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root)
    with pytest.raises(PreparedSourceError, match="Git|git"):
        open_prepared_source_records(
            root, expected_sha256=manifest.sha256, input_commit="a" * 40
        )
    commit = accept_prepared(root)
    with pytest.raises(PreparedSourceError, match="commit pin mismatch"):
        open_prepared_source_records(
            root, expected_sha256=manifest.sha256, input_commit="a" * 40
        )
    with pytest.raises(PreparedSourceError, match="hash mismatch"):
        open_prepared_source_records(
            root, expected_sha256="0" * 64, input_commit=commit
        )
    path = root / "manifest.json"
    document = json.loads(path.read_bytes())
    document["schema_version"] = 14
    payload = json.dumps(document).encode()
    path.write_bytes(payload)
    commit = accept_prepared(root)
    with pytest.raises(PreparedSourceError, match="schema_version"):
        open_prepared_source_records(
            root,
            expected_sha256=hashlib.sha256(payload).hexdigest(),
            input_commit=commit,
        )


@pytest.mark.parametrize(
    "change", ["dirty", "missing", "untracked", "assume-unchanged", "skip-worktree"]
)
def test_accepted_worktree_changes_and_hidden_index_flags_fail_closed(
    tmp_path: Path, change: str
) -> None:
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root)
    commit = accept_prepared(root)
    database = root / "files" / "records.sqlite"
    if change == "dirty":
        with database.open("ab") as handle:
            handle.write(b"changed")
    elif change == "missing":
        database.unlink()
    elif change == "untracked":
        (root / "files" / "extra.sqlite").write_bytes(b"unexpected")
    else:
        _git(root.parent, "update-index", f"--{change}", "records/files/records.sqlite")
    with pytest.raises(PreparedSourceError, match="clean|materialized"):
        open_prepared_source_records(
            root, expected_sha256=manifest.sha256, input_commit=commit
        )


def test_wrong_kind_payload_fails_at_decode_boundary(tmp_path: Path) -> None:
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root)
    with sqlite3.connect(root / "files" / "records.sqlite") as conn:
        conn.execute("UPDATE occurrence SET fields_payload = scope_payload")
    # Exercise the decoder directly: the public opener rejects any committed
    # database change that lacks a matching preparation proof.
    opened = PreparedSourceRecords(root=root, manifest=manifest, input_commit="a" * 40)
    with pytest.raises(PreparedSourceError, match="wrong-kind"):
        tuple(opened.records)


def test_committed_same_kind_payload_edit_invalidates_preparation_proof(
    tmp_path: Path,
) -> None:
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root)
    accept_prepared(root)
    database = root / "files" / "records.sqlite"
    with sqlite3.connect(database) as conn:
        key, body = conn.execute(
            "SELECT id, body FROM payload WHERE kind='field'"
        ).fetchone()
        changed = json.loads(body)
        changed["value"] = "Other"
        conn.execute("UPDATE payload SET body=? WHERE id=?", (json.dumps(changed), key))
    assert database.stat().st_size == manifest.database_size
    commit = accept_prepared(root)
    with pytest.raises(PreparedSourceError, match="preparation proof"):
        open_prepared_source_records(
            root, expected_sha256=manifest.sha256, input_commit=commit
        )


@pytest.mark.parametrize("accepted", [False, True])
def test_extra_payload_is_rejected_even_when_ignored_or_committed(
    tmp_path: Path, accepted: bool
) -> None:
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root)
    commit = accept_prepared(root)
    (root / "files" / "extra.sqlite").write_bytes(b"unexpected")
    if accepted:
        commit = accept_prepared(root)
    else:
        (root.parent / ".git" / "info" / "exclude").write_text("extra.sqlite\n")
    with pytest.raises(PreparedSourceError, match="inventory"):
        open_prepared_source_records(
            root, expected_sha256=manifest.sha256, input_commit=commit
        )


@pytest.mark.parametrize("member", ["manifest.json", "files/records.sqlite"])
def test_committed_symlink_cannot_replace_a_prepared_member(
    tmp_path: Path, member: str
) -> None:
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root)
    path = root / member
    outside = tmp_path / "original"
    outside.write_bytes(path.read_bytes())
    path.unlink()
    path.symlink_to(outside)
    commit = accept_prepared(root)
    with pytest.raises(PreparedSourceError):
        open_prepared_source_records(
            root, expected_sha256=manifest.sha256, input_commit=commit
        )


@pytest.mark.parametrize("flag", ["assume-unchanged", "skip-worktree"])
def test_manifest_index_flags_cannot_hide_its_worktree_state(
    tmp_path: Path, flag: str
) -> None:
    root = tmp_path / "inputs" / "records"
    manifest = _prepare(root)
    commit = accept_prepared(root)
    _git(root.parent, "update-index", f"--{flag}", "records/manifest.json")
    with pytest.raises(PreparedSourceError, match="materialized"):
        open_prepared_source_records(
            root, expected_sha256=manifest.sha256, input_commit=commit
        )
