"""Fixture input repositories: Git repositories that commit the same on every
machine, and the SCB snapshots and catalog bundles committed in them, repinned,
sparsified and hydrated as a build reads them."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from pathlib import Path

from reg_meta_build.input_snapshot import (
    SCB_CSV_FILES,
    CatalogBundleSelection,
    LisaWorkbookSelection,
    ScbSnapshotSelection,
    prepare_input_bundle,
    prepare_snapshot,
)


def _git(repo: Path, *args: str) -> None:
    # An absolute executable and `close_fds=False` let CPython posix_spawn git
    # instead of forking the test process, as `input_snapshot._git_bytes` does.
    subprocess.run(
        [shutil.which("git") or "git", "-C", str(repo), *args],
        check=True,
        close_fds=False,
    )


def init_fixture_repo(repo: Path, *, name: str, email: str) -> None:
    """Create a fixture Git repository that commits the same on every machine.

    It sets its own identity and line endings, and turns off commit signing: a
    developer's global `commit.gpgsign` otherwise signs every fixture commit with
    their key, one signing process per commit. The config is appended to
    `.git/config` directly, the file `git config` would write, because a `git
    config` process per key was a measurable share of the suite's CPU.
    """
    _git(repo, "init", "-q", "-b", "main")
    with (repo / ".git" / "config").open("a", encoding="utf-8") as config:
        config.write(
            f"[user]\n\tname = {name}\n\temail = {email}\n"
            "[core]\n\tautocrlf = false\n[commit]\n\tgpgsign = false\n"
        )


def commit_fixture(repo: Path, message: str, *paths: str) -> str:
    """Stage ``paths`` (default: everything), commit, and return the new commit."""
    _git(repo, "add", *(("--", *paths) if paths else ("-A",)))
    _git(repo, "commit", "-q", "-m", message)
    # The commit just wrote HEAD's loose ref; reading it skips a `git rev-parse`.
    # A reftable repository (`init.defaultRefFormat`) has no loose ref to read.
    head = (repo / ".git" / "HEAD").read_text(encoding="utf-8").strip()
    if not head.startswith("ref: "):
        return head
    ref = repo / ".git" / head.removeprefix("ref: ")
    if ref.is_file():
        return ref.read_text(encoding="utf-8").strip()
    return subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def write_scb_snapshot(root: Path, scb_dir: Path) -> ScbSnapshotSelection:
    """Commit a normalized snapshot of ``scb_dir`` in a synthetic input repo."""
    root.mkdir(parents=True, exist_ok=True)
    archive = root / "retained-delivery.zip"
    archive.write_bytes(b"independently retained archive fixture")
    present = {path.name for path in scb_dir.glob("*.csv")}
    inventory = root / "inventory.json"
    inventory.write_text(
        json.dumps(
            {
                "bundle_id": "scb-mikrometadata",
                "edition": "fixture",
                "source_dir": str(scb_dir),
                "files": [
                    {
                        "name": name,
                        "required": name == "Registerinformation.csv",
                    }
                    for name in SCB_CSV_FILES
                ],
                "archives": [
                    {
                        "path": str(archive),
                        "locator": "offline/scb-fixture.zip",
                        "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                        "members": sorted(present),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    repo = root / "input-repo"
    snapshot = repo / "snapshot"
    prepare_snapshot(inventory, snapshot, converter_commit="f" * 40)
    init_fixture_repo(repo, name="Test", email="test@example.invalid")
    return ScbSnapshotSelection(
        path=snapshot,
        input_commit=commit_fixture(repo, "snapshot"),
        manifest_sha256=hashlib.sha256(
            (snapshot / "manifest.json").read_bytes()
        ).hexdigest(),
    )


def repin_scb_snapshot(
    selection: ScbSnapshotSelection, message: str = "fixture update"
) -> ScbSnapshotSelection:
    """Commit fixture mutations and return their updated snapshot selection."""
    commit = commit_fixture(selection.path.parent, message)
    return ScbSnapshotSelection(
        path=selection.path,
        input_commit=commit,
        manifest_sha256=hashlib.sha256(
            (selection.path / "manifest.json").read_bytes()
        ).hexdigest(),
    )


def write_input_bundle(
    root: Path,
    input_dir: Path,
    *,
    lisa_workbook: LisaWorkbookSelection | None = None,
) -> CatalogBundleSelection:
    """Commit a partial synthetic bundle for explicit skip-curation test builds."""
    snapshot = write_scb_snapshot(root, input_dir / "SCB")
    return write_input_bundle_from_snapshot(
        input_dir, snapshot, lisa_workbook=lisa_workbook
    )


def write_input_bundle_from_snapshot(
    input_dir: Path,
    snapshot: ScbSnapshotSelection,
    *,
    lisa_workbook: LisaWorkbookSelection | None = None,
) -> CatalogBundleSelection:
    """Capture and commit a bundle referencing an existing fixture snapshot."""
    repo = snapshot.path.parent
    bundle = repo / "bundle"
    prepare_input_bundle(input_dir, snapshot, bundle, lisa_workbook=lisa_workbook)
    commit = commit_fixture(repo, "catalog bundle")
    return CatalogBundleSelection(
        path=bundle,
        input_commit=commit,
        manifest_sha256=hashlib.sha256(
            (bundle / "catalog-bundle.json").read_bytes()
        ).hexdigest(),
    )


def repin_input_bundle(
    selection: CatalogBundleSelection, message: str = "fixture bundle update"
) -> CatalogBundleSelection:
    """Commit fixture mutations and return updated bundle commit identity."""
    commit = commit_fixture(selection.path.parent, message)
    return CatalogBundleSelection(
        path=selection.path,
        input_commit=commit,
        manifest_sha256=hashlib.sha256(
            (selection.path / "catalog-bundle.json").read_bytes()
        ).hexdigest(),
    )


def _selected_snapshot_root(
    selection: CatalogBundleSelection | ScbSnapshotSelection,
) -> Path:
    if isinstance(selection, ScbSnapshotSelection):
        return selection.path
    manifest = json.loads(
        (selection.path / "catalog-bundle.json").read_text(encoding="utf-8")
    )
    return selection.path.parent / manifest["scb_snapshot_path"]


def scb_values_role(
    selection: CatalogBundleSelection | ScbSnapshotSelection,
) -> str:
    """Return the repository-relative cone directory for prepared SCB values."""
    snapshot = _selected_snapshot_root(selection)
    return (
        (snapshot / "files" / "Vardemangder.csv")
        .relative_to(selection.path.parent)
        .as_posix()
    )


def sparsify_scb_values(
    selection: CatalogBundleSelection | ScbSnapshotSelection,
) -> tuple[str, ...]:
    """Apply the supported fixture cone layout and return its saved directory list."""
    repo = selection.path.parent
    cold = Path(scb_values_role(selection))
    tracked = subprocess.run(
        ["git", "-C", str(repo), "ls-tree", "-r", "--name-only", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    included: set[str] = set()
    for raw_path in tracked:
        path = Path(raw_path)
        if path == cold or cold in path.parents:
            continue
        parent = path.parent
        if parent == Path() or parent in cold.parents:
            continue
        included.add(parent.as_posix())
    directories = tuple(sorted(included))
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "sparse-checkout",
            "set",
            "--cone",
            "--no-sparse-index",
            "--stdin",
        ],
        input="".join(f"{path}\n" for path in directories),
        check=True,
        text=True,
    )
    return directories


def hydrate_scb_values(
    selection: CatalogBundleSelection | ScbSnapshotSelection,
) -> None:
    """Materialize only the prepared SCB value role in a fixture checkout."""
    subprocess.run(
        [
            "git",
            "-C",
            str(selection.path.parent),
            "sparse-checkout",
            "add",
            "--stdin",
        ],
        input=f"{scb_values_role(selection)}\n",
        check=True,
        text=True,
    )


def omit_scb_snapshot_file(
    selection: ScbSnapshotSelection, name: str
) -> ScbSnapshotSelection:
    """Remove one optional normalized file and commit the fixture mutation."""
    manifest_path = selection.path / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    item = next(item for item in manifest["files"] if item["name"] == name)
    assert not item["required"]
    for normalized in item["records"]:
        (selection.path / normalized["path"]).unlink()
    for group in item["groups"]:
        (selection.path / group["dictionary"]["path"]).unlink()
    item.clear()
    item.update({"name": name, "required": False, "present": False})
    for archive in manifest["archives"]:
        archive["members"] = [member for member in archive["members"] if member != name]
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return repin_scb_snapshot(selection, f"omit {name}")
