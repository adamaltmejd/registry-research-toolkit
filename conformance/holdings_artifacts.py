"""Build paired synthetic holdings through the public compiler CLI.

The private steward repository is never used by this harness. Fixture pins are
computed from the supplied readable source bytes, then committed in a temporary
repository, exactly as the real compiler's input contract requires.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import subprocess
from contextlib import closing
from typing import TYPE_CHECKING

from reader_artifacts import CASES, REG_BUILD, REPO, cached_reader_artifact

if TYPE_CHECKING:
    from pathlib import Path

HOLDINGS_CASE = CASES / "holdings"


def run(command, *, cwd=REPO):
    return subprocess.run(
        command,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def sha(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def pin_steward(root: Path, registry: Path) -> None:
    """Pin an authored fixture, including deliberate refusal-case mutations."""
    roles = [
        "scope.toml",
        "series",
        "curation",
        "inputs/slots",
        "branding/steward.json",
        "branding/logo.svg",
        "census.csv",
        "storage.csv",
        "holdings.toml",
        "curation/readers/holdings.toml",
    ]
    if (root / "reference-occurrences.toml").exists():
        roles.extend(["reference-occurrences.toml", "inputs/reference"])
    hashes = {}
    for role in roles:
        path = root / role
        files = (
            sorted(p for p in path.rglob("*") if p.is_file())
            if path.is_dir()
            else [path]
        )
        if len(files) == 1:
            hashes[role] = sha(files[0])
        else:
            content = "".join(
                f"{p.relative_to(path).as_posix()}\t{sha(p)}\n" for p in files
            )
            hashes[role] = hashlib.sha256(content.encode()).hexdigest()
    config = """owner = "fixture"
scope = "scope.toml"
series_root = "series"
curation_root = "curation/registers"

[presentation]
steward = "branding/steward.json"
logo = "branding/logo.svg"

[inputs]
census = "census.csv"
storage_export = "storage.csv"
holdings = "holdings.toml"
census_reader = "curation/readers/holdings.toml"

[pins]
"""
    if (root / "reference-occurrences.toml").exists():
        config = config.replace(
            "[inputs]", '[inputs]\nreference_occurrences = "reference-occurrences.toml"'
        )
    config += "".join(f"{json.dumps(k)} = {json.dumps(v)}\n" for k, v in hashes.items())
    (root / "steward.toml").write_text(config)
    for command in (
        ["git", "init", "--quiet"],
        ["git", "config", "user.name", "Synthetic fixture"],
        ["git", "config", "user.email", "fixture@example.invalid"],
        ["git", "config", "commit.gpgsign", "false"],
        ["git", "add", "."],
        ["git", "commit", "--quiet", "--allow-empty", "-m", "Pin synthetic inputs"],
    ):
        result = run(command, cwd=root)
        if result.returncode:
            raise RuntimeError(result.stderr)
    commit = run(["git", "rev-parse", "HEAD"], cwd=root).stdout.strip()
    remote = run(["git", "remote", "get-url", "origin"], cwd=root)
    if remote.returncode:
        result = run(["git", "remote", "add", "origin", "fixture"], cwd=root)
        if result.returncode:
            raise RuntimeError(result.stderr)
    registry.write_text(
        f'[fixture]\nrepo = "fixture"\ncommit = "{commit}"\nconfig = "steward.toml"\n'
    )


def prepare_holdings(directory: Path) -> tuple[Path, Path, list]:
    """Copy small synthetic inputs; the base artifact remains read-only and shared."""
    root = directory / "steward"
    shutil.copytree(HOLDINGS_CASE / "steward", root)
    registry = directory / "stewards.toml"
    base = cached_reader_artifact(HOLDINGS_CASE / "base")
    locked = run(
        [
            REG_BUILD,
            "lock",
            "--base",
            base,
            "--inputs",
            root,
            "--curation",
            root / "curation",
        ]
    )
    if locked.returncode:
        raise RuntimeError(locked.stderr)
    pin_steward(root, registry)
    command = [
        REG_BUILD,
        "holdings",
        "--base",
        base,
        "--steward",
        "fixture",
        "--steward-repo",
        root,
        "--registry",
        registry,
    ]
    return base, root, command


def verify_refusals(tmp_path, output, base, root, variant="base"):
    for case in json.loads((HOLDINGS_CASE / "verify-refusals.json").read_text()):
        if case.get("variant", "base") != variant:
            continue
        damaged = tmp_path / "damaged.db"
        shutil.copyfile(output, damaged)
        with closing(sqlite3.connect(damaged)) as db:
            db.executescript(case["sql"])
        refused = run(
            [REG_BUILD, "verify", damaged, "--catalog", base, "--steward-repo", root]
        )
        assert refused.returncode == 1
        assert case["code"] in refused.stderr, refused.stderr
        assert case["location"] in refused.stderr, refused.stderr
