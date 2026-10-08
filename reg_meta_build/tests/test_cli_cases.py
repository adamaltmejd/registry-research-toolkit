"""The `cases/cli/` corpus: `reg-meta-build` commands run on a built catalog.

Each case directory is one boundary claim (`cases/cli/README.md`): the argument list
a maintainer types, the files laid into the working directory first, and the
oracle: the exit code, a projection of the stdout JSON envelope, stderr, and the
files the command writes. Every case reads one synthetic artifact built from the
readable source in `cases/cli/_artifact/`, once per test session and read-only.
The command runs in-process through `reg_meta_build.cli.run`, the real CLI entry
point (argv to JSON envelope and exit code). Expected values are read from the
test each case replaces.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from _case_projection import mismatch, unclaimed
from _pipeline_catalog_support import CatalogFixture
from reg_meta_build.cli import run

from reg_meta_build.fqid_slugs import load_slug_dir, snapshot_payload

if TYPE_CHECKING:
    from _build_case_runner import PreparedCache

CASES = Path(__file__).resolve().parent / "cases" / "cli"
ARTIFACT = CASES / "_artifact"


@dataclass(frozen=True)
class Artifact:
    """The built catalog's ``--db`` directory and the curation tree that built it."""

    db_dir: Path
    curation: Path

    def fingerprint(self) -> list[tuple[str, int, int]]:
        return sorted(
            (path.name, path.stat().st_size, path.stat().st_mtime_ns)
            for path in self.db_dir.iterdir()
        )


def _tree_bytes(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


@pytest.fixture(scope="session")
def cli_artifact(prepared_cache: PreparedCache) -> Artifact:
    """The artifact built once from `cases/cli/_artifact/`.

    It sits beside the prepared inputs, keyed by the content hash of its source,
    its curation and this runner (which sets the build options), and is published
    by an atomic rename, so xdist workers share one build and never write a path
    another worker reads.
    """
    spec = json.loads((ARTIFACT / "source.json").read_text(encoding="utf-8"))
    inputs = _tree_bytes(ARTIFACT / "curation")
    inputs["runner"] = Path(__file__).read_bytes()
    key = hashlib.sha256(
        json.dumps(
            [spec, {path: data.hex() for path, data in inputs.items()}],
            sort_keys=True,
        ).encode()
    ).hexdigest()
    root = prepared_cache.root.with_name("cli-artifact")
    root.mkdir(parents=True, exist_ok=True)
    entry = root / key
    if not (entry / "db" / "reg_meta.db").is_file():
        prepared = prepared_cache.get(spec)
        staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=root))
        try:
            shutil.copytree(ARTIFACT / "curation", staging / "curation")
            (staging / "curation" / "classifications").mkdir()
            output = staging / "db" / "reg_meta.db"
            output.parent.mkdir()
            fixture = CatalogFixture(
                prepared.prepared,
                prepared.commit,
                prepared.digest,
                staging / "curation",
            )
            # A diagnostic build: a publishable one stamps the builder commit and
            # so refuses a working tree with uncommitted changes.
            result = fixture.build(output, staging / "report", diagnostic=True)
            if result["status"] != "diagnostic_complete" or result["counts"].get(
                "error"
            ):
                raise RuntimeError(f"the CLI artifact did not build: {result}")
            shutil.rmtree(staging / "report")
            try:
                staging.rename(entry)
            except OSError:
                if not (entry / "db" / "reg_meta.db").is_file():
                    raise
        finally:
            shutil.rmtree(staging, ignore_errors=True)
    return Artifact(entry / "db", entry / "curation")


def case_dirs() -> list[Path]:
    return sorted(
        path.parent
        for path in CASES.glob("*/*/request.json")
        if not path.parent.parent.name.startswith("_")
    )


def _fill(value: Any, places: dict[str, str]) -> Any:
    """``value`` with each ``{name}`` placeholder in its strings replaced."""
    if isinstance(value, str):
        for name, text in places.items():
            value = value.replace("{" + name + "}", text)
        return value
    if isinstance(value, list):
        return [_fill(item, places) for item in value]
    if isinstance(value, dict):
        return {key: _fill(item, places) for key, item in value.items()}
    return value


def _check_file(work: Path, path: str, claim: dict, before: dict[str, bytes]) -> None:
    matches = sorted(work.glob(path))
    if claim.get("absent"):
        assert not matches, (path, matches)
        return
    assert len(matches) == 1, (path, matches)
    (found,) = matches
    text = found.read_text(encoding="utf-8")
    if "toml" in claim:
        assert tomllib.loads(text) == claim["toml"], (path, text)
    if "json" in claim:
        assert json.loads(text) == claim["json"], (path, text)
    if claim.get("unchanged"):
        assert found.read_bytes() == before[path], path
    for snippet in claim.get("contains", ()):
        assert snippet in text, (path, snippet, text)
    for snippet in claim.get("excludes", ()):
        assert snippet not in text, (path, snippet, text)


_REQUEST_KEYS = {"replaces", "fails_if", "note", "argv", "env", "runs", "curation_dirs"}
_STEP_KEYS = {"argv", "env"}
_EXPECTED_KEYS = {
    "exit_code",
    "stdout_contains",
    "stderr",
    "files",
    "same_bytes",
    "reloads_with",
}
_FILE_CLAIMS = {"absent", "toml", "json", "unchanged", "contains", "excludes"}


def _check_keys(case: Path, request: dict, expected: dict) -> None:
    """Refuse a case the runner would read only in part: a misspelled key would
    otherwise drop its claim silently."""
    assert request.get("fails_if", "").strip(), f"{case.name}: fails_if is required"
    assert request.get("replaces"), f"{case.name}: replaces is required"
    assert ("argv" in request) != ("runs" in request), f"{case.name}: argv or runs"
    assert request.keys() <= _REQUEST_KEYS, (case.name, request.keys() - _REQUEST_KEYS)
    for step in request.get("runs", []):
        # A misspelled step key (say "evn") would otherwise be ignored silently.
        assert "argv" in step, f"{case.name}: every run step needs argv"
        assert step.keys() <= _STEP_KEYS, (case.name, step.keys() - _STEP_KEYS)
    assert "exit_code" in expected, f"{case.name}: exit_code is required"
    assert expected.keys() <= _EXPECTED_KEYS, (
        case.name,
        expected.keys() - _EXPECTED_KEYS,
    )
    for path, claim in expected.get("files", {}).items():
        assert claim and claim.keys() <= _FILE_CLAIMS, (case.name, path, claim)
    stderr = expected.get("stderr", {})
    assert stderr.keys() <= {"empty", "contains", "excludes"}, (case.name, stderr)
    if "reloads_with" in expected:
        assert expected["reloads_with"].keys() == {"loader", "path", "slugs"}, case.name


_RELOADERS = {"slug_dir": lambda root: snapshot_payload(load_slug_dir(root))}


@pytest.mark.parametrize(
    "case", case_dirs(), ids=lambda case: f"{case.parent.name}/{case.name}"
)
def test_cli_case(
    case: Path,
    cli_artifact: Artifact,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = json.loads((case / "request.json").read_text(encoding="utf-8"))
    expected = json.loads((case / "expected.json").read_text(encoding="utf-8"))
    _check_keys(case, request, expected)
    runs = request.get("runs", [request])

    work = tmp_path / "work"
    for name in request.get("curation_dirs", ["curation"]):
        shutil.copytree(cli_artifact.curation, work / name)
    if (case / "files").is_dir():
        shutil.copytree(case / "files", work, dirs_exist_ok=True)
    before = _tree_bytes(work)
    places = {"db": str(cli_artifact.db_dir), "work": str(work)}
    fingerprint = cli_artifact.fingerprint()

    monkeypatch.delenv("REG_META_QUIET", raising=False)
    for step in runs:
        argv = _fill(step["argv"], places)
        assert case.parent.name in argv, f"{case.name}: argv names another command"
        with monkeypatch.context() as env:
            for name, value in step.get("env", {}).items():
                env.setenv(name, value)
            capsys.readouterr()
            code = run(argv)
            captured = capsys.readouterr()
        assert code == expected["exit_code"], (argv, captured.out, captured.err)

    assert cli_artifact.fingerprint() == fingerprint, "a case wrote the shared artifact"
    stdout = case / "stdout.json"
    if stdout.is_file():
        claim = _fill(json.loads(stdout.read_text(encoding="utf-8")), places)
        assert unclaimed(claim, "$stdout", exact=False) is None
        departure = mismatch(json.loads(captured.out), claim, "$stdout", exact=False)
        assert departure is None, departure
    for snippet in _fill(expected.get("stdout_contains", []), places):
        assert snippet in captured.out, (snippet, captured.out)
    stderr = expected.get("stderr", {})
    if stderr.get("empty"):
        assert captured.err == "", captured.err
    for snippet in stderr.get("contains", ()):
        assert snippet in captured.err, (snippet, captured.err)
    for snippet in stderr.get("excludes", ()):
        assert snippet not in captured.err, (snippet, captured.err)
    for path, claim in expected.get("files", {}).items():
        _check_file(work, path, claim, before)
    for first, second in expected.get("same_bytes", ()):
        assert (work / first).read_bytes() == (work / second).read_bytes(), (
            first,
            second,
        )
    if "reloads_with" in expected:
        reload = expected["reloads_with"]
        loaded = _RELOADERS[reload["loader"]](work / reload["path"])
        assert loaded == reload["slugs"], loaded
