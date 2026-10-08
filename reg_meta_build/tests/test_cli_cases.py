"""The `cases/cli/` corpus: `reg-meta-build` commands run on a built catalog.

Each case directory is one boundary claim (`cases/cli/README.md`): the argument list
a maintainer types, the files laid into the working directory first, and the
oracle: the exit code, a projection of the stdout JSON envelope, stderr, and the
files the command writes. Every case reads a synthetic artifact built from the
readable source in `cases/cli/_artifact/` (or in another artifact directory beside
it, or with the case's curation files laid over it), once per test session and
read-only.
The command runs in-process through its real entry point: `reg_meta_build.cli.run`
(argv to JSON envelope and exit code) for a `reg-meta-build` subcommand, or the
program's own `main` for a command directory in `_PROGRAMS`. A command that compares
databases of its own builds them from the case's SQL files (`databases`). Expected
values are read from the test each case replaces.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from _build_case_runner import fixture_generation
from _case_projection import mismatch, unclaimed
from _pipeline_catalog_support import CatalogFixture
from reg_meta_build.cli import run
from reg_meta_build.concept_groups import load_worklist_concept_groups
from reg_meta_build.curation_tree import ClassificationBinding
from reg_meta_build.dbdiff import main as dbdiff_main
from reg_meta_build.doc_db import build_doc_db
from reg_meta_build.relations import load_relations

from reg_meta_build.fqid_slugs import load_slug_dir, snapshot_payload

if TYPE_CHECKING:
    from collections.abc import Callable

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


class Artifacts:
    """The artifacts the cases read, each built once from `cases/cli/_artifact/` or
    from the artifact directory beside it that a case names in `artifact`.

    A case that names `artifact_curation` reads its own artifact, built from that
    directory's curation tree with the case's files laid over it; every other case
    reads its directory's artifact as is. Each lives in the shared fixture cache's
    generation beside the prepared inputs (`fixture_generation`), so it is reused
    across sessions, worktrees and xdist workers and dropped with the generation
    when a builder source changes. Within it, an entry is keyed by the content hash
    of the source, the curation it is built from, its docs and this runner (which
    sets the build options), and published by an atomic rename, so workers share
    one build and never write a path another worker reads.
    """

    def __init__(self, prepared_cache: PreparedCache) -> None:
        self._prepared_cache = prepared_cache
        self._built: dict[tuple[Path, Path | None], Artifact] = {}

    def get(self, base: Path, overlay: Path | None) -> Artifact:
        if (base, overlay) not in self._built:
            self._built[base, overlay] = self._build(base, overlay)
        return self._built[base, overlay]

    def _build(self, base: Path, overlay: Path | None) -> Artifact:
        spec = json.loads((base / "source.json").read_text(encoding="utf-8"))
        curation = _tree_bytes(base / "curation")
        if overlay is not None:
            curation.update(_tree_bytes(overlay))
        docs = base / "docs"
        key = hashlib.sha256(
            json.dumps(
                [
                    spec,
                    {path: data.hex() for path, data in curation.items()},
                    {
                        path: data.hex()
                        for path, data in (
                            _tree_bytes(docs) if docs.is_dir() else {}
                        ).items()
                    },
                    Path(__file__).read_bytes().hex(),
                ],
                sort_keys=True,
            ).encode()
        ).hexdigest()
        root = fixture_generation() / "cli-artifact"
        root.mkdir(parents=True, exist_ok=True)
        entry = root / key
        if not (entry / "db" / "reg_meta.db").is_file():
            prepared = self._prepared_cache.get(spec)
            staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=root))
            try:
                # Written from the hashed bytes, so the key names what was built.
                for path, data in curation.items():
                    target = staging / "curation" / path
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(data)
                (staging / "curation" / "classifications").mkdir(exist_ok=True)
                output = staging / "db" / "reg_meta.db"
                output.parent.mkdir()
                fixture = CatalogFixture(
                    prepared.prepared,
                    prepared.commit,
                    prepared.digest,
                    staging / "curation",
                )
                # A diagnostic build: a publishable one stamps the builder commit
                # and so refuses a working tree with uncommitted changes.
                result = fixture.build(output, staging / "report", diagnostic=True)
                if result["status"] != "diagnostic_complete" or result["counts"].get(
                    "error"
                ):
                    raise RuntimeError(f"the CLI artifact did not build: {result}")
                shutil.rmtree(staging / "report")
                if docs.is_dir():
                    build_doc_db(docs, output.parent)
                try:
                    staging.rename(entry)
                except OSError:
                    if not (entry / "db" / "reg_meta.db").is_file():
                        raise
            finally:
                shutil.rmtree(staging, ignore_errors=True)
        return Artifact(entry / "db", entry / "curation")


@pytest.fixture(scope="session")
def cli_artifacts(prepared_cache: PreparedCache) -> Artifacts:
    return Artifacts(prepared_cache)


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


def _refuse_constant(name: str) -> object:
    raise ValueError(f"stdout is not JSON: it holds {name}")


def _strict_json(text: str) -> Any:
    """``text`` parsed as JSON. Python's parser also accepts ``NaN``, ``Infinity``
    and ``-Infinity``, which are not JSON; they are refused here."""
    return json.loads(text, parse_constant=_refuse_constant)


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
        # A glob claim names its pattern; the snapshot is keyed by the matched file.
        relative = found.relative_to(work).as_posix()
        assert found.read_bytes() == before.get(relative), (path, relative)
    for snippet in claim.get("contains", ()):
        assert snippet in text, (path, snippet, text)
    for snippet in claim.get("excludes", ()):
        assert snippet not in text, (path, snippet, text)


# The entry point each command directory runs, when it is a program of its own
# rather than a `reg-meta-build` subcommand run through `cli.run`.
_PROGRAMS: dict[str, Callable[[list[str]], int]] = {"dbdiff": dbdiff_main}

_REQUEST_KEYS = {
    "replaces",
    "fails_if",
    "note",
    "argv",
    "env",
    "runs",
    "curation_dirs",
    "artifact",
    "artifact_curation",
    "databases",
}
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
    if "runs" in request:
        # Each step carries its own argv and env; one beside `runs` would be
        # ignored silently, so the case would run without it.
        assert not request.keys() & {"argv", "env"}, (
            f"{case.name}: argv and env go inside each run step, not beside runs"
        )
    else:
        assert "argv" in request, f"{case.name}: argv or runs is required"
    assert request.keys() <= _REQUEST_KEYS, (case.name, request.keys() - _REQUEST_KEYS)
    if "artifact" in request:
        # An artifact directory sits beside `_artifact/`, named with a leading
        # underscore so the runner never reads it as a command directory.
        base = CASES / request["artifact"]
        assert request["artifact"].startswith("_") and "/" not in request["artifact"], (
            f"{case.name}: artifact names a directory beside _artifact/"
        )
        assert (base / "source.json").is_file() and (base / "curation").is_dir(), (
            f"{case.name}: artifact {request['artifact']} has no source.json or curation/"
        )
    if "artifact_curation" in request:
        overlay = case / request["artifact_curation"]
        assert overlay.is_dir(), f"{case.name}: artifact_curation names no directory"
    for step in request.get("runs", []):
        # A misspelled step key (say "evn") would otherwise be ignored silently.
        assert "argv" in step, f"{case.name}: every run step needs argv"
        assert step.keys() <= _STEP_KEYS, (case.name, step.keys() - _STEP_KEYS)
    for path, source in request.get("databases", {}).items():
        # A misspelled SQL file name would otherwise fail later as a missing table.
        assert isinstance(source, str), (case.name, path, source)
        assert (case / source).is_file(), f"{case.name}: no SQL file {source}"
    assert "exit_code" in expected, f"{case.name}: exit_code is required"
    assert expected.keys() <= _EXPECTED_KEYS, (
        case.name,
        expected.keys() - _EXPECTED_KEYS,
    )
    for path, claim in expected.get("files", {}).items():
        assert claim and claim.keys() <= _FILE_CLAIMS, (case.name, path, claim)
        # An absent file has nothing else to check; a second claim beside it would
        # be skipped silently.
        assert not claim.get("absent") or claim.keys() == {"absent"}, (case.name, path)
    stderr = expected.get("stderr", {})
    assert stderr.keys() <= {"empty", "contains", "excludes"}, (case.name, stderr)
    if "reloads_with" in expected:
        reload = expected["reloads_with"]
        assert reload.keys() == {"loader", "path", "result"}, case.name
        assert reload["loader"] in _RELOADERS, (case.name, reload["loader"])


def _relations(path: Path) -> dict[str, Any]:
    """The `same_as` and `replaced_by` edges `load_relations` reads, in file order."""
    relations = load_relations(path)
    return {
        "same_as": [
            {"a": edge.a_fqid(), "b": edge.b_fqid(), "note": edge.note}
            for edge in relations.same_as
        ],
        "replaced_by": [
            {
                "from": str(edge.predecessor),
                "to": str(edge.successor),
                "from_column": edge.predecessor_column,
                "to_column": edge.successor_column,
                "variant": edge.variant,
                "effective_year": edge.effective_year,
            }
            for edge in relations.replaced_by
        ],
    }


def _classification_binding(path: Path) -> dict[str, Any]:
    """The `[binding]` table of a worklist, read by the classification curation
    model a `classifications/<short_name>.toml` file's binding loads through."""
    binding = ClassificationBinding.model_validate(
        tomllib.loads(path.read_text(encoding="utf-8")).get("binding", {})
    )
    return {
        "variable": [
            {"variable": bound.variable, "note": bound.note}
            for bound in binding.variable
        ]
    }


def _concept_group_worklist(path: Path) -> dict[str, Any]:
    """The groups `load_worklist_concept_groups` reads, by `provider/register/key`."""
    return {
        f"{group.provider}/{group.register}/{group.key}": {
            "label": group.label,
            "axes": [list(axis) for axis in group.axes],
            "members": [
                {
                    "variable": member.variable,
                    "delivery_column": member.delivery_column,
                    "coords": [list(coord) for coord in member.coords],
                }
                for member in group.members
            ],
        }
        for group in load_worklist_concept_groups(path)
    }


_RELOADERS = {
    "slug_dir": lambda root: snapshot_payload(load_slug_dir(root)),
    "relations": _relations,
    "classification_binding": _classification_binding,
    "concept_group_worklist": _concept_group_worklist,
}


@pytest.mark.parametrize(
    "case", case_dirs(), ids=lambda case: f"{case.parent.name}/{case.name}"
)
def test_cli_case(
    case: Path,
    cli_artifacts: Artifacts,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = json.loads((case / "request.json").read_text(encoding="utf-8"))
    expected = json.loads((case / "expected.json").read_text(encoding="utf-8"))
    _check_keys(case, request, expected)
    runs = request.get("runs", [request])
    overlay = request.get("artifact_curation")
    cli_artifact = cli_artifacts.get(
        CASES / request.get("artifact", ARTIFACT.name),
        None if overlay is None else case / overlay,
    )

    # Resolved, so `{work}` matches the resolved paths a command prints.
    work = tmp_path.resolve() / "work"
    for name in request.get("curation_dirs", ["curation"]):
        shutil.copytree(cli_artifact.curation, work / name)
    if (case / "files").is_dir():
        shutil.copytree(case / "files", work, dirs_exist_ok=True)
    for path, source in request.get("databases", {}).items():
        target = work / path
        assert not target.exists(), f"{case.name}: {path} is already in {{work}}"
        target.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(target)
        try:
            conn.executescript((case / source).read_text(encoding="utf-8"))
        finally:
            conn.close()
    before = _tree_bytes(work)
    places = {"db": str(cli_artifact.db_dir), "work": str(work)}
    fingerprint = cli_artifact.fingerprint()

    program = _PROGRAMS.get(case.parent.name, run)
    monkeypatch.delenv("REG_META_QUIET", raising=False)
    for step in runs:
        argv = _fill(step["argv"], places)
        if program is run:
            assert case.parent.name in argv, f"{case.name}: argv names another command"
        with monkeypatch.context() as env:
            for name, value in step.get("env", {}).items():
                env.setenv(name, value)
            capsys.readouterr()
            code = program(argv)
            captured = capsys.readouterr()
        assert code == expected["exit_code"], (argv, captured.out, captured.err)

    assert cli_artifact.fingerprint() == fingerprint, "a case wrote the shared artifact"
    stdout = case / "stdout.json"
    if stdout.is_file():
        claim = _fill(json.loads(stdout.read_text(encoding="utf-8")), places)
        assert unclaimed(claim, "$stdout", exact=False) is None
        departure = mismatch(_strict_json(captured.out), claim, "$stdout", exact=False)
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
        assert loaded == reload["result"], loaded
