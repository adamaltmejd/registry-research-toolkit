"""The `cases/curation_toml/` corpus: committed curation files through their loaders.

Each case directory is one boundary claim (`cases/curation_toml/README.md`): a tree
of files as a curator commits them, the public loader that reads them, and the
oracle in `expected.json`. The oracle is either the loaded result, projected
through the loader's public return model, or the located configuration error the
loader refuses with. Expected values are read from the test each case replaces.

Every loader reads the case's `files/` directory in place, so the corpus costs no
fixture IO and no subprocess.
"""

from __future__ import annotations

import dataclasses
import enum
import hashlib
import json
from collections.abc import Mapping
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from _case_projection import MATCH_MODES, mismatch, unclaimed
from pydantic import BaseModel
from reg_core_py import Fqid
from reg_meta_build.cis2016_matrix import MatrixSelector, load_matrix
from reg_meta_build.classifications import load_valid_codes
from reg_meta_build.concept_groups import (
    load_concept_groups,
    load_worklist_concept_groups,
)
from reg_meta_build.curation_tree import (
    load_classification_families,
    load_classification_groups,
    load_classifications,
    load_curation_tree,
    load_register_files,
)
from reg_meta_build.doc_db import load_related_documents
from reg_meta_build.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.relations import load_relations
from reg_meta_build.scb_errata import resolve_scb_errata
from reg_meta_build.source_evidence import SourceRevision
from reg_meta_build.sources.curated_records import read_curated_source
from reg_meta_build.tags import load_tags

from reg_meta_build.fqid_slugs import (
    load_freeze_states,
    load_lineage_config,
    load_provider_toml,
    load_slug_dir,
)

if TYPE_CHECKING:
    from collections.abc import Callable

CASES = Path(__file__).resolve().parent / "cases" / "curation_toml"
REPO = Path(__file__).resolve().parents[2]


def _sole(files: Path, pattern: str) -> Path:
    (path,) = sorted(files.glob(pattern))
    return path


def _authored_revision(path: Path) -> SourceRevision:
    payload = path.read_bytes()
    return SourceRevision.create(
        dataset=f"authored-{path.name}",
        publisher="Agency",
        purpose="source fixture",
        upstream_revision="fixture-1",
        artifact_path=path.name,
        artifact_size=len(payload),
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
    )


def _curated_source(files: Path, args: dict[str, Any]) -> Any:
    path = files / args["file"]
    # `revision_from` declares the revision from other bytes: the reviewed file a
    # later edit no longer matches.
    revision = _authored_revision(files / args.get("revision_from", args["file"]))
    return read_curated_source(path, revision, provider=args["provider"])


def _scb_errata(files: Path, args: dict[str, Any]) -> Any:
    # The build's own call (`curation_compile`): loaded registers, declared books.
    errata = resolve_scb_errata(
        load_register_files(files),
        classifications=frozenset(
            book.classification.short_name for book in load_classifications(files)
        ),
    )
    # A column's `key` (the minted variable's identity) is a property, so it is
    # projected beside the column's fields.
    return {
        "versions": errata.versions,
        "delivered": errata.delivered,
        "columns": [
            {**to_json(column), "key": column.key} for column in errata.columns
        ],
    }


def _classifications(files: Path, args: dict[str, Any]) -> Any:
    books = load_classifications(files)
    return {"classifications": books, "families": load_classification_families(books)}


LOADERS: dict[str, Callable[[Path, dict[str, Any]], Any]] = {
    "curation_tree": lambda files, args: load_curation_tree(files),
    "register_files": lambda files, args: load_register_files(files),
    "classifications": _classifications,
    "relations": lambda files, args: load_relations(files / "relations.toml"),
    "tags": lambda files, args: load_tags(files / "tags.toml"),
    "lineage": lambda files, args: load_lineage_config(files / "lineage.toml"),
    "scb_errata": _scb_errata,
    "concept_groups": lambda files, args: load_concept_groups(files),
    "classification_groups": lambda files, args: load_classification_groups(files),
    "worklist_concept_groups": lambda files, args: load_worklist_concept_groups(
        files / "concept_groups.auto.toml"
    ),
    "slug_dir": lambda files, args: load_slug_dir(files),
    "provider_slugs": lambda files, args: load_provider_toml(_sole(files, "*.toml")),
    "freeze_states": lambda files, args: load_freeze_states(files),
    "matrix_evidence": lambda files, args: load_matrix(
        files / "matrix.json",
        source_mode=args["source_mode"],
        expected_selector=MatrixSelector.model_validate(args["expected_selector"]),
    ),
    "valid_codes": lambda files, args: load_valid_codes(files / "codes.csv"),
    "related_documents": lambda files, args: load_related_documents(
        files / "related_documents.toml"
    ),
    "curated_source": _curated_source,
}


def to_json(value: Any) -> Any:
    """The loaded result as JSON data: models by their TOML (alias) field names,
    dataclasses by field, FQIDs as their string, tuples and lists as lists, sets
    sorted, and a tuple mapping key joined with `/`."""
    if isinstance(value, Fqid):
        return str(value)
    if isinstance(value, BaseModel):
        return to_json(value.model_dump(mode="python", by_alias=True))
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: to_json(getattr(value, field.name))
            for field in dataclasses.fields(value)
        }
    if isinstance(value, Mapping):
        return {
            "/".join(key) if isinstance(key, tuple) else str(key): to_json(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [to_json(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted((to_json(item) for item in value), key=json.dumps)
    if isinstance(value, enum.Enum):
        return to_json(value.value)
    if isinstance(value, (Path, date)):
        return str(value)
    return value


def read_case(case: Path) -> dict[str, Any]:
    expected = json.loads((case / "expected.json").read_text(encoding="utf-8"))
    fails_if = expected.get("fails_if")
    if not isinstance(fails_if, str) or not fails_if.strip():
        raise ValueError(f"{case.name}: expected.json needs a `fails_if` string")
    if expected.get("loader") not in LOADERS:
        raise ValueError(f"{case.name}: unknown loader {expected.get('loader')!r}")
    if ("loads" in expected) == ("error" in expected):
        raise ValueError(f"{case.name}: expected.json needs `loads` or `error`")
    if expected.get("match", "exact") not in MATCH_MODES:
        raise ValueError(f"{case.name}: unknown match {expected['match']!r}")
    exact = expected.get("match", "exact") == "exact"
    if problem := unclaimed(expected.get("loads"), exact=exact):
        raise ValueError(f"{case.name}: {problem}")
    error = expected.get("error", {})
    if error and "code" not in error:
        raise ValueError(f"{case.name}: error needs a located `code`")
    return expected


def check_error(exc: RegMetaError, error: dict[str, Any]) -> str | None:
    if exc.code != error["code"]:
        return f"code {exc.code!r} != {error['code']!r}: {exc.message}"
    exit_code = error.get("exit_code", EXIT_CONFIG)
    if exc.exit_code != exit_code:
        return f"exit code {exc.exit_code} != {exit_code}"
    # Fails if `RegMetaError.__str__` stops printing the message: any log or
    # traceback of a refusal would print empty.
    if str(exc) != exc.message:
        return f"str(exc) {str(exc)!r} != message {exc.message!r}"
    # Fails if a refusal prints a checkout path absolute instead of repo-relative.
    if str(REPO) in exc.message:
        return f"absolute path in message {exc.message!r}"
    message, remediation = exc.message, exc.remediation
    locator = error.get("locator")
    if not isinstance(locator, str) or not locator:
        return "error.locator must name where the refusal points"
    for part in (locator, *error.get("message_contains", ())):
        if part not in message:
            return f"{part!r} not in message {message!r}"
    for part in error.get("remediation_contains", ()):
        if part not in remediation:
            return f"{part!r} not in remediation {remediation!r}"
    return None


def run_case(case: Path) -> str | None:
    """Load one case and return how it departs from its oracle, or None."""
    expected = read_case(case)
    error = expected.get("error")
    try:
        result = LOADERS[expected["loader"]](case / "files", expected.get("args", {}))
    except RegMetaError as exc:
        if error is None:
            return f"refused {exc.code}: {exc.message}"
        return check_error(exc, error)
    if error is not None:
        return f"loaded, expected {error['code']}"
    if expected["loads"] is not True:
        return mismatch(
            to_json(result),
            expected["loads"],
            "$result",
            exact=expected.get("match", "exact") == "exact",
        )
    return None


def case_dirs() -> list[Path]:
    return sorted(path for path in CASES.iterdir() if path.is_dir())


@pytest.mark.parametrize("case", case_dirs(), ids=lambda case: case.name)
def test_curation_toml_case(case: Path) -> None:
    departure = run_case(case)
    assert departure is None, departure
