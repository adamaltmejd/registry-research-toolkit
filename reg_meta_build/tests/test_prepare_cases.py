"""The `cases/prepare/` corpus: raw provider deliveries through the prepare readers.

Each case directory is one boundary claim (`cases/prepare/README.md`): a delivery as
a provider ships it, the reader the prepare step dispatches that input role to, and
the oracle in `expected.json`. The oracle is a set of projections of the prepared
records (or the reader's other prepared evidence), or the located refusal the
prepare command reports. Expected values are read from the test each case replaces.

Readers run in process on the case's delivery, so a case costs one small fixture
write and no build; the SCB readers also commit the one fixture snapshot they read.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from _build_case_runner import scb_row
from _case_projection import MATCH_MODES, mismatch, unclaimed
from _csv_fixtures import (
    IDENTIFIERARE_HEADER,
    write_csv,
    write_scb_input,
    write_scb_snapshot,
)
from _lisa_fixtures import write_lisa_workbook
from _workbook_spec import write_workbook
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.source_evidence import SourceRevision
from reg_meta_build.input_snapshot import LISA_DATASET_ID, open_scb_snapshot
from reg_meta_build.sources.code_lists import read_code_list
from reg_meta_build.sources.lisa import read_lisa_source
from reg_meta_build.sources.scb_auxiliary import iter_scb_auxiliary_records
from reg_meta_build.sources.scb_records import iter_scb_observations
from reg_meta_build.sources.scb_reference_records import (
    read_scb_column_types,
    read_scb_events,
    read_scb_join_keys,
)
from reg_meta_build.sources.scb_values import clean_scb_values
from reg_meta_build.sources.sos import (
    SosParseError,
    parse_directory,
    parse_register_file,
)
from reg_meta_build.sources.sos_records import clean_sos_source
from test_curation_toml_cases import to_json

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from reg_meta_build.input_snapshot import ScbSnapshotReader

CASES = Path(__file__).resolve().parent / "cases" / "prepare"
SOURCES = CASES / "_sources"
# The keys an expected file may carry; the runner refuses any other.
_EXPECTED_KEYS = frozenset(
    {"reader", "args", "replaces", "fails_if", "note", "projections", "agree"}
    | {"error"}
)
_PROJECTION_KEYS = frozenset({"of", "where", "rows", "match", "note"})
_ERROR_KEYS = frozenset(
    {"code", "exit_code", "type", "locator", "message_contains", "note"}
)


# -- Deliveries ------------------------------------------------------------------


@dataclass(frozen=True)
class Delivery:
    """A case's delivery written to disk: bundle files under ``root``, an SCB
    snapshot when the source spec has one."""

    root: Path
    files: tuple[str, ...]
    snapshot: ScbSnapshotReader | None

    def file(self, args: Mapping[str, Any]) -> Path:
        """The file the reader reads: ``args.file``, else the one delivered file."""
        if "file" in args:
            return self.root / args["file"]
        (only,) = self.files
        return self.root / only

    def revision(self, path: Path, args: Mapping[str, Any]) -> SourceRevision:
        """The revision prepare declares for a selected bundle file.

        As `prepared_catalog._inventory`: the dataset is the file's path in the
        bundle and the publisher its provider directory. `args.revision_from` names
        another delivered file whose bytes the revision declares, for a claim about a
        file that no longer matches its selected revision.
        """
        relative = path.relative_to(self.root).as_posix()
        declared = self.root / args.get("revision_from", relative)
        payload = declared.read_bytes()
        lisa = args.get("dataset") == LISA_DATASET_ID
        return SourceRevision.create(
            dataset=args.get("dataset", relative),
            publisher="SCB" if lisa else relative.split("/", 1)[0],
            purpose="Selected machine-readable source declarations",
            upstream_revision="fixture",
            artifact_path=f"catalog/{relative}",
            artifact_size=len(payload),
            artifact_sha256=hashlib.sha256(payload).hexdigest(),
        )

    def snapshot_revision(self, name: str) -> SourceRevision:
        """The revision prepare declares for one SCB snapshot file."""
        assert self.snapshot is not None, "this reader needs an `scb` source"
        (item,) = (i for i in self.snapshot.manifest.files if i.name == name)
        assert item.raw_size is not None and item.raw_sha256 is not None
        return SourceRevision.create(
            dataset=f"scb-{Path(name).stem.lower()}",
            publisher="SCB",
            purpose="Lossless provider machine-source observations",
            upstream_revision=self.snapshot.manifest.edition,
            artifact_path=f"snapshot:source/{name}",
            artifact_size=item.raw_size,
            artifact_sha256=item.raw_sha256,
        )


def source_spec(case: Path) -> dict[str, Any]:
    path = case / "source.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else {}


def _workbook_base(spec: dict[str, Any], work: Path) -> Path | None:
    """The workbook a spec starts from: a `_sources` spec, `lisa`, or none."""
    base = spec.get("from")
    if base is None:
        return None
    path = work / ".base" / f"{base}.xlsx"
    if base == "lisa":
        return write_lisa_workbook(path)
    parent = json.loads((SOURCES / f"{base}.json").read_text(encoding="utf-8"))
    workbook = parent["workbook"]
    return write_workbook(workbook, path, base=_workbook_base(workbook, work))


def write_delivery(
    spec: dict[str, Any], work: Path, *, layer: dict[str, Any] | None = None
) -> Delivery:
    """Materialize a source spec under ``work``; ``layer`` is a variant's workbook
    operations, applied to the spec's own workbook."""
    root = work / "catalog"
    root.mkdir(parents=True)
    files = []
    for relative, text in spec.get("files", {}).items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(text, dict):
            # Exact bytes: every character U+0000..U+00FF is one byte in latin-1.
            path.write_bytes(text["text"].encode(text["encoding"]))
        else:
            path.write_bytes(text.encode("utf-8"))
        files.append(relative)
    if (workbook := spec.get("workbook")) is not None:
        path = root / workbook["file"]
        base = _workbook_base(workbook, work)
        if layer is not None:
            base = write_workbook(workbook, work / ".base" / "layered.xlsx", base=base)
            workbook = layer
        write_workbook(workbook, path, base=base)
        files.append(path.relative_to(root).as_posix())
    snapshot = None
    if (scb := spec.get("scb")) is not None:
        snapshot = _write_snapshot(scb, work)
    return Delivery(root, tuple(files), snapshot)


_SCB_FILES = {
    "registerinformation": "Registerinformation.csv",
    "unika": "UnikaRegisterOchVariabler.csv",
    "identifierare": "Identifierare.csv",
    "timeseries": "Timeseries.csv",
    "vardemangder": "Vardemangder.csv",
    "valid_dates": "VardemangderValidDates.csv",
}


_DEFAULT_MEMBER = {"cvid": 1001, "var_id": 101, "colname": "VALUE"}


def _write_snapshot(scb: dict[str, Any], work: Path) -> ScbSnapshotReader:
    """Write the named SCB files and commit them as an accepted snapshot."""
    if unknown := sorted(scb.keys() - _SCB_FILES.keys() - {"replace_bytes"}):
        raise ValueError(f"unknown scb source keys {unknown}")
    # A snapshot requires Registerinformation.csv; a case about another file gets
    # one default member row.
    scb = {"registerinformation": [_DEFAULT_MEMBER], **scb}
    if "vardemangder" in scb and "valid_dates" not in scb:
        # The two value files come as a pair; by default every listed item is
        # valid over the whole fixture window (as `cases/build`).
        items = {row.rsplit("|", 1)[-1] for row in scb["vardemangder"]} - {""}
        scb["valid_dates"] = [f"{item}|2000-01-01|2030-12-31" for item in sorted(items)]
    rows = {
        key: [row if isinstance(row, str) else scb_row(row) for row in scb[key]]
        for key in _SCB_FILES
        if key in scb
    }
    identifiers = rows.pop("identifierare", None)
    directory = write_scb_input(
        work / "delivery",
        include=tuple(rows),
        **{f"{key}_rows": value for key, value in rows.items()},
    )
    if identifiers is not None:
        write_csv(
            directory / _SCB_FILES["identifierare"], IDENTIFIERARE_HEADER, identifiers
        )
    for name, replacements in scb.get("replace_bytes", {}).items():
        # Raw bytes a CSV writer cannot produce, such as DOS remnants undefined in
        # cp1252: each placeholder becomes the latin-1 bytes of its replacement.
        path = directory / name
        content = path.read_bytes()
        for placeholder, raw in replacements.items():
            content = content.replace(placeholder.encode(), raw.encode("latin-1"))
        path.write_bytes(content)
    return open_scb_snapshot(write_scb_snapshot(work / "accepted", directory))


# -- Readers -----------------------------------------------------------------------


def _sos_workbook(delivery: Delivery, args: dict[str, Any]) -> Any:
    path = delivery.file(args)
    return clean_sos_source(parse_register_file(path), delivery.revision(path, args))


def _sos_parsed(delivery: Delivery, args: dict[str, Any]) -> Any:
    """`parse-sos` on one workbook, or on every workbook of `args.directory`."""
    if "directory" in args:
        return parse_directory(delivery.root / args["directory"])
    return parse_register_file(delivery.file(args))


def _lisa_workbook(delivery: Delivery, args: dict[str, Any]) -> Any:
    path = delivery.file(args)
    revision = delivery.revision(path, {"dataset": LISA_DATASET_ID, **args})
    return read_lisa_source(path, revision)


def _code_list(delivery: Delivery, args: dict[str, Any]) -> Any:
    path = delivery.file(args)
    revision = delivery.revision(path, args)
    return read_code_list(path, revision, name=args.get("name", path.stem))


def _bundle_reader(read: Callable[[Path, SourceRevision], Any]):
    def reader(delivery: Delivery, args: dict[str, Any]) -> Any:
        path = delivery.file(args)
        return read(path, delivery.revision(path, args))

    return reader


def _scb_values(delivery: Delivery, args: dict[str, Any]) -> Any:
    assert delivery.snapshot is not None, "scb_values needs an `scb` source"
    cleaned = clean_scb_values(delivery.snapshot)
    return {
        "provenance": cleaned.provenance,
        "descriptors": cleaned.descriptors,
        "values": cleaned.values,
        "associations": list(cleaned.associations()),
        "validity": cleaned.validity,
    }


def _scb_auxiliary(delivery: Delivery, args: dict[str, Any]) -> Any:
    assert delivery.snapshot is not None, "scb_auxiliary needs an `scb` source"
    revision = delivery.snapshot_revision(args["file"])
    return list(iter_scb_auxiliary_records(delivery.snapshot, args["file"], revision))


def _scb_records(delivery: Delivery, args: dict[str, Any]) -> Any:
    assert delivery.snapshot is not None, "scb_records needs an `scb` source"
    revision = delivery.snapshot_revision("Registerinformation.csv")
    return list(iter_scb_observations(delivery.snapshot, revision))


def _scb_events(delivery: Delivery, args: dict[str, Any]) -> Any:
    assert delivery.snapshot is not None, "scb_events needs an `scb` source"
    revision = delivery.snapshot_revision("Timeseries.csv")
    if "revision_from" in args:
        revision = delivery.revision(delivery.root / args["revision_from"], {})
    return read_scb_events(delivery.snapshot, revision)


# `reg_meta.errors.EXIT_INTERNAL`, the top-level handler's exit code; no case
# claims it, so it is spelled here rather than imported.
_EXIT_INTERNAL = 30


def _prepare_refusal(exc: Exception) -> tuple[str, int]:
    """The code and exit code `prepare-sources` reports (`cli._cmd_prepare_sources`):
    a located refusal keeps its own, a `ValueError` or `OSError` is wrapped, and any
    other exception reaches the CLI's top-level `internal_error`."""
    if isinstance(exc, RegMetaError):
        return exc.code, exc.exit_code
    if isinstance(exc, (ValueError, OSError)):
        return "source_preparation_failed", EXIT_CONFIG
    return "internal_error", _EXIT_INTERNAL


def _parse_sos_refusal(exc: Exception) -> tuple[str, int]:
    """The code and exit code `parse-sos` reports (`cli._cmd_parse_sos`)."""
    if isinstance(exc, RegMetaError):
        return exc.code, exc.exit_code
    if isinstance(exc, SosParseError):
        return "sos_parse_error", EXIT_CONFIG
    return "internal_error", _EXIT_INTERNAL


@dataclass(frozen=True)
class Reader:
    read: Callable[[Delivery, dict[str, Any]], Any]
    refusal: Callable[[Exception], tuple[str, int]] = _prepare_refusal


# Each prepare input role's reader, as `prepared_catalog.prepare_catalog_sources`
# dispatches it, plus the `parse-sos` inspection command.
READERS: dict[str, Reader] = {
    "sos_workbook": Reader(_sos_workbook),
    "sos_parsed": Reader(_sos_parsed, _parse_sos_refusal),
    "lisa_workbook": Reader(_lisa_workbook),
    "code_list": Reader(_code_list),
    "scb_column_types": Reader(_bundle_reader(read_scb_column_types)),
    "scb_join_keys": Reader(_bundle_reader(read_scb_join_keys)),
    "scb_records": Reader(_scb_records),
    "scb_auxiliary": Reader(_scb_auxiliary),
    "scb_events": Reader(_scb_events),
    "scb_values": Reader(_scb_values),
}


# -- Projections -------------------------------------------------------------------


def result_json(result: Any) -> Any:
    """The reader's result as JSON data (`test_curation_toml_cases.to_json`), with
    each value-list association joined to the value and the list it names, as
    `value` and `descriptor`."""
    data = to_json(result)
    if isinstance(data, dict) and isinstance(data.get("values"), dict):
        for association in data.get("associations", ()):
            association["value"] = data["values"][association["value_key"]]
            association["descriptor"] = data["descriptors"].get(
                association["descriptor_key"]
            )
    return data


def at(data: Any, path: str) -> list[Any]:
    """The elements at a dotted ``path``; a mapping lists `{"key", "value"}` items."""
    for key in path.split(".") if path else ():
        data = data[key]
    if isinstance(data, dict):
        return [{"key": key, "value": value} for key, value in data.items()]
    if not isinstance(data, list):
        raise TypeError(f"{path!r} is neither a list nor a mapping")
    return data


# A content hash (64 hex digits) or a commit (40).
_DIGEST = re.compile(r"(?<![0-9a-f])[0-9a-f]{40}(?:[0-9a-f]{24})?(?![0-9a-f])")


def alias_digests(value: Any) -> Any:
    """``value`` with each distinct digest replaced by `#1`, `#2`, ... in order of
    first appearance, so equal identities stay equal without a literal hash."""
    aliases: dict[str, str] = {}

    def alias(match: re.Match[str]) -> str:
        return aliases.setdefault(match.group(), f"#{len(aliases) + 1}")

    def walk(item: Any) -> Any:
        if isinstance(item, str):
            return _DIGEST.sub(alias, item)
        if isinstance(item, dict):
            return {walk(key): walk(child) for key, child in item.items()}
        if isinstance(item, list):
            return [walk(child) for child in item]
        return item

    return walk(value)


def project(data: Any, spec: dict[str, Any]) -> str | None:
    """How the selected elements depart from the projection's rows, or None."""
    exact = spec.get("match", "includes") == "exact"
    where = spec.get("where")
    selected = [
        item
        for item in at(data, spec["of"])
        if where is None or mismatch(item, where, exact=False) is None
    ]
    return mismatch(
        alias_digests(selected), spec["rows"], f"$.{spec['of']}", exact=exact
    )


# A source revision id, `<dataset>@sha256:<digest of the delivered bytes>`.
_REVISION = re.compile(r"@sha256:[0-9a-f]{64}")


def without_revision(data: Any) -> str:
    """``data`` as JSON text with its source revision ids blanked: a variant
    delivers other bytes, so only its revision may differ."""
    return _REVISION.sub("@<revision>", json.dumps(data, ensure_ascii=False))


# -- Running a case ----------------------------------------------------------------


def read_case(case: Path) -> dict[str, Any]:
    """The case's expected file, after the format checks the README states."""
    expected = json.loads((case / "expected.json").read_text(encoding="utf-8"))
    name = case.name
    if unknown := sorted(expected.keys() - _EXPECTED_KEYS):
        raise ValueError(f"{name}: unknown expected.json keys {unknown}")
    fails_if = expected.get("fails_if")
    if not isinstance(fails_if, str) or not fails_if.strip():
        raise ValueError(f"{name}: expected.json needs a `fails_if` string")
    if expected.get("reader") not in READERS:
        raise ValueError(f"{name}: unknown reader {expected.get('reader')!r}")
    if ("projections" in expected) == ("error" in expected):
        raise ValueError(f"{name}: expected.json needs `projections` or `error`")
    for spec in expected.get("projections", ()):
        if unknown := sorted(spec.keys() - _PROJECTION_KEYS):
            raise ValueError(f"{name}: unknown projection keys {unknown}")
        match = spec.get("match", "includes")
        if match not in MATCH_MODES:
            raise ValueError(f"{name}: unknown match {match!r}")
        if not isinstance(spec.get("rows"), list):
            raise TypeError(f"{name}: a projection needs a `rows` list")
        for part, exact in (("rows", match == "exact"), ("where", False)):
            if problem := unclaimed(spec.get(part), f"$.{part}", exact=exact):
                raise ValueError(f"{name}: {problem}")
    if error := expected.get("error"):
        if unknown := sorted(error.keys() - _ERROR_KEYS):
            raise ValueError(f"{name}: unknown error keys {unknown}")
        if not {"code", "type", "locator"} <= error.keys():
            raise ValueError(f"{name}: error needs `code`, `type` and `locator`")
    if ("agree" in expected) != bool(source_spec(case).get("variants")):
        raise ValueError(f"{name}: `agree` and source.json `variants` go together")
    return expected


def check_error(exc: Exception, reader: Reader, error: dict[str, Any]) -> str | None:
    code, exit_code = reader.refusal(exc)
    message = exc.message if isinstance(exc, RegMetaError) else str(exc)
    if code != error["code"]:
        return f"code {code!r} != {error['code']!r}: {type(exc).__name__}: {message}"
    if exit_code != error.get("exit_code", EXIT_CONFIG):
        return f"exit code {exit_code} != {error.get('exit_code', EXIT_CONFIG)}"
    if type(exc).__name__ != error["type"]:
        return f"type {type(exc).__name__} != {error['type']}: {message}"
    for part in (error["locator"], *error.get("message_contains", ())):
        if part not in message:
            return f"{part!r} not in message {message!r}"
    return None


def run_case(case: Path, work: Path) -> str | None:
    """Read one case's delivery and return how it departs from its oracle, or None."""
    expected = read_case(case)
    spec = source_spec(case)
    reader = READERS[expected["reader"]]
    args = expected.get("args", {})
    delivery = write_delivery(spec, work / "base")
    error = expected.get("error")
    try:
        data = result_json(reader.read(delivery, args))
    except Exception as exc:  # noqa: BLE001 - every refusal is mapped as the CLI maps it
        if error is None:
            code, _ = reader.refusal(exc)
            return f"refused {code}: {type(exc).__name__}: {exc}"
        return check_error(exc, reader, error)
    if error is not None:
        return f"read, expected {error['code']}"
    for projection in expected["projections"]:
        if departure := project(data, projection):
            return departure
    for name, layer in spec.get("variants", {}).items():
        variant = write_delivery(spec, work / name, layer=layer)
        try:
            other = result_json(reader.read(variant, args))
        except Exception as exc:  # noqa: BLE001 - a refused variant is a departure
            return f"variant {name!r} refused: {type(exc).__name__}: {exc}"
        for path in expected["agree"]:
            if without_revision(at(data, path)) != without_revision(at(other, path)):
                return f"variant {name!r}: {path!r} differs from the delivery's"
    return None


def case_dirs() -> list[Path]:
    """Every case directory, sorted; `_`-prefixed directories hold shared data."""
    return sorted(
        path
        for path in CASES.iterdir()
        if path.is_dir() and not path.name.startswith("_")
    )


@pytest.mark.parametrize("case", case_dirs(), ids=lambda case: case.name)
def test_prepare_case(case: Path, tmp_path: Path) -> None:
    departure = run_case(case, tmp_path)
    assert departure is None, departure
