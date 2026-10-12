"""The resolve phase's output as a bundle directory that `materialize-db` places.

A bundle is a cache artifact keyed by the exact resolve code (the import walk from
`pipeline`, which includes this module), never a public contract: `bundle.json`
records the resolve code's fingerprint (`resolve_code`), and a builder whose resolve
code differs refuses the bundle rather than migrating it. `pipeline` imports this module,
so it must not import `db`, `derive`, `validate` or `materialize`.

Layout:

- `bundle.json`: the strict `BundleRecord`, written last, so a partial bundle has none.
- `code_sets.pickle.zst`: every distinct `ResolvedCodeSet`, one pickle frame each.
- `handoff.pickle.zst`: the writer's other inputs in one frame, then one frame per
  variable; a code set is a persistent reference into `code_sets.pickle.zst`.
- `events.jsonl.gz`: a copy of the closed resolve-phase ledger member.

Each frame has its own pickler and unpickler, so the pickle memo stays bounded by
the largest variable. Loading checks every payload's size and SHA-256 before any
unpickling and admits only `reg_meta_build` models through `find_class`.

A bundle serves either mode. A strict resolution that did not raise took neither
strict/diagnostic fork (`source_scope` names them), so a diagnostic one of the same
inputs is identical. A diagnostic resolution equals the strict one only if it took
neither fork (`strict_refusals == 0`), so only such a bundle, and only with no
errors, is placed strictly. Publishability and `publication_ready` follow the
request, never the resolving run.
"""

from __future__ import annotations

import json
import pickle
import shutil
from compression import zstd
from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, Any, Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    ValidationError,
    model_validator,
)

from reg_meta_build.resolve_code import resolve_code_sha256
from reg_meta_build.resolved_catalog import ResolvedCodeSet, ResolvedVariable
from reg_meta_build.source_files import _file_sha256

from .errors import EXIT_CONFIG, EXIT_USAGE, RegMetaError

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta_build._curation import SearchPin, SearchSynonym
    from reg_meta_build.data_warnings import DataWarning
    from reg_meta_build.resolved_catalog import (
        ResolvedClassification,
        ResolvedClassificationSuccession,
        ResolvedEdition,
        ResolvedRegister,
        ResolvedVariant,
    )
    from reg_meta_build.resolved_metadata import ResolvedMetadata

FORMAT_VERSION = 3
BUNDLE_FILE = "bundle.json"
CODE_SETS_FILE = "code_sets.pickle.zst"
HANDOFF_FILE = "handoff.pickle.zst"
EVENTS_FILE = "events.jsonl.gz"
_PROTOCOL = 5

BUNDLE_INVALID = "resolved_bundle_invalid"
BUNDLE_DIGEST_MISMATCH = "resolved_bundle_digest_mismatch"
BUNDLE_GLOBAL_REFUSED = "resolved_bundle_global_refused"
BUNDLE_COUNT_MISMATCH = "resolved_bundle_count_mismatch"
BUNDLE_SCOPE_MISMATCH = "resolved_bundle_scope_mismatch"
BUNDLE_BLOCKED = "resolved_bundle_blocked"
BUNDLE_STRICT_REFUSED = "resolved_bundle_strict_refused"
BUNDLE_CODE_MISMATCH = "resolved_bundle_code_mismatch"


@dataclass(frozen=True)
class ResolvedBuild:
    """A resolved build handed to materialization: the writer's inputs, the mode
    and the report so far. Its ledger member is closed."""

    output: Path
    report_dir: Path
    ledger: Path
    diagnostic: bool
    scoped: bool
    publishable: bool
    # Copied, never mutated, by `materialize_build`.
    build_result: dict[str, object]
    variables: tuple[ResolvedVariable, ...]
    parent_registers: tuple[ResolvedRegister, ...]
    parent_variants: tuple[tuple[ResolvedRegister, ResolvedVariant], ...]
    editions: tuple[ResolvedEdition, ...]
    classifications: tuple[ResolvedClassification, ...]
    classification_successions: tuple[ResolvedClassificationSuccession, ...]
    metadata: ResolvedMetadata
    # Only the catalog's: `BUILD_ONLY_CODES` stay in the ledger.
    data_warnings: tuple[DataWarning, ...]
    search_pins: tuple[SearchPin, ...]
    search_synonyms: tuple[SearchSynonym, ...]
    # Without `builder_commit`, which materialization rechecks and adds.
    manifest: dict[str, str]


# Fields a loader supplies or `bundle.json` records; every other field is a writer
# input and is pickled, so a new one cannot be left out of the bundle.
_PLACEMENT = frozenset({"output", "report_dir", "ledger"})
_RECORDED = frozenset({"diagnostic", "scoped", "publishable", "build_result"})
_FRAMED = frozenset({"variables"})
_HEADER = tuple(
    f.name
    for f in fields(ResolvedBuild)
    if f.name not in _PLACEMENT | _RECORDED | _FRAMED
)

_SHA256 = r"^[0-9a-f]{64}$"


class _Record(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class BundlePayload(_Record):
    sha256: str = Field(pattern=_SHA256)
    size: int = Field(ge=0)


class BundlePayloads(_Record):
    handoff: BundlePayload
    code_sets: BundlePayload
    events: BundlePayload


class BundleRecord(_Record):
    """`bundle.json`: what was resolved, from which pins, and the payload digests."""

    format_version: Literal[3]
    # `resolve_code.resolve_code_sha256()` of the builder that resolved.
    resolve_code_sha256: str = Field(pattern=_SHA256)
    # The resolving run's mode, not a requirement on the materializing one.
    mode: Literal["strict", "diagnostic"]
    # The `--registers` selection as a sorted set; empty for a full build.
    registers: tuple[str, ...]
    publishable: bool
    # Times a diagnostic resolution took a path a strict one refuses
    # (`build_result.counts.strict_refusals`); always 0 for a strict bundle.
    strict_refusals: int = Field(ge=0)
    prepared_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    prepared_manifest_sha256: str = Field(pattern=_SHA256)
    curation_tree_sha256: str = Field(pattern=_SHA256)
    # Resolution's report before the writer runs; materialization starts from it.
    build_result: dict[str, JsonValue]
    variables: int = Field(ge=0)
    states: int = Field(ge=0)
    code_sets: int = Field(ge=0)
    payloads: BundlePayloads

    @model_validator(mode="after")
    def _consistent(self) -> Self:
        if self.registers != tuple(sorted(set(self.registers))):
            raise ValueError("registers must be a sorted set")
        if self.publishable != (self.mode == "strict" and not self.registers):
            raise ValueError("publishable must mean a strict full build")
        counts = self.build_result.get("counts")
        recorded = (
            counts.get("strict_refusals", 0) if isinstance(counts, dict) else None
        )
        if recorded != self.strict_refusals or (
            self.mode == "strict" and self.strict_refusals
        ):
            raise ValueError(
                "strict_refusals must match build_result and be 0 for a strict bundle"
            )
        if (
            self.build_result.get("variables"),
            self.build_result.get("states"),
            self.build_result.get("curation_tree_sha256"),
        ) != (self.variables, self.states, self.curation_tree_sha256):
            raise ValueError("counts and curation hash must match build_result")
        return self


def _refusal(
    code: str, message: str, remediation: str, *, usage: bool = False
) -> RegMetaError:
    return RegMetaError(
        exit_code=EXIT_USAGE if usage else EXIT_CONFIG,
        code=code,
        error_class="usage" if usage else "configuration",
        message=message,
        remediation=remediation,
    )


_REBUILD = (
    "Rebuild the bundle with `build-db --resolved-out` from this builder revision."
)


class _Pickler(pickle.Pickler):
    """Writes each distinct code set once, as a reference into `code_sets`."""

    def __init__(self, file: Any, code_sets: dict[ResolvedCodeSet, int]) -> None:
        super().__init__(file, protocol=_PROTOCOL)
        self._code_sets = code_sets

    def persistent_id(self, obj: object) -> int | None:
        if type(obj) is not ResolvedCodeSet:
            return None
        # Frozen models hash and compare by field values, so equal memberships
        # held by distinct objects share one entry.
        return self._code_sets.setdefault(obj, len(self._code_sets))


class _Unpickler(pickle.Unpickler):
    """Admits only `reg_meta_build` Pydantic models; anything else is refused."""

    def __init__(
        self, file: Any, path: Path, code_sets: tuple[ResolvedCodeSet, ...]
    ) -> None:
        super().__init__(file)
        self._path = path
        self._code_sets = code_sets

    def find_class(self, module: str, name: str) -> Any:
        # Resolving the name may import a module, so check the module first;
        # then require a model defined there, not a name it merely imports.
        found = (
            super().find_class(module, name)
            if module.startswith("reg_meta_build.") and "." not in name
            else None
        )
        if not (
            isinstance(found, type)
            and issubclass(found, BaseModel)
            and found.__module__ == module
        ):
            raise _refusal(
                BUNDLE_GLOBAL_REFUSED,
                f"{self._path}: refusing pickled global {module}.{name}; a bundle "
                "holds only reg_meta_build models",
                "Do not load this bundle; " + _REBUILD,
            )
        return found

    def persistent_load(self, pid: Any) -> ResolvedCodeSet:
        if type(pid) is not int or not 0 <= pid < len(self._code_sets):
            raise _refusal(
                BUNDLE_INVALID,
                f"{self._path}: code-set reference {pid!r} is out of range",
                _REBUILD,
            )
        return self._code_sets[pid]


def write_resolved_bundle(
    resolved: ResolvedBuild, directory: Path, *, resolve_code_sha256: str
) -> None:
    """Write `resolved` as a new bundle directory; `bundle.json` comes last.

    `resolve_code_sha256` is the fingerprint of the code that resolved it.
    """
    result = resolved.build_result
    states = sum(len(variable.states) for variable in resolved.variables)
    if (len(resolved.variables), states) != (result["variables"], result["states"]):
        raise ValueError("resolved variables and states differ from the build result")
    directory.mkdir(parents=True, exist_ok=False)
    code_sets: dict[ResolvedCodeSet, int] = {}
    with zstd.open(directory / HANDOFF_FILE, "wb") as stream:
        _Pickler(stream, code_sets).dump(
            {name: getattr(resolved, name) for name in _HEADER}
        )
        for variable in resolved.variables:
            _Pickler(stream, code_sets).dump(variable)
    with zstd.open(directory / CODE_SETS_FILE, "wb") as stream:
        # Insertion order is reference order: entry i is persistent id i.
        for code_set in code_sets:
            pickle.Pickler(stream, protocol=_PROTOCOL).dump(code_set)
    shutil.copyfile(resolved.ledger, directory / EVENTS_FILE)
    registers = result.get("registers", [])
    assert isinstance(registers, list)
    counts = result["counts"]
    assert isinstance(counts, dict)
    manifest = resolved.manifest
    record = BundleRecord(
        format_version=FORMAT_VERSION,
        resolve_code_sha256=resolve_code_sha256,
        mode="diagnostic" if resolved.diagnostic else "strict",
        registers=tuple(registers),
        publishable=resolved.publishable,
        strict_refusals=counts.get("strict_refusals", 0),
        prepared_commit=manifest["prepared_commit"],
        prepared_manifest_sha256=manifest["prepared_manifest_sha256"],
        curation_tree_sha256=manifest["curation_tree_sha256"],
        build_result=json.loads(json.dumps(result)),
        variables=len(resolved.variables),
        states=states,
        code_sets=len(code_sets),
        payloads=BundlePayloads(
            handoff=_payload(directory / HANDOFF_FILE),
            code_sets=_payload(directory / CODE_SETS_FILE),
            events=_payload(directory / EVENTS_FILE),
        ),
    )
    (directory / BUNDLE_FILE).write_text(
        record.model_dump_json(indent=2) + "\n", encoding="utf-8"
    )


def _payload(path: Path) -> BundlePayload:
    return BundlePayload(sha256=_file_sha256(path), size=path.stat().st_size)


def read_bundle_record(directory: Path) -> BundleRecord:
    """The strictly validated `bundle.json`; nothing else is read."""
    path = directory / BUNDLE_FILE
    try:
        return BundleRecord.model_validate_json(path.read_bytes())
    except (OSError, ValidationError) as exc:
        raise _refusal(
            BUNDLE_INVALID,
            f"{path}: not a resolved bundle of format {FORMAT_VERSION}: {exc}",
            _REBUILD,
        ) from exc


def admit_bundle_code(record: BundleRecord, directory: Path) -> None:
    """Refuse a bundle resolved by other resolve code than this builder's.

    Its resolutions would be stale, yet a publishable materialization would stamp
    them with this builder's commit.
    """
    current = resolve_code_sha256()
    if record.resolve_code_sha256 != current:
        raise _refusal(
            BUNDLE_CODE_MISMATCH,
            f"{directory / BUNDLE_FILE}: resolved by resolve code "
            f"{record.resolve_code_sha256}, but this builder's is {current}",
            "Resolve again with build-db --resolved-out from this builder; a bundle "
            "is reusable only by the resolve code that wrote it.",
        )


def admit_bundle_mode(
    record: BundleRecord,
    directory: Path,
    *,
    diagnostic: bool,
    registers: tuple[str, ...],
) -> None:
    """Refuse a bundle resolved for other registers, or one a strict request may
    not place: a diagnostic bundle that took a path a strict resolution refuses, or
    any bundle with resolution errors. Either mode's bundle is otherwise placed in
    either mode."""
    requested = tuple(sorted(set(registers)))
    if requested != record.registers:
        raise _refusal(
            BUNDLE_SCOPE_MISMATCH,
            f"{directory / BUNDLE_FILE}: resolved for registers "
            f"{list(record.registers)}, requested {list(requested)}",
            "Request the bundle's own --registers, or resolve the requested "
            "artifact with build-db.",
            usage=True,
        )
    if diagnostic:
        return
    if record.strict_refusals:
        raise _refusal(
            BUNDLE_STRICT_REFUSED,
            f"{directory / BUNDLE_FILE}: this diagnostic resolution took "
            f"{record.strict_refusals} path(s) a strict resolution refuses (a "
            "per-column state overlap, or a partition split with no converted "
            "naming), so it is not the strict resolution of its inputs",
            "Find the overlapping_* issues, and the issues on an "
            "'accepted-partition' key, in the bundle's events.jsonl.gz; fix their "
            "curation rather than acknowledge them and resolve again, or place this "
            "bundle with --diagnostic.",
        )
    counts = record.build_result.get("counts")
    errors = counts.get("error", 0) if isinstance(counts, dict) else None
    if errors != 0:
        raise _refusal(
            BUNDLE_BLOCKED,
            f"{directory / BUNDLE_FILE}: a bundle with {errors} resolution errors "
            "cannot be materialized strictly",
            "Resolve the errors in the bundle's events.jsonl.gz and rebuild, or "
            "place the bundle with --diagnostic.",
        )


def load_resolved_bundle(
    directory: Path,
    record: BundleRecord,
    *,
    output: Path,
    report_dir: Path,
    diagnostic: bool,
) -> ResolvedBuild:
    """Verify and unpickle an admitted bundle; its ledger is placed by the caller.

    `report_dir / events.jsonl.gz` is where materialization appends its member.
    The writer revalidates every model strictly. `diagnostic` is the requested
    mode: with the bundle's registers it sets publishability and
    `publication_ready`, whichever mode resolved.
    """
    for name, payload in (
        (HANDOFF_FILE, record.payloads.handoff),
        (CODE_SETS_FILE, record.payloads.code_sets),
        (EVENTS_FILE, record.payloads.events),
    ):
        path = directory / name
        try:
            actual = _payload(path)
        except OSError as exc:
            raise _refusal(
                BUNDLE_INVALID, f"{path}: unreadable bundle payload: {exc}", _REBUILD
            ) from exc
        if actual != payload:
            raise _refusal(
                BUNDLE_DIGEST_MISMATCH,
                f"{path}: size {actual.size} and SHA-256 {actual.sha256} differ from "
                f"bundle.json's {payload.size} and {payload.sha256}",
                _REBUILD,
            )
    code_sets = tuple(_frames(directory / CODE_SETS_FILE, record.code_sets, ()))
    header, *variables = _frames(
        directory / HANDOFF_FILE, 1 + record.variables, code_sets
    )
    if (
        not all(type(code_set) is ResolvedCodeSet for code_set in code_sets)
        or not isinstance(header, dict)
        or tuple(header) != _HEADER
        or not all(type(variable) is ResolvedVariable for variable in variables)
    ):
        raise _refusal(
            BUNDLE_INVALID,
            f"{directory}: the payload frames are not code sets, a header holding "
            f"exactly {list(_HEADER)} and variables",
            _REBUILD,
        )
    states = sum(len(variable.states) for variable in variables)
    result = record.build_result
    if (len(variables), states) != (result["variables"], result["states"]):
        raise _refusal(
            BUNDLE_COUNT_MISMATCH,
            f"{directory / HANDOFF_FILE}: {len(variables)} variables and {states} "
            f"states, but the build result records {result['variables']} and "
            f"{result['states']}",
            _REBUILD,
        )
    publishable = not diagnostic and not record.registers
    counts = result["counts"]
    assert isinstance(counts, dict)
    return ResolvedBuild(
        output=output,
        report_dir=report_dir,
        ledger=report_dir / "events.jsonl.gz",
        diagnostic=diagnostic,
        scoped=bool(record.registers),
        publishable=publishable,
        # As `pipeline._resolve` sets it for the requested mode.
        build_result={
            **result,
            "publication_ready": publishable and not counts.get("error", 0),
        },
        variables=tuple(variables),
        **header,
    )


def _frames(
    path: Path, count: int, code_sets: tuple[ResolvedCodeSet, ...]
) -> list[Any]:
    """Exactly `count` frames, then the end of the stream."""
    # Zero frames compress to a zero-byte file, which the zstd reader refuses as
    # truncated; the payload size check already pinned it.
    if count == 0 and path.stat().st_size == 0:
        return []
    try:
        with zstd.open(path, "rb") as stream:
            frames = [_Unpickler(stream, path, code_sets).load() for _ in range(count)]
            trailing = stream.read(1)
    except RegMetaError:
        raise
    # Any decode failure (truncation, a corrupt frame, a model that no longer
    # accepts its pickled state) is a bad bundle.
    except Exception as exc:
        raise _refusal(
            BUNDLE_INVALID, f"{path}: unreadable pickle frames: {exc}", _REBUILD
        ) from exc
    if trailing:
        raise _refusal(
            BUNDLE_INVALID, f"{path}: more than the {count} recorded frames", _REBUILD
        )
    return frames
