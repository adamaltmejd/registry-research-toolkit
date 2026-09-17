"""Bind accepted naming entries to finite, source-native identities.

This is an offline conversion boundary, not an identity resolver. Callers supply
exact native bindings; a legacy split key needs its own explicit binding. Generated
pins are authoritative only in curating/frozen zones, while snapshots remain
comparison evidence. No new slug is derived here.
"""

from __future__ import annotations

import json
import tomllib
from collections import defaultdict
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Self

from pydantic import BaseModel, ConfigDict, model_validator

from reg_meta_build.fqid_slugs import (
    AUTO_FILE_SUFFIX,
    CLASSIFICATIONS_FILE,
    FREEZE_STATE_FILE,
    SNAPSHOT_FILENAME,
    EntityKind,
    SlugEntry,
    SlugFreezeState,
    _parse_register_id,
    _parse_variable_id,
    _parse_variant_id,
    load_classifications_toml,
    load_freeze_states,
    load_provider_toml,
)
from reg_meta_build.id import mint, mint_canonical_scb
from reg_meta_build.source_curation import (
    PeerGuard,
    RecordExpectation,
    ResolutionDiagnostic,
    SourceEvidence,
    evaluate_source_expectations,
)
from reg_meta_build.source_records import SourceRevision, canonical_sha256
from reg_meta_build.sources.code_lists import read_selected_bytes

if TYPE_CHECKING:
    from collections.abc import Iterable

    from reg_meta_build.source_records import SourceRecord


type NativeKey = tuple[str | int, ...]
type NamingKey = tuple[EntityKind, str | None, str]


class NamingConversionError(ValueError):
    """A naming input or explicit conversion binding is invalid."""


class _NamingModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class NativeNamingTarget(_NamingModel):
    """One exact native entity, guarded by source evidence or a source artifact.

    Keys are opaque to this module and retain integer/string distinctions. Variable
    and variant names are scoped by their exact native register_key. A declaration
    without row members (for example a standalone code-list identity) is guarded by
    its selected identity_revision instead of invented variable observations.
    A native variable can be named by its exact existing key without asserting
    delivery membership. A curated partition still requires its checked members.
    Parent names can use checked identity anchors without a peer-set assertion.
    """

    kind: EntityKind
    provider: str | None
    source_key: NativeKey
    register_key: NativeKey | None = None
    expectations: tuple[RecordExpectation, ...] = ()
    peer_guards: tuple[PeerGuard, ...] = ()
    identity_revision: SourceRevision | None = None

    @model_validator(mode="after")
    def _bounded_identity(self) -> Self:
        if not self.source_key or any(part == "" for part in self.source_key):
            raise ValueError("a naming target needs a nonempty exact source key")
        if (self.kind == "classification") != (self.provider is None):
            raise ValueError("only classification naming is provider-independent")
        if self.provider is not None and not self.provider.strip():
            raise ValueError("provider cannot be blank")
        if self.kind in ("variable", "register_variant"):
            if not self.register_key:
                raise ValueError(
                    "variable/variant naming needs its native register key"
                )
        elif self.register_key is not None:
            raise ValueError("only variable/variant naming takes a register key")
        if self.identity_revision is not None:
            if self.expectations or self.peer_guards:
                raise ValueError("use member guards or an identity revision, not both")
        elif self.kind == "variable" and not self.expectations and not self.peer_guards:
            pass  # Exact native existence is checked against the selected evidence.
        elif not self.expectations or (
            not self.peer_guards and self.kind not in {"register", "register_variant"}
        ):
            raise ValueError(
                "member naming needs expectations and non-parent identities need complete peer guards"
            )
        expected = {
            (e.ref.source, e.ref.semantic_record_key) for e in self.expectations
        }
        if len(expected) != len(self.expectations):
            raise ValueError("naming expectations must have unique semantic members")
        guarded = {
            (ref.source, ref.semantic_record_key)
            for guard in self.peer_guards
            for ref in guard.expected_members
        }
        if self.peer_guards and expected != guarded:
            raise ValueError(
                "naming peer guards must cover exactly the expected members"
            )
        return self


class LegacyNamingBinding(_NamingModel):
    kind: EntityKind
    provider: str | None
    source_id: str
    target: NativeNamingTarget

    @model_validator(mode="after")
    def _same_namespace(self) -> Self:
        if self.kind != self.target.kind or self.provider != self.target.provider:
            raise ValueError(
                "legacy binding and native target must share kind/provider"
            )
        _validate_legacy_key(self.kind, self.source_id)
        return self


class AcceptedNamingEntry(_NamingModel):
    """One validated original TOML entry, including absent slug intent."""

    revision: SourceRevision
    origin: Literal["authored", "generated"]
    entry: SlugEntry
    supplied_fields: tuple[str, ...]
    content_sha256: str

    @property
    def entry_id(self) -> str:
        return f"{self.revision.artifact_path}#{self.entry.kind}/{self.entry.source_id}"


class NamingInputFile(_NamingModel):
    revision: SourceRevision
    role: Literal["authored", "generated", "control", "comparison"]
    entry_count: int


class NamingFreezeSetting(_NamingModel):
    zone: str
    state: SlugFreezeState


class NamingSelection(_NamingModel):
    files: tuple[NamingInputFile, ...]
    entries: tuple[AcceptedNamingEntry, ...]
    freeze: tuple[NamingFreezeSetting, ...]


class NamingDeclaration(_NamingModel):
    target: NativeNamingTarget
    naming: SlugEntry
    contributors: tuple[AcceptedNamingEntry, ...]


class NamingAmbiguity(_NamingModel):
    """Accepted names affected by unresolved ownership within one source family.

    This is diagnostic attribution, not a binding or an accepted unresolved
    decision. It cannot form a variable, assign a column or lower error severity.
    The offline conversion must retain the exact original names and whole-family
    evidence after checking for an existing accepted ownership declaration.
    """

    family: NativeNamingTarget
    entries: tuple[AcceptedNamingEntry, ...]
    candidate_columns: tuple[tuple[str, str], ...]
    reason: str

    @property
    def names(self) -> tuple[SlugEntry, ...]:
        grouped: defaultdict[str, list[AcceptedNamingEntry]] = defaultdict(list)
        for entry in self.entries:
            grouped[entry.entry.source_id].append(entry)
        return tuple(_effective(grouped[key])[0] for key in sorted(grouped))

    @model_validator(mode="after")
    def _bounded(self) -> Self:
        if (
            self.family.kind != "variable"
            or not self.family.expectations
            or not self.family.peer_guards
        ):
            raise ValueError(
                "naming ambiguity needs a complete original variable family"
            )
        if not self.entries or not self.reason.strip():
            raise ValueError("naming ambiguity needs original entries and a reason")
        if any(
            projection.subject is None
            or "column_name" not in {field.name for field in projection.fields}
            for expectation in self.family.expectations
            for projection in expectation.alternatives
        ):
            raise ValueError(
                "naming ambiguity must pin every original subject and column"
            )
        origins = set()
        for item in self.entries:
            if (
                item.entry.kind != "variable"
                or item.entry.provider != self.family.provider
            ):
                raise ValueError(
                    "ambiguous names must share the source family's namespace"
                )
            key = item.entry.source_id, item.origin
            if key in origins:
                raise ValueError("duplicate ambiguous naming origin")
            origins.add(key)
        names = self.names
        if any(not name.slug for name in names) or len({n.slug for n in names}) != len(
            names
        ):
            raise ValueError("ambiguous names need distinct effective slugs")
        if (
            {key for key, _ in self.candidate_columns} != {n.source_id for n in names}
            or len(set(self.candidate_columns)) != len(self.candidate_columns)
            or any(not column.strip() for _, column in self.candidate_columns)
        ):
            raise ValueError(
                "each ambiguous name needs unique literal column candidates"
            )
        observed = {
            field.value
            for expectation in self.family.expectations
            for projection in expectation.alternatives
            for field in projection.fields
            if field.name == "column_name" and field.status == "value"
        }
        if any(column not in observed for _, column in self.candidate_columns):
            raise ValueError(
                "ambiguous name candidates must occur in the original family"
            )
        return self


class NamingEntryDisposition(_NamingModel):
    entry_id: str
    status: Literal[
        "bound", "shadowed", "inactive_generated", "pending_source_binding", "blocked"
    ]
    detail: str


class NamingConversion(_NamingModel):
    declarations: tuple[NamingDeclaration, ...]
    dispositions: tuple[NamingEntryDisposition, ...]
    diagnostics: tuple[ResolutionDiagnostic, ...]

    @property
    def complete(self) -> bool:
        return not self.diagnostics


def _validate_legacy_key(kind: EntityKind, source_id: str) -> None:
    if kind == "register":
        _parse_register_id(source_id)
    elif kind == "register_variant":
        _parse_variant_id(source_id)
    elif kind == "variable":
        _parse_variable_id(source_id)
    elif not source_id:
        raise ValueError("classification naming requires an exact source key")


def native_scb_naming_id(
    kind: Literal["register", "register_variant", "variable"],
    register_id: int,
    member_id: int | None = None,
) -> str:
    """Translate literal native SCB IDs into the accepted naming key grammar."""
    if type(register_id) is not int or register_id < 0:
        raise NamingConversionError("SCB register ID must be a nonnegative integer")
    if kind == "register":
        if member_id is not None:
            raise NamingConversionError("register naming cannot take a member ID")
        return str(register_id)
    if type(member_id) is not int or member_id < 0:
        raise NamingConversionError(
            "SCB variant/variable ID must be a nonnegative integer"
        )
    return f"{register_id}.{member_id}"


def authored_naming_id(
    kind: Literal["register", "register_variant", "variable"],
    *,
    provider: str,
    register_key: str,
    member_key: str | None = None,
    canonical_scb: bool = False,
) -> str:
    """Reproduce existing global SOS/thin keys from explicit source coordinates.

    SOS register_key is the delivered filename abbreviation and member_key is the
    delivered subset/variable token. Thin providers use their authored register,
    variant and variable keys. Neither abbreviation inference nor synthesized
    default variants nor split discriminators are performed here.
    """
    if not provider or not register_key:
        raise NamingConversionError("provider and register key must be explicit")
    if canonical_scb != (provider == "scb"):
        raise NamingConversionError(
            "canonical SCB hashing must be explicit for scb only"
        )
    mint_id = mint_canonical_scb if canonical_scb else mint
    register_id = mint_id(provider, register_key)
    if kind == "register":
        if member_key is not None:
            raise NamingConversionError("register naming cannot take a member key")
        return str(register_id)
    if not member_key:
        raise NamingConversionError("variant/variable naming requires its explicit key")
    if kind == "register_variant":
        return f"{register_id}.{mint_id(provider, register_key, member_key)}"
    source_id = f"{register_id}.{member_key}"
    _parse_variable_id(source_id)
    if "." in member_key:
        raise NamingConversionError("authored variable key cannot imply a split suffix")
    return source_id


def read_naming_selection(
    slug_dir: Path, revisions: Iterable[SourceRevision]
) -> NamingSelection:
    """Read exactly pinned naming files, retaining original per-entry provenance."""
    revisions = tuple(revisions)
    pins = {Path(revision.artifact_path).name: revision for revision in revisions}
    if len(pins) != len(revisions):
        raise NamingConversionError("naming input filenames must be unique")
    paths = tuple(sorted(slug_dir.iterdir()))
    if {path.name for path in paths} != set(pins):
        raise NamingConversionError(
            "naming directory and selected file inventory differ"
        )
    if any(path.is_symlink() or not path.is_file() for path in paths):
        raise NamingConversionError("naming inputs must be regular files")
    payloads = {
        path.name: read_selected_bytes(
            path, pins[path.name], error_type=NamingConversionError
        )
        for path in paths
    }
    states = load_freeze_states(slug_dir)
    entries: list[AcceptedNamingEntry] = []
    files: list[NamingInputFile] = []
    for path in paths:
        revision = pins[path.name]
        payload = payloads[path.name]
        if path.name == FREEZE_STATE_FILE:
            files.append(
                NamingInputFile(
                    revision=revision, role="control", entry_count=len(states)
                )
            )
            continue
        if path.name == SNAPSHOT_FILENAME:
            try:
                snapshot = json.loads(payload)
                if not isinstance(snapshot, dict) or any(
                    not isinstance(table, dict)
                    or any(not isinstance(value, str) for value in table.values())
                    for table in snapshot.values()
                ):
                    raise ValueError("expected tables of source keys and slug strings")
            except (ValueError, UnicodeDecodeError) as exc:
                raise NamingConversionError(f"invalid naming snapshot: {exc}") from exc
            files.append(
                NamingInputFile(
                    revision=revision,
                    role="comparison",
                    entry_count=sum(map(len, snapshot.values())),
                )
            )
            continue
        if path.suffix != ".toml":
            raise NamingConversionError(f"unsupported naming input: {path.name}")
        loaded = (
            load_classifications_toml(path)
            if path.name == CLASSIFICATIONS_FILE
            else load_provider_toml(path)
        )
        raw = tomllib.loads(payload.decode("utf-8"))
        origin = "generated" if path.name.endswith(AUTO_FILE_SUFFIX) else "authored"
        for entry in loaded:
            supplied = raw[entry.kind][entry.source_id]
            entries.append(
                AcceptedNamingEntry(
                    revision=revision,
                    origin=origin,
                    entry=entry,
                    supplied_fields=tuple(sorted(supplied)),
                    content_sha256=canonical_sha256(supplied),
                )
            )
        files.append(
            NamingInputFile(revision=revision, role=origin, entry_count=len(loaded))
        )
    # The existing pure validators read paths; recheck pins after them so an input
    # update during conversion cannot publish a selection from mixed revisions.
    for path in paths:
        read_selected_bytes(path, pins[path.name], error_type=NamingConversionError)
    return NamingSelection(
        files=tuple(files),
        entries=tuple(entries),
        freeze=tuple(
            NamingFreezeSetting(zone=zone, state=state)
            for zone, state in sorted(states.items())
        ),
    )


def _key(entry: SlugEntry) -> NamingKey:
    return entry.kind, entry.provider, entry.source_id


def _target_key(target: NativeNamingTarget) -> tuple[EntityKind, str | None, NativeKey]:
    return target.kind, target.provider, target.source_key


def _effective(
    entries: list[AcceptedNamingEntry],
) -> tuple[SlugEntry, tuple[AcceptedNamingEntry, ...]]:
    ordered = tuple(
        sorted(entries, key=lambda item: (item.origin == "authored", item.entry_id))
    )
    effective = asdict(ordered[0].entry)
    for item in ordered[1:]:
        for name in item.supplied_fields:
            effective[name] = getattr(item.entry, name)
    return SlugEntry(**effective), ordered


def convert_naming(
    selection: NamingSelection, bindings: Iterable[LegacyNamingBinding]
) -> NamingConversion:
    """Convert existing intent only; missing/ambiguous bridges remain explicit.

    This operation does not scan source records. Check each unique target against
    its complete native family with check_naming_target before forming output.
    """
    entries_by_key: defaultdict[NamingKey, list[AcceptedNamingEntry]] = defaultdict(
        list
    )
    targets_by_key: defaultdict[NamingKey, list[NativeNamingTarget]] = defaultdict(list)
    dispositions: dict[str, NamingEntryDisposition] = {}
    diagnostics: list[ResolutionDiagnostic] = []
    states = {item.zone: item.state for item in selection.freeze}
    for entry in selection.entries:
        if entry.entry_id in dispositions:
            raise NamingConversionError(f"duplicate naming entry: {entry.entry_id}")
        if (
            entry.origin == "generated"
            and states.get(entry.entry.provider or "classifications", "churning")
            == "churning"
        ):
            dispositions[entry.entry_id] = NamingEntryDisposition(
                entry_id=entry.entry_id,
                status="inactive_generated",
                detail="generated file is comparison-only while this zone is churning",
            )
        else:
            # Placeholders also detect duplicate active IDs before conversion.
            dispositions[entry.entry_id] = NamingEntryDisposition(
                entry_id=entry.entry_id,
                status="pending_source_binding",
                detail="exact native source binding is not supplied",
            )
            entries_by_key[_key(entry.entry)].append(entry)
    for binding in bindings:
        key = (binding.kind, binding.provider, binding.source_id)
        if binding.target not in targets_by_key[key]:
            targets_by_key[key].append(binding.target)
    candidates: list[NamingDeclaration] = []
    for key, entries in entries_by_key.items():
        targets = targets_by_key[key]
        if len(targets) != 1:
            code = (
                "naming_binding_ambiguous"
                if targets
                else "naming_source_binding_pending"
            )
            detail = (
                "multiple nonidentical exact bindings supplied"
                if targets
                else (
                    "exact split-member binding requires conversion of the accepted split decision"
                    if key[0] == "variable" and len(key[2].split(".")) == 3
                    else "exact native binding is pending implementation or source accounting; no label match was attempted"
                )
            )
            diagnostics.append(
                ResolutionDiagnostic(
                    code=code,
                    severity="error",
                    subject="/".join(str(part) for part in key),
                    detail=detail,
                )
            )
            for entry in entries:
                dispositions[entry.entry_id] = NamingEntryDisposition(
                    entry_id=entry.entry_id,
                    status="blocked" if targets else "pending_source_binding",
                    detail=detail,
                )
            continue
        if len({entry.origin for entry in entries}) != len(entries):
            raise NamingConversionError(f"duplicate origin for naming key {key!r}")
        effective, contributors = _effective(entries)
        candidates.append(
            NamingDeclaration(
                target=targets[0], naming=effective, contributors=contributors
            )
        )
        for entry in contributors:
            shadowed = entry.origin == "generated" and any(
                peer.origin == "authored" and peer.entry.slug is not None
                for peer in contributors
            )
            dispositions[entry.entry_id] = NamingEntryDisposition(
                entry_id=entry.entry_id,
                status="shadowed" if shadowed else "bound",
                detail="authored slug supersedes generated pin"
                if shadowed
                else "bound to an exact guarded native identity",
            )

    by_target: defaultdict[
        tuple[EntityKind, str | None, NativeKey], list[NamingDeclaration]
    ] = defaultdict(list)
    for declaration in candidates:
        by_target[_target_key(declaration.target)].append(declaration)
    candidates = []
    for same_target in by_target.values():
        first = same_target[0]
        first_effect = asdict(first.naming) | {"source_id": ""}
        if any(
            item.target != first.target
            or (asdict(item.naming) | {"source_id": ""}) != first_effect
            for item in same_target[1:]
        ):
            diagnostics.append(
                ResolutionDiagnostic(
                    code="naming_target_multiple_bindings",
                    severity="error",
                    subject=repr(first.target.source_key),
                    detail="nonidentical accepted names or guards bind one native identity",
                )
            )
            for item in same_target:
                for entry in item.contributors:
                    dispositions[entry.entry_id] = NamingEntryDisposition(
                        entry_id=entry.entry_id,
                        status="blocked",
                        detail="nonidentical naming assignments for one native identity",
                    )
        else:
            candidates.append(
                NamingDeclaration(
                    target=first.target,
                    naming=first.naming,
                    contributors=tuple(
                        entry for item in same_target for entry in item.contributors
                    ),
                )
            )
    by_slug: defaultdict[
        tuple[EntityKind, str | None, NativeKey | None, str], list[int]
    ] = defaultdict(list)
    for index, declaration in enumerate(candidates):
        if declaration.naming.slug is not None:
            by_slug[
                (
                    declaration.target.kind,
                    declaration.target.provider,
                    declaration.target.register_key,
                    declaration.naming.slug,
                )
            ].append(index)
    blocked: set[int] = set()
    for indices in by_slug.values():
        if len({_target_key(candidates[index].target) for index in indices}) > 1:
            blocked.update(indices)
            diagnostics.append(
                ResolutionDiagnostic(
                    code="naming_slug_collision",
                    severity="error",
                    subject=candidates[indices[0]].naming.slug or "",
                    detail="accepted slug is used by distinct native identities in the same FQID scope",
                )
            )
    for index in blocked:
        for entry in candidates[index].contributors:
            dispositions[entry.entry_id] = NamingEntryDisposition(
                entry_id=entry.entry_id,
                status="blocked",
                detail="native target or FQID-scope collision",
            )
    return NamingConversion(
        declarations=tuple(
            item for index, item in enumerate(candidates) if index not in blocked
        ),
        dispositions=tuple(dispositions[item.entry_id] for item in selection.entries),
        diagnostics=tuple(diagnostics),
    )


def check_naming_target(
    target: NativeNamingTarget,
    records: Iterable[SourceRecord],
    *,
    revisions: Iterable[SourceRevision] = (),
) -> tuple[ResolutionDiagnostic, ...]:
    """Check naming applicability against its original source evidence.

    Call once per unique target, not once per entry or over the full source corpus.
    When membership guards are present, include new members of this native identity,
    not only its previously pinned members. Parent identity anchors without membership
    guards need only their exact referenced records. Independent source identities are
    never peers by name resemblance.
    """
    if target.identity_revision is not None:
        if target.identity_revision in revisions:
            return ()
        return (
            ResolutionDiagnostic(
                code="naming_identity_revision_changed",
                severity="error",
                subject=repr(target.source_key),
                detail="the selected source declaration revision no longer matches its naming binding",
            ),
        )
    if target.kind == "variable" and not target.expectations and not target.peer_guards:
        evidence = (
            records if isinstance(records, SourceEvidence) else SourceEvidence(records)
        )
        if evidence.native_variable_anchors.get(target.source_key) == (
            target.provider,
            target.register_key,
        ):
            return ()
        return (
            ResolutionDiagnostic(
                code="naming_native_identity_missing",
                severity="error",
                subject=repr(target.source_key),
                detail="Naming requires this exact existing native variable in its declared provider and register; curated partitions need their ownership guards.",
            ),
        )
    return tuple(
        ResolutionDiagnostic(
            code="naming_source_expectation_stale",
            severity="error",
            subject=repr(target.source_key),
            detail=issue.detail,
            applicability_issue=issue,
        )
        for issue in evaluate_source_expectations(
            target.expectations, (), target.peer_guards, records
        )
    )
