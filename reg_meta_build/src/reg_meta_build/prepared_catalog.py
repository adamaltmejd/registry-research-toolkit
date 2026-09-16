"""Prepare all selected machine sources once; reopen their accepted proofs cheaply.

The bundle remains the inventory authority. Curation and naming files are pinned
but deliberately excluded from cleaning. No catalog identities are formed here.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

from reg_meta_build._accepted_prepared import (
    check_accepted_files,
    read_accepted_manifest,
)
from reg_meta_build.db import _CURATED_PROVIDERS, _file_sha256
from reg_meta_build.input_snapshot import (
    CATALOG_CURATION_FILES,
    LISA_BUNDLE_PATH,
    SCB_CSV_FILES,
    CatalogBundleReader,
    CatalogBundleSelection,
    SnapshotFile,
    _decode_cell,
    _git,
    _index_tags,
    open_input_bundle,
)
from reg_meta_build.prepared_sources import (
    _COMMIT_RE,
    _HASH_RE,
    PreparedSourceManifest,
    PreparedSourceRecords,
    _json,
    _manifest as _record_manifest,
    prepare_source_records,
)
from reg_meta_build.prepared_values import (
    PreparedSourceValues,
    PreparedValueManifest,
    _manifest as _value_manifest,
    prepare_source_values,
)
from reg_meta_build.source_records import (
    SourceEvidenceTable,
    SourceRecord,
    SourceRevision,
)
from reg_meta_build.source_reference_records import (
    SourceReferenceDeclaration,  # noqa: TC001
)
from reg_meta_build.source_support import SourceSupportJoin  # noqa: TC001
from reg_meta_build.source_values import SourceValueJoin
from reg_meta_build.sources.code_lists import read_code_list, read_selected_bytes
from reg_meta_build.sources.curated_records import read_curated_source
from reg_meta_build.sources.lisa import read_lisa_source
from reg_meta_build.sources.scb_auxiliary import (
    iter_scb_auxiliary_records,
    scb_support_joins,
)
from reg_meta_build.sources.scb_records import (
    ScbInterpretationIssue,
    iter_scb_observations,
)
from reg_meta_build.sources.scb_reference_records import (
    read_scb_column_types,
    read_scb_events,
    read_scb_join_keys,
)
from reg_meta_build.sources.scb_values import CleanedScbValues, clean_scb_values
from reg_meta_build.sources.sos import parse_register_file
from reg_meta_build.sources.sos_records import clean_sos_source

if TYPE_CHECKING:
    from collections.abc import Iterator

    from reg_meta_build.source_values import SourceValueValidity
    from reg_meta_build.sources.code_lists import CleanedCodeList
    from reg_meta_build.sources.sos_records import CleanedSosSource

_RECORDS = "files/records"
_EVIDENCE = "files/evidence.jsonl"
_THIN = {
    f"catalog/{directory}/{provider}.toml": provider
    for provider, directory in _CURATED_PROVIDERS
} | {"catalog/scb_canonical/scb_canonical.toml": "scb"}


class PreparedCatalogError(ValueError):
    """The complete source selection cannot be faithfully prepared or opened."""


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class SourceCounts(_Model):
    records: int = Field(default=0, ge=0)
    tables: int = Field(default=0, ge=0)
    table_rows: int = Field(default=0, ge=0)
    declarations: int = Field(default=0, ge=0)
    descriptors: int = Field(default=0, ge=0)
    values: int = Field(default=0, ge=0)
    associations: int = Field(default=0, ge=0)
    validity: int = Field(default=0, ge=0)
    interpretation_issues: int = Field(default=0, ge=0)
    context_entries: int = Field(default=0, ge=0)
    headers: int = Field(default=0, ge=0)


type InputRole = Literal[
    "scb_records",
    "scb_auxiliary",
    "scb_events",
    "scb_values",
    "scb_validity",
    "column_types",
    "join_keys",
    "sos_workbook",
    "lisa_workbook",
    "thin_provider",
    "code_list",
    "curation",
    "naming",
]


class PreparedInputAccounting(_Model):
    origin: Literal["snapshot", "bundle"]
    path: str
    role: InputRole
    record_usage: Literal["occurrence", "field_support", "support", "none"]
    present: bool
    disposition: Literal["prepared", "absent", "excluded_curation", "excluded_naming"]
    revision: SourceRevision | None
    counts: SourceCounts = SourceCounts()
    exclusion_reason: str | None = None

    @model_validator(mode="after")
    def _coherent(self) -> Self:
        if self.role != _role(self.path, origin=self.origin):
            raise ValueError("input role differs from its source path")
        if self.present != (self.revision is not None):
            raise ValueError(
                "present inputs require exact revisions; absent inputs cannot have one"
            )
        expected = (
            "absent"
            if not self.present
            else "excluded_curation"
            if self.role == "curation"
            else "excluded_naming"
            if self.role == "naming"
            else "prepared"
        )
        if self.disposition != expected:
            raise ValueError("input disposition differs from its role/presence")
        if self.record_usage != _record_usage(self.role, self.present):
            raise ValueError("record usage differs from its source format/presence")
        if self.disposition != "prepared" and self.counts != SourceCounts():
            raise ValueError("unprepared inputs cannot claim output counts")
        if self.disposition.startswith("excluded_") and not self.exclusion_reason:
            raise ValueError("excluded input requires an explicit reason")
        return self


class _FileProof(_Model):
    path: str
    size: int = Field(ge=0)
    sha256: str
    git_blob: str

    @model_validator(mode="after")
    def _valid(self) -> Self:
        path = Path(self.path)
        if (
            path.is_absolute()
            or ".." in path.parts
            or len(path.parts) < 2
            or path.parts[0] != "files"
            or path.as_posix() != self.path
        ):
            raise ValueError("prepared member must be a normalized files/ path")
        if not _HASH_RE.fullmatch(self.sha256) or not _COMMIT_RE.fullmatch(
            self.git_blob
        ):
            raise ValueError("invalid prepared file hashes")
        return self


class _ValueChild(_Model):
    path: str
    revision_id: str
    manifest_sha256: str


class _ManifestDocument(_Model):
    format: Literal["reg-meta-prepared-catalog-sources"] = (
        "reg-meta-prepared-catalog-sources"
    )
    schema_version: Literal[2] = 2
    input_commit: str
    bundle_path: str
    bundle_manifest_sha256: str
    snapshot_path: str
    snapshot_manifest_sha256: str
    inputs: tuple[PreparedInputAccounting, ...]
    records_manifest_sha256: str
    values: tuple[_ValueChild, ...]
    support_joins: tuple[SourceSupportJoin, ...]
    evidence_count: int = Field(ge=0)
    files: tuple[_FileProof, ...]

    @model_validator(mode="after")
    def _layout(self) -> Self:
        if not _COMMIT_RE.fullmatch(self.input_commit) or not all(
            _HASH_RE.fullmatch(value)
            for value in (
                self.bundle_manifest_sha256,
                self.snapshot_manifest_sha256,
                self.records_manifest_sha256,
                *(child.manifest_sha256 for child in self.values),
            )
        ):
            raise ValueError("invalid source selection pins")
        keys = [(item.origin, item.path) for item in self.inputs]
        if keys != sorted(set(keys)):
            raise ValueError("input accounting must be unique and ordered")
        if {item.path for item in self.inputs if item.origin == "snapshot"} != set(
            SCB_CSV_FILES
        ):
            raise ValueError(
                "input accounting must retain every SCB role including absent files"
            )
        expected_evidence = sum(
            item.counts.headers
            + item.counts.declarations
            + item.counts.interpretation_issues
            + int(item.role == "lisa_workbook" and item.present)
            for item in self.inputs
        )
        if self.evidence_count != expected_evidence:
            raise ValueError("auxiliary evidence count differs from input accounting")
        revisions = {item.revision.revision_id for item in self.inputs if item.revision}
        support_sources = {
            item.revision.dataset
            for item in self.inputs
            if item.revision and item.record_usage == "field_support"
        }
        occurrence_sources = {
            item.revision.dataset
            for item in self.inputs
            if item.revision and item.record_usage == "occurrence"
        }
        if (
            len({join.source for join in self.support_joins}) != len(self.support_joins)
            or {join.source for join in self.support_joins} != support_sources
        ):
            raise ValueError(
                "support relationships must cover each selected support source once"
            )
        if any(
            not set(join.target_sources) <= occurrence_sources
            for join in self.support_joins
        ):
            raise ValueError(
                "support relationship targets an undeclared occurrence source"
            )
        child_paths = [child.path for child in self.values]
        if len(set(child_paths)) != len(child_paths):
            raise ValueError("duplicate value child")
        expected = {
            _EVIDENCE,
            f"{_RECORDS}/manifest.json",
            f"{_RECORDS}/files/records.sqlite",
        }
        for child in self.values:
            if child.revision_id not in revisions or child.path != _value_path(
                child.revision_id
            ):
                raise ValueError("value child must identify a selected source revision")
            expected.update(
                f"{child.path}/{name}"
                for name in (
                    "manifest.json",
                    "files/values.sqlite",
                    "files/associations.bin",
                    "files/member-positions.bin",
                )
            )
        paths = [item.path for item in self.files]
        if paths != sorted(expected):
            raise ValueError(
                "prepared artifact inventory differs from supported layout"
            )
        proofs = {item.path: item.sha256 for item in self.files}
        child_hashes = {
            f"{_RECORDS}/manifest.json": self.records_manifest_sha256,
            **{
                f"{child.path}/manifest.json": child.manifest_sha256
                for child in self.values
            },
        }
        if any(proofs[path] != digest for path, digest in child_hashes.items()):
            raise ValueError("child manifest SHA-256 differs from its outer proof")
        return self


class PreparedCatalogManifest(_ManifestDocument):
    artifact_sha256: str

    @property
    def sha256(self) -> str:
        return self.artifact_sha256


class ReferenceEvidence(_Model):
    kind: Literal["reference"] = "reference"
    revision_id: str
    declaration: SourceReferenceDeclaration


class HeaderEvidence(_Model):
    kind: Literal["header"] = "header"
    revision_id: str
    cells: tuple[str | None, ...]


class WorksheetContextEvidence(_Model):
    kind: Literal["worksheet_context"] = "worksheet_context"
    revision_id: str
    entries: tuple[str, ...]


class InterpretationEvidence(_Model):
    kind: Literal["interpretation"] = "interpretation"
    revision_id: str
    issue: ScbInterpretationIssue


type PreparedEvidence = Annotated[
    ReferenceEvidence
    | HeaderEvidence
    | WorksheetContextEvidence
    | InterpretationEvidence,
    Field(discriminator="kind"),
]
_EVIDENCE_ADAPTER = TypeAdapter(PreparedEvidence)


def _manifest(payload: bytes) -> PreparedCatalogManifest:
    document = _ManifestDocument.model_validate_json(payload)
    return PreparedCatalogManifest(
        **document.model_dump(), artifact_sha256=hashlib.sha256(payload).hexdigest()
    )


def _value_path(revision_id: str) -> str:
    return "files/values/" + hashlib.sha256(revision_id.encode()).hexdigest()


def _role(path: str, *, origin: str) -> InputRole:
    if origin == "snapshot":
        roles: dict[str, InputRole] = {
            "Registerinformation.csv": "scb_records",
            "UnikaRegisterOchVariabler.csv": "scb_auxiliary",
            "Identifierare.csv": "scb_auxiliary",
            "Timeseries.csv": "scb_events",
            "Vardemangder.csv": "scb_values",
            "VardemangderValidDates.csv": "scb_validity",
        }
        if path in roles:
            return roles[path]
    elif path in _THIN:
        return "thin_provider"
    elif path == "catalog/SCB/Tabelldefinitioner.sql":
        return "column_types"
    elif path == "catalog/SCB/ID-kolumner.xlsx":
        return "join_keys"
    elif path == LISA_BUNDLE_PATH:
        return "lisa_workbook"
    elif path.startswith("catalog/Socialstyrelsen/") and Path(path).suffix == ".xlsx":
        return "sos_workbook"
    elif (
        Path(path).parent.as_posix() == "catalog/scb_canonical"
        or Path(path).is_relative_to("catalog/classifications")
    ) and Path(path).suffix == ".csv":
        return "code_list"
    elif path in {f"curation/{name}" for name in CATALOG_CURATION_FILES}:
        return "curation"
    elif Path(path).parent.as_posix() == "fqid_slugs" and (
        path.endswith(".toml") or path == "fqid_slugs/.snapshot.json"
    ):
        return "naming"
    raise PreparedCatalogError(
        f"no stage-1 input role for {origin}:{path}; update its reader/selection explicitly"
    )


def _record_usage(
    role: InputRole, present: bool
) -> Literal["occurrence", "field_support", "support", "none"]:
    """Stage 1 declares how its records may enter the common resolver."""
    if not present:
        return "none"
    if role in {"scb_records", "sos_workbook", "thin_provider"}:
        return "occurrence"
    if role == "scb_auxiliary":
        return "field_support"
    if role == "lisa_workbook":
        return "support"
    return "none"


def _inventory(bundle: CatalogBundleReader) -> tuple[PreparedInputAccounting, ...]:
    entries = []
    if {item.name for item in bundle.snapshot.manifest.files} != set(SCB_CSV_FILES):
        raise PreparedCatalogError(
            "SCB snapshot must account for exactly the six supported CSV roles"
        )
    for origin, files in (
        ("snapshot", bundle.snapshot.manifest.files),
        ("bundle", bundle.manifest.files),
    ):
        for item in files:
            path = item.name if isinstance(item, SnapshotFile) else item.path
            role = _role(path, origin=origin)
            revision = None
            if item.present:
                if isinstance(item, SnapshotFile):
                    size, digest = item.raw_size, item.raw_sha256
                    artifact = f"{bundle.manifest.scb_snapshot_path}:source/{path}"
                    dataset = f"scb-{Path(path).stem.lower()}"
                    publisher, upstream = "SCB", bundle.snapshot.manifest.edition
                    purpose = "Lossless provider machine-source observations"
                else:
                    size, digest, artifact = item.size, item.sha256, path
                    dataset = path.removeprefix("catalog/")
                    publisher, upstream = Path(path).parts[1], bundle.manifest.edition
                    purpose = "Selected machine-readable source declarations"
                    if role == "lisa_workbook":
                        dataset_info = bundle.manifest.supplemental_datasets[0]
                        dataset, publisher = (
                            dataset_info.dataset,
                            dataset_info.publisher,
                        )
                        upstream, purpose = (
                            dataset_info.upstream_revision,
                            dataset_info.purpose,
                        )
                assert size is not None and digest is not None and upstream is not None
                revision = SourceRevision.create(
                    dataset=dataset,
                    publisher=publisher,
                    purpose=purpose,
                    upstream_revision=upstream,
                    artifact_path=artifact,
                    artifact_size=size,
                    artifact_sha256=digest,
                )
            disposition = (
                "absent"
                if not item.present
                else "excluded_curation"
                if role == "curation"
                else "excluded_naming"
                if role == "naming"
                else "prepared"
            )
            exclusion = (
                "Decision declarations are pinned for stage 2; they are not provider observations."
                if role == "curation"
                else "Catalog identity declarations are pinned for stage 2; stage 1 does not assign FQIDs."
                if role == "naming"
                else None
            )
            if role == "lisa_workbook" and not item.present:
                exclusion = bundle.manifest.supplemental_datasets[0].exclusion_reason
            entries.append(
                PreparedInputAccounting(
                    origin=origin,
                    path=path,
                    role=role,
                    record_usage=_record_usage(role, item.present),
                    present=item.present,
                    disposition=disposition,
                    revision=revision,
                    exclusion_reason=exclusion,
                )
            )
    return tuple(sorted(entries, key=lambda entry: (entry.origin, entry.path)))


def prepare_catalog_sources(
    selection: CatalogBundleSelection, output: Path
) -> PreparedCatalogManifest:
    """Validate every selected input and write a new, not-yet-accepted candidate."""
    output = output.expanduser().absolute()
    if output.exists() or output.is_symlink():
        raise PreparedCatalogError(f"prepared catalog output already exists: {output}")
    bundle = open_input_bundle(selection)
    if any(
        output.resolve().is_relative_to(root)
        for root in (bundle.root, bundle.snapshot.root)
    ):
        raise PreparedCatalogError(
            "prepared output must be outside the selected source directories"
        )
    inventory = _inventory(bundle)
    selected_roots = (
        bundle.root.relative_to(bundle.repository),
        bundle.snapshot.root.relative_to(bundle.repository),
    )
    selected_index_tags = {
        path: tag
        for path, tag in _index_tags(bundle.repository).items()
        if any(Path(path).is_relative_to(root) for root in selected_roots)
    }
    if bundle.snapshot.has_file("Vardemangder.csv"):
        bundle.snapshot.require_vardemangder_materialized()
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}.", dir=output.parent))
    try:
        (staging / "files").mkdir()
        entries = {(entry.origin, entry.path): entry for entry in inventory}
        counters = {key: entry.counts.model_dump() for key, entry in entries.items()}
        revisions = tuple(
            entry.revision
            for entry in inventory
            if entry.revision and entry.disposition == "prepared"
        )
        children: list[_ValueChild] = []
        value_manifests: dict[str, PreparedValueManifest] = {}
        evidence_count = 0
        spool = staging / "tables.jsonl"

        with (
            spool.open("w+", encoding="utf-8") as table_file,
            (staging / _EVIDENCE).open("w", encoding="utf-8") as evidence_file,
        ):

            def evidence(value: PreparedEvidence) -> None:
                nonlocal evidence_count
                checked = _EVIDENCE_ADAPTER.validate_json(
                    _EVIDENCE_ADAPTER.dump_json(value)
                )
                evidence_file.write(
                    _EVIDENCE_ADAPTER.dump_json(checked).decode() + "\n"
                )
                evidence_count += 1

            def tables(
                items: tuple[SourceEvidenceTable, ...], count: dict[str, int]
            ) -> None:
                for table in items:
                    table_file.write(table.model_dump_json() + "\n")
                    count["tables"] += 1
                    count["table_rows"] += len(table.rows)

            def references(
                items: tuple[SourceReferenceDeclaration, ...],
                revision: SourceRevision,
                count: dict[str, int],
            ) -> None:
                for declaration in items:
                    if declaration.revision != revision:
                        raise PreparedCatalogError(
                            "reference declaration has an unselected revision"
                        )
                    evidence(
                        ReferenceEvidence(
                            revision_id=revision.revision_id, declaration=declaration
                        )
                    )
                    count["declarations"] += 1

            def values(
                revision: SourceRevision,
                cleaned: CleanedCodeList | CleanedScbValues | CleanedSosSource,
                count: dict[str, int],
                *,
                validity: tuple[SourceValueValidity, ...] = (),
                validity_revision: SourceRevision | None = None,
                join: SourceValueJoin | None = None,
            ) -> None:
                path = _value_path(revision.revision_id)
                manifest = prepare_source_values(
                    staging / path,
                    revision=revision,
                    descriptors=cleaned.descriptors.values(),
                    values=cleaned.values.values(),
                    associations=cleaned.associations()
                    if isinstance(cleaned, CleanedScbValues)
                    else cleaned.associations,
                    validity=validity,
                    validity_revision=validity_revision,
                    join=join,
                )
                children.append(
                    _ValueChild(
                        path=path,
                        revision_id=revision.revision_id,
                        manifest_sha256=manifest.sha256,
                    )
                )
                value_manifests[path] = manifest
                for key in ("descriptors", "values", "associations"):
                    count[key] = getattr(
                        manifest,
                        {
                            "descriptors": "descriptor_count",
                            "values": "value_count",
                            "associations": "association_count",
                        }[key],
                    )

            def records() -> Iterator[SourceRecord]:
                for entry in inventory:
                    revision = entry.revision
                    if entry.disposition != "prepared" or revision is None:
                        continue
                    count = counters[(entry.origin, entry.path)]
                    if entry.origin == "snapshot":
                        item = next(
                            item
                            for item in bundle.snapshot.manifest.files
                            if item.name == entry.path
                        )
                        evidence(
                            HeaderEvidence(
                                revision_id=revision.revision_id,
                                cells=tuple(map(_decode_cell, item.header)),
                            )
                        )
                        count["headers"] += 1
                    if entry.role == "scb_records":
                        for observation in iter_scb_observations(
                            bundle.snapshot, revision
                        ):
                            if observation.issue:
                                evidence(
                                    InterpretationEvidence(
                                        revision_id=revision.revision_id,
                                        issue=observation.issue,
                                    )
                                )
                                count["interpretation_issues"] += 1
                            count["records"] += 1
                            yield observation.record
                    elif entry.role == "scb_auxiliary":
                        for record in iter_scb_auxiliary_records(
                            bundle.snapshot, entry.path, revision
                        ):
                            count["records"] += 1
                            yield record
                    elif entry.role == "scb_events":
                        result = read_scb_events(bundle.snapshot, revision)
                        references(result.declarations, revision, count)
                        tables(result.tables, count)
                    elif entry.role == "scb_values":
                        result = clean_scb_values(bundle.snapshot)
                        validity_entry = entries[
                            ("snapshot", "VardemangderValidDates.csv")
                        ]
                        record_revision = entries[
                            ("snapshot", "Registerinformation.csv")
                        ].revision
                        if record_revision is None:
                            raise PreparedCatalogError(
                                "value source requires its declared member source"
                            )
                        values(
                            revision,
                            result,
                            count,
                            validity=result.validity,
                            validity_revision=validity_entry.revision,
                            join=SourceValueJoin(
                                record_sources=(record_revision.dataset,),
                                member_target="native_member",
                                member_format="integer",
                                validity_target="item",
                                missing_validity="unrestricted"
                                if validity_entry.present
                                else "unknown",
                                rule="Integer member identifiers join source member IDs; item IDs join the complete validity table. Listed blank bounds are open; an unlisted item has no additional restriction only when that table is present.",
                                provenance=(
                                    revision.artifact_path,
                                    validity_entry.revision.artifact_path
                                    if validity_entry.revision
                                    else "validity-source:declared-absent",
                                ),
                            ),
                        )
                        counters[("snapshot", "VardemangderValidDates.csv")][
                            "validity"
                        ] = len(result.validity)
                    elif entry.role == "scb_validity":
                        # The accepted snapshot requires the two value files as
                        # a pair. The value reader prepares both in one pass.
                        if not bundle.snapshot.has_file("Vardemangder.csv"):
                            raise PreparedCatalogError(
                                "selected validity source lacks its value source"
                            )
                    else:
                        path = bundle.root / entry.path
                        if entry.role in {"column_types", "join_keys"}:
                            result = (
                                read_scb_column_types
                                if entry.role == "column_types"
                                else read_scb_join_keys
                            )(path, revision)
                            references(result.declarations, revision, count)
                            tables(result.tables, count)
                        elif entry.role == "lisa_workbook":
                            result = read_lisa_source(path, revision)
                            evidence(
                                WorksheetContextEvidence(
                                    revision_id=revision.revision_id,
                                    entries=result.worksheet_context,
                                )
                            )
                            count["context_entries"] = len(result.worksheet_context)
                            count["records"] = len(result.records)
                            yield from result.records
                        elif entry.role == "sos_workbook":
                            read_selected_bytes(path, revision)
                            result = clean_sos_source(
                                parse_register_file(path), revision
                            )
                            tables(result.tables, count)
                            references(result.declarations, revision, count)
                            values(
                                revision,
                                result,
                                count,
                                validity=result.validity,
                                validity_revision=revision,
                                join=SourceValueJoin(
                                    record_sources=(revision.dataset,),
                                    member_target="member_name",
                                    member_format="none",
                                    validity_target="row",
                                    missing_validity="unrestricted",
                                    rule="Explicit source variable references and inline record IDs bind only within this workbook; sheet-name suffixes are hints. Row validity and supplied or section periods constrain code membership.",
                                    provenance=(revision.artifact_path,),
                                ),
                            )
                            count["validity"] = len(result.validity)
                            count["records"] = len(result.records)
                            yield from result.records
                        elif entry.role == "thin_provider":
                            result = read_curated_source(
                                path, revision, provider=_THIN[entry.path]
                            )
                            tables(result.tables, count)
                            count["records"] = len(result.records)
                            yield from result.records
                        elif entry.role == "code_list":
                            result = read_code_list(path, revision, name=path.stem)
                            tables(result.tables, count)
                            canonical = (
                                entries.get(
                                    (
                                        "bundle",
                                        "catalog/scb_canonical/scb_canonical.toml",
                                    )
                                )
                                if Path(entry.path).parent.as_posix()
                                == "catalog/scb_canonical"
                                else None
                            )
                            target_sources = (
                                (canonical.revision.dataset,)
                                if canonical is not None
                                and canonical.revision is not None
                                else ()
                            )
                            values(
                                revision,
                                result,
                                count,
                                validity=result.validity,
                                validity_revision=revision if result.validity else None,
                                join=SourceValueJoin(
                                    record_sources=target_sources,
                                    member_target="declared_list"
                                    if target_sources
                                    else "unbound",
                                    member_format="none",
                                    validity_target="row",
                                    missing_validity="unrestricted",
                                    rule="An exact declared list name binds only in its selected provider source namespace. Classification lists remain unbound until common curation explicitly assigns them.",
                                    provenance=(revision.artifact_path,),
                                ),
                            )
                            count["validity"] = len(result.validity)
                        else:
                            raise PreparedCatalogError(
                                f"unsupported prepared input role {entry.role}"
                            )

            def spooled_tables() -> Iterator[SourceEvidenceTable]:
                table_file.flush()
                table_file.seek(0)
                for line in table_file:
                    yield SourceEvidenceTable.model_validate_json(line)

            record_manifest = prepare_source_records(
                staging / _RECORDS,
                records=records(),
                revisions=revisions,
                scope="complete selected machine-source bundle",
                tables=spooled_tables(),
            )
            evidence_file.flush()
            os.fsync(evidence_file.fileno())
        spool.unlink()
        final_inputs = tuple(
            entry.model_copy(
                update={"counts": SourceCounts(**counters[(entry.origin, entry.path)])}
            )
            for entry in inventory
        )
        for item in bundle.snapshot.manifest.files:
            count = counters[("snapshot", item.name)]
            observed = count[
                {
                    "Registerinformation.csv": "records",
                    "UnikaRegisterOchVariabler.csv": "records",
                    "Identifierare.csv": "records",
                    "Timeseries.csv": "declarations",
                    "Vardemangder.csv": "associations",
                    "VardemangderValidDates.csv": "validity",
                }[item.name]
            ]
            if observed != item.record_count:
                raise PreparedCatalogError(
                    f"{item.name}: prepared {observed} rows, selected input declares {item.record_count}"
                )
        # Child preparation already hashed its large payloads. Compose those
        # proofs; only manifests and the auxiliary stream need new hashes here.
        proofs = [
            _FileProof(
                path=f"{_RECORDS}/{record_manifest.database_path}",
                size=record_manifest.database_size,
                sha256=record_manifest.database_sha256,
                git_blob=record_manifest.database_git_blob,
            )
        ]
        for child_path, child_manifest in value_manifests.items():
            proofs.extend(
                _FileProof(
                    path=f"{child_path}/{file.path}",
                    size=file.size,
                    sha256=file.sha256,
                    git_blob=file.git_blob,
                )
                for file in child_manifest.files
            )
        for name in (
            _EVIDENCE,
            f"{_RECORDS}/manifest.json",
            *(f"{child.path}/manifest.json" for child in children),
        ):
            path = staging / name
            proofs.append(
                _FileProof(
                    path=name,
                    size=path.stat().st_size,
                    sha256=_file_sha256(path),
                    git_blob=_git(staging, "hash-object", "--no-filters", name),
                )
            )
        document = _ManifestDocument(
            input_commit=selection.input_commit,
            bundle_path=bundle.root.relative_to(bundle.repository).as_posix(),
            bundle_manifest_sha256=selection.manifest_sha256,
            snapshot_path=bundle.manifest.scb_snapshot_path,
            snapshot_manifest_sha256=bundle.manifest.scb_manifest_sha256,
            inputs=final_inputs,
            records_manifest_sha256=record_manifest.sha256,
            values=tuple(sorted(children, key=lambda child: child.path)),
            support_joins=scb_support_joins(
                {
                    entry.path: entry.revision.dataset
                    for entry in final_inputs
                    if entry.origin == "snapshot" and entry.revision is not None
                }
            ),
            evidence_count=evidence_count,
            files=tuple(sorted(proofs, key=lambda proof: proof.path)),
        )
        _check_children(
            document,
            record_manifest,
            tuple(value_manifests[child.path] for child in document.values),
        )
        current_index_tags = _index_tags(bundle.repository)
        if any(
            current_index_tags.get(path) != tag
            for path, tag in selected_index_tags.items()
        ):
            raise PreparedCatalogError(
                "selected source index flags changed during preparation; candidate was not published"
            )
        if _git(
            bundle.repository, "rev-parse", "HEAD"
        ) != selection.input_commit or _git(
            bundle.repository,
            "status",
            "--porcelain",
            "--untracked-files=all",
            "--",
            bundle.root.relative_to(bundle.repository).as_posix(),
            bundle.manifest.scb_snapshot_path,
        ):
            raise PreparedCatalogError(
                "selected source inputs changed during preparation; candidate was not published"
            )
        payload = (_json(document.model_dump(mode="json")) + "\n").encode()
        with (staging / "manifest.json").open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        if output.exists() or output.is_symlink():
            raise PreparedCatalogError(
                f"prepared catalog output appeared during preparation: {output}"
            )
        staging.rename(output)
        return _manifest(payload)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


@dataclass(frozen=True)
class PreparedCatalogSources:
    root: Path
    manifest: PreparedCatalogManifest
    input_commit: str
    records: PreparedSourceRecords
    value_sources: tuple[PreparedSourceValues, ...]

    def iter_evidence(self) -> Iterator[PreparedEvidence]:
        seen = 0
        with (self.root / _EVIDENCE).open(encoding="utf-8") as handle:
            for seen, line in enumerate(handle, start=1):
                yield _EVIDENCE_ADAPTER.validate_json(line)
        if seen != self.manifest.evidence_count:
            raise PreparedCatalogError(
                "prepared auxiliary evidence count differs from manifest"
            )


def open_prepared_catalog_sources(
    path: Path, *, expected_sha256: str, input_commit: str
) -> PreparedCatalogSources:
    """Check the entire accepted artifact once; never open original provider inputs."""
    selection = read_accepted_manifest(
        path, expected_sha256=expected_sha256, input_commit=input_commit
    )
    manifest = _manifest(selection.manifest_bytes)
    check_accepted_files(
        selection, {item.path: (item.size, item.git_blob) for item in manifest.files}
    )

    def child_bytes(relative: str, expected: str) -> bytes:
        payload = (selection.root / relative / "manifest.json").read_bytes()
        if hashlib.sha256(payload).hexdigest() != expected:
            raise PreparedCatalogError(
                f"child manifest differs from selection: {relative}"
            )
        return payload

    record_manifest = _record_manifest(
        child_bytes(_RECORDS, manifest.records_manifest_sha256)
    )
    records = PreparedSourceRecords(
        selection.root / _RECORDS, record_manifest, input_commit
    )
    value_sources = []
    for child in manifest.values:
        value_manifest = _value_manifest(child_bytes(child.path, child.manifest_sha256))
        if value_manifest.revision.revision_id != child.revision_id:
            raise PreparedCatalogError("value child revision differs from selection")
        value_sources.append(
            PreparedSourceValues(
                selection.root / child.path, value_manifest, input_commit
            )
        )
    _check_children(
        manifest, record_manifest, tuple(value.manifest for value in value_sources)
    )
    return PreparedCatalogSources(
        selection.root, manifest, input_commit, records, tuple(value_sources)
    )


def _check_children(
    manifest: _ManifestDocument,
    records: PreparedSourceManifest,
    values: tuple[PreparedValueManifest, ...],
) -> None:
    """Check small manifest totals and proofs, not the already validated payloads."""
    counts = [entry.counts for entry in manifest.inputs]
    revisions = {
        entry.revision.revision_id: entry.revision
        for entry in manifest.inputs
        if entry.revision and entry.disposition == "prepared"
    }
    if {revision.revision_id: revision for revision in records.revisions} != revisions:
        raise PreparedCatalogError(
            "record child revisions differ from selected machine sources"
        )
    occurrence_sources = {
        entry.revision.dataset
        for entry in manifest.inputs
        if entry.revision is not None and entry.record_usage == "occurrence"
    }
    for value in values:
        if (
            value.join is None
            or not set(value.join.record_sources) <= occurrence_sources
        ):
            raise PreparedCatalogError(
                "value join uses an undeclared occurrence source namespace"
            )
        if revisions.get(value.revision.revision_id) != value.revision or (
            value.validity_revision is not None
            and revisions.get(value.validity_revision.revision_id)
            != value.validity_revision
        ):
            raise PreparedCatalogError("value child uses an unselected source revision")
        entry = next(
            entry for entry in manifest.inputs if entry.revision == value.revision
        )
        if (value.descriptor_count, value.value_count, value.association_count) != (
            entry.counts.descriptors,
            entry.counts.values,
            entry.counts.associations,
        ):
            raise PreparedCatalogError(
                "value child source counts differ from input accounting"
            )
        if value.validity_revision is not None:
            validity_entry = next(
                entry
                for entry in manifest.inputs
                if entry.revision == value.validity_revision
            )
            if value.validity_count != validity_entry.counts.validity:
                raise PreparedCatalogError(
                    "value child validity count differs from input accounting"
                )
    if (records.record_count, records.table_count, records.table_row_count) != tuple(
        sum(getattr(count, key) for count in counts)
        for key in ("records", "tables", "table_rows")
    ):
        raise PreparedCatalogError("record child counts differ from input accounting")
    for field, key in (
        ("descriptor_count", "descriptors"),
        ("value_count", "values"),
        ("association_count", "associations"),
        ("validity_count", "validity"),
    ):
        if sum(getattr(value, field) for value in values) != sum(
            getattr(count, key) for count in counts
        ):
            raise PreparedCatalogError(
                f"value child {key} count differs from input accounting"
            )
    proofs = {
        item.path: (item.size, item.sha256, item.git_blob) for item in manifest.files
    }
    if proofs[f"{_RECORDS}/{records.database_path}"] != (
        records.database_size,
        records.database_sha256,
        records.database_git_blob,
    ):
        raise PreparedCatalogError(
            "record child preparation proof differs from outer proof"
        )
    for child, value in zip(manifest.values, values, strict=True):
        for file in value.files:
            if proofs[f"{child.path}/{file.path}"] != (
                file.size,
                file.sha256,
                file.git_blob,
            ):
                raise PreparedCatalogError(
                    "value child preparation proof differs from outer proof"
                )


__all__ = [
    "PreparedCatalogError",
    "PreparedCatalogManifest",
    "PreparedCatalogSources",
    "PreparedInputAccounting",
    "open_prepared_catalog_sources",
    "prepare_catalog_sources",
]
