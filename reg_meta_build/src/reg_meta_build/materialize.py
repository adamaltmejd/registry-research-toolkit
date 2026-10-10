"""Write a resolved catalog: storage IDs, rows, derivation and validation.

The resolved models and their pre-write validators live in resolved_catalog.py
and resolved_metadata.py; this module only materializes them.
"""

from __future__ import annotations

import gzip
import json
import shutil
import sqlite3
import time
from contextlib import closing, contextmanager
from io import TextIOWrapper
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any

from pydantic import TypeAdapter
from reg_core_py import parse_fqid

from reg_meta_build._curation import (
    SEARCH_PINS_FILE,
    SearchPin,
    curation_error,
    resolve_register_id,
)
from reg_meta_build._resolved_common import (
    CLASSIFICATION_SUCCESSION_AS_OF_YEAR,
    CLASSIFICATION_SUCCESSION_AS_OF_YEAR_KEY,
    _classification_id,
    _provider_id_for,
    _storage_id,
)
from reg_meta_build.artifact_identity import (
    builder_commit,
    generation_id,
    search_pins_sha256,
)
from reg_meta_build.data_warnings import DataWarning  # noqa: TC001
from reg_meta_build.db import (
    DB_FILENAME,
    DDL,
    SCHEMA_VERSION,
    _value_set_hash,
    default_db_dir,
    publish_db,
    register_py_lower,
    seed_providers,
    write_data_warnings,
)
from reg_meta_build.derive import derive
from reg_meta_build.documentary import DocumentaryRelationship
from reg_meta_build.id import mint
from reg_meta_build.pipeline import (
    admit_build_paths,
    admit_catalog_outputs,
    ledger_line,
    resolve_catalog,
    write_summary,
)
from reg_meta_build.resolved_bundle import (
    EVENTS_FILE,
    admit_bundle_code,
    admit_bundle_mode,
    load_resolved_bundle,
    read_bundle_record,
)
from reg_meta_build.resolved_catalog import (
    CURATION_TREE_SHA256_KEY,
    ResolvedClassification,
    ResolvedClassificationSuccession,
    ResolvedCodeSet,
    ResolvedConformance,
    ResolvedEdition,
    ResolvedRegister,
    ResolvedVariable,
    ResolvedVariant,
    _prepare_classification_succession,
    _validate_catalog_metadata,
    validate_resolved_variables,
)
from reg_meta_build.resolved_metadata import (
    ResolvedMetadata,
    ResolvedStateRef,
    ResolvedVariableGroup,
    RetainedDocumentaryRelationship,
    state_reference_key,
    validate_metadata_structure,
)
from reg_meta_build.source_files import _emit_timing
from reg_meta_build.validate import validate_built_db

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from reg_meta_build.resolved_bundle import ResolvedBuild

_COLUMNS = {
    "concept_group": "group_id,kind,register_id,group_key,label,source",
    "concept_group_axis": "group_id,axis,ordinal,label",
    "concept_group_variable": "member_id,group_id,variable_id,delivery_column_name",
    "concept_group_variable_facet": "member_id,axis,value,label",
    "concept_group_classification": "classification_id,group_id,facet_value,facet_label",
    "tag": "tag_id,slug,label",
    "tag_member": "tag_id,register_id,variable_id,rank,starred,note",
    "variable_same_as": "a_provider,a_register,a_variable,b_provider,b_register,b_variable",
    "register_replaced_by": "predecessor_provider,predecessor_register,successor_provider,successor_register,effective_year,beskrivning",
    "variable_replaced_by": "predecessor_provider,predecessor_register,predecessor_variable,successor_provider,successor_register,successor_variable,effective_year,note,beskrivning",
    "variant_replaced_by": "predecessor_provider,predecessor_register,predecessor_variant,successor_provider,successor_register,successor_variant",
    "representation_replaced_by": "predecessor_provider,predecessor_register,predecessor_variable,predecessor_column,successor_provider,successor_register,successor_variable,successor_column,variant,effective_year,note,beskrivning",
    "classification_derived_from": "derived_slug,source_slug,note",
    "variable_state_lineage": "consumer_state_id,source_state_id,valid_from,valid_to",
    "variable_state_lineage_warning": "consumer_state_id,warning_kind,message",
    "source_relationship": "relationship_id,owner_variable_id,kind,source_dataset,source_revision_id,declaration_json,binding_status,unresolved_json,provenance",
}


def prepare_resolved_metadata(
    metadata: ResolvedMetadata,
    variables: tuple[ResolvedVariable, ...],
    variable_storage_ids: dict[tuple[str, str, str], int],
    registers: dict[tuple[str, str], ResolvedRegister],
    variants: dict[tuple[str, str, str], ResolvedVariant],
    classifications: tuple[ResolvedClassification, ...],
) -> dict[str, list[tuple[Any, ...]]]:
    """Validate exact dependent references before any staged or live DB is touched."""
    metadata = ResolvedMetadata.model_validate(metadata)
    validate_metadata_structure(metadata)
    rows: dict[str, list[tuple[Any, ...]]] = {name: [] for name in _COLUMNS}
    if not any(getattr(metadata, name) for name in type(metadata).model_fields):
        return rows
    register_ids = {
        "/".join(key): _storage_id(key[0], "register", key[1]) for key in registers
    }
    variable_ids = {
        "/".join(key): variable_id for key, variable_id in variable_storage_ids.items()
    }
    variant_keys = {("/".join(key[:2]), key[2]) for key in variants}
    classification_ids = {
        item.slug: _classification_id(item.slug) for item in classifications
    }
    historical = {item.target: item for item in metadata.historical_predecessors}
    for target, declaration in historical.items():
        live = register_ids if declaration.kind == "register" else variable_ids
        if target in live:
            raise ValueError(
                f"obsolete historical predecessor declaration now names a live entity: {target}"
            )
    representations = set()
    states = {}
    for variable in variables:
        fqid = f"{variable.register_ref.provider}/{variable.register_ref.slug}/{variable.slug}"
        for state in variable.states:
            key = (
                fqid,
                state.variant.slug,
                state.valid_from
                if state.valid_from is not None
                else "year_independent",
                state.value_set_version_label,
            )
            states[key] = state
            representations.add((fqid, state.variant.slug, state.delivery_column_name))
        for alias in variable.aliases:
            representations.add((fqid, alias.variant.slug, alias.delivery_column_name))
    literal_representation_columns = {
        (variable, column) for variable, _, column in representations
    }
    # Groups enumerate literal delivered spellings; succession has a separate
    # case-insensitive endpoint contract. Keep both indexes without merging facts.
    representations = {
        (variable, variant, column.lower())
        for variable, variant, column in representations
    }
    representation_columns = {
        (variable, column) for variable, _, column in representations
    }

    def require(mapping: Any, key: Any, label: str) -> Any:
        if key not in mapping:
            raise ValueError(f"unknown resolved {label}: {key!r}")
        return mapping[key] if isinstance(mapping, dict) else key

    def representation(variable: str, column: str, variant: str | None = None) -> None:
        require(variable_ids, variable, "variable")
        if (
            (variable, column.lower()) not in representation_columns
            if variant is None
            else (variable, variant, column.lower()) not in representations
        ):
            raise ValueError(
                f"unknown resolved representation: {variable}, {column}, {variant}"
            )

    def state_id(ref: ResolvedStateRef) -> int:
        key = state_reference_key(ref)
        state = require(states, key, "state")
        if (state.valid_to, state.delivery_column_name, state.period_scope) != (
            ref.valid_to,
            ref.delivery_column_name,
            ref.period_scope,
        ):
            raise ValueError(
                "resolved state reference does not match its exact scope/column"
            )
        provider, register, variable = ref.variable.split("/")
        return _storage_id(
            provider,
            "state",
            register,
            variable,
            ref.variant,
            key[2],
            ref.value_set_version_label,
        )

    for group in (*metadata.variable_groups, *metadata.classification_groups):
        is_variable = isinstance(group, ResolvedVariableGroup)
        scope = group.register_ref if is_variable else ""
        register_id = require(register_ids, scope, "register") if is_variable else None
        kind = "variable" if is_variable else "classification"
        group_id = mint("resolved-catalog", "group", kind, scope, group.key)
        rows["concept_group"].append(
            (group_id, kind, register_id, group.key, group.label, group.source)
        )
        rows["concept_group_axis"].extend(
            (group_id, axis.axis, axis.ordinal, axis.label) for axis in group.axes
        )
        if isinstance(group, ResolvedVariableGroup):
            for member in group.members:
                variable_id = require(variable_ids, member.variable, "group variable")
                if member.delivery_column_name is not None:
                    require(
                        literal_representation_columns,
                        (member.variable, member.delivery_column_name),
                        "group representation",
                    )
                member_id = mint(
                    "resolved-catalog",
                    "group-member",
                    str(group_id),
                    member.variable,
                    member.delivery_column_name or "",
                )
                rows["concept_group_variable"].append(
                    (member_id, group_id, variable_id, member.delivery_column_name)
                )
                rows["concept_group_variable_facet"].extend(
                    (member_id, f.axis, f.value, f.label) for f in member.facets
                )
        else:
            for member in group.members:
                classification_id = require(
                    classification_ids, member.classification, "group classification"
                )
                rows["concept_group_classification"].append(
                    (
                        classification_id,
                        group_id,
                        member.facet_value,
                        member.facet_label,
                    )
                )
    for tag in metadata.tags:
        tag_id = mint("resolved-catalog", "tag", tag.slug)
        rows["tag"].append((tag_id, tag.slug, tag.label))
        for member in tag.members:
            register_id = variable_id = None
            if parse_fqid(member.target).kind == "register":
                register_id = require(register_ids, member.target, "tag register")
            else:
                variable_id = require(variable_ids, member.target, "tag variable")
            rows["tag_member"].append(
                (
                    tag_id,
                    register_id,
                    variable_id,
                    member.rank,
                    member.starred,
                    member.note,
                )
            )
    for edge in metadata.variable_same_as:
        require(variable_ids, edge.a, "same_as variable")
        require(variable_ids, edge.b, "same_as variable")
        a, b = tuple(edge.a.split("/")), tuple(edge.b.split("/"))
        rows["variable_same_as"].extend(((*a, *b), (*b, *a)))
    for edge in metadata.successions:
        kind = parse_fqid(edge.predecessor).kind
        targets = register_ids if kind == "register" else variable_ids
        table = "register_replaced_by" if kind == "register" else "variable_replaced_by"
        if edge.predecessor not in targets:
            require(historical, edge.predecessor, "succession predecessor")
        require(targets, edge.successor, "succession successor")
        # Only the variable grain stores `note`: validate_built_db selects the
        # vintage-lift edges by it.
        note = (edge.note,) if kind == "variable" else ()
        rows[table].append(
            (
                *edge.predecessor.split("/"),
                *edge.successor.split("/"),
                edge.effective_year,
                *note,
                edge.description,
            )
        )
    for edge in metadata.variant_successions:
        a, b = edge.predecessor, edge.successor
        for endpoint in (a, b):
            require(
                variant_keys,
                (endpoint.register_ref, endpoint.variant),
                "succession variant",
            )
        ka, kb = (
            (*a.register_ref.split("/"), a.variant),
            (*b.register_ref.split("/"), b.variant),
        )
        rows["variant_replaced_by"].append((*ka, *kb))
    for edge in metadata.representation_successions:
        a, b = edge.predecessor, edge.successor
        for endpoint in (a, b):
            representation(
                endpoint.variable, endpoint.delivery_column_name, edge.variant
            )
        rows["representation_replaced_by"].append(
            (
                *a.variable.split("/"),
                a.delivery_column_name,
                *b.variable.split("/"),
                b.delivery_column_name,
                edge.variant or "",
                edge.effective_year,
                edge.note,
                edge.description,
            )
        )
    for edge in metadata.classification_derivations:
        require(classification_ids, edge.derived, "derived classification")
        require(classification_ids, edge.source, "source classification")
        rows["classification_derived_from"].append(
            (edge.derived, edge.source, edge.note)
        )
    for edge in metadata.state_lineage:
        consumer, source = state_id(edge.consumer), state_id(edge.source)
        rows["variable_state_lineage"].append(
            (consumer, source, edge.valid_from, edge.valid_to)
        )
    for warning in metadata.lineage_warnings:
        consumer = state_id(warning.consumer)
        rows["variable_state_lineage_warning"].append(
            (consumer, warning.kind, warning.message)
        )
    for relationship in metadata.documentary_relationships:
        relationship = type(relationship).model_validate_json(
            relationship.model_dump_json()
        )
        if isinstance(relationship, RetainedDocumentaryRelationship):
            require(
                register_ids, relationship.register_ref, "retained documentary register"
            )
        rows["source_relationship"].append(
            (
                relationship.relationship_id,
                variable_ids[relationship.owner]
                if isinstance(relationship, DocumentaryRelationship)
                else None,
                relationship.declaration.kind,
                relationship.declaration.revision.dataset,
                relationship.declaration.revision.revision_id,
                relationship.declaration.model_dump_json(),
                relationship.binding_status,
                json.dumps(
                    [u.model_dump(mode="json") for u in relationship.unresolved]
                    if isinstance(relationship, DocumentaryRelationship)
                    else [],
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                relationship.provenance,
            )
        )
    return rows


def write_resolved_metadata(
    conn: sqlite3.Connection, rows: dict[str, list[tuple[Any, ...]]]
) -> None:
    for table, columns in _COLUMNS.items():
        placeholders = ",".join("?" for _ in columns.split(","))
        conn.executemany(
            f"INSERT INTO {table} ({columns}) VALUES ({placeholders})",
            sorted(rows[table], key=lambda row: json.dumps(row, ensure_ascii=False)),
        )


def _write_value_sets(
    conn: sqlite3.Connection,
    variables: tuple[ResolvedVariable, ...],
    classifications: tuple[ResolvedClassification, ...],
) -> tuple[dict[ResolvedCodeSet, int], dict[tuple[str, str], int]]:
    """Store content-shared memberships without provider or validity inference.

    Returns the value-set IDs and the code IDs. `code_id` is dense in (label,
    code) order, so codes with one label sit together on disk (#1296).
    """
    code_sets = sorted(
        {
            state.value_set
            for variable in variables
            for state in variable.states
            if state.value_set is not None
        }
        | {
            window.value_set
            for variable in variables
            for alias in variable.aliases
            for window in alias.windows
            if window.value_set is not None
        },
        key=lambda code_set: code_set.members,
    )
    pairs = {pair for code_set in code_sets for pair in code_set.members}
    pairs.update(
        (code.code, code.label)
        for classification in classifications
        for code in classification.codes
    )
    code_ids = {
        pair: code_id
        for code_id, pair in enumerate(
            sorted(pairs, key=lambda pair: (pair[1], pair[0])), start=1
        )
    }
    conn.executemany(
        "INSERT INTO value_code (code_id, code, label) VALUES (?, ?, ?)",
        ((code_id, *pair) for pair, code_id in code_ids.items()),
    )
    set_ids: dict[ResolvedCodeSet, int] = {}
    for code_set in code_sets:
        member_hash = _value_set_hash(list(code_set.members))
        set_id = mint("resolved-catalog", "value-set", member_hash.hex())
        conn.execute(
            "INSERT INTO value_set (value_set_id, member_hash) VALUES (?, ?)",
            (set_id, member_hash),
        )
        conn.executemany(
            "INSERT INTO value_set_member (value_set_id, code_id) VALUES (?, ?)",
            ((set_id, code_ids[pair]) for pair in code_set.members),
        )
        set_ids[code_set] = set_id
    return set_ids, code_ids


def _write_editions(
    conn: sqlite3.Connection, editions: tuple[ResolvedEdition, ...]
) -> None:
    for edition in sorted(
        editions,
        key=lambda e: (
            e.register_ref.provider,
            e.register_ref.slug,
            e.variant.slug,
            e.name,
        ),
    ):
        register = edition.register_ref
        edition_id = _storage_id(
            register.provider,
            "edition",
            register.slug,
            edition.variant.slug,
            edition.name,
        )
        conn.execute(
            "INSERT INTO register_version (regver_id, register_variant_id, "
            "registerversionnamn, registerversionbeskrivning, registerversionmatinformation, "
            "registerversion_docstaus, registerversion_forstagodkannandedatum, "
            "registerversion_senastgodkanddatum) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                edition_id,
                _storage_id(
                    register.provider, "variant", register.slug, edition.variant.slug
                ),
                edition.name,
                edition.description,
                edition.measurement_information,
                edition.documentation_status,
                edition.first_approved_at,
                edition.last_approved_at,
            ),
        )
        conn.executemany(
            "INSERT INTO population (regver_id, name, definition, comment, date_range) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                (
                    edition_id,
                    population.name,
                    population.definition,
                    population.comment,
                    population.date_range,
                )
                for population in sorted(edition.populations, key=lambda p: p.name)
            ),
        )
        conn.executemany(
            "INSERT INTO object_type (regver_id, name, definition) VALUES (?, ?, ?)",
            (
                (edition_id, item.name, item.definition)
                for item in sorted(edition.object_types, key=lambda item: item.name)
            ),
        )


def _write_classifications(
    conn: sqlite3.Connection,
    classifications: tuple[ResolvedClassification, ...],
    predecessors: dict[str, str],
    code_ids: dict[tuple[str, str], int],
) -> None:
    for classification in classifications:
        classification_id = _classification_id(classification.slug)
        conn.execute(
            "INSERT INTO classification (id, short_name, name, name_en, publisher, valid_from, "
            "valid_to, description, url, supersedes_id, code_count, valid_code_count, slug) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                classification_id,
                classification.short_name,
                classification.name,
                classification.name_en,
                classification.publisher,
                classification.valid_from,
                classification.valid_to,
                classification.description,
                classification.url,
                _classification_id(predecessors[classification.slug])
                if classification.slug in predecessors
                else None,
                len(classification.codes),
                len({code.code for code in classification.codes}),
                classification.slug,
            ),
        )
        conn.executemany(
            "INSERT INTO classification_code (classification_id, code_id, level, is_valid) "
            "VALUES (?, ?, ?, 1)",
            (
                (classification_id, code_ids[code.code, code.label], code.level)
                for code in sorted(
                    classification.codes, key=lambda c: (c.code, c.label)
                )
            ),
        )


def _write_conformance(
    conn: sqlite3.Connection,
    state_id: int,
    conformance: ResolvedConformance,
    classification: ResolvedClassification,
    code_ids: dict[tuple[str, str], int],
) -> None:
    checked = len(conformance.checked_codes)
    extensions = set(conformance.nonconforming_members) | set(
        conformance.sentinel_members
    )
    nonconforming = len({code for code, _ in extensions})
    matched = checked - nonconforming
    sentinel_pairs = set(conformance.sentinel_members)
    sentinel_meanings = {
        sentinel.code: sentinel.meaning for sentinel in classification.sentinel_codes
    }
    conn.execute(
        "INSERT INTO classification_conformance (state_id, declared_classification_id, status, "
        "checked_code_count, matched_code_count, nonconforming_code_count, overlap) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            state_id,
            _classification_id(conformance.declared_classification),
            conformance.status,
            checked,
            matched,
            nonconforming,
            matched / checked if checked else 1.0,
        ),
    )
    conn.executemany(
        "INSERT INTO classification_conformance_code "
        "(state_id, declared_classification_id, code_id, member_kind, sentinel_meaning, scoped_sentinels) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (
            (
                state_id,
                _classification_id(conformance.declared_classification),
                code_ids[pair],
                "sentinel" if pair in sentinel_pairs else "nonstandard",
                sentinel_meanings.get(pair[0]) if pair in sentinel_pairs else None,
                json.dumps(
                    [
                        certificate.model_dump(mode="json")
                        for certificate in conformance.scoped_sentinels
                        if pair in certificate.members
                    ],
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            )
            for pair in sorted(extensions)
        ),
    )


def _write_search_pins(
    conn: sqlite3.Connection,
    search_pins: tuple[SearchPin, ...],
    rows: Iterable[tuple[str, str, int, str]],
) -> None:
    """Store `rows`, the pins' rows, after their registers and classifications; a
    pin that does not resolve fails the build, located by entry and FQID."""
    for index, pin in enumerate(search_pins, start=1):
        for fqid in pin.fqids:
            if pin.type == "register":
                provider, register = fqid.split("/")
                resolves = resolve_register_id(conn, provider, register) is not None
            else:
                resolves = (
                    conn.execute(
                        "SELECT 1 FROM classification WHERE slug = ?",
                        (fqid.removeprefix("class/"),),
                    ).fetchone()
                    is not None
                )
            if not resolves:
                raise curation_error(
                    "search_pin_unresolved",
                    f"{SEARCH_PINS_FILE} [[pin]] entry {index}: {fqid} does not "
                    f"resolve to a {pin.type} in this catalog.",
                    "Fix the FQID or drop it from the pin.",
                )
    conn.executemany(
        "INSERT INTO search_pin (key, type, position, entity) VALUES (?, ?, ?, ?)",
        rows,
    )


def write_resolved_catalog(
    variables: tuple[ResolvedVariable, ...],
    output: Path,
    *,
    manifest: dict[str, str],
    diagnostic: bool = False,
    scoped: bool = False,
    corpus: bool = False,
    parent_registers: tuple[ResolvedRegister, ...] = (),
    parent_variants: tuple[tuple[ResolvedRegister, ResolvedVariant], ...] = (),
    editions: tuple[ResolvedEdition, ...] = (),
    classifications: tuple[ResolvedClassification, ...] = (),
    classification_successions: tuple[ResolvedClassificationSuccession, ...] = (),
    metadata: ResolvedMetadata | None = None,
    data_warnings: tuple[DataWarning, ...] = (),
    search_pins: tuple[SearchPin, ...] = (),
) -> Path:
    """Validate and atomically place a strict catalog or create-only diagnostic.

    No time, source precedence, slug derivation, or state coalescing is inferred.
    The caller supplies reproducible manifest values; schema-owned keys are fixed.
    Both modes run the same contract and structural checks. Diagnostic and
    register-scoped artifacts are marked incomplete/nonpublishable and can never
    replace an existing file.
    Independently resolved registers and (register, variant) pairs remain present
    even when their variable states are withheld. Shared definitions must agree.
    The complete pipeline additionally requires corpus safeguards before strict
    publication; partial writer fixtures leave those volume expectations disabled.
    Only a complete catalog stores `search_pins`, each of which must resolve; a
    partial one stores none.
    """
    diagnostic = TypeAdapter(bool).validate_python(diagnostic, strict=True)
    partial = diagnostic or TypeAdapter(bool).validate_python(scoped, strict=True)
    variables, registers, variants = validate_resolved_variables(
        variables, allow_empty=diagnostic
    )
    parent_registers = TypeAdapter(tuple[ResolvedRegister, ...]).validate_python(
        parent_registers, strict=True
    )
    parent_variants = TypeAdapter(
        tuple[tuple[ResolvedRegister, ResolvedVariant], ...]
    ).validate_python(parent_variants, strict=True)
    for register in (*parent_registers, *(r for r, _ in parent_variants)):
        key = register.provider, register.slug
        _provider_id_for(register.provider)
        if key in registers and registers[key] != register:
            raise ValueError(f"inconsistent resolved parent register: {key!r}")
        registers[key] = register
    for register, variant in parent_variants:
        key = register.provider, register.slug, variant.slug
        if key in variants and variants[key] != variant:
            raise ValueError(f"inconsistent resolved parent variant: {key!r}")
        variants[key] = variant
    editions = TypeAdapter(tuple[ResolvedEdition, ...]).validate_python(
        editions, strict=True
    )
    classifications = TypeAdapter(tuple[ResolvedClassification, ...]).validate_python(
        classifications, strict=True
    )
    _validate_catalog_metadata(
        variables, editions, classifications, registers, variants
    )
    classification_successions = TypeAdapter(
        tuple[ResolvedClassificationSuccession, ...]
    ).validate_python(classification_successions, strict=True)
    classifications, classification_predecessors = _prepare_classification_succession(
        classifications, classification_successions
    )
    # Internal and dense (1..n) in (provider, register, slug) order, so a
    # register's variables and their states share pages (#1296 2b). Readers
    # never order or break ties on it.
    variable_ids = {
        key: variable_id
        for variable_id, key in enumerate(
            sorted(
                (v.register_ref.provider, v.register_ref.slug, v.slug)
                for v in variables
            ),
            start=1,
        )
    }
    metadata_rows = prepare_resolved_metadata(
        ResolvedMetadata() if metadata is None else metadata,
        variables,
        variable_ids,
        registers,
        variants,
        classifications,
    )
    search_pins = TypeAdapter(tuple[SearchPin, ...]).validate_python(
        () if partial else search_pins
    )
    pin_rows = [
        (pin.key, pin.type, position, fqid)
        for pin in search_pins
        for position, fqid in enumerate(pin.fqids)
    ]
    import_metadata = TypeAdapter(dict[str, str]).validate_python(manifest, strict=True)
    for key in (CURATION_TREE_SHA256_KEY,):
        if (value := import_metadata.get(key)) is not None and (
            len(value) != 64 or any(char not in "0123456789abcdef" for char in value)
        ):
            raise ValueError(f"manifest {key} must be a lowercase SHA-256 digest")
    for key, value in {
        "schema_version": SCHEMA_VERSION,
        "catalog_artifact_kind": "diagnostic" if diagnostic else "catalog",
        "catalog_publishable": "false" if partial else "true",
        "catalog_completeness": "incomplete" if partial else "complete",
        CLASSIFICATION_SUCCESSION_AS_OF_YEAR_KEY: str(
            CLASSIFICATION_SUCCESSION_AS_OF_YEAR
        ),
        "search_pins_sha256": search_pins_sha256(pin_rows),
    }.items():
        if key in import_metadata and import_metadata[key] != value:
            raise ValueError(
                f"manifest conflicts with catalog {key}: {import_metadata[key]!r}"
            )
        import_metadata[key] = value

    captured_revision = None
    if partial:
        import_metadata.pop("builder_commit", None)
        import_metadata.pop("generation_id", None)
    else:
        if corpus or "builder_commit" not in import_metadata:
            revision = builder_commit()
            if "builder_commit" not in import_metadata:
                captured_revision = revision
                import_metadata["builder_commit"] = revision
        expected_generation = generation_id(import_metadata)
        if (
            "generation_id" in import_metadata
            and import_metadata["generation_id"] != expected_generation
        ):
            raise ValueError(
                "manifest generation_id conflicts with canonical semantic inputs"
            )
        import_metadata["generation_id"] = expected_generation

    output = Path(output)
    # An existing destination needs no check here: the create-only hardlink below
    # refuses any existing path. The active catalog path may not exist yet, so it
    # is refused explicitly.
    if partial and output.resolve() == (default_db_dir() / DB_FILENAME).resolve():
        raise ValueError(
            "diagnostic or register-scoped output must be a new explicit path separate from the active catalog"
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix=f".{output.name}.", dir=output.parent) as temporary:
        staged = Path(temporary) / output.name
        with closing(sqlite3.connect(staged)) as conn:
            conn.execute("PRAGMA foreign_keys = ON")
            register_py_lower(conn)
            conn.executescript(DDL)
            seed_providers(conn)
            value_set_ids, code_ids = _write_value_sets(
                conn, variables, classifications
            )
            _write_classifications(
                conn, classifications, classification_predecessors, code_ids
            )
            classifications_by_slug = {book.slug: book for book in classifications}
            conn.executemany(
                "INSERT INTO classification_replaced_by "
                "(predecessor_slug, successor_slug, effective_year, note) VALUES (?, ?, ?, ?)",
                (
                    (edge.predecessor, edge.successor, edge.effective_year, edge.note)
                    for edge in sorted(
                        classification_successions,
                        key=lambda edge: (edge.predecessor, edge.successor),
                    )
                ),
            )
            for (provider, slug), register in sorted(registers.items()):
                conn.execute(
                    "INSERT INTO register (register_id, provider_id, name, slug, purpose) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        _storage_id(provider, "register", slug),
                        _provider_id_for(provider),
                        register.name,
                        slug,
                        register.purpose,
                    ),
                )
            for (provider, register_slug, slug), variant in sorted(variants.items()):
                conn.execute(
                    "INSERT INTO register_variant "
                    "(register_variant_id, register_id, name, slug, description, display_group, "
                    "panel_entity_key, panel_time_key, panel_time_grain) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        _storage_id(provider, "variant", register_slug, slug),
                        _storage_id(provider, "register", register_slug),
                        variant.name,
                        slug,
                        variant.description,
                        variant.display_group,
                        json.dumps(variant.panel_entity_key)
                        if isinstance(variant.panel_entity_key, tuple)
                        else variant.panel_entity_key,
                        json.dumps(variant.panel_time_key)
                        if isinstance(variant.panel_time_key, tuple)
                        else variant.panel_time_key,
                        variant.panel_time_grain,
                    ),
                )
            _write_editions(conn, editions)
            for variable in sorted(
                variables,
                key=lambda v: (v.register_ref.provider, v.register_ref.slug, v.slug),
            ):
                provider, register_slug = (
                    variable.register_ref.provider,
                    variable.register_ref.slug,
                )
                variable_id = variable_ids[provider, register_slug, variable.slug]
                conn.execute(
                    "INSERT INTO variable (variable_id, register_id, provider_key, slug, "
                    "name, definition, description, operational_definition, measurement_unit, "
                    "is_sensitive, is_identifier, deprecated, source_register_id, source_register_text, "
                    "source_label) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        variable_id,
                        _storage_id(provider, "register", register_slug),
                        variable.provider_key,
                        variable.slug,
                        variable.name,
                        variable.definition,
                        variable.description,
                        variable.operational_definition,
                        variable.measurement_unit,
                        variable.is_sensitive,
                        variable.is_identifier,
                        variable.deprecated,
                        _storage_id(
                            variable.source_register.provider,
                            "register",
                            variable.source_register.slug,
                        )
                        if variable.source_register is not None
                        else None,
                        variable.source_register_text,
                        variable.source_label,
                    ),
                )
                for state in sorted(
                    variable.states,
                    key=lambda s: (
                        s.variant.slug,
                        s.period_scope,
                        s.valid_from or "",
                        s.value_set_version_label,
                    ),
                ):
                    variant_id = _storage_id(
                        provider, "variant", register_slug, state.variant.slug
                    )
                    state_coordinate = (
                        state.valid_from
                        if state.period_scope == "intervals"
                        else "year_independent"
                    )
                    assert state_coordinate is not None
                    state_id = _storage_id(
                        provider,
                        "state",
                        register_slug,
                        variable.slug,
                        state.variant.slug,
                        state_coordinate,
                        state.value_set_version_label,
                    )
                    conn.execute(
                        "INSERT INTO variable_state (state_id, variable_id, "
                        "register_variant_id, valid_from, valid_to, delivery_column_name, "
                        "data_type, data_length, operational_definition, provenance, pooled, "
                        "value_set_id, value_set_version_label, source_register_text, period_scope, definition, measurement_unit, name, description) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            state_id,
                            variable_id,
                            variant_id,
                            state.valid_from,
                            state.valid_to,
                            state.delivery_column_name,
                            state.data_type,
                            state.data_length,
                            state.operational_definition,
                            state.provenance,
                            int(state.pooled),
                            value_set_ids[state.value_set]
                            if state.value_set is not None
                            else None,
                            state.value_set_version_label,
                            state.source_register_text,
                            state.period_scope,
                            state.definition,
                            state.measurement_unit,
                            state.name,
                            state.description,
                        ),
                    )
                    for link in state.classification_links:
                        conn.execute(
                            "INSERT INTO state_classification (state_id, classification_id, provenance) VALUES (?, ?, ?)",
                            (
                                state_id,
                                _classification_id(link.classification),
                                link.provenance,
                            ),
                        )
                        if link.conformance is not None:
                            _write_conformance(
                                conn,
                                state_id,
                                link.conformance,
                                classifications_by_slug[link.classification],
                                code_ids,
                            )
                    conn.execute(
                        "INSERT OR IGNORE INTO variable_alias "
                        "(variable_id, register_variant_id, delivery_column_name) VALUES (?, ?, ?)",
                        (variable_id, variant_id, state.delivery_column_name),
                    )
                for alias in sorted(
                    variable.aliases,
                    key=lambda a: (a.variant.slug, a.delivery_column_name),
                ):
                    variant_id = _storage_id(
                        provider, "variant", register_slug, alias.variant.slug
                    )
                    conn.execute(
                        "INSERT OR IGNORE INTO variable_alias "
                        "(variable_id, register_variant_id, delivery_column_name) VALUES (?, ?, ?)",
                        (variable_id, variant_id, alias.delivery_column_name),
                    )
                    conn.executemany(
                        "INSERT INTO variable_alias_window "
                        "(variable_id, register_variant_id, delivery_column_name, valid_from, valid_to, provenance, column_metadata, data_type, data_length, operational_definition, source_register_text, coding_metadata, value_set_id, value_set_version_label, definition, measurement_unit, name, description) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            (
                                variable_id,
                                variant_id,
                                alias.delivery_column_name,
                                window.valid_from,
                                window.valid_to,
                                window.provenance,
                                window.column_metadata,
                                window.data_type,
                                window.data_length,
                                window.operational_definition,
                                window.source_register_text,
                                window.coding_metadata,
                                value_set_ids[window.value_set]
                                if window.value_set is not None
                                else None,
                                window.value_set_version_label,
                                window.definition,
                                window.measurement_unit,
                                window.name,
                                window.description,
                            )
                            for window in sorted(
                                alias.windows, key=lambda w: w.valid_from
                            )
                        ),
                    )
                    for window in sorted(alias.windows, key=lambda w: w.valid_from):
                        for link in window.classification_links:
                            conn.execute(
                                "INSERT INTO alias_window_classification "
                                "(variable_id, register_variant_id, delivery_column_name, valid_from, classification_id, provenance, conformance) "
                                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                                (
                                    variable_id,
                                    variant_id,
                                    alias.delivery_column_name,
                                    window.valid_from,
                                    _classification_id(link.classification),
                                    link.provenance,
                                    link.conformance.model_dump_json()
                                    if link.conformance is not None
                                    else None,
                                ),
                            )
            _write_search_pins(conn, search_pins, pin_rows)
            write_data_warnings(conn, data_warnings)
            write_resolved_metadata(conn, metadata_rows)
            conn.execute(
                "INSERT INTO code_variable_map (code_id, variable_id) "
                "SELECT DISTINCT member.code_id, state.variable_id "
                "FROM variable_state state JOIN value_set_member member "
                "ON state.value_set_id = member.value_set_id "
                "UNION SELECT DISTINCT member.code_id, alias.variable_id "
                "FROM variable_alias_window alias JOIN value_set_member member "
                "ON alias.value_set_id = member.value_set_id"
            )
            conn.execute(
                "UPDATE value_code SET mapping_count = ("
                "SELECT COUNT(*) FROM code_variable_map "
                "WHERE code_id = value_code.code_id)"
            )
            conn.executemany(
                "INSERT INTO import_manifest (key, value) VALUES (?, ?)",
                sorted(import_metadata.items()),
            )
            for table in (
                "variable_alias_build",
                "variable_instance",
                "classification_candidate",
                "unika_summary",
            ):
                conn.execute(f"DROP TABLE {table}")
            conn.commit()
            derive(conn)
            # Last write: readers plan with these statistics. ANALYZE is a pure
            # function of the table contents, so rebuilds stay byte-identical.
            conn.execute("ANALYZE")
            conn.commit()
            conn.execute("VACUUM")
        validation = validate_built_db(staged, corpus=corpus)
        if not validation.passed:
            raise ValueError(
                "resolved catalog validation failed: " + "; ".join(validation.failures)
            )
        if partial:
            # Create-only placement is atomic and cannot clobber a normal catalog
            # even if another process creates the destination during the build.
            output.hardlink_to(staged)
        else:
            if (corpus or captured_revision is not None) and (
                builder_commit() != import_metadata["builder_commit"]
            ):
                raise ValueError("Builder revision changed during compilation")
            publish_db(staged, output)
    return output


class CompletedArtifactError(Exception):
    """Report finalization failed after the catalog reached its destination."""

    def __init__(
        self, cause: Exception, result: dict[str, object], report_dir: Path
    ) -> None:
        super().__init__(str(cause))
        self.result = dict(result)
        self.report_dir = report_dir


@contextmanager
def _retain_completed_artifact(
    result: dict[str, object], report_dir: Path
) -> Iterator[None]:
    try:
        yield
    except Exception as exc:
        if result.get("database"):
            raise CompletedArtifactError(exc, result, report_dir) from exc
        raise


def build_catalog(
    prepared_path: Path,
    input_commit: str,
    input_manifest_sha256: str,
    output: Path,
    report_dir: Path,
    *,
    diagnostic: bool = False,
    registers: tuple[str, ...] = (),
    curation_dir: Path | None = None,
    dump_decisions: Path | None = None,
    resolved_out: Path | None = None,
) -> dict[str, object]:
    """Build the compiled curation; failures never replace an active catalog.

    Resolves (`pipeline.resolve_catalog`), then materializes. A diagnostic
    completion retains strict errors and returns publication_ready false.
    `registers` builds only the named scopes; its output is never publishable and
    its corpus volume guards do not apply. `resolved_out` also writes the
    resolution as a bundle that `materialize_resolved` places.
    """
    started = time.perf_counter()
    publishable = not diagnostic and not registers
    # Refuse bad paths and an unclean publishable builder before resolving.
    # `resolve_catalog` admits the paths again; the checks are cheap and pure.
    admit_build_paths(
        prepared_path,
        input_commit,
        input_manifest_sha256,
        output,
        report_dir,
        publishable=publishable,
        curation_dir=curation_dir,
        dump_decisions=dump_decisions,
        resolved_out=resolved_out,
    )
    revision = builder_commit() if publishable else None
    resolved = resolve_catalog(
        prepared_path,
        input_commit,
        input_manifest_sha256,
        output,
        report_dir,
        diagnostic=diagnostic,
        registers=registers,
        curation_dir=curation_dir,
        dump_decisions=dump_decisions,
        resolved_out=resolved_out,
    )
    result = materialize_build(resolved, revision=revision)
    # Reported after publication: a failure here keeps the completed-artifact
    # status, as it did when this line sat inside the pipeline's last guard.
    with _retain_completed_artifact(result, report_dir):
        _emit_timing("pipeline: total", started)
    return result


def materialize_resolved(
    bundle: Path,
    output: Path,
    report_dir: Path,
    *,
    diagnostic: bool = False,
    registers: tuple[str, ...] = (),
) -> dict[str, object]:
    """Place a resolved bundle (`build_catalog(resolved_out=...)`) as the build
    that wrote it would have.

    The bundle must come from this builder's resolve code, the requested mode and
    registers must be the bundle's, and a strict bundle must have no errors. The report directory gets the bundle's ledger member,
    then materialization's, so it reads as the unphased build's report.
    """
    started = time.perf_counter()
    bundle, output, report_dir = (
        bundle.resolve(),
        output.resolve(),
        report_dir.resolve(),
    )
    record = read_bundle_record(bundle)
    admit_bundle_code(record, bundle)
    admit_bundle_mode(record, bundle, diagnostic=diagnostic, registers=registers)
    admit_catalog_outputs(output, report_dir, (bundle,), publishable=record.publishable)
    if report_dir.exists():
        raise ValueError(f"the report directory must be new: {report_dir}")
    # Captured before the load, as `build_catalog` captures it before resolving.
    revision = builder_commit() if record.publishable else None
    resolved = load_resolved_bundle(
        bundle, record, output=output, report_dir=report_dir
    )
    report_dir.mkdir(parents=True)
    shutil.copyfile(bundle / EVENTS_FILE, resolved.ledger)
    result = materialize_build(resolved, revision=revision)
    with _retain_completed_artifact(result, report_dir):
        _emit_timing("pipeline: total", started)
    return result


def materialize_build(
    resolved: ResolvedBuild, *, revision: str | None
) -> dict[str, object]:
    """Place a resolved build and finish its report.

    A strict build with errors places nothing. `revision` is the builder commit
    captured before resolution; a publishable build refuses one that moved.
    Events go to the ledger as a second gzip member, written only if any.
    """
    build_result = dict(resolved.build_result)
    output = resolved.output
    with _retain_completed_artifact(build_result, resolved.report_dir):
        try:
            if resolved.diagnostic or build_result["status"] != "blocked":
                phase_started = time.perf_counter()
                identity = {}
                if resolved.publishable:
                    assert revision is not None
                    if builder_commit() != revision:
                        raise ValueError("Builder revision changed during compilation")
                    identity = {
                        "builder_commit": revision,
                    }
                write_resolved_catalog(
                    resolved.variables,
                    output,
                    diagnostic=resolved.diagnostic,
                    scoped=resolved.scoped,
                    corpus=resolved.publishable,
                    manifest={**identity, **resolved.manifest},
                    parent_registers=resolved.parent_registers,
                    parent_variants=resolved.parent_variants,
                    editions=resolved.editions,
                    classifications=resolved.classifications,
                    classification_successions=resolved.classification_successions,
                    metadata=resolved.metadata,
                    data_warnings=resolved.data_warnings,
                    search_pins=resolved.search_pins,
                )
                build_result.update(
                    status="diagnostic_complete" if resolved.diagnostic else "complete",
                    database=str(output),
                )
                _emit_timing("pipeline: database materialization", phase_started)
                if resolved.diagnostic and not resolved.scoped:
                    # Structural validation already passed before placement. Keep
                    # the unchanged corpus safeguards visible on partial output.
                    validation = validate_built_db(output, corpus=True)
                    corpus_report: dict[str, object] = {
                        "passed": validation.passed,
                        "failures": validation.failures,
                    }
                    build_result["corpus_validation"] = corpus_report
                    with (
                        resolved.ledger.open("ab") as raw_events,
                        gzip.GzipFile(
                            filename="", mode="wb", fileobj=raw_events, mtime=0
                        ) as compressed_events,
                        TextIOWrapper(compressed_events, encoding="utf-8") as events,
                    ):
                        events.write(ledger_line("corpus_validation", corpus_report))
        except Exception as exc:
            if build_result.get("database"):
                raise
            write_summary(
                resolved.report_dir,
                {
                    "status": "engineering_failure",
                    "error": str(exc),
                    "counts": build_result["counts"],
                },
            )
            raise
        write_summary(resolved.report_dir, build_result)
    return build_result
