"""Exercise the actual builder connection using a complete prepared fixture."""

from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import pytest
from _csv_fixtures import _var_row, timeseries_row, write_input_bundle, write_scb_input
from _prepared_fixtures import accept_prepared
from _sos_fixtures import (
    BU_SPEC_LINED,
    BU_SPEC_MEMBERS,
    BU_SPEC_WRAPPED,
    inline_value_set_register,
    write_sos_input,
)
from reg_meta.errors import EXIT_CONFIG, EXIT_OUTPUT, EXIT_USAGE
from reg_meta_build.catalog_dependencies import CatalogDependencyError
from reg_meta_build.cli import run
from reg_meta_build.concept_groups import CodeLabelPair
from reg_meta_build.convert_errata import capture_expectations
from reg_meta_build.curation_tree import load_classifications
from reg_meta_build.input_snapshot import _git, input_bundle_repository
from reg_meta_build.pipeline import (
    CodebookDeclaration,
    PipelineSelection,
    ScopeDeclarations,
    ScopeFile,
    UnappliedCuration,
    build_selected_catalog,
)
from reg_meta_build.prepared_catalog import (
    open_prepared_catalog_sources,
    prepare_catalog_sources,
)
from reg_meta_build.resolved_catalog import ResolvedCodeSet
from reg_meta_build.resolved_metadata import (
    ResolvedMetadata,
    ResolvedTag,
    ResolvedTagMember,
    ResolvedVariableSameAs,
)
from reg_meta_build.source_coordinates import (
    native_parent_key,
    native_variable_key,
)
from reg_meta_build.source_curation import (
    CheckedFieldChange,
    CurationCase,
    FieldExpectation,
    OccurrenceCorrectionDecision,
    PeerGuard,
)
from reg_meta_build.source_effects import record_ref
from reg_meta_build.source_naming import (
    AcceptedNamingEntry,
    NamingAmbiguity,
    NamingDeclaration,
    NativeNamingTarget,
)
from reg_meta_build.source_records import NativeCoordinates, canonical_sha256
from reg_meta_build.validate import validate_built_db

from reg_meta_build.fqid_slugs import SlugEntry

# The SOS `Värdemängd` cells the inline code-list classifier decides between: a
# newline-delimited list resolves to its members, the wrapped one does not.
_SOS_CELLS = {"sos_lined": BU_SPEC_LINED, "sos_wrapped": BU_SPEC_WRAPPED}
_SCB_SLUGS = {"register": "sample", "register_variant": "people", "variable": "value"}
_SOS_SLUGS = {"register": "kodregister", "register_variant": "vy-a", "variable": "spec"}

# The sentinel-build fixture: the classifications seed carries the canonical book
# (codes 2/3/4) with its curated sentinel; the SOS `SPEC` variable declares that
# book and observes one extra bulk/missing token (`9`).
_SENTINEL_SHORT_NAME = "INSATS"
_SENTINEL_BOOK = (
    '[classification]\nshort_name = "INSATS"\nslug = "insats"\nname = "Insats"\n'
    'codes_file = "insats.csv"\n'
    'sentinel_codes = [{code = "9", meaning = "ej aktuellt"}]\n'
)
_SENTINEL_CSV = "code,label\n2,Miljo\n3,Beteende\n4,Bada\n"
_SENTINEL_CELL = f"{BU_SPEC_LINED}\n9 = ej aktuellt"


def _scope(records, *, slugs, revision, cases=(), register_key=None):
    """One scope, whole-source unless a `register_key` is given, declaring every
    native target its records name."""
    provider = records[0].subject.provider
    variables = sorted(
        {key for key in map(native_variable_key, records) if key is not None}, key=repr
    )
    # A renumbered native variable is a catalog variable of its own and needs its
    # own slug; a scope naming one variable keeps the plain fixture slug.
    variable_slugs = {
        key: slugs["variable"] if index == 0 else f"{slugs['variable']}-{index}"
        for index, key in enumerate(variables)
    }
    targets = {("variable", key) for key in variables}
    for record in records:
        for parent in record.parent_facts:
            if parent.kind in {"register", "variant"}:
                targets.add(
                    (
                        "register" if parent.kind == "register" else "register_variant",
                        native_parent_key(record.source, provider, parent),
                    )
                )
    # A scope may name several registers; each parent kind keeps the plain
    # fixture slug for its first target and takes a suffixed slug after that,
    # exactly like renumbered variables do above.
    parent_slugs = {
        (kind, key): slugs[kind] if index == 0 else f"{slugs[kind]}-{index}"
        for kind in ("register", "register_variant")
        for index, key in enumerate(
            sorted({key for k, key in targets if k == kind}, key=repr)
        )
    }

    def _target_register(kind, key):
        if kind == "register":
            return None
        marker = "variable" if kind == "variable" else "variant"
        return key[: key.index(marker)]

    def _source_id(kind, key):
        if kind == "register":
            return str(key[-1])
        return f"{_target_register(kind, key)[-1]}.{key[-1]}"

    return ScopeDeclarations(
        source=revision.dataset,
        register_key=register_key,
        cases=cases,
        naming=tuple(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind=kind,
                    provider=provider,
                    source_key=key,
                    register_key=_target_register(kind, key),
                    identity_revision=revision,
                ),
                naming=SlugEntry(
                    kind=kind,
                    provider=provider,
                    source_id=_source_id(kind, key),
                    slug=variable_slugs[key]
                    if kind == "variable"
                    else parent_slugs[(kind, key)],
                ),
                contributors=(),
            )
            for kind, key in sorted(targets, key=repr)
        ),
        provider_keys=tuple((key, str(key[-1])) for key in variables),
    )


def _sos_flag_cases(records):
    """The flags the SOS format has no column for, as the reviewed curation a
    real build carries. Nothing here touches the delivered `Värdemängd` cell."""
    fields = ("sensitivity", "identifier")
    occurrences = tuple(r for r in records if native_variable_key(r) is not None)
    return (
        CurationCase(
            case_id="accepted-errata:sos-declared-flags",
            targets=capture_expectations(occurrences, fields=fields),
            decision=OccurrenceCorrectionDecision(
                reviewed=True,
                effects=tuple(
                    CheckedFieldChange(
                        ref=record_ref(record),
                        replacement=FieldExpectation(
                            name=name, status="value", value=False
                        ),
                    )
                    for record in occurrences
                    for name in fields
                ),
                reason="The synthetic workbook omits both catalog flags.",
                provenance="fixture declaration",
            ),
        ),
    )


@pytest.fixture
def structural_validation_only(monkeypatch):
    """Retain real structural validation and publication, minus the real-corpus
    floors no fixture this small can meet."""
    from reg_meta_build import resolved_catalog

    validate = resolved_catalog.validate_built_db
    monkeypatch.setattr(
        resolved_catalog,
        "validate_built_db",
        lambda path, *, corpus: validate(path, corpus=False),
    )


def _input_revision(manifest, role):
    """The revision of the one prepared input filling this role."""
    return next(e.revision for e in manifest.inputs if e.role == role and e.revision)


def _scope_file(directory, name, scope):
    """Write one scope payload and pin it exactly as the selection requires."""
    payload = gzip.compress(scope.model_dump_json().encode(), mtime=0)
    (directory / name).write_bytes(payload)
    return ScopeFile(
        source=scope.source,
        register_key=scope.register_key,
        path=name,
        sha256=hashlib.sha256(payload).hexdigest(),
    )


@pytest.fixture
def selection(tmp_path, request, monkeypatch):
    param = getattr(request, "param", None)
    source = tmp_path / "source"
    # SCB renumbered one delivered column: two native variables share the summary's
    # whole literal key and separate only on their declared version endpoints. Only
    # the later summary row declares an identifier, so the built flags say which
    # native variable each row reached.
    renumbered = param == "renumbered"
    # Two registers, one scope file each, referring to each other: OTHERREG's
    # source label and a replacement event name TESTREG, and curation declares
    # the two variables the same. A tag lies wholly inside the second register.
    # The dangling variant also refers to what no register holds: a replacement
    # into native ID 404 and an external source label.
    dangling = param == "register_dangling"
    register_scoped = param in {"register_scoped", "events_outside"} or dangling
    lineage_warning = (
        param in {"lineage_warning", "cross_register_ack"} or register_scoped
    )
    columnless = param == "columnless"
    sentinel = param == "sentinel"
    write_scb_input(
        source,
        registerinformation_rows=[
            _var_row(
                cvid=1001,
                var_id=101,
                colname="VALUE",
                data_type="int"
                if param
                in {
                    "typed",
                    "renumbered",
                    "lineage_warning",
                    "cross_register_ack",
                    "register_scoped",
                    "register_dangling",
                    "columnless",
                    "coding_gap",
                    "sentinel",
                    *_SOS_CELLS,
                }
                else "",
            ),
            *(
                [
                    _var_row(
                        cvid=1002,
                        var_id=101,
                        colname="VALUE",
                        data_type="int",
                        year="2021",
                        regver_id=111,
                    )
                ]
                if param == "coding_gap"
                else []
            ),
            *(
                [
                    _var_row(
                        cvid=1002,
                        var_id=102,
                        colname="VALUE",
                        data_type="int",
                        year="2021",
                        regver_id=111,
                    )
                ]
                if renumbered
                else []
            ),
            *(
                [
                    _var_row(
                        cvid=2001,
                        var_id=102,
                        # Quoted empty: a delivered blank Kolumnnamn (SCB
                        # states the member has no physical column). An
                        # unquoted empty field would read as undelivered.
                        colname='""',
                        varname="ColumnlessVar",
                        data_type="int",
                    ),
                    # The same variable in a second variant: resolution runs
                    # per variant, but the omission is one warning per variable.
                    _var_row(
                        cvid=2002,
                        var_id=102,
                        colname='""',
                        varname="ColumnlessVar",
                        data_type="int",
                        register=("TESTREG", 1, 11),
                    ),
                ]
                if columnless
                else []
            ),
            *(
                [
                    _var_row(
                        cvid=2001,
                        var_id=201,
                        colname="OTHCOL",
                        varname="OtherVar",
                        varsource="NOSUCH" if dangling else "TESTREG",
                        data_type="int",
                        register=("OTHERREG", 2, 20),
                    )
                ]
                if lineage_warning
                else []
            ),
        ],
        timeseries_rows=[
            timeseries_row(entitet="AktuellVariabel", id1="1001", id2="404")
        ]
        if param in {"source_event", "source_event_sos"}
        else [
            timeseries_row(
                entitet="AktuellVariabel",
                id1="2001" if param == "events_outside" else "1001",
                id2="2001",
            ),
            *(
                [timeseries_row(entitet="AktuellVariabel", id1="2001", id2="404")]
                if dangling
                else []
            ),
        ]
        if register_scoped
        else None,
        unika_rows=[
            "TESTREG|Testregistret|Individer|Individer|GenericVar|VALUE|2020|2020|0|0|0",
            *(
                [
                    "TESTREG|Testregistret|Individer|Individer|GenericVar|VALUE|2021|2021|0|0|0"
                ]
                if param == "coding_gap"
                else []
            ),
            *(
                [
                    "TESTREG|Testregistret|Individer|Individer|GenericVar|VALUE|2021|2021|0|0|1"
                ]
                if renumbered
                else []
            ),
            *(
                [
                    "OTHERREG|Testregistret|Individer|Individer|OtherVar|OTHCOL|2020|2020|0|0|0"
                ]
                if lineage_warning
                else []
            ),
        ],
        include=("registerinformation", "unika", "timeseries")
        if lineage_warning
        else (
            ("registerinformation", "unika", "identifierare", "timeseries")
            if param is not False
            else ("registerinformation",)
        ),
    )
    if sentinel:
        write_sos_input(
            source,
            registers=(
                inline_value_set_register(
                    _SENTINEL_CELL,
                    external_classification=_SENTINEL_SHORT_NAME,
                ),
            ),
        )
    elif param in _SOS_CELLS or param == "source_event_sos":
        write_sos_input(
            source,
            registers=(
                inline_value_set_register(_SOS_CELLS.get(param, BU_SPEC_LINED)),
            ),
        )
    if param == "unbound_values":
        write_scb_input(
            source,
            include=("vardemangder", "valid_dates"),
            vardemangder_rows=["List|1|1|One|broken|5001"],
            valid_dates_rows=["5001|2020-01-01|2020-12-31"],
        )
    if param == "coding_gap":
        write_scb_input(
            source,
            include=("vardemangder", "valid_dates"),
            vardemangder_rows=["Earlier list|1|1|One|1001|5001"],
            valid_dates_rows=["5001|2019-01-01|2019-12-31"],
        )
    if sentinel:
        class_dir = source / "classifications"
        class_dir.mkdir()
        (class_dir / "insats.csv").write_text(_SENTINEL_CSV, encoding="utf-8")
    bundle = write_input_bundle(tmp_path / "inputs", source)
    destination = tmp_path / "prepared" / "catalog"
    manifest = prepare_catalog_sources(bundle, destination)
    commit = accept_prepared(destination)
    prepared = open_prepared_catalog_sources(
        destination, input_commit=commit, expected_sha256=manifest.sha256
    )
    revision = _input_revision(manifest, "scb_records")
    records = tuple(prepared.records.iter_records(source=revision.dataset))
    directory = tmp_path / "selection"
    directory.mkdir()
    scb_scope = _scope(records, slugs=_SCB_SLUGS, revision=revision)
    if param == "cross_register_ack":
        other = next(
            item.target.source_key
            for item in scb_scope.naming
            if item.target.kind == "register" and item.naming.slug == "sample-1"
        )
        scb_scope = scb_scope.model_copy(
            update={
                "provider_keys": tuple(
                    (key, None if key[: len(other)] == other else value)
                    for key, value in scb_scope.provider_keys
                )
            }
        )
    scopes = [
        _scope_file(
            directory,
            "scope.json.gz",
            scb_scope,
        )
    ]
    if register_scoped:
        # The whole-source scope's own slugs, register by register.
        scopes = [
            _scope_file(
                directory,
                f"scope-{index}.json.gz",
                _scope(
                    members,
                    slugs={
                        kind: f"{slug}-{index}" if index else slug
                        for kind, slug in _SCB_SLUGS.items()
                    },
                    revision=revision,
                    register_key=key,
                ),
            )
            for index, (key, members) in enumerate(
                sorted(
                    prepared.records.iter_register_slices(revision.dataset),
                    key=lambda item: repr(item[0]),
                )
            )
        ]
    if param in _SOS_CELLS or sentinel or param == "source_event_sos":
        sos_revision = _input_revision(manifest, "sos_workbook")
        sos_records = tuple(prepared.records.iter_records(source=sos_revision.dataset))
        scopes.append(
            _scope_file(
                directory,
                "sos-scope.json.gz",
                _scope(
                    sos_records,
                    slugs=_SOS_SLUGS,
                    revision=sos_revision,
                    cases=_sos_flag_cases(sos_records),
                ),
            )
        )
    classifications: tuple[CodebookDeclaration, ...] = ()
    if sentinel:
        descriptor = next(
            descriptor.payload_key
            for values in prepared.value_sources
            if values.manifest.revision is not None
            and values.manifest.revision.dataset == "classifications/insats.csv"
            for descriptor in values.descriptors()
        )
        # Pass the curation reader's validated sentinels through as the plain
        # `{code, meaning}` tables a selection carries.
        curation = tmp_path / "curation" / "classifications"
        curation.mkdir(parents=True)
        (curation / f"{_SENTINEL_SHORT_NAME}.toml").write_text(
            _SENTINEL_BOOK, encoding="utf-8"
        )
        (entry,) = load_classifications(curation.parent)
        book = entry.classification
        classifications = (
            CodebookDeclaration(
                source="classifications/insats.csv",
                descriptor=descriptor,
                metadata={
                    **book.model_dump(
                        mode="json", exclude={"codes_file", "sentinel_codes"}
                    ),
                    "sentinel_codes": [
                        sentinel.model_dump() for sentinel in book.sentinel_codes
                    ],
                },
            ),
        )
    selected = PipelineSelection(
        prepared_path=str(destination),
        prepared_commit=commit,
        prepared_sha256=manifest.sha256,
        identifier_sources=tuple(
            e.revision.dataset
            for e in manifest.inputs
            if e.revision and e.path == "Identifierare.csv"
        ),
        event_sources=tuple(
            (e.revision.dataset, revision.dataset)
            for e in manifest.inputs
            if e.revision and e.path == "Timeseries.csv"
        ),
        scopes=tuple(scopes),
        classifications=classifications,
        metadata=ResolvedMetadata(
            variable_same_as=(
                ResolvedVariableSameAs(a="scb/sample/value", b="scb/sample-1/value-1"),
            ),
            tags=(
                ResolvedTag(
                    slug="other",
                    label="Other",
                    members=(
                        ResolvedTagMember(
                            target="scb/sample-1/value-1", rank=1, starred=False
                        ),
                    ),
                ),
            ),
        )
        if register_scoped
        else ResolvedMetadata(),
    )
    path = directory / "selection.json"
    path.write_text(selected.model_dump_json())
    # Legacy pipeline assertions exercise their individual resolver paths. Hybrid
    # compilation has separate tests below with a real tracked tree.
    from reg_meta_build.curation_compile import CompiledCuration, compile_curation

    from reg_meta_build import _curation, pipeline

    curation = tmp_path / "curation"
    (curation / "classifications").mkdir(parents=True, exist_ok=True)
    from reg_meta_build.curation_compile import _naming_source_id

    parent_members = {}
    for source in {item.source for item in (scb_scope,)} | {
        ScopeDeclarations.model_validate_json(
            gzip.decompress((directory / item.path).read_bytes())
        ).source
        for item in scopes
    }:
        for record in prepared.records.iter_records(source=source):
            for parent in record.parent_facts:
                if parent.kind == "variant":
                    key = native_parent_key(
                        record.source, record.subject.provider, parent
                    )
                    assert parent.variant is not None
                    parent_members[key] = parent.variant.name
    for item in scopes:
        declaration_scope = ScopeDeclarations.model_validate_json(
            gzip.decompress((directory / item.path).read_bytes())
        )
        for register in (
            name for name in declaration_scope.naming if name.target.kind == "register"
        ):
            target = register.target
            slug = register.naming.slug
            assert slug is not None and target.provider is not None
            register_path = curation / "registers" / target.provider / f"{slug}.toml"
            register_path.parent.mkdir(parents=True, exist_ok=True)
            lines = [
                "[register]",
                f"provider = {json.dumps(target.provider)}",
                f"slug = {json.dumps(slug)}",
                f"native_id = {json.dumps(_naming_source_id('register', target.source_key))}",
            ]
            for name in declaration_scope.naming:
                if name.target.register_key != target.source_key:
                    continue
                kind = name.target.kind
                if kind not in {"register_variant", "variable"}:
                    continue
                member = (
                    parent_members.get(name.target.source_key)
                    if kind == "register_variant"
                    else None
                )
                source_id = _naming_source_id(
                    kind, name.target.source_key, member=member
                )
                lines.extend(
                    (
                        "",
                        "[[variant]]" if kind == "register_variant" else "[[variable]]",
                        f"native_id = {json.dumps(source_id)}",
                    )
                )
                if name.naming.slug is not None:
                    lines.append(f"slug = {json.dumps(name.naming.slug)}")
            register_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    if register_scoped:
        (curation / "relations.toml").write_text(
            '[[edge]]\ntype = "same_as"\na = "scb/sample/value"\n'
            'b = "scb/sample-1/value-1"\n',
            encoding="utf-8",
        )
        (curation / "tags.toml").write_text(
            '[[tag]]\nslug = "other"\nlabel = "Other"\n'
            '[[tag.member]]\nvariable = "scb/sample-1/value-1"\n'
            "rank = 1\nstarred = false\n",
            encoding="utf-8",
        )
    monkeypatch.setattr(_curation, "repo_curation_dir", lambda: curation)
    monkeypatch.setattr(
        pipeline,
        "compile_curation",
        lambda tree, prepared, scopes, *, subset=False: CompiledCuration(
            fields={},
            cases=compile_curation(tree, prepared, scopes, subset=subset).cases,
            report={},
            naming={
                (scope.source, scope.register_key): scope.naming for scope in scopes
            },
            variants={
                (scope.source, scope.register_key): scope.variants for scope in scopes
            },
        ),
    )
    return path


@pytest.mark.parametrize("selection", ["source_event"], indirect=True)
def test_native_source_event_is_resolved_and_missing_endpoint_is_reported(
    selection, tmp_path
):
    output, report = tmp_path / "event.db", tmp_path / "report"
    result = build_selected_catalog(
        selection, output=output, report_dir=report, diagnostic=True
    )
    assert result["status"] == "diagnostic_complete"
    with gzip.open(report / "events.jsonl.gz", "rt") as stream:
        issues = [
            json.loads(line)
            for line in stream
            if '"unresolved_source_event_endpoint"' in line
        ]
    assert len(issues) == 1 and issues[0]["severity"] == "error"
    assert "404" in issues[0]["detail"]
    with sqlite3.connect(output) as conn:
        assert conn.execute("SELECT COUNT(*) FROM variable").fetchone()[0] == 1
        assert (
            conn.execute("SELECT COUNT(*) FROM variable_replaced_by").fetchone()[0] == 0
        )


def test_real_build_command_writes_nonpublishable_full_selection(
    selection, tmp_path, capsys, monkeypatch
):
    monkeypatch.delenv("REG_META_BUILD_TIMING", raising=False)
    output, report = tmp_path / "diagnostic.db", tmp_path / "report"
    status = run(
        [
            "build-db",
            "--selection",
            str(selection),
            "--report-dir",
            str(report),
            "--diagnostic",
            "--timing",
            "--diagnostic-db-path",
            str(output),
        ]
    )
    captured = capsys.readouterr()
    assert "[timing] pipeline: total:" in captured.err
    result = json.loads(captured.out)
    assert status == EXIT_CONFIG
    assert result["status"] == "diagnostic_complete"
    assert result["publication_ready"] is False
    assert result["corpus_validation"]["passed"] is False
    assert result["corpus_validation"]["failures"]
    assert result["counts"]["physical_occurrences"] == 1
    with sqlite3.connect(output) as conn:
        assert conn.execute("SELECT COUNT(*) FROM variable").fetchone()[0] == 1
        flags = dict(conn.execute("SELECT key, value FROM import_manifest"))
        assert flags["catalog_publishable"] == "false"
        assert flags["catalog_completeness"] == "incomplete"
        assert (
            conn.execute("SELECT COUNT(*) FROM identifier_semantics").fetchone()[0] == 1
        )
        assert conn.execute("SELECT COUNT(*) FROM timeseries_event").fetchone()[0] > 0
    with gzip.open(report / "events.jsonl.gz", "rt") as stream:
        events = [json.loads(line) for line in stream]
    assert sum(e["kind"] == "source_occurrence" for e in events) == 1
    assert any(e["kind"] == "issue" and e["severity"] == "error" for e in events)
    assert json.loads((report / "summary.json").read_text()) == result


@pytest.mark.parametrize(
    "overrides",
    [
        ["--input-dir", "raw"],
        ["--skip-slugs"],
        ["--no-validate"],
        ["--providers", "scb"],
        ["--trace-scb-cvids", "1001"],
        ["--scb-value-prestage-cache", "cache.sqlite"],
    ],
)
def test_build_command_has_no_legacy_or_validation_bypass(overrides):
    assert (
        run(
            [
                "build-db",
                "--selection",
                "selection.json",
                "--report-dir",
                "report",
                *overrides,
            ]
        )
        == EXIT_USAGE
    )


@pytest.mark.parametrize(
    "options",
    [
        ["--diagnostic"],
        ["--diagnostic-db-path", "comparison.db"],
        ["--diagnostic", "--diagnostic-db-path", "comparison.db", "--db", "active"],
        ["--registers", "1"],
        ["--registers", ""],
        ["--registers", "1,,2", "--diagnostic", "--diagnostic-db-path", "x.db"],
    ],
)
def test_partial_build_commands_require_valid_separate_output(options):
    assert (
        run(
            [
                "build-db",
                "--selection",
                "selection.json",
                "--report-dir",
                "report",
                *options,
            ]
        )
        == EXIT_USAGE
    )


def test_cli_summary_cannot_overwrite_selected_declarations(selection, tmp_path):
    original = selection.read_bytes()
    status = run(
        [
            "--output",
            str(selection),
            "build-db",
            "--selection",
            str(selection),
            "--report-dir",
            str(tmp_path / "report"),
        ]
    )
    assert status == EXIT_USAGE
    assert selection.read_bytes() == original


@pytest.mark.parametrize("member", ["selection", "scope", "manifest", "records"])
@pytest.mark.parametrize("link", ["hard", "symbolic"])
def test_cli_summary_temporary_alias_cannot_overwrite_inputs(
    selection, tmp_path, member, link
):
    selected = PipelineSelection.model_validate_json(selection.read_bytes())
    prepared = Path(selected.prepared_path)
    target = {
        "selection": selection,
        "scope": selection.parent / selected.scopes[0].path,
        "manifest": prepared / "manifest.json",
        "records": prepared / "files/records/files/records.sqlite",
    }[member]
    original = target.read_bytes()
    summary = tmp_path / "summary.json"
    temporary = summary.with_suffix(".json.tmp")
    if link == "hard":
        temporary.hardlink_to(target)
    else:
        temporary.symlink_to(target)
    report = tmp_path / "report"
    assert (
        run(
            [
                "--output",
                str(summary),
                "build-db",
                "--selection",
                str(selection),
                "--report-dir",
                str(report),
            ]
        )
        == EXIT_USAGE
    )
    assert not report.exists()
    assert target.read_bytes() == original


@pytest.mark.parametrize("backup", [False, True])
def test_catalog_paths_cannot_alias_prepared_inputs(selection, tmp_path, backup):
    selected = PipelineSelection.model_validate_json(selection.read_bytes())
    target = Path(selected.prepared_path) / "files/records/files/records.sqlite"
    original = target.read_bytes()
    output = tmp_path / "catalog.db"
    alias = Path(str(output) + ".prev") if backup else output
    alias.hardlink_to(target)
    with pytest.raises(ValueError, match="must not alias selected inputs"):
        build_selected_catalog(selection, output, tmp_path / "report")
    assert target.read_bytes() == original
    assert not (tmp_path / "report").exists()


def test_late_cli_summary_failure_reports_completed_artifact(
    selection, tmp_path, capsys, monkeypatch
):
    from reg_meta_build import cli

    def fail(*args, **kwargs):
        raise OSError("late summary failure")

    monkeypatch.setattr(cli, "write_json", fail)
    output, report = tmp_path / "diagnostic.db", tmp_path / "report"
    assert (
        run(
            [
                "build-db",
                "--selection",
                str(selection),
                "--report-dir",
                str(report),
                "--diagnostic",
                "--diagnostic-db-path",
                str(output),
            ]
        )
        == EXIT_OUTPUT
    )
    receipt = json.loads(capsys.readouterr().err.splitlines()[-1])["error"]
    assert receipt["artifact_complete"] is True
    assert receipt["status"] == "diagnostic_complete"
    assert receipt["publication_ready"] is False
    assert receipt["curation_exit_code"] == EXIT_CONFIG
    assert receipt["database"] == str(output)
    assert json.loads((report / "summary.json").read_text())["database"] == str(output)


@pytest.mark.parametrize("selection", ["typed"], indirect=True)
def test_pipeline_artifact_dates_its_pinned_preparation_and_boots_backend(
    selection, tmp_path, monkeypatch
):
    from fastapi.testclient import TestClient
    from reg_webapp.app import create_app

    selected = PipelineSelection.model_validate_json(selection.read_bytes())
    committed = _git(
        input_bundle_repository(Path(selected.prepared_path)),
        "show",
        "-s",
        "--format=%cI",
        selected.prepared_commit,
    )
    expected = datetime.fromisoformat(committed).astimezone(UTC)
    output = tmp_path / "artifact" / "reg_meta.db"
    build_selected_catalog(selection, output, tmp_path / "report", diagnostic=True)
    monkeypatch.setenv("REG_META_DB", str(output.parent))
    monkeypatch.delenv("REG_WEBAPP_STEWARD", raising=False)
    monkeypatch.delenv("REG_WEBAPP_DELIVERY_INVENTORY", raising=False)
    with TestClient(create_app()) as client:
        response = client.get("/api/context")
    assert response.status_code == 200
    import_date = response.json()["reg_meta"]["import_date"]
    assert import_date.endswith("Z")
    assert datetime.fromisoformat(import_date) == expected


@pytest.mark.parametrize("selection", ["typed"], indirect=True)
@pytest.mark.parametrize("diagnostic", [False, True])
@pytest.mark.parametrize("failure", ["summary", "events", "summary_and_cli"])
def test_internal_report_failure_retains_completed_artifact_receipt(
    selection,
    tmp_path,
    monkeypatch,
    capsys,
    diagnostic,
    failure,
    structural_validation_only,
):
    from reg_meta_build import cli, pipeline

    output, report = tmp_path / "artifact" / "reg_meta.db", tmp_path / "report"
    if not diagnostic:
        output.parent.mkdir()
        output.write_bytes(b"previous catalog")
    original_write = Path.write_text
    original_open = gzip.open

    def fail_summary(path, *args, **kwargs):
        if path == report / "summary.json":
            raise OSError("internal summary failure")
        return original_write(path, *args, **kwargs)

    @contextmanager
    def fail_events(*args, **kwargs):
        with original_open(*args, **kwargs) as stream:
            yield stream
        if Path(args[0]) == report / "events.jsonl.gz":
            raise OSError("event close failure")

    if failure.startswith("summary"):
        monkeypatch.setattr(Path, "write_text", fail_summary)
    else:
        monkeypatch.setattr(pipeline.gzip, "open", fail_events)
    if failure == "summary_and_cli":

        def fail_cli(*args, **kwargs):
            raise OSError("CLI summary failure")

        monkeypatch.setattr(cli, "write_json", fail_cli)
    options = (
        ["--diagnostic", "--diagnostic-db-path", str(output)]
        if diagnostic
        else ["--db", str(output.parent)]
    )
    assert (
        run(
            [
                "build-db",
                "--selection",
                str(selection),
                "--report-dir",
                str(report),
                *options,
            ]
        )
        == EXIT_OUTPUT
    )
    captured = capsys.readouterr()
    receipt = json.loads(
        captured.err.splitlines()[-1] if failure == "summary_and_cli" else captured.out
    )["error"]
    assert receipt["code"] == "pipeline_report_failed"
    assert receipt["artifact_complete"] is True
    assert receipt["status"] == ("diagnostic_complete" if diagnostic else "complete")
    assert receipt["publication_ready"] is (not diagnostic)
    assert receipt["database"] == str(output)
    assert receipt["curation_exit_code"] == (EXIT_CONFIG if diagnostic else 0)
    assert receipt["report_dir"] == str(report)
    with sqlite3.connect(output) as conn:
        assert conn.execute("SELECT COUNT(*) FROM variable").fetchone()[0] == 1
    if not diagnostic:
        assert Path(str(output) + ".prev").read_bytes() == b"previous catalog"


@pytest.mark.parametrize("selection", ["unbound_values"], indirect=True)
def test_unbindable_value_rows_are_reported_without_a_target_occurrence(
    selection, tmp_path
):
    report = tmp_path / "report"
    result = build_selected_catalog(
        selection, tmp_path / "diagnostic.db", report, diagnostic=True
    )
    assert result["status"] == "diagnostic_complete"
    assert result["counts"]["value_associations"] == 1
    with gzip.open(report / "events.jsonl.gz", "rt") as stream:
        events = [json.loads(line) for line in stream]
    (problem,) = [e for e in events if e["kind"] == "value_source_issue"]
    assert problem["code"] == "unknown_native_member_token"
    assert problem["raw_member_tokens"] == ["broken"]
    assert problem["occurrence_count"] == 1
    assert any(
        e["kind"] == "issue"
        and e["code"] == problem["code"]
        and e["severity"] == "error"
        for e in events
    )


@pytest.mark.parametrize(
    ("selection", "resolved"),
    [("sos_lined", True), ("sos_wrapped", False)],
    indirect=["selection"],
)
def test_unresolved_inline_code_list_is_reported_and_refused_for_publication(
    selection, tmp_path, resolved, structural_validation_only
):
    # The delivered `Värdemängd` cell is the only difference between the
    # publishable build and the refused one.
    strict_db = tmp_path / "catalog.db"
    strict = build_selected_catalog(selection, strict_db, tmp_path / "report")
    diagnostic_db, diagnostic_report = tmp_path / "diagnostic.db", tmp_path / "diag"
    diagnostic = build_selected_catalog(
        selection, diagnostic_db, diagnostic_report, diagnostic=True
    )
    with gzip.open(diagnostic_report / "events.jsonl.gz", "rt") as stream:
        issues = [
            event
            for event in (json.loads(line) for line in stream)
            if event["kind"] == "issue" and event["code"] == "unresolved_member_list"
        ]
    with sqlite3.connect(diagnostic_db) as conn:
        codes = conn.execute(
            "SELECT code, label FROM value_code ORDER BY code"
        ).fetchall()
        variables = conn.execute("SELECT COUNT(*) FROM variable").fetchone()[0]

    # The diagnostic scan completes either way; the wrapped cell states no
    # membership but is reported against its own source record, so a list the
    # format cannot separate never reads as an absent coding declaration.
    assert diagnostic["status"] == "diagnostic_complete"
    assert variables == 2
    if resolved:
        assert diagnostic["counts"].get("error", 0) == 0
        assert strict["status"] == "complete"
        assert strict["publication_ready"] is True
        assert strict_db.exists()
        assert not issues
        assert codes == BU_SPEC_MEMBERS
    else:
        # The delivered cell is the fixture's only defect.
        assert diagnostic["counts"]["error"] == 1
        assert strict["status"] == "blocked"
        assert strict["publication_ready"] is False
        assert strict["database"] is None
        assert not strict_db.exists()
        (issue,) = issues
        assert issue["severity"] == "error"
        assert issue["fields"] == ["coding"]
        assert issue["withheld_output"] == ["state.value_set"]
        assert "SPEC" in issue["subject"]
        assert "inline:" in issue["detail"]
        assert issue["refs"]
        assert codes == []


def _acknowledging(selection, issue):
    """Declare the issue in the register's tracked TOML."""
    selected = PipelineSelection.model_validate_json(selection.read_bytes())
    scope_file = next(
        file for file in selected.scopes if file.source == issue["refs"][0]["source"]
    )
    scope = ScopeDeclarations.model_validate_json(
        gzip.decompress((selection.parent / scope_file.path).read_bytes())
    )
    register = next(
        item.naming for item in scope.naming if item.target.kind == "register"
    )
    directory = selection.parent.parent / "curation" / "registers" / register.provider
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{register.slug}.toml"
    refs = [
        json.dumps(ref, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        for ref in issue["refs"]
    ]
    path.write_text(
        "[register]\n"
        f"provider = {json.dumps(register.provider)}\n"
        f"slug = {json.dumps(register.slug)}\n"
        f"native_id = {json.dumps(register.source_id if register.source_id.isdecimal() else '1')}\n"
        "\n[[acknowledge]]\n"
        f"code = {json.dumps(issue['code'])}\n"
        f"subject = {json.dumps(issue['subject'])}\n"
        f"refs = {json.dumps(refs, ensure_ascii=False)}\n"
        'reason = "The delivered cell cannot separate its members."\n'
        'evidence = "Diagnostic build ledger."\n',
        encoding="utf-8",
    )
    return selection


@pytest.mark.parametrize("selection", ["sos_wrapped"], indirect=True)
def test_strict_build_publishes_only_when_every_error_is_acknowledged(
    selection, tmp_path, structural_validation_only
):
    blocked = build_selected_catalog(
        selection, tmp_path / "blocked.db", tmp_path / "blocked"
    )
    assert blocked["status"] == "blocked"
    assert blocked["acknowledged"] == {}
    with gzip.open(tmp_path / "blocked" / "events.jsonl.gz", "rt") as stream:
        (issue,) = (
            event
            for event in map(json.loads, stream)
            if event["kind"] == "issue" and event["severity"] == "error"
        )
    strict_db, strict_report = tmp_path / "catalog.db", tmp_path / "strict"
    strict = build_selected_catalog(
        _acknowledging(selection, issue), strict_db, strict_report
    )
    assert strict["status"] == "complete"
    assert strict["publication_ready"] is True
    assert strict["counts"].get("error", 0) == 0
    assert strict["acknowledged"] == {issue["code"]: 1}
    with gzip.open(strict_report / "events.jsonl.gz", "rt") as stream:
        (warning,) = (
            event
            for event in map(json.loads, stream)
            if event["kind"] == "issue" and event["acknowledged_by"]
        )
    assert warning == {
        **issue,
        "severity": "warning",
        "acknowledged_by": "curation/registers/sos/kodregister.toml#/acknowledge/1",
    }
    # Acknowledging fabricates nothing: the unseparated list stays withheld.
    with sqlite3.connect(strict_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM value_code").fetchone()[0] == 0


@pytest.mark.parametrize("selection", ["cross_register_ack"], indirect=True)
def test_register_file_cannot_acknowledge_another_registers_error(
    selection, tmp_path, structural_validation_only
):
    blocked = build_selected_catalog(
        selection, tmp_path / "blocked.db", tmp_path / "blocked"
    )
    assert blocked["status"] == "blocked"
    with gzip.open(tmp_path / "blocked" / "events.jsonl.gz", "rt") as stream:
        issue = next(
            event
            for event in map(json.loads, stream)
            if event["kind"] == "issue"
            and event["code"] == "unresolved_catalog_identity"
        )
    assert issue["refs"]
    # The helper writes sample.toml; this issue belongs to sample-1.
    _acknowledging(selection, issue)
    result = build_selected_catalog(
        selection, tmp_path / "cross.db", tmp_path / "cross"
    )
    assert result["status"] == "blocked"
    assert result["acknowledged"] == {}
    with gzip.open(tmp_path / "cross" / "events.jsonl.gz", "rt") as stream:
        issues = [
            event
            for event in map(json.loads, stream)
            if event["kind"] == "issue" and event["severity"] == "error"
        ]
    assert any(
        event["code"] == issue["code"]
        and event["subject"] == issue["subject"]
        and event["refs"] == issue["refs"]
        for event in issues
    )
    assert any(event["code"] == "stale_curation_entry" for event in issues)


@pytest.mark.parametrize("selection", ["sentinel", "register_scoped"], indirect=True)
def test_hybrid_compiler_preserves_stored_global_selection(
    selection, tmp_path, structural_validation_only, monkeypatch
):
    from reg_meta_build.curation_compile import compile_curation, tree_sha256
    from reg_meta_build.semantic_diff import diff_catalog_semantics

    from reg_meta_build import pipeline

    stored = PipelineSelection.model_validate_json(selection.read_bytes())
    baseline = tmp_path / "baseline.db"
    original = build_selected_catalog(selection, baseline, tmp_path / "baseline-report")
    assert original["status"] == "complete"
    monkeypatch.setattr(pipeline, "compile_curation", compile_curation)
    compiled = tmp_path / "compiled.db"
    dump = tmp_path / "decisions"
    result = build_selected_catalog(
        selection,
        compiled,
        tmp_path / "compiled-report",
        curation_dir=tmp_path / "curation",
        dump_decisions=dump,
    )
    assert result["status"] == "complete"
    assert result["curation_tree_sha256"] == tree_sha256(tmp_path / "curation")
    with sqlite3.connect(compiled) as conn:
        assert conn.execute(
            "SELECT value FROM import_manifest WHERE key = 'curation_tree_sha256'"
        ).fetchone() == (result["curation_tree_sha256"],)
    assert diff_catalog_semantics(baseline, compiled).identical
    declarations = json.loads((dump / "global.json").read_text())
    assert declarations["classifications"] == [
        entry.model_dump(mode="json") for entry in stored.classifications
    ]
    assert declarations["metadata"] == stored.metadata.model_dump(mode="json")
    assert declarations["code_label_pairs"] == []
    assert declarations["identifier_sources"] == list(stored.identifier_sources)
    assert declarations["event_sources"] == [
        list(pair) for pair in stored.event_sources
    ]
    second_dump = tmp_path / "decisions-again"
    build_selected_catalog(
        selection,
        tmp_path / "compiled-again.db",
        tmp_path / "compiled-report-again",
        curation_dir=tmp_path / "curation",
        dump_decisions=second_dump,
    )
    assert {p.name: p.read_bytes() for p in dump.iterdir()} == {
        p.name: p.read_bytes() for p in second_dump.iterdir()
    }


@pytest.mark.parametrize("selection", ["coding_gap"], indirect=True)
def test_tracked_uncoded_window_changes_build_and_stale_entry_is_reported(
    selection, tmp_path, monkeypatch
):
    from reg_meta_build.curation_compile import compile_curation

    from reg_meta_build import pipeline

    monkeypatch.setattr(pipeline, "compile_curation", compile_curation)
    baseline_report = tmp_path / "coding-baseline-report"
    build_selected_catalog(
        selection,
        tmp_path / "coding-baseline.db",
        baseline_report,
        diagnostic=True,
    )
    # The 2020 list does not establish coding for the later 2021 occurrence.
    assert any(
        issue["code"] == "missing_coding_period" for issue in _issues(baseline_report)
    )

    register_dir = tmp_path / "curation" / "registers" / "scb"
    register_dir.mkdir(parents=True)
    register_file = register_dir / "sample.toml"
    header = '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
    entry = (
        '\n[[coding.uncoded]]\nvariable = "1.101"\nvariant = "1.10"\n'
        'column = "VALUE"\nperiods = [["2021-01-01", "2021-12-31"]]\n'
        'reason = "The selected source has no code list in this window."\n'
        'source = "fixture"\n'
    )
    register_file.write_text(header + entry)
    applied_report = tmp_path / "coding-applied-report"
    dump = tmp_path / "coding-decisions"
    build_selected_catalog(
        selection,
        tmp_path / "coding-applied.db",
        applied_report,
        diagnostic=True,
        dump_decisions=dump,
    )
    assert not any(
        issue["code"] == "missing_coding_period" for issue in _issues(applied_report)
    )
    case_id = "curation/registers/scb/sample.toml#/coding.uncoded/1/period/1"
    assert (
        case_id
        in json.loads((dump / "compile-report.json").read_text())["scb/sample"][
            "entries_matched"
        ]
    )
    assert any(
        case["case_id"] == case_id
        for case in json.loads((dump / "scope-00000.json").read_text())["cases"]
    )

    register_file.write_text(header + entry.replace("2021-01-01", "2019-01-01"))
    stale_report = tmp_path / "coding-stale-report"
    stale_dump = tmp_path / "coding-stale-decisions"
    build_selected_catalog(
        selection,
        tmp_path / "coding-stale.db",
        stale_report,
        diagnostic=True,
        dump_decisions=stale_dump,
    )
    assert any(
        issue["code"] == "stale_curation_entry"
        and issue["case_id"] == case_id
        and issue["severity"] == "error"
        for issue in _issues(stale_report)
    )
    assert (
        case_id
        in json.loads((stale_dump / "compile-report.json").read_text())["scb/sample"][
            "stale"
        ]
    )


@pytest.mark.parametrize("selection", ["sentinel"], indirect=True)
def test_unmatched_classification_label_is_stale_only_in_full_build(
    selection, tmp_path, structural_validation_only, monkeypatch
):
    from reg_meta_build.curation_compile import compile_curation

    from reg_meta_build import pipeline

    monkeypatch.setattr(pipeline, "compile_curation", compile_curation)
    book = tmp_path / "curation" / "classifications" / f"{_SENTINEL_SHORT_NAME}.toml"
    book.write_text(
        book.read_text() + '\n[binding]\nvalue_set_labels = ["No such descriptor"]\n'
    )
    full_dump = tmp_path / "full-decisions"
    build_selected_catalog(
        selection,
        tmp_path / "full.db",
        tmp_path / "full-report",
        diagnostic=True,
        curation_dir=tmp_path / "curation",
        dump_decisions=full_dump,
    )
    ref = f"classifications/{_SENTINEL_SHORT_NAME}.toml#/binding/value_set_labels/1"
    assert any(
        issue["code"] == "stale_curation_entry" and issue["subject"] == ref
        for issue in _issues(tmp_path / "full-report")
    )
    subset_dump = tmp_path / "subset-decisions"
    build_selected_catalog(
        selection,
        tmp_path / "subset.db",
        tmp_path / "subset-report",
        diagnostic=True,
        registers=("scb-registerinformation",),
        curation_dir=tmp_path / "curation",
        dump_decisions=subset_dump,
    )
    report = json.loads((subset_dump / "compile-report.json").read_text())
    assert ref in report["_classifications"]["not_evaluated_in_subset"]
    assert not any(
        issue["code"] == "stale_curation_entry" and issue["subject"] == ref
        for issue in _issues(tmp_path / "subset-report")
    )


def test_dump_decisions_refuses_input_and_output_aliases(selection, tmp_path):
    for alias in (selection.parent, tmp_path / "report", tmp_path / "catalog.db"):
        with pytest.raises(ValueError, match="--dump-decisions"):
            build_selected_catalog(
                selection,
                tmp_path / "catalog.db",
                tmp_path / "report",
                dump_decisions=alias,
            )


@pytest.mark.parametrize("selection", ["register_scoped"], indirect=True)
def test_hybrid_subset_preserves_dependency_diagnostics_and_tag_rows(
    selection, tmp_path, monkeypatch
):
    from reg_meta_build.curation_compile import compile_curation

    from reg_meta_build import pipeline

    raw = json.loads(selection.read_bytes())
    path = "scope-0.json.gz"
    scope = ScopeDeclarations.model_validate_json(
        gzip.decompress((selection.parent / path).read_bytes())
    )
    variable_key = next(key for key, _ in scope.provider_keys)
    pinned = next(item for item in raw["scopes"] if item["path"] == path)
    pinned.update(
        _scope_file(
            selection.parent,
            path,
            scope.model_copy(update={"provider_keys": ((variable_key, None),)}),
        ).model_dump(mode="json")
    )
    selection.write_text(json.dumps(raw))
    stored_db = tmp_path / "stored.db"
    stored_report = tmp_path / "stored-report"
    build_selected_catalog(
        selection,
        stored_db,
        stored_report,
        diagnostic=True,
        registers=("1",),
    )
    monkeypatch.setattr(pipeline, "compile_curation", compile_curation)
    dump = tmp_path / "subset-decisions"
    compiled_db = tmp_path / "compiled.db"
    compiled_report = tmp_path / "compiled-report"
    build_selected_catalog(
        selection,
        compiled_db,
        compiled_report,
        diagnostic=True,
        registers=("1",),
        dump_decisions=dump,
    )
    stored_issues = _issues(stored_report)
    assert {"deferred_out_of_slice_reference", "withheld_catalog_dependency"} <= {
        item["code"] for item in stored_issues
    }
    assert _issues(compiled_report) == stored_issues
    for table in ("tag", "tag_member"):
        with (
            sqlite3.connect(stored_db) as stored,
            sqlite3.connect(compiled_db) as compiled,
        ):
            assert compiled.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall() == (
                stored.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()
            )
    with sqlite3.connect(compiled_db) as conn:
        assert conn.execute("SELECT COUNT(*) FROM tag").fetchone() == (0,)
    globals_ = json.loads((dump / "global.json").read_text())
    assert len(globals_["metadata"]["variable_same_as"]) == 1
    assert len(globals_["metadata"]["tags"][0]["members"]) == 1
    report = json.loads((dump / "compile-report.json").read_text())
    assert not any("same_as" in entry for entry in report["_subset"]["dropped"])
    assert not any("tag" in entry for entry in report["_subset"]["dropped"])


@pytest.mark.parametrize("selection", ["source_event_sos"], indirect=True)
def test_hybrid_subset_keeps_event_binding_for_scoped_resolver(
    selection, tmp_path, monkeypatch
):
    from reg_meta_build.curation_compile import compile_curation

    from reg_meta_build import pipeline

    monkeypatch.setattr(pipeline, "compile_curation", compile_curation)
    selected = PipelineSelection.model_validate_json(selection.read_bytes())
    sos_source = next(
        scope.source
        for scope in selected.scopes
        if scope.source != "scb-registerinformation"
    )
    dump = tmp_path / "sos-decisions"
    result = build_selected_catalog(
        selection,
        tmp_path / "sos.db",
        tmp_path / "sos-report",
        diagnostic=True,
        registers=(sos_source,),
        dump_decisions=dump,
    )
    assert result["status"] == "diagnostic_complete"
    globals_ = json.loads((dump / "global.json").read_text())
    assert globals_["event_sources"] == [list(pair) for pair in selected.event_sources]
    report = json.loads((dump / "compile-report.json").read_text())
    assert report["_subset"]["dropped"] == []


@pytest.mark.parametrize("selection", ["events_outside"], indirect=True)
def test_hybrid_subset_reports_source_event_wholly_outside_register(
    selection, tmp_path, monkeypatch
):
    from reg_meta_build.curation_compile import compile_curation

    from reg_meta_build import pipeline

    monkeypatch.setattr(pipeline, "compile_curation", compile_curation)
    dump = tmp_path / "decisions"
    build_selected_catalog(
        selection,
        tmp_path / "subset.db",
        tmp_path / "subset-report",
        diagnostic=True,
        registers=("1",),
        dump_decisions=dump,
    )
    report = json.loads((dump / "compile-report.json").read_text())
    assert any("#/event/" in item for item in report["_subset"]["dropped"])


@pytest.mark.parametrize("selection", ["sos_wrapped"], indirect=True)
def test_tracked_acknowledgement_matching_nothing_is_an_error(selection, tmp_path):
    build_selected_catalog(selection, tmp_path / "blocked.db", tmp_path / "blocked")
    with gzip.open(tmp_path / "blocked" / "events.jsonl.gz", "rt") as stream:
        issue = next(
            event
            for event in map(json.loads, stream)
            if event["kind"] == "issue" and event["severity"] == "error"
        )
    stale = {**issue, "code": "issue-that-is-not-emitted"}
    _acknowledging(selection, stale)
    result = build_selected_catalog(
        selection, tmp_path / "stale.db", tmp_path / "stale"
    )
    assert result["status"] == "blocked"
    with gzip.open(tmp_path / "stale" / "events.jsonl.gz", "rt") as stream:
        assert any(
            event["kind"] == "issue" and event["code"] == "stale_curation_entry"
            for event in map(json.loads, stream)
        )


def test_malformed_selection_sentinels_are_refused():
    from reg_meta_build.curation_compile import validate_sentinels

    assert validate_sentinels(None, subject="insats") == ()
    assert (
        validate_sentinels([{"code": "9", "meaning": "ej aktuellt"}], subject="insats")[
            0
        ].code
        == "9"
    )
    with pytest.raises(ValueError, match="more than once"):
        validate_sentinels(
            [
                {"code": "9", "meaning": "a"},
                {"code": "9", "meaning": "b"},
            ],
            subject="insats",
        )
    with pytest.raises(ValueError, match="sentinel_codes"):
        validate_sentinels([{"code": 9, "meaning": "ej aktuellt"}], subject="insats")


@pytest.mark.parametrize("selection", ["sentinel"], indirect=True)
def test_curated_sentinel_keeps_binding_in_strict_build(
    selection, tmp_path, structural_validation_only
):
    # The SOS `SPEC` variable declares the `INSATS` book and observes one extra
    # bulk token (`9`) the selection's `sentinel_codes` names. The binding is
    # kept with a warning, so the strict build stays publication_ready with no
    # errors; the token stays variable-local (value set, never canonical).
    strict_db, strict_report = tmp_path / "catalog.db", tmp_path / "strict-report"
    strict = build_selected_catalog(selection, strict_db, strict_report)
    assert strict["status"] == "complete"
    assert strict["publication_ready"] is True
    summary = json.loads((strict_report / "summary.json").read_text())
    assert summary["counts"].get("error", 0) == 0
    assert summary["counts"]["warning"] >= 1
    with gzip.open(strict_report / "events.jsonl.gz", "rt") as stream:
        issues = [
            event
            for event in (json.loads(line) for line in stream)
            if event["kind"] == "issue"
        ]
    assert not [issue for issue in issues if issue["severity"] == "error"]
    (warning,) = [
        issue for issue in issues if issue["code"] == "sentinel_classification_codes"
    ]
    assert warning["severity"] == "warning"
    assert warning["withheld_output"] == []
    assert warning["fields"] == ["coding", "classification"]
    assert "'9'" in warning["detail"] and "ej aktuellt" in warning["detail"]
    with sqlite3.connect(strict_db) as conn:
        (slug,) = conn.execute(
            "SELECT c.slug FROM variable v "
            "JOIN variable_state vs USING(variable_id) "
            "JOIN classification c ON vs.classification_id = c.id "
            "WHERE v.slug = 'spec'"
        ).fetchone()
        assert slug == "insats"
        members = conn.execute(
            "SELECT vc.code FROM variable v "
            "JOIN variable_state vs USING(variable_id) "
            "JOIN value_set_member vsm USING(value_set_id) "
            "JOIN value_code vc USING(code_id) "
            "WHERE v.slug = 'spec' ORDER BY vc.code"
        ).fetchall()
        assert [code for (code,) in members] == ["2", "3", "4", "9"]
        (sentinel_canonical,) = conn.execute(
            "SELECT COUNT(*) FROM classification_code cc "
            "JOIN classification c ON cc.classification_id = c.id "
            "JOIN value_code vc USING(code_id) "
            "WHERE c.slug = 'insats' AND vc.code = '9'"
        ).fetchone()
        assert sentinel_canonical == 0
        assert conn.execute(
            "SELECT status, checked_code_count, matched_code_count, "
            "nonconforming_code_count FROM classification_conformance"
        ).fetchall() == [("kept", 4, 4, 0)]


@pytest.mark.parametrize("selection", ["lineage_warning"], indirect=True)
def test_lineage_warning_withholds_only_the_edge(
    selection, tmp_path, structural_validation_only
):
    # One consumer state names a second fixture register with no accepted
    # same_as edge, so the only diagnostic is unresolved_lineage_no_source_state.
    # As a warning it must not count toward counts["error"] and must leave
    # the strict build complete and publishable.
    strict_db, strict_report = tmp_path / "catalog.db", tmp_path / "strict-report"
    strict = build_selected_catalog(selection, strict_db, strict_report)
    assert strict["status"] == "complete"
    assert strict["publication_ready"] is True
    strict_summary = json.loads((strict_report / "summary.json").read_text())
    assert strict_summary["status"] == "complete"
    assert strict_summary["publication_ready"] is True
    assert strict_summary["counts"].get("error", 0) == 0
    assert strict_summary["counts"]["warning"] >= 1
    diagnostic_db, diagnostic_report = (
        tmp_path / "diagnostic.db",
        tmp_path / "diagnostic-report",
    )
    diagnostic = build_selected_catalog(
        selection, diagnostic_db, diagnostic_report, diagnostic=True
    )
    assert diagnostic["status"] == "diagnostic_complete"
    diagnostic_summary = json.loads((diagnostic_report / "summary.json").read_text())
    assert diagnostic_summary["counts"]["warning"] >= 1
    assert diagnostic_summary["counts"].get("error", 0) == 0
    with gzip.open(diagnostic_report / "events.jsonl.gz", "rt") as stream:
        issues = [
            event
            for event in (json.loads(line) for line in stream)
            if event["kind"] == "issue"
        ]
    (issue,) = issues
    assert issue["code"] == "unresolved_lineage_no_source_state"
    assert issue["severity"] == "warning"
    assert issue["withheld_output"] == [issue["subject"] + ":lineage"]


@pytest.mark.parametrize("selection", ["columnless"], indirect=True)
def test_columnless_variable_is_omitted_as_an_explained_warning(
    selection, tmp_path, structural_validation_only
):
    # A delivered blank Kolumnnamn states the member has no physical column, so
    # the variable stays out of the catalog. The omission is a warning, so the
    # strict build still completes as publication_ready with no errors.
    strict_db, strict_report = tmp_path / "catalog.db", tmp_path / "strict-report"
    strict = build_selected_catalog(selection, strict_db, strict_report)
    assert strict["status"] == "complete"
    assert strict["publication_ready"] is True
    strict_summary = json.loads((strict_report / "summary.json").read_text())
    assert strict_summary["status"] == "complete"
    assert strict_summary["counts"].get("error", 0) == 0
    assert strict_summary["counts"]["warning"] >= 1
    with sqlite3.connect(strict_db) as conn:
        assert conn.execute("SELECT provider_key FROM variable").fetchall() == [
            ("101",)
        ]
    diagnostic_report = tmp_path / "diagnostic-report"
    diagnostic = build_selected_catalog(
        selection, tmp_path / "diagnostic.db", diagnostic_report, diagnostic=True
    )
    assert diagnostic["status"] == "diagnostic_complete"
    assert diagnostic["counts"].get("error", 0) == 0
    with gzip.open(diagnostic_report / "events.jsonl.gz", "rt") as stream:
        issues = [
            event
            for event in (json.loads(line) for line in stream)
            if event["kind"] == "issue"
        ]
    assert not [issue for issue in issues if issue["severity"] == "error"]
    (omitted,) = [
        issue for issue in issues if issue["code"] == "omitted_columnless_occurrence"
    ]
    assert omitted["severity"] == "warning"
    assert omitted["fields"] == ["column_name"]
    assert omitted["withheld_output"] == ["occurrence"]
    assert len(omitted["refs"]) == 2
    assert any(
        issue["code"] == "no_supported_states" and issue["severity"] == "warning"
        for issue in issues
    )


@pytest.mark.parametrize("selection", ["renumbered"], indirect=True)
def test_renumbered_variables_each_take_their_own_summary_flags(
    selection, tmp_path, structural_validation_only
):
    # Both summary rows carry the same four literal names; only their declared
    # version endpoints say which renumbered variable each one describes.
    output = tmp_path / "catalog.db"
    strict = build_selected_catalog(selection, output, tmp_path / "report")
    assert strict["status"] == "complete"
    assert strict["publication_ready"] is True
    with sqlite3.connect(output) as conn:
        assert conn.execute(
            "SELECT provider_key, is_sensitive, is_identifier FROM variable "
            "ORDER BY provider_key"
        ).fetchall() == [("101", 0, 0), ("102", 0, 1)]


def test_strict_curation_failure_preserves_previous_catalog(selection, tmp_path):
    output = tmp_path / "active.db"
    output.write_bytes(b"previous catalog")
    result = build_selected_catalog(selection, output, tmp_path / "strict-report")
    assert result["status"] == "blocked"
    assert result["database"] is None
    assert output.read_bytes() == b"previous catalog"
    assert not output.with_suffix(".db.prev").exists()


@pytest.mark.parametrize("selection", ["typed"], indirect=True)
def test_strict_corpus_failure_preserves_previous_catalog(selection, tmp_path):
    output = tmp_path / "catalog.db"
    output.write_bytes(b"previous catalog")
    report = tmp_path / "report"
    with pytest.raises(ValueError, match="resolved catalog validation failed"):
        build_selected_catalog(selection, output, report)
    summary = json.loads((report / "summary.json").read_text())
    assert summary["status"] == "engineering_failure"
    assert summary["counts"].get("error", 0) == 0
    assert output.read_bytes() == b"previous catalog"
    assert not output.with_suffix(".db.prev").exists()


def test_unapplied_existing_curation_is_an_error_and_cannot_hide_a_resolved_target(
    selection, tmp_path
):
    selected = PipelineSelection.model_validate_json(selection.read_bytes())
    prepared = open_prepared_catalog_sources(
        Path(selected.prepared_path),
        input_commit=selected.prepared_commit,
        expected_sha256=selected.prepared_sha256,
    )
    file = selected.scopes[0]
    scope = ScopeDeclarations.model_validate_json(
        gzip.decompress((selection.parent / file.path).read_bytes())
    )
    records = tuple(prepared.records.iter_records(source=scope.source))
    expected = capture_expectations(records, fields=("column_name",))
    # Transitional stand-in: bundles no longer carry curation files, so the gap
    # anchors on a selected source revision until it can name the curation tree's.
    gap = UnappliedCuration(
        revision=next(
            e.revision
            for e in prepared.manifest.inputs
            if e.revision is not None and e.revision.dataset == scope.source
        ),
        pointer="/description/0",
        reason="Existing input decision has unresolved target evidence.",
        targets=expected,
        peer_guards=(
            PeerGuard(
                guard_id="gap",
                source=scope.source,
                native=NativeCoordinates(variable_id=101),
                expected_members=tuple(e.ref for e in expected),
            ),
        ),
    )

    def save(gap):
        written = _scope_file(
            selection.parent,
            file.path,
            scope.model_copy(update={"unapplied_curation": (gap,)}),
        )
        selection.write_text(
            selected.model_copy(update={"scopes": (written,)}).model_dump_json()
        )

    save(gap)
    report = tmp_path / "gap-report"
    result = build_selected_catalog(
        selection, tmp_path / "gap.db", report, diagnostic=True
    )
    assert result["status"] == "diagnostic_complete"
    assert result["counts"]["unapplied_curation"] == 1
    with gzip.open(report / "events.jsonl.gz", "rt") as stream:
        assert any(
            json.loads(line).get("code") == "unapplied_existing_curation"
            for line in stream
        )
    save(gap.model_copy(update={"missing_variable": "scb/sample/value"}))
    with pytest.raises(ValueError, match="finish its conversion"):
        build_selected_catalog(
            selection, tmp_path / "bad-gap.db", tmp_path / "bad-gap", diagnostic=True
        )


def test_diagnostic_database_is_deterministic_and_create_only(selection, tmp_path):
    first, second = tmp_path / "one.db", tmp_path / "two.db"
    build_selected_catalog(selection, first, tmp_path / "one", diagnostic=True)
    build_selected_catalog(selection, second, tmp_path / "two", diagnostic=True)
    assert first.read_bytes() == second.read_bytes()
    with pytest.raises(ValueError, match="separate"):
        build_selected_catalog(selection, first, tmp_path / "retry", diagnostic=True)


@pytest.mark.parametrize("selection", [False], indirect=True)
def test_all_variables_withheld_remains_a_curation_failure(selection, tmp_path):
    output = tmp_path / "all-withheld.db"
    strict = build_selected_catalog(selection, output, tmp_path / "strict")
    assert strict["status"] == "blocked"
    assert not output.exists()
    diagnostic = build_selected_catalog(
        selection, output, tmp_path / "diagnostic", diagnostic=True
    )
    assert diagnostic["status"] == "diagnostic_complete"
    assert diagnostic["variables"] == 0
    with sqlite3.connect(output) as conn:
        assert conn.execute("SELECT COUNT(*) FROM variable").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM register").fetchone()[0] == 1
        assert (
            conn.execute(
                "SELECT value FROM import_manifest WHERE key='catalog_publishable'"
            ).fetchone()[0]
            == "false"
        )


def test_lost_delivery_coverage_refuses_the_build_before_any_database(
    selection, tmp_path, monkeypatch
):
    """The operator's witness: a defect inside formation truncates one supported
    2020 occurrence, nothing else reports it, and the strict build must refuse
    before any database is placed."""
    from reg_meta_build import source_formation

    original = source_formation._coded_states

    def truncate(segment, variant, coding, subject):
        states, diagnostics, withheld = original(segment, variant, coding, subject)
        return (
            [s.model_copy(update={"valid_to": "2020-06-30"}) for s in states],
            diagnostics,
            withheld,
        )

    monkeypatch.setattr(source_formation, "_coded_states", truncate)
    output, report = tmp_path / "lost.db", tmp_path / "lost-report"
    with pytest.raises(ValueError, match="delivery coverage was lost") as failure:
        build_selected_catalog(selection, output, report, diagnostic=False)
    missing = "2020-07-01..2020-12-31"
    assert missing in str(failure.value)
    assert "scb/sample/value people/VALUE" in str(failure.value)
    assert not output.exists()
    summary = json.loads((report / "summary.json").read_text())
    assert summary["status"] == "engineering_failure"
    assert missing in summary["error"]


def test_lost_delivery_coverage_diagnostic_completes_with_error_diagnostic(
    selection, tmp_path, monkeypatch, capsys
):
    """The operator's witness in diagnostic mode: the same truncation is recorded
    as an error diagnostic, and the run still completes with a nonpublishable
    database instead of failing after output is written."""
    from reg_meta_build import source_formation

    original = source_formation._coded_states

    def truncate(segment, variant, coding, subject):
        states, diagnostics, withheld = original(segment, variant, coding, subject)
        return (
            [s.model_copy(update={"valid_to": "2020-06-30"}) for s in states],
            diagnostics,
            withheld,
        )

    monkeypatch.setattr(source_formation, "_coded_states", truncate)
    output, report = tmp_path / "lost.db", tmp_path / "lost-report"
    status = run(
        [
            "build-db",
            "--selection",
            str(selection),
            "--report-dir",
            str(report),
            "--diagnostic",
            "--diagnostic-db-path",
            str(output),
        ]
    )
    result = json.loads(capsys.readouterr().out)
    missing = "2020-07-01..2020-12-31"
    assert status == EXIT_CONFIG
    assert result["status"] == "diagnostic_complete"
    assert result["publication_ready"] is False
    assert output.exists()
    with gzip.open(report / "events.jsonl.gz", "rt") as stream:
        issues = [
            json.loads(line)
            for line in stream
            if '"unexplained_delivery_coverage_loss"' in line
        ]
    assert len(issues) == 1 and issues[0]["severity"] == "error"
    assert missing in issues[0]["detail"]
    assert "scb/sample/value people/VALUE" in issues[0]["detail"]
    assert json.loads((report / "summary.json").read_text()) == result


@pytest.mark.parametrize("selection", ["typed"], indirect=True)
def test_changed_delivery_facts_refuse_the_build_before_any_database(
    selection, tmp_path, monkeypatch
):
    """The operator's witness: a defect inside formation retypes one supported
    2020 state, nothing else reports it, and the strict build must refuse before
    any database is placed."""
    from reg_meta_build import source_formation

    original = source_formation._coded_states

    def retype(segment, variant, coding, subject):
        states, diagnostics, withheld = original(segment, variant, coding, subject)
        return (
            [s.model_copy(update={"data_type": "text"}) for s in states],
            diagnostics,
            withheld,
        )

    monkeypatch.setattr(source_formation, "_coded_states", retype)
    output, report = tmp_path / "changed.db", tmp_path / "changed-report"
    with pytest.raises(
        ValueError,
        match="supported delivery facts changed without an explicit source outcome",
    ) as failure:
        build_selected_catalog(selection, output, report, diagnostic=False)
    assert "scb/sample/value people/VALUE" in str(failure.value)
    assert "claimed data_type=" in str(failure.value)
    assert "written 'text'" in str(failure.value)
    assert not output.exists()
    summary = json.loads((report / "summary.json").read_text())
    assert summary["status"] == "engineering_failure"
    assert "supported delivery facts changed" in summary["error"]


@pytest.mark.parametrize("selection", ["typed"], indirect=True)
def test_changed_delivery_facts_diagnostic_completes_with_error_diagnostic(
    selection, tmp_path, monkeypatch, capsys
):
    """The operator's witness in diagnostic mode: the same retype is recorded as
    an error diagnostic, and the run still completes with a nonpublishable
    database instead of failing after output is written."""
    from reg_meta_build import source_formation

    original = source_formation._coded_states

    def retype(segment, variant, coding, subject):
        states, diagnostics, withheld = original(segment, variant, coding, subject)
        return (
            [s.model_copy(update={"data_type": "text"}) for s in states],
            diagnostics,
            withheld,
        )

    monkeypatch.setattr(source_formation, "_coded_states", retype)
    output, report = tmp_path / "changed.db", tmp_path / "changed-report"
    status = run(
        [
            "build-db",
            "--selection",
            str(selection),
            "--report-dir",
            str(report),
            "--diagnostic",
            "--diagnostic-db-path",
            str(output),
        ]
    )
    result = json.loads(capsys.readouterr().out)
    assert status == EXIT_CONFIG
    assert result["status"] == "diagnostic_complete"
    assert result["publication_ready"] is False
    assert output.exists()
    with gzip.open(report / "events.jsonl.gz", "rt") as stream:
        issues = [
            json.loads(line)
            for line in stream
            if '"unexplained_delivery_fact_change"' in line
        ]
    assert len(issues) == 1 and issues[0]["severity"] == "error"
    assert "scb/sample/value people/VALUE" in issues[0]["detail"]
    assert "claimed data_type=" in issues[0]["detail"]
    assert "written 'text'" in issues[0]["detail"]
    assert json.loads((report / "summary.json").read_text()) == result


def _coded(label, *members):
    return {
        "value_set_version_label": label,
        "value_set": ResolvedCodeSet(members=members),
    }


# One per written per-column window check: the overlapping state copies a defect
# inside formation adds to the fixture's single code-less `VALUE` state.
_COLUMN_OVERLAPS = {
    "overlapping_distinct_value_sets": (
        _coded("a", ("1", "One")),
        _coded("b", ("2", "Two")),
    ),
    "overlapping_codeless_codebearing_states": ({}, _coded("a", ("1", "One"))),
    "overlapping_pooled_explicit_states": (
        {},
        {"pooled": True, "value_set_version_label": "p"},
    ),
}


@pytest.mark.parametrize("selection", ["typed"], indirect=True)
@pytest.mark.parametrize("code", _COLUMN_OVERLAPS)
def test_overlapping_column_states_withhold_the_variable_only_in_diagnostic(
    selection, tmp_path, monkeypatch, code
):
    """A per-column window conflict is the variable's, not the build's: strict
    refuses before any database, diagnostic reports the same text as an error,
    withholds the variable and writes a database that still validates."""
    from reg_meta_build import source_formation

    original = source_formation._coded_states

    def overlap(segment, variant, coding, subject):
        states, diagnostics, withheld = original(segment, variant, coding, subject)
        return (
            [s.model_copy(update=u) for s in states for u in _COLUMN_OVERLAPS[code]],
            diagnostics,
            withheld,
        )

    monkeypatch.setattr(source_formation, "_coded_states", overlap)
    output, report = tmp_path / "overlap.db", tmp_path / "strict"
    with pytest.raises(ValueError) as failure:
        build_selected_catalog(selection, output, report, diagnostic=False)
    assert "scb/sample/value people/VALUE" in str(failure.value)
    assert not output.exists()
    summary = json.loads((report / "summary.json").read_text())
    assert summary["status"] == "engineering_failure"

    report = tmp_path / "diagnostic"
    result = build_selected_catalog(selection, output, report, diagnostic=True)
    assert result["status"] == "diagnostic_complete"
    assert result["publication_ready"] is False
    assert result["variables"] == 0
    with gzip.open(report / "events.jsonl.gz", "rt") as stream:
        issues = [json.loads(line) for line in stream if f'"{code}"' in line]
    assert len(issues) == 1 and issues[0]["severity"] == "error"
    assert issues[0]["detail"] == str(failure.value)
    assert issues[0]["withheld_output"] == ["scb/sample/value"]
    assert validate_built_db(output).passed


@pytest.mark.parametrize("failure", ["unconverted", "scope", "hash", "escape"])
def test_engineering_failures_never_become_diagnostic_waivers(
    selection, tmp_path, failure
):
    raw = json.loads(selection.read_bytes())
    if failure == "unconverted":
        raw["unconverted"] = ["Existing relation requires conversion"]
    elif failure == "scope":
        raw["scopes"] = []
    elif failure == "hash":
        raw["scopes"][0]["sha256"] = "b" * 64
    else:
        raw["scopes"][0]["path"] = "../scope.json.gz"
    selection.write_text(json.dumps(raw))
    output = tmp_path / "invalid.db"
    with pytest.raises(ValueError):
        build_selected_catalog(
            selection, output, tmp_path / "invalid-report", diagnostic=True
        )
    assert not output.exists()


def _issues(report):
    with gzip.open(report / "events.jsonl.gz", "rt") as stream:
        return [e for e in map(json.loads, stream) if e["kind"] == "issue"]


@pytest.mark.parametrize("selection", ["register_scoped"], indirect=True)
def test_one_register_of_two_builds_a_nonpublishable_strict_subset(
    selection, tmp_path, capsys
):
    # Real corpus validation: a subset cannot meet the corpus volume floors, so
    # they must not apply rather than fail. Structural validation still runs.
    report = tmp_path / "report"
    status = run(
        [
            "--db",
            str(tmp_path / "subset"),
            "build-db",
            "--selection",
            str(selection),
            "--report-dir",
            str(report),
            "--registers",
            "1",
        ]
    )
    result = json.loads(capsys.readouterr().out)
    assert status == 0
    assert result["status"] == "complete"
    assert result["publication_ready"] is False
    assert result["registers"] == ["1"]
    assert result["corpus_validation"] == "not_applicable"
    assert result["counts"]["physical_occurrences"] == 1
    assert result["counts"].get("error", 0) == 0
    assert json.loads((report / "summary.json").read_text()) == result
    with sqlite3.connect(result["database"]) as conn:
        assert conn.execute("SELECT slug FROM register").fetchall() == [("sample",)]
        flags = dict(conn.execute("SELECT key, value FROM import_manifest"))
    assert flags["catalog_publishable"] == "false"
    assert flags["catalog_completeness"] == "incomplete"
    # Create-only, like a diagnostic: a subset never replaces a catalog.
    with pytest.raises(ValueError, match="build outputs must be separate"):
        build_selected_catalog(
            selection,
            Path(result["database"]),
            tmp_path / "again",
            registers=("1",),
        )


@pytest.mark.parametrize("selection", ["register_scoped"], indirect=True)
def test_references_into_unselected_registers_are_deferred_warnings(
    selection, tmp_path, structural_validation_only
):
    tables = ("variable_same_as", "variable_replaced_by", "variable_state_lineage")
    full = tmp_path / "full.db"
    result = build_selected_catalog(selection, full, tmp_path / "full-report")
    assert result["publication_ready"] is True
    assert "deferred_references" not in result["counts"]
    assert "skipped_curation" not in result["counts"]
    with sqlite3.connect(full) as conn:
        for table in tables:
            assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone() != (0,)
    # Selecting register 1 leaves the tag wholly outside: skipped, not deferred.
    for register, deferred, skipped in (
        ("1", ["catalog_succession", "variable_same_as:0"], 1),
        (
            "2",
            [
                "catalog_succession",
                "scb/sample-1/value-1:source_register",
                "variable_same_as:0",
            ],
            0,
        ),
    ):
        output, report = tmp_path / f"{register}.db", tmp_path / f"report-{register}"
        result = build_selected_catalog(
            selection, output, report, diagnostic=True, registers=(register,)
        )
        assert result["status"] == "diagnostic_complete"
        assert result["publication_ready"] is False
        assert result["corpus_validation"] == "not_applicable"
        assert result["counts"].get("error", 0) == 0
        issues = _issues(report)
        assert {(i["code"], i["severity"]) for i in issues} == {
            ("deferred_out_of_slice_reference", "warning")
        }
        assert result["counts"]["deferred_references"] == len(deferred)
        assert result["counts"]["skipped_curation"] == skipped
        assert sorted(o for i in issues for o in i["withheld_output"]) == deferred
        with sqlite3.connect(output) as conn:
            for table in tables:
                assert conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone() == (0,)
            # A tag wholly outside the slice is omitted, not written empty;
            # a tag wholly inside keeps its members.
            expected_tags = 0 if register == "1" else 1
            assert conn.execute("SELECT COUNT(*) FROM tag").fetchone() == (
                expected_tags,
            )
            assert conn.execute("SELECT COUNT(*) FROM tag_member").fetchone() == (
                expected_tags,
            )


@pytest.mark.parametrize("selection", ["register_scoped"], indirect=True)
# The unselected register 2 (scb/sample-1) exists but declares no `no-such`: it
# does not defer a reference from an entry that touches the slice.
@pytest.mark.parametrize("target", ["scb/nosuch/value-1", "scb/sample-1/no-such"])
def test_a_reference_to_what_no_scope_declares_stays_fatal_when_scoped(
    selection, tmp_path, target
):
    raw = json.loads(selection.read_bytes())
    raw["metadata"]["variable_same_as"][0]["b"] = target
    selection.write_text(json.dumps(raw))
    for name, registers in (("full", ()), ("scoped", ("1",))):
        with pytest.raises(CatalogDependencyError, match=target):
            build_selected_catalog(
                selection,
                tmp_path / f"{name}.db",
                tmp_path / name,
                diagnostic=True,
                registers=registers,
            )


def _as_naming_ambiguity(selection, path):
    """Rewrite one scope file so its variable exists only as an ambiguity name."""
    raw = json.loads(selection.read_bytes())
    (pinned,) = (s for s in raw["scopes"] if s["path"] == path)
    file = selection.parent / path
    scope = ScopeDeclarations.model_validate_json(gzip.decompress(file.read_bytes()))
    (declaration,) = (d for d in scope.naming if d.target.kind == "variable")
    prepared = open_prepared_catalog_sources(
        Path(raw["prepared_path"]),
        input_commit=raw["prepared_commit"],
        expected_sha256=raw["prepared_sha256"],
    )
    records = tuple(
        r
        for _, members in prepared.records.iter_register_slices(scope.source)
        for r in members
        if native_variable_key(r) == declaration.target.source_key
    )
    expectations = capture_expectations(records, fields=("column_name",))
    name = declaration.naming
    ambiguity = NamingAmbiguity(
        family=declaration.target.model_copy(
            update={
                "identity_revision": None,
                "expectations": expectations,
                "peer_guards": (
                    PeerGuard(
                        guard_id="exact-original-variable",
                        source=scope.source,
                        coordinates=(
                            ("register", records[0].subject.register_name),
                            ("variable", records[0].subject.variable),
                        ),
                        expected_members=tuple(e.ref for e in expectations),
                    ),
                ),
            }
        ),
        entries=(
            AcceptedNamingEntry(
                revision=declaration.target.identity_revision,
                origin="authored",
                entry=name,
                supplied_fields=("slug",),
                content_sha256=canonical_sha256({"slug": name.slug}),
            ),
        ),
        candidate_columns=tuple(
            (name.source_id, r.fields.column_name.value) for r in records
        ),
        reason="Ownership of the accepted name is unresolved.",
    )
    pinned.update(
        _scope_file(
            selection.parent,
            path,
            scope.model_copy(
                update={
                    "naming": tuple(d for d in scope.naming if d is not declaration),
                    "naming_ambiguities": (ambiguity,),
                }
            ),
        ).model_dump(mode="json")
    )
    return raw


@pytest.mark.parametrize("selection", ["register_scoped"], indirect=True)
def test_a_naming_ambiguity_name_in_an_unselected_scope_is_deferred(
    selection, tmp_path
):
    # Register 2 (scb/sample-1) names value-1 only as an ambiguity candidate:
    # the complete build withholds it with a cause, so a scoped build defers it.
    raw = _as_naming_ambiguity(selection, "scope-1.json.gz")
    selection.write_text(json.dumps(raw))
    report = tmp_path / "scoped"
    result = build_selected_catalog(
        selection, tmp_path / "scoped.db", report, diagnostic=True, registers=("1",)
    )
    assert result["counts"].get("error", 0) == 0
    issues = [
        i for i in _issues(report) if "variable_same_as:0" in i["withheld_output"]
    ]
    assert [i["code"] for i in issues] == ["deferred_out_of_slice_reference"]
    # A slug in neither its naming nor an ambiguity name stays fatal.
    raw["metadata"]["variable_same_as"][0]["b"] = "scb/sample-1/no-such"
    selection.write_text(json.dumps(raw))
    with pytest.raises(CatalogDependencyError, match="scb/sample-1/no-such"):
        build_selected_catalog(
            selection,
            tmp_path / "fatal.db",
            tmp_path / "fatal",
            diagnostic=True,
            registers=("1",),
        )


@pytest.mark.parametrize("selection", ["register_scoped"], indirect=True)
@pytest.mark.parametrize("target", ["scb/nosuch/value-1", "scb/sample-1/no-such"])
def test_curation_wholly_outside_the_slice_is_skipped_unproven(
    selection, tmp_path, target
):
    # A code/label pair and a tag touching only the unselected register 2 and
    # what no scope declares: the complete build fails on the undeclared end,
    # while a strict build of register 1 skips both entries without proof.
    raw = json.loads(selection.read_bytes())
    raw["metadata"]["tags"][0]["members"][0]["target"] = target
    raw["code_label_pairs"] = [
        asdict(CodeLabelPair("scb", "sample-1", "value-1", *target.split("/")))
    ]
    selection.write_text(json.dumps(raw))
    with pytest.raises(CatalogDependencyError, match=target):
        build_selected_catalog(
            selection, tmp_path / "full.db", tmp_path / "full", diagnostic=True
        )
    report = tmp_path / "scoped"
    result = build_selected_catalog(
        selection, tmp_path / "scoped.db", report, registers=("1",)
    )
    assert result["status"] == "complete"
    assert result["counts"].get("error", 0) == 0
    assert result["counts"]["skipped_curation"] == 2
    assert sorted(o for i in _issues(report) for o in i["withheld_output"]) == [
        "catalog_succession",
        "variable_same_as:0",
    ]


@pytest.mark.parametrize("selection", ["register_dangling"], indirect=True)
def test_a_scoped_build_reports_targets_no_register_holds_as_the_full_build(
    selection, tmp_path
):
    # Native ID 404 and the label NOSUCH lie in no register. Neither register
    # alone may defer them: the endpoint stays an error, the label a literal.
    for registers in ((), ("1",), ("2",)):
        name = "-".join(registers) or "full"
        report = tmp_path / f"report-{name}"
        result = build_selected_catalog(
            selection,
            tmp_path / f"{name}.db",
            report,
            diagnostic=True,
            registers=registers,
        )
        issues = _issues(report)
        errors = [i for i in issues if i["severity"] == "error"]
        assert [i["code"] for i in errors] == ["unresolved_source_event_endpoint"]
        assert "404" in errors[0]["detail"]
        assert result["counts"]["error"] == 1
        assert not any(
            output.endswith(":source_register")
            for i in issues
            for output in i["withheld_output"]
        )


@pytest.mark.parametrize("selection", ["register_scoped"], indirect=True)
def test_a_register_id_in_several_sources_must_name_its_source(selection, tmp_path):
    raw = json.loads(selection.read_bytes())
    first = raw["scopes"][0]
    result = build_selected_catalog(
        selection,
        tmp_path / "q.db",
        tmp_path / "q",
        registers=(f"{first['source']}:1",),
    )
    assert result["status"] == "complete"
    assert result["counts"]["physical_occurrences"] == 1
    raw["scopes"].append(
        {
            **first,
            "source": "other",
            "register_key": ["other", *first["register_key"][1:]],
        }
    )
    selection.write_text(json.dumps(raw))
    with pytest.raises(ValueError, match="several sources"):
        build_selected_catalog(
            selection, tmp_path / "a.db", tmp_path / "a", registers=("1",)
        )
    # The qualified form picks the one scope; this one names no prepared source.
    with pytest.raises(ValueError, match="source outside the prepared"):
        build_selected_catalog(
            selection, tmp_path / "o.db", tmp_path / "o", registers=("other:1",)
        )


@pytest.mark.parametrize("selection", ["sos_lined"], indirect=True)
def test_only_a_register_subset_lifts_the_complete_coverage_refusal(
    selection, tmp_path
):
    raw = json.loads(selection.read_bytes())
    raw["scopes"] = [s for s in raw["scopes"] if s["source"].startswith("scb-")]
    selection.write_text(json.dumps(raw))
    with pytest.raises(ValueError, match="do not cover the complete prepared"):
        build_selected_catalog(selection, tmp_path / "full.db", tmp_path / "full")
    with pytest.raises(ValueError, match="names no selected scope"):
        build_selected_catalog(
            selection, tmp_path / "x.db", tmp_path / "x", registers=("1",)
        )
    result = build_selected_catalog(
        selection,
        tmp_path / "scb.db",
        tmp_path / "scb",
        registers=("scb-registerinformation",),
    )
    assert result["status"] == "complete"
    assert result["publication_ready"] is False
    assert result["registers"] == ["scb-registerinformation"]
