"""Shared source-coordinate builders for the curation-compile tests (records, trees, scopes and prepared stores)."""

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast

from _csv_fixtures import REGISTERINFORMATION_HEADER, var_row
from reg_meta.source_evidence import (
    SourceRevision,
)
from reg_meta_build.curation_compile import (
    compile_occurrence_corrections,
)
from reg_meta_build.curation_tree import (
    ErrataFieldEntry,
    ErrataOccurrencePeriodEntry,
    load_curation_tree,
)
from reg_meta_build.pipeline import CompiledScope
from reg_meta_build.source_coordinates import (
    native_variable_key,
    native_variant_key,
    source_register_key,
)
from reg_meta_build.source_curation import (
    capture_expectations,
)
from reg_meta_build.source_effects import (
    record_ref,
)
from reg_meta_build.source_naming import (
    NamingDeclaration,
    NativeNamingTarget,
)
from reg_meta_build.source_records import (
    NativeCoordinates,
    ScopeInterval,
    SourceCoordinate,
    SourceRecord,
    TemporalScope,
)
from reg_meta_build.sources.scb_records import clean_scb_row

from reg_meta_build.fqid_slugs import SlugEntry

if TYPE_CHECKING:
    from pathlib import Path


def make_revision(
    dataset: str, *, artifact_path: str | None = None, upstream_revision: str = "v1"
) -> SourceRevision:
    return SourceRevision.create(
        dataset=dataset,
        publisher="fixture",
        purpose="fixture",
        upstream_revision=upstream_revision,
        artifact_path=artifact_path or f"{dataset}.csv",
        artifact_size=1,
        artifact_sha256="0" * 64,
    )


def make_tree(root: Path):
    classes = root / "classifications"
    classes.mkdir(parents=True)
    for short, slug in (("ALPHA", "alpha"), ("BETA", "beta")):
        (classes / f"{short}.toml").write_text(
            f'[classification]\nshort_name = "{short}"\nslug = "{slug}"\n'
            f'name = "{short}"\ncodes_file = "{slug}.csv"\n'
            + (
                '[binding]\nlabel_source = "sos"\n'
                'value_set_labels = ["Unmatched label"]\n'
                '[[binding.variable]]\nvariable = "scb/other/one"\n'
                if short == "BETA"
                else '[binding]\nvalue_set_labels = ["Selected provider label"]\n'
            ),
            encoding="utf-8",
        )
    registers = root / "registers" / "scb"
    registers.mkdir(parents=True)
    (registers / "sample.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
        '[[group]]\nregister = "scb/sample"\nkey = "pair"\nlabel = "Pair"\n'
        'axis = "rank"\nmembers = [{variable = "one", value = "1", label = "First"}, '
        '{variable = "two", value = "2", label = "Second"}]\n'
        '[[code_label_pair]]\ncode = "scb/sample/one"\nlabel = "scb/sample/two"\n',
        encoding="utf-8",
    )
    (registers / "other.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "other"\nnative_id = "2"\n',
        encoding="utf-8",
    )
    (root / "classification_groups.toml").write_text(
        '[[classification_group]]\nkey = "umbrella"\nlabel = "Umbrella"\n'
        'members = [{classification = "alpha", value = "a", label = "A"}, '
        '{classification = "beta", value = "b", label = "B"}]\n',
        encoding="utf-8",
    )
    (root / "relations.toml").write_text(
        '[[edge]]\ntype = "replaced_by"\nfrom = "class/alpha"\nto = "class/beta"\n'
        '[[edge]]\ntype = "derived_from"\nderived = "class/beta"\nsource = "class/alpha"\n'
        '[[edge]]\ntype = "replaced_by"\nfrom = "scb/sample/one"\n'
        'to = "scb/sample/two"\neffective_year = 2020\n'
        '[[edge]]\ntype = "same_as"\na = "scb/sample/one"\nb = "scb/other/one"\n',
        encoding="utf-8",
    )
    (root / "tags.toml").write_text(
        '[[tag]]\nslug = "theme"\nlabel = "Theme"\n'
        '[[tag.member]]\nvariable = "scb/sample/one"\n'
        '[[tag.member]]\nvariable = "scb/other/one"\n',
        encoding="utf-8",
    )
    (root / "lineage.toml").write_text(
        '[lineage_defaults]\n"scb/sample" = "people"\n"scb/other" = "people"\n',
        encoding="utf-8",
    )
    return load_curation_tree(root)


def make_scope() -> CompiledScope:
    return CompiledScope(
        source="scb-registerinformation",
        register_key=None,
        naming=(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="register",
                    provider="scb",
                    source_key=("scb", "register", 1),
                ),
                naming=SlugEntry(
                    kind="register", provider="scb", source_id="1", slug="sample"
                ),
                contributors=(),
            ),
        ),
    )


def make_naming_reader(reader: Any) -> Any:
    reader.iter_naming_families = reader.iter_native_families

    def partitions(source, registers=None, select_family=None):
        return (
            (key, members)
            for key, members in reader.iter_native_families(source, registers)
            if select_family is None or select_family(key)
        )

    reader.iter_partition_families = partitions

    def slices(source, registers):
        if None in registers:
            return iter(((None, tuple(reader.iter_records(source=source))),))
        return reader.iter_register_slices(source, registers)

    reader.iter_naming_register_slices = slices
    return reader


def make_prepared():
    class Records:
        def iter_native_families(self, source, registers=None):
            return iter(())

        def iter_records(self, *, source):
            return iter((self._record(source),))

        def iter_register_slices(self, source, registers):
            return iter(((None, (self._record(source),)),))

        @staticmethod
        def _record(source):
            register = SourceCoordinate(status="value", native_id=1)
            return SimpleNamespace(
                source=source,
                context=(),
                subject=SimpleNamespace(
                    provider="scb",
                    register_name=register,
                    variant=SourceCoordinate(status="unknown"),
                ),
                parent_facts=(
                    SimpleNamespace(
                        kind="register", register_name=register, variant=None
                    ),
                ),
            )

    inputs = tuple(
        SimpleNamespace(
            origin="snapshot",
            role=role,
            path=path,
            revision=make_revision(dataset, artifact_path=f"snapshot-a:source/{path}"),
        )
        for path, dataset, role in (
            ("Identifierare.csv", "scb-identifierare", "scb_auxiliary"),
            ("Timeseries.csv", "scb-timeseries", "scb_events"),
            ("Registerinformation.csv", "scb-registerinformation", "scb_records"),
        )
    )
    return SimpleNamespace(
        manifest=SimpleNamespace(inputs=inputs),
        iter_evidence=lambda: iter(()),
        records=make_naming_reader(Records()),
        value_sources=(),
    )


def compiled_bytes(compiled) -> bytes:
    return json.dumps(
        {
            "fields": compiled.fields,
            "cases": {repr(key): value for key, value in compiled.cases.items()},
            "naming": {
                repr(key): value for key, value in (compiled.naming or {}).items()
            },
            "provider_keys": {
                repr(key): value
                for key, value in (compiled.provider_keys or {}).items()
            },
            "naming_ambiguities": {
                repr(key): value
                for key, value in (compiled.naming_ambiguities or {}).items()
            },
            "variants": {
                repr(key): value for key, value in (compiled.variants or {}).items()
            },
            "report": compiled.report,
        },
        default=lambda value: (
            value.model_dump(mode="json")
            if hasattr(value, "model_dump")
            else value.__dict__
        ),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


def partition_scope(records: tuple[SourceRecord, ...]) -> CompiledScope:
    first = records[0]
    register = source_register_key(first)
    assert register is not None
    provider = first.subject.provider
    register_id = str(register[-1]) if provider == "scb" else "5891427617861710725"
    slug = "sample" if provider == "scb" else "par"
    return CompiledScope(
        source=first.source,
        register_key=None,
        naming=(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="register",
                    provider=provider,
                    source_key=register,
                ),
                naming=SlugEntry(
                    kind="register",
                    provider=provider,
                    source_id=register_id,
                    slug=slug,
                ),
                contributors=(),
            ),
        ),
    )


def errata_record(
    *,
    column: str,
    year: str,
    variable: int = 5,
    member: int = 20,
    variant: int = 2,
    edition_name: str | None = None,
    edition_id: int | None = None,
    data_type: str = "int",
) -> SourceRecord:
    header = REGISTERINFORMATION_HEADER.split("|")
    row = var_row(
        colname=column,
        cvid=member,
        var_id=variable,
        year=year,
        versionname=edition_name,
        regver_id=edition_id if edition_id is not None else int(year),
        register=("TEST", 1, variant),
        data_type=data_type,
    ).split("|")
    cells = {
        name: (True, value, value) for name, value in zip(header, row, strict=True)
    }
    return clean_scb_row(
        header, member, cells, make_revision("scb-registerinformation")
    ).record


def errata_fixture(tmp_path: Path, records: tuple[SourceRecord, ...], fragment: str):
    root = tmp_path / "curation"
    make_tree(root)
    path = root / "registers/scb/sample.toml"
    path.write_text(
        path.read_text()
        + '\n[[variant]]\nnative_id = "1.2"\nslug = "people"\n'
        + fragment
        + (
            '\n[[variable]]\nnative_id = "1.NewCol"\nslug = "new-col"\n'
            if "[[errata.column]]" in fragment
            else ""
        ),
        encoding="utf-8",
    )
    scope = partition_scope(records)
    native = source_register_key(records[0])
    assert native is not None
    reader = SimpleNamespace(
        iter_register_slices=lambda source, registers: iter(((native, records),))
    )
    return (
        load_curation_tree(root),
        cast("Any", SimpleNamespace(records=reader, iter_evidence=lambda: iter(()))),
        scope,
    )


def enrichment_fixture(
    tmp_path: Path,
    records: tuple[SourceRecord, ...],
    fragment: str,
    *,
    target: NativeNamingTarget | None = None,
):
    root = tmp_path / "curation"
    make_tree(root)
    path = root / "registers/scb/sample.toml"
    path.write_text(path.read_text() + fragment, encoding="utf-8")
    scope = partition_scope(records)
    native = source_register_key(records[0])
    variable = native_variable_key(records[0])
    assert native is not None and variable is not None
    declaration = NamingDeclaration(
        target=target
        or NativeNamingTarget(
            kind="variable",
            provider="scb",
            source_key=variable,
            register_key=native,
        ),
        naming=SlugEntry(kind="variable", provider="scb", source_id="1.5", slug="a"),
        contributors=(),
    )
    reader = SimpleNamespace(
        iter_register_slices=lambda source, registers: iter(((native, records),))
    )
    key = (scope.source, scope.register_key)
    return (
        load_curation_tree(root),
        cast("Any", SimpleNamespace(records=reader)),
        scope,
        {key: (declaration,)},
    )


DESCRIPTION = (
    '\n[[enrichment.description]]\nregister = "scb/sample"\n'
    'variable = "a"\ndescription = "Accepted prose"\n'
    'provenance = "delivery list"\n'
)


ALIAS = (
    '\n[[enrichment.alias]]\nregister = "scb/sample"\n'
    'variable = "a"\ndelivery_column = "FormerA"\n'
    'provenance = "delivery list"\n'
)


def scb_partition_tree(root: Path, extra: str):
    tree = make_tree(root)
    path = root / "registers" / "scb" / "sample.toml"
    path.write_text(path.read_text() + extra, encoding="utf-8")
    return tree


def pooled_parallel_fixture(tmp_path, *, co_delivered=False):
    from reg_meta_build.source_curation import PeerGuard, capture_expectations

    header = REGISTERINFORMATION_HEADER.split("|")
    records = []
    for i, (column, edition) in enumerate(
        (("First", "2022"), ("Second", "2022"))
        if co_delivered
        else (("First", "2020-2022"), ("Second", "2022-2024"))
    ):
        values = var_row(
            colname=column,
            cvid=100 if co_delivered else 100 + i,
            var_id=1,
            varname="Income",
            year="2022" if co_delivered else "2020",
            versionname=edition,
            regver_id=110 if co_delivered else 110 + i,
        ).split("|")
        records.append(
            clean_scb_row(
                header,
                i + 1,
                {
                    name: (True, value, value)
                    for name, value in zip(header, values, strict=True)
                },
                make_revision("fixture"),
            ).record
        )
    records = tuple(records)
    register_key = source_register_key(records[0])
    variant_key = native_variant_key(records[0])
    assert register_key is not None and variant_key is not None
    owner = ("accepted", "fixture", "income")
    guard = PeerGuard(
        guard_id="fixture-parallel-family",
        source=records[0].source,
        native=NativeCoordinates(register_id=1, register_variant_id=10, variable_id=1),
        expected_members=tuple(dict.fromkeys(record_ref(r) for r in records)),
    )
    naming = tuple(
        NamingDeclaration(
            target=NativeNamingTarget(
                kind=kind,
                provider="scb",
                source_key=key,
                register_key=None if kind == "register" else register_key,
                expectations=(
                    capture_expectations(records, fields=("column_name",), coding=True)
                    if kind == "variable"
                    else ()
                ),
                peer_guards=(guard,) if kind == "variable" else (),
            ),
            naming=SlugEntry(kind=kind, provider="scb", source_id=source_id, slug=slug),
            contributors=(),
        )
        for kind, key, source_id, slug in (
            ("register", register_key, "1", "sample"),
            ("register_variant", variant_key, "1.10", "people"),
            ("variable", owner, "1.1.income", "income"),
        )
    )
    root = tmp_path / "curation"
    path = root / "registers/scb/sample.toml"
    path.parent.mkdir(parents=True)
    path.write_text(
        '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
        '[[representation.parallel]]\nvariable = "1.1.income"\n'
        'variant = "1.10"\nvalid_from = "2022-01-01"\n'
        'valid_to = "2022-12-31"\nevidence = "Reviewed original boundary"\n'
        'noted = "2026-09-29"\n'
        'columns = [{column = "First", valid_from = "2020-01-01", '
        'valid_to = "2022-12-31", source_editions = ["2020-2022"]}, '
        '{column = "Second", valid_from = "2022-01-01", '
        'valid_to = "2024-12-31", source_editions = ["2022-2024"]}]\n'
    )
    if co_delivered:
        path.write_text(
            path.read_text()
            .replace("2020-01-01", "2022-01-01")
            .replace("2024-12-31", "2022-12-31")
            .replace("2020-2022", "2022")
            .replace("2022-2024", "2022")
            + 'co_delivered = true\ncolumn_metadata = "per_column"\n'
        )
    return path, records, naming


def checked_correction_fixture(tmp_path, *, period=False):
    root = tmp_path / "curation"
    scb_partition_tree(root, "")
    selected = errata_record(column="ANSWER", year="2009", edition_id=99)
    negative = errata_record(column="ANSWER", year="2017", edition_id=100, member=21)
    tree = load_curation_tree(root)
    scope = partition_scope((selected, negative))
    base = {
        "variable": "1.5",
        "variant": "1.2",
        "column": "ANSWER",
        "edition": "99",
        "expected_fields": list(
            capture_expectations(
                (selected,),
                fields=("name", "definition", "description", "operational_definition"),
            )[0]
            .alternatives[0]
            .fields
        ),
        "expected_period_text": selected.original_period_text,
        "expected_scope": selected.edition_scope,
        "expected_period": selected.edition_period_scope,
        "evidence": "Exact supplied definition establishes the reviewed correction.",
        "noted": "2026-09-30",
    }
    if period:
        entry = ErrataOccurrencePeriodEntry(
            **base,
            edition_scope=TemporalScope(
                kind="intervals",
                intervals=(ScopeInterval(start="2016-01-01", end=None),),
            ),
            edition_period_scope=TemporalScope(
                kind="unknown", label="Original unknown period retained"
            ),
        )
        field = "occurrence_period"
    else:
        entry = ErrataFieldEntry(**base, field="name", value="Reviewed label")
        field = "field"
    reg = next(r for r in tree.registers if r.register_info.slug == "sample")
    reg = reg.model_copy(
        update={"errata": reg.errata.model_copy(update={field: [entry]})}
    )
    return replace(tree, registers=(reg,)), scope, selected, negative, entry


def run_checked_correction(tree, scope, records):
    native = native_variable_key(records[0])
    reader = SimpleNamespace(
        iter_native_families=lambda source, registers=None, select_family=None: iter(
            ((native, records),)
        )
    )
    return compile_occurrence_corrections(
        tree, cast("Any", SimpleNamespace(records=reader)), (scope,), subset=False
    )
