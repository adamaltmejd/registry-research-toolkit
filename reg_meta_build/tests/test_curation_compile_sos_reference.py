"""SOS data-type corrections, MFR and LOVA classification references and thin native naming compile per selected scope."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

from _curation_compile_support import (
    case_record as _case_record,
    make_naming_reader as _naming_reader,
    make_tree as _tree,
)
from reg_meta_build.curation_compile import (
    compile_native_naming,
    compile_provider_declarations,
)
from reg_meta_build.curation_tree import (
    ErrataDataTypeEntry,
    load_curation_tree,
    load_register_files,
)
from reg_meta_build.id import mint
from reg_meta_build.pipeline import CompiledScope
from reg_meta_build.source_coordinates import (
    native_variable_key,
    source_register_key,
)
from reg_meta_build.source_effects import (
    record_ref,
)
from reg_meta_build.source_naming import (
    NamingDeclaration,
    NativeNamingTarget,
)
from reg_meta_build.source_records import (
    SourceFields,
    SourceRecord,
    value_field,
)

from reg_meta_build.fqid_slugs import SlugEntry


def _route_register(*routes):
    return SimpleNamespace(
        register_info=SimpleNamespace(provider="sos", slug="sample"),
        source_file="curation/registers/sos/sample.toml",
        variant=[],
        identity=SimpleNamespace(
            route=tuple(
                SimpleNamespace(deldatamangd=token, variants=names)
                for token, names in routes
            )
        ),
        errata=SimpleNamespace(data_type=(), classification_reference=()),
    )


def _sos_type_register():
    register = _route_register()
    register.errata.data_type = (
        ErrataDataTypeEntry(
            deldatamangd="A_LOVA_HOSP",
            variable="DESLEG_DATUM",
            column="DESLEG_DATUM",
            expected_type="Decimal",
            expected_representation="YYYY-MM-DD",
            data_type="date",
            evidence="Workbook row 28 declares a date.",
            noted="2026-09-29",
        ),
    )
    return register


def _sos_type_record(
    *,
    column: str = "DESLEG_DATUM",
    data_type: str = "Decimal",
    representation: str = "YYYY-MM-DD",
) -> SourceRecord:
    return _case_record(
        provider="sos",
        register="LOVA",
        variant="A_LOVA_HOSP",
        variable="DESLEG_DATUM",
        fields=SourceFields(
            column_name=value_field(column),
            data_type=value_field(data_type),
            representation=value_field(representation),
        ),
    )


def test_sos_data_type_skipped_register_is_accounted_in_subset():
    register = _sos_type_register()
    tree = SimpleNamespace(registers=(register,))
    prepared = SimpleNamespace(
        value_sources=(), records=SimpleNamespace(), manifest=SimpleNamespace(inputs=())
    )
    case_id = "curation/registers/sos/sample.toml#/errata.data_type/1"
    cases, diagnostics, report = compile_provider_declarations(
        tree, prepared, (), subset=True
    )
    assert cases == {}
    assert diagnostics == ()
    assert report["sos/sample"]["entries_read"] == [case_id]
    assert report["sos/sample"]["not_evaluated_in_subset"] == [case_id]
    _, diagnostics, report = compile_provider_declarations(
        tree, prepared, (), subset=False
    )
    assert [issue.code for issue in diagnostics] == ["stale_curation_entry"]
    assert report["sos/sample"]["stale"] == [case_id]


def test_sos_data_type_compiles_only_selected_native_source_scope():
    record = _sos_type_record()
    register_key = source_register_key(record)
    assert register_key is not None
    register = _sos_type_register()
    tree = SimpleNamespace(registers=(register,))
    scope = CompiledScope(
        source=record.source,
        register_key=None,
        naming=(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="register", provider="sos", source_key=register_key
                ),
                naming=SlugEntry(
                    kind="register", provider="sos", source_id="1", slug="sample"
                ),
                contributors=(),
            ),
        ),
    )
    other = record.model_copy(update={"source": "Socialstyrelsen/other.xlsx"})
    prepared = SimpleNamespace(
        value_sources=(),
        manifest=SimpleNamespace(inputs=()),
        records=SimpleNamespace(
            iter_records=lambda *, source: iter(
                item for item in (record, other) if item.source == source
            )
        ),
    )
    cases, diagnostics, report = compile_provider_declarations(
        tree, prepared, (scope,), subset=True
    )
    assert diagnostics == ()
    case = next(
        case
        for case in cases[record.source, None]
        if "/errata.data_type/" in case.case_id
    )
    assert case.peer_guards[0].source == record.source
    assert case.peer_guards[0].expected_members == (record_ref(record),)
    assert report["sos/sample"]["entries_matched"] == [case.case_id]


def _mfr_reference_register(variable: str):
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "curation"
    register = next(
        item
        for item in load_register_files(root)
        if item.register_info.provider == "sos" and item.register_info.slug == "mfr"
    )
    entry = next(
        item
        for item in register.errata.classification_reference
        if item.variable == variable
    )
    return register.model_copy(
        update={
            "errata": register.errata.model_copy(
                update={"classification_reference": (entry,)}
            )
        }
    ), entry


def test_mfr_reference_entries_exclude_missing_sheet_and_bdiag_consumers():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "curation"
    register = next(
        item
        for item in load_register_files(root)
        if item.register_info.provider == "sos" and item.register_info.slug == "mfr"
    )
    assert {
        (item.deldatamangd, item.variable)
        for item in register.errata.classification_reference
    } == {
        ("MFR_IVF", name)
        for name in ("BPNR", "BPSEUDO", "EMBRYON", "ETDATUM", "HINNSACK")
    } | {("MFR", name) for name in ("SECMARK", "SUGMARK", "TANGMARK")} | {
        (variant, "BPNRQ") for variant in ("MFR", "MFR_FOK", "MFR_IVF")
    } | {(variant, "MPNRQ") for variant in ("MFR", "MFR_FOK", "MFR_IVF", "MFR_LMED")}
    assert all(
        item.expected_reference == "Kodlista_förlossningssätt!A1"
        and item.expected_representation == "1 = ja" + " " * 538 + "0 = nej"
        for item in register.errata.classification_reference
        if item.deldatamangd == "MFR"
        and item.variable in {"SECMARK", "SUGMARK", "TANGMARK"}
    )


def test_mfr_reference_absent_full_source_vs_skipped_subset():
    register, _ = _mfr_reference_register("BPNR")
    tree = SimpleNamespace(registers=(register,))
    prepared = SimpleNamespace(
        value_sources=(), records=SimpleNamespace(), manifest=SimpleNamespace(inputs=())
    )
    case_id = f"{register.source_file}#/errata.classification_reference/1"
    _, diagnostics, report = compile_provider_declarations(
        tree, prepared, (), subset=True
    )
    assert diagnostics == ()
    assert report["sos/mfr"]["entries_read"] == [case_id]
    assert report["sos/mfr"]["not_evaluated_in_subset"] == [case_id]
    _, diagnostics, report = compile_provider_declarations(
        tree, prepared, (), subset=False
    )
    assert [issue.code for issue in diagnostics] == ["stale_curation_entry"]
    assert report["sos/mfr"]["stale"] == [case_id]


_LOVA_SSYK = (
    "https://www.scb.se/dokumentation/klassifikationer-och-standarder/"
    "standard-for-svensk-yrkesklassificering-ssyk/"
)


_LOVA_SUN = (
    "https://www.scb.se/dokumentation/klassifikationer-och-standarder/"
    "svensk-utbildningsnomenklatur-sun/"
)


_LOVA_REFERENCE_ROWS = (
    ("A_LOVA_HOSP", "EU_EES", "Fritext, land eller område", _LOVA_SSYK),
    ("A_LOVA", "EXAMAR", "YYYY", _LOVA_SSYK),
    ("A_LOVA_EXAMEN", "EXAMAR", "YYYY", _LOVA_SSYK),
    ("A_LOVA_HOSP", "EXAMAR", "YYYY", _LOVA_SSYK),
    ("A_LOVA_EXAMEN", "EXAMEN", "fritext", _LOVA_SSYK),
    ("A_LOVA_HOSP", "TEMPBEHORIGHETFRAN", "YYYY-MM-DD", _LOVA_SUN),
    ("A_LOVA_HOSP", "TEMPBEHORIGHETTILL", "YYYY-MM-DD", _LOVA_SUN),
    (
        "A_LOVA",
        "CFARNR",
        "Åttaställigt nummer för arbetsställe",
        "CfarNrSok - SCB - Sökning efter arbetsställen",
    ),
    (
        "A_LOVA_LISA",
        "CFARNR",
        "Åttaställigt nummer för arbetsställe",
        "CfarNrSok - SCB - Sökning efter arbetsställen",
    ),
    (
        "A_LOVA",
        "SSYKSTATUS",
        "1 vid överenstämmelse",
        "fel i SCB dokumentaion försök igen",
    ),
    (
        "A_LOVA_LISA",
        "SSYKSTATUS",
        "1 vid överenstämmelse",
        "fel i SCB dokumentaion försök igen",
    ),
    (
        "A_LOVA",
        "SSYKSTATUS_J16",
        "1 vid överenstämmelse",
        "fel i SCB dokumentaion försök igen",
    ),
    (
        "A_LOVA_LISA",
        "SSYKSTATUS_J16",
        "1 vid överenstämmelse",
        "fel i SCB dokumentaion försök igen",
    ),
    ("A_LOVA_HOSP", "EJ_PNR", "0, 1", "1 = personnumer saknas"),
    ("A_LOVA_HOSP", "FORSKRIVNINGSRATT", "J;N", "J = ja, N=nej"),
    (
        "A_LOVA_HOSP",
        "KALLA",
        "hosp; DESL, sk_spec",
        "hosp = HOSP, DESL=uppgifter om deslegitimation, sk_spec = uppgifter om specialistsjuksköterskor",
    ),
    ("A_LOVA_LISA", "SEKTORKOD", "Se A_LOVA_STYR_SEKTORKOD", "Administrativ"),
)


def test_lova_reference_inventory_is_exact():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "curation"
    register = next(
        item
        for item in load_register_files(root)
        if item.register_info.provider == "sos" and item.register_info.slug == "lova"
    )
    assert {
        (
            item.deldatamangd,
            item.variable,
            item.expected_representation,
            item.expected_reference,
        )
        for item in register.errata.classification_reference
    } == set(_LOVA_REFERENCE_ROWS)
    assert len(register.errata.classification_reference) == len(_LOVA_REFERENCE_ROWS)


def test_thin_native_naming_captures_complete_family_guard(tmp_path):
    root = tmp_path / "curation"
    _tree(root)
    path = root / "registers/fk/r.toml"
    path.parent.mkdir()
    register_id = mint("fk", "r")
    path.write_text(
        f'[register]\nprovider = "fk"\nslug = "r"\nnative_id = "{register_id}"\n'
        f'[[variant]]\nnative_id = "{register_id}.{mint("fk", "r", "_default")}"\nslug = "_default"\n'
        f'[[variable]]\nnative_id = "{register_id}.col"\nslug = "col"\n'
    )
    parent = _case_record(provider="fk", register="r", parent="register")
    variable = _case_record(provider="fk", register="r", variable="col")
    records = (parent, variable)

    class Reader:
        def iter_native_families(self, source, registers=None):
            return iter(((native_variable_key(variable), (variable,)),))

        def iter_records(self, *, source):
            return iter(records)

    register_key = source_register_key(variable)
    assert register_key is not None
    scope = CompiledScope(
        source=variable.source,
        register_key=None,
        naming=(
            NamingDeclaration(
                target=NativeNamingTarget(
                    kind="register", provider="fk", source_key=register_key
                ),
                naming=SlugEntry(
                    kind="register", provider="fk", source_id=str(register_id), slug="r"
                ),
                contributors=(),
            ),
        ),
    )
    names, _, _, diagnostics, _ = compile_native_naming(
        load_curation_tree(root),
        cast("Any", SimpleNamespace(records=_naming_reader(Reader()))),
        (scope,),
        subset=True,
    )
    assert diagnostics == ()
    target = next(
        item.target
        for item in names[(variable.source, None)]
        if item.target.kind == "variable"
    )
    assert len(target.expectations) == 1
    assert target.peer_guards[0].expected_members == (target.expectations[0].ref,)
