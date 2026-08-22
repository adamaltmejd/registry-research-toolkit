"""A4.3b SOSAdapter tests (fixture-level, no 14GB build).

Drives `SOSAdapter.emit()` with synthesized `SosRegister` trees (one per failure
mode) plus a small combined SCB+SOS `build_db` integration. Mirrors the plan's
test plan section 5. The SOS workbook strings the adapter hard-codes (abbrev
derivation, the BU/PAR split allow-list, the MFR IVF_klinik entity registry) are
R4-verified against the live workbooks in the implementation; these tests pin the
BEHAVIOR those strings drive.
"""

from __future__ import annotations

import sqlite3
from itertools import pairwise
from pathlib import Path

import pytest
from _sos_fixtures import (
    _Deldat as _SosFixtureDeldat,
    _Register as _SosFixtureRegister,
    _Var as _SosFixtureVar,
)
from reg_meta_build.db import DDL, build_db, seed_providers
from reg_meta_build.id import _MINT_BIT, mint
from reg_meta_build.ir import (
    IRRegister,
    IRValueCode,
    IRVariable,
    IRVariableAlias,
    IRVariableState,
    IRVariant,
    IRWarning,
)
from reg_meta_build.sources.sos import (
    SOSAdapter,
    SosDcatAp,
    SosDeldatamangd,
    SosKodlista,
    SosKodlistaRow,
    SosRegister,
    SosVariable,
    _classify_value_set_text,
    _intersect_window,
    _iso_bound,
    _parse_tidsperiod,
    _resolve_by_signal,
    _resolve_classification,
    _segment_windowed_codes,
    _sos_abbrev,
)

_HIGH = 1 << 63


# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def _var(
    name: str,
    *,
    deldatamangd: str | None = None,
    data_type: str | None = "Sträng (text)",
    label: str | None = None,
    description: str | None = None,
    data_from: int | None = None,
    data_to: int | None = None,
    external_classification: str | None = None,
    value_set_text: str | None = None,
) -> SosVariable:
    return SosVariable(
        deldatamangd=deldatamangd,
        name=name,
        label=label,
        description=description,
        object_type=None,
        value_set_text=value_set_text,
        external_classification=external_classification,
        data_type=data_type,
        is_join_variable=None,
        join_description=None,
        presentation_order=None,
        data_from=data_from,
        data_to=data_to,
        quality_note=None,
        origin=None,
        source_detail=None,
    )


def _deldat(
    name: str,
    *,
    data_from: int | None = None,
    data_to: int | None = None,
    label: str | None = None,
    aggregation_level: str | None = None,
) -> SosDeldatamangd:
    return SosDeldatamangd(
        name=name,
        label=label,
        description=None,
        data_from=data_from,
        data_to=data_to,
        update_frequency=None,
        aggregation_level=aggregation_level,
    )


def _kodlista(
    variable_hint: str, rows: list[SosKodlistaRow], *, raw: bool = False
) -> SosKodlista:
    return SosKodlista(
        sheet_name=f"Kodlista_{variable_hint}",
        variable_hint=variable_hint,
        codeset_name=None,
        variable_header=None,
        background=None,
        rows=tuple(rows),
        raw_rows=((("junk",),) if raw else ()),
    )


def _register(
    abbrev: str,
    variables: list[SosVariable],
    *,
    deldatamangder: tuple[SosDeldatamangd, ...] = (),
    kodlistor: tuple[SosKodlista, ...] = (),
    title: str | None = None,
) -> SosRegister:
    # The abbrev is sourced from the FILENAME stem's parenthesized code, so name
    # the synthetic source file accordingly.
    return SosRegister(
        source_file=Path(f"Metadata_Synthetic ({abbrev.upper()})_webb.xlsx"),
        dataset_name=title or abbrev.upper(),
        dataset_version="1.0",
        dataset_date=None,
        template_version=None,
        template_date=None,
        contact_email=None,
        dcat_ap=SosDcatAp(title_sv=title or abbrev.upper()),
        deldatamangder=deldatamangder,
        variables=tuple(variables),
        kodlistor=kodlistor,
        quality_sheets=(),
        warnings=(),
    )


def _emit(reg: SosRegister) -> tuple[list, SOSAdapter]:
    """Run the adapter's per-register emit against an in-memory DB and return
    ``(ir_objects, adapter)``. The DB (and the returned adapter, which owns it)
    is needed for the value-table read-back path."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(DDL)
    seed_providers(conn)
    adapter = SOSAdapter(conn)
    objs = list(adapter._emit_register(reg))
    return objs, adapter


def _of(objs: list, kind: type) -> list:
    return [o for o in objs if isinstance(o, kind)]


# ---------------------------------------------------------------------------
# 1. Merge no-op (default)
# ---------------------------------------------------------------------------


def test_merge_single_name_multi_deldatamangd() -> None:
    reg = _register(
        "thr",
        [
            _var("BESOKSDATUM", deldatamangd="THR_A", data_type="Datum"),
            _var("BESOKSDATUM", deldatamangd="THR_B", data_type="Datum"),
        ],
        deldatamangder=(_deldat("THR_A"), _deldat("THR_B")),
    )
    objs, _ = _emit(reg)
    variables = _of(objs, IRVariable)
    assert len(variables) == 1, "same-name members merge to one variable"
    assert variables[0].provider_key == "BESOKSDATUM"
    # one state per (deldatamängd/variant)
    states = _of(objs, IRVariableState)
    assert len(states) == 2
    assert {s.register_variant_id for s in states} == {
        v.register_variant_id for v in _of(objs, IRVariant)
    }


# ---------------------------------------------------------------------------
# 2 + 3. Known splits (BU FOD_DATUMN, PAR ATC)
# ---------------------------------------------------------------------------


def test_known_split_bu_fod_datumn() -> None:
    # BU is variant-less (no deldatamängder sheet); the FOD_DATUMN conflict is
    # Datum vs Heltal. var.deldatamangd is populated (Datavynamn) but ignored.
    reg = _register(
        "bu",
        [
            _var("FOD_DATUMN", deldatamangd="bu_insats", data_type="Heltal"),
            _var("FOD_DATUMN", deldatamangd="bu_plac", data_type="Datum"),
        ],
        deldatamangder=(),
    )
    objs, _ = _emit(reg)
    variables = _of(objs, IRVariable)
    assert len(variables) == 2, "allow-listed conflict splits into 2 siblings"
    ids = {v.variable_id for v in variables}
    assert len(ids) == 2, "distinct minted sibling ids"
    assert all(v.provider_key == "FOD_DATUMN" for v in variables)


def test_known_split_bu_records_sibling_edge() -> None:
    reg = _register(
        "bu",
        [
            _var("FOD_DATUMN", deldatamangd="bu_insats", data_type="Heltal"),
            _var("FOD_DATUMN", deldatamangd="bu_plac", data_type="Datum"),
        ],
    )
    _, adapter = _emit(reg)
    # The split records ONE sibling pair (the in-build concept-group fold input,
    # never persisted to a shipped table).
    assert len(adapter.sibling_edges) == 1
    a, b = adapter.sibling_edges[0]
    assert a != b


def test_known_split_par_atc() -> None:
    reg = _register(
        "par",
        [
            _var("ATC", deldatamangd="PAR_OV", data_type="Sträng (text)"),
            _var("ATC", deldatamangd="PAR_SV", data_type="Heltal"),
        ],
        deldatamangder=(_deldat("PAR_OV"), _deldat("PAR_SV")),
    )
    objs, adapter = _emit(reg)
    assert len(_of(objs, IRVariable)) == 2
    assert len(adapter.sibling_edges) == 1


# ---------------------------------------------------------------------------
# 4. Warn-merge fallback (unanticipated conflict)
# ---------------------------------------------------------------------------


def test_warn_merge_unanticipated_conflict() -> None:
    # PAR ATCO has the same shape as ATC but is NOT in the allow-list -> merge +
    # warn.
    reg = _register(
        "par",
        [
            _var("ATCO", deldatamangd="PAR_OV", data_type="Sträng (text)"),
            _var("ATCO", deldatamangd="PAR_SV", data_type="Heltal"),
        ],
        deldatamangder=(_deldat("PAR_OV"), _deldat("PAR_SV")),
    )
    objs, adapter = _emit(reg)
    assert len(_of(objs, IRVariable)) == 1, "unanticipated conflict warn-MERGEs"
    assert adapter.sibling_edges == []
    warns = [
        w
        for w in _of(objs, IRWarning)
        if w.code == "sos_unanticipated_same_name_conflict"
    ]
    assert len(warns) == 1


# ---------------------------------------------------------------------------
# 4b. Curated single-row name correction (#362) + known-merge warn-silence
# ---------------------------------------------------------------------------


def test_variable_name_correction_demerges_invarn() -> None:
    # LOVA A_LOVA_PERSON mistypes the 9th immigration-date row as a SECOND
    # INVARN8 (both Heltal). Disambiguated only by etikett; the curated
    # ("lova", "INVARN8", "Invandringsdatum 9 numerisk") -> "INVARN9" correction
    # re-keys it so the two no longer merge silently.
    reg = _register(
        "lova",
        [
            _var(
                "INVARN8",
                deldatamangd="A_LOVA_PERSON",
                data_type="Heltal",
                label="Invandringsdatum 8 numerisk",
            ),
            _var(
                "INVARN8",
                deldatamangd="A_LOVA_PERSON",
                data_type="Heltal",
                label="Invandringsdatum 9 numerisk",
            ),
        ],
        # The variable rows key on the A_LOVA_PERSON extraction token; the
        # Deldatamängder sheet names the variant 'LOVA PERSON', and the curated
        # map routes the token there (without it both members drop, unstated).
        deldatamangder=(_deldat("LOVA PERSON"),),
    )
    objs, _ = _emit(reg)
    variables = _of(objs, IRVariable)
    assert len(variables) == 2, "the correction de-merges into two variables"
    by_key = {v.provider_key: v for v in variables}
    assert set(by_key) == {"INVARN8", "INVARN9"}, "corrected name keys its own var"
    # IRVariable.name is the etikett (label or name); each variable carries its
    # OWN row's etikett, so the corrected variable keeps the mistyped row's label.
    assert by_key["INVARN8"].name == "Invandringsdatum 8 numerisk"
    assert by_key["INVARN9"].name == "Invandringsdatum 9 numerisk"
    # The de-merge must actually emit one state per variable (the regression: a
    # dropped invarn9 state would still pass the IRVariable count above). Both
    # belong to the LOVA PERSON variant the token resolves to, with no
    # unresolved-token warning.
    assert [
        w for w in _of(objs, IRWarning) if w.code == "sos_deldatamangd_unresolved"
    ] == []
    states = _of(objs, IRVariableState)
    lova_person = mint("sos", "lova", "LOVA PERSON")
    assert len(states) == 2, "one state per de-merged variable"
    assert {s.register_variant_id for s in states} == {lova_person}
    # Regression (#362 bug): for SOS the delivery column IS the variable name, so
    # the correction must also rewrite the column. If only the group key were
    # corrected, the de-merged INVARN9 variable would emit its alias/state with
    # column "INVARN8" — colliding with the real INVARN8 in the same variant. The
    # two variables must NOT share a delivery column.
    aliases = _of(objs, IRVariableAlias)
    assert len(aliases) == 2, "one alias per de-merged variable"
    assert {a.delivery_column_name for a in aliases} == {"INVARN8", "INVARN9"}
    # The corrected INVARN9 *variable* owns the INVARN9 column; INVARN8 owns its own.
    col_by_var = {a.variable_id: a.delivery_column_name for a in aliases}
    assert col_by_var[by_key["INVARN8"].variable_id] == "INVARN8"
    assert col_by_var[by_key["INVARN9"].variable_id] == "INVARN9"
    # Each de-merged variable's state carries its OWN column, too.
    state_col_by_var = {s.variable_id: s.delivery_column_name for s in states}
    assert state_col_by_var[by_key["INVARN8"].variable_id] == "INVARN8"
    assert state_col_by_var[by_key["INVARN9"].variable_id] == "INVARN9"


def test_known_merge_allowlist_silences_warn() -> None:
    # LOVA EXAMAR is an intentional, type-lossless same-name merge (data_type is
    # per-state): Examensår (Heltal) vs Utbildningsår… (Sträng (text)). It is in
    # KNOWN_MERGE_ALLOWLIST, so it still MERGEs to one variable but emits no warn.
    reg = _register(
        "lova",
        [
            _var(
                "EXAMAR",
                deldatamangd="A_LOVA",
                data_type="Heltal",
                label="Examensår",
            ),
            _var(
                "EXAMAR",
                deldatamangd="A_LOVA_EXAMEN",
                data_type="Sträng (text)",
                label="Utbildningsår (avslutningsår högsta utb.)",
            ),
        ],
        # Variable rows key on the A_LOVA / A_LOVA_EXAMEN tokens; the
        # Deldatamängder sheet names the variants 'LOVA' / 'LOVA EXAMEN' that the
        # curated map routes the tokens to (else both members drop, unstated).
        deldatamangder=(_deldat("LOVA"), _deldat("LOVA EXAMEN")),
    )
    objs, _ = _emit(reg)
    assert len(_of(objs, IRVariable)) == 1, "allow-listed conflict still MERGEs"
    warns = [
        w
        for w in _of(objs, IRWarning)
        if w.code == "sos_unanticipated_same_name_conflict"
    ]
    assert warns == [], "allow-listed merge emits NO conflict warning"
    # The single merged variable must carry a state per variant (the regression
    # this guards: a dropped state would still pass the merge/no-warn asserts).
    assert [
        w for w in _of(objs, IRWarning) if w.code == "sos_deldatamangd_unresolved"
    ] == []
    states = _of(objs, IRVariableState)
    assert len(states) == 2, "one state per variant on the merged variable"
    assert {s.register_variant_id for s in states} == {
        mint("sos", "lova", "LOVA"),
        mint("sos", "lova", "LOVA EXAMEN"),
    }


# ---------------------------------------------------------------------------
# 5. Variant synthesis (LSS/BU/SOL) — detect via sheet absence
# ---------------------------------------------------------------------------


def test_variant_synthesis_on_empty_deldatamangder() -> None:
    reg = _register(
        "lss", [_var("INSATS", data_type="Sträng (text)")], deldatamangder=()
    )
    objs, _ = _emit(reg)
    variants = _of(objs, IRVariant)
    assert len(variants) == 1
    assert variants[0].synthesized is True
    assert variants[0].name == "_default"


def test_bu_trap_populated_deldatamangd_but_no_sheet() -> None:
    # BU populates var.deldatamangd (Datavynamn) yet has NO Deldatamängder sheet:
    # must STILL synthesize _default (trigger is the empty deldatamangder tuple,
    # NOT var.deldatamangd).
    reg = _register(
        "bu",
        [_var("PNR", deldatamangd="bu_insats", data_type="Sträng (text)")],
        deldatamangder=(),
    )
    objs, _ = _emit(reg)
    variants = _of(objs, IRVariant)
    assert len(variants) == 1
    assert variants[0].synthesized is True
    states = _of(objs, IRVariableState)
    assert all(s.register_variant_id == variants[0].register_variant_id for s in states)


# ---------------------------------------------------------------------------
# 6. MFR IVF_klinik entity-registry collapse
# ---------------------------------------------------------------------------


def test_mfr_ivf_klinik_collapses_to_one_state() -> None:
    rows = [
        SosKodlistaRow(tidsperiod="1986-2007", kod="01", beskrivning="Karolinska"),
        SosKodlistaRow(tidsperiod="1990-2005", kod="02", beskrivning="Lucina"),
        SosKodlistaRow(tidsperiod="1985-2007", kod="03", beskrivning="Sophiahemmet"),
    ]
    reg = _register(
        "mfr",
        [_var("IVF_KLINIK", deldatamangd="MFR_IVF", data_type="Heltal")],
        deldatamangder=(_deldat("MFR_IVF"),),
        kodlistor=(_kodlista("IVF_klinik", rows),),
    )
    objs, _ = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 1, "entity registry collapses to ONE state, not per-period"
    # All 3 codes present, each with its own valid_from/to.
    # (codes live in value_set_member; assert via the adapter's written value set)


def test_non_allowlisted_kodlista_windows_per_period() -> None:
    # A normal drift kodlista with 2 disjoint tidsperiods -> 2 states.
    rows = [
        SosKodlistaRow(tidsperiod="2000-2010", kod="1", beskrivning="A"),
        SosKodlistaRow(tidsperiod="2011-2020", kod="2", beskrivning="B"),
    ]
    reg = _register(
        "dors",
        [_var("FODLAND", deldatamangd="DORS", data_type="Sträng (text)")],
        deldatamangder=(_deldat("DORS"),),
        kodlistor=(_kodlista("FODLAND", rows),),
    )
    objs, _ = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 2, "value-set drift fans into one state per window"


# ---------------------------------------------------------------------------
# 7. Tidsperiod parse + 3-way intersection
# ---------------------------------------------------------------------------


def test_parse_tidsperiod_forms() -> None:
    assert _parse_tidsperiod("1982-2007") == ("1982-01-01", "2007-12-31")
    assert _parse_tidsperiod("1990-") == ("1990-01-01", None)
    assert _parse_tidsperiod("2005") == ("2005-01-01", "2005-12-31")
    assert _parse_tidsperiod(None) == (None, None)
    assert _parse_tidsperiod("garbage") == (None, None)


def test_intersect_window_three_way() -> None:
    # max(froms), min(tos)
    assert _intersect_window(
        [("2000-01-01", "2010-12-31"), ("2005-01-01", None), (None, "2008-12-31")]
    ) == ("2005-01-01", "2008-12-31")
    # empty window -> None
    assert (
        _intersect_window([("2010-01-01", "2010-12-31"), ("2011-01-01", None)]) is None
    )


def test_iso_bound_handles_4_6_8_digit() -> None:
    assert _iso_bound(2005, end=False) == "2005-01-01"
    assert _iso_bound(2005, end=True) == "2005-12-31"
    assert _iso_bound(200502, end=False) == "2005-02-01"
    assert _iso_bound(200502, end=True) == "2005-02-28"
    assert _iso_bound(20050701, end=False) == "2005-07-01"
    assert _iso_bound(20050701, end=True) == "2005-07-01"
    assert _iso_bound(None, end=False) is None


def test_empty_window_drops_code() -> None:
    # A code whose tidsperiod is disjoint from the variable's data window is
    # dropped; the surviving window still yields a state.
    rows = [
        SosKodlistaRow(tidsperiod="1990-1995", kod="1", beskrivning="old"),
        SosKodlistaRow(tidsperiod="2010-2020", kod="2", beskrivning="new"),
    ]
    reg = _register(
        "dors",
        [
            _var(
                "X",
                deldatamangd="DORS",
                data_type="Sträng (text)",
                data_from=2008,
            )
        ],
        deldatamangder=(_deldat("DORS", data_from=2008),),
        kodlistor=(_kodlista("X", rows),),
    )
    objs, _ = _emit(reg)
    states = _of(objs, IRVariableState)
    # only the 2010-2020 window survives the >=2008 floor
    assert len(states) == 1
    assert states[0].valid_from >= "2010-01-01"


# ---------------------------------------------------------------------------
# Classification resolver (PR2): _resolve_by_signal + _resolve_classification.
# These are CURATED maps; the tests pin every signal + the precedence rules
# against real-data-shaped values (verified against input_data/Socialstyrelsen/).
# ---------------------------------------------------------------------------


class TestResolveBySignal:
    """`_resolve_by_signal` matches a `Länk kodverk` value to a classification
    short_name by curated substring (case-insensitive), or None."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            # All 8 curated signals → their short_name.
            ("https://icd.who.int/browse10/2019/en", "ICD-10-SE"),
            (
                "https://www.socialstyrelsen.se/statistik-och-data/"
                "klassifikationer-och-koder/icd-10/",
                "ICD-10-SE",
            ),
            ("https://icd.who.int/browse/releases/all-releases/en", "ICD-10-SE"),
            ("https://www.fass.se/LIF/atcregister?userType=0", "ATC"),
            ("https://atcddd.fhi.no/atc_ddd_index/", "ATC"),
            (
                "https://www.socialstyrelsen.se/statistik-och-data/"
                "klassifikationer-och-koder/kva/",
                "KVA",
            ),
            ("Klassifikation av vårdåtgärder (KVÅ) - Socialstyrelsen", "KVA"),
            (
                "https://www.socialstyrelsen.se/statistik-och-data/"
                "klassifikationer-och-koder/drg/drg-koder-och-definitioner/",
                "DRG",
            ),
            # Tolerances: trailing slash, uppercase URL, query string after signal.
            ("https://icd.who.int/browse10/2019/en/", "ICD-10-SE"),
            ("HTTPS://ICD.WHO.INT/BROWSE10/2019/EN", "ICD-10-SE"),
            ("https://icd.who.int/browse10/2019/en?lang=sv", "ICD-10-SE"),
            # The KVA bundled-PDF value: two Op6 PDFs THEN the kva fragment — the
            # CONTAINS match still resolves it to KVA.
            (
                "https://www.socialstyrelsen.se/globalassets/sharepoint-dokument/"
                "dokument-webb/klassifikationer-och-koder/"
                "klassifikation-av-operationer-sjatte-upplagan.pdf  "
                "https://www.socialstyrelsen.se/statistik-och-data/"
                "klassifikationer-och-koder/kva/",
                "KVA",
            ),
            # Unmatched signals → None (SCB systems, sheet refs, prose, empty).
            (
                "https://www.scb.se/.../standard-for-svensk-yrkesklassificering-ssyk/",
                None,
            ),
            (
                "https://www.scb.se/hitta-statistik/regional-statistik-och-kartor/"
                "regionala-indelningar/lan-och-kommuner/",
                None,
            ),
            ("Kodlista_DIAG", None),
            ("", None),
            (None, None),
        ],
    )
    def test_signal_map(self, value: str | None, expected: str | None) -> None:
        assert _resolve_by_signal(value) == expected


class TestResolveClassification:
    """`_resolve_classification` layers the CAN variable-name override on top of
    the signal map, with the signal map taking precedence."""

    def test_can_varname_override_resolves_icd9(self) -> None:
        # CAN's ext value is a globalassets PDF (no signal); the var name ICD9
        # names the seeded ICD-9-KS87.
        ext = (
            "https://www.socialstyrelsen.se/globalassets/sharepoint-dokument/"
            "artikelkatalog/statistik/2023-5-8512.pdf"
        )
        assert _resolve_classification(ext, "can", "ICD9") == "ICD-9-KS87"

    @pytest.mark.parametrize("var_name", ["ICD7", "M", "MORF"])
    def test_can_varname_override_unseeded_is_none(self, var_name: str) -> None:
        # The historical tumour/morphology systems are not seeded → None.
        ext = (
            "https://www.socialstyrelsen.se/globalassets/sharepoint-dokument/"
            "artikelkatalog/statistik/2023-5-8512.pdf"
        )
        assert _resolve_classification(ext, "can", var_name) is None

    def test_override_does_not_fire_for_non_can_register(self) -> None:
        # The variable-name override is CAN-only: a non-CAN register with a var
        # literally named "ICD9" must NOT inherit ICD-9-KS87.
        assert _resolve_classification(None, "par", "ICD9") is None

    def test_signal_wins_over_can_override(self) -> None:
        # A CAN ICD9 variable that DOES carry an icd-10 URL resolves by the
        # signal (ICD-10-SE), not the var-name override (ICD-9-KS87).
        assert (
            _resolve_classification(
                "https://icd.who.int/browse10/2019/en", "can", "ICD9"
            )
            == "ICD-10-SE"
        )


# ---------------------------------------------------------------------------
# Classification candidate accumulation (PR2): a RESOLVED variable contributes
# one `(variable_id, value_set_id, short_name)` per emitted state to
# `adapter.classification_candidates`; an UNRESOLVED variable contributes none.
# ---------------------------------------------------------------------------


def test_resolved_variable_accumulates_codeless_candidate() -> None:
    # A code-less SOS variable (no kodlista) whose ext value resolves to ICD-10-SE
    # yields one state with value_set_id None → one candidate keyed on
    # (variable_id, None).
    reg = _register(
        "par",
        [
            _var(
                "DIAGNOS",
                deldatamangd="PAR_OV",
                data_type="Sträng (text)",
                data_from=2001,
                external_classification="https://icd.who.int/browse10/2019/en",
            )
        ],
        deldatamangder=(_deldat("PAR_OV", data_from=2001),),
    )
    objs, adapter = _emit(reg)
    states = _of(objs, IRVariableState)
    variables = _of(objs, IRVariable)
    assert len(states) == 1 and len(variables) == 1
    variable_id = variables[0].variable_id
    assert states[0].value_set_id is None
    assert adapter.classification_candidates == [(variable_id, None, "ICD-10-SE")]


def test_resolved_variable_accumulates_code_bearing_candidate() -> None:
    # A code-bearing variable whose ext value resolves: the candidate's
    # value_set_id matches the emitted state's (non-None) value_set_id.
    rows = [SosKodlistaRow(tidsperiod="2001-2010", kod="A01", beskrivning="d")]
    reg = _register(
        "par",
        [
            _var(
                "OP",
                deldatamangd="PAR_OV",
                data_type="Sträng (text)",
                data_from=2001,
                external_classification=(
                    "https://www.socialstyrelsen.se/statistik-och-data/"
                    "klassifikationer-och-koder/kva/"
                ),
            )
        ],
        deldatamangder=(_deldat("PAR_OV", data_from=2001),),
        kodlistor=(_kodlista("OP", rows),),
    )
    objs, adapter = _emit(reg)
    states = _of(objs, IRVariableState)
    variables = _of(objs, IRVariable)
    assert len(states) == 1 and len(variables) == 1
    state = states[0]
    assert state.value_set_id is not None
    assert adapter.classification_candidates == [
        (variables[0].variable_id, state.value_set_id, "KVA")
    ]


def test_unresolved_variable_accumulates_no_candidate() -> None:
    # A variable whose ext value names no seeded system (an SCB SSYK URL) emits
    # states but contributes ZERO candidates.
    reg = _register(
        "par",
        [
            _var(
                "YRKE",
                deldatamangd="PAR_OV",
                data_type="Sträng (text)",
                data_from=2001,
                external_classification=(
                    "Standard för svensk yrkesklassificering (SSYK)"
                ),
            )
        ],
        deldatamangder=(_deldat("PAR_OV", data_from=2001),),
    )
    objs, adapter = _emit(reg)
    assert _of(objs, IRVariableState), "the variable still emits a state"
    assert adapter.classification_candidates == []


# ---------------------------------------------------------------------------
# valid_to reconciliation: merged members sharing valid_from but differing
# valid_to must collapse to the WIDEST window, not the first in delivery order
# (the state_id / idx_variable_state_unique basis excludes valid_to).
# ---------------------------------------------------------------------------


def test_merged_members_same_start_widen_valid_to() -> None:
    # Two same-name variant-less members (BU shape: all -> _default) share
    # valid_from=1960 but end in different years. They mint the same state_id
    # (valid_to is not in the basis); the surviving state must carry the WIDEST
    # valid_to, never the narrower first-in-order one.
    reg = _register(
        "bu",
        [
            _var("KON", data_from=1960, data_to=2013),
            _var("KON", data_from=1960, data_to=2016),
        ],
    )
    states = _of(_emit(reg)[0], IRVariableState)
    assert len(states) == 1, "same-start members collapse to one state"
    assert states[0].valid_from == "1960-01-01"
    assert states[0].valid_to == "2016-12-31", "widest valid_to wins, not the first"


def test_merged_member_open_end_not_silently_closed() -> None:
    # PAR shape: one member is open-ended (data_to=None, still delivered), another
    # closes at 2014. The reconciliation must keep the state OPEN (valid_to None),
    # never report the variable as retired. Order-independent.
    forward = _register(
        "par",
        [_var("SLUT", data_from=2001, data_to=2014), _var("SLUT", data_from=2001)],
    )
    reverse = _register(
        "par",
        [_var("SLUT", data_from=2001), _var("SLUT", data_from=2001, data_to=2014)],
    )
    for reg in (forward, reverse):
        states = _of(_emit(reg)[0], IRVariableState)
        assert len(states) == 1
        assert states[0].valid_to is None, "open-ended member must not be closed"


# ---------------------------------------------------------------------------
# #401: inline `Värdemängd` -> value set, fallback when the variable has no
# Kodlista_* sheet (the #373 deferral). Classifier rules are pinned against the
# real-corpus shapes (verified against input_data/Socialstyrelsen/).
# ---------------------------------------------------------------------------


class TestClassifyValueSetText:
    """`_classify_value_set_text` is conservative: it returns (code, label)
    pairs ONLY for clean enumerations and ``None`` for anything ambiguous (a
    wrong reject is a no-op — the variable stays code-less, exactly today)."""

    @pytest.mark.parametrize(
        "text",
        [
            None,
            "",
            "   ",
            "Fritext",  # single descriptor, not an enumeration
            "1=ja",  # single pair -> not an enumeration
        ],
    )
    def test_none_empty_or_single_segment_rejected(self, text: str | None) -> None:
        assert _classify_value_set_text(text) is None

    def test_kod_klartext_semicolon(self) -> None:
        assert _classify_value_set_text("1=ja; 0=nej; 9=uppgift saknas") == [
            ("1", "ja"),
            ("0", "nej"),
            ("9", "uppgift saknas"),
        ]

    def test_kod_klartext_newline_is_dominant_form(self) -> None:
        text = (
            "0 = Korrekt personnummer\n4 = Samordningsnummer\n8 = Ogiltigt personnummer"
        )
        assert _classify_value_set_text(text) == [
            ("0", "Korrekt personnummer"),
            ("4", "Samordningsnummer"),
            ("8", "Ogiltigt personnummer"),
        ]

    def test_alpha_codes_with_labels(self) -> None:
        # Letter codes (BM/LK/...) carry labels; labels may contain spaces.
        assert _classify_value_set_text("1=Man; 2=Kvinna") == [
            ("1", "Man"),
            ("2", "Kvinna"),
        ]

    def test_label_may_contain_comma_and_colon(self) -> None:
        # Only the CODE is charset-constrained; the label is free text.
        assert _classify_value_set_text(
            "1=riksavtal; 2=regionalt, flerregionalt: avtal"
        ) == [("1", "riksavtal"), ("2", "regionalt, flerregionalt: avtal")]

    def test_label_may_contain_equals(self) -> None:
        # Partition on the FIRST `=` only; a label may itself contain `=`.
        assert _classify_value_set_text("1=a=b; 2=c") == [("1", "a=b"), ("2", "c")]

    def test_swedish_char_codes_accepted(self) -> None:
        # Codes may carry Swedish letters (_clean_value_code accepts them).
        assert _classify_value_set_text("Å=alternativ; Ö=övrigt") == [
            ("Å", "alternativ"),
            ("Ö", "övrigt"),
        ]

    def test_bare_codes_semicolon(self) -> None:
        # The LOVA styrtabell case: bare codes, no inline labels.
        assert _classify_value_set_text("1;2;3;4;5;9") == [
            ("1", None),
            ("2", None),
            ("3", None),
            ("4", None),
            ("5", None),
            ("9", None),
        ]

    def test_bare_alpha_codes(self) -> None:
        assert _classify_value_set_text("LEG;SPEC") == [("LEG", None), ("SPEC", None)]

    @pytest.mark.parametrize(
        "text",
        [
            "0-744 = antal timmar",  # numeric range in code
            "1964-1968: ICD-7 i Klassifikation",  # year-prose (single segment anyway)
            "0=giltigt pnr; 4=samordningsnummer; strängen är tom",  # trailing prose (mixed =)
            "1;5,6;7;8",  # comma inside a code
            "1;2;1",  # duplicate code
            "01=till moder  02=till fader",  # multi-space (single segment, embedded =)
            "1= ; 2=nej",  # whitespace-only label -> rejected (empty after strip)
        ],
    )
    def test_messy_cells_rejected(self, text: str) -> None:
        assert _classify_value_set_text(text) is None


def _value_set_codes(
    conn: sqlite3.Connection, value_set_id: int
) -> set[tuple[str, str]]:
    return {
        (r[0], r[1])
        for r in conn.execute(
            "SELECT vc.code, vc.label FROM value_set_member vsm "
            "JOIN value_code vc ON vc.code_id = vsm.code_id "
            "WHERE vsm.value_set_id = ?",
            (value_set_id,),
        )
    }


def test_vardemangd_binds_when_no_kodlista() -> None:
    # No Kodlista_KON sheet -> the inline Värdemängd promotes to a value set.
    reg = _register(
        "bu",
        [_var("KON", value_set_text="1=Man; 2=Kvinna", data_from=2001)],
    )
    objs, adapter = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 1
    assert states[0].value_set_id is not None
    assert states[0].value_set_version_label is None, "Värdemängd has no Tidsperiod"
    assert _value_set_codes(adapter.conn, states[0].value_set_id) == {
        ("1", "Man"),
        ("2", "Kvinna"),
    }


def test_vardemangd_bare_codes_bind_with_empty_labels() -> None:
    # bu shape: variant-less (all -> _default), so no deldatamängd token to
    # resolve — isolates the bare-code binding. (The LOVA styrtabell case is the
    # real-corpus source of bare codes; LOVA tokens go through the curated map.)
    reg = _register(
        "bu",
        [_var("STYR", value_set_text="1;2;3", data_from=2001)],
    )
    objs, adapter = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 1
    assert states[0].value_set_id is not None
    # bare codes -> label is the empty string in value_code (label=None classifies
    # to "" at IRValueCode, matching the kodlista path's `r.beskrivning or ""`).
    assert _value_set_codes(adapter.conn, states[0].value_set_id) == {
        ("1", ""),
        ("2", ""),
        ("3", ""),
    }


def test_vardemangd_free_text_stays_codeless() -> None:
    # A free-text descriptor (single segment) is NOT promoted — exactly today's
    # behavior: the variable still emits a state, with value_set_id None.
    reg = _register(
        "bu",
        [_var("NOTE", value_set_text="Fritext, ingen kodning", data_from=2001)],
    )
    objs, _ = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 1
    assert states[0].value_set_id is None


def test_kodlista_wins_over_vardemangd() -> None:
    # A variable WITH a Kodlista_* sheet ignores its Värdemängd cell entirely:
    # binding only fires in the kodlista-less branch.
    rows = [SosKodlistaRow(tidsperiod="2001-2010", kod="A01", beskrivning="d")]
    reg = _register(
        "dors",
        [
            _var(
                "DIAG",
                deldatamangd="DORS",
                value_set_text="1=Man; 2=Kvinna",  # would classify, but kodlista wins
                data_from=2001,
            )
        ],
        deldatamangder=(_deldat("DORS", data_from=2001),),
        kodlistor=(_kodlista("DIAG", rows),),
    )
    objs, adapter = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 1
    assert states[0].value_set_id is not None
    # the kodlista code, NOT the Värdemängd codes
    assert _value_set_codes(adapter.conn, states[0].value_set_id) == {("A01", "d")}


def test_vardemangd_content_shares_value_set() -> None:
    # Two distinct kodlista-less variables with the SAME Värdemängd cell
    # content-share one value_set (the _ensure_value_set member-hash dedup).
    reg = _register(
        "bu",
        [
            _var("A", value_set_text="1=ja; 0=nej", data_from=2001),
            _var("B", value_set_text="1=ja; 0=nej", data_from=2001),
        ],
    )
    objs, _adapter = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 2
    ids = {s.value_set_id for s in states}
    assert len(ids) == 1 and None not in ids, "identical Värdemängd shares one set"


def test_merged_member_vardemangd_conflict_warns_keeps_first() -> None:
    # Two variant-less merged members (BU shape: all -> _default) share
    # valid_from=2001 -> the SAME state_id (value_set_version_label is None for
    # the Värdemängd path), but classify to DIFFERENT value sets. The second's
    # value_set_id would be silently dropped by the valid_to-widening
    # reconciliation; instead the adapter WARNS and keeps the first
    # deterministically. The first member ends EARLIER (data_to=2010) than the
    # second (data_to=2016): because the sets diverge, `valid_to` must NOT widen
    # — the surviving (first) set keeps its OWN 2010 window, never claiming its
    # codes for 2011-2016.
    reg = _register(
        "bu",
        [
            _var(
                "VARUTYP",
                value_set_text="EX=Receptfritt; HA=Handelsvara",
                data_from=2001,
                data_to=2010,
            ),
            _var(
                "VARUTYP",
                value_set_text="EX=Receptfritt; HA=Handelsvara; OV=Ovrigt",
                data_from=2001,
                data_to=2016,
            ),
        ],
    )
    objs, adapter = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 1, "same-start members collapse to one state"
    warns = [w for w in _of(objs, IRWarning) if w.code == "sos_value_set_text_conflict"]
    assert len(warns) == 1, "divergent Värdemängd on a shared state_id must WARN"
    assert "VARUTYP" in (warns[0].detail or "")
    # first member's value set kept (2 codes), not the second's (3 codes).
    assert _value_set_codes(adapter.conn, states[0].value_set_id) == {
        ("EX", "Receptfritt"),
        ("HA", "Handelsvara"),
    }
    assert states[0].valid_to == "2010-12-31", (
        "divergent sets: surviving set keeps its OWN window, valid_to NOT widened"
    )


@pytest.mark.parametrize("coded_first", [True, False])
def test_merged_member_asymmetric_vardemangd_prefers_coded(coded_first: bool) -> None:
    # #401 regression (prefer-coded + Codex P2 window-clamp): one merged member
    # classifies, the other is free text (value_set_id None). They share
    # valid_from=1960 -> the SAME state_id, so they collide in the reconciliation.
    # The CODED member must win regardless of delivery order — a codeless member
    # arriving first must NOT silently drop the sibling's value set — and this is
    # a coalesce, not a conflict, so NO warning fires.
    #
    # This is the real `bu/SPEC` shape: coded set ends 2014, codeless tail runs
    # to 2016. The surviving coded set must keep ITS OWN window (valid_to 2014),
    # never widen to 2016 — extending codes 2015-2016 where the metadata never
    # defined them is a code-search false positive. The codeless tail's extra
    # coverage is forfeited (a dropped codeless tail < an over-claimed code
    # window).
    coded = _var("SPEC", value_set_text="1=Man; 2=Kvinna", data_from=1960, data_to=2014)
    codeless = _var(
        "SPEC", value_set_text="Fritext, ingen kodning", data_from=1960, data_to=2016
    )
    reg = _register(
        "bu",
        [coded, codeless] if coded_first else [codeless, coded],
    )
    objs, adapter = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 1, "same-start members collapse to one state"
    assert [
        w for w in _of(objs, IRWarning) if w.code == "sos_value_set_text_conflict"
    ] == [], "coded-vs-codeless is a coalesce, not a conflict"
    assert states[0].value_set_id is not None, "the coded value set survives"
    assert _value_set_codes(adapter.conn, states[0].value_set_id) == {
        ("1", "Man"),
        ("2", "Kvinna"),
    }
    assert states[0].valid_to == "2014-12-31", (
        "coded set keeps its OWN window; codeless 2016 tail must NOT extend codes"
    )


def test_merged_member_prefer_coded_keeps_prior_data_type() -> None:
    # #401 correctness: prefer-coded must NOT flip `data_type`. A divergent
    # data_type across same-name members is a WARN-MERGE (one variable), and the
    # members share valid_from=2001 -> the SAME state_id, so they collide in
    # `_emit_states._collect`. The codeless member arrives FIRST (becomes
    # `prior`, data_type "heltal"); the coded member arrives second (`obj`,
    # data_type "sträng (text)"). The stored row must adopt obj's value_set_id
    # (prefer-coded) while KEEPING prior's data_type — `data_type` is excluded
    # from the state_id basis, so basing the row on obj would silently flip it.
    codeless = _var(
        "KON",
        value_set_text="Fritext, ingen kodning",
        data_type="Heltal",
        data_from=2001,
    )
    coded = _var(
        "KON",
        value_set_text="1=Man; 2=Kvinna",
        data_type="Sträng (text)",
        data_from=2001,
    )
    reg = _register("bu", [codeless, coded])  # codeless first -> prior
    objs, adapter = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 1, "same-start members collapse to one state"
    assert states[0].data_type == "heltal", (
        "stored row keeps prior (codeless, first) data_type, not obj's"
    )
    assert states[0].value_set_id is not None, "the coded value set is still adopted"
    assert _value_set_codes(adapter.conn, states[0].value_set_id) == {
        ("1", "Man"),
        ("2", "Kvinna"),
    }
    # Prefer-coded coalesce is not a value-set conflict; the divergent data_type
    # is the separate (expected) warn-merge signal.
    assert [
        w for w in _of(objs, IRWarning) if w.code == "sos_value_set_text_conflict"
    ] == []


def test_merged_member_identical_vardemangd_does_not_warn() -> None:
    # Same shape as the conflict test but the members carry the SAME Värdemängd:
    # they content-share one value_set, no collision, no warning.
    reg = _register(
        "bu",
        [
            _var("KON", value_set_text="1=Man; 2=Kvinna", data_from=2001),
            _var("KON", value_set_text="1=Man; 2=Kvinna", data_from=2001),
        ],
    )
    objs, _ = _emit(reg)
    assert _of(objs, IRVariableState)
    assert [
        w for w in _of(objs, IRWarning) if w.code == "sos_value_set_text_conflict"
    ] == []


def test_merged_member_same_value_set_different_window_widens() -> None:
    # Two merged members with the SAME Värdemängd (content-shared value set) but
    # DIFFERENT end years collide on one state_id. Because the value set is
    # identical, widening `valid_to` is a genuine coverage union (no code
    # over-claim) — the surviving state must carry the WIDEST window, not the
    # narrower first-in-order one.
    reg = _register(
        "bu",
        [
            _var("KON", value_set_text="1=Man; 2=Kvinna", data_from=2001, data_to=2010),
            _var("KON", value_set_text="1=Man; 2=Kvinna", data_from=2001, data_to=2016),
        ],
    )
    objs, adapter = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 1, "same-start members collapse to one state"
    assert states[0].value_set_id is not None
    assert _value_set_codes(adapter.conn, states[0].value_set_id) == {
        ("1", "Man"),
        ("2", "Kvinna"),
    }
    assert states[0].valid_to == "2016-12-31", (
        "identical value set: widening valid_to is a safe coverage union"
    )
    assert [
        w for w in _of(objs, IRWarning) if w.code == "sos_value_set_text_conflict"
    ] == []


# ---------------------------------------------------------------------------
# Overlap-suppression post-pass (#401): two merged members on the same
# variant+column with DIFFERENT valid_from (so DISTINCT state_ids, never
# reconciled by `_collect`) whose Värdemängd cells classify to DISTINCT value
# sets over OVERLAPPING windows would resolve a period to >1 value set (the
# build invariant). The Värdemängd path has no Tidsperiod to segment on, so
# the adapter conservatively nulls BOTH conflicting states back to code-less
# and WARNS. Disjoint windows are a legitimate era change and stay bound.
# ---------------------------------------------------------------------------


def test_overlapping_members_distinct_vardemangd_suppressed() -> None:
    # bu/SPEC shape: two variant-less merged members (all -> _default, one
    # column) classify to DIFFERENT value sets over OVERLAPPING windows
    # (1960-2016 vs 2001-2014). Different valid_from -> distinct state_ids, so
    # `_collect` never sees them; the post-pass must null BOTH back to
    # code-less (no Tidsperiod to segment the Värdemängd on) and warn once.
    reg = _register(
        "bu",
        [
            _var(
                "SPEC",
                value_set_text="1=Man; 2=Kvinna",
                data_from=1960,
                data_to=2016,
            ),
            _var(
                "SPEC",
                value_set_text="1=Man; 2=Kvinna; 3=Annat",
                data_from=2001,
                data_to=2014,
            ),
        ],
    )
    objs, _ = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 2, "different valid_from -> two distinct states"
    assert all(s.value_set_id is None for s in states), (
        "both conflicting states reverted to code-less (pre-#401 behavior)"
    )
    warns = [w for w in _of(objs, IRWarning) if w.code == "sos_value_set_text_overlap"]
    assert len(warns) == 1, "one warning per affected (variable, column)"
    assert "SPEC" in (warns[0].detail or "")


def test_disjoint_members_distinct_vardemangd_stay_bound() -> None:
    # Negative: same variant+column, distinct Värdemängd, but DISJOINT windows
    # (2001-2005 vs 2006-2010) — a legitimate era change. No overlap, so both
    # states keep their (distinct) value sets and NO suppression warning fires.
    reg = _register(
        "bu",
        [
            _var(
                "SPEC",
                value_set_text="1=Man; 2=Kvinna",
                data_from=2001,
                data_to=2005,
            ),
            _var(
                "SPEC",
                value_set_text="1=Man; 2=Kvinna; 3=Annat",
                data_from=2006,
                data_to=2010,
            ),
        ],
    )
    objs, _ = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 2
    assert all(s.value_set_id is not None for s in states), (
        "disjoint-window era change must stay bound"
    )
    assert len({s.value_set_id for s in states}) == 2, (
        "the two eras carry their own distinct value sets"
    )
    assert [
        w for w in _of(objs, IRWarning) if w.code == "sos_value_set_text_overlap"
    ] == []


# ---------------------------------------------------------------------------
# #464: defer the Värdemängd value-set write until AFTER conflict resolution, so
# a set that reconciliation later drops (a `_collect` divergent collision or an
# overlap-post-pass null) never leaves orphaned value_set/value_code rows. Pre-#464
# the eager member-loop write persisted those rows even when the surviving state
# referenced a different set (or none) — they then leaked into unscoped value
# search as context-less mapping_count=0 hits. These tests assert at the DB-ROW
# level (the behavior tests above only inspect the surviving state's value_set_id,
# which was already correct — the orphan was invisible to them).
# ---------------------------------------------------------------------------


def _orphan_value_sets(objs: list, conn: sqlite3.Connection) -> set[int]:
    """value_set rows in the DB NOT referenced by any emitted state's
    value_set_id — i.e. orphans. Empty set == no orphan."""
    referenced = {
        s.value_set_id for s in _of(objs, IRVariableState) if s.value_set_id is not None
    }
    written = {r[0] for r in conn.execute("SELECT value_set_id FROM value_set")}
    return written - referenced


def _all_value_codes(conn: sqlite3.Connection) -> set[tuple[str, str]]:
    """Every (code, label) reachable from a value_set_member row — i.e. every
    code that `_populate_fts(include_value_code=True)` would index."""
    return {
        (r[0], r[1])
        for r in conn.execute(
            "SELECT DISTINCT vc.code, vc.label FROM value_set_member vsm "
            "JOIN value_code vc ON vc.code_id = vsm.code_id"
        )
    }


def test_overlap_suppressed_vardemangd_writes_no_orphan_rows() -> None:
    # bu/SPEC shape (the measured 2-set / 3-unique-code orphan): two merged
    # members, distinct Värdemängd, overlapping windows -> the post-pass nulls
    # BOTH. Pre-#464 each member's eager write left its value_set + the unique
    # codes orphaned (no surviving state references them); #464 defers the write,
    # so the suppressed sets are NEVER written. Assert ZERO orphans and that the
    # suppressed-only codes (the unique "3=Annat" tail) are absent from the DB.
    reg = _register(
        "bu",
        [
            _var(
                "SPEC", value_set_text="1=Man; 2=Kvinna", data_from=1960, data_to=2016
            ),
            _var(
                "SPEC",
                value_set_text="1=Man; 2=Kvinna; 3=Annat",
                data_from=2001,
                data_to=2014,
            ),
        ],
    )
    objs, adapter = _emit(reg)
    states = _of(objs, IRVariableState)
    assert all(s.value_set_id is None for s in states), "both states suppressed"
    assert _orphan_value_sets(objs, adapter.conn) == set(), (
        "#464: a post-pass-nulled Värdemängd set must leave NO orphaned value_set row"
    )
    # The suppressed codes never reach the DB -> they can't surface in value search.
    codes = _all_value_codes(adapter.conn)
    assert ("3", "Annat") not in codes, "suppressed-set-only code must be absent"
    assert ("1", "Man") not in codes and ("2", "Kvinna") not in codes, (
        "both members were suppressed; none of their codes should be written"
    )


def test_collect_divergent_collision_writes_only_survivor() -> None:
    # `_collect` prefer-coded path: a codeless `prior` + a coded `obj` share one
    # state_id (same valid_from). The coded set survives. There is no losing set
    # here (the dropped side is free text), so this asserts the survivor IS the
    # only value_set written and there are no orphans. The both-coded conflict
    # variant (a real dropped set) is the next test.
    reg = _register(
        "bu",
        [
            _var("KON", value_set_text="Fritext, ingen kodning", data_from=2001),
            _var("KON", value_set_text="1=Man; 2=Kvinna", data_from=2001),
        ],
    )
    objs, adapter = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 1 and states[0].value_set_id is not None
    assert _orphan_value_sets(objs, adapter.conn) == set(), "survivor only, no orphan"
    assert _value_set_codes(adapter.conn, states[0].value_set_id) == {
        ("1", "Man"),
        ("2", "Kvinna"),
    }


def test_collect_both_coded_conflict_drops_loser_no_orphan() -> None:
    # `_collect` both-coded conflict: two merged members share state_id but
    # classify to DIFFERENT non-None sets. `prior` (first in delivery order) is
    # kept + a conflict warning fires; the loser's set is dropped. Pre-#464 the
    # loser's eager write orphaned its value_set + its unique code ("OV"); #464
    # never writes it. Assert the survivor's 2 codes are present, the loser's
    # unique code is ABSENT, and there are no orphaned value_set rows.
    reg = _register(
        "bu",
        [
            _var(
                "VARUTYP",
                value_set_text="EX=Receptfritt; HA=Handelsvara",
                data_from=2001,
                data_to=2010,
            ),
            _var(
                "VARUTYP",
                value_set_text="EX=Receptfritt; HA=Handelsvara; OV=Ovrigt",
                data_from=2001,
                data_to=2016,
            ),
        ],
    )
    objs, adapter = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 1
    assert [
        w for w in _of(objs, IRWarning) if w.code == "sos_value_set_text_conflict"
    ], "both-coded divergence still warns"
    assert _orphan_value_sets(objs, adapter.conn) == set(), (
        "#464: the dropped (loser) value set must leave NO orphaned row"
    )
    codes = _all_value_codes(adapter.conn)
    assert ("OV", "Ovrigt") not in codes, "loser-set-only code must be absent"
    # survivor's codes are still written and bound.
    assert _value_set_codes(adapter.conn, states[0].value_set_id) == {
        ("EX", "Receptfritt"),
        ("HA", "Handelsvara"),
    }


def test_surviving_vardemangd_set_is_still_written_and_bound() -> None:
    # Regression lock (happy path unchanged): a NON-dropped Värdemängd set is
    # still written and bound after the deferral, with NO orphans.
    reg = _register(
        "bu",
        [_var("KON", value_set_text="1=Man; 2=Kvinna", data_from=2001)],
    )
    objs, adapter = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 1 and states[0].value_set_id is not None
    assert _orphan_value_sets(objs, adapter.conn) == set()
    assert _value_set_codes(adapter.conn, states[0].value_set_id) == {
        ("1", "Man"),
        ("2", "Kvinna"),
    }


def test_deferred_identical_vardemangd_still_content_shares_one_set() -> None:
    # Regression lock (content-share intact): two SURVIVING states with identical
    # Värdemängd content must still collapse to ONE value_set_id after the
    # deferral — the deferred survivor write hashes the SAME (code, label) pairs,
    # so `_ensure_value_set` content-addresses them onto the same row. (Two
    # distinct variables so neither is dropped.)
    reg = _register(
        "bu",
        [
            _var("A", value_set_text="1=ja; 0=nej", data_from=2001),
            _var("B", value_set_text="1=ja; 0=nej", data_from=2001),
        ],
    )
    objs, adapter = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 2
    ids = {s.value_set_id for s in states}
    assert len(ids) == 1 and None not in ids, (
        "#464: identical deferred content still shares one value_set_id"
    )
    assert _orphan_value_sets(objs, adapter.conn) == set()
    # exactly one value_set row exists for this single shared set.
    assert adapter.conn.execute("SELECT COUNT(*) FROM value_set").fetchone()[0] == 1, (
        "content-share writes exactly one value_set row"
    )


def test_eager_kodlista_and_deferred_vardemangd_share_one_value_set() -> None:
    # Cross-path content-share lock: an EAGERLY-written kodlista set and a
    # DEFERRED Värdemängd set with IDENTICAL (code, label) content must collapse
    # to ONE value_set_id. The two write paths hash differently if they ever
    # diverge — the kodlista path calls `_ensure_value_set` inline, the #464
    # Värdemängd path computes a deferred `member_hash` over the SAME
    # `[(c.code, c.label)]` pairs and replays them into `_ensure_value_set` at the
    # survivor write. Both must content-address onto the same row. In ONE adapter
    # run: variable A is kodlista-backed {1=Man; 2=Kvinna} (eager write), variable
    # B carries the same content as a Värdemängd cell (deferred write). Assert both
    # surviving states reference the SAME non-None value_set_id AND exactly one
    # value_set row exists. Fails if the deferred path hashed differently and
    # created a duplicate set.
    rows = [
        SosKodlistaRow(tidsperiod=None, kod="1", beskrivning="Man"),
        SosKodlistaRow(tidsperiod=None, kod="2", beskrivning="Kvinna"),
    ]
    reg = _register(
        "bu",
        [
            _var("A", data_type="Sträng (text)", data_from=2001),
            _var("B", value_set_text="1=Man; 2=Kvinna", data_from=2001),
        ],
        kodlistor=(_kodlista("A", rows),),
    )
    objs, adapter = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 2, "one state per variable (A kodlista, B Värdemängd)"
    ids = {s.value_set_id for s in states}
    assert len(ids) == 1 and None not in ids, (
        "#464: eager kodlista + deferred Värdemängd of identical content share "
        "ONE value_set_id"
    )
    assert _orphan_value_sets(objs, adapter.conn) == set()
    # the cross-path share writes exactly one value_set row, not two.
    assert adapter.conn.execute("SELECT COUNT(*) FROM value_set").fetchone()[0] == 1, (
        "cross-path content-share writes exactly one value_set row"
    )
    assert _value_set_codes(adapter.conn, states[0].value_set_id) == {
        ("1", "Man"),
        ("2", "Kvinna"),
    }


def test_resolved_surviving_vardemangd_candidate_reads_final_value_set_id() -> None:
    # Survivor-write-BEFORE-candidate-append ordering lock. The #464 survivor write
    # materializes the deferred Värdemängd value set and `model_copy`s its id onto
    # the state; the classification-candidate append then reads `state.value_set_id`.
    # If that append were ever reordered ahead of the survivor write it would read
    # the pre-write placeholder (None) and record a STALE code-less candidate for a
    # variable whose set actually survives.
    #
    # DEVIATION from the original sketch (a RESOLVED variable whose overlapping
    # members are overlap-SUPPRESSED, asserting a `(variable_id, None, ...)`
    # candidate): that shape is VACUOUS for this invariant. A suppressed state's
    # `value_set_id` is None under BOTH orderings (the pre-survivor-write state
    # already carries None, and suppression nulls the pending), so the candidate is
    # `(variable_id, None, ...)` whether or not the append is reordered — it does
    # NOT fail-on-break (verified by simulating the reorder). The ordering is only
    # observable on a SURVIVING set, where the correct order records the non-None id
    # and the reordered break records None. So this locks the surviving case: one
    # resolved (ICD-10-SE via an icd.who.int URL) Värdemängd-backed variable whose
    # set survives -> the candidate's value_set_id must equal the state's non-None
    # id (only true if the survivor write ran first). The Värdemängd path is the
    # only candidate path sensitive to this reorder (kodlista/entity-registry write
    # their id onto the state directly, not via the deferred survivor write).
    reg = _register(
        "bu",
        [
            _var(
                "DX",
                value_set_text="1=Man; 2=Kvinna",
                data_from=2001,
                external_classification="https://icd.who.int/browse10/2019/en",
            )
        ],
    )
    objs, adapter = _emit(reg)
    states = _of(objs, IRVariableState)
    variables = _of(objs, IRVariable)
    assert len(states) == 1 and len(variables) == 1
    state = states[0]
    assert state.value_set_id is not None, "the Värdemängd set survives (not dropped)"
    assert adapter.classification_candidates == [
        (variables[0].variable_id, state.value_set_id, "ICD-10-SE")
    ], (
        "#464: the candidate append must read the FINAL (survivor-written) "
        "value_set_id, never the pre-write None"
    )


# ---------------------------------------------------------------------------
# #401 Fix A (Codex P2): the divergent-window reconciliation + overlap-
# suppression post-pass are VÄRDEMÄNGD-ONLY (kodlista is None). A KODLISTA-backed
# variable keeps the ORIGINAL pre-#401 reconciliation (always widen valid_to) and
# is EXEMPT from the post-pass — kodlista paths own their own value sets and a
# genuine conflict is caught by the build invariant, never silently nulled here.
# ---------------------------------------------------------------------------


def test_kodlista_collision_widens_valid_to_and_stays_bound() -> None:
    # Two variant-less merged members (bu shape: all -> _default) of a
    # KODLISTA-backed variable share valid_from=1960 but end in different years.
    # The shared kodlista is open (no Tidsperiod) -> each member emits one
    # single-segment state (label "") with seg_from=1960 -> the SAME state_id, and
    # since `_ensure_value_set` hashes only (code, label) both carry the SAME
    # value_set_id. For a kodlista collision the reconciliation keeps the ORIGINAL
    # behavior: ALWAYS widen valid_to (no divergent-window clamp, no prefer-coded
    # path). The post-pass is Värdemängd-only, so the kodlista value set is NOT
    # nulled.
    rows = [SosKodlistaRow(tidsperiod=None, kod="A01", beskrivning="d")]
    reg = _register(
        "bu",
        [
            _var("DIAG", data_type="Sträng (text)", data_from=1960, data_to=2013),
            _var("DIAG", data_type="Sträng (text)", data_from=1960, data_to=2016),
        ],
        kodlistor=(_kodlista("DIAG", rows),),
    )
    objs, adapter = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 1, "same-start kodlista members collapse to one state"
    assert states[0].value_set_id is not None, (
        "kodlista value set must NOT be nulled by the Värdemängd-only post-pass"
    )
    assert _value_set_codes(adapter.conn, states[0].value_set_id) == {("A01", "d")}
    assert states[0].valid_to == "2016-12-31", (
        "kodlista collision keeps the original widen-always reconciliation"
    )
    # neither the Värdemängd conflict nor the overlap-suppression warning fires
    # for a kodlista-backed variable.
    assert [
        w
        for w in _of(objs, IRWarning)
        if w.code in ("sos_value_set_text_conflict", "sos_value_set_text_overlap")
    ] == []


def test_kodlista_windowed_drift_states_not_suppressed() -> None:
    # A kodlista-backed variable whose codes drift across two windows fans into
    # two states with DISTINCT value sets (one per era). They sit on the same
    # variant+column with distinct value sets — exactly the shape the Värdemängd
    # post-pass would null — but because the variable is kodlista-backed the
    # post-pass leaves both value sets intact (the kodlista path segmented on
    # Tidsperiod, so the windows are disjoint and the invariant already holds).
    rows = [
        SosKodlistaRow(tidsperiod="2000-2010", kod="1", beskrivning="A"),
        SosKodlistaRow(tidsperiod="2011-2020", kod="2", beskrivning="B"),
    ]
    reg = _register(
        "dors",
        [_var("FODLAND", deldatamangd="DORS", data_type="Sträng (text)")],
        deldatamangder=(_deldat("DORS"),),
        kodlistor=(_kodlista("FODLAND", rows),),
    )
    objs, _ = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 2
    assert all(s.value_set_id is not None for s in states), (
        "kodlista-derived value sets must never be nulled by the post-pass"
    )
    assert len({s.value_set_id for s in states}) == 2
    assert [
        w for w in _of(objs, IRWarning) if w.code == "sos_value_set_text_overlap"
    ] == []


# ---------------------------------------------------------------------------
# #401 Fix B: a variable with a kodlista sheet that was skipped as unparseable
# (raw_rows) reaches the Värdemängd branch with kodlista is None, but
# kodlista-wins -> it stays code-less, never fabricating codes from Värdemängd.
# Defensive (0 corpus occurrences); the warning still fires per the existing
# unparseable-kodlista path.
# ---------------------------------------------------------------------------


def test_raw_kodlista_wins_over_vardemangd_stays_codeless() -> None:
    # The variable HAS a Kodlista_DIAG sheet, but it parsed as raw_rows
    # (unparseable header) -> skipped from kodlista_by_var. The variable also
    # carries a classifiable Värdemängd. Kodlista-wins: the variable must stay
    # code-less (value_set_id None), the Värdemängd is NOT bound.
    reg = _register(
        "dors",
        [
            _var(
                "DIAG",
                deldatamangd="DORS",
                value_set_text="1=Man; 2=Kvinna",  # would classify if not suppressed
                data_from=2001,
            )
        ],
        deldatamangder=(_deldat("DORS", data_from=2001),),
        kodlistor=(_kodlista("DIAG", [], raw=True),),
    )
    objs, _ = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 1
    assert states[0].value_set_id is None, (
        "an unparseable kodlista sheet still wins: no Värdemängd fabrication"
    )
    # the existing unparseable-kodlista warning still fires.
    assert [w for w in _of(objs, IRWarning) if w.code == "sos_kodlista_unparseable"], (
        "the raw kodlista still surfaces its skip warning"
    )


# ---------------------------------------------------------------------------
# P2#1: unresolved variable-sheet deldatamängd -> WARN + drop (not silent)
# ---------------------------------------------------------------------------


def test_unresolved_deldatamangd_warns_and_drops_member() -> None:
    # The variable names a deldatamängd token that has NO row in the
    # Deldatamängder sheet AND no DELDATAMANGD_TOKEN_MAP entry (a new workbook
    # revision shape). The adapter WARNS (sos_deldatamangd_unresolved) and drops
    # the member — it does NOT silently `continue`, and does NOT invent a
    # variant mapping (the fix is a curated token-map entry).
    reg = _register(
        "lova",
        [_var("KOMMUN", deldatamangd="A_UNCURATED", data_type="Sträng (text)")],
        # the only sheet deldatamängd is a DIFFERENT name -> A_UNCURATED unresolved
        deldatamangder=(_deldat("LOVA"),),
    )
    objs, _ = _emit(reg)
    warns = [w for w in _of(objs, IRWarning) if w.code == "sos_deldatamangd_unresolved"]
    assert len(warns) == 1, "unresolved deldatamängd must WARN, not drop silently"
    assert "A_UNCURATED" in (warns[0].detail or "")
    assert "DELDATAMANGD_TOKEN_MAP" in (warns[0].detail or "")
    # the member is dropped -> no state for it (the var still gets an IRVariable
    # row, but zero variable_state rows)
    assert _of(objs, IRVariableState) == []


# ---------------------------------------------------------------------------
# #211: curated deldatamängd token -> variant mapping (LOVA/LVM/DORS/LMED)
# ---------------------------------------------------------------------------


def test_token_map_resolves_lova_token_to_variant() -> None:
    # LOVA shape: the variable row keys on the A_LOVA extraction token; the
    # Deldatamängder sheet names the variant 'LOVA'. The curated map routes the
    # member there -> a real state + alias in that variant, no warning.
    reg = _register(
        "lova",
        [_var("KOMMUN", deldatamangd="A_LOVA", data_type="Sträng (text)")],
        deldatamangder=(_deldat("LOVA"),),
    )
    objs, _ = _emit(reg)
    assert [
        w for w in _of(objs, IRWarning) if w.code == "sos_deldatamangd_unresolved"
    ] == []
    lova_variant = mint("sos", "lova", "LOVA")
    states = _of(objs, IRVariableState)
    assert len(states) == 1
    assert states[0].register_variant_id == lova_variant
    aliases = _of(objs, IRVariableAlias)
    assert len(aliases) == 1
    assert aliases[0].register_variant_id == lova_variant
    assert aliases[0].delivery_column_name == "KOMMUN"


def test_token_map_two_tokens_one_variant_merge() -> None:
    # A_LOVA and A_LOVA_LISA both map to the single 'LOVA' variant (the workbook
    # ships two same-named Deldatamängder rows that dedup to one minted variant).
    # Same column + same window start -> ONE reconciled state, valid_to widened
    # to the open-ended member.
    reg = _register(
        "lova",
        [
            _var("AGARKAT", deldatamangd="A_LOVA", data_from=1995, data_to=2022),
            _var("AGARKAT", deldatamangd="A_LOVA_LISA", data_from=1995),
        ],
        deldatamangder=(_deldat("LOVA"),),
    )
    objs, _ = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 1
    assert states[0].register_variant_id == mint("sos", "lova", "LOVA")
    assert states[0].valid_to is None, "open-ended member widens the merged state"


def test_token_map_multi_target_emits_into_each_variant() -> None:
    # LMED FDDD shape: one member whose token names BOTH variants -> a state and
    # an alias in each.
    reg = _register(
        "lmed",
        [_var("FDDD", deldatamangd="LMED VARA/LMED", data_type="Decimal")],
        deldatamangder=(_deldat("LMED"), _deldat("LMED VARA")),
    )
    objs, _ = _emit(reg)
    assert [
        w for w in _of(objs, IRWarning) if w.code == "sos_deldatamangd_unresolved"
    ] == []
    expected = {mint("sos", "lmed", "LMED"), mint("sos", "lmed", "LMED VARA")}
    states = _of(objs, IRVariableState)
    assert {s.register_variant_id for s in states} == expected
    assert len(states) == 2
    aliases = _of(objs, IRVariableAlias)
    assert {a.register_variant_id for a in aliases} == expected
    variables = _of(objs, IRVariable)
    assert len(variables) == 1, "multi-target mapping shares ONE variable"


def test_duplicate_deldat_name_contributes_no_advisory_window() -> None:
    # LOVA ships TWO 'LOVA' Deldatamängder rows (one minted variant). The rows
    # may carry different data_från/till windows and there is no curated
    # token<->row pairing, so a duplicate name is AMBIGUOUS as an
    # advisory-window source: members resolving there get NO deldat bound (the
    # variable window stands), instead of silently inheriting whichever
    # duplicate row happened to be parsed last.
    reg = _register(
        "lova",
        [_var("KOMMUN", deldatamangd="A_LOVA", data_from=1995, data_to=2022)],
        deldatamangder=(
            _deldat("LOVA", data_from=2010, data_to=2012),
            _deldat("LOVA", data_from=1995),
        ),
    )
    objs, _ = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 1
    # last-row window (1995-) would clip nothing, first-row (2010-2012) would
    # clip hard; with the ambiguity rule NEITHER applies — the variable window
    # survives untouched and no contradiction warning fires.
    assert (states[0].valid_from, states[0].valid_to) == ("1995-01-01", "2022-12-31")
    assert [
        w
        for w in _of(objs, IRWarning)
        if w.code == "sos_deldatamangd_bound_contradicts_variable"
    ] == []


def test_token_map_target_missing_from_sheet_warns() -> None:
    # Curation drift: the token IS mapped but its target name has no
    # Deldatamängder-sheet row (e.g. the workbook dropped the view). The member
    # warn-drops like any unresolved token instead of crashing or guessing.
    reg = _register(
        "dors",
        [_var("ULORSAK", deldatamangd="DORS-COV")],
        deldatamangder=(_deldat("DORS"),),  # COV_DORS_HERMES row absent
    )
    objs, _ = _emit(reg)
    warns = [w for w in _of(objs, IRWarning) if w.code == "sos_deldatamangd_unresolved"]
    assert len(warns) == 1
    assert _of(objs, IRVariableState) == []


def test_token_map_lvm_tokens_resolve() -> None:
    # LVM shape: lowercase technical tokens map to the long Swedish row names.
    ansok = "Ansökningar om tvångsvård enligt lagen om vård av missbrukare i vissa fall, LVM"
    reg = _register(
        "lvm",
        [_var("PNR", deldatamangd="lvm_ansok", data_from=1994)],
        deldatamangder=(_deldat(ansok),),
    )
    objs, _ = _emit(reg)
    assert [
        w for w in _of(objs, IRWarning) if w.code == "sos_deldatamangd_unresolved"
    ] == []
    states = _of(objs, IRVariableState)
    assert len(states) == 1
    assert states[0].register_variant_id == mint("sos", "lvm", ansok)
    assert states[0].valid_from == "1994-01-01"


# ---------------------------------------------------------------------------
# #373: styrtabell (value-set decode table) exclusion
# ---------------------------------------------------------------------------


def test_styrtabell_excluded_from_variant_and_variable_minting() -> None:
    # LOVA shape: a real deldatamängd (Individ-grained) carrying a coded
    # variable, plus a styrtabell deldatamängd (Aggregeringsnivå='Ej relevant' +
    # a 'Styrtabell …' label) whose variable rows include the coded column AND a
    # decode-only KLARTEXT column. The styrtabell deldatamängd mints NO variant,
    # its decode-only column mints NO variable/state, but the real coded variable
    # (also present under the real deldatamängd) still emits.
    reg = _register(
        "lova",
        [
            _var("AGARKAT", deldatamangd="A_LOVA", data_from=1995),
            # styrtabell rows route via DELDATAMANGD_TOKEN_MAP to the excluded
            # 'LOVA AGARKAT' deldatamängd: AGARKAT (the code) + KLARTEXT (decode).
            _var("AGARKAT", deldatamangd="A_LOVA_STYR_AGARKAT", data_from=1995),
            _var("KLARTEXT", deldatamangd="A_LOVA_STYR_AGARKAT", data_from=1995),
        ],
        deldatamangder=(
            _deldat("LOVA", aggregation_level="Individ"),
            _deldat(
                "LOVA AGARKAT",
                label="Styrtabell för ägarkategori",
                aggregation_level="Ej relevant",
            ),
        ),
    )
    objs, _ = _emit(reg)
    # No variant for the styrtabell deldatamängd.
    variant_names = {v.name for v in _of(objs, IRVariant)}
    assert variant_names == {"LOVA"}
    assert "LOVA AGARKAT" not in variant_names
    # No variable for the decode-only column; the real coded variable survives.
    variable_names = {v.name for v in _of(objs, IRVariable)}
    assert variable_names == {"AGARKAT"}
    assert "KLARTEXT" not in variable_names
    # No mismatch warning (both signals agree) and no unresolved-drop warning.
    assert [
        w
        for w in _of(objs, IRWarning)
        if w.code in {"sos_styrtabell_signal_mismatch", "sos_deldatamangd_unresolved"}
    ] == []
    # The surviving coded variable emits its state in the real variant only.
    states = _of(objs, IRVariableState)
    assert {s.register_variant_id for s in states} == {mint("sos", "lova", "LOVA")}


def test_styrtabell_signal_mismatch_warns_and_does_not_exclude() -> None:
    # Drift: only ONE styrtabell signal present (the 'Styrtabell …' label, but
    # Aggregeringsnivå is a normal grain). The adapter WARNS
    # (sos_styrtabell_signal_mismatch) and does NOT exclude the deldatamängd — it
    # still mints a variant and its variable.
    reg = _register(
        "lova",
        [_var("KLARTEXT", deldatamangd="A_LOVA_STYR_AGARKAT", data_from=1995)],
        deldatamangder=(
            _deldat(
                "LOVA AGARKAT",
                label="Styrtabell för ägarkategori",
                aggregation_level="Individ",  # the second signal is ABSENT
            ),
        ),
    )
    objs, _ = _emit(reg)
    warns = [
        w for w in _of(objs, IRWarning) if w.code == "sos_styrtabell_signal_mismatch"
    ]
    assert len(warns) == 1
    assert "LOVA AGARKAT" in (warns[0].detail or "")
    # Not excluded: the variant is minted and the variable emits its state.
    assert "LOVA AGARKAT" in {v.name for v in _of(objs, IRVariant)}
    assert "KLARTEXT" in {v.name for v in _of(objs, IRVariable)}
    assert {s.register_variant_id for s in _of(objs, IRVariableState)} == {
        mint("sos", "lova", "LOVA AGARKAT")
    }


# ---------------------------------------------------------------------------
# P2#2: deldatamängd bound contradicts variable -> keep variable window + WARN
# ---------------------------------------------------------------------------


def test_deldat_bound_contradicts_variable_keeps_variable_window() -> None:
    # EKB shape: deldatamängd data_to=1997 but the variable declares
    # data_from=2010 -> [.. 1997] INT [2010 ..] is EMPTY. The deldat window is
    # ADVISORY; the variable window is authoritative -> keep a code-less state on
    # [2010 ..] and WARN (sos_deldatamangd_bound_contradicts_variable).
    reg = _register(
        "ekb",
        [
            _var(
                "EKB_AR",
                deldatamangd="EKB",
                data_type="Heltal",
                data_from=2010,
            )
        ],
        deldatamangder=(_deldat("EKB", data_to=1997),),
    )
    objs, _ = _emit(reg)
    states = _of(objs, IRVariableState)
    assert len(states) == 1, "variable window rescues the otherwise-empty state"
    assert states[0].valid_from == "2010-01-01"
    warns = [
        w
        for w in _of(objs, IRWarning)
        if w.code == "sos_deldatamangd_bound_contradicts_variable"
    ]
    assert len(warns) == 1
    assert "EKB_AR" in (warns[0].detail or "")


def test_deldat_bound_contradicts_variable_code_bearing() -> None:
    # Same contradiction, but the variable carries a kodlista whose code
    # tidsperiod overlaps the variable window (>=2010). The deldat data_to=1997
    # would empty every code window; dropping it (advisory) keeps the codes on
    # the variable window and WARNS once for the member.
    rows = [
        SosKodlistaRow(tidsperiod="2010-2020", kod="1", beskrivning="A"),
        SosKodlistaRow(tidsperiod="2011-2019", kod="2", beskrivning="B"),
    ]
    reg = _register(
        "dors",
        [
            _var(
                "FODLAND",
                deldatamangd="DORS",
                data_type="Sträng (text)",
                data_from=2010,
            )
        ],
        deldatamangder=(_deldat("DORS", data_to=1997),),
        kodlistor=(_kodlista("FODLAND", rows),),
    )
    objs, _ = _emit(reg)
    states = _of(objs, IRVariableState)
    assert states, "code-bearing state survives on the variable window"
    assert all(s.valid_from >= "2010-01-01" for s in states)
    warns = [
        w
        for w in _of(objs, IRWarning)
        if w.code == "sos_deldatamangd_bound_contradicts_variable"
    ]
    # one WARN per member, regardless of how many code windows dropped the bound
    assert len(warns) == 1


def test_deldat_bound_genuinely_empty_drops_without_warn() -> None:
    # When the VARIABLE window alone is already empty (var data_from > data_to),
    # there's nothing to rescue: the state is dropped and NO contradiction WARN
    # fires (the deldat bound wasn't the cause).
    reg = _register(
        "ekb",
        [
            _var(
                "X",
                deldatamangd="EKB",
                data_type="Heltal",
                data_from=2020,
                data_to=2010,
            )
        ],
        deldatamangder=(_deldat("EKB"),),
    )
    objs, _ = _emit(reg)
    assert _of(objs, IRVariableState) == []
    assert [
        w
        for w in _of(objs, IRWarning)
        if w.code == "sos_deldatamangd_bound_contradicts_variable"
    ] == []


# ---------------------------------------------------------------------------
# 8. Unparseable kodlista
# ---------------------------------------------------------------------------


def test_unparseable_kodlista_warns_no_value_set() -> None:
    reg = _register(
        "lvm",
        [_var("behandlingshem", deldatamangd="LVM", data_type="Sträng (text)")],
        deldatamangder=(_deldat("LVM"),),
        kodlistor=(_kodlista("behandlingshem", [], raw=True),),
    )
    objs, adapter = _emit(reg)
    warns = [w for w in _of(objs, IRWarning) if w.code == "sos_kodlista_unparseable"]
    assert len(warns) == 1
    # no value_set written
    n = adapter.conn.execute("SELECT COUNT(*) FROM value_set").fetchone()[0]
    assert n == 0


# ---------------------------------------------------------------------------
# 9. Leading-zero codes + cross-provider content-share
# ---------------------------------------------------------------------------


def test_leading_zero_codes_content_share() -> None:
    conn = sqlite3.connect(":memory:")
    conn.executescript(DDL)
    seed_providers(conn)
    adapter = SOSAdapter(conn)
    # Pre-seed a value_set as if SCB had written it (code "001"/"002").
    codes = [
        IRValueCode(
            code_id=0,
            value_set_id=0,
            code="001",
            label="A",
            valid_from=None,
            valid_to=None,
        ),
        IRValueCode(
            code_id=0,
            value_set_id=0,
            code="002",
            label="B",
            valid_from=None,
            valid_to=None,
        ),
    ]
    first = adapter._ensure_value_set(codes)
    # An identical SOS code list collapses onto the same value_set_id / code_ids.
    second = adapter._ensure_value_set(
        [
            IRValueCode(
                code_id=0,
                value_set_id=0,
                code="001",
                label="A",
                valid_from=None,
                valid_to=None,
            ),
            IRValueCode(
                code_id=0,
                value_set_id=0,
                code="002",
                label="B",
                valid_from=None,
                valid_to=None,
            ),
        ]
    )
    assert first == second
    assert conn.execute("SELECT COUNT(*) FROM value_set").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM value_code").fetchone()[0] == 2
    # leading zeros preserved verbatim
    assert {r[0] for r in conn.execute("SELECT code FROM value_code")} == {"001", "002"}


# ---------------------------------------------------------------------------
# 10. Mint determinism + stable shape discriminator
# ---------------------------------------------------------------------------


def test_mint_determinism_across_rebuilds() -> None:
    reg = _register(
        "par",
        [
            _var("ATC", deldatamangd="PAR_OV", data_type="Sträng (text)"),
            _var("ATC", deldatamangd="PAR_SV", data_type="Heltal"),
        ],
        deldatamangder=(_deldat("PAR_OV"), _deldat("PAR_SV")),
    )
    ids1 = sorted(v.variable_id for v in _of(_emit(reg)[0], IRVariable))
    ids2 = sorted(v.variable_id for v in _of(_emit(reg)[0], IRVariable))
    assert ids1 == ids2, "split-sibling ids are shape-derived, not counters"


def test_all_minted_ids_in_band() -> None:
    reg = _register(
        "thr",
        [_var("X", deldatamangd="THR", data_type="Sträng (text)")],
        deldatamangder=(_deldat("THR"),),
    )
    objs, _ = _emit(reg)
    for o in objs:
        for attr in ("register_id", "register_variant_id", "variable_id", "state_id"):
            v = getattr(o, attr, None)
            if v is not None:
                assert _MINT_BIT <= v < _HIGH, f"{attr}={v} out of minted band"


def test_sos_abbrev_from_filename() -> None:
    reg = _register("lmed", [])
    assert _sos_abbrev(reg) == "lmed"


# ---------------------------------------------------------------------------
# 11. Band assertion + SOS coverage guard (validate.py)
# ---------------------------------------------------------------------------


def test_check_minted_id_bands_fails_on_unminted_sos() -> None:
    from reg_meta_build.validate import ValidationResult, _check_minted_id_bands

    conn = sqlite3.connect(":memory:")
    conn.executescript(DDL)
    seed_providers(conn)
    # A SOS register (provider_id 2) with a LOW (un-minted) id -> band FAIL.
    conn.execute(
        "INSERT INTO register (register_id, provider_id, name) VALUES (?, 2, ?)",
        (42, "BadSos"),
    )
    tables = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    result = ValidationResult()
    _check_minted_id_bands(conn, result, tables)
    assert not result.passed
    assert any("below the minted band" in f for f in result.failures)


def test_check_minted_id_bands_passes_clean() -> None:
    from reg_meta_build.validate import ValidationResult, _check_minted_id_bands

    conn = sqlite3.connect(":memory:")
    conn.executescript(DDL)
    seed_providers(conn)
    conn.execute(
        "INSERT INTO register (register_id, provider_id, name) VALUES (?, 1, ?)",
        (7, "ScbReg"),
    )
    conn.execute(
        "INSERT INTO register (register_id, provider_id, name) VALUES (?, 2, ?)",
        (mint("sos", "thr"), "SosReg"),
    )
    tables = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    result = ValidationResult()
    _check_minted_id_bands(conn, result, tables)
    assert result.passed


def _seed_sos_variable(
    conn: sqlite3.Connection, *, reg_name: str, with_state: bool
) -> int:
    """Insert one SOS register + variant + variable; optionally a state row.
    Returns the variable_id."""
    rid = mint("sos", reg_name)
    vid = mint("sos", reg_name, "X")
    variant_id = mint("sos", reg_name, "_default")
    conn.execute(
        "INSERT INTO register (register_id, provider_id, name) VALUES (?, 2, ?)",
        (rid, reg_name),
    )
    conn.execute(
        "INSERT INTO register_variant (register_variant_id, register_id, name) "
        "VALUES (?, ?, ?)",
        (variant_id, rid, "_default"),
    )
    conn.execute(
        "INSERT INTO variable (variable_id, register_id, provider_key, "
        "is_sensitive, is_identifier) VALUES (?, ?, 'X', 0, 0)",
        (vid, rid),
    )
    if with_state:
        conn.execute(
            "INSERT INTO variable_state (state_id, variable_id, "
            "register_variant_id, valid_from, valid_to, value_set_version_label) "
            "VALUES (?, ?, ?, '2000-01-01', '2010-12-31', '')",
            (mint("sos", "state", str(vid)), vid, variant_id),
        )
    return vid


def test_check_sos_stateless_variables_warns_not_fails() -> None:
    # P2#1 validate guard: a SOS variable with ZERO variable_state rows surfaces
    # as an INFO/warn line, NOT a failure — A4.3b must still ship.
    from reg_meta_build.validate import (
        ValidationResult,
        _check_sos_stateless_variables,
    )

    conn = sqlite3.connect(":memory:")
    conn.executescript(DDL)
    seed_providers(conn)
    _seed_sos_variable(conn, reg_name="lova", with_state=False)
    tables = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    result = ValidationResult()
    _check_sos_stateless_variables(conn, result, tables)
    # WARN, not FAIL: the gate must still pass.
    assert result.passed, result.format_report()
    report = result.format_report()
    assert "ZERO variable_state" in report
    assert "lova" in report.lower()


def test_check_sos_stateless_variables_clean_when_all_have_states() -> None:
    from reg_meta_build.validate import (
        ValidationResult,
        _check_sos_stateless_variables,
    )

    conn = sqlite3.connect(":memory:")
    conn.executescript(DDL)
    seed_providers(conn)
    _seed_sos_variable(conn, reg_name="thr", with_state=True)
    tables = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    result = ValidationResult()
    _check_sos_stateless_variables(conn, result, tables)
    assert result.passed
    assert "ZERO variable_state" not in result.format_report()


def test_check_sos_stateless_variables_skips_scb_only() -> None:
    # No SOS variables -> the check self-skips (SCB-only build) and never warns.
    from reg_meta_build.validate import (
        ValidationResult,
        _check_sos_stateless_variables,
    )

    conn = sqlite3.connect(":memory:")
    conn.executescript(DDL)
    seed_providers(conn)
    tables = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    result = ValidationResult()
    _check_sos_stateless_variables(conn, result, tables)
    assert result.passed
    assert "SCB-only build" in result.format_report()


# ---------------------------------------------------------------------------
# 12. --providers selection + 13. combined build additivity (synthetic)
#
# SOS is purely additive (minted ids, content-shared value_sets), so a
# --providers=scb build never picks up SOS, and a combined scb,sos build leaves
# the SCB-shaped rows byte-identical. These pin that on synthetic data. (The
# real-corpus volume sanity check lived in a real-delivery test, now dropped —
# a maintainer's actual build over the real input is what surfaces real drift.)
# ---------------------------------------------------------------------------


def _write_scb_only(tmp: Path) -> Path:
    from _csv_fixtures import write_scb_input

    inp = tmp / "input"
    inp.mkdir()
    write_scb_input(inp)
    return inp


def test_synthetic_scb_only_excludes_present_sos_dir(tmp_path: Path) -> None:
    # A providers=("scb",) build must ignore a present Socialstyrelsen dir: SOS
    # is opt-in per --providers, so excluding it yields zero SOS rows — the
    # A4.3b gate that SOS never leaks into an SCB-only build.
    from _sos_fixtures import write_sos_input

    inp = _write_scb_only(tmp_path)
    write_sos_input(inp)  # SOS workbooks present but excluded by --providers
    build_db(
        input_dir=inp,
        db_dir=tmp_path / "scb",
        skip_classifications=True,
        skip_slugs=True,
        providers=("scb",),
    )
    conn = sqlite3.connect(tmp_path / "scb" / "reg_meta.db")
    try:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM register WHERE provider_id=2"
            ).fetchone()[0]
            == 0
        )
    finally:
        conn.close()


def test_synthetic_combined_scb_subset_identical_to_scb_only(tmp_path: Path) -> None:
    # SOS is purely additive: the SCB-shaped rows of a combined scb,sos build are
    # byte-identical to an SCB-only build over the same input. Diff the
    # provider-shaped tables with SOS filtered out (value tables are
    # content-shared, so they legitimately differ by the SOS-only value_sets and
    # are excluded from the proxy). (Under skip_slugs the slug-keyed tables are
    # empty, so this is the same contract pin the deleted real-data byte-identity
    # proxy relied on.)
    from _sos_fixtures import write_sos_input
    from reg_meta_build.dbdiff import DEFAULT_IGNORE, TableIgnore, diff_db_content

    inp = _write_scb_only(tmp_path)
    write_sos_input(inp)
    build_db(
        input_dir=inp,
        db_dir=tmp_path / "comb",
        skip_classifications=True,
        skip_slugs=True,
        providers=("scb", "sos"),
    )
    build_db(
        input_dir=inp,
        db_dir=tmp_path / "scb",
        skip_classifications=True,
        skip_slugs=True,
        providers=("scb",),
    )
    comb = tmp_path / "comb" / "reg_meta.db"
    scb = tmp_path / "scb" / "reg_meta.db"

    ignore = dict(DEFAULT_IGNORE)
    for table, where in (
        ("register", "provider_id = 2"),
        (
            "register_variant",
            "register_id IN (SELECT register_id FROM register WHERE provider_id = 2)",
        ),
        (
            "variable",
            "register_id IN (SELECT register_id FROM register WHERE provider_id = 2)",
        ),
    ):
        ignore[table] = TableIgnore(skip_where=where)
    rep = diff_db_content(scb, comb, ignore=ignore)
    proxy_tables = {"register", "register_variant", "variable"}
    seen = set()
    for tr in rep.table_results:
        if tr.table in proxy_tables:
            seen.add(tr.table)
            assert tr.identical, f"{tr.table} drifted: {tr}"
    assert proxy_tables.issubset(seen), (
        f"proxy did not diff all expected tables: missing {proxy_tables - seen}"
    )


# ---------------------------------------------------------------------------
# PR2 classification feed: end-to-end resolution + SCB byte-identity WITH
# classifications ENABLED. The skip_classifications=True byte-identity test above
# never exercises the candidate feed/backfill; these run the real path.
# ---------------------------------------------------------------------------

# Seed covering the SCB fixture's "Kön" vardemangdsversion (so SCB classification
# linkage is NON-empty) plus the SOS ICD-10-SE entry the resolving SOS variable
# below points at. ICD-10-SE is provider="sos"; it is SEEDED on the SCB-only
# build too (classifications are always seeded), but the SCB-subset stays
# byte-identical because the SOS feed tags only SOS states.
_PR2_SEED_TOML = """\
[[classification]]
short_name = "TESTKON"
name = "Test classification for gender codes"
publisher = "TEST"
valid_from = 2000
valid_codes_file = "testkon.csv"
vardemangdsversion = ["Kön"]

[[classification]]
short_name = "ICD-10-SE"
name = "ICD-10-SE"
publisher = "Socialstyrelsen"
provider = "sos"
valid_codes_file = "icd-10-se.csv"
"""

# A SOS register whose DIAGNOS variable carries an icd-10 `Länk kodverk`, so the
# resolver tags it ICD-10-SE during a combined build with classifications on.
_PR2_SOS_REGISTERS = (
    _SosFixtureRegister(
        abbrev="SYN",
        title_sv="Syntetiskt register",
        description_sv="Ett syntetiskt SOS-register för testbygget.",
        deldatamangder=(_SosFixtureDeldat("SYN_A", data_from=2005, data_to=2015),),
        variables=(
            _SosFixtureVar(
                "DIAGNOS",
                deldatamangd="SYN_A",
                label="Diagnoskod",
                data_type="Sträng (text)",
                data_from=2005,
                data_to=2015,
                external_classification="https://icd.who.int/browse10/2019/en",
            ),
        ),
    ),
)


def _write_pr2_seed(tmp_path: Path) -> Path:
    # Every classification needs a valid_codes CSV under a classifications dir.
    seed = tmp_path / "classifications.toml"
    seed.write_text(_PR2_SEED_TOML, encoding="utf-8")
    cls_dir = tmp_path / "input" / "classifications"
    cls_dir.mkdir(parents=True, exist_ok=True)
    # TESTKON codes mirror the SCB fixture's observed Kön codes (1=Man, 2=Kvinna)
    # so no canonical-only rows are added.
    (cls_dir / "testkon.csv").write_text(
        "code,label\n1,Man\n2,Kvinna\n", encoding="utf-8"
    )
    (cls_dir / "icd-10-se.csv").write_text(
        "code,label\nA01,Diagnos A\nB02,Diagnos B\n", encoding="utf-8"
    )
    return seed


def test_pr2_combined_build_tags_sos_state_classification(tmp_path: Path) -> None:
    # End-to-end: a combined ("scb","sos") build with classifications ENABLED and
    # a SOS variable whose external_classification resolves → at least one SOS
    # `variable_state.classification_id` is non-null (tagged ICD-10-SE).
    from _sos_fixtures import write_sos_input

    inp = _write_scb_only(tmp_path)
    write_sos_input(inp, registers=_PR2_SOS_REGISTERS)
    seed = _write_pr2_seed(tmp_path)
    build_db(
        input_dir=inp,
        db_dir=tmp_path / "comb",
        seed_path=seed,
        skip_slugs=True,
        providers=("scb", "sos"),
    )
    conn = sqlite3.connect(tmp_path / "comb" / "reg_meta.db")
    try:
        icd_id = conn.execute(
            "SELECT id FROM classification WHERE short_name = 'ICD-10-SE'"
        ).fetchone()[0]
        # The SOS DIAGNOS state (provider_id=2) is tagged ICD-10-SE.
        tagged = conn.execute(
            "SELECT COUNT(*) FROM variable_state vs "
            "JOIN variable v USING (variable_id) "
            "JOIN register r USING (register_id) "
            "WHERE r.provider_id = 2 AND vs.classification_id = ?",
            (icd_id,),
        ).fetchone()[0]
        assert tagged >= 1, "the resolving SOS variable_state must be tagged ICD-10-SE"
    finally:
        conn.close()


def test_pr2_scb_linkage_identical_with_classifications_enabled(
    tmp_path: Path,
) -> None:
    # The REAL PR2 byte-identity gate: with classifications ENABLED (so the SOS
    # candidate feed + backfill actually run), the SCB variable→classification
    # linkage of a combined ("scb","sos") build is IDENTICAL to an SCB-only build.
    # Proves the SOS feed tags only SOS states and never touches SCB linkage.
    from _sos_fixtures import write_sos_input
    from reg_meta_build.db import dump_classification_linkage

    inp = _write_scb_only(tmp_path)
    write_sos_input(inp, registers=_PR2_SOS_REGISTERS)
    seed = _write_pr2_seed(tmp_path)

    build_db(
        input_dir=inp,
        db_dir=tmp_path / "comb",
        seed_path=seed,
        skip_slugs=True,
        providers=("scb", "sos"),
    )
    build_db(
        input_dir=inp,
        db_dir=tmp_path / "scb",
        seed_path=seed,
        skip_slugs=True,
        providers=("scb",),
    )

    def _scb_linkage(db: Path) -> list[tuple[int, int | None, int]]:
        conn = sqlite3.connect(db)
        try:
            scb_vars = {
                r[0]
                for r in conn.execute(
                    "SELECT v.variable_id FROM variable v "
                    "JOIN register r USING (register_id) WHERE r.provider_id = 1"
                )
            }
            return [t for t in dump_classification_linkage(conn) if t[0] in scb_vars]
        finally:
            conn.close()

    comb_db = tmp_path / "comb" / "reg_meta.db"
    scb_db = tmp_path / "scb" / "reg_meta.db"
    scb_linkage = _scb_linkage(scb_db)
    assert scb_linkage, "the SCB fixture must produce a non-empty SCB linkage (TESTKON)"
    assert _scb_linkage(comb_db) == scb_linkage, (
        "SOS feed must leave SCB variable→classification linkage byte-identical"
    )

    # And the combined build DID exercise the SOS feed (a SOS state is tagged).
    conn = sqlite3.connect(comb_db)
    try:
        sos_tagged = conn.execute(
            "SELECT COUNT(*) FROM variable_state vs "
            "JOIN variable v USING (variable_id) "
            "JOIN register r USING (register_id) "
            "WHERE r.provider_id = 2 AND vs.classification_id IS NOT NULL"
        ).fetchone()[0]
        assert sos_tagged >= 1, (
            "combined build must actually tag a SOS state (feed ran)"
        )
    finally:
        conn.close()


def test_unreadable_workbook_becomes_warning(tmp_path: Path) -> None:
    # A non-xlsx-shaped file in the SOS dir parses to an IRWarning, not an abort.
    sosdir = tmp_path / "Socialstyrelsen"
    sosdir.mkdir()
    (sosdir / "broken (XYZ)_webb.xlsx").write_bytes(b"not a zip")
    conn = sqlite3.connect(":memory:")
    conn.executescript(DDL)
    seed_providers(conn)
    adapter = SOSAdapter(conn)
    objs = list(adapter.emit(sosdir))
    warns = [
        o
        for o in objs
        if isinstance(o, IRWarning) and o.code == "sos_workbook_unreadable"
    ]
    assert len(warns) == 1
    assert _of(objs, IRRegister) == []


def test_alias_emitted_per_variant_column(tmp_path: Path) -> None:
    reg = _register(
        "thr",
        [_var("KON", deldatamangd="THR_A", data_type="Heltal")],
        deldatamangder=(_deldat("THR_A"),),
    )
    objs, _ = _emit(reg)
    aliases = _of(objs, IRVariableAlias)
    assert len(aliases) == 1
    assert aliases[0].delivery_column_name == "KON"


# ---------------------------------------------------------------------------
# 14. Synthetic SOS workbooks — full build coverage
#
# SOS has no real-data tests (the Socialstyrelsen workbooks are gitignored and
# real-corpus drift is left to a maintainer's actual `build-db`), so these
# synthetic .xlsx workbooks (`_sos_fixtures.write_sos_input`, the SOS analog of
# `_csv_fixtures.write_scb_input`) are the entire CI coverage for the SOS adapter
# end-to-end, the combined scb,sos build, and the SOS-only (scb_ran=False)
# lifecycle. They run `validate_built_db(corpus=False)` — the full structural
# suite minus the real-corpus volume gate (`_check_sos_sanity`, >= 13 registers,
# which `corpus=False` skips) — plus targeted SQL for the invariants validate
# doesn't cover (cross-provider edges, FTS parity).
# ---------------------------------------------------------------------------


def _write_combined_input(tmp: Path) -> Path:
    """Synthetic SCB CSVs + synthetic SOS workbooks under one input dir."""
    from _sos_fixtures import write_sos_input

    inp = _write_scb_only(tmp)
    write_sos_input(inp)
    return inp


def test_synthetic_sos_emit_in_band_and_populates(tmp_path: Path) -> None:
    from _sos_fixtures import write_sos_input

    sos_dir = write_sos_input(tmp_path)
    conn = sqlite3.connect(":memory:")
    conn.executescript(DDL)
    seed_providers(conn)
    adapter = SOSAdapter(conn)
    objs = list(adapter.emit(sos_dir))

    # Every minted grain lands in the SOS band [2^62, 2^63).
    for o in objs:
        for attr in ("register_id", "register_variant_id", "variable_id", "state_id"):
            v = getattr(o, attr, None)
            if v is not None:
                assert _MINT_BIT <= v < _HIGH, f"{attr}={v} out of minted band"

    registers = _of(objs, IRRegister)
    assert {r.name for r in registers} == {
        "Syntetiskt register",
        "Syntetiskt variantlöst register",
    }
    assert all(r.provider == "sos" for r in registers)

    # Real deldatamängd variants (SYN) + the synthesized _default (variant-less
    # SYT, which ships no Deldatamängder sheet).
    variants = _of(objs, IRVariant)
    assert {v.name for v in variants} == {"SYN_A", "SYN_B", "_default"}
    assert [v.synthesized for v in variants if v.name == "_default"] == [True]

    # DIAGNOS appears under both SYN deldatamängder -> merges to one variable;
    # KON (SYN) and LOPNR (SYT) stay distinct.
    variables = _of(objs, IRVariable)
    assert sorted(v.provider_key for v in variables) == ["DIAGNOS", "KON", "LOPNR"]

    # The Kodlista_DIAGNOS sheet populates a value set; both DIAGNOS states
    # content-share it (the KON/LOPNR states carry none).
    assert conn.execute("SELECT COUNT(*) FROM value_set").fetchone()[0] == 1
    assert {r[0] for r in conn.execute("SELECT code FROM value_code")} == {"A01", "B02"}
    states = _of(objs, IRVariableState)
    assert sum(1 for s in states if s.value_set_id is not None) == 2


def test_synthetic_combined_build_validates_edges_fts(tmp_path: Path) -> None:
    from reg_meta_build.validate import validate_built_db

    inp = _write_combined_input(tmp_path)
    build_db(
        input_dir=inp,
        db_dir=tmp_path / "comb",
        skip_classifications=True,
        skip_slugs=True,
        providers=("scb", "sos"),
    )
    db = tmp_path / "comb" / "reg_meta.db"

    # (1) Full structural validation over the synthetic combined build.
    # corpus=False skips only the real-corpus SOS volume gate (>= 13 registers);
    # every other invariant — schema shape, state-projection integrity, alias
    # coverage, minted-id band disjointness, freelist ceiling — runs against a
    # SOS-containing build in CI (where the real-data tests never could).
    res = validate_built_db(db, corpus=False)
    assert res.passed, res.format_report()

    conn = sqlite3.connect(db)
    try:
        # Both providers materialized.
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM register WHERE provider_id=1"
            ).fetchone()[0]
            > 0
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM register WHERE provider_id=2"
            ).fetchone()[0]
            == 2
        )

        # (2) No cross-provider equivalence edges (validate doesn't cover
        # cross-provider edges). Under --skip-slugs the table is empty (slug-keyed;
        # can't resolve), so this is the same contract pin the real-data combined
        # test relies on — a future regression that leaks a cross-provider edge
        # (e.g. the P3#1 leaked-loop-var merge) is caught.
        n_cross = conn.execute(
            "SELECT COUNT(*) FROM variable_same_as WHERE a_provider != b_provider"
        ).fetchone()[0]
        assert n_cross == 0, f"variable_same_as has {n_cross} cross-provider edge(s)"

        # (3) FTS row-count parity with the base tables (content-synced
        # indexes; validate doesn't cover the FTS mirrors).
        assert (
            conn.execute("SELECT COUNT(*) FROM register_fts").fetchone()[0]
            == conn.execute("SELECT COUNT(*) FROM register").fetchone()[0]
        )
        assert (
            conn.execute("SELECT COUNT(*) FROM variable_fts").fetchone()[0]
            == conn.execute("SELECT COUNT(*) FROM variable").fetchone()[0]
        )
    finally:
        conn.close()


def test_corpus_gate_off_under_synthetic_on_under_corpus_true(tmp_path: Path) -> None:
    # Pins the `corpus` contract: the real-corpus SOS volume gate (>= 13
    # registers / >= 1,400 variables) is SKIPPED under corpus=False so synthetic
    # CI passes, and FIRES under corpus=True so a real build catches a short
    # delivery. The synthetic combined build has 2 SOS registers / 3 variables,
    # well under the gate.
    from reg_meta_build.validate import validate_built_db

    inp = _write_combined_input(tmp_path)
    build_db(
        input_dir=inp,
        db_dir=tmp_path / "comb",
        skip_classifications=True,
        skip_slugs=True,
        providers=("scb", "sos"),
    )
    db = tmp_path / "comb" / "reg_meta.db"

    assert validate_built_db(db, corpus=False).passed, "volume gate must be off"
    corpus_res = validate_built_db(db, corpus=True)
    assert not corpus_res.passed, "volume gate must fire on a sub-13-register build"
    assert any("SOS registers" in f for f in corpus_res.failures), corpus_res.failures


def test_synthetic_sos_only_build_drops_classification_candidate(
    tmp_path: Path,
) -> None:
    # SOS-only build: scb_ran=False, so the SCB feed of classification_candidate
    # is skipped — but materialize() still runs the UNCONDITIONAL
    # `DROP TABLE classification_candidate` against the empty-but-present BASE-DDL
    # table (and _backfill_state_classifications reads it as a no-op). The A4.4e
    # review verified this safe by simulation; this pins it in CI.
    from _sos_fixtures import write_sos_input
    from reg_meta_build.validate import validate_built_db

    inp = tmp_path / "input"
    inp.mkdir()
    write_sos_input(inp)
    build_db(
        input_dir=inp,
        db_dir=tmp_path / "sos",
        skip_classifications=True,
        skip_slugs=True,
        providers=("sos",),
    )
    db = tmp_path / "sos" / "reg_meta.db"

    # Structural validation holds on a provider=sos-only build too (scb_ran=False).
    res = validate_built_db(db, corpus=False)
    assert res.passed, res.format_report()

    conn = sqlite3.connect(db)
    try:
        # SOS-only: no SCB rows, SOS registers present.
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM register WHERE provider_id=1"
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM register WHERE provider_id=2"
            ).fetchone()[0]
            == 2
        )
        # The build-scratch tables were dropped before ship despite scb_ran=False.
        tables = {
            r[0]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert "classification_candidate" not in tables
        assert "variable_instance" not in tables
        # The SOS variables still landed in the shipped catalog.
        assert conn.execute("SELECT COUNT(*) FROM variable").fetchone()[0] == 3
    finally:
        conn.close()


def test_synthetic_combined_build_with_slugs_split_stays_sos_internal(
    tmp_path: Path,
) -> None:
    # The skip-slugs combined test above pins the edge-count CONTRACT but can't
    # exercise the slug-keyed derivation (the fold is empty without slugs). This
    # runs the build WITH slugs so the PAR ATC split materializes its concept-
    # group EDGE fold (the sibling pairs feed the fold directly; they never land
    # in a shipped table), then asserts SOS's split siblings stay SOS-internal —
    # the LIVE guard for the P3#1 leaked-loop-var regression that crossed SOS
    # edges onto SCB. populate_slugs(strict=True) demands a slug for every
    # register/variant, so we curate them from a throwaway no-slug "probe" build
    # (ids are deterministic, so the probe and the real build share them).
    from _sos_fixtures import (
        DEFAULT_REGISTERS,
        PAR_SPLIT_REGISTER,
        write_slug_dir_from_db,
        write_sos_input,
    )
    from reg_meta_build.validate import validate_built_db

    inp = _write_scb_only(tmp_path)
    # PAR triggers the ("par", "ATC") split -> one sibling pair between its
    # two sibling variables.
    write_sos_input(inp, registers=DEFAULT_REGISTERS + (PAR_SPLIT_REGISTER,))

    # Probe build (no slugs) -> harvest deterministic ids -> curate slug TOMLs.
    build_db(
        input_dir=inp,
        db_dir=tmp_path / "probe",
        skip_classifications=True,
        skip_slugs=True,
        providers=("scb", "sos"),
    )
    probe = sqlite3.connect(tmp_path / "probe" / "reg_meta.db")
    slug_dir = write_slug_dir_from_db(probe, tmp_path / "slugs")
    probe.close()

    # Real build WITH slugs.
    build_db(
        input_dir=inp,
        db_dir=tmp_path / "comb",
        skip_classifications=True,
        skip_slugs=False,
        slug_dir=slug_dir,
        providers=("scb", "sos"),
    )
    conn = sqlite3.connect(tmp_path / "comb" / "reg_meta.db")
    try:
        # SOS variable slugs all populate (provider-blind auto-derivation,
        # including the two ATC split siblings disambiguated to atc / atc-2).
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM variable v JOIN register r USING (register_id) "
                "WHERE r.provider_id=2 AND v.slug IS NULL"
            ).fetchone()[0]
            == 0
        )

        # The PAR ATC split's siblings feed the concept-group EDGE fold directly
        # (never a shipped table). The split's regression guard is the edge group:
        # the PAR
        # ATC siblings folded into ONE `source='edge'` group, and EVERY variable
        # in any edge group is SOS-internal — none leaked to SCB (the P3#1
        # leaked-loop-var regression that crossed SOS edges onto SCB).
        n_edge_groups = conn.execute(
            "SELECT COUNT(*) FROM concept_group WHERE source = 'edge'"
        ).fetchone()[0]
        assert n_edge_groups == 1, (
            f"expected the PAR ATC split to fold into 1 edge group, got {n_edge_groups}"
        )
        non_sos_edge_members = conn.execute(
            "SELECT COUNT(*) FROM concept_group_variable m "
            "JOIN concept_group g ON g.group_id = m.group_id "
            "JOIN variable v ON v.variable_id = m.variable_id "
            "JOIN register r ON r.register_id = v.register_id "
            "WHERE g.source = 'edge' AND r.provider_id != 2"
        ).fetchone()[0]
        assert non_sos_edge_members == 0, "an edge group member leaked off SOS"

        # same_as is curated-only (none here), and never crosses providers.
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM variable_same_as WHERE a_provider != b_provider"
            ).fetchone()[0]
            == 0
        )

        # FTS parity still holds with slugs populated (validate doesn't cover
        # the FTS mirrors).
        assert (
            conn.execute("SELECT COUNT(*) FROM register_fts").fetchone()[0]
            == conn.execute("SELECT COUNT(*) FROM register").fetchone()[0]
        )
        assert (
            conn.execute("SELECT COUNT(*) FROM variable_fts").fetchone()[0]
            == conn.execute("SELECT COUNT(*) FROM variable").fetchone()[0]
        )
    finally:
        conn.close()

    # Full structural validation also holds on the WITH-slugs combined build.
    res = validate_built_db(tmp_path / "comb" / "reg_meta.db", corpus=False)
    assert res.passed, res.format_report()


# ---------------------------------------------------------------------------
# _segment_windowed_codes: sweep-line into non-overlapping period segments
# ---------------------------------------------------------------------------


def _wc(
    code: str, label: str, vf: str | None, vt: str | None
) -> tuple[str | None, str | None, IRValueCode]:
    """One windowed code for the segmenter: (valid_from, valid_to, IRValueCode)."""
    return (
        vf,
        vt,
        IRValueCode(
            code_id=0,
            value_set_id=0,
            code=code,
            label=label,
            valid_from=vf,
            valid_to=vt,
        ),
    )


def _seg_codes(
    seg: tuple[str | None, str | None, list[IRValueCode]],
) -> set[tuple[str, str]]:
    return {(c.code, c.label) for c in seg[2]}


class TestSegmentWindowedCodes:
    def test_empty(self) -> None:
        assert _segment_windowed_codes([]) == []

    def test_alkohol_shape_wide_code_over_two_sub_windows(self) -> None:
        # '0' lives the whole time; '1' means one thing 1987-1996, another 1997+.
        # Bucketing by exact window overlapped (the bug); segmentation must yield
        # TWO non-overlapping period states, each unioning the live codes.
        segs = _segment_windowed_codes(
            [
                _wc("0", "noll", "1987-01-01", None),
                _wc("1", "A", "1987-01-01", "1996-12-31"),
                _wc("1", "B", "1997-01-01", None),
            ]
        )
        assert [(s[0], s[1]) for s in segs] == [
            ("1987-01-01", "1996-12-31"),
            ("1997-01-01", None),
        ]
        assert _seg_codes(segs[0]) == {("0", "noll"), ("1", "A")}
        assert _seg_codes(segs[1]) == {("0", "noll"), ("1", "B")}

    def test_main_list_plus_sub_window_addition(self) -> None:
        # A stable main code over the whole span + a code added for a later window
        # → two non-overlapping segments; the second is the superset.
        segs = _segment_windowed_codes(
            [
                _wc("A", "a", "1961-01-01", None),
                _wc("X", "x", "1979-01-01", None),
            ]
        )
        assert [(s[0], s[1]) for s in segs] == [
            ("1961-01-01", "1978-12-31"),
            ("1979-01-01", None),
        ]
        assert _seg_codes(segs[0]) == {("A", "a")}
        assert _seg_codes(segs[1]) == {("A", "a"), ("X", "x")}

    def test_rle_merges_contiguous_identical_unions(self) -> None:
        # The same (code, label) delivered across two abutting windows must
        # collapse to ONE segment — no spurious fragmentation.
        segs = _segment_windowed_codes(
            [
                _wc("A", "a", "2000-01-01", "2002-12-31"),
                _wc("A", "a", "2003-01-01", "2005-12-31"),
            ]
        )
        assert len(segs) == 1
        assert (segs[0][0], segs[0][1]) == ("2000-01-01", "2005-12-31")

    def test_dedups_identical_codes_in_one_segment(self) -> None:
        # Two identical (code, label) rows with overlapping (here identical)
        # windows must collapse to ONE live code in the segment — else the
        # value_set member_hash (hashed from the code list) desyncs from the
        # stored value_set_member set (PK-collapsed) and content-share breaks.
        segs = _segment_windowed_codes(
            [
                _wc("1", "Ett", "2000-01-01", "2005-12-31"),
                _wc("1", "Ett", "2000-01-01", "2005-12-31"),
            ]
        )
        assert len(segs) == 1
        assert [(c.code, c.label) for c in segs[0][2]] == [("1", "Ett")]

    def test_gap_is_not_merged_across_uncovered_stretch(self) -> None:
        # Identical union on both sides of an UNCOVERED stretch must stay two
        # segments (the catalog has no coding for the gap).
        segs = _segment_windowed_codes(
            [
                _wc("A", "a", "2000-01-01", "2001-12-31"),
                _wc("A", "a", "2005-01-01", "2006-12-31"),
            ]
        )
        assert [(s[0], s[1]) for s in segs] == [
            ("2000-01-01", "2001-12-31"),
            ("2005-01-01", "2006-12-31"),
        ]

    def test_no_overlap_invariant_holds_for_arbitrary_windows(self) -> None:
        # Property: emitted segments never overlap (closed intervals).
        segs = _segment_windowed_codes(
            [
                _wc("a", "a", None, "1990-12-31"),
                _wc("b", "b", "1985-01-01", "1995-12-31"),
                _wc("c", "c", "1992-01-01", None),
            ]
        )
        bounds = [(s[0] or "0000-01-01", s[1] or "9999-12-31") for s in segs]
        for (_, hi), (lo_next, _) in pairwise(bounds):
            assert hi < lo_next  # strictly before the next segment's start

    def test_daldkl5_shape_three_segments(self) -> None:
        # The motivating real shape (DALDKL5): a stable main code-list over [1961-]
        # plus a sub-window code that changes across THREE eras → three
        # non-overlapping segments, each = the main list ∪ that era's code. Locks
        # in the multi-cut + per-segment union the function is built for.
        segs = _segment_windowed_codes(
            [
                _wc("M1", "main1", "1961-01-01", None),
                _wc("M2", "main2", "1961-01-01", None),
                _wc("A", "era-a", "1961-01-01", "1978-12-31"),
                _wc("B", "era-b", "1979-01-01", "1986-12-31"),
                _wc("C", "era-c", "1987-01-01", None),
            ]
        )
        assert [(s[0], s[1]) for s in segs] == [
            ("1961-01-01", "1978-12-31"),
            ("1979-01-01", "1986-12-31"),
            ("1987-01-01", None),
        ]
        assert _seg_codes(segs[0]) == {("M1", "main1"), ("M2", "main2"), ("A", "era-a")}
        assert _seg_codes(segs[1]) == {("M1", "main1"), ("M2", "main2"), ("B", "era-b")}
        assert _seg_codes(segs[2]) == {("M1", "main1"), ("M2", "main2"), ("C", "era-c")}
