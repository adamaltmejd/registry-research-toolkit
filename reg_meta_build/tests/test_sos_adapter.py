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
from pathlib import Path

import pytest
from reg_meta_build.db import DDL, build_db, seed_providers
from reg_meta_build.id import mint
from reg_meta_build.ir import (
    IRRegister,
    IRRelatedToEdge,
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
    _intersect_window,
    _iso_bound,
    _parse_tidsperiod,
    _sos_abbrev,
)

_MINT_BIT = 1 << 62
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
) -> SosVariable:
    return SosVariable(
        deldatamangd=deldatamangd,
        name=name,
        label=label,
        description=description,
        object_type=None,
        value_set_text=None,
        external_classification=None,
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
    name: str, *, data_from: int | None = None, data_to: int | None = None
) -> SosDeldatamangd:
    return SosDeldatamangd(
        name=name,
        label=None,
        description=None,
        data_from=data_from,
        data_to=data_to,
        update_frequency=None,
        aggregation_level=None,
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
    edges = _of(objs, IRRelatedToEdge)
    # IRRelatedToEdge is not emitted in-stream by SOS (the adapter pushes to
    # related_edges); the materializer writes them. So assert via the adapter.
    assert edges == []


def test_known_split_bu_records_related_edge() -> None:
    reg = _register(
        "bu",
        [
            _var("FOD_DATUMN", deldatamangd="bu_insats", data_type="Heltal"),
            _var("FOD_DATUMN", deldatamangd="bu_plac", data_type="Datum"),
        ],
    )
    _, adapter = _emit(reg)
    assert len(adapter.related_edges) == 1
    a, b, kind = adapter.related_edges[0]
    assert kind == "same_definition_different_column"
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
    assert len(adapter.related_edges) == 1


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
    assert adapter.related_edges == []
    warns = [
        w
        for w in _of(objs, IRWarning)
        if w.code == "sos_unanticipated_same_name_conflict"
    ]
    assert len(warns) == 1


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
# P2#1: unresolved variable-sheet deldatamängd -> WARN + drop (not silent)
# ---------------------------------------------------------------------------


def test_unresolved_deldatamangd_warns_and_drops_member() -> None:
    # LOVA/LVM shape: the variable names a deldatamängd (A_LOVA) that has NO row
    # in the Deldatamängder sheet. A4.3b WARNS (sos_deldatamangd_unresolved) and
    # drops the member — it does NOT silently `continue`, and does NOT invent a
    # variant mapping (A4.4 curation).
    reg = _register(
        "lova",
        [_var("KOMMUN", deldatamangd="A_LOVA", data_type="Sträng (text)")],
        # the only sheet deldatamängd is a DIFFERENT name -> A_LOVA unresolved
        deldatamangder=(_deldat("LOVA"),),
    )
    objs, _ = _emit(reg)
    warns = [w for w in _of(objs, IRWarning) if w.code == "sos_deldatamangd_unresolved"]
    assert len(warns) == 1, "unresolved deldatamängd must WARN, not drop silently"
    assert "A_LOVA" in (warns[0].detail or "")
    assert "A4.4" in (warns[0].detail or "")
    # the member is dropped -> no state for it (the var still gets an IRVariable
    # row, but zero variable_state rows)
    assert _of(objs, IRVariableState) == []


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
# 12. --providers selection + 13. combined integration / byte-identity proxy
# ---------------------------------------------------------------------------

REAL_SOS = Path(__file__).resolve().parents[1] / "input_data" / "Socialstyrelsen"
requires_real_sos = pytest.mark.skipif(
    not REAL_SOS.is_dir(), reason="real SOS workbooks not present (gitignored)"
)


def _write_scb_only(tmp: Path) -> Path:
    from _csv_fixtures import write_scb_input

    inp = tmp / "input"
    inp.mkdir()
    write_scb_input(inp)
    return inp


@requires_real_sos
def test_providers_scb_only_emits_no_sos_rows(tmp_path: Path) -> None:
    import shutil

    inp = _write_scb_only(tmp_path)
    sosdir = inp / "Socialstyrelsen"
    sosdir.mkdir()
    for f in REAL_SOS.glob("*.xlsx"):
        shutil.copy(f, sosdir / f.name)
    build_db(
        input_dir=inp,
        db_dir=tmp_path / "scb",
        skip_classifications=True,
        skip_slugs=True,
        providers=("scb",),
    )
    conn = sqlite3.connect(tmp_path / "scb" / "reg_meta.db")
    assert (
        conn.execute("SELECT COUNT(*) FROM register WHERE provider_id=2").fetchone()[0]
        == 0
    )


@requires_real_sos
def test_combined_build_validates_and_scb_subset_identical(tmp_path: Path) -> None:
    import shutil

    from reg_meta_build.dbdiff import DEFAULT_IGNORE, TableIgnore, diff_db_content
    from reg_meta_build.validate import validate_built_db

    inp = _write_scb_only(tmp_path)
    sosdir = inp / "Socialstyrelsen"
    sosdir.mkdir()
    for f in REAL_SOS.glob("*.xlsx"):
        shutil.copy(f, sosdir / f.name)

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

    # Combined build passes validation (incl. the 3 new A4.3b checks).
    res = validate_built_db(comb)
    assert res.passed, res.format_report()

    # SCB subset of the combined build == SCB-only build (byte-identity proxy):
    # filter both DBs to provider_id != 2 on the provider-shaped tables. Same
    # input dir, so import_manifest matches; SOS rows are the only delta.
    #
    # P3#1: the proxy also diffs `variable_related_to` (filtered to its SCB
    # endpoints) — the P1 the orchestrator fixed (leaked-loop-var merge) leaked
    # SOS related-to edges, which a register/variant/variable-only diff would
    # MISS. `variable_related_to` rows are slug-keyed (a_provider/b_provider), so
    # filter SOS-touching rows instead of by register_id.
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
        ("variable_related_to", "a_provider = 'sos' OR b_provider = 'sos'"),
    ):
        ignore[table] = TableIgnore(skip_where=where)
    # The core graph (register/variant/variable) matches once SOS rows are
    # filtered; the value tables are content-shared (SOS collapses onto SCB), so
    # an SCB-only build and the combined build differ there by the SOS-only
    # value_sets — compare only the provider-shaped tables for the proxy.
    rep = diff_db_content(scb, comb, ignore=ignore)
    # register/variant/variable AND variable_related_to rows match after
    # filtering SOS out of the combined build.
    proxy_tables = {"register", "register_variant", "variable", "variable_related_to"}
    seen = set()
    for tr in rep.table_results:
        if tr.table in proxy_tables:
            seen.add(tr.table)
            assert tr.identical, f"{tr.table} drifted: {tr}"
    assert proxy_tables.issubset(seen), (
        f"proxy did not diff all expected tables: missing {proxy_tables - seen}"
    )

    # Explicit, skip_slugs-independent guards: the SCB variable_related_to row
    # count and the folded SCB variable slug multiset must be byte-identical
    # between the SCB-only and combined builds. (Under skip_slugs both are empty,
    # but these assertions pin the contract so a future regression — incl. one
    # that runs WITH slugs — is caught when SOS pollutes SCB's edges/slugs.)
    conn_scb = sqlite3.connect(scb)
    conn_comb = sqlite3.connect(comb)
    try:
        scb_related = conn_scb.execute(
            "SELECT COUNT(*) FROM variable_related_to "
            "WHERE a_provider != 'sos' AND b_provider != 'sos'"
        ).fetchone()[0]
        comb_related = conn_comb.execute(
            "SELECT COUNT(*) FROM variable_related_to "
            "WHERE a_provider != 'sos' AND b_provider != 'sos'"
        ).fetchone()[0]
        assert scb_related == comb_related, (
            f"SCB variable_related_to count drifted: {scb_related} -> {comb_related}"
        )

        slug_sql = (
            "SELECT v.slug FROM variable v JOIN register r USING (register_id) "
            "WHERE r.provider_id != 2 ORDER BY v.slug"
        )
        scb_slugs = [r[0] for r in conn_scb.execute(slug_sql)]
        comb_slugs = [r[0] for r in conn_comb.execute(slug_sql)]
        assert scb_slugs == comb_slugs, "SCB folded variable slugs drifted"
    finally:
        conn_scb.close()
        conn_comb.close()


@requires_real_sos
def test_combined_sos_volume_sanity(tmp_path: Path) -> None:
    import shutil

    inp = _write_scb_only(tmp_path)
    sosdir = inp / "Socialstyrelsen"
    sosdir.mkdir()
    for f in REAL_SOS.glob("*.xlsx"):
        shutil.copy(f, sosdir / f.name)
    build_db(
        input_dir=inp,
        db_dir=tmp_path / "comb",
        skip_classifications=True,
        skip_slugs=True,
        providers=("scb", "sos"),
    )
    conn = sqlite3.connect(tmp_path / "comb" / "reg_meta.db")
    n_reg = conn.execute(
        "SELECT COUNT(*) FROM register WHERE provider_id=2"
    ).fetchone()[0]
    n_var = conn.execute(
        "SELECT COUNT(*) FROM variable v JOIN register r USING(register_id) "
        "WHERE r.provider_id=2"
    ).fetchone()[0]
    assert n_reg >= 13
    assert n_var >= 1400


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
