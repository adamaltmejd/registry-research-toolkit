"""Shape tests for the Pydantic models.

These verify the contract that consumers depend on: frozenness,
hashability, list→tuple coercion for sequence fields, defaults for
optional fields, and that ``model_json_schema()`` exposes the nested
models (the SPA's TypeScript codegen source). Structural rule
enforcement (FQID well-formedness, panel ordering, etc.) belongs to
the structural validator — these tests deliberately stay shape-only.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from reg_schema import (
    Binding,
    LiteralPeriod,
    Panel,
    PanelMember,
    PeriodRange,
    ProjectData,
    Source,
    StudyWindow,
    validate_structural,
)


def _binding(variable: str = "scb/lisa/kon") -> Binding:
    return Binding(variable=variable, type="categorical")


def _source(name: str = "lisa_2018") -> Source:
    return Source(
        name=name,
        register_variant="scb/lisa/individer-15plus",
        period=2018,
        bindings=(_binding(),),
    )


def _project(sources: tuple[Source, ...] | None = None) -> ProjectData:
    return ProjectData(
        schema_version="2.0.0",
        steward="global",
        reg_meta_version="reg_meta/v1.0.0",
        name="demo",
        sources=sources if sources is not None else (_source(),),
    )


# Binding ----------------------------------------------------------------


def test_binding_defaults_are_none() -> None:
    binding = _binding()
    assert binding.display_name is None
    assert binding.id_subtype is None
    assert binding.numeric_subtype is None
    assert binding.date_format is None
    assert binding.datetime_format is None
    assert binding.value_set is None


def test_binding_is_frozen_and_hashable() -> None:
    binding = _binding()
    with pytest.raises(ValidationError):
        binding.display_name = "Kon"  # type: ignore[misc]
    assert {binding, _binding()} == {binding}


def test_binding_variable_stored_verbatim_model_does_not_validate() -> None:
    # The Pydantic model stores the raw FQID string and does NOT validate its
    # grammar — well-formedness is the structural validator's job. So
    # even a malformed value (here the retired @version suffix, now an
    # invalid_fqid) is accepted at the model layer and stored verbatim.
    binding = Binding(variable="scb/lisa/naringsgren@sni2007", type="categorical")
    assert binding.variable == "scb/lisa/naringsgren@sni2007"


def test_binding_rejects_unknown_field() -> None:
    # _Model sets extra="forbid": a typo fails loudly instead of being
    # silently dropped.
    with pytest.raises(ValidationError):
        Binding(variable="scb/lisa/kon", type="categorical", bogus=1)  # type: ignore[call-arg]


# Source ----------------------------------------------------------------


def test_source_coerces_bindings_list_to_tuple() -> None:
    bindings_list = [_binding(), _binding(variable="scb/lisa/alder")]
    src = Source(
        name="lisa_2018",
        register_variant="scb/lisa/individer-15plus",
        period=2018,
        bindings=bindings_list,  # type: ignore[arg-type]
    )
    assert isinstance(src.bindings, tuple)
    # Coercion takes a snapshot — mutating the input list must not affect it.
    bindings_list.clear()
    assert len(src.bindings) == 2


def test_source_accepts_period_forms() -> None:
    # int, period-token string, snapshot sentinel, and range object all
    # construct (the Period union).
    for period in (2018, "2018-01", "_default"):
        src = Source(
            name="s",
            register_variant="scb/lisa/individer-15plus",
            period=period,
            bindings=(_binding(),),
        )
        assert src.period == period
    ranged = Source(
        name="s",
        register_variant="scb/par/_default",
        period={"from": "2018-01-01", "to": "2020-06-30"},  # type: ignore[arg-type]
        bindings=(_binding(variable="scb/par/lopnr"),),
    )
    assert ranged.period.from_ == "2018-01-01"
    assert ranged.period.to == "2020-06-30"


def test_period_range_round_trips_through_validator() -> None:
    # serialize_by_alias=True: model_dump emits "from"/"to" (not the
    # Python-safe "from_"), so a dumped spec re-validates clean
    # (_is_period_range_obj requires exactly {"from","to"}). Guards the
    # webapp/SPA serialization path against the alias footgun.
    src = Source(
        name="par",
        register_variant="scb/par/_default",
        period={"from": "2018-01-01", "to": "2020-06-30"},  # type: ignore[arg-type]
        bindings=(_binding(variable="scb/par/lopnr"),),
    )
    assert src.model_dump(mode="json")["period"] == {
        "from": "2018-01-01",
        "to": "2020-06-30",
    }
    result = validate_structural(_project(sources=(src,)).model_dump(mode="json"))
    assert "invalid_period" not in {i.code for i in result.issues}


def test_period_list_form_coerces_and_round_trips() -> None:
    # #307: the interrupted-series LIST form coerces to a tuple of segments
    # (list → tuple like every other tuple field; range members become
    # PeriodRange), and a json-mode dump re-validates clean through the
    # structural list rules (sorted, disjoint).
    src = Source(
        name="s",
        register_variant="scb/lisa/individer-15plus",
        period=[{"from": 2005, "to": 2010}, 2013, "HT2022"],  # type: ignore[arg-type]
        bindings=(_binding(),),
    )
    assert isinstance(src.period, tuple)
    assert src.period[0] == PeriodRange.model_validate({"from": 2005, "to": 2010})
    assert src.period[1:] == (2013, "HT2022")
    dumped = src.model_dump(mode="json")["period"]
    assert dumped == [{"from": 2005, "to": 2010}, 2013, "HT2022"]
    result = validate_structural(_project(sources=(src,)).model_dump(mode="json"))
    assert "invalid_period" not in {i.code for i in result.issues}


def test_source_is_frozen() -> None:
    src = _source()
    with pytest.raises(ValidationError):
        src.name = "other"  # type: ignore[misc]


# Panel / PanelMember --------------------------------------------------


def test_panel_member_coerces_composite_keys_to_tuple() -> None:
    member = PanelMember(
        source="rams_2018",
        entity_key=["LopNr_PeOrgNr", "LopNr_CFAR"],  # type: ignore[arg-type]
        time_key=["AR", "KVARTAL"],  # type: ignore[arg-type]
    )
    assert member.entity_key == ("LopNr_PeOrgNr", "LopNr_CFAR")
    assert member.time_key == ("AR", "KVARTAL")


def test_panel_member_scalar_keys_pass_through() -> None:
    # Scalars (str column ref, int literal period, LiteralPeriod) must
    # not be wrapped in a tuple — the union types accept the bare form.
    int_member = PanelMember(source="lisa_2018", entity_key="LopNr", time_key=2018)
    assert int_member.entity_key == "LopNr"
    assert int_member.time_key == 2018

    lp_member = PanelMember(
        source="data_201801", time_key=LiteralPeriod(period="2018-01")
    )
    assert lp_member.time_key == LiteralPeriod(period="2018-01")


def test_panel_coerces_members_and_keys() -> None:
    panel = Panel(
        panel_id="workplace",
        members=[  # type: ignore[arg-type]
            PanelMember(source="rams_2018"),
            PanelMember(source="rams_2019"),
        ],
        entity_key=["LopNr_PeOrgNr", "LopNr_CFAR"],  # type: ignore[arg-type]
        time_key="AR",
    )
    assert isinstance(panel.members, tuple)
    assert panel.entity_key == ("LopNr_PeOrgNr", "LopNr_CFAR")
    assert panel.time_key == "AR"


def test_panel_optional_fields_default_none() -> None:
    panel = Panel(panel_id="p", members=(PanelMember(source="s"),))
    assert panel.entity_key is None
    assert panel.time_key is None
    assert panel.comment is None


def test_literal_period_is_frozen_and_equal_by_value() -> None:
    a = LiteralPeriod(period="2018-01")
    b = LiteralPeriod(period="2018-01")
    assert a == b
    assert hash(a) == hash(b)
    with pytest.raises(ValidationError):
        a.period = "2019-01"  # type: ignore[misc]


# StudyWindow ----------------------------------------------------------


def test_study_window_year_pair() -> None:
    win = StudyWindow.model_validate({"from": 2000, "to": 2020})
    assert win.from_ == 2000
    assert win.to == 2020


def test_study_window_same_year_is_valid() -> None:
    # from == to is a degenerate-but-legal single-year window.
    win = StudyWindow.model_validate({"from": 2018, "to": 2018})
    assert win.from_ == win.to == 2018


def test_study_window_to_before_from_is_rejected() -> None:
    with pytest.raises(ValidationError):
        StudyWindow.model_validate({"from": 2020, "to": 2000})


def test_study_window_round_trips_through_alias() -> None:
    # serialize_by_alias=True: model_dump emits "from"/"to" (not "from_"),
    # so a dumped window re-validates clean — the same alias-footgun guard
    # PeriodRange has.
    win = StudyWindow.model_validate({"from": 2005, "to": 2010})
    dumped = win.model_dump(mode="json")
    assert dumped == {"from": 2005, "to": 2010}
    assert StudyWindow.model_validate(dumped) == win


def test_study_window_is_frozen_and_hashable() -> None:
    win = StudyWindow.model_validate({"from": 2000, "to": 2020})
    with pytest.raises(ValidationError):
        win.to = 2021  # type: ignore[misc]
    assert {win, StudyWindow.model_validate({"from": 2000, "to": 2020})} == {win}


def test_study_window_rejects_unknown_field() -> None:
    with pytest.raises(ValidationError):
        StudyWindow.model_validate({"from": 2000, "to": 2020, "bogus": 1})


# ProjectData -----------------------------------------------------------


def test_project_data_required_fields() -> None:
    pd = _project()
    assert pd.schema_version == "2.0.0"
    assert pd.steward == "global"
    assert pd.reg_meta_version == "reg_meta/v1.0.0"
    assert pd.name == "demo"
    assert len(pd.sources) == 1


def test_project_data_defaults() -> None:
    pd = _project()
    assert pd.panels == ()
    assert pd.window is None  # absent window = full history (backward compatible)


def test_project_data_carries_optional_window() -> None:
    # The window is an additive optional field: a spec WITH one constructs,
    # dumps the alias keys, and re-validates clean through the structural gate.
    pd = ProjectData(
        schema_version="2.0.0",
        steward="global",
        reg_meta_version="reg_meta/v1.0.0",
        name="demo",
        sources=(_source(),),
        window=StudyWindow.model_validate({"from": 2000, "to": 2020}),
    )
    assert pd.window == StudyWindow.model_validate({"from": 2000, "to": 2020})
    # exclude_none mirrors wire serialization: an absent optional field (e.g.
    # window) must not dump as null (the structural validator rejects a null
    # window).
    dumped = pd.model_dump(mode="json", exclude_none=True)
    assert dumped["window"] == {"from": 2000, "to": 2020}
    result = validate_structural(dumped)
    assert result.ok, result.issues


def test_project_data_coerces_sources_and_panels_list_to_tuple() -> None:
    sources = [_source("a"), _source("b")]
    panels = [Panel(panel_id="p", members=(PanelMember(source="a"),))]
    pd = ProjectData(
        schema_version="2.0.0",
        steward="global",
        reg_meta_version="reg_meta/v1.0.0",
        name="demo",
        sources=sources,  # type: ignore[arg-type]
        panels=panels,  # type: ignore[arg-type]
    )
    assert isinstance(pd.sources, tuple)
    assert isinstance(pd.panels, tuple)
    assert len(pd.sources) == 2
    assert len(pd.panels) == 1


def test_project_data_is_frozen() -> None:
    pd = _project()
    with pytest.raises(ValidationError):
        pd.name = "other"  # type: ignore[misc]


def test_project_data_ignores_extra_steward_blocks() -> None:
    # extra="ignore" (overriding _Model's forbid): unmodeled steward
    # blocks ride through on the dict side and are handled by the owning
    # package (see DESIGN.md → Not in scope (intentionally)); the model
    # neither stores them nor errors. reg_monabundle is no longer a typed
    # field — it is a plain namespaced block exactly like swecov.
    pd = ProjectData(
        schema_version="2.0.0",
        steward="global",
        reg_meta_version="reg_meta/v1.0.0",
        name="demo",
        sources=(_source(),),
        swecov={"filters": {}},  # type: ignore[call-arg]
        reg_monabundle={"binding_options": {}},  # type: ignore[call-arg]
    )
    assert not hasattr(pd, "swecov")
    assert not hasattr(pd, "reg_monabundle")


# JSON schema (SPA TypeScript codegen source) ---------------------------


def test_model_json_schema_exposes_nested_models() -> None:
    # model_json_schema() is the SPA's TS-codegen source; it must
    # expose the nested models as $defs so the generated types are not
    # inlined into one opaque blob.
    schema = ProjectData.model_json_schema()
    assert isinstance(schema, dict)
    defs = schema.get("$defs", {})
    assert {"Source", "Binding", "Panel"} <= set(defs)
