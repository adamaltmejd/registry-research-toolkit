"""Shape tests for §6.1-§6.4 dataclasses.

These verify the contract that consumers depend on: frozenness,
hashability, list→tuple coercion for sequence fields, defaults for
optional fields. Structural rule enforcement (FQID well-formedness,
panel ordering, etc.) belongs to the §6.8.1 validator that lands in a
follow-up phase — these tests deliberately stay shape-only.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from reg_schema import (
    Column,
    LiteralPeriod,
    Panel,
    PanelMember,
    ProjectData,
    Source,
)


def _column(name: str = "scb/lisa/individer-15plus/2018/kon") -> Column:
    return Column(name=name, type="categorical")


def _source(name: str = "lisa_2018") -> Source:
    return Source(
        name=name,
        register_version="scb/lisa/individer-15plus/2018",
        columns=(_column(),),
    )


def _project(sources: tuple[Source, ...] | None = None) -> ProjectData:
    return ProjectData(
        schema_version="1.0.0",
        steward="global",
        reg_meta_version="reg_meta/v0.11.1",
        name="demo",
        sources=sources if sources is not None else (_source(),),
    )


# Column ----------------------------------------------------------------


def test_column_defaults_are_none() -> None:
    col = _column()
    assert col.display_name is None
    assert col.id_subtype is None
    assert col.numeric_subtype is None
    assert col.date_format is None
    assert col.datetime_format is None
    assert col.value_set is None


def test_column_is_frozen_and_hashable() -> None:
    col = _column()
    with pytest.raises(FrozenInstanceError):
        col.display_name = "Kon"  # type: ignore[misc]
    assert {col, _column()} == {col}


# Source ----------------------------------------------------------------


def test_source_coerces_columns_list_to_tuple() -> None:
    cols_list = [_column(), _column(name="scb/lisa/individer-15plus/2018/alder")]
    src = Source(
        name="lisa_2018",
        register_version="scb/lisa/individer-15plus/2018",
        columns=cols_list,
    )  # type: ignore[arg-type]
    assert isinstance(src.columns, tuple)
    # Coercion takes a snapshot — mutating the input list must not affect it.
    cols_list.clear()
    assert len(src.columns) == 2


def test_source_is_frozen() -> None:
    src = _source()
    with pytest.raises(FrozenInstanceError):
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
    with pytest.raises(FrozenInstanceError):
        a.period = "2019-01"  # type: ignore[misc]


# ProjectData -----------------------------------------------------------


def test_project_data_required_fields() -> None:
    pd = _project()
    assert pd.schema_version == "1.0.0"
    assert pd.steward == "global"
    assert pd.reg_meta_version == "reg_meta/v0.11.1"
    assert pd.name == "demo"
    assert len(pd.sources) == 1


def test_project_data_defaults() -> None:
    pd = _project()
    assert pd.panels == ()
    assert pd.reg_monabundle is None


def test_project_data_coerces_sources_and_panels_list_to_tuple() -> None:
    sources = [_source("a"), _source("b")]
    panels = [Panel(panel_id="p", members=(PanelMember(source="a"),))]
    pd = ProjectData(
        schema_version="1.0.0",
        steward="global",
        reg_meta_version="reg_meta/v0.11.1",
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
    with pytest.raises(FrozenInstanceError):
        pd.name = "other"  # type: ignore[misc]


def test_project_data_accepts_opaque_reg_monabundle_block() -> None:
    block = {
        "column_options": {"scb/lisa/individer-15plus/2018/salary": {"suppress_k": 20}}
    }
    pd = ProjectData(
        schema_version="1.0.0",
        steward="global",
        reg_meta_version="reg_meta/v0.11.1",
        name="demo",
        sources=(_source(),),
        reg_monabundle=block,
    )
    # Treated as opaque — pass-through, not deep-copied or frozen.
    assert pd.reg_monabundle is block


def test_project_data_hashable_with_dict_namespaced_block() -> None:
    # Regression: the namespaced block is a plain (unhashable) dict in
    # practice, so it must be excluded from __hash__ — otherwise
    # `hash(pd)` raises TypeError as soon as the block is populated.
    pd_no_block = _project()
    pd_with_block = ProjectData(
        schema_version="1.0.0",
        steward="global",
        reg_meta_version="reg_meta/v0.11.1",
        name="demo",
        sources=(_source(),),
        reg_monabundle={"column_options": {"a": {"suppress_k": 20}}},
    )
    assert hash(pd_with_block) == hash(pd_no_block)


def test_project_data_eq_still_compares_namespaced_block() -> None:
    # `hash=False` excludes the block from __hash__ only; __eq__ still
    # sees it, so specs differing only in the opaque block are unequal.
    base_kwargs = {
        "schema_version": "1.0.0",
        "steward": "global",
        "reg_meta_version": "reg_meta/v0.11.1",
        "name": "demo",
        "sources": (_source(),),
    }
    pd_a = ProjectData(**base_kwargs, reg_monabundle={"k": 1})
    pd_b = ProjectData(**base_kwargs, reg_monabundle={"k": 2})
    pd_c = ProjectData(**base_kwargs, reg_monabundle={"k": 1})
    assert pd_a != pd_b
    assert pd_a == pd_c
