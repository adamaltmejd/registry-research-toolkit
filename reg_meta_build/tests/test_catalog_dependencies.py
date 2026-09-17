"""Known withheld facts prune dependencies; unexplained missing refs stay fatal."""

from contextlib import closing

import pytest
from reg_meta.db import open_db
from reg_meta.errors import RegMetaError
from reg_meta_build.catalog_dependencies import (
    CatalogDependencyError,
    resolve_metadata_dependencies,
    resolve_panel_dependencies,
)
from reg_meta_build.resolved_catalog import (
    ResolvedAlias,
    ResolvedEdition,
    ResolvedRegister,
    ResolvedState,
    ResolvedVariable,
    ResolvedVariant,
    write_resolved_catalog,
)
from reg_meta_build.resolved_metadata import (
    ResolvedGroupVariable,
    ResolvedHistoricalPredecessor,
    ResolvedMetadata,
    ResolvedRepresentationRef,
    ResolvedRepresentationSuccession,
    ResolvedStateLineage,
    ResolvedStateRef,
    ResolvedSuccession,
    ResolvedTag,
    ResolvedTagMember,
    ResolvedVariableGroup,
    ResolvedVariableSameAs,
)
from reg_meta_build.source_curation import ResolutionDiagnostic, SourceRecordRef


def _cause():
    return ResolutionDiagnostic(
        code="unknown_code_membership",
        severity="error",
        subject="scb/example/key",
        detail="Coding is unresolved.",
        refs=(SourceRecordRef(source="fixture", semantic_record_key=("key",)),),
        fields=("coding",),
        valid_from="2000-01-01",
        valid_to="2000-12-31",
        withheld_output=("scb/example/key:state",),
    )


def _variable(variant, slug="value"):
    return ResolvedVariable(
        register=ResolvedRegister(provider="scb", slug="example", name="Example"),
        slug=slug,
        provider_key=slug,
        name=slug,
        definition=None,
        description=None,
        operational_definition=None,
        measurement_unit=None,
        is_sensitive=False,
        is_identifier=False,
        states=(
            ResolvedState(
                variant=variant,
                valid_from="2000-01-01",
                valid_to="2000-12-31",
                delivery_column_name=slug,
                data_type=None,
                data_length=None,
                operational_definition=None,
                provenance="fixture",
            ),
        ),
        aliases=(ResolvedAlias(variant=variant, delivery_column_name="old"),),
    )


def test_panel_withholds_whole_composite_axis_and_preserves_shared_facts(tmp_path):
    variant = ResolvedVariant(
        slug="people",
        name="People",
        description="Independent prose",
        panel_entity_key=("value", "key"),
        panel_time_key="period",
        panel_time_grain="delivery",
    )
    variable = _variable(variant)
    edition = ResolvedEdition(
        register=variable.register_ref, variant=variant, name="2000"
    )
    result = resolve_panel_dependencies(
        (variable,),
        variants=((variable.register_ref, variant),),
        editions=(edition,),
        withheld={("variable", "scb/example/key"): (_cause(),)},
    )
    resolved = result.variants[0][1]
    assert resolved.panel_entity_key is None
    assert resolved.panel_time_key == "period"
    assert resolved.panel_time_grain == "delivery"
    assert resolved.description == "Independent prose"
    assert (
        result.variables[0].states[0].variant
        == result.variables[0].aliases[0].variant
        == result.editions[0].variant
        == resolved
    )
    (issue,) = result.diagnostics
    assert issue.refs == _cause().refs
    assert issue.valid_from == "2000-01-01"
    assert issue.withheld_output == ("scb/example/people:panel_entity_key",)
    path = write_resolved_catalog(
        result.variables,
        tmp_path / "diagnostic.db",
        manifest={},
        diagnostic=True,
        parent_registers=result.registers,
        parent_variants=result.variants,
        editions=result.editions,
    )
    with closing(open_db(path)) as conn:
        assert tuple(
            conn.execute(
                "SELECT panel_entity_key, panel_time_key, description FROM register_variant"
            ).fetchone()
        ) == (None, "period", "Independent prose")


def test_panel_requires_state_in_its_own_variant_not_only_variable():
    populated = ResolvedVariant(slug="populated", name="Populated")
    independent = ResolvedVariant(slug="other", name="Other", panel_entity_key="key")
    key = _variable(populated, "key")
    with pytest.raises(ValueError, match="unexplained missing catalog dependenc"):
        resolve_panel_dependencies(
            (key,), variants=((key.register_ref, independent),), withheld={}
        )
    result = resolve_panel_dependencies(
        (key,),
        variants=((key.register_ref, independent),),
        withheld={("variant_states", "scb/example/key", "other"): (_cause(),)},
    )
    assert len(result.variants) == 2
    assert (
        next(v for _, v in result.variants if v.slug == "other").panel_entity_key
        is None
    )
    assert result.variables == (key,)


def test_panel_checks_every_composite_member_before_withholding():
    variant = ResolvedVariant(
        slug="people", name="People", panel_entity_key=("key", "typo")
    )
    with pytest.raises(ValueError, match="typo"):
        resolve_panel_dependencies(
            (_variable(variant),),
            withheld={("variable", "scb/example/key"): (_cause(),)},
        )


def test_panel_rejects_unexplained_or_contradictory_omission():
    variant = ResolvedVariant(slug="people", name="People", panel_entity_key="value")
    variable = _variable(variant)
    assert resolve_panel_dependencies((variable,), withheld={}).diagnostics == ()
    with pytest.raises(ValueError, match="both present and withheld"):
        resolve_panel_dependencies(
            (variable,), withheld={("variable", "scb/example/value"): (_cause(),)}
        )
    with pytest.raises(ValueError, match="lacks source evidence"):
        resolve_panel_dependencies(
            (variable,), withheld={("variable", "scb/example/key"): ()}
        )


def test_panel_does_not_reconcile_conflicting_shared_definitions():
    variant = ResolvedVariant(slug="people", name="People")
    variable = _variable(variant)
    with pytest.raises(ValueError, match="inconsistent resolved parent variant"):
        resolve_panel_dependencies(
            (variable,),
            variants=(
                (
                    variable.register_ref,
                    variant.model_copy(update={"name": "Different"}),
                ),
            ),
            withheld={},
        )


def _metadata_result(metadata, *, withheld=None, variables=None):
    variant = ResolvedVariant(slug="people", name="People")
    variables = variables or tuple(_variable(variant, slug) for slug in ("one", "two"))
    return resolve_metadata_dependencies(
        metadata,
        variables,
        registers=(variables[0].register_ref,),
        variants=((variables[0].register_ref, variant),),
        classifications=(),
        withheld=withheld or {},
    )


def test_metadata_retains_supported_group_members_tags_and_relations():
    group = ResolvedVariableGroup(
        register="scb/example",
        key="family",
        label="Family",
        source="curated",
        members=tuple(
            ResolvedGroupVariable(variable=f"scb/example/{slug}")
            for slug in ("one", "two", "key")
        ),
    )
    safe_edge = ResolvedVariableSameAs(a="scb/example/one", b="scb/example/two")
    metadata = ResolvedMetadata(
        variable_groups=(group,),
        tags=(
            ResolvedTag(
                slug="topic",
                label="Topic",
                members=(
                    ResolvedTagMember(target="scb/example/key", rank=1, starred=False),
                ),
            ),
        ),
        variable_same_as=(
            safe_edge,
            ResolvedVariableSameAs(a="scb/example/two", b="scb/example/key"),
        ),
    )
    result = _metadata_result(
        metadata, withheld={("variable", "scb/example/key"): (_cause(),)}
    )
    (retained,) = result.metadata.variable_groups
    assert tuple(m.variable for m in retained.members) == (
        "scb/example/one",
        "scb/example/two",
    )
    assert retained.label == group.label
    assert result.metadata.tags[0].members == ()
    assert result.metadata.variable_same_as == (safe_edge,)
    assert len(result.diagnostics) == 3
    assert all(d.refs == _cause().refs for d in result.diagnostics)


@pytest.mark.parametrize("slugs", [("one", "key"), ("key", "absent")])
def test_group_with_fewer_than_two_members_is_explicitly_withheld(slugs):
    metadata = ResolvedMetadata(
        variable_groups=(
            ResolvedVariableGroup(
                register="scb/example",
                key="family",
                label="Family",
                source="curated",
                members=tuple(
                    ResolvedGroupVariable(variable=f"scb/example/{slug}")
                    for slug in slugs
                ),
            ),
        )
    )
    result = _metadata_result(
        metadata,
        withheld={
            ("variable", f"scb/example/{slug}"): (_cause(),)
            for slug in slugs
            if slug != "one"
        },
    )
    assert result.metadata.variable_groups == ()
    assert result.diagnostics[-1].withheld_output == (
        "variable_group:scb/example/family",
    )
    assert result.diagnostics[-1].refs == _cause().refs


def test_missing_relation_endpoint_does_not_hide_unconverted_other_endpoint():
    metadata = ResolvedMetadata(
        variable_same_as=(
            ResolvedVariableSameAs(a="scb/example/key", b="scb/example/typo"),
        )
    )
    with pytest.raises(ValueError, match="typo"):
        _metadata_result(
            metadata, withheld={("variable", "scb/example/key"): (_cause(),)}
        )


def test_accepted_omission_preserves_warning_severity_on_dependent_group():
    metadata = ResolvedMetadata(
        variable_groups=(
            ResolvedVariableGroup(
                register="scb/example",
                key="family",
                label="Family",
                source="curated",
                members=(
                    ResolvedGroupVariable(variable="scb/example/one"),
                    ResolvedGroupVariable(variable="scb/example/key"),
                ),
            ),
        )
    )
    cause = _cause().model_copy(
        update={
            "code": "curated_state_omission",
            "severity": "warning",
            "case_id": "accepted-omit",
        }
    )
    result = _metadata_result(
        metadata, withheld={("variable", "scb/example/key"): (cause,)}
    )
    assert result.metadata.variable_groups == ()
    assert len(result.diagnostics) == 2
    assert all(d.severity == "warning" for d in result.diagnostics)
    assert result.diagnostics[0].case_id == "accepted-omit"


def test_cycle_remains_fatal_when_all_its_endpoints_are_withheld():
    metadata = ResolvedMetadata(
        successions=(
            ResolvedSuccession(
                predecessor="scb/example/key", successor="scb/example/absent"
            ),
            ResolvedSuccession(
                predecessor="scb/example/absent", successor="scb/example/key"
            ),
        )
    )
    with pytest.raises(RegMetaError) as error:
        _metadata_result(
            metadata,
            withheld={
                ("variable", f"scb/example/{slug}"): (_cause(),)
                for slug in ("key", "absent")
            },
        )
    assert error.value.code == "replaced_by_cycle"


def test_representation_withholding_is_literal_but_succession_preserves_its_contract():
    metadata = ResolvedMetadata(
        representation_successions=(
            ResolvedRepresentationSuccession(
                predecessor=ResolvedRepresentationRef(
                    variable="scb/example/one", delivery_column_name="ONE"
                ),
                successor=ResolvedRepresentationRef(
                    variable="scb/example/two", delivery_column_name="TWO"
                ),
            ),
        )
    )
    assert _metadata_result(metadata).metadata == metadata
    group = ResolvedVariableGroup(
        register="scb/example",
        key="family",
        label="Family",
        source="curated",
        members=(
            ResolvedGroupVariable(
                variable="scb/example/one", delivery_column_name="ONE"
            ),
            ResolvedGroupVariable(
                variable="scb/example/two", delivery_column_name="two"
            ),
        ),
    )
    with pytest.raises(ValueError, match="unexplained missing catalog dependenc"):
        _metadata_result(ResolvedMetadata(variable_groups=(group,)))
    result = _metadata_result(
        ResolvedMetadata(variable_groups=(group,)),
        withheld={("representation", "scb/example/one", "ONE"): (_cause(),)},
    )
    assert result.metadata.variable_groups == ()


def test_exact_missing_state_withholds_lineage_without_removing_live_variables():
    common = {"variant": "people", "valid_from": "2000-01-01", "valid_to": "2000-12-31"}
    source = ResolvedStateRef(
        variable="scb/example/one", delivery_column_name="one", **common
    )
    consumer = ResolvedStateRef(
        variable="scb/example/two", delivery_column_name="missing", **common
    )
    metadata = ResolvedMetadata(
        state_lineage=(
            ResolvedStateLineage(
                consumer=consumer,
                source=source,
                valid_from=common["valid_from"],
                valid_to=common["valid_to"],
            ),
        )
    )
    with pytest.raises(ValueError, match="unexplained missing catalog dependenc"):
        _metadata_result(metadata)
    result = _metadata_result(
        metadata,
        withheld={
            (
                "state",
                "scb/example/two",
                "people",
                "2000-01-01",
                "2000-12-31",
                "missing",
                "",
            ): (_cause(),)
        },
    )
    assert result.metadata.state_lineage == ()
    assert result.diagnostics[0].withheld_output == ("state_lineage:0",)


def test_explicit_historical_predecessor_is_not_confused_with_withheld_entity():
    metadata = ResolvedMetadata(
        successions=(
            ResolvedSuccession(
                predecessor="scb/example/old", successor="scb/example/key"
            ),
        ),
        historical_predecessors=(
            ResolvedHistoricalPredecessor(
                target="scb/example/old",
                kind="variable",
                reason="Retired",
                decision_reference="accepted:7",
            ),
        ),
    )
    result = _metadata_result(
        metadata, withheld={("variable", "scb/example/key"): (_cause(),)}
    )
    assert result.metadata.successions == result.metadata.historical_predecessors == ()
    with pytest.raises(
        ValueError, match="historical predecessor is a withheld selected entity"
    ):
        _metadata_result(
            metadata, withheld={("variable", "scb/example/old"): (_cause(),)}
        )


@pytest.mark.parametrize("surface", ["panel", "relation"])
def test_dependency_failure_reports_every_missing_endpoint(surface):
    with pytest.raises(CatalogDependencyError) as error:
        if surface == "panel":
            variant = ResolvedVariant(
                slug="people", name="People", panel_entity_key=("key", "absent")
            )
            resolve_panel_dependencies((_variable(variant),), withheld={})
        else:
            _metadata_result(
                ResolvedMetadata(
                    variable_same_as=(
                        ResolvedVariableSameAs(
                            a="scb/example/key", b="scb/example/absent"
                        ),
                    )
                )
            )
    assert tuple(m.key[1] for m in error.value.missing) == (
        "scb/example/key",
        "scb/example/absent",
    )
    assert all(m.output for m in error.value.missing)
