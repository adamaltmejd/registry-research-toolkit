"""Year-independent lineage endpoints, which no build case can deliver yet.

Every other lineage claim is a build case (`cases/build/lineage-*`). These stay here
because a year-independent state comes only from the LISA reader
(`sources/lisa.py`, the `Individ årsoberoende` layout), and the build-case runner
prepares no LISA workbook. `unsupported_lineage_scope` and the ambiguity below are
what stop the build from writing a dated edge for an undated endpoint. Move them to
`cases/build/` when the runner can select a LISA delivery.
"""

import pytest
from _catalog_dependency_support import variable as _variable
from reg_meta_build.catalog_lineage import resolve_catalog_lineage
from reg_meta_build.resolved_catalog import ResolvedRegister, ResolvedVariant
from reg_meta_build.resolved_metadata import ResolvedMetadata, ResolvedVariableSameAs
from reg_meta_build.source_curation import SourceRecordRef


def fixture(
    *,
    same_as=True,
    second_variant=False,
    source_label="Original register (ORIG) : People",
):
    consumer = _variable(ResolvedVariant(slug="people", name="People"))
    origin = ResolvedRegister(
        provider="scb", slug="origin", name="Original register (ORIG)"
    )
    source = consumer.model_copy(update={"register_ref": origin})
    consumer = consumer.model_copy(
        update={
            "source_register_text": source_label,
            "states": tuple(
                s.model_copy(update={"source_register_text": source_label})
                for s in consumer.states
            ),
        }
    )
    if second_variant:
        source = source.model_copy(
            update={
                "states": (
                    *source.states,
                    source.states[0].model_copy(
                        update={"variant": ResolvedVariant(slug="other", name="Other")}
                    ),
                )
            }
        )
    metadata = ResolvedMetadata(
        variable_same_as=(
            ResolvedVariableSameAs(a="scb/example/value", b="scb/origin/value"),
        )
        if same_as
        else ()
    )
    return (consumer, source), {
        "registers": (consumer.register_ref, origin),
        "variants": tuple(
            (v.register_ref, s.variant) for v in (consumer, source) for s in v.states
        ),
        "defaults": {},
        "metadata": metadata,
        "evidence": {
            key: (SourceRecordRef(source="fixture", semantic_record_key=(key,)),)
            for key in ("scb/example/value", "scb/origin/value")
        },
        "withheld": {},
    }


def _year_independent(variable):
    return variable.model_copy(
        update={
            "states": tuple(
                state.model_copy(
                    update={
                        "period_scope": "year_independent",
                        "valid_from": None,
                        "valid_to": None,
                    }
                )
                for state in variable.states
            )
        }
    )


def test_independent_source_variant_cannot_be_filtered_as_disjoint():
    variables, options = fixture(
        second_variant=True, source_label="Original register (ORIG)"
    )
    consumer, source = variables
    dated, independent = source.states
    independent = independent.model_copy(
        update={
            "period_scope": "year_independent",
            "valid_from": None,
            "valid_to": None,
        }
    )
    source = source.model_copy(update={"states": (dated, independent)})
    result = resolve_catalog_lineage((consumer, source), **options)
    assert not result.metadata.state_lineage
    assert [issue.code for issue in result.diagnostics] == [
        "unresolved_lineage_ambiguous_source_variant"
    ]


@pytest.mark.parametrize("independent_endpoint", [0, 1])
def test_independent_source_attribution_never_invents_dated_lineage(
    independent_endpoint,
):
    variables, options = fixture()
    variables = tuple(
        _year_independent(variable) if index == independent_endpoint else variable
        for index, variable in enumerate(variables)
    )
    result = resolve_catalog_lineage(variables, **options)
    assert not result.metadata.state_lineage
    assert not result.metadata.lineage_warnings
    assert result.variables[0].source_register.slug == "origin"
    assert [issue.code for issue in result.diagnostics] == ["unsupported_lineage_scope"]


def test_independent_register_only_attribution_retains_missing_endpoint_warning():
    variables, options = fixture(same_as=False)
    consumer = _year_independent(variables[0])
    result = resolve_catalog_lineage((consumer, variables[1]), **options)
    assert not result.metadata.state_lineage
    assert result.variables[0].source_register.slug == "origin"
    assert result.variables[0].source_register_text == consumer.source_register_text
    assert [issue.code for issue in result.diagnostics] == [
        "unresolved_lineage_no_source_state"
    ]
    assert result.diagnostics[0].severity == "warning"
    (warning,) = result.metadata.lineage_warnings
    assert warning.kind == "no_source_state"
    assert warning.consumer.period_scope == "year_independent"
    assert warning.consumer.valid_from is warning.consumer.valid_to is None
