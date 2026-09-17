"""Source claims and accepted identity are separate requirements for lineage."""

import pytest
from reg_meta_build.catalog_dependencies import CatalogDependencyError
from reg_meta_build.catalog_lineage import resolve_catalog_lineage
from reg_meta_build.resolved_catalog import ResolvedRegister, ResolvedVariant
from reg_meta_build.resolved_metadata import ResolvedMetadata, ResolvedVariableSameAs
from reg_meta_build.source_curation import SourceRecordRef
from test_catalog_dependencies import _variable


def fixture(*, same_as=True, duplicate_name=False, second_variant=False):
    consumer = _variable(ResolvedVariant(slug="people", name="People"))
    origin = ResolvedRegister(
        provider="scb", slug="origin", name="Original register (ORIG)"
    )
    source = consumer.model_copy(update={"register_ref": origin})
    consumer = consumer.model_copy(
        update={
            "source_register_text": "Original register (ORIG) : People",
            "states": tuple(
                s.model_copy(
                    update={"source_register_text": "Original register (ORIG) : People"}
                )
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
    registers = (consumer.register_ref, origin)
    if duplicate_name:
        registers += (origin.model_copy(update={"slug": "duplicate"}),)
    metadata = ResolvedMetadata(
        variable_same_as=(
            ResolvedVariableSameAs(a="scb/example/value", b="scb/origin/value"),
        )
        if same_as
        else ()
    )
    return (consumer, source), {
        "registers": registers,
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


def test_explicit_identity_and_source_claim_generate_intersection_edges():
    variables, options = fixture()
    result = resolve_catalog_lineage(variables, **options)
    assert not result.diagnostics
    assert result.variables[0].source_register.slug == "origin"
    assert result.variables[0].source_label == "ORIG"
    assert len(result.metadata.state_lineage) == 1
    edge = result.metadata.state_lineage[0]
    assert (edge.valid_from, edge.valid_to) == ("2000-01-01", "2000-12-31")
    assert resolve_catalog_lineage(tuple(reversed(variables)), **options) == result


def test_matching_slugs_are_not_identity_evidence():
    variables, options = fixture(same_as=False)
    result = resolve_catalog_lineage(variables, **options)
    assert not result.metadata.state_lineage
    assert result.diagnostics[0].code == "unresolved_lineage_no_source_state"
    assert result.metadata.lineage_warnings[0].kind == "no_source_state"


def test_ambiguous_register_label_does_not_pick_input_order():
    variables, options = fixture(duplicate_name=True)
    result = resolve_catalog_lineage(variables, **options)
    assert result.variables[0].source_register is None
    assert not result.metadata.state_lineage
    assert result.diagnostics[0].code == "ambiguous_source_register"


def test_variant_default_resolves_ambiguity_and_unused_dangling_pin_fails():
    variables, options = fixture(second_variant=True)
    result = resolve_catalog_lineage(variables, **options)
    assert not result.metadata.state_lineage
    assert result.diagnostics[0].code == "unresolved_lineage_ambiguous_source_variant"
    options["defaults"] = {"scb/origin": "people"}
    resolved = resolve_catalog_lineage(variables, **options)
    assert len(resolved.metadata.state_lineage) == 1 and not resolved.diagnostics
    options["defaults"] = {"scb/missing": "people"}
    with pytest.raises(CatalogDependencyError):
        resolve_catalog_lineage(variables, **options)


def test_disjoint_validity_keeps_identity_without_inventing_edges_or_errors():
    variables, options = fixture()
    consumer, source = variables
    source = source.model_copy(
        update={
            "states": tuple(
                s.model_copy(
                    update={"valid_from": "2001-01-01", "valid_to": "2001-12-31"}
                )
                for s in source.states
            )
        }
    )
    result = resolve_catalog_lineage((consumer, source), **options)
    assert not result.metadata.state_lineage and not result.diagnostics
