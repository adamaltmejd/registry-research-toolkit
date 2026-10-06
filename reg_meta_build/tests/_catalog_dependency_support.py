"""Shared resolved-catalog builders for the catalog dependency tests."""

from reg_meta_build.resolved_catalog import (
    ResolvedAlias,
    ResolvedRegister,
    ResolvedState,
    ResolvedVariable,
)
from reg_meta_build.source_curation import ResolutionDiagnostic, SourceRecordRef


def cause():
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


def variable(variant, slug="value"):
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
