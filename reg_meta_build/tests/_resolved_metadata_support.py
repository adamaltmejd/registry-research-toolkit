"""Shared resolved-metadata inputs and writer call for the resolved metadata tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

from catalog_manifest import synthetic_manifest
from reg_meta_build.resolved_catalog import (
    ResolvedAlias,
    ResolvedClassification,
    ResolvedClassificationCode,
    ResolvedRegister,
    ResolvedState,
    ResolvedVariable,
    ResolvedVariant,
    write_resolved_catalog,
)
from reg_meta_build.resolved_metadata import (
    ResolvedClassificationDerivation,
    ResolvedClassificationGroup,
    ResolvedClassificationRef,
    ResolvedClassificationSameAs,
    ResolvedGroupAxis,
    ResolvedGroupClassification,
    ResolvedGroupFacet,
    ResolvedGroupVariable,
    ResolvedIdentifierMetadata,
    ResolvedLineageWarning,
    ResolvedMetadata,
    ResolvedRepresentationRef,
    ResolvedRepresentationSuccession,
    ResolvedSourceColumn,
    ResolvedSourceJoinKey,
    ResolvedStateLineage,
    ResolvedStateRef,
    ResolvedSuccession,
    ResolvedTag,
    ResolvedTagMember,
    ResolvedTimeseriesEvent,
    ResolvedVariableGroup,
    ResolvedVariableSameAs,
    ResolvedVariantRef,
    ResolvedVariantSuccession,
)

if TYPE_CHECKING:
    from pathlib import Path


def metadata_variable(slug: str, provider: str = "scb") -> ResolvedVariable:
    variant = ResolvedVariant(slug="individuals", name="Individuals")
    return ResolvedVariable(
        register=ResolvedRegister(provider=provider, slug="example", name="Example"),
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
                delivery_column_name=f"{slug}Column",
                data_type=None,
                data_length=None,
                operational_definition=None,
                provenance="exact:source",
            ),
        ),
        aliases=(ResolvedAlias(variant=variant, delivery_column_name="OldColumn"),),
    )


def metadata_classification(slug: str) -> ResolvedClassification:
    return ResolvedClassification(
        slug=slug,
        short_name=slug,
        name=slug,
        codes=(ResolvedClassificationCode(code="01", label="One"),),
    )


def state_ref(slug: str = "one", provider: str = "scb") -> ResolvedStateRef:
    return ResolvedStateRef(
        variable=f"{provider}/example/{slug}",
        variant="individuals",
        valid_from="2000-01-01",
        valid_to="2000-12-31",
        delivery_column_name=f"{slug}Column",
    )


def full_metadata() -> ResolvedMetadata:
    group = ResolvedVariableGroup(
        register="scb/example",
        key="family-",
        label="Family",
        source="curated",
        axes=(
            ResolvedGroupAxis(axis="rank", ordinal=1, label="Rank"),
            ResolvedGroupAxis(axis="unit", ordinal=0, label="Unit"),
        ),
        members=tuple(
            ResolvedGroupVariable(
                variable=f"scb/example/{slug}",
                delivery_column_name=f"{slug}Column",
                facets=(
                    ResolvedGroupFacet(axis="rank", value=str(i), label=f"Rank {i}"),
                    ResolvedGroupFacet(axis="unit", value="person", label="Person"),
                ),
            )
            for i, slug in enumerate(("one", "two"), 1)
        ),
    )
    event = ResolvedTimeseriesEvent(
        name="Raw\u00a0name",
        event="Ersätts av",
        description="",
        entity="Variabel",
        first_token="0001",
        second_token="",
        file_token=None,
    )
    return ResolvedMetadata(
        variable_groups=(group,),
        classification_groups=(
            ResolvedClassificationGroup(
                key="codes",
                label="Code family",
                source="curated",
                axes=(ResolvedGroupAxis(axis="edition", ordinal=0, label="Edition"),),
                members=(
                    ResolvedGroupClassification(
                        classification="first-codes",
                        facet_value="1",
                        facet_label="First",
                    ),
                    ResolvedGroupClassification(
                        classification="second-codes",
                        facet_value="2",
                        facet_label="Second",
                    ),
                ),
            ),
        ),
        tags=(
            ResolvedTag(
                slug="topic",
                label="Topic",
                description="Thematic note",
                members=(
                    ResolvedTagMember(target="scb/example", rank=0, starred=False),
                    ResolvedTagMember(
                        target="scb/example/one", rank=2, starred=True, note="Useful"
                    ),
                ),
            ),
        ),
        variable_same_as=(
            ResolvedVariableSameAs(a="scb/example/one", b="sos/example/consumer"),
        ),
        classification_same_as=(
            ResolvedClassificationSameAs(
                a=ResolvedClassificationRef(
                    provider="scb", classification="first-codes"
                ),
                b=ResolvedClassificationRef(
                    provider="who", classification="second-codes"
                ),
            ),
        ),
        successions=(
            ResolvedSuccession(
                predecessor="scb/example",
                successor="sos/example",
                effective_year=2001,
                note="curated:register",
                description="Register transition",
            ),
            ResolvedSuccession(
                predecessor="scb/example/one",
                successor="scb/example/two",
                effective_year=2001,
                note="curated:variable",
                description="Variable transition",
            ),
        ),
        variant_successions=(
            ResolvedVariantSuccession(
                predecessor=ResolvedVariantRef(
                    register="scb/example", variant="individuals"
                ),
                successor=ResolvedVariantRef(
                    register="sos/example", variant="individuals"
                ),
                effective_year=2001,
                note="curated:variant",
                description="Variant transition",
            ),
        ),
        representation_successions=(
            ResolvedRepresentationSuccession(
                predecessor=ResolvedRepresentationRef(
                    variable="scb/example/one", delivery_column_name="OldColumn"
                ),
                successor=ResolvedRepresentationRef(
                    variable="scb/example/one", delivery_column_name="oneColumn"
                ),
                variant="individuals",
                effective_year=2000,
                note="curated:column",
                description="Column transition",
            ),
        ),
        classification_derivations=(
            ResolvedClassificationDerivation(
                derived="third-codes", source="first-codes", note="Specialization"
            ),
        ),
        state_lineage=(
            ResolvedStateLineage(
                consumer=state_ref("consumer", "sos"),
                source=state_ref(),
                valid_from="2000-03-01",
                valid_to="2000-11-30",
            ),
        ),
        lineage_warnings=(
            ResolvedLineageWarning(
                consumer=state_ref("two"),
                kind="no_source_state",
                message="No linked source state",
            ),
        ),
        source_columns=(
            ResolvedSourceColumn(
                table_name="RawTable",
                column_name="LöpNr",
                sql_type="varchar(12)",
                nullable=False,
            ),
        ),
        source_join_keys=(
            ResolvedSourceJoinKey(
                table_name="RawTable", column_name="LöpNr", description="Source key"
            ),
        ),
        identifiers=(
            ResolvedIdentifierMetadata(
                native_variable_id=44, name="Native key", definition=None
            ),
        ),
        timeseries_events=(
            event,
            event,
            event.model_copy(update={"first_token": None}),
        ),
    )


def write_metadata_catalog(path: Path, metadata: ResolvedMetadata) -> None:
    write_resolved_catalog(
        (
            metadata_variable("one"),
            metadata_variable("two"),
            metadata_variable("consumer", "sos"),
        ),
        path,
        manifest=synthetic_manifest(),
        metadata=metadata,
        classifications=tuple(
            metadata_classification(slug)
            for slug in ("first-codes", "second-codes", "third-codes")
        ),
    )
