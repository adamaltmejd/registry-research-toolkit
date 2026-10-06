"""Shared resolved-writer inputs for the resolved catalog tests."""

from __future__ import annotations

from reg_meta_build.resolved_catalog import (
    ResolvedAlias,
    ResolvedAliasWindow,
    ResolvedClassification,
    ResolvedClassificationCode,
    ResolvedClassificationLink,
    ResolvedCodeSet,
    ResolvedConformance,
    ResolvedRegister,
    ResolvedState,
    ResolvedVariable,
    ResolvedVariant,
)


def resolved_state(year: int, *, column: str = "AmPolTyp") -> ResolvedState:
    return ResolvedState(
        variant=ResolvedVariant(slug="individuals", name="Individuals"),
        valid_from=f"{year}-01-01",
        valid_to=f"{year}-12-31",
        delivery_column_name=column,
        data_type="integer",
        data_length="8",
        operational_definition=f"State definition for {year}",
        provenance=f"curation:fixture:{year}",
    )


def resolved_variable(
    provider: str = "scb", slug: str = "ampoltyp"
) -> ResolvedVariable:
    return ResolvedVariable(
        register=ResolvedRegister(provider=provider, slug="example", name="Example"),
        slug=slug,
        provider_key="44",
        name="Arbetsmarknadspolitisk åtgärd",
        definition="An explicitly resolved definition",
        description="A searchable description",
        operational_definition="The canonical operational definition",
        measurement_unit=None,
        is_sensitive=True,
        is_identifier=False,
        states=(resolved_state(2000), resolved_state(2002, column="AmPolTypUpdated")),
    )


def resolved_classification(slug: str = "example-codes") -> ResolvedClassification:
    return ResolvedClassification(
        slug=slug,
        short_name=slug,
        name="Canonical example",
        name_en="Example",
        publisher="Publisher",
        valid_from=2000,
        valid_to=2020,
        description="Codebook description",
        url="https://example.invalid/codebook",
        codes=(
            ResolvedClassificationCode(code="001", label="Canonical one", level=1),
            ResolvedClassificationCode(code="002", label="Canonical two", level=2),
        ),
    )


def scoped_sentinel_variable():
    from reg_meta.source_evidence import canonical_sha256
    from reg_meta_build.resolved_catalog import ResolvedScopedSentinels

    book = resolved_classification()
    certificate = ResolvedScopedSentinels(
        delivery_column_name="AmPolTyp",
        valid_from="2000-01-01",
        valid_to="2000-12-31",
        classification_sha256=canonical_sha256(book.model_dump(mode="json")),
        source_fingerprints=("a" * 64,),
        members=(("09350", "Okänt"),),
        provenance="checked-source-sentinel: exact original coding and codebook guards",
    )
    conformance = ResolvedConformance(
        declared_classification=book.slug,
        status="extended",
        checked_codes=("001", "09350"),
        sentinel_members=certificate.members,
        scoped_sentinels=(certificate,),
    )
    state = resolved_state(2000).model_copy(
        update={
            "value_set": ResolvedCodeSet(
                members=(("001", "Source label"), ("09350", "Okänt"))
            ),
            "classification_links": (
                ResolvedClassificationLink(
                    classification=book.slug, conformance=conformance
                ),
            ),
        }
    )
    return resolved_variable().model_copy(update={"states": (state,)}), book


def classified_alias_variable():
    variable, book = scoped_sentinel_variable()
    original = variable.states[0]
    window = ResolvedAliasWindow(
        valid_from=original.valid_from,
        valid_to=original.valid_to,
        coding_metadata="per_column",
        value_set=original.value_set,
        value_set_version_label="Original physical list",
        classification_links=original.classification_links,
    )
    backing = original.model_copy(
        update={
            "value_set": None,
            "value_set_version_label": "",
            "classification_links": (),
        }
    )
    alias = ResolvedAlias(
        variant=original.variant,
        delivery_column_name=original.delivery_column_name,
        windows=(window,),
    )
    return variable.model_copy(update={"states": (backing,), "aliases": (alias,)}), book
