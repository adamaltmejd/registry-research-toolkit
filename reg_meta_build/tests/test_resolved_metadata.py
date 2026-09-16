"""Explicit discovery, relation and reference facts survive direct catalog writing."""

from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from pydantic import ValidationError
from reg_meta.db import open_db
from reg_meta.errors import RegMetaError
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
    ResolvedHistoricalPredecessor,
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


def _variable(slug: str, provider: str = "scb") -> ResolvedVariable:
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


def _classification(slug: str) -> ResolvedClassification:
    return ResolvedClassification(
        slug=slug,
        short_name=slug,
        name=slug,
        codes=(ResolvedClassificationCode(code="01", label="One"),),
    )


def _state_ref(slug: str = "one", provider: str = "scb") -> ResolvedStateRef:
    return ResolvedStateRef(
        variable=f"{provider}/example/{slug}",
        variant="individuals",
        valid_from="2000-01-01",
        valid_to="2000-12-31",
        delivery_column_name=f"{slug}Column",
    )


def _metadata() -> ResolvedMetadata:
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
                consumer=_state_ref("consumer", "sos"),
                source=_state_ref(),
                valid_from="2000-03-01",
                valid_to="2000-11-30",
            ),
        ),
        lineage_warnings=(
            ResolvedLineageWarning(
                consumer=_state_ref("two"),
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


def _write(path: Path, metadata: ResolvedMetadata) -> None:
    write_resolved_catalog(
        (_variable("one"), _variable("two"), _variable("consumer", "sos")),
        path,
        manifest={},
        metadata=metadata,
        classifications=tuple(
            _classification(slug)
            for slug in ("first-codes", "second-codes", "third-codes")
        ),
    )


def test_all_explicit_metadata_surfaces_are_written_without_derivation(
    tmp_path: Path,
) -> None:
    output = tmp_path / "catalog.db"
    _write(output, _metadata())
    with closing(open_db(output)) as conn:
        counts = {
            "concept_group": 2,
            "concept_group_axis": 3,
            "concept_group_variable": 2,
            "concept_group_variable_facet": 4,
            "concept_group_classification": 2,
            "tag": 1,
            "tag_member": 2,
            "variable_same_as": 2,
            "classification_same_as": 2,
            "register_replaced_by": 1,
            "variable_replaced_by": 1,
            "variant_replaced_by": 1,
            "representation_replaced_by": 1,
            "classification_derived_from": 1,
            "variable_state_lineage": 1,
            "variable_state_lineage_warning": 1,
            "source_column_type": 1,
            "source_join_key": 1,
            "identifier_semantics": 1,
            "timeseries_event": 3,
        }
        for table, count in counts.items():
            assert conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == count
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT a.axis, a.ordinal, a.label FROM concept_group_axis a JOIN concept_group g USING(group_id) "
                "WHERE g.kind='variable' ORDER BY a.ordinal"
            )
        ] == [("unit", 0, "Unit"), ("rank", 1, "Rank")]
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT v.slug, m.delivery_column_name, f.axis, f.value, f.label "
                "FROM concept_group_variable m JOIN variable v USING(variable_id) "
                "JOIN concept_group_variable_facet f USING(member_id) ORDER BY v.slug, f.axis"
            )
        ] == [
            ("one", "oneColumn", "rank", "1", "Rank 1"),
            ("one", "oneColumn", "unit", "person", "Person"),
            ("two", "twoColumn", "rank", "2", "Rank 2"),
            ("two", "twoColumn", "unit", "person", "Person"),
        ]
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT c.slug, m.facet_value, m.facet_label FROM concept_group_classification m "
                "JOIN classification c ON c.id=m.classification_id ORDER BY c.slug"
            )
        ] == [("first-codes", "1", "First"), ("second-codes", "2", "Second")]
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT t.slug, r.slug, v.slug, m.rank, m.starred, m.note FROM tag_member m "
                "JOIN tag t USING(tag_id) LEFT JOIN register r USING(register_id) LEFT JOIN variable v USING(variable_id) ORDER BY m.rank"
            )
        ] == [
            ("topic", "example", None, 0, 0, None),
            ("topic", None, "one", 2, 1, "Useful"),
        ]
        for table, source, description in (
            ("register_replaced_by", "curated:register", "Register transition"),
            ("variable_replaced_by", "curated:variable", "Variable transition"),
            ("variant_replaced_by", "curated:variant", "Variant transition"),
        ):
            assert tuple(
                conn.execute(
                    f"SELECT effective_year, note, beskrivning FROM {table}"
                ).fetchone()
            ) == (2001, source, description)
        assert tuple(
            conn.execute(
                "SELECT derived_slug, source_slug, note FROM classification_derived_from"
            ).fetchone()
        ) == ("third-codes", "first-codes", "Specialization")
        assert tuple(
            conn.execute(
                "SELECT c.slug, s.slug FROM variable_state_lineage l "
                "JOIN variable_state cs ON cs.state_id=l.consumer_state_id JOIN variable c ON c.variable_id=cs.variable_id "
                "JOIN variable_state ss ON ss.state_id=l.source_state_id JOIN variable s ON s.variable_id=ss.variable_id"
            ).fetchone()
        ) == ("consumer", "one")
        assert tuple(
            conn.execute(
                "SELECT warning_kind, message FROM variable_state_lineage_warning"
            ).fetchone()
        ) == ("no_source_state", "No linked source state")
        assert tuple(
            conn.execute(
                "SELECT var_id, variabelnamn, variabeldefinition FROM identifier_semantics"
            ).fetchone()
        ) == (44, "Native key", None)
        assert tuple(
            conn.execute(
                "SELECT table_name, column_name, description FROM source_join_key"
            ).fetchone()
        ) == ("RawTable", "LöpNr", "Source key")
        assert tuple(
            conn.execute(
                "SELECT name, is_sensitive, is_identifier FROM variable WHERE slug='one'"
            ).fetchone()
        ) == ("one", 0, 0)
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT a_provider, a_classification_slug, b_provider, b_classification_slug FROM classification_same_as ORDER BY a_provider"
            )
        ] == [
            ("scb", "first-codes", "who", "second-codes"),
            ("who", "second-codes", "scb", "first-codes"),
        ]
        assert tuple(
            conn.execute(
                "SELECT predecessor_column, successor_column, variant, effective_year, note, beskrivning FROM representation_replaced_by"
            ).fetchone()
        ) == (
            "OldColumn",
            "oneColumn",
            "individuals",
            2000,
            "curated:column",
            "Column transition",
        )
        assert tuple(
            conn.execute(
                "SELECT valid_from, valid_to FROM variable_state_lineage"
            ).fetchone()
        ) == ("2000-03-01", "2000-11-30")
        assert tuple(
            conn.execute(
                "SELECT table_name, column_name, sql_type, nullable FROM source_column_type"
            ).fetchone()
        ) == ("RawTable", "LöpNr", "varchar(12)", 0)
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT namn, id1, id2, fil_id FROM timeseries_event ORDER BY id1"
            )
        ] == [
            ("Raw\u00a0name", None, "", None),
            ("Raw\u00a0name", "0001", "", None),
            ("Raw\u00a0name", "0001", "", None),
        ]
        assert (
            conn.execute("SELECT count(*) FROM classification_replaced_by").fetchone()[
                0
            ]
            == 0
        )
        assert conn.execute("SELECT count(*) FROM variable_state").fetchone()[0] == 3


def test_metadata_reordering_produces_identical_bytes(tmp_path: Path) -> None:
    metadata = _metadata()
    output = tmp_path / "catalog.db"
    _write(output, metadata)
    original = output.read_bytes()
    group = metadata.variable_groups[0]
    updated = metadata.model_copy(
        update={
            name: tuple(reversed(getattr(metadata, name)))
            for name in type(metadata).model_fields
        }
    )
    updated = updated.model_copy(
        update={
            "variable_groups": (
                group.model_copy(
                    update={
                        "axes": tuple(reversed(group.axes)),
                        "members": tuple(
                            member.model_copy(
                                update={"facets": tuple(reversed(member.facets))}
                            )
                            for member in reversed(group.members)
                        ),
                    }
                ),
            )
        }
    )
    _write(output, updated)
    assert output.read_bytes() == original


@pytest.mark.parametrize(
    "surface",
    [
        "group",
        "tag",
        "same_as",
        "successor",
        "variant",
        "representation",
        "classification",
        "state",
        "join_key",
    ],
)
def test_unknown_dependent_references_preserve_previous_catalog(
    tmp_path: Path, surface: str
) -> None:
    metadata = _metadata()
    if surface == "group":
        group = metadata.variable_groups[0]
        metadata = metadata.model_copy(
            update={
                "variable_groups": (
                    group.model_copy(
                        update={
                            "members": (
                                group.members[0],
                                group.members[1].model_copy(
                                    update={"variable": "scb/example/absent"}
                                ),
                            )
                        }
                    ),
                )
            }
        )
    elif surface == "tag":
        metadata = metadata.model_copy(
            update={
                "tags": (
                    metadata.tags[0].model_copy(
                        update={
                            "members": (
                                ResolvedTagMember(
                                    target="scb/example/absent", rank=0, starred=False
                                ),
                            )
                        }
                    ),
                )
            }
        )
    elif surface == "same_as":
        metadata = metadata.model_copy(
            update={
                "variable_same_as": (
                    ResolvedVariableSameAs(a="scb/example/one", b="scb/example/absent"),
                )
            }
        )
    elif surface == "successor":
        metadata = metadata.model_copy(
            update={
                "successions": (
                    ResolvedSuccession(
                        predecessor="scb/example/one", successor="scb/example/absent"
                    ),
                )
            }
        )
    elif surface == "variant":
        edge = metadata.variant_successions[0]
        metadata = metadata.model_copy(
            update={
                "variant_successions": (
                    edge.model_copy(
                        update={
                            "successor": edge.successor.model_copy(
                                update={"variant": "absent"}
                            )
                        }
                    ),
                )
            }
        )
    elif surface == "representation":
        edge = metadata.representation_successions[0]
        metadata = metadata.model_copy(
            update={
                "representation_successions": (
                    edge.model_copy(
                        update={
                            "predecessor": edge.predecessor.model_copy(
                                update={"delivery_column_name": "Missing"}
                            )
                        }
                    ),
                )
            }
        )
    elif surface == "classification":
        metadata = metadata.model_copy(
            update={
                "classification_derivations": (
                    ResolvedClassificationDerivation(
                        derived="absent", source="first-codes"
                    ),
                )
            }
        )
    elif surface == "state":
        edge = metadata.state_lineage[0]
        metadata = metadata.model_copy(
            update={
                "state_lineage": (
                    edge.model_copy(
                        update={
                            "source": edge.source.model_copy(
                                update={"delivery_column_name": "WrongColumn"}
                            )
                        }
                    ),
                )
            }
        )
    else:
        metadata = metadata.model_copy(
            update={
                "source_join_keys": (
                    ResolvedSourceJoinKey(table_name="Missing", column_name="Column"),
                )
            }
        )
    output = tmp_path / "existing.db"
    output.write_bytes(b"previous")
    with pytest.raises(ValueError, match="unknown resolved|exact scope/column"):
        _write(output, metadata)
    assert output.read_bytes() == b"previous"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["existing.db"]


@pytest.mark.parametrize(
    "defect",
    [
        "axes",
        "mixed_grain",
        "multiple_groups",
        "starred",
        "nullable",
        "lineage_scope",
        "state_duplicate",
        "no_source_warning",
        "classification_self_equivalence",
        "case_duplicate_members",
    ],
)
def test_invalid_metadata_contracts_fail_before_output(
    tmp_path: Path, defect: str
) -> None:
    metadata = _metadata()
    group = metadata.variable_groups[0]
    if defect == "axes":
        metadata = metadata.model_copy(
            update={"variable_groups": (group.model_copy(update={"axes": ()}),)}
        )
    elif defect == "mixed_grain":
        metadata = metadata.model_copy(
            update={
                "variable_groups": (
                    group.model_copy(
                        update={
                            "members": (
                                group.members[0],
                                group.members[0].model_copy(
                                    update={"delivery_column_name": None}
                                ),
                            )
                        }
                    ),
                )
            }
        )
    elif defect == "multiple_groups":
        metadata = metadata.model_copy(
            update={
                "variable_groups": (group, group.model_copy(update={"key": "other"}))
            }
        )
    elif defect == "starred":
        tag = metadata.tags[0]
        metadata = metadata.model_copy(
            update={
                "tags": (
                    tag.model_copy(
                        update={
                            "members": (
                                tag.members[0].model_copy(update={"starred": 1}),
                            )
                        }
                    ),
                )
            }
        )
    elif defect == "nullable":
        metadata = metadata.model_copy(
            update={
                "source_columns": (
                    metadata.source_columns[0].model_copy(update={"nullable": 0}),
                )
            }
        )
    elif defect == "lineage_scope":
        metadata = metadata.model_copy(
            update={
                "state_lineage": (
                    metadata.state_lineage[0].model_copy(
                        update={"valid_to": "2001-01-01"}
                    ),
                )
            }
        )
    elif defect == "no_source_warning":
        metadata = metadata.model_copy(
            update={
                "lineage_warnings": (
                    metadata.lineage_warnings[0].model_copy(
                        update={
                            "consumer": metadata.state_lineage[0].consumer,
                        }
                    ),
                ),
            }
        )
    elif defect == "classification_self_equivalence":
        edge = metadata.classification_same_as[0]
        metadata = metadata.model_copy(
            update={
                "classification_same_as": (
                    edge.model_copy(
                        update={
                            "b": edge.b.model_copy(
                                update={"classification": edge.a.classification}
                            ),
                        }
                    ),
                ),
            }
        )
    elif defect == "case_duplicate_members":
        first = group.members[0]
        metadata = metadata.model_copy(
            update={
                "variable_groups": (
                    group.model_copy(
                        update={
                            "members": (
                                first,
                                first.model_copy(
                                    update={"delivery_column_name": "ONECOLUMN"}
                                ),
                            ),
                        }
                    ),
                )
            }
        )
    else:
        metadata = metadata.model_copy(
            update={"state_lineage": metadata.state_lineage * 2}
        )
    output = tmp_path / "new.db"
    with pytest.raises((ValueError, ValidationError)):
        _write(output, metadata)
    assert not output.exists()


@pytest.mark.parametrize(
    "surface", ["same_as", "succession", "derivation", "lineage", "representation"]
)
def test_cycles_are_rejected_without_publishing(tmp_path: Path, surface: str) -> None:
    metadata = ResolvedMetadata()
    if surface == "same_as":
        metadata = metadata.model_copy(
            update={
                "variable_same_as": (
                    ResolvedVariableSameAs(a="scb/example/one", b="scb/example/one"),
                )
            }
        )
    elif surface == "succession":
        metadata = metadata.model_copy(
            update={
                "successions": (
                    ResolvedSuccession(
                        predecessor="scb/example/one", successor="scb/example/two"
                    ),
                    ResolvedSuccession(
                        predecessor="scb/example/two", successor="scb/example/one"
                    ),
                )
            }
        )
    elif surface == "derivation":
        metadata = metadata.model_copy(
            update={
                "classification_derivations": (
                    ResolvedClassificationDerivation(
                        derived="first-codes", source="second-codes"
                    ),
                    ResolvedClassificationDerivation(
                        derived="second-codes", source="first-codes"
                    ),
                )
            }
        )
    elif surface == "lineage":
        metadata = metadata.model_copy(
            update={
                "state_lineage": (
                    ResolvedStateLineage(
                        consumer=_state_ref(),
                        source=_state_ref(),
                        valid_from="2000-01-01",
                        valid_to="2000-12-31",
                    ),
                )
            }
        )
    else:
        edge = (
            _metadata()
            .representation_successions[0]
            .model_copy(update={"variant": None})
        )
        metadata = metadata.model_copy(
            update={
                "representation_successions": (
                    edge,
                    edge.model_copy(
                        update={
                            "predecessor": edge.successor,
                            "successor": edge.predecessor,
                            "effective_year": 2001,
                        }
                    ),
                )
            }
        )
    output = tmp_path / "catalog.db"
    with pytest.raises(RegMetaError) as error:
        _write(output, metadata)
    assert "cycle" in error.value.code
    assert not output.exists()


def test_variant_scoped_temporal_round_trip_is_explicit_and_validated(
    tmp_path: Path,
) -> None:
    edge = _metadata().representation_successions[0]
    reverse = edge.model_copy(
        update={
            "predecessor": edge.successor,
            "successor": edge.predecessor,
            "effective_year": 2001,
        }
    )
    metadata = ResolvedMetadata(representation_successions=(edge, reverse))
    output = tmp_path / "catalog.db"
    _write(output, metadata)
    original = output.read_bytes()
    with closing(open_db(output)) as conn:
        assert (
            conn.execute("SELECT count(*) FROM representation_replaced_by").fetchone()[
                0
            ]
            == 2
        )
    with pytest.raises(RegMetaError) as error:
        _write(
            output,
            metadata.model_copy(
                update={
                    "representation_successions": (
                        edge,
                        reverse.model_copy(
                            update={"effective_year": edge.effective_year}
                        ),
                    )
                }
            ),
        )
    assert "cycle" in error.value.code
    assert output.read_bytes() == original


@pytest.mark.parametrize("kind", ["register", "variable"])
def test_documented_historical_predecessor_creates_only_the_edge(
    tmp_path: Path, kind: str
) -> None:
    predecessor, successor = (
        ("scb/retired-register", "scb/example")
        if kind == "register"
        else ("scb/example/retired-variable", "scb/example/one")
    )
    declaration = ResolvedHistoricalPredecessor.model_validate(
        {
            "target": predecessor,
            "kind": kind,
            "reason": "Documented retired entity retained for navigation",
            "decision_reference": "curation:reviewed-example:decision-7",
        }
    )
    metadata = ResolvedMetadata(
        historical_predecessors=(declaration,),
        successions=(
            ResolvedSuccession(
                predecessor=predecessor,
                successor=successor,
                effective_year=2000,
                note="curation:reviewed-example:decision-7",
            ),
        ),
    )
    output = tmp_path / "catalog.db"
    _write(output, metadata)
    original = output.read_bytes()
    with closing(open_db(output)) as conn:
        assert conn.execute("SELECT count(*) FROM register").fetchone()[0] == 2
        assert conn.execute("SELECT count(*) FROM variable").fetchone()[0] == 3
        assert (
            conn.execute(f"SELECT count(*) FROM {kind}_replaced_by").fetchone()[0] == 1
        )
    with pytest.raises(ValueError, match="unknown resolved succession predecessor"):
        _write(output, metadata.model_copy(update={"historical_predecessors": ()}))
    assert output.read_bytes() == original


@pytest.mark.parametrize(
    "defect",
    [
        "typo",
        "unused",
        "now_live",
        "missing_successor",
        "same_as",
        "tag",
        "representation",
    ],
)
def test_historical_declarations_are_exact_used_and_predecessor_only(
    tmp_path: Path, defect: str
) -> None:
    declaration = ResolvedHistoricalPredecessor(
        target="scb/example/retired",
        kind="variable",
        reason="Reviewed absent source identity",
        decision_reference="review:7",
    )
    edge = ResolvedSuccession(
        predecessor=declaration.target, successor="scb/example/one"
    )
    metadata = ResolvedMetadata(
        historical_predecessors=(declaration,), successions=(edge,)
    )
    if defect == "typo":
        metadata = metadata.model_copy(
            update={
                "historical_predecessors": (
                    declaration.model_copy(update={"target": "scb/example/retiired"}),
                )
            }
        )
    elif defect == "unused":
        metadata = metadata.model_copy(update={"successions": ()})
    elif defect == "now_live":
        metadata = metadata.model_copy(
            update={
                "historical_predecessors": (
                    declaration.model_copy(update={"target": "scb/example/one"}),
                )
            }
        )
    elif defect == "missing_successor":
        metadata = metadata.model_copy(
            update={
                "successions": (
                    edge.model_copy(update={"successor": "scb/example/missing"}),
                )
            }
        )
    elif defect == "same_as":
        metadata = metadata.model_copy(
            update={
                "variable_same_as": (
                    ResolvedVariableSameAs(a=declaration.target, b=edge.successor),
                )
            }
        )
    elif defect == "tag":
        metadata = metadata.model_copy(
            update={
                "tags": (
                    ResolvedTag(
                        slug="topic",
                        label="Topic",
                        members=(
                            ResolvedTagMember(
                                target=declaration.target, rank=0, starred=False
                            ),
                        ),
                    ),
                )
            }
        )
    else:
        metadata = metadata.model_copy(
            update={
                "representation_successions": (
                    ResolvedRepresentationSuccession(
                        predecessor=ResolvedRepresentationRef(
                            variable=declaration.target,
                            delivery_column_name="OldColumn",
                        ),
                        successor=ResolvedRepresentationRef(
                            variable=edge.successor, delivery_column_name="oneColumn"
                        ),
                    ),
                )
            }
        )
    output = tmp_path / "existing.db"
    output.write_bytes(b"previous")
    with pytest.raises(
        ValueError, match="unknown resolved|unused historical|obsolete historical"
    ):
        _write(output, metadata)
    assert output.read_bytes() == b"previous"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["existing.db"]
