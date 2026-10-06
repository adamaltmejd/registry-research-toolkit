"""Explicit discovery, relation and reference metadata written by the resolved writer."""

from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from _resolved_metadata_support import (
    full_metadata as _metadata,
    metadata_variable as _variable,
    state_ref as _state_ref,
    write_metadata_catalog as _write,
)
from catalog_manifest import synthetic_manifest
from pydantic import ValidationError
from reg_meta_build.db import open_built_db
from reg_meta_build.resolved_catalog import (
    ResolvedAlias,
    write_resolved_catalog,
)
from reg_meta_build.resolved_metadata import (
    ResolvedClassificationDerivation,
    ResolvedClassificationRef,
    ResolvedGroupVariable,
    ResolvedMetadata,
    ResolvedSourceJoinKey,
    ResolvedStateRef,
    ResolvedSuccession,
    ResolvedTagMember,
    ResolvedVariableGroup,
    ResolvedVariableSameAs,
    ResolvedVariantRef,
)

if TYPE_CHECKING:
    from pathlib import Path


def test_default_variant_references_preserve_reserved_slug_scope() -> None:
    assert (
        ResolvedVariantRef(register="scb/example", variant="_default").variant
        == "_default"
    )
    assert (
        ResolvedStateRef.model_validate(
            _state_ref().model_dump() | {"variant": "_default"}
        ).variant
        == "_default"
    )
    with pytest.raises(ValidationError, match="reserved"):
        ResolvedClassificationRef(provider="scb", classification="_default")


def test_all_explicit_metadata_surfaces_are_written_without_derivation(
    tmp_path: Path,
) -> None:
    output = tmp_path / "catalog.db"
    _write(output, _metadata())
    with closing(open_built_db(output)) as conn:
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


def test_group_members_preserve_case_distinct_declared_columns(tmp_path: Path) -> None:
    variable = _variable("one")
    variable = variable.model_copy(
        update={
            "aliases": (
                *variable.aliases,
                ResolvedAlias(
                    variant=variable.states[0].variant, delivery_column_name="ONECOLUMN"
                ),
            )
        }
    )
    group = ResolvedVariableGroup(
        register="scb/example",
        key="spellings",
        label="Accepted spellings",
        source="curated",
        members=tuple(
            ResolvedGroupVariable(
                variable="scb/example/one", delivery_column_name=column
            )
            for column in ("oneColumn", "ONECOLUMN")
        ),
    )
    output = tmp_path / "catalog.db"
    write_resolved_catalog(
        (variable,),
        output,
        manifest=synthetic_manifest(),
        metadata=ResolvedMetadata(variable_groups=(group,)),
    )
    with closing(open_built_db(output)) as conn:
        assert [
            row[0]
            for row in conn.execute(
                "SELECT delivery_column_name FROM concept_group_variable "
                "ORDER BY delivery_column_name"
            )
        ] == ["ONECOLUMN", "oneColumn"]


def test_multi_axis_group_can_attach_whole_variables(tmp_path: Path) -> None:
    group = _metadata().variable_groups[0]
    group = group.model_copy(
        update={
            "members": tuple(
                member.model_copy(update={"delivery_column_name": None})
                for member in group.members
            )
        }
    )
    output = tmp_path / "catalog.db"
    write_resolved_catalog(
        (_variable("one"), _variable("two")),
        output,
        manifest=synthetic_manifest(),
        metadata=ResolvedMetadata(variable_groups=(group,)),
    )
    with closing(open_built_db(output)) as conn:
        assert [
            tuple(row)
            for row in conn.execute(
                "SELECT v.slug, m.delivery_column_name, f.axis, f.value "
                "FROM concept_group_variable m JOIN variable v USING(variable_id) "
                "JOIN concept_group_variable_facet f USING(member_id) ORDER BY v.slug, f.axis"
            )
        ] == [
            ("one", None, "rank", "1"),
            ("one", None, "unit", "person"),
            ("two", None, "rank", "2"),
            ("two", None, "unit", "person"),
        ]


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
