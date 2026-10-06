"""Resolved metadata contracts: invalid shapes, cycles and historical predecessors."""

from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from _resolved_metadata_support import (
    full_metadata as _metadata,
    state_ref as _state_ref,
    write_metadata_catalog as _write,
)
from pydantic import ValidationError
from reg_meta.errors import RegMetaError
from reg_meta_build.db import open_built_db
from reg_meta_build.resolved_metadata import (
    ResolvedClassificationDerivation,
    ResolvedHistoricalPredecessor,
    ResolvedMetadata,
    ResolvedRepresentationRef,
    ResolvedRepresentationSuccession,
    ResolvedStateLineage,
    ResolvedSuccession,
    ResolvedTag,
    ResolvedTagMember,
    ResolvedVariableSameAs,
)

if TYPE_CHECKING:
    from pathlib import Path


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
        "column_case_typo",
        "duplicate_members",
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
    elif defect in {"column_case_typo", "duplicate_members"}:
        first = group.members[0]
        metadata = metadata.model_copy(
            update={
                "variable_groups": (
                    group.model_copy(
                        update={
                            "members": (
                                first,
                                first.model_copy(
                                    update={
                                        "delivery_column_name": "ONECOLUMN"
                                        if defect == "column_case_typo"
                                        else first.delivery_column_name
                                    }
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
    with closing(open_built_db(output)) as conn:
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
    with closing(open_built_db(output)) as conn:
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
