"""The resolved writer's metadata contract and structural validation.

Metadata that breaks its own structure is refused before the catalog path is touched,
and a staged catalog that fails `validate_built_db` is never placed. No build reaches
these refusals. Curation loading refuses the curated shapes first (the
`curation_toml` cases named per row), formation forms lineage only between two
registers and only over the endpoint states' intersection (`resolve_catalog_lineage`),
the build compiles no classification same_as edge and no historical predecessor
declaration (`ResolvedMetadata.historical_predecessors` has no producer; the Rust
reader's retired-FQID redirect reads the edges it allows), and the pipeline refuses an
unknown panel key before it writes. The metadata refusals a build does reach are build
cases: `relations-same-as-cycle-fails-the-build`,
`relations-derived-from-cycle-fails-the-build`,
`relations-replaced-by-representation-round-trip-needs-distinct-years`,
`lineage-mutual-source-registers-fail-the-build` (a two-state lineage loop) and, for a
variable succession cycle, `dependency-withheld-variable-prunes-its-dependents/3-cycle`.
A variable in two curated groups is refused earlier, at curation load
(`group-variable-in-two-groups-fails-curation-load`).
"""

from __future__ import annotations

import os
from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from _resolved_catalog_support import resolved_variable
from _resolved_metadata_support import (
    full_metadata,
    state_ref,
    write_metadata_catalog,
)
from catalog_manifest import synthetic_manifest
from reg_meta_build.db import open_built_db
from reg_meta_build.errors import RegMetaError
from reg_meta_build.resolved_catalog import ResolvedVariable, write_resolved_catalog
from reg_meta_build.resolved_metadata import (
    ResolvedHistoricalPredecessor,
    ResolvedMetadata,
    ResolvedRepresentationRef,
    ResolvedRepresentationSuccession,
    ResolvedSuccession,
    ResolvedTag,
    ResolvedTagMember,
    ResolvedVariableSameAs,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


def _with(field: str, entry: object) -> ResolvedMetadata:
    """The support module's full metadata with `field` replaced by one entry."""
    return full_metadata().model_copy(update={field: (entry,)})


def _first(field: str) -> object:
    return getattr(full_metadata(), field)[0]


def _group_members(*members: object, **update: object) -> ResolvedMetadata:
    group = full_metadata().variable_groups[0]
    return _with(
        "variable_groups", group.model_copy(update={"members": members} | update)
    )


def _first_member(**update: object) -> object:
    return full_metadata().variable_groups[0].members[0].model_copy(update=update)


_RETIRED = "scb/example/retired"


def _historical(
    target: str = _RETIRED, predecessor: str = _RETIRED, **update: object
) -> ResolvedMetadata:
    """A reviewed historical predecessor and its succession into scb/example/one."""
    declaration = ResolvedHistoricalPredecessor(
        target=target,
        kind="variable",
        reason="Reviewed absent source identity",
        decision_reference="review:7",
    )
    edge = ResolvedSuccession(predecessor=predecessor, successor="scb/example/one")
    metadata = ResolvedMetadata(
        historical_predecessors=(declaration,), successions=(edge,)
    )
    return metadata.model_copy(update=update)


# One row per stated rule at its hardest case: the metadata written beside the support
# module's variables (scb/example/one and two, sos/example/consumer) and books, the
# refusal's message and, for a located refusal, its code.
INVALID_METADATA: dict[str, tuple[Callable[[], ResolvedMetadata], str, str | None]] = {
    # Loader twin: worklist-group-axisless-member-facets-refused.
    "group-without-axes-whose-members-carry-facets": (
        lambda: _group_members(*full_metadata().variable_groups[0].members, axes=()),
        "member must supply one facet per declared axis",
        None,
    ),
    # Loader twin: worklist-group-mixed-member-grain-refused.
    "group-mixing-whole-variable-and-column-members": (
        lambda: _group_members(
            _first_member(), _first_member(delivery_column_name=None)
        ),
        "group mixes whole-variable and representation members",
        None,
    ),
    # Loader twin: worklist-group-duplicate-member-refused.
    "group-listing-one-member-twice": (
        lambda: _group_members(_first_member(), _first_member()),
        r"duplicate resolved group member: \('scb/example/one', 'oneColumn'\)",
        None,
    ),
    # The second group shares one member with the first. Load twin, for two curated
    # groups of one register: group-variable-in-two-groups-fails-curation-load.
    "variable-in-two-groups": (
        lambda: full_metadata().model_copy(
            update={
                "variable_groups": (
                    full_metadata().variable_groups[0],
                    full_metadata()
                    .variable_groups[0]
                    .model_copy(update={"key": "other"}),
                )
            }
        ),
        "variable belongs to multiple resolved groups",
        None,
    ),
    # A member column is literal: one's column is oneColumn, so ONECOLUMN names a
    # representation the catalog does not write. Build twin:
    # dependency-withheld-variable-prunes-its-dependents/8-literal-group-column.
    "group-member-under-a-column-spelled-in-another-case": (
        lambda: _group_members(
            _first_member(), _first_member(delivery_column_name="ONECOLUMN")
        ),
        r"unknown resolved group representation: \('scb/example/one', 'ONECOLUMN'\)",
        None,
    ),
    # 1 is truthy: a lax flag would publish it as starred or not-null.
    "tag-star-not-a-boolean": (
        lambda: _with(
            "tags",
            _first("tags").model_copy(
                update={
                    "members": (
                        _first("tags").members[0].model_copy(update={"starred": 1}),
                    )
                }
            ),
        ),
        r"tags\.0\.members\.0\.starred\n.*valid boolean",
        None,
    ),
    "source-column-nullable-not-a-boolean": (
        lambda: _with(
            "source_columns",
            _first("source_columns").model_copy(update={"nullable": 0}),
        ),
        r"source_columns\.0\.nullable\n.*valid boolean",
        None,
    ),
    "classification-same-as-to-itself": (
        lambda: _with(
            "classification_same_as",
            _first("classification_same_as").model_copy(
                update={
                    "b": _first("classification_same_as").b.model_copy(
                        update={"classification": "first-codes"}
                    )
                }
            ),
        ),
        "classification same_as needs distinct global classifications",
        None,
    ),
    # Both endpoint states cover 2000; the edge runs into 2001.
    "lineage-wider-than-its-endpoint-states": (
        lambda: _with(
            "state_lineage",
            _first("state_lineage").model_copy(update={"valid_to": "2001-01-01"}),
        ),
        "lineage scope exceeds endpoint state intersection",
        None,
    ),
    "lineage-edge-twice": (
        lambda: full_metadata().model_copy(
            update={"state_lineage": full_metadata().state_lineage * 2}
        ),
        "duplicate resolved state lineage",
        None,
    ),
    "lineage-from-a-state-into-itself": (
        lambda: _with(
            "state_lineage",
            _first("state_lineage").model_copy(
                update={"consumer": state_ref(), "source": state_ref()}
            ),
        ),
        "state lineage forms a cycle",
        "lineage_cycle",
    ),
    # The warning says the consumer has no source state; the edge gives it one.
    "no-source-state-warning-on-a-linked-consumer": (
        lambda: _with(
            "lineage_warnings",
            _first("lineage_warnings").model_copy(
                update={"consumer": _first("state_lineage").consumer}
            ),
        ),
        "no_source_state warning contradicts explicit lineage",
        None,
    ),
    # A declaration must name the succession predecessor exactly.
    "historical-declaration-naming-no-predecessor": (
        lambda: _historical(target="scb/example/retiired"),
        r"unused historical predecessor declarations: \['scb/example/retiired'\]",
        None,
    ),
    "historical-declaration-naming-a-live-variable": (
        lambda: _historical(
            target="scb/example/two",
            predecessor="scb/example/two",
        ),
        "obsolete historical predecessor declaration now names a live entity: "
        "scb/example/two",
        None,
    ),
    # Without the declaration an absent predecessor is an unknown reference.
    "succession-from-an-undeclared-absent-predecessor": (
        lambda: _historical(historical_predecessors=()),
        f"unknown resolved succession predecessor: '{_RETIRED}'",
        None,
    ),
    # A declaration admits its target only as a succession predecessor.
    "historical-predecessor-in-a-same-as-edge": (
        lambda: _historical(
            variable_same_as=(ResolvedVariableSameAs(a=_RETIRED, b="scb/example/one"),)
        ),
        f"unknown resolved same_as variable: '{_RETIRED}'",
        None,
    ),
    "historical-predecessor-as-a-tag-member": (
        lambda: _historical(
            tags=(
                ResolvedTag(
                    slug="topic",
                    label="Topic",
                    members=(
                        ResolvedTagMember(target=_RETIRED, rank=0, starred=False),
                    ),
                ),
            )
        ),
        f"unknown resolved tag variable: '{_RETIRED}'",
        None,
    ),
    "historical-predecessor-in-a-representation-succession": (
        lambda: _historical(
            representation_successions=(
                ResolvedRepresentationSuccession(
                    predecessor=ResolvedRepresentationRef(
                        variable=_RETIRED, delivery_column_name="OldColumn"
                    ),
                    successor=ResolvedRepresentationRef(
                        variable="scb/example/one", delivery_column_name="oneColumn"
                    ),
                ),
            )
        ),
        f"unknown resolved variable: '{_RETIRED}'",
        None,
    ),
}


@pytest.mark.parametrize("row", INVALID_METADATA)
def test_invalid_metadata_is_refused_before_the_previous_catalog_is_touched(
    tmp_path: Path, row: str
) -> None:
    # Fails if the writer drops the structure rule the row names (a group, tag,
    # export-fact, classification, lineage or historical-declaration check), refuses
    # a located row under another code, or runs the check only after it has replaced
    # the previous catalog or left staging behind.
    make, message, code = INVALID_METADATA[row]
    output = tmp_path / "reg_meta.db"
    output.write_bytes(b"previous catalog")
    with pytest.raises((ValueError, RegMetaError), match=message) as error:
        write_metadata_catalog(output, make())
    assert getattr(error.value, "code", None) == code
    assert output.read_bytes() == b"previous catalog"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["reg_meta.db"]


def test_a_historical_predecessor_is_written_only_as_its_succession_edge(
    tmp_path: Path,
) -> None:
    # Fails if the writer drops the succession from a declared absent predecessor, or
    # writes a variable row for the declared target. The register grain is the build
    # case `cases/derive/succession-chains` (scb/old -> scb/reg); the reader's
    # redirect of a retired variable is the conformance case `states-deliveries`.
    output = tmp_path / "catalog.db"
    write_metadata_catalog(output, _historical())
    with closing(open_built_db(output)) as conn:
        variables = {row[0] for row in conn.execute("SELECT slug FROM variable")}
        edges = conn.execute(
            "SELECT predecessor_variable, successor_variable FROM variable_replaced_by"
        ).fetchall()
    assert variables == {"one", "two", "consumer"}
    assert [tuple(edge) for edge in edges] == [("retired", "one")]


def _with_unknown_panel_key() -> ResolvedVariable:
    variable = resolved_variable()
    variant = variable.states[0].variant.model_copy(
        update={"panel_entity_key": "missing"}
    )
    states = tuple(s.model_copy(update={"variant": variant}) for s in variable.states)
    return variable.model_copy(update={"states": states})


@pytest.mark.parametrize("diagnostic", [False, True])
def test_a_catalog_failing_structural_validation_is_never_placed(
    tmp_path: Path, diagnostic: bool
) -> None:
    # Fails if the writer places the staged catalog before `validate_built_db` passes
    # it, in either mode: a diagnostic catalog is created anyway, or a strict one
    # replaces the previous catalog, links it aside to `.prev` or leaves staging
    # behind. The variant panel key names no variable of its register, a refusal of
    # the staged artifact itself (not of the writer's input contract).
    output = tmp_path / ("diagnostic.db" if diagnostic else "reg_meta.db")
    if not diagnostic:
        output.write_bytes(b"previous catalog")
    with pytest.raises(
        ValueError, match="resolved catalog validation failed.*panel_entity_key"
    ):
        write_resolved_catalog(
            (_with_unknown_panel_key(),),
            output,
            manifest=synthetic_manifest(),
            diagnostic=diagnostic,
        )
    if diagnostic:
        assert not any(tmp_path.iterdir())
    else:
        assert output.read_bytes() == b"previous catalog"
        assert sorted(p.name for p in tmp_path.iterdir()) == ["reg_meta.db"]


def test_a_diagnostic_never_replaces_a_destination_created_during_the_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Fails if the writer places a diagnostic catalog by anything but an atomic
    # create-only link (a copy, `os.replace` or a check-then-write), so a destination
    # another process created after the writer's existence check is overwritten.
    # The build places its diagnostic through this writer.
    output = tmp_path / "diagnostic.db"
    real_link = os.link

    # Filesystem boundary: another process creates the destination just before the
    # finished diagnostic is placed.
    def competing_link(src, dst, *args, **kwargs):
        if os.fspath(dst) == os.fspath(output):
            output.write_bytes(b"published concurrently")
        return real_link(src, dst, *args, **kwargs)

    monkeypatch.setattr(os, "link", competing_link)
    with pytest.raises(FileExistsError):
        write_resolved_catalog(
            (resolved_variable(),),
            output,
            manifest=synthetic_manifest(),
            diagnostic=True,
        )
    assert output.read_bytes() == b"published concurrently"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["diagnostic.db"]
