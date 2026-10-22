"""The resolved writer's metadata contract and structural validation.

Metadata that breaks its own structure, or names what the catalog does not write, is
refused before the catalog path is touched, and a staged catalog that fails
`validate_built_db` is never placed. No build reaches
these refusals. Curation loading refuses the curated shapes first (the
`curation_toml` cases named per row), formation forms lineage only between two
registers and only over the endpoint states' intersection (`resolve_catalog_lineage`),
the build compiles no classification same_as edge, and the pipeline refuses an
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
import re
from typing import TYPE_CHECKING, Any

import pytest
from _resolved_catalog_support import resolved_variable
from _resolved_metadata_support import (
    full_metadata,
    state_ref,
    write_metadata_catalog,
)
from catalog_manifest import synthetic_manifest
from reg_meta_build.errors import RegMetaError
from reg_meta_build.materialize import write_resolved_catalog
from reg_meta_build.resolved_metadata import (
    ResolvedClassificationDerivation,
    ResolvedMetadata,
    ResolvedSuccession,
    ResolvedTagMember,
    ResolvedVariableSameAs,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from reg_meta_build.resolved_catalog import ResolvedVariable


def _with(field: str, entry: object) -> ResolvedMetadata:
    """The support module's full metadata with `field` replaced by one entry."""
    return full_metadata().model_copy(update={field: (entry,)})


def _first(field: str) -> Any:
    return getattr(full_metadata(), field)[0]


def _changed_edge(field: str, end: str, **update: object) -> Any:
    edge = _first(field)
    return edge.model_copy(update={end: getattr(edge, end).model_copy(update=update)})


def _group_members(*members: object, **update: object) -> ResolvedMetadata:
    group = full_metadata().variable_groups[0]
    return _with(
        "variable_groups", group.model_copy(update={"members": members} | update)
    )


def _first_member(**update: object) -> object:
    return full_metadata().variable_groups[0].members[0].model_copy(update=update)


_ABSENT = "scb/example/absent"


# One row per stated rule at its hardest case: the metadata written beside the support
# module's variables (scb/example/one and two, sos/example/consumer) and books, the
# refusal's message and, for a located refusal, its code.
INVALID_METADATA: dict[str, tuple[Callable[[], ResolvedMetadata], str, str | None]] = {
    # The register loader refuses the same rule (a member missing one of two
    # declared axes): curation_toml/group-member-missing-axis-facet-refused.
    "group-without-axes-whose-members-carry-facets": (
        lambda: _group_members(*full_metadata().variable_groups[0].members, axes=()),
        "member must supply one facet per declared axis",
        None,
    ),
    # Register-loader twin: curation_toml/group-mixed-member-grain-refused.
    "group-mixing-whole-variable-and-column-members": (
        lambda: _group_members(
            _first_member(), _first_member(delivery_column_name=None)
        ),
        "group mixes whole-variable and representation members",
        None,
    ),
    # Register-loader twin: curation_toml/group-repeated-member-refused.
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
    # Metadata that names what the catalog does not write. A build never hands the
    # writer such an edge: `resolve_metadata_dependencies` withholds or refuses each
    # dangling reference first (`catalog_dependency_missing`, pinned by the steps of
    # the build case `dependency-withheld-variable-prunes-its-dependents`), so these
    # checks are defense in depth.
    "group-member-not-written": (
        lambda: _group_members(
            _first_member(),
            full_metadata()
            .variable_groups[0]
            .members[1]
            .model_copy(update={"variable": _ABSENT}),
        ),
        f"unknown resolved group variable: '{_ABSENT}'",
        None,
    ),
    "tag-member-not-written": (
        lambda: _with(
            "tags",
            _first("tags").model_copy(
                update={
                    "members": (
                        ResolvedTagMember(target=_ABSENT, rank=0, starred=False),
                    )
                }
            ),
        ),
        f"unknown resolved tag variable: '{_ABSENT}'",
        None,
    ),
    "same-as-to-a-variable-not-written": (
        lambda: _with(
            "variable_same_as", ResolvedVariableSameAs(a="scb/example/one", b=_ABSENT)
        ),
        f"unknown resolved same_as variable: '{_ABSENT}'",
        None,
    ),
    "succession-to-a-variable-not-written": (
        lambda: _with(
            "successions",
            ResolvedSuccession(predecessor="scb/example/one", successor=_ABSENT),
        ),
        f"unknown resolved succession successor: '{_ABSENT}'",
        None,
    ),
    # The predecessor side is its own check: the writer once admitted an absent
    # predecessor (the retired-FQID redirect's historical declarations).
    "succession-from-a-variable-not-written": (
        lambda: _with(
            "successions",
            ResolvedSuccession(predecessor=_ABSENT, successor="scb/example/one"),
        ),
        f"unknown resolved succession predecessor: '{_ABSENT}'",
        None,
    ),
    "variant-succession-to-a-variant-not-written": (
        lambda: _with(
            "variant_successions",
            _changed_edge("variant_successions", "successor", variant="absent"),
        ),
        re.escape("unknown resolved succession variant: ('sos/example', 'absent')"),
        None,
    ),
    "representation-succession-from-a-column-not-delivered": (
        lambda: _with(
            "representation_successions",
            _changed_edge(
                "representation_successions",
                "predecessor",
                delivery_column_name="Missing",
            ),
        ),
        "unknown resolved representation: scb/example/one, Missing, individuals",
        None,
    ),
    "derivation-of-a-book-not-written": (
        lambda: _with(
            "classification_derivations",
            ResolvedClassificationDerivation(derived="absent", source="first-codes"),
        ),
        "unknown resolved derived classification: 'absent'",
        None,
    ),
    # The state is found by its variable, variant, start and code version; its
    # column must then match too.
    "lineage-from-a-state-under-another-column": (
        lambda: _with(
            "state_lineage",
            _changed_edge(
                "state_lineage", "source", delivery_column_name="WrongColumn"
            ),
        ),
        "resolved state reference does not match its exact scope/column",
        None,
    ),
}


@pytest.mark.parametrize("row", INVALID_METADATA)
def test_invalid_metadata_is_refused_before_the_previous_catalog_is_touched(
    tmp_path: Path, row: str
) -> None:
    # Fails if the writer drops the structure rule the row names (a group, tag,
    # export-fact, classification or lineage check, or a
    # reference to something it does not write), refuses a located row under another
    # code, or runs the check only after it has replaced the previous catalog or left
    # staging behind.
    make, message, code = INVALID_METADATA[row]
    output = tmp_path / "reg_meta.db"
    output.write_bytes(b"previous catalog")
    with pytest.raises((ValueError, RegMetaError), match=message) as error:
        write_metadata_catalog(output, make())
    assert getattr(error.value, "code", None) == code
    assert output.read_bytes() == b"previous catalog"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["reg_meta.db"]


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
