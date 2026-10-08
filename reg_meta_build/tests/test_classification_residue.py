"""The classification-residue worklist renderer on a state the CLI corpus cannot
reach cheaply.

Every other residue claim is a `cases/cli/classification-residue/` case on a built
catalog. A NULL variable slug comes only from `extend-db --skip-slugs`, a steward
overlay onto a base catalog; a boundary case would need a steward overlay built to
carry a residual value set and a case that runs two different commands, which the
CLI runner does not. This pins the renderer directly instead, as input -> output.
"""

from __future__ import annotations

import tomllib

from reg_meta_build.classifications import (
    ResidualState,
    ResidueCandidate,
    ResidueResult,
    ResidueValueSet,
    render_residue_toml,
)


def test_unslugged_safe_variable_is_held_back_beside_its_slugged_twin() -> None:
    """A safe value set with two unclassified states: `scb/ulf/inkomst` and
    `scb/ulf/`, the FQID a variable with a NULL slug (an `extend-db --skip-slugs`
    build) renders as. The curation loader refuses the empty segment, so the
    worklist binds only the slugged variable and flags the other UNSLUGGED in a
    comment.

    Fails if render_residue_toml stops excluding an FQID with an empty segment
    from its copyable bindings.
    """
    target = ResidueCandidate(
        "FAM_A", containment=1.0, label_agree=1.0, standalone=True
    )
    result = ResidueResult(
        value_sets=(
            ResidueValueSet(
                value_set_id=1,
                n_codes=4,
                candidates=(
                    target,
                    ResidueCandidate(
                        "FAM_B", containment=1.0, label_agree=0.0, standalone=True
                    ),
                ),
                states=(
                    ResidualState(1, "scb/ulf/inkomst", "Inkomst"),
                    ResidualState(2, "scb/ulf/", "Utan slug"),
                ),
                safe_target=target,
            ),
        ),
        total=1,
        safe_count=1,
    )

    rendered = render_residue_toml(result)

    assert tomllib.loads(rendered) == {
        "binding": {
            "variable": [{"variable": "scb/ulf/inkomst", "note": "residue:safe"}]
        }
    }
    assert "# UNSLUGGED: variable scb/ulf/ (Utan slug) has a safe value set" in (
        rendered
    )
