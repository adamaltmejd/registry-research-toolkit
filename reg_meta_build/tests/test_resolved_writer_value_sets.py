"""Resolved writer value sets: membership identity, determinism and column overlaps."""

from __future__ import annotations

from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from _resolved_catalog_support import (
    resolved_state as _state,
    resolved_variable as _variable,
)
from catalog_manifest import synthetic_manifest
from pydantic import ValidationError
from reg_meta_build.db import open_built_db
from reg_meta_build.resolved_catalog import (
    ResolvedCodeSet,
    column_state_overlaps,
    validate_resolved_variables,
    write_resolved_catalog,
)
from reg_meta_build.validate import validate_built_db

if TYPE_CHECKING:
    from pathlib import Path


def _coded(label: str, code: str) -> dict[str, object]:
    return {
        "value_set_version_label": label,
        "value_set": ResolvedCodeSet(members=((code, label),)),
    }


@pytest.mark.parametrize(
    ("updates", "code"),
    [
        ((_coded("A", "01"), _coded("B", "02")), "overlapping_distinct_value_sets"),
        (({}, _coded("A", "01")), "overlapping_codeless_codebearing_states"),
        (
            ({}, {"pooled": True, "value_set_version_label": "P"}),
            "overlapping_pooled_explicit_states",
        ),
        # One code list under two labels is a representation, not a conflict.
        (
            (_coded("A", "01"), {**_coded("A", "01"), "value_set_version_label": "B"}),
            None,
        ),
    ],
)
def test_formation_attributes_exactly_the_column_overlaps_the_writer_refuses(
    tmp_path: Path, updates: tuple[dict[str, object], ...], code: str | None
) -> None:
    state = _state(2000)
    variable = _variable().model_copy(
        update={"states": tuple(state.model_copy(update=u) for u in updates)}
    )
    overlaps = column_state_overlaps(variable)
    output = tmp_path / "reg_meta.db"
    if code is None:
        assert overlaps == ()
        write_resolved_catalog((variable,), output, manifest=synthetic_manifest())
        return
    ((found, message),) = overlaps
    assert found == code
    with pytest.raises(ValueError) as failure:
        write_resolved_catalog((variable,), output, manifest=synthetic_manifest())
    assert message.split(": ")[0] in str(failure.value)


def test_shared_memberships_have_stable_ids_and_deterministic_replay(
    tmp_path: Path,
) -> None:
    members = (("01", "Participation"), ("02", "Employment"))
    variables = tuple(
        _variable(provider).model_copy(
            update={
                "states": tuple(
                    _state(year).model_copy(
                        update={"value_set": ResolvedCodeSet(members=members)}
                    )
                    for year in (2000, 2002)
                )
            }
        )
        for provider in ("scb", "sos")
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(variables, output, manifest=synthetic_manifest())
    original = output.read_bytes()
    with closing(open_built_db(output)) as conn:
        assert conn.execute("SELECT count(*) FROM value_set").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM value_code").fetchone()[0] == 2
        assert conn.execute("SELECT count(*) FROM value_set_member").fetchone()[0] == 2
        assert conn.execute("SELECT count(*) FROM code_variable_map").fetchone()[0] == 4
        assert {
            row[0] for row in conn.execute("SELECT mapping_count FROM value_code")
        } == {2}
        assert (
            conn.execute(
                "SELECT count(DISTINCT value_set_id) FROM variable_state"
            ).fetchone()[0]
            == 1
        )
        shared_id = conn.execute("SELECT value_set_id FROM value_set").fetchone()[0]
    reordered = tuple(
        variable.model_copy(
            update={
                "states": tuple(
                    state.model_copy(
                        update={
                            "value_set": ResolvedCodeSet(
                                members=tuple(reversed(members)) + members
                            )
                        }
                    )
                    for state in reversed(variable.states)
                )
            }
        )
        for variable in reversed(variables)
    )
    write_resolved_catalog(reordered, output, manifest=synthetic_manifest())
    assert output.read_bytes() == original
    # Adding an unrelated membership cannot renumber an existing content ID.
    other = _variable(slug="other").model_copy(
        update={
            "states": (
                _state(2000).model_copy(
                    update={"value_set": ResolvedCodeSet(members=(("0", "Other"),))}
                ),
            )
        }
    )
    write_resolved_catalog((other, *variables), output, manifest=synthetic_manifest())
    with closing(open_built_db(output)) as conn:
        ids = conn.execute(
            "SELECT DISTINCT state.value_set_id FROM variable_state state "
            "JOIN variable USING (variable_id) WHERE slug = 'ampoltyp'"
        ).fetchall()
        assert [row[0] for row in ids] == [shared_id]


def test_distinct_memberships_and_changed_labels_remain_distinct(
    tmp_path: Path,
) -> None:
    memberships = (
        (("01", "Participation"),),
        (("01", "Revised label"),),
        (("01", "Participation"), ("02", "Employment")),
        # Same concatenated code+label bytes, split differently.
        (("ab", "c"),),
        (("a", "bc"),),
    )
    variable = _variable().model_copy(
        update={
            "states": tuple(
                _state(year).model_copy(
                    update={"value_set": ResolvedCodeSet(members=members)}
                )
                for year, members in zip(
                    range(2000, 2000 + len(memberships)), memberships, strict=True
                )
            )
        }
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((variable,), output, manifest=synthetic_manifest())
    with closing(open_built_db(output)) as conn:
        states = conn.execute(
            "SELECT value_set_id FROM variable_state ORDER BY valid_from"
        ).fetchall()
        assert len({row[0] for row in states}) == len(memberships) == 5
        for state, members in zip(states, memberships, strict=True):
            assert (
                tuple(
                    tuple(row)
                    for row in conn.execute(
                        "SELECT code, label FROM value_set_member JOIN value_code USING (code_id) WHERE value_set_id=? ORDER BY code, label",
                        (state[0],),
                    )
                )
                == members
            )
    assert validate_built_db(output, corpus=False).passed


@pytest.mark.parametrize(
    "members",
    [(), ((1, "Label"),), (("01", None),), (("01", "Label", "Extra"),)],
)
def test_malformed_memberships_fail_shared_preflight_and_preserve_catalog(
    tmp_path: Path, members: tuple
) -> None:
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog((_variable(),), output, manifest=synthetic_manifest())
    original = output.read_bytes()
    malformed = ResolvedCodeSet(members=(("01", "Label"),)).model_copy(
        update={"members": members}
    )
    variable = _variable().model_copy(
        update={"states": (_state(2000).model_copy(update={"value_set": malformed}),)}
    )
    with pytest.raises(ValidationError):
        validate_resolved_variables((variable,))
    with pytest.raises(ValidationError):
        write_resolved_catalog((variable,), output, manifest=synthetic_manifest())
    assert output.read_bytes() == original
    assert sorted(p.name for p in tmp_path.iterdir()) == ["reg_meta.db"]


def test_a_leading_space_code_stays_a_distinct_written_member(tmp_path: Path) -> None:
    """A value set with `" 01"` and `"01"` (same label) is written as two members, and
    the label search finds both codes.

    No build reaches it: every source reader normalizes the code token at its read
    boundary (`sources/code_lists.py` `normalize_token`, the SCB reader's trim), so a
    leading-space code never reaches formation. The source-built members (a labelled
    blank, two labels for one code, `01` beside `1`) are the `members` rows of
    `cases/build/classification-bindings-conform-extend-or-stay-unbound-per-register`.
    Fails if the resolved writer or `value_code` storage trims or folds codes, so the
    two members collapse into one.
    """
    members = (("01", "Participation"), (" 01", "Participation"))
    state = _state(2000).model_copy(
        update={"value_set": ResolvedCodeSet(members=members)}
    )
    output = tmp_path / "reg_meta.db"
    write_resolved_catalog(
        (_variable().model_copy(update={"states": (state,)}),),
        output,
        manifest=synthetic_manifest(),
    )
    with closing(open_built_db(output)) as conn:
        written = conn.execute(
            "SELECT code, label FROM value_set_member JOIN value_code USING (code_id) "
            "JOIN variable_state USING (value_set_id)"
        ).fetchall()
        hits = conn.execute(
            "SELECT c.code FROM value_code_fts f JOIN value_code c ON c.code_id=f.rowid "
            "WHERE value_code_fts MATCH 'Participation'"
        ).fetchall()
    assert sorted(tuple(row) for row in written) == sorted(members)
    assert sorted(row["code"] for row in hits) == [" 01", "01"]
