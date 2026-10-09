"""The resolved writer refuses invalid input before it touches the catalog path.

No build reaches these refusals. Formation constructs consistent resolved models, and
it withholds a variable whose flags are unresolved (`unresolved_flag`, pinned by the
build case `dependency-withheld-variable-prunes-its-dependents`) before the writer
runs. They stay as writer-boundary tests because the writer is the last guard on the
disclosure flags and on the catalog contract. Each one drives `write_resolved_catalog`,
the writer `build-db` calls: an unvalidated copy of a resolved model reaches it,
because the writer revalidates every instance it is given.

Publication, byte identity and create-only placement are pinned beside the build-db
contract, in `test_build_db_cli_outputs.py`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from _resolved_catalog_support import (
    resolved_state as _state,
    resolved_variable as _variable,
)
from catalog_manifest import synthetic_manifest
from reg_meta_build.errors import RegMetaError
from reg_meta_build.resolved_catalog import (
    CURATION_TREE_SHA256_KEY,
    ResolvedAlias,
    ResolvedAliasWindow,
    ResolvedEdition,
    ResolvedVariable,
    write_resolved_catalog,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


def _changed(**update: object) -> ResolvedVariable:
    return _variable().model_copy(update=update)


def _with_state(**update: object) -> ResolvedVariable:
    return _changed(states=(_state(2000).model_copy(update=update),))


def _year_independent(**update: object) -> ResolvedVariable:
    undated = {"period_scope": "year_independent", "valid_from": None, "valid_to": None}
    return _with_state(**(undated | update))


def _with_alias_windows(*windows: ResolvedAliasWindow) -> ResolvedVariable:
    variable = _variable()
    alias = ResolvedAlias(
        variant=variable.states[0].variant,
        delivery_column_name="Alias",
        windows=(ResolvedAliasWindow(valid_from="2000-01-01", valid_to="2000-12-31"),),
    )
    return _changed(aliases=(alias.model_copy(update={"windows": windows}),))


def _alias_window_ending(end: str) -> ResolvedVariable:
    window = ResolvedAliasWindow(valid_from="2021-02-01", valid_to="2021-02-28")
    return _with_alias_windows(window.model_copy(update={"valid_to": end}))


def _without_sensitivity() -> ResolvedVariable:
    fields = {k: v for k, v in vars(_variable()).items() if k != "is_sensitive"}
    return ResolvedVariable.model_construct(**fields)


def _second_variable(conflict: str) -> ResolvedVariable:
    variable = _variable()
    other = _variable(slug="another")
    if conflict == "register":
        register = variable.register_ref.model_copy(update={"name": "Other"})
        return other.model_copy(update={"register_ref": register})
    variant = variable.states[0].variant.model_copy(update={"name": "Other"})
    states = tuple(s.model_copy(update={"variant": variant}) for s in other.states)
    return other.model_copy(update={"states": states})


def _conflicting_parent(kind: str) -> dict[str, Any]:
    variable = _variable()
    if kind == "register":
        register = variable.register_ref.model_copy(update={"name": "Conflict"})
        return {"parent_registers": (register,)}
    variant = variable.states[0].variant.model_copy(update={"name": "Conflict"})
    return {"parent_variants": ((variable.register_ref, variant),)}


def _conflicting_edition(defect: str) -> dict[str, Any]:
    variable = _variable()
    edition = ResolvedEdition(
        register=variable.register_ref, variant=variable.states[0].variant, name="2000"
    )
    if defect == "duplicate":
        return {"editions": (edition, edition)}
    if defect == "register":
        register = variable.register_ref.model_copy(update={"name": "Conflicting name"})
        return {"editions": (edition.model_copy(update={"register_ref": register}),)}
    variant = edition.variant.model_copy(update={"name": "Conflicting name"})
    return {"editions": (edition.model_copy(update={"variant": variant}),)}


_YEAR = ResolvedAliasWindow(valid_from="2000-01-01", valid_to="2000-12-31")

# The writer's input contract, one row per stated rule at its hardest case: the
# variables handed to the writer and the refusal. "20000101" parses as an ISO date and
# is refused only because it does not round-trip; "false" is truthy, so a lax flag
# would publish it as sensitive. An off-calendar date is refused by the date parser,
# whose wording differs between implementations, so those rows match the refused field.
INVALID_VARIABLES: dict[str, tuple[Callable[[], tuple[ResolvedVariable, ...]], str]] = {
    "empty-catalog": (tuple, "empty resolved catalog"),
    "variable-without-states": (
        lambda: (_changed(states=()),),
        "at least one delivery state",
    ),
    "unnamed-variable-without-positive-delivery-names": (
        lambda: (_changed(name=None),),
        "positive delivery names",
    ),
    "sensitivity-missing": (
        lambda: (_without_sensitivity(),),
        r"is_sensitive\n.*Field required",
    ),
    **{
        f"{field}-{value!r}": (
            lambda field=field, value=value: (_changed(**{field: value}),),
            rf"{field}\n.*valid boolean",
        )
        for field in ("is_sensitive", "is_identifier")
        for value in (1, "false")
    },
    "slug-not-kebab-case": (lambda: (_changed(slug="Bad_slug"),), "invalid slug"),
    "slug-reserved": (lambda: (_changed(slug="_default"),), "reserved"),
    "states-overlapping": (
        lambda: (_changed(states=(_state(2000), _state(2000))),),
        "overlapping states",
    ),
    "state-date-not-round-tripping": (
        lambda: (_with_state(valid_from="20000101"),),
        "full ISO dates",
    ),
    "state-date-not-on-the-calendar": (
        lambda: (_with_state(valid_from="2000-02-30"),),
        r"states\.0\.valid_from\n",
    ),
    "state-bounds-reversed": (
        lambda: (_with_state(valid_from="2001-01-01"),),
        "must not exceed valid_to",
    ),
    "state-open-end-not-the-sentinel": (
        lambda: (_with_state(valid_to="9999-01-01"),),
        "open-ended sentinel",
    ),
    "state-column-untrimmed": (
        lambda: (_with_state(delivery_column_name=" AmPolTyp"),),
        "nonempty and trimmed",
    ),
    "year-independent-with-start": (
        lambda: (_year_independent(valid_from="2000-01-01"),),
        "no calendar bounds",
    ),
    "year-independent-with-open-end": (
        lambda: (_year_independent(valid_to="9999-12-31"),),
        "no calendar bounds",
    ),
    "year-independent-pooled": (
        lambda: (_year_independent(pooled=True),),
        "pooled flag",
    ),
    "dated-without-end": (
        lambda: (_with_state(valid_to=None),),
        "requires both ISO bounds",
    ),
    "alias-window-date-not-on-the-calendar": (
        lambda: (_alias_window_ending("2021-02-29"),),
        r"aliases\.0\.windows\.0\.valid_to\n",
    ),
    "alias-windows-overlapping": (
        lambda: (_with_alias_windows(_YEAR, _YEAR),),
        "overlapping windows",
    ),
    "variables-duplicate": (
        lambda: (_variable(), _variable()),
        "duplicate variable FQID",
    ),
    **{
        f"variables-inconsistent-{kind}": (
            lambda kind=kind: (_variable(), _second_variable(kind)),
            f"inconsistent {kind} definition",
        )
        for kind in ("register", "variant")
    },
}

# Writer arguments beside one valid variable: parents, editions and the manifest.
INVALID_ARGUMENTS: dict[str, tuple[Callable[[], dict[str, Any]], str]] = {
    **{
        f"parent-inconsistent-{kind}": (
            lambda kind=kind: _conflicting_parent(kind),
            f"inconsistent resolved parent {kind}",
        )
        for kind in ("register", "variant")
    },
    **{
        f"edition-inconsistent-{kind}": (
            lambda kind=kind: _conflicting_edition(kind),
            f"inconsistent edition {kind} definition",
        )
        for kind in ("register", "variant")
    },
    "edition-duplicate": (
        lambda: _conflicting_edition("duplicate"),
        "duplicate resolved edition",
    ),
    "manifest-overrides-schema-version": (
        lambda: {"manifest": {"schema_version": "invalid"}},
        "manifest conflicts with catalog schema_version",
    ),
    **{
        f"manifest-curation-hash-{name}": (
            lambda digest=digest: {
                "manifest": synthetic_manifest() | {CURATION_TREE_SHA256_KEY: digest}
            },
            f"manifest {CURATION_TREE_SHA256_KEY} must be a lowercase SHA-256",
        )
        for name, digest in (("short", "short"), ("uppercase", "A" * 64))
    },
}


@pytest.mark.parametrize("row", [*INVALID_VARIABLES, *INVALID_ARGUMENTS])
def test_invalid_input_is_refused_before_the_previous_catalog_is_touched(
    tmp_path: Path, row: str
) -> None:
    # Fails if the writer drops the rule the row names (a lax or optional flag, a
    # date, slug, overlap, consistency or manifest check), or runs it only after it
    # has replaced the previous catalog, linked it aside to `.prev` or left staging
    # behind.
    arguments: dict[str, Any] = {"manifest": synthetic_manifest()}
    if row in INVALID_VARIABLES:
        variables, message = INVALID_VARIABLES[row]
        given = variables()
    else:
        make, message = INVALID_ARGUMENTS[row]
        arguments, given = arguments | make(), (_variable(),)
    output = tmp_path / "reg_meta.db"
    output.write_bytes(b"previous catalog")
    with pytest.raises(ValueError, match=message):
        write_resolved_catalog(given, output, **arguments)
    assert output.read_bytes() == b"previous catalog"
    assert sorted(p.name for p in tmp_path.iterdir()) == ["reg_meta.db"]


@pytest.mark.parametrize("field", ["is_sensitive", "is_identifier"])
@pytest.mark.parametrize("diagnostic", [False, True])
def test_unknown_flags_are_refused_even_in_a_diagnostic_catalog(
    tmp_path: Path, field: str, diagnostic: bool
) -> None:
    # Fails if the writer stores an unknown sensitivity or identifier flag (as NULL
    # or as a default) in either mode. The model keeps the unknown flag as evidence
    # for formation's `unresolved_flag` withholding; only the writer refuses it.
    output = tmp_path / ("diagnostic.db" if diagnostic else "reg_meta.db")
    with pytest.raises(ValueError, match=f"unknown flags.*{field}"):
        write_resolved_catalog(
            (_changed(**{field: None}),),
            output,
            manifest=synthetic_manifest(),
            diagnostic=diagnostic,
        )
    assert not any(tmp_path.iterdir())


def test_unknown_provider_is_a_located_refusal(tmp_path: Path) -> None:
    # Fails if a provider without a provider_id seed is written (an id invented for
    # it) or refused under another code.
    output = tmp_path / "reg_meta.db"
    with pytest.raises(RegMetaError) as error:
        write_resolved_catalog(
            (_variable("unknown-provider"),), output, manifest=synthetic_manifest()
        )
    assert error.value.code == "unknown_provider"
    assert "No provider_id seed" in error.value.message
    assert not any(tmp_path.iterdir())
