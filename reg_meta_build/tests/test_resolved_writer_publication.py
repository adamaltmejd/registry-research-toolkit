"""The resolved writer refuses invalid input before it touches the catalog path.

No build reaches these refusals. Formation constructs consistent resolved models, and
it withholds a variable whose flags are unresolved (`unresolved_flag`, pinned by the
build case `dependency-withheld-variable-prunes-its-dependents`) before the writer
runs. They stay as writer-boundary tests because the writer is the last guard on the
disclosure flags and on the catalog contract. The pipeline also computes every
classification link, conformance decision and sentinel certificate from the build it
writes, so the writer's cross-checks of them against the written books are defense in
depth; the written outcomes are the build cases
`classification-book-written-with-conformance-sentinels-and-successions` and
`representation-column-coding-and-books-stay-per-column`. Each one drives
`write_resolved_catalog`, the writer `build-db` calls: an unvalidated copy of a
resolved model reaches it, because the writer revalidates every instance it is given.

Publication, byte identity and create-only placement are pinned beside the build-db
contract, in `test_build_db_cli_outputs.py`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from _resolved_catalog_support import (
    classified_alias_variable as _classified_alias_variable,
    resolved_classification as _classification,
    resolved_state as _state,
    resolved_variable as _variable,
    scoped_sentinel_variable as _scoped_sentinel_variable,
)
from catalog_manifest import synthetic_manifest
from reg_meta_build.errors import RegMetaError
from reg_meta_build.resolved_catalog import (
    CURATION_TREE_SHA256_KEY,
    ResolvedAlias,
    ResolvedAliasWindow,
    ResolvedClassificationLink,
    ResolvedCodeSet,
    ResolvedConformance,
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


def _with_members(members: tuple) -> ResolvedVariable:
    value_set = ResolvedCodeSet(members=(("01", "Label"),))
    return _with_state(value_set=value_set.model_copy(update={"members": members}))


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
    # A value set is a nonempty set of (code, label) text pairs. Formation builds
    # only valid ones; a copy bypasses the constructor, so the writer's own
    # revalidation is the guard.
    **{
        f"value-set-{name}": (
            lambda members=members: (_with_members(members),),
            message,
        )
        for name, members, message in (
            ("empty", (), r"value_set\.members\n.*at least 1 item"),
            ("code-not-text", ((1, "Label"),), r"members\.0\.0\n.*valid string"),
            ("label-missing", (("01", None),), r"members\.0\.1\n.*valid string"),
            (
                "member-not-a-pair",
                (("01", "Label", "Extra"),),
                r"members\.0\n.*at most 2 items",
            ),
        )
    },
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


def _classified(
    variable: ResolvedVariable, book: Any
) -> tuple[tuple[ResolvedVariable, ...], dict[str, Any]]:
    return (variable,), {"classifications": (book,)}


def _linked_state(
    **update: object,
) -> tuple[tuple[ResolvedVariable, ...], dict[str, Any]]:
    return _classified(_with_state(**update), _classification())


def _conforming(*codes: str) -> tuple[ResolvedClassificationLink, ...]:
    book = _classification().slug
    conformance = ResolvedConformance(
        declared_classification=book, status="conforming", checked_codes=codes
    )
    return (ResolvedClassificationLink(classification=book, conformance=conformance),)


def _certified(
    certificate: dict[str, object] | None,
    *,
    recorded: bool = False,
    delivered: tuple[tuple[str, str], ...] = (),
) -> tuple[tuple[ResolvedVariable, ...], dict[str, Any]]:
    """The state certified to deliver sentinel 09350, its certificate changed.

    `certificate` updates the certificate (None drops it). `recorded` also records
    the changed members as the conformance's sentinels, so the model accepts them
    and only the state or the book disagrees. `delivered` adds value-set members.
    """
    variable, book = _scoped_sentinel_variable()
    state = variable.states[0]
    link = state.classification_links[0]
    conformance = link.conformance
    assert conformance is not None
    certificates = (
        ()
        if certificate is None
        else (conformance.scoped_sentinels[0].model_copy(update=certificate),)
    )
    sentinels = {"sentinel_members": certificates[0].members} if recorded else {}
    conformance = conformance.model_copy(
        update={"scoped_sentinels": certificates} | sentinels
    )
    update: dict[str, object] = {
        "classification_links": (link.model_copy(update={"conformance": conformance}),)
    }
    if delivered:
        assert state.value_set is not None
        members = (*state.value_set.members, *delivered)
        update["value_set"] = ResolvedCodeSet(members=members)
    states = (state.model_copy(update=update),)
    return _classified(variable.model_copy(update={"states": states}), book)


def _alias_window_certified_for_another_book() -> tuple[
    tuple[ResolvedVariable, ...], dict[str, Any]
]:
    # The link sits on a per-column alias window; the backing state has no value
    # set and no link, so only the window's own check can refuse it.
    variable, book = _classified_alias_variable()
    alias = variable.aliases[0]
    window = alias.windows[0]
    link = window.classification_links[0]
    conformance = link.conformance
    assert conformance is not None
    certificate = conformance.scoped_sentinels[0].model_copy(
        update={"classification_sha256": "b" * 64}
    )
    conformance = conformance.model_copy(update={"scoped_sentinels": (certificate,)})
    link = link.model_copy(update={"conformance": conformance})
    window = window.model_copy(update={"classification_links": (link,)})
    alias = alias.model_copy(update={"windows": (window,)})
    return _classified(variable.model_copy(update={"aliases": (alias,)}), book)


_CERTIFICATE_DISAGREES = (
    "scoped sentinel certificate disagrees with source state or canonical book"
)

# Classification links written beside their books: every link names a written book,
# a conformance decision checks every distinct code of the state's value set and
# matches the book, and a scoped sentinel certificate matches the book's fingerprint,
# the column, the state's window and the state's labels. The fixture book has codes
# 001 and 002 and no curated sentinels; the certified state delivers 001 and the
# sentinel 09350 "Okänt".
INVALID_CLASSIFIED_VARIABLES: dict[
    str, tuple[Callable[[], tuple[tuple[ResolvedVariable, ...], dict[str, Any]]], str]
] = {
    "classification-link-to-an-unwritten-book": (
        lambda: _linked_state(
            classification_links=(ResolvedClassificationLink(classification="missing"),)
        ),
        "unknown classification reference: missing",
    ),
    "conformance-conforming-with-a-code-outside-the-book": (
        lambda: _linked_state(
            value_set=ResolvedCodeSet(members=(("999", "Missing"),)),
            classification_links=_conforming("999"),
        ),
        "conformance disagrees with canonical code membership",
    ),
    # The blank code is falsy: a partition check that skips falsy codes passes it.
    "conformance-leaves-the-blank-code-unchecked": (
        lambda: _linked_state(
            value_set=ResolvedCodeSet(
                members=(("", "Source missing"), ("001", "Source label"))
            ),
            classification_links=_conforming("001"),
        ),
        "conformance must check every distinct code in the value set",
    ),
    # Without its certificate the sentinel is an uncertified extension, so the
    # recorded conformance no longer matches the book.
    "scoped-sentinel-without-a-certificate": (
        lambda: _certified(None),
        "conformance disagrees with canonical code membership",
    ),
    "scoped-sentinel-certificate-for-another-book": (
        lambda: _certified({"classification_sha256": "b" * 64}),
        _CERTIFICATE_DISAGREES,
    ),
    "scoped-sentinel-certificate-for-another-column": (
        lambda: _certified({"delivery_column_name": "Other"}),
        _CERTIFICATE_DISAGREES,
    ),
    "scoped-sentinel-certificate-not-covering-the-state": (
        lambda: _certified({"valid_to": "2000-06-30"}),
        _CERTIFICATE_DISAGREES,
    ),
    "scoped-sentinel-certificate-member-not-recorded": (
        lambda: _certified({"members": (("09350", "Different meaning"),)}),
        "scoped sentinel certificate members must be recorded sentinels",
    ),
    "scoped-sentinel-certificate-for-a-canonical-code": (
        lambda: _certified({"members": (("001", "Source label"),)}, recorded=True),
        _CERTIFICATE_DISAGREES,
    ),
    # Distinct labels for one code stay distinct: a certificate for 09350 "Okänt"
    # does not cover the state's 09350 under another label.
    "scoped-sentinel-certificate-misses-a-label-of-its-code": (
        lambda: _certified({}, delivered=(("09350", "Different meaning"),)),
        _CERTIFICATE_DISAGREES,
    ),
    "scoped-sentinel-certificate-on-an-alias-window-for-another-book": (
        _alias_window_certified_for_another_book,
        _CERTIFICATE_DISAGREES,
    ),
}


@pytest.mark.parametrize(
    "row", [*INVALID_VARIABLES, *INVALID_ARGUMENTS, *INVALID_CLASSIFIED_VARIABLES]
)
def test_invalid_input_is_refused_before_the_previous_catalog_is_touched(
    tmp_path: Path, row: str
) -> None:
    # Fails if the writer drops the rule the row names (a lax or optional flag, a
    # date, slug, overlap, consistency or manifest check, a classification link or
    # sentinel certificate check), or runs it only after it has replaced the
    # previous catalog, linked it aside to `.prev` or left staging behind.
    arguments: dict[str, Any] = {"manifest": synthetic_manifest()}
    if row in INVALID_VARIABLES:
        variables, message = INVALID_VARIABLES[row]
        given = variables()
    elif row in INVALID_ARGUMENTS:
        make, message = INVALID_ARGUMENTS[row]
        arguments, given = arguments | make(), (_variable(),)
    else:
        classified, message = INVALID_CLASSIFIED_VARIABLES[row]
        given, books = classified()
        arguments |= books
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
