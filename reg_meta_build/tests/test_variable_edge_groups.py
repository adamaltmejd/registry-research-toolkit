"""Month groups, code/label pairs and checked sibling edges resolve into variable groups before writing."""

from contextlib import closing

import pytest
from _catalog_dependency_support import cause as _cause, variable as _variable
from catalog_manifest import synthetic_manifest
from reg_meta_build.catalog_dependencies import (
    DEFERRED_REFERENCE,
    CatalogDependencyError,
    resolve_month_groups,
    resolve_variable_edge_groups,
)
from reg_meta_build.concept_groups import CodeLabelPair
from reg_meta_build.db import open_built_db
from reg_meta_build.resolved_catalog import (
    ResolvedCodeSet,
    ResolvedRegister,
    ResolvedVariant,
    write_resolved_catalog,
)
from reg_meta_build.resolved_metadata import (
    ResolvedGroupVariable,
    ResolvedMetadata,
    ResolvedVariableGroup,
)
from reg_meta_build.source_curation import SourceRecordRef


def _pair(code="code", label="label"):
    return CodeLabelPair("scb", "example", code, "scb", "example", label)


def _pair_variables():
    variant = ResolvedVariant(slug="people", name="People")
    code = _variable(variant, "code")
    code = code.model_copy(
        update={
            "states": (
                code.states[0].model_copy(
                    update={"value_set": ResolvedCodeSet(members=(("01", "Name"),))}
                ),
            )
        }
    )
    return code, _variable(variant, "label")


def _pair_evidence(variables):
    return {
        f"{v.register_ref.provider}/{v.register_ref.slug}/{v.slug}": (
            SourceRecordRef(source="fixture", semantic_record_key=(v.slug,)),
        )
        for v in variables
    }


def _month_variables(stem="ink"):
    variant = ResolvedVariant(slug="people", name="People")
    return tuple(
        _variable(variant, stem + token).model_copy(
            update={"name": f"Inkomst i {token}, totalt"}
        )
        for token in ("jan", "februari", "mars")
    )


def _month_resolution(variables, **kwargs):
    return resolve_month_groups(
        variables,
        **(
            {
                "edge_groups": (),
                "curated_groups": (),
                "evidence": _pair_evidence(variables),
            }
            | kwargs
        ),
    )


def test_a_scoped_build_skips_only_curation_wholly_outside_the_slice():
    variables = _pair_variables()

    def resolve(code, label):
        return resolve_variable_edge_groups(
            (CodeLabelPair(*code.split("/"), *label.split("/")),),
            variables,
            curated_groups=(),
            evidence=_pair_evidence(variables),
            withheld={},
            unselected={("register", "scb/other"), ("variable", "scb/other/label")},
            slice_registers={"scb/example"},
        )

    # Ends in an unselected register or one no scope declares, one of them
    # undeclared: not the slice's to prove, so skipped without a diagnostic.
    for code in ("scb/other/label", "scb/nosuch/code"):
        outside = resolve(code, "scb/other/no-such")
        assert (outside.skipped, outside.diagnostics) == (1, ())
        assert [d.status for d in outside.dispositions] == ["withheld"]
    # Beside the slice, a declared out-of-slice end is deferred ...
    deferred = resolve("scb/example/code", "scb/other/label")
    assert deferred.skipped == 0
    assert [d.code for d in deferred.diagnostics] == [DEFERRED_REFERENCE]
    # ... and an undeclared one stays fatal.
    with pytest.raises(CatalogDependencyError) as error:
        resolve("scb/example/code", "scb/other/no-such")
    assert [m.key for m in error.value.missing] == [("variable", "scb/other/no-such")]


def test_month_groups_resolve_before_writing_with_ordered_facets(tmp_path):
    variables = _month_variables("ink-")
    result = _month_resolution(variables)
    assert not result.diagnostics
    (group,) = result.groups
    assert (group.key, group.label, group.source) == ("ink", "Inkomst", "token")
    assert [m.facets[0].value for m in group.members] == ["01", "02", "03"]
    assert result == _month_resolution(tuple(reversed(variables)))
    path = write_resolved_catalog(
        variables,
        tmp_path / "diagnostic.db",
        manifest=synthetic_manifest(),
        diagnostic=True,
        metadata=ResolvedMetadata(variable_groups=result.groups),
    )
    with closing(open_built_db(path)) as conn:
        assert tuple(
            conn.execute("SELECT group_key,label,source FROM concept_group").fetchone()
        ) == ("ink", "Inkomst", "token")
        assert [
            tuple(r)
            for r in conn.execute("SELECT axis,ordinal,label FROM concept_group_axis")
        ] == [("month", 0, "månad")]
        assert [
            tuple(r)
            for r in conn.execute(
                "SELECT axis,value,label FROM concept_group_variable_facet ORDER BY value"
            )
        ] == [
            ("month", "01", "januari"),
            ("month", "02", "februari"),
            ("month", "03", "mars"),
        ]


def test_month_group_threshold_uses_unclaimed_members_in_one_register():
    variables = _month_variables()
    peer = _variable(variables[0].states[0].variant, "other")
    edge = ResolvedVariableGroup(
        register="scb/example",
        key="edge",
        label="Edge",
        source="edge",
        members=tuple(
            ResolvedGroupVariable(variable="scb/example/" + v.slug)
            for v in (variables[0], peer)
        ),
    )
    assert not _month_resolution((*variables, peer), edge_groups=(edge,)).groups
    different_register = variables[-1].model_copy(
        update={
            "register_ref": ResolvedRegister(
                provider="scb", slug="elsewhere", name="Elsewhere"
            )
        }
    )
    assert not _month_resolution((*variables[:2], different_register)).groups


@pytest.mark.parametrize("collision", ["stem_collision", "reserved_key"])
def test_month_group_collision_keeps_exact_source_evidence(collision):
    variables = _month_variables("ink-")
    curated = ()
    if collision == "stem_collision":
        variables += _month_variables()
    else:
        curated = (
            ResolvedVariableGroup(
                register="scb/example",
                key="ink",
                label="Reserved",
                source="curated",
                members=(
                    ResolvedGroupVariable(variable="scb/example/other"),
                    ResolvedGroupVariable(variable="scb/example/another"),
                ),
            ),
        )
    result = _month_resolution(variables, curated_groups=curated)
    assert not result.groups
    (issue,) = result.diagnostics
    assert issue.code == "month_group_" + collision
    assert issue.severity == "warning"
    assert issue.withheld_output == ("variable_group:scb/example/ink",)
    assert set(issue.refs) == {
        r for refs in _pair_evidence(variables).values() for r in refs
    }


def test_month_group_does_not_silently_replace_curated_membership():
    variables = _month_variables()
    curated = ResolvedVariableGroup(
        register="scb/example",
        key="other",
        label="Other",
        source="curated",
        members=(
            ResolvedGroupVariable(variable="scb/example/inkjan"),
            ResolvedGroupVariable(variable="scb/example/another"),
        ),
    )
    with pytest.raises(ValueError, match="multiple resolved groups"):
        _month_resolution(variables, curated_groups=(curated,))


def test_month_group_requires_evidence_and_valid_input_references():
    variables = _month_variables()
    with pytest.raises(ValueError, match="lacks source evidence"):
        _month_resolution(variables, evidence={})
    with pytest.raises(ValueError, match="duplicate variable identity"):
        _month_resolution((*variables, variables[0]))
    missing_edge = ResolvedVariableGroup(
        register="scb/example",
        key="edge",
        label="Edge",
        source="edge",
        members=(
            ResolvedGroupVariable(variable="scb/example/inkjan"),
            ResolvedGroupVariable(variable="scb/example/missing"),
        ),
    )
    with pytest.raises(ValueError, match="missing variables"):
        _month_resolution(variables, edge_groups=(missing_edge,))


def test_code_label_pair_resolves_without_sql_and_writes_standard_group(tmp_path):
    variables = _pair_variables()
    result = resolve_variable_edge_groups(
        (_pair(),),
        variables,
        curated_groups=(),
        evidence=_pair_evidence(variables),
        withheld={},
    )
    assert not result.diagnostics
    assert [(d.status, d.group_key) for d in result.dispositions] == [
        ("grouped", "code")
    ]
    assert [m.variable for m in result.groups[0].members] == [
        "scb/example/code",
        "scb/example/label",
    ]
    path = write_resolved_catalog(
        variables,
        tmp_path / "diagnostic.db",
        manifest=synthetic_manifest(),
        diagnostic=True,
        metadata=ResolvedMetadata(variable_groups=result.groups),
    )
    with closing(open_built_db(path)) as conn:
        assert conn.execute("SELECT source FROM concept_group").fetchone()[0] == "edge"
        assert (
            conn.execute("SELECT COUNT(*) FROM concept_group_variable").fetchone()[0]
            == 2
        )


def test_checked_siblings_and_code_label_pairs_form_one_component():
    code, label = _pair_variables()
    sibling = _variable(ResolvedVariant(slug="earlier", name="Earlier"), "sibling")
    variables = (code, label, sibling)
    kwargs = {
        "curated_groups": (),
        "evidence": _pair_evidence(variables),
        "withheld": {},
        "foldable_sibling_pairs": (("scb/example/code", "scb/example/sibling"),),
    }
    result = resolve_variable_edge_groups((_pair(),), variables, **kwargs)
    assert not result.diagnostics and len(result.groups) == 1
    assert [m.variable for m in result.groups[0].members] == [
        "scb/example/code",
        "scb/example/label",
        "scb/example/sibling",
    ]
    assert {(d.source, d.status, d.group_key) for d in result.dispositions} == {
        ("code_label", "grouped", "code"),
        ("same_definition", "grouped", "code"),
    }
    assert (
        result.groups
        == resolve_variable_edge_groups(
            (_pair(),), tuple(reversed(variables)), **kwargs
        ).groups
    )


def test_curated_bridge_is_removed_before_combining_sibling_and_pair_edges():
    code, label = _pair_variables()
    variants = ResolvedVariant(slug="people", name="People")
    sibling, other = (_variable(variants, slug) for slug in ("sibling", "other"))
    variables = (code, label, sibling, other)
    curated = ResolvedVariableGroup(
        register="scb/example",
        key="curated",
        label="Curated",
        source="curated",
        members=(
            ResolvedGroupVariable(variable="scb/example/label"),
            ResolvedGroupVariable(variable="scb/example/other"),
        ),
    )
    result = resolve_variable_edge_groups(
        (_pair(),),
        variables,
        foldable_sibling_pairs=(("scb/example/label", "scb/example/sibling"),),
        curated_groups=(curated,),
        evidence=_pair_evidence(variables),
        withheld={},
    )
    assert not result.groups and not result.diagnostics
    assert all(d.status == "claimed_by_curated" for d in result.dispositions)


def test_sibling_omission_requires_evidence_and_unexplained_endpoint_stays_fatal():
    variables = _pair_variables()
    kwargs = {
        "curated_groups": (),
        "evidence": _pair_evidence(variables),
        "withheld": {("variable", "scb/example/absent"): (_cause(),)},
    }
    result = resolve_variable_edge_groups(
        (),
        variables,
        foldable_sibling_pairs=(("scb/example/code", "scb/example/absent"),),
        **kwargs,
    )
    assert not result.groups and result.dispositions[0].status == "withheld"
    assert result.diagnostics[0].refs == _cause().refs
    with pytest.raises(CatalogDependencyError) as error:
        resolve_variable_edge_groups(
            (),
            variables,
            foldable_sibling_pairs=(("scb/example/absent", "scb/example/unknown"),),
            **kwargs,
        )
    assert [d.key for d in error.value.missing] == [("variable", "scb/example/unknown")]


@pytest.mark.parametrize(
    "failure", ["self", "duplicate", "cross_register", "no_evidence"]
)
def test_sibling_edges_cannot_bypass_declaration_or_source_guards(failure):
    code, label = _pair_variables()
    if failure == "cross_register":
        label = label.model_copy(
            update={
                "register_ref": ResolvedRegister(
                    provider="scb", slug="other", name="Other"
                )
            }
        )
    variables = (code, label)
    a, b = "scb/example/code", f"scb/{label.register_ref.slug}/label"
    evidence = _pair_evidence(variables) if failure != "no_evidence" else {}
    pairs = (
        ((a, a),)
        if failure == "self"
        else ((a, b), (b, a))
        if failure == "duplicate"
        else ((a, b),)
    )
    if failure == "cross_register":
        result = resolve_variable_edge_groups(
            (),
            variables,
            foldable_sibling_pairs=pairs,
            curated_groups=(),
            evidence=evidence,
            withheld={},
        )
        assert (
            not result.groups
            and result.diagnostics[0].code == "unresolved_same_definition_pair"
        )
        assert set(result.diagnostics[0].refs) == {*evidence[a], *evidence[b]}
    else:
        with pytest.raises(
            ValueError, match="unique non-self pairs|lacks source evidence"
        ):
            resolve_variable_edge_groups(
                (),
                variables,
                foldable_sibling_pairs=pairs,
                curated_groups=(),
                evidence=evidence,
                withheld={},
            )


@pytest.mark.parametrize(
    "change", ["uncoded", "coded_label", "other_variant", "other_register"]
)
def test_code_label_pair_failed_guards_retain_evidence_and_withhold_only_group(change):
    code, label = _pair_variables()
    pair = _pair()
    if change == "uncoded":
        code = code.model_copy(
            update={"states": (code.states[0].model_copy(update={"value_set": None}),)}
        )
    elif change == "coded_label":
        label = label.model_copy(
            update={
                "states": (
                    label.states[0].model_copy(
                        update={"value_set": code.states[0].value_set}
                    ),
                )
            }
        )
    elif change == "other_variant":
        label = label.model_copy(
            update={
                "states": (
                    label.states[0].model_copy(
                        update={"variant": ResolvedVariant(slug="other", name="Other")}
                    ),
                )
            }
        )
    else:
        label = label.model_copy(
            update={
                "register_ref": ResolvedRegister(
                    provider="scb", slug="other", name="Other"
                )
            }
        )
        pair = CodeLabelPair("scb", "example", "code", "scb", "other", "label")
    evidence = _pair_evidence((code, label))
    result = resolve_variable_edge_groups(
        (pair,),
        (code, label),
        curated_groups=(),
        evidence=evidence,
        withheld={},
    )
    assert not result.groups and result.dispositions[0].status == "withheld"
    (issue,) = result.diagnostics
    assert (issue.code, issue.severity) == ("unresolved_code_label_pair", "error")
    assert issue.refs == tuple(ref for refs in evidence.values() for ref in refs)
    assert issue.withheld_output == (issue.subject,)


def test_code_label_pair_known_omission_does_not_hide_unknown_reference():
    code, _ = _pair_variables()
    result = resolve_variable_edge_groups(
        (_pair(),),
        (code,),
        curated_groups=(),
        evidence=_pair_evidence((code,)),
        withheld={("variable", "scb/example/label"): (_cause(),)},
    )
    assert not result.groups and result.dispositions[0].status == "withheld"
    assert result.diagnostics[0].refs == _cause().refs
    with pytest.raises(CatalogDependencyError, match="typo"):
        resolve_variable_edge_groups(
            (_pair("typo"),),
            (),
            curated_groups=(),
            evidence={},
            withheld={("variable", "scb/example/label"): (_cause(),)},
        )
    with pytest.raises(ValueError, match="unique non-self"):
        resolve_variable_edge_groups(
            (_pair(), _pair()),
            (),
            curated_groups=(),
            evidence={},
            withheld={},
        )
    with pytest.raises(ValueError, match="lacks source evidence"):
        resolve_variable_edge_groups(
            (_pair(),),
            _pair_variables(),
            curated_groups=(),
            evidence={},
            withheld={},
        )


def test_code_label_curated_bridge_is_excluded_before_components_form():
    code, label = _pair_variables()
    variables = tuple(
        code.model_copy(update={"slug": s, "provider_key": s}) for s in ("a", "b")
    ) + tuple(
        label.model_copy(update={"slug": s, "provider_key": s})
        for s in ("bridge", "left", "right", "extra")
    )
    curated = ResolvedVariableGroup(
        register="scb/example",
        key="curated",
        label="Curated",
        source="curated",
        members=tuple(
            ResolvedGroupVariable(variable=f"scb/example/{s}")
            for s in ("bridge", "extra")
        ),
    )
    pairs = (
        _pair("a", "bridge"),
        _pair("b", "bridge"),
        _pair("a", "left"),
        _pair("b", "right"),
    )
    result = resolve_variable_edge_groups(
        pairs,
        variables,
        curated_groups=(curated,),
        evidence=_pair_evidence(variables),
        withheld={},
    )
    assert [d.status for d in result.dispositions] == [
        "claimed_by_curated",
        "claimed_by_curated",
        "grouped",
        "grouped",
    ]
    assert [
        [m.variable.rsplit("/", 1)[1] for m in g.members] for g in result.groups
    ] == [["a", "left"], ["b", "right"]]
    assert (
        resolve_variable_edge_groups(
            pairs[::-1],
            variables[::-1],
            curated_groups=(curated,),
            evidence=_pair_evidence(variables),
            withheld={},
        ).groups
        == result.groups
    )
