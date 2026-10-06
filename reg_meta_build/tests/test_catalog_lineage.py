"""Source claims and accepted identity are separate requirements for lineage."""

import pytest
from _catalog_dependency_support import variable as _variable
from catalog_manifest import synthetic_manifest
from reg_meta_build.catalog_dependencies import CatalogDependencyError
from reg_meta_build.catalog_lineage import resolve_catalog_lineage
from reg_meta_build.resolved_catalog import ResolvedRegister, ResolvedVariant
from reg_meta_build.resolved_metadata import ResolvedMetadata, ResolvedVariableSameAs
from reg_meta_build.source_curation import SourceRecordRef


def fixture(
    *,
    same_as=True,
    duplicate_name=False,
    shared_abbreviation=False,
    second_variant=False,
    source_label="Original register (ORIG) : People",
):
    consumer = _variable(ResolvedVariant(slug="people", name="People"))
    origin = ResolvedRegister(
        provider="scb", slug="origin", name="Original register (ORIG)"
    )
    source = consumer.model_copy(update={"register_ref": origin})
    consumer = consumer.model_copy(
        update={
            "source_register_text": source_label,
            "states": tuple(
                s.model_copy(update={"source_register_text": source_label})
                for s in consumer.states
            ),
        }
    )
    if second_variant:
        source = source.model_copy(
            update={
                "states": (
                    *source.states,
                    source.states[0].model_copy(
                        update={"variant": ResolvedVariant(slug="other", name="Other")}
                    ),
                )
            }
        )
    registers = (consumer.register_ref, origin)
    if duplicate_name:
        registers += (origin.model_copy(update={"slug": "duplicate"}),)
    if shared_abbreviation:
        registers += (
            origin.model_copy(
                update={"slug": "other", "name": "Other register (ORIG)"}
            ),
        )
    metadata = ResolvedMetadata(
        variable_same_as=(
            ResolvedVariableSameAs(a="scb/example/value", b="scb/origin/value"),
        )
        if same_as
        else ()
    )
    return (consumer, source), {
        "registers": registers,
        "variants": tuple(
            (v.register_ref, s.variant) for v in (consumer, source) for s in v.states
        ),
        "defaults": {},
        "metadata": metadata,
        "evidence": {
            key: (SourceRecordRef(source="fixture", semantic_record_key=(key,)),)
            for key in ("scb/example/value", "scb/origin/value")
        },
        "withheld": {},
    }


def test_explicit_identity_and_source_claim_generate_intersection_edges():
    variables, options = fixture()
    result = resolve_catalog_lineage(variables, **options)
    assert not result.diagnostics
    assert result.variables[0].source_register.slug == "origin"
    assert result.variables[0].source_label == "ORIG"
    assert len(result.metadata.state_lineage) == 1
    edge = result.metadata.state_lineage[0]
    assert (edge.valid_from, edge.valid_to) == ("2000-01-01", "2000-12-31")
    assert resolve_catalog_lineage(tuple(reversed(variables)), **options) == result


def test_matching_slugs_are_not_identity_evidence():
    variables, options = fixture(same_as=False)
    result = resolve_catalog_lineage(variables, **options)
    assert not result.metadata.state_lineage
    assert result.diagnostics[0].code == "unresolved_lineage_no_source_state"
    assert result.diagnostics[0].severity == "warning"
    assert result.diagnostics[0].withheld_output == ("scb/example/value:lineage",)
    assert result.metadata.lineage_warnings[0].kind == "no_source_state"


def test_duplicate_source_variant_label_without_variable_identity_stays_a_gap():
    variables, options = fixture(same_as=False, second_variant=True)
    options["variants"] += (
        (
            variables[1].register_ref,
            ResolvedVariant(slug="duplicate", name="People"),
        ),
    )
    options["defaults"] = {"scb/origin": "people"}
    result = resolve_catalog_lineage(variables, **options)
    assert not result.metadata.state_lineage
    assert (
        result.variables[0].source_register_text == "Original register (ORIG) : People"
    )
    assert [d.code for d in result.diagnostics] == [
        "unresolved_lineage_no_source_state"
    ]
    assert result.diagnostics[0].severity == "warning"
    assert result.diagnostics[0].refs == options["evidence"]["scb/example/value"]
    assert "No source variable identity is established" in result.diagnostics[0].detail
    assert "matches multiple admitted source variants" in result.diagnostics[0].detail
    assert (
        "no default or other variant has been substituted"
        in result.diagnostics[0].detail
    )
    assert result.metadata.lineage_warnings[0].kind == "no_source_state"
    assert resolve_catalog_lineage(tuple(reversed(variables)), **options) == result


def test_ambiguous_register_label_does_not_pick_input_order():
    variables, options = fixture(duplicate_name=True)
    result = resolve_catalog_lineage(variables, **options)
    assert result.variables[0].source_register is None
    assert not result.metadata.state_lineage
    assert result.diagnostics[0].code == "ambiguous_source_register"
    assert result.diagnostics[0].severity == "error"


@pytest.mark.parametrize(
    "source_label",
    ("Original register (ORIG)", "Original register (ORIG) : People"),
)
def test_specific_register_name_wins_over_shared_abbreviation(source_label):
    variables, options = fixture(shared_abbreviation=True, source_label=source_label)
    result = resolve_catalog_lineage(variables, **options)
    assert not result.diagnostics
    assert result.variables[0].source_register.slug == "origin"
    assert len(result.metadata.state_lineage) == 1


def test_curated_register_source_label_resolves_shared_abbreviation_by_prefix():
    variables, options = fixture(
        shared_abbreviation=True,
        source_label="Former source (ORIG) : People",
    )
    ambiguous = resolve_catalog_lineage(variables, **options)
    assert ambiguous.variables[0].source_register is None
    assert [issue.code for issue in ambiguous.diagnostics] == [
        "ambiguous_source_register"
    ]

    options["source_labels"] = {"scb/origin": ("Former source (ORIG)",)}
    result = resolve_catalog_lineage(variables, **options)
    assert not result.diagnostics
    assert result.variables[0].source_register.slug == "origin"
    assert len(result.metadata.state_lineage) == 1


def test_abbreviation_resolves_when_full_name_and_prefix_do_not_match():
    variables, options = fixture(source_label="External source (ORIG) : People")
    result = resolve_catalog_lineage(variables, **options)
    assert not result.diagnostics
    assert result.variables[0].source_register.slug == "origin"
    assert len(result.metadata.state_lineage) == 1


def test_unknown_source_label_remains_unresolved():
    source_label = "External source (MISSING) : People"
    variables, options = fixture(source_label=source_label)
    result = resolve_catalog_lineage(variables, **options)
    assert not result.diagnostics
    assert result.variables[0].source_register is None
    assert result.variables[0].source_label == source_label
    assert not result.metadata.state_lineage


def test_variant_default_resolves_ambiguity_and_unused_dangling_pin_fails():
    variables, options = fixture(
        second_variant=True, source_label="Original register (ORIG)"
    )
    result = resolve_catalog_lineage(variables, **options)
    assert not result.metadata.state_lineage
    assert result.diagnostics[0].code == "unresolved_lineage_ambiguous_source_variant"
    assert result.diagnostics[0].severity == "error"
    options["defaults"] = {"scb/origin": "people"}
    resolved = resolve_catalog_lineage(variables, **options)
    assert len(resolved.metadata.state_lineage) == 1 and not resolved.diagnostics
    options["defaults"] = {"scb/missing": "people"}
    with pytest.raises(CatalogDependencyError):
        resolve_catalog_lineage(variables, **options)


def test_explicit_source_variant_selects_its_states_over_a_different_default():
    variables, options = fixture(second_variant=True)
    options["defaults"] = {"scb/origin": "other"}
    result = resolve_catalog_lineage(variables, **options)
    assert not result.diagnostics
    (edge,) = result.metadata.state_lineage
    assert edge.source.variant == "people"
    assert edge.consumer.variable == "scb/example/value"
    assert edge.source.variable == "scb/origin/value"
    assert resolve_catalog_lineage(tuple(reversed(variables)), **options) == result


def test_explicit_source_variant_without_identity_linked_states_stays_unavailable():
    label = "Original register (ORIG) : Missing delivery"
    variables, options = fixture(second_variant=True, source_label=label)
    options["variants"] += (
        (
            variables[1].register_ref,
            ResolvedVariant(slug="missing", name="Missing delivery"),
        ),
    )
    options["defaults"] = {"scb/origin": "people"}
    result = resolve_catalog_lineage(variables, **options)
    assert not result.metadata.state_lineage
    assert result.variables[0].source_register_text == label
    assert [d.code for d in result.diagnostics] == [
        "unresolved_lineage_no_source_state"
    ]
    assert result.diagnostics[0].severity == "warning"
    assert "'Missing delivery'" in result.diagnostics[0].detail


@pytest.mark.parametrize("suffix", ["Unknown delivery", "", "People"])
def test_unresolved_explicit_variant_cannot_fall_back_to_a_default(suffix):
    variables, options = fixture(
        second_variant=True, source_label=f"Original register (ORIG) : {suffix}"
    )
    if suffix == "People":
        options["variants"] += (
            (
                variables[1].register_ref,
                ResolvedVariant(slug="duplicate", name="People"),
            ),
        )
    options["defaults"] = {"scb/origin": "people"}
    result = resolve_catalog_lineage(variables, **options)
    assert not result.metadata.state_lineage
    assert [d.code for d in result.diagnostics] == [
        "unresolved_lineage_ambiguous_source_variant"
        if suffix == "People"
        else "unresolved_lineage_no_source_state"
    ]
    assert result.diagnostics[0].severity == (
        "error" if suffix == "People" else "warning"
    )
    assert result.diagnostics[0].refs == options["evidence"]["scb/example/value"]
    assert "no default or other variant has been substituted" in (
        result.diagnostics[0].detail
    )


def test_disjoint_validity_keeps_identity_without_inventing_edges_or_errors():
    variables, options = fixture()
    consumer, source = variables
    source = source.model_copy(
        update={
            "states": tuple(
                s.model_copy(
                    update={"valid_from": "2001-01-01", "valid_to": "2001-12-31"}
                )
                for s in source.states
            )
        }
    )
    result = resolve_catalog_lineage((consumer, source), **options)
    assert not result.metadata.state_lineage and not result.diagnostics


def test_historical_source_variant_does_not_make_current_lineage_ambiguous():
    variables, options = fixture(
        second_variant=True, source_label="Original register (ORIG)"
    )
    consumer, source = variables
    current, historical = source.states
    historical = historical.model_copy(
        update={"valid_from": "1999-01-01", "valid_to": "1999-12-31"}
    )
    source = source.model_copy(update={"states": (current, historical)})
    result = resolve_catalog_lineage((consumer, source), **options)
    assert not result.diagnostics
    (edge,) = result.metadata.state_lineage
    assert edge.source.variant == "people"
    assert (edge.valid_from, edge.valid_to) == ("2000-01-01", "2000-12-31")


def test_multiple_disjoint_source_variants_preserve_attribution_without_edges():
    variables, options = fixture(
        second_variant=True, source_label="Original register (ORIG)"
    )
    consumer, source = variables
    source = source.model_copy(
        update={
            "states": tuple(
                state.model_copy(
                    update={"valid_from": "1999-01-01", "valid_to": "1999-12-31"}
                )
                for state in source.states
            )
        }
    )
    result = resolve_catalog_lineage((consumer, source), **options)
    assert not result.diagnostics and not result.metadata.state_lineage
    assert result.variables[0].source_register.slug == "origin"


def test_independent_source_variant_cannot_be_filtered_as_disjoint():
    variables, options = fixture(
        second_variant=True, source_label="Original register (ORIG)"
    )
    consumer, source = variables
    dated, independent = source.states
    independent = independent.model_copy(
        update={
            "period_scope": "year_independent",
            "valid_from": None,
            "valid_to": None,
        }
    )
    source = source.model_copy(update={"states": (dated, independent)})
    result = resolve_catalog_lineage((consumer, source), **options)
    assert not result.metadata.state_lineage
    assert [issue.code for issue in result.diagnostics] == [
        "unresolved_lineage_ambiguous_source_variant"
    ]


@pytest.mark.parametrize("independent_endpoint", [0, 1])
def test_independent_source_attribution_never_invents_dated_lineage(
    independent_endpoint,
):
    variables, options = fixture()
    endpoint = variables[independent_endpoint]
    independent = endpoint.model_copy(
        update={
            "states": tuple(
                state.model_copy(
                    update={
                        "period_scope": "year_independent",
                        "valid_from": None,
                        "valid_to": None,
                    }
                )
                for state in endpoint.states
            )
        }
    )
    variables = tuple(
        independent if index == independent_endpoint else variable
        for index, variable in enumerate(variables)
    )
    result = resolve_catalog_lineage(variables, **options)
    assert not result.metadata.state_lineage
    assert not result.metadata.lineage_warnings
    assert result.variables[0].source_register.slug == "origin"
    assert [issue.code for issue in result.diagnostics] == ["unsupported_lineage_scope"]


def test_independent_register_only_attribution_retains_missing_endpoint_warning():
    variables, options = fixture(same_as=False)
    consumer = variables[0].model_copy(
        update={
            "states": tuple(
                state.model_copy(
                    update={
                        "period_scope": "year_independent",
                        "valid_from": None,
                        "valid_to": None,
                    }
                )
                for state in variables[0].states
            ),
        }
    )
    result = resolve_catalog_lineage((consumer, variables[1]), **options)
    assert not result.metadata.state_lineage
    assert result.variables[0].source_register.slug == "origin"
    assert result.variables[0].source_register_text == consumer.source_register_text
    assert [issue.code for issue in result.diagnostics] == [
        "unresolved_lineage_no_source_state"
    ]
    assert result.diagnostics[0].severity == "warning"
    (warning,) = result.metadata.lineage_warnings
    assert warning.kind == "no_source_state"
    assert warning.consumer.period_scope == "year_independent"
    assert warning.consumer.valid_from is warning.consumer.valid_to is None


def _guarded_ambiguous_lineage():
    from reg_meta.source_evidence import canonical_sha256
    from reg_meta_build.catalog_lineage import lineage_acknowledgement_sha256
    from reg_meta_build.source_coordinates import source_register_key
    from reg_meta_build.source_curation import AcknowledgeDecision, CurationCase
    from test_source_scope import record

    variables, options = fixture(second_variant=True)
    options["variants"] += (
        (variables[1].register_ref, ResolvedVariant(slug="duplicate", name="People")),
    )
    result = resolve_catalog_lineage(variables, **options)
    (issue,) = result.diagnostics
    original = record(1, 5, 2)
    case = CurationCase(
        case_id="registers/scb/example.toml#/acknowledge/1",
        targets=(),
        decision=AcknowledgeDecision(
            code=issue.code,
            subject=issue.subject,
            refs=issue.refs,
            valid_from=issue.valid_from,
            valid_to=issue.valid_to,
            register_key=source_register_key(original),
            reason="The literal source variant label identifies multiple admitted deliveries.",
            evidence="Exact source declaration and complete candidate closure reviewed.",
            expected_diagnostic_sha256=canonical_sha256(issue.model_dump(mode="json")),
            expected_evidence_sha256=lineage_acknowledgement_sha256(
                (original,),
                variables[0],
                (variables[1],),
                tuple(
                    (r, v)
                    for r, v in options["variants"]
                    if r == variables[1].register_ref and v.name == "People"
                ),
                options["metadata"],
            ),
        ),
    )
    options.update(
        acknowledgements=(case,), acknowledgement_originals={issue.refs[0]: (original,)}
    )
    return variables, options, case


def test_exact_lineage_acknowledgement_keeps_source_text_and_persists_warning(tmp_path):
    import json
    import sqlite3

    from reg_meta_build.data_warnings import acknowledged_data_warnings
    from reg_meta_build.resolved_catalog import write_resolved_catalog

    variables, options, case = _guarded_ambiguous_lineage()
    result = resolve_catalog_lineage(variables, **options)
    (issue,) = result.diagnostics
    assert issue.severity == "warning" and issue.acknowledged_by == case.case_id
    assert not result.metadata.state_lineage
    assert result.metadata.lineage_warnings[0].kind == "ambiguous_source_variant"
    assert [v.source_register_text for v in result.variables] == [
        v.source_register_text for v in variables
    ]
    (warning,) = acknowledged_data_warnings(
        result.diagnostics,
        (case,),
        {case.decision.register_key: variables[0].register_ref},
    )
    output = tmp_path / "lineage.db"
    write_resolved_catalog(
        result.variables,
        output,
        manifest=synthetic_manifest(),
        metadata=result.metadata,
        data_warnings=(warning,),
    )
    with sqlite3.connect(f"file:{output}?mode=ro", uri=True) as conn:
        assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
        (stored,) = conn.execute("SELECT warning_json FROM data_warning").fetchone()
        assert json.loads(stored) == json.loads(warning.model_dump_json())
        assert conn.execute(
            "SELECT COUNT(*) FROM variable_state_lineage_warning"
        ).fetchone() == (1,)


@pytest.mark.parametrize(
    "change", ["original", "new_candidate", "changed_candidate", "identity_edge"]
)
def test_lineage_acknowledgement_refuses_changed_source_or_candidate_distinction(
    change,
):
    from reg_meta_build.resolved_metadata import ResolvedVariableSameAs

    variables, options, case = _guarded_ambiguous_lineage()
    if change == "original":
        ref = case.decision.refs[0]
        options["acknowledgement_originals"][ref] = (
            options["acknowledgement_originals"][ref][0].model_copy(
                update={"original_period_text": "Changed"}
            ),
        )
    elif change == "new_candidate":
        options["variants"] += (
            (variables[1].register_ref, ResolvedVariant(slug="another", name="People")),
        )
    elif change == "changed_candidate":
        source = variables[1].model_copy(
            update={"definition": "Changed source candidate"}
        )
        variables = (variables[0], source)
    else:
        other = variables[1].model_copy(update={"slug": "new-source"})
        variables = (*variables, other)
        options["metadata"] = options["metadata"].model_copy(
            update={
                "variable_same_as": (
                    *options["metadata"].variable_same_as,
                    ResolvedVariableSameAs(
                        a="scb/example/value", b="scb/origin/new-source"
                    ),
                )
            }
        )
    result = resolve_catalog_lineage(variables, **options)
    assert not any(d.acknowledged_by for d in result.diagnostics)
    assert any(d.code == "stale_curation_entry" for d in result.diagnostics)
    assert not result.metadata.state_lineage


def test_scoped_lineage_acknowledgement_defers_only_positively_unselected_source():
    from reg_meta_build.catalog_dependencies import DEFERRED_REFERENCE

    variables, options, _ = _guarded_ambiguous_lineage()
    source = variables[1].register_ref
    options.update(
        registers=(variables[0].register_ref,),
        variants=tuple((r, v) for r, v in options["variants"] if r != source),
        metadata=ResolvedMetadata(),
        unselected={("register", "scb/origin")},
        unselected_names={"scb/origin": (source.name,)},
        slice_registers={"scb/example"},
    )
    result = resolve_catalog_lineage((variables[0],), **options)
    assert all(d.code == DEFERRED_REFERENCE for d in result.diagnostics)
    assert not any(d.acknowledged_by for d in result.diagnostics)
    assert result.skipped == 1
    options["slice_registers"] = None
    complete = resolve_catalog_lineage((variables[0],), **options)
    assert any(d.code == "stale_curation_entry" for d in complete.diagnostics)


def test_lineage_guard_retains_earlier_context_when_later_source_origin_differs():
    from reg_meta.source_evidence import canonical_sha256
    from reg_meta_build.catalog_lineage import lineage_acknowledgement_sha256
    from reg_meta_build.resolved_metadata import ResolvedVariableSameAs

    variables, options, case = _guarded_ambiguous_lineage()
    consumer, first = variables
    second_register = ResolvedRegister(
        provider="scb", slug="second", name="Second origin"
    )
    second = first.model_copy(update={"register_ref": second_register})
    a, b = consumer.states[0], consumer.states[0]
    consumer = consumer.model_copy(
        update={
            "states": (
                a.model_copy(update={"valid_to": "2000-06-30"}),
                b.model_copy(
                    update={
                        "valid_from": "2000-07-01",
                        "source_register_text": "Second origin : People",
                    }
                ),
            )
        }
    )
    variables = (consumer, first, second)
    options["registers"] += (second_register,)
    second_variants = tuple(
        (second_register, v) for r, v in options["variants"] if r == first.register_ref
    )
    options["variants"] += second_variants
    options["metadata"] = options["metadata"].model_copy(
        update={
            "variable_same_as": (
                *options["metadata"].variable_same_as,
                ResolvedVariableSameAs(a="scb/example/value", b="scb/second/value"),
            )
        }
    )
    options["evidence"]["scb/second/value"] = ()
    base = resolve_catalog_lineage(
        variables,
        **{
            k: v
            for k, v in options.items()
            if k not in {"acknowledgements", "acknowledgement_originals"}
        },
    )
    issue = next(d for d in base.diagnostics if d.valid_from == "2000-01-01")
    raw = options["acknowledgement_originals"][case.decision.refs[0]]
    decision = case.decision.model_copy(
        update={
            "valid_to": issue.valid_to,
            "expected_diagnostic_sha256": canonical_sha256(
                issue.model_dump(mode="json")
            ),
            "expected_evidence_sha256": lineage_acknowledgement_sha256(
                raw,
                consumer,
                (first, second),
                tuple(
                    (r, v)
                    for r, v in options["variants"]
                    if r in {first.register_ref, second_register} and v.name == "People"
                ),
                options["metadata"],
            ),
        }
    )
    case = case.model_copy(update={"decision": decision})
    options["acknowledgements"] = (case,)
    accepted = resolve_catalog_lineage(variables, **options)
    assert any(d.acknowledged_by == case.case_id for d in accepted.diagnostics)
    changed = first.model_copy(
        update={"definition": "Changed earlier source candidate"}
    )
    rejected = resolve_catalog_lineage((consumer, changed, second), **options)
    assert not any(d.acknowledged_by for d in rejected.diagnostics)
    assert any(d.code == "stale_curation_entry" for d in rejected.diagnostics)
