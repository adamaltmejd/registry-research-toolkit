"""Tests for the §6.8.1 structural validator.

Issue ``code`` values are pinned: they are stable across releases and
the SPA maps them to UI affordances (§6.8.0). Renames here would break
downstream consumers.

The validator runs against a parsed dict, not the dataclasses, so
fixtures are JSON-shaped (lists not tuples, raw strings). The
``_spec()`` helper builds a minimum-viable, all-rules-pass payload
that tests then mutate to exercise individual rules.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from reg_schema import ValidationResult, validate_structural


def _spec(**overrides: Any) -> dict[str, Any]:
    """Minimal valid spec; mutate via overrides to exercise rules."""

    base: dict[str, Any] = {
        "schema_version": "1.0.0",
        "steward": "global",
        "reg_meta_version": "reg_meta/v0.11.1",
        "name": "demo",
        "sources": [
            {
                "name": "lisa_2018",
                "register_version": "scb/lisa/individer-15plus/2018",
                "columns": [
                    {
                        "name": "scb/lisa/individer-15plus/2018/lopnr",
                        "display_name": "LopNr_PersonNr",
                        "type": "id",
                        "id_subtype": "integer",
                    },
                    {
                        "name": "scb/lisa/individer-15plus/2018/kon",
                        "display_name": "Kon",
                        "type": "categorical",
                        "value_set": "class/sun/2020",
                    },
                ],
            },
        ],
    }
    base.update(overrides)
    return base


def _codes(result: ValidationResult) -> list[str]:
    return [i.code for i in result.issues]


def _at(result: ValidationResult, code: str) -> list[str]:
    return [i.path for i in result.issues if i.code == code]


# --- Happy path ---------------------------------------------------------


def test_minimum_spec_is_ok() -> None:
    result = validate_structural(_spec())
    assert result.ok, result.issues
    assert result.issues == ()


def test_accepts_panels_and_namespaced_block() -> None:
    spec = _spec(
        panels=[
            {
                "panel_id": "lisa",
                "entity_key": "LopNr_PersonNr",
                "time_key": 2018,
                "members": ["lisa_2018"],
            }
        ],
        reg_monabundle={"column_options": {}},
    )
    result = validate_structural(spec)
    assert result.ok, result.issues


def test_non_mapping_root_emits_invalid_root() -> None:
    result = validate_structural([])  # type: ignore[arg-type]
    assert _codes(result) == ["invalid_root"]
    assert result.issues[0].path == ""


# --- Top-level ----------------------------------------------------------


def test_missing_required_top_level_fields() -> None:
    result = validate_structural({})
    missing_paths = _at(result, "missing_required_field")
    assert set(missing_paths) == {
        "/schema_version",
        "/steward",
        "/reg_meta_version",
        "/name",
        "/sources",
    }


def test_unknown_steward_is_rejected() -> None:
    result = validate_structural(_spec(steward="lkf"))
    assert _at(result, "invalid_enum_value") == ["/steward"]


def test_steward_must_be_string() -> None:
    result = validate_structural(_spec(steward=42))
    assert _at(result, "invalid_field_type") == ["/steward"]


def test_top_level_string_fields_must_be_strings() -> None:
    result = validate_structural(_spec(schema_version=1, name=False))
    assert set(_at(result, "invalid_field_type")) >= {"/schema_version", "/name"}


def test_required_top_level_field_set_to_null_is_invalid_field_type() -> None:
    # JSON null deserializes to Python None, which `dict.get` returns
    # for both absent and explicit-null cases. The validator must
    # distinguish them so `{"schema_version": null}` doesn't bypass
    # both the missing-field and the type check.
    result = validate_structural(_spec(schema_version=None, sources=None))
    assert "missing_required_field" not in _codes(result)
    assert set(_at(result, "invalid_field_type")) >= {
        "/schema_version",
        "/sources",
    }


def test_optional_baseline_field_null_is_invalid_field_type() -> None:
    # `panels` defaults to [] when absent, but an explicit null is not
    # an array and shouldn't be silently coerced.
    result = validate_structural(_spec(panels=None))
    assert "/panels" in _at(result, "invalid_field_type")


def test_namespaced_block_must_be_object() -> None:
    result = validate_structural(_spec(reg_monabundle="not-an-object"))
    assert _at(result, "invalid_field_type") == ["/reg_monabundle"]


def test_unknown_top_level_field_is_treated_as_namespaced_block() -> None:
    # A non-baseline key must just be a mapping — its contents are
    # opaque at this layer.
    ok = validate_structural(_spec(swecov={"filters": {}}))
    assert ok.ok, ok.issues
    bad = validate_structural(_spec(swecov=[1, 2, 3]))
    assert _at(bad, "invalid_field_type") == ["/swecov"]


# --- Sources / Columns --------------------------------------------------


def test_sources_must_be_array() -> None:
    result = validate_structural(_spec(sources={"not": "an array"}))
    assert _at(result, "invalid_field_type") == ["/sources"]


def test_panel_unknown_source_skipped_when_sources_shape_invalid() -> None:
    # When `/sources` itself isn't an array, source-name resolution
    # is impossible. The primary `/sources` error is already reported;
    # cascading `panel_member_unknown_source` on every member is just
    # noise that obscures the real failure.
    result = validate_structural(
        _spec(
            sources={"not": "an array"},
            panels=[
                {
                    "panel_id": "p",
                    "entity_key": "LopNr_PersonNr",
                    "members": [{"source": "lisa_2018", "time_key": 2018}],
                }
            ],
        )
    )
    assert "panel_member_unknown_source" not in _codes(result)
    assert "/sources" in _at(result, "invalid_field_type")


def test_duplicate_source_name() -> None:
    spec = _spec()
    spec["sources"].append(dict(spec["sources"][0]))
    result = validate_structural(spec)
    assert _at(result, "duplicate_source_name") == ["/sources/1/name"]


def test_register_version_wrong_segment_count_is_invalid_fqid() -> None:
    spec = _spec()
    spec["sources"][0]["register_version"] = "scb/lisa/individer-15plus"  # 3 segments
    result = validate_structural(spec)
    assert _at(result, "invalid_fqid") == ["/sources/0/register_version"]


def test_register_version_class_prefix_rejected() -> None:
    spec = _spec()
    spec["sources"][0]["register_version"] = "class/lisa/individer-15plus/2018"
    result = validate_structural(spec)
    assert _at(result, "invalid_fqid") == ["/sources/0/register_version"]


def test_register_version_bad_chars_rejected() -> None:
    spec = _spec()
    spec["sources"][0]["register_version"] = "scb/lisa/individer 15plus/2018"
    result = validate_structural(spec)
    assert _at(result, "invalid_fqid") == ["/sources/0/register_version"]


def test_column_name_segment_count() -> None:
    spec = _spec()
    spec["sources"][0]["columns"][0]["name"] = (
        "scb/lisa/individer-15plus/2018"  # 4 segs
    )
    result = validate_structural(spec)
    assert "/sources/0/columns/0/name" in _at(result, "invalid_fqid")


def test_column_name_must_match_source_register_version() -> None:
    spec = _spec()
    spec["sources"][0]["columns"][0]["name"] = "scb/lisa/individer-15plus/2019/kon"
    result = validate_structural(spec)
    assert _at(result, "fqid_register_version_mismatch") == [
        "/sources/0/columns/0/name"
    ]


def test_column_name_mismatch_skipped_when_register_version_malformed() -> None:
    # When register_version itself is malformed, the validator must not
    # compound the noise with a mismatch error against undefined truth.
    spec = _spec()
    spec["sources"][0]["register_version"] = "broken"
    result = validate_structural(spec)
    assert _at(result, "fqid_register_version_mismatch") == []


def test_column_type_must_be_in_enum() -> None:
    spec = _spec()
    spec["sources"][0]["columns"][0]["type"] = "boolean"
    result = validate_structural(spec)
    assert _at(result, "invalid_enum_value") == ["/sources/0/columns/0/type"]


def test_subtype_on_wrong_type_is_rejected() -> None:
    spec = _spec()
    # id_subtype on a categorical column.
    spec["sources"][0]["columns"][1]["id_subtype"] = "integer"
    result = validate_structural(spec)
    assert _at(result, "subtype_on_wrong_type") == ["/sources/0/columns/1/id_subtype"]


def test_numeric_subtype_must_be_in_enum() -> None:
    spec = _spec()
    spec["sources"][0]["columns"][0]["type"] = "numeric"
    spec["sources"][0]["columns"][0].pop("id_subtype", None)
    spec["sources"][0]["columns"][0]["numeric_subtype"] = "biginteger"
    result = validate_structural(spec)
    assert _at(result, "invalid_enum_value") == ["/sources/0/columns/0/numeric_subtype"]


def test_value_set_must_be_classification_fqid() -> None:
    spec = _spec()
    spec["sources"][0]["columns"][1]["value_set"] = "sun/2020"  # missing class/ prefix
    result = validate_structural(spec)
    assert _at(result, "invalid_fqid") == ["/sources/0/columns/1/value_set"]


def test_empty_columns_emits_explicit_code() -> None:
    spec = _spec()
    spec["sources"][0]["columns"] = []
    result = validate_structural(spec)
    assert _at(result, "empty_columns") == ["/sources/0/columns"]


# --- Panels -------------------------------------------------------------


def _spec_with_panels(**panel_overrides: Any) -> dict[str, Any]:
    spec = _spec()
    spec["sources"].append(
        {
            "name": "lisa_2019",
            "register_version": "scb/lisa/individer-15plus/2019",
            "columns": [
                {
                    "name": "scb/lisa/individer-15plus/2019/lopnr",
                    "display_name": "LopNr_PersonNr",
                    "type": "id",
                    "id_subtype": "integer",
                },
                {
                    "name": "scb/lisa/individer-15plus/2019/ar",
                    "display_name": "AR",
                    "type": "numeric",
                    "numeric_subtype": "integer",
                },
            ],
        }
    )
    spec["sources"][0]["columns"].append(
        {
            "name": "scb/lisa/individer-15plus/2018/ar",
            "display_name": "AR",
            "type": "numeric",
            "numeric_subtype": "integer",
        }
    )
    panel: dict[str, Any] = {
        "panel_id": "lisa",
        "entity_key": "LopNr_PersonNr",
        "members": [
            {"source": "lisa_2018", "time_key": 2018},
            {"source": "lisa_2019", "time_key": 2019},
        ],
    }
    panel.update(panel_overrides)
    spec["panels"] = [panel]
    return spec


def test_panel_happy_path() -> None:
    result = validate_structural(_spec_with_panels())
    assert result.ok, result.issues


def test_panel_string_member_inherits_panel_defaults() -> None:
    spec = _spec_with_panels(
        time_key="AR",
        members=["lisa_2018", "lisa_2019"],
    )
    result = validate_structural(spec)
    assert result.ok, result.issues


def test_panel_string_member_missing_panel_defaults_emits_missing_effective() -> None:
    spec = _spec_with_panels(members=["lisa_2018"])  # no panel time_key
    spec["panels"][0].pop("time_key", None)
    result = validate_structural(spec)
    assert "missing_effective_time_key" in _codes(result)


def test_duplicate_panel_id() -> None:
    spec = _spec_with_panels()
    spec["panels"].append(dict(spec["panels"][0]))
    result = validate_structural(spec)
    assert _at(result, "duplicate_panel_id") == ["/panels/1/panel_id"]


def test_same_panel_duplicate_source_does_not_fire_cross_panel_collision() -> None:
    # The §6.4 "at most one panel" rule is cross-panel. Two members of
    # one panel sharing a source is a degenerate panel, not a
    # cross-panel collision — firing the code here would lie.
    spec = _spec_with_panels()
    spec["panels"][0]["members"].append({"source": "lisa_2018", "time_key": 2020})
    result = validate_structural(spec)
    assert "source_referenced_by_multiple_panels" not in _codes(result)


def test_undefined_source_reused_across_panels_only_fires_unknown_source() -> None:
    # An undefined source name reused in two panels should fire only
    # `panel_member_unknown_source` per panel — not a misleading
    # cross-panel collision on top, since the source isn't defined in
    # /sources to begin with.
    spec = _spec_with_panels()
    spec["panels"][0]["members"][0]["source"] = "ghost"
    spec["panels"].append(
        {
            "panel_id": "second",
            "entity_key": "LopNr_PersonNr",
            "members": [{"source": "ghost", "time_key": 2020}],
        }
    )
    result = validate_structural(spec)
    assert "source_referenced_by_multiple_panels" not in _codes(result)
    assert _codes(result).count("panel_member_unknown_source") == 2


def test_panel_member_unknown_source_is_flagged() -> None:
    # A panel member must point at a real /sources entry. Silently
    # skipping unknown sources pushes a schema error into runtime.
    spec = _spec_with_panels()
    spec["panels"][0]["members"][0]["source"] = "not_a_source"
    result = validate_structural(spec)
    assert _at(result, "panel_member_unknown_source") == ["/panels/0/members/0/source"]


def test_panel_member_unknown_source_string_form() -> None:
    # String-shaped members get the same check (path differs — no
    # `/source` suffix because the member itself is the source name).
    spec = _spec_with_panels(time_key="AR", members=["lisa_2018", "ghost_source"])
    result = validate_structural(spec)
    assert "/panels/0/members/1" in _at(result, "panel_member_unknown_source")


def test_ref_existence_not_emitted_on_unknown_source() -> None:
    # When the source itself is unknown, the unknown-column refs check
    # should NOT also fire — that's noise on top of the real error.
    spec = _spec_with_panels()
    spec["panels"][0]["members"][0]["source"] = "ghost"
    spec["panels"][0]["members"][0]["entity_key"] = "NOPE"
    result = validate_structural(spec)
    assert "panel_member_unknown_source" in _codes(result)
    # No entity_key_unknown_column path against /panels/0/members/0
    paths = _at(result, "entity_key_unknown_column")
    assert not any(p.startswith("/panels/0/members/0") for p in paths)


def test_ref_existence_skipped_when_columns_are_malformed() -> None:
    # If a column entry isn't an object or its display_name isn't a
    # string, `all_have_display` must flip False so the ref-existence
    # check doesn't compound the noise on an already-broken source.
    spec = _spec_with_panels(time_key="AR", members=["lisa_2018", "lisa_2019"])
    spec["sources"][0]["columns"][0] = "not-a-mapping"  # type: ignore[assignment]
    result = validate_structural(spec)
    assert "time_key_unknown_column" not in _codes(result)


def test_source_referenced_by_multiple_panels() -> None:
    spec = _spec_with_panels()
    spec["panels"].append(
        {
            "panel_id": "lisa_dup",
            "entity_key": "LopNr_PersonNr",
            "members": [{"source": "lisa_2018", "time_key": 2018}],
        }
    )
    result = validate_structural(spec)
    assert "source_referenced_by_multiple_panels" in _codes(result)


def test_literal_time_key_duplicate_within_panel() -> None:
    spec = _spec_with_panels()
    spec["panels"][0]["members"][1]["time_key"] = 2018  # same as member 0
    result = validate_structural(spec)
    assert "literal_time_key_duplicate" in _codes(result)


def test_literal_time_key_duplicate_across_int_and_period_forms() -> None:
    # `2018` and `{"period": 2018}` encode the same year. The
    # uniqueness rule must canonicalize them; otherwise a user trips
    # the rule trivially by writing the same period two different
    # ways and the validator stays silent.
    spec = _spec_with_panels()
    spec["panels"][0]["members"][0]["time_key"] = 2018
    spec["panels"][0]["members"][1]["time_key"] = {"period": 2018}
    result = validate_structural(spec)
    assert "literal_time_key_duplicate" in _codes(result)


def test_literal_period_object_form_accepted() -> None:
    spec = _spec_with_panels()
    spec["panels"][0]["members"] = [
        {"source": "lisa_2018", "time_key": {"period": "2018-01"}},
        {"source": "lisa_2019", "time_key": {"period": "2018-02"}},
    ]
    result = validate_structural(spec)
    assert result.ok, result.issues


def test_literal_period_invalid_shape() -> None:
    spec = _spec_with_panels()
    spec["panels"][0]["members"][0]["time_key"] = {"period": "2018", "extra": 1}
    result = validate_structural(spec)
    assert "literal_period_invalid" in _codes(result)


def test_time_key_column_ref_must_exist_on_source() -> None:
    spec = _spec_with_panels(
        time_key="NOT_A_COLUMN",
        members=["lisa_2018", "lisa_2019"],
    )
    result = validate_structural(spec)
    paths = _at(result, "time_key_unknown_column")
    # Both members inherit the unknown column ref from the panel default.
    assert len(paths) == 2


def test_time_key_column_ref_skipped_when_display_names_missing() -> None:
    # When any column on the source lacks display_name, the structural
    # layer can't be sure the ref doesn't resolve later — skip the
    # check rather than emit a spurious error.
    spec = _spec_with_panels(
        time_key="AR",
        members=["lisa_2018", "lisa_2019"],
    )
    # Drop one display_name on lisa_2018 so all_have_display flips false.
    del spec["sources"][0]["columns"][0]["display_name"]
    result = validate_structural(spec)
    assert "time_key_unknown_column" not in _codes(result)


def test_entity_key_column_ref_must_exist_on_source() -> None:
    spec = _spec_with_panels(entity_key="NOT_A_COLUMN")
    result = validate_structural(spec)
    assert "entity_key_unknown_column" in _codes(result)


def test_composite_time_key_mixed_kinds_rejected() -> None:
    spec = _spec_with_panels()
    spec["panels"][0]["members"][0]["time_key"] = [2018, "AR"]
    result = validate_structural(spec)
    assert "composite_time_key_mixed_kinds" in _codes(result)


def test_composite_entity_key_ordering_inconsistent() -> None:
    spec = _spec_with_panels(entity_key=["LopNr_PeOrgNr", "LopNr_CFAR"])
    # Override second member with reversed order.
    spec["panels"][0]["members"][1]["entity_key"] = [
        "LopNr_CFAR",
        "LopNr_PeOrgNr",
    ]
    # Add display_names so refs resolve and isolate the ordering error.
    for col_name, src_idx in (
        ("LopNr_PeOrgNr", 0),
        ("LopNr_CFAR", 0),
        ("LopNr_PeOrgNr", 1),
        ("LopNr_CFAR", 1),
    ):
        spec["sources"][src_idx]["columns"].append(
            {
                "name": (
                    f"scb/lisa/individer-15plus/{2018 if src_idx == 0 else 2019}/"
                    f"{col_name.lower()}"
                ),
                "display_name": col_name,
                "type": "id",
                "id_subtype": "string",
            }
        )
    result = validate_structural(spec)
    assert "composite_key_inconsistent" in _codes(result)


def test_member_composite_time_key_kind_mismatch() -> None:
    # Panel-level composite time_key is a literal composite; the
    # member override is a column-ref composite. Cross-kind composite
    # overrides are rejected by §6.8.1.
    spec = _spec_with_panels(time_key=[2018, 2019])
    spec["panels"][0]["members"][1]["time_key"] = ["AR", "AR"]
    result = validate_structural(spec)
    assert "time_key_member_kind_mismatch" in _codes(result)


def test_panel_member_must_be_string_or_object() -> None:
    spec = _spec_with_panels()
    spec["panels"][0]["members"][0] = 42
    result = validate_structural(spec)
    assert "invalid_field_type" in _codes(result)
    assert any(
        p.startswith("/panels/0/members/0") for p in _at(result, "invalid_field_type")
    )


def test_panel_must_have_at_least_one_member() -> None:
    spec = _spec_with_panels()
    spec["panels"][0]["members"] = []
    result = validate_structural(spec)
    assert _at(result, "empty_members") == ["/panels/0/members"]


def test_panel_comment_must_be_string_when_present() -> None:
    # `comment` is documented as string in §6.4; accepting non-strings
    # here would let structurally invalid specs through and break
    # downstream consumers that treat comments as text.
    spec = _spec_with_panels()
    spec["panels"][0]["comment"] = 123
    result = validate_structural(spec)
    assert _at(result, "invalid_field_type") == ["/panels/0/comment"]

    spec["panels"][0]["comment"] = "valid comment"
    assert validate_structural(spec).ok


# --- Composition / contract -------------------------------------------


def test_result_issues_is_tuple_of_validation_issues() -> None:
    result = validate_structural(_spec())
    assert isinstance(result, ValidationResult)
    assert isinstance(result.issues, tuple)


def test_validator_accepts_arbitrary_mapping() -> None:
    # The signature is ``Mapping[str, object]``, not ``dict``; any
    # mapping should work (matters for SPA-shaped or proxy inputs).
    class FrozenMap(Mapping):  # type: ignore[type-arg]
        def __init__(self, data: dict[str, Any]) -> None:
            self._d = data

        def __getitem__(self, key: str) -> Any:
            return self._d[key]

        def __iter__(self):
            return iter(self._d)

        def __len__(self) -> int:
            return len(self._d)

    result = validate_structural(FrozenMap(_spec()))
    assert result.ok, result.issues
