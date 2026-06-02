"""Tests for the §6.8.1 structural validator (Model A grammar).

Issue ``code`` values are pinned: they are stable across releases and
the SPA maps them to UI affordances (§6.8.0). Renames here would break
downstream consumers.

The validator runs against a parsed dict, not the Pydantic models, so
fixtures are JSON-shaped (lists not tuples, raw strings). The
``_spec()`` helper builds a minimum-viable, all-rules-pass payload
that tests then mutate to exercise individual rules.

Model A (§6.2-§6.3): a source carries a 3-part ``register_variant``
coordinate plus a required ``period``; bindings (renamed from the v0.x
``columns``) name a 3-segment binding FQID via ``variable`` and a
2-segment ``class/<slug>`` ``value_set``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from reg_schema import ValidationResult, validate_structural


def _spec(**overrides: Any) -> dict[str, Any]:
    """Minimal valid spec; mutate via overrides to exercise rules."""

    base: dict[str, Any] = {
        "schema_version": "2.0.0",
        "steward": "global",
        "reg_meta_version": "reg_meta/v1.0.0",
        "name": "demo",
        "sources": [
            {
                "name": "lisa_2018",
                "register_variant": "scb/lisa/individer-15plus",
                "period": 2018,
                "bindings": [
                    {
                        "variable": "scb/lisa/lopnr",
                        "display_name": "LopNr_PersonNr",
                        "type": "id",
                        "id_subtype": "integer",
                    },
                    {
                        "variable": "scb/lisa/kon",
                        "display_name": "Kon",
                        "type": "categorical",
                        "value_set": "class/sun2020",
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


# --- Sources ------------------------------------------------------------


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


def test_display_name_collision_within_source() -> None:
    # §6.3: two bindings on the same source sharing an explicit
    # display_name must produce display_name_collision. The implicit-
    # resolution case (one explicit + one resolving to the same
    # reg_meta default) needs reg_meta and is §6.8.3.
    spec = _spec()
    # _spec()'s two bindings already have distinct display_names; set
    # the second to match the first.
    spec["sources"][0]["bindings"][1]["display_name"] = spec["sources"][0]["bindings"][
        0
    ]["display_name"]
    result = validate_structural(spec)
    assert _at(result, "display_name_collision") == [
        "/sources/0/bindings/1/display_name"
    ]


def test_display_name_collision_scoped_to_one_source() -> None:
    # Two sources can each have a binding named "LopNr_PersonNr"
    # without colliding. The check is per-source.
    spec = _spec()
    spec["sources"].append(
        {
            "name": "lisa_2019",
            "register_variant": "scb/lisa/individer-15plus",
            "period": 2019,
            "bindings": [
                {
                    "variable": "scb/lisa/lopnr",
                    "display_name": "LopNr_PersonNr",  # same as source 0's binding 0
                    "type": "id",
                    "id_subtype": "integer",
                },
            ],
        }
    )
    result = validate_structural(spec)
    assert "display_name_collision" not in _codes(result)


# --- register_variant ---------------------------------------------------


def test_register_variant_wrong_segment_count_is_invalid_fqid() -> None:
    spec = _spec()
    spec["sources"][0]["register_variant"] = "scb/lisa"  # 2 segments
    result = validate_structural(spec)
    assert _at(result, "invalid_fqid") == ["/sources/0/register_variant"]


def test_register_variant_with_period_segment_is_invalid_fqid() -> None:
    # The v0.x 4-segment register_version (period in slot 4) is no longer
    # accepted — period lives in its own field now (§6.2).
    spec = _spec()
    spec["sources"][0]["register_variant"] = "scb/lisa/individer-15plus/2018"
    result = validate_structural(spec)
    assert _at(result, "invalid_fqid") == ["/sources/0/register_variant"]


def test_register_variant_class_prefix_rejected() -> None:
    spec = _spec()
    spec["sources"][0]["register_variant"] = "class/lisa/individer-15plus"
    result = validate_structural(spec)
    assert _at(result, "invalid_fqid") == ["/sources/0/register_variant"]


def test_register_variant_bad_chars_rejected() -> None:
    spec = _spec()
    spec["sources"][0]["register_variant"] = "scb/lisa/individer 15plus"
    result = validate_structural(spec)
    assert _at(result, "invalid_fqid") == ["/sources/0/register_variant"]


# --- Period (§6.2) ------------------------------------------------------


def test_period_int_year_is_ok() -> None:
    assert validate_structural(_spec()).ok


def test_period_token_strings_are_ok() -> None:
    for period in (
        "2018",
        "2018-01",
        "2018-12-31",
        "HT2020",
        "VT2020",
        "2018-Q1",
        "2018-H1",
    ):
        spec = _spec()
        spec["sources"][0]["period"] = period
        result = validate_structural(spec)
        assert result.ok, (period, result.issues)


def test_period_snapshot_sentinel_is_ok() -> None:
    spec = _spec()
    spec["sources"][0]["period"] = "_default"
    assert validate_structural(spec).ok


def test_period_range_object_is_ok() -> None:
    spec = _spec()
    spec["sources"][0]["period"] = {"from": "2018-01-01", "to": "2020-06-30"}
    result = validate_structural(spec)
    assert result.ok, result.issues


def test_period_range_with_int_endpoints_is_ok() -> None:
    spec = _spec()
    spec["sources"][0]["period"] = {"from": 2018, "to": 2020}
    assert validate_structural(spec).ok


def test_period_bad_string_is_invalid_period() -> None:
    spec = _spec()
    spec["sources"][0]["period"] = "nope"
    result = validate_structural(spec)
    assert _at(result, "invalid_period") == ["/sources/0/period"]


def test_period_bool_is_invalid_period() -> None:
    # bool is an int subclass; the period grammar must not accept it.
    spec = _spec()
    spec["sources"][0]["period"] = True
    result = validate_structural(spec)
    assert _at(result, "invalid_period") == ["/sources/0/period"]


def test_period_range_bad_endpoint_is_invalid_period() -> None:
    spec = _spec()
    spec["sources"][0]["period"] = {"from": "2018-01-01", "to": "nope"}
    result = validate_structural(spec)
    assert _at(result, "invalid_period") == ["/sources/0/period"]


def test_period_range_extra_keys_is_invalid_period() -> None:
    spec = _spec()
    spec["sources"][0]["period"] = {"from": 2018, "to": 2020, "step": 1}
    result = validate_structural(spec)
    assert _at(result, "invalid_period") == ["/sources/0/period"]


def test_period_out_of_bounds_tokens_are_invalid() -> None:
    # The grammar is bound-for-bound identical to reg_meta.fqid (year
    # 1900-2099, month 01-12, day 01-31, quarter 1-4, half 1-2); a looser
    # copy would pass specs that reg_meta's resolver later rejects.
    for period in (
        "2018-13",  # month > 12
        "2018-00",  # month 0
        "2018-13-31",  # month > 12 in a full date
        "2018-Q5",  # quarter > 4
        "2018-Q0",
        "2018-H3",  # half > 2
        "2018-H0",
        "1899",  # year below 1900
        "2100",  # year above 2099
    ):
        spec = _spec()
        spec["sources"][0]["period"] = period
        result = validate_structural(spec)
        assert _at(result, "invalid_period") == ["/sources/0/period"], period


def test_period_range_out_of_bounds_endpoint_is_invalid() -> None:
    # The same bounded grammar gates range endpoints (_is_period_endpoint).
    for endpoint in ("2018-13", "1899", "2018-Q5"):
        spec = _spec()
        spec["sources"][0]["period"] = {"from": endpoint, "to": "2020"}
        result = validate_structural(spec)
        assert _at(result, "invalid_period") == ["/sources/0/period"], endpoint


def test_period_null_is_invalid_field_type() -> None:
    spec = _spec()
    spec["sources"][0]["period"] = None
    result = validate_structural(spec)
    assert "/sources/0/period" in _at(result, "invalid_field_type")
    assert "/sources/0/period" not in _at(result, "missing_required_field")


def test_period_missing_is_missing_required_field() -> None:
    spec = _spec()
    del spec["sources"][0]["period"]
    result = validate_structural(spec)
    assert "/sources/0/period" in _at(result, "missing_required_field")


# --- Bindings -----------------------------------------------------------


def test_binding_variable_segment_count() -> None:
    spec = _spec()
    spec["sources"][0]["bindings"][0]["variable"] = "scb/lisa"  # 2 segs
    result = validate_structural(spec)
    assert "/sources/0/bindings/0/variable" in _at(result, "invalid_fqid")


def test_binding_variable_class_prefix_rejected() -> None:
    spec = _spec()
    spec["sources"][0]["bindings"][0]["variable"] = "class/lisa/kon"
    result = validate_structural(spec)
    assert "/sources/0/bindings/0/variable" in _at(result, "invalid_fqid")


def test_binding_variable_with_version_suffix_is_ok() -> None:
    # The leaf parses as ``slug[@version]`` — the ``@`` is split off
    # before the slug regex (§5.2).
    spec = _spec()
    spec["sources"][0]["bindings"][1]["variable"] = "scb/lisa/naringsgren@sni2007"
    spec["sources"][0]["bindings"][1]["value_set"] = "class/sni2007"
    result = validate_structural(spec)
    assert result.ok, result.issues


def test_binding_variable_empty_version_suffix_is_invalid_fqid() -> None:
    spec = _spec()
    spec["sources"][0]["bindings"][0]["variable"] = "scb/lisa/naringsgren@"
    result = validate_structural(spec)
    assert "/sources/0/bindings/0/variable" in _at(result, "invalid_fqid")


def test_binding_variable_double_at_is_invalid_fqid() -> None:
    spec = _spec()
    spec["sources"][0]["bindings"][0]["variable"] = "scb/lisa/naringsgren@a@b"
    result = validate_structural(spec)
    assert "/sources/0/bindings/0/variable" in _at(result, "invalid_fqid")


def test_binding_variable_must_match_source_register_variant_prefix() -> None:
    spec = _spec()
    # Right register, wrong provider — prefix mismatch.
    spec["sources"][0]["bindings"][0]["variable"] = "ifau/lisa/lopnr"
    result = validate_structural(spec)
    assert _at(result, "fqid_register_variant_mismatch") == [
        "/sources/0/bindings/0/variable"
    ]


def test_binding_prefix_mismatch_skipped_when_register_variant_malformed() -> None:
    # When register_variant itself is malformed, the validator must not
    # compound the noise with a mismatch error against undefined truth.
    spec = _spec()
    spec["sources"][0]["register_variant"] = "broken"
    result = validate_structural(spec)
    assert _at(result, "fqid_register_variant_mismatch") == []


def test_binding_type_must_be_in_enum() -> None:
    spec = _spec()
    spec["sources"][0]["bindings"][0]["type"] = "boolean"
    result = validate_structural(spec)
    assert _at(result, "invalid_enum_value") == ["/sources/0/bindings/0/type"]


def test_subtype_on_wrong_type_is_rejected() -> None:
    spec = _spec()
    # id_subtype on a categorical binding.
    spec["sources"][0]["bindings"][1]["id_subtype"] = "integer"
    result = validate_structural(spec)
    assert _at(result, "subtype_on_wrong_type") == ["/sources/0/bindings/1/id_subtype"]


def test_numeric_subtype_must_be_in_enum() -> None:
    spec = _spec()
    spec["sources"][0]["bindings"][0]["type"] = "numeric"
    spec["sources"][0]["bindings"][0].pop("id_subtype", None)
    spec["sources"][0]["bindings"][0]["numeric_subtype"] = "biginteger"
    result = validate_structural(spec)
    assert _at(result, "invalid_enum_value") == [
        "/sources/0/bindings/0/numeric_subtype"
    ]


def test_value_set_must_be_classification_fqid() -> None:
    spec = _spec()
    # 3-segment value_set is the old grammar; 2-segment class/<slug> is
    # required now.
    spec["sources"][0]["bindings"][1]["value_set"] = "class/sun/2020"
    result = validate_structural(spec)
    assert _at(result, "invalid_fqid") == ["/sources/0/bindings/1/value_set"]


def test_value_set_missing_class_prefix_is_invalid_fqid() -> None:
    spec = _spec()
    spec["sources"][0]["bindings"][1]["value_set"] = "sun2020"
    result = validate_structural(spec)
    assert _at(result, "invalid_fqid") == ["/sources/0/bindings/1/value_set"]


def test_binding_value_set_version_mismatch() -> None:
    # The FQID's @<version> pin and the value_set's class slug must name
    # the same version (§6.8.1).
    spec = _spec()
    spec["sources"][0]["bindings"][1]["variable"] = "scb/lisa/naringsgren@sni2007"
    spec["sources"][0]["bindings"][1]["value_set"] = "class/sni92"
    result = validate_structural(spec)
    assert _at(result, "binding_value_set_version_mismatch") == [
        "/sources/0/bindings/1/value_set"
    ]


def test_binding_value_set_version_match_is_ok() -> None:
    spec = _spec()
    spec["sources"][0]["bindings"][1]["variable"] = "scb/lisa/naringsgren@sni2007"
    spec["sources"][0]["bindings"][1]["value_set"] = "class/sni2007"
    assert validate_structural(spec).ok


def test_binding_value_set_without_version_pin_is_ok() -> None:
    # No @version on the FQID → no cross-check; any well-formed value_set
    # passes the structural layer.
    spec = _spec()
    spec["sources"][0]["bindings"][1]["value_set"] = "class/sni92"
    assert validate_structural(spec).ok


def test_empty_bindings_emits_explicit_code() -> None:
    spec = _spec()
    spec["sources"][0]["bindings"] = []
    result = validate_structural(spec)
    assert _at(result, "empty_bindings") == ["/sources/0/bindings"]


# --- Unexpected fields on closed objects --------------------------------
#
# Source/Binding/Panel/PanelMember are ``extra="forbid"`` (_Model in
# project_data.py): an unrecognized key is a structural error
# (`unexpected_field`), not a silently-ignored extra. The TOP LEVEL stays
# open (ProjectData is extra="ignore" for steward-namespaced blocks) —
# see test_unknown_top_level_field_is_treated_as_namespaced_block.


def test_unexpected_field_on_source() -> None:
    spec = _spec()
    spec["sources"][0]["registr_variant"] = "scb/lisa/individer-15plus"  # typo
    result = validate_structural(spec)
    issues = [i for i in result.issues if i.code == "unexpected_field"]
    assert [i.path for i in issues] == ["/sources/0/registr_variant"]
    assert all(i.level == "error" for i in issues)


def test_unexpected_field_on_binding() -> None:
    spec = _spec()
    spec["sources"][0]["bindings"][0]["typ"] = "id"  # typo for `type`
    result = validate_structural(spec)
    issues = [i for i in result.issues if i.code == "unexpected_field"]
    assert [i.path for i in issues] == ["/sources/0/bindings/0/typ"]
    assert all(i.level == "error" for i in issues)


def test_unexpected_field_on_panel() -> None:
    spec = _spec_with_panels()
    spec["panels"][0]["coment"] = "typo for comment"
    result = validate_structural(spec)
    issues = [i for i in result.issues if i.code == "unexpected_field"]
    assert [i.path for i in issues] == ["/panels/0/coment"]
    assert all(i.level == "error" for i in issues)


def test_unexpected_field_on_panel_member() -> None:
    spec = _spec_with_panels()
    spec["panels"][0]["members"][0]["time_kye"] = 2018  # typo for time_key
    result = validate_structural(spec)
    issues = [i for i in result.issues if i.code == "unexpected_field"]
    assert [i.path for i in issues] == ["/panels/0/members/0/time_kye"]
    assert all(i.level == "error" for i in issues)


def test_unexpected_field_key_is_rfc6901_escaped() -> None:
    # The key is arbitrary steward input; a typo containing `/` or `~` must be
    # RFC 6901-escaped in the JSON pointer (`/`→`~1`, `~`→`~0`), else the SPA's
    # pointer-based field jump resolves to the wrong (nested) location.
    spec = _spec()
    spec["sources"][0]["bindings"][0]["foo/bar~baz"] = "oops"
    result = validate_structural(spec)
    issues = [i for i in result.issues if i.code == "unexpected_field"]
    assert [i.path for i in issues] == ["/sources/0/bindings/0/foo~1bar~0baz"]


def test_namespaced_block_key_is_rfc6901_escaped() -> None:
    # Same pointer-escaping contract for the OTHER user-controlled key site: a
    # non-object namespaced block whose top-level key contains `/` or `~`.
    result = validate_structural(_spec(**{"weird/block~x": "not-an-object"}))
    issues = [i for i in result.issues if i.code == "invalid_field_type"]
    assert "/weird~1block~0x" in [i.path for i in issues]


def test_unexpected_field_not_emitted_for_top_level_namespaced_block() -> None:
    # Top level is open: an unknown top-level key is a namespaced block,
    # never `unexpected_field`. Guards against a future reader "fixing"
    # _check_namespaced_blocks to mirror the closed-object check.
    result = validate_structural(_spec(swecov={"filters": {}}))
    assert result.ok, result.issues
    assert "unexpected_field" not in _codes(result)


# --- Nested required-field null handling -------------------------------
#
# `{"name": null}` is a different JSON shape than `{}`. The validator
# emits `invalid_field_type` for the former and `missing_required_field`
# for the latter so error messages don't lie about the input — same
# distinction `_check_top_level_fields` already makes for top-level
# required fields.


def test_source_name_null_is_invalid_field_type() -> None:
    spec = _spec()
    spec["sources"][0]["name"] = None
    result = validate_structural(spec)
    assert "/sources/0/name" in _at(result, "invalid_field_type")
    assert "/sources/0/name" not in _at(result, "missing_required_field")


def test_source_register_variant_null_is_invalid_field_type() -> None:
    spec = _spec()
    spec["sources"][0]["register_variant"] = None
    result = validate_structural(spec)
    assert "/sources/0/register_variant" in _at(result, "invalid_field_type")
    assert "/sources/0/register_variant" not in _at(result, "missing_required_field")


def test_source_bindings_null_is_invalid_field_type() -> None:
    spec = _spec()
    spec["sources"][0]["bindings"] = None
    result = validate_structural(spec)
    assert "/sources/0/bindings" in _at(result, "invalid_field_type")
    assert "/sources/0/bindings" not in _at(result, "missing_required_field")


def test_binding_variable_null_is_invalid_field_type() -> None:
    spec = _spec()
    spec["sources"][0]["bindings"][0]["variable"] = None
    result = validate_structural(spec)
    assert "/sources/0/bindings/0/variable" in _at(result, "invalid_field_type")
    assert "/sources/0/bindings/0/variable" not in _at(result, "missing_required_field")


def test_binding_type_null_is_invalid_field_type() -> None:
    spec = _spec()
    spec["sources"][0]["bindings"][0]["type"] = None
    result = validate_structural(spec)
    assert "/sources/0/bindings/0/type" in _at(result, "invalid_field_type")
    assert "/sources/0/bindings/0/type" not in _at(result, "missing_required_field")


# --- Panels -------------------------------------------------------------


def _spec_with_panels(**panel_overrides: Any) -> dict[str, Any]:
    spec = _spec()
    spec["sources"].append(
        {
            "name": "lisa_2019",
            "register_variant": "scb/lisa/individer-15plus",
            "period": 2019,
            "bindings": [
                {
                    "variable": "scb/lisa/lopnr",
                    "display_name": "LopNr_PersonNr",
                    "type": "id",
                    "id_subtype": "integer",
                },
                {
                    "variable": "scb/lisa/ar",
                    "display_name": "AR",
                    "type": "numeric",
                    "numeric_subtype": "integer",
                },
            ],
        }
    )
    spec["sources"][0]["bindings"].append(
        {
            "variable": "scb/lisa/ar",
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


def test_panel_string_member_missing_panel_defaults_is_not_structural() -> None:
    # Effective-key *presence* is no longer a structural rule (§6.8.1):
    # an omitted entity_key/time_key inherits from the member's variant
    # panel_template, which needs reg_meta — so the "no effective key"
    # case is the semantic `panel_inheritance_unresolvable` check
    # (§6.8.3), not emitted here.
    spec = _spec_with_panels(members=["lisa_2018"])  # no panel time_key
    spec["panels"][0].pop("time_key", None)
    spec["panels"][0].pop("entity_key", None)
    result = validate_structural(spec)
    assert result.ok, result.issues
    # Pin the §6.8.1 removal: neither removed code is emitted any more.
    assert "missing_effective_time_key" not in _codes(result)
    assert "missing_effective_entity_key" not in _codes(result)


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


def test_ref_existence_skipped_when_bindings_are_malformed() -> None:
    # If a binding entry isn't an object or its display_name isn't a
    # string, `all_have_display` must flip False so the ref-existence
    # check doesn't compound the noise on an already-broken source.
    spec = _spec_with_panels(time_key="AR", members=["lisa_2018", "lisa_2019"])
    spec["sources"][0]["bindings"][0] = "not-a-mapping"  # type: ignore[assignment]
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


def test_time_range_object_form_accepted() -> None:
    # The {"range": {"from","to"}} TimePoint wrapper (§6.4) is a valid
    # literal time_key — distinct from a bare {"from","to"} period.
    spec = _spec_with_panels()
    spec["panels"][0]["members"] = [
        {"source": "lisa_2018", "time_key": {"range": {"from": 2018, "to": 2019}}},
        {"source": "lisa_2019", "time_key": {"range": {"from": 2019, "to": 2020}}},
    ]
    result = validate_structural(spec)
    assert result.ok, result.issues


def test_time_range_object_with_token_endpoints_accepted() -> None:
    spec = _spec_with_panels()
    spec["panels"][0]["members"] = [
        {
            "source": "lisa_2018",
            "time_key": {"range": {"from": "2018-01", "to": "2018-06"}},
        },
        {
            "source": "lisa_2019",
            "time_key": {"range": {"from": "2019-01", "to": "2019-06"}},
        },
    ]
    result = validate_structural(spec)
    assert result.ok, result.issues


def test_time_range_duplicate_within_panel() -> None:
    # Two identical range literals collide on the uniqueness rule.
    spec = _spec_with_panels()
    spec["panels"][0]["members"][0]["time_key"] = {"range": {"from": 2018, "to": 2019}}
    spec["panels"][0]["members"][1]["time_key"] = {"range": {"from": 2018, "to": 2019}}
    result = validate_structural(spec)
    assert "literal_time_key_duplicate" in _codes(result)


def test_literal_period_invalid_shape() -> None:
    spec = _spec_with_panels()
    spec["panels"][0]["members"][0]["time_key"] = {"period": "2018", "extra": 1}
    result = validate_structural(spec)
    assert "literal_period_invalid" in _codes(result)


def test_time_range_bad_endpoint_is_literal_period_invalid() -> None:
    # A {"range": ...} with a non-period endpoint isn't a valid literal
    # period object — falls through to literal_period_invalid.
    spec = _spec_with_panels()
    spec["panels"][0]["members"][0]["time_key"] = {"range": {"from": 2018, "to": "no"}}
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
    # When any binding on the source lacks display_name, the structural
    # layer can't be sure the ref doesn't resolve later — skip the
    # check rather than emit a spurious error.
    spec = _spec_with_panels(
        time_key="AR",
        members=["lisa_2018", "lisa_2019"],
    )
    # Drop one display_name on lisa_2018 so all_have_display flips false.
    del spec["sources"][0]["bindings"][0]["display_name"]
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
        spec["sources"][src_idx]["bindings"].append(
            {
                "variable": f"scb/lisa/{col_name.lower()}",
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


def test_member_composite_kind_mismatch_does_not_also_fire_inconsistent() -> None:
    # A composite override whose kind differs from the panel-level
    # composite kind is a kind mismatch, not an ordering inconsistency.
    # Feeding the cross-kind tuple into the §6.8.1 ordering check would
    # double-fire `composite_key_inconsistent` (a ref tuple can never
    # equal a canonicalized literal tuple) and leak the internal literal
    # canonical form into the message. Only the kind-mismatch code fires.
    #
    # Needs ≥2 composites in the accumulator to exercise the ordering
    # check: a string member inherits the panel-level literal composite,
    # the object member overrides with a ref composite.
    spec = _spec_with_panels(
        time_key=[2018, 2019],
        members=["lisa_2018", {"source": "lisa_2019", "time_key": ["AR", "AR"]}],
    )
    result = validate_structural(spec)
    codes = _codes(result)
    assert "time_key_member_kind_mismatch" in codes
    assert "composite_key_inconsistent" not in codes


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


def test_panel_panel_id_null_is_invalid_field_type() -> None:
    spec = _spec_with_panels()
    spec["panels"][0]["panel_id"] = None
    result = validate_structural(spec)
    assert "/panels/0/panel_id" in _at(result, "invalid_field_type")
    assert "/panels/0/panel_id" not in _at(result, "missing_required_field")


def test_panel_members_null_is_invalid_field_type() -> None:
    spec = _spec_with_panels()
    spec["panels"][0]["members"] = None
    result = validate_structural(spec)
    assert "/panels/0/members" in _at(result, "invalid_field_type")
    assert "/panels/0/members" not in _at(result, "missing_required_field")


def test_panel_member_source_null_is_invalid_field_type() -> None:
    spec = _spec_with_panels()
    spec["panels"][0]["members"][0]["source"] = None
    result = validate_structural(spec)
    assert "/panels/0/members/0/source" in _at(result, "invalid_field_type")
    assert "/panels/0/members/0/source" not in _at(result, "missing_required_field")


def test_panel_member_entity_key_override_null_is_invalid_field_type() -> None:
    # An explicit ``"entity_key": null`` on a panel member is not a
    # valid EntityKey shape. Without the explicit-null check, the
    # member would silently inherit the panel default and the
    # malformed override would slip through with ok=True.
    spec = _spec_with_panels()
    spec["panels"][0]["members"][0]["entity_key"] = None
    result = validate_structural(spec)
    assert "/panels/0/members/0/entity_key" in _at(result, "invalid_field_type")


def test_panel_member_time_key_override_null_is_invalid_field_type() -> None:
    # Same semantics as entity_key: explicit null is not a valid
    # TimeKey shape, so emit invalid_field_type and fall back to the
    # panel default for downstream effective-key checks.
    spec = _spec_with_panels(time_key=2018)
    # _spec_with_panels gives members their own time_keys; drop one so
    # the panel default is the only candidate, then set the member's
    # override explicitly to null to exercise the new check.
    spec["panels"][0]["members"][0].pop("time_key", None)
    spec["panels"][0]["members"][0]["time_key"] = None
    result = validate_structural(spec)
    assert "/panels/0/members/0/time_key" in _at(result, "invalid_field_type")


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


# --- Frozenset drift guard ----------------------------------------------
#
# The allowed-key frozensets MUST equal the Pydantic models' WIRE-key sets they
# mirror. Introspect ``Model.model_fields`` here — a closed object growing a
# field in project_data.py without the frozenset following would otherwise
# let the new key fire a false ``unexpected_field``. We compare against the
# WIRE name (``field.alias or name``), NOT the bare Python attribute name: the
# structural validator checks JSON KEYS, so if a field ever gains an alias the
# frozenset must carry the alias (the models are alias-free today, so this is
# future-proofing). This test file may import pydantic freely: only the
# BUNDLE-amalgamated modules must stay pydantic-free; structural.py is build-side.


def _wire_keys(model: type) -> set[str]:
    return {f.alias or n for n, f in model.model_fields.items()}


def test_allowed_key_frozensets_match_pydantic_models() -> None:
    from reg_schema.project_data import Binding, Panel, PanelMember, Source
    from reg_schema.structural import (
        _BINDING_KEYS,
        _PANEL_KEYS,
        _PANEL_MEMBER_KEYS,
        _SOURCE_KEYS,
    )

    assert _wire_keys(Source) == _SOURCE_KEYS
    assert _wire_keys(Binding) == _BINDING_KEYS
    assert _wire_keys(Panel) == _PANEL_KEYS
    assert _wire_keys(PanelMember) == _PANEL_MEMBER_KEYS
