"""Structural validator for ``project_data.json`` (REFACTOR_SPEC.md §6.8.1).

Pure-stdlib, reg_meta-free. The entrypoint operates on a parsed dict
(typically ``json.loads(...)`` output), not on the §6.1-§6.4
dataclasses, because rules like "type is one of the enum values" must
fire on raw JSON values before any ``Literal`` cast — the dataclass
constructors deliberately don't enforce them (see ``project_data.py``).

Same code is consumed by three runtimes (browser SPA via TS mirror,
MONA bundle via amalgamation, webapp via direct import); see
``DESIGN.md`` for the dependency direction. FQID well-formedness is
checked locally (segment count + per-segment chars) rather than
importing reg_meta — keeps the dependency direction one-way and the
duplicated surface tight.

Issue ``code`` values are stable across releases (§6.8.0): tests pin
codes; the SPA maps them to UI affordances; new codes are additive.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, get_args

from .project_data import ColumnType, IdSubtype, NumericSubtype, Steward
from .validation import ValidationIssue, ValidationResult

# Mirror the §6.1-§6.4 Literal types at runtime so the structural layer
# can't drift from the dataclass declarations. Same drift-protection
# pattern as ``IssueLevel`` in ``validation.py``. Adding a fourth
# steward is a single-point edit in ``project_data.py``.
_STEWARDS: frozenset[str] = frozenset(get_args(Steward))
_COLUMN_TYPES: frozenset[str] = frozenset(get_args(ColumnType))
_ID_SUBTYPES: frozenset[str] = frozenset(get_args(IdSubtype))
_NUMERIC_SUBTYPES: frozenset[str] = frozenset(get_args(NumericSubtype))

_TOP_LEVEL_REQUIRED: tuple[str, ...] = (
    "schema_version",
    "steward",
    "reg_meta_version",
    "name",
    "sources",
)
_TOP_LEVEL_OPTIONAL_BASELINE: tuple[str, ...] = ("panels",)
_TOP_LEVEL_BASELINE: frozenset[str] = frozenset(
    _TOP_LEVEL_REQUIRED + _TOP_LEVEL_OPTIONAL_BASELINE
)

# Subtype/format fields are only valid on the matching column type
# (§6.3). Mapping a field to its owning type keeps the per-field check
# uniform regardless of which subtype the spec adds next.
_SUBTYPE_FIELDS: dict[str, str] = {
    "id_subtype": "id",
    "numeric_subtype": "numeric",
    "date_format": "date",
    "datetime_format": "datetime",
}

# Tight per-segment FQID character class: alphanumerics, hyphen,
# underscore. Wide enough to cover kebab-case slugs (``individer-15plus``),
# the ``_default`` reserved slug, and the version-baked classification
# slugs (``sun2020``). Under Model A the period is no longer an FQID
# segment (§6.2) — it is the ``Source.period`` field, checked separately by
# ``_check_period``. This layer only checks a segment is non-empty and free
# of stray characters (the ``@`` in a binding leaf is split off first).
_FQID_TOKEN: re.Pattern[str] = re.compile(r"^[A-Za-z0-9_-]+$")


# --- Entrypoint ---------------------------------------------------------


def validate_structural(data: Mapping[str, object]) -> ValidationResult:
    """Run §6.8.1 structural rules against ``data``.

    Accepts a ``Mapping`` (typically a dict from ``json.loads``).
    Returns a ``ValidationResult`` whose ``issues`` capture every
    structural problem found. The result is dependency-free: nothing
    here consults reg_meta. Semantic resolution (§6.8.3) and
    namespaced-block validation (§6.8.2) are owned by other layers.
    """

    issues: list[ValidationIssue] = []
    if not isinstance(data, Mapping):
        issues.append(
            _error("invalid_root", "", "project_data.json root must be an object")
        )
        return ValidationResult(issues=tuple(issues))

    _check_top_level_fields(data, issues)
    _check_sources(data.get("sources"), issues)
    _check_panels(data.get("panels"), data.get("sources"), issues)
    _check_namespaced_blocks(data, issues)
    return ValidationResult(issues=tuple(issues))


# --- Issue helpers ------------------------------------------------------


def _error(code: str, path: str, message: str) -> ValidationIssue:
    return ValidationIssue(level="error", code=code, path=path, message=message)


def _present_and_not_null(
    container: Mapping[str, object],
    field: str,
    base: str,
    label: str,
    issues: list[ValidationIssue],
) -> bool:
    """True iff ``field`` is present in ``container`` with a non-null value.

    Emits ``missing_required_field`` when absent and ``invalid_field_type``
    when present-but-null, mirroring the distinction `_check_top_level_fields`
    makes. Without this split, ``"name": null`` reports as "missing" — a
    lie about the actual JSON shape that confuses anyone looking at the
    input alongside the error.
    """
    if field not in container:
        issues.append(
            _error(
                "missing_required_field",
                f"{base}/{field}",
                f"{label} is required",
            )
        )
        return False
    if container[field] is None:
        issues.append(
            _error(
                "invalid_field_type",
                f"{base}/{field}",
                f"{label} must not be null",
            )
        )
        return False
    return True


def _reject_null_override(
    container: Mapping[str, object],
    field: str,
    base: str,
    issues: list[ValidationIssue],
) -> None:
    """Emit ``invalid_field_type`` if ``field`` is explicitly null.

    For optional override fields (panel-member ``entity_key`` /
    ``time_key``): absence means "inherit panel default" and is fine;
    explicit null is not a valid shape and would otherwise be silently
    swallowed by the same ``.get(field)`` pattern used for the absent
    case. Caller still falls through to the inherit-default path so
    downstream effective-key checks have something to compare against.
    """
    if field in container and container[field] is None:
        issues.append(
            _error(
                "invalid_field_type",
                f"{base}/{field}",
                f"panel member {field!r} must not be null when present",
            )
        )


# --- FQID well-formedness (duplicated from reg_meta per DESIGN.md) ------


def _segments_well_formed(segments: list[str]) -> bool:
    return all(bool(_FQID_TOKEN.match(s)) for s in segments)


def _is_register_variant_coord(value: object) -> bool:
    """A 3-part variant coordinate ``<provider>/<register>/<variant>`` (§5.2).

    Not an FQID *kind* (the variant is not addressable as an FQID), but the
    same 3-part grammar. The structural layer only checks shape; resolution
    to a real ``variant`` row is reg_meta's job (§6.8.3).
    """
    if not isinstance(value, str):
        return False
    segs = value.split("/")
    return len(segs) == 3 and segs[0] != "class" and _segments_well_formed(segs)


def _binding_leaf_parts(leaf: str) -> list[str] | None:
    """Split a binding-FQID leaf ``slug[@version]`` into its tokens.

    Returns ``[slug]`` or ``[slug, version]``, or ``None`` when the leaf is
    malformed (empty slug, empty version, or a stray second ``@``). The
    ``@`` is split off *before* the per-segment regex so it never reaches
    the slug grammar (§6.8.1 / §5.2) — ``_FQID_TOKEN`` would reject the ``@``.
    """
    if "@" not in leaf:
        return [leaf] if leaf else None
    slug, _, version = leaf.partition("@")
    if not slug or not version or "@" in version:
        return None
    return [slug, version]


def _parse_binding_fqid(value: object) -> list[str] | None:
    """Parse a binding FQID into ``[provider, register, slug(, version)]``.

    3-segment ``<provider>/<register>/<slug>`` with an optional ``@<version>``
    pin on the leaf (§5.2). Returns the parsed parts (length 3, or 4 when a
    version is pinned), else ``None``. Callers parse once and reuse the parts
    for the prefix and version checks instead of re-splitting the FQID.
    """
    if not isinstance(value, str):
        return None
    segs = value.split("/")
    if len(segs) != 3 or segs[0] == "class":
        return None
    leaf_parts = _binding_leaf_parts(segs[2])
    if leaf_parts is None:
        return None
    parts = [segs[0], segs[1], *leaf_parts]
    return parts if _segments_well_formed(parts) else None


def _is_binding_fqid(value: object) -> bool:
    return _parse_binding_fqid(value) is not None


def _is_classification_fqid(value: object) -> bool:
    if not isinstance(value, str):
        return False
    segs = value.split("/")
    return len(segs) == 2 and segs[0] == "class" and _segments_well_formed(segs)


# --- Period grammar (§6.2) ----------------------------------------------

# Period-token grammar: bare year, year-month, full date, Swedish terms
# (HT/VT), quarters, half-years. The bounds (year 1900-2099, month 01-12,
# day 01-31) mirror the canonical grammar in ``reg_meta.fqid._PERIOD_PATTERNS``.
# reg_schema can't import reg_meta (one-way dep + MONA amalgamation; see
# DESIGN.md), so the grammar is duplicated here — keep the two in sync: a looser
# copy would let a spec pass this structural gate yet fail reg_meta's period
# resolution. The snapshot sentinel ``_default`` is matched separately (it is
# not a token form).
_YEAR = r"(?:19|20)\d{2}"
_MONTH = r"(?:0[1-9]|1[0-2])"
_DAY = r"(?:0[1-9]|[12]\d|3[01])"
_PERIOD_TOKEN: re.Pattern[str] = re.compile(
    rf"^(?:{_YEAR}(?:-{_MONTH}(?:-{_DAY})?|-Q[1-4]|-H[12])?|[HV]T{_YEAR})$"
)


def _is_int_literal(value: object) -> bool:
    # ``bool`` is a subclass of ``int`` in Python; the period grammar
    # never accepts True/False as a literal year, so filter it out.
    return isinstance(value, int) and not isinstance(value, bool)


def _is_period_endpoint(value: object) -> bool:
    """A period int or period-token string (range endpoints; no ``_default``)."""
    return _is_int_literal(value) or (
        isinstance(value, str) and bool(_PERIOD_TOKEN.match(value))
    )


def _is_period_range_obj(value: object) -> bool:
    """A bare ``{"from": ..., "to": ...}`` range with period-token endpoints (§6.2).

    Single definition of the range shape, shared by ``Source.period``
    (``_check_period``) and the ``TimeKey`` ``{"range": ...}`` wrapper
    (``_is_time_range_obj``) — mirroring the Pydantic side, where one
    ``PeriodRange`` model backs both ``Source.period`` and ``TimeRange.range``.
    """
    return (
        isinstance(value, Mapping)
        and set(value.keys()) == {"from", "to"}
        and _is_period_endpoint(value["from"])
        and _is_period_endpoint(value["to"])
    )


def _check_period(period: object, base: str, issues: list[ValidationIssue]) -> None:
    """Validate ``Source.period`` (§6.2): int / period-token / range / sentinel."""
    path = f"{base}/period"
    if _is_period_endpoint(period) or period == "_default":
        return
    if isinstance(period, str):
        issues.append(
            _error(
                "invalid_period",
                path,
                f"period string {period!r} must match a period grammar form "
                "(YYYY, YYYY-MM, YYYY-MM-DD, HTYYYY, VTYYYY, YYYY-Q[1-4], "
                "YYYY-H[12]) or the snapshot sentinel '_default'",
            )
        )
        return
    if isinstance(period, Mapping):
        if _is_period_range_obj(period):
            return
        issues.append(
            _error(
                "invalid_period",
                path,
                "period object must be {'from': ..., 'to': ...} with int or "
                "period-token endpoints",
            )
        )
        return
    issues.append(
        _error(
            "invalid_period",
            path,
            "period must be an int, a period-token string, or a {'from','to'} "
            "range object",
        )
    )


# --- Top-level ----------------------------------------------------------


def _check_top_level_fields(
    data: Mapping[str, object], issues: list[ValidationIssue]
) -> None:
    # Distinguish "absent" from "present-but-null". JSON null deserializes
    # to Python None, which `dict.get` returns for both cases. Without this
    # split, ``"schema_version": null`` bypasses both missing-field and
    # type checks and ``ok`` would return True for a malformed document.
    for field in _TOP_LEVEL_REQUIRED:
        if field not in data:
            issues.append(
                _error(
                    "missing_required_field",
                    f"/{field}",
                    f"required top-level field {field!r} is missing",
                )
            )
        elif data[field] is None:
            issues.append(
                _error(
                    "invalid_field_type",
                    f"/{field}",
                    f"{field} must not be null",
                )
            )

    for field in _TOP_LEVEL_OPTIONAL_BASELINE:
        if field in data and data[field] is None:
            issues.append(
                _error(
                    "invalid_field_type",
                    f"/{field}",
                    f"{field} must not be null when present",
                )
            )

    for field in ("schema_version", "reg_meta_version", "name"):
        value = data.get(field)
        if value is None:
            continue  # absence + nullness already handled above
        if not isinstance(value, str):
            issues.append(
                _error("invalid_field_type", f"/{field}", f"{field} must be a string")
            )

    steward = data.get("steward")
    if steward is None:
        return
    if not isinstance(steward, str):
        issues.append(
            _error("invalid_field_type", "/steward", "steward must be a string")
        )
    elif steward not in _STEWARDS:
        issues.append(
            _error(
                "invalid_enum_value",
                "/steward",
                f"steward must be one of {sorted(_STEWARDS)}; got {steward!r}",
            )
        )


def _check_namespaced_blocks(
    data: Mapping[str, object], issues: list[ValidationIssue]
) -> None:
    # Any non-baseline top-level key is treated as a namespaced block
    # (§6.1). The owning package validates contents (§6.8.2); the
    # structural layer only checks the block is an object.
    for key, value in data.items():
        if key in _TOP_LEVEL_BASELINE:
            continue
        if not isinstance(value, Mapping):
            issues.append(
                _error(
                    "invalid_field_type",
                    f"/{key}",
                    f"namespaced block {key!r} must be an object",
                )
            )


# --- Sources / Columns --------------------------------------------------


def _check_sources(sources: object, issues: list[ValidationIssue]) -> None:
    if sources is None:
        return  # `missing_required_field` already reported by top-level check.
    if not isinstance(sources, list):
        issues.append(
            _error("invalid_field_type", "/sources", "sources must be an array")
        )
        return

    seen_names: dict[str, int] = {}
    for i, source in enumerate(sources):
        base = f"/sources/{i}"
        if not isinstance(source, Mapping):
            issues.append(
                _error("invalid_field_type", base, "source must be an object")
            )
            continue
        _check_source(source, base, i, seen_names, issues)


def _check_source(
    source: Mapping[str, object],
    base: str,
    index: int,
    seen_names: dict[str, int],
    issues: list[ValidationIssue],
) -> None:
    if _present_and_not_null(source, "name", base, "source 'name'", issues):
        name = source["name"]
        if not isinstance(name, str):
            issues.append(
                _error(
                    "invalid_field_type",
                    f"{base}/name",
                    "source 'name' must be a string",
                )
            )
        elif name in seen_names:
            issues.append(
                _error(
                    "duplicate_source_name",
                    f"{base}/name",
                    f"source name {name!r} duplicates /sources/{seen_names[name]}",
                )
            )
        else:
            seen_names[name] = index

    # The variant coordinate's first 2 segments (provider/register) scope
    # every binding's FQID prefix (§6.3). The variant segment itself is NOT
    # repeated on bindings, so only the 2-seg prefix is the cross-field key.
    rv_prefix: list[str] | None = None
    if _present_and_not_null(
        source, "register_variant", base, "source 'register_variant'", issues
    ):
        register_variant = source["register_variant"]
        if not isinstance(register_variant, str):
            issues.append(
                _error(
                    "invalid_field_type",
                    f"{base}/register_variant",
                    "source 'register_variant' must be a string",
                )
            )
        elif not _is_register_variant_coord(register_variant):
            issues.append(
                _error(
                    "invalid_fqid",
                    f"{base}/register_variant",
                    "register_variant must be a 3-part variant coordinate "
                    f"<provider>/<register>/<variant>; got {register_variant!r}",
                )
            )
        else:
            rv_prefix = register_variant.split("/")[:2]

    if _present_and_not_null(source, "period", base, "source 'period'", issues):
        _check_period(source["period"], base, issues)

    if not _present_and_not_null(source, "bindings", base, "source 'bindings'", issues):
        return
    bindings = source["bindings"]
    if not isinstance(bindings, list):
        issues.append(
            _error(
                "invalid_field_type",
                f"{base}/bindings",
                "source 'bindings' must be an array",
            )
        )
        return
    if not bindings:
        issues.append(
            _error(
                "empty_bindings",
                f"{base}/bindings",
                "source must have at least one binding",
            )
        )
        return

    # Per-source explicit-display_name collisions (§6.3
    # `display_name_collision`). The other half of the spec — one
    # explicit + one resolving to the same reg_meta default — needs
    # reg_meta and lives in §6.8.3.
    seen_display_names: dict[str, str] = {}
    for j, binding in enumerate(bindings):
        bbase = f"{base}/bindings/{j}"
        if not isinstance(binding, Mapping):
            issues.append(
                _error("invalid_field_type", bbase, "binding must be an object")
            )
            continue
        _check_binding(binding, bbase, rv_prefix, issues)
        dn = binding.get("display_name")
        if isinstance(dn, str):
            prior = seen_display_names.get(dn)
            if prior is None:
                seen_display_names[dn] = f"{bbase}/display_name"
            else:
                issues.append(
                    _error(
                        "display_name_collision",
                        f"{bbase}/display_name",
                        f"display_name {dn!r} duplicates the one at {prior}",
                    )
                )


def _check_binding(
    binding: Mapping[str, object],
    base: str,
    rv_prefix: list[str] | None,
    issues: list[ValidationIssue],
) -> None:
    # Parsed binding FQID parts ([provider, register, slug(, version)]) once the
    # FQID is well-formed — reused by the prefix and value-set-version checks.
    variable_parts: list[str] | None = None
    if _present_and_not_null(binding, "variable", base, "binding 'variable'", issues):
        variable = binding["variable"]
        if not isinstance(variable, str):
            issues.append(
                _error(
                    "invalid_field_type",
                    f"{base}/variable",
                    "binding 'variable' must be a string",
                )
            )
        else:
            parsed = _parse_binding_fqid(variable)
            if parsed is None:
                issues.append(
                    _error(
                        "invalid_fqid",
                        f"{base}/variable",
                        "binding 'variable' must be a 3-segment binding FQID "
                        f"<provider>/<register>/<slug>[@version]; got {variable!r}",
                    )
                )
            else:
                variable_parts = parsed
                if rv_prefix is not None and parsed[:2] != rv_prefix:
                    issues.append(
                        _error(
                            "fqid_register_variant_mismatch",
                            f"{base}/variable",
                            f"binding FQID prefix {parsed[:2]} must equal the "
                            f"source register_variant prefix {rv_prefix}",
                        )
                    )

    typ_valid: str | None = None
    if _present_and_not_null(binding, "type", base, "binding 'type'", issues):
        typ = binding["type"]
        if not isinstance(typ, str):
            issues.append(
                _error(
                    "invalid_field_type",
                    f"{base}/type",
                    "binding 'type' must be a string",
                )
            )
        elif typ not in _COLUMN_TYPES:
            issues.append(
                _error(
                    "invalid_enum_value",
                    f"{base}/type",
                    f"binding type must be one of {sorted(_COLUMN_TYPES)}; got {typ!r}",
                )
            )
        else:
            typ_valid = typ

    display_name = binding.get("display_name")
    if display_name is not None and not isinstance(display_name, str):
        issues.append(
            _error(
                "invalid_field_type",
                f"{base}/display_name",
                "binding 'display_name' must be a string",
            )
        )

    for field, required_type in _SUBTYPE_FIELDS.items():
        value = binding.get(field)
        if value is None:
            continue
        if typ_valid is not None and typ_valid != required_type:
            issues.append(
                _error(
                    "subtype_on_wrong_type",
                    f"{base}/{field}",
                    f"{field!r} is only valid on type={required_type!r}; "
                    f"binding type is {typ_valid!r}",
                )
            )
            continue
        if not isinstance(value, str):
            issues.append(
                _error(
                    "invalid_field_type",
                    f"{base}/{field}",
                    f"{field} must be a string",
                )
            )
            continue
        if field == "id_subtype" and value not in _ID_SUBTYPES:
            issues.append(
                _error(
                    "invalid_enum_value",
                    f"{base}/id_subtype",
                    f"id_subtype must be one of {sorted(_ID_SUBTYPES)}; got {value!r}",
                )
            )
        elif field == "numeric_subtype" and value not in _NUMERIC_SUBTYPES:
            issues.append(
                _error(
                    "invalid_enum_value",
                    f"{base}/numeric_subtype",
                    f"numeric_subtype must be one of {sorted(_NUMERIC_SUBTYPES)}; "
                    f"got {value!r}",
                )
            )

    value_set = binding.get("value_set")
    if value_set is not None:
        if not isinstance(value_set, str):
            issues.append(
                _error(
                    "invalid_field_type",
                    f"{base}/value_set",
                    "value_set must be a string",
                )
            )
        elif not _is_classification_fqid(value_set):
            issues.append(
                _error(
                    "invalid_fqid",
                    f"{base}/value_set",
                    "value_set must be a 2-segment classification FQID "
                    f"class/<slug>; got {value_set!r}",
                )
            )
        elif variable_parts is not None:
            # §6.8.1: when a binding pins a value-set version via the FQID's
            # `@<version>` suffix AND names a `value_set`, they must agree —
            # the classification slug is version-baked (§5.2), so the `@`
            # version equals the `class/<slug>` leaf. The version is the 4th
            # parsed part when present.
            version = variable_parts[3] if len(variable_parts) == 4 else None
            class_slug = value_set.split("/")[1]
            if version is not None and version != class_slug:
                issues.append(
                    _error(
                        "binding_value_set_version_mismatch",
                        f"{base}/value_set",
                        f"binding pins @{version} but value_set names "
                        f"class/{class_slug}; they must name the same version",
                    )
                )


# --- Panel helpers ------------------------------------------------------


def _is_literal_period_obj(value: object) -> bool:
    if not isinstance(value, Mapping):
        return False
    if set(value.keys()) != {"period"}:
        return False
    period = value["period"]
    # The object form's job is to disambiguate a literal period from a bare
    # column ref; unlike Source.period, the string's period-token validity is
    # NOT grammar-checked here — that is a reg_meta semantic concern (§6.8.3).
    return _is_int_literal(period) or isinstance(period, str)


def _is_time_range_obj(value: object) -> bool:
    """The ``{"range": {"from", "to"}}`` TimePoint form (§6.4).

    Distinct from a bare ``{"from", "to"}`` (which is legal only as a
    ``Source.period``): the ``range`` wrapper keeps the ``TimePoint`` union
    unambiguous. The inner object reuses ``_is_period_range_obj``.
    """
    return (
        isinstance(value, Mapping)
        and set(value.keys()) == {"range"}
        and _is_period_range_obj(value["range"])
    )


def _time_point_kind(value: object) -> str | None:
    """Returns ``'literal'``, ``'ref'``, or ``None`` for malformed input."""
    if _is_int_literal(value):
        return "literal"
    if isinstance(value, str):
        return "ref"
    if _is_literal_period_obj(value) or _is_time_range_obj(value):
        return "literal"
    return None


def _canonicalize_time_literal(value: object) -> object | None:
    """Canonical hashable key for a literal time_key (scalar or composite).

    Returns ``None`` for column-ref or malformed shapes — callers use
    that to skip the uniqueness check.

    Both ``2018`` and ``{"period": 2018}`` collapse to the same key:
    the inner integer/string value, tagged ``"scalar"``. The two forms
    encode the same period — keeping them distinct here would let a
    user trip the uniqueness rule by writing the same year two
    different ways. A ``{"range": ...}`` literal canonicalizes to its
    endpoints so two identical ranges collide.
    """
    if _is_int_literal(value):
        return ("scalar", value)
    if _is_literal_period_obj(value):
        return ("scalar", value["period"])  # type: ignore[index]
    if _is_time_range_obj(value):
        inner = value["range"]  # type: ignore[index]
        return ("scalar", ("range", inner["from"], inner["to"]))
    if isinstance(value, list):
        canon: list[object] = []
        for item in value:
            c = _canonicalize_time_literal(item)
            if c is None:
                return None
            canon.append(c)
        return ("composite", tuple(canon))
    return None


def _build_source_index(sources: object) -> dict[str, dict[str, Any]]:
    """Index sources by name with their explicit display_names.

    ``all_have_display`` is False when any binding on the source lacks
    an explicit ``display_name`` — in that case the bare-string key
    refs may resolve to a reg_meta-derived default at runtime, and the
    structural layer skips the "ref exists on source" check rather
    than emitting spurious errors against a pre-kit/SPA-state spec.
    """
    index: dict[str, dict[str, Any]] = {}
    if not isinstance(sources, list):
        return index
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        name = source.get("name")
        if not isinstance(name, str) or name in index:
            continue
        bindings = source.get("bindings")
        display_names: set[str] = set()
        # `all_have_display` controls whether the panel-member ref-existence
        # check actually fires (see `_check_panel_member`). Flip it False on
        # *any* binding-shape uncertainty — non-list `bindings`, non-mapping
        # binding entries, missing or non-string `display_name` — so a
        # malformed source doesn't get its panel refs flagged as unknown
        # on top of the structural errors already emitted against it.
        all_have_display = isinstance(bindings, list)
        if isinstance(bindings, list):
            for binding in bindings:
                if not isinstance(binding, Mapping):
                    all_have_display = False
                    continue
                dn = binding.get("display_name")
                if isinstance(dn, str):
                    display_names.add(dn)
                else:
                    all_have_display = False
        index[name] = {
            "display_names": display_names,
            "all_have_display": all_have_display,
        }
    return index


def _check_entity_key_shape(
    value: object, path: str, issues: list[ValidationIssue]
) -> bool:
    """Validate EntityKey shape. Returns True if shape is valid."""
    if isinstance(value, str):
        return True
    if isinstance(value, list):
        if not value:
            issues.append(
                _error(
                    "invalid_field_type",
                    path,
                    "entity_key array must be non-empty",
                )
            )
            return False
        ok = True
        for i, item in enumerate(value):
            if not isinstance(item, str):
                issues.append(
                    _error(
                        "invalid_field_type",
                        f"{path}/{i}",
                        "entity_key array elements must be strings",
                    )
                )
                ok = False
        return ok
    issues.append(
        _error(
            "invalid_field_type",
            path,
            "entity_key must be a string or array of strings",
        )
    )
    return False


def _check_time_key_shape(
    value: object, path: str, issues: list[ValidationIssue]
) -> str | None:
    """Validate TimeKey shape. Returns one of:

    - ``'literal_scalar'``  — int, ``{"period": ...}``, or ``{"range": ...}``
    - ``'ref_scalar'``      — bare string (column ref)
    - ``'literal_composite'`` — array of literals
    - ``'ref_composite'``     — array of column refs
    - ``None``              — shape invalid; an issue was emitted

    Composite homogeneity is checked here; the
    ``composite_time_key_mixed_kinds`` code fires when an array mixes
    literal and ref kinds.
    """
    if _is_int_literal(value):
        return "literal_scalar"
    if isinstance(value, str):
        return "ref_scalar"
    if _is_literal_period_obj(value) or _is_time_range_obj(value):
        return "literal_scalar"
    if isinstance(value, Mapping):
        issues.append(
            _error(
                "literal_period_invalid",
                path,
                "time_key object form must be {'period': int|str} or "
                "{'range': {'from','to'}} with period-token endpoints",
            )
        )
        return None
    if isinstance(value, list):
        if not value:
            issues.append(
                _error(
                    "invalid_field_type",
                    path,
                    "time_key array must be non-empty",
                )
            )
            return None
        kinds: set[str] = set()
        shape_ok = True
        for i, item in enumerate(value):
            kind = _time_point_kind(item)
            if kind is None:
                issues.append(
                    _error(
                        "invalid_field_type",
                        f"{path}/{i}",
                        "time_key element must be int, string, {'period': int|str}, "
                        "or {'range': {'from','to'}}",
                    )
                )
                shape_ok = False
                continue
            kinds.add(kind)
        if not shape_ok:
            return None
        if len(kinds) > 1:
            issues.append(
                _error(
                    "composite_time_key_mixed_kinds",
                    path,
                    "composite time_key array must be homogeneous: all "
                    "column refs or all literals",
                )
            )
            return None
        return "literal_composite" if kinds == {"literal"} else "ref_composite"
    issues.append(
        _error(
            "invalid_field_type",
            path,
            "time_key must be int, string, {'period': ...}, or an array",
        )
    )
    return None


def _entity_key_refs(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if isinstance(value, list):
        return tuple(s for s in value if isinstance(s, str))
    return ()


def _time_key_refs(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if isinstance(value, list):
        return tuple(item for item in value if isinstance(item, str))
    return ()


# --- Panels -------------------------------------------------------------


@dataclass
class _PanelScope:
    """Per-panel scope shared with each ``_check_panel_member`` call.

    Bundles the panel-level defaults a member may inherit and the
    cross-member accumulators the panel-level rules read after every
    member has been processed. Mutable on purpose: ``composite_*``,
    ``literal_time_seen``, and ``source_to_panel`` are appended to /
    mutated in place during member iteration.
    """

    panel_base: str
    panel_entity: object
    panel_entity_shape_ok: bool
    panel_time: object
    panel_time_kind: str | None
    source_index: dict[str, dict[str, Any]]
    sources_resolvable: bool
    source_to_panel: dict[str, str]
    composite_entities: list[tuple[str, tuple[str, ...]]]
    composite_times: list[tuple[str, tuple[object, ...]]]
    literal_time_seen: dict[object, str]


def _check_panels(
    panels: object, sources: object, issues: list[ValidationIssue]
) -> None:
    if panels is None:
        return
    if not isinstance(panels, list):
        issues.append(
            _error("invalid_field_type", "/panels", "panels must be an array")
        )
        return

    source_index = _build_source_index(sources)
    # When `/sources` itself is malformed (not a list, or absent),
    # source-name resolution is impossible. Skip per-member
    # `panel_member_unknown_source` emission in that state — the
    # primary `/sources` error is already reported, and cascading
    # secondary errors just obscure it.
    sources_resolvable = isinstance(sources, list)
    source_to_panel: dict[str, str] = {}  # source name -> first panel path
    seen_panel_ids: dict[str, int] = {}

    for pi, panel in enumerate(panels):
        pbase = f"/panels/{pi}"
        if not isinstance(panel, Mapping):
            issues.append(
                _error("invalid_field_type", pbase, "panel must be an object")
            )
            continue
        _check_panel(
            panel,
            pbase,
            pi,
            source_index,
            sources_resolvable,
            source_to_panel,
            seen_panel_ids,
            issues,
        )


def _check_panel(
    panel: Mapping[str, object],
    base: str,
    panel_index: int,
    source_index: dict[str, dict[str, Any]],
    sources_resolvable: bool,
    source_to_panel: dict[str, str],
    seen_panel_ids: dict[str, int],
    issues: list[ValidationIssue],
) -> None:
    if _present_and_not_null(panel, "panel_id", base, "panel 'panel_id'", issues):
        panel_id = panel["panel_id"]
        if not isinstance(panel_id, str):
            issues.append(
                _error(
                    "invalid_field_type",
                    f"{base}/panel_id",
                    "panel 'panel_id' must be a string",
                )
            )
        elif panel_id in seen_panel_ids:
            issues.append(
                _error(
                    "duplicate_panel_id",
                    f"{base}/panel_id",
                    f"panel_id {panel_id!r} duplicates "
                    f"/panels/{seen_panel_ids[panel_id]}",
                )
            )
        else:
            seen_panel_ids[panel_id] = panel_index

    comment = panel.get("comment")
    if comment is not None and not isinstance(comment, str):
        issues.append(
            _error(
                "invalid_field_type",
                f"{base}/comment",
                "panel 'comment' must be a string",
            )
        )

    panel_entity = panel.get("entity_key")
    panel_entity_shape_ok = panel_entity is None or _check_entity_key_shape(
        panel_entity, f"{base}/entity_key", issues
    )

    panel_time = panel.get("time_key")
    panel_time_kind: str | None
    if panel_time is None:
        panel_time_kind = None
    else:
        panel_time_kind = _check_time_key_shape(panel_time, f"{base}/time_key", issues)

    if not _present_and_not_null(panel, "members", base, "panel 'members'", issues):
        return
    members = panel["members"]
    if not isinstance(members, list):
        issues.append(
            _error(
                "invalid_field_type",
                f"{base}/members",
                "panel 'members' must be an array",
            )
        )
        return
    if not members:
        issues.append(
            _error(
                "empty_members",
                f"{base}/members",
                "panel must have at least one member",
            )
        )
        return

    scope = _PanelScope(
        panel_base=base,
        panel_entity=panel_entity,
        panel_entity_shape_ok=panel_entity_shape_ok,
        panel_time=panel_time,
        panel_time_kind=panel_time_kind,
        source_index=source_index,
        sources_resolvable=sources_resolvable,
        source_to_panel=source_to_panel,
        composite_entities=[],
        composite_times=[],
        literal_time_seen={},
    )

    for mi, member in enumerate(members):
        _check_panel_member(member, f"{base}/members/{mi}", scope, issues)

    # Cross-member composite ordering consistency (§6.8.1). Scalar keys
    # may differ across members (the "heterogeneous" example in §6.4),
    # so only composite-vs-composite mismatches fire.
    _check_composite_consistency(scope.composite_entities, "entity_key", issues)
    _check_composite_consistency(scope.composite_times, "time_key", issues)


def _check_composite_consistency(
    composites: list[tuple[str, tuple[object, ...]]],
    label: str,
    issues: list[ValidationIssue],
) -> None:
    if len(composites) < 2:
        return
    first_path, first_tuple = composites[0]
    for path, tup in composites[1:]:
        if tup != first_tuple:
            issues.append(
                _error(
                    "composite_key_inconsistent",
                    path,
                    f"composite {label} {list(tup)} differs from "
                    f"{list(first_tuple)} at {first_path}",
                )
            )


def _check_panel_member(
    member: object,
    mbase: str,
    scope: _PanelScope,
    issues: list[ValidationIssue],
) -> None:
    pbase = scope.panel_base
    member_time_key_overridden = False

    if isinstance(member, str):
        source_name: str | None = member
        eff_entity = scope.panel_entity
        eff_entity_path = f"{pbase}/entity_key"
        eff_entity_shape_ok = scope.panel_entity_shape_ok
        eff_time = scope.panel_time
        eff_time_path = f"{pbase}/time_key"
        eff_time_kind = scope.panel_time_kind
    elif isinstance(member, Mapping):
        source_name = _resolve_member_source(member, mbase, issues)

        # Override fields (entity_key, time_key): distinguish absent
        # from present-but-null. Absent → inherit panel default.
        # Present-but-null → not a valid EntityKey/TimeKey shape; emit
        # invalid_field_type and fall back to the panel default so the
        # rest of the member checks (effective-key presence, ref
        # existence) still have something to compare against.
        _reject_null_override(member, "entity_key", mbase, issues)
        m_entity = member.get("entity_key")
        if m_entity is not None:
            eff_entity = m_entity
            eff_entity_path = f"{mbase}/entity_key"
            eff_entity_shape_ok = _check_entity_key_shape(
                m_entity, eff_entity_path, issues
            )
        else:
            eff_entity = scope.panel_entity
            eff_entity_path = f"{pbase}/entity_key"
            eff_entity_shape_ok = scope.panel_entity_shape_ok

        _reject_null_override(member, "time_key", mbase, issues)
        m_time = member.get("time_key")
        if m_time is not None:
            member_time_key_overridden = True
            eff_time = m_time
            eff_time_path = f"{mbase}/time_key"
            eff_time_kind = _check_time_key_shape(m_time, eff_time_path, issues)
        else:
            eff_time = scope.panel_time
            eff_time_path = f"{pbase}/time_key"
            eff_time_kind = scope.panel_time_kind
    else:
        issues.append(
            _error(
                "invalid_field_type",
                mbase,
                "panel member must be a string (source name) or an object",
            )
        )
        return

    # NOTE: effective-key *presence* is no longer a structural rule (§6.8.1).
    # Under Model A an omitted entity_key/time_key inherits from the member's
    # variant `panel_template` (§6.4), which needs reg_meta — so the
    # "no effective key" case is the semantic `panel_inheritance_unresolvable`
    # check (§6.8.3), raised by kit/bundle-build, not here. A `None` eff_entity
    # / eff_time simply has no refs to check below.

    # Member-vs-panel composite kind match: only fires when both panel
    # default and member override are composite time_keys (§6.4 — scalar
    # kind mixing across members is permitted).
    member_composite_kind_mismatch = (
        member_time_key_overridden
        and scope.panel_time_kind in ("literal_composite", "ref_composite")
        and eff_time_kind in ("literal_composite", "ref_composite")
        and scope.panel_time_kind != eff_time_kind
    )
    if member_composite_kind_mismatch:
        issues.append(
            _error(
                "time_key_member_kind_mismatch",
                f"{mbase}/time_key",
                f"member composite time_key kind {eff_time_kind!r} must "
                f"match panel-level kind {scope.panel_time_kind!r}",
            )
        )

    # Cross-member composite collection (homogeneity-of-order check
    # happens in the caller after every member has been processed).
    if eff_entity_shape_ok and isinstance(eff_entity, list):
        scope.composite_entities.append(
            (eff_entity_path, tuple(s for s in eff_entity if isinstance(s, str)))
        )
    # Skip a member whose composite kind already mismatched the panel-level
    # kind: ref tuples and canonicalized literal tuples can never compare
    # equal, so feeding both into `_check_composite_consistency` would
    # re-report the kind mismatch as a spurious `composite_key_inconsistent`
    # (an ordering error) on top of the `time_key_member_kind_mismatch`
    # already emitted above — and leak the internal canonical literal form.
    if (
        eff_time_kind in ("literal_composite", "ref_composite")
        and isinstance(eff_time, list)
        and not member_composite_kind_mismatch
    ):
        # For ref composites we track the raw tuple of string refs; for
        # literal composites the canonicalized form. Both kinds compare
        # cleanly inside `_check_composite_consistency`.
        if eff_time_kind == "ref_composite":
            tup: tuple[object, ...] = tuple(
                item for item in eff_time if isinstance(item, str)
            )
        else:
            canonical = _canonicalize_time_literal(eff_time)
            tup = canonical[1] if isinstance(canonical, tuple) else ()  # type: ignore[assignment]
        scope.composite_times.append((eff_time_path, tup))

    # Literal time_key uniqueness within the panel (§6.8.1).
    if eff_time_kind in ("literal_scalar", "literal_composite"):
        canon = _canonicalize_time_literal(eff_time)
        if canon is not None:
            if canon in scope.literal_time_seen:
                issues.append(
                    _error(
                        "literal_time_key_duplicate",
                        eff_time_path,
                        f"literal time_key duplicates the one at "
                        f"{scope.literal_time_seen[canon]}",
                    )
                )
            else:
                scope.literal_time_seen[canon] = eff_time_path

    if source_name is None:
        return

    # A member's source must point at a /sources entry. Resolve this
    # *before* the cross-panel bookkeeping so an undefined source
    # reused across panels produces just `panel_member_unknown_source`
    # and not a misleading `source_referenced_by_multiple_panels` on
    # top. When `/sources` itself is malformed, source-name resolution
    # is impossible — the primary error is already reported, so skip
    # this secondary check to keep the output readable.
    entry = scope.source_index.get(source_name)
    if entry is None:
        if scope.sources_resolvable:
            issues.append(
                _error(
                    "panel_member_unknown_source",
                    f"{mbase}/source" if isinstance(member, Mapping) else mbase,
                    f"panel member references source {source_name!r} which is "
                    "not defined in /sources",
                )
            )
        return

    # Cross-panel source-collision (§6.4 "at most one panel"). Two
    # members of the *same* panel sharing a source is a different
    # condition (probably a degenerate panel) — don't fire this code
    # for that, otherwise the path-shaped error message lies about
    # what's wrong.
    prior_panel = scope.source_to_panel.get(source_name)
    if prior_panel is None:
        scope.source_to_panel[source_name] = pbase
    elif prior_panel != pbase:
        issues.append(
            _error(
                "source_referenced_by_multiple_panels",
                mbase,
                f"source {source_name!r} already referenced by panel at {prior_panel}",
            )
        )

    # Column-ref existence against the member's source. Skip when any
    # column on the source has display_name absent — the bare ref may
    # resolve to a reg_meta-derived default later (§6.3).
    if not entry["all_have_display"]:
        return
    display_names: set[str] = entry["display_names"]
    for ref in _entity_key_refs(eff_entity):
        if ref not in display_names:
            issues.append(
                _error(
                    "entity_key_unknown_column",
                    eff_entity_path,
                    f"entity_key {ref!r} does not match any display_name "
                    f"on source {source_name!r}",
                )
            )
    if eff_time_kind in ("ref_scalar", "ref_composite"):
        for ref in _time_key_refs(eff_time):
            if ref not in display_names:
                issues.append(
                    _error(
                        "time_key_unknown_column",
                        eff_time_path,
                        f"time_key {ref!r} does not match any display_name "
                        f"on source {source_name!r}",
                    )
                )


def _resolve_member_source(
    member: Mapping[str, object], mbase: str, issues: list[ValidationIssue]
) -> str | None:
    """Extract and validate ``member['source']``, returning the string or None."""
    if not _present_and_not_null(
        member, "source", mbase, "panel member 'source'", issues
    ):
        return None
    source_raw = member["source"]
    if not isinstance(source_raw, str):
        issues.append(
            _error(
                "invalid_field_type",
                f"{mbase}/source",
                "panel member 'source' must be a string",
            )
        )
        return None
    return source_raw
