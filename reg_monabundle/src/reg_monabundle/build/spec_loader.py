"""Build-time structural validation + ``LoadedSpec`` conversion (§9.6).

This is the **Pydantic side** of the §9.6 boundary. It imports
``reg_schema`` and is **never amalgamated into the bundle** — it runs at
bundle-build time only:

- ``validate_project_data`` is the structural-validation **gate**
  (§6.8.1): it runs the full Pydantic ``reg_schema`` validator on the
  input ``project_data.json``, runs the ``reg_monabundle`` namespaced-
  block validator (§6.8.2), and runs the cross-block referential checks
  (orphan FQID, suppress_k-on-non-categorical) that need FQID-typed
  bindings. A structurally broken spec fails *here*, at build, never on
  MONA. Returns the validated Pydantic ``reg_schema.ProjectData``.
- ``project_data_to_loadedspec`` is the conversion boundary: it turns a
  validated Pydantic ``ProjectData`` into the stdlib-dataclass
  ``LoadedSpec`` the bundle runtime consumes. It defers to
  ``reg_monabundle.runtime.spec.loadedspec_from_dict`` so the
  dict->dataclass deserialization (and its step-4 capability gates)
  lives in exactly one place.

Kept out of ``reg_monabundle.build.__init__`` imports on purpose: the
``build`` package must stay importable without pulling
``reg_monabundle.runtime.*`` (MONA-only duckdb/pyodbc). The runtime
import in ``project_data_to_loadedspec`` is therefore lazy (inside the
function body), and nothing in ``build/__init__`` imports this module.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import reg_schema
from reg_schema import ValidationIssue

if TYPE_CHECKING:
    from collections.abc import Mapping

    from reg_monabundle.runtime.spec import LoadedSpec

PROJECT_DATA_FILENAME = "project_data.json"

# Canonical issue codes for the build-side cross-block referential checks
# (§6.8.3-adjacent; build-time only). These mirror the long-standing raises
# in ``validate_project_data`` — PR1 adds the issue-based forms; PR2 rewires
# ``reg_webapp`` to consume issues directly instead of catching the raise.
COLUMN_OPTIONS_ORPHAN_CODE = "column_options_orphan_fqid"
SUPPRESS_K_NON_CATEGORICAL_CODE = "suppress_k_on_non_categorical"
# The §6.8.2 ``reg_monabundle`` namespaced-block validator
# (``validate_block``) is raise-based, pure-stdlib, and amalgamated into the
# MONA bundle (it must stay reg_schema-free — see ``validate.py``). This
# build-side form translates its single raise into one ``ValidationIssue``.
BLOCK_INVALID_CODE = "invalid_block"


def _json_pointer_escape(token: str) -> str:
    """RFC 6901 escape a JSON-pointer reference token: ``~`` → ``~0``, ``/`` →
    ``~1`` (in that order). A binding FQID is the ``column_options`` map KEY and
    contains ``/`` (``provider/register/slug``), so it must be escaped or the
    ``ValidationIssue.path`` is a malformed pointer the SPA can't resolve."""
    return token.replace("~", "~0").replace("/", "~1")


def _orphan_message(orphans: list[str]) -> str:
    return (
        f"reg_monabundle.column_options has key(s) that don't match "
        f"any binding FQID in sources: {orphans}. Check for typos "
        f"against the binding FQIDs declared in sources[*].bindings[*].variable."
    )


def _suppress_k_non_categorical_message(fqid: str, non_categorical: list[str]) -> str:
    return (
        f"reg_monabundle.column_options[{fqid!r}].suppress_k is only "
        f"honored on categorical bindings, but this FQID is bound as "
        f"{non_categorical} in at least one source — suppress_k is a "
        f"no-op there. The runtime applies suppress_k to the categorical "
        f"frequency cutoff only. For panel-level k-anonymity tunability "
        f"see panels[*].suppress_k (not yet implemented)."
    )


def column_options_issues(
    block: object, project_data: reg_schema.ProjectData
) -> list[ValidationIssue]:
    """Cross-check ``column_options`` keys against actual columns, as issues.

    Two checks, both requiring access to the FQID-typed bindings:

    1. **Orphan keys.** Well-formedness (3-segment, non-class,
       ``[A-Za-z0-9_-]+`` with optional ``@version`` leaf) is checked in
       ``reg_monabundle.validate_block``; that catches typos that
       mangle the shape but not typos where the shape survives and
       the key just doesn't match any column. Without this check, a
       misspelled FQID silently no-ops at lookup time.

    2. **Per-option type compatibility.** ``suppress_k`` only feeds
       ``_suppress_below_k`` (categorical frequency cutoff) in
       ``summarize_column``; the id / numeric / date / opaque
       branches ignore it. Accepting it on those types would silently
       no-op the same way an orphan FQID would. Future panel-level
       k-anonymity tunability lives at ``panels[*].suppress_k`` (not
       yet implemented), not here.

    Build-time only: the bundle on MONA trusts the embedded JSON and
    does not re-run these checks (§9.6). The message text is shared with
    the raising path in ``validate_project_data`` so the issue and the
    raise can never drift; ``path`` is the RFC 6901 pointer into the
    ``reg_monabundle.column_options`` map.
    """
    issues: list[ValidationIssue] = []
    if not isinstance(block, dict):
        return issues
    block_obj = cast("Mapping[str, Any]", block)
    options = block_obj.get("column_options")
    if not isinstance(options, dict):
        return issues
    # A binding FQID is the 3-seg variable identity (the period left the FQID
    # in Model A), so the SAME FQID is bound once per period-source. Collect
    # ALL bindings per FQID — checking only one (whichever source came last)
    # would let a suppress_k on a mixed-type FQID slip past against a sibling
    # source where it is a no-op (Codex P2 #155).
    bindings_by_fqid: dict[str, list[reg_schema.Binding]] = {}
    for source in project_data.sources:
        for binding in source.bindings:
            bindings_by_fqid.setdefault(binding.variable, []).append(binding)
    orphans = sorted(set(options) - set(bindings_by_fqid))
    if orphans:
        issues.append(
            ValidationIssue(
                level="error",
                code=COLUMN_OPTIONS_ORPHAN_CODE,
                path="/reg_monabundle/column_options",
                message=_orphan_message(orphans),
            )
        )
    for fqid, opts in options.items():
        if "suppress_k" not in opts or fqid not in bindings_by_fqid:
            # Orphan FQIDs are already reported above; skip the type check
            # (there are no bindings to inspect for them).
            continue
        # ``b.type`` is a ``ColumnType`` Literal; widen to ``str`` for the
        # message helper. The string VALUES are identical, so the rendered
        # ``{non_categorical}`` text matches the original raise byte-for-byte.
        non_categorical = sorted(
            {str(b.type) for b in bindings_by_fqid[fqid] if b.type != "categorical"}
        )
        if non_categorical:
            issues.append(
                ValidationIssue(
                    level="error",
                    code=SUPPRESS_K_NON_CATEGORICAL_CODE,
                    path=(
                        "/reg_monabundle/column_options/"
                        f"{_json_pointer_escape(fqid)}/suppress_k"
                    ),
                    message=_suppress_k_non_categorical_message(fqid, non_categorical),
                )
            )
    return issues


def block_issue(block: object) -> ValidationIssue | None:
    """Translate ``validate_block``'s raise into a single ``ValidationIssue``.

    ``validate_block`` (§6.8.2) is raise-based, pure-stdlib, and amalgamated
    into the MONA bundle, so it stays reg_schema-free and cannot itself
    return ``ValidationIssue``. This build-side adapter runs it and, on the
    first violation, wraps the ``ValueError`` message into one issue (code
    ``invalid_block``). Returns ``None`` when the block is clean. The message
    is the validator's own text verbatim, so the issue and the bundle's raise
    can never drift.
    """
    from reg_monabundle import validate_block

    try:
        validate_block(block)
    except ValueError as exc:
        return ValidationIssue(
            level="error",
            code=BLOCK_INVALID_CODE,
            path="/reg_monabundle",
            message=str(exc),
        )
    return None


def _validate_column_options_against_columns(
    block: object, project_data: reg_schema.ProjectData
) -> None:
    """Raising wrapper over ``column_options_issues`` (build gate).

    Computes the issues, then raises off the first one so
    ``validate_project_data`` keeps its message-stable fail-fast contract
    (the substrings ``don't match any binding FQID`` and ``only honored on
    categorical`` are pinned by ``test_spec_loader.py``). The issue form is
    the source of truth — both paths share ``_orphan_message`` /
    ``_suppress_k_non_categorical_message`` so they cannot drift.
    """
    issues = column_options_issues(block, project_data)
    if issues:
        raise ValueError(issues[0].message)


def validate_project_data(payload: Mapping[str, Any]) -> reg_schema.ProjectData:
    """Structural-validation gate (§6.8.1) — run at bundle-build time.

    Runs the full Pydantic ``reg_schema`` structural validator, the
    ``reg_monabundle`` namespaced-block validator (§6.8.2), and the
    cross-block referential checks, then returns the validated Pydantic
    ``reg_schema.ProjectData``. Raises ``ValueError`` on any structural
    failure so bundle-build refuses to amalgamate a broken spec.
    """
    result = reg_schema.validate_structural(payload)
    if not result.ok:
        errors = [
            f"{issue.code} @ {issue.path}: {issue.message}"
            for issue in result.issues
            if issue.level == "error"
        ]
        raise ValueError(
            f"{PROJECT_DATA_FILENAME} failed structural validation:\n  - "
            + "\n  - ".join(errors)
        )
    # The reg_monabundle block validator (§6.8.2) is pure-Python and also
    # runs in the bundle's lightweight surface — but the cross-block
    # referential checks below need the FQID-typed bindings, so both
    # run here at the build gate.
    reg_schema_block = payload.get("reg_monabundle")
    from reg_monabundle import validate_block

    validate_block(reg_schema_block)
    project_data = reg_schema.ProjectData.model_validate(payload)
    _validate_column_options_against_columns(reg_schema_block, project_data)
    return project_data


def project_data_to_loadedspec(project_data: reg_schema.ProjectData) -> LoadedSpec:
    """Convert a validated Pydantic ``ProjectData`` into a stdlib ``LoadedSpec``.

    The §9.6 conversion boundary: the Pydantic model is dumped back to a
    plain dict (``by_alias=True`` so ``period``'s ``from`` alias and the
    discriminated time-key wrappers round-trip) and deserialized by the
    runtime's ``loadedspec_from_dict``. Routing through the same
    deserializer the bundle uses keeps the dict->dataclass mapping (and
    its step-4 capability gates) in one place.

    The runtime import is lazy so importing ``reg_monabundle.build`` (the
    local amalgamator) never pulls ``reg_monabundle.runtime.*`` — those
    modules need MONA-only duckdb/pyodbc.
    """
    from reg_monabundle.runtime.spec import loadedspec_from_dict

    return loadedspec_from_dict(project_data.model_dump(by_alias=True))
