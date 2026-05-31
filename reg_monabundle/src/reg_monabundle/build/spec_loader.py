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

if TYPE_CHECKING:
    from collections.abc import Mapping

    from reg_monabundle.runtime.spec import LoadedSpec

PROJECT_DATA_FILENAME = "project_data.json"


def _validate_column_options_against_columns(
    block: object, project_data: reg_schema.ProjectData
) -> None:
    """Cross-check ``column_options`` keys against actual columns.

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
    does not re-run these checks (§9.6).
    """
    if not isinstance(block, dict):
        return
    block_obj = cast("Mapping[str, Any]", block)
    options = block_obj.get("column_options")
    if not isinstance(options, dict):
        return
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
        raise ValueError(
            f"reg_monabundle.column_options has key(s) that don't match "
            f"any binding FQID in sources: {orphans}. Check for typos "
            f"against the binding FQIDs declared in sources[*].bindings[*].variable."
        )
    for fqid, opts in options.items():
        if "suppress_k" not in opts:
            continue
        non_categorical = sorted(
            {b.type for b in bindings_by_fqid[fqid] if b.type != "categorical"}
        )
        if non_categorical:
            raise ValueError(
                f"reg_monabundle.column_options[{fqid!r}].suppress_k is only "
                f"honored on categorical bindings, but this FQID is bound as "
                f"{non_categorical} in at least one source — suppress_k is a "
                f"no-op there. The runtime applies suppress_k to the categorical "
                f"frequency cutoff only. For panel-level k-anonymity tunability "
                f"see panels[*].suppress_k (not yet implemented)."
            )


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
