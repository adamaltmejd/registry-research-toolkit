"""Cross-runtime validation contract (REFACTOR_SPEC.md §6.8.0).

The same shape is consumed by three runtimes:

- ``reg_schema`` itself (Python, structural layer §6.8.1).
- ``reg_monabundle`` (Python, amalgamated into the MONA bundle for
  embedded JSON validation at bundle load time).
- The SPA (TypeScript, codegen'd from OpenAPI).

Composition of layers concatenates ``issues`` — no merge semantics
beyond tuple concatenation. Issue ``code`` values are namespaced and
stable across releases; tests pin codes, the SPA maps them to UI
affordances.

Unrelated namesake: ``reg_meta_build.validate.ValidationResult`` is a
mutable CLI report-builder for the build pipeline. Different layer,
different shape; do not conflate.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, get_args

IssueLevel = Literal["error", "warning", "info"]

# Mirrored at runtime because `Literal` is a typing hint, not a runtime
# guard — JSON deserialization (SPA, bundle) and `# type: ignore` paths
# can otherwise smuggle in `"ERROR"` / `"fatal"` and silently flip
# `ValidationResult.ok` to True for a result that should block.
# Derived from `IssueLevel` so the two cannot drift.
_VALID_LEVELS: frozenset[str] = frozenset(get_args(IssueLevel))


@dataclass(frozen=True)
class ValidationIssue:
    level: IssueLevel
    # Stable identifier, e.g. ``"fqid_outside_steward_catalog"``.
    code: str
    # RFC 6901 JSON pointer into ``project_data.json``; empty string for
    # whole-document issues. The SPA uses this to jump to the field.
    path: str
    message: str

    def __post_init__(self) -> None:
        if self.level not in _VALID_LEVELS:
            raise ValueError(
                f"invalid level {self.level!r}; expected one of {sorted(_VALID_LEVELS)}"
            )


@dataclass(frozen=True)
class ValidationResult:
    issues: tuple[ValidationIssue, ...]

    def __post_init__(self) -> None:
        # Coerce list/generator/etc. to tuple so the frozen+hashable
        # contract holds regardless of how callers construct the value.
        # `object.__setattr__` is the standard frozen-dataclass escape.
        if not isinstance(self.issues, tuple):
            object.__setattr__(self, "issues", tuple(self.issues))

    @property
    def ok(self) -> bool:
        # `ok = True` means no error-level issues — it does NOT mean the
        # result was complete. At catalog-load time (§6.8.3) unresolved
        # FQIDs are downgraded to `warning`; affected bindings drop out
        # of the in-memory index but `ok` stays True. Callers that need
        # completeness must inspect the warnings list.
        return not any(i.level == "error" for i in self.issues)
