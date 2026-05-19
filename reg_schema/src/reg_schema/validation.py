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
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

IssueLevel = Literal["error", "warning", "info"]


@dataclass(frozen=True)
class ValidationIssue:
    level: IssueLevel
    # Stable identifier, e.g. ``"fqid_outside_steward_catalog"``.
    code: str
    # RFC 6901 JSON pointer into ``project_data.json``; empty string for
    # whole-document issues. The SPA uses this to jump to the field.
    path: str
    message: str


@dataclass(frozen=True)
class ValidationResult:
    issues: tuple[ValidationIssue, ...]

    @property
    def ok(self) -> bool:
        # `ok = True` means no error-level issues — it does NOT mean the
        # result was complete. At catalog-load time (§6.8.3) unresolved
        # FQIDs are downgraded to `warning`; affected bindings drop out
        # of the in-memory index but `ok` stays True. Callers that need
        # completeness must inspect the warnings list.
        return not any(i.level == "error" for i in self.issues)
