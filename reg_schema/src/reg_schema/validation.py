"""Cross-runtime validation contract (see DESIGN.md → Structural rules and issue codes).

The same shape is consumed by multiple runtimes:

- ``reg_schema`` itself (Python, structural layer).
- the ``reg_webapp`` API ingress (Python).
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
from typing import Literal

# A typing hint only, like the tuple `issues`: every producer passes a literal
# level and a tuple (checked by ty), and no product path decodes JSON into these
# dataclasses. JSON is checked where it is read (see DESIGN.md → What this layer
# does NOT validate).
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
    # Optional structured successor hint for semantic succession findings. Kept
    # as a scalar field (rather than a mutable dict payload) so the frozen issue
    # remains hashable for set/equality-based tests.
    successor_fqid: str | None = None


@dataclass(frozen=True)
class ValidationResult:
    issues: tuple[ValidationIssue, ...]

    @property
    def ok(self) -> bool:
        # `ok = True` means no error-level issues — warnings and infos never
        # flip it. It is NOT a clean bill of health: the semantic layer (see
        # reg_meta/DESIGN.md → Project semantic validation (semantic.py))
        # reports a binding the steward does not hold as a `warning`
        # (`*_outside_steward_catalog`) and an availability clip as `info`,
        # and a valid project is resolvable, not proven orderable — the order
        # materializer still gates physical coverage. Callers that care must
        # inspect the non-error issues.
        return not any(i.level == "error" for i in self.issues)
