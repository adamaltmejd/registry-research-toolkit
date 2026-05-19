# reg_schema

`project_data.json` schema and structural validator. Importable by
`mock_data_wizard`, the webapp, and (amalgamated) the MONA bundle.

Pure-stdlib Python ≥3.11; no runtime dependencies. See
[DESIGN.md](DESIGN.md) for scope and dependency direction.

## Status

Phase 1 of `REFACTOR_SPEC.md` §15 step 3 — only the §6.8.0 cross-runtime
contract (`ValidationIssue`, `ValidationResult`) has landed. Top-level
`ProjectData`, `Source` / `Column`, `Panel`, and the unified
`validate_structural()` entrypoint follow in subsequent phases.

```python
from reg_schema import ValidationIssue, ValidationResult

result = ValidationResult(issues=(
    ValidationIssue(level="error", code="fqid_unresolved", path="/sources/0/name", message="..."),
))
assert result.ok is False
```
