# reg_schema

`project_data.json` schema and structural validator. Importable by
`mock_data_wizard`, the webapp, and (amalgamated) the MONA bundle.

Pure-stdlib Python ≥3.11; no runtime dependencies. See
[DESIGN.md](DESIGN.md) for scope and dependency direction.

## Status

Phase 3 of `REFACTOR_SPEC.md` §15 step 3. Landed so far: the §6.8.0
cross-runtime contract (`ValidationIssue`, `ValidationResult`), the
§6.1-§6.4 shape dataclasses (`ProjectData`, `Source`, `Column`,
`Panel`, `PanelMember`, `LiteralPeriod`, plus the `EntityKey` /
`TimeKey` / `TimePoint` aliases), and the unified
`validate_structural()` entrypoint implementing §6.8.1. See
[DESIGN.md](DESIGN.md) for the issue-code table.

```python
import json

from reg_schema import validate_structural

with open("project_data.json") as f:
    spec = json.load(f)

result = validate_structural(spec)
if not result.ok:
    for issue in result.issues:
        if issue.level == "error":
            print(f"{issue.path}: {issue.code} — {issue.message}")
```

The validator operates on a parsed dict, not on the §6.1-§6.4
dataclasses, because rules like "type is one of the enum values" must
fire on raw JSON values before any `Literal` cast.
