# reg_schema

`project_data.json` schema and structural validator. Importable by the webapp, the SPA
(via TS codegen), and any future MONA-side runner.

Python ≥3.14. One runtime dependency: Pydantic v2 (the deliberate exception to the
workspace no-Pydantic rule); the structural validator itself operates on raw dicts and
needs no third-party deps. See [DESIGN.md](DESIGN.md) for scope and dependency
direction.

## Status

v2.0.0 — Model A grammar. The surface: the §6.8.0 cross-runtime contract
(`ValidationIssue`, `ValidationResult`), the Pydantic v2 models (`ProjectData`,
`Source`, `Binding`, `Panel`, `PanelMember`, `Period`, `PeriodRange`, `LiteralPeriod`,
plus the `EntityKey` / `TimeKey` / `TimePoint` aliases), and the unified
`validate_structural()` entrypoint implementing §6.8.1. See [DESIGN.md](DESIGN.md) for
the issue-code table.

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

The validator operates on a parsed dict, not on the Pydantic models, because rules like
"type is one of the enum values" must fire on raw JSON values before any `Literal` cast.
