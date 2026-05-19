# reg_schema

`project_data.json` schema and structural validator. Importable by
`mock_data_wizard`, the webapp, and (amalgamated) the MONA bundle.

Pure-stdlib Python ≥3.11; no runtime dependencies. See
[DESIGN.md](DESIGN.md) for scope and dependency direction.

## Status

Phase 2 of `REFACTOR_SPEC.md` §15 step 3. Landed so far: the §6.8.0
cross-runtime contract (`ValidationIssue`, `ValidationResult`) and
the §6.1-§6.4 shape dataclasses (`ProjectData`, `Source`, `Column`,
`Panel`, `PanelMember`, `LiteralPeriod`, plus the `EntityKey` /
`TimeKey` / `TimePoint` aliases). The unified `validate_structural()`
entrypoint follows in the next phase.

```python
from reg_schema import (
    Column,
    IssueLevel,
    ProjectData,
    Source,
    ValidationIssue,
    ValidationResult,
)

spec = ProjectData(
    schema_version="1.0.0",
    steward="global",
    reg_meta_version="reg_meta/v0.11.1",
    name="demo",
    sources=(
        Source(
            name="lisa_2018",
            register_version="scb/lisa/individer-15plus/2018",
            columns=(
                Column(
                    name="scb/lisa/individer-15plus/2018/kon",
                    type="categorical",
                    value_set="class/sun/2020",
                ),
            ),
        ),
    ),
)

level: IssueLevel = "error"
result = ValidationResult(issues=(
    ValidationIssue(level=level, code="fqid_unresolved", path="/sources/0/name", message="..."),
))
assert result.ok is False
```
