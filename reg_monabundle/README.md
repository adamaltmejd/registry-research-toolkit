# reg_monabundle

MONA bundle builder, bundle runtime, PII scanner, and (eventually) type
compatibility map. See [DESIGN.md](DESIGN.md) for scope, dependency
direction, and the two-half lightweight/runtime split.

`REFACTOR_SPEC.md` §15 step 5. Phase 1 (this scaffold) lands the
`reg_monabundle` namespaced-block validator that previously lived inline
in `mock_data_wizard.spec`. The bundle builder
(`mock_data_wizard._bundle`), PII scanner (`mock_data_wizard.scan`),
and bundle-runtime modules move here in phase 2.

```python
from reg_monabundle import validate_block

validate_block({"column_options": {"scb/lisa/_default/2020/lopnr": {"suppress_k": 25}}})
```

## Status

**Phase 1** of `REFACTOR_SPEC.md` §15 step 5. Lightweight surface only:

- `reg_monabundle.validate_block` — §6.8.2 namespaced-block validator.
- `reg_monabundle.constants.SUPPRESS_K` — global k-anonymity floor.
- `reg_monabundle.VALID_OPTION_KEYS` — allowed `column_options` keys.

Phase 2 will land `build`, `scan`, `types`, and the
`reg_monabundle.runtime.*` amalgamation modules. Phase 3 wires the 1 MB
bundle-size budget gate per §12.
