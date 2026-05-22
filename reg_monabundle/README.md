# reg_monabundle

MONA bundle builder, bundle runtime, PII scanner, and (eventually) type
compatibility map. See [DESIGN.md](DESIGN.md) for scope, dependency
direction, and the two-half lightweight/runtime split.

`REFACTOR_SPEC.md` §15 step 5. Phase 1 landed the namespaced-block
validator; phase 2a landed the bundle builder (`build_bundle`).

```python
from pathlib import Path

import mock_data_wizard
from reg_monabundle import build_bundle, validate_block

validate_block({"column_options": {"scb/lisa/_default/2020/lopnr": {"suppress_k": 25}}})

build_bundle(
    Path("mdw_runner.py"),
    runtime_pkg_dir=mock_data_wizard.BUNDLE_PKG_DIR,
    runtime_module_order=mock_data_wizard.BUNDLE_MODULE_ORDER,
)
```

## Status

**Phase 2a** of `REFACTOR_SPEC.md` §15 step 5. Lightweight surface:

- `reg_monabundle.validate_block` — §6.8.2 namespaced-block validator.
- `reg_monabundle.constants.SUPPRESS_K` — global k-anonymity floor.
- `reg_monabundle.VALID_OPTION_KEYS` — allowed `column_options` keys.
- `reg_monabundle.build_bundle` — amalgamates a caller-supplied
  runtime (today: mdw) + reg_schema + reg_monabundle into a single
  ``.py`` for upload to MONA.
- `reg_monabundle.DEFAULT_OUTPUT_NAME` — default bundle filename
  (`mdw_runner.py`).

Phase 2b will land `scan` (PII scanner) and `types.is_compatible`.
Phase 2c will move the bundle-runtime modules (classify, sql_emit,
sources, summarize, spec, extract) under `reg_monabundle.runtime.*`.
Phase 3 wires the 1 MB bundle-size budget gate per §12.
