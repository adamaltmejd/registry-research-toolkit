# reg_monabundle

MONA bundle builder, bundle runtime, PII scanner, and (eventually) type
compatibility map. See [DESIGN.md](DESIGN.md) for scope, dependency
direction, and the two-half lightweight/runtime split.

`REFACTOR_SPEC.md` §15 step 5. Phase 1 landed the namespaced-block
validator; phase 2a landed the bundle builder; phase 2b landed the
PII scanner.

```python
from pathlib import Path

import mock_data_wizard
from reg_monabundle import build_bundle, scan_file, validate_block, write_export

validate_block({"column_options": {"scb/lisa/_default/2020/lopnr": {"suppress_k": 25}}})

build_bundle(
    Path("mdw_runner.py"),
    runtime_pkg_dir=mock_data_wizard.BUNDLE_PKG_DIR,
    runtime_module_order=mock_data_wizard.BUNDLE_MODULE_ORDER,
)

# Pre-export atomic write with the PII scanner gating disk creation.
write_export(Path("mock_data_stats.json"), {"sources": []})
# Ad-hoc audit of an existing JSON file.
matches = scan_file(Path("mock_data_stats.json"))
```

## Status

**Phase 2b** of `REFACTOR_SPEC.md` §15 step 5. Lightweight surface:

- `reg_monabundle.validate_block` — §6.8.2 namespaced-block validator.
- `reg_monabundle.constants.SUPPRESS_K` — global k-anonymity floor.
- `reg_monabundle.VALID_OPTION_KEYS` — allowed `column_options` keys.
- `reg_monabundle.build_bundle` — amalgamates a caller-supplied
  runtime (today: mdw) + reg_schema + reg_monabundle into a single
  ``.py`` for upload to MONA.
- `reg_monabundle.DEFAULT_OUTPUT_NAME` — default bundle filename
  (`mdw_runner.py`).
- `reg_monabundle.scan` — pre-export PII scanner. Public surface:
  `write_export` (the atomic stamp + scan + rename used inside the
  bundle), `scan_file` / `scan_payload` / `scan_string` (the standalone
  helpers), and `PIIScannerError` / `ScanMatch` / `SCANNER_VERSION` /
  `PATTERNS_APPLIED`.

Phase 2c will move the bundle-runtime modules (classify, sql_emit,
sources, summarize, spec, extract) under `reg_monabundle.runtime.*`.
Phase 3 wires the 1 MB bundle-size budget gate per §12.
