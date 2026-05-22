# reg_monabundle

MONA bundle builder, bundle runtime, PII scanner, and (eventually) type
compatibility map. See [DESIGN.md](DESIGN.md) for scope, dependency
direction, and the two-half lightweight/runtime split.

`REFACTOR_SPEC.md` §15 step 5. Phase 1 landed the namespaced-block
validator; phase 2a landed the bundle builder; phase 2b landed the
PII scanner; phase 2c relocated the runtime modules under
`reg_monabundle.runtime.*` and gave `build_bundle` default runtime
arguments.

```python
from pathlib import Path

from reg_monabundle import build_bundle, scan_file, validate_block, write_export

validate_block({"column_options": {"scb/lisa/_default/2020/lopnr": {"suppress_k": 25}}})

# Defaults to amalgamating reg_monabundle.runtime.* — no caller wiring needed.
build_bundle(Path("mdw_runner.py"))

# Pre-export atomic write with the PII scanner gating disk creation.
write_export(Path("mock_data_stats.json"), {"sources": []})
# Ad-hoc audit of an existing JSON file.
matches = scan_file(Path("mock_data_stats.json"))
```

## Status

**Phase 2c** of `REFACTOR_SPEC.md` §15 step 5. Lightweight surface:

- `reg_monabundle.validate_block` — §6.8.2 namespaced-block validator.
- `reg_monabundle.constants.SUPPRESS_K` — global k-anonymity floor.
- `reg_monabundle.VALID_OPTION_KEYS` — allowed `column_options` keys.
- `reg_monabundle.build_bundle` — amalgamates the runtime modules +
  reg_schema + reg_monabundle into a single ``.py`` for upload to MONA.
  Defaults to the in-package `reg_monabundle.runtime`; pass
  `runtime_pkg_dir` + `runtime_module_order` to plug a steward-private
  pipeline.
- `reg_monabundle.DEFAULT_OUTPUT_NAME` — default bundle filename
  (`mdw_runner.py`).
- `reg_monabundle.{write_export, scan_file, PIIScannerError}` —
  pre-export PII scanner. `write_export` does atomic stamp + scan +
  rename and is the only file-emitting path inside the bundle;
  `scan_file` re-runs the scanner against an on-disk JSON. The
  remaining scanner internals (`scan_payload`, `scan_string`,
  `ScanMatch`, `SCANNER_VERSION`, `PATTERNS_APPLIED`) live on the
  `reg_monabundle.scan` submodule for callers that need them.

Heavy bundle runtime (`reg_monabundle.runtime.*`): `classify`,
`sql_emit`, `sources`, `summarize`, `spec`, `extract`. Amalgamated
into the bundle by `build_bundle` and executed on MONA's WinPython
env (duckdb / pyodbc / numpy pre-installed there). Local callers
typically never import the runtime tier — the CI gate
([tests/test_lightweight_surface.py](tests/test_lightweight_surface.py))
asserts the top-level surface never pulls it transitively.

Phase 3 wires the 1 MB bundle-size budget gate per §12.
