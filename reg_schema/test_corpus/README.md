# reg_schema test corpus

Golden `(input.json, expected_ValidationResult.json)` pairs that pin the cross-runtime
contract for `project_data.json` validation. The corpus is the single artifact that
makes the §6.8.0 `ValidationResult` shape coherent across every runtime that consumes
it.

## Layout

Each case is one subdirectory:

```text
reg_schema/test_corpus/
├── README.md
├── <case_id>/
│   ├── input.json                       # a project_data.json payload
│   └── expected_ValidationResult.json   # the ValidationResult the
│                                        # validator must produce
└── ...
```

Discovery rule: any directory under `test_corpus/` that contains both files is a case.
The directory name is the case ID; recommended form is lowercase snake_case
(`[a-z0-9_]+`) to keep IDs portable across runtimes, but the harness does not enforce
this — any directory name the filesystem accepts will be picked up. Subdirectories that
lack one of the two files are ignored, so README assets and helper files coexist without
confusing the runners.

## File formats

### `input.json`

A `project_data.json` payload as authored by a researcher or steward. Schema is the
`project_data.json` model (see reg_schema/DESIGN.md → Two layers: models vs. validator)
(Model A: `register_variant` 3-part coordinate + `period`, `bindings` with 3-segment
binding FQIDs, 2-segment `class/<slug>` value sets). Both well-formed and
deliberately-malformed inputs live here — the case ID indicates which. The payload is
**not** required to be deserializable into the `reg_schema` Pydantic models; the
structural validator accepts and reports on parsed-dict input directly.

A string `schema_version` must be the current contract version
(`reg_schema.__version__`) so every input reads as a project the `/validate` door would
accept; the harness fails on a stale one. Structural validation ignores the value, so a
contract bump re-authors the corpus by rewriting that one line. Only cases about the
field's shape (absent, null, non-string, or a non-object root) carry anything else.

### `expected_ValidationResult.json`

The `ValidationResult` the structural validator must produce for the paired
`input.json`. JSON shape mirrors the `@dataclass` fields in `reg_schema.validation`
(§6.8.0):

```json
{
  "issues": [
    {
      "level": "error",
      "code": "<stable_identifier>",
      "path": "<RFC_6901_JSON_pointer>",
      "message": "<human readable>"
    }
  ]
}
```

- `level` ∈ `{"error", "warning", "info"}`. Mis-cased or unknown values are rejected at
  deserialization: the Python harness refuses them when it decodes the file.
- `code` is a namespaced, stable identifier. Tests pin codes; the SPA maps codes to UI
  affordances. New codes are additive.
- `path` is an RFC 6901 JSON pointer into the paired `input.json` root; empty string for
  whole-document issues.
- `message` is English in v1; safe to localize later.

`ValidationResult.ok` is a derived property (no error-level issues), so it is not
serialized — the runners recompute it.

`issues` is unordered for the **set** of expected issues, but the validator's runtime
output is a tuple. Test harnesses compare as unordered sets
(`set(actual.issues) == set(expected_issues)`) so cases do not pin emission order. If a
future use case needs ordering guarantees, that becomes a separate corpus dimension.

## Consumers

1. **`reg_schema` Python tests** — `reg_schema/tests/test_corpus.py` discovers cases and
   runs `validate_structural()` on each `input.json`.
2. **SPA TypeScript tests** — `reg_webapp/frontend/src/lib/validation.test.ts` imports a
   case's `expected_ValidationResult.json` directly to pin the issue shape the SPA
   decodes.

Any other runtime that re-validates `project_data.json` is expected to read the same
corpus. All consumers read the same JSON, so if any one diverges, the corpus catches it
before downstream consumers do.

## Coverage

The corpus is the oracle for every structural rule: one or more cases per rule, positive
and negative, each a whole payload with its complete expected issue set. Case names
state the behavior; a rule exercised over several values carries the value as a
`__<value>` suffix (`period_out_of_bounds_tokens_are_invalid__2018_q5`). Negative cases
for §6.8.3 (reg_meta-backed semantic) rules land in their owning packages, not here —
`reg_schema` only owns the structural layer's corpus.

## Adding a case

1. Pick a case ID — short, lowercase, snake_case, descriptive.
2. Create `test_corpus/<case_id>/input.json` with the payload.
3. Create `test_corpus/<case_id>/expected_ValidationResult.json` with the validator
   output the structural rules must produce.
4. Run `uv run python -m pytest reg_schema/tests/test_corpus.py`. The harness picks up
   the new case automatically.
