# reg_schema test corpus

Golden `(input.json, expected_ValidationResult.json)` pairs that pin
the cross-runtime contract for `project_data.json` validation. The
corpus is the single artifact that makes the §6.8.0 `ValidationResult`
shape coherent across the three runtimes that consume it
(`REFACTOR_SPEC.md` §15 step 5.5).

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

Discovery rule: any directory under `test_corpus/` that contains both
files is a case. The directory name is the case ID; it must be a
valid filesystem identifier (`[a-z0-9_]+`) but is otherwise free-form.
Subdirectories that lack one of the two files are ignored, so README
assets and helper files coexist without confusing the runners.

## File formats

### `input.json`

A `project_data.json` payload as authored by a researcher or steward.
Schema is `REFACTOR_SPEC.md` §6.1-§6.4. Both well-formed and
deliberately-malformed inputs live here — the case ID indicates which.
The payload is **not** required to be deserializable into the
`reg_schema` dataclasses; the structural validator accepts and reports
on parsed-dict input directly.

### `expected_ValidationResult.json`

The `ValidationResult` the structural validator must produce for the
paired `input.json`. JSON shape mirrors the `@dataclass` fields in
`reg_schema.validation` (§6.8.0):

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

- `level` ∈ `{"error", "warning", "info"}`. Mis-cased or unknown values
  are rejected at deserialization — see `ValidationIssue.__post_init__`.
- `code` is a namespaced, stable identifier. Tests pin codes; the SPA
  maps codes to UI affordances. New codes are additive.
- `path` is an RFC 6901 JSON pointer into the paired `input.json` root;
  empty string for whole-document issues.
- `message` is English in v1; safe to localize later.

`ValidationResult.ok` is a derived property (no error-level issues), so
it is not serialized — the runners recompute it.

`issues` is unordered for the **set** of expected issues, but the
validator's runtime output is a tuple. Test harnesses compare as
unordered sets (`set(actual.issues) == set(expected_issues)`) so cases
do not pin emission order. If a future use case needs ordering
guarantees, that becomes a separate corpus dimension.

## Three consumers

The three runtimes that validate `project_data.json` all read this
corpus to confirm they produce the same `ValidationResult` for the
same input:

1. **`reg_schema` Python tests** — `reg_schema/tests/test_corpus.py`
   discovers cases and runs `validate_structural()` on each
   `input.json`. Lands as part of §15 step 3 phase 3.
2. **`reg_monabundle` amalgamated bundle** — the bundle build pulls
   the corpus into a self-test that runs on MONA load, so the
   amalgamated copy of the validator stays in sync with the
   reg_schema source. Lands with §15 step 5.
3. **SPA TypeScript tests** — the SPA imports the corpus as JSON
   fixtures and runs its TS port of the structural validator against
   them. Lands with §15 step 6.

All three read the same JSON. If any one diverges, the corpus catches
it before downstream consumers do.

## Growth

The corpus starts with well-formed inputs and an empty-issues
expectation — these prove the format, harness, and round-trip work
end-to-end before §6.8.1 rule-emission cases pile on. Phase 3 of
§15 step 3 grows the corpus alongside `validate_structural()`,
adding one (or more) cases per rule. Negative cases for §6.8.2
(namespaced blocks) and §6.8.3 (reg_meta-backed semantic) layers
land in their owning packages, not here — `reg_schema` only owns
the structural layer's corpus.

## Adding a case

1. Pick a case ID — short, lowercase, snake_case, descriptive.
2. Create `test_corpus/<case_id>/input.json` with the payload.
3. Create `test_corpus/<case_id>/expected_ValidationResult.json` with
   the validator output the structural rules must produce.
4. Run `uv run python -m pytest reg_schema/tests/test_corpus.py`. The
   harness picks up the new case automatically.
