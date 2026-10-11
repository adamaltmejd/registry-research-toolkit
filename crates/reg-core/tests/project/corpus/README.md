# Structural validation corpus

Golden `(input.json, expected_ValidationResult.json)` pairs that pin the contract for
`project_data.json` structural validation: `reg_core::validate_structural`, which the
server's validate and order operations run first. The retired `reg_schema` validator
wrote most cases; the rest pin the intentional differences named in `src/structural.rs`
and shapes only the project types need.

## Layout

Each case is one subdirectory:

```text
crates/reg-core/tests/project/corpus/
├── README.md
├── <case_id>/
│   ├── input.json                       # a project_data.json payload
│   └── expected_ValidationResult.json   # the ValidationResult the
│                                        # validator must produce
└── ...
```

Every subdirectory is a case. The directory name is the case ID, in lowercase snake_case
(`[a-z0-9_]+`).

## File formats

### `input.json`

A `project_data.json` payload as authored by a researcher or steward (Model A:
`register_variant` 3-part coordinate + `period`, `bindings` with 3-segment binding
FQIDs, 2-segment `class/<slug>` value sets). Both well-formed and deliberately malformed
inputs live here; the case ID says which. The validator reads any JSON value, so a
payload need not deserialize into the project types.

A string `schema_version` is the current contract version
(`reg_core::project::SCHEMA_VERSION`), so every input reads as a project the validate
operation would accept. Structural validation ignores the value, so a contract bump
re-authors the corpus by rewriting that one line. Only cases about the field's shape
(absent, null, non-string, or a non-object root) carry anything else.

### `expected_ValidationResult.json`

The `ValidationResult` the structural validator must produce for the paired
`input.json`:

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

- `level` ∈ `{"error", "warning", "info"}`.
- `code` is a namespaced, stable identifier. Tests pin codes; the SPA maps codes to UI
  affordances. New codes are additive.
- `path` is an RFC 6901 JSON pointer into the paired `input.json` root; empty string for
  whole-document issues.
- `message` is English in v1; safe to localize later.

`ValidationResult.ok` is derived (no error-level issues), so it is not serialized. The
issues are compared in emission order.

## Consumers

1. **`reg-core`** — `tests/project.rs` runs `validate_structural` on every case
   (`structural_corpus`) and hashes every accepted project (`project_hashes`, keyed in
   `../hashes.json`).
2. **SPA TypeScript tests** — `reg_webapp/frontend/src/lib/reg_core.test.ts` runs every
   case through `reg-core-wasm`'s `check_project` (the browser's copy of the server's
   structural door), and `validation.test.ts` checks that the SPA can locate each
   expected issue's path and names each code.

## Adding a case

1. Pick a case ID — short, lowercase, snake_case, descriptive.
2. Create `<case_id>/input.json` with the payload.
3. Create `<case_id>/expected_ValidationResult.json` with the validator output the
   structural rules must produce.
4. Run `cargo test -p reg-core --test project`. An accepted project also needs its hash
   in `../hashes.json`, from `reg_core::project::project_hash`.
