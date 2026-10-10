# Grammar corpus

The oracle for the FQID and period grammars of `RUST_RUNTIME_SPEC.md` sections 5 and 7,
read by `crates/reg-core/tests/grammar.rs` (`cargo test --workspace`) and, the period
files, by the SPA's unit suite through `reg-core-wasm`
(`reg_webapp/frontend/src/lib/reg_core.test.ts`). The files are hand-written from the
grammar in `crates/DESIGN.md` → "FQID grammar", not generated from an implementation.

## Format

One JSON object per line. An accepted input is `{"in": <string>, "out": <value>}`; a
refused one is `{"in": <string>, "error": <code>}`, where the code is the
`conformance/api/errors.toml` code the request gets. An optional `note` explains a case.
Every accepted input is the canonical spelling of its value: rendering `out` gives back
`in`.

### `fqid.jsonl`

Refusals are `invalid_ref`. `out` is one of:

- `{"kind": "provider", "provider"}` — `scb`
- `{"kind": "register", "provider", "register"}` — `scb/lisa`
- `{"kind": "variable", "provider", "register", "variable"}` — `scb/lisa/kon`
- `{"kind": "classification", "classification"}` — `class/sun2020`

Every slug matches `^[a-z](?:-?[a-z0-9])*$` and is not `class`, which is reserved in
every slot. `group` is reserved as the first segment, for group refs
(`conformance/api/operations.toml`, `show`).

### `period.jsonl`

Refusals are `invalid_period`. `out` is a token or a range; years are 1900–2099:

- `{"kind": "year", "year"}` — `2019`
- `{"kind": "month", "year", "month"}` — `2019-03`
- `{"kind": "day", "year", "month", "day"}` — `2019-01-31`, a real calendar day
- `{"kind": "term", "term": "HT" | "VT", "year"}` — `HT2020` (July to December),
  `VT2020` (January to June)
- `{"kind": "school_year", "year"}` — `LA2019`, July 2019 through June 2020
- `{"kind": "quarter", "year", "quarter"}` — `2019-Q3`
- `{"kind": "half", "year", "half"}` — `2019-H1`
- `{"kind": "range", "from": <token>, "to": <token>}` — `2015..2019`; the first day of
  `from` is not after the last day of `to`

Each accepted case also carries `years: [lo, hi]`, the calendar years the period
touches: a month or day covers its year, and `LA2019` touches 2019 and 2020. Each
accepted case also carries `bounds: [lo, hi]`, the first and last day the period covers
as ISO dates: `2019-02` is `2019-02-01` to `2019-02-28`.

### `source_period.jsonl`

A `project_data.json` `Source.period` against the SPA's wire. Each case has:

- `period`: the `Source.period` JSON value.
- `from_wire` (optional): a `?period` wire that shapes into `period`. Shaping does not
  validate: comma-separated members become a list (unless one is blank), `from..to` a
  range, a grammar year an int; other text stays a trimmed string.
- `wire`: the `?period` wire of `period`, or `null` when it has none (a blank value or
  endpoint, an empty list, a list member holding a comma, or not a period's shape).
- `error: "invalid_period"` when the period's structural check, or its range order,
  refuses it; otherwise `intervals`, the days it requests (merged; `null` for the
  year-independent `"_default"`), and `years`, their calendar-year spans, `null` unless
  every endpoint is a year.
- `render` (with non-null `intervals`): the period spelling of `intervals`, as the
  interval algebra writes it.

`crates/reg-core/tests/grammar.rs` and the SPA (through `reg-core-wasm`) both run it.
