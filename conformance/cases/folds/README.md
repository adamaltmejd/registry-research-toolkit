# Fold corpus

The oracle for the text folds of `crates/DESIGN.md` ("Text folds"), read by
`crates/reg-core/tests/fold_corpus.rs` (`cargo test --workspace`).

## Files

Each generated file holds one fold's cases, one JSON object per line, sorted by input:
`{"in": <input>, "out": <expected>}`.

- `fold_identity.jsonl` — column identity: Unicode lowercase with `Final_Sigma`.
- `normalized_search_query.jsonl` — the query normalizer.

`unicode17.jsonl` is hand-written: `{"op", "in", "out", "note"}`. It covers characters
that Unicode 17.0 added or changed, which the generated files exclude. Each `note` names
the UCD 17.0.0 data the expected value comes from.

Strings are escaped where a character would be invisible or ambiguous in the file:
whitespace other than U+0020, marks, controls and format characters appear as `\uXXXX`
(astral characters as surrogate pairs); printable letters, numbers, punctuation and
symbols appear as is.

## Provenance

The generated files are frozen data. They were written by a generator, since deleted,
from the retired Python reader (`reg_meta.queries` and `str.lower`), so expected values
are the Python behavior `reg-core` must keep. They cannot be regenerated: a changed or
added expected value is a hand edit and a content change, reviewed in the diff, with its
source named in the commit.

Selection per fold, as generated, over assigned scalars excluding private use:

- every scalar the fold changes;
- a seeded sample of 1,000 other scalars;
- a seeded sample of scalars in four contexts: after `ΑΣ` and between `Α` and `Σ`
  (`Final_Sigma`), before ` x` and padded with spaces (token boundaries, strip);
- 300 seeded random strings and a fixed list of edge inputs, among them `straße`, `ﬁlm`
  and `ＡＢＣ`.

Python's UCD (16.0 in Python 3.14) is older than `reg-core`'s (17.0). Any input holding
a character Python leaves unassigned, or U+0295 (Ll in 16.0, Lo in 17.0), is excluded.
