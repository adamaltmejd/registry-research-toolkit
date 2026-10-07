# Fold corpus

The oracle for the text folds of `RUST_RUNTIME_SPEC.md` section 5, read by
`crates/reg-core/tests/fold_corpus.rs` (`cargo test --workspace`).

## Files

Each generated file holds one fold's cases, one JSON object per line, sorted by input:
`{"in": <input>, "out": <expected>}`. `out` is `null` where the fold returns nothing
(`fts_match_query` with no word token).

- `fold_identity.jsonl` — column identity: Unicode lowercase with `Final_Sigma`.
- `fold_search.jsonl` — search-text fold: case fold, NFKD, drop combining marks until
  nothing changes, then one space between words.
- `normalized_search_query.jsonl` — the query normalizer.
- `fts_match_query.jsonl` — the FTS5 `MATCH` expression builder.

`unicode17.jsonl` is hand-written: `{"op", "in", "out", "note"}`. It covers characters
that Unicode 17.0 added or changed, which the generated files exclude. Each `note` names
the UCD 17.0.0 data the expected value comes from.

Strings are escaped where a character would be invisible or ambiguous in the file:
whitespace other than U+0020, marks, controls and format characters appear as `\uXXXX`
(astral characters as surrogate pairs); printable letters, numbers, punctuation and
symbols appear as is.

## Generation

The generated files are written by `crates/reg-core/tools/gen_fold_corpus.py` from
today's Python reader (`reg_meta.queries` and `str.lower`). Expected values are
therefore the Python behavior `reg-core` must keep. Regenerating is deterministic for a
given Python version; a changed expected value is a content change, reviewed in the
diff.

```sh
uv run python crates/reg-core/tools/gen_fold_corpus.py
```

Selection per fold, over assigned scalars excluding private use:

- every scalar the fold changes (for `fts_match_query`: every scalar it rejects among
  marks, separators, controls, format characters and connector punctuation, the classes
  where Rust's `char::is_alphanumeric` and Python's `[^\W_]` disagree);
- except precomposed Hangul syllables with a final consonant, whose decomposition is
  algorithmic; they are sampled;
- a seeded sample of 1,000 other scalars;
- a seeded sample of scalars in four contexts: after `ΑΣ` and between `Α` and `Σ`
  (`Final_Sigma`), before ` x` and padded with spaces (token boundaries, strip);
- 300 seeded random strings and a fixed list of edge inputs, among them `straße`, `ﬁlm`
  and `ＡＢＣ`, which `fold_search` matches to `strasse`, `film` and `abc` but FTS5
  `unicode61` does not.

Python's UCD (16.0 in Python 3.14) is older than `reg-core`'s (17.0). Any input holding
a character Python leaves unassigned, or U+0295 (Ll in 16.0, Lo in 17.0), is excluded.
