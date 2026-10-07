"""Generate the fold corpus in `conformance/cases/folds/` from today's Python folds.

A tool, not a test: the expected values are what `reg_meta`'s fold helpers return
today, so the corpus pins the Python reader's behavior for `reg-core` to match. It
writes one JSON-lines file per fold, each line `{"in": <input>, "out": <expected>}`,
sorted by input. The selection rules are in `conformance/cases/folds/README.md`.
`unicode17.jsonl` is hand-written and not touched here.

Python's UCD is older than `reg-core`'s (17.0.0). Inputs holding a character that
Python leaves unassigned, or one whose properties changed in 17.0 (`UCD_17_CHANGED`),
are excluded; `unicode17.jsonl` covers them. Output is deterministic (fixed seed) for
a given Python version.

Run from the repository root (`reg_meta` must import):

    uv run python crates/reg-core/tools/gen_fold_corpus.py
"""

from __future__ import annotations

import json
import random
import unicodedata
from pathlib import Path
from typing import TYPE_CHECKING

from reg_meta.queries import (
    _fts_match_query,
    _normalized_search_query,
    fold_search,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

OUT_DIR = Path(__file__).resolve().parents[3] / "conformance" / "cases" / "folds"
SEED = 20261007

FOLDS: dict[str, Callable[[str], str | None]] = {
    "fold_identity": str.lower,
    "fold_search": fold_search,
    "normalized_search_query": _normalized_search_query,
    "fts_match_query": _fts_match_query,
}

# Assigned in both UCD 16 and 17, with properties changed in 17. U+0295 moved
# Ll -> Lo, so it stopped being Cased for Final_Sigma.
UCD_17_CHANGED = frozenset("ʕ")

# The parity sweep's contexts: Final_Sigma on both sides, token boundaries, strip.
CONTEXTS: tuple[Callable[[str], str], ...] = (
    lambda c: "ΑΣ" + c,
    lambda c: "Α" + c + "Σ",
    lambda c: c + " x",
    lambda c: " " + c + " ",
)

# Distinguishing and edge inputs, run through every fold. The first three separate
# `fold_search` from FTS5 `unicode61` (checkpoint 1 decides which the index uses).
FIXED_INPUTS = (
    "straße",
    "STRASSE",
    "ﬁlm",
    "ＡＢＣ",
    "",
    " ",
    "\t\n\x1c\x1f 　",
    "  Inkomst   av  tjänst  ",
    "ålder",
    "ÅLDER Ärende Öre",
    "ΟΔΟΣ",
    "ΟΔΟΣ ΣΟΦΟΣ",
    "Σ",
    "İstanbul",
    "ǅemal",
    "ŉ",
    "ΐ",
    "ﬃ",
    "café",
    "café",
    "x²",
    "①②",
    "Ⓐ",
    "a_b",
    "_",
    "___",
    "-",
    "inkomst AND kön",
    "a OR b",
    "NOT x",
    "NEAR(a b)",
    "-negated",
    "col:value",
    "^start",
    "(group)",
    "*",
    "***",
    '"',
    '"quoted"',
    'say "hi"',
    "o'clock",
    "lön/inkomst",
    "2020-01-01",
    "LopNr",
    "SUN2000niva",
    "ͅ",
    "aͅ",
    "⃝",
)

LETTERS = "abcxyzABCXYZåäöÅÄÖßẞΣσςİıǅ"
SPACES = " \t\n  　\x1c"
PUNCT = "\"'_-*^():.,/"


def scalars() -> Iterable[str]:
    for cp in range(0x110000):
        if not 0xD800 <= cp <= 0xDFFF:
            yield chr(cp)


def version_dependent(s: str) -> bool:
    return any(unicodedata.category(ch) == "Cn" or ch in UCD_17_CHANGED for ch in s)


def json_string(s: str) -> str:
    """A JSON string literal: printable letters, numbers, punctuation and symbols
    as is; everything else (spaces but U+0020, marks, controls, format characters)
    escaped, so no invisible or combining character sits raw in the file."""
    out = ['"']
    for ch in s:
        if ch in '"\\':
            out.append("\\" + ch)
        elif ch == " " or (ch.isprintable() and unicodedata.category(ch)[0] in "LNPS"):
            out.append(ch)
        elif ord(ch) > 0xFFFF:
            hi, lo = divmod(ord(ch) - 0x10000, 0x400)
            out.append(f"\\u{0xD800 + hi:04x}\\u{0xDC00 + lo:04x}")
        else:
            out.append(f"\\u{ord(ch):04x}")
    out.append('"')
    return "".join(out)


def json_value(v: str | None) -> str:
    return "null" if v is None else json_string(v)


def random_strings(rng: random.Random, pool: list[str], n: int) -> list[str]:
    alphabet = [*LETTERS, *SPACES, *PUNCT]
    strings = []
    for _ in range(n):
        length = rng.randint(1, 12)
        strings.append(
            "".join(
                rng.choice(pool) if rng.random() < 0.3 else rng.choice(alphabet)
                for _ in range(length)
            )
        )
    return strings


def is_hangul_with_final(c: str) -> bool:
    """A precomposed Hangul syllable with a trailing consonant (its NFKD has three
    jamo). The decomposition is algorithmic, so a sample stands for all 10,773."""
    offset = ord(c) - 0xAC00
    return 0 <= offset < 11172 and offset % 28 != 0


def inputs_for(
    name: str, fold: Callable[[str], str | None], assigned: list[str]
) -> set[str]:
    rng = random.Random(f"{SEED}:{name}")
    if name == "fts_match_query":
        # Every scalar the builder rejects in the classes where the stage-0 port went
        # wrong (marks, spaces, controls, format characters, connector punctuation).
        def notable(c: str) -> bool:
            category = unicodedata.category(c)
            return fold(c) is None and (category[0] in "MZC" or category == "Pc")

    else:

        def notable(c: str) -> bool:
            return fold(c) != c and not is_hangul_with_final(c)

    every = [c for c in assigned if notable(c)]
    rest = [c for c in assigned if not notable(c)]
    selected = set(every)
    selected.update(rng.sample(rest, 1000))
    for c in rng.sample(every, 200) + rng.sample(assigned, 100):
        selected.update(ctx(c) for ctx in CONTEXTS)
    selected.update(random_strings(rng, assigned, 300))
    selected.update(FIXED_INPUTS)
    return {s for s in selected if not version_dependent(s)}


def main() -> int:
    # Private use (Co) folds to itself everywhere; unassigned (Cn) is excluded.
    assigned = [c for c in scalars() if unicodedata.category(c) not in {"Cn", "Co"}]
    assigned = [c for c in assigned if c not in UCD_17_CHANGED]
    for name, fold in FOLDS.items():
        cases = sorted(inputs_for(name, fold, assigned))
        lines = [
            f'{{"in": {json_string(s)}, "out": {json_value(fold(s))}}}\n' for s in cases
        ]
        path = OUT_DIR / f"{name}.jsonl"
        path.write_text("".join(lines), encoding="utf-8")
        # Every line must read back as the case it was written from.
        for line, s in zip(path.read_text(encoding="utf-8").splitlines(), cases):
            assert json.loads(line) == {"in": s, "out": fold(s)}, line
        print(f"{name}: {len(cases)} cases, {path.stat().st_size} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
