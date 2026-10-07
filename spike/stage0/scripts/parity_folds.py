"""Stage-0 parity check: `reg_core_spike` (Rust) against reg_meta's Python text folds.

Compares every fold over (a) every Unicode scalar value, alone and in a few context
strings, and (b) the distinct text values of a real catalog opened read-only. A
mismatch whose input holds a character unassigned in Python's UCD (or one whose
properties changed between UCD versions) is reported separately: that residual is a
Unicode-version difference, not a porting bug.

Run from the worktree root (so `reg_meta` imports):

    uv run --with ./spike/stage0/core \
        python spike/stage0/scripts/parity_folds.py --db /path/to/reg_meta.db

(`core/pyproject.toml` sets uv `cache-keys`, so a Rust edit triggers a rebuild.)

Exit status is 1 when any mismatch is not explained by a Unicode-version difference.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
import unicodedata
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import reg_core_spike as rs
from reg_meta.queries import (
    _FTS_WORD_CHAR,
    _fold_search_text,
    _fts_match_query,
    _normalized_search_query,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

MAX_EXAMPLES = 10

# name -> (Python reference, Rust port)
FOLDS: dict[str, tuple[Callable[[str], object], Callable[[str], object]]] = {
    "fold_identity": (str.lower, rs.fold_identity),
    "fold_search": (_fold_search_text, rs.fold_search),
    "normalized_search_query": (_normalized_search_query, rs.normalized_search_query),
    "fts_match_query": (_fts_match_query, rs.fts_match_query),
}
CHAR_PREDICATES: dict[str, tuple[Callable[[str], bool], Callable[[str], bool]]] = {
    "py_isspace": (str.isspace, rs.py_isspace),
    "py_isalnum": (lambda c: _FTS_WORD_CHAR.match(c) is not None, rs.py_isalnum),
}

# Context templates for the sweep: Final_Sigma on both sides, token boundaries, strip.
CONTEXTS: dict[str, Callable[[str], str]] = {
    "c": lambda c: c,
    "'ΑΣ'+c": lambda c: "ΑΣ" + c,
    "'Α'+c+'Σ'": lambda c: "Α" + c + "Σ",
    "c+' x'": lambda c: c + " x",
    "' '+c+' '": lambda c: " " + c + " ",
}

# (table, column) pairs whose DISTINCT values form the corpus.
CORPUS_COLUMNS = [
    ("variable", "name"),
    ("variable", "definition"),
    ("variable", "description"),
    ("variable", "operational_definition"),
    ("variable_alias", "delivery_column_name"),
    ("variable_state", "delivery_column_name"),
    ("variable_state", "name"),
    ("value_code", "code"),
    ("value_code", "label"),
    ("register", "name"),
    ("register", "purpose"),
    ("register_variant", "name"),
    ("classification", "short_name"),
    ("classification", "name"),
    ("classification", "name_en"),
    ("concept_group", "label"),
]


def hexs(value: object) -> str:
    if value is None:
        return "None"
    return " ".join(f"{ord(ch):04X}" for ch in str(value)) or "''"


# Characters assigned in both UCD 16 and 17 whose properties changed in 17 (Rust std
# is on 17). U+0295 moved Ll -> Lo, so it stopped being Cased for Final_Sigma.
UCD_17_CHANGED = frozenset("\u0295")


def version_dependent(s: str) -> bool:
    """True when `s` holds a character the two UCD versions disagree on."""
    return any(unicodedata.category(ch) == "Cn" or ch in UCD_17_CHANGED for ch in s)


@dataclass
class Tally:
    checked: int = 0
    mismatches: int = 0
    version_explained: int = 0
    examples: list[str] = field(default_factory=list)
    version_examples: list[str] = field(default_factory=list)

    def record(self, label: str, inp: str, py: object, rust: object) -> None:
        self.checked += 1
        if py == rust:
            return
        self.mismatches += 1
        explained = version_dependent(inp)
        self.version_explained += explained
        # Unexplained mismatches get the example budget; version residue gets a few.
        bucket, cap = (
            (self.version_examples, 3) if explained else (self.examples, MAX_EXAMPLES)
        )
        if len(bucket) < cap:
            tag = f" [UCD {unicodedata.unidata_version} vs 17 difference]"
            bucket.append(
                f"{label}: in=[{hexs(inp)}] py=[{hexs(py)}] rust=[{hexs(rust)}]"
                + (tag if explained else "")
            )


def scalars() -> Iterable[str]:
    for cp in range(0x110000):
        if not 0xD800 <= cp <= 0xDFFF:
            yield chr(cp)


def sweep() -> dict[str, Tally]:
    tallies: dict[str, Tally] = {}
    chars = list(scalars())
    for name, (py, rust) in CHAR_PREDICATES.items():
        t = tallies.setdefault(name, Tally())
        for c in chars:
            t.record("c", c, py(c), rust(c))
    for name, (py, rust) in FOLDS.items():
        t = tallies.setdefault(name, Tally())
        for ctx_name, ctx in CONTEXTS.items():
            for c in chars:
                s = ctx(c)
                t.record(ctx_name, s, py(s), rust(s))
    return tallies


def corpus_strings(db: str) -> set[str]:
    conn = sqlite3.connect(f"file:{db}?mode=ro&immutable=1", uri=True)
    try:
        strings: set[str] = set()
        for table, column in CORPUS_COLUMNS:
            rows = conn.execute(
                f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL"
            )
            strings.update(str(v) for (v,) in rows)
        return strings
    finally:
        conn.close()


def corpus(strings: set[str]) -> dict[str, Tally]:
    tallies: dict[str, Tally] = {}
    ordered = sorted(strings)
    for name, (py, rust) in FOLDS.items():
        t = tallies.setdefault(name, Tally())
        for s in ordered:
            t.record("corpus", s, py(s), rust(s))
    return tallies


def report(title: str, tallies: dict[str, Tally]) -> int:
    print(f"\n== {title}")
    unexplained = 0
    for name, t in tallies.items():
        unexplained += t.mismatches - t.version_explained
        print(
            f"{name:25s} checked={t.checked:>9d} mismatches={t.mismatches:>6d}"
            f" (unicode-version={t.version_explained})"
        )
        for ex in t.examples + t.version_examples:
            print(f"    {ex}")
    return unexplained


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", help="catalog reg_meta.db (opened read-only)")
    parser.add_argument("--skip-sweep", action="store_true")
    args = parser.parse_args()

    print(
        f"python {sys.version.split()[0]}  unicodedata {unicodedata.unidata_version}"
        f"  sqlite {sqlite3.sqlite_version}"
    )
    unexplained = 0
    if not args.skip_sweep:
        t0 = time.perf_counter()
        unexplained += report("code-point sweep", sweep())
        print(f"   ({time.perf_counter() - t0:.1f}s)")
    if args.db:
        t0 = time.perf_counter()
        strings = corpus_strings(args.db)
        unexplained += report(
            f"catalog corpus ({len(strings)} distinct strings)", corpus(strings)
        )
        print(f"   ({time.perf_counter() - t0:.1f}s)")
    print(f"\nunexplained mismatches: {unexplained}")
    return 1 if unexplained else 0


if __name__ == "__main__":
    raise SystemExit(main())
