"""The reader's public search fold agrees with the committed fold corpus."""

import json
from pathlib import Path

from reg_meta.queries import fold_search

CORPUS = Path(__file__).parent / "cases" / "folds" / "fold_search.jsonl"


def test_reader_fold_search_matches_corpus():
    with CORPUS.open(encoding="utf-8") as lines:
        cases = [json.loads(line) for line in lines if line.strip()]
    assert [c for c in cases if fold_search(c["in"]) != c["out"]] == []
