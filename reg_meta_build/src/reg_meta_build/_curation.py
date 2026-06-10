"""Shared helpers for the maintainer-edited curation TOML loaders
(`codelivery.py`, `fold_overrides.py`, `column_merges.py`). All key their entries
on SCB/SOS source ids (`register_id` / `var_id`) and MUST canonicalize them
identically — a leniently coerced id (`int(1.5)`, `int(True)`, `int("01")`, a
negative) silently produces an inert never-matching curation pin instead of an
actionable load-time error, so the canonicalization lives in one place all
import. The same single-definition rule applies to `fold_column`: the loaders'
column keys must fold EXACTLY like the SCB coalescer's union-find node-col
(`sources/scb.py` `_ascii_fold_lower` delegates here), or a curated column
silently stops matching its triage component.
"""

from __future__ import annotations

import functools
import unicodedata


@functools.cache
def fold_column(s: str) -> str:
    """Canonical column-identity key: NFKD-decompose, strip non-ASCII, lowercase
    (`Kön` → `kon`, `PersonNr` → `personnr`). This is the SCB rule-2 connectivity
    key — case/diacritic column twins fold to one union-find node — and therefore
    the form every curated column key is normalized to at load time. Cached: the
    coalescer folds per row-column over ~515K instance rows, but the domain is
    the corpus's distinct header spellings (tens of thousands), so a process-
    lifetime cache is small and saves repeated NFKD passes."""
    return (
        unicodedata.normalize("NFKD", s)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )


def canonical_int(value: object) -> int | None:
    """Coerce a TOML `register_id` / `var_id` value to its canonical int, or None
    if it isn't one. A TOML integer is already canonical (the format forbids
    leading zeros); a string is accepted only in canonical form — no leading
    zeros, so `"01"` can't alias `1` (mirrors fqid_slugs `_parse_canonical_int`).
    A bool (TOML true/false, a Python int subclass) and a float are rejected."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str):
        if not value or not value.isdigit():
            return None
        if len(value) > 1 and value[0] == "0":
            return None
        return int(value)
    return None
