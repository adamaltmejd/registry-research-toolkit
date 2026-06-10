"""Shared helpers for the maintainer-edited curation TOML loaders
(`codelivery.py`, `fold_overrides.py`). Both key their entries on SCB/SOS source
ids (`register_id` / `var_id`) and MUST canonicalize them identically — a
leniently coerced id (`int(1.5)`, `int(True)`, `int("01")`, a negative) silently
produces an inert never-matching curation pin instead of an actionable load-time
error, so the canonicalization lives in one place both import.
"""

from __future__ import annotations


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
