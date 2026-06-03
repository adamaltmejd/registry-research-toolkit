"""Co-delivery curation (§5.7): pin which value-set version a single delivery
column KEEPS when it carries two distinct codings in one period and the SCB
coalescer's deterministic cascade can't resolve it.

This is the explicit, provider-agnostic escape hatch for genuine ONE-OFF
re-codings — `Br92-kod` vs `Br07-kod`, `Ja nej 1` vs `Ja nej 3`, `Valdistrikt
2006` vs `… inkl poströster`. The RECURRING families (preliminär/final, sub-annual
HT/VT and dated snapshots, calendar vs academic year) are handled deterministically
by `sources/scb.py`'s label rules, NOT here — they'd need an entry per year.

A curation entry is keyed on `(register_id, var_id, column)`. All three key parts
are stable across builds (register_id / var_id are SCB source ids; `value_set_id`
is autoincrement and is NOT used). It resolves the conflict one of two ways:
  - `keep = "<label>"` — pin one `value_set_version_label` (one-off re-codings,
    where the residual is distinct-label so the label uniquely names a coding).
  - `keep_rule = "latest_year"` — for a RECURRING per-year-vintage column (e.g.
    SFI `Skolkod` with a per-year label pair every year), keep, at each contested
    year, the coding whose label embeds the latest 4-digit year. One entry covers
    every year; scoped to this (register, var, column) so it can't over-reach.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

# (register_id, var_id, delivery-column component) — the same coordinates the
# coalescer's per-column resolver carries (gkey[0], gkey[2], gkey[8]). The column
# component is "" for a code-bearing cvid that has no delivery alias.
CodeliveryKey = tuple[int, int, str]
# (keep_label, keep_rule) — exactly one is non-None per entry.
CodeliveryRule = tuple[str | None, str | None]
CodeliveryMap = dict[CodeliveryKey, CodeliveryRule]

_KEEP_RULES = frozenset({"latest_year"})


def repo_codelivery_path() -> Path | None:
    """`reg_meta_build/codelivery.toml` from a repo checkout, or None (wheel
    installs don't ship curation — it's a maintainer artifact like the slug
    TOMLs)."""
    candidate = Path(__file__).resolve().parent.parent.parent / "codelivery.toml"
    return candidate if candidate.is_file() else None


def load_codelivery(path: Path | None) -> CodeliveryMap:
    """Parse the curation TOML into `{(register_id, var_id, column): (keep_label,
    keep_rule)}`. Empty when no file (synthetic test builds, wheel installs)."""
    if path is None or not path.is_file():
        return {}
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    out: CodeliveryMap = {}
    for entry in data.get("resolve", []):
        key: CodeliveryKey = (
            int(entry["register_id"]),
            int(entry["var_id"]),
            str(entry.get("column", "")),
        )
        keep_label = entry.get("keep")
        keep_rule = entry.get("keep_rule")
        if (keep_label is None) == (keep_rule is None):
            raise ValueError(
                f"codelivery entry {key} must set exactly one of `keep`/`keep_rule`"
            )
        if keep_rule is not None and keep_rule not in _KEEP_RULES:
            raise ValueError(
                f"codelivery entry {key} has unknown keep_rule {keep_rule!r} "
                f"(known: {sorted(_KEEP_RULES)})"
            )
        out[key] = (
            str(keep_label) if keep_label is not None else None,
            str(keep_rule) if keep_rule is not None else None,
        )
    return out
