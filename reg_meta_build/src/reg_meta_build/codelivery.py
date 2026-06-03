"""Co-delivery curation (§5.7): pin which value-set version a single delivery
column KEEPS when it carries two distinct codings in one period and the SCB
coalescer's deterministic cascade can't resolve it.

This is the explicit, provider-agnostic escape hatch for genuine ONE-OFF
re-codings — `Br92-kod` vs `Br07-kod`, `Ja nej 1` vs `Ja nej 3`, `Valdistrikt
2006` vs `… inkl poströster`. The RECURRING families (preliminär/final, sub-annual
HT/VT and dated snapshots, calendar vs academic year) are handled deterministically
by `sources/scb.py`'s label rules, NOT here — they'd need an entry per year.

A curation entry is keyed on `(register_id, var_id, column)` → the
`value_set_version_label` to KEEP. All three key parts are stable across builds
(register_id / var_id are SCB source ids; `value_set_id` is autoincrement and is
NOT used). The genuine residual is distinct-label by construction, so the kept
label uniquely names one of the conflicting value sets.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

# (register_id, var_id, delivery-column component) — the same coordinates the
# coalescer's per-column resolver carries (gkey[0], gkey[2], gkey[8]). The column
# component is "" for a code-bearing cvid that has no delivery alias.
CodeliveryKey = tuple[int, int, str]


def repo_codelivery_path() -> Path | None:
    """`reg_meta_build/codelivery.toml` from a repo checkout, or None (wheel
    installs don't ship curation — it's a maintainer artifact like the slug
    TOMLs)."""
    candidate = Path(__file__).resolve().parent.parent.parent / "codelivery.toml"
    return candidate if candidate.is_file() else None


def load_codelivery(path: Path | None) -> dict[CodeliveryKey, str]:
    """Parse the curation TOML into `{(register_id, var_id, column): keep_label}`.
    Empty when no file (synthetic test builds, wheel installs)."""
    if path is None or not path.is_file():
        return {}
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    out: dict[CodeliveryKey, str] = {}
    for entry in data.get("resolve", []):
        key: CodeliveryKey = (
            int(entry["register_id"]),
            int(entry["var_id"]),
            str(entry.get("column", "")),
        )
        out[key] = str(entry["keep"])
    return out
