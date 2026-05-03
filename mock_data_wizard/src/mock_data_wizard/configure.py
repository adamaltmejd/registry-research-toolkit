"""Local ``configure`` step: discover.json -> mdw_config.json.

Reads ``discover.json`` produced by the bundle in discover mode and
writes a ``mdw_config.json`` next to it. Per-column type assignment
priority:

1. **Name pattern.** Re-uses the canonical id / categorical patterns
   from :mod:`classify` plus a small extra set (date / numeric word
   roots) so the wizard's classifier and configure agree on the same
   names without two source-of-truth lists.
2. **High-cardinality default.** Anything that doesn't pattern-match
   gets ``high_cardinality`` -- the safe choice. Misclassified, you
   get string-length stats; you fix it in mdw_config.json for the
   next iteration.

regmeta integration is **deliberately out of scope** for the first cut.
A follow-up can add it as an opt-in ``--register`` flag that overrides
patterns when the variable is documented in regmeta. The pattern path
already covers the common SCB shapes (lopnr, ssyk, sun20*, kommun,
*Datum*, *Tidpunkt*, *Belopp*, *Ink*, ...) so the marginal value of
regmeta is mostly the long tail.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from .classify import is_known_id, known_categorical_cap

log = logging.getLogger("mdw.configure")

CONFIG_FILENAME = "mdw_config.json"
SCHEMA_VERSION = 1

# Extra categorical name roots beyond classify.CATEGORICAL_PATTERNS.
# These are register-specific glossary entries that the data-driven
# classifier doesn't see when running on names alone.
EXTRA_CATEGORICAL = (
    re.compile(r"(^|_)kon$", re.IGNORECASE),  # sex
    re.compile(r"civil", re.IGNORECASE),  # CivilStand
    re.compile(r"(^|_)lan$", re.IGNORECASE),  # län (region)
    re.compile(r"land(skap)?$", re.IGNORECASE),  # FodelseLand
    re.compile(r"_kod$", re.IGNORECASE),
)

# Date roots. *Datum*, *Tidpunkt*, *Date* etc.
DATE_PATTERNS = (
    re.compile(r"datum", re.IGNORECASE),
    re.compile(r"tidpunkt", re.IGNORECASE),
    re.compile(r"(^|_)ar$", re.IGNORECASE),  # AR (year) is date-ish; YYYYMMDD-int
    re.compile(r"birth.*date|fodelse.*datum", re.IGNORECASE),
)

# Numeric roots. Money, income, pension, compensation.
NUMERIC_PATTERNS = (
    re.compile(r"belopp", re.IGNORECASE),
    re.compile(r"(^|_)ink", re.IGNORECASE),  # InkomstSumma, IncomeKr
    re.compile(r"pens", re.IGNORECASE),
    re.compile(r"erst", re.IGNORECASE),
    re.compile(r"sum$|^sum_|_sum$", re.IGNORECASE),
    re.compile(r"alder", re.IGNORECASE),  # age
)


def _classify_name(col_name: str) -> str:
    """Return one of the five mock_data_wizard column types for ``col_name``."""
    if is_known_id(col_name):
        return "id"
    if known_categorical_cap(col_name) is not None:
        return "categorical"
    for pat in EXTRA_CATEGORICAL:
        if pat.search(col_name):
            return "categorical"
    for pat in DATE_PATTERNS:
        if pat.search(col_name):
            return "date"
    for pat in NUMERIC_PATTERNS:
        if pat.search(col_name):
            return "numeric"
    return "high_cardinality"


def build_config(discover: dict[str, Any]) -> dict[str, Any]:
    """Author a mdw_config.json payload from a discover.json payload."""
    column_types: dict[str, dict[str, dict[str, str]]] = {}
    for src in discover.get("sources", []):
        source_name = src["source_name"]
        cols_out: dict[str, dict[str, str]] = {}
        for col in src.get("columns", []):
            col_name = col["name"]
            cols_out[col_name] = {"type": _classify_name(col_name)}
        if cols_out:
            column_types[source_name] = cols_out
    return {
        "version": SCHEMA_VERSION,
        "column_types": column_types,
    }


def _summary_counts(payload: dict[str, Any]) -> Counter[str]:
    """Count assigned types across all sources for the per-source summary."""
    c: Counter[str] = Counter()
    for cols in payload.get("column_types", {}).values():
        for entry in cols.values():
            c[entry["type"]] += 1
    return c


def write_config(path: Path, payload: dict[str, Any]) -> None:
    """Pretty-write ``mdw_config.json``. UTF-8, keys preserved as-is."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def configure_from_discover(
    discover_path: Path,
    output_path: Path | None = None,
    *,
    overwrite: bool = False,
) -> Path:
    """Top-level entry point: read ``discover_path``, write mdw_config.json.

    Returns the path of the written file. Raises on:
    - missing discover.json
    - existing mdw_config.json without ``overwrite=True``
    - empty discover (zero sources -- nothing to configure).
    """
    discover_path = Path(discover_path)
    if not discover_path.exists():
        raise FileNotFoundError(f"discover.json not found: {discover_path}")

    target = (
        Path(output_path)
        if output_path is not None
        else discover_path.with_name(CONFIG_FILENAME)
    )
    if target.exists() and not overwrite:
        raise FileExistsError(
            f"{target} already exists; pass --overwrite to replace it."
        )

    payload = json.loads(discover_path.read_text(encoding="utf-8"))
    if not payload.get("sources"):
        raise ValueError(f"{discover_path} has no sources -- nothing to configure.")

    config = build_config(payload)
    write_config(target, config)

    counts = _summary_counts(config)
    summary = ", ".join(f"{t}={n}" for t, n in sorted(counts.items()))
    n_sources = len(config["column_types"])
    n_cols = sum(counts.values())
    print(
        f"Wrote {target} ({n_sources} source(s), {n_cols} column(s)): {summary}",
        file=sys.stderr,
    )
    return target
