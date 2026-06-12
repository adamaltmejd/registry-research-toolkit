"""Classify the variable_state diff between two reg_meta.db builds (#271).

The PR-B gate instrument (DESIGN.md → Interval-native co-delivery resolution →
Measurement and verification plan): the interval sweep's real-corpus diff must
be confined to the enumerated populations — windows that move to or from
sub-annual bounds, dissolved genuine conflicts, mid-year handoffs — with ZERO
diff on everything else. This script diffs `variable_state` row multisets
between a BEFORE and AFTER build, groups changed rows by
`(register, variable, variant, column)`, and flags any changed group whose
bounds are purely year-grain on both sides (a diff the design says must not
happen). It also re-validates every `codelivery.toml` pin: the pinned column's
state rows must be identical before/after, or the pin needs review.

Usage:
    uv run python scripts/diff_variable_states.py BEFORE_DB AFTER_DB \
        [--codelivery reg_meta_build/codelivery.toml]
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import tomllib
from collections import defaultdict
from pathlib import Path

# The content identity of a state row (state_id is generation-local).
_ROW_KEY = (
    "variable_id, register_variant_id, delivery_column_name, valid_from, "
    "valid_to, value_set_id, value_set_version_label, data_type, data_length"
)


def _connect(before: Path, after: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", uri=True)
    conn.execute("ATTACH DATABASE ? AS a", (f"file:{before.resolve()}?mode=ro",))
    conn.execute("ATTACH DATABASE ? AS b", (f"file:{after.resolve()}?mode=ro",))
    return conn


def _is_subannual(vf: str, vt: str) -> bool:
    return not (vf.endswith("-01-01") and (vt.endswith("-12-31") or vt >= "9999"))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Classify the variable_state diff between two builds (#271)."
    )
    parser.add_argument("before", type=Path)
    parser.add_argument("after", type=Path)
    parser.add_argument(
        "--codelivery", type=Path, default=Path("reg_meta_build/codelivery.toml")
    )
    args = parser.parse_args()
    conn = _connect(args.before, args.after)
    conn.row_factory = sqlite3.Row

    # Multiset difference per direction (same shape as dbdiff's mismatch dump).
    changed = conn.execute(
        f"SELECT {_ROW_KEY}, SUM(side) AS delta FROM ("
        f"  SELECT {_ROW_KEY}, -1 AS side FROM a.variable_state"
        f"  UNION ALL SELECT {_ROW_KEY}, +1 AS side FROM b.variable_state"
        f") GROUP BY {_ROW_KEY} HAVING SUM(side) != 0"
    ).fetchall()

    n_before = conn.execute("SELECT COUNT(*) FROM a.variable_state").fetchone()[0]
    n_after = conn.execute("SELECT COUNT(*) FROM b.variable_state").fetchone()[0]
    print(f"variable_state rows: before {n_before:,} → after {n_after:,}")
    print(f"changed row-identities: {len(changed)}")

    names = {
        r["variable_id"]: (r["register_id"], r["slug"], r["provider_key"])
        for r in conn.execute(
            "SELECT variable_id, register_id, slug, provider_key FROM b.variable "
            "UNION SELECT variable_id, register_id, slug, provider_key FROM a.variable"
        )
    }

    by_group: dict[tuple, dict[str, list]] = defaultdict(
        lambda: {"removed": [], "added": []}
    )
    for r in changed:
        gkey = (r["variable_id"], r["register_variant_id"], r["delivery_column_name"])
        side = "added" if r["delta"] > 0 else "removed"
        by_group[gkey][side].append(
            (r["valid_from"], r["valid_to"], r["value_set_id"], r["delta"])
        )

    out_of_population = []
    print(f"\nchanged (variable, variant, column) groups: {len(by_group)}")
    for gkey in sorted(by_group, key=str):
        vid, rv, col = gkey
        reg, slug, pkey = names.get(vid, ("?", "?", "?"))
        rows = by_group[gkey]
        subannual = any(
            _is_subannual(vf, vt) for vf, vt, _, _ in rows["removed"] + rows["added"]
        )
        if not subannual:
            out_of_population.append(gkey)
        print(
            f"  reg={reg} var={slug} (pk {pkey}) variant={rv} col={col}"
            f"{'' if subannual else '  ** NO SUB-ANNUAL BOUND — OUT OF POPULATION **'}"
        )
        for tag in ("removed", "added"):
            # value_set_id is None for code-less states; map to -1 so the
            # sort key stays comparable (real ids are positive rowids).
            for vf, vt, vs, _ in sorted(
                rows[tag], key=lambda r: (r[0], r[1], r[2] if r[2] is not None else -1)
            ):
                print(f"    {tag[0]:>1} [{vf} .. {vt}] vs={vs}")

    # Pin re-validation: every pinned column's state rows must be unchanged.
    pins = tomllib.loads(args.codelivery.read_text())["resolve"]
    print(f"\npin re-validation ({len(pins)} codelivery.toml pins):")
    pin_changed = 0
    changed_cols = {(names.get(vid, ("?",))[0], col) for vid, _, col in by_group}
    for pin in pins:
        hit = any(
            reg == pin["register_id"] and (col or "").lower() == pin["column"].lower()
            for reg, col in changed_cols
        )
        if hit:
            pin_changed += 1
            print(f"  CHANGED: register={pin['register_id']} column={pin['column']}")
    if not pin_changed:
        print("  all pinned columns unchanged")

    if out_of_population:
        print(
            f"\nFAIL: {len(out_of_population)} group(s) outside the enumerated populations"
        )
        sys.exit(1)
    if pin_changed:
        # Not automatically wrong (a pin's conflict can legitimately gain a
        # disjoint sibling state), but never automation-passable: a changed
        # pinned column requires human review of the printout above.
        print(
            f"\nREVIEW: {pin_changed} pinned column(s) changed — verify the pinned winners above"
        )
        sys.exit(2)
    print("\nOK: every changed group involves a sub-annual bound")


if __name__ == "__main__":
    main()
