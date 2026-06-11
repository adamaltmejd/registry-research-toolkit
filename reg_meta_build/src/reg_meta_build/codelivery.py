"""Co-delivery curation (see DESIGN.md → Build-time triage (SCB)): pin which value-set version a single delivery
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

from pathlib import Path

from ._curation import canonical_int, curation_error, fold_column, load_curation_entries

# (register_id, var_id, delivery-column component) — the same coordinates the
# coalescer's per-column resolver carries (gkey[0], gkey[2], gkey[8]). The column
# component is "" for a code-bearing cvid that has no delivery alias, and is the
# coalescer's CASE-FOLDED rule-2 connectivity key otherwise — the loader folds
# the TOML's `column` to match (a raw-cased pin would silently never match).
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
    keep_rule)}`. Empty when no file (synthetic test builds, wheel installs).

    Load-time validation (all EXIT_CONFIG, actionable):
      - Only `[[resolve]]` top-level (a misspelled `[[resolves]]` is a loud error,
        not a silent no-op that disables ALL curation); `resolve` is an array of
        tables (a scalar / single `[resolve]` table is rejected before the loop,
        not a raw uncaught crash).
      - `register_id` / `var_id` present and canonical int (no leading zeros,
        no bool/float — shared `_curation.canonical_int`, identical to
        fold_overrides), and `column` a string (or absent → ""); a leniently
        coerced id or a str()-coerced column would produce an inert
        never-matching pin instead of a load-time error.
      - Exactly one of `keep` / `keep_rule`, each a string (`keep_rule` from a
        known set); a non-string rule — including an unhashable list/dict — is
        rejected, not crashed on the membership test."""
    # Shared scaffold (parse + top-level typo guard + array-of-tables +
    # per-entry table check) — see `_curation.load_curation_entries`.
    entries = load_curation_entries(
        path,
        entry_key="resolve",
        label="co-delivery",
        prefix="codelivery",
        code_base="codelivery",
        file_name="codelivery.toml",
        entry_fields="register_id / var_id / column",
    )
    out: CodeliveryMap = {}
    for entry in entries:
        # Canonicalize ids identically to fold_overrides (shared `canonical_int`):
        # `int(...)` would silently accept `1.5` (→1), `true` (→1), `"01"` (→1),
        # and negatives, producing an inert never-matching pin instead of a
        # load-time error.
        reg = canonical_int(entry.get("register_id"))
        var = canonical_int(entry.get("var_id"))
        if reg is None or var is None:
            raise curation_error(
                "codelivery_invalid",
                f"codelivery [[resolve]] entry {entry!r} needs `register_id` and "
                f"`var_id` as canonical integers (no leading zeros).",
                "Each [[resolve]] entry needs integer `register_id` and `var_id`.",
            )
        # `column` must be a string (or absent → ""): a list/dict/number/bool would
        # str()-coerce into a column name that can never match a real delivery
        # column — reject it at load, not as a confusing downstream mismatch.
        column = entry.get("column", "")
        if not isinstance(column, str):
            raise curation_error(
                "codelivery_invalid",
                f"codelivery [[resolve]] entry {entry!r} `column` must be a string, "
                f"got {type(column).__name__}.",
                'Give `column = "<delivery-column>"` as a string, or omit it.',
            )
        # Folded to the coalescer's connectivity key (see CodeliveryKey above);
        # "" (no delivery alias) folds to itself.
        key: CodeliveryKey = (reg, var, fold_column(column))
        keep_label = entry.get("keep")
        keep_rule = entry.get("keep_rule")
        if (keep_label is None) == (keep_rule is None):
            raise curation_error(
                "codelivery_invalid",
                f"codelivery entry {key} must set exactly one of `keep`/`keep_rule`",
                'Give the [[resolve]] entry either `keep = "<label>"` or '
                "`keep_rule`, not both and not neither.",
            )
        # The set field must be a string: `keep` is a value-set label, `keep_rule`
        # a rule name. Reject a non-string (list/dict/bool/number) at load rather
        # than str()-coercing it into an inert never-matching pin — and, for
        # `keep_rule`, BEFORE the membership test below, since an unhashable value
        # (`keep_rule = [1]`) would raise a raw TypeError there (the test hashes the
        # candidate against the frozenset), escaping the EXIT_CONFIG contract.
        if keep_label is not None and not isinstance(keep_label, str):
            raise curation_error(
                "codelivery_invalid",
                f"codelivery entry {key} `keep` must be a string label, got "
                f"{type(keep_label).__name__}.",
                'Give `keep = "<value_set_version_label>"` as a string.',
            )
        if keep_rule is not None and (
            not isinstance(keep_rule, str) or keep_rule not in _KEEP_RULES
        ):
            raise curation_error(
                "codelivery_invalid",
                f"codelivery entry {key} has unknown keep_rule {keep_rule!r} "
                f"(known: {sorted(_KEEP_RULES)})",
                f"Use a supported keep_rule value: {sorted(_KEEP_RULES)}.",
            )
        out[key] = (
            str(keep_label) if keep_label is not None else None,
            str(keep_rule) if keep_rule is not None else None,
        )
    return out
