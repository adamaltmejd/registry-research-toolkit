"""Canonical-SCB attach post-pass (#400 PR2).

Mint CANONICAL-SCB `variable`s onto an EXISTING `(register, variant)` — the rich
analog of `variable_grafts`. A graft mints a real SCB-export column reg_meta
lacks but a steward delivers, with a banded SCB id just above the SCB max and an
open-ended window. A canonical-attach mints a column that is NOT in SCB's machine
export at all (LISA's hand-documented SSYK/SNI variables, …) and so carries the
RICHER shape a curated canonical-SCB row gets:

* **Canonical-SCB ids.** `mint_canonical_scb("scb", register, variant, column)`
  puts every minted id in the reserved sub-band ``[2^61, 2^62)`` (deterministic,
  disjoint from real source-derived SCB ids and from the high minted band) — NOT
  the graft's MAX+1 sequential id. The same `mint_canonical_scb` the
  `CanonicalScbAdapter` uses (#444), so a minted id is in the same band a
  full-adapter canonical row gets, on an existing register. (The columns the pass
  SETS still mirror grafts — `variable.name` + `description`, `variable_state`
  type/window — and leave `variable.definition` NULL; the full
  `CanonicalScbAdapter` additionally populates `definition`.)
* **Closed validity window.** `valid_from` / `valid_to` come from the seed entry
  (a closed era like 2010–2013); an omitted `valid_to` writes the open-ended
  `9999-12-31` sentinel.
* **Classification link.** An entry's optional `classification` is appended to
  the build's shared `classification_candidates` list (value_set_id None), so the
  existing provider-blind backfill (`_backfill_state_classifications`) tags
  `variable_state.classification_id` for free.
* **No value sets.** Maintainer decision: classification-link-only. A `value_set`
  key is out of scope (the full `CanonicalScbAdapter` is the home for code lists).

Why a post-pass (Strategy B), not a `CanonicalScbAdapter` extension: the target
`(register, variant)` is materialized by the MAIN build (SCB adapter), so its
register/variant slugs only exist after `populate_slugs`. The attach resolves the
existing target BY SLUG exactly as `materialize_grafts` does — it cannot run at
adapter-emit time. So it runs in the slug-guarded block right after
`materialize_grafts`, sharing that block's `classification_candidates` list (fed
to the backfill below the block).

Guards / discipline (mirrors `variable_grafts`):
* **Gap-fill only.** An attach whose column already exists as a `variable_state`
  column in that `(register, variant)` is skipped — never duplicate.
* **Strict load, lenient resolve.** A structural TOML defect fails the build
  (EXIT_CONFIG); a `(register, variant)` that doesn't resolve is counted
  `unresolved`, not fatal (pre-v1 slug churn).
* **No `SCHEMA_VERSION` bump** — rows on existing tables.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

from ._curation import (
    curation_error,
    fold_column,
    load_curation_entries,
    require_bool,
    require_str,
    resolve_register_variant_id,
)
from .classifications import declared_short_names
from .db import _VALID_TO_SENTINEL
from .id import mint_canonical_scb

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable
    from pathlib import Path

# The `variable.source_label` stamped on every attached row — the canonical
# analog of `variable_grafts`' `swecov-graft`. Distinct from every existing
# label (`swecov-graft`; the SCB/SOS adapters leave it NULL), so the validation
# invariant (every `canonical-scb` row is `is_canonical_scb`) is unambiguous.
CANONICAL_ATTACH_SOURCE_LABEL = "canonical-scb"

# Open-ended validity sentinel (`_VALID_TO_SENTINEL`, imported from `db.py`) — the
# same exact string the materializer / validator (_check_open_ended_sentinel)
# demand for an omitted upper bound; `db.py` imports this module function-locally,
# so a top-level import here is cycle-free.

# data_type vocabulary shared with scb_canonical.toml (CanonicalScbAdapter). The
# attach only stores the string verbatim on the state; this gate keeps a typo
# (`txt`, `int`) from silently shipping a meaningless data_type.
_DATA_TYPES = frozenset({"text", "decimal", "integer", "date"})

_REQUIRED_KEYS = frozenset(
    {"register", "variant", "column", "name", "definition", "data_type", "valid_from"}
)
_OPTIONAL_KEYS = frozenset(
    {"valid_to", "classification", "is_identifier", "is_sensitive"}
)
_ALLOWED_KEYS = _REQUIRED_KEYS | _OPTIONAL_KEYS

# Bind the shared non-empty-string leaf to this surface's code/path once (same
# idiom as variable_grafts), so the per-field checks reuse the ONE repo rule.
_require_str = functools.partial(
    require_str,
    code="canonical_attach_invalid",
    prefix="canonical_attach",
    file_name="input_data/scb_canonical/lisa_canonical.toml",
)

# Strict-bool leaf for the PII/identifier guardrail flags — same shared rule as
# `sources/curated.py`'s `is_identifier`/`is_sensitive`, bound to this surface's
# code/path. Absent → False (DDL default); a present non-bool is rejected.
_require_bool = functools.partial(
    require_bool,
    code="canonical_attach_invalid",
    prefix="canonical_attach",
    file_name="input_data/scb_canonical/lisa_canonical.toml",
)


@dataclass(frozen=True)
class _CanonicalAttach:
    """One `[[attach]]`: mint a canonical-SCB variable delivered as `column`
    under the existing `scb`-provider register `register` (a 2-segment
    `provider/register` slug FQID), variant `variant`. `definition` → the
    variable's `description`; the window is `[valid_from, valid_to]` (open-ended
    when `valid_to` is None). `classification`, when set, is an existing catalog
    classification short_name linked via the shared backfill."""

    provider: str
    register: str
    variant: str
    column: str
    name: str
    definition: str
    data_type: str
    valid_from: str
    valid_to: str | None  # None → open-ended (materializer writes the sentinel)
    classification: str | None
    # PII / identifier guardrails — must survive onto the minted row exactly like
    # the CanonicalScbAdapter sets them (`is_identifier`/`is_sensitive` on a
    # curated [[register.variable]]). Omitting them would default to 0, publishing
    # a person/org-number identifier column as ordinary text. Default False; set
    # to match the column's existing catalog sibling.
    is_identifier: bool
    is_sensitive: bool


def canonical_attach_path(canonical_dir: Path | None) -> Path | None:
    """`<canonical_dir>/lisa_canonical.toml`. `canonical_dir` is the active
    `CanonicalScbAdapter`'s paired `--input-dir/scb_canonical/` (the directory it
    reads `scb_canonical.toml` from), so the two co-located canonical-SCB seeds
    resolve from the SAME root — NOT package-relative like the graft/relation
    curation TOMLs.

    `canonical_dir is None` ⇒ this build has no canonical-SCB adapter at all
    (synthetic / SCB-only without the dir / SOS-only) → None (legitimately
    no-op). But when `canonical_dir` IS present, `lisa_canonical.toml` is a
    committed part of that seed (#400): a missing file then means a STALE
    `--input-dir` checkout that predates this content, so we'd silently mint 0
    attaches and omit the 32 documented LISA variables. Fail fast with the same
    staleness discipline as the #556 `scb_canonical_seed_missing` preflight rather
    than skip silently."""
    if canonical_dir is None:
        return None
    candidate = canonical_dir / "lisa_canonical.toml"
    if not candidate.is_file():
        raise curation_error(
            "canonical_attach_seed_missing",
            f"Canonical-attach seed not found: {candidate}. It is a committed part "
            "of the scb_canonical seed (#400), but this build's scb_canonical/ "
            "directory lacks it.",
            "input_data/scb_canonical/lisa_canonical.toml is a small committed seed; "
            "if --input-dir points at a separate seed checkout it likely predates "
            "this content — update it (e.g. `git -C <seed-checkout> pull`).",
        )
    return candidate


def load_canonical_attach(
    path: Path | None, *, classification_seed_path: Path | None = None
) -> list[_CanonicalAttach]:
    """Parse the canonical-attach TOML. Empty when no file (synthetic builds,
    wheel installs — like `load_variable_grafts(None)`).

    Strict load (EXIT_CONFIG): only `[[attach]]`; every required key present; no
    unknown key; `register` a 2-segment `provider/register` FQID; ISO
    `valid_from` (and `valid_to` when present) with `valid_from <= valid_to`;
    `data_type` in the canonical vocabulary; `classification` (when present) a
    declared catalog short_name; `is_identifier`/`is_sensitive` (when present)
    real TOML booleans; each `(register, variant, column)` unique.
    """
    # Shared read + strict top-level-key / array-of-tables / per-entry-table
    # scaffold (same as variable_grafts), keeping the established
    # `canonical_attach_{toml_unreadable,invalid}` codes. `file_name` carries the
    # seed's repo-relative subpath since it lives under `input_data/`, not the
    # package root like the curation TOMLs.
    entries = load_curation_entries(
        path,
        entry_key="attach",
        label="canonical-attach",
        prefix="canonical_attach",
        code_base="canonical_attach",
        file_name="input_data/scb_canonical/lisa_canonical.toml",
        entry_fields="register / variant / column / name / definition / "
        "data_type / valid_from",
    )

    out: list[_CanonicalAttach] = []
    seen: set[tuple[str, str, str]] = set()
    for entry in entries:
        out.append(_load_one(entry, seen))

    # Validate `classification` references against the seed manifest in a single
    # pass once everything is parsed (PROVIDER-AGNOSTIC: any declared short_name
    # passes; only an UNDECLARED one — a typo — fails). Resolve the seed only when
    # something references a classification (mirrors curated.py:238-257).
    if any(a.classification is not None for a in out):
        declared = declared_short_names(classification_seed_path)
        for a in out:
            if a.classification is not None and a.classification not in declared:
                raise curation_error(
                    "canonical_attach_invalid",
                    f"canonical_attach {a.register}/{a.variant}/{a.column}: "
                    f"classification {a.classification!r} is not a declared "
                    "classification (classifications.toml).",
                    "Use an existing classification short_name (e.g. 'SSYK96') or "
                    "declare it in classifications.toml.",
                )
    return out


def _load_one(entry: dict, seen: set[tuple[str, str, str]]) -> _CanonicalAttach:
    # `load_curation_entries` already guaranteed each entry is a `[[attach]]`
    # table; only the per-FIELD validation remains here.
    unknown = sorted(set(entry) - _ALLOWED_KEYS)
    if unknown:
        raise curation_error(
            "canonical_attach_invalid",
            f"canonical_attach entry has unknown key(s) {unknown}.",
            f"Allowed keys: {sorted(_ALLOWED_KEYS)}.",
        )
    missing = sorted(_REQUIRED_KEYS - set(entry))
    if missing:
        raise curation_error(
            "canonical_attach_invalid",
            f"canonical_attach entry is missing required key(s) {missing}.",
            f"Required keys: {sorted(_REQUIRED_KEYS)}.",
        )

    register_fqid = _require_str(entry, "register", "[[attach]]")
    parts = register_fqid.split("/")
    if len(parts) != 2 or not all(parts):
        raise curation_error(
            "canonical_attach_invalid",
            f"canonical_attach register {register_fqid!r} must be a 2-segment "
            "`provider/register` FQID.",
            'Give `register = "scb/lisa"`-style 2-segment FQIDs.',
        )
    # Canonical-SCB-only: the materializer mints every id via
    # `mint_canonical_scb("scb", …)` (the reserved SCB sub-band) and stamps
    # `source_label = "canonical-scb"` unconditionally. A non-scb provider here
    # would mint into the SCB namespace and mislabel the row, breaking the
    # id-disjointness/band rationale — reject it at load time.
    if parts[0] != "scb":
        raise curation_error(
            "canonical_attach_invalid",
            f"canonical_attach register {register_fqid!r} has provider "
            f"{parts[0]!r}: the canonical-attach seed only supports `scb/...` "
            "registers.",
            'Give an `scb/<register>` FQID (e.g. `register = "scb/lisa"`); the '
            "canonical-attach pass is canonical-SCB-only.",
        )
    variant = _require_str(entry, "variant", "[[attach]]")
    column = _require_str(entry, "column", "[[attach]]")
    name = _require_str(entry, "name", "[[attach]]")
    definition = _require_str(entry, "definition", "[[attach]]")

    data_type = _require_str(entry, "data_type", "[[attach]]")
    if data_type not in _DATA_TYPES:
        raise curation_error(
            "canonical_attach_invalid",
            f"canonical_attach {register_fqid}/{variant}/{column}: data_type "
            f"{data_type!r} is not one of {sorted(_DATA_TYPES)}.",
            f"Use a canonical data_type: {sorted(_DATA_TYPES)}.",
        )

    valid_from = _require_str(entry, "valid_from", "[[attach]]")
    _check_iso(valid_from, f"{register_fqid}/{variant}/{column} valid_from")
    valid_to = entry.get("valid_to")
    if valid_to is not None:
        if not isinstance(valid_to, str) or not valid_to.strip():
            raise curation_error(
                "canonical_attach_invalid",
                f"canonical_attach {register_fqid}/{variant}/{column}: `valid_to` "
                f"must be a non-empty ISO date string when present, got {valid_to!r}.",
                "Give `valid_to` as YYYY-MM-DD or omit it for an open-ended window.",
            )
        valid_to = valid_to.strip()
        _check_iso(valid_to, f"{register_fqid}/{variant}/{column} valid_to")
        if valid_from > valid_to:
            raise curation_error(
                "canonical_attach_invalid",
                f"canonical_attach {register_fqid}/{variant}/{column}: valid_from "
                f"{valid_from!r} is after valid_to {valid_to!r}.",
                "Order the window so valid_from <= valid_to.",
            )

    classification = entry.get("classification")
    if classification is not None and (
        not isinstance(classification, str) or not classification.strip()
    ):
        raise curation_error(
            "canonical_attach_invalid",
            f"canonical_attach {register_fqid}/{variant}/{column}: `classification` "
            f"must be a non-empty string when present, got {classification!r}.",
            "Give an existing classification short_name or omit the key.",
        )
    classification = classification.strip() if classification is not None else None

    ctx = f"{register_fqid}/{variant}/{column}"
    is_identifier = _require_bool(entry, "is_identifier", ctx)
    is_sensitive = _require_bool(entry, "is_sensitive", ctx)

    # Column identity uses the ONE repo normalization rule (NFKD + ASCII-strip +
    # lower) so the dedup key folds EXACTLY like the SCB coalescer's node-col and
    # the gap-fill LOWER() match downstream.
    key = (parts[1], variant, fold_column(column))
    if key in seen:
        raise curation_error(
            "canonical_attach_invalid",
            f"canonical_attach duplicate attach {register_fqid}/{variant}/{column}.",
            "Each (register, variant, column) may appear once.",
        )
    seen.add(key)

    return _CanonicalAttach(
        provider=parts[0],
        register=parts[1],
        variant=variant,
        column=column,
        name=name,
        definition=definition,
        data_type=data_type,
        valid_from=valid_from,
        valid_to=valid_to,
        classification=classification,
        is_identifier=is_identifier,
        is_sensitive=is_sensitive,
    )


def _check_iso(value: str, ctx: str) -> None:
    # Same two-step ISO check as `sources/curated.py`'s `_check_iso` (kept local
    # here because that one is an adapter method raising `curated_toml_invalid`
    # with a `path.name` prefix — not a clean drop-in for this surface's
    # `canonical_attach_invalid` code/message).
    # `date.fromisoformat` accepts other ISO forms (e.g. `2010-01`), so length-pin
    # the exact YYYY-MM-DD shape first, then reject a calendar-impossible date.
    valid = len(value) == 10 and value[4] == "-" and value[7] == "-"
    if valid:
        try:
            date.fromisoformat(value)
        except ValueError:
            valid = False
    if not valid:
        raise curation_error(
            "canonical_attach_invalid",
            f"canonical_attach {ctx}: {value!r} must be a valid ISO date YYYY-MM-DD.",
            "Use a real ten-character ISO 8601 date.",
        )


def materialize_canonical_attach(
    conn: sqlite3.Connection,
    entries: list[_CanonicalAttach],
    *,
    providers: frozenset[str],
    classification_candidates: list[tuple[int, int | None, str]],
    warn: Callable[[str], None] | None = None,
) -> dict[str, int]:
    """Mint variable + state + alias for each attach, onto its existing
    `(register, variant)`. Runs AFTER `populate_slugs` (register/variant slugs
    resolve the target) and BEFORE `populate_variable_slugs` (the minted
    variable's NULL slug auto-derives from its delivery column). `providers`
    gates entries to this build.

    Ids come from `mint_canonical_scb` (the canonical-SCB sub-band `[2^61, 2^62)`,
    deterministic), NOT a MAX+1 sequence. An entry's `classification`, when set,
    is appended to `classification_candidates` (value_set_id None) — the same list
    `materialize` feeds to `_feed_classification_candidates`, so the existing
    backfill tags `variable_state.classification_id`.

    Returns `{minted, skipped, unresolved}`. `skipped` = the column already exists
    as a state in that variant (never duplicate); `unresolved` = the register or
    variant slug didn't resolve."""
    warn = warn or (lambda _msg: None)
    counts = {"minted": 0, "skipped": 0, "unresolved": 0}
    active = [a for a in entries if a.provider in providers]
    if not active:
        return counts

    for a in active:
        resolved = resolve_register_variant_id(conn, a.provider, a.register, a.variant)
        if resolved is None:
            counts["unresolved"] += 1
            continue
        register_variant_id, register_id = resolved
        # Gap-fill only: skip if the column is already a delivered state column in
        # this variant (case/diacritic-folded — same LOWER() match as grafts).
        exists = conn.execute(
            "SELECT 1 FROM variable_state vs JOIN variable v "
            "ON vs.variable_id = v.variable_id "
            "WHERE v.register_id = ? AND vs.register_variant_id = ? "
            "AND LOWER(vs.delivery_column_name) = LOWER(?) LIMIT 1",
            (register_id, register_variant_id, a.column),
        ).fetchone()
        if exists:
            counts["skipped"] += 1
            continue

        variable_id = mint_canonical_scb("scb", a.register, a.variant, a.column)
        state_id = mint_canonical_scb("scb", a.register, a.variant, a.column, "state")
        conn.execute(
            "INSERT INTO variable (variable_id, register_id, provider_key, name, "
            "description, source_label, is_identifier, is_sensitive) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                variable_id,
                register_id,
                a.column,  # the column is the natural key (matches CuratedAdapter)
                a.name,
                a.definition,
                CANONICAL_ATTACH_SOURCE_LABEL,
                # PII/identifier guardrails — carry the seed flags onto the row so a
                # minted identifier column (PeOrgNrSregJ, …) is NOT published as
                # ordinary text. INTEGER columns; the seed carries bools.
                int(a.is_identifier),
                int(a.is_sensitive),
            ),
        )
        conn.execute(
            "INSERT INTO variable_state (state_id, variable_id, register_variant_id, "
            "valid_from, valid_to, data_type, delivery_column_name) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                state_id,
                variable_id,
                register_variant_id,
                a.valid_from,
                a.valid_to or _VALID_TO_SENTINEL,
                a.data_type,
                a.column,
            ),
        )
        conn.execute(
            "INSERT INTO variable_alias (variable_id, register_variant_id, "
            "delivery_column_name) VALUES (?, ?, ?)",
            (variable_id, register_variant_id, a.column),
        )
        if a.classification is not None:
            # ONE candidate per variable (value_set_id None — no codes): the
            # shared backfill resolves short_name → classification_id and tags
            # this variable's state. Appended to the SAME list `materialize`
            # threads to `_feed_classification_candidates`.
            classification_candidates.append((variable_id, None, a.classification))
        counts["minted"] += 1

    if counts["unresolved"]:
        warn(
            f"  WARN canonical-attach: {counts['unresolved']:,} attach(es) did not "
            "resolve (register/variant slug churn) — regenerate lisa_canonical.toml"
        )
    return counts
