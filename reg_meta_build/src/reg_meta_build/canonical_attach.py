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
  `CanonicalScbAdapter` uses (#444), so an attach row is indistinguishable from a
  full-adapter canonical row except that it lands on an existing register.
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

import tomllib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

from ._curation import curation_error
from .classifications import declared_short_names
from .id import mint_canonical_scb

if TYPE_CHECKING:
    import sqlite3
    from collections.abc import Callable

# The `variable.source_label` stamped on every attached row — the canonical
# analog of `variable_grafts`' `swecov-graft`. Distinct from every existing
# label (`swecov-graft`; the SCB/SOS adapters leave it NULL), so the validation
# invariant (every `canonical-scb` row is `is_canonical_scb`) is unambiguous.
CANONICAL_ATTACH_SOURCE_LABEL = "canonical-scb"

# Open-ended validity sentinel — same exact string the materializer / validator
# (_check_open_ended_sentinel) demand for an omitted upper bound.
_VALID_TO_SENTINEL = "9999-12-31"

# data_type vocabulary shared with scb_canonical.toml (CanonicalScbAdapter). The
# attach only stores the string verbatim on the state; this gate keeps a typo
# (`txt`, `int`) from silently shipping a meaningless data_type.
_DATA_TYPES = frozenset({"text", "decimal", "integer", "date"})

_REQUIRED_KEYS = frozenset(
    {"register", "variant", "column", "name", "definition", "data_type", "valid_from"}
)
_OPTIONAL_KEYS = frozenset({"valid_to", "classification"})
_ALLOWED_KEYS = _REQUIRED_KEYS | _OPTIONAL_KEYS


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


def repo_canonical_attach_path() -> Path | None:
    """`input_data/scb_canonical/lisa_canonical.toml` from a repo checkout, or
    None (wheel installs / synthetic builds don't ship the seed). Lives beside
    the other canonical-SCB seed (`scb_canonical.toml`), NOT at the package root
    like the graft/relation TOMLs — it's source-delivery seed data, not curation."""
    candidate = (
        Path(__file__).resolve().parent.parent.parent
        / "input_data"
        / "scb_canonical"
        / "lisa_canonical.toml"
    )
    return candidate if candidate.is_file() else None


def load_canonical_attach(
    path: Path | None, *, classification_seed_path: Path | None = None
) -> list[_CanonicalAttach]:
    """Parse the canonical-attach TOML. Empty when no file (synthetic builds,
    wheel installs — like `load_variable_grafts(None)`).

    Strict load (EXIT_CONFIG): only `[[attach]]`; every required key present; no
    unknown key; `register` a 2-segment `provider/register` FQID; ISO
    `valid_from` (and `valid_to` when present) with `valid_from <= valid_to`;
    `data_type` in the canonical vocabulary; `classification` (when present) a
    declared catalog short_name; each `(register, variant, column)` unique.
    """
    if path is None or not path.is_file():
        return []
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise curation_error(
            "canonical_attach_toml_unreadable",
            f"Could not parse canonical-attach TOML {path}: {exc}",
            "Fix the TOML syntax in input_data/scb_canonical/lisa_canonical.toml.",
        ) from exc

    unknown_top = set(data) - {"attach"}
    if unknown_top:
        raise curation_error(
            "canonical_attach_invalid",
            f"canonical_attach TOML has unknown top-level key(s): {sorted(unknown_top)}.",
            "The only legal table is `[[attach]]` — check for a typo like `[[attaches]]`.",
        )
    entries = data.get("attach", [])
    if not isinstance(entries, list):
        raise curation_error(
            "canonical_attach_invalid",
            f"canonical_attach `attach` must be an array of tables (`[[attach]]`), "
            f"got {type(entries).__name__}.",
            "Use `[[attach]]` table entries, not `attach = …` or a single `[attach]`.",
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


def _load_one(entry: object, seen: set[tuple[str, str, str]]) -> _CanonicalAttach:
    if not isinstance(entry, dict):
        raise curation_error(
            "canonical_attach_invalid",
            f"canonical_attach entry {entry!r} must be an `[[attach]]` table.",
            "Each entry is an `[[attach]]` table with register / variant / column / "
            "name / definition / data_type / valid_from.",
        )
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

    register_fqid = _req_str(entry, "register")
    parts = register_fqid.split("/")
    if len(parts) != 2 or not all(parts):
        raise curation_error(
            "canonical_attach_invalid",
            f"canonical_attach register {register_fqid!r} must be a 2-segment "
            "`provider/register` FQID.",
            'Give `register = "scb/lisa"`-style 2-segment FQIDs.',
        )
    variant = _req_str(entry, "variant")
    column = _req_str(entry, "column")
    name = _req_str(entry, "name")
    definition = _req_str(entry, "definition")

    data_type = _req_str(entry, "data_type")
    if data_type not in _DATA_TYPES:
        raise curation_error(
            "canonical_attach_invalid",
            f"canonical_attach {register_fqid}/{variant}/{column}: data_type "
            f"{data_type!r} is not one of {sorted(_DATA_TYPES)}.",
            f"Use a canonical data_type: {sorted(_DATA_TYPES)}.",
        )

    valid_from = _req_str(entry, "valid_from")
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

    key = (parts[1], variant, column.casefold())
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
    )


def _req_str(entry: dict, field: str) -> str:
    value = entry.get(field)
    if not isinstance(value, str) or not value.strip():
        raise curation_error(
            "canonical_attach_invalid",
            f"canonical_attach `{field}` must be a non-empty string, got {value!r}.",
            f'Give `{field} = "<value>"`.',
        )
    return value.strip()


def _check_iso(value: str, ctx: str) -> None:
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
        variant_row = conn.execute(
            "SELECT rv.register_variant_id, r.register_id FROM register_variant rv "
            "JOIN register r ON r.register_id = rv.register_id "
            "JOIN provider p ON p.provider_id = r.provider_id "
            "WHERE p.slug = ? AND r.slug = ? AND rv.slug = ?",
            (a.provider, a.register, a.variant),
        ).fetchone()
        if variant_row is None:
            counts["unresolved"] += 1
            continue
        register_variant_id, register_id = variant_row
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
            "description, source_label) VALUES (?, ?, ?, ?, ?, ?)",
            (
                variable_id,
                register_id,
                a.column,  # the column is the natural key (matches CuratedAdapter)
                a.name,
                a.definition,
                CANONICAL_ATTACH_SOURCE_LABEL,
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
