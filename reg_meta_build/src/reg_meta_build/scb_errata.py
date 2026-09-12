"""SCB export errata (Y-114) — the upstream-error log for rows SCB's
`Registerinformation.csv` omits but SCB actually delivered.

`Registerinformation.csv` is one row per (register variant, version, variable,
column); the coalescer turns those rows into `variable_state` windows. When SCB
omits a row, nothing downstream can put it back — grafts and `canonical_attach`
mint a NEW variable (the attach loader refuses a column that already exists) and
`delivery_enrichment` only backfills prose. So the correction is made at the
PROVIDER'S OWN GRAIN: `scb_errata.toml` entries become synthetic
Registerinformation rows INSIDE the SCB adapter, early enough that every pass
after the import sees them. Windows, gaps, fusing, alias windows, types, value
sets, the A1.2 sensitivity/identifier lift and the classification backfill then
fall out of the existing passes — there is no post-pass state surgery and
no generic `variable_state_overrides.toml` (see DESIGN.md → Curation surface
taxonomy).

Two typed entry kinds:

* `[[version]]` — a register version SCB has not documented. Mints a
  `register_version` row.
* `[[delivered]]` — the (version, column) rows SCB omitted. Clones the column's
  attributes (var_id, Variabelnamn, definition-carrying prose, Datatyp,
  Datalängd, value-set link, grain) from the NEAREST real version of the same
  (variant, column) — EVERY row that edition carries for the column, so a column
  co-delivered there under two variables is replayed as two rows. Each synthetic
  row lands in the same coalescer group as the real row it copies and simply
  extends that group's claim years.

The curator names SCB versions, never dates: a version name is the coordinate
SCB itself publishes, and the year parsing already lives in `edition_bounds`.

Ids come from `mint_canonical_scb` — the reserved SCB sub-band `[2^61, 2^62)`
for rows that belong to the `scb` provider but are absent from its machine
export. Deterministic (same entry → same id every build) and disjoint from every
real source-derived cvid/regver_id by construction. That band sits above every
id SCB's export can produce, and the coalescer reads `regver_id` order as era
order — so a `[[version]]`-declared edition is unconditionally the LATEST era for
the latest-alias / latest-type trackers. Rather than leave that to chance, a
declared edition that does not START at or after the variant's newest documented
year is REFUSED (`scb_errata_version_not_latest`): the motivating shape (SCB
documents 2019-2020 for a register the steward holds through 2023) is exactly
what the band gets right, and the rest would silently overwrite a real delivery's
published spelling and type. Several declared editions on ONE variant still carry
no chronology relative to each OTHER — reserved-band ids order by hash, not by
year — which is inert as long as their `[[delivered]]` rows clone the same
nearest documented source, and that is the case this guard leaves open.

Self-cleaning: an entry whose row is present in SCB's export FAILS the build
(`scb_errata_now_present`), naming the version so a multi-version entry loses
only the version SCB fixed; git keeps the history of what was retired.
"""

from __future__ import annotations

import functools
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from ._curation import (
    curation_error,
    fold_column,
    load_curation_entries,
    require_str,
)
from .edition_bounds import edition_claims
from .fqid_slugs import (
    PROVIDER_FILE_SUFFIX,
    _parse_register_id,
    _parse_variant_id,
    load_provider_toml,
)
from .id import mint_canonical_scb

_FILE_NAME = "scb_errata.toml"
_CODE = "scb_errata_invalid"
# The provider whose export this surface corrects. `register` FQIDs are
# 2-segment and must name it — errata is an SCB-adapter operation, so another
# provider's slug could never resolve to an SCB (register, variant).
_PROVIDER = "scb"

_VERSION_FIELDS = frozenset({"register", "variant", "name", "evidence", "noted"})
_DELIVERED_FIELDS = frozenset(
    {"register", "variant", "column", "versions", "evidence", "noted", "upstream"}
)

_require_str = functools.partial(
    require_str, code=_CODE, prefix="scb_errata", file_name=_FILE_NAME
)


@dataclass(frozen=True)
class ErrataVersion:
    """One `[[version]]`, resolved to SCB source ids. `name` is SCB's
    `Registerversionnamn` token verbatim."""

    register_variant_id: int
    name: str


@dataclass(frozen=True)
class ErrataDelivered:
    """One `[[delivered]]`, resolved to SCB source ids. `column` is the curator's
    delivery-column spelling (matched folded, like every curated column key);
    `versions` are `Registerversionnamn` tokens verbatim."""

    register_id: int
    register_variant_id: int
    column: str
    versions: tuple[str, ...]


@dataclass(frozen=True)
class ScbErrata:
    """The loaded errata log. Empty when the file is absent (wheel installs,
    synthetic builds)."""

    versions: tuple[ErrataVersion, ...] = ()
    delivered: tuple[ErrataDelivered, ...] = ()

    def __bool__(self) -> bool:
        return bool(self.versions or self.delivered)


def repo_scb_errata_path() -> Path | None:
    """`reg_meta_build/scb_errata.toml` from a repo checkout, or None (wheel
    installs don't ship curation). Package-root sibling, like the other curation
    TOMLs."""
    candidate = Path(__file__).resolve().parent.parent.parent / _FILE_NAME
    return candidate if candidate.is_file() else None


def _scb_slug_ids(slug_dir: Path | None) -> tuple[dict[str, int], dict[str, int]]:
    """`({register_slug: register_id}, {"<register_slug>/<variant_slug>":
    register_variant_id})` from the curated `scb.toml`.

    The errata applies inside the adapter, BEFORE `populate_slugs` — the DB's
    slug columns are still NULL there, so the curated TOML (the same source
    `populate_slugs` writes from) is what resolves the entries' FQIDs.
    """
    registers: dict[str, int] = {}
    variants: dict[str, int] = {}
    if slug_dir is None:
        return registers, variants
    path = slug_dir / f"{_PROVIDER}{PROVIDER_FILE_SUFFIX}"
    if not path.is_file():
        return registers, variants
    # Deprecated entries are grow-only slug HISTORY, not live coordinates (the
    # rule `populate_slugs` and `slug_dir_curates_canonical_scb` both apply): a
    # retired slug must not shadow the live entry that replaced it.
    entries = [e for e in load_provider_toml(path) if not e.deprecated]
    by_register_id = {
        _parse_register_id(e.source_id): e.slug
        for e in entries
        if e.kind == "register" and e.slug
    }
    registers = {slug: rid for rid, slug in by_register_id.items()}
    for entry in entries:
        if entry.kind != "register_variant" or not entry.slug:
            continue
        register_id, variant_id = _parse_variant_id(entry.source_id)
        register_slug = by_register_id.get(register_id)
        if register_slug is not None:
            variants[f"{register_slug}/{entry.slug}"] = variant_id
    return registers, variants


def _unknown_keys(entry: dict, allowed: frozenset[str], table: str) -> None:
    unknown = sorted(set(entry) - allowed)
    if unknown:
        raise curation_error(
            _CODE,
            f"scb_errata [[{table}]] entry has unknown key(s): {unknown}.",
            f"A [[{table}]] entry takes only {sorted(allowed)} — "
            f"fix the typo in reg_meta_build/{_FILE_NAME}.",
        )


def _require_noted(entry: dict, context: str) -> None:
    """`noted` is the date the maintainer recorded the omission. Validated (the
    log is only useful if every entry is dated) but not carried into the build —
    like `evidence`, it documents the entry for whoever retires it."""
    noted = _require_str(entry, "noted", context)
    try:
        parsed = date.fromisoformat(noted)
    except ValueError:
        parsed = None
    if parsed is None or parsed.isoformat() != noted:
        raise curation_error(
            _CODE,
            f"scb_errata {context} needs `noted` as YYYY-MM-DD, got {noted!r}.",
            'Use the date the omission was recorded, e.g. `noted = "2026-09-11"`.',
        )


def _resolve_variant(
    entry: dict,
    table: str,
    registers: dict[str, int],
    variants: dict[str, int],
) -> tuple[int, int, str]:
    """`(register_id, register_variant_id, "<register_fqid>/<variant>")` for an
    entry's `register` / `variant` pair."""
    register_fqid = _require_str(entry, "register", f"[[{table}]]")
    parts = register_fqid.split("/")
    if len(parts) != 2 or not all(parts):
        raise curation_error(
            _CODE,
            f"scb_errata register {register_fqid!r} must be a 2-segment "
            "`provider/register` FQID.",
            'Give `register = "scb/lisa"`-style 2-segment FQIDs.',
        )
    provider, register = parts
    variant = _require_str(entry, "variant", f"[[{table}]]")
    context = f"{register_fqid}/{variant}"
    if provider != _PROVIDER:
        raise curation_error(
            "scb_errata_unknown_variant",
            f"scb_errata {context} names provider {provider!r}; this surface "
            f"corrects the {_PROVIDER!r} export only.",
            f"Move the entry to the {provider!r} adapter's own curation, or fix "
            f"the FQID in reg_meta_build/{_FILE_NAME}.",
        )
    register_id = registers.get(register)
    variant_id = variants.get(f"{register}/{variant}")
    if register_id is None or variant_id is None:
        raise curation_error(
            "scb_errata_unknown_variant",
            f"scb_errata {context} does not name a curated SCB "
            f"{'register' if register_id is None else 'register variant'}.",
            f"Use a `[register]` / `[register_variant]` slug curated in "
            f"fqid_slugs/{_PROVIDER}{PROVIDER_FILE_SUFFIX}.",
        )
    return register_id, variant_id, context


def load_scb_errata(path: Path | None, slug_dir: Path | None) -> ScbErrata:
    """Parse the errata TOML, resolving each entry's `register`/`variant` slugs
    against the curated `scb.toml` in `slug_dir`. Empty when no file (synthetic
    builds, wheel installs).

    Strict load, all EXIT_CONFIG with a remediation: only `[[version]]` /
    `[[delivered]]` top-level; no unknown key inside an entry; `register` a
    2-segment SCB FQID and `register`/`variant` curated; `evidence` and `noted`
    (canonical `YYYY-MM-DD`) present; `versions` a non-empty list of non-empty
    strings naming each version at most once; and no duplicate `(variant, name)`
    / `(variant, column)` entry — two entries for one column must be ONE entry
    listing both versions, or the log stops being readable as the record of what
    SCB missed.
    """
    version_entries = load_curation_entries(
        path,
        entry_key="version",
        label="SCB-errata",
        prefix="scb_errata",
        code_base="scb_errata",
        file_name=_FILE_NAME,
        entry_fields="register / variant / name / evidence / noted",
        sibling_keys=frozenset({"delivered"}),
    )
    delivered_entries = load_curation_entries(
        path,
        entry_key="delivered",
        label="SCB-errata",
        prefix="scb_errata",
        code_base="scb_errata",
        file_name=_FILE_NAME,
        entry_fields="register / variant / column / versions / evidence / noted",
        sibling_keys=frozenset({"version"}),
    )
    if not version_entries and not delivered_entries:
        # Before touching the slug dir: resolving FQIDs parses the whole
        # curated scb.toml (~20k entries), and the common case — no file, or a
        # build whose provider set never reaches it — has nothing to resolve.
        return ScbErrata()
    registers, variants = _scb_slug_ids(slug_dir)

    versions: list[ErrataVersion] = []
    seen_versions: set[tuple[int, str]] = set()
    for entry in version_entries:
        _unknown_keys(entry, _VERSION_FIELDS, "version")
        _, variant_id, context = _resolve_variant(entry, "version", registers, variants)
        name = _require_str(entry, "name", f"[[version]] {context}")
        _require_str(entry, "evidence", f"[[version]] {context}/{name}")
        _require_noted(entry, f"[[version]] {context}/{name}")
        if (variant_id, name) in seen_versions:
            raise curation_error(
                _CODE,
                f"scb_errata duplicate [[version]] {context}/{name}.",
                "Each (register, variant, name) may appear once.",
            )
        seen_versions.add((variant_id, name))
        versions.append(ErrataVersion(variant_id, name))

    delivered: list[ErrataDelivered] = []
    seen_columns: set[tuple[int, str]] = set()
    for entry in delivered_entries:
        _unknown_keys(entry, _DELIVERED_FIELDS, "delivered")
        register_id, variant_id, context = _resolve_variant(
            entry, "delivered", registers, variants
        )
        column = _require_str(entry, "column", f"[[delivered]] {context}")
        ctx = f"[[delivered]] {context}/{column}"
        _require_str(entry, "evidence", ctx)
        _require_noted(entry, ctx)
        raw_versions = entry.get("versions")
        if (
            not isinstance(raw_versions, list)
            or not raw_versions
            or not all(isinstance(v, str) and v.strip() for v in raw_versions)
        ):
            raise curation_error(
                _CODE,
                f"scb_errata {ctx} needs `versions` as a non-empty list of "
                f"`Registerversionnamn` strings, got {raw_versions!r}.",
                'Give `versions = ["2010", "2011"]` — SCB version names verbatim, '
                "never dates.",
            )
        named = tuple(v.strip() for v in raw_versions)
        repeated = sorted({n for n in named if named.count(n) > 1})
        if repeated:
            raise curation_error(
                _CODE,
                f"scb_errata {ctx} repeats version(s) {repeated} in `versions`.",
                "Each version may appear once in an entry's `versions` — the "
                "second mention would mint the same synthetic row twice.",
            )
        if "upstream" in entry and not isinstance(entry["upstream"], str):
            raise curation_error(
                _CODE,
                f"scb_errata {ctx} `upstream` must be a string when present, "
                f"got {entry['upstream']!r}.",
                'Give `upstream = "<class>"` as a string, or omit it.',
            )
        key = (variant_id, fold_column(column))
        if key in seen_columns:
            raise curation_error(
                _CODE,
                f"scb_errata duplicate [[delivered]] {context}/{column}.",
                "Each (register, variant, column) may appear once — list every "
                "omitted version in that entry's `versions`.",
            )
        seen_columns.add(key)
        delivered.append(ErrataDelivered(register_id, variant_id, column, named))

    return ScbErrata(tuple(versions), tuple(delivered))


# Columns cloned from a real source row onto the synthetic one. `cvid` /
# `regver_id` are minted; `classification_id` and `variable_id` stay NULL —
# they are stamped by later passes that see the synthetic row like any other.
_CLONED = (
    "var_id",
    "variabelnamn",
    "data_type",
    "data_length",
    "value_set_version_label",
    "vardemangdsniva",
    "operational_definition",
    "source_register_text",
    "value_set_id",
)


def _edition_years(name: str) -> tuple[int, ...]:
    """The calendar years an edition NAME claims, ascending — read through the
    coalescer's own parser (`edition_bounds.edition_claims`), so the era guard
    below and the windows a synthetic row goes on to produce cannot disagree
    about what a version covers. Empty when the name carries no parseable year.

    Name-only, without `sources/scb.py::register_edition_claims`' projection-set
    policy (a forecast register's version IS its vintage, so it claims one year
    rather than its span). Reading a projection register's span in full can only
    push the documented top LATER, i.e. refuse a declared edition the policy
    would have allowed — the safe direction, and it keeps the guard off the
    adapter's import cycle.
    """
    return tuple(year for year, _lo, _hi in edition_claims(name))


def _check_declared_era(context: str, name: str, documented_top: int | None) -> None:
    """A `[[version]]` may declare only the variant's NEWEST era.

    A declared edition's `regver_id` is minted into the reserved canonical-SCB
    band, so it sorts above every id SCB's export can produce — and the coalescer
    reads `regver_id` order as era order for `latest_alias` / `latest_type`. An
    edition declared BEFORE the documented range would therefore publish its
    cloned column spelling and type/length as the variable's latest, silently
    overriding the genuinely newest delivery. Refuse rather than publish that.

    Same-year is fine (a declared `HT2022` alongside a documented `2022` is the
    ordinary within-year convention), and a variant with no year-bearing
    documented edition has nothing to be newer than.
    """
    if documented_top is None:
        return
    years = _edition_years(name)
    if years and years[0] >= documented_top:
        return
    detail = (
        f"it starts in {years[0]}, before the variant's latest documented "
        f"year ({documented_top})"
        if years
        else f"no year parses from the name, so it cannot be placed after the "
        f"variant's latest documented year ({documented_top})"
    )
    raise curation_error(
        "scb_errata_version_not_latest",
        f"scb_errata {context} cannot be declared: {detail}.",
        "A [[version]] edition is minted into the reserved canonical-SCB id "
        "band, which sorts above every id SCB's export can produce, and the "
        "coalescer reads that order as era order — so the build can only honour "
        "a declared edition that is the variant's newest. Name a later edition, "
        "or point the [[delivered]] entry at a version SCB already documents, "
        f"in reg_meta_build/{_FILE_NAME}.",
    )


def _now_present(context: str, what: str, remedy: str) -> None:
    """Self-cleaning failure: SCB now ships what an entry says it omitted.

    `remedy` is what the maintainer should actually do, which is NOT always
    "delete the entry" — one version of a multi-version `[[delivered]]` can be
    fixed upstream while the rest of the entry still records a live omission.
    """
    raise curation_error(
        "scb_errata_now_present",
        f"scb_errata {context}: {what} is present in SCB's export.",
        f"SCB fixed it — {remedy} in reg_meta_build/{_FILE_NAME} "
        "(git keeps the record of the upstream error).",
    )


def apply_scb_errata(conn: sqlite3.Connection, errata: ScbErrata) -> dict[str, int]:
    """Write the errata entries as synthetic Registerinformation rows.

    Runs inside `SCBAdapter.emit()` after the value-set projection — so a cloned
    row carries its column's real value-set link — and before the A1.2
    sensitivity lift and the coalescer, so the PII/identifier classification and
    every pass after it read the synthetic rows as ordinary deliveries.

    Returns `{"versions": n, "rows": n}`. Raises EXIT_CONFIG when an entry
    names a variant this export doesn't have (`scb_errata_unknown_variant`), a
    version the variant neither documents nor declares
    (`scb_errata_unknown_version`), a column with no real row anywhere on the
    variant (`scb_errata_no_source_row` — that is a graft/attach, not errata), a
    `[[version]]` that is not the variant's newest edition
    (`scb_errata_version_not_latest`), or a version/row SCB now ships
    (`scb_errata_now_present`).
    """
    counts = {"versions": 0, "rows": 0}
    if not errata:
        return counts

    variant_ids = {e.register_variant_id for e in errata.versions} | {
        e.register_variant_id for e in errata.delivered
    }
    placeholders = ",".join("?" * len(variant_ids))
    params = tuple(sorted(variant_ids))
    live_variants = {
        row[0]
        for row in conn.execute(
            f"SELECT register_variant_id FROM register_variant "
            f"WHERE register_variant_id IN ({placeholders})",
            params,
        )
    }
    missing = variant_ids - live_variants
    if missing:
        raise curation_error(
            "scb_errata_unknown_variant",
            f"scb_errata names register variant(s) {sorted(missing)} that this "
            "SCB export does not contain.",
            f"Check the `register`/`variant` slugs in reg_meta_build/{_FILE_NAME} "
            "against the export, or re-export Registerinformation.csv.",
        )

    # (variant_id, registerversionnamn) → the editions carrying that name.
    documented: dict[tuple[int, str], list[int]] = {}
    for regver_id, variant_id, name in conn.execute(
        f"SELECT regver_id, register_variant_id, registerversionnamn "
        f"FROM register_version WHERE register_variant_id IN ({placeholders})",
        params,
    ):
        documented.setdefault((variant_id, name or ""), []).append(regver_id)

    # The newest year SCB's own export documents per variant. Computed before any
    # synthetic edition lands, so the era guard reads the same answer whatever
    # order the `[[version]]` entries appear in.
    documented_top: dict[int, int] = {}
    for variant_id, name in documented:
        years = _edition_years(name)
        if years:
            documented_top[variant_id] = max(
                documented_top.get(variant_id, years[-1]), years[-1]
            )

    for v in errata.versions:
        context = f"[[version]] {v.name}"
        if (v.register_variant_id, v.name) in documented:
            _now_present(
                context,
                f"version {v.name!r}",
                "delete the [[version]] entry (its [[delivered]] entries keep "
                "working against SCB's own edition)",
            )
        _check_declared_era(context, v.name, documented_top.get(v.register_variant_id))
        regver_id = mint_canonical_scb(
            "scb-errata-version", str(v.register_variant_id), v.name
        )
        conn.execute(
            "INSERT INTO register_version "
            "(regver_id, register_variant_id, registerversionnamn) VALUES (?, ?, ?)",
            (regver_id, v.register_variant_id, v.name),
        )
        documented[(v.register_variant_id, v.name)] = [regver_id]
        counts["versions"] += 1

    # Real rows for the errata's columns, keyed (variant_id, folded column). One
    # scan over the named variants; rows for other columns are dropped on read.
    wanted = {(e.register_variant_id, fold_column(e.column)) for e in errata.delivered}
    sources: dict[tuple[int, str], list[sqlite3.Row]] = {}
    if wanted:
        cur = conn.cursor()
        # By-name access without touching the build connection's own
        # tuple row_factory (the coalescer's idiom, `_coalesce_variable_states`).
        cur.row_factory = sqlite3.Row
        cur.execute(
            f"SELECT vi.cvid, vi.register_variant_id, vi.regver_id, "
            f"       va.delivery_column_name, rv.registerversionnamn, "
            f"       {', '.join('vi.' + c for c in _CLONED)} "
            f"FROM variable_instance vi "
            f"JOIN variable_alias_build va ON va.cvid = vi.cvid "
            f"JOIN register_version rv ON rv.regver_id = vi.regver_id "
            f"WHERE vi.register_variant_id IN ({placeholders})",
            params,
        )
        for row in cur:
            key = (
                row["register_variant_id"],
                fold_column(row["delivery_column_name"]),
            )
            if key in wanted:
                sources.setdefault(key, []).append(row)

    for d in errata.delivered:
        context = f"[[delivered]] {d.column}"
        candidates = sources.get((d.register_variant_id, fold_column(d.column)))
        if not candidates:
            raise curation_error(
                "scb_errata_no_source_row",
                f"scb_errata {context}: column {d.column!r} has no real row on "
                "this register variant.",
                "Errata re-adds an OMITTED delivery of a column SCB documents "
                "elsewhere on the variant. A column SCB never documents is a "
                "variable_grafts.toml graft or a canonical_attach entry.",
            )
        present = {row["regver_id"] for row in candidates}
        for name in d.versions:
            regvers = documented.get((d.register_variant_id, name))
            if not regvers:
                raise curation_error(
                    "scb_errata_unknown_version",
                    f"scb_errata {context}: version {name!r} is neither "
                    "documented by SCB nor declared by a [[version]] entry.",
                    f"Fix the `Registerversionnamn` spelling in "
                    f"reg_meta_build/{_FILE_NAME}, or add a [[version]] entry "
                    "for it.",
                )
            if present & set(regvers):
                # One version of a multi-version entry can be fixed upstream
                # while the rest still record a live omission — say which, or the
                # maintainer retires an entry that is still carrying its weight.
                _now_present(
                    context,
                    f"the {name!r} row for {d.column!r}",
                    "delete the entry"
                    if len(d.versions) == 1
                    else f"drop {name!r} from that entry's `versions`",
                )
            clones = _nearest_rows(candidates, name)
            for regver_id in regvers:
                for source in clones:
                    # Keyed on the SOURCE row's cvid as well: a co-delivered
                    # column contributes several clones to one (version, column),
                    # and each needs its own id.
                    cvid = mint_canonical_scb(
                        "scb-errata-row",
                        str(d.register_variant_id),
                        fold_column(d.column),
                        name,
                        str(regver_id),
                        str(source["cvid"]),
                    )
                    conn.execute(
                        f"INSERT INTO variable_instance "
                        f"(cvid, register_id, register_variant_id, regver_id, "
                        f"{', '.join(_CLONED)}) "
                        f"VALUES (?, ?, ?, ?, {', '.join('?' * len(_CLONED))})",
                        (
                            cvid,
                            d.register_id,
                            d.register_variant_id,
                            regver_id,
                            *(source[c] for c in _CLONED),
                        ),
                    )
                    # The source row's OWN spelling, not the curator's: inventing
                    # a spelling would shard the coalescer's case-twin components.
                    conn.execute(
                        "INSERT INTO variable_alias_build "
                        "(cvid, delivery_column_name) VALUES (?, ?)",
                        (cvid, source["delivery_column_name"]),
                    )
                    counts["rows"] += 1
    return counts


def _nearest_rows(candidates: list[sqlite3.Row], name: str) -> list[sqlite3.Row]:
    """The real rows to clone for added version `name`: the whole of the NEAREST
    real edition, ascending `cvid`.

    Distance is measured over the years the two names CLAIM, on both sides — a
    source edition spelled `2010-2012` is ONE year from an added `2013`, where
    reading only a name's first year would have put it three away and handed the
    clone to a `2015`. An equidistant tie goes to the LATER edition (the
    coalescer's own latest-era convention — a delivery's shape carries forward,
    not back), and a remaining tie, or a name no year parses from on either side,
    to the lowest `regver_id`.

    That edition can carry the column MORE THAN ONCE — a column co-delivered
    under two variables, each with its own value set and type. Every one of those
    rows is cloned, one synthetic row per source row: it is the faithful replay
    of what SCB delivered in the edition being copied, each clone lands in the
    coalescer group of the row it came from rather than dragging one group's
    value set onto the other, and the result does not depend on which row the
    unordered scan happened to return first.
    """
    target = _edition_years(name)

    def key(row: sqlite3.Row) -> tuple[int, int, int, int]:
        years = _edition_years(row["registerversionnamn"] or "")
        if not target or not years:
            return (1, 0, 0, row["regver_id"])
        return (
            0,
            min(abs(y - t) for y in years for t in target),
            0 if years[-1] >= target[-1] else 1,
            row["regver_id"],
        )

    nearest = min(candidates, key=key)["regver_id"]
    return sorted(
        (row for row in candidates if row["regver_id"] == nearest),
        key=lambda row: row["cvid"],
    )
