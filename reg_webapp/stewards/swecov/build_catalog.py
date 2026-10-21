#!/usr/bin/env python3
"""Build SWECOV steward metadata from SWECOV's holdings inventory CSV.

The inventory (``SWECOV_variables_full_<date>.csv``) has one row per physical
delivery table: ``Category`` (SWECOV-internal register name), ``Detail``
(variant), ``Table`` (physical table name with an embedded period token), and
``V1..V1396`` delivery column names (empty cells interspersed and trailing).

Stages (each a subcommand, deterministic, no network):

* ``normalize`` — parse the CSV into per-(Category, Detail) holdings: the
  union of delivery column names + the period set derived from table-name
  tokens, collapsed to contiguous segments (#307 interrupted-series list
  form for genuine gaps).
* ``ground`` — propose the (Category, Detail) → reg_meta (register, variant)
  mapping by EXACT delivery-column overlap against the reg_meta DB (no
  name-similarity / regex inference — standing rule). Categories outside the
  SCB/SOS catalog universe are reported as residue, never mapped.
* ``enrich`` — join per-column documentation (description, type, source
  attribution) from the SWECOV-internal delivery variable lists under
  ``--docs-root`` (see README.md in this directory) onto the holdings, apply
  the curated MAPPING, and emit ``<--out>/derived/holdings_enriched.json`` —
  the single derived artifact every downstream consumer (delivery inventory
  emission, reg_meta flavor ingest, alias curation) reads.

This script is tracked next to the steward spec it reads and maintainer-run:
its census CSV and delivery documents are confidential and live in the private
inputs repo, so every input path is an explicit flag and every intermediate
lands under ``--out``. Only the steward slug pins (``flavor``) are written into
this repo, under ``reg_meta_build/fqid_slugs/<steward>/``.

Run from the repo root::

    uv run python reg_webapp/stewards/swecov/build_catalog.py \\
        --csv <census CSV> enrich --docs-root <delivery-docs dir> --out <scratch dir>
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import sys
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import count, pairwise
from pathlib import Path
from typing import TYPE_CHECKING

from reg_meta_build.holdings_census import census_rows
from reg_meta_build.steward_spec import (
    STEWARDS_DIR,
    YEAR_INDEPENDENT_EDITION,
    StewardSpec,
    check_census,
    edition_bounds_of,
    load_steward_spec,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from reg_meta_build.inventory import ColumnMapping, EditionRange, MappingWarning
    from reg_meta_build.steward_spec import RawEdition

DEFAULT_DB = Path.home() / ".local/share/reg_meta/reg_meta.db"

# Pseudonymized-id prefix: ``P1105_LopNr_PERSONNR`` is the lopnr that replaced
# PERSONNR in SWECOV's delivery. Strip it to recover the original column name
# for catalog matching (the physical SWECOV column keeps the prefix).
LOPNR_PREFIX = re.compile(r"^P1105_LopNr_", re.IGNORECASE)

# Categories whose data does not come from SCB/SOS — outside the reg_meta
# catalog universe by construction. They go to the residue list, never into
# the steward catalog. Grounding still runs over them so an unexpected
# high-overlap match would surface in the report instead of being silently
# discarded.


# --- normalize: CSV -> holdings --------------------------------------------


@dataclass
class Holding:
    """Normalized holdings for one (Category, Detail) pair."""

    category: str
    detail: str
    tables: list[str] = field(default_factory=list)
    # Original-case column names as delivered (P1105_LopNr_ prefix intact).
    columns: set[str] = field(default_factory=set)
    # Per-table original-case column names. The union lives in `columns`; this
    # keeps the table→column association the union loses, so a `split` mapping
    # (selector = table-name stem) can place each column in its variant
    # (#365 graft variant tagging).
    table_columns: dict[str, set[str]] = field(default_factory=dict)
    # Period tokens per table: list of (table, tokens) for audit; the
    # pair-level set/segments are derived.
    table_periods: dict[str, list[str]] = field(default_factory=dict)

    @property
    def match_columns(self) -> set[str]:
        """Uppercased, lopnr-prefix-stripped column names for DB matching."""
        return {LOPNR_PREFIX.sub("", c).upper() for c in self.columns}

    def period_segments(self) -> list[object]:
        return collapse_periods(
            sorted({t for toks in self.table_periods.values() for t in toks})
        )


# Period-token extraction from physical table names. Rules are ordered most
# specific → most generic; the first rule that matches consumes the name.
# Grammar of emitted tokens (reg_meta period grammar): YYYY int, "YYYY-MM",
# "YYYY-Q[1-4]", "HTYYYY"/"VTYYYY".
# Left boundary mirrors fqid's period extractor: without it, letters ht/vt
# abutting digits inside a word (…avt2020) would mint a bogus term token that
# WINS over the year branch below.
_RE_TERM = re.compile(r"(?<![A-Za-z0-9])(HT|VT)(\d{4}|\d{2})(?!\d)", re.IGNORECASE)
_RE_DATE8 = re.compile(
    r"(?<!\d)((?:19|20)\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])(?!\d)"
)
_RE_MONTH = re.compile(r"(?<!\d)((?:19|20)\d{2})(0[1-9]|1[0-2])(?!\d)")
_RE_KSJU_QUARTER = re.compile(r"^KSju_((?:19|20)\d{2})([1-4])$", re.IGNORECASE)
_RE_YEAR = re.compile(r"(?<!\d)((?:19|20)\d{2})(?!\d)")
# Two-digit-year families (no 4-digit token anywhere in the name). Verified
# against the inventory: ElevGymn00..ElevGymn19 etc. Pivot: <60 → 20xx.
_RE_YY_FAMILY = re.compile(r"^(ElevGymn)(\d{2})$", re.IGNORECASE)


def _term_token(prefix: str, year: str) -> str:
    if len(year) == 2:
        year = ("20" if int(year) < 60 else "19") + year
    return f"{prefix.upper()}{year}"


def extract_periods(table: str) -> list[str]:
    """Period tokens embedded in a physical table name (possibly several)."""
    m = _RE_YY_FAMILY.match(table)
    if m:
        yy = int(m.group(2))
        return [str((2000 if yy < 60 else 1900) + yy)]
    m = _RE_KSJU_QUARTER.match(table)
    if m:
        return [f"{m.group(1)}-Q{m.group(2)}"]
    terms = [_term_token(p, y) for p, y in _RE_TERM.findall(table)]
    if terms:
        return sorted(set(terms))
    # Mask full dates first so YYYYMMDD doesn't shed a bogus YYYYMM month;
    # an 8-digit stock date (Doda_20211231) denotes the year.
    dates = _RE_DATE8.findall(table)
    masked = _RE_DATE8.sub("", table)
    months = [f"{y}-{mm}" for y, mm in _RE_MONTH.findall(masked)]
    masked = _RE_MONTH.sub("", masked)
    years = list(_RE_YEAR.findall(masked)) + [y for y, _, _ in dates]
    return sorted(set(months)) + sorted(set(years))


def _next_token(token: str) -> str:
    """Successor of a period token, for contiguity checks when collapsing."""
    if re.fullmatch(r"\d{4}", token):
        return str(int(token) + 1)
    m = re.fullmatch(r"(\d{4})-(\d{2})", token)
    if m:
        y, mm = int(m.group(1)), int(m.group(2))
        return f"{y + 1}-01" if mm == 12 else f"{y}-{mm + 1:02d}"
    m = re.fullmatch(r"(\d{4})-Q([1-4])", token)
    if m:
        y, q = int(m.group(1)), int(m.group(2))
        return f"{y + 1}-Q1" if q == 4 else f"{y}-Q{q + 1}"
    m = re.fullmatch(r"(HT|VT)(\d{4})", token)
    if m:
        season, y = m.group(1), int(m.group(2))
        return f"HT{y}" if season == "VT" else f"VT{y + 1}"
    return ""  # unknown grammar: never contiguous


def _sort_key(token: str) -> tuple:
    """Chronological sort across the token grammars used here."""
    m = re.fullmatch(r"(HT|VT)(\d{4})", token)
    if m:
        return (int(m.group(2)), 1 if m.group(1) == "VT" else 2, token)
    m = re.fullmatch(r"(\d{4})(?:-(?:0?(\d{1,2})|Q(\d)))?", token)
    if m:
        sub = int(m.group(2) or (int(m.group(3)) * 3 if m.group(3) else 0))
        return (int(m.group(1)), sub, token)
    return (9999, 0, token)


def collapse_periods(tokens: list[str]) -> list[object]:
    """Collapse sorted tokens to #307 segments: scalars + {from,to} ranges.

    Bare-year tokens become ints (the schema's year form); everything else
    stays a period-token string. Mixed grammars in one pair never merge into
    a single range (contiguity is only defined within one grammar).
    """

    def scalar(tok: str) -> object:
        return int(tok) if re.fullmatch(r"\d{4}", tok) else tok

    segments: list[object] = []
    run: list[str] = []
    for tok in sorted(set(tokens), key=_sort_key):
        if run and _next_token(run[-1]) == tok:
            run.append(tok)
            continue
        if run:
            segments.append(
                scalar(run[0])
                if len(run) == 1
                else {"from": scalar(run[0]), "to": scalar(run[-1])}
            )
        run = [tok]
    if run:
        segments.append(
            scalar(run[0])
            if len(run) == 1
            else {"from": scalar(run[0]), "to": scalar(run[-1])}
        )
    return segments


def parse_inventory(csv_path: Path) -> dict[tuple[str, str], Holding]:
    holdings: dict[tuple[str, str], Holding] = {}
    for category, detail, table, row_cols in census_rows(csv_path):
        holding = holdings.setdefault((category, detail), Holding(category, detail))
        holding.tables.append(table)
        holding.columns.update(row_cols)
        holding.table_columns.setdefault(table, set()).update(row_cols)
        holding.table_periods[table] = extract_periods(table)
    return holdings


# --- ground: holdings x reg_meta DB -> mapping proposal ---------------------


@dataclass(frozen=True)
class VariantInfo:
    coordinate: str  # provider/register/variant slug path
    register_name: str
    variant_name: str
    columns: frozenset[str]  # uppercased delivery_column_name set


def load_variant_index(db_path: Path) -> list[VariantInfo]:
    if not db_path.exists():
        raise SystemExit(f"reg_meta DB not found: {db_path} (run `reg-meta update`)")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    rows = conn.execute(
        """
        SELECT p.slug, r.slug, rv.slug, r.name, rv.name,
               UPPER(vs.delivery_column_name)
        FROM variable_state vs
        JOIN variable v USING (variable_id)
        JOIN register r ON r.register_id = v.register_id
        JOIN provider p ON p.provider_id = r.provider_id
        JOIN register_variant rv USING (register_variant_id)
        WHERE vs.delivery_column_name IS NOT NULL
        """
    ).fetchall()
    conn.close()
    grouped: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    names: dict[tuple[str, str, str], tuple[str, str]] = {}
    for p, r, rv, rname, rvname, col in rows:
        grouped[(p, r, rv)].add(col)
        names[(p, r, rv)] = (rname, rvname or "")
    return [
        VariantInfo("/".join(key), names[key][0], names[key][1], frozenset(cols))
        for key, cols in sorted(grouped.items())
    ]


@dataclass(frozen=True)
class Candidate:
    variant: VariantInfo
    matched: int
    csv_total: int

    @property
    def csv_share(self) -> float:
        return self.matched / self.csv_total if self.csv_total else 0.0

    @property
    def variant_share(self) -> float:
        return self.matched / len(self.variant.columns)


def ground_pair(
    holding: Holding, index: list[VariantInfo], top: int = 3
) -> list[Candidate]:
    cols = holding.match_columns
    scored = [
        Candidate(vi, len(cols & vi.columns), len(cols))
        for vi in index
        if cols & vi.columns
    ]
    # Deterministic order: matched desc, then tightest variant, then slug.
    scored.sort(
        key=lambda c: (-c.matched, len(c.variant.columns), c.variant.coordinate)
    )
    return scored[:top]


# --- curated mapping ---------------------------------------------------------
#
# (Category, Detail) -> register_variant coordinate, curated from the ``ground``
# report (column-overlap evidence; see the stage-1 report in the PR/issue
# trail). Statuses:
#   mapped         exact column grounding, single dominant candidate
#   split          one pair becomes several sources (period- or stem-driven);
#                  value is a list of (selector, coordinate, note)
#   flavor         no canonical register (or register absent from reg_meta);
#                  destined for the swecov-flavored DB ingest
#   lookup         SWECOV-side helper/crosswalk table, not register data
#   unmapped       retained physical holding without verified catalog ownership
# Evidence notes ride along so the emitted catalog can cite them.


# The steward spec every stage reads (reg_meta_build.steward_spec): census
# dispositions, census-holding routing, flavor definitions and the series. This
# generator's directory names its steward; the spec directory's name is the
# steward id every stage writes.
REPO_ROOT = Path(__file__).resolve().parents[3]
SPEC_DIR = REPO_ROOT / STEWARDS_DIR / Path(__file__).parent.name


def steward_id(spec_dir: Path) -> str:
    return spec_dir.resolve().name


def holding_mapping(
    spec: StewardSpec, category: str, detail: str, tables: list[str]
) -> dict:
    """The routing the enrichment stages read for one census holding.

    `to` maps each of the holding's series tables to its admissible variants:
    the series, not a table-name selector, says which variant a table delivers.
    `graft_columns` carries the curated `graft_onto` placements of those series,
    which the grafts stage reads before `to`. A
    holding whose tables are all lookups is a lookup; one with no route is
    residue.
    """
    lookups = {entry.table: entry.reason for entry in spec.scope.lookups}
    if tables and all(table in lookups for table in tables):
        return {
            "status": "lookup",
            "note": "; ".join(sorted({lookups[table] for table in tables})),
        }
    route = spec.holding_routes().get((category, detail))
    if route is None:
        return {
            "status": "residue",
            "note": spec.scope.non_catalog_categories.get(category, ""),
        }
    value: dict = {"status": route.status}
    if route.graft is not None:
        value["graft"] = route.graft
    if route.note is not None:
        value["note"] = route.note
    if route.status == "mapped":
        series_of = spec.series_of()
        held = [series_of[table] for table in sorted(tables) if table in series_of]
        value["to"] = {
            table: series_of[table].table_variants()[table]
            for table in sorted(tables)
            if table in series_of
        }
        grafts = {
            column: variant
            for series in held
            for column, variant in series.graft_targets().items()
        }
        if grafts:
            value["graft_columns"] = dict(sorted(grafts.items()))
    return value


# --- enrich: join delivery documentation onto holdings -----------------------
#
# Sources (see README.md): the master P1105 variabellistor carry
# ``Variabelnamn | Beskrivning | År | Källa | Vy`` per register sheet; the SoS
# delivery lists carry ``Variabelnamn | Klartext`` (2024 also type+length) per
# dataset; per-agency lists carry name+description. The join is by EXACT
# normalized column name within an explicitly scoped sheet->category map — no
# fuzzy matching (standing rule); fuzzy candidates are a separate curation
# report, never an automatic join.

# Both lopnr conventions appear in the documentation lists: SWECOV's physical
# ``P1105_LopNr_X`` and SCB metadata's bare ``LopNr_X``.
_ANY_LOPNR_PREFIX = re.compile(r"^(P1105_)?LOPNR_", re.IGNORECASE)

# A master-list `Källa` that is a provenance phrase ("Inrapporterat från <agency>")
# names where steward data was REPORTED FROM, not a canonical reg_meta register, so
# such columns belong in the steward flavor rather than the global graft track
# (Tillväxtverket korttidsarbete; #443). Every other Källa value is a register
# abbreviation (LISA, RTB, AKU, …) and keeps its canonical-home routing.
_PROVENANCE_KALLA = re.compile(r"^Inrapporterat från\b", re.IGNORECASE)


def _canonical_kalla(col: dict) -> bool:
    """A column whose `Källa` names a canonical register: the graft track owns it."""
    kalla = col.get("kalla")
    return bool(kalla) and not _PROVENANCE_KALLA.match(kalla)


def norm_col(name: str) -> str:
    return _ANY_LOPNR_PREFIX.sub("", name.strip()).upper()


def load_workbook_lenient(path: Path):
    """openpyxl loader tolerating the malformed docProps timestamps some SoS
    exports carry ('2022- 8-30T...'); patches core.xml in memory only —
    input files are never modified."""
    import io
    import zipfile

    import openpyxl

    try:
        return openpyxl.load_workbook(path, read_only=True, data_only=True)
    except ValueError:
        zin = zipfile.ZipFile(io.BytesIO(path.read_bytes()))
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zout:
            for item in zin.infolist():
                data = zin.read(item.filename)
                if item.filename == "docProps/core.xml":
                    data = re.sub(rb"(\d{4})-\s*(\d)-", rb"\1-0\2-", data)
                zout.writestr(item, data)
        return openpyxl.load_workbook(
            io.BytesIO(buf.getvalue()), read_only=True, data_only=True
        )


# Master-list sheet -> inventory Category (or (Category, Detail)) scope. A
# scope of just the category enriches every Detail under it; description text
# is column-level and register-wide, so cross-detail collisions are harmless.
MASTER_SHEET_SCOPE: dict[str, tuple[str, str | None]] = {
    "RTB": ("RTB", None),
    "LISA individ": ("LISA", "Individ"),
    "STATIV": ("STATIV", None),
    "Geografi": ("Geografidatabasen", None),
    "PedPers": ("Lärarregistret", None),
    "Skolverkets elevreg.": ("Elevregistret", None),
    "Högskola": ("Högskoleregistret", None),
    "UREG": ("Utbildningsregistret", None),
    "AKU_RL 2021-": ("AKU", None),
    "AKU": ("AKU", None),
    "AGI": ("AGI", None),
    "Korttidsarbete": ("Korttidsarbete", None),
    "LISA FtgAst": ("LISA", None),
    "RAMS": ("RAMS", None),
    "FDB": ("Firms", "Företagsdatabasen"),
    "FEK": ("Firms", "Företagens ekonomi"),
    "UHV": ("Utrikeshandel", "Varor"),
    "UHT": ("Utrikeshandel", "Tjänster"),
    "Momsreg": ("Momsregistret", None),
    "Konkurser och offentliga ackord": ("Konkurser", None),
    "KSju": ("Konjunkturstatistik sjuklöner", None),
    "SUS": ("Sjukfränvaro under sjuklöneperioden", None),
    "IVP": ("Industrins varuproduktion", None),
    "ITFtg": ("Survey", None),
    "CIS": ("Survey", "Innovation"),
    "FoU": ("Survey", "FOU"),
    "Distansutb": ("Survey", "Distansutbildning"),
}

# SoS dataset-name token -> (Category, Detail) scope.
SOS_DATASET_SCOPE: list[tuple[str, tuple[str, str]]] = [
    ("PAR_OV", ("Socialstyrelsen", "Öppenvård")),
    ("PAR_SV", ("Socialstyrelsen", "Slutenvård")),
    ("SIR", ("Socialstyrelsen", "Intensivvårdsregistret")),
    ("CAN", ("Socialstyrelsen", "Cancerregistret")),
    ("DORS", ("Socialstyrelsen", "Dödsorsaksregistret")),
    ("EKB", ("Socialstyrelsen", "Ekonomiskt bistand")),
    ("HSL", ("Socialstyrelsen", "Kommunal hälso- och sjukvård")),
    ("LSS", ("Socialstyrelsen", "LSS")),
    ("LMED", ("Socialstyrelsen", "Läkemedelsregistret")),
    ("SOL", ("Socialstyrelsen", "Socialtjänst (Äldrevård)")),
    ("BU", ("Socialstyrelsen", "Barn")),
]


@dataclass
class DocRecord:
    """One documented column from a delivery variable list."""

    description: str
    kalla: str = ""  # master-list Källa: source register, '' = SWECOV-constructed
    vy: str = ""  # master-list Vy: physical view/table pattern
    ar: str = ""  # documented coverage, as written
    data_type: str = ""  # SoS lists: char/num
    length: str = ""
    provenance: str = ""  # which file documented it


# Enrichment index: scope -> normalized column -> DocRecord. Scope keys are
# 'Category' or 'Category/Detail'; lookup tries the narrow scope first.
DocIndex = dict[str, dict[str, DocRecord]]


def _scope_key(category: str, detail: str | None) -> str:
    return f"{category}/{detail}" if detail else category


def _master_sheet_rows(ws):
    """Yield (record_dict) data rows from a master-list register sheet.

    Sheets carry preamble text, one or more ``Variabelnamn | ...`` header rows
    (multi-section sheets like Korttidsarbete restate the header per table),
    and section text rows. Column layout is read from each header row; data
    rows need a name in column A plus at least one populated metadata cell.
    """
    colmap: dict[str, int] = {}
    for row in ws.iter_rows(values_only=True):
        first = str(row[0]).strip() if row[0] is not None else ""
        if first == "Variabelnamn":
            colmap = {}
            for i, cell in enumerate(row):
                label = str(cell).strip().rstrip(":") if cell is not None else ""
                if label in ("Beskrivning", "Klartext"):
                    colmap["description"] = i
                elif label == "År":
                    colmap["ar"] = i
                elif label == "Källa":
                    colmap["kalla"] = i
                elif label == "Vy":
                    colmap["vy"] = i
            continue
        if not colmap or not first:
            continue
        rec = {
            field_: str(row[i]).strip()
            for field_, i in colmap.items()
            if i < len(row) and row[i] is not None and str(row[i]).strip()
        }
        if rec:  # at least one metadata cell -> a data row, not section text
            yield first, rec


def extract_master_list(path: Path, index: DocIndex) -> int:
    """Master P1105 variabellista -> index. Returns rows added. Existing
    entries win (callers pass newest file first)."""
    wb = load_workbook_lenient(path)
    added = 0
    for ws in wb.worksheets:
        scope = MASTER_SHEET_SCOPE.get(ws.title)
        if scope is None:
            continue
        bucket = index.setdefault(_scope_key(*scope), {})
        for name, rec in _master_sheet_rows(ws):
            key = norm_col(name)
            if key in bucket:
                continue
            bucket[key] = DocRecord(
                description=rec.get("description", ""),
                kalla=rec.get("kalla", ""),
                vy=rec.get("vy", ""),
                ar=rec.get("ar", ""),
                provenance=path.name,
            )
            added += 1
    wb.close()
    return added


def _sos_scope(dataset: str) -> str | None:
    for token, scope in SOS_DATASET_SCOPE:
        if f"_{token}_" in f"{dataset}_":
            return _scope_key(*scope)
    return None


def extract_sos_list(path: Path, index: DocIndex) -> int:
    """SoS delivery variabellista -> index. Two shapes: a consolidated
    'variabler' sheet (2024: Dataset|Variabelnamn|Klartext|nr|typ|längd), or
    one sheet per dataset (2020/2022: Variabelnr|Variabelnamn|Klartext)."""
    wb = load_workbook_lenient(path)
    added = 0
    if "variabler" in wb.sheetnames:
        for row in wb["variabler"].iter_rows(min_row=2, values_only=True):
            if not row[0] or not row[1]:
                continue
            scope = _sos_scope(str(row[0]))
            if scope is None:
                continue
            bucket = index.setdefault(scope, {})
            key = norm_col(str(row[1]))
            if key not in bucket:
                bucket[key] = DocRecord(
                    description=str(row[2] or "").strip(),
                    data_type=str(row[4] or "").strip(),
                    length=str(row[5] or "").strip(),
                    provenance=path.name,
                )
                added += 1
    else:
        for ws in wb.worksheets:
            scope = _sos_scope(ws.title)
            if scope is None:
                continue
            bucket = index.setdefault(scope, {})
            header_seen = False
            for row in ws.iter_rows(values_only=True):
                if not header_seen:
                    header_seen = (
                        row[1] is not None and str(row[1]).strip() == "Variabelnamn"
                    )
                    continue
                if not row[1]:
                    continue
                key = norm_col(str(row[1]))
                if key not in bucket:
                    bucket[key] = DocRecord(
                        description=str(row[2] or "").strip(), provenance=path.name
                    )
                    added += 1
    wb.close()
    return added


def extract_simple_xlsx(
    path: Path,
    index: DocIndex,
    scope: str,
    name_header: str,
    desc_header: str,
    extra: dict[str, str] | None = None,
) -> int:
    """Generic two-column-ish list: header row located by ``name_header``;
    section rows (description cell empty) are skipped."""
    wb = load_workbook_lenient(path)
    added = 0
    bucket = index.setdefault(scope, {})
    for ws in wb.worksheets:
        cols: dict[str, int] = {}
        for row in ws.iter_rows(values_only=True):
            cells = [str(v).strip() if v is not None else "" for v in row]
            if not cols:
                if name_header in cells:
                    cols["name"] = cells.index(name_header)
                    cols["desc"] = cells.index(desc_header)
                    for field_, header in (extra or {}).items():
                        if header in cells:
                            cols[field_] = cells.index(header)
                continue
            name = cells[cols["name"]] if cols["name"] < len(cells) else ""
            desc = cells[cols["desc"]] if cols["desc"] < len(cells) else ""
            if not name or not desc:
                continue
            key = norm_col(name)
            if key not in bucket:
                rec = DocRecord(description=desc, provenance=path.name)
                if "ar" in cols and cols["ar"] < len(cells):
                    rec.ar = cells[cols["ar"]]
                bucket[key] = rec
                added += 1
    wb.close()
    return added


def extract_ndr_csv(path: Path, index: DocIndex) -> int:
    bucket = index.setdefault(_scope_key("Quality register", "NDR"), {})
    added = 0
    with path.open(encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh, delimiter=";"):
            name, desc = row.get("Variabelnamn", ""), row.get("Beskrivning", "")
            if name and desc and norm_col(name) not in bucket:
                bucket[norm_col(name)] = DocRecord(
                    description=desc, provenance=path.name
                )
                added += 1
    return added


def extract_stockholm_xlsx(path: Path, index: DocIndex) -> int:
    """Region Stockholm primärvård: bare ``name | klartext`` rows, no header."""
    wb = load_workbook_lenient(path)
    bucket = index.setdefault(_scope_key("Primary care", "Stockholm"), {})
    added = 0
    for ws in wb.worksheets:
        for row in ws.iter_rows(max_col=2, values_only=True):
            if row[0] and row[1]:
                key = norm_col(str(row[0]))
                if key not in bucket:
                    bucket[key] = DocRecord(
                        description=str(row[1]).strip(), provenance=path.name
                    )
                    added += 1
    wb.close()
    return added


def build_doc_index(base: Path) -> DocIndex:
    """All extractable documentation sources under the delivery-docs root
    `base` -> one index. Order matters: newest master/SoS lists first (first
    write wins per scope+column)."""
    index: DocIndex = {}
    sources: list[tuple[str, Callable[[], int]]] = [
        (
            "master v5 2025",
            lambda: extract_master_list(
                base
                / "SCB/Dataleverans 2025/Utkast_P1105_Variabellista_uppd2025_v5.xlsx",
                index,
            ),
        ),
        (
            "master 2023-24",
            lambda: extract_master_list(
                base / "SCB/Dataleverans 2023-2024/Bilaga 2 - Variabellista.xlsx", index
            ),
        ),
        (
            "master 2022",
            lambda: extract_master_list(
                base / "SCB/Tidigare dataleverans/P1105_Variabellista 2022.xlsx", index
            ),
        ),
        (
            "sos 2024",
            lambda: extract_sos_list(
                base
                / "Socialstyrelsen/Databeställning 2 (2023)/Leverans 2024/Variabellista_73799_2024.xlsx",
                index,
            ),
        ),
        (
            "sos 2022",
            lambda: extract_sos_list(
                base / "Socialstyrelsen/Variabellista_Lev5_41350_2022.xlsx", index
            ),
        ),
        (
            "sos 2020",
            lambda: extract_sos_list(
                base / "Socialstyrelsen/Variabellista_39803_2020.xlsx", index
            ),
        ),
        (
            "fohm",
            lambda: extract_simple_xlsx(
                base / "Fohm/variabellista_alla_variabler.xlsx",
                index,
                "FOHM",
                "Variabelnamn",
                "Beskrivning",
                {"ar": "Period"},
            ),
        ),
        (
            "skv",
            lambda: extract_simple_xlsx(
                base / "Skatteverket/Variabellista_20210421_2.xlsx",
                index,
                "Skatteverket",
                "Variabel",
                "Klartext",
            ),
        ),
        (
            "ndr",
            lambda: extract_ndr_csv(
                base
                / "Kvalitetsregister/Nationella Diabetesregistret/variabler-ndr-2024-01-22.csv",
                index,
            ),
        ),
        (
            "sthlm",
            lambda: extract_stockholm_xlsx(
                base
                / "Primärvård/Region Stockholm/SWECOV Variabellista Stockholm.xlsx",
                index,
            ),
        ),
        (
            "skane",
            lambda: extract_simple_xlsx(
                base / "Primärvård/Region Skåne/Variabel_spec_rsvd_till_scb.xlsx",
                index,
                "Primary care/Skane",
                "Variabel i RSVD",
                "Kommentar",
            ),
        ),
    ]
    for label, run in sources:
        n = run()
        print(f"  doc source {label}: {n} columns", file=sys.stderr)
    return index


def lookup_doc(
    index: DocIndex, category: str, detail: str, column: str
) -> DocRecord | None:
    key = norm_col(column)
    for scope in (f"{category}/{detail}", category):
        rec = index.get(scope, {}).get(key)
        if rec:
            return rec
    return None


def cmd_enrich(args: argparse.Namespace) -> None:
    holdings = parse_inventory(args.csv)
    spec = _checked_spec(args, holdings)
    print("extracting documentation sources:", file=sys.stderr)
    index = build_doc_index(args.docs_root)

    out: dict[str, dict] = {}
    n_cols = n_doc = 0
    per_cat: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for (cat, det), h in sorted(holdings.items()):
        mapping = holding_mapping(spec, cat, det, h.tables)
        cols = []
        for col in sorted(h.columns):
            rec = lookup_doc(index, cat, det, col)
            n_cols += 1
            per_cat[cat][1] += 1
            entry: dict[str, object] = {"name": col, "normalized": norm_col(col)}
            if rec:
                n_doc += 1
                per_cat[cat][0] += 1
                entry["description"] = rec.description
                for f_ in ("kalla", "vy", "ar", "data_type", "length", "provenance"):
                    if getattr(rec, f_):
                        entry[f_] = getattr(rec, f_)
            cols.append(entry)
        out[_scope_key(cat, det or None)] = {
            "category": cat,
            "detail": det,
            "tables": sorted(h.tables),
            # Per-table column lists (the union lives in `columns`). Lets a flavor
            # disposition entry select a holding's columns by physical table when
            # one (Category, Detail) holding carries several distinct registers —
            # e.g. Skatteverket's 10 SKV_* tables → 5 schemes. See
            # `_FLAVOR_VARIANT_TABLES`.
            "table_columns": {
                t: sorted(cs) for t, cs in sorted(h.table_columns.items())
            },
            "periods": h.period_segments(),
            "mapping": mapping,
            "columns": cols,
        }

    dest = args.out / "derived" / "holdings_enriched.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(out, ensure_ascii=False, indent=1, default=str))
    print(f"\nwrote {dest} ({n_doc}/{n_cols} columns documented, {n_doc / n_cols:.0%})")
    print("\nper category (documented/total):")
    for cat in sorted(per_cat):
        d, t = per_cat[cat]
        print(f"  {cat:42} {d:5}/{t:<5} {d / t:4.0%}")


# --- globals: mine holdings for shared-world catalog improvements ------------
#
# Classifies every documented holding column against its mapped/graft register:
#   matched            already in reg_meta (state or alias column)
#   backfill           matched, but the reg_meta variable has NO description and
#                      the SWECOV delivery list has one -> description backfill
#   pivot              wide-format delivery artifact (suffix-stripped base exists)
#   alias_candidate    normalized form equals an existing column or variable slug
#                      -> curated variable_alias candidate (HUMAN-CONFIRMED; this
#                      list is curation input, never an automatic join)
#   gapfill            documented register fact (master-list Källa set, or SoS
#                      delivery list) reg_meta lacks -> global graft candidate
#   constructed        documented but no Källa -> steward-flavor only
# Output: derived/global_enrichment.json + alias/backfill review lists.


def _norm_alnum(c: str) -> str:
    # NFKD-transliterate (ä→a) BEFORE dropping non-alnum, so Swedish letters MAP
    # instead of being DELETED: 'ÄMNESKOD' → 'AMNESKOD', not 'MNESKOD'. The bare
    # [^A-Z0-9] form silently dropped ä/ö/å, so a de-diacritic'd steward name
    # could never match reg_meta's diacritic column. Mirrors the shipped
    # reg_meta_build._curation.fold_column.
    folded = unicodedata.normalize("NFKD", c).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^A-Z0-9]", "", folded.upper())


def _strip_pivot_suffix(c: str) -> str:
    return re.sub(r"(_?(19|20)\d{2}(_\d{4})?|\d{1,2})$", "", c).rstrip("_")


def _load_register_columns(db_path: Path):
    """register coord -> {UPPER(col): (slug, name, has_description)} and
    coord -> {normalized slug: (slug, name, has_description)}."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    reg_cols: dict[str, dict] = defaultdict(dict)
    reg_slugs: dict[str, dict] = defaultdict(dict)
    for table in ("variable_state", "variable_alias"):
        for p, r, col, slug, name, desc in conn.execute(
            f"""SELECT pr.slug, r.slug, UPPER(t.delivery_column_name), v.slug,
                       coalesce(v.name,''), coalesce(v.description,'')
                FROM {table} t JOIN variable v USING(variable_id)
                JOIN register r ON r.register_id=v.register_id
                JOIN provider pr USING(provider_id)
                WHERE t.delivery_column_name IS NOT NULL
                  AND trim(t.delivery_column_name) != ''"""
        ):
            reg_cols[f"{p}/{r}"].setdefault(col, (slug, name, bool(desc)))
    for p, r, slug, name, desc in conn.execute(
        """SELECT pr.slug, r.slug, v.slug, coalesce(v.name,''),
                  coalesce(v.description,'')
           FROM variable v JOIN register r ON r.register_id=v.register_id
           JOIN provider pr USING(provider_id) WHERE v.slug IS NOT NULL"""
    ):
        reg_slugs[f"{p}/{r}"][slug.replace("-", "").upper()] = (slug, name, bool(desc))
    conn.close()
    return reg_cols, reg_slugs


def _mapping_registers(mapping: dict) -> set[str]:
    regs = {
        "/".join(coord.split("/")[:2])
        for coords in mapping.get("to", {}).values()
        for coord in coords
    }
    if mapping.get("graft"):
        regs.add(mapping["graft"])
    return regs


# SoS delivery lists document SOS register facts without a Källa column.
_SOS_LIST_PROVENANCE = re.compile(r"^Variabellista_")


def cmd_globals(args: argparse.Namespace) -> None:
    enriched_path = args.out / "derived" / "holdings_enriched.json"
    if not enriched_path.exists():
        raise SystemExit(f"{enriched_path} missing — run `enrich` first")
    data = json.loads(enriched_path.read_text())
    reg_cols, reg_slugs = _load_register_columns(args.db)

    counts: dict[str, int] = defaultdict(int)
    aliases: list[dict] = []
    backfill: list[dict] = []
    gapfill: list[dict] = []
    constructed: list[dict] = []
    seen_alias: set[tuple[str, str]] = set()
    for _key, pair in sorted(data.items()):
        regs = sorted(_mapping_registers(pair["mapping"]))
        if not regs:
            continue
        for col in pair["columns"]:
            nc = col["normalized"]
            documented = "description" in col
            hit = next(
                ((reg, reg_cols[reg][nc]) for reg in regs if nc in reg_cols[reg]), None
            )
            if hit:
                counts["matched"] += 1
                reg, (slug, _name, has_desc) = hit
                if documented and not has_desc:
                    counts["backfill"] += 1
                    backfill.append(
                        {
                            "register": reg,
                            "variable": slug,
                            "column": col["name"],
                            "description": col["description"],
                            "provenance": col.get("provenance", ""),
                        }
                    )
                continue
            if _strip_pivot_suffix(nc) and any(
                _strip_pivot_suffix(nc) in reg_cols[reg] for reg in regs
            ):
                counts["pivot"] += 1
                continue
            # Strip a trailing `_omkodad` ("recoded") marker before alias lookup:
            # `Kurskod_omkodad` is a recoded delivery of the existing `kurs`
            # variable's `KursKod` column, not a new variable. (The codes differ —
            # see #365 — so the masked representation is a finer downstream call,
            # but it must NOT be grafted as a duplicate variable.)
            nn = re.sub(r"OMKODAD$", "", _norm_alnum(nc))
            cand = next(
                (
                    (reg, info)
                    for reg in regs
                    for c2, info in reg_cols[reg].items()
                    if _norm_alnum(c2) == nn
                ),
                None,
            ) or next(
                ((reg, reg_slugs[reg][nn]) for reg in regs if nn in reg_slugs[reg]),
                None,
            )
            if cand:
                reg, (slug, name, _has_desc) = cand
                counts["alias_candidate"] += 1
                if (reg, nn) not in seen_alias:  # case-variant dupes collapse
                    seen_alias.add((reg, nn))
                    aliases.append(
                        {
                            "register": reg,
                            "delivery_column": col["name"],
                            "variable": slug,
                            "variable_name": name,
                            "swecov_description": col.get("description", ""),
                        }
                    )
                continue
            if documented and (
                col.get("kalla")
                or _SOS_LIST_PROVENANCE.match(col.get("provenance", ""))
            ):
                counts["gapfill"] += 1
                gapfill.append(
                    {
                        "register": regs[0],
                        "column": col["name"],
                        "kalla": col.get("kalla", ""),
                        "description": col["description"],
                        "data_type": col.get("data_type", ""),
                        "provenance": col.get("provenance", ""),
                    }
                )
            elif documented:
                counts["constructed"] += 1
                constructed.append(
                    {
                        "register": regs[0],
                        "column": col["name"],
                        "description": col["description"],
                    }
                )
            else:
                counts["undocumented_unmatched"] += 1

    dest = args.out / "derived" / "global_enrichment.json"
    dest.write_text(
        json.dumps(
            {
                "counts": dict(sorted(counts.items())),
                "alias_candidates": aliases,
                "description_backfill": backfill,
                "gapfill": gapfill,
                "constructed": constructed,
            },
            ensure_ascii=False,
            indent=1,
        )
    )
    print(f"wrote {dest}")
    for k, v in sorted(counts.items()):
        print(f"  {k:24} {v}")
    print(
        f"  ({len(aliases)} alias candidates after case-dedupe; pairs without a"
        " mapped/graft register are excluded from this scan)"
    )


# --- grafts: the [[errata.column]] candidates for columns SCB never documents -------
#
# `globals` emits gapfill rows keyed only on `regs[0]` (the register), dropping
# the variant. A `reg_meta_build/curation/registers/scb/<slug>.toml` [[errata.column]] entry mints a variable onto a
# (register, variant), so this stage recovers the variant: each column → the
# variant its table's series delivers, and a column of a series with no variant
# is steward FLAVOR, excluded from the global set. A column no series table
# holds takes its holding's variant when the holding has exactly one for the
# register. Also applies the content filters (drop
# the project-specific P<n>_ pseudonym columns, decode/klartext label columns →
# #373, and rows with no real documentation).
#
# Output: derived/column_candidates.toml. The cross-reference against reg_meta's
# ingested SCB docs decides the entry's `source`, no longer where it is routed: a
# column the docs describe (e.g. lisa Ssyk4_J16) is `scb-docs` — SCB documents it,
# its machine metadata omits it — and one only a delivery list attests is
# `steward-holdings`. Since Y-116 both are the same entry kind in the same file.

_GRAFT_DROP = re.compile(r"klartext|_txt$|beskrivning|, *text$", re.IGNORECASE)
_PROJECT_PREFIX = re.compile(r"^P\d+_", re.IGNORECASE)


def _split_key(key: str) -> tuple[str, str]:
    parts = key.split(" / ", 1)
    return (parts[0], parts[1] if len(parts) > 1 else "")


def _resolve_graft_variant(mapping: dict, csv_holding, column: str, target_reg: str):
    """Variant slug for a gapfill column under its holding's mapping, or one of
    the sentinels 'FLAVOR' / 'AMBIGUOUS' / 'UNRESOLVED'. ``target_reg`` is the
    provider/register the gapfill row was attributed to (a holding can target
    several registers). The column's own tables decide, through the variants
    their series deliver; a series table with no variant is steward FLAVOR. A
    curated `graft_onto` placement for the column decides first."""
    graft = mapping.get("graft_columns", {}).get(column)
    if graft is not None and "/".join(graft.split("/")[:2]) == target_reg:
        return graft.split("/")[2]
    to = mapping.get("to", {})
    tables = (
        [t for t, cols in csv_holding.table_columns.items() if column in cols]
        if csv_holding
        else []
    )
    found: set[str] = set()
    for t in tables:
        if t not in to:
            continue
        if not to[t]:
            found.add("FLAVOR")
        found.update(
            coord.split("/")[2]
            for coord in to[t]
            if "/".join(coord.split("/")[:2]) == target_reg
        )
    if not found:
        # No series table of this holding holds the column (a superseded
        # re-delivery holds it): the holding's own variant, when it has exactly
        # one for the register.
        found = {
            coord.split("/")[2]
            for coords in to.values()
            for coord in coords
            if "/".join(coord.split("/")[:2]) == target_reg
        }
        if len(found) > 1:
            return "AMBIGUOUS"
    variants = {f for f in found if f != "FLAVOR"}
    if len(variants) == 1:
        return next(iter(variants))
    if len(variants) > 1:
        return "AMBIGUOUS"
    if found == {"FLAVOR"}:
        return "FLAVOR"
    return "UNRESOLVED"


def _ground_fallback(col, target_reg, csv_holdings, enriched, variant_index):
    """For a column the curated mapping didn't place: if its only holdings are
    flavor/lookup/residue → 'FLAVOR'; else ground the SPECIFIC table(s) the column
    lives in against `target_reg`'s reg_meta variants (column overlap) and return
    the best-grounded variant slug (or 'UNRESOLVED')."""
    located = []  # (cat, det, table, holding)
    for (cat, det), h in csv_holdings.items():
        for t, cols in h.table_columns.items():
            if col in cols:
                located.append((cat, det, t, h))
    if not located:
        return "UNRESOLVED"
    statuses = set()
    for cat, det, _t, _h in located:
        key = f"{cat} / {det}" if det else cat
        pair = enriched.get(key)
        if pair:
            statuses.add((pair.get("mapping") or {}).get("status"))
    if "unmapped" in statuses:
        return "UNRESOLVED"
    if statuses and statuses <= {"flavor", "lookup", "residue"}:
        return "FLAVOR"
    best: dict[str, int] = {}
    for _cat, _det, t, h in located:
        tcols = {_ANY_LOPNR_PREFIX.sub("", c).upper() for c in h.table_columns[t]}
        for vi in variant_index:
            if "/".join(vi.coordinate.split("/")[:2]) != target_reg:
                continue
            ov = len(tcols & {c.upper() for c in vi.columns})
            var = vi.coordinate.split("/")[2]
            best[var] = max(best.get(var, 0), ov)
    if best and max(best.values()) > 0:
        top = max(best.values())
        return min(v for v, o in best.items() if o == top)
    return "UNRESOLVED"


# The two evidence sources a candidate can carry, in worklist order, each with
# what a curator still has to do to its section. The `source` vocabulary itself is
# `load_scb_errata`'s; `column_stanza` refuses anything else.
_SOURCE_NOTES = (
    (
        "scb-docs",
        "SCB's own ingested docs describe the column; narrow `all_versions` to "
        "the years its doc page carries",
    ),
    (
        "steward-holdings",
        "only a delivery list attests the column, and a holdings list dates "
        "nothing, so `all_versions` stands",
    ),
)


def _candidate_line(row: dict) -> str:
    """The scan's own evidence about one column, as a comment: what the delivery
    said about it. The curator writes `evidence` from this."""
    parts = [f"{row['register']}/{row['variant']} {row['column']}"]
    parts += [
        f"{label} {row[key]}"
        for label, key in (
            ("källa", "kalla"),
            ("provenance", "provenance"),
            ("delivery type", "data_type"),
        )
        if row.get(key)
    ]
    return "; ".join(parts)


def _column_worklist(
    candidates: list[dict],
    flavor: list[dict],
    unresolved: list[dict],
    dropped: list[str],
) -> str:
    """The gapfill scan as a candidate `reg_meta_build/curation/registers/scb/<slug>.toml` fragment: the `[[errata.column]]`
    entries, one section per `source`, then — as COMMENTS, because none of them is
    an entry of this file — the three buckets the scan did not place.

    The stanzas come from `reg_meta_build.inventory_coverage.column_stanza`, the
    one renderer of this grammar, for the reason stated over `cmd_errata`: a
    worklist that spelled the entries itself could drift from the loader that has
    to accept them.
    """
    from reg_meta_build.inventory_coverage import column_stanza

    lines = [
        "# GENERATED by reg_webapp/stewards/swecov/build_catalog.py grafts — candidate",
        "# reg_meta_build/curation/registers/scb/<slug>.toml [[errata.column]] entries: columns a SWECOV",
        "# delivery holds on an existing (register, variant) that SCB's own",
        "# Registerinformation export documents on no version of it.",
        "#",
        "# CURATE, don't paste wholesale: `evidence` and `noted` ride as TODO",
        "# placeholders, and the loader refuses a placeholder `noted`, so nothing",
        "# here reaches a build until a maintainer has established the omission and",
        "# dated the finding. A column SCB DOES document on the variant but omits",
        "# from some versions is a [[errata.delivered]] entry instead — it re-adds the",
        "# omitted rows rather than minting the variable.",
    ]
    for source, note in _SOURCE_NOTES:
        rows = [row for row in candidates if row["source"] == source]
        lines += [
            "",
            f"# ── source = {source}: {len(rows)} column(s) — {note} ──",
        ]
    for register in sorted({row["register"] for row in candidates}):
        provider, _, slug = register.partition("/")
        lines += ["", f"# reg_meta_build/curation/registers/{provider}/{slug}.toml"]
        register_rows = [row for row in candidates if row["register"] == register]
        for source, _note in _SOURCE_NOTES:
            rows = [row for row in register_rows if row["source"] == source]
            for row in rows:
                lines += [
                    "",
                    f"# {_candidate_line(row)}",
                    column_stanza(
                        row["variant"],
                        row["column"],
                        row["description"],
                        source,
                    ),
                ]
    for title, rows in (
        (
            f"no variant: {len(unresolved)} column(s) the curated mapping and the "
            "column-overlap fallback both left unplaced — a [[errata.column]] mints onto a "
            "(register, variant), so each needs one named before it is an entry",
            [_candidate_line(row) for row in unresolved],
        ),
        (
            f"steward flavor: {len(flavor)} column(s) whose holding is the steward's "
            "own content, not SCB's — the flavored DB carries them (`flavor`), and "
            "this file corrects SCB's export alone",
            [_candidate_line(row) for row in flavor],
        ),
        (
            f"content-dropped: {len(dropped)} column(s) the filters excluded — "
            "project-local P<n>_ pseudonyms, klartext label columns (#373), and rows "
            "whose only documentation is the column name",
            dropped,
        ),
    ):
        lines += ["", f"# ── {title} ──"]
        lines += [f"# {line}" for line in rows]
    return "\n".join(lines) + "\n"


def cmd_grafts(args: argparse.Namespace) -> None:
    docs_root = args.lisa_docs_dir
    # A wrong path would silently tag every column `steward-holdings`.
    if not docs_root.is_dir() or not any(d.is_dir() for d in docs_root.iterdir()):
        raise SystemExit(
            f"--lisa-docs-dir {docs_root} is not a directory of "
            "<register>/*.md subdirectories"
        )
    derived = args.out / "derived"
    enriched = json.loads((derived / "holdings_enriched.json").read_text())
    ge = json.loads((derived / "global_enrichment.json").read_text())
    csv_holdings = parse_inventory(args.csv)
    variant_index = load_variant_index(args.db)
    flavor_registers = frozenset(load_steward_spec(args.spec).scope.flavor_registers)

    # Docs cross-reference: a column documented in reg_meta's ingested SCB docs is
    # one SCB itself describes and its machine metadata missed, so the entry says
    # `source = "scb-docs"` rather than resting on a steward's word. Only registers
    # with ingested docs (lisa) can be discriminated; a column in any other register
    # falls through as `steward-holdings`. `--lisa-docs-dir` holds one
    # `<register>/*.md` directory per documented register.
    doc_cols: dict[str, set[str]] = {
        d.name: {
            _norm_alnum(p.stem) for p in d.glob("*.md") if not p.stem.startswith("_")
        }
        for d in docs_root.iterdir()
        if d.is_dir()
    }

    # register -> [(mapping, csv_holding, set(column names in this holding))]
    by_reg: dict[str, list] = defaultdict(list)
    for key, pair in enriched.items():
        csvh = csv_holdings.get(_split_key(key))
        names = {c["name"] for c in pair["columns"]}
        for reg in _mapping_registers(pair["mapping"]):
            by_reg[reg].append((pair["mapping"], csvh, names))

    candidates: list[dict] = []
    flavor: list[dict] = []
    unresolved: list[dict] = []
    dropped: list[str] = []
    seen: set[tuple[str, str]] = set()
    for g in ge["gapfill"]:
        reg, col = g["register"], g["column"]
        desc = (g.get("description") or "").strip()
        if (reg, col) in seen:
            continue
        seen.add((reg, col))
        if (
            _PROJECT_PREFIX.match(col)
            or _GRAFT_DROP.search(f"{col} {desc}")
            or not desc
            or col.strip().lower() == desc.lower()
        ):
            dropped.append(f"{reg}/{col}")
            continue
        variant = "UNRESOLVED"
        for mapping, csvh, names in by_reg.get(reg, []):
            if col not in names:
                continue
            variant = _resolve_graft_variant(mapping, csvh, col, reg)
            if variant != "UNRESOLVED":
                break
        if variant == "UNRESOLVED":
            # Curated mapping didn't place it — fall back to grounding the
            # column's own table(s) (and flavor-only holdings → FLAVOR).
            variant = _ground_fallback(col, reg, csv_holdings, enriched, variant_index)
        # A privacy-aggregation register's delivered columns (GDB's grid
        # coordinates, the spatial analogue of a LopNr) are steward flavor,
        # never global (maintainer call 2026-06-14).
        if reg in flavor_registers:
            variant = "FLAVOR"
        row = {
            "register": reg,
            "variant": variant,
            "column": col,
            "description": desc,
            "source": (
                "scb-docs"
                if _norm_alnum(col) in doc_cols.get(reg.split("/")[-1], set())
                else "steward-holdings"
            ),
            "data_type": g.get("data_type", ""),
            "kalla": g.get("kalla", ""),
            "provenance": g.get("provenance", ""),
        }
        if variant == "FLAVOR":
            flavor.append(row)
        elif variant in ("UNRESOLVED", "AMBIGUOUS"):
            unresolved.append(row)
        else:
            candidates.append(row)

    candidates.sort(
        key=lambda r: (r["source"], r["register"], r["variant"], r["column"])
    )
    dest = derived / "column_candidates.toml"
    dest.write_text(
        _column_worklist(candidates, flavor, unresolved, dropped), encoding="utf-8"
    )
    print(f"wrote {dest}")
    for source, _note in _SOURCE_NOTES:
        n = sum(1 for r in candidates if r["source"] == source)
        print(f"  [[errata.column]] source = {source:17} {n}")
    print(f"  flavor_excluded:                     {len(flavor)}")
    print(f"  unresolved/ambiguous:                {len(unresolved)}")
    print(f"  content_dropped:                     {len(dropped)}")
    by_dest: dict[tuple[str, str], int] = defaultdict(int)
    for r in candidates:
        by_dest[(r["register"], r["variant"])] += 1
    for (reg, var), n in sorted(by_dest.items()):
        print(f"    {reg}/{var}: {n}")


# --- flavor: emit extend-db curated-provider TOMLs ---------------------------
#
# The steward-FLAVORED slice (#421, #365 PR2): SWECOV holdings with NO global
# home, projected into the shared curated-provider TOML contract (see
# reg_meta_build/DESIGN.md → "Steward-flavored DB — extend-db"). Scope follows
# "what a fact is ABOUT": only steward-private content lands here. Excluded and
# routed elsewhere by the curated MAPPING / dispositions below:
#   - mapped/split holdings           -> the GLOBAL build (already in reg_meta)
#   - graft-keyed flavor holdings     -> reg_meta_build/curation/registers/scb/<slug>.toml [[errata.column]] (the `grafts` scan)
#   - public-agency residue           -> global providers, separate effort (#422)
#   - canonical-SCB gaps              -> reg_meta_build/curation/registers/scb/<slug>.toml [[errata.column]], source = scb-docs
#   - lookup tables                   -> SWECOV-side helpers, not catalog rows
# Membership decided with the maintainer (2026-06-15, "Principled mid"): the
# flavor carries commercial (Swedbank/Telia), regional/municipal, the national
# quality registers, and SWECOV-constructed RTB extracts. Providers are modeled
# per real-world source organization (per-org), one provider slug each.

# The flavor definitions are the scope's `[[flavor]]` entries
# (reg_meta_build.steward_spec.FlavorDefinition). Single-variant registers use
# the `_default` variant slug (matches the global catalog convention) and repeat
# the register name as the variant name; a register named by several entries
# (Skatteverket's schemes, Tillväxtverket's delivery models) accretes one variant
# per entry, each naming its own delivery — entries share a register only where
# they DELIVER that one register's variables, so deliveries with disjoint schemas
# are separate registers (crates/DESIGN.md → "The variant is a coordinate, not an
# identity level"). An entry's `tables` selects the physical table(s) whose
# columns that variant draws when one holding bundles several registers; without
# it the variant draws the holding's full column union.


def _kebab(text: str) -> str:
    """ASCII kebab slug: NFKD-transliterate (ä→a), lower, non-alnum→'-'. Used
    for the curated variable `key` (becomes `variable.provider_key`, which the
    slug source-ID grammar forbids a '.' in)."""
    folded = (
        unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    )
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", folded.lower())).strip("-")


def _flavor_variables(
    columns: list[dict],
    keep: frozenset[str] = frozenset(),
    sensitive: frozenset[str] = frozenset(),
    *,
    year_independent: bool = False,
) -> list[dict]:
    """Project an enriched holding's columns into curated variable dictionaries.

    A column whose master-list `Källa` names a canonical source REGISTER (LISA,
    RTB, AKU, …) has a canonical home and is routed to the global graft/enrichment
    track — never flavored (scope follows what a fact is ABOUT; see #365). So only
    Källa-empty columns — genuinely SWECOV-constructed — survive into the flavor.
    (For the commercial / regional / quality-register providers no column carries a
    Källa at all, so this is a no-op there; it only trims the mixed
    SWECOV-constructed RTB extracts.) `cmd_flavor` refuses a holding that drops
    such a column while its route names no graft/target register (#1302).

    EXCEPTION: a `Källa` that is a PROVENANCE phrase ("Inrapporterat från <agency>")
    is NOT a canonical register — it marks data reported FROM an external public
    agency that has no reg_meta home (Tillväxtverket's korttidsarbete). That is
    precisely steward-flavor content, so such columns are KEPT in the flavor rather
    than routed to a graft target that doesn't exist (#443). The disposition's
    `keep` names (exact physical columns) are kept too: the steward table must stay
    orderable as one variant, so its Källa columns are the steward's own copies
    (#1302). `sensitive` names flag their variable `is_sensitive`.

    Surviving columns are GROUPED by their `_norm_alnum` fold (lopnr-stripped,
    NFKD-transliterated, upper-cased, non-alphanumerics dropped), so the vintage
    spellings one delivery uses for one column — `Covid-19 antikroppar` and
    `Covid_19_antikroppar` — become ONE variable. The spellings are co-delivered in
    one window, so the group is ONE state whose `aliases` carry the other physical
    columns; extend-db gives each column its own `variable_alias` + window row, so
    every literal delivery column stays orderable (README → "Near-duplicate
    physical columns"). The key, name, description and `data_type` come from the
    first-seen spelling. A pair that differs in LETTERS (`AVERAGE_SPENDING` /
    `AVERAGE_SPENDINGS`) folds to two forms and stays two variables — grouping
    those is a curation call, not a mechanical one. The pseudonym
    `P<n>_LopNr_` prefix is stripped from the catalog `column`/`name` using the
    same `_ANY_LOPNR_PREFIX` that produced the `normalized` fold input (it is an
    order-template artifact, not a column identity — see #365); a stripped column
    flags `is_identifier`. Validity is left open: delivery coverage is a project
    selection, not a register fact.

    A `year_independent` variant does not group: a co-delivered alias needs a
    dated window, which such a state has none of, so CuratedAdapter refuses
    aliases there (#1538). Each spelling becomes its own variable; a key that
    two spellings kebab to alike (`Birth-country` / `Birth_country`) takes an
    ordinal suffix in name order."""
    # fold -> {physical column: its enriched entry}, first-seen first.
    groups: dict[str, dict[str, dict]] = {}
    for col in sorted(columns, key=lambda c: c["name"]):
        if _canonical_kalla(col) and col["name"] not in keep:
            # Källa names a canonical register -> global graft track, not flavor.
            # A provenance phrase ("Inrapporterat från …") is steward-reported data
            # with no canonical home -> keep it in the flavor (see docstring, #443).
            continue
        logical = _ANY_LOPNR_PREFIX.sub("", col["name"]).strip()
        if not logical:
            continue
        fold = logical if year_independent else _norm_alnum(col["normalized"])
        groups.setdefault(fold, {}).setdefault(logical, col)

    variables: list[dict] = []
    keys: set[str] = set()
    for spellings in groups.values():
        first_column, first_entry = next(iter(spellings.items()))
        # Multistate curated contract (#981): the delivery column and its window
        # live in a `states` list, not flat on the variable.
        state: dict = {
            "column": first_column,
            "data_type": first_entry.get("data_type") or None,
            "valid_from": None,
            "valid_to": None,
        }
        if aliases := list(spellings)[1:]:
            # The other spellings are co-delivered, so they ride as `aliases` of
            # this one state — see the docstring.
            state["aliases"] = aliases
        key = _kebab(first_entry["normalized"]) or f"v{len(variables)}"
        if key in keys:
            # Spellings of one fold that a year-independent variant split apart.
            key = next(f"{key}-{n}" for n in count(2) if f"{key}-{n}" not in keys)
        keys.add(key)
        variables.append(
            {
                "key": key,
                "name": first_column,
                "definition": None,
                "description": first_entry.get("description") or None,
                "is_identifier": bool(_ANY_LOPNR_PREFIX.match(first_entry["name"])),
                "is_sensitive": any(e["name"] in sensitive for e in spellings.values()),
                "states": [state],
            }
        )
    return variables


def _source_label(csv_path: Path, steward: str) -> str:
    """`<steward>-inventory-<YYYY-MM-DD>` from the inventory CSV filename."""
    m = re.search(r"(\d{4}-\d{2}-\d{2})", csv_path.stem)
    return f"{steward}-inventory-{m.group(1)}" if m else f"{steward}-inventory"


def _flavor_provider_toml(
    provider_name: str, source_label: str, registers: list[dict]
) -> str:
    """Render one steward provider in CuratedAdapter's strict TOML shape."""
    lines = [
        "[provider]",
        f"name = {_toml_str(provider_name)}",
        f"source_label = {_toml_str(source_label)}",
    ]
    for reg in registers:
        lines += [
            "",
            "[[register]]",
            f"key = {_toml_str(reg['key'])}",
            f"name = {_toml_str(reg['name'])}",
        ]
        for attribute in ("purpose", "description"):
            if reg.get(attribute):
                lines.append(f"{attribute} = {_toml_str(reg[attribute])}")
        for variant in reg["variants"]:
            lines += [
                "",
                "[[register.variant]]",
                f"key = {_toml_str(variant['key'])}",
                f"name = {_toml_str(variant['name'])}",
            ]
            if variant.get("description"):
                lines.append(f"description = {_toml_str(variant['description'])}")
            if variant.get("period_scope", "intervals") != "intervals":
                lines.append(f"period_scope = {_toml_str(variant['period_scope'])}")
        for variant in reg["variants"]:
            for variable in variant["variables"]:
                lines += [
                    "",
                    "[[register.variable]]",
                    f"key = {_toml_str(variable['key'])}",
                    f"name = {_toml_str(variable['name'])}",
                    f"variants = [{_toml_str(variant['key'])}]",
                ]
                for attribute in ("definition", "description", "measurement_unit"):
                    if variable.get(attribute):
                        lines.append(f"{attribute} = {_toml_str(variable[attribute])}")
                if variable.get("is_identifier"):
                    lines.append("is_identifier = true")
                if variable.get("is_sensitive"):
                    lines.append("is_sensitive = true")
                for state in variable["states"]:
                    lines += [
                        "",
                        "[[register.variable.state]]",
                        f"column = {_toml_str(state['column'])}",
                    ]
                    for attribute in (
                        "data_type",
                        "valid_from",
                        "valid_to",
                        "value_set_version_label",
                    ):
                        if state.get(attribute):
                            lines.append(f"{attribute} = {_toml_str(state[attribute])}")
                    if state.get("aliases"):
                        aliases = ", ".join(_toml_str(v) for v in state["aliases"])
                        lines.append(f"aliases = [{aliases}]")
    return "\n".join(lines) + "\n"


def cmd_flavor(args: argparse.Namespace) -> None:
    enriched_path = args.out / "derived" / "holdings_enriched.json"
    if not enriched_path.exists():
        raise SystemExit(f"{enriched_path} missing — run `enrich` first")
    enriched = json.loads(enriched_path.read_text())

    spec = load_steward_spec(args.spec)
    providers: dict[str, str] = {}
    # Källa-routed columns of holdings with no graft register: `globals` never scans
    # those holdings, so the columns would land nowhere.
    ungrafted: list[str] = []
    # (provider, register_key) -> register dict; variants keyed for accretion.
    registers: dict[tuple[str, str], dict] = {}
    for flavor in spec.scope.flavors:
        key, prov_slug, prov_name = (
            flavor.holding,
            flavor.provider,
            flavor.provider_name,
        )
        reg_key, reg_name = flavor.register_key, flavor.register_name
        var_key, var_slug, var_name = (
            flavor.variant,
            flavor.variant_slug,
            flavor.variant_name,
        )
        holding = enriched.get(key)
        if holding is None:
            raise SystemExit(
                f"flavor disposition key not in enriched holdings: {key!r}"
            )
        if providers.setdefault(prov_slug, prov_name) != prov_name:
            raise SystemExit(
                f"provider {prov_slug!r} declared with two names: "
                f"{providers[prov_slug]!r} vs {prov_name!r}"
            )
        reg = registers.setdefault(
            (prov_slug, reg_key),
            {
                "provider": prov_slug,
                "key": reg_key,
                "name": reg_name,
                "purpose": None,
                "description": None,
                "_variants": {},
                "_variant_slugs": {},
            },
        )
        if var_key in reg["_variants"]:
            raise SystemExit(
                f"variant {var_key!r} declared twice on {prov_slug}/{reg_key}"
            )
        columns = holding["columns"]
        tables = flavor.tables
        if tables is not None:
            # Restrict to the named physical table(s) when one holding bundles
            # several registers (Skatteverket's SKV_* schemes). `table_columns`
            # is keyed by original-case column name (matches `col["name"]`).
            table_cols: set[str] = set()
            for t in tables:
                table_cols.update(holding["table_columns"].get(t, ()))
            missing = [t for t in tables if not holding["table_columns"].get(t)]
            if missing:
                raise SystemExit(
                    f"flavor table selector {(prov_slug, reg_key, var_key)} names "
                    f"table(s) absent from holding {key!r}: {missing}"
                )
            columns = [c for c in columns if c["name"] in table_cols]
        keep = frozenset(flavor.keep_columns)
        sensitive = frozenset(flavor.sensitive_columns)
        if absent := sorted((keep | sensitive) - {c["name"] for c in columns}):
            raise SystemExit(
                f"flavor column list of {(prov_slug, reg_key, var_key)} names "
                f"column(s) absent from holding {key!r}: {absent}"
            )
        dropped = sorted(
            {
                f"{c['name']} (Källa={c['kalla']})"
                for c in columns
                if _canonical_kalla(c) and c["name"] not in keep
            }
        )
        if dropped and not _mapping_registers(holding.get("mapping", {})):
            ungrafted.append(f"{key}: {', '.join(dropped)}")
        independent = flavor.period_scope == "year_independent"
        variables = _flavor_variables(
            columns, keep, sensitive, year_independent=independent
        )
        if not variables:
            # Every column was Källa-sourced (canonical) — nothing steward-only
            # remains. The curated contract requires >= 1 variable per variant,
            # and such a holding does not belong in the flavor anyway: drop its
            # disposition entry (route the canonical columns via the graft track).
            raise SystemExit(
                f"flavor holding {key!r} ({prov_slug}/{reg_key}) yielded 0 "
                "steward-only variables (all columns carry a Källa). Remove its "
                "[[flavor]] entry — its content is canonical, not flavor."
            )
        reg["_variants"][var_key] = {
            "key": var_key,
            "name": var_name,
            "description": None,
            "period_scope": "year_independent" if independent else "intervals",
            "variables": variables,
        }
        reg["_variant_slugs"][var_key] = var_slug
    if ungrafted:
        raise SystemExit(
            "flavor holding columns carry a register Källa, so the flavor leaves "
            "them to the global graft track, but their holding names no `graft` "
            "(or mapped series) register, so `globals` never scans them:\n  "
            + "\n  ".join(ungrafted)
            + '\nAdd `graft = "<provider>/<register>"` to the holding\'s [[holding]] '
            "in the steward scope.toml and rerun `enrich`. "
            "No provider output was written."
        )

    # --- curated-provider TOMLs (the extend-db contract) ---
    providers_dir = args.out / "providers"
    providers_dir.mkdir(parents=True, exist_ok=True)
    expected_provider_files = {f"{provider}.toml" for provider in providers}
    unexpected_provider_files = sorted(
        path.name
        for path in providers_dir.glob("*.toml")
        if path.name not in expected_provider_files
    )
    if unexpected_provider_files:
        raise SystemExit(
            "unexpected existing provider TOML(s) not emitted by the current "
            f"flavor disposition: {', '.join(unexpected_provider_files)}. Review "
            "and remove obsolete files before rerunning; no provider output was written."
        )
    steward = steward_id(args.spec)
    source_label = _source_label(args.csv, steward)
    for prov_slug, provider_name in sorted(providers.items()):
        provider_registers = []
        for (candidate_provider, _reg_key), reg in sorted(registers.items()):
            if candidate_provider != prov_slug:
                continue
            provider_registers.append(
                {
                    "key": reg["key"],
                    "name": reg["name"],
                    "purpose": reg["purpose"],
                    "description": reg["description"],
                    "variants": [
                        reg["_variants"][variant_key]
                        for variant_key in reg["_variants"]
                    ],
                }
            )
        (providers_dir / f"{prov_slug}.toml").write_text(
            _flavor_provider_toml(provider_name, source_label, provider_registers),
            encoding="utf-8",
        )

    # --- steward slug TOMLs (committed; register + register_variant only) ---
    # Deterministic high-band ids via reg_meta_build.id.mint mirror what
    # extend_db mints, so the `[register."<id>"]` / `[register_variant."<id>.<id>"]`
    # source-IDs line up with the inserted rows. Variable slugs auto-derive at
    # build time (not committed pre-v1), so they are not emitted here.
    from reg_meta_build.id import mint

    slug_root = REPO_ROOT / "reg_meta_build" / "fqid_slugs" / steward
    slug_root.mkdir(parents=True, exist_ok=True)
    by_provider: dict[str, list[str]] = {}
    for (prov_slug, reg_key), reg in sorted(registers.items()):
        register_id = mint("register", prov_slug, reg_key)
        lines = by_provider.setdefault(prov_slug, [])
        lines.append(f'[register."{register_id}"]')
        lines.append(f'slug = "{reg_key}"')
        lines.append("")
        for var_key in reg["_variants"]:
            variant_id = mint("variant", prov_slug, reg_key, var_key)
            lines.append(f'[register_variant."{register_id}.{variant_id}"]')
            lines.append(f'slug = "{reg["_variant_slugs"][var_key]}"')
            lines.append("")
    n_reg = n_var = 0
    for prov_slug, lines in sorted(by_provider.items()):
        header = (
            f"# Curated FQID slugs — SWECOV steward flavor (#421), provider "
            f"{prov_slug!r}.\n# Generated by build_catalog.py flavor; "
            f"register + register_variant only\n# (variable slugs auto-derive "
            f"each build while the zone is churning).\n\n"
        )
        (slug_root / f"{prov_slug}.toml").write_text(
            header + "\n".join(lines).rstrip() + "\n", encoding="utf-8"
        )
        n_reg += sum(1 for line in lines if line.startswith("[register."))
        n_var += sum(1 for line in lines if line.startswith("[register_variant."))

    n_vars = sum(
        len(v["variables"])
        for reg in registers.values()
        for v in reg["_variants"].values()
    )
    print(f"wrote curated-provider TOMLs to {providers_dir}/")
    print(f"  providers: {len(providers)}")
    print(f"  registers: {len(registers)}  variants: {n_var}  variables: {n_vars}")
    print(f"wrote steward slug TOMLs to {slug_root}/ ({len(by_provider)} providers)")


# --- steward inventory resolution ------------------------------------------


def _steward_load_db(db_path: Path):
    """Build the column-resolution index from a FLAVORED DB.

    Returns ``by_regcol[(provider/register, UPPER col)]`` → a list of
    ``{coord, vslug, col, valid_from, valid_to, period_scope}`` resolution records (``coord`` is the 3-part
    ``provider/register/variant`` coordinate).

    A co-delivered second spelling — the flavor group's other Covid column,
    every #945 multi-alias cvid on the global track — has no
    ``variable_state`` row of its own, so the union's second arm resolves it
    like a state column, using the window from ``variable_alias_window``.
    Admission is column-based, so a literal delivery column left unresolved is
    not orderable at all (steward README → "Near-duplicate physical columns").

    That arm reads the alias WINDOW table, not ``variable_alias``, because a
    window is what makes a spelling an orderable REPRESENTATION: the resolver
    (`Catalog._expand_state_windows`) surfaces an alias column only where its
    shared source window has a participating base column; provenance-bearing
    curated windows are additive and need no base-column window. Shared windows
    must be contained, while per-column storage or coding intersects states.
    Participating source windows replace the canonical state's window; otherwise
    the canonical representation remains. The holdings compiler's consistency
    gate (`reg_meta_build.holdings_compile`) rejects inventory mappings outside
    this delivery universe. A bare ``variable_alias`` row is the search-only
    delivery-column history — mapping one would state holdings no order could
    ever fill — so those two conditions are mirrored here rather than unioning
    the two tables flat.
    """
    from reg_meta_build.db import register_py_lower

    if not db_path.exists():
        raise SystemExit(f"mapping DB not found: {db_path}")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    # Unicode-aware LOWER for the participation test below: SQLite's own is
    # ASCII-only, so a Swedish header (`Ägare`) would not fold — the reason
    # every other delivery-column case comparison in the build uses it (#843).
    register_py_lower(conn)
    by_regcol: dict[tuple[str, str], list] = defaultdict(list)
    # The ORDER BY spans the whole union (alias rows sort INTO the state rows,
    # they are not appended after them), so the resolution order is stable
    # across reg_meta rebuilds. `coord` orders exactly as the variant slug did:
    # its `provider/register/` prefix is constant within the two preceding keys.
    for (
        prov,
        reg,
        coord,
        vslug,
        col,
        _dtype,
        _isid,
        _vsid,
        vf,
        vt,
        scope,
    ) in conn.execute(
        """SELECT p.slug AS prov, r.slug AS reg,
                  p.slug||'/'||r.slug||'/'||rv.slug AS coord, v.slug AS vslug,
                  vs.delivery_column_name AS col, vs.data_type AS dtype,
                  v.is_identifier AS isid, vs.value_set_id AS vsid,
                  vs.valid_from AS vf, vs.valid_to AS vt, vs.period_scope
           FROM variable_state vs JOIN variable v USING(variable_id)
           JOIN register r ON r.register_id=v.register_id
           JOIN provider p ON p.provider_id=r.provider_id
           JOIN register_variant rv USING(register_variant_id)
           WHERE vs.delivery_column_name IS NOT NULL
             AND trim(vs.delivery_column_name) != '' AND v.slug IS NOT NULL
             AND NOT (vs.period_scope='intervals' AND EXISTS (
                 SELECT 1 FROM variable_alias_window b
                 WHERE b.variable_id=vs.variable_id
                   AND b.register_variant_id=vs.register_variant_id
                   AND b.valid_from<=vs.valid_to AND vs.valid_from<=b.valid_to
                   AND (b.provenance IS NULL OR b.coding_metadata='per_column')
                   AND (b.column_metadata='per_column' OR b.coding_metadata='per_column'
                        OR (vs.valid_from<=b.valid_from AND b.valid_to<=vs.valid_to))
                   AND py_lower(b.delivery_column_name)=py_lower(vs.delivery_column_name)))
           UNION
           SELECT p.slug, r.slug, p.slug||'/'||r.slug||'/'||rv.slug, v.slug,
                  w.delivery_column_name, vs.data_type, v.is_identifier,
                  vs.value_set_id, MAX(vs.valid_from, w.valid_from),
                  MIN(vs.valid_to, w.valid_to), 'intervals'
           FROM variable_alias_window w JOIN variable v USING(variable_id)
           JOIN register r ON r.register_id=v.register_id
           JOIN provider p ON p.provider_id=r.provider_id
           JOIN register_variant rv USING(register_variant_id)
           JOIN variable_state vs ON vs.variable_id=w.variable_id
                AND vs.register_variant_id=w.register_variant_id
                AND vs.period_scope='intervals'
                AND vs.valid_from<=w.valid_to AND w.valid_from<=vs.valid_to
           WHERE trim(w.delivery_column_name) != '' AND v.slug IS NOT NULL
             AND (w.column_metadata='per_column' OR w.coding_metadata='per_column'
                  OR (vs.valid_from<=w.valid_from AND w.valid_to<=vs.valid_to))
             AND (
                 (w.provenance IS NOT NULL AND w.coding_metadata!='per_column')
                 OR EXISTS (SELECT 1 FROM variable_alias_window b
                            WHERE b.variable_id=w.variable_id
                              AND b.register_variant_id=w.register_variant_id
                              AND b.valid_from<=vs.valid_to AND vs.valid_from<=b.valid_to
                              AND (b.provenance IS NULL OR b.coding_metadata='per_column')
                              AND (b.column_metadata='per_column'
                                   OR b.coding_metadata='per_column'
                                   OR (vs.valid_from<=b.valid_from AND b.valid_to<=vs.valid_to))
                              AND py_lower(b.delivery_column_name)
                                  =py_lower(vs.delivery_column_name)))
           ORDER BY prov, reg, coord, vslug, col, vf, dtype"""
    ):
        u = col.upper()
        rec = {
            "coord": coord,
            "vslug": vslug,
            "col": col,
            "valid_from": vf,
            "valid_to": vt,
            "period_scope": scope,
        }
        by_regcol[(f"{prov}/{reg}", u)].append(rec)
    conn.close()
    return by_regcol


def _inventory_period_records(
    records: list[dict],
    edition: RawEdition,
    *,
    assess_ranges: bool = False,
    licensed: tuple[tuple[str, str], ...] = (),
) -> tuple[list[dict], str | None]:
    """Choose already-declared owners by their complete delivery windows.

    A range/list is record coverage, not per-column availability; it remains
    unassessed unless `assess_ranges` (a reviewed `[[mapping]]`, which must
    cover its whole edition). The year-independent edition `_default` admits
    only year-independent states, and a finite edition only dated ones: neither
    scope stands in for the other. Distinct owners active together in one
    variant stay ambiguous. `licensed` days (a mapping warning's, #1684) need
    no owner coverage.
    """
    from reg_core_py import merge
    from reg_meta_build._resolved_common import remaining_windows
    from reg_meta_build.inventory_coverage import _covers

    independent = edition == YEAR_INDEPENDENT_EDITION
    scope = "year_independent" if independent else "intervals"
    # Scope before the unassessed-range return: a range/list still never admits
    # a year-independent state. An emptied list falls through to the refusal.
    records = [r for r in records if r["period_scope"] == scope]
    if (
        records
        and not independent
        and not assess_ranges
        and not isinstance(edition, (str, int))
    ):
        return records, None
    bounds = (
        ()
        if independent
        else tuple(
            part
            for lo, hi in edition_bounds_of(edition)
            for part in remaining_windows(licensed, lo, hi)
        )
    )
    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for record in records:
        grouped[(record["coord"], record["vslug"])].append(record)
    admitted: list[dict] = []
    owners: dict[str, set[str]] = defaultdict(set)
    for (coord, owner), members in sorted(grouped.items()):
        # Separate case spellings remain literal representations. Do not borrow
        # another spelling's dates to admit a column in an unsupported edition.
        by_column: dict[str, list[dict]] = defaultdict(list)
        for member in members:
            by_column[member["col"]].append(member)
        active = []
        for column_records in by_column.values():
            if independent:
                active.extend(column_records)
                continue
            windows = merge(
                [
                    (r["valid_from"], r["valid_to"])
                    for r in column_records
                    if r["valid_from"] is not None and r["valid_to"] is not None
                ]
            )
            if all(_covers(interval, tuple(windows)) for interval in bounds):
                active.extend(column_records)
        if active:
            owners[coord].add(owner)
            admitted.extend(active)
    if any(len(active) > 1 for active in owners.values()):
        return [], "ambiguous_delivered_owners"
    if not admitted:
        return [], "no_covering_delivery_owner"
    return admitted, None


def _representative_spelling(db_path: Path) -> Callable[[str, str, str], str | None]:
    """The spelling the holdings compiler stores for a delivery column, or None
    when the fold is not in the resolver's delivery universe. Reads the same
    derived `resolver_column` universe and exact-coordinate resolution
    (`catalog_coordinate_ids`) that `holdings_compile.canonical_inventory`
    canonicalizes with. Opened per call: only a case-only twin with neither a
    curated nor a physical literal asks."""
    from contextlib import closing

    from reg_meta_build.db import catalog_coordinate_ids, register_py_lower

    def representative(coord: str, vslug: str, column: str) -> str | None:
        provider, register, _variant = coord.split("/")
        variable = f"{provider}/{register}/{vslug}"
        with closing(sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)) as conn:
            conn.row_factory = sqlite3.Row
            register_py_lower(conn)
            variables, variants = catalog_coordinate_ids(
                conn, variable_fqids={variable}, variant_coords={coord}
            )
            if variable not in variables or coord not in variants:
                return None
            universe = dict(
                conn.execute(
                    "SELECT delivery_column_lower, delivery_column_name "
                    "FROM resolver_column "
                    "WHERE variable_id = ? AND register_variant_id = ?",
                    (variables[variable], variants[coord]),
                ).fetchall()
            )
        return universe.get(column.lower())

    return representative


def _fold_case_twins(
    records: list[dict],
    physical: str,
    declared: set[tuple[str, str, str]],
    representative: Callable[[str, str, str], str | None],
) -> tuple[list[dict], str | None]:
    """Keep ONE literal per owner where admitted spellings differ only in case.

    The holdings compiler folds each representation with `str.lower()` and
    rejects two mappings of one physical column naming the same canonical
    triple (`reg_meta_build.holdings_compile.canonical_inventory`), so case-only
    twins are one delivery declared twice, not two holdings. Twins are grouped
    per `(variant, owner)`; different variants or owners never fold. Precedence:
    the curated `[[mapping]]` literal (`declared`, as `(coord, vslug, col)`);
    else the literal equal to the physical field (LopNr prefix stripped); else
    the representative spelling the compiler stores; else fail for review. Only
    records of the dropped spellings are removed, so mapping order is unchanged.
    """
    spellings: dict[tuple[str, str, str], set[str]] = defaultdict(set)
    for record in records:
        spellings[record["coord"], record["vslug"], record["col"].lower()].add(
            record["col"]
        )
    keep: dict[tuple[str, str, str], str] = {}
    for key, literals in spellings.items():
        if len(literals) == 1:
            continue
        coord, vslug, _fold = key
        curated = sorted(c for c in literals if (coord, vslug, c) in declared)
        if len(curated) > 1:
            # The overlay loader already allows one override per physical
            # column and variant; two curated twins would be contradictory.
            return [], "case_twin_conflicting_curated_literals"
        if curated:
            keep[key] = curated[0]
        elif physical in literals:
            keep[key] = physical
        elif (spelling := representative(coord, vslug, min(literals))) in literals:
            keep[key] = spelling
        else:
            return [], "case_twin_without_physical_or_representative_literal"
    return [
        record
        for record in records
        if keep.get(
            (record["coord"], record["vslug"], record["col"].lower()), record["col"]
        )
        == record["col"]
    ], None


# --- inventory: emit the delivery inventory (reality ∩ spec) -----------------
#
# The physical delivery-topology contract (reg_meta_build/DESIGN.md → Inventory TOML;
# curation rules in reg_meta_build/DESIGN.md → Holdings curation rules): one
# `[[table]]` per CURRENT physical table on MONA, one explicit finite edition, literal
# columns, and zero-or-more (register_variant, variable, representation) mappings per
# column. Reality is the holdings CSV (the maintainer's own re-extractable listing of
# everything on MONA — superseded snapshots included, so it is NEVER hand-edited);
# every decision is the steward spec (`reg_webapp/stewards/<id>/`, read through
# `reg_meta_build.steward_spec`):
#
#   scope.toml       lookups, exclusions and retained-unknown tables: not emitted
#   series/*.toml    each member table with its explicit edition (spelled by the
#                    series' `edition_spelling`), partition and curated
#                    `projection` (#1634); the series'
#                    `variants` are the table's admissible coordinates; curated
#                    column decisions: a reviewed mapping (guarded by the physical
#                    coordinates and full catalog coverage of the edition, or of
#                    the mapping's own `window` (#1645), emitted verbatim), an
#                    unmap or an interim hold
#
# Every other column is auto-mapped by the single-candidate rule against the
# series' variants. The emitted `inventory.toml` is a local builder input,
# validated through `reg_meta_build.inventory.load_inventory` — its one-to-one
# conflicts are the maintainer's supersession worklist, and the build stays red
# until every conflict has a spec disposition. Spec entries naming tables no
# longer in the CSV are flagged stale.
#
# Every emitted mapping carries an EXPLICIT `representation` (the resolved
# canonical `delivery_column_name`): exact-triple matching in `order.py` never
# needs the single-representation proof, and the conflict validator then only
# conflates genuinely identical cells.
#
# A wide-pivot member (DIA1..30 → representation DIA) maps only through a curated
# decision naming its `member` index (#1495): exact names, never a suffix rule.
# The decision is the column's one mapping; the
# inventory's member-set rules check the set. A member with no decision is an
# unresolved column like any other.


def _inventory_mapping_records(
    mapping: ColumnMapping, select_owner: bool, records: list[dict], edition: RawEdition
) -> tuple[list[dict], str | None]:
    """The delivered records a reviewed mapping pins, or a stale-target code.

    A warned mapping (#1684) whose owner leaves part of the reach uncovered is
    admitted when its warning, clipped to the table, states exactly those days
    (`inventory_coverage.warning_gap_problem`, the rule extend-db refuses by);
    the owner must then cover the rest. A mismatch is
    `inventory_coverage_warning_invalid`. An unwarned gap stays stale."""
    from reg_core_py import gaps, merge
    from reg_meta_build.inventory_coverage import warning_gap_problem

    owner = mapping.variable.rsplit("/", 1)[1]
    selected = [
        r
        for r in records
        if r["coord"] == mapping.register_variant
        and r["col"] == mapping.representation
        and (not select_owner or r["vslug"] == owner)
    ]
    licensed: tuple[tuple[str, str], ...] = ()
    warning = mapping.data_warning
    # A warning whose window misses this table is not emitted for it
    # (`_warning_line`), so the table reviews as unwarned: a gap stays stale.
    if (
        warning is not None
        and edition != YEAR_INDEPENDENT_EDITION
        and (warning.window is None or _warning_window(warning, edition) is not None)
    ):
        served = edition_bounds_of(edition)
        # simplify: coverage here is the owner's delivered records for the exact
        # representation spelling (states and the alias windows the records
        # query admits); extend-db reads `inventory_coverage._load_windows`,
        # folded by NFC casefold and with its own alias-window admission, and is
        # the authority. Read `_load_windows` here if the two ever disagree on a
        # SWECOV decision (a case-only twin or a filtered alias window).
        covered = merge(
            [
                (r["valid_from"], r["valid_to"])
                for r in selected
                if r["vslug"] == owner
                and r["period_scope"] == "intervals"
                and r["valid_from"] is not None
                and r["valid_to"] is not None
            ]
        )
        # An edition's segments may abut, so the gaps are merged again.
        if uncovered := merge(gaps(served, covered)):
            clip = None if warning.window is None else _warning_window(warning, edition)
            stated = served if clip is None else (clip,)
            if warning_gap_problem(served, uncovered, stated):
                return [], "inventory_coverage_warning_invalid"
            licensed = uncovered
    admitted, issue = _inventory_period_records(
        selected, edition, assess_ranges=True, licensed=licensed
    )
    owned = [r for r in admitted if r["vslug"] == owner]
    return owned, "stale_declared_mapping_target" if issue or not owned else None


def _overlaps_edition(record: dict, edition: RawEdition) -> bool:
    """Whether a delivered record's window reaches a finite edition; an undated
    record counts as reaching it. A year-independent state never does: a finite
    edition admits only dated ones (`_inventory_period_records`)."""
    if record["period_scope"] != "intervals":
        return False
    if record["valid_from"] is None or record["valid_to"] is None:
        return True
    return any(
        record["valid_from"] <= hi and lo <= record["valid_to"]
        for lo, hi in edition_bounds_of(edition)
    )


def _competing_owner(
    mappings: list[ColumnMapping],
    by_coordcol: dict,
    reach: Callable[[ColumnMapping], RawEdition],
) -> bool:
    """Whether another owner delivers a mapped representation on its variant over
    the mapping's reach (the edition, or a column window). Looked up by the
    declared representation: the physical-column lookup misses these when the
    representation differs from the physical name."""
    owners = {(m.register_variant, m.variable.rsplit("/", 1)[1]) for m in mappings}
    return any(
        r["coord"] == mapping.register_variant
        and (r["coord"], r["vslug"]) not in owners
        and _overlaps_edition(r, reach(mapping))
        for mapping in mappings
        for r in by_coordcol[(mapping.register_variant, mapping.representation.upper())]
    )


def _union_mapping_records(
    mappings: list[ColumnMapping],
    select_owner: bool,
    by_coordcol: dict,
    edition: RawEdition,
) -> tuple[list[tuple[ColumnMapping, list[dict]]], str | None]:
    """The records of reviewed mappings that cover the edition only together (#1555).

    A column whose edition straddles a variant split (one variant to 2018-09, its
    successor from 2018-10) maps once per variant. Each mapping's owner windows,
    clipped to the edition, must be nonempty; together they tile the edition with
    no gap and no overlap. Without `select_owner`, another owner of a mapped
    representation active in the edition competes with the union. The records are
    marked `union`: each covers only its own part, so the whole-edition owner check
    does not apply to them. Ordering clips each variant to its own states.
    """
    from reg_core_py import merge
    from reg_meta_build.inventory_coverage import _covers

    if edition == YEAR_INDEPENDENT_EDITION:
        return [], "stale_declared_mapping_target"
    bounds = edition_bounds_of(edition)
    if not select_owner and _competing_owner(mappings, by_coordcol, lambda _: edition):
        return [], "ambiguous_delivered_owners"
    pieces: list[tuple[ColumnMapping, list[dict]]] = []
    clipped: list[tuple[str, str]] = []
    for mapping in mappings:
        owner = mapping.variable.rsplit("/", 1)[1]
        owned = [
            r
            for r in by_coordcol[
                (mapping.register_variant, mapping.representation.upper())
            ]
            if r["coord"] == mapping.register_variant
            and r["col"] == mapping.representation
            and r["vslug"] == owner
            and r["period_scope"] == "intervals"
            and r["valid_from"] is not None
            and r["valid_to"] is not None
        ]
        windows = [
            (max(lo, b_lo), min(hi, b_hi))
            for lo, hi in merge([(r["valid_from"], r["valid_to"]) for r in owned])
            for b_lo, b_hi in bounds
            if lo <= b_hi and b_lo <= hi
        ]
        if not windows:
            return [], "stale_declared_mapping_target"
        clipped += windows
        pieces.append((mapping, [{**r, "union": True} for r in owned]))
    clipped.sort()
    if any(hi >= next_lo for (_, hi), (next_lo, _) in pairwise(clipped)):
        return [], "union_mapping_overlap"
    merged = tuple(merge(clipped))
    if not all(_covers(interval, merged) for interval in bounds):
        return [], "union_mapping_gap"
    return pieces, None


def _toml_str(value: str) -> str:
    # JSON string escaping is valid TOML basic-string escaping except U+007F,
    # which JSON leaves raw and TOML forbids in a basic string.
    return json.dumps(value, ensure_ascii=False).replace("\x7f", "\\u007f")


def _raw_window(window: EditionRange | str) -> RawEdition:
    """A mapping's column window in its authored TOML shape (#1645)."""
    if isinstance(window, str):
        return window
    return {"from": window.from_, "to": window.to}


def _warning_window(
    warning: MappingWarning, reach: RawEdition
) -> tuple[str, str] | None:
    """A windowed mapping warning's window clipped to what the mapping serves in
    one table (`reach`: its window, its member's period or the table edition), or
    None where it misses (a year-independent reach is always missed)."""
    from reg_core_py import intersect

    assert warning.window is not None
    if reach == YEAR_INDEPENDENT_EDITION:
        return None
    window = edition_bounds_of(_raw_window(warning.window))[0]
    parts = [
        clipped
        for segment in edition_bounds_of(reach)
        if (clipped := intersect(segment, window))
    ]
    # simplify: across an interrupted edition the clip is its hull, which the
    # inventory refuses (a warning window lies in one served segment). Emit one
    # warning per segment if a decision's warning must span such a gap.
    return (parts[0][0], parts[-1][1]) if parts else None


def _warning_line(warning: MappingWarning, reach: RawEdition) -> str | None:
    """A decision's mapping warning (#1684) as one table's `data_warning` line,
    its window clipped to the table (`_warning_window`), or None where it misses."""
    from reg_core_py import render

    fields = (
        f"summary = {_toml_str(warning.summary)}, detail = {_toml_str(warning.detail)}"
    )
    if warning.window is None:
        return f"data_warning = {{ {fields} }}"
    if (clip := _warning_window(warning, reach)) is None:
        return None
    token = render([clip])
    ends = [int(end) if end.isdigit() else end for end in token.split("..")]
    clipped = ends[0] if len(ends) == 1 else {"from": ends[0], "to": ends[1]}
    return f"data_warning = {{ {fields}, window = {_toml_edition(clipped)} }}"


def _toml_edition(edition: RawEdition) -> str:
    if isinstance(edition, int):
        return str(edition)
    if isinstance(edition, str):
        return _toml_str(edition)
    if isinstance(edition, dict):
        return (
            f"{{ from = {_toml_edition(edition['from'])}, "
            f"to = {_toml_edition(edition['to'])} }}"
        )
    if isinstance(edition, list):
        return "[" + ", ".join(_toml_edition(e) for e in edition) + "]"
    raise SystemExit(f"unrepresentable edition value: {edition!r}")


def cmd_inventory(args: argparse.Namespace) -> None:
    """Emit `inventory.toml` from the census, the spec and a mapping DB this
    stage builds itself from the pinned global plus the candidate's providers.

    The candidate manifest pins that pair (`global_db_sha256`) and the spec
    (`steward_spec_sha256`), so `extend-db` can refuse an inventory auto-mapped
    against another catalog or decided by another spec. The storage export is a
    required input, read first: no run skips the storage check (#1594).

    Both printed pins are the inputs generation used, never a later re-read: the
    global is hashed before the mapping DB is built and `extend-db` refuses a
    base whose bytes differ from that digest, and the spec digest is taken from
    the bytes the generation's spec load parsed."""
    import hashlib
    from tempfile import TemporaryDirectory

    from reg_meta_build.steward_storage import StorageSchemaError, read_storage_types

    try:
        storage = read_storage_types(args.storage_csv)
    except StorageSchemaError as exc:
        raise SystemExit(f"inventory: {exc}") from exc
    with args.base_db.open("rb") as base:
        global_sha = hashlib.file_digest(base, "sha256").hexdigest()
    with TemporaryDirectory(prefix="steward-mapping-db-") as scratch:
        mapping_db = _mapping_db(
            args.base_db,
            global_sha,
            args.providers,
            steward_id(args.spec),
            Path(scratch),
        )
        spec = write_inventory(args, mapping_db)
    _check_storage_types(args, storage)
    print("  candidate manifest pins:")
    print(f'    "steward_spec_sha256": "{spec.sha256}"')
    print(f'    "global_db_sha256": "{global_sha}"')


def _mapping_db(
    base_db: Path, base_sha256: str, providers: Path, steward: str, scratch: Path
) -> Path:
    """The flavored DB the inventory maps against: `extend-db`'s metadata-only
    diagnostic of `base_db` (refused unless its bytes hash to `base_sha256`) plus
    `providers`, named by the committed steward slug pins (copied, so its auto
    slugs never write into the checkout)."""
    import shutil

    from reg_meta_build.extend_db import extend_db

    slugs = scratch / "slugs"
    slugs.mkdir()
    for path in sorted(
        (REPO_ROOT / "reg_meta_build/fqid_slugs" / steward).glob("*.toml")
    ):
        if not path.name.endswith(".auto.toml"):
            shutil.copyfile(path, slugs / path.name)
    result = extend_db(
        base_db,
        providers,
        scratch / "db",
        steward=steward,
        slug_dir=slugs,
        diagnostic=True,
        base_sha256=base_sha256,
    )
    return Path(result["db_path"])


def _checked_spec(args: argparse.Namespace, holdings: dict) -> StewardSpec:
    """The spec at `args.spec`, held to the census at `args.csv`.

    The stages that take their decisions from the census (`ground`, `enrich`,
    `inventory`) run the census guard and the one-disposition rule here, so an
    unseriesed table refuses instead of quietly getting no admissible variant.
    `globals`, `grafts` and `flavor` read `enrich`'s output, already held."""
    spec = load_steward_spec(args.spec)
    tables = {table for h in holdings.values() for table in h.table_columns}
    check_census(spec, args.csv, tables)
    return spec


def write_inventory(args: argparse.Namespace, db: Path) -> StewardSpec:
    """`inventory`'s emitter over an already-built mapping DB: `args.csv`,
    `args.spec` and `args.out`. Returns the loaded spec."""
    holdings = parse_inventory(args.csv)
    spec = _checked_spec(args, holdings)
    steward_dir = args.out
    steward_dir.mkdir(parents=True, exist_ok=True)
    # A table may appear under several (Category, Detail) keys; its columns merge.
    census: dict[str, set[str]] = {}
    for h in holdings.values():
        for table, cols in h.table_columns.items():
            census.setdefault(table, set()).update(cols)
    by_regcol = _steward_load_db(db)
    representative = _representative_spelling(db)
    # coord -> {UPPER(col): [(coord, vslug, canonical col)]} narrowed per lookup.
    by_coordcol: dict[tuple[str, str], list] = defaultdict(list)
    for (_reg, u), recs in by_regcol.items():
        for r in recs:
            by_coordcol[(r["coord"], u)].append(r)

    series_of = spec.series_of()
    members = {t.table: t for series in spec.series for t in series.tables}
    table_variants = {
        table: variants
        for series in spec.series
        for table, variants in series.table_variants().items()
    }
    decisions = {
        key: entry
        for series in spec.series
        for key, entry in series.decisions().items()
    }
    retained_unknown = {entry.table: entry for entry in spec.scope.retain_unknown}
    excluded = {entry.table for entry in spec.scope.excludes}
    n_lookup_tables = sum(entry.table in census for entry in spec.scope.lookups)

    # Stale = gone from the CSV entirely.
    stale = sorted(t for t in spec.dispositions() if t not in census)
    worklist: dict[str, list] = {
        "retained_unknown": [],
        "stale_spec_entries": stale,
        "mapping_scope_needed": [],
        "unresolved": [],
    }
    for (table, column), entry in decisions.items():
        if entry.mappings is not None and (
            table not in census or column not in census[table]
        ):
            worklist["mapping_scope_needed"].append(
                {
                    "table": table,
                    "column": column,
                    "code": "stale_declared_mapping_physical_coordinate",
                }
            )
    n_cols = n_mapped = n_unmapped = 0
    # Tables re-spelled per series edition_spelling (printed).
    n_school_year: dict[str, int] = defaultdict(int)
    lines: list[str] = [
        "# GENERATED by reg_webapp/stewards/swecov/build_catalog.py inventory — do not",
        "# hand-edit. Reality is the holdings CSV; policy is the committed",
        "# inventory_overlay.toml next to this file (reg_meta_build/DESIGN.md →",
        "# Holdings curation rules).",
        "",
        "version = 1",
        f"steward = {_toml_str(steward_id(args.spec))}",
    ]
    for table in sorted(census, key=str.upper):
        if table in retained_unknown:
            retention = retained_unknown[table]
            worklist["retained_unknown"].append(
                {
                    "table": table,
                    "columns": sorted(census[table]),
                    "register": retention.register_fqid,
                    "rows_sha256": retention.rows_sha256,
                    "reason": retention.reason,
                }
            )
            continue
        series = series_of.get(table)
        if series is None:  # a lookup or an exclusion
            continue
        edition = series.spelled_edition(members[table])
        if edition != members[table].inventory_edition:
            n_school_year[series.id] += 1
        # The table maps only onto its own admissible variants, all of the
        # series' register (`steward_series_register_mismatch` at load), so an
        # auto-mapped column cannot pull the series into a second register.
        coords = set(table_variants[table])
        cols = sorted(census[table])
        lines += ["", "[[table]]", f"id = {_toml_str(table)}"]
        lines.append(f"edition = {_toml_edition(edition)}")
        if edition == YEAR_INDEPENDENT_EDITION:
            lines.append('period_scope = "year_independent"')
        if (partition := members[table].partition) is not None:
            lines.append(f"partition = {_toml_str(partition)}")
        if (projection := members[table].projection) is not None:
            lines.append(
                f"projection = {{ of = {_toml_str(projection.of)}, "
                f"token = {_toml_str(projection.token)} }}"
            )
        if (period := members[table].period_column) is not None:
            lines.append(
                f"period_column = {{ name = {_toml_str(period.name)}, "
                f"encoding = {_toml_str(period.encoding)} }}"
            )
        for col in cols:
            n_cols += 1
            decision = decisions.get((table, col))
            if decision is not None and decision.unmapped_reason is not None:
                n_unmapped += 1
                lines += [
                    "",
                    "[[table.column]]",
                    f"name = {_toml_str(col)}",
                    f"unmapped_reason = {_toml_str(decision.unmapped_reason)}",
                ]
                continue
            u = LOPNR_PREFIX.sub("", col).upper()
            recs = [r for coord in sorted(coords) for r in by_coordcol[(coord, u)]]
            mapping_issue = None
            curated: set[tuple[str, str, str]] = set()
            declared: list[dict]
            # A year or month member serves only its own period, so its owner
            # must cover that period, not the table's whole edition.
            column_edition = edition
            mappings = (decision.mappings if decision is not None else None) or []
            select_owner = decision is not None and decision.select_owner
            # Mappings that cover the edition only together, tried as a union.
            partial: list[ColumnMapping] = []
            adopted: list[tuple[ColumnMapping, list[dict]]] = []
            for mapping in mappings:
                if mapping.member is not None and mapping.member.kind != "position":
                    column_edition = mapping.member.index
                # A column window (#1645) is that mapping's own delivered period,
                # so its owner must cover the window only.
                declared, mapping_issue = _inventory_mapping_records(
                    mapping,
                    select_owner,
                    by_coordcol[
                        (mapping.register_variant, mapping.representation.upper())
                    ],
                    column_edition
                    if mapping.window is None
                    else _raw_window(mapping.window),
                )
                if mapping_issue == "stale_declared_mapping_target" and (
                    len(mappings) > 1
                    and mapping.member is None
                    and mapping.window is None
                ):
                    partial.append(mapping)
                    mapping_issue = None
                    continue
                if mapping_issue:
                    break
                adopted.append((mapping, declared))
            if len(partial) == 1:
                mapping_issue = "stale_declared_mapping_target"
            elif partial and not mapping_issue:
                pieces, mapping_issue = _union_mapping_records(
                    partial, select_owner, by_coordcol, column_edition
                )
                adopted += pieces
            # A decision with a window decides each mapping over its own reach:
            # its window, or the edition for a plain mapping beside it.
            windowed = any(mapping.window is not None for mapping, _ in adopted)
            if (
                windowed
                and not mapping_issue
                and not select_owner
                and _competing_owner(
                    [mapping for mapping, _ in adopted],
                    by_coordcol,
                    lambda m, column_edition=column_edition: (
                        column_edition if m.window is None else _raw_window(m.window)
                    ),
                )
            ):
                mapping_issue = "ambiguous_delivered_owners"
            for mapping, declared in [] if mapping_issue else adopted:
                if mapping.member is not None:
                    declared = [{**r, "member": mapping.member} for r in declared]
                if mapping.window is not None:
                    window = _raw_window(mapping.window)
                    declared = [{**r, "window": window} for r in declared]
                elif windowed:
                    declared = [{**r, "decided": True} for r in declared]
                if mapping.data_warning is not None:
                    line = _warning_line(
                        mapping.data_warning,
                        column_edition
                        if mapping.window is None
                        else _raw_window(mapping.window),
                    )
                    declared = [{**r, "data_warning": line} for r in declared]
                owner = mapping.variable.rsplit("/", 1)[1]
                # select_owner drops every other owner of the representation and
                # of the physical name on this variant. Every record on the
                # variant here came through the folded physical-name lookup (a
                # decision names each variant once), so all of them go: case
                # twins (neighbouring waves spell one cell `co11` and `CO11`,
                # #1571) and, for a renamed column, the physical name's own
                # owner, which would otherwise survive as a second mapping or
                # an ambiguity (#1664). The declared owner is re-added.
                recs = [
                    r
                    for r in recs
                    if r["coord"] != mapping.register_variant
                    or (
                        not select_owner
                        and (r["col"] != mapping.representation or r["vslug"] != owner)
                    )
                ] + declared
                curated.update((r["coord"], r["vslug"], r["col"]) for r in declared)
            if any(r.get("member") for r in recs):
                # A member column carries only its decision's mappings; a second
                # curated one stays, so the inventory's one-mapping rule refuses it.
                recs = [
                    r for r in recs if (r["coord"], r["vslug"], r["col"]) in curated
                ]
            if mapping_issue:
                worklist["mapping_scope_needed"].append(
                    {
                        "table": table,
                        "column": col,
                        "edition": edition,
                        "code": mapping_issue,
                    }
                )
                lines += ["", "[[table.column]]", f"name = {_toml_str(col)}"]
                continue
            if not recs:
                worklist["unresolved"].append(
                    {
                        "table": table,
                        "column": col,
                        "edition": edition,
                        "register_variants": sorted(coords),
                    }
                )
                lines += ["", "[[table.column]]", f"name = {_toml_str(col)}"]
                continue

            # Union records, windowed records and the plain records of a decision
            # with a window cover only their own part, so they skip the
            # whole-edition owner check. On their variants they decide over their
            # reach, the window for a windowed record and the edition otherwise:
            # another owner active there competes with them (ambiguous, as for any
            # reviewed mapping without select_owner), and their owner's other
            # spellings drop. Other variants take the usual check.
            def decides(r: dict) -> bool:
                return bool(r.get("union") or r.get("window") or r.get("decided"))

            union = [r for r in recs if decides(r)]
            owners = {(r["coord"], r["vslug"]) for r in union}
            # Keyed by variant: a decision names each variant once
            # (`steward_spec` refuses a repeated mapping register_variant).
            reach = {r["coord"]: r.get("window") or column_edition for r in union}
            union_coords = set(reach)
            competing = any(
                r["coord"] in union_coords
                and (r["coord"], r["vslug"]) not in owners
                and _overlaps_edition(r, reach[r["coord"]])
                for r in recs
                if not decides(r)
            )
            rest = [
                r for r in recs if not decides(r) and r["coord"] not in union_coords
            ]
            if union:
                # The mapped variants decide over their reach, so an owner on
                # another variant matters only if it reaches one of them, where it
                # competes; a historical one (a variant ending before it) is
                # dropped.
                competing = competing or any(
                    _overlaps_edition(r, period)
                    for r in rest
                    for period in reach.values()
                )
                rest = []
            recs, scope_issue = (
                ([], "ambiguous_delivered_owners")
                if competing
                else _inventory_period_records(rest, column_edition)
                if rest
                else ([], None)
            )
            recs += [] if scope_issue else union
            if not scope_issue:
                recs, scope_issue = _fold_case_twins(
                    recs, LOPNR_PREFIX.sub("", col), curated, representative
                )
            if scope_issue:
                worklist["mapping_scope_needed"].append(
                    {
                        "table": table,
                        "column": col,
                        "edition": edition,
                        "code": scope_issue,
                        "candidate_records": [
                            r
                            for coord in sorted(coords)
                            for r in by_coordcol[(coord, u)]
                        ],
                    }
                )
                lines += ["", "[[table.column]]", f"name = {_toml_str(col)}"]
                continue
            n_mapped += 1
            lines += ["", "[[table.column]]", f"name = {_toml_str(col)}"]
            seen: set[tuple[str, str, str]] = set()
            for r in recs:
                triple = (r["coord"], r["vslug"], r["col"])
                if triple in seen:
                    continue
                seen.add(triple)
                prov, regslug, _var = r["coord"].split("/")
                variable_fqid = f"{prov}/{regslug}/{r['vslug']}"
                lines += [
                    "",
                    "[[table.column.mapping]]",
                    f"register_variant = {_toml_str(r['coord'])}",
                    f"variable = {_toml_str(variable_fqid)}",
                    f"representation = {_toml_str(r['col'])}",
                ]
                if (member := r.get("member")) is not None:
                    index = member.index
                    value = _toml_str(index) if isinstance(index, str) else index
                    lines.append(f"member = {{ {member.kind} = {value} }}")
                if (window := r.get("window")) is not None:
                    lines.append(f"window = {_toml_edition(window)}")
                if (warning := r.get("data_warning")) is not None:
                    lines.append(warning)

    dest = steward_dir / "inventory.toml"
    wl_path = args.out / "derived" / "inventory_worklist.json"
    wl_path.parent.mkdir(parents=True, exist_ok=True)
    wl_path.write_text(
        json.dumps(worklist, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    if worklist["mapping_scope_needed"]:
        raise SystemExit(
            f"{len(worklist['mapping_scope_needed'])} physical column(s) need "
            f"positive delivery-owner scope or a case-only twin decision; see "
            f"{wl_path}. Inventory not replaced."
        )
    # The inventory refuses a column with neither a mapping nor an
    # unmapped_reason (#1294), so an unresolved column needs a decision first.
    if worklist["unresolved"]:
        raise SystemExit(
            f"{len(worklist['unresolved'])} physical column(s) resolve to no "
            f"catalog variable and need a spec decision (a mapping, unmap or "
            f"hold); see {wl_path}. Inventory not replaced."
        )
    dest.write_text("\n".join(lines) + "\n", encoding="utf-8")
    n_excluded = len(excluded & set(census))
    n_tables = (
        len(census) - n_excluded - n_lookup_tables - len(worklist["retained_unknown"])
    )
    print(f"wrote {dest}")
    print(
        f"  tables: {n_tables} emitted ({n_excluded} excluded,"
        f" {n_lookup_tables} lookup-skipped,"
        f" {len(worklist['retained_unknown'])} retained unknown)"
    )
    print(f"  columns: {n_cols}  mapped: {n_mapped}  curated-unmapped: {n_unmapped}")
    for series_id, n in sorted(n_school_year.items()):
        print(f"  edition_spelling {series_id}: {n} tables re-spelled")
    if stale:
        print(f"  STALE spec entries (nothing to apply them to): {len(stale)}")
        for t in stale:
            print(f"    {t}")
    print(f"  wrote worklist to {wl_path}")

    from reg_meta_build.errors import RegMetaError
    from reg_meta_build.inventory import load_inventory

    try:
        inv = load_inventory(dest)
    except RegMetaError as exc:
        print(f"\nVALIDATION FAILED — the supersession worklist:\n{exc.message}")
        raise SystemExit(1) from exc
    print(f"  load_inventory: OK ({len(inv.tables)} tables)")
    return spec


def _check_storage_types(
    args: argparse.Namespace, storage: dict[tuple[str, str], str]
) -> None:
    """Report curated global data types the steward's storage disagrees with (#1562).

    The global build never reads the storage schema, so this refresh is where a
    disagreement surfaces. It is a worklist, never an automatic global change.
    Each curated entry is checked against the physical columns the inventory
    just written maps to its variable (#1666).
    """
    from reg_meta_build.curation_tree import load_curation_tree
    from reg_meta_build.inventory import load_inventory
    from reg_meta_build.steward_storage import (
        storage_type_findings,
        write_storage_findings,
    )

    findings = storage_type_findings(
        load_curation_tree(REPO_ROOT / "reg_meta_build" / "curation"),
        load_inventory(args.out / "inventory.toml"),
        storage,
    )
    dest = args.out / "derived" / "storage_type_findings.tsv"
    dest.parent.mkdir(parents=True, exist_ok=True)
    write_storage_findings(findings, dest)
    counts = {
        kind: sum(item.kind == kind for item in findings)
        for kind in ("disagrees", "uncurated", "unmatched", "narrower")
    }
    line = (
        f"storage type check: {counts['disagrees']} curated types disagree with"
        f" steward storage, {counts['uncurated']} steward-holdings columns lack a"
        f" curated type, {counts['unmatched']} curated entries match no storage"
        f" column, {counts['narrower']} curated types are wider than storage;"
        f" worklist {dest}"
    )
    # In the run summary on stdout beside the other counts, so it is not lost.
    warning = (
        "WARNING: "
        if counts["disagrees"] or counts["uncurated"] or counts["unmatched"]
        else ""
    )
    print(f"  {warning}{line}")


# --- errata: the columns held in editions the catalog has no window for ------
#
# The generated inventory states what SWECOV HOLDS; the flavored DB states what
# reg_meta can deliver. Where they disagree — a column held in an edition with no
# covering `variable_state` / `variable_alias_window` — the researcher hits
# "period outside validity" on data the steward has, and `extend-db`'s flavored
# validation now refuses to ship it (Y-115). For an SCB-export variable the repair
# is an upstream-error record in `reg_meta_build/curation/registers/scb/<slug>.toml`, so this command
# renders `[[errata.version]]` entries for undocumented editions and `[[errata.delivered]]`
# entries for omitted rows. An errata-created variable has no SCB row to clone and
# instead gets bounded `[[errata.column]]`/mapping inspection guidance. A miss on any
# OTHER provider cannot be an entry in that file at all (its loader refuses one),
# so it is a comment naming the curated surface carrying the window. Multi-period
# range/list tables are excluded from every section: their dates describe the
# records in the file, not each column's availability.
#
# The reading rules live in `reg_meta_build.inventory_coverage` — ONE
# implementation, so the worklist can never disagree with the gate that made it.


def cmd_errata(args: argparse.Namespace) -> None:
    from reg_meta_build.db import open_built_db
    from reg_meta_build.inventory import load_inventory
    from reg_meta_build.inventory_coverage import (
        coverage_misses,
        errata_worklist,
        skipped_tables_line,
        version_candidates,
    )

    inventory = load_inventory(args.out / "inventory.toml")
    conn = open_built_db(args.db)
    try:
        report = coverage_misses(conn, inventory)
    finally:
        conn.close()

    dest = args.out / "derived" / "errata_worklist.toml"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(errata_worklist(report), encoding="utf-8")
    print(f"wrote {dest}")
    print(
        f"  held column × edition pairs: {report.pairs} assessed,"
        f" {report.missed_pairs} with no catalog window"
        f" ({report.unresolved} mapping(s) not judged — coordinate unresolved)"
    )
    print(f"  {skipped_tables_line(report.skipped_tables)}")
    print(
        f"  version-missing: {len(version_candidates(report.misses))} "
        f"[[errata.version]] candidate(s)"
        f"  column-missing: {len(report.delivered_misses)} [[errata.delivered]] candidate(s)"
        f"  errata-column: {len(report.errata_column_misses)} miss(es) to inspect"
        f"  curated-window: {len(report.curated_misses)} non-scb miss(es)"
    )


# --- reporting ---------------------------------------------------------------


def cmd_normalize(args: argparse.Namespace) -> None:
    holdings = parse_inventory(args.csv)
    out = {
        f"{cat} / {det}" if det else cat: {
            "tables": len(h.tables),
            "columns": len(h.columns),
            "match_columns": len(h.match_columns),
            "periods": h.period_segments(),
        }
        for (cat, det), h in sorted(holdings.items())
    }
    json.dump(out, sys.stdout, ensure_ascii=False, indent=2, default=str)
    print()


def cmd_ground(args: argparse.Namespace) -> None:
    holdings = parse_inventory(args.csv)
    index = load_variant_index(args.db)
    non_catalog = _checked_spec(args, holdings).scope.non_catalog_categories
    print(f"{len(holdings)} (Category, Detail) pairs; {len(index)} catalog variants\n")
    for universe, label in (
        (True, "IN-UNIVERSE (SCB/SOS)"),
        (False, "RESIDUE (outside catalog universe)"),
    ):
        print(f"=== {label} ===")
        for (cat, det), h in sorted(holdings.items()):
            if (cat not in non_catalog) is not universe:
                continue
            pair = f"{cat} / {det}" if det else cat
            print(f"\n{pair}  [{len(h.tables)} tables, {len(h.match_columns)} cols]")
            print(f"  periods: {json.dumps(h.period_segments(), ensure_ascii=False)}")
            if not universe:
                print(f"  residue: {non_catalog[cat]}")
            for c in ground_pair(h, index):
                flag = (
                    " <<< unexpected for residue"
                    if not universe and c.csv_share >= 0.5
                    else ""
                )
                print(
                    f"    {c.matched:4d}/{c.csv_total:<4d} ({c.csv_share:4.0%} of csv,"
                    f" {c.variant_share:4.0%} of variant)  {c.variant.coordinate}"
                    f"  [{c.variant.register_name} / {c.variant.variant_name}]{flag}"
                )
        print()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv",
        type=Path,
        required=True,
        help="holdings inventory CSV (SWECOV_variables_full_<date>.csv)",
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument(
        "--spec",
        type=Path,
        default=SPEC_DIR,
        help="steward spec directory: scope.toml + series/*.toml (default: %(default)s)",
    )
    # The scratch dir every intermediate stage writes into and the next stage
    # reads from: `derived/` for the enrichment chain, `providers/` for flavor.
    scratch = argparse.ArgumentParser(add_help=False)
    scratch.add_argument(
        "--out",
        type=Path,
        required=True,
        help="scratch output dir shared by enrich, globals, grafts and flavor;"
        " each reads the previous stage's files under its derived/",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("normalize", help="dump normalized holdings as JSON")
    sub.add_parser("ground", help="propose the register mapping by column overlap")
    enrich_p = sub.add_parser(
        "enrich",
        parents=[scratch],
        help="join delivery documentation; write <--out>/derived/holdings_enriched.json",
    )
    enrich_p.add_argument(
        "--docs-root",
        type=Path,
        required=True,
        help="SWECOV delivery-docs root whose children are the per-agency"
        " folders (SCB/, Socialstyrelsen/, Fohm/, ...)",
    )
    sub.add_parser(
        "globals",
        parents=[scratch],
        help="mine documented holdings for global catalog improvements;"
        " write <--out>/derived/global_enrichment.json",
    )
    grafts_p = sub.add_parser(
        "grafts",
        parents=[scratch],
        help="variant-tag the gapfill columns; write <--out>/derived/column_candidates.toml"
        " (candidate reg_meta_build/curation/registers/scb/<slug>.toml [[errata.column]] entries)",
    )
    grafts_p.add_argument(
        "--lisa-docs-dir",
        type=Path,
        required=True,
        help="markdown register docs root whose children are register dirs"
        " (<register>/*.md, e.g. lisa/); a column documented there is"
        " tagged source = scb-docs",
    )
    sub.add_parser(
        "flavor",
        parents=[scratch],
        help="emit steward-only curated-provider TOMLs to <--out>/providers/ +"
        " slug pins to reg_meta_build/fqid_slugs/<steward>/ (#421)",
    )
    inventory_p = sub.add_parser(
        "inventory",
        help="emit the delivery inventory.toml (reality = holdings CSV, policy ="
        " the --spec steward spec) against a mapping DB built from --base-db plus"
        " --providers, then check curated types against --storage-csv",
    )
    inventory_p.add_argument(
        "--base-db",
        type=Path,
        required=True,
        help="the global reg_meta.db the candidate pins (global_db_sha256)",
    )
    inventory_p.add_argument(
        "--providers",
        type=Path,
        required=True,
        help="the candidate's providers/ directory (curated-provider TOMLs)",
    )
    inventory_p.add_argument(
        "--storage-csv",
        type=Path,
        required=True,
        help="the steward's storage-schema export (table, column, SQL type)",
    )
    inventory_p.add_argument(
        "--out",
        type=Path,
        required=True,
        help="scratch output dir for inventory.toml and the derived/ worklists;"
        " copy inventory.toml into the candidate's policy/ to accept it",
    )
    errata_p = sub.add_parser(
        "errata",
        help="write <--out>/derived/errata_worklist.toml: candidate reg_meta_build/curation/registers/scb/<slug>.toml entries"
        " for the columns the generated inventory holds in editions the FLAVORED"
        " --db has no window for",
    )
    errata_p.add_argument(
        "--out",
        type=Path,
        required=True,
        help="the inventory step's scratch --out (holds inventory.toml); the"
        " worklist is written under its derived/",
    )
    args = parser.parse_args()
    {
        "normalize": cmd_normalize,
        "ground": cmd_ground,
        "enrich": cmd_enrich,
        "globals": cmd_globals,
        "grafts": cmd_grafts,
        "flavor": cmd_flavor,
        "inventory": cmd_inventory,
        "errata": cmd_errata,
    }[args.command](args)


if __name__ == "__main__":
    main()
