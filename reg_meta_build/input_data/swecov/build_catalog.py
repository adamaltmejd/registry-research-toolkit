#!/usr/bin/env python3
"""Build the SWECOV steward catalog from SWECOV's holdings inventory CSV.

The inventory (``SWECOV_variables_2025-12-11.csv``) has one row per physical
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
  attribution) from the SWECOV-internal delivery variable lists (see
  README.md in this directory) onto the holdings, apply the curated MAPPING,
  and emit ``derived/holdings_enriched.json`` — the single derived artifact
  every downstream consumer (steward catalog emission, reg_meta flavor
  ingest, alias curation) reads.

This script is TRACKED (force-added 2026-09-02) and maintainer-run: it lives
next to its confidential inputs, which stay untracked along with everything it
writes to `derived/`. Only its committed catalog outputs leave this directory.

Run from this directory::

    python3 build_catalog.py enrich
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
from pathlib import Path

DEFAULT_CSV = Path(__file__).with_name("SWECOV_variables_2025-12-11.csv")
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
NON_CATALOG_CATEGORIES: dict[str, str] = {
    "Arbetsförmedlingen": "Arbetsförmedlingen delivery (AMS_*), not SCB/SOS",
    "FOHM": "Folkhälsomyndigheten delivery (FHM_*)",
    "Försäkringskassan": "Försäkringskassan delivery (FK_*)",
    "Högskoleprovet": "Umeå university delivery (UMU_*)",
    "IAF": "Inspektionen för arbetslöshetsförsäkringen delivery (IFA_*)",
    "Inera/1177": "Inera delivery",
    "Kolorektalcancer": "Quality register (SCRCR)",
    "Korttidsarbete": "Tillväxtverket delivery (KTA_*)",
    "Läkemedelsverket": "Läkemedelsverket delivery (LV_*)",
    "Military enlistment": "Pliktverket / Riksarkivet delivery",
    "Pandemrix vaccinations": "Regional health-care deliveries",
    "Primary care": "Regional health-care deliveries",
    "Quality register": "Quality registers (Graviditetsregistret, NDR)",
    "SCB": "LopNr key-change crosswalk table, not register data",
    "SOS Alarm": "SOS Alarm delivery",
    "Skatteverket": "Skatteverket delivery (SKV_*)",
    "Swedbank": "Swedbank delivery",
    "SÄBO": "Municipal / county deliveries",
    "Telia": "Telia delivery",
}


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
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        if header[:3] != ["Category", "Detail", "Table"]:
            raise SystemExit(f"unexpected header: {header[:3]}")
        for row in reader:
            if len(row) < 3:  # blank line / short row: nothing to unpack
                continue
            category, detail, table = (cell.strip() for cell in row[:3])
            if not category:
                continue
            holding = holdings.setdefault((category, detail), Holding(category, detail))
            holding.tables.append(table)
            # Empty cells appear mid-row, not just as trailing padding —
            # filter everywhere.
            row_cols = {c.strip() for c in row[3:] if c.strip()}
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
# Evidence notes ride along so the emitted catalog can cite them.

MAPPING: dict[tuple[str, str], dict] = {
    ("AGI", "ARB Individ"): {"status": "mapped", "to": "scb/agi/individuppgifter-agi"},
    ("AGI", "SOC Individ"): {
        "status": "mapped",
        "to": "scb/agi/individuppgifter-agi",
        "note": "ERSATTNINGS_KOD/BELOPP1-4 are wide pivots of the kod+belopp concepts",
    },
    ("AGI", "Huvud"): {
        "status": "flavor",
        "note": "employer-level AGI declaration header; register absent from reg_meta",
    },
    ("AKU", ""): {
        "status": "split",
        "to": [
            ("stem:AKU_", "scb/aku/aku-april-2005-dec-2020", "2015-2020 holdings"),
            ("stem:AKU_RL_", "scb/aku/aku-januari-2021", "2021-2024 holdings"),
            ("stem:AKU_Corona_", None, "flavor: SWECOV covid add-on questions"),
        ],
    },
    ("Elevregistret", "Betyg Ak6"): {
        "status": "mapped",
        "to": "scb/grundskola-betyg-ak6/_default",
    },
    ("Elevregistret", "Elever gymnasiet"): {
        "status": "mapped",
        "to": "scb/gymnasieskola-elever/_default",
    },
    ("Elevregistret", "Grund, förskola, fritidshem"): {
        "status": "mapped",
        "to": "scb/grundskola-elever/individregister",
        "note": "combined table; förskola/fritidshem-only columns go to flavor graft",
    },
    ("Elevregistret", "Gymnasieskola avgangar"): {
        "status": "split",
        "to": [
            (
                "stem:GymnasieskolaAvg_Individ_",
                "scb/gymnasieskola-betyg/slutbetyg",
                "75% grounded",
            ),
            (
                "stem:GymnasieskolaAvg_Kurs_",
                "scb/gymnasieskola-betyg/kursbetyg",
                "tie broken semantically",
            ),
            (
                "stem:GymnasieskolaAvg_Amne_",
                None,
                "flavor: 1985-1996 ämne shape ungrounded",
            ),
        ],
    },
    ("Elevregistret", "Kursprov"): {
        "status": "mapped",
        "to": "scb/gymnasieskola-prov/_default",
    },
    ("Elevregistret", "School code key"): {
        "status": "lookup",
        "note": "skolkod<->skolenhetskod crosswalk",
    },
    ("Elevregistret", "Slutbetyg Ak9"): {
        "status": "mapped",
        "to": "scb/grundskola-ak9/grundskolan-betyg-ak9",
    },
    ("Elevregistret", "Ämnesprov Ak3"): {
        "status": "mapped",
        "to": "scb/grundskola-prov-ak3/_default",
    },
    ("Elevregistret", "Ämnesprov Ak6"): {
        "status": "mapped",
        "to": "scb/grundskola-prov-ak6/_default",
    },
    ("Elevregistret", "Ämnesprov Ak9"): {
        "status": "split",
        "to": [
            (
                "period:2003-2012",
                "scb/grundskola-prov-ak9/amne-sv-eng-ma",
                "variant validity window",
            ),
            (
                "period:2013-",
                "scb/grundskola-prov-ak9/amne-sv-eng-ma-no-so",
                "variant validity window",
            ),
        ],
    },
    ("Firms", "Företagens ekonomi"): {
        "status": "mapped",
        "to": "scb/fek/fek-slutdata",
        "note": "weak column grounding (18%); SWECOV FE extract is heavily derived — most columns to flavor graft",
    },
    ("Firms", "Företagsdatabasen"): {
        "status": "split",
        "to": [
            (
                "stem:FDB_AE_",
                "scb/fdb/arbetsstalleenheter",
                "incl. _Ng_ näringsgren extras",
            ),
            ("stem:FDB_AST_", "scb/fdb/administrativa", "7/7 grounded"),
        ],
    },
    ("Geografidatabasen", "Arbetsställen"): {
        "status": "flavor",
        "graft": "scb/gdb",
        "note": "master-list Källa=Geo.databas for every column -> graft onto scb/gdb;"
        " reg_meta gdb variants don't enumerate the DeSO/Ruta250 delivery columns",
    },
    ("Geografidatabasen", "Individer"): {
        "status": "flavor",
        "graft": "scb/gdb",
        "note": "as Arbetsställen",
    },
    ("Geografidatabasen", "Skolor"): {
        "status": "flavor",
        "graft": "scb/gdb",
        "note": "as Arbetsställen",
    },
    ("Högskoleregistret", ""): {
        "status": "split",
        "to": [
            ("stem:Hreg_Reg_", "scb/hreg/grundutbildning-1993", "42% grounded"),
            ("stem:Hreg_Poang_", "scb/hreg/grundutbildning-poang", "65% grounded"),
            ("stem:Hreg_Sok_", "scb/hreg/sokande-grund-avancerad", "58% grounded"),
            ("stem:Hreg_Examina_", "scb/hreg/grundutbildning-examina", "33% grounded"),
            ("stem:Hreg_H_", None, "flavor: klartext lookup tables"),
            ("stem:klartext_utbildning", None, "flavor: klartext lookup"),
        ],
    },
    ("Industrins varuproduktion", ""): {
        "status": "mapped",
        "to": "scb/ivp/industrins-varuproduktion",
        "note": "6/7 tie with ivp/returravara broken semantically (IVP_ = main survey)",
    },
    ("Industrins varuproduktion", "Text"): {
        "status": "lookup",
        "note": "VaraText code lookup",
    },
    ("Inkomst- och taxeringsregistret", ""): {
        "status": "mapped",
        "to": "scb/iot/individer-och-dodsbon",
        "note": "thin 7-column extract",
    },
    ("Konjunkturstatistik sjuklöner", ""): {
        "status": "mapped",
        "to": "scb/ksju/sjukfranvaro-sjukloneperioden",
        "note": "ERSDAG/ERSK/FALLK etc. are wide pivots",
    },
    ("Konkurser", ""): {
        "status": "mapped",
        "to": "scb/konkurser/konkurser-offentliga-ackord",
    },
    ("LISA", "Arbetsställe"): {"status": "mapped", "to": "scb/lisa/arbetsstallen"},
    ("LISA", "Företag"): {"status": "mapped", "to": "scb/lisa/foretag"},
    ("LISA", "Individ"): {
        "status": "split",
        "to": [
            (
                "period:1990-2009",
                "scb/lisa/individer-16plus",
                "variant validity window",
            ),
            ("period:2010-", "scb/lisa/individer-15plus", "variant validity window"),
        ],
    },
    ("Lärarregistret", ""): {"status": "mapped", "to": "scb/lararreg/tjansteregistret"},
    ("Momsregistret", ""): {
        "status": "mapped",
        "to": "scb/moms/momsdeklarationsregistret",
    },
    ("RAMS", ""): {"status": "mapped", "to": "scb/rams/jobbregistret"},
    ("RAMS", "ASTRA"): {"status": "mapped", "to": "scb/rams/arbetsstallen"},
    ("RTB", "Adress särskilt boende"): {
        "status": "flavor",
        "note": "SWECOV-specific SÄBO address extract",
    },
    ("RTB", "Döda"): {"status": "mapped", "to": "scb/rtb/doda"},
    ("RTB", "Emigranter/Immigranter"): {
        "status": "split",
        "to": [
            ("stem:emigranter_", "scb/rtb/utvandringar", "3/3 grounded"),
            ("stem:immigranter_", "scb/rtb/invandringar", "mirror of utvandringar"),
        ],
    },
    ("RTB", "Familj"): {"status": "mapped", "to": "scb/rtb/familjer-fran-1998"},
    ("RTB", "Flergenerationsregistret"): {
        "status": "split",
        "to": [
            (
                "stem:FlerGen_Bioforaldrar_",
                "scb/flergenreg/folkbokforda-biologiska",
                "3/3; semantic tiebreak",
            ),
            (
                "stem:FlerGen_Adopforaldrar_",
                "scb/flergenreg/folkbokforda-adoptivforaldrar",
                "3/3",
            ),
        ],
    },
    ("RTB", "Födelseuppgifter"): {
        "status": "flavor",
        "graft": "scb/rtb",
        "note": "Källa=RTB for FodelseLan/Fodelseland/UtlSvBakg -> graft onto scb/rtb"
        " (SCB metadata gap, not SWECOV construction); EU groupings are SWECOV lookups",
    },
    ("RTB", "HB"): {
        "status": "mapped",
        "to": "scb/hushallens-boende/individer",
        "note": "HUSHALLSID_YYYY are year-pivots of HUSHALLSID",
    },
    ("RTB", "HP"): {
        "status": "mapped",
        "to": "scb/rtb/hushall",
        "note": "HUSHALLSID_YYYY year-pivots as HB",
    },
    ("RTB", "Inrikes flyttningar"): {
        "status": "mapped",
        "to": "scb/rtb/inrikes-flyttningar",
        "note": "RTB_SaBo_InrFlyttFlode extras to flavor",
    },
    ("RTB", "Land"): {"status": "lookup", "note": "country-code EU-grouping lookup"},
    ("RTB", "Partner"): {
        "status": "flavor",
        "graft": "scb/rtb",
        "note": "Källa=RTB/Hushållsreg. for Famstall/HushallsStallning/partner-lopnr;"
        " FST90-97 wide pivots from familjer-1990-1997 -> derived link table grafted on scb/rtb",
    },
    ("RTB", "Population"): {
        "status": "flavor",
        "note": "Källa empty for IndexPop/Partner/LopNrByte etc. -> SWECOV-constructed population spine",
    },
    ("RTB", "PostNr"): {
        "status": "flavor",
        "note": "postnr-per-person extract, 2 columns",
    },
    ("RTB", "RTB"): {"status": "mapped", "to": "scb/rtb/folkbokforda-personer"},
    ("SCB", ""): {"status": "lookup", "note": "LopNrByte key-change crosswalk"},
    ("STATIV", ""): {"status": "mapped", "to": "scb/stativ/_default"},
    ("Sjukfränvaro under sjuklöneperioden", ""): {
        "status": "mapped",
        "to": "scb/anst/sus",
    },
    ("Socialstyrelsen", "Barn"): {"status": "mapped", "to": "sos/bu/_default"},
    ("Socialstyrelsen", "Cancerregistret"): {"status": "mapped", "to": "sos/can/can"},
    ("Socialstyrelsen", "Dödsorsaksregistret"): {
        "status": "mapped",
        "to": "sos/dors/dors",
    },
    ("Socialstyrelsen", "Ekonomiskt bistand"): {
        "status": "mapped",
        "to": "sos/ekb/ekb-manad",
    },
    ("Socialstyrelsen", "Intensivvårdsregistret"): {
        "status": "flavor",
        "note": "SIR is a quality register routed via SoS; not in the SOS catalog",
    },
    ("Socialstyrelsen", "Kommunal hälso- och sjukvård"): {
        "status": "mapped",
        "to": "sos/hsl/hsl",
    },
    ("Socialstyrelsen", "LSS"): {"status": "mapped", "to": "sos/lss/_default"},
    ("Socialstyrelsen", "Läkemedelsregistret"): {
        "status": "mapped",
        "to": "sos/lmed/lmed",
        "note": "ATC/FORPDDD live in lmed-vara; SWECOV table is a denormalized join",
    },
    ("Socialstyrelsen", "Slutenvård"): {
        "status": "mapped",
        "to": "sos/par/par-sv",
        "note": "DIA1..30/EKOD1.. wide pivots of long-format concepts; SV_comorb derived",
    },
    ("Socialstyrelsen", "Socialtjänst (Äldrevård)"): {
        "status": "mapped",
        "to": "sos/sol/sol",
    },
    ("Socialstyrelsen", "Öppenvård"): {
        "status": "mapped",
        "to": "sos/par/par-ov",
        "note": "wide pivots as Slutenvård",
    },
    ("Survey", "Distansutbildning"): {
        "status": "split",
        "to": [
            (
                "stem:Distansutb_grund_",
                "scb/utbildningsanalyser/distansundervisning-grundskola",
                "100%",
            ),
            (
                "stem:Distansutb_gymn_",
                "scb/utbildningsanalyser/distansundervisning-gymnasieskolan",
                "100%",
            ),
        ],
    },
    ("Survey", "FOU"): {
        "status": "mapped",
        "to": "scb/fou/foretagssektorn",
        "note": "wave columns mostly undocumented in reg_meta -> flavor graft",
    },
    ("Survey", "IT"): {
        "status": "mapped",
        "to": "scb/it-anvandning/it-anvandning-i-foretag",
        "note": "2008-2012 waves poorly enumerated in reg_meta",
    },
    ("Survey", "IT Mikro"): {
        "status": "mapped",
        "to": "scb/it-anvandning/it-anvandning-i-foretag",
    },
    ("Survey", "IT Stora"): {
        "status": "mapped",
        "to": "scb/it-anvandning/it-anvandning-i-foretag",
    },
    ("Survey", "Innovation"): {
        "status": "mapped",
        "to": "scb/innovation-foretag/_default",
        "note": "CIS wave drift; unmatched wave items -> flavor graft",
    },
    ("Utbildningsregistret", ""): {
        "status": "mapped",
        "to": "scb/ureg/personens-hogsta-utbildning",
    },
    ("Utrikeshandel", "Tjänster"): {
        "status": "flavor",
        "note": "no tjänster register in reg_meta",
    },
    ("Utrikeshandel", "Varor"): {
        "status": "mapped",
        "to": "scb/utrikeshandel/varu-landfordelat-intrastat",
        "note": "exact tie with landfordelad-extrastat; SWECOV UHV is the combined flow — needs sign-off",
    },
}


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
    """All extractable documentation sources -> one index. Order matters:
    newest master/SoS lists first (first write wins per scope+column)."""
    index: DocIndex = {}
    sources: list[tuple[str, object]] = [
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
    base = args.csv.parent
    holdings = parse_inventory(args.csv)
    print("extracting documentation sources:", file=sys.stderr)
    index = build_doc_index(base)

    out: dict[str, dict] = {}
    n_cols = n_doc = 0
    per_cat: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    for (cat, det), h in sorted(holdings.items()):
        mapping = MAPPING.get((cat, det))
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
            "mapping": mapping
            or {"status": "residue", "note": NON_CATALOG_CATEGORIES.get(cat, "")},
            "columns": cols,
        }

    dest = base / "derived" / "holdings_enriched.json"
    dest.parent.mkdir(exist_ok=True)
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
    regs: set[str] = set()
    to = mapping.get("to")
    if isinstance(to, str):
        regs.add("/".join(to.split("/")[:2]))
    elif isinstance(to, list):
        for _sel, coord, _note in to:
            if coord:
                regs.add("/".join(coord.split("/")[:2]))
    if mapping.get("graft"):
        regs.add(mapping["graft"])
    return regs


# SoS delivery lists document SOS register facts without a Källa column.
_SOS_LIST_PROVENANCE = re.compile(r"^Variabellista_")


def cmd_globals(args: argparse.Namespace) -> None:
    enriched_path = args.csv.parent / "derived" / "holdings_enriched.json"
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

    dest = args.csv.parent / "derived" / "global_enrichment.json"
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


# --- grafts: variant-tag the gapfill graft candidates ------------------------
#
# `globals` emits gapfill rows keyed only on `regs[0]` (the register), dropping
# the variant. A graft mints a variable_state, which needs a register_variant.
# This stage recovers it: for a `mapped` holding every column → the single
# variant; for a `split` holding each column → the variant of the table(s) it
# lives in (selector = table-name stem), and a column whose stem maps to a
# `None` coordinate is steward FLAVOR (e.g. AKU_Corona_ covid add-ons), excluded
# from the global graft set. Also applies the graft content filters (drop the
# project-specific P<n>_ pseudonym columns, decode/klartext label columns → #373,
# and rows with no real documentation). Output: derived/graft_candidates.json.

_GRAFT_DROP = re.compile(r"klartext|_txt$|beskrivning|, *text$", re.IGNORECASE)
_PROJECT_PREFIX = re.compile(r"^P\d+_", re.IGNORECASE)
# Registers whose delivered columns are a privacy AGGREGATION, not research
# variables → steward flavor, never global (maintainer call 2026-06-14): GDB's
# 250m/1000m grid coordinates are Geografidatabasen's pseudonymized location
# (exact point never delivered), the spatial analogue of a LopNr.
_FLAVOR_REGISTERS = frozenset({"scb/gdb"})


def _split_key(key: str) -> tuple[str, str]:
    parts = key.split(" / ", 1)
    return (parts[0], parts[1] if len(parts) > 1 else "")


def _resolve_graft_variant(
    mapping: dict, csv_holding, column: str, target_reg: str, kalla: str = ""
):
    """Variant slug for a gapfill column under its holding's mapping, or one of
    the sentinels 'FLAVOR' / 'AMBIGUOUS' / 'UNRESOLVED'. ``target_reg`` is the
    provider/register the gapfill row was attributed to (a split holding can
    target several registers)."""
    to = mapping.get("to")
    if isinstance(to, str) and to.count("/") == 2:
        prov, reg, var = to.split("/")
        return var if f"{prov}/{reg}" == target_reg else "UNRESOLVED"
    if isinstance(to, list):
        # FLAVOR by source: a column's `kalla` (delivery source) matching a
        # selector whose coordinate is None is steward-flavor regardless of which
        # physical table carries it — covid add-ons (kalla=AKU_Corona) ride the
        # shared AKU_ tables, so the table-stem check below would miss them.
        kn = re.sub(r"[^A-Z0-9]", "", (kalla or "").upper())
        if kn:
            for sel, coord, _note in to:
                if coord is None:
                    stem = re.sub(r"[^A-Z0-9]", "", sel.replace("stem:", "").upper())
                    # kalla must START WITH the flavor stem (AKUCORONA ⊃ AKU_Corona_);
                    # NOT the reverse, or a register kalla (HREG) would match a
                    # longer flavor sub-stem (Hreg_H_ → HREGH).
                    if len(stem) >= 4 and kn.startswith(stem):
                        return "FLAVOR"
        tables = (
            [t for t, cols in csv_holding.table_columns.items() if column in cols]
            if csv_holding
            else []
        )
        found: set[str] = set()
        for t in tables:
            for sel, coord, _note in to:
                stem = sel.replace("stem:", "")
                if t.upper().startswith(stem.upper()):
                    if coord is None:
                        found.add("FLAVOR")
                    elif (
                        coord.count("/") == 2
                        and "/".join(coord.split("/")[:2]) == target_reg
                    ):
                        found.add(coord.split("/")[2])
                    break
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


def cmd_grafts(args: argparse.Namespace) -> None:
    base = args.csv.parent
    enriched = json.loads((base / "derived" / "holdings_enriched.json").read_text())
    ge = json.loads((base / "derived" / "global_enrichment.json").read_text())
    csv_holdings = parse_inventory(args.csv)
    variant_index = load_variant_index(args.db)

    # Docs cross-reference (the discriminator): a column documented in reg_meta's
    # ingested SCB docs is a CANONICAL column the machine metadata missed (#400) —
    # route it there, NOT to the steward graft list (wrong provenance). Only
    # registers with ingested docs (lisa) can be discriminated; others fall
    # through as grafts.
    docs_root = base.parent.parent / "docs"
    doc_cols: dict[str, set[str]] = {}
    if docs_root.is_dir():
        for d in docs_root.iterdir():
            if d.is_dir():
                doc_cols[d.name] = {
                    _norm_alnum(p.stem)
                    for p in d.glob("*.md")
                    if not p.stem.startswith("_")
                }

    # register -> [(mapping, csv_holding, set(column names in this holding))]
    by_reg: dict[str, list] = defaultdict(list)
    for key, pair in enriched.items():
        csvh = csv_holdings.get(_split_key(key))
        names = {c["name"] for c in pair["columns"]}
        for reg in _mapping_registers(pair["mapping"]):
            by_reg[reg].append((pair["mapping"], csvh, names))

    grafts: list[dict] = []
    flavor: list[dict] = []
    unresolved: list[dict] = []
    doc_documented: list[dict] = []
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
        if _norm_alnum(col) in doc_cols.get(reg.split("/")[-1], set()):
            # Canonical SCB column the machine metadata missed but the docs have
            # (e.g. lisa Ssyk4_J16) — belongs to the reg_meta completeness fix
            # (#400), not a steward graft.
            doc_documented.append({"register": reg, "column": col, "description": desc})
            continue
        variant = "UNRESOLVED"
        for mapping, csvh, names in by_reg.get(reg, []):
            if col not in names:
                continue
            variant = _resolve_graft_variant(
                mapping, csvh, col, reg, g.get("kalla", "")
            )
            if variant != "UNRESOLVED":
                break
        if variant == "UNRESOLVED":
            # Curated mapping didn't place it — fall back to grounding the
            # column's own table(s) (and flavor-only holdings → FLAVOR).
            variant = _ground_fallback(col, reg, csv_holdings, enriched, variant_index)
        row = {
            "register": reg,
            "variant": variant,
            "column": col,
            "description": desc,
            "data_type": g.get("data_type", ""),
            "kalla": g.get("kalla", ""),
            "provenance": g.get("provenance", ""),
        }
        if reg in _FLAVOR_REGISTERS:
            variant = "FLAVOR"
            row["variant"] = "FLAVOR"
        if variant == "FLAVOR":
            flavor.append(row)
        elif variant in ("UNRESOLVED", "AMBIGUOUS"):
            unresolved.append(row)
        else:
            grafts.append(row)

    dest = base / "derived" / "graft_candidates.json"
    dest.write_text(
        json.dumps(
            {
                "grafts": sorted(
                    grafts, key=lambda r: (r["register"], r["variant"], r["column"])
                ),
                "flavor_excluded": flavor,
                "unresolved": unresolved,
                "doc_documented": doc_documented,
                "content_dropped": dropped,
            },
            ensure_ascii=False,
            indent=1,
        )
    )
    print(f"wrote {dest}")
    print(f"  grafts (variant-tagged):  {len(grafts)}")
    print(f"  flavor_excluded:          {len(flavor)}")
    print(f"  unresolved/ambiguous:     {len(unresolved)}")
    print(f"  doc_documented (→ #400):  {len(doc_documented)}")
    print(f"  content_dropped:          {len(dropped)}")
    by_dest: dict[tuple[str, str], int] = defaultdict(int)
    for r in grafts:
        by_dest[(r["register"], r["variant"])] += 1
    for (reg, var), n in sorted(by_dest.items()):
        print(f"    {reg}/{var}: {n}")


# --- flavor: emit the extend-db steward inventory JSON -----------------------
#
# The steward-FLAVORED slice (#421, #365 PR2): SWECOV holdings with NO global
# home, projected into the `extend-db` inventory JSON contract (see
# reg_meta_build/DESIGN.md → "Steward-flavored DB — extend-db"). Scope follows
# "what a fact is ABOUT": only steward-private content lands here. Excluded and
# routed elsewhere by the curated MAPPING / dispositions below:
#   - mapped/split holdings           -> the GLOBAL build (already in reg_meta)
#   - graft-keyed flavor holdings     -> the global graft track (variable_grafts)
#   - public-agency residue           -> global providers, separate effort (#422)
#   - canonical-SCB gaps              -> reg_meta doc/metadata completeness (#400/#422)
#   - lookup tables                   -> SWECOV-side helpers, not catalog rows
# Membership decided with the maintainer (2026-06-15, "Principled mid"): the
# flavor carries commercial (Swedbank/Telia), regional/municipal, the national
# quality registers, and SWECOV-constructed RTB extracts. Providers are modeled
# per real-world source organization (per-org), one provider slug each.

# (enriched-JSON key, provider_slug, provider_name, register_key, register_name,
#  variant_key, variant_slug, variant_name). Single-variant registers use the
#  `_default` variant slug (matches the global catalog convention) and repeat the
#  register name as the variant name; a register named by several entries
#  (Skatteverket's schemes, Tillväxtverket's delivery models) accretes one
#  variant per entry, each naming its own delivery — entries share a register
#  only where they DELIVER that one register's variables, so deliveries with
#  disjoint schemas are separate registers (reg_meta/DESIGN.md → "Why the
#  variant is a coordinate, not an identity level"). Exactly one entry names a
#  given (provider, register, variant) — a second is fatal in `cmd_flavor`.
_FLAVOR_DISPOSITION: list[tuple[str, str, str, str, str, str, str, str]] = [
    # Commercial deliveries — no global home by construction.
    (
        "Swedbank",
        "swedbank",
        "Swedbank AB",
        "konsumtion",
        "Konsumtionsstatistik",
        "_default",
        "_default",
        "Konsumtionsstatistik",
    ),
    (
        "Telia",
        "telia",
        "Telia Company AB",
        "mobilitet",
        "Mobilitetsdata",
        "_default",
        "_default",
        "Mobilitetsdata",
    ),
    # Regional primary care (region-owned, not a national register).
    (
        "Primary care/Skane",
        "region-skane",
        "Region Skåne",
        "primarvard",
        "Primärvård",
        "_default",
        "_default",
        "Primärvård",
    ),
    (
        "Primary care/Stockholm",
        "region-stockholm",
        "Region Stockholm",
        "primarvard",
        "Primärvård",
        "_default",
        "_default",
        "Primärvård",
    ),
    (
        "Primary care/VGR - Primärvård",
        "vgr",
        "Västra Götalandsregionen",
        "primarvard",
        "Primärvård",
        "_default",
        "_default",
        "Primärvård",
    ),
    (
        "Primary care/VGR - Diagnoser",
        "vgr",
        "Västra Götalandsregionen",
        "primarvard-diagnoser",
        "Primärvård – diagnoser",
        "_default",
        "_default",
        "Primärvård – diagnoser",
    ),
    # Municipal special-housing (SÄBO) deliveries.
    (
        "SÄBO/Adresses",
        "sabo",
        "Kommunala SÄBO-leveranser",
        "adresser",
        "SÄBO-adresser",
        "_default",
        "_default",
        "SÄBO-adresser",
    ),
    (
        "SÄBO/Patients",
        "sabo",
        "Kommunala SÄBO-leveranser",
        "patienter",
        "SÄBO-patienter",
        "_default",
        "_default",
        "SÄBO-patienter",
    ),
    # Pandemrix vaccination deliveries — one provider per delivering region.
    (
        "Pandemrix vaccinations/Region Dalarna",
        "region-dalarna",
        "Region Dalarna",
        "pandemrix-vaccinationer",
        "Pandemrix-vaccinationer",
        "_default",
        "_default",
        "Pandemrix-vaccinationer",
    ),
    (
        "Pandemrix vaccinations/Region Gävleborg",
        "region-gavleborg",
        "Region Gävleborg",
        "pandemrix-vaccinationer",
        "Pandemrix-vaccinationer",
        "_default",
        "_default",
        "Pandemrix-vaccinationer",
    ),
    (
        "Pandemrix vaccinations/Region Jönköping",
        "region-jonkoping",
        "Region Jönköping",
        "pandemrix-vaccinationer",
        "Pandemrix-vaccinationer",
        "_default",
        "_default",
        "Pandemrix-vaccinationer",
    ),
    (
        "Pandemrix vaccinations/Region Kalmar Län",
        "region-kalmar",
        "Region Kalmar län",
        "pandemrix-vaccinationer",
        "Pandemrix-vaccinationer",
        "_default",
        "_default",
        "Pandemrix-vaccinationer",
    ),
    (
        "Pandemrix vaccinations/Region Kronoberg",
        "region-kronoberg",
        "Region Kronoberg",
        "pandemrix-vaccinationer",
        "Pandemrix-vaccinationer",
        "_default",
        "_default",
        "Pandemrix-vaccinationer",
    ),
    (
        "Pandemrix vaccinations/Region Norrbotten",
        "region-norrbotten",
        "Region Norrbotten",
        "pandemrix-vaccinationer",
        "Pandemrix-vaccinationer",
        "_default",
        "_default",
        "Pandemrix-vaccinationer",
    ),
    (
        "Pandemrix vaccinations/Region Uppsala",
        "region-uppsala",
        "Region Uppsala",
        "pandemrix-vaccinationer",
        "Pandemrix-vaccinationer",
        "_default",
        "_default",
        "Pandemrix-vaccinationer",
    ),
    (
        "Pandemrix vaccinations/Region Värmland",
        "region-varmland",
        "Region Värmland",
        "pandemrix-vaccinationer",
        "Pandemrix-vaccinationer",
        "_default",
        "_default",
        "Pandemrix-vaccinationer",
    ),
    (
        "Pandemrix vaccinations/Region Västerbotten",
        "region-vasterbotten",
        "Region Västerbotten",
        "pandemrix-vaccinationer",
        "Pandemrix-vaccinationer",
        "_default",
        "_default",
        "Pandemrix-vaccinationer",
    ),
    (
        "Pandemrix vaccinations/Region Östergötland",
        "region-ostergotland",
        "Region Östergötland",
        "pandemrix-vaccinationer",
        "Pandemrix-vaccinationer",
        "_default",
        "_default",
        "Pandemrix-vaccinationer",
    ),
    # Inera / 1177 Vårdguiden — two REGISTERS: the deliveries are disjoint
    # schemas sharing only `PersonNr` (the rule above), and each is its own
    # enriched-holding key, so neither needs a `_FLAVOR_VARIANT_TABLES` selector.
    # The 1177 service names the PROVIDER.
    (
        "Inera/1177/Calls to 1177",
        "inera",
        "Inera AB / 1177 Vårdguiden",
        "samtal",
        "Samtal 1177",
        "_default",
        "_default",
        "Samtal 1177",
    ),
    (
        "Inera/1177/Ordered tests",
        "inera",
        "Inera AB / 1177 Vårdguiden",
        "bestallda-prover",
        "Beställda prover",
        "_default",
        "_default",
        "Beställda prover",
    ),
    # National quality registers — no SCB/SOS catalog home.
    (
        "Kolorektalcancer",
        "scrcr",
        "Svenska Kolorektalcancerregistret (SCRCR)",
        "kolorektalcancer",
        "Kolorektalcancerregistret",
        "_default",
        "_default",
        "Kolorektalcancerregistret",
    ),
    (
        "Quality register/Graviditetsregistret",
        "graviditetsregistret",
        "Graviditetsregistret",
        "graviditetsregistret",
        "Graviditetsregistret",
        "_default",
        "_default",
        "Graviditetsregistret",
    ),
    (
        "Quality register/NDR",
        "ndr",
        "Nationella Diabetesregistret (NDR)",
        "nationella-diabetesregistret",
        "Nationella diabetesregistret",
        "_default",
        "_default",
        "Nationella diabetesregistret",
    ),
    (
        "Socialstyrelsen/Intensivvårdsregistret",
        "sir",
        "Svenska Intensivvårdsregistret (SIR)",
        "intensivvardsregistret",
        "Svenska Intensivvårdsregistret",
        "_default",
        "_default",
        "Svenska Intensivvårdsregistret",
    ),
    (
        "SOS Alarm",
        "sos-alarm",
        "SOS Alarm Sverige AB",
        "ambulanslarm",
        "Ambulanslarm",
        "_default",
        "_default",
        "Ambulanslarm",
    ),
    # SWECOV-constructed columns on top of RTB — only the Källa-empty columns
    # survive _flavor_variables (the Källa=RTB ones are canonical, routed to the
    # global graft track). RTB/PostNr is intentionally NOT listed: its only
    # content column (`postnr`, Källa=RTB) is canonical RTB postnummer (→ graft),
    # leaving nothing steward-authored, so it would flavor to a bare linkage id.
    (
        "RTB/Population",
        "swecov",
        "SWECOV (konstruerade variabler)",
        "population",
        "Populationsspine (konstruerad)",
        "_default",
        "_default",
        "Populationsspine (konstruerad)",
    ),
    (
        "RTB/Adress särskilt boende",
        "swecov",
        "SWECOV (konstruerade variabler)",
        "adress-sarskilt-boende",
        "Adress särskilt boende",
        "_default",
        "_default",
        "Adress särskilt boende",
    ),
    # Skatteverket COVID-19 business-support delivery (SKV_*). The single
    # ("Skatteverket", "") holding carries 10 physical tables = 5 schemes; each
    # disposition entry below names a register/variant and `_FLAVOR_VARIANT_TABLES`
    # selects the physical table(s) whose columns it draws (the bespoke pandemic
    # extract is steward-only — P1105-keyed delivery, not a standing register).
    (
        "Skatteverket",
        "skatteverket",
        "Skatteverket",
        "omstallningsstod",
        "Omställningsstöd",
        "ansokt",
        "ansokt",
        "Ansökt",
    ),
    (
        "Skatteverket",
        "skatteverket",
        "Skatteverket",
        "omstallningsstod",
        "Omställningsstöd",
        "beviljat",
        "beviljat",
        "Beviljat",
    ),
    (
        "Skatteverket",
        "skatteverket",
        "Skatteverket",
        "omstallningsstod",
        "Omställningsstöd",
        "avslag",
        "avslag",
        "Avslag",
    ),
    (
        "Skatteverket",
        "skatteverket",
        "Skatteverket",
        "tillfalligt-anstand",
        "Tillfälligt anstånd med skatteinbetalning",
        "ansokt",
        "ansokt",
        "Ansökt",
    ),
    (
        "Skatteverket",
        "skatteverket",
        "Skatteverket",
        "tillfalligt-anstand",
        "Tillfälligt anstånd med skatteinbetalning",
        "beviljat",
        "beviljat",
        "Beviljat",
    ),
    (
        "Skatteverket",
        "skatteverket",
        "Skatteverket",
        "tillfalligt-anstand",
        "Tillfälligt anstånd med skatteinbetalning",
        "upphort",
        "upphort",
        "Upphört",
    ),
    (
        "Skatteverket",
        "skatteverket",
        "Skatteverket",
        "tillfalligt-anstand",
        "Tillfälligt anstånd med skatteinbetalning",
        "aterkallat",
        "aterkallat",
        "Återkallat",
    ),
    (
        "Skatteverket",
        "skatteverket",
        "Skatteverket",
        "arbetsgivardeklaration",
        "Uppgifter från arbetsgivardeklaration",
        "_default",
        "_default",
        "Uppgifter från arbetsgivardeklaration",
    ),
    (
        "Skatteverket",
        "skatteverket",
        "Skatteverket",
        "reducerad-egenavgift",
        "Reducerad egenavgift inkomstår 2020",
        "_default",
        "_default",
        "Reducerad egenavgift inkomstår 2020",
    ),
    (
        "Skatteverket",
        "skatteverket",
        "Skatteverket",
        "momsdeklaration",
        "Uppgifter från momsdeklaration",
        "_default",
        "_default",
        "Uppgifter från momsdeklaration",
    ),
    # Tillväxtverket korttidsarbete (KTA) — COVID-19 short-time-work support,
    # reported FROM Tillväxtverket (Källa = "Inrapporterat från Tillväxtverket",
    # kept in the flavor via _PROVENANCE_KALLA). One register, a variant per delivery
    # model: individmodellen (person-keyed) and transaktionsmodellen (ärende-keyed).
    (
        "Korttidsarbete/Individer",
        "tillvaxtverket",
        "Tillväxtverket",
        "korttidsarbete",
        "Korttidsarbete (KTA)",
        "individer",
        "individer",
        "Individer",
    ),
    (
        "Korttidsarbete/Transaktioner",
        "tillvaxtverket",
        "Tillväxtverket",
        "korttidsarbete",
        "Korttidsarbete (KTA)",
        "transaktioner",
        "transaktioner",
        "Transaktioner",
    ),
    # Arbetsförmedlingen (AMS) — jobseeker administrative delivery (AMS_*), no SCB/SOS
    # home (#443/#444 routed these to the flavor, not global; #365 needs them for 100%
    # coverage). The one ("Arbetsförmedlingen", "") holding bundles 3 distinct tables
    # (each with a 2021-vintage twin carrying identical columns) → 3 registers, split
    # by `_FLAVOR_VARIANT_TABLES`. All columns are Källa-empty (no master-list docs),
    # so definitions stay null — thin holdings-column coverage.
    (
        "Arbetsförmedlingen",
        "arbetsformedlingen",
        "Arbetsförmedlingen",
        "aktso",
        "Aktivitet och sökandekategori (AKTSO)",
        "_default",
        "_default",
        "Aktivitet och sökandekategori (AKTSO)",
    ),
    (
        "Arbetsförmedlingen",
        "arbetsformedlingen",
        "Arbetsförmedlingen",
        "insper",
        "Inskrivningsperioder (INSPER)",
        "_default",
        "_default",
        "Inskrivningsperioder (INSPER)",
    ),
    (
        "Arbetsförmedlingen",
        "arbetsformedlingen",
        "Arbetsförmedlingen",
        "sokatper",
        "Sökandekategoriperioder (SOKATPER)",
        "_default",
        "_default",
        "Sökandekategoriperioder (SOKATPER)",
    ),
    # IAF (Inspektionen för arbetslöshetsförsäkringen) — unemployment-insurance (a-kassa)
    # administrative delivery (IFA_*). One ("IAF", "") holding bundles 8 distinct tables
    # (Lev2/Lev3 access tiers and year-range vintages carry identical columns → collapse
    # into one register each via `_FLAVOR_VARIANT_TABLES`). All columns Källa-empty.
    (
        "IAF",
        "iaf",
        "Inspektionen för arbetslöshetsförsäkringen",
        "beslut",
        "Beslut",
        "_default",
        "_default",
        "Beslut",
    ),
    (
        "IAF",
        "iaf",
        "Inspektionen för arbetslöshetsförsäkringen",
        "diverse",
        "Diverse beslut",
        "_default",
        "_default",
        "Diverse beslut",
    ),
    (
        "IAF",
        "iaf",
        "Inspektionen för arbetslöshetsförsäkringen",
        "ersattningsperiod",
        "Ersättningsperioder",
        "_default",
        "_default",
        "Ersättningsperioder",
    ),
    (
        "IAF",
        "iaf",
        "Inspektionen för arbetslöshetsförsäkringen",
        "utbetalning",
        "Utbetalningar",
        "_default",
        "_default",
        "Utbetalningar",
    ),
    (
        "IAF",
        "iaf",
        "Inspektionen för arbetslöshetsförsäkringen",
        "medlemskap",
        "Medlemskap i a-kassa",
        "_default",
        "_default",
        "Medlemskap i a-kassa",
    ),
    (
        "IAF",
        "iaf",
        "Inspektionen för arbetslöshetsförsäkringen",
        "kassakod",
        "Kassakoder (a-kassor)",
        "_default",
        "_default",
        "Kassakoder (a-kassor)",
    ),
    (
        "IAF",
        "iaf",
        "Inspektionen för arbetslöshetsförsäkringen",
        "kassakortsvecka",
        "Kassakortsveckor",
        "_default",
        "_default",
        "Kassakortsveckor",
    ),
    (
        "IAF",
        "iaf",
        "Inspektionen för arbetslöshetsförsäkringen",
        "deltidsveckor",
        "Deltidsveckor",
        "_default",
        "_default",
        "Deltidsveckor",
    ),
]

# Table selectors for disposition entries whose holding bundles several physical
# tables under one (Category, Detail) key: (provider, register, variant) -> the
# SKV_* table name(s) whose columns that variant draws. Entries absent here draw
# the holding's full column union (the default for every other flavor provider).
_FLAVOR_VARIANT_TABLES: dict[tuple[str, str, str], tuple[str, ...]] = {
    ("skatteverket", "omstallningsstod", "ansokt"): ("SKV_omststod_ansokt",),
    ("skatteverket", "omstallningsstod", "beviljat"): ("SKV_omststod_beviljat",),
    ("skatteverket", "omstallningsstod", "avslag"): ("SKV_omststod_avslag",),
    ("skatteverket", "tillfalligt-anstand", "ansokt"): ("SKV_anstand_tillf_ansokan",),
    ("skatteverket", "tillfalligt-anstand", "beviljat"): (
        "SKV_anstand_tillf_beviljat",
    ),
    ("skatteverket", "tillfalligt-anstand", "upphort"): ("SKV_anstand_tillf_upphort",),
    ("skatteverket", "tillfalligt-anstand", "aterkallat"): ("SKV_anstand_tillf_aterk",),
    ("skatteverket", "arbetsgivardeklaration", "_default"): ("SKV_ag_skatt",),
    ("skatteverket", "reducerad-egenavgift", "_default"): ("SKV_reducerad_egenavg",),
    ("skatteverket", "momsdeklaration", "_default"): ("SKV_Moms",),
    # Arbetsförmedlingen (AMS): one holding, 3 registers; each pairs a base table with
    # its 2021-vintage twin (identical columns → deduped by norm_col into one set).
    ("arbetsformedlingen", "aktso", "_default"): (
        "AMS_HIST_AKTSO",
        "AMS_HIST_AKTSO_2021_20230726",
    ),
    ("arbetsformedlingen", "insper", "_default"): (
        "AMS_INSPER",
        "AMS_INSPER_2021_20230726",
    ),
    ("arbetsformedlingen", "sokatper", "_default"): (
        "AMS_SOKATPER",
        "AMS_SOKATPER_2021_20230726",
    ),
    # IAF: one holding, 8 registers; Lev2/Lev3 access tiers and year-range vintages
    # carry identical columns, so each register names all its physical tables.
    ("iaf", "beslut", "_default"): (
        "IFA_Beslut",
        "IFA_Beslut_From2018_Lev2",
        "IFA_Beslut_Lev3",
        "IFA_Beslut_Tom2017_Lev2",
    ),
    ("iaf", "diverse", "_default"): ("IFA_Diverse_Lev2", "IFA_Diverse_Lev3"),
    ("iaf", "ersattningsperiod", "_default"): (
        "IFA_Ersperiod_Lev2",
        "IFA_Ersperiod_Lev3",
    ),
    ("iaf", "utbetalning", "_default"): (
        "IFA_Utbet_2015_2017_Lev2",
        "IFA_Utbet_2018_2019_Lev2",
        "IFA_Utbet_2020_2021_Lev2",
        "IFA_Utbet_Lev3",
    ),
    ("iaf", "medlemskap", "_default"): (
        "IFA_Medlemskap_Lev2",
        "IFA_Medlemskap_Lev3",
    ),
    ("iaf", "kassakod", "_default"): (
        "IFA_Kassakod",
        "IFA_Kassakod_Lev2",
        "IFA_Kassakod_Lev3",
    ),
    ("iaf", "kassakortsvecka", "_default"): ("IFA_KKvecka_Lev2", "IFA_KKvecka_Lev3"),
    ("iaf", "deltidsveckor", "_default"): (
        "IFA_Deltidsveckor_Lev2",
        "IFA_Deltidsveckor_Lev3",
    ),
}


def _kebab(text: str) -> str:
    """ASCII kebab slug: NFKD-transliterate (ä→a), lower, non-alnum→'-'. Used
    for the inventory variable `key` (becomes `variable.provider_key`, which the
    slug source-ID grammar forbids a '.' in)."""
    folded = (
        unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    )
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", folded.lower())).strip("-")


def _flavor_variables(columns: list[dict]) -> list[dict]:
    """Project an enriched holding's columns into inventory variable dicts.

    A column whose master-list `Källa` names a canonical source REGISTER (LISA,
    RTB, AKU, …) has a canonical home and is routed to the global graft/enrichment
    track — never flavored (scope follows what a fact is ABOUT; see #365). So only
    Källa-empty columns — genuinely SWECOV-constructed — survive into the flavor.
    (For the commercial / regional / quality-register providers no column carries a
    Källa at all, so this is a no-op there; it only trims the mixed
    SWECOV-constructed RTB extracts.)

    EXCEPTION: a `Källa` that is a PROVENANCE phrase ("Inrapporterat från <agency>")
    is NOT a canonical register — it marks data reported FROM an external public
    agency that has no reg_meta home (Tillväxtverket's korttidsarbete). That is
    precisely steward-flavor content, so such columns are KEPT in the flavor rather
    than routed to a graft target that doesn't exist (#443).

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
    selection, not a register fact."""
    # fold -> {physical column: its enriched entry}, first-seen first.
    groups: dict[str, dict[str, dict]] = {}
    for col in sorted(columns, key=lambda c: c["name"]):
        kalla = col.get("kalla")
        if kalla and not _PROVENANCE_KALLA.match(kalla):
            # Källa names a canonical register -> global graft track, not flavor.
            # A provenance phrase ("Inrapporterat från …") is steward-reported data
            # with no canonical home -> keep it in the flavor (see docstring, #443).
            continue
        logical = _ANY_LOPNR_PREFIX.sub("", col["name"]).strip()
        if not logical:
            continue
        groups.setdefault(_norm_alnum(col["normalized"]), {}).setdefault(logical, col)

    variables: list[dict] = []
    for spellings in groups.values():
        first_column, first_entry = next(iter(spellings.items()))
        # Multistate inventory contract (#981): the delivery column and its window
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
        variables.append(
            {
                "key": _kebab(first_entry["normalized"]) or f"v{len(variables)}",
                "name": first_column,
                "definition": None,
                "description": first_entry.get("description") or None,
                "is_identifier": bool(_ANY_LOPNR_PREFIX.match(first_entry["name"])),
                "is_sensitive": False,
                "states": [state],
            }
        )
    return variables


def _source_label(csv_path: Path) -> str:
    """`swecov-inventory-<YYYY-MM-DD>` from the inventory CSV filename."""
    m = re.search(r"(\d{4}-\d{2}-\d{2})", csv_path.stem)
    return f"swecov-inventory-{m.group(1)}" if m else "swecov-inventory"


def cmd_flavor(args: argparse.Namespace) -> None:
    base = args.csv.parent
    enriched_path = base / "derived" / "holdings_enriched.json"
    if not enriched_path.exists():
        raise SystemExit(f"{enriched_path} missing — run `enrich` first")
    enriched = json.loads(enriched_path.read_text())

    providers: dict[str, str] = {}
    # (provider, register_key) -> register dict; variants keyed for accretion.
    registers: dict[tuple[str, str], dict] = {}
    for (
        key,
        prov_slug,
        prov_name,
        reg_key,
        reg_name,
        var_key,
        var_slug,
        var_name,
    ) in _FLAVOR_DISPOSITION:
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
        tables = _FLAVOR_VARIANT_TABLES.get((prov_slug, reg_key, var_key))
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
        variables = _flavor_variables(columns)
        if not variables:
            # Every column was Källa-sourced (canonical) — nothing steward-only
            # remains. The inventory contract requires >= 1 variable per variant,
            # and such a holding does not belong in the flavor anyway: drop its
            # disposition entry (route the canonical columns via the graft track).
            raise SystemExit(
                f"flavor holding {key!r} ({prov_slug}/{reg_key}) yielded 0 "
                "steward-only variables (all columns carry a Källa). Remove it "
                "from _FLAVOR_DISPOSITION — its content is canonical, not flavor."
            )
        reg["_variants"][var_key] = {
            "key": var_key,
            "name": var_name,
            "description": None,
            "variables": variables,
        }
        reg["_variant_slugs"][var_key] = var_slug

    # --- inventory JSON (the extend-db contract) ---
    inv_registers = []
    for (prov_slug, reg_key), reg in sorted(registers.items()):
        inv_registers.append(
            {
                "provider": reg["provider"],
                "key": reg["key"],
                "name": reg["name"],
                "purpose": reg["purpose"],
                "description": reg["description"],
                "variants": [reg["_variants"][vk] for vk in reg["_variants"]],
            }
        )
    inventory = {
        "steward": "swecov",
        "source_label": _source_label(args.csv),
        "providers": [{"slug": s, "name": providers[s]} for s in sorted(providers)],
        "registers": inv_registers,
    }
    dest = base / "derived" / "flavor_inventory.json"
    dest.parent.mkdir(exist_ok=True)
    dest.write_text(json.dumps(inventory, ensure_ascii=False, indent=1))

    # --- steward slug TOMLs (committed; register + register_variant only) ---
    # Deterministic high-band ids via reg_meta_build.id.mint mirror what
    # extend_db mints, so the `[register."<id>"]` / `[register_variant."<id>.<id>"]`
    # source-IDs line up with the inserted rows. Variable slugs auto-derive at
    # build time (not committed pre-v1), so they are not emitted here.
    from reg_meta_build.id import mint

    slug_root = base / "derived" / "fqid_slugs_swecov"
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
            f"each build while UNFROZEN holds).\n\n"
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
    print(f"wrote {dest}")
    print(f"  providers: {len(providers)}")
    print(f"  registers: {len(registers)}  variants: {n_var}  variables: {n_vars}")
    print(f"wrote steward slug TOMLs to {slug_root}/ ({len(by_provider)} providers)")


# --- steward catalog (#423 PR3) ---------------------------------------------
#
# Emit the committed steward catalog — `reg_webapp/stewards/swecov/steward.toml`
# + `steward.project_data.json` — by resolving every physical SWECOV holding
# column against a FLAVORED reg_meta DB (`extend-db` output: released global +
# the steward providers from `cmd_flavor`). Admission is COLUMN-based (#206), so
# the catalog is a `project_data.json` whose bindings pin the resolved
# `delivery_column_name` of each held column. Pass `--db <flavored>/reg_meta.db`.
#
# Coverage is bounded by what reg_meta currently mints: ~63% of columns ground
# exactly, pivots recover a few more, and the dominant residue is the ~2k
# undocumented survey-wave items (FOU/CIS/IT) that need a global graft effort
# (#400-adjacent), tracked as a follow-up. The catalog auto-improves as that
# upstream content lands — just re-run this against a fresh flavored DB. The
# residue is written to `derived/steward_coverage.json` for review.

# Map each flavor-disposition enriched key to its provider slug, so flavor
# holdings (no SCB/SOS `mapping.to`) resolve against their steward provider.
_STEWARD_DISP_PROVIDER = {entry[0]: entry[1] for entry in _FLAVOR_DISPOSITION}

# Holdings with no SCB/SOS `mapping.to` whose content has since landed under a
# GLOBAL provider (not steward flavor): FOHM + FK as global thin providers
# (#422), and the remaining public agencies onboarded by #443 (Umeå
# Högskoleprovet, Läkemedelsverket, Pliktverket/Riksarkivet enlistment). Their
# columns resolve against that provider; any miss is a global alias/onboarding
# gap, never steward flavor. Keyed by an enriched-key PREFIX (the Military
# enlistment key has a `/Pliktverket` vs `/Riksarkivet` detail tail).
_STEWARD_RESIDUE_PROVIDER = {
    "FOHM": "fohm",
    "Försäkringskassan": "fk",
    "Högskoleprovet": "umu",
    "Läkemedelsverket": "lakemedelsverket",
    "Military enlistment/Pliktverket": "pliktverket",
    "Military enlistment/Riksarkivet": "riksarkivet",
}

# Holdings whose canonical-SCB content landed on a specific register after the
# enrich stage ran (so `mapping` doesn't point at it): #444 added AGI's
# employer-header register and the utrikeshandel-tjänster register. Keyed by
# exact enriched key → register coord (`provider/register`).
_STEWARD_HOLDING_REGISTER = {
    "AGI/Huvud": "scb/agi-huvud",
    "Utrikeshandel/Tjänster": "scb/utrikeshandel-tjanster",
}

# Survey-wave holdings: documented in SWECOV's delivery lists but the individual
# wave items are absent from reg_meta machine metadata. The dominant residue;
# classified as a follow-up GLOBAL graft effort, not minted here.
_STEWARD_SURVEY = ("Survey/FOU", "Survey/Innovation", "Survey/IT")

# A binding's reg_schema `type`. The catalog is a holdings statement (admission
# is column-based), so types are best-effort from the state's `data_type`; the
# value_set is intentionally omitted (optional, and pinning a vintage is the
# retired `@version` job).
_STEWARD_NUMERIC = {
    "int",
    "tinyint",
    "smallint",
    "bigint",
    "float",
    "numeric",
    "decimal",
    "numerisk",
    "heltal",
    "decimaltal",
    "double",
    "real",
}
_STEWARD_DATETIME = {"datetime", "smalldatetime", "datetime2", "timestamp"}


def _steward_col_type(data_type: str | None, is_identifier: int) -> str:
    if is_identifier:
        return "id"
    d = (data_type or "").strip().lower()
    if d in _STEWARD_DATETIME:
        return "datetime"
    if d == "date":
        return "date"
    if d in _STEWARD_NUMERIC:
        return "numeric"
    return "opaque"


def _steward_pivot_base(col: str) -> str | None:
    """Concept base of a wide-pivot column (`DIA1..30`, `Hushallsid_2011`,
    `LonFInkJan`) — the stem with its trailing index / year / month token
    stripped. Returns None when there is no such token. A bare stem is NOT yet a
    pivot: the caller only collapses onto the base when ≥2 sibling columns in the
    same holding share it (a true wide pivot has many indexed members), which
    keeps a lone digit-suffixed column from spuriously over-admitting a base."""
    months = "Jan|Feb|Mars|Mar|Apr|Maj|Juni|Juli|Aug|Sep|Okt|Nov|Dec"
    for pat in (rf"(?:{months})$", r"_?\d{4}$", r"\d+$"):
        m = re.search(pat, col)
        if m and m.start() > 0:
            return col[: m.start()].rstrip("_")
    return None


def _steward_load_db(db_path: Path):
    """Build the column-resolution index from a FLAVORED DB.

    Returns ``(by_regcol, by_provcol, states_vc)``:

    * ``by_regcol[(provider/register, UPPER col)]`` and
      ``by_provcol[(provider, UPPER col)]`` → list of resolution records
      ``{coord, vslug, col, dtype, isid}`` (``coord`` = 3-part
      ``provider/register/variant``).
    * ``states_vc[(coord, vslug, col)]`` → the state windows
      ``[(value_set_id, valid_from, valid_to)]`` for co-delivery detection.

    A co-delivered second spelling — the flavor group's other Covid column,
    every #945 multi-alias cvid on the global track — has no
    ``variable_state`` row of its own, so the union's second arm resolves it
    like a state column: shape (``data_type`` / ``is_identifier``) and value
    set from the state that owns its window, the window itself from
    ``variable_alias_window``. Admission is column-based, so a literal delivery
    column left unresolved is not orderable at all (steward README →
    "Near-duplicate physical columns").

    That arm reads the alias WINDOW table, not ``variable_alias``, because a
    window is what makes a spelling an orderable REPRESENTATION: the resolver
    (`Catalog._expand_state_windows`) surfaces an alias column only where its
    window is CONTAINED in a state's validity and that state's own column
    participates in the contained set, and §12's consistency gate
    (`reg_meta.inventory_check`) refuses to boot a deployment whose inventory
    maps anything else. A bare ``variable_alias`` row is the search-only
    delivery-column history — mapping one would state holdings no order could
    ever fill — so those two conditions are mirrored here rather than unioning
    the two tables flat.
    """
    from reg_meta.db import register_py_lower

    if not db_path.exists():
        raise SystemExit(f"reg_meta DB not found: {db_path} (pass --db <flavored>)")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    # Unicode-aware LOWER for the participation test below: SQLite's own is
    # ASCII-only, so a Swedish header (`Ägare`) would not fold — the reason
    # every other delivery-column case comparison in the build uses it (#843).
    register_py_lower(conn)
    by_regcol: dict[tuple[str, str], list] = defaultdict(list)
    by_provcol: dict[tuple[str, str], list] = defaultdict(list)
    states_vc: dict[tuple[str, str, str], list] = defaultdict(list)
    # The ORDER BY spans the whole union (alias rows sort INTO the state rows,
    # they are not appended after them), so the cursor order — and thus the
    # `type` picked for a column whose states disagree on `data_type` (first row
    # wins below) — is stable across reg_meta rebuilds and the emitted catalog
    # is byte-stable, not just sorted. `coord` orders exactly as the variant
    # slug did: its `provider/register/` prefix is constant within the two
    # preceding keys.
    for prov, reg, coord, vslug, col, dtype, isid, vsid, vf, vt in conn.execute(
        """SELECT p.slug AS prov, r.slug AS reg,
                  p.slug||'/'||r.slug||'/'||rv.slug AS coord, v.slug AS vslug,
                  vs.delivery_column_name AS col, vs.data_type AS dtype,
                  v.is_identifier AS isid, vs.value_set_id AS vsid,
                  vs.valid_from AS vf, vs.valid_to AS vt
           FROM variable_state vs JOIN variable v USING(variable_id)
           JOIN register r ON r.register_id=v.register_id
           JOIN provider p ON p.provider_id=r.provider_id
           JOIN register_variant rv USING(register_variant_id)
           WHERE vs.delivery_column_name IS NOT NULL
             AND trim(vs.delivery_column_name) != '' AND v.slug IS NOT NULL
           UNION ALL
           SELECT p.slug, r.slug, p.slug||'/'||r.slug||'/'||rv.slug, v.slug,
                  w.delivery_column_name, vs.data_type, v.is_identifier,
                  vs.value_set_id, w.valid_from, w.valid_to
           FROM variable_alias_window w JOIN variable v USING(variable_id)
           JOIN register r ON r.register_id=v.register_id
           JOIN provider p ON p.provider_id=r.provider_id
           JOIN register_variant rv USING(register_variant_id)
           JOIN variable_state vs ON vs.variable_id=w.variable_id
                AND vs.register_variant_id=w.register_variant_id
                AND vs.valid_from<=w.valid_from AND w.valid_to<=vs.valid_to
           WHERE trim(w.delivery_column_name) != '' AND v.slug IS NOT NULL
             AND NOT EXISTS (SELECT 1 FROM variable_state s
                             WHERE s.variable_id=w.variable_id
                               AND s.register_variant_id=w.register_variant_id
                               AND s.delivery_column_name=w.delivery_column_name)
             AND EXISTS (SELECT 1 FROM variable_alias_window b
                         WHERE b.variable_id=w.variable_id
                           AND b.register_variant_id=w.register_variant_id
                           AND vs.valid_from<=b.valid_from
                           AND b.valid_to<=vs.valid_to
                           AND py_lower(b.delivery_column_name)
                               =py_lower(vs.delivery_column_name))
           ORDER BY prov, reg, coord, vslug, col, vf, dtype"""
    ):
        u = col.upper()
        rec = {"coord": coord, "vslug": vslug, "col": col, "dtype": dtype, "isid": isid}
        by_regcol[(f"{prov}/{reg}", u)].append(rec)
        by_provcol[(prov, u)].append(rec)
        states_vc[(coord, vslug, col)].append((vsid, vf, vt))
    conn.close()
    return by_regcol, by_provcol, states_vc


def _steward_codelivered(states_vc, coord: str, vslug: str, col: str) -> bool:
    """True iff ≥2 states on this exact (variant, variable, column) carry
    DISTINCT value sets (`value_set_id` unequal; two NULLs are the SAME, so an
    unversioned column never self-trips) with OVERLAPPING validity windows —
    the reg_meta co-delivered-value-set situation the steward `_default` period
    surfaces as `binding_value_set_version_ambiguous`. Such a binding is
    un-authorable (the value-set-version pin is retired), so it is pruned and
    recorded as residue. Mirrors `reg_webapp.semantic._has_codelivered_versions`
    for the whole-history (`_default`) case."""
    st = states_vc.get((coord, vslug, col), [])
    for i in range(len(st)):
        for j in range(i + 1, len(st)):
            (v1, f1, t1), (v2, f2, t2) = st[i], st[j]
            if v1 != v2 and max(f1, f2) <= min(t1, t2):
                return True
    return False


def _steward_scope(key: str, mapping: dict) -> tuple[set[str], set[str]]:
    """(register coords `provider/register`, provider slugs) to resolve a
    holding's columns against. SCB/SOS mappings give register coords; flavor
    holdings give their steward provider; the residue map routes a no-mapping.to
    holding to the global provider its content landed under (#422/#443); the
    holding-register map routes a canonical-on-a-specific-register holding
    (#444)."""
    regs = _mapping_registers(mapping)
    provs: set[str] = set()
    if reg := _STEWARD_HOLDING_REGISTER.get(key):
        regs.add(reg)
    if key in _STEWARD_DISP_PROVIDER:
        provs.add(_STEWARD_DISP_PROVIDER[key])
    if mapping["status"] == "flavor":
        provs.add("swecov")  # SWECOV-constructed columns live under the swecov provider
    for prefix, prov in _STEWARD_RESIDUE_PROVIDER.items():
        if key.startswith(prefix):
            provs.add(prov)
    return regs, provs


def _steward_residue_bucket(key: str, mapping: dict) -> str:
    if key.startswith(_STEWARD_SURVEY):
        return "survey_wave_global_graft_followup"
    # A holding routed to a global provider/register whose columns still miss is
    # an alias/onboarding gap on that global track, not steward flavor.
    if key in _STEWARD_HOLDING_REGISTER:
        return "canonical_scb_alias_gap"
    for prefix in _STEWARD_RESIDUE_PROVIDER:
        if key.startswith(prefix):
            return "global_provider_alias_gap"
    return "other"


def _steward_phys_columns(holding: dict) -> list[str]:
    """The physical delivery columns a holding delivers — the union across its
    tables (the authoritative holdings set), falling back to the documented
    column list."""
    cols: set[str] = set()
    for table_cols in (holding.get("table_columns") or {}).values():
        cols.update(table_cols)
    if not cols:
        cols = {c["name"] for c in holding.get("columns", [])}
    return sorted(cols)


def cmd_steward(args: argparse.Namespace) -> None:
    enriched_path = args.csv.parent / "derived" / "holdings_enriched.json"
    if not enriched_path.exists():
        raise SystemExit(f"{enriched_path} missing — run `enrich` first")
    enriched = json.loads(enriched_path.read_text())
    by_regcol, by_provcol, states_vc = _steward_load_db(args.db)

    def resolve(col: str, regs: set[str], provs: set[str]) -> list[dict]:
        u = LOPNR_PREFIX.sub("", col).upper()
        out: list[dict] = []
        for rg in regs:
            out += by_regcol.get((rg, u), [])
        for pr in provs:
            out += by_provcol.get((pr, u), [])
        return out

    # coord -> {(vslug, representation) -> binding dict}
    src_bindings: dict[str, dict] = defaultdict(dict)
    total = covered = pivots = 0
    pruned: set[tuple[str, str, str]] = set()
    residue: dict[str, dict] = {}
    excluded_cols = 0
    for key, holding in sorted(enriched.items()):
        mapping = holding["mapping"]
        status = mapping["status"]
        cols = _steward_phys_columns(holding)
        if status == "lookup":
            # Pure crosswalk/key tables — no catalogable variables, excluded from
            # the coverage denominator (a documented non-gap).
            excluded_cols += len(cols)
            continue
        regs, provs = _steward_scope(key, mapping)
        # A column may collapse onto a pivot base only when ≥2 columns in this
        # holding share that base — a true wide pivot (`DIA1..30`) has many
        # indexed members; a lone digit-suffixed column must not over-admit a
        # base concept it merely resembles.
        base_counts: dict[str, int] = defaultdict(int)
        for c in cols:
            b = _steward_pivot_base(LOPNR_PREFIX.sub("", c))
            if b:
                base_counts[b] += 1
        unresolved: list[str] = []
        for col in cols:
            total += 1
            recs = resolve(col, regs, provs)
            is_pivot = False
            if not recs:
                base = _steward_pivot_base(LOPNR_PREFIX.sub("", col))
                if base and base_counts[base] >= 2:
                    recs = resolve(base, regs, provs)
                    is_pivot = bool(recs)
            if not recs:
                unresolved.append(LOPNR_PREFIX.sub("", col))
                continue
            covered += 1
            pivots += int(is_pivot)
            for r in recs:
                rep = r["col"]  # pin the resolved delivery column on every binding
                if _steward_codelivered(states_vc, r["coord"], r["vslug"], rep):
                    # Un-authorable (co-delivered value sets) — prune + record.
                    pruned.add((r["coord"], r["vslug"], rep))
                    continue
                bkey = (r["vslug"], rep)
                if bkey not in src_bindings[r["coord"]]:
                    prov, regslug, _variant = r["coord"].split("/")
                    src_bindings[r["coord"]][bkey] = {
                        "variable": f"{prov}/{regslug}/{r['vslug']}",
                        "type": _steward_col_type(r["dtype"], r["isid"]),
                        "representation": rep,
                    }
        if unresolved:
            residue[key] = {
                "status": status,
                "bucket": _steward_residue_bucket(key, mapping),
                "unresolved": len(unresolved),
                "of": len(cols),
                "columns": unresolved,
            }

    # --- assemble project_data.json ---
    # One source per register_variant at period `_default` (the whole-history,
    # no-period-filter sentinel): a steward catalog is a statement of holdings,
    # not a time window, so `_default` admits every held column regardless of
    # delivery vintage. Bindings sort by (variable slug, representation) for a
    # byte-stable regenerate.
    sources = []
    for coord in sorted(src_bindings):
        binds = [
            src_bindings[coord][bk]
            for bk in sorted(src_bindings[coord], key=lambda t: (t[0], t[1] or ""))
        ]
        sources.append(
            {
                "name": coord.replace("/", "."),
                "register_variant": coord,
                "period": "_default",
                "bindings": binds,
            }
        )
    project = {
        "schema_version": "2.0.0",
        "steward": "swecov",
        "reg_meta_version": args.reg_meta_version,
        "name": "SWECOV holdings catalog",
        "sources": sources,
    }

    repo_root = Path(__file__).resolve().parents[3]
    dest = args.out or (repo_root / "reg_webapp" / "stewards" / "swecov")
    dest.mkdir(parents=True, exist_ok=True)
    (dest / "steward.project_data.json").write_text(
        json.dumps(project, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    (dest / "steward.toml").write_text(
        'id = "swecov"\n'
        'name = "SWECOV"\n'
        'long_name = "SWECOV register-data catalog"\n'
        'hostname = "data.swecov.se"\n',
        encoding="utf-8",
    )

    # --- coverage manifest (untracked; for review + the PR body) ---
    bucket_totals: dict[str, int] = defaultdict(int)
    for info in residue.values():
        bucket_totals[info["bucket"]] += info["unresolved"]
    n_bindings = sum(len(s["bindings"]) for s in sources)
    manifest = {
        "source_label": _source_label(args.csv),
        "reg_meta_version": args.reg_meta_version,
        "physical_columns": total,
        "resolved_columns": covered,
        "coverage": round(covered / total, 4) if total else 0.0,
        "pivots_recovered": pivots,
        "pruned_codelivered_value_sets": sorted(
            f"{c}/{v} ({col})" for c, v, col in pruned
        ),
        "excluded_columns_lookup_or_out_of_scope": excluded_cols,
        "sources": len(sources),
        "bindings": n_bindings,
        "residue_by_bucket": dict(sorted(bucket_totals.items())),
        "residue_by_holding": dict(sorted(residue.items())),
    }
    manifest_path = args.csv.parent / "derived" / "steward_coverage.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=1), encoding="utf-8"
    )

    print(f"wrote {dest}/steward.toml + steward.project_data.json")
    print(f"  coverage: {covered}/{total} = {covered / total:.1%}  (pivots: {pivots})")
    print(f"  sources: {len(sources)}  bindings: {n_bindings}")
    print(f"  pruned (co-delivered value sets): {len(pruned)}")
    print(
        f"  residue: {sum(i['unresolved'] for i in residue.values())} cols, by bucket:"
    )
    for bucket, n in sorted(bucket_totals.items(), key=lambda kv: -kv[1]):
        print(f"      {n:5d}  {bucket}")
    print(f"  wrote coverage manifest to {manifest_path}")


# --- inventory: emit the §12 delivery inventory (reality ∩ policy) -----------
#
# The physical delivery-topology contract (REFACTOR_SPEC.md §12): one
# `[[table]]` per CURRENT physical table on MONA, one explicit finite edition,
# literal columns, and zero-or-more (register_variant, variable, representation)
# mappings per column. Reality is the holdings CSV (the maintainer's own
# re-extractable listing of everything on MONA — superseded snapshots included,
# so it is NEVER hand-edited); policy is the COMMITTED overlay TOML
# (`reg_webapp/stewards/swecov/inventory_overlay.toml`):
#
#   [[exclude]]  table, reason      — discard a superseded delivery
#   [[edition]]  table, edition     — explicit edition where the table name
#                                     yields zero or several period tokens
#   [[assign]]   table, register_variant (str or list)
#                                   — variant coordinate(s) where the curated
#                                     MAPPING cannot place the table (residue
#                                     providers, grafted holdings)
#
# The emitted `inventory.toml` is committed and validated through
# `reg_meta.inventory.load_inventory` — §12's one-to-one conflicts are the
# maintainer's supersession worklist, and the build stays red until every
# conflict has an overlay disposition. Overlay entries naming tables no longer
# in the CSV are flagged stale.
#
# Every emitted mapping carries an EXPLICIT `representation` (the resolved
# canonical `delivery_column_name`): exact-triple matching in `order.py` never
# needs the single-representation proof, and the conflict validator then only
# conflates genuinely identical cells.
#
# simplify: wide-pivot columns (DIA1..30 → concept DIA) are left UNMAPPED — §12
# has no cell shape for N physical columns serving one logical variable yet, so
# mapping all members to one representation would self-conflict inside the
# table. They stay in the denominator and are counted in the worklist; upgrade
# trigger: the first real order that needs a pivot concept.

_DISP_BY_KEY: dict[str, list[tuple[str, str, str, str]]] = defaultdict(list)
for _entry in _FLAVOR_DISPOSITION:
    _DISP_BY_KEY[_entry[0]].append((_entry[1], _entry[3], _entry[5], _entry[6]))


def _load_overlay(path: Path) -> dict:
    import tomllib

    if not path.exists():
        return {"exclude": {}, "edition": {}, "assign": {}}
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    try:
        overlay = {
            "exclude": {
                e["table"]: e.get("reason", "") for e in raw.get("exclude", [])
            },
            "edition": {e["table"]: e["edition"] for e in raw.get("edition", [])},
            # §12 disjoint-partition labels: explicit curated facts, never
            # inferred — the emitter only copies them onto the [[table]].
            "partition": {e["table"]: e["label"] for e in raw.get("partition", [])},
            # Suppress the mapping(s) on ONE physical column: the column stays
            # inventoried (and still ships whenever its table is ordered), but
            # another table owns the shared variable — the aux-table join-key
            # disposition.
            "unmap": {(e["table"], e["column"]) for e in raw.get("unmap", [])},
            "assign": defaultdict(set),
        }
        for e in raw.get("assign", []):
            rv = e["register_variant"]
            overlay["assign"][e["table"]].update([rv] if isinstance(rv, str) else rv)
    except (KeyError, TypeError) as exc:
        raise SystemExit(
            f"malformed overlay entry in {path}: {exc!r} — every [[exclude]] "
            "and [[edition]] needs `table` (+ `edition`), every [[partition]] "
            "needs `table` and `label`, every [[assign]] needs `table` and "
            "`register_variant`"
        ) from exc
    return overlay


def _edition_year(edition: object) -> int | None:
    """Best-effort year of an edition, for `period:` split selectors."""
    if isinstance(edition, int):
        return edition
    if isinstance(edition, str):
        m = re.match(r"(?:HT|VT)?((?:19|20)\d{2})", edition)
        return int(m.group(1)) if m else None
    if isinstance(edition, dict):
        return _edition_year(edition.get("from"))
    if isinstance(edition, list) and edition:
        return _edition_year(edition[0])
    return None


def _table_edition(table: str, overlay: dict) -> object | None:
    """One explicit finite edition for a table, or None → edition worklist.
    Overlay wins; otherwise exactly ONE name-derived period token is trusted
    (a zero- or multi-token name is period-ambiguous by construction)."""
    if table in overlay["edition"]:
        return overlay["edition"][table]
    tokens = extract_periods(table)
    if len(tokens) != 1:
        return None
    tok = tokens[0]
    return int(tok) if re.fullmatch(r"\d{4}", tok) else tok


def _table_coords(
    key: str, mapping: dict, table: str, year: int | None, overlay: dict
) -> set[str] | None:
    """Admissible 3-part variant coordinates for a physical table, or None →
    assignment worklist. Overlay `assign` wins; then the curated MAPPING
    (mapped → its coordinate; split → stem/period selectors against THIS
    table); then the flavor disposition (+ `_FLAVOR_VARIANT_TABLES`)."""
    if table in overlay["assign"]:
        return set(overlay["assign"][table])
    status = mapping.get("status")
    if status == "mapped":
        return {mapping["to"]}
    if status == "split":
        coords: set[str] = set()
        # Stem selectors nest (AKU_ ⊂ AKU_RL_ ⊂ AKU_Corona_), so the LONGEST
        # matching stem wins — a table belongs to its most specific selector,
        # never to every prefix of its name.
        best_stem: tuple[int, str | None] | None = None
        for sel, coord, _note in mapping["to"]:
            if sel.startswith("stem:"):
                stem = sel[5:]
                if table.upper().startswith(stem.upper()) and (
                    best_stem is None or len(stem) > best_stem[0]
                ):
                    best_stem = (len(stem), coord)
            elif sel.startswith("period:") and year is not None:
                lo, _, hi = sel[7:].partition("-")
                if int(lo) <= year and (not hi or year <= int(hi)) and coord:
                    coords.add(coord)
        # A flavor selector (coord None, e.g. AKU_Corona_) carries no
        # coordinate — such a table falls through to the assignment worklist
        # like any other placed-by-hand table.
        if best_stem is not None and best_stem[1]:
            coords.add(best_stem[1])
        return coords or None
    disp = _DISP_BY_KEY.get(key, [])
    coords = set()
    for prov, reg_key, var_key, var_slug in disp:
        tables = _FLAVOR_VARIANT_TABLES.get((prov, reg_key, var_key))
        if tables is None or table in tables:
            coords.add(f"{prov}/{reg_key}/{var_slug}")
    return coords or None


def _toml_str(value: str) -> str:
    # JSON string escaping is valid TOML basic-string escaping except U+007F,
    # which JSON leaves raw and TOML forbids in a basic string.
    return json.dumps(value, ensure_ascii=False).replace("\x7f", "\\u007f")


def _toml_edition(edition: object) -> str:
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
    holdings = parse_inventory(args.csv)
    repo_root = Path(__file__).resolve().parents[3]
    steward_dir = args.out or (repo_root / "reg_webapp" / "stewards" / "swecov")
    overlay = _load_overlay(steward_dir / "inventory_overlay.toml")
    by_regcol, _by_provcol, _states = _steward_load_db(args.db)
    # coord -> {UPPER(col): [(coord, vslug, canonical col)]} narrowed per lookup.
    by_coordcol: dict[tuple[str, str], list] = defaultdict(list)
    # Auto-assign support (maintainer-approved 2026-09-01): a table whose
    # resolution scope names a register/provider with exactly ONE variant in
    # the flavored DB leaves no choice — assign it deterministically instead
    # of worklisting. No name inference involved.
    reg_variants: dict[str, set[str]] = defaultdict(set)
    prov_variants: dict[str, set[str]] = defaultdict(set)
    for (_reg, u), recs in by_regcol.items():
        for r in recs:
            by_coordcol[(r["coord"], u)].append(r)
            reg_variants["/".join(r["coord"].split("/")[:2])].add(r["coord"])
            prov_variants[r["coord"].split("/")[0]].add(r["coord"])

    # Merge holdings by physical table (a table may appear under several
    # (Category, Detail) keys); carry every (key, mapping) that lists it.
    tables: dict[str, dict] = {}
    all_csv_tables: set[str] = set()
    n_lookup_tables = 0
    for (cat, det), h in sorted(holdings.items()):
        key = _scope_key(cat, det or None)
        mapping = MAPPING.get((cat, det)) or {"status": "residue"}
        all_csv_tables.update(h.table_columns)
        if mapping.get("status") == "lookup":
            n_lookup_tables += len(h.table_columns)
            continue
        for table, cols in h.table_columns.items():
            entry = tables.setdefault(table, {"columns": set(), "scopes": []})
            entry["columns"].update(cols)
            entry["scopes"].append((key, mapping))

    # Stale = gone from the CSV entirely; an overlay entry naming a
    # lookup-skipped table is merely inert, not stale.
    stale = sorted(
        t
        for section in ("exclude", "edition", "partition", "assign")
        for t in overlay[section]
        if t not in all_csv_tables
    ) + sorted(
        f"{t} (unmap {c!r})" for t, c in overlay["unmap"] if t not in all_csv_tables
    )
    excluded = {t: overlay["exclude"][t] for t in overlay["exclude"] if t in tables}

    worklist: dict[str, list] = {
        "edition_needed": [],
        "assignment_needed": [],
        "stale_overlay_entries": stale,
    }
    n_cols = n_mapped = n_pivot = n_unresolved = n_auto_assigned = n_unmapped = 0
    lines: list[str] = [
        "# GENERATED by input_data/swecov/build_catalog.py inventory — do not",
        "# hand-edit. Reality is the holdings CSV; policy is the committed",
        "# inventory_overlay.toml next to this file (REFACTOR_SPEC.md §12).",
        "",
        "version = 1",
        'steward = "swecov"',
    ]
    for table in sorted(tables, key=str.upper):
        if table in excluded:
            continue
        entry = tables[table]
        edition = _table_edition(table, overlay)
        if edition is None:
            worklist["edition_needed"].append(
                {"table": table, "tokens": extract_periods(table)}
            )
            continue
        year = _edition_year(edition)
        coords: set[str] = set()
        unassigned = True
        for key, mapping in entry["scopes"]:
            got = _table_coords(key, mapping, table, year, overlay)
            if got is not None:
                unassigned = False
                coords.update(got)
        if unassigned:
            # Two tiers: REGISTER-scoped candidates first (a holding routed to
            # a specific register whose only variant leaves no choice), then
            # provider-scoped — the provider fallback (e.g. the swecov flavor
            # provider on flavor-status holdings) would otherwise pollute a
            # unique register-scoped answer.
            reg_candidates: set[str] = set()
            candidates: set[str] = set()
            for key, mapping in entry["scopes"]:
                regs, provs = _steward_scope(key, mapping)
                for r in regs:
                    reg_candidates.update(reg_variants.get(r, ()))
                for p in provs:
                    candidates.update(prov_variants.get(p, ()))
            candidates |= reg_candidates
            if len(reg_candidates) == 1:
                candidates = reg_candidates
            if len(candidates) == 1:
                coords = candidates
                n_auto_assigned += 1
            else:
                worklist["assignment_needed"].append(
                    {
                        "table": table,
                        "holdings": sorted({k for k, _ in entry["scopes"]}),
                        "candidate_variants": sorted(candidates),
                    }
                )
                continue
        cols = sorted(entry["columns"])
        base_counts: dict[str, int] = defaultdict(int)
        for c in cols:
            b = _steward_pivot_base(LOPNR_PREFIX.sub("", c))
            if b:
                base_counts[b] += 1
        lines += ["", "[[table]]", f"id = {_toml_str(table)}"]
        lines.append(f"edition = {_toml_edition(edition)}")
        if table in overlay["partition"]:
            lines.append(f"partition = {_toml_str(overlay['partition'][table])}")
        for col in cols:
            n_cols += 1
            if (table, col) in overlay["unmap"]:
                n_unmapped += 1
                lines += ["", "[[table.column]]", f"name = {_toml_str(col)}"]
                continue
            u = LOPNR_PREFIX.sub("", col).upper()
            recs = [r for coord in sorted(coords) for r in by_coordcol[(coord, u)]]
            if not recs:
                base = _steward_pivot_base(LOPNR_PREFIX.sub("", col))
                if base and base_counts[base] >= 2:
                    hit = any(by_coordcol[(coord, base.upper())] for coord in coords)
                    if hit:
                        n_pivot += 1  # simplify: pivots stay unmapped (header)
                        lines += ["", "[[table.column]]", f"name = {_toml_str(col)}"]
                        continue
                n_unresolved += 1
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

    dest = steward_dir / "inventory.toml"
    dest.write_text("\n".join(lines) + "\n", encoding="utf-8")
    wl_path = args.csv.parent / "derived" / "inventory_worklist.json"
    wl_path.parent.mkdir(parents=True, exist_ok=True)
    wl_path.write_text(
        json.dumps(worklist, ensure_ascii=False, indent=1), encoding="utf-8"
    )
    n_tables = (
        len(tables)
        - len(excluded)
        - len(worklist["edition_needed"])
        - len(worklist["assignment_needed"])
    )
    print(f"wrote {dest}")
    print(
        f"  tables: {n_tables} emitted"
        f" ({len(worklist['edition_needed'])} need edition,"
        f" {len(worklist['assignment_needed'])} need assignment,"
        f" {len(excluded)} excluded, {n_lookup_tables} lookup-skipped,"
        f" {n_auto_assigned} auto-assigned single-variant)"
    )
    print(
        f"  columns: {n_cols}  mapped: {n_mapped}  pivot-unmapped: {n_pivot}"
        f"  unresolved: {n_unresolved}  curated-unmapped: {n_unmapped}"
    )
    if stale:
        print(f"  STALE overlay entries (tables not in CSV): {len(stale)}")
        for t in stale:
            print(f"    {t}")
    print(f"  wrote worklist to {wl_path}")

    from reg_meta.errors import RegMetaError
    from reg_meta.inventory import load_inventory

    try:
        inv = load_inventory(dest)
    except RegMetaError as exc:
        print(f"\nVALIDATION FAILED — the §12 worklist:\n{exc.message}")
        raise SystemExit(1) from exc
    print(f"  load_inventory: OK ({len(inv.tables)} tables)")


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
    print(f"{len(holdings)} (Category, Detail) pairs; {len(index)} catalog variants\n")
    for universe, label in (
        (True, "IN-UNIVERSE (SCB/SOS)"),
        (False, "RESIDUE (outside catalog universe)"),
    ):
        print(f"=== {label} ===")
        for (cat, det), h in sorted(holdings.items()):
            if (cat not in NON_CATALOG_CATEGORIES) is not universe:
                continue
            pair = f"{cat} / {det}" if det else cat
            print(f"\n{pair}  [{len(h.tables)} tables, {len(h.match_columns)} cols]")
            print(f"  periods: {json.dumps(h.period_segments(), ensure_ascii=False)}")
            if not universe:
                print(f"  residue: {NON_CATALOG_CATEGORIES[cat]}")
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
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("normalize", help="dump normalized holdings as JSON")
    sub.add_parser("ground", help="propose the register mapping by column overlap")
    sub.add_parser(
        "enrich",
        help="join delivery documentation; write derived/holdings_enriched.json",
    )
    sub.add_parser(
        "globals", help="mine documented holdings for global catalog improvements"
    )
    sub.add_parser(
        "grafts",
        help="variant-tag the gapfill graft candidates (mapped+split, flavor-excluded)",
    )
    sub.add_parser(
        "flavor",
        help="emit the steward-only extend-db inventory JSON + slug TOMLs (#421)",
    )
    inventory_p = sub.add_parser(
        "inventory",
        help="emit the §12 delivery inventory.toml against a FLAVORED --db"
        " (reality = holdings CSV, policy = committed inventory_overlay.toml)",
    )
    inventory_p.add_argument(
        "--out",
        type=Path,
        default=None,
        help="stewards/swecov output dir (default: <repo>/reg_webapp/stewards/swecov)",
    )
    steward_p = sub.add_parser(
        "steward",
        help="emit reg_webapp/stewards/swecov/ catalog against a FLAVORED --db (#423)",
    )
    steward_p.add_argument(
        "--out",
        type=Path,
        default=None,
        help="stewards/swecov output dir (default: <repo>/reg_webapp/stewards/swecov)",
    )
    steward_p.add_argument(
        "--reg-meta-version",
        required=True,
        help=(
            "reg_meta release tag the flavored --db was built from, e.g. "
            "reg_meta/v0.25.0 — stamped into the catalog's reg_meta_version. "
            "Required: a fixed default would silently downgrade the stamp on a "
            "later release (caught in the 0.25.0 release)."
        ),
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
        "steward": cmd_steward,
    }[args.command](args)


if __name__ == "__main__":
    main()
