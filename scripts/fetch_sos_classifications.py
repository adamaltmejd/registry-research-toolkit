#!/usr/bin/env python3
"""Fetch Socialstyrelsen / Läkemedelsverket classification code lists.

Snapshot fetcher for the external code systems that Socialstyrelsen (SOS)
registers reference via `Länk kodverk` but do NOT ship inline in their
metadata workbooks: ICD-10-SE, KVÅ, ICF, KSI (all from the Swedish
eHälsomyndigheten), ICD-11-SE (WHO's Swedish ICD-11 MMS release), and ATC
(from Läkemedelsverket / the MPA NSL register).

This is the SOS analog of `scripts/extract_lkf.py`: it is run on demand by a
maintainer to refresh local snapshots, NOT fetched during `reg-meta-build
build-db`. Builds stay deterministic and offline — they read the committed
normalized CSVs, never the network. Re-run this when you want to pull newer
upstream code lists. (The reg_meta wiring of these CSVs is a separate,
later step — this script only acquires + normalizes the data.)

Two upstream mechanisms:

  - ATC: `https://nsl.mpa.se/sensl-v2.0.zip` (rebuilt nightly). The zip's
    `codesystems/atc-code.xml` is the authoritative Swedish ATC code list —
    the same register fass.se/LIF/atcregister is built from. Concepts carry
    Swedish + English labels and a `throughDate` for retired codes.

  - eHälsomyndigheten: a public Confluence "samarbetsyta" page hosts one
    tab-separated `.tsv` per classification (the full code list incl. the
    latest changes). As of June 2026 this is the new home for the files that
    used to live at socialstyrelsen.se/.../kodtextfiler/. Attachments are
    enumerated via the public REST API so we pick up version bumps without
    hardcoding query strings.

  - WHO ICD-11 MMS: the ICD browser publishes a language-specific simple
    tabulation zip per release. The Swedish `sv` file is the source for
    `icd-11-se.csv`.

Normalized output: one CSV per classification under `--out-dir`, columns
`code,label,label_en,parent_code,valid_from,valid_to` (a faithful superset;
blank where a source doesn't provide a field). The first two columns match
the `vardekod,vardebenamning` shape the existing classification loader uses,
so reducing to that format later is trivial. A `manifest.json` records the
source URL, raw-file sha256, fetch time, and code counts for provenance.

Run (use `--with xlrd` — the historical ICD sources are legacy .xls):
    uv run --with xlrd python scripts/fetch_sos_classifications.py

    # subset + force re-download of raw sources:
    uv run --with xlrd python scripts/fetch_sos_classifications.py --only atc icd-8 --force

    # just show what would be fetched:
    uv run python scripts/fetch_sos_classifications.py --list

Raw downloads are cached under `--cache-dir` (gitignored); normalized CSVs go
to `--out-dir` (under the tracked input_data/classifications/ tree).
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

# Repo-relative defaults. input_data/* is gitignored EXCEPT classifications/,
# so normalized CSVs (committed snapshots) go under classifications/sos/ while
# the bulky raw sources land in an ignored sibling cache dir.
_REPO = Path(__file__).resolve().parent.parent
DEFAULT_OUT_DIR = _REPO / "reg_meta_build/input_data/classifications/sos"
DEFAULT_CACHE_DIR = _REPO / "reg_meta_build/input_data/_classification_sources"

# eHälsomyndigheten "Ladda ner filer för klassifikationer" samarbetsyta page.
EHALSA_HOST = "https://samarbetsyta.ehalsomyndigheten.se"
EHALSA_PAGE_ID = "451267009"

# Socialstyrelsen SharePoint asset root hosting the legacy historical .xls
# classifications. (Migrating to eHälsomyndigheten through 2026; URLs live now.)
_SOS_ASSETS = (
    "https://www.socialstyrelsen.se/globalassets/sharepoint-dokument"
    "/dokument-webb/klassifikationer-och-koder"
)

# NordDRG Explorer FileExport API (DRG/MDC definition data).
_NORDDRG_API = "https://norddrg-explorer.socialstyrelsen.se/api"

_USER_AGENT = (
    "registry-research-toolkit/fetch_sos_classifications (+maintainer snapshot)"
)

# Normalized CSV header. code/label first so a 2-col reduction is a column slice.
CSV_HEADER = ("code", "label", "label_en", "parent_code", "valid_from", "valid_to")


@dataclass(frozen=True)
class Code:
    code: str
    label: str = ""
    label_en: str = ""
    parent_code: str = ""
    valid_from: str = ""
    valid_to: str = ""


@dataclass(frozen=True)
class Source:
    key: str  # CLI selector + output stem
    name: str  # human-readable classification name
    publisher: str
    kind: str  # mpa_zip | ehalsa_tsv | sos_xls | xlsx_url | norddrg_api
    # mpa_zip
    url: str | None = None
    member_suffix: str | None = None  # zip member whose name ends with this
    # ehalsa_tsv
    attachment: str | None = None  # filename on the samarbetsyta page
    # ehalsa_tsv_merge: several samarbetsyta TSVs concatenated into one code list,
    # deduped on the code column (earlier attachment's label wins on a collision).
    # KVÅ uses this — KMÅ + KKÅ are disjoint upstream but one åtgärd field
    # downstream. Mutually exclusive with `attachment`.
    merge_attachments: tuple[str, ...] = ()
    # sos_xls (legacy .xls on socialstyrelsen.se globalassets; needs xlrd)
    sheet: str | None = None
    code_col: int = 0
    label_col: int = 1
    parent_col: int | None = None
    skip_rows: int = 1  # header rows to skip
    # xlsx_url / norddrg_api (modern .xlsx; parsed by header NAME, stdlib reader)
    code_header: str | None = None
    label_header: str | None = None
    label_en_header: str | None = None
    valid_from_header: str | None = None
    valid_to_header: str | None = None
    note: str = ""


# Registry of SOS-referenced external classifications. KVÅ is split upstream
# into KMÅ (medical) + KKÅ (surgical) disjoint code spaces; we fetch both and
# MERGE them into one `kva.csv` (deduped on cross-space collisions — the ~50
# 1–2-char organizational headers that appear in both, never leaf åtgärd codes),
# since SOS registers reference KVÅ as a single åtgärd field.
SOURCES: tuple[Source, ...] = (
    Source(
        key="atc",
        name="ATC – Anatomical Therapeutic Chemical (Swedish register)",
        publisher="Läkemedelsverket (MPA)",
        kind="mpa_zip",
        url="https://nsl.mpa.se/sensl-v2.0.zip",
        member_suffix="atc-code.xml",
        note="Nightly-rebuilt NSL register; sv+en labels, throughDate for retired codes.",
    ),
    Source(
        key="icd-10-se",
        name="ICD-10-SE – diagnoses (Swedish version)",
        publisher="Socialstyrelsen / eHälsomyndigheten",
        kind="ehalsa_tsv",
        attachment="icd-10-se.tsv",
        note="Multi-row per code (continuation rows for includes/excludes); grouped by Kod.",
    ),
    Source(
        key="icd-11-se",
        name="ICD-11 MMS – Swedish release",
        publisher="WHO",
        kind="who_icd11_simple_tabulation",
        url="https://icdcdn.who.int/static/releasefiles/2026-01/SimpleTabulation-ICD-11-MMS-sv.zip",
        note=(
            "WHO ICD-11 MMS 2026-01 Swedish simple tabulation; categories with "
            "ICD-11 codes only, Swedish labels plus TitleEN."
        ),
    ),
    Source(
        key="kva",
        name="KVÅ – care measures (KMÅ medical + KKÅ surgical, merged)",
        publisher="Socialstyrelsen / eHälsomyndigheten",
        kind="ehalsa_tsv_merge",
        # KMÅ first so its label wins on the ~50 cross-space collision codes
        # (1–2-char organizational headers like A, AA, … that carry DIFFERENT
        # meanings in KMÅ vs KKÅ; never leaf åtgärd codes registers reference).
        merge_attachments=(
            "kva-medicinska-atgarder-kma.tsv",
            "kva-kirurgiska-atgarder-kka.tsv",
        ),
    ),
    Source(
        key="icf",
        name="ICF – Functioning, Disability and Health",
        publisher="Socialstyrelsen / eHälsomyndigheten",
        kind="ehalsa_tsv",
        attachment="icf.tsv",
    ),
    Source(
        key="ksi",
        name="KSI – Social Services Interventions",
        publisher="Socialstyrelsen / eHälsomyndigheten",
        kind="ehalsa_tsv",
        attachment="ksi.tsv",
    ),
    # Historical disease classifications (pre-ICD-10-SE). Legacy .xls on
    # socialstyrelsen.se globalassets — orphaned from the public index but the
    # URLs are stable. Needed for the per-era diagnosis chain (DORS cause-of-
    # death + PAR DIAGNOS span ICD-8→9→10; cancer uses ICD-7/9 too). Require
    # `--with xlrd`. Codes verified clean (no blank/dup rows).
    Source(
        key="icd-8",
        name="ICD-8 / Klassifikation av sjukdomar 1968 (Swedish)",
        publisher="Socialstyrelsen",
        kind="sos_xls",
        url=f"{_SOS_ASSETS}/icd-8-klassifikation-av-sjukdomar-mm-1968.xls",
        sheet="ICD8",
        note="Used 1969–1986 (cause of death). Codes use comma-decimal form '000,01'.",
    ),
    Source(
        key="icd-9-ks87",
        name="ICD-9 / KS87 – Klassifikation av sjukdomar 1987 (Swedish)",
        publisher="Socialstyrelsen",
        kind="sos_xls",
        url=f"{_SOS_ASSETS}/icd-9-klassifikation-av-sjukdomar-1987-ks87.xls",
        sheet="DIA",
        note="Used 1987–1996. Letter-suffix subdivisions (001X, 002A).",
    ),
    Source(
        key="ks87-p",
        name="KS87-P – Klassifikation av sjukdomar 1987, primärvård",
        publisher="Socialstyrelsen",
        kind="sos_xls",
        url=f"{_SOS_ASSETS}/klassifikation-av-sjukdomar-1987-primarvard-ks87-p-excel.xls",
        sheet="ks87pvdia",
        parent_col=3,  # KAPITEL (chapter)
        note="Coarse primary-care subset of KS87 (~383 codes, '008-').",
    ),
    # Country codes — Skatteverket landskoder (ISO-3166 alpha-2). Referenced by
    # SOS/SCB födelseland & medborgarskap variables. The .xlsx also has an
    # 'Ersättningskod' (benefit-code) sheet we ignore.
    Source(
        key="landskoder",
        name="Landskoder – country codes (Skatteverket, ISO-3166 alpha-2)",
        publisher="Skatteverket",
        kind="xlsx_url",
        url=(
            "https://www.skatteverket.se/download/18.6de6b99e16e94f3973b1ee"
            "/1574772028612/Land-%20och%20ers%C3%A4ttningskoder.xlsx"
        ),
        sheet="Landskod",
        code_header="Landskod",
        label_header="Text",
        valid_from_header="Period from",
        valid_to_header="Period tom",
        note="Verify SOS/SCB registers store ISO-2 before integrating (much "
        "födelseland data is inline kodlista or SCB-numeric).",
    ),
    # NordDRG (Socialstyrelsen) — DRG groups + MDC chapters, from the NordDRG
    # Explorer FileExport API (latest version). LICENSED by Nordic Casemix
    # Centre — Socialstyrelsen publishes the Swedish definitions freely, but
    # flag the license before redistributing. One 5 MB definition-data .xlsx
    # backs both 'drg' (drg_name sheet) and 'mdc' (mdc_name sheet).
    Source(
        key="drg",
        name="NordDRG – diagnosis-related groups (Swedish, latest)",
        publisher="Socialstyrelsen / Nordic Casemix Centre (licensed)",
        kind="norddrg_api",
        sheet="drg_name",
        code_header="drg_nat",  # Swedish DRG code (A03A…); drg_comb is the Nordic code
        label_header="drg_text_nat",
        label_en_header="drg_text_comb",
        note="Yearly versions SWE2022PR1…; fetches the latest. NordDRG licensed.",
    ),
    Source(
        key="mdc",
        name="MDC – major diagnostic categories (NordDRG, Swedish, latest)",
        publisher="Socialstyrelsen / Nordic Casemix Centre (licensed)",
        kind="norddrg_api",
        sheet="mdc_name",
        code_header="mdc",
        label_header="mdc_text_nat",
        label_en_header="mdc_text_comb",
        note="MDC chapter list (~27); same NordDRG definition-data .xlsx as drg.",
    ),
)
SOURCE_BY_KEY = {s.key: s for s in SOURCES}


# ---------------------------------------------------------------------------
# IO helpers
# ---------------------------------------------------------------------------


def _log(msg: str) -> None:
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()


def _ssl_context() -> ssl.SSLContext:
    """Verify against certifi's CA bundle: uv-managed CPython has no system
    trust store, so the default context fails cert verification on the gov
    hosts. Fall back to the default context if certifi isn't importable."""
    try:
        import certifi

        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        return ssl.create_default_context()


def _http_get(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(req, timeout=120, context=_ssl_context()) as resp:
        return resp.read()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def download(url: str, dest: Path, *, force: bool) -> bytes:
    """Fetch `url` to `dest`, caching. Returns the bytes either way."""
    if dest.is_file() and not force:
        _log(f"  cached  {dest.name} ({dest.stat().st_size:,} B)")
        return dest.read_bytes()
    _log(f"  GET     {url}")
    data = _http_get(url)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    _log(f"  saved   {dest.name} ({len(data):,} B, sha256={_sha256(data)[:12]}…)")
    return data


def confluence_attachment_url(page_id: str, filename: str) -> str:
    """Resolve a samarbetsyta attachment's download URL via the public REST API.

    The REST manifest carries a version-stamped download link; falling back to
    the stable `/download/attachments/<page>/<file>` path (which 200s without
    the query string) keeps the fetch working if the API shape changes.
    """
    api = f"{EHALSA_HOST}/rest/api/content/{page_id}/child/attachment?limit=200"
    try:
        manifest = json.loads(_http_get(api))
        for att in manifest.get("results", []):
            if att.get("title") == filename:
                link = att.get("_links", {}).get("download")
                if link:
                    return EHALSA_HOST + link
    except (urllib.error.URLError, json.JSONDecodeError, KeyError) as exc:
        _log(f"  (REST attachment lookup failed: {exc}; using direct path)")
    return f"{EHALSA_HOST}/download/attachments/{page_id}/{filename}"


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def parse_atc(zip_bytes: bytes, member_suffix: str) -> list[Code]:
    """Parse the MPA NSL `atc-code.xml` into `Code` rows.

    Zip members use Windows backslash separators; match on the basename suffix.
    Each `<concept code=… throughDate=?>` carries `<designation language=…>`
    children (sv/en). `throughDate` marks a retired code → `valid_to`.
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        member = next(
            (n for n in zf.namelist() if n.replace("\\", "/").endswith(member_suffix)),
            None,
        )
        if member is None:
            raise SystemExit(f"ATC: no zip member ending in {member_suffix!r}")
        xml_bytes = zf.read(member)

    # Strip the default namespace so element lookups stay readable.
    root = ET.fromstring(xml_bytes)
    ns = (
        {"c": root.tag[root.tag.find("{") + 1 : root.tag.find("}")]}
        if "{" in root.tag
        else {}
    )

    def find(el: ET.Element, tag: str) -> list[ET.Element]:
        return el.findall(f"c:{tag}", ns) if ns else el.findall(tag)

    out: list[Code] = []
    concepts_parent = find(root, "concepts")
    concepts = (
        find(concepts_parent[0], "concept")
        if concepts_parent
        else find(root, "concept")
    )
    for concept in concepts:
        code = (concept.get("code") or "").strip()
        if not code:
            continue
        valid_to = (concept.get("throughDate") or "").strip()
        sv = en = ""
        for des_parent in find(concept, "designations"):
            for des in find(des_parent, "designation"):
                lang = (des.get("language") or "").strip().lower()
                text = (des.text or "").strip()
                if lang == "sv" and not sv:
                    sv = text
                elif lang == "en" and not en:
                    en = text
        out.append(Code(code=code, label=sv, label_en=en, valid_to=valid_to))
    return out


def parse_tsv(tsv_bytes: bytes) -> list[Code]:
    """Parse an eHälsomyndigheten classification `.tsv` into `Code` rows.

    Columns are quoted, tab-separated, BOM-prefixed; every file has at least
    `Kod`, `Överordnad kod`, `Titel` (ICD-10-SE adds `Giltig från`). A code may
    span several rows (continuation rows leave most columns blank, carrying only
    e.g. an extra Utesluter note) — group by `Kod`, first non-empty value wins.
    """
    text = tsv_bytes.decode("utf-8-sig")
    reader = csv.DictReader(text.splitlines(), delimiter="\t")
    grouped: OrderedDict[str, dict[str, str]] = OrderedDict()
    for row in reader:
        code = (row.get("Kod") or "").strip()
        if not code:
            continue
        acc = grouped.setdefault(code, {})
        for src_col, dst in (
            ("Titel", "label"),
            ("Överordnad kod", "parent_code"),
            ("Giltig från", "valid_from"),
        ):
            val = (row.get(src_col) or "").strip()
            if val and not acc.get(dst):
                acc[dst] = val
    return [
        Code(
            code=code,
            label=acc.get("label", ""),
            parent_code=acc.get("parent_code", ""),
            valid_from=acc.get("valid_from", ""),
        )
        for code, acc in grouped.items()
    ]


def _strip_icd11_tabulation_prefix(title: str) -> str:
    """Remove WHO simple-tabulation hierarchy bullets from a title."""
    text = title.strip()
    while text.startswith("- "):
        text = text[2:].strip()
    return text


def parse_who_icd11_simple_tabulation(zip_bytes: bytes) -> list[Code]:
    """Parse WHO's ICD-11 MMS simple tabulation zip into `Code` rows.

    The zip carries a TSV-like `.txt` with both `Title` (requested language) and
    `TitleEN`. Chapters/blocks have no ICD-11 code; keep only category rows with
    a non-empty `Code`.
    """
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        member = next(
            (
                name
                for name in zf.namelist()
                if name.endswith(".txt") and "SimpleTabulation-ICD-11-MMS" in name
            ),
            None,
        )
        if member is None:
            raise SystemExit("ICD-11: no SimpleTabulation .txt member found")
        text = zf.read(member).decode("utf-8-sig")

    rows = list(csv.DictReader(text.splitlines(), delimiter="\t"))
    code_by_uri: dict[str, str] = {}
    for row in rows:
        code = (row.get("Code") or "").strip()
        if not code:
            continue
        for field in ("Linearization URI", "Foundation URI"):
            uri = (row.get(field) or "").strip()
            if uri:
                code_by_uri[uri] = code

    out: list[Code] = []
    seen: set[str] = set()
    for row in rows:
        if (row.get("ClassKind") or "").strip() != "category":
            continue
        code = (row.get("Code") or "").strip()
        if not code:
            continue
        if code in seen:
            raise SystemExit(f"ICD-11: duplicate code {code!r}")
        seen.add(code)
        label_en = _strip_icd11_tabulation_prefix(row.get("TitleEN") or "")
        label = _strip_icd11_tabulation_prefix(row.get("Title") or "") or label_en
        parent_code = code_by_uri.get((row.get("Parent") or "").strip(), "")
        out.append(
            Code(code=code, label=label, label_en=label_en, parent_code=parent_code)
        )
    return out


def parse_xls(xls_bytes: bytes, src: Source) -> list[Code]:
    """Parse a legacy Socialstyrelsen `.xls` (BIFF) classification.

    One row per code; column indices come from the `Source` (these old
    workbooks have no consistent header names, just positional `Kod`/`Text`).
    `xlrd` reads the BIFF format openpyxl can't — run with `--with xlrd`.
    """
    try:
        import xlrd
    except ImportError as exc:
        raise SystemExit(
            f"{src.key}: reading .xls needs xlrd — rerun with "
            "`uv run --with xlrd python scripts/fetch_sos_classifications.py`"
        ) from exc

    wb = xlrd.open_workbook(file_contents=xls_bytes)
    sheet = wb.sheet_by_name(src.sheet) if src.sheet else wb.sheet_by_index(0)
    out: list[Code] = []
    for r in range(src.skip_rows, sheet.nrows):

        def cell(c: int | None) -> str:
            if c is None or c >= sheet.ncols:
                return ""
            v = sheet.cell_value(r, c)  # noqa: B023
            if isinstance(v, float) and v.is_integer():
                v = int(v)
            return str(v).strip()

        code = cell(src.code_col)
        if not code:
            continue
        out.append(
            Code(code=code, label=cell(src.label_col), parent_code=cell(src.parent_col))
        )
    return out


_XLSX_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_REL_NS = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"


def _read_xlsx_sheet(xlsx_bytes: bytes, sheet_name: str) -> list[list[str]]:
    """Read one sheet of a modern `.xlsx` into rows of string cells (stdlib).

    Avoids openpyxl, whose stylesheet reader crashes on some agency exports
    (e.g. the NordDRG definition data). Resolves shared strings, inline strings
    (`t="inlineStr"`), and inline `t="str"` values. Cells are placed by their
    column letter so blank columns aren't silently dropped.
    """
    with zipfile.ZipFile(io.BytesIO(xlsx_bytes)) as zf:
        names = zf.namelist()
        # Resolve sheet name -> r:id -> rels Target -> worksheet path. (sheetId
        # is NOT the file ordinal — they coincide in some exports but not the
        # Skatteverket workbook, so go through the relationship like Excel does.)
        wbroot = ET.fromstring(zf.read("xl/workbook.xml"))
        rid = next(
            (
                sh.get(f"{_REL_NS}id")
                for sh in wbroot.iter(f"{_XLSX_NS}sheet")
                if sh.get("name") == sheet_name
            ),
            None,
        )
        if rid is None:
            raise SystemExit(f"xlsx: sheet {sheet_name!r} not found")
        relsroot = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        target = next((r.get("Target") for r in relsroot if r.get("Id") == rid), None)
        if target is None:
            raise SystemExit(f"xlsx: no rels target for sheet {sheet_name!r}")
        path = target.lstrip("/") if target.startswith("/") else "xl/" + target
        if path not in names:
            raise SystemExit(f"xlsx: {path} missing for sheet {sheet_name!r}")

        shared: list[str] = []
        if "xl/sharedStrings.xml" in names:
            sroot = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            shared = [
                "".join(t.text or "" for t in si.iter(f"{_XLSX_NS}t")) for si in sroot
            ]

        root = ET.fromstring(zf.read(path))
    rows: list[list[str]] = []
    for row in root.iter(f"{_XLSX_NS}row"):
        cells: dict[int, str] = {}
        width = 0
        next_col = 0
        for c in row.findall(f"{_XLSX_NS}c"):
            ref = "".join(ch for ch in c.get("r", "") if ch.isalpha())
            # NordDRG omits the `r` column ref → fall back to sequential order.
            col = _col_index(ref) if ref else next_col
            next_col = col + 1
            t = c.get("t")
            if t == "inlineStr":
                is_el = c.find(f"{_XLSX_NS}is")
                val = (
                    "".join(tt.text or "" for tt in is_el.iter(f"{_XLSX_NS}t"))
                    if is_el is not None
                    else ""
                )
            else:
                v = c.find(f"{_XLSX_NS}v")
                raw = v.text if v is not None and v.text is not None else ""
                val = shared[int(raw)] if t == "s" and raw else raw
            cells[col] = val.strip()
            width = max(width, col + 1)
        rows.append([cells.get(i, "") for i in range(width)])
    return rows


def _col_index(letters: str) -> int:
    """Excel column letters (A, B, …, AA) -> 0-based index."""
    idx = 0
    for ch in letters:
        idx = idx * 26 + (ord(ch.upper()) - ord("A") + 1)
    return idx - 1


def parse_xlsx_by_headers(xlsx_bytes: bytes, src: Source) -> list[Code]:
    """Parse a modern `.xlsx` sheet, mapping columns by header NAME.

    Finds the header row (first row containing ``code_header``) so a sheet with
    a title row above the header (e.g. Skatteverket 'Landskod') still maps
    correctly. One `Code` per data row with a non-empty code.
    """
    assert src.sheet and src.code_header
    rows = _read_xlsx_sheet(xlsx_bytes, src.sheet)
    hdr_idx = next((i for i, r in enumerate(rows) if src.code_header in r), None)
    if hdr_idx is None:
        raise SystemExit(
            f"{src.key}: header {src.code_header!r} not found in {src.sheet!r}"
        )
    header = rows[hdr_idx]
    col = {name: header.index(name) for name in header if name}

    def at(row: list[str], name: str | None) -> str:
        i = col.get(name) if name else None
        return row[i].strip() if i is not None and i < len(row) else ""

    out: list[Code] = []
    for row in rows[hdr_idx + 1 :]:
        code = at(row, src.code_header)
        if not code:
            continue
        out.append(
            Code(
                code=code,
                label=at(row, src.label_header),
                label_en=at(row, src.label_en_header),
                valid_from=at(row, src.valid_from_header),
                valid_to=at(row, src.valid_to_header),
            )
        )
    return out


def fetch_norddrg_xlsx(cache_dir: Path, *, force: bool) -> tuple[bytes, str]:
    """Run the NordDRG Explorer FileExport flow → (xlsx_bytes, version_name).

    Three steps: GetDrgVersions → DownloadDefdata (generates a timestamped file,
    returns its name) → FileExport/<name> (the actual .xlsx). Cached under a
    stable per-version name so 'drg' and 'mdc' share one download.
    """
    versions = json.loads(_http_get(f"{_NORDDRG_API}/Defdata/GetDrgVersions"))
    latest = versions[0]  # API returns newest-first (SWE2026PR1, …)
    guid, name = latest["Guid"], latest["Name"]
    dest = cache_dir / f"norddrg-{name}.xlsx"
    if dest.is_file() and not force:
        _log(f"  cached  {dest.name} ({dest.stat().st_size:,} B)")
        return dest.read_bytes(), name
    q = urllib.parse.urlencode({"drgVersionGuid": guid, "defdataName": name})
    fname = (
        _http_get(f"{_NORDDRG_API}/Defdata/DownloadDefdata?{q}")
        .decode()
        .strip()
        .strip('"')
    )
    _log(f"  GET     NordDRG {name} → {fname}")
    data = _http_get(f"{_NORDDRG_API}/FileExport/{urllib.parse.quote(fname)}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(data)
    _log(f"  saved   {dest.name} ({len(data):,} B)")
    return data, name


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def write_csv(codes: list[Code], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(CSV_HEADER)
        for c in codes:
            writer.writerow(
                [c.code, c.label, c.label_en, c.parent_code, c.valid_from, c.valid_to]
            )


def source_location(src: Source) -> str:
    if src.url:
        return src.url
    if src.attachment:
        return f"{EHALSA_HOST}/.../{src.attachment}"
    if src.merge_attachments:
        return ", ".join(f"{EHALSA_HOST}/.../{a}" for a in src.merge_attachments)
    if src.kind == "norddrg_api":
        return f"{_NORDDRG_API}/Defdata/DownloadDefdata"
    return "(source resolved by parser)"


def fetch_source(src: Source, cache_dir: Path, out_dir: Path, *, force: bool) -> dict:
    _log(f"\n[{src.key}] {src.name}")
    if src.kind == "mpa_zip":
        assert src.url and src.member_suffix
        raw = download(src.url, cache_dir / Path(src.url).name, force=force)
        codes = parse_atc(raw, src.member_suffix)
        source_url = src.url
    elif src.kind == "ehalsa_tsv":
        assert src.attachment
        url = confluence_attachment_url(EHALSA_PAGE_ID, src.attachment)
        raw = download(url, cache_dir / src.attachment, force=force)
        codes = parse_tsv(raw)
        source_url = url
    elif src.kind == "who_icd11_simple_tabulation":
        assert src.url
        raw = download(src.url, cache_dir / Path(src.url).name, force=force)
        codes = parse_who_icd11_simple_tabulation(raw)
        source_url = src.url
    elif src.kind == "ehalsa_tsv_merge":
        # Fetch each disjoint code space, then concatenate + dedup on the code
        # column (first attachment's row wins on a collision — KMÅ before KKÅ).
        # Provenance for both raw files survives in the manifest entry built
        # below (source_urls list + per-file raw_sha256), keyed off the per-file
        # `attachment` stem so a future re-run is auditable.
        assert src.merge_attachments
        merged: OrderedDict[str, Code] = OrderedDict()
        raw_by_stem: dict[str, bytes] = {}
        urls: list[str] = []
        for attachment in src.merge_attachments:
            url = confluence_attachment_url(EHALSA_PAGE_ID, attachment)
            raw_one = download(url, cache_dir / attachment, force=force)
            stem = Path(attachment).stem
            raw_by_stem[stem] = raw_one
            urls.append(url)
            for code in parse_tsv(raw_one):
                merged.setdefault(code.code, code)
        codes = list(merged.values())
        out_path = out_dir / f"{src.key}.csv"
        write_csv(codes, out_path)
        n_retired = sum(1 for c in codes if c.valid_to)
        _log(f"  wrote   {out_path.name}: {len(codes):,} codes ({n_retired:,} retired)")
        return {
            "key": src.key,
            "name": src.name,
            "publisher": src.publisher,
            "source_urls": urls,
            "raw_sha256": {stem: _sha256(b) for stem, b in raw_by_stem.items()},
            "raw_bytes": sum(len(b) for b in raw_by_stem.values()),
            "n_codes": len(codes),
            "n_retired": n_retired,
            "out_file": out_path.name,
            "fetched_at": datetime.now(UTC).isoformat(timespec="seconds"),
        }
    elif src.kind == "sos_xls":
        assert src.url
        raw = download(src.url, cache_dir / Path(src.url).name, force=force)
        codes = parse_xls(raw, src)
        source_url = src.url
    elif src.kind == "xlsx_url":
        assert src.url
        raw = download(
            src.url, cache_dir / Path(src.url).name.split("?")[0], force=force
        )
        codes = parse_xlsx_by_headers(raw, src)
        source_url = src.url
    elif src.kind == "norddrg_api":
        raw, version = fetch_norddrg_xlsx(cache_dir, force=force)
        codes = parse_xlsx_by_headers(raw, src)
        source_url = f"{_NORDDRG_API}/Defdata/DownloadDefdata ({version})"
    else:  # pragma: no cover - registry is closed
        raise SystemExit(f"unknown source kind {src.kind!r}")

    out_path = out_dir / f"{src.key}.csv"
    write_csv(codes, out_path)
    n_retired = sum(1 for c in codes if c.valid_to)
    _log(f"  wrote   {out_path.name}: {len(codes):,} codes ({n_retired:,} retired)")
    return {
        "key": src.key,
        "name": src.name,
        "publisher": src.publisher,
        "source_url": source_url,
        "raw_sha256": _sha256(raw),
        "raw_bytes": len(raw),
        "n_codes": len(codes),
        "n_retired": n_retired,
        "out_file": out_path.name,
        "fetched_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help="normalized CSV output dir",
    )
    ap.add_argument(
        "--cache-dir",
        type=Path,
        default=DEFAULT_CACHE_DIR,
        help="raw download cache dir",
    )
    ap.add_argument(
        "--only",
        nargs="+",
        metavar="KEY",
        help=f"subset of: {', '.join(SOURCE_BY_KEY)}",
    )
    ap.add_argument(
        "--force", action="store_true", help="re-download cached raw sources"
    )
    ap.add_argument("--list", action="store_true", help="list sources and exit")
    args = ap.parse_args(argv)

    if args.list:
        for s in SOURCES:
            print(f"{s.key:12s} {s.name}\n{'':12s} {source_location(s)}")
        return 0

    selected = SOURCES
    if args.only:
        unknown = [k for k in args.only if k not in SOURCE_BY_KEY]
        if unknown:
            ap.error(
                f"unknown source(s): {', '.join(unknown)}; choose from {', '.join(SOURCE_BY_KEY)}"
            )
        selected = tuple(SOURCE_BY_KEY[k] for k in args.only)

    _log(f"Fetching {len(selected)} classification(s) → {args.out_dir}")
    manifest = [
        fetch_source(s, args.cache_dir, args.out_dir, force=args.force)
        for s in selected
    ]

    manifest_path = args.out_dir / "manifest.json"
    existing = {}
    if manifest_path.is_file():
        existing = {
            m["key"]: m
            for m in json.loads(manifest_path.read_text()).get("sources", [])
        }
    existing.update({m["key"]: m for m in manifest})
    manifest_path.write_text(
        json.dumps({"sources": list(existing.values())}, ensure_ascii=False, indent=2)
        + "\n",
        encoding="utf-8",
    )
    _log(f"\nManifest → {manifest_path}")
    total = sum(m["n_codes"] for m in manifest)
    _log(f"Done: {len(manifest)} classification(s), {total:,} codes total.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
