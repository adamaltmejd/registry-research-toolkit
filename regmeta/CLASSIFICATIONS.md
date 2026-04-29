# Adding canonical code CSVs

Maintainer guide for populating `valid_codes_file` for each classification.
Background: see `DESIGN.md` § "Classifications" → "Canonical vs observed codes".

## How it works

Each `[[classification]]` in `regmeta/classifications.toml` may declare a
`valid_codes_file = "<filename>.csv"`. The CSV lives under
`regmeta/input_data/classifications/` (gitignored — these are local
maintainer artifacts, just like `input_data/SCB/`). At build time:

1. Every code in the CSV is ensured to exist in `value_code` (codes that
   never appeared in any register get inserted as canonical-but-unobserved).
2. Every `classification_code` row is marked `is_valid = 1` (in CSV) or
   `is_valid = 0` (observed-only noise).
3. `classification.valid_code_count` is set to the canonical count.

Without a CSV, every code carries `is_valid = NULL` ("validity unknown").

## CSV format

- Filename: lowercased classification `short_name`, e.g. `sun2000-niva.csv`.
- Encoding: UTF-8.
- Header (exact): `vardekod,vardebenamning`
- One code per row. Whitespace is trimmed on both columns at load time.
- Duplicate `vardekod` values → build fails.

Example:

```csv
vardekod,vardebenamning
000,Övrig och ospecificerad förskoleutbildning
001,Förskola
002,Förskoleklass
```

## Workflow

1. Find SCB's authoritative code list (often a downloadable Excel/CSV on
   the classification's documentation page).
2. Save as `regmeta/input_data/classifications/<short_name>.csv` with the
   header above.
3. Add `valid_codes_file = "<short_name>.csv"` to the matching seed entry
   in `regmeta/classifications.toml`.
4. Run `regmeta maintain build-db --input-dir regmeta/input_data/`.
   Build output reports per-classification: canonical / observed-only /
   canonical-but-unobserved counts.
5. Spot-check with
   `regmeta get classification <SHORT_NAME> --codes --only-valid`
   and review observed-only and canonical-but-unobserved lists for
   data-quality issues (mislabeled codes, truncated labels, etc.).

## Status overview

69 classifications (47 per-year LKF entries + 22 others). All currently
declared in `classifications.toml` ship with a `valid_codes_file`.

| short_name | status | code_count | valid | notes |
|---|---|---:|---:|---|
| `SUN2000-NIVA` | ✓ | 172 | 86 | maintainer-extracted CSV |
| `SUN2000-INRIKTNING` | ✓ | 680 | 647 | maintainer-extracted CSV |
| `SUN2000-GRUPP` | ✓ | 140 | 139 | from `utbildningsgrupper-sun-2000.xlsx` |
| `SUN2020-NIVA` | ✓ | 106 | 73 | from `sun-2020_niva_inriktning2.xlsx` |
| `SUN2020-INRIKTNING` | ✓ | 551 | 531 | from `sun-2020_niva_inriktning2.xlsx` |
| `SUN2020-GRUPP` | ✓ | 231 | 229 | merged main + `sun2020grp_detalj.xlsx` |
| `SSYK2012` | ✓ | 1 125 | 635 | from `ssyk-2012-koder.xlsx`; UTGÅR rows filtered |
| `SNI2007` | ✓ | 6 529 | 3 326 | from `sni2007.xlsx` |
| `SNI2002` | ✓ | 3 151 | 3 088 | from `sni2002.xlsx` |
| `SNI92` | ✓ | 3 131 | 3 051 | from `sni92.xlsx` |
| `SNI69` | ✓ | 1 286 | 1 055 | from `sni69.xlsx` |
| `SSYK96` | ✓ | ~830 | 501 | scraped from ssyksok.scb.se |
| `NIVA-OLD` | ✓ | 32 | 7 | hand-written from LISA/UREG docs |
| `NIVA-GROV` | ✓ | 14 | 5 | hand-written from LISA/UREG docs |
| `SUN1996` | ✓ | 4 818 | 4 601 | scraped from metadata.scb.se klassdb (2-, 3-, 5-pos levels) |
| `LKF{1980..2026}` | ✓ | 47 entries | varies | per-year split, see "Geography — LKF" |
| `ISCED2011` | ✓ | 53 | 66 | full UNESCO spec via SSB Klass 3426 |
| `ISCED-F2013` | ✓ | 164 | 218 | full UNESCO spec via SSB Klass 3428 (+ 2 missing UNESCO codes) |
| `SEKTOR2000` | ✓ | 53 | 73 | INSEKT 2000 from metadata.scb.se klassdb (Sektor + Undersektor + Delsektor) |
| `JURFORM2000` | ✓ | 45 | 36 | scraped from metadata.scb.se klassdb |
| `JURFORM2020` | ✓ | 39 | 36 | scraped from metadata.scb.se klassdb |
| `AGARKAT2000` | ✓ | 19 | 11 | scraped from metadata.scb.se klassdb (Nivå 1 + Nivå 2) |
| `AGARKAT2020` | ✓ | 10 | 11 | scraped from metadata.scb.se klassdb (Nivå 1 + Nivå 2) |

`code_count` and `valid` columns reflect the latest build. `valid` may exceed
the CSV row count when SCB exports carry the same canonical code under
multiple labels (each label variant becomes its own `value_code` row, all
marked `is_valid=1`).

## Done — extraction details

The conversion was a one-shot script run against XLSX files in `/tmp/scb_xlsx/`.
For each, this section lists where the source came from and which sheet/columns
to use. If you need to re-extract, the conversion logic is straightforward
enough to redo from these notes — there's no committed extraction script
because each file's quirks are different and the result is what we ship.

### SUN — Svensk utbildningsnomenklatur

Page: <https://www.scb.se/dokumentation/klassifikationer-och-standarder/svensk-utbildningsnomenklatur-sun/>

#### `SUN2000-NIVA`, `SUN2000-INRIKTNING`

User-provided. Likely extracted manually from `sun-2000_niva_inriktning.xlsx`
on the SCB SUN page. CSVs include all hierarchy levels (1-, 2-, 3-digit for
NIVA; 1- through 4-character including suffixes like `010a` for INRIKTNING).

#### `SUN2020-NIVA`, `SUN2020-INRIKTNING`

Source file: `sun-2020_niva_inriktning2.xlsx`
([download](https://www.scb.se/contentassets/aeeedec0e28c465aa524429407dcd5ba/sun-2020_niva_inriktning2.xlsx))

Layout: each sheet has multiple "Kod | Benämning | (gap)" column triples
side by side, one per hierarchy level (1-siffer, 2-siffer, 3-siffer).
Header at row 3.

Extraction rules:

- Use sheets `Nivåer, klartext` and `Inriktning, klartext` (skip the
  `_alt` variants — those are legacy mappings).
- For each "Kod" position in the header, read consecutive rows until the
  first blank in that column. Crucial: the sheet has a legacy-mapping
  table further down with code `7` "Forskarutbildning" — stopping at the
  per-column blank avoids picking up these rogue rows.

#### `SUN2000-GRUPP`

Source file: `utbildningsgrupper-sun-2000.xlsx`
([download](https://www.scb.se/contentassets/aeeedec0e28c465aa524429407dcd5ba/utbildningsgrupper-sun-2000.xlsx))

Layout: code in column 1 (e.g. `01Z`), label in column 2, **multi-line**
labels span 2–3 rows (continuation rows have empty col 1).
Section headers like `Allmän utbildning (0)` appear in col 2 with no code
in col 1.

Extraction: walk rows. When col 1 has a code → start new entry (label = col 2).
When col 1 is empty but col 2 has text → append to current label.
Blank row → flush current entry.

#### `SUN2020-GRUPP`

**Two source files merged**:

1. `utbildningsgrupper-sun-20202.xlsx`
   ([download](https://www.scb.se/contentassets/aeeedec0e28c465aa524429407dcd5ba/utbildningsgrupper-sun-20202.xlsx))
   — main groups, 96 codes (3-char like `01Z`). Header at row 3, code in col 0,
   label in col 1. Skip row 4 ("0 Allmän utbildning" section header — code is
   None).
2. `sun2020grp_detalj_schema-260223_utskrift_260223.xlsx`
   ([download](https://www.scb.se/contentassets/aeeedec0e28c465aa524429407dcd5ba/sun2020grp_detalj_schema-260223_utskrift_260223.xlsx))
   — detailed groups, 133 codes (4-char like `01ZA`). Same multi-line layout
   as SUN 2000 GRUPP, but code in col 0 instead of col 1.

Both code spaces map to `vardemangdsversion` strings already grouped under
`SUN2020-GRUPP` (`SUN 2020 - Gruppering` and `SUN 2020 - Gruppering - Detaljerad`),
so they're merged into one CSV. The detailed file contains a `23XA` 2025
addition that hasn't reached our register exports yet — surfaces as
canonical-but-unobserved.

### SSYK — Standard för svensk yrkesklassificering

Page: <https://www.scb.se/dokumentation/klassifikationer-och-standarder/standard-for-svensk-yrkesklassificering-ssyk/>

#### `SSYK2012`

Source file: `ssyk-2012-koder.xlsx`
([download](https://www.scb.se/contentassets/0c0089cc085a45d49c1dc83923ad933a/ssyk-2012-koder.xlsx))

Use sheet `Hela strukturen`. Code in col 0, label in col 1. Codes range
from 1 (1-digit) through 9999 (4-digit) following SSYK's nested hierarchy.

Edge case: 8 placeholder rows have label `UTGÅR[]` ("removed") for codes
that were retired between revisions (e.g. `215`, `252`, `263`). Filter these
out — they're not real canonical codes.

#### `SSYK96`

No clean structured XLSX exists. The MIS 1998:3 PDF is scanned (no
extractable text), and the conversion XLSX
(`webb_nyckel_ssyk96_ssyk2012_20160905.xlsx`) has 4-digit codes but
without canonical category labels (only individual job titles).

Approach: derive the canonical 4-digit codes from the conversion file's
`Nyckel_4siffer` sheet (355 codes), generate parent levels (1-/2-/3-digit
prefixes → 505 codes total), then scrape category labels from
<https://ssyksok.scb.se/SsykSok/SSYK96/{code}> at ~150ms/req. SSYK-Sök's
HTML has a stable `<h2><strong>{code}</strong> {label}</h2>` pattern.
Yields ~501 codes (a handful 404 in SSYK-Sök).
The CSV ships as a one-off snapshot — it's not regenerated by a script
in this repo since the scrape is a single-use extraction. If SCB's
naming changes, re-extract by running the scrape inline.

### SNI — Svensk näringsgrensindelning

Page: <https://www.scb.se/dokumentation/klassifikationer-och-standarder/standard-for-svensk-naringsgrensindelning-sni/>

Source files (one XLSX per version):

- `sni2007.xlsx` ([download](https://www.scb.se/contentassets/d43b798da37140999abf883e206d0545/sni2007.xlsx))
- `sni2002.xlsx` ([download](https://www.scb.se/contentassets/d43b798da37140999abf883e206d0545/sni2002.xlsx))
- `sni92.xlsx` ([download](https://www.scb.se/contentassets/d43b798da37140999abf883e206d0545/sni92.xlsx))
- `sni69.xlsx` ([download](https://www.scb.se/contentassets/d43b798da37140999abf883e206d0545/sni69.xlsx))

Layout: one sheet per hierarchy level (Detaljgrupp/Undergrupp/Grupp/
Huvudgrupp/Avdelning, plus extras for SNI 69). Column structure varies
across sheets:

- Detaljgrupp/Undergrupp/Grupp: `(Officiell kodstruktur, flat code, Benämning)` at cols 0,1,2.
- Huvudgrupp/Avdelning: `(code, Benämning, ...)` at cols 0,1.

Extraction rule: find the column header starting with `Benämning` (some
sheets have a trailing space) — the **flat code is in the column
immediately to the left** regardless of sheet. Use that pair across all
sheets to build the union.

Notes:

- Use the flat code (no dots), since that's what registers store.
- For SNI 69, the lowest-level sheet is "Sexsiffer" (6-digit); for the
  others it's "Femsiffer" (5-digit).
- SNI 69 has 113 1-/2-/3-digit prefix codes that don't appear in our
  register exports — surfaces as canonical-but-unobserved.
- SNI 2007 has the largest observed-only set (~3 200) because data
  carries codes in many alt notations not in the canonical list (dotted
  `01.110`, ranges `102-103`, letters from Avdelning) — most are real
  references in alt format, not noise.

### SUN 1996, SEKTOR/INSEKT, JURFORM, AGARKAT — scraped from SCB Klassdb

Page: <https://metadata.scb.se/klassdb.aspx>

The SCB Klassifikationsdatabasen UI uses ASP.NET WebForms `__doPostBack`
navigation, so there are no stable deep-link URLs. The data is reachable
through the values pane (`tvKlass2`) once a version is selected via
`__doPostBack('tvKlass', 'sVMF\\<vmf>\\VMK\\<vmk>\\VMV\\<vmv>')`. Each
version then exposes one or more "Nivå N" levels addressed by appending
`\\VMN\\<id1>\\<id2>...`.

These were one-shot scrapes (browser-driven JS in the page DOM, output
piped via `console.log` or `Blob` download). The CSVs ship as snapshots —
re-extract only if SCB updates the codes. Reference IDs (in case
re-scrape is needed):

| short_name | vmk | vmv | levels |
|---|---|---|---|
| `SUN1996` | 801 | 781 | 2-pos / 3-pos / 5-pos (4601 codes) |
| `SEKTOR2000` | 652 | 491 | Sektor / Undersektor / Delsektor (73 codes) |
| `JURFORM2000` | 653 | 493 | Nivå 1 (36 codes) |
| `JURFORM2020` | 653 | 80811 | Nivå 1 (36 codes) |
| `AGARKAT2000` | 654 | 495 | Nivå 1 + Nivå 2 (11 codes) |
| `AGARKAT2020` | 654 | 80806 | Nivå 1 + Nivå 2 (11 codes) |

Edge cases:

- `SEKTOR2000` toml lists two vardemängdsversionen: the canonical
  `Standard för institutionell sektorindelning 2000` (53 observed
  3-digit codes — match Delsektor) and a register-local `Sektor 2000`
  (23 codes with a different 1-2 digit code scheme that mostly stays
  `is_valid=0`). The canonical CSV serves the standard one.
- `JURFORM 2020` is identical to `JURFORM 2000` except code `84`
  changed from "Landsting" to "Regioner".
- `AGARKAT` 2000 and 2020 differ only in code `30/3` Region(kontrollerade)
  rename.

### Geography — LKF (per-year split)

Page: <https://www.scb.se/hitta-statistik/regional-statistik-och-kartor/regionala-indelningar/lan-och-kommuner/>

LKF is split into 47 per-year classifications `LKF1980` … `LKF2026`,
each with its own canonical CSV. Single-year codes change across the
sequence (Skåne 1997, Västra Götaland 1998, parish mergers ongoing), so
each year is its own published snapshot — 665 codes had conflicting
labels under the previous unified entry. The data already
disambiguates: `variable_instance.vardemangdsversion` is year-stamped
(`LKF 1990-01-01/...`), so the split is purely seed-side — no schema
change, no FK rework. `supersedes` chains the years sequentially.

CSVs come from `scripts/extract_lkf.py`:

- **1980–2015** (kommun + derived län): from `knkodnyckel.xls`, SCB's
  kommun-history file with 12 period-snapshots. Script derives 2-digit
  län codes as kommun prefixes and labels them via a year-aware map
  (handles 1997 Skåne/Dalarna and 1998 Västra Götaland reforms). ~310
  codes/year. **Församlings (6-digit) NOT included** — pre-2016 parishes
  are a remaining gap.
- **2016–2026** (full LKF — län + kommun + församling): per-year
  `lkf{year}.xls`/`.xlsx`. Script handles the three different sheet
  layouts SCB has used (2018–19 / 2021–22 / 2023+) and the non-standard
  filenames for 2020 (`_justerad`) and 2026 (date-stamped subdir). ~1500–1700
  codes/year.
- **Pre-1980 / 2015 parishes**: not wired. The script can produce
  1974–1979 CSVs but the regmeta data has no instances for those years
  so no seed entries exist for them. The 2015 PDF can be OCR'd to
  recover that year's parishes.

Re-run extraction:

```bash
uv run --with openpyxl --with xlrd python scripts/extract_lkf.py \
    --out regmeta/input_data/classifications/

# Optional: also download PDFs for OCR / cross-checking:
uv run --with openpyxl --with xlrd python scripts/extract_lkf.py \
    --download-pdfs
```

The current toml entries were generated by `--emit-toml` then spliced in.
If SCB adds a new yearly snapshot, add an `LKF{year}` block manually (or
re-run --emit-toml and merge).

Then add `LKF{year}` seed entries (`--emit-toml` prints starters; the
year-string variants need to be reconciled against the existing LKF entry
in `classifications.toml`). The current single `LKF` entry should then be
removed, with each year's vardemangdsversion strings moved to the
appropriate `LKF{year}` entry.

### Education — ISCED 2011

`ISCED2011` canonical CSV (`isced2011.csv`) was sourced from SSB's Klass
register, version 3426 (Norwegian translation of UNESCO ISCED 2011 with
all three levels: 1-digit, 2-digit, 3-digit — 66 codes total). All 39
observed Nivå codes match. The 3-digit codes I'd previously flagged as
"SCB-specific" turn out to be standard UNESCO level-3 detail codes.

The "ISCED 2011 AES" vardemängd uses different padded codes (`000`,
`200`, `300`, `302`, …) which appear to be SCB-specific. Only `100`
matches the canonical set; the other 13 stay as `is_valid=0`.

### UNESCO ISCED-F 2013

`ISCED-F2013` canonical CSV (`isced-f2013.csv`) was sourced from SSB's
Klass register, version 3428 (UNESCO ISCED-F 2013 — 216 codes spanning
broad/narrow/detailed fields). 124/126 observed `ISCED-F 2013 -
Inriktning` codes match. The two SSB drops (`0110` Education NFD, `0739`
Architecture and construction NEC) were re-added since they're standard
UNESCO and present in SCB data.

The second vardemängd `ISCED F 2013` (with a trailing space in the source) carries 38
codes mostly in Swedish (`Pedagogik och lärarutbildning`, etc.) — only
12 match UNESCO. This looks like a register-local mapping mislabelled as
ISCED-F; left at `is_valid=0` for the non-matching codes.
