#!/usr/bin/env python3
"""Parse LISA bakgrundsfakta PDF into per-variable and topic markdown files.

Outputs to regmeta/docs/lisa/ with Obsidian-style YAML frontmatter,
wiki-style links, and hierarchical tags.

Two-step workflow:

1. Convert PDFs to markdown with marker (requires GEMINI_API_KEY):

    # Bakgrundsfakta (the big 467-page reference doc):
    caffeinate -i uv run marker_single lisa-bakgrundsfakta-1990-2017.pdf \
      --use_llm --gemini_model_name gemini-3-flash-preview \
      --gemini_api_key "$GEMINI_API_KEY" \
      --output_dir regmeta/docs/_raw \
      --disable_image_extraction \
      --MarkdownRenderer_keep_pageheader_in_output \
      --disable_multiprocessing

    # Förändringar and other small docs:
    for pdf in lisa-2019---forandringar.pdf lisa_2020-forandringar.pdf \
               lisa-2022-forandringar.pdf lisa_2023-forandringar.pdf \
               hushallsinformation-i-lisa-2011-.pdf; do
      uv run marker_single "$pdf" --use_llm \
        --gemini_model_name gemini-3-flash-preview \
        --gemini_api_key "$GEMINI_API_KEY" \
        --output_dir regmeta/docs/_raw \
        --disable_image_extraction
    done

2. Split markdown into per-variable files with this script:

    uv run python scripts/parse_lisa_docs.py \
      --cached-md regmeta/docs/_raw/lisa-bakgrundsfakta-1990-2017/lisa-bakgrundsfakta-1990-2017.md \
      --forandringar regmeta/docs/_raw/*-forandringar*/*.md \
                     regmeta/docs/_raw/hushallsinformation*/*.md \
      --out regmeta/docs/lisa

Notes:
- marker + Gemini 3 Flash costs ~$1-2 for the full bakgrundsfakta.
- --disable_multiprocessing avoids a surya batch-size crash on some pages.
- --MarkdownRenderer_keep_pageheader_in_output is critical: without it,
  marker drops ~30 variable headers it misclassifies as page headers.
- The Variabelförteckning table (pp 12-24) is used as the ground truth
  for variable names, not the regmeta database (LISA is a composite DB
  whose variables are registered under source registers like RTB, RAMS).
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "regmeta" / "docs" / "lisa"

# Footer pattern from pymupdf4llm output
FOOTER_RE = re.compile(
    r"^(?:\d+\s+)?SCB\s*[–-]\s*(?:LISA|Föränd)",
    re.IGNORECASE,
)
PAGE_NUM_RE = re.compile(r"^\d{1,3}\s*$")

# HTML tags injected by marker (span anchors, sup footnotes, bold/italic)
HTML_TAG_RE = re.compile(r"</?(?:span|sup)[^>]*>")
# Bare URLs/emails (MD034) — wrap in angle brackets
BARE_EMAIL_RE = re.compile(
    r"(?<![<\[(/])(\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b)(?![>\]])"
)
BARE_URL_RE = re.compile(
    r"(?<![<\[(/])((?:www\.)[A-Za-z0-9.-]+\.[A-Za-z]{2,})(?![>\])])"
)
# Broken image refs left by marker when --disable_image_extraction is used
BROKEN_IMG_RE = re.compile(r"!\[\]\([^)]*\)\s*")

# Bold text patterns for variable headers
# Pattern 1: **Display Name ColumnName**  (single line)
# Uses greedy .+ so it grabs the LAST CamelCase word as column name
SINGLE_LINE_RE = re.compile(r"^\*\*(.+)\s+([A-ZÅÄÖ][A-Za-zÅÄÖåäö0-9_]{1,40})\*\*\s*$")
# Pattern 2: **ColumnName** alone on a line (split header)
BOLD_ONLY_RE = re.compile(r"^\*\*([A-ZÅÄÖ][A-Za-zÅÄÖåäö0-9_]{1,40})\*\*\s*$")
# Pattern 3: ## **ColumnName** (heading-style, any heading level)
HEADING_COL_RE = re.compile(r"^#{2,4}\s*\*\*([A-ZÅÄÖ][A-Za-zÅÄÖåäö0-9_]{1,40})\*\*\s*$")
# Pattern 4: table header |**Display**|**ColName**| or | Display | ... | ColName |
TABLE_HEADER_RE = re.compile(
    r"^\|\*\*(.+?)\*\*\|\*\*([A-ZÅÄÖ][A-Za-zÅÄÖåäö0-9_]{1,40})\*\*\|"
)
TABLE_HEADER_PLAIN_RE = re.compile(
    r"^\|\s*([^|]+?)\s*\|[^|]*\|\s*([A-ZÅÄÖ][A-Za-zÅÄÖåäö0-9_]{1,40})\s*\|"
)
# Bold display name on its own line (preceding a split column name)
BOLD_DISPLAY_RE = re.compile(r"^\*\*([^*]+)\*\*\s*$")

# Section headings that mark topic boundaries (## or ####)
SECTION_HEADING_RE = re.compile(r"^#{2,4}\s*\*\*(.+?)\*\*\s*$")

# Section heading → primary topic tag for variables in that section
SECTION_TOPIC: dict[str, str] = {
    "Demografiska variabler": "topic/demographic",
    "Demografiska variabler – För familj": "topic/demographic",
    "Utbildningsvariabler": "topic/education",
    "Sysselsättningsvariabler": "topic/employment",
    "Förvärvsarbetande": "topic/employment",
    "Yrke": "topic/employment",
    "Näringsgren": "topic/employment",
    "Inkomstvariabler": "topic/income",
    "Förvärvsinkomst": "topic/income",
    "Kapitalinkomst": "topic/income",
    "Annan inkomst": "topic/income",
    "Ålderspension": "topic/income",
    "Tjänstepension": "topic/income",
    "Disponibel inkomst": "topic/income",
    "Familjerelaterade inkomster": "topic/income",
    "Ekonomiskt bistånd": "topic/income",
    "Bostadsbidrag": "topic/income",
    "Bostadstillägg": "topic/income",
    "Sjukförsäkring": "topic/income",
    "Arbetsmarknadsåtgärder": "topic/income",
    "Kopplingsidentiteter": "topic/identifier",
    "Individ": "topic/identifier",
    "Företag": "topic/identifier",
    "Arbetsställeidentitet": "topic/identifier",
    "Registerbaserad aktivitetsstatistik": "topic/activity-status",
    "Övriga inkomster": "topic/income",
}

# Secondary topic tags based on variable name patterns.
# These are ADDED alongside the section-derived primary tag.
SECONDARY_TAGS: list[tuple[str, str]] = [
    # Social insurance: sickness, disability, parental, rehab, injury, pension
    (
        r"^(SjukP|SjukSum|SjukFall|SjukTyp|SjukRe|SjukErs|SjukBidr|SjukPA|SjukPP)",
        "topic/social-insurance",
    ),
    (r"^ForbSjukP", "topic/social-insurance"),
    (r"^Smitt", "topic/social-insurance"),
    (r"^ArbSk", "topic/social-insurance"),
    (r"^AGSTFA$", "topic/social-insurance"),
    (r"^(Rehab|RehabErs|RehabTyp)", "topic/social-insurance"),
    (r"^(ForPeng|ForVAB|TfForPeng|ForLed|HavPeng)", "topic/social-insurance"),
    (r"^(VardBidr|KomVardBidr|NarPeng)$", "topic/social-insurance"),
    (r"^(AktErs|AktStod)", "topic/social-insurance"),
    (r"^(ForTid|FortPens|DelPens)", "topic/social-insurance"),
    (
        r"^Folk(Egen|Fort|FortSjuk|Sjuk|Ald|Hust|Bel|ATPFam|Fam)",
        "topic/social-insurance",
    ),
    (r"^ATP(egen|Fort|FortSjuk|Sjuk|Ald|Bel|Fam)", "topic/social-insurance"),
    (r"^Liv(Arb|ArbF|Yrke|Annan|Rta)", "topic/social-insurance"),
    (r"^(GenErs|TAE|BoTill|Karens_Foretagare)$", "topic/social-insurance"),
    (r"^(GarPens|InkPens|PremPens|TillPens|SumAld|SPenTill)", "topic/social-insurance"),
    (
        r"^(ITP|KTjP|STjP|STP|SBTjP|KUPens|OvrTjp|PrivPens|SumTjP|SumEftPens|AldTjPTyp)$",
        "topic/social-insurance",
    ),
    (r"^AldPens$", "topic/social-insurance"),
    (r"^(VPLErs|VPLTyp|GMUErs|GMUTyp|ForsvarErs)$", "topic/social-insurance"),
    (r"^HKapErs$", "topic/social-insurance"),
    (r"^(BostBidr|BostTill)", "topic/social-insurance"),
    (r"^(SocBidr|BidrFor|UnderHBidr|EtablErs)", "topic/social-insurance"),
    # Labour market programs / unemployment
    (r"^(ADelDag|AK14Dag|ALosDag|AStuDag|ASysDag|ANysDag)$", "topic/employment"),
    (r"^(ALKod|ArbLos|ArbLosTyp|ArbSokNov|IAKod|TillfTimDag)$", "topic/employment"),
    (r"^(AmPol|Akassa|KAS$|KASEES)", "topic/employment"),
    (r"^(ALU|OTA|AMK|RekrBidr|LarlErs)", "topic/employment"),
    (r"^(UtbBArb|UtbBLan|UtbBidr)$", "topic/employment"),
    # Study grants/loans
    (r"^(Stud$|StudMed|StudTyp)", "topic/education"),
    (r"^(SUtKun|Svux|SVux|VuxLan|KortStu)", "topic/education"),
    (r"^(UtbDok|UtbFor|SFI)$", "topic/education"),
]

# Variables that should NOT keep topic/employment when they also have
# topic/social-insurance (pension/disability, not employment)
REMOVE_EMPLOYMENT: list[str] = [
    r"^ATP(egen|Fort|FortSjuk|Sjuk|Ald|Bel|Fam)$",
    r"^Folk(Egen|Fort|FortSjuk|Sjuk|Ald|Hust|Bel|ATPFam|Fam)$",
    r"^(ForTid$|ForTidAGS|ForTidTyp)",
    r"^FortPens_",
    r"^(DelPens|DelPensTyp)$",
    r"^(SjukErs$|SjukErsGarAnd|SjukErsInkAnd|SjukErsVilAnd|SjukErs_)",
    r"^SjukBidr_",
    r"^(GenErs|TAE|NarPeng)$",
    r"^Liv(Arb|ArbF|Yrke|Annan|Rta)$",
    r"^(SumEftPens|SocInk)$",
]

# Topic file consolidation: section heading slug → (target_slug, display_name, tags)
# Sections whose slugs match a key get merged into the target file.
TOPIC_CONSOLIDATION: dict[str, tuple[str, str, list[str]]] = {}

# Build consolidation map from target definitions
_CONSOLIDATION_TARGETS: list[tuple[str, str, list[str], list[str]]] = [
    # (target_slug, display_name, tags, source_slug_patterns)
    (
        "_overview",
        "LISA — Översikt",
        ["type/overview", "topic/lisa"],
        [
            "oversikt",
            "lisa-longitudin",
            "bakgrund-till-lisa",
            "syfte",
            "omfattning",
            "anvandning",
            "tillgang-till-data",
            "referensperiod",
            "i-databasen-ingaende",
            "innehall",
            "markering-for-de-ar",
            "endast-dessa-kopplingar",
            "lisa-longitudinal-integrated",
        ],
    ),
    (
        "_methodology-education",
        "Utbildningsvariabler — Metodik och källor",
        ["type/methodology", "topic/education"],
        [
            "utbildningsvariabler",
            "kallor-som-anvands",
            "1990",
            "2002",
            "2014",
            "forandringar-i-ureg",
            "forandringar-i-kallorna",
            "validering",
            "utbildningsvariabler",
        ],
    ),
    (
        "_methodology-employment",
        "Sysselsättningsvariabler — Metodik och avgränsning",
        ["type/methodology", "topic/employment"],
        [
            "forvarvsarbetande",
            "val-av-november",
            "tackningsproblem",
            "avgransningen-av-forvarvsarbetande",
            "foretagarpopulationen",
            "forandrad-ovre-aldersgrans",
            "forandring-i-avgransningen",
            "forandring-i-modellgrupper",
            "resultat-av-forandringarna",
            "resultat",
            "variabler-enligt-justerad",
            "nivaforandringar",
            "inriktningsforandringar",
            "foretag",
            "justerad-metod-for",
            "diagram",
            "bortfall-i-anstallningstid",
            "anstallda",
            "arbetsstallen",
            "sysselsattningsavgransningen",
        ],
    ),
    (
        "_methodology-labour-market-programs",
        "Arbetsmarknadspolitiska åtgärder — Program och ersättningar",
        ["type/methodology", "topic/employment"],
        [
            "antal-personer-som-nagon",
            "utbildningsvikariat",
            "resursarbete",
            "individuellt-anstallningsstod",
            "allmant-anstallningsstod",
            "forstarkt-anstallningsstod",
            "sarskilt-anstallningsstod",
            "trygghetsanstallning",
            "arbetspraktik",
            "modernt-beredskapsjobb",
            "forberedande-insatser",
            "extratjanster",
            "stod-till-start",
            "antal-personer-som-deltagit",
            "akademikerjobb",
            "instegsjobb",
            "plusjobb",
            "trainee",
            "yrkesintroduktion",
            "utbildningskontrakt",
            "utvecklingsanstallning",
            "anstallningsstod-for-lang",
        ],
    ),
    (
        "_methodology-social-assistance",
        "Ekonomiskt bistånd — Metodik och riksnorm",
        ["type/methodology", "topic/income"],
        [
            "ekonomiskt-bistand",
            "riksnorm",
        ],
    ),
    (
        "_methodology-raks",
        "Registerbaserad aktivitetsstatistik (RAKS)",
        ["type/methodology", "topic/activity-status"],
        [
            "registerbaserad-aktivitets",
        ],
    ),
    (
        "_appendix-basbelopp",
        "Bilaga 1 — Basbelopp",
        ["type/appendix", "topic/income"],
        [
            "basbelopp",
            "det-minskade-basbeloppet",
            "det-forhojda-basbeloppet",
            "inkomstbasbeloppet",
        ],
    ),
    (
        "_appendix-af-datalager",
        "Bilaga 2 — Bearbetning av data från AF Datalager",
        ["type/appendix", "topic/employment"],
        [
            "bearbetning-av-data",
        ],
    ),
    (
        "_appendix-midas",
        "Bilaga 4 — STORE MiDAS",
        ["type/appendix", "topic/social-insurance"],
        [
            "store-midas",
        ],
    ),
    (
        "_appendix-fk-data",
        "Bilaga 5 — Hur FK-data tolkas",
        ["type/appendix", "topic/social-insurance"],
        [
            "sjukfall-antal-fall",
            "hur-fk-data-tolkas",
        ],
    ),
    (
        "_appendix-variabelkalla",
        "Bilaga 6 — Ursprunglig variabelkälla",
        ["type/appendix", "topic/lisa"],
        [
            "ursprunglig-variabelkalla",
        ],
    ),
    (
        "_appendix-year-coverage",
        "Bilaga 8–9 — Regionala koder och variabeltäckning per år",
        ["type/appendix", "topic/lisa"],
        [
            "2011-01-01",  # regional codes + year coverage
        ],
    ),
]

# Slugs to drop entirely (reference tables or bare section headings)
DROP_SLUGS: list[str] = [
    "alfabetisk-ordning-efter-variabelnamn",
    "alfabetisk-ordning-efter-variabelns",
    "variabler-efter-amnesinnehall",
    "variabelforteckning",
    # Bare section headings (no content beyond the heading itself)
    "demografiska-variabler",
    "inkomstvariabler",
    "sysselsattningsvariabler",
    "kopplingsidentiteter",
    "kopplingar-mellan",
    "familjerelaterade-inkomster",
    "inkomst-av-forvarvskalla",
    "individ",
    # Bilaga headings (content is in the appendix files)
    "bilaga-",
    # Year-only fragments (date headings with no content)
    "1992-01-01",
    "1995-01-01",
    "1997-01-01",
    "1998-01-01",
    "1999-01-01",
    "2003-01-01",
    "2007-01-01",
    "2008-01-01",
    # Section header stubs
    "forord",
    "databasens-uppbyggnad",
    "evalveringar-och-analyser",
    "regionala-koder",
]


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class DocEntry:
    """A parsed document entry — either a variable or a topic."""

    slug: str  # filename without .md
    display_name: str
    column_name: str | None  # None for topic files
    tags: list[str] = field(default_factory=list)
    lines: list[str] = field(default_factory=list)
    start_line: int = 0
    source_file: str | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def strip_noise(lines: list[str]) -> tuple[list[str], dict[str, str]]:
    """Remove noise and extract footnotes.

    Returns (cleaned_lines, footnotes) where footnotes is {number: text}.
    Footnote definitions are removed from the content and returned separately
    so they can be placed in the correct variable file later.
    """
    footnotes: dict[str, str] = {}
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if FOOTER_RE.match(stripped):
            continue
        if PAGE_NUM_RE.match(stripped):
            continue

        # Extract footnote definitions BEFORE converting refs.
        # Definitions start with <sup>N</sup> at the beginning of the line.
        fn_def = re.match(r"^\s*<sup>(\d+)</sup>\s+(.+)$", stripped)
        if fn_def:
            footnotes[fn_def.group(1)] = fn_def.group(2)
            continue

        # Convert inline footnote references: "word<sup>N</sup>" → "word[^N]"
        line = re.sub(r"<sup>(\d+)</sup>", r"[^\1]", line)
        # Convert remaining <sup> (non-numeric) to plain text
        line = re.sub(r"<sup>(.*?)</sup>", r"\1", line)
        # Convert HTML bold/italic/underline to markdown equivalents
        line = re.sub(r"<b>(.*?)</b>", r"**\1**", line)
        line = re.sub(r"<i>(.*?)</i>", r"*\1*", line)
        line = re.sub(r"</?u>", "", line)
        # Strip remaining HTML tags (span anchors, etc.)
        line = re.sub(r"</?span[^>]*>", "", line)
        # Unescape markdown-escaped underscores in bold text (marker does this)
        line = line.replace("\\_", "_")
        cleaned.append(line)
    return cleaned, footnotes


def resolve_topic(heading: str, current_topic: str) -> str:
    """Find the best matching topic tag for a section heading."""
    for key, tag in SECTION_TOPIC.items():
        if key.lower() in heading.lower():
            return tag
    return current_topic


def make_frontmatter(
    entry: DocEntry,
    default_source: str = "lisa-bakgrundsfakta-1990-2017",
) -> str:
    """Generate Obsidian-style YAML frontmatter."""
    fm_lines = ["---"]
    if entry.column_name:
        fm_lines.append(f"variable: {entry.column_name}")
    display = entry.display_name
    if '"' in display:
        fm_lines.append(f"display_name: '{display}'")
    else:
        fm_lines.append(f'display_name: "{display}"')
    if entry.tags:
        fm_lines.append("tags:")
        for tag in sorted(set(entry.tags)):
            fm_lines.append(f"  - {tag}")
    source = entry.source_file or default_source
    fm_lines.append(f'source: "{source}"')
    fm_lines.append("---")
    return "\n".join(fm_lines)


def extract_wiki_links(
    text: str, known_cols: set[str], own_col: str | None = None
) -> str:
    """Convert references to known column names into [[wiki-links]].

    Matches column names at word boundaries, skipping:
    - The variable's own name (to avoid self-links)
    - Names inside frontmatter, headings, or table headers
    - Names already inside wiki-links
    - Names inside markdown link syntax [text](url)
    """
    # Build a regex matching any known column name at word boundaries.
    # Sort longest-first so e.g. SyssStat11 matches before SyssStat.
    cols_pattern = "|".join(
        re.escape(c) for c in sorted(known_cols, key=len, reverse=True)
    )
    col_re = re.compile(r"(?<!\[\[)\b(" + cols_pattern + r")\b(?!\]\])")

    lines = text.split("\n")
    result = []
    in_frontmatter = False

    for line in lines:
        stripped = line.strip()

        # Skip frontmatter
        if stripped == "---":
            in_frontmatter = not in_frontmatter
            result.append(line)
            continue
        if in_frontmatter:
            result.append(line)
            continue

        # Skip lines that are headings (variable header contains the name)
        if stripped.startswith("#") or stripped.startswith("**"):
            result.append(line)
            continue

        # Skip table header rows (| **Bold** | **Bold** |)
        if stripped.startswith("|") and "**" in stripped:
            result.append(line)
            continue

        # Skip separator rows
        if stripped.startswith("|--"):
            result.append(line)
            continue

        def replace_col(m: re.Match) -> str:
            col = m.group(1)
            if col == own_col:
                return col
            # Don't link inside markdown URLs: check if we're between ( and )
            # that looks like a markdown link
            before = line[: m.start()]
            if before.rstrip().endswith("]("):
                return col
            return f"[[{col}]]"

        line = col_re.sub(replace_col, line)
        result.append(line)

    return "\n".join(result)


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------


def find_variable_boundaries(
    lines: list[str], known_cols: set[str]
) -> list[tuple[int, str, str]]:
    """Scan lines and return (line_number, display_name, column_name) tuples.

    Uses known LISA column names as anchors (case-insensitive).
    """
    # Build case-insensitive lookup: lowered -> canonical name
    col_lookup: dict[str, str] = {c.lower(): c for c in known_cols}

    def match_col(candidate: str) -> str | None:
        return col_lookup.get(candidate.lower())

    entries: list[tuple[int, str, str]] = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        # Strip markdown heading prefix for pattern matching
        line = re.sub(r"^#{1,4}\s+", "", line)

        # Pattern 1: **Display Name ColumnName**
        m = SINGLE_LINE_RE.match(line)
        if m:
            canonical = match_col(m.group(2).strip())
            if canonical:
                entries.append((i, m.group(1).strip(), canonical))
                i += 1
                continue

        # Pattern 4: table header |**Display**|**ColName**|
        m = TABLE_HEADER_RE.match(line) or TABLE_HEADER_PLAIN_RE.match(line)
        if m:
            canonical = match_col(m.group(2).strip())
            if canonical:
                entries.append((i, m.group(1).strip(), canonical))
                i += 1
                continue

        # Pattern 3: ## **ColumnName** (check if preceded by display heading)
        m = HEADING_COL_RE.match(line)
        if m:
            canonical = match_col(m.group(1).strip())
            if canonical:
                # Look back for display name
                display = canonical
                for j in range(i - 1, max(i - 4, -1), -1):
                    prev = lines[j].strip()
                    if not prev:
                        continue
                    pm = SECTION_HEADING_RE.match(prev)
                    if pm:
                        display = pm.group(1).strip()
                        entries.append((j, display, canonical))
                        break
                    break
                else:
                    entries.append((i, display, canonical))
                i += 1
                continue

        # Pattern 2: **ColumnName** alone (split header)
        m = BOLD_ONLY_RE.match(line)
        if m:
            canonical = match_col(m.group(1).strip())
            if canonical:
                # Look back for display name
                display = canonical
                for j in range(i - 1, max(i - 4, -1), -1):
                    prev = lines[j].strip()
                    if not prev:
                        continue
                    pm = BOLD_DISPLAY_RE.match(prev)
                    if pm:
                        display = pm.group(1).strip()
                        entries.append((j, display, canonical))
                        break
                    break
                else:
                    entries.append((i, display, canonical))
                i += 1
                continue

        i += 1

    return entries


def find_section_boundaries(lines: list[str]) -> list[tuple[int, str]]:
    """Find major section headings (## **Heading**)."""
    sections = []
    for i, line in enumerate(lines):
        m = SECTION_HEADING_RE.match(line.strip())
        if m:
            sections.append((i, m.group(1).strip()))
    return sections


def get_section_topic(line_num: int, sections: list[tuple[int, str]]) -> str:
    """Find the topic tag for a variable based on its position in the document."""
    topic = ""
    for sec_line, sec_name in sections:
        if sec_line > line_num:
            break
        resolved = resolve_topic(sec_name, topic)
        if resolved != topic:
            topic = resolved
    return topic


def compute_variable_tags(col: str, section_topic: str) -> list[str]:
    """Compute the full tag set for a variable."""
    tags = ["type/variable"]
    if section_topic:
        tags.append(section_topic)

    # Add secondary topic tags based on variable name
    for pattern, tag in SECONDARY_TAGS:
        if re.match(pattern, col) and tag not in tags:
            tags.append(tag)

    # Remove incorrect employment from pension/disability variables
    if "topic/employment" in tags and "topic/social-insurance" in tags:
        for pattern in REMOVE_EMPLOYMENT:
            if re.match(pattern, col):
                tags.remove("topic/employment")
                break

    return tags


def parse_bakgrundsfakta(md_text: str, known_cols: set[str]) -> list[DocEntry]:
    """Parse the full bakgrundsfakta markdown into DocEntry objects."""
    lines = md_text.split("\n")
    lines, footnotes = strip_noise(lines)

    var_boundaries = find_variable_boundaries(lines, known_cols)
    sections = find_section_boundaries(lines)

    # Sort variable boundaries by line number
    var_boundaries.sort(key=lambda x: x[0])

    # Identify hard section boundaries that should cap variable content.
    # Bilaga headings, front-matter headings, and major category headings
    # prevent a variable from absorbing unrelated content.
    hard_boundaries = sorted(
        s[0]
        for s in sections
        if re.match(
            r"(Bilaga|Förord|Syfte|Omfattning|Innehåll|Variabelförteckning"
            r"|Kopplingsidentiteter|Demografiska|Utbildnings|Sysselsättnings"
            r"|Inkomstvariabler|Familjerelaterade|Registerbaserad aktivitets"
            r"|I databasen ingående)",
            s[1],
        )
    )

    # Build variable entries: each entry spans from its start to the next entry
    entries: list[DocEntry] = []
    seen_cols: set[str] = set()

    for idx, (start, display, col) in enumerate(var_boundaries):
        # End is the start of the next variable, or a reasonable cutoff
        if idx + 1 < len(var_boundaries):
            end = var_boundaries[idx + 1][0]
        else:
            end = len(lines)

        # Cap at next hard section boundary if it falls within range
        for hb in hard_boundaries:
            if start < hb < end:
                end = hb
                break

        # Compute tags from section position + variable name patterns
        section_topic = get_section_topic(start, sections)
        tags = compute_variable_tags(col, section_topic)

        content_lines = lines[start:end]

        # Deduplicate: if we already have this column, append content
        if col in seen_cols:
            for e in entries:
                if e.column_name == col:
                    e.lines.extend(["", "---", ""])
                    e.lines.extend(content_lines)
                    break
            continue

        seen_cols.add(col)
        entry = DocEntry(
            slug=col,
            display_name=display,
            column_name=col,
            tags=tags,
            lines=content_lines,
            start_line=start,
        )
        entries.append(entry)

    # Now find "gaps" — text between variable entries that is general/topic content
    # Collect all line ranges covered by variables (respecting hard boundaries)
    covered = set()
    for idx, (start, _, _) in enumerate(var_boundaries):
        if idx + 1 < len(var_boundaries):
            end = var_boundaries[idx + 1][0]
        else:
            end = len(lines)
        for hb in hard_boundaries:
            if start < hb < end:
                end = hb
                break
        for j in range(start, end):
            covered.add(j)

    # Find contiguous uncovered ranges that are substantial (>5 non-empty lines)
    topic_entries = collect_topic_entries(lines, covered, sections)
    entries.extend(topic_entries)

    # Fill missing variables using the Variabelförteckning as ground truth.
    # Some variable headers are dropped by marker (PageHeader misclassification).
    # For these, create entries using the display name from the Variabelförteckning
    # and any unclaimed text near their page anchor.
    found_cols = {e.column_name for e in entries if e.column_name}
    vf = extract_variabelforteckning(lines)
    missing = {col: v for col, v in vf.items() if col not in found_cols}
    if missing:
        stub_entries = fill_missing_variables(lines, missing, sections, covered)
        entries.extend(stub_entries)

    # Attach footnote definitions to entries that reference them
    if footnotes:
        _attach_footnotes(entries, footnotes)

    return entries


def _attach_footnotes(entries: list[DocEntry], footnotes: dict[str, str]) -> None:
    """Append footnote definitions to entries that contain references."""
    for entry in entries:
        content = "\n".join(entry.lines)
        refs = set(re.findall(r"\[\^(\d+)\]", content))
        if not refs:
            continue
        fn_lines = []
        for num in sorted(refs, key=int):
            if num in footnotes:
                fn_lines.append(f"[^{num}]: {footnotes[num]}")
        if fn_lines:
            entry.lines.extend(["", *fn_lines])


def fill_missing_variables(
    lines: list[str],
    missing: dict[str, tuple[str, int]],
    sections: list[tuple[int, str]],
    covered: set[int],
) -> list[DocEntry]:
    """Create stub entries for variables the parser missed.

    These are variables listed in the Variabelförteckning whose headers
    marker dropped (typically PageHeader misclassification). We create
    a minimal entry with the display name from the Variabelförteckning.
    """
    # Find where the variable descriptions begin (after the reference tables).
    # Content before this point is TOC/index — not variable descriptions.
    desc_start = 0
    for i, line in enumerate(lines):
        if re.match(r"^#{2,4}\s*\*\*I databasen ingående", line.strip()):
            desc_start = i
            break
        if re.match(r"^#{2,4}\s*\*\*Kopplingsidentiteter", line.strip()):
            desc_start = i
            break

    entries: list[DocEntry] = []
    for col, (display, page) in missing.items():
        # Search for column name in uncovered lines AFTER the description start,
        # skipping any table rows (TOC tables, year-coverage tables, etc.)
        best_start = None
        for i in range(desc_start, len(lines)):
            if i in covered:
                continue
            line = lines[i].strip()
            if re.search(r"\b" + re.escape(col) + r"\b", line):
                if line.startswith("|"):
                    continue
                best_start = i
                break

        section_topic = get_section_topic(best_start or desc_start, sections)
        tags = compute_variable_tags(col, section_topic)

        if best_start is not None:
            content_lines = []
            for j in range(
                max(desc_start, best_start - 3), min(len(lines), best_start + 30)
            ):
                if j not in covered:
                    content_lines.append(lines[j])
                elif content_lines:
                    break
        else:
            content_lines = [f"**{display} {col}**"]

        entries.append(
            DocEntry(
                slug=col,
                display_name=display,
                column_name=col,
                tags=tags,
                lines=content_lines,
                start_line=best_start or 0,
            )
        )

    return entries


def collect_topic_entries(
    lines: list[str],
    covered: set[int],
    sections: list[tuple[int, str]],
) -> list[DocEntry]:
    """Collect uncovered text into consolidated topic files.

    Instead of creating one file per section heading, groups text into
    deliberate target files defined in _CONSOLIDATION_TARGETS. Fragments
    whose slug matches a target's source patterns get merged into that
    target. Unmatched fragments and fragments matching DROP_SLUGS are
    discarded.
    """
    # First, collect all uncovered text fragments keyed by section slug
    uncovered_ranges: list[tuple[int, int]] = []
    start = None
    for i in range(len(lines)):
        if i not in covered:
            if start is None:
                start = i
        else:
            if start is not None:
                uncovered_ranges.append((start, i))
                start = None
    if start is not None:
        uncovered_ranges.append((start, len(lines)))

    # Group fragments by section slug
    slug_content: dict[str, list[str]] = {}
    for range_start, range_end in uncovered_ranges:
        content = lines[range_start:range_end]
        non_empty = [l for l in content if l.strip()]
        if len(non_empty) < 2:
            continue

        section_name = "Översikt"
        for sec_line, sec_name in sections:
            if sec_line <= range_start:
                section_name = sec_name
            elif sec_line < range_end:
                # Split at section boundary
                pre = lines[range_start:sec_line]
                if any(l.strip() for l in pre):
                    slug = _slugify(section_name)
                    slug_content.setdefault(slug, []).extend(pre)
                section_name = sec_name
                range_start = sec_line
            else:
                break

        slug = _slugify(section_name)
        remaining = lines[range_start:range_end]
        if any(l.strip() for l in remaining):
            slug_content.setdefault(slug, []).extend([""])
            slug_content[slug].extend(remaining)

    # Now consolidate fragments into target files
    target_content: dict[str, list[str]] = {}  # target_slug -> content
    claimed_slugs: set[str] = set()

    for target_slug, display, tags, patterns in _CONSOLIDATION_TARGETS:
        for frag_slug, frag_lines in slug_content.items():
            if any(p in frag_slug for p in patterns):
                target_content.setdefault(target_slug, [])
                if target_content[target_slug]:
                    target_content[target_slug].append("")
                target_content[target_slug].extend(frag_lines)
                claimed_slugs.add(frag_slug)

    # Drop explicitly excluded slugs
    for frag_slug in list(slug_content.keys()):
        if any(p in frag_slug for p in DROP_SLUGS):
            claimed_slugs.add(frag_slug)

    # Build DocEntry objects for consolidated targets
    entries: list[DocEntry] = []
    for target_slug, display, tags, _patterns in _CONSOLIDATION_TARGETS:
        content_lines = target_content.get(target_slug, [])
        non_empty = [l for l in content_lines if l.strip()]
        if len(non_empty) < 3:
            continue
        entries.append(
            DocEntry(
                slug=target_slug,
                display_name=display,
                column_name=None,
                tags=list(tags),
                lines=content_lines,
            )
        )

    # Report unclaimed fragments (may indicate missing consolidation rules)
    unclaimed = set(slug_content.keys()) - claimed_slugs
    if unclaimed:
        print(f"  Unclaimed topic fragments ({len(unclaimed)}):", file=sys.stderr)
        for slug in sorted(unclaimed):
            n = len([l for l in slug_content[slug] if l.strip()])
            print(f"    {slug} ({n} lines)", file=sys.stderr)

    return entries


def _slugify(name: str) -> str:
    """Convert a Swedish heading to a filesystem-safe slug."""
    slug = name.lower()
    replacements = {
        "å": "a",
        "ä": "a",
        "ö": "o",
        "–": "-",
        "—": "-",
    }
    for k, v in replacements.items():
        slug = slug.replace(k, v)
    slug = re.sub(r"[^a-z0-9-]", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug[:60]


# Known OCR errors from marker output. Applied during write_entries so
# corrections survive parser re-runs without editing generated files.
OCR_CORRECTIONS: dict[str, str] = {
    "KU2SsykAn": "KU2SsykAr",
    "KU2PeOrgNm": "KU2PeOrgNr",
    "Inv UtvGrEq2": "Inv_UtvGrEg2",
    "näringsverksamet": "näringsverksamhet",
}


def write_entries(
    entries: list[DocEntry],
    known_cols: set[str],
    out_dir: Path,
) -> tuple[int, int]:
    """Write entries to markdown files. Returns (variable_count, topic_count)."""
    out_dir.mkdir(parents=True, exist_ok=True)
    var_count = 0
    topic_count = 0

    for entry in entries:
        entry.lines = _normalize_heading_levels(entry.lines)
        content = "\n".join(entry.lines).strip()
        if not content:
            continue

        # Fix known OCR errors
        for wrong, right in OCR_CORRECTIONS.items():
            content = content.replace(wrong, right)

        # Add wiki-links to cross-references
        content = extract_wiki_links(content, known_cols, entry.column_name)

        # Markdownlint compliance
        content = re.sub(r"\n{3,}", "\n\n", content)  # MD012: no multiple blanks
        content = re.sub(
            r"[^\S\n]+$", "", content, flags=re.MULTILINE
        )  # MD009: trailing spaces
        content = BARE_EMAIL_RE.sub(r"<\1>", content)  # MD034: wrap bare emails
        content = BARE_URL_RE.sub(r"<\1>", content)  # MD034: wrap bare URLs
        content = BROKEN_IMG_RE.sub("", content)  # MD045: remove broken image refs
        content = content.replace("\n```\n", "\n```text\n")  # MD040: add language tag

        fm = make_frontmatter(entry)
        full = f"{fm}\n\n{content}\n"

        filepath = out_dir / f"{entry.slug}.md"
        filepath.write_text(full, encoding="utf-8")

        if entry.column_name:
            var_count += 1
        else:
            topic_count += 1

    return var_count, topic_count


# ---------------------------------------------------------------------------
# Förändringar (change log) parser — simpler, whole-file
# ---------------------------------------------------------------------------


def _normalize_heading_levels(lines: list[str]) -> list[str]:
    """Ensure single H1 and no heading-level jumps (MD001/MD025)."""
    out = []
    seen_h1 = False
    prev_level = 0
    for line in lines:
        m = re.match(r"^(#{1,6})\s", line)
        if not m:
            out.append(line)
            continue
        level = len(m.group(1))
        # Demote all H1 after the first to H2
        if level == 1:
            if seen_h1:
                line = "#" + line
                level = 2
            seen_h1 = True
        # Fix heading-level jumps: at most prev_level + 1
        if prev_level and level > prev_level + 1:
            target = prev_level + 1
            line = "#" * target + line[level:]
            level = target
        prev_level = level
        out.append(line)
    return out


def parse_forandringar(md_text: str, year: str, source_file: str) -> DocEntry:
    """Parse a förändringar PDF as a single topic entry."""
    lines = md_text.split("\n")
    lines, _footnotes = strip_noise(lines)
    # Also strip SCB header/footer lines specific to förändringar docs
    cleaned = []
    for line in lines:
        s = line.strip()
        if any(
            [
                s.startswith("www.scb.se"),
                re.match(r"^Datum\s+Version", s),
                re.match(r"^\d+\s+av\s+\d+\s*$", s),
            ]
        ):
            continue
        cleaned.append(line)
    cleaned = _normalize_heading_levels(cleaned)
    return DocEntry(
        slug=f"_changelog-{year}",
        display_name=f"Förändringar i LISA {year}",
        column_name=None,
        tags=["type/changelog"],
        lines=cleaned,
        source_file=source_file,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def extract_variabelforteckning(lines: list[str]) -> dict[str, tuple[str, int]]:
    """Extract column names from the Variabelförteckning table in the document.

    Returns {column_name: (display_name, page_number)}.
    The table appears in the first ~2500 lines with rows like:
    | Display name | ColumnName | PageNum |
    Some rows have merged columns: | Display ColumnName | PageNum |
    """
    var_list: dict[str, tuple[str, int]] = {}
    for i, line in enumerate(lines):
        if i > 2500:
            break

        # Standard 3-column: | Display | ColumnName | PageNum |
        m = re.match(
            r"^\|\s*(.+?)\s*\|\s*([A-ZÅÄÖ][A-Za-zÅÄÖåäö0-9_]{1,40})\s*\|\s*(\d+)\s*\|",
            line,
        )
        if m:
            col = m.group(2).strip()
            display = re.sub(r"<[^>]+>", "", m.group(1)).strip()
            page = int(m.group(3))
            if page > 50 and col not in var_list:
                var_list[col] = (display, page)
            continue

        # Merged: column name in display cell. Handles:
        #   | Display ColumnName | PageNum |
        #   | Display ColumnName |         | PageNum |
        m2 = re.match(
            r"^\|\s*(.+?)\s+([A-ZÅÄÖ][A-Za-zÅÄÖåäö0-9_]{2,40})\s*\|[\s|]*(\d+)\s*\|",
            line,
        )
        if m2:
            col = m2.group(2).strip()
            display = re.sub(r"<[^>]+>", "", m2.group(1)).strip()
            page = int(m2.group(3))
            if page > 50 and col not in var_list:
                var_list[col] = (display, page)
            continue

        # Single-column with no page: | Display ColumnName |
        # Skip common table headers that look like variable names
        _TABLE_HEADERS = {
            "Afrika",
            "Asien",
            "Europa",
            "Nordamerika",
            "Oceanien",
            "Sydamerika",
            "Sverige",
            "Norden",
            "Sovjetunionen",
            "Gruppering",
            "Sida",
            "Variabel",
            "Klartext",
            "Beskrivning",
            "Årtal",
            "Familjeställning",
            "Okänt",
        }
        m3 = re.match(
            r"^\|\s*(.+?)\s+([A-ZÅÄÖ][A-Za-zÅÄÖåäö0-9_]{2,40})\s*\|\s*$",
            line.rstrip(),
        )
        if m3:
            col = m3.group(2).strip()
            if col in _TABLE_HEADERS:
                continue
            display = re.sub(r"<[^>]+>", "", m3.group(1)).strip()
            if col not in var_list:
                var_list[col] = (display, 0)  # page 0 = unknown

    return var_list


def get_lisa_columns(md_text: str | None = None) -> set[str]:
    """Get LISA column names.

    Primary source: Variabelförteckning table extracted from the document.
    Fallback: regmeta database.
    """
    if md_text:
        cleaned = re.sub(
            r"^(\s*)<sup>(\d+)</sup>\s*", r"\1[^\2]: ", md_text, flags=re.MULTILINE
        )
        cleaned = re.sub(r"<sup>(\d+)</sup>", r"[^\1]", cleaned)
        cleaned = re.sub(r"</?span[^>]*>", "", cleaned).replace("\\_", "_")
        vf = extract_variabelforteckning(cleaned.split("\n"))
        if vf:
            print(f"Extracted {len(vf)} columns from Variabelförteckning")
            return set(vf.keys())

    # Fallback to regmeta
    try:
        from regmeta.db import open_db

        db_path = Path.home() / ".local/share/regmeta/regmeta.db"
        conn = open_db(db_path)
        cur = conn.execute("""
            SELECT DISTINCT va.kolumnnamn
            FROM variable_alias va
            JOIN variable_instance vi ON vi.cvid = va.cvid
            JOIN register_version rv ON rv.regver_id = vi.regver_id
            JOIN register_variant rvt ON rvt.regvar_id = rv.regvar_id
            JOIN register r ON r.register_id = rvt.register_id
            WHERE r.registernamn LIKE '%LISA%'
        """)
        cols = {row[0] for row in cur.fetchall()}
        conn.close()
        print(f"Loaded {len(cols)} columns from regmeta database")
        return cols
    except Exception as e:
        print(f"Warning: could not load columns: {e}", file=sys.stderr)
        return set()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bakgrundsfakta",
        type=Path,
        help="Path to bakgrundsfakta PDF",
    )
    parser.add_argument(
        "--forandringar",
        type=Path,
        nargs="*",
        help="Paths to förändringar PDFs",
    )
    parser.add_argument(
        "--cached-md",
        type=Path,
        help="Use pre-converted markdown instead of PDF (for bakgrundsfakta)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=OUT_DIR,
        help=f"Output directory (default: {OUT_DIR})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be written without writing",
    )
    args = parser.parse_args()

    # Read bakgrundsfakta markdown first (needed for column extraction)
    if args.cached_md:
        print(f"Reading cached markdown: {args.cached_md}")
        md_text = args.cached_md.read_text(encoding="utf-8")
    elif args.bakgrundsfakta:
        print(f"Converting PDF: {args.bakgrundsfakta}")
        import pymupdf4llm

        md_text = pymupdf4llm.to_markdown(str(args.bakgrundsfakta))
    else:
        print("ERROR: Supply --bakgrundsfakta or --cached-md")
        sys.exit(1)

    known_cols = get_lisa_columns(md_text)

    if not known_cols:
        print("ERROR: No column names found in document or regmeta database.")
        sys.exit(1)

    entries = parse_bakgrundsfakta(md_text, known_cols)

    var_entries = [e for e in entries if e.column_name]
    topic_entries = [e for e in entries if not e.column_name]

    if args.dry_run:
        print(f"\nWould write {len(var_entries)} variable files:")
        for e in sorted(var_entries, key=lambda x: x.slug):
            print(f"  {e.slug}.md  ({len(e.lines)} lines, tags: {e.tags})")
        print(f"\nWould write {len(topic_entries)} topic files:")
        for e in sorted(topic_entries, key=lambda x: x.slug):
            print(f"  {e.slug}.md  ({len(e.lines)} lines, tags: {e.tags})")

        # Coverage check
        found = {e.column_name for e in var_entries}
        missing = known_cols - found
        extra = found - known_cols
        print(f"\nCoverage: {len(found)}/{len(known_cols)} LISA columns documented")
        if missing:
            print(
                f"  Missing from doc ({len(missing)}): {', '.join(sorted(missing)[:20])}..."
            )
        if extra:
            print(f"  Extra (not in regmeta): {', '.join(sorted(extra)[:20])}")
    else:
        var_count, topic_count = write_entries(entries, known_cols, args.out)
        print(
            f"Wrote {var_count} variable files, {topic_count} topic files to {args.out}"
        )

        # Coverage check
        found = {e.column_name for e in var_entries}
        missing = known_cols - found
        print(f"Coverage: {len(found)}/{len(known_cols)} LISA columns documented")
        if missing:
            print(f"Missing ({len(missing)}): {', '.join(sorted(missing)[:30])}...")

    # Parse förändringar files (PDF or pre-converted markdown)
    forandringar_paths = list(args.forandringar or [])
    for path in forandringar_paths:
        year_match = re.search(r"(\d{4})", path.name)
        year = year_match.group(1) if year_match else "unknown"
        if path.suffix == ".md":
            print(f"Reading förändringar markdown: {path.name} (year: {year})")
            md = path.read_text(encoding="utf-8")
        else:
            print(f"Converting förändringar PDF: {path.name} (year: {year})")
            import pymupdf4llm

            md = pymupdf4llm.to_markdown(str(path))
        entry = parse_forandringar(md, year, path.name)
        if not args.dry_run:
            write_entries([entry], known_cols, args.out)
            print(f"  Wrote {entry.slug}.md")
        else:
            print(f"  Would write {entry.slug}.md ({len(entry.lines)} lines)")


if __name__ == "__main__":
    main()
