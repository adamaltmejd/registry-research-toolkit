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

# HTML tags injected by marker (span anchors, sup footnotes)
HTML_TAG_RE = re.compile(r"</?(?:span|sup)[^>]*>")

# Bold text patterns for variable headers
# Pattern 1: **Display Name ColumnName**  (single line)
# Uses greedy .+ so it grabs the LAST CamelCase word as column name
SINGLE_LINE_RE = re.compile(
    r"^\*\*(.+)\s+([A-ZÅÄÖ][A-Za-zÅÄÖåäö0-9_]{1,40})\*\*\s*$"
)
# Pattern 2: **ColumnName** alone on a line (split header)
BOLD_ONLY_RE = re.compile(r"^\*\*([A-ZÅÄÖ][A-Za-zÅÄÖåäö0-9_]{1,40})\*\*\s*$")
# Pattern 3: ## **ColumnName** (heading-style, any heading level)
HEADING_COL_RE = re.compile(
    r"^#{2,4}\s*\*\*([A-ZÅÄÖ][A-Za-zÅÄÖåäö0-9_]{1,40})\*\*\s*$"
)
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

# Tag mapping: section heading keywords → tag paths
SECTION_TAGS: dict[str, str] = {
    "Demografiska variabler": "variable/demographic",
    "Demografiska variabler – För familj": "variable/demographic/family",
    "Utbildningsvariabler": "variable/education",
    "Sysselsättningsvariabler": "variable/employment",
    "Förvärvsarbetande": "variable/employment/gainful-employment",
    "Yrke": "variable/employment/occupation",
    "Näringsgren": "variable/employment/industry",
    "Inkomstvariabler": "variable/income",
    "Förvärvsinkomst": "variable/income/earned",
    "Kapitalinkomst": "variable/income/capital",
    "Annan inkomst": "variable/income/other",
    "Ålderspension": "variable/income/pension",
    "Tjänstepension": "variable/income/pension/occupational",
    "Disponibel inkomst": "variable/income/disposable",
    "Familjerelaterade inkomster": "variable/income/family",
    "Ekonomiskt bistånd": "variable/income/social-assistance",
    "Bostadsbidrag": "variable/income/housing-benefit",
    "Bostadstillägg": "variable/income/housing-supplement",
    "Sjukförsäkring": "variable/social-insurance/sickness",
    "Arbetsmarknadsåtgärder": "variable/social-insurance/labour-market",
    "Kopplingsidentiteter": "variable/identifier",
    "Individ": "variable/identifier/individual",
    "Företag": "variable/identifier/firm",
    "Arbetsställeidentitet": "variable/identifier/establishment",
    "Registerbaserad aktivitetsstatistik": "variable/activity-status",
    "Övriga inkomster": "variable/income/other-benefits",
}

# General sections that become topic files (not variable-specific)
TOPIC_SECTIONS = {
    "Förord",
    "Bakgrund till LISA-databasen",
    "Syfte",
    "Omfattning och innehåll",
    "Omfattning",
    "Användning",
    "Tillgång till data",
    "Databasens uppbyggnad",
    "Referensperiod",
    "Variabelförteckning",
}


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


def strip_noise(lines: list[str]) -> list[str]:
    """Remove SCB footer lines, orphan page numbers, and HTML tags."""
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if FOOTER_RE.match(stripped):
            continue
        if PAGE_NUM_RE.match(stripped):
            continue
        # Strip HTML span/sup tags (marker page anchors and footnotes)
        line = HTML_TAG_RE.sub("", line)
        # Unescape markdown-escaped underscores in bold text (marker does this)
        line = line.replace("\\_", "_")
        cleaned.append(line)
    return cleaned


def resolve_tag(heading: str, current_section_tag: str) -> str:
    """Find the best matching tag for a heading."""
    for key, tag in SECTION_TAGS.items():
        if key.lower() in heading.lower():
            return tag
    return current_section_tag


def make_frontmatter(
    entry: DocEntry,
    default_source: str = "lisa-bakgrundsfakta-1990-2017",
) -> str:
    """Generate Obsidian-style YAML frontmatter."""
    fm_lines = ["---"]
    if entry.column_name:
        fm_lines.append(f"variable: {entry.column_name}")
    fm_lines.append(f"display_name: \"{entry.display_name}\"")
    if entry.tags:
        fm_lines.append("tags:")
        for tag in entry.tags:
            fm_lines.append(f"  - {tag}")
    source = entry.source_file or default_source
    fm_lines.append(f"source: \"{source}\"")
    fm_lines.append("---")
    return "\n".join(fm_lines)


def extract_wiki_links(text: str, known_cols: set[str]) -> str:
    """Convert references to known column names into wiki-links.

    Looks for patterns like:
    - (ColumnName) — parenthesized reference
    - se ColumnName — Swedish cross-reference
    """
    def replace_ref(m: re.Match) -> str:
        col = m.group(1)
        if col in known_cols:
            return f"[[{col}]]"
        return m.group(0)

    # (ColumnName) pattern — common in "I övrigt se X (ColName)"
    text = re.sub(
        r"\((" + "|".join(re.escape(c) for c in sorted(known_cols, key=len, reverse=True)) + r")\)",
        replace_ref,
        text,
    )
    return text


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


def get_section_tag(line_num: int, sections: list[tuple[int, str]]) -> str:
    """Find the innermost section tag for a given line number."""
    tag = "variable"
    for sec_line, sec_name in sections:
        if sec_line > line_num:
            break
        resolved = resolve_tag(sec_name, tag)
        if resolved != tag:
            tag = resolved
    return tag


def parse_bakgrundsfakta(
    md_text: str, known_cols: set[str]
) -> list[DocEntry]:
    """Parse the full bakgrundsfakta markdown into DocEntry objects."""
    lines = md_text.split("\n")
    lines = strip_noise(lines)

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

        # Find the section tag for this variable
        tag = get_section_tag(start, sections)

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
            tags=[tag],
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

    return entries


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

        tag = get_section_tag(best_start or desc_start, sections)

        if best_start is not None:
            content_lines = []
            for j in range(max(desc_start, best_start - 3), min(len(lines), best_start + 30)):
                if j not in covered:
                    content_lines.append(lines[j])
                elif content_lines:
                    break
        else:
            content_lines = [f"**{display} {col}**"]

        entries.append(DocEntry(
            slug=col,
            display_name=display,
            column_name=col,
            tags=[tag],
            lines=content_lines,
            start_line=best_start or 0,
        ))

    return entries


def collect_topic_entries(
    lines: list[str],
    covered: set[int],
    sections: list[tuple[int, str]],
) -> list[DocEntry]:
    """Collect uncovered text as topic entries, split by section heading."""
    topics: list[DocEntry] = []
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

    # Build a mapping: for each uncovered range, find the nearest section heading
    # that either falls within the range or immediately precedes it
    section_buckets: dict[str, list[str]] = {}  # slug -> accumulated lines
    section_meta: dict[str, tuple[str, str, int]] = {}  # slug -> (name, tag, start)
    seen_slugs: set[str] = set()

    for range_start, range_end in uncovered_ranges:
        content = lines[range_start:range_end]
        non_empty = [l for l in content if l.strip()]
        if len(non_empty) < 2:
            continue

        # Find the active section heading for this range
        section_name = "Översikt"
        for sec_line, sec_name in sections:
            if sec_line <= range_start:
                section_name = sec_name
            elif sec_line < range_end:
                # A section heading falls within this uncovered range —
                # it starts a new topic. Split at this boundary.
                # Emit text before the heading under the previous section
                pre = lines[range_start:sec_line]
                pre_non_empty = [l for l in pre if l.strip()]
                if len(pre_non_empty) >= 2:
                    slug = _slugify(section_name)
                    _append_to_bucket(
                        section_buckets, section_meta, seen_slugs,
                        slug, section_name, pre, range_start, sections,
                    )
                # Continue with the new section
                section_name = sec_name
                range_start = sec_line
            else:
                break

        slug = _slugify(section_name)
        remaining = lines[range_start:range_end]
        remaining_non_empty = [l for l in remaining if l.strip()]
        if len(remaining_non_empty) >= 2:
            _append_to_bucket(
                section_buckets, section_meta, seen_slugs,
                slug, section_name, remaining, range_start, sections,
            )

    # Convert buckets to DocEntry objects
    for slug, content_lines in section_buckets.items():
        name, tag, start_line = section_meta[slug]
        non_empty = [l for l in content_lines if l.strip()]
        if len(non_empty) < 3:
            continue
        topics.append(DocEntry(
            slug=f"_topic-{slug}",
            display_name=name,
            column_name=None,
            tags=[tag],
            lines=content_lines,
            start_line=start_line,
        ))

    return topics


def _append_to_bucket(
    buckets: dict[str, list[str]],
    meta: dict[str, tuple[str, str, int]],
    seen: set[str],
    slug: str,
    name: str,
    content: list[str],
    start_line: int,
    sections: list[tuple[int, str]],
) -> None:
    """Append content lines to a topic bucket, deduplicating slugs."""
    # Make slug unique if needed
    base_slug = slug
    counter = 2
    while slug in seen and slug not in buckets:
        slug = f"{base_slug}-{counter}"
        counter += 1

    tag = resolve_tag(name, "topic")
    if not tag.startswith("topic"):
        tag = f"topic/{tag.split('/')[-1]}" if "/" in tag else f"topic/{tag}"

    if slug in buckets:
        buckets[slug].extend([""])
        buckets[slug].extend(content)
    else:
        seen.add(slug)
        buckets[slug] = list(content)
        meta[slug] = (name, tag, start_line)


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
        content = "\n".join(entry.lines).strip()
        if not content:
            continue

        # Add wiki-links to cross-references
        content = extract_wiki_links(content, known_cols)

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


def parse_forandringar(md_text: str, year: str, source_file: str) -> DocEntry:
    """Parse a förändringar PDF as a single topic entry."""
    lines = md_text.split("\n")
    lines = strip_noise(lines)
    # Also strip SCB header/footer lines specific to förändringar docs
    cleaned = []
    for line in lines:
        s = line.strip()
        if s in ("", ) or any([
            s.startswith("www.scb.se"),
            re.match(r"^Datum\s+Version", s),
            re.match(r"^\d+\s+av\s+\d+\s*$", s),
        ]):
            continue
        cleaned.append(line)
    return DocEntry(
        slug=f"_forandringar-{year}",
        display_name=f"Förändringar i LISA {year}",
        column_name=None,
        tags=["topic/changelog", f"topic/changelog/{year}"],
        lines=cleaned,
        source_file=source_file,
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def extract_variabelforteckning(lines: list[str]) -> dict[str, tuple[str, int]]:
    """Extract column names from the Variabelförteckning table in the document.

    Returns {column_name: (display_name, page_number)}.
    The table appears in the first ~2000 lines with rows like:
    | Display name | ColumnName | PageNum |
    """
    var_list: dict[str, tuple[str, int]] = {}
    for i, line in enumerate(lines):
        if i > 2500:
            break
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
    return var_list


def get_lisa_columns(md_text: str | None = None) -> set[str]:
    """Get LISA column names.

    Primary source: Variabelförteckning table extracted from the document.
    Fallback: regmeta database.
    """
    if md_text:
        cleaned = HTML_TAG_RE.sub("", md_text).replace("\\_", "_")
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
            print(f"  Missing from doc ({len(missing)}): {', '.join(sorted(missing)[:20])}...")
        if extra:
            print(f"  Extra (not in regmeta): {', '.join(sorted(extra)[:20])}")
    else:
        var_count, topic_count = write_entries(entries, known_cols, args.out)
        print(f"Wrote {var_count} variable files, {topic_count} topic files to {args.out}")

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
