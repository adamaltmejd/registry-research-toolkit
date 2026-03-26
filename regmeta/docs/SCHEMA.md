# regmeta docs — Schema

Parsed register documentation as Obsidian-compatible markdown files.
Each register gets its own subdirectory (e.g., `lisa/`).

## Frontmatter

All files use YAML frontmatter:

```yaml
---
variable: ColumnName        # only for variable files; omit for non-variable files
display_name: "Human name"  # Swedish display name from the source document
tags:
  - type/<type>
  - topic/<topic>
source: "source-document-identifier"
---
```

## Tags

Two orthogonal dimensions:

### `type/` — what kind of document

| Tag                | Description                                              |
|--------------------|----------------------------------------------------------|
| `type/variable`    | Per-variable documentation (definition, codes, history)  |
| `type/methodology` | How variables are derived, data sources, quality notes   |
| `type/appendix`    | Reference tables, basbelopp, regional codes              |
| `type/changelog`   | Yearly change documents (new/removed/renamed variables)  |
| `type/overview`    | Register-level introduction, purpose, scope              |

### `topic/` — what domain area

| Tag                      | Description                                          |
|--------------------------|------------------------------------------------------|
| `topic/demographic`      | Age, sex, residence, civil status, family, migration |
| `topic/education`        | UREG, SUN codes, study participation                 |
| `topic/employment`       | RAMS/BAS, occupation, industry, employer             |
| `topic/income`           | Earned, capital, disposable, transfers, benefits     |
| `topic/identifier`       | Linking keys: person, firm, establishment            |
| `topic/social-insurance` | Sickness, parental, rehabilitation, disability       |
| `topic/activity-status`  | RAKS activity classification                         |
| `topic/lisa`             | Cross-cutting LISA documentation                     |

A file may have one type tag and one or more topic tags.

## File naming

| Pattern               | Example                  | Description                    |
|-----------------------|--------------------------|--------------------------------|
| `{ColumnName}.md`     | `FodelseAr.md`           | Variable documentation         |
| `_overview.md`        |                          | Register overview              |
| `_methodology-*.md`   | `_methodology-education.md` | Methodology documentation   |
| `_appendix-*.md`      | `_appendix-basbelopp.md` | Appendix / reference tables    |
| `_changelog-*.md`     | `_changelog-2022.md`     | Yearly change documentation    |

Variable files are named by their canonical column name (case-sensitive).
Non-variable files are prefixed with `_` and use lowercase kebab-case.

## Wiki-links

Files may contain `[[ColumnName]]` wiki-links to cross-reference other
variable files. These are compatible with Obsidian's link resolution.

## Sources

Source documents are converted to markdown using
[marker](https://github.com/VikParuchuri/marker) with Gemini Flash for
LLM-assisted OCR, then split into per-variable files by
`scripts/parse_lisa_docs.py`. Source PDFs are not tracked in git.

| Source identifier                  | Document                                  |
|------------------------------------|-------------------------------------------|
| `lisa-bakgrundsfakta-1990-2017`    | LISA Bakgrundsfakta 1990–2017 (467 pages) |
| `lisa-2019---forandringar.pdf`     | Förändringar i LISA 2019                  |
| `lisa_2020-forandringar.pdf`       | Förändringar i LISA 2020                  |
| `lisa-2022-forandringar.pdf`       | Förändringar i LISA 2022                  |
| `lisa_2023-forandringar.pdf`       | Förändringar i LISA 2023                  |
| `hushallsinformation-i-lisa-2011-` | Hushållsinformation i LISA 2011           |
