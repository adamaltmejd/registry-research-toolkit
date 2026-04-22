# Detailed Workflow

Use this file for the exact execution sequence. Keep `SKILL.md` loaded as the
compact control surface and come here when you need the full procedure.

## Phase 1: Interview and bootstrap

### Opening message

Start with a short orientation aimed at a first-time user. Explain:

- what this skill does locally
- that setup happens in two phases:
  - Phase 1 creates the local scaffold and `extract_stats.R`
  - Phase 2 starts only after the user returns from MONA with `stats.json`
- what inputs you need right now
- where you plan to scaffold, if that is already known

Do not open with a terse list of questions or lead with a non-blocking tooling
detail. If a command works via `--help` or a subcommand but lacks `--version`,
that is not the first thing the user needs to hear.

Use wording like:

> I can set up the project locally and prepare the MONA handoff. This happens
> in two phases: first I create the local scaffold and `extract_stats.R`; after
> you run that on MONA and bring back `stats.json`, I generate mock data and
> finish the project docs and repo setup.
>
> To start, I need:
>
> 1. project slug
> 2. SCB project number
> 3. research plan

### Interview

Collect these things from the user:

1. **Project slug**: use `$ARGUMENTS` if provided; otherwise ask. It must be
   `lowercase-with-hyphens`, with no spaces.
2. **Target directory**:
   - if the current working directory contains only invisible agent metadata,
     scaffold into it directly
   - if it is otherwise non-empty, ask whether to scaffold into the current
     directory or into a `{slug}/` subdirectory
3. **SCB project number**: normalize to `P{num}` with no leading zeros
4. **Research plan**: strongly encourage the user to paste it or point to a
   file because it materially improves the scaffold

### Create the minimal scaffold

```bash
mkdir -p {projdir}/src {projdir}/notes {projdir}/tests/testthat {projdir}/output {projdir}/output_mock
```

Generate these files immediately using
[generated-files.md](generated-files.md):

- `{projdir}/{slug}.Rproj`
- `{projdir}/.gitignore`
- `{projdir}/_targets.yaml`

### Preflight CLI checks

Confirm both tools are installed and runnable. Prefer checks that prove the
commands work, not just `--version`:

```bash
command -v mock-data-wizard
command -v regmeta
mock-data-wizard --help
regmeta --help
```

If `--version` happens to work, that is fine. If it does not but `--help` and
the required subcommands do work, treat the tool as installed and move on
without bothering the user about it.

If either command fails, stop and install it before proceeding:

```bash
uv tool install regmeta
regmeta maintain update

uv tool install "mock-data-wizard @ git+https://github.com/adamaltmejd/registry-research-toolkit#subdirectory=mock_data_wizard"
```

If a prior install is broken, reinstall with `--force`:

```bash
uv tool install --force regmeta
uv tool install --force "mock-data-wizard @ git+https://github.com/adamaltmejd/registry-research-toolkit#subdirectory=mock_data_wizard"
```

### Generate the extraction script

```bash
mock-data-wizard generate-script -p P{num} -o {projdir}/extract_stats.R
```

### MONA handoff

Tell the user exactly what to do:

> **Next steps on MONA:**
>
> 1. Upload `extract_stats.R` to MONA via **My Files** and move it from your
>    Inkorg into the project folder.
> 2. Open the **Batch client**, choose **R**, select `extract_stats.R`, and
>    run it.
> 3. The script writes `stats.json`.
> 4. Review `stats.json` in Notepad++ before export. It must contain only
>    aggregate statistics such as counts, shares, value frequencies, or year
>    coverage summaries. Do **not** export the file if you see personal
>    identifiers, row-level records, names, addresses, free-text fields, exact
>    dates of birth, lists of households, or anything else that looks like data
>    about identifiable people rather than aggregates. If an individual field is
>    unsafe but the rest of the file is fine, replace that field with `null`.
>    If the problem is broader than that, stop and fix the extraction before
>    exporting anything.
> 5. Export the file via the file exporter and download it from **My Files**.
> 6. Place `stats.json` in `{projdir}/` and return to this skill.

Phase 1 ends here. Do not generate the remaining project files yet.
When you report back to the user, present this as the expected checkpoint, not
as a failure or an inability to continue.

Use wording like:

> Phase 1 is ready. I created the local scaffold and `extract_stats.R`, and the
> next step is the MONA round-trip for `stats.json`.

## Mock data generation

When `stats.json` exists but `mock_data/manifest.json` does not:

```bash
cd {projdir} && mock-data-wizard generate --stats stats.json --output-dir mock_data/ -y --force
```

Verify that `mock_data/manifest.json` was created. If generation fails,
diagnose the problem before continuing.

After successful generation, delete `{projdir}/extract_stats.R` unless the
user explicitly asked to keep it for reruns. It is a setup artifact, not part
of the working project scaffold.

## Phase 2: Enrichment

### Data documentation

Read `mock_data/manifest.json` and group files by register. Write one
`notes/data_{register_slug}.md` per register group, not one per CSV.

For each register group:

1. Resolve representative columns:

```bash
regmeta --format json resolve --columns "{comma-separated columns}" --register {register_hint}
```

2. Fetch the human-readable register name:

```bash
regmeta get register {register_hint}
```

3. If the register hint is the generic catch-all 366, mark the result as
   low-confidence.

Use this structure:

````markdown
# {Human-readable register name} (register {register_id})

{Brief description of the register and what it provides to this project.}

**Files**: `{filename pattern}`
**Year range**: {year range}
**Row count**: {approximate range across years}

## Available variables

| Variable | Column | Explanation |
| --- | --- | --- |
| {var_name} | `{column_name}` | {definition from regmeta} |

## How it is used

<!-- TODO: describe how the pipeline uses these files -->

## Caveats and issues

<!-- TODO: missing years, encoding quirks, low-confidence matches -->

## Useful regmeta commands

```bash
regmeta get register {register_id}
regmeta get schema --register {register_id} --years {year_hint}
regmeta resolve --columns "{key_columns}" --register {register_id}
```
````

If a file cannot be matched confidently to a register, give it its own
`data_{filename_slug}.md` and say so explicitly.

Interpret file and year coverage literally. `mock-data-wizard` generates mock
data from the exported `stats.json`; it is not randomly omitting years or
inventing a smaller panel for convenience. If only one year appears in the
mock-data scaffold, say that only one year was present in the extracted stats
unless the user tells you the MONA export was intentionally incomplete.

If regmeta fails to resolve a column, say that the metadata resolution failed
in this probe. Do not jump from "unresolved in regmeta" to "invalid linkage" or
"not available" unless the data itself shows a real problem.

Generate `notes/README.md` as an index:

```markdown
# Project Notes

Working documentation for humans and agents. The `data_*.md` files document
pipeline inputs. Other notes capture analysis decisions, dead ends, and
findings that need to persist across sessions.

## Data Sources

- `data_{name}.md` - {one-line description}
```

### Mock data assessment

This file is the evidence summary for what the scaffold learned locally. Do
not paraphrase the manifest or invent issues.

Build it from three layers:

1. **Standardized scripted checks**: repeatable baseline metrics with explicit
   inputs and calculations.
2. **Additional runtime probing**: one-off R checks for project-specific
   questions the standardized checks did not cover.
3. **Interpretation and synthesis**: careful reading of script output,
   `manifest.json`, regmeta results, and relevant docs.

Hard rules:

- Never fabricate a finding, warning, mismatch, or code-set issue.
- If something was not measured, say `Not assessed`.
- If something comes from a heuristic, metadata hint, or low-confidence
  register match, label it that way.
- If prose disagrees with script output, the prose is wrong. Fix it.

Actually probe the mock data. Start with repeatable scripted checks. Prefer a
shared assessment script if one exists; otherwise write an explicit baseline
`Rscript` block and record it in the note. Then add extra targeted probes when
the project needs them.

For each mock CSV, check:

- plausible row counts
- identifier cardinality
- variables with more than 30% nulls
- categorical codes present in mock data but absent from regmeta, and the
  reverse
- whether shared identifier columns overlap across linked files
- schema or code-set mismatches, but only when backed by a direct comparison
  artifact or script output; otherwise mark them `Not assessed`

Write `notes/mock_data_assessment.md` using this structure:

```markdown
# Mock Data Assessment

Generated from `mock_data/manifest.json`, scripted probes, and targeted
runtime checks. These findings should be verified against the real data on MONA.

## Evidence sources

- Standardized checks: {script path, command, or inline R block}
- Additional probes: {project-specific checks, or `None`}
- Metadata/docs consulted: {regmeta commands, docs, or `None`}

## Summary

{Overall assessment. Separate measured results from interpretation.}

## Verify on MONA

- [ ] {Specific item to check}
- [ ] {Another item}

## Standardized checks

- {Metric}: {Result}
- {Metric}: {Result or `Not assessed`}

## Additional probes

- {Question}: {Result or `Not assessed`}

## Per-file details

### {filename}
- Row count: {value}
- Join coverage: {value or `Not assessed`}
- Null-rate flags: {value or `None`}
- Category/code-set issues: {value or `Not assessed`}
- Notes: {short interpretation; label heuristics clearly}
```

### Continue the interview if needed

If the user did not provide enough research context earlier, ask for:

- research question
- identification strategy
- key outcomes
- key treatment or exposure variables
- key controls
- known data quirks or pitfalls

### Generate the remaining project files

Use [generated-files.md](generated-files.md). Generate:

- `src/pipeline.R`
- `src/helpers.R`
- `src/data_processing.R`
- `src/analysis.R`
- `src/plotting.R`
- `src/manage_packages.R`
- `run.R`
- `tests/testthat.R`
- `tests/testthat/test-guards.R`
- `{MEMORY}.md`
- `ROADMAP.md`

Rules:

- `src/data_processing.R` and `src/analysis.R` stay as exact stubs
- `src/manage_packages.R` stays verbatim from the bundled template
- `src/pipeline.R` keeps the minimal scaffold package list
- `run.R` is the non-interactive project entry point and should stay minimal

### Run the test suite

Before declaring success:

```bash
cd {projdir} && Rscript tests/testthat.R
```

If it fails, the most likely cause is non-ASCII characters in generated R
files. Replace them with `\\uXXXX` escapes or ASCII equivalents and re-run.

### Initialize git

Only do this after the tests pass.

1. If `{projdir}/.git/` already exists, skip git initialization entirely.
2. Check whether git is installed with `command -v git`.
3. If git is missing, ask whether the user wants to install it. Do not assume.
4. Check `git config --get user.email` and `git config --get user.name`.
5. If identity is missing, ask the user whether to set global or repo-local
   config. Do not set global config silently.
6. Then initialize and commit:

```bash
cd {projdir}
git init -q
git add -A
git commit -q -m "Initial project scaffold"
```

If the user declines git install or configuration, add this note directly
under the project title in `{MEMORY}.md`:

```markdown
> **Note:** This project is not under version control. The user declined
> to install or configure git at scaffold time. If that changes, run
> `git init && git add -A && git commit -m "Initial project scaffold"`
> from the project root.
```

## Post-scaffolding summary

Keep the final chat summary under about six lines:

1. confirm scaffolding is complete
2. mention the project directory, register-doc count, and green tests
3. tell the user to start a fresh agent session for implementation work

Use runtime-neutral wording such as:

> The scaffold is ready. Review `ROADMAP.md`, fill any obvious gaps, then
> start a fresh agent session from `{projdir}` to work on data processing and
> analysis code.
