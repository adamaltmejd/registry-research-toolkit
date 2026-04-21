---
name: init-mona-project
description: "Scaffold a local R environment for an existing SCB MONA research project"
argument-hint: "[project-slug]"
---

# Initialize MONA Research Project

Scaffold a local R development environment for an existing SCB MONA registry
research project. The project must already exist on MONA (ethics approval and
data access granted). This skill bridges the isolated MONA server to local
agent-assisted development using mock data.

Arguments: `$ARGUMENTS`

## Your role: prepare, do not research

You are **the scaffolder**. Your job is to set up a clean, well-documented
foundation so that the researcher and future agent sessions can do the
actual research work. That foundation is what everything else rests on —
get it right and the next six months of work go smoothly; cut corners
and every downstream session pays the tax.

You are explicitly **not** expected to:

- Write data-processing functions, estimators, or analysis code. Those go
  in `src/data_processing.R` and `src/analysis.R`, which you leave as
  stubs. The researcher (or a future session) writes them after
  verifying the data on MONA.
- Decide the identification strategy, pick control variables, or make
  research-design calls. Record what the user told you, flag open
  questions in `ROADMAP.md`, and stop.
- Guess column codings, value sets, or variable semantics that have not
  been confirmed on real MONA data. Mock data is synthetic — anything
  inferred from it is provisional.
- Pre-populate packages you are *imagining* the user will need. Start
  from the minimal template list; future sessions will add packages
  when real code demands them.

You **are** expected to:

- Conduct the interview carefully and capture the answers faithfully.
- Run the tools (`mock-data-wizard`, `regmeta`) exactly as specified and
  surface what they return without embellishment.
- Copy the template files verbatim — they are the result of prior
  design decisions and should not be "improved" during scaffolding.
- Produce documentation (`notes/data_*.md`, `notes/mock_data_assessment.md`,
  `{MEMORY}.md`, `ROADMAP.md`) that is concrete, honest about uncertainty,
  and useful for the next agent who walks in cold.
- Leave the project in a state where the researcher can open it, read
  `ROADMAP.md`, and know exactly what to do next.

Think of yourself as the person building the workbench, not the person
who will use it. A good workbench is solid, obvious, and unambiguous.
A bad one is full of pre-set jigs for problems the carpenter hadn't
decided to solve yet.

## Agent memory file

This skill writes an agent memory file into the scaffolded project. Which
name you use depends on the runtime you are running under:

- **Claude Code** → write `CLAUDE.md` as the primary file.
- **Codex** → write `AGENTS.md` as the primary file.

After writing the primary file, create a symlink at the *other* name
pointing to it, so the project is portable if a future session is handled
by a different agent:

```bash
# If you wrote CLAUDE.md:
cd {projdir} && ln -s CLAUDE.md AGENTS.md
# If you wrote AGENTS.md:
cd {projdir} && ln -s AGENTS.md CLAUDE.md
```

Throughout the rest of this skill, the placeholder `{MEMORY}.md` refers to
whichever name you wrote as primary. When you see it, substitute the
correct filename.

## Phase detection

Detect the current state by checking the filesystem. The project directory
is either `$ARGUMENTS/` (a subfolder) or the current working directory
itself, depending on how the user answered the setup questions (see
Phase 1.1). Check both: if `$ARGUMENTS` is set and a directory with that
name exists, use it; else if the current working directory already
contains `.Rproj`, `stats.json`, or `mock_data/`, treat CWD as the
project directory.

| Condition | Action |
|-----------|--------|
| No project signals found (no slug-named dir, no scaffold in CWD) | Phase 1: interview + scaffold |
| Project dir exists, no `stats.json` | Waiting for MONA round-trip — remind user to run the extraction script and bring back `stats.json` |
| Project dir exists, has `stats.json`, no `mock_data/manifest.json` | Generate mock data, then Phase 2 |
| Project dir exists, has `mock_data/manifest.json`, no `CLAUDE.md` or `AGENTS.md` | Phase 2: enrichment |
| Project dir exists, has `CLAUDE.md` or `AGENTS.md` | Already initialized — tell the user |

---

## Phase 1: Interview and mock data bootstrap

### 1.1 Interview

Collect these things from the user:

1. **Project slug** — use `$ARGUMENTS` if provided, otherwise ask. Must be
   `lowercase-with-hyphens`, no spaces. Used as the `.Rproj` filename and
   suggested git repo name.

2. **Target directory** — decide where to scaffold:
   - **If the current working directory is empty** (no files, only
     `.claude/`, `.codex/`, or similar invisible metadata), scaffold *into it*
     directly. This is the common case when the user has already created
     and navigated to the project folder. Do not ask — just announce
     what you are doing.
   - **If the current working directory is non-empty**, ask whether to
     (a) create `{slug}/` as a subfolder, or (b) scaffold into the
     current dir anyway. Do not assume; a non-empty cwd could be an
     existing repo the user doesn't want polluted.

   The scaffold layout is the same either way — the only difference is
   whether `{slug}/` is nested or the current dir itself is the project
   root.

3. **SCB project number** — the P-number (e.g. `P1405` or just `1405`).
   Normalize to the format `P{num}` with no leading zeros. This determines
   the MONA UNC path: `//micro.intra/Projekt/P{num}$/P{num}_Data`.

4. **Research plan** — strongly encourage the user to paste their research
   plan or point to a file. Say:

   > Pasting your research plan will significantly improve the project setup.
   > I will use it to generate project-specific agent instructions, data
   > documentation priorities, and a pipeline skeleton tailored to your
   > identification strategy.

   If the user declines, proceed — but the `{MEMORY}.md` will be less specific.

In the rest of this skill, `{projdir}` refers to the resolved project
directory (either `{slug}/` or the current working directory, based on
the user's choice above).

### 1.2 Create minimal scaffold

```bash
mkdir -p {projdir}/src {projdir}/notes {projdir}/tests/testthat {projdir}/output {projdir}/output_mock
```

Generate these files immediately (templates in Section 5):

- `{projdir}/{slug}.Rproj`
- `{projdir}/.gitignore`
- `{projdir}/_targets.yaml`

### 1.3 Preflight: verify tools are installed

Before doing anything else, confirm both CLIs are on `PATH` and runnable:

```bash
mock-data-wizard --version
regmeta --version
```

If either command fails (not found, dependency error, broken install),
**stop and install**. Both tools live in the `registry-research-toolkit`
repo at <https://github.com/adamaltmejd/registry-research-toolkit>. Install
with `uv` — not `pip`, not `uv run`:

```bash
# regmeta: PyPI + prebuilt database
uv tool install regmeta
regmeta maintain update       # downloads the SCB registry metadata DB (~400 MB)

# mock-data-wizard: from GitHub (not published to PyPI)
uv tool install "mock-data-wizard @ git+https://github.com/adamaltmejd/registry-research-toolkit#subdirectory=mock_data_wizard"
```

If a previous install is broken (rare — dependency conflict after an
upgrade), reinstall with `--force`:

```bash
uv tool install --force regmeta
uv tool install --force "mock-data-wizard @ git+https://github.com/adamaltmejd/registry-research-toolkit#subdirectory=mock_data_wizard"
```

Do not proceed to Phase 1 file generation until both `--version` checks
succeed.

### 1.4 Generate the extraction script

Both CLIs are invoked **directly** (they are on `PATH` after
`uv tool install`). Never use `uv run mock-data-wizard` or
`uv run regmeta` — that attempts a fresh environment resolution and
fails with dependency errors.

```bash
mock-data-wizard generate-script -p P{num} -o {projdir}/extract_stats.R
```

### 1.5 MONA handoff

Tell the user exactly what to do. Print something like:

> **Next steps on MONA:**
>
> 1. Upload `extract_stats.R` to MONA via **My Files** (the web file
>    manager). The file lands in your Inkorg — move it to your project
>    folder.
> 2. Open the **Batch client**, select **R** as the language, pick
>    `extract_stats.R`, and run it.
> 3. The script produces `stats.json` in the working directory.
> 4. **Review `stats.json` in Notepad++ before exporting.** Open the file
>    and carefully check that it contains no individual-level data. The
>    script is designed to export only aggregates with disclosure
>    control, but *you* are ultimately responsible for what leaves MONA.
>    If you see any value that could be tied to an individual, replace
>    that number with `null` (keeping the JSON valid).
> 5. Once you are certain the file has no micro data, export it using
>    the **file exporter**, then download it from **My Files**.
> 6. Place `stats.json` in `{projdir}/` and come back here — run
>    `/init-mona-project {slug}` or just tell me you have `stats.json`.

**Phase 1 ends here.** Do not generate the remaining project files yet.

---

## Mock data generation

When `stats.json` exists but `mock_data/manifest.json` does not:

```bash
cd {projdir} && mock-data-wizard generate --stats stats.json --output-dir mock_data/ -y --force
```

(Call `mock-data-wizard` directly — not via `uv run`. See Phase 1.3.)

Verify that `mock_data/manifest.json` was created. If the command fails,
diagnose and help the user fix it (common issues: stats.json format,
missing regmeta database). Then proceed to Phase 2.

---

## Phase 2: Project enrichment

Mock data is now available. This phase generates all remaining project files.

### 2.1 Data documentation

Read `mock_data/manifest.json` to get the file inventory. Group the entries
**by register** — a typical MONA project has many year-by-year files per
register (e.g. 15 `Äp3_{year}.csv` files all from register 305). Write
**one `notes/data_{register_slug}.md` per register group**, not one per
file. Year-by-year variation goes inside the doc (a year-range line + a
note on any missing years).

For each register group:

1. Run `regmeta resolve --columns "{comma-separated columns from a representative file}" --register {register_hint} --format json`
   to map column names to variable definitions.
2. Run `regmeta get register {register_hint}` for the human-readable
   register name.
3. If the register hint is the catch-all 366 ("Analyser och statistik..."),
   treat it as low-confidence and say so in the doc.
4. Generate `notes/data_{register_slug}.md` following this format:

```markdown
# {Human-readable register name} (register {register_id})

{Brief description of the register and what it provides to this project.}

**Files**: `{filename pattern, e.g. Äp3_{year}.csv}`
**Year range**: {e.g. 2010–2024, no 2020/21}
**Row count**: {approximate range across years}

## Available variables

| Variable | Column | Explanation |
| --- | --- | --- |
| {var_name} | `{column_name}` | {definition from regmeta} |

## How it is used

<!-- TODO: describe how the pipeline uses these files -->

## Caveats and issues

<!-- TODO: note any known issues, missing years, encoding quirks -->

## Useful regmeta commands

\`\`\`bash
regmeta get register {register_id}
regmeta get schema --register {register_id} --years {year_hint}
regmeta resolve --columns "{key_columns}" --register {register_id}
\`\`\`
```

If a file cannot be confidently matched to a register (e.g. survey files
with generic columns), give it its own `data_{filename_slug}.md` and flag
the uncertainty in the doc.

Generate `notes/README.md` as an index of all `data_*.md` files, following
this pattern:

```markdown
# Project Notes

Working documentation for humans and agents. The `data_*.md` files document
pipeline inputs. Other notes capture analysis decisions, dead ends, and
findings that need to persist across sessions.

## Data Sources

- [data_{name}.md](data_{name}.md) — {one-line description}
```

### 2.2 Mock data assessment

**This is the most important file you write.** Everything else you
produce (`{MEMORY}.md`, `ROADMAP.md`, data docs) is either boilerplate or
mechanical translation of tool output. The mock-data assessment is
where you add real judgment — concrete, file-specific observations
that the researcher could not trivially re-derive. A generic "verify
data on MONA" checklist is a failure; a sentence like *"person-ID
overlap between Äp6 and slutbetyg_Ak9 is <1% on mock, real data should
show ≥80%, investigate if not"* is the output.

Actually probe the data. Load sample rows with `fread`, compute
`n_distinct`, cross-tabulate shared ID columns across files, check
null rates. Do not just read the manifest and paraphrase it.

Examine the mock data to flag potential representativeness issues. For each
mock CSV, check:

- **Row counts** — are they plausible for the population? (use regmeta and
  the research plan for context)
- **ID column cardinality** — low `n_distinct` on shared ID columns will
  cause joins to drop rows; compare across files
- **Null rates** — flag variables with >30% nulls for verification
- **Categorical distributions** — compare against regmeta value sets; flag
  codes present in mock data but absent from regmeta (or vice versa)
- **Cross-file join coverage** — do shared ID columns have overlapping values
  across the files that will be linked?

Write findings to `notes/mock_data_assessment.md`:

```markdown
# Mock Data Assessment

Generated from `mock_data/manifest.json`. These findings should be verified
against the real data on MONA.

## Summary

{Overall assessment}

## Verify on MONA

- [ ] {Specific item to check}
- [ ] {Another item}

## Per-file details

### {filename}
{Row count, key observations, flags}
```

### 2.3 Continue interview

If the research plan was not provided in Phase 1, or if it lacks detail, ask:

- Research question
- Identification strategy (diff-in-diff, RDD, panel FE, IV, etc.)
- Key outcome variables
- Key treatment/exposure variables
- Key control variables
- Any known data quirks or pitfalls

### 2.4 Generate project files

Generate all remaining files using the templates in Section 5:

- `src/pipeline.R`
- `src/helpers.R`
- `src/data_processing.R`
- `src/analysis.R`
- `src/plotting.R`
- `src/manage_packages.R`
- `tests/testthat.R`
- `tests/testthat/test-guards.R`
- `{MEMORY}.md` (plus the cross-tool symlink — see "Agent memory file" above)
- `ROADMAP.md`

**Scaffolding is not coding.** Do not write function bodies, analysis code,
or package imports beyond what Section 5 shows.

- `src/data_processing.R` and `src/analysis.R` must be **exact copies of
  the Section 5 stubs** (header comment only). The user writes the functions
  in a follow-up session, using the research plan and `notes/`. Pre-filling
  them tempts the agent into speculating about column codings, value sets,
  and identification details that have not been verified on MONA.
- `src/manage_packages.R` must be copied **verbatim from the template**.
  Do not add packages based on code you are imagining the user will write.
  If a real need arises later, add packages in a follow-up session.
- `tar_option_set(packages = ...)` in `src/pipeline.R` must match the
  template's minimal list. No `fixest`, no `scales`, no project-specific
  additions at scaffold time.

### 2.5 Run the test suite before declaring done

Before printing the post-scaffolding summary, run:

```bash
cd {projdir} && Rscript tests/testthat.R
```

This runs the ASCII guard and other unit tests. If it fails, the most
common cause is non-ASCII characters (e.g. `Ä`, `ö`, `—` em-dash) that
slipped into the generated `.R` files. Fix by replacing with `\uXXXX`
escapes or ASCII equivalents (use `-` not `—`) and re-run. Do not finish
Phase 2 until the suite is green.

### 2.6 Initialize the git repo

After the test suite passes, initialize git in `{projdir}` and make the
first commit yourself — do **not** tell the user to do this. Work through
these checks in order:

**1. Already a repo?** If `{projdir}/.git/` exists, skip this phase entirely.

**2. Is git installed?** Run `command -v git`. If absent:

- Tell the user git is not installed and ask whether they want to
  install it now. On macOS, `xcode-select --install` pulls it in;
  on Debian/Ubuntu, `sudo apt install git`. Offer to run the command
  for them (it will prompt for their password).
- If they decline, skip to "not using git" below. **Do not just move on
  silently** — the user needs to know git was never initialized.

**3. Is git configured?** Run `git config --get user.email` and
`git config --get user.name`. If either is empty:

- Tell the user git has no identity set and ask for email + name.
  Offer to set them with `git config --global user.email ...` /
  `user.name ...` (explain this affects all their git repos, not
  just this one). If they prefer repo-local, use `git config` without
  `--global` from inside `{projdir}` *after* running `git init`.
- If they decline to configure git at all, skip to "not using git" below.

**4. Initialize and commit:**

```bash
cd {projdir}
git init -q
git add -A
git commit -q -m "Initial project scaffold"
```

**"Not using git" path.** If the user declined to install or configure
git, append this block to `{projdir}/{MEMORY}.md` (at the very top, right
under the project title) and tell the user you did so:

```markdown
> **Note:** This project is not under version control. The user declined
> to install or configure git at scaffold time. If that changes, run
> `git init && git add -A && git commit -m "Initial project scaffold"`
> from the project root.
```

Do not silently configure git globally on the user's behalf under any
circumstance.

---

## Section 5: File templates

### `{slug}.Rproj`

```text
Version: 1.0

RestoreWorkspace: No
SaveWorkspace: No
AlwaysSaveHistory: No

EnableCodeIndexing: Yes
UseSpacesForTab: Yes
NumSpacesForTab: 2
Encoding: UTF-8

AutoAppendNewline: Yes
StripTrailingWhitespace: Yes
LineEndingConversion: Native
```

### `_targets.yaml`

```yaml
main:
  script: src/pipeline.R
```

### `.gitignore`

```text
mock_data/
output_mock/
_targets/
extract_stats*.R
.Rhistory
.DS_Store
Rplots.pdf
*.Rproj.user
```

### `src/pipeline.R`

Interpolate `{P_NUM}` with the actual project number (e.g. `P1405`).
Populate the `packages` vector in `tar_option_set` based on what
`manage_packages.R` lists.

```r
library(targets)
library(here)

tar_option_set(
  packages = c(
    "data.table",
    "bit64",
    "lubridate",
    "assertr",
    "ggplot2"
  ),
  format = "qs",
  workspace_on_error = FALSE,
  error = "stop",
  trust_timestamps = TRUE,
  memory = "persistent",
  garbage_collection = FALSE
)

source(here::here("src", "helpers.R"))
source(here::here("src", "data_processing.R"))
source(here::here("src", "analysis.R"))
source(here::here("src", "plotting.R"))

list(
  # -- Paths ------------------------------------------------------------------
  tar_target(
    raw_data_path,
    {
      path <- trimws(Sys.getenv("RAW_DATA_PATH", unset = ""))
      if (nzchar(path)) {
        path.expand(path)
      } else if (dir.exists("//micro.intra/Projekt/{P_NUM}$/{P_NUM}_Data")) {
        "//micro.intra/Projekt/{P_NUM}$/{P_NUM}_Data"
      } else {
        here::here("mock_data")
      }
    },
    cue = tar_cue(mode = "always")
  ),
  tar_target(
    output_dir,
    {
      is_mock <- identical(raw_data_path, here::here("mock_data"))
      if (is_mock) {
        message("Using mock data -- outputs go to output_mock/")
      }
      dir <- here::here(if (is_mock) "output_mock" else "output")
      dir.create(
        file.path(dir, "tables"),
        recursive = TRUE,
        showWarnings = FALSE
      )
      dir.create(
        file.path(dir, "plots"),
        recursive = TRUE,
        showWarnings = FALSE
      )
      dir.create(
        file.path(dir, "logs"),
        recursive = TRUE,
        showWarnings = FALSE
      )
      dir
    }
  )
  # Add targets below. Remember the comma after output_dir when you do.
  # -- Data loading targets ----------------------------------------------------
  # Example:
  # ,tar_target(
  #   raw_persons,
  #   read_data(file.path(raw_data_path, "persons_2020.csv"))
  # )
  # -- Processing targets ------------------------------------------------------
  # -- Analysis targets --------------------------------------------------------
  # -- Output targets ----------------------------------------------------------
)
```

### Template files (copy verbatim)

The following files live in `templates/` next to this SKILL.md
(i.e. `plugins/microdata-tools-se/skills/init-mona-project/templates/` in
the `registry-research-toolkit` repo, or the corresponding path in your
agent tool's plugin cache). Copy them to the project directory without
modification:

| Template | Destination |
|----------|-------------|
| `templates/air.toml` | `{projdir}/air.toml` |
| `templates/src/helpers.R` | `{projdir}/src/helpers.R` |
| `templates/src/plotting.R` | `{projdir}/src/plotting.R` |
| `templates/src/manage_packages.R` | `{projdir}/src/manage_packages.R` |
| `templates/tests/testthat.R` | `{projdir}/tests/testthat.R` |
| `templates/tests/testthat/test-guards.R` | `{projdir}/tests/testthat/test-guards.R` |

Read each template file from the skill directory (relative to this SKILL.md)
and write it to the project. The templates are well-documented, general-purpose,
and ready to use. Do not modify them during scaffolding.

After copying, adjust `manage_packages.R` if the project needs additional
packages beyond the defaults (add them to the `managed_packages` vector).

### `src/data_processing.R`

```r
# Data processing functions for {slug}
# Each function takes raw data (from read_data) and returns a data.table
```

### `src/analysis.R`

```r
# Analysis functions for {slug}
```

### `{MEMORY}.md`

This is the most critical generated file — write it under the filename
appropriate to your runtime (see "Agent memory file" at the top of this
skill). Interpolate all `{placeholders}` with project-specific content
from the interview. The agent must write this file thoughtfully — it is
the primary interface between the user, future agent sessions, and the
MONA workflow.

````markdown
# {Project title}

**Research question**: {from interview or research plan}

**Identification strategy**: {from interview or research plan}

## CRITICAL: PERSONAL DATA MUST NEVER LEAVE MONA

This project uses individual-level registry data from SCB, accessed through
MONA. **Under no circumstances may any personal data be exported from MONA.**
Only aggregate statistics, tables, and figures may be exported.

- The `extract_stats.R` script exports only aggregate statistics (means,
  frequencies, quantiles) with disclosure control (k-anonymity censoring,
  noise perturbation).
- Pipeline outputs in `output/` must contain only aggregate results.
- Never include individual-level data in code comments, commit messages,
  documentation, or any file that leaves MONA.

## You only have mock data

- All data in `mock_data/` is synthetic, generated from aggregate statistics.
- The mock data preserves the schema (column names, types, value codes) but
  not the statistical properties of the real data.
- Issues related to data properties (distributions, missing patterns, outliers)
  might be mock-data artifacts — verify on MONA before drawing conclusions.
- **The MONA schema is fixed.** If column names do not match what the code
  expects, treat it as a bug to investigate, not something to paper over
  with tolerance logic.

## Project structure

- `src/` — all R code. This is what gets uploaded to MONA.
  - `pipeline.R` — targets pipeline definition
  - `helpers.R` — IO, assertions, target factories
  - `data_processing.R` — data wrangling functions
  - `analysis.R` — estimation and analysis functions
  - `plotting.R` — plot helpers and theme
  - `manage_packages.R` — package installer for MONA
- `notes/` — project documentation (local only, not uploaded to MONA)
  - `data_*.md` — one file per registry extract
  - Other `.md` files for analysis notes, dead ends, decisions
- `mock_data/` — synthetic CSVs + `manifest.json` (gitignored)
- `output/` — pipeline outputs from MONA (git-tracked; must not contain PII)
- `output_mock/` — pipeline outputs from local mock runs (gitignored)
- `tests/` — testthat unit tests (local only; MONA validation goes in the
  pipeline via assertr)

## Path handling

- Use `here::here()` for all repo-relative paths. The `.Rproj` file is the
  root anchor (`here` finds it even without `.git`).
- MONA paths are UNC: `//micro.intra/Projekt/{P_NUM}$/{P_NUM}_Data`.
  These are Windows paths — normalize separators before matching.
- Never use `setwd()`.
- The `raw_data_path` target auto-detects: env var > MONA UNC path > mock_data/.

## Output conventions

- `output/` for MONA pipeline results. Git-tracked. Must contain only
  aggregate statistics, tables, and plots — never individual-level data.
- `output_mock/` for local mock-data runs. Gitignored.
- Both have subdirectories: `tables/`, `plots/`, `logs/`.

## MONA workflow

**Upload.** Upload files via **My Files** (the MONA web file manager).
They land in your Inkorg — move them into the project folder.

**Run.** Open the **Batch client**, select **R** as the language, pick
the script (e.g. a wrapper that calls `targets::tar_make()`, or open
`.Rproj` in the MONA RStudio session and run there).

**Export.** Before exporting anything from `output/`:

1. Open every file you intend to export (CSV in Notepad++ or Excel, plots
   by eye). Confirm the content is aggregate only — no row counts that
   could re-identify an individual, no rare categories that violate
   k-anonymity, no raw text fields.
2. You are ultimately responsible for what leaves MONA. The pipeline's
   `export_privacy_audit` or equivalent checks help but do not substitute
   for human review.
3. Export via the **file exporter**, wait for approval, then download from
   **My Files**.

Housekeeping:

- First MONA setup: upload `src/manage_packages.R` and run it to install
  packages.
- After code changes: upload only the changed `src/*.R` files.
- Keep `output/` (MONA results, git-tracked) and `output_mock/` (local
  mock runs, gitignored) strictly separate — never overwrite MONA exports
  with local mock results.

## Data file inventory

{Generate a table from manifest.json, grouped by register:}

| Register | Files | Year range | Key columns | Notes doc |
|----------|-------|------------|-------------|-----------|
| {register_id} — {register name} | `{filename pattern}` | {year range} | {important cols} | [data_{slug}.md](notes/data_{slug}.md) |

> Filenames in this table are shown verbatim (including any Swedish
> characters). When these filenames appear in `.R` source code, they
> must be written using `\uXXXX` escapes — e.g. `"\u00c4p3_2024.csv"`
> instead of `"Äp3_2024.csv"`. The ASCII-guard test (`tests/testthat/`)
> enforces this on every `src/*.R` file.

**Column casing.** Most files use `P{num}_LopNr_*` (camel-case); a few
use `P{num}_Lopnr_*` (all-lowercase `Lopnr`). `read_data()` in
`src/helpers.R` normalizes everything to the `_LopNr_` form
automatically. If you load a table some other way, call
`normalize_mona_cols()` on it explicitly before joining.

## Pipeline audits

Use `assertr` for data validation that runs on both mock and real data:

```r
library(assertr)
dt |>
  verify(nrow(.) > 0) |>                    # not empty
  assert(not_na, id_column) |>              # key not null
  assert(in_set(expected_values), cat_var)   # valid codes only
```

Place audits in data processing functions so they run as part of the pipeline
on both mock data and MONA. If a merge drops all observations on mock data
but works on MONA, the ID columns have likely drifted — check `n_distinct`
and cross-file overlap.

## Testing strategy

Two layers — local tests and pipeline audits:

- **`tests/testthat/` (local only)** — unit tests for functions in `src/`.
  Run with `Rscript tests/testthat.R`. These test function logic: edge cases,
  type contracts, expected output shapes. They are not uploaded to MONA.
- **Pipeline audits (run on MONA)** — assertr assertions baked into data
  processing functions in `src/`. These run as part of `targets::tar_make()`
  on both mock data and real data. They catch mock-vs-real divergence:
  unexpected row drops, invalid codes, join failures.
- The ASCII guard test (in `tests/testthat/`) ensures all `src/*.R` files
  contain only ASCII characters before upload to MONA.

## Code formatting

Format all R code with `air` before uploading to MONA:

```bash
air format src/
```

Run this after every change to `src/*.R` files. All R code in this project
follows `air` formatting conventions.

## Encoding

- **R source files must be ASCII-only.** Use `\\uXXXX` escapes for non-ASCII
  characters (e.g. Swedish: `\\u00C4` for Ae, `\\u00D6` for Oe, `\\u00E5` for aa).
- MONA runs Windows with a locale that uses Windows-1252 or Latin-1 encoding.
- Mock data CSVs are UTF-8. Real MONA data files may be Windows-encoded.
  Use `fread(..., encoding = "UTF-8")` or `encoding = "Latin-1"` as needed.
- The testthat ASCII guard enforces this — run tests before uploading to MONA.
````

Adapt this template based on the actual interview answers and data inventory.
The sections above are mandatory — do not omit any of them. Add
project-specific sections as needed (e.g. variable construction notes,
sample selection criteria).

### `ROADMAP.md`

This is the **handoff document for future agent sessions**. Write it like
a note to the next agent (or the user starting fresh tomorrow) — concise,
concrete, and oriented around what to do next. Do not list everything
that could ever be done; list what comes first and why.

Structure:

```markdown
# Roadmap: {Project title}

## What this project is
{2–4 sentences: research question + identification strategy, drawn from
the interview. Keep it short — `{MEMORY}.md` has the detail.}

## Where we are
- Local R scaffold is set up.
- Mock data generated from `stats.json` ({N} files across {M} registers).
- Register docs in `notes/data_*.md`; mock-data caveats in
  `notes/mock_data_assessment.md`.
- MONA packages not yet installed — `src/manage_packages.R` still needs
  to be uploaded and run there.

## Open questions for the user
{Anything that came up during scaffolding but wasn't answered — e.g. value
set for UtlSvBakg, year cutoffs, treatment definition. List these so the
next agent knows to resolve them before coding.}

## Next steps

### 1. Confirm mock-data findings on MONA
Upload `src/manage_packages.R` + `extract_stats.R`, run them, and spot-check
the items flagged in `notes/mock_data_assessment.md`. Do not proceed to
coding before doing this — mock-vs-real divergence can invalidate later
work silently.

### 2. Write data processing
{Specific first function to define — name it, say what it takes and
returns, point to the relevant `notes/data_*.md`. Two or three bullets of
concrete starter work.}

Example:
- `build_population(population, fodelseuppg)` — one row per LopNr with
  birth year, sex, country of birth, imm status. See `notes/data_rtb.md`.
- Add an `assertr` check that `P1105_LopNr_PersonNr` is unique after the
  merge.

### 3. Write analysis
{One sentence pointing the next agent at the estimator they should
scaffold first, e.g. the baseline DiDiD without controls, in `src/analysis.R`.}

### 4. Output and export review
{Reminder that `output/` goes to MONA, must be PII-free, reviewed in
Notepad++ before export.}

## Pointers
- `{MEMORY}.md` — the authoritative agent briefing. Read it first.
- `notes/` — data docs + mock-data assessment.
- `src/pipeline.R` — targets entry point; currently scaffolded with paths
  only, no data targets yet.

---

*This roadmap was generated at scaffold time. Update it as the project
evolves — it is the fastest way for a new agent session to get oriented.*
```

Tailor the "Next steps" section to the actual research plan. If the user
gave a detailed plan in the interview, the roadmap can be specific; if
they declined to share one, keep the next-steps section generic and add a
top-level line: *"Before coding, ask the user for the research plan and
identification strategy."*

---

## Post-scaffolding summary

After generating all files and verifying `Rscript tests/testthat.R`
passes, print **a short** message to the user — two things only:

1. Scaffolding complete. One line confirming what was set up (project
   directory path, register count in `notes/`, tests green).
2. **Start a new session to do any further work.** The project is now
   self-contained: `{MEMORY}.md` orients the next agent, `ROADMAP.md` lists
   what to tackle first. Tell the user something like:

   > The project is ready and committed to a fresh git repo. Review
   > `ROADMAP.md` and fill in any gaps, then start a fresh `claude`
   > session from `{projdir}` to begin working on the data processing
   > and analysis code.

Do **not** print a giant tree, a long status dump, or next-step
recommendations in chat. Everything the next session needs is in
`ROADMAP.md` and `{MEMORY}.md`. Keep the chat summary under ~6 lines.

---

## Conventions (hard rules for file generation)

These rules apply when generating all project files:

1. **Format with `air`** — run `air format src/` after every R code change.
   All generated R code must conform to `air` style.
2. **ASCII only in `.R` files** — no Swedish characters, no UTF-8 literals.
   Use `\uXXXX` escapes. The testthat guard enforces this.
3. **`here::here()` for all paths** — never `setwd()`, never bare relative paths.
4. **`data.table`** for data manipulation, not tidyverse.
5. **No renv** — `manage_packages.R` handles package installation on MONA.
6. **UNC path format** for MONA: `//micro.intra/Projekt/P{num}$/P{num}_Data`.
7. **`output/` is git-tracked**, `output_mock/` is gitignored.
8. **No interactive R** — everything must run via `Rscript` in MONA Batch client.
9. **Deterministic seeds** wherever randomness is used in the pipeline.
