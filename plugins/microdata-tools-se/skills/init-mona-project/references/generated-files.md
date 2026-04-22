# Generated Files and Templates

Use this file when you are ready to write files. It contains the required
templates and document structure.

## Bundled template files

Copy these files verbatim from `templates/`:

| Template | Destination |
|----------|-------------|
| `templates/air.toml` | `{projdir}/air.toml` |
| `templates/run.R` | `{projdir}/run.R` |
| `templates/src/helpers.R` | `{projdir}/src/helpers.R` |
| `templates/src/plotting.R` | `{projdir}/src/plotting.R` |
| `templates/src/manage_packages.R` | `{projdir}/src/manage_packages.R` |
| `templates/tests/testthat.R` | `{projdir}/tests/testthat.R` |
| `templates/tests/testthat/test-guards.R` | `{projdir}/tests/testthat/test-guards.R` |

Do not modify those files during scaffolding.

## `{slug}.Rproj`

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

## `_targets.yaml`

```yaml
main:
  script: src/pipeline.R
```

## `.gitignore`

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

## `src/pipeline.R`

Interpolate `{P_NUM}` with the actual project number such as `P1405`.

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

c(
  list(
    # -- Paths ----------------------------------------------------------------
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
  ),
  list(
    # Add targets below inside this second list.
    # -- Data loading targets --------------------------------------------------
    # Example:
    # tar_target(
    #   raw_persons,
    #   read_data(file.path(raw_data_path, "persons_2020.csv"))
    # )
    # -- Processing targets ----------------------------------------------------
    # -- Analysis targets ------------------------------------------------------
    # -- Output targets --------------------------------------------------------
  )
)
```

## `src/data_processing.R`

```r
# Data processing functions for {slug}
# Each function takes raw data (from read_data) and returns a data.table
```

## `src/analysis.R`

```r
# Analysis functions for {slug}
```

## Memory files

Write this content to `{MEMORY}.md` only.

- In Claude Code, `{MEMORY}.md` is `CLAUDE.md`.
- In other agent runtimes, `{MEMORY}.md` is `AGENTS.md`.
- Never create both files or a symlink between them during one scaffold run.

````markdown
# {Project title}

**Research question**: {from interview or research plan}

**Identification strategy**: {from interview or research plan}

## CRITICAL: PERSONAL DATA MUST NEVER LEAVE MONA

This project uses individual-level registry data from SCB, accessed through
MONA. **Under no circumstances may any personal data be exported from MONA.**
Only aggregate statistics, tables, and figures may be exported.

- `output/` must contain only aggregate results that are safe to export.
- Never include row-level data in commits, documentation, issue text, agent
  messages, or any file that leaves MONA.
- `output_mock/` is local synthetic output only and must never be mistaken for
  real MONA output.

## You only have mock data

- All data in `mock_data/` is synthetic, generated from aggregate statistics.
- The mock data preserves schema but not the real statistical properties.
- Distributional findings must be verified on MONA before they drive research
  decisions.
- If schema expectations do not match real MONA data, investigate the mismatch
  instead of papering over it.

## Project structure

- `run.R` - non-interactive entry point for running the pipeline on MONA
- `src/` - R code uploaded to MONA
- `src/pipeline.R` - targets graph and shared path/output targets
- `notes/` - local project documentation
- `mock_data/` - synthetic CSVs plus `manifest.json`
- `output/` - MONA outputs, git-tracked, aggregate only
- `output_mock/` - local mock outputs, gitignored
- `tests/` - local testthat tests

## Path handling

- Use `here::here()` for repo-relative paths.
- MONA paths are UNC paths: `//micro.intra/Projekt/{P_NUM}$/{P_NUM}_Data`.
- Never use `setwd()`.
- `raw_data_path` should auto-detect env var, MONA path, then `mock_data/`.

## Output conventions

- `output/` is for MONA results and must remain aggregate-only.
- `output_mock/` is for local mock-data runs.
- Both use `tables/`, `plots/`, and `logs/` subdirectories.

## MONA workflow

- Upload files via **My Files** and move them from Inkorg into the project folder.
- Run scripts via the **Batch client** or the MONA RStudio session.
- Review every export manually before it leaves MONA.

## Targets pipeline

- `run.R` is the entry point for non-interactive runs on MONA.
- `src/pipeline.R` defines the targets graph and top-level path logic.
- `raw_data_path` resolves `RAW_DATA_PATH`, then the MONA UNC path, then
  `mock_data/`.
- `output_dir` switches between `output/` and `output_mock/` depending on the
  active data source.
- Put reusable data-cleaning logic in `src/data_processing.R`.
- Put analysis code in `src/analysis.R` and plotting helpers in `src/plotting.R`.

## Documentation

- `notes/data_*.md` - register-level documentation and caveats
- `notes/mock_data_assessment.md` - measured mock-data findings plus what to
  verify on MONA
- `ROADMAP.md` - handoff for the next agent session

## Pipeline audits

Use `assertr` checks in data-processing code so they run on mock data and on
MONA:

```r
library(assertr)
dt |>
  verify(nrow(.) > 0) |>
  assert(not_na, id_column) |>
  assert(in_set(expected_values), cat_var)
```

## Testing strategy

- `tests/testthat/` is for local checks only; run it before upload, not on MONA
- pipeline audits inside `src/` should catch mock-versus-real divergence on both
  local runs and MONA runs
- the ASCII guard is a local pre-upload safety check

## Code formatting

Format all R code with:

```bash
air format src/
```

## Encoding

- R source files must be ASCII-only.
- Use `\\uXXXX` escapes for non-ASCII characters.
- Real MONA data may require explicit encoding handling.
````

## `ROADMAP.md`

```markdown
# Roadmap: {Project title}

## What this project is
{2-4 sentences from the research question and identification strategy.}

## Where we are
- Local R scaffold is set up.
- Mock data generated from `stats.json` ({N} files across {M} registers).
- `run.R` is ready as the MONA pipeline entry point.
- Register docs live in `notes/data_*.md`.
- Mock-data caveats live in `notes/mock_data_assessment.md`.
- MONA packages still need installation there via `src/manage_packages.R`.

## Open questions for the user
{Anything unresolved during scaffolding.}

## Next steps

### 1. Confirm mock-data findings on MONA
Spot-check the items flagged in `notes/mock_data_assessment.md` before coding.

### 2. Write data processing
{Name the first register or file family to load only if the research plan
makes it clearly primary. Otherwise say `Not yet specified` and list what
must be clarified first. Point to the relevant `notes/data_*.md`. Keep this
to one loading or harmonization step, not a full pipeline.}

### 3. Write analysis
{If the research design is already pinned down, state the first analysis task
briefly. Otherwise say `Not yet specified` and list the unresolved design
choices. Do not invent an estimator, function name, control set, or outcome
hierarchy the user did not provide.}

### 4. Output and export review
Keep `output/` aggregate-only and manually review exports before they leave MONA.

## Pointers
- `{MEMORY}.md` - authoritative project briefing for the current runtime
- `notes/` - data docs plus mock-data assessment
- `run.R` - non-interactive entry point for MONA runs
- `src/pipeline.R` - current targets entry point
```

## Conventions

- Format generated R code with `air`.
- Keep `.R` files ASCII-only.
- Use `here::here()` for paths.
- Use `data.table`, not tidyverse, in scaffolded code.
- Do not introduce `renv`.
- `output/` is git-tracked; `output_mock/` is gitignored.
- Everything must run non-interactively through `Rscript` on MONA.
