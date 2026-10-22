# React port tracker

Cross-PR coordination for porting `reg_webapp/frontend` from Svelte 5 to React 19 +
React Aria Components (maintainer decision, 2026-10-22). This is the one root-level
tracker AGENTS.md allows for a multi-PR refactor.

**Completion gate:** deleted when the last Svelte file is removed and the docs PR
merges. PR 10 deletes this file.

## Branching and freeze

- Integration branch: `react-port`. Every port PR is stacked on it and targets it; CI
  runs on PRs into it. One merge-commit PR takes `react-port` to `main` at the end.
- `main` is frozen for `reg_webapp/frontend/**` code until that merge.
- Design and spec edits to `reg_webapp/frontend/DESIGN.md` still land on `main`; `main`
  is merged into `react-port` the same day.
- Urgent fixes land on `main` and are merged forward into `react-port`.
- Renovate is paused for `reg_webapp/frontend/**` (a disable rule in `renovate.json`);
  the merge PR removes it and adds one lockstep group for the new toolchain.

## Scope

Structural v2: the app shell and top bar, the browse tree (RAC `Tree`), the column list,
in-place filter, the ⌘K palette, the project banner and drop-in primitives. The
Timeline, VariableRow and LaneGraph redesign comes after the port. The `/search` page is
deleted in bundle B.

## Baseline

Recorded at `origin/main` `10123f165` before branching.

- `dev.sh --fixture-db flows` (all scenarios): OK, 32 cases, 64 screenshots, 102 s wall.
  The screenshots and log are kept locally by the Operator.
- SPA JavaScript: 126,151 bytes, the sum of `gzip -9` over `dist/assets/*.js` after
  `bun run build`.
- `reg_core_wasm_bg.wasm`: 144,629 bytes at `gzip -9`.

Measure later PRs the same way, so the numbers compare.

## Per-PR checklist

- The `reg-webapp-frontend` CI job is green, inside its 6-minute budget.
- `reg_webapp/dev/dev.sh --fixture-db flows` run locally and compared with the baseline.
- The `check(...)` lines in `driver.mjs` are unchanged. Only navigation locators may
  change, with the reason in the commit message.
- `bun run lint:tokens` is clean.
- Tests are not ported (maintainer decision, 2026-10-22). A ported component's Svelte
  tests are deleted with it; pure-TS unit tests stay as they are. New tests are few and
  black-box only, in the `flows` driver.
- An independent review (a fresh agent that did not write the change).
- UI PRs are authored with the `frontend-design:frontend-design` and
  `reg-webapp-frontend-design` skills and judged with `reg-webapp-design-reviewer`.

## PRs

  | PR  | Scope                                                                                                                        | Status                                                      |
  | --- | ---------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
  | 0a  | Vite+ toolchain on Svelte                                                                                                    | closed #1775; toolchain taken into PR 1                     |
  | 0b  | This tracker and the baseline, on `main`                                                                                     | merged #1777                                                |
  | 1   | The toolchain and the shell (below)                                                                                          | merged #1782; JS 241 kB gzip (+115 kB), accepted until PR 9 |
  | 2   | ColumnList, the CatalogNodeView arms, primitives with their first consumer; follow-up: arrow keys between the browse columns | merged #1784; JS 250 kB gzip (+9 kB)                        |
  | 3   | BindingLeafView, PeriodPicker, PeriodWindowSlider, Lineage, DocMentions, TechnicalDetails, ValueSet\*, CodeList              | merged #1787; JS 253 kB gzip (+3 kB)                        |
  | 4   | ConceptGroupView                                                                                                             | not started                                                 |
  | 5   | RepresentationPicker (split into modules)                                                                                    | not started                                                 |
  | 6   | Classification\*, VariantBrowser, DocView, Home                                                                              | not started                                                 |
  | B   | Bundle of the old 7 and 8: ProjectEditor, SourceEditor, BindingEditor, StudyWindowSummary, ValidationPanel, ConfirmDialog; CommandPalette (RAC Autocomplete); SearchView and the `/search` route deleted | in review; JS 250 kB gzip (±0)                               |
  | 9   | Last Svelte removed: Svelte deps, svelte-check, Bits UI, the temporary island, the `PortGate` gate component                 | not started                                                 |
  | 10  | Docs and skills; deletes this file and its sentence in `AGENTS.md`                                                           | not started                                                 |
  | M   | `react-port` → `main` (full gate, flows, independent review); swaps the Renovate pause for the lockstep group                | not started                                                 |

PR 1 adds the toolchain (React 19, RAC, plugin-react, locale optimization, Oxlint, CSS
Modules, `lint_tokens` over tsx and css) and the shell: router, stores, `App`, top bar,
project banner and the browse tree.
