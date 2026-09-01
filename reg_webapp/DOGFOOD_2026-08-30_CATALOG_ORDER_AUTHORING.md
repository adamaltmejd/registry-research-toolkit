# `catalog.swecov.se` researcher dogfooding report

Date: 2026-08-30\
Surface: live `https://catalog.swecov.se/` in Chrome\
Repository head during review: `2d988f676d0a`\
Live catalog footer: `reg_meta v0.39.1`, schema `6.7.0`, built 2026-08-22\
Exported project schema: `2.0.0`

This is a point-in-time product report, not an implementation tracker. It records an
observed researcher journey and translates the blockers into ticket-ready
recommendations for the builder agent.

## Executive outcome

The evaluated task could not be completed safely.

The researcher wanted to reproduce an existing SWECOV data selection for a project on
inequalities in Covid-19 testing behavior. The reference selection contains 81 physical
table rows and 750 encoded table-variable cells, with 752 likely intended after
repairing a shifted SmiNet row. The catalog produced a structurally valid project with 5
logical sources and 12 bindings, but it could not represent or export the exact physical
dataset.

The main blockers were:

1. The evaluated global catalog did not route a SWECOV order-authoring task to the
   separate SWECOV steward catalog. The committed SWECOV catalog contains Inera/1177,
   but the researcher reasonably concluded that it was missing from the surface they had
   been given.
2. Requested SmiNet and NVR fields are missing from the evaluated path.
3. The exported order has no physical edition, table, or column identity.
4. The project model widens periods at source level, so it cannot safely preserve a
   variable-by-period request matrix within one register variant.
5. Some variable pickers do not distinguish register variants and are visibly clipped,
   making a correct selection impossible without guessing.
6. A green **Valid** state establishes current structural and semantic validity, not
   that the project is complete or materializable as an extraction order.

The exported files are useful as diagnostic artifacts only. They must not be used as the
data order.

## Researcher task and success criterion

### Research question

Map inequalities in Covid-19 testing behavior by linking ordered-test activity and
timestamps to demographic, geographic, socioeconomic, employment, benefit, vaccination,
morbidity, and mortality data.

### Starting point

The existing selection manifest was:

```text
/Users/adam/Downloads/SWECOV_DATA_P1229.csv
SHA-256 110d181fdfa3aaacc47bd99dcf87481e14724e2a1776863255697f156c31c5ed
```

The file is a table/variable manifest, not microdata. It contains no individual records.

### Success criterion

The catalog should produce a durable request file from which a data builder can
deterministically recover the same physical tables, editions, columns, and requested
periods as the reference selection, or fail closed with an explicit list of unresolved
items.

Logical similarity is insufficient: an extraction request must distinguish physical
deliveries such as annual versus one-off/incremental tables and preserve cases where the
requested columns differ by year.

## Evidence artifacts

The live UI produced:

```text
/Users/adam/Downloads/project_data.json
SHA-256 b7815183f129df5cdffcc49a31e2dc693a41063177eefae2fb94fd2e7e51e4d2

/Users/adam/Downloads/order.csv
SHA-256 4c606b8489ade2e6d3afb5ca29903a576ef1ad23e276d6364e61a2cd99ac7bdc
```

Local structural validation passed:

```sh
uv run python -c \
  'import json,sys; from reg_schema import validate_structural; r=validate_structural(json.load(open(sys.argv[1]))); print(r.ok, r.issues)' \
  /Users/adam/Downloads/project_data.json
```

Result:

```text
True []
```

The order export contained exactly these columns and 12 rows:

```text
provider,register,variant,variable,representation,period,display_name
```

All 12 `representation` values were blank. The exporter uses a resolved display name as
a best-effort delivery-column label when a representation is absent, but its provisional
schema has no edition, physical table, or physical-column coordinate. This is the real
extraction blocker. It matches the unfinished physical materializer already recorded in
§12 of `REFACTOR_SPEC.md`; blank representations alone are not the defect.

The three downloaded files above are local provenance evidence and will not be available
to isolated Yard lanes. Any admitted implementation ticket needs a small, sanitized
repository fixture rather than depending on `/Users/adam/Downloads`.

## Reference selection summary

  | Source family     | Physical table rows | Selections        |
  | ----------------- | ------------------: | ----------------: |
  | RTB               |                   7 |                42 |
  | Geografidatabasen |                   2 |                 4 |
  | LISA              |                   9 |                65 |
  | AGI               |                  48 |               360 |
  | Försäkringskassan |                   6 |                60 |
  | Inera/1177        |                   1 |                 7 |
  | FoHM NVR          |                   1 |                 6 |
  | FoHM SmiNet       |     1 malformed row | 6 likely intended |
  | Socialstyrelsen   |                   5 |               199 |
  | IAF               |                   1 |                 3 |

The CSV directly encodes 750 variable cells. The SmiNet row places two apparent
variables in the `Detail` and `Table` fields, so repairing that row yields six likely
SmiNet variables and 752 likely intended selections in total.

The central testing outcome is the Inera/1177 table `Inera_CK_EP`, including:

- `P1105_LopNr_PersonNr`
- `AnswerArrivedFromLabDateTime`
- `Covid_19_antikroppar`
- `Covid_19_diagnostik`
- `HandledDateTime`
- `OfferGroupName`
- `OfferID`

The reference also varies requested columns across years. For example, LISA Individual
requests only three income fields for 2014-2018, but eighteen fields for 2019-2020. That
matrix must survive materialization; widening every binding to one shared source period
would over-order data.

## Journey exercised

1. Opened the catalog home page and reviewed its provider and project model.
2. Searched in English and Swedish for `testing`, `provtagning`, and `covid`.
3. Browsed Folkhälsomyndigheten directly and identified SmiNet and NVR.
4. Created and named a browser-local project and set a 2014-2022 study window.
5. Added the four available requested SmiNet concepts.
6. Confirmed that `Provtagningsdatum` and `AnswerArrivedFromLabDateTime` return no
   search matches.
7. Added the five available requested NVR concepts for 2021-2022.
8. Used the RTB `Personnummer` group picker to select family, population, and death
   variants.
9. Attempted to select LISA `DispInk04` and GDB `DeSO`; stopped rather than guess when
   the picker did not identify the required variant/delivery unambiguously.
10. Downloaded `project_data.json` and `order.csv`, inspected both, and ran structural
    validation.

## What worked and should be preserved

- The home page makes the three main entry points—browse, search, and project—easy to
  find.
- Provider and register descriptions helped confirm that SmiNet and NVR were relevant to
  the research question.
- Variable detail pages generally expose definitions, units, coverage, sensitivity,
  provenance, and state changes.
- Selection is staged before mutation, and the UI reports the number of columns about to
  be added.
- Register variants become explicit sources in `project_data.json`.
- The project is browser-local and can be downloaded as a durable JSON file.
- The exported JSON passed the independent structural validator.
- The application also performs catalog-backed semantic validation; this is more than a
  schema parse, even though it is not an extraction-readiness check.

These strengths do not offset the extraction blockers, but they are useful foundations
for the physical-order workflow.

## P0 — task-blocking defects

### P0.1 — The export is not a physical extraction contract

**Researcher intention:** Hand the final catalog artifact to a data builder and receive
the selected dataset without a second manual reconciliation against Excel.

**Observed behavior:** `order.csv` contains logical provider/register/variant/variable
identifiers and a source period, but no physical edition, table, or column. Every
`representation` field in this run was blank.

**Impact:** The builder cannot distinguish `ENGANG` from `KLEV4`/`lev3`, resolve annual
or monthly tables, or prove that the delivered data matches the request. The artifact
cannot satisfy the stated product goal.

**Desired behavior:** Materialize every binding against the steward's delivery inventory
and export at least:

```text
steward,provider,register,variant,requested_period,edition,table,column,variable
```

Fail closed when any row cannot be grounded to a held physical column.

**Acceptance criteria:**

- Each requested physical table/column in the reference manifest has one deterministic
  materialized row, modulo an explicit documented normalization.
- Missing or ambiguous mappings block the extraction-ready export.
- The UI distinguishes a logical project download from an extraction-ready order.
- A round-trip test proves byte-stable materialization for a fixed catalog and steward
  inventory.

**Relevant code/design:** `REFACTOR_SPEC.md` physical materializer work;
`reg_webapp/backend/src/reg_webapp/order_export.py`; `reg_webapp/DESIGN.md`
order-manifest sections.

### P0.2 — The global surface does not route a SWECOV ordering task

**Researcher intention:** Select ordered tests and their handling/lab-answer timestamps,
which are the project's outcome.

**Observed behavior:** On `catalog.swecov.se`, Inera/1177 was not listed as a provider
and exact search for `AnswerArrivedFromLabDateTime` returned “No matches.” The domain
and project-authoring flow gave no prominent route to the separate SWECOV steward
deployment at `data.swecov.se`. Repository inspection shows that the committed SWECOV
steward catalog does contain `inera/vardguiden`, including the seven requested testing
fields. This live run did not evaluate `data.swecov.se`, so its current deployed
behavior remains unverified.

**Impact:** The researcher can build a plausible global-catalog project that omits the
outcome defining the study, while the relevant source exists in another product scope.
The empty search result incorrectly presents a routing/scope problem as a metadata gap.

**Desired behavior:** Project creation should make the global metadata universe and the
SWECOV holdings scope unmistakable, and route order-authoring intent to the SWECOV
steward surface. Global metadata browsing can remain available.

**Acceptance criteria:**

- The global landing and project-creation flows explicitly identify their scope and
  provide a visible route or chooser for the SWECOV steward catalog.
- A route test proves that a researcher starting at `catalog.swecov.se` can reach the
  SWECOV Inera/1177 source without knowing the second hostname.
- The resulting artifact names the steward scope against which it was resolved.
- A separate live dogfood run verifies the deployed `data.swecov.se` journey before the
  Inera ordering path is considered complete.

### P0.3 — Requested FoHM deliveries are incomplete

**Researcher intention:** Add the requested SmiNet case/test dates and the requested NVR
vaccination fields.

**Observed behavior:**

- SmiNet lacked `Provtagningsdatum` and `Statistikdatum`.
- NVR lacked `nplid`.
- Exact search for `Provtagningsdatum` returned no matches.
- Four SmiNet and five NVR concepts could be selected, producing a superficially valid
  partial request.

**Impact:** The health-data portion is incomplete, yet nothing in validation says that
the requested source manifest was only partially covered.

**Desired behavior:** Complete the relevant FoHM catalog and steward-inventory mappings,
and block physical materialization when a requested FoHM field is unresolved.

**Acceptance criteria:**

- The six intended SmiNet and six NVR fields resolve to held physical columns.
- Missing or ambiguous FoHM mappings are first-class blocking findings in the
  materialization result.
- A focused fixture covers the requested SmiNet and NVR mappings, including the
  malformed SmiNet input row as a separately reported source-manifest problem.

### P0.4 — Source-level periods cannot preserve the requested variable matrix

**Researcher intention:** Request different variables for different years within the
same register variant—for example, three LISA income fields in 2014-2018 and eighteen in
2019-2020.

**Observed behavior:** The frontend identifies an existing source by `register_variant`
alone and extends that source's period when adding more bindings. Bindings do not retain
their own requested periods. The resulting source-period × binding cross-product would
widen later-only variables into earlier years.

**Impact:** Even after physical mappings exist, the current project cannot encode the
reference dataset exactly. It can silently over-order columns for years in which they
were not requested.

**Desired behavior:** Preserve the variable-by-period matrix either through distinct
binding sets for the same register variant or equivalent materializer semantics. Do not
infer one shared period for every binding merely because the register variant is the
same.

**Acceptance criteria:**

- A fixture can request variable A for 2014-2020 and variable B for 2019-2020 from the
  same variant without widening B to 2014-2018.
- Project download, reload, validation, and materialization preserve both ranges.
- The materialized order contains no source-period × binding cross-product beyond the
  requested cells.

**Relevant code/design:** `reg_webapp/frontend/src/lib/stores/project_store.svelte.ts`;
§12 of `REFACTOR_SPEC.md`; `reg_schema/DESIGN.md`.

## P1 — high-risk usability and correctness problems

### P1.1 — “Valid” overstates what has been established

**Researcher intention:** Use validation as the gate that says the request can be sent
to a data builder.

**Observed behavior:** The 5-source/12-binding project was **Valid** under the current
structural and catalog-backed semantic validator despite lacking the outcome, most
requested sources, physical columns, and exact periods. An empty project is deliberately
a valid editable draft under the schema, but it cannot be materialized.

**Impact:** Researchers can mistake schema validity for completeness or extractability.

**Desired behavior:** Name the current state as draft/project validity and, when the §12
materializer exists, separate at least three states:

1. structurally valid;
2. catalog-resolved and steward-available;
3. extraction-ready.

The project card and download controls should name the achieved state precisely.

**Acceptance criteria:**

- Empty projects may remain valid editable drafts but cannot be materialized or marked
  “order ready.”
- Unresolved physical mappings block extraction-ready status.
- Warnings do not collapse into the same green state as a fully materialized order.
- Tooltips or inline copy explain each validation layer.

### P1.2 — Variant rows can be indistinguishable and visually clipped

**Researcher intention:** Select LISA physical column `DispInk04` for 2014-2020.

**Observed behavior:** On `/catalog/scb/lisa/delkomponent-disponibel-inkomst-2004`, the
picker exposed two checkboxes with the same visible label, `DispInk04 2004 – 2023`, for
different register variants. The variant was not displayed beside either checkbox. At
the ordinary Chrome viewport, one row was clipped/overlapped beneath the other.

**Impact:** Choosing a delivery requires guessing. A wrong selection can remain valid
and survive export.

**Desired behavior:** Every selectable row must state its register variant, delivery
column or representation, and effective period in a non-overlapping layout. Rows that
collapse to the same display label must never be visually or accessibly identical.

**Acceptance criteria:**

- The two `DispInk04` rows have distinct visible and accessible names.
- No row overlaps or clips at supported desktop widths or responsive breakpoints.
- Keyboard focus and screen-reader output identify the same variant information.
- A browser test covers duplicate representations across variants.

### P1.3 — Period-only changes do not have a clear save path

**Researcher intention:** Correct an already-added SmiNet source from the project window
2014-2022 to the pandemic-specific period 2020-2022.

**Observed behavior:** Changing the subject period and clicking **Apply period**
narrowed the metadata/state view but did not change the source period in the project.
Because no checkbox delta existed, **Add to project** remained disabled. The project
editor itself does not edit periods.

**Impact:** Researchers cannot tell whether “Apply” edits the project or only the local
view, and may export a wider period than intended.

**Desired behavior:** Period changes for an existing source should produce an explicit
project diff and an enabled **Apply changes** action, or the button should be renamed to
make its filtering-only behavior unmistakable.

**Acceptance criteria:**

- Editing only a period can be persisted without removing and recreating a source.
- The UI previews the old and new period before applying.
- Returning to the project confirms the new period.
- A regression test covers period-only edits with unchanged bindings.

### P1.4 — The dual-slider control can silently create the wrong range

**Researcher intention:** Change an RTB death-source period from 2019-2020 to the single
year 2022.

**Observed behavior:** Moving the lower bound first caused it to clamp to the current
upper bound; moving the upper bound next produced 2020-2022 rather than 2022. The
project accepted `2020..2022` and remained valid.

**Impact:** A small interaction-order mistake can over-request years without a blocking
signal.

**Desired behavior:** Support exact typed year inputs, a single-year shortcut, or an
atomic range update. Always show a clear pending range and confirmation before it
changes the project.

**Acceptance criteria:**

- A user can set `2022` without knowing which slider handle must move first.
- Crossing or clamping a bound is announced and never silently changes the requested
  range.
- Unit and browser tests cover collapsing, widening, and moving a range in both
  directions.

### P1.5 — Study-window hints produce misleading remediation

**Researcher intention:** Use a broad project window while requesting sources only for
the periods relevant to each source.

**Observed behavior:** NVR was intentionally selected for 2021-2022, but the
warning-styled **Study window coverage** hint said it did not cover 2014-2020 and
offered **Extend in catalog**. Similar hints appeared for intentionally narrower RTB
sources. This client-side hint is deliberately not a semantic `ValidationIssue`.

**Impact:** Correct source-specific periods look erroneous, while real over-broad
periods can still validate. Warning noise trains users to ignore the validation panel.

**Desired behavior:** Use neutral wording for source windows that differ from the study
window, and offer an extension only when compatible catalog data actually exist. Keep
the hint visually and semantically distinct from validation failures.

**Acceptance criteria:**

- Explicitly narrower source periods do not appear as error-like findings.
- “Extend” is offered only when the catalog can identify a valid extension.
- The hint is visibly separated from structural and semantic validation issues.

### P1.6 — Search does not support discovery from the research concept

**Researcher intention:** Start with “inequalities in testing behavior” and learn which
sources contain tests, outcomes, and relevant covariates.

**Observed behavior:**

- `testing` prioritized unrelated classification codes.
- `provtagning` prioritized unrelated clinical procedures and education codes.
- `covid` returned useful results, but SmiNet was not prominent enough to discover the
  required path without provider knowledge.
- Inera appeared only as a generic no-match result because the researcher was on the
  global surface and was not routed to the SWECOV steward surface.

**Impact:** The catalog works best for users who already know the register and variable,
which preserves much of the Excel workflow's expertise burden.

**Desired behavior:** Add provider/source-aware ranking, synonyms, and concepts within
the active catalog. Research-topic search should favor register and variable definitions
over incidental code-label matches. Scope routing, rather than a new cross-inventory
search contract, should lead SWECOV researchers to steward-only sources.

**Acceptance criteria:**

- On the global catalog, `covid test`, `testing`, and `provtagning` surface SmiNet ahead
  of incidental classification-code matches.
- The scope chooser or routing flow exposes the SWECOV catalog where Inera is available.
- Exact variable identifiers remain searchable.
- Result groups explain why each match is relevant.

## P2 — efficiency and polish

### P2.1 — Search text persists outside the search context

After searching for `Provtagningsdatum`, that value remained visible in the global
search box while browsing providers, registers, variables, and the project. Those pages
also have separate local filters. The stale value suggests that the current page is
filtered when it is not.

Clear the query when closing search, or display it as an explicit inactive/reusable
query rather than an apparently active filter.

**Acceptance criteria:** Navigating away from search cannot leave text that appears to
filter the destination page; restoring a prior query, if supported, is explicit.

### P2.2 — Generated source names obscure the project

The three RTB sources were named `RTB`, `RTB_2`, and `RTB_3`, while the meaningful
distinction—family, population, and deaths—appeared only under each card's register
variant.

Default source names should derive from the variant, for example `RTB_FAMILJER`,
`RTB_FOLKBOKFORDA`, and `RTB_DODA`, while remaining editable and unique.

**Acceptance criteria:** Three RTB variants receive distinct, meaningful default names
and round-trip without collisions.

### P2.3 — Large register pages remain enormous after filtering

The RTB page contains 287 variable entries plus extensive version documentation.
Filtering the variable table did not remove or collapse the variant documentation, so
the page remained very large and difficult to scan.

Collapse variant history by default, virtualize or paginate large lists, and keep the
active task—finding and selecting variables—above exhaustive documentation.

**Acceptance criteria:** Filtering RTB leaves a compact, task-focused result region at
supported viewport sizes, while all documentation remains reachable on demand.

## Input-manifest problems that are not catalog defects

The builder should not treat every mismatch as a webapp regression. The reference CSV
itself has defects and unresolved semantics:

1. The SmiNet row is shifted:

   ```text
   Category=FHM_SMINET
   Detail=P1105_LopNr_PersonNr
   Table=Diagnosdatum
   V1=Grundfördiagnos
   V2=Insjukningsdatum
   V3=Provtagningsdatum
   V4=Statistikdatum
   ```

   `P1105_LopNr_PersonNr` and `Diagnosdatum` are variables, so the table identifier and
   actual detail are missing.

2. `TTTT`/`tttt` remains an unresolved year placeholder in two Patient Register table
   names.

3. The manifest gives no cohort, join, append/deduplication, inclusion/exclusion, or
   date-filter rules.

4. Identifier spelling and case differ across sources.

5. Both one-off and incremental Socialstyrelsen deliveries are requested without an
   append or deduplication rule.

6. The CSV has 16,384 fields per record—Excel's maximum width—although actual data
   reaches only `V52`. It is 1.3 MB largely because of empty trailing fields.

Any future import/reconciliation feature should report these as source-manifest problems
rather than silently infer intent. This dogfood run does not establish that a generic
manifest importer is required to fix the concrete authoring defects above.

## Candidate ticket slices requiring maintainer admission

This section is not a build queue. Each admitted Yard ticket should stay within one
lane, identify one consumer, and end in one observable outcome.

### Already planned in §12

- Materialize logical bindings against physical steward inventory and fail closed on
  unresolved coordinates. The global fallback's blank table and resolved canonical
  column are intentional until a global physical inventory exists; steward exactness is
  the separate completion boundary.
- Complete SWECOV steward-inventory coverage. Inera is already present in the committed
  steward catalog; its deployed path still needs direct live verification.

### Newly observed candidates

- Route global-catalog researchers to the SWECOV steward scope when they begin an order.
- Preserve distinct binding periods within one register variant without cross-product
  widening.
- Label draft validity separately from order materialization/readiness.
- Make duplicate picker rows visibly and accessibly identify variant, column, and
  period.
- Make period-only source edits persistable.
- Replace order-dependent dual-slider updates with an atomic exact-range interaction.
- Reword study-window coverage hints and gate **Extend** on actual availability.
- Rank research concepts above incidental code labels within the active catalog.
- Remove stale search-state ambiguity, improve generated source names, and compact large
  register pages. These are separate polish tickets if admitted.

The P0 completion fixture should be sanitized and minimal, not the 1.3 MB local CSV. It
needs two focused cases:

1. one LISA variant with variable A requested for 2014-2020 and variable B requested
   only for 2019-2020, proving that no widened cross-product is emitted; and
2. one physical-inventory mapping with edition, table, and column coordinates plus one
   unresolved row, proving deterministic materialization or a complete fail-closed
   finding.

A green project status alone is not sufficient evidence of either outcome.
