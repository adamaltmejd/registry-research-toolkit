# Field earnings descriptives: independent scaffold trial

Create a new, small descriptive research repository using the register-research-scaffold skill. This is a real workflow pilot grounded in education-returns; it is not an RDD project and cannot establish causal returns. Use Python with DuckDB for the data query; Polars only where useful, not a required extra conversion. Keep the stack replaceable. Start with attained education. Build one working local batch roundtrip, with documented restricted-server integration. Do not connect to a server or read real data.

## Scientific proposal for this prototype
These are PROPOSED defaults for mock execution, awaiting researcher confirmation before real execution:
- Earnings year 2019, ages 30 through 45, persons with a recorded completed higher-education degree (SUN2000Niva >= 500).
- Degree must have a known degree_year <= outcome year - 1. Select highest recorded qualifying education level, then latest qualifying year; if this still yields different fields, mark field ambiguous and exclude from field comparisons with explicit diagnostics. Record missing-degree-year exclusions separately. This is a descriptive attained-degree population, not all entrants.
- Compare annual recorded total earned income (the source's earnings variable) by completed field and background group. Means and counts suffice for the first question. Distinguish recorded zero from absence of an earnings record; exclude unmatched records from means but report safe coverage diagnostics.
- Express earnings in thousands of 2020 SEK using CPI. No regressions or causal language. Show composition/coverage before interpreting gaps.
- Background: foreign-born; Swedish-born with two foreign-born parents; Swedish-born without two foreign-born parents; unknown. Avoid labelling the last observed group as two Swedish-born parents.
- All numbers from local mock execution are labelled invented/mock and cannot close empirical question tickets.
- Disclosure settings are PROVISIONAL. Real execution can be prepared but release must fail closed until operator-supplied policy and review requirements are met. Counts/means alone do not establish clearance.

Create a small first batch with two research questions: sample/coverage and earnings differences by field/background. Keep definitions/configuration pending researcher confirmation visible in the workbench. Use an invented mock sample adequate to exercise joins, missingness, ties and assertions. It must never be derived from source data. Keep mock sample ignored and separate from committed hand-authored unit fixtures. A small portable invented sample supplied as part of this trial is acceptable; do not build a mock-data-generator CLI/subsystem.

## Source evidence (code inspected only; schemas/freshness/coverage unverified)
Reference repository: education-returns@59e3d55.
keys submodule at 9fbad4571c9f5949f6fb9cb164871c5aefd8b6a2.
Upstream processing repository: education-choice-data-processing@bffee44.
Helpers: education_choice_functions@9bd199c.

Do not modify these reference repositories. Inspect source and nonpersonal lookup keys only. Do not inspect datasets, target stores, cached outputs or credentials.

education-returns/.Rprofile configures DATA_PATH = <restricted data directory>.
education-returns/_targets.R reads:
- processed/iot.parquet: id, year, earnings. Intended grain person-year.
- processed/birth_stats.fst: id, birthyear, female, foreign_born, foreign_parents. Intended grain person.
- processed/degrees.fst: id, degree_year, SUN2000Niva, SUN2000Inr, source. Multiple degree rows/person.
A small R source adapter on restricted compute can project the two FST files to Parquet (fst + arrow, existing R environment) before the Python batch. No local FST conversion from real inputs.
Nonpersonal lookups may be copied with provenance: keys/inflation.csv (year,kpi_1980); keys/SUN2000Inr_to_field.csv (SUN2000Inr,field). Treat classification as string and use exact full code joins. education-returns/src/data_processing.R:1 stops on unmapped nonmissing field code; missing code maps Other. Use/assert this contract rather than guessing truncations.
education-returns/src/data_processing.R:73 uses CPI_2020/CPI_year/1000. Do not copy its multi-year leads into this annual descriptive trial.

Upstream src/background_characteristics.R:165 computes foreign_born = UtlSvBakg == 11 and foreign_parents = UtlSvBakg <= 12. Thus foreign_parents includes foreign-born persons; it is not independently a both-parents flag for them. Missing propagates NA. Supplemented UHR persons may lack background. Group the flags accordingly and assert contradictory known combinations are invalid.

Upstream src/background_characteristics.R:251 labels earnings Sammanräknad förvärvsinkomst. >=1998 uses CSFVI; historical variables vary. Missing source components are zero-filled upstream; residual missing earnings also set to zero. Keep recorded zero, never convert an unmatched joined person-year to zero. Detailed economic interpretation of CSFVI remains a documentation prerequisite; do not call it pure wages.

Upstream src/education_attainment.R:324 maps HREG kar and UREG ExamAr to degree_year; UREG SUN2020 is crosswalked to SUN2000. :375 deduplicates id x level x field preferring HREG source then earliest degree year within that source. This is not earliest date across all sources. Missing years can remain. Current source _targets.R references HSK_EXAMEN 2025 and UREG2022 2024. Real coverage and versions require server confirmation.

## Deliverables and constraints
- Use the skill in this clean session. Create actual code, concise docs, tickets, explicit source/config contracts, offline HTML view and minimal paper/slides build targets. No custom web app service.
- Keep a narrow command interface. Two questions do not warrant a general scheduler.
- Package source from an explicit local Git commit into a single Python execution file; no native dependencies embedded. Include safe lookups and required R bridge as a resource if feasible. Document runtime/dependencies and how an operator runs the bridge and batch on the actual Linux server. MONA remains a portability target, not a verified deployment.
- Use local Git only, no remote. User authorizes local commits for this trial. Unit tests local with fixtures only; small high-value suite. Same assertions on mocks/restricted inputs. The exact packaged mock run can be the full pipeline gate.
- Validate/import a result ZIP using declared files/schema/hashes, safe paths and explicit mock/real provenance. No unsafe extraction or arbitrary output passthrough.
- Demonstrate archive exclusion and explicit committed rerun using a workflow-only demonstration ticket, not closing an empirical question from mocks.
- Record skill version, execution revision, commands and outcomes. Track evidence metadata, not individual records. Research answers reference committed analysis/processing code and the run.
- No real data run or claim of disclosure clearance. List exact remaining server/definition/policy prerequisites.

