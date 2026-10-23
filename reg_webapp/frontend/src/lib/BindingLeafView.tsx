import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  getGraph,
  getRelatedDocuments,
  getStates,
  relatedDocumentFileHref,
  type VariableShow,
  type VariableStateModel,
} from "./api";
import styles from "./BindingLeafView.module.css";
import type { PeriodProps } from "./CatalogNodeView";
import {
  coverageFromStates,
  deliveredYears,
  formatDataType,
  formatStateWindow,
  groupLinkFromFocus,
  narrowStatesByModifier,
  pickerRepresentations,
  pickerWindowYears,
  qualifierFromFocus,
  registerPrefixOf,
  windowTitle,
} from "./catalog";
import {
  matrixAnswerSource,
  pagesLabel,
  provenanceBlocks,
} from "./curated_source";
import { DocMentionsPanel } from "./DocMentionsPanel";
import { LineageDetails } from "./LineageDetails";
import {
  clampYearPeriodWire,
  clampYearWindow,
  nextResolutionQuery,
  periodWireValid,
  VALUE_SET_VERSION_NONE,
} from "./period";
import { RepresentationPicker } from "./RepresentationPicker";
import { router } from "./router";
import { ScopedDataWarnings } from "./ScopedDataWarnings";
import { StagedAddStatus } from "./StagedAddStatus";
import { SubjectView, TechnicalDetails } from "./SubjectView";
import {
  ADD_PERIOD_REQUIRED_MESSAGE,
  applyStagedPicks,
  committedPickerRows,
  type StagedApplyOutcome,
  type StagedApplyPayload,
  stagedApplyRefusal,
} from "./staged_picker";
import { VariableTimeline } from "./Timeline";
import { ColumnChip } from "./ui/ColumnChip";
import type { Definition, MemberOf } from "./ui/Definitions";
import { KeyValue } from "./ui/KeyValue";
import { Tag } from "./ui/Tag";
import type { KeyValueRow } from "./ui/types";
import { useAsyncResource } from "./use_async_resource";
import { useProject, useStudyWindow } from "./use_project";
import { useQueryParam } from "./use_router";
import { ValueSetView } from "./ValueSetView";

interface Props extends PeriodProps {
  node: VariableShow;
  /** The variable's whole state history (the `states` read, no modifiers). */
  states: VariableStateModel[];
}

interface Correction {
  state: VariableStateModel;
  className: string;
  evidence: string;
  sourceEditions: string[];
  providerDocumented: boolean;
}

/** A state's curated corrections (`errata:` provenance blocks). The class line
 * may stand alone: the catalog publishes only source locators after it, never
 * curator evidence (#1708). A malformed attribution list yields nothing. */
function correctionsFromState(state: VariableStateModel): Correction[] {
  return provenanceBlocks(state.provenance)
    .filter((block) => block.startsWith("errata:"))
    .flatMap((block): Correction[] => {
      const separator = block.indexOf("\n");
      const head = separator < 0 ? block : block.slice(0, separator);
      const className = head.slice("errata:".length).trim();
      const body = separator < 0 ? "" : block.slice(separator + 1).trim();
      const providerDocumented = className === "scoped-attributions";
      if (!providerDocumented && className !== "overlapping-attributions") {
        return className
          ? [
              {
                state,
                className,
                evidence: body,
                sourceEditions: [],
                providerDocumented: false,
              },
            ]
          : [];
      }
      let parsed: unknown;
      try {
        parsed = JSON.parse(body);
      } catch {
        return [];
      }
      if (!Array.isArray(parsed)) return [];
      const corrections: Correction[] = [];
      for (const item of parsed) {
        if (typeof item !== "object" || item === null) return [];
        const record = item as Record<string, unknown>;
        if (
          typeof record.class !== "string" ||
          !record.class.trim() ||
          typeof record.evidence !== "string" ||
          !record.evidence.trim() ||
          !Array.isArray(record.source_editions) ||
          record.source_editions.length === 0 ||
          !record.source_editions.every(
            (edition) => typeof edition === "string" && edition.trim(),
          )
        ) {
          return [];
        }
        corrections.push({
          state,
          className: record.class.trim(),
          evidence: record.evidence.trim(),
          sourceEditions: record.source_editions as string[],
          providerDocumented,
        });
      }
      return corrections;
    });
}

function documentName(url: string): string {
  try {
    const parsed = new URL(url);
    const file = parsed.pathname.split("/").pop();
    return file ? decodeURIComponent(file) : parsed.hostname;
  } catch {
    return url;
  }
}

function interval(state: VariableStateModel): string {
  return windowTitle(state.valid_from, state.valid_to, state.period_scope);
}

function stateKey(state: VariableStateModel): string {
  return `${state.state_id}|${state.delivery_column_name ?? ""}|${state.valid_from}`;
}

/** The value-set view keeps local state (an isolated value set, a filter, open
 * code panels) that a move of its states resets: a period Apply re-reads the
 * states without remounting this leaf. */
function valueSetViewKey(
  states: VariableStateModel[],
  scope: VariableStateModel[] | null,
): string {
  const ids = (list: VariableStateModel[]) => list.map(stateKey).join(",");
  return [ids(states), scope === null ? "-" : ids(scope)].join("\u0000");
}

/** A facet's axis as a sentence-case term ("enhet" → "Enhet"). */
function facetTerm(axis: string | null | undefined): string {
  return axis ? axis.charAt(0).toUpperCase() + axis.slice(1) : "Facet";
}

/**
 * The binding leaf (#638): the addressable variable, its period card and
 * representation picker, its value sets, lineage and parsed documentation. The
 * variable (`node`) and its whole history (`states`) arrive loaded, so a cold
 * deep link to `…/kon?period=2020` renders the metadata at once while the
 * states narrow. Each other facet is read here as its own failure domain.
 *
 * Resolution state (`?period`, `?variant`, `?value_set_version`) lives in the
 * URL (reg_webapp/DESIGN.md → Catalog router structure): changing it re-reads
 * the narrowed states without remounting (Y-65).
 */
export function BindingLeafView({
  node,
  states: history,
  regMetaVersion,
  steward,
  windowMinYear,
  windowMaxYear,
  enforcePeriodBounds,
  vintageYear,
}: Props) {
  const period = useQueryParam("period");
  const variant = useQueryParam("variant");
  const valueSetVersion = useQueryParam("value_set_version");

  const boundedPeriod = enforcePeriodBounds
    ? clampYearPeriodWire(period, windowMinYear, windowMaxYear)
    : period;
  const hasModifier = variant !== null || valueSetVersion !== null;

  // With a `?period`, the resolved subset narrows the visible states; without
  // one the full history shows and nothing is read.
  const periodResource = useAsyncResource(
    boundedPeriod
      ? `${node.fqid}\u0000${boundedPeriod}\u0000${variant ?? ""}\u0000${valueSetVersion ?? ""}`
      : null,
    () =>
      getStates(node.fqid, {
        period: boundedPeriod ?? undefined,
        variant: variant ?? undefined,
        value_set_version: valueSetVersion ?? undefined,
      }),
  );
  // A modifier still needs a period-only scope for #744's outside-period
  // disclosure, or other variants' same-period rows would read as outside it.
  const scopeResource = useAsyncResource(
    boundedPeriod && hasModifier ? `${node.fqid}\u0000${boundedPeriod}` : null,
    () => getStates(node.fqid, { period: boundedPeriod ?? undefined }),
  );
  // Any failure of the primary resolve (a 422 modifier, a 5xx, a dropped
  // network) shows inline and falls back to the full history, rather than
  // wedging on "Loading states…".
  const narrowedError = boundedPeriod ? periodResource.error : null;
  const scopeError =
    boundedPeriod && !periodResource.error && hasModifier
      ? scopeResource.error
      : null;
  const narrowedStates = periodResource.data;
  const periodScopeStates = hasModifier ? scopeResource.data : null;
  const resolvedStates =
    boundedPeriod && !narrowedError ? narrowedStates : history;
  const isNarrowed = !!boundedPeriod && !narrowedError;

  // Null while a resolve is in flight; one resolved state is the detail;
  // otherwise the whole history with `scope` marking what is in period (#744).
  let states: VariableStateModel[] | null = history;
  if (isNarrowed) {
    states =
      narrowedStates === null ||
      (hasModifier && periodScopeStates === null && scopeResource.loading)
        ? null
        : narrowedStates.length === 1
          ? narrowedStates
          : history;
  }
  const stateScope = isNarrowed
    ? hasModifier
      ? (periodScopeStates ?? resolvedStates)
      : resolvedStates
    : null;
  // #905: the value-set view takes the SAME modifier narrowing the picker
  // does, on top of the derivation above — but not on a failed resolve, whose
  // full-history fallback a stale `?variant` would otherwise empty.
  const narrow = (list: VariableStateModel[] | null) =>
    list === null || narrowedError || !hasModifier
      ? list
      : [...narrowStatesByModifier(list, variant, valueSetVersion)];
  const valueSetStates = narrow(states);
  const valueSetScope = narrow(stateScope);
  const technicalState =
    valueSetStates?.length === 1 &&
    !(isNarrowed && valueSetScope !== null && valueSetScope.length === 0)
      ? valueSetStates[0]
      : null;

  const shownStates = valueSetStates ?? [];
  const corrections = shownStates.flatMap(correctionsFromState);
  const inferredGaps = shownStates.filter(
    (s) => s.provenance === "inferred:resolution-gap",
  );
  // Y-202: one marked state over a pooled multi-year edition.
  const pooledStates = shownStates.filter((s) => s.pooled);

  // A curated matrix answer cites a page of an SCB PDF: one line per distinct
  // (document, pages), the state's own column's page where it names one.
  const matrixSources = new Map<
    string,
    { documentUrl: string; sha256: string; pages: string }
  >();
  for (const state of shownStates) {
    const source = matrixAnswerSource(state.provenance);
    if (source === null) continue;
    const own = state.delivery_column_name
      ? source.pages[state.delivery_column_name]
      : undefined;
    const pages = pagesLabel(
      own === undefined ? Object.values(source.pages) : [own],
    );
    matrixSources.set(`${source.sha256} ${pages}`, { ...source, pages });
  }
  const registerPrefix = registerPrefixOf(node.fqid);
  // The register's rehosted PDFs, read only when a state cites one, to title
  // and link it by its digest; otherwise the provider URL stands.
  const cited = useAsyncResource(
    matrixSources.size > 0 ? registerPrefix : null,
    (signal) => getRelatedDocuments(registerPrefix, { signal }),
  );

  // One `/graph` read (#761/#792) feeds the Timeline's related-variable lanes
  // and the #670 header identity. Its own failure domain: an error leaves the
  // Timeline on this variable's lane and the header on `node.name`.
  const graphResource = useAsyncResource(node.fqid, (signal) =>
    getGraph(node.fqid, { signal }),
  );
  const graph = graphResource.data;
  const focus = graph?.nodes.find((n) => n.id === graph.focus_id);
  const focusNode = focus?.kind === "variable" ? focus : null;
  const qualifier = qualifierFromFocus(focusNode, node.fqid);
  const groupLink = groupLinkFromFocus(focusNode, node.group);

  function setResolution(
    next: {
      period?: string | null;
      variant?: string | null;
      value_set_version?: string | null;
    },
    replace = false,
  ): void {
    const query = nextResolutionQuery(
      {
        period: period ?? undefined,
        variant: variant ?? undefined,
        value_set_version: valueSetVersion ?? undefined,
      },
      next,
    );
    router.navigate(window.location.pathname + (query ? `?${query}` : ""), {
      replace,
    });
  }

  // ── Add to project (#678/#995) ───────────────────────────────────────────
  // The picker lists the variable's representations over the full history;
  // the period only dims rows. A `?variant`/`?value_set_version` modifier
  // scopes the rows too, so "select all" stays inside the narrowing.
  const seedReady = regMetaVersion !== "" && steward !== "";
  const coverage = useMemo(() => coverageFromStates(history), [history]);
  const coverageYears = useMemo(
    () =>
      deliveredYears(
        history.map((s) => ({ from: s.valid_from, to: s.valid_to })),
        windowMaxYear,
      ),
    [history, windowMaxYear],
  );
  const pickerStates = useMemo(
    () => narrowStatesByModifier(history, variant, valueSetVersion),
    [history, variant, valueSetVersion],
  );
  const yearIndependent =
    pickerStates.length > 0 &&
    pickerStates.every((s) => s.period_scope === "year_independent");
  const pickerBands = useMemo(
    () => [
      {
        key: node.fqid,
        name: node.name ?? node.fqid,
        registerPrefix: registerPrefixOf(node.fqid),
        rows: pickerRepresentations(pickerStates),
      },
    ],
    [node, pickerStates],
  );
  const studyWindow = useStudyWindow();
  const projectWindow = useMemo(
    () =>
      studyWindow === null || !enforcePeriodBounds
        ? studyWindow
        : clampYearWindow(studyWindow, windowMinYear, windowMaxYear),
    [studyWindow, enforcePeriodBounds, windowMinYear, windowMaxYear],
  );
  const activePeriod =
    boundedPeriod && !narrowedError && periodWireValid(boundedPeriod)
      ? boundedPeriod
      : null;
  // The window to dim against: a valid `?period`, else the study window.
  const pickerWindow = useMemo(
    () => pickerWindowYears(activePeriod, projectWindow),
    [activePeriod, projectWindow],
  );
  const draft = useProject((store) => store.draft);
  const committedRows = useMemo(
    () =>
      committedPickerRows(draft, pickerBands, {
        period: activePeriod,
        window: pickerWindow,
        studyWindow: projectWindow,
      }),
    [draft, pickerBands, activePeriod, pickerWindow, projectWindow],
  );

  // The last Apply's outcome and refusal, each tagged with what it was made
  // under: a resolution change retires both, and a study-window move retires
  // the refusal (moving it is what an outside-window refusal asks for).
  const resolutionKey = `${period ?? ""}\u0000${variant ?? ""}\u0000${valueSetVersion ?? ""}`;
  const windowKey = pickerWindow ? pickerWindow.join("..") : "";
  const [outcome, setOutcome] = useState<{
    value: StagedApplyOutcome | null;
    under: string;
  } | null>(null);
  const [refusal, setRefusal] = useState<{
    message: string;
    under: string;
  } | null>(null);
  const shownOutcome = outcome?.under === resolutionKey ? outcome.value : null;
  const refusalUnder = `${resolutionKey}\u0000${windowKey}`;
  const shownRefusal = refusal?.under === refusalUnder ? refusal.message : null;

  // A pick still waiting on the restore gate is abandoned, not committed into
  // a draft from a page the researcher has left.
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const onApply = useCallback(
    async (payload: StagedApplyPayload): Promise<boolean> => {
      const scope = { period: activePeriod, window: pickerWindow };
      const result = await applyStagedPicks(payload, {
        scope,
        studyWindow: projectWindow,
        seed: { regMetaVersion, steward },
        cancelled: () => !mounted.current,
      });
      if (!mounted.current) return false;
      const message = stagedApplyRefusal(
        result,
        scope,
        ADD_PERIOD_REQUIRED_MESSAGE,
      );
      setRefusal(message === null ? null : { message, under: refusalUnder });
      if (result.kind === "applied") {
        setOutcome({ value: result.outcome, under: resolutionKey });
      }
      return result.kind === "applied";
    },
    [
      activePeriod,
      pickerWindow,
      projectWindow,
      regMetaVersion,
      steward,
      refusalUnder,
      resolutionKey,
    ],
  );
  const onStageChange = useCallback((hasDiff: boolean) => {
    // Clearing the staging retires the refusal too: there is no pick left.
    setRefusal(null);
    if (hasDiff) setOutcome(null);
  }, []);

  const variableWarnings = useMemo(() => ({ fqid: node.fqid }), [node.fqid]);
  const registerWarnings = useMemo(
    () => ({
      fqid: registerPrefixOf(node.fqid),
      registerOnly: true,
      collapsed: true,
    }),
    [node.fqid],
  );

  const narrowedLabel = (boundedPeriod ?? "").split(",").join(" + ");
  const hasMatrixSources = matrixSources.size > 0;
  // #670: the group this member belongs to, once the graph says so. The
  // register is not repeated here: the breadcrumb already names it.
  const memberOf: MemberOf[] = groupLink
    ? [{ relation: "Member of", ...groupLink }]
    : [];
  const facets =
    qualifier?.kind === "facets"
      ? (focusNode?.facets ?? []).map((f) => ({
          label: facetTerm(f.axis),
          value: f.label,
        }))
      : [];
  const definitions: Definition[] = [
    ...(node.definition
      ? [{ term: "Definition", value: node.definition }]
      : []),
    ...(node.description
      ? [{ term: "Description", value: node.description }]
      : []),
    ...(node.operational_definition
      ? [{ term: "Operational definition", value: node.operational_definition }]
      : []),
    ...(node.measurement_unit
      ? [{ term: "Unit", value: node.measurement_unit }]
      : []),
  ];

  return (
    <SubjectView
      kind="variable"
      title={node.name ?? node.fqid}
      // A grouped member with no facets: its slug or column is the only
      // differentiator, an identifier, so the mono line under the title.
      fqid={qualifier?.kind === "slug" ? qualifier.text : undefined}
      memberOf={memberOf}
      facets={facets}
      definitions={definitions}
    >
      {node.tags && node.tags.length > 0 && (
        <ul className={styles.tags} aria-label="Thematic tags">
          {node.tags.map((tag) => (
            <li key={tag.slug} className={styles.tag}>
              <Tag>{tag.label}</Tag>
              {tag.starred && tag.note && (
                <span className={`muted ${styles.small}`}>
                  Recommended: {tag.note}
                </span>
              )}
            </li>
          ))}
        </ul>
      )}

      <div className={styles.warnings}>
        <ScopedDataWarnings {...variableWarnings} />
        <ScopedDataWarnings {...registerWarnings} />
      </div>
      {yearIndependent ? (
        <>
          <p className="muted">
            Year-independent delivery. An annual availability period does not
            apply.
          </p>
          <RepresentationPicker
            bands={pickerBands}
            window={pickerWindow}
            canAdd={seedReady}
            committedRows={committedRows}
            studyWindow={projectWindow}
            focusKey={node.fqid}
            graph={graph}
            neighbours
            vintageYear={vintageYear}
            catalogSpan={[windowMinYear, windowMaxYear]}
            onApply={onApply}
            onStageChange={onStageChange}
          />
        </>
      ) : (
        <VariableTimeline
          period={boundedPeriod}
          window={projectWindow}
          pickerWindow={pickerWindow}
          coverage={coverage}
          years={coverageYears}
          windowMinYear={windowMinYear}
          windowMaxYear={windowMaxYear}
          enforcePeriodBounds={enforcePeriodBounds}
          // The slider commits on each key step: replace, so Back leaves the
          // page rather than walking the steps back.
          onSubmit={(next) => setResolution({ period: next }, true)}
          onClear={() => setResolution({ period: null })}
          bands={pickerBands}
          canAdd={seedReady}
          committedRows={committedRows}
          studyWindow={projectWindow}
          focusKey={node.fqid}
          graph={graph}
          neighbours
          vintageYear={vintageYear}
          onApply={onApply}
          onStageChange={onStageChange}
        />
      )}
      {/* Applying a period re-resolves this view; the picker's staging
          survives it, so the refusal only asks for a period. */}
      <StagedAddStatus outcome={shownOutcome} blocked={shownRefusal} />
      {period && (variant || valueSetVersion) && (
        // Each modifier is clearable, so narrowing to one state is not a
        // one-way trap. Gated on a period: the server only applies them to a
        // period resolve.
        <div className={styles.modifiers}>
          <span className="muted">Narrowed by:</span>
          {variant && (
            <button
              type="button"
              className={styles.modifier}
              aria-label="Clear variant filter"
              onClick={() => setResolution({ variant: null })}
            >
              variant: {variant} <span aria-hidden="true">✕</span>
            </button>
          )}
          {valueSetVersion && (
            <button
              type="button"
              className={styles.modifier}
              aria-label="Clear value-set version filter"
              onClick={() => setResolution({ value_set_version: null })}
            >
              version:{" "}
              {valueSetVersion === VALUE_SET_VERSION_NONE
                ? "unlabeled"
                : valueSetVersion}{" "}
              <span aria-hidden="true">✕</span>
            </button>
          )}
        </div>
      )}
      {narrowedError && (
        <p className={`error ${styles.inlineError}`} role="alert">
          {narrowedError}
        </p>
      )}
      {scopeError && (
        <p className={`error ${styles.inlineError}`} role="alert">
          Could not load full period value-set context: {scopeError}
        </p>
      )}

      <section className={styles.states} aria-labelledby="states-heading">
        <h2 id="states-heading" className={styles.statesHeading}>
          States
          {/* A #307 list reads as segments joined with "+". */}
          {isNarrowed && narrowedLabel && (
            <span className={`muted ${styles.narrowed}`}>
              {" "}
              narrowed to {narrowedLabel}
            </span>
          )}
        </h2>
        {valueSetStates ? (
          <ValueSetView
            key={valueSetViewKey(valueSetStates, valueSetScope)}
            fqid={node.fqid}
            states={valueSetStates}
            narrowed={isNarrowed}
            scopeStates={valueSetScope}
            common={{
              name: node.name ?? null,
              description: node.description ?? null,
              definition: node.definition ?? null,
              unit: node.measurement_unit ?? null,
            }}
          />
        ) : (
          // Y-65: a query-only navigation keeps this subtree, so this line
          // makes the announcement the node view's loading branch used to.
          <output className={`muted ${styles.loading}`} aria-busy="true">
            Loading states…
          </output>
        )}
      </section>

      <LineageDetails node={node} />
      <DocMentionsPanel node={node} />

      {/* #638 PR4 / #1038: the backend and structural rows. The flags are
          always known; type and column join only for one resolved state. */}
      <TechnicalDetails>
        <dl className={styles.meta}>
          <dt className="micro-label">Sensitive</dt>
          <dd>{node.is_sensitive ? "yes" : "no"}</dd>
          <dt className="micro-label">Identifier</dt>
          <dd>{node.is_identifier ? "yes" : "no"}</dd>
          {technicalState?.data_type && (
            <>
              <dt className="micro-label">Data type</dt>
              <dd>
                {formatDataType(
                  technicalState.data_type,
                  technicalState.data_length,
                )}
              </dd>
            </>
          )}
          {technicalState?.delivery_column_name && (
            <>
              <dt className="micro-label">Delivery column</dt>
              <dd>
                <ColumnChip name={technicalState.delivery_column_name} />
              </dd>
            </>
          )}
          {/* Waits for the document lookup, so the link never swaps target
              under the cursor. */}
          {hasMatrixSources && !cited.loading && (
            <>
              <dt className="micro-label">Source</dt>
              <dd>
                {[...matrixSources.entries()].map(([key, source]) => {
                  const doc = cited.data?.find(
                    (d) => d.sha256 === source.sha256,
                  );
                  return (
                    <p key={key} className={styles.matrixSource}>
                      {doc ? (
                        <a
                          href={relatedDocumentFileHref(
                            registerPrefix,
                            doc.filename,
                          )}
                        >
                          {doc.title}
                        </a>
                      ) : (
                        <a
                          href={source.documentUrl}
                          target="_blank"
                          rel="noreferrer"
                        >
                          {documentName(source.documentUrl)}
                        </a>
                      )}
                      , {source.pages}
                    </p>
                  );
                })}
              </dd>
            </>
          )}
          {corrections.length > 0 && (
            <>
              <dt className="micro-label">Corrected deliveries</dt>
              <dd>
                <ul className={styles.facts}>
                  {corrections.map((c) => (
                    <li
                      key={`${stateKey(c.state)}:${c.className}:${c.sourceEditions.join(",")}`}
                      title={interval(c.state)}
                    >
                      <KeyValue rows={correctionRows(c)} />
                      {c.evidence && (
                        <p className={styles.evidence}>{c.evidence}</p>
                      )}
                    </li>
                  ))}
                </ul>
              </dd>
            </>
          )}
          {inferredGaps.length > 0 && (
            <IntervalFacts
              title="Inferred intervals"
              states={inferredGaps}
              attribution="Unattributed; retained by catalog resolution"
            />
          )}
          {pooledStates.length > 0 && (
            <IntervalFacts
              title="Pooled intervals"
              states={pooledStates}
              attribution="Pooled source edition; annual availability inside the interval is not inferred"
            />
          )}
        </dl>
      </TechnicalDetails>
    </SubjectView>
  );
}

function correctionRows(c: Correction): KeyValueRow[] {
  return [
    { label: "Variant", value: c.state.variant_label ?? c.state.variant },
    {
      label: c.providerDocumented ? "Catalog interval" : "Interval",
      value: formatStateWindow(c.state) ?? interval(c.state),
      mono: true,
    },
    ...(c.providerDocumented
      ? [
          {
            label: "Attribution",
            value: "Provider-documented with a scoped correction",
          },
        ]
      : []),
    ...(c.sourceEditions.length > 0
      ? [
          {
            label:
              c.sourceEditions.length === 1
                ? "Source edition"
                : "Source editions",
            value: c.sourceEditions.join(", "),
            mono: true,
          },
        ]
      : []),
    { label: "Class", value: c.className, mono: true },
  ];
}

function IntervalFacts({
  title,
  states,
  attribution,
}: {
  title: string;
  states: VariableStateModel[];
  attribution: string;
}) {
  return (
    <>
      <dt className="micro-label">{title}</dt>
      <dd>
        <ul className={styles.facts}>
          {states.map((state) => (
            <li key={stateKey(state)} title={interval(state)}>
              <KeyValue
                rows={[
                  {
                    label: "Variant",
                    value: state.variant_label ?? state.variant,
                  },
                  { label: "Interval", value: interval(state), mono: true },
                  { label: "Attribution", value: attribution },
                ]}
              />
            </li>
          ))}
        </ul>
      </dd>
    </>
  );
}
