import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  getGraph,
  getStates,
  type VariableShow,
  type VariableStateModel,
} from "./api";
import styles from "./BindingLeafView.module.css";
import type { PeriodProps } from "./CatalogNodeView";
import {
  coverageFromStates,
  deliveredYears,
  narrowStatesByModifier,
  pickerRepresentations,
  pickerWindowYears,
  registerPrefixOf,
  stateKey,
} from "./catalog";
import { registerRead } from "./catalog_names";
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
import { SubjectView } from "./SubjectView";
import {
  ADD_PERIOD_REQUIRED_MESSAGE,
  applyStagedPicks,
  committedPickerRows,
  type StagedApplyOutcome,
  type StagedApplyPayload,
  stagedApplyRefusal,
} from "./staged_picker";
import { VariableTimeline } from "./Timeline";
import type { Definition } from "./ui/Definitions";
import { useAsyncResource } from "./use_async_resource";
import { useProject, useStudyWindow } from "./use_project";
import { useQueryParam } from "./use_router";
import { useCatalogRead } from "./use_store_read";
import { ValueSetView } from "./ValueSetView";
import { VariableReference, VariableTags } from "./VariableReference";

interface Props extends PeriodProps {
  node: VariableShow;
  /** The variable's whole state history (the `states` read, no modifiers). */
  states: VariableStateModel[];
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
  const registerPrefix = registerPrefixOf(node.fqid);

  // One `/graph` read (#761/#792) feeds the Timeline's related-variable lanes.
  // Its own failure domain: an error leaves the Timeline on this variable's
  // lane. A grouped member never renders here: its page is its group's
  // (#1909).
  const graphResource = useAsyncResource(node.fqid, (signal) =>
    getGraph(node.fqid, { signal }),
  );
  const graph = graphResource.data;

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
  // The register's variants name the family members that do not deliver this
  // column (#1840); until the read lands, the rows' own members stand in.
  const registerVariants = useCatalogRead(registerRead, registerPrefix).value
    ?.variants;
  const committedRows = useMemo(
    () => committedPickerRows(draft, pickerBands, registerVariants ?? []),
    [draft, pickerBands, registerVariants],
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
      definitions={definitions}
    >
      <VariableTags tags={node.tags} />

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

      <VariableReference
        node={node}
        states={shownStates}
        technicalState={technicalState}
      />
    </SubjectView>
  );
}
