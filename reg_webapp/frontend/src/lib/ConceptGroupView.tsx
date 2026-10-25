import { useCallback, useEffect, useRef, useState } from "react";
import {
  type ConceptGroupMember,
  type ConceptGroupShow,
  conceptGroupRef,
  type GroupFacetModel,
  getGraph,
  getShow,
  getStates,
  type VariableGraphNode,
} from "./api";
import type { PeriodProps } from "./CatalogNodeView";
import styles from "./ConceptGroupView.module.css";
import {
  catalogHref,
  deliveredYears,
  facetLabelJoin,
  groupHref,
  informativeAxes,
  leafSlug,
  nodeLabel,
  type PickerRepresentation,
  pickerRepresentations,
  pickerWindowYears,
  registerPrefixOf,
  stateKey,
} from "./catalog";
import { registerRead } from "./catalog_names";
import {
  clampYearPeriodWire,
  clampYearWindow,
  coverageBandEdges,
  periodWindowRelation,
  periodWireValid,
  yearWindowFromWire,
} from "./period";
import { bandFacets, type PickerBand } from "./picker_lanes";
import { studyWindowInterval } from "./reg_core";
import { router } from "./router";
import { ScopedDataWarnings } from "./ScopedDataWarnings";
import { StagedAddStatus } from "./StagedAddStatus";
import { SubjectSection, TechnicalDetails } from "./SubjectView";
import {
  ADD_PERIOD_REQUIRED_MESSAGE,
  applyStagedPicks,
  committedPickerRows,
  type StagedApplyOutcome,
  type StagedApplyPayload,
  stagedApplyRefusal,
} from "./staged_picker";
import { CoverageTimeline } from "./Timeline";
import { Button } from "./ui/Button";
import { Definitions } from "./ui/Definitions";
import { KeyValue } from "./ui/KeyValue";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import type { KeyValueRow } from "./ui/types";
import { useAsyncResource } from "./use_async_resource";
import { useProject, useStudyWindow } from "./use_project";
import { useQueryParam } from "./use_router";
import { useCatalogRead } from "./use_store_read";
import { ValueSetView } from "./ValueSetView";
import { VariableReference, VariableTags } from "./VariableReference";

interface Props extends PeriodProps {
  provider: string;
  register: string;
  groupKey: string;
}

/** A member delivered under a column the catalog knows but never delivered:
 * a row with no years, so the graph lists it under the lane's name. */
function neverDelivered(
  column: string,
  seed: PickerRepresentation | undefined,
): PickerRepresentation {
  const variant = seed?.variant ?? "_default";
  return {
    key: `${variant}::${column}`,
    variant,
    variantLabel: seed?.variantLabel ?? "default",
    column,
    representation: column,
    from: null,
    to: null,
    windows: [],
    period: "not delivered",
    wirePeriod: null,
    valueSetLabel: seed?.valueSetLabel ?? "",
    codingsVary: false,
    selectable: false,
    renamedColumns: [],
  };
}

function zeroCoverage(member: ConceptGroupMember): boolean {
  const c = member.coverage;
  return (
    member.delivery_column != null &&
    c != null &&
    c.state_count === 0 &&
    c.coverage_from == null &&
    c.coverage_to == null &&
    !c.open_ended
  );
}

/** One band per distinct member variable, in member order. A variable can be
 * several members, one per delivery column; then only those columns are
 * offered, so a column outside the concept is never selectable. A member
 * standing for the whole variable offers every column. */
function groupBands(
  group: ConceptGroupShow,
  nodes: ReadonlyMap<string, VariableGraphNode>,
  memberHref: (fqid: string) => string,
): PickerBand[] {
  const byFqid = new Map<string, ConceptGroupMember[]>();
  for (const member of group.members) {
    byFqid.set(member.fqid, [...(byFqid.get(member.fqid) ?? []), member]);
  }
  // A lane per member: the facets tell members apart, so two columns of one
  // variable are two lanes. A whole-variable member owns every column of its
  // variable, which then stays one lane.
  const units = [...byFqid].flatMap(([fqid, members]) =>
    members.some((m) => m.delivery_column == null)
      ? [{ fqid, members, laneKey: fqid }]
      : members.map((m) => ({
          fqid,
          members: [m],
          laneKey: `${fqid}#${m.delivery_column}`,
        })),
  );
  return units.map(({ fqid, members, laneKey }) => {
    const whole = members.some((m) => m.delivery_column == null);
    const columns = new Set(
      members.flatMap((m) => (m.delivery_column ? [m.delivery_column] : [])),
    );
    const node = nodes.get(fqid);
    const states = (node?.states ?? []).filter(
      (s) =>
        whole ||
        (s.delivery_column_name != null && columns.has(s.delivery_column_name)),
    );
    const rows = pickerRepresentations(states);
    for (const member of members) {
      const column = member.delivery_column;
      if (
        column &&
        zeroCoverage(member) &&
        !rows.some((row) => row.column === column)
      ) {
        rows.push(neverDelivered(column, rows[0]));
      }
    }
    // The clicked column is the researcher's choice, pinned even where a
    // period sees a sibling member's column under the same variable.
    const pinned = !whole && (byFqid.get(fqid)?.length ?? 0) > 1;
    const facetsByColumn: Record<string, GroupFacetModel[]> = {};
    for (const member of members) {
      if (member.delivery_column && member.facets.length > 0) {
        facetsByColumn[member.delivery_column] ??= member.facets;
      }
    }
    const lead = members[0];
    const wholeFacets = members.find(
      (m) => m.delivery_column == null && m.facets.length > 0,
    )?.facets;
    return {
      key: fqid,
      laneKey,
      name: lead.name ?? leafSlug(fqid),
      registerPrefix: registerPrefixOf(fqid),
      facetLabel: lead.facets.length > 0 ? facetLabelJoin(lead.facets) : null,
      facetsByColumn,
      facets: wholeFacets,
      operationalDefinition: node?.operational_definition ?? null,
      href: memberHref(fqid),
      rows: pinned
        ? rows.map((row) =>
            row.representation != null
              ? { ...row, pinRepresentation: true }
              : row,
          )
        : rows,
    };
  });
}

/** The one value every member agrees on, or null when they differ or none
 * carries one: a group's definition is only honest when it is shared (#900). */
function sharedText(
  group: ConceptGroupShow,
  nodes: ReadonlyMap<string, VariableGraphNode>,
  field: "definition" | "description",
): string | null {
  const seen = new Set<string>();
  for (const member of group.members) {
    const value = nodes.get(member.fqid)?.[field]?.trim();
    if (value) seen.add(value);
  }
  return seen.size === 1 ? [...seen][0] : null;
}

/**
 * A concept group's page (#617): its head (DESIGN.md → Definitions), the
 * period lens, and its members on a LaneGraph, one lane per variable, ordered
 * by facet, where the researcher picks columns for the project. The group and
 * its relationship graph are separate reads: a failed graph leaves the head
 * and an empty picker rather than an error page.
 */
export function ConceptGroupView({
  provider,
  register,
  groupKey,
  regMetaVersion,
  steward,
  windowMinYear,
  windowMaxYear,
  enforcePeriodBounds,
  vintageYear,
}: Props) {
  const ref = conceptGroupRef(provider, register, groupKey);
  const registerFqid = `${provider}/${register}`;
  const resource = useAsyncResource(ref, async (signal) => {
    const shown = await getShow(ref, { signal });
    if (shown.kind !== "concept_group") {
      throw new Error(`${ref} is not a concept group`);
    }
    return shown;
  });
  const graphResource = useAsyncResource(ref, (signal) =>
    getGraph(ref, { signal }),
  );
  const registerNode = useCatalogRead(registerRead, registerFqid).value;

  const period = useQueryParam("period");
  const memberHint = useQueryParam("member");
  const boundedPeriod = enforcePeriodBounds
    ? clampYearPeriodWire(period, windowMinYear, windowMaxYear)
    : period;
  const activePeriod =
    boundedPeriod && periodWireValid(boundedPeriod) ? boundedPeriod : null;
  const studyWindow = useStudyWindow();
  const projectWindow =
    studyWindow === null || !enforcePeriodBounds
      ? studyWindow
      : clampYearWindow(studyWindow, windowMinYear, windowMaxYear);
  const pickerWindow = pickerWindowYears(activePeriod, projectWindow);
  const draft = useProject((store) => store.draft);

  // The last Apply's outcome and refusal, each tagged with what it was made
  // under: a period or member change retires both, a window move the refusal.
  const resolutionKey = `${period ?? ""}\u0000${memberHint ?? ""}`;
  const refusalKey = `${resolutionKey}\u0000${pickerWindow?.join("..") ?? ""}`;
  const [outcome, setOutcome] = useState<{
    value: StagedApplyOutcome | null;
    under: string;
  } | null>(null);
  const [refusal, setRefusal] = useState<{
    message: string;
    under: string;
  } | null>(null);

  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  // A member's URL lands here focused on it (#1909): bring its lane into view
  // once the lanes are drawn, as its own page opened on it. `nearest`, so a
  // lane already on screen (a click on its name) does not move.
  const pageRef = useRef<HTMLElement>(null);
  const lanesDrawn = resource.data != null && graphResource.data != null;
  useEffect(() => {
    if (!memberHint || !lanesDrawn) return;
    const lane = pageRef.current?.querySelector<HTMLElement>(
      '[aria-current="page"]',
    );
    const reduce = window.matchMedia(
      "(prefers-reduced-motion: reduce)",
    ).matches;
    lane?.scrollIntoView({
      behavior: reduce ? "auto" : "smooth",
      block: "nearest",
    });
  }, [memberHint, lanesDrawn]);

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
      setRefusal(message === null ? null : { message, under: refusalKey });
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
      refusalKey,
      resolutionKey,
    ],
  );

  if (resource.loading) {
    return (
      <section aria-busy="true" aria-live="polite">
        <p className="visually-hidden">Loading…</p>
        <Skeleton width="min(24rem, 70%)" count={2} />
        <Skeleton variant="block" />
      </section>
    );
  }
  if (resource.error || !resource.data) {
    return (
      <StatusRow tone="error" live="alert">
        {resource.status === 404 ? (
          <>
            No variable group <code>{groupKey}</code> in{" "}
            <code>{registerFqid}</code>. Choose a variable in the list.
          </>
        ) : (
          <>
            This group did not load ({resource.error}). Reload the page to try
            again.
          </>
        )}
      </StatusRow>
    );
  }

  const group = resource.data;
  const nodes = new Map<string, VariableGraphNode>();
  for (const node of graphResource.data?.nodes ?? []) {
    if (node.kind !== "variable") continue;
    nodes.set(node.fqid, node);
    for (const alias of node.same_as) nodes.set(alias.fqid, node);
  }
  // A member is this page focused on it (#1909): its link keeps the period
  // the group was narrowed to.
  const memberHref = (fqid: string) => {
    const query = new URLSearchParams();
    if (period) query.set("period", period);
    query.set("member", fqid);
    return `${groupHref(registerFqid, groupKey)}?${query}`;
  };
  const lanes = groupBands(group, nodes, memberHref);
  // The axes that tell lanes apart in bulk: the header's facet values, the
  // filter row and the lane titles all read this one list (#1904).
  const shownAxes = informativeAxes(
    group.axes,
    lanes.map((lane) => ({ facets: bandFacets(lane) })),
  );
  // With every axis one-to-one the facets only number the lanes: each is
  // titled by its variable's name.
  const bands =
    shownAxes.length > 0
      ? lanes
      : lanes.map((lane) => ({ ...lane, facetLabel: null }));
  const focusBand = bands.find((band) => band.key === memberHint) ?? null;
  const focusKey = focusBand?.key ?? null;
  const committedRows = committedPickerRows(
    draft,
    bands,
    registerNode?.variants ?? [],
  );
  // Where the group as a whole has data: the Timeline's coverage.
  const coverage = unionCoverage(bands);
  const coverageYears = deliveredYears(
    bands.flatMap((band) =>
      band.rows.flatMap((row) =>
        row.selectable === false ? [] : (row.windows ?? []),
      ),
    ),
    windowMaxYear,
  );
  // The band is the active window: a `?period` narrowing (a member link
  // carries one), else the project window.
  const periodYears = yearWindowFromWire(activePeriod);
  const band = periodYears ?? projectWindow;
  // Coverage as drawn: an unknown start at the catalog's first year, an open
  // end at its last (DESIGN.md → Time).
  const covered = coverageBandEdges(
    coverage,
    windowMinYear,
    windowMaxYear,
    windowMaxYear,
  );
  // A `?period` that is not the project window: with a window it differs
  // from it (a warning), without one it is only a narrowing (information).
  // Judged inside the group's coverage, as the variable page judges its
  // period: a member link carrying window ∩ availability is no deviation.
  const coveredDays = covered && studyWindowInterval(covered);
  const periodNarrows =
    activePeriod !== null &&
    !(
      periodYears !== null &&
      projectWindow !== null &&
      periodWindowRelation(
        periodYears,
        projectWindow,
        coveredDays ? [coveredDays] : null,
      ) === "same"
    );
  const definition = sharedText(group, nodes, "definition");
  const description = sharedText(group, nodes, "description");
  const facetValues = shownAxes.map((axis) => ({
    axis,
    values: [
      ...new Set(
        group.members.flatMap((m) =>
          m.facets.filter((f) => f.axis === axis.name).map((f) => f.label),
        ),
      ),
    ],
  }));

  function writePeriod(next: string | null): void {
    const query = new URLSearchParams();
    if (next) query.set("period", next);
    if (memberHint) query.set("member", memberHint);
    const search = query.toString();
    router.navigate(window.location.pathname + (search ? `?${search}` : ""));
  }

  return (
    <article ref={pageRef} className={styles.page}>
      <Definitions
        title={group.label}
        memberOf={[
          {
            relation: "Variable group in",
            label: registerNode ? nodeLabel(registerNode) : registerFqid,
            href: catalogHref(registerFqid),
          },
        ]}
        facets={facetValues.map(({ axis, values }) => ({
          label: axis.label,
          value: values.join(", "),
        }))}
        definitions={[
          ...(definition ? [{ term: "Definition", value: definition }] : []),
          ...(description ? [{ term: "Description", value: description }] : []),
        ]}
      />
      <VariableTags tags={group.tags} />
            </li>
          ))}
        </ul>
      )}
      <TechnicalDetails>
        <KeyValue
          rows={[
            { label: "Group", value: group.key, mono: true },
            { label: "Source", value: group.source },
          ]}
        />
      </TechnicalDetails>
      {/* A member's URL whose variable has no lane here (a stale link, or a
          member out of this catalog's scope): its own page still opens. */}
      {memberHint && !focusKey && (
        <StatusRow tone="info">
          <code>{memberHint}</code> is not a member of this group here.{" "}
          <a href={catalogHref(memberHint)}>Open its variable page</a>.
        </StatusRow>
      )}
      {/* A lane link moves this page to another member without a new page
          load: say which, as a page change would. */}
      <output className="visually-hidden" aria-live="polite">
        {focusBand ? `Showing ${focusBand.facetLabel || focusBand.name}.` : ""}
      </output>

      {graphResource.loading ? (
        <Skeleton variant="block" />
      ) : graphResource.error ? (
        <StatusRow tone="error" live="alert">
          The members' columns did not load ({graphResource.error}). Reload the
          page to try again.
        </StatusRow>
      ) : (
        // The variable page's one drawing (#1909): coverage heads the members'
        // lanes on one axis, the filters between them as on a variable.
        <CoverageTimeline
          band={band}
          bandKind={periodYears === null ? "study" : "add"}
          pickerWindow={pickerWindow}
          coverage={coverage}
          years={coverageYears}
          windowMinYear={windowMinYear}
          windowMaxYear={windowMaxYear}
          bands={bands}
          // Every axis orders the lanes; only an informative one filters.
          axes={group.axes}
          filterAxes={shownAxes}
          // A curated group's axes are its browse facets; without them the row
          // dimensions are the only filters the page has.
          includeRowDimensionFilters={
            !(group.source === "curated" && group.axes.length > 0)
          }
          canAdd={regMetaVersion !== "" && steward !== ""}
          committedRows={committedRows}
          studyWindow={projectWindow}
          focusKey={focusKey}
          graph={graphResource.data}
          // The focused member's predecessor and successor, as on its own
          // variable page.
          neighbours={focusKey !== null}
          vintageYear={vintageYear}
          onApply={onApply}
          onStageChange={(hasDiff) => {
            setRefusal(null);
            if (hasDiff) setOutcome(null);
          }}
          foot={
            // The group page draws the period; it does not set one. A
            // `?period` it was opened with is the exception, cleared back to
            // the window.
            periodNarrows && (
              <StatusRow
                tone={projectWindow ? "warn" : "info"}
                action={
                  <Button variant="ghost" onPress={() => writePeriod(null)}>
                    {projectWindow ? "Reset" : "Clear"}
                  </Button>
                }
              >
                {projectWindow
                  ? `Differs from the project window (${projectWindow.from}–${projectWindow.to}).`
                  : `Narrowed to ${activePeriod}.`}
              </StatusRow>
            )
          }
        />
      )}
      <StagedAddStatus
        outcome={outcome?.under === resolutionKey ? outcome.value : null}
        blocked={refusal?.under === refusalKey ? refusal.message : null}
      />
      {focusKey && (
        <MemberDetails
          key={focusKey}
          fqid={focusKey}
          period={activePeriod}
          shared={{ definition, description }}
        />
      )}
    </article>
  );
}

/** The focused member's own page (#1909): everything its variable page showed
 * below the picker, now on the group page that is its page. Facts the group's
 * head already shows as shared are not repeated. With a `?period` its states
 * narrow as on the variable page, so one resolved state names its type and
 * column. */
function MemberDetails({
  fqid,
  period,
  shared,
}: {
  fqid: string;
  period: string | null;
  shared: { definition: string | null; description: string | null };
}) {
  const resource = useAsyncResource(fqid, async (signal) => {
    const node = await getShow(fqid, { signal });
    if (node.kind !== "variable") {
      throw new Error(`${fqid} is not a variable`);
    }
    return { node, history: await getStates(node.fqid) };
  });
  const periodResource = useAsyncResource(
    period ? `${fqid}\u0000${period}` : null,
    () => getStates(fqid, { period: period ?? undefined }),
  );
  if (resource.loading) {
    return (
      <section aria-busy="true" aria-live="polite">
        <p className="visually-hidden">Loading…</p>
        <Skeleton width="min(16rem, 50%)" />
        <Skeleton variant="block" />
      </section>
    );
  }
  if (resource.error || !resource.data) {
    return (
      <StatusRow tone="error" live="alert">
        This member's details did not load ({resource.error}). Reload the page
        to try again.
      </StatusRow>
    );
  }
  const { node, history } = resource.data;
  // As the variable page: one resolved state is the detail; otherwise the
  // whole history, `scope` marking what is in period (#744). A failed or
  // pending resolve shows the whole history.
  const narrowed = period !== null ? periodResource.data : null;
  const states = narrowed?.length === 1 ? narrowed : history;
  const technicalState =
    states.length === 1 && !(narrowed !== null && narrowed.length === 0)
      ? states[0]
      : null;
  const own = (value: string | null | undefined, groupValue: string | null) =>
    value && value.trim() !== groupValue ? value : null;
  const facts: KeyValueRow[] = (
    [
      ["Definition", own(node.definition, shared.definition)],
      ["Description", own(node.description, shared.description)],
      ["Operational definition", node.operational_definition],
      ["Unit", node.measurement_unit],
    ] as const
  ).flatMap(([label, value]) => (value ? [{ label, value }] : []));
  return (
    <SubjectSection title={nodeLabel(node)}>
      <div className={styles.member}>
        <VariableTags tags={node.tags} />
        {facts.length > 0 && <KeyValue rows={facts} />}
        <ScopedDataWarnings fqid={node.fqid} />
        <ScopedDataWarnings
          fqid={registerPrefixOf(node.fqid)}
          registerOnly
          collapsed
        />
        <ValueSetView
          key={narrowed === null ? "-" : narrowed.map(stateKey).join(",")}
          fqid={node.fqid}
          states={states}
          narrowed={narrowed !== null}
          scopeStates={narrowed}
          common={{
            name: node.name ?? null,
            description: node.description ?? null,
            definition: node.definition ?? null,
            unit: node.measurement_unit ?? null,
          }}
        />
        <VariableReference
          node={node}
          states={states}
          technicalState={technicalState}
        />
      </div>
    </SubjectSection>
  );
}

/** The union span of every selectable row, in years, for the period card's
 * coverage track; null when no row is dated. */
function unionCoverage(
  bands: readonly PickerBand[],
): { from: number | null; to: number | null } | null {
  let from: number | null = null;
  let to: number | null = null;
  let open = false;
  // An unknown start is earlier than any dated one: the union's is unknown.
  let unknownStart = false;
  for (const band of bands) {
    for (const row of band.rows) {
      if (row.selectable === false || row.from === null || row.to === null) {
        continue;
      }
      if (row.from === "0001-01-01") unknownStart = true;
      else {
        const lo = Number(row.from.slice(0, 4));
        from = from === null ? lo : Math.min(from, lo);
      }
      if (row.to === "9999-12-31") open = true;
      else {
        const hi = Number(row.to.slice(0, 4));
        to = to === null ? hi : Math.max(to, hi);
      }
    }
  }
  if (from === null && to === null) return null;
  return { from: unknownStart ? null : from, to: open ? null : to };
}
