import { useCallback, useEffect, useRef, useState } from "react";
import {
  type ConceptGroupMember,
  type ConceptGroupShow,
  conceptGroupRef,
  type GroupFacetModel,
  getGraph,
  getShow,
  type VariableGraphNode,
} from "./api";
import type { PeriodProps } from "./CatalogNodeView";
import styles from "./ConceptGroupView.module.css";
import {
  catalogHref,
  facetLabelJoin,
  leafSlug,
  nodeLabel,
  type PickerRepresentation,
  pickerRepresentations,
  pickerWindowYears,
  registerPrefixOf,
} from "./catalog";
import { registerRead } from "./catalog_names.svelte";
import { PeriodPicker } from "./PeriodPicker";
import {
  clampYearPeriodWire,
  clampYearWindow,
  periodWireValid,
} from "./period";
import type { PickerBand } from "./picker_lanes";
import { RepresentationPicker } from "./RepresentationPicker";
import { router } from "./router";
import { StagedAddStatus } from "./StagedAddStatus";
import { TechnicalDetails } from "./SubjectView";
import {
  ADD_PERIOD_REQUIRED_MESSAGE,
  applyStagedPicks,
  committedPickerRows,
  type StagedApplyOutcome,
  type StagedApplyPayload,
  stagedApplyRefusal,
} from "./staged_picker";
import { useRune } from "./svelte_bridge.svelte";
import { Definitions } from "./ui/Definitions";
import { KeyValue } from "./ui/KeyValue";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { Tag } from "./ui/Tag";
import { useAsyncResource } from "./use_async_resource";
import { useProject, useStudyWindow } from "./use_project";
import { useQueryParam } from "./use_router";

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
  const registerNode = useRune(registerRead, registerFqid).value;

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
  // Opening a member keeps the period the group was narrowed to.
  const memberHref = (fqid: string) =>
    period
      ? `${catalogHref(fqid)}?period=${encodeURIComponent(period)}`
      : catalogHref(fqid);
  const bands = groupBands(group, nodes, memberHref);
  const focusKey =
    bands.find((band) => leafSlug(band.key) === memberHint)?.key ?? null;
  const committedRows = committedPickerRows(draft, bands, {
    period: activePeriod,
    window: pickerWindow,
  });
  // Where the group as a whole has data: the period card's coverage track.
  const coverage = unionCoverage(bands);
  const definition = sharedText(group, nodes, "definition");
  const description = sharedText(group, nodes, "description");
  const facetValues = group.axes.map((axis) => ({
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
    <article className={styles.page}>
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
      {group.tags.length > 0 && (
        <ul className={styles.tags} aria-label="Thematic tags">
          {group.tags.map((tag) => (
            <li key={tag.slug} className={styles.tag}>
              <Tag>{tag.label}</Tag>
              {tag.starred && tag.note && (
                <span className={styles.note}>Recommended: {tag.note}</span>
              )}
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

      <PeriodPicker
        period={boundedPeriod}
        window={projectWindow}
        coverage={coverage}
        windowMinYear={windowMinYear}
        vintageYear={windowMaxYear}
        enforcePeriodBounds={enforcePeriodBounds}
        onSubmit={writePeriod}
        onClear={() => writePeriod(null)}
      />
      {graphResource.loading ? (
        <Skeleton variant="block" />
      ) : graphResource.error ? (
        <StatusRow tone="error" live="alert">
          The members' columns did not load ({graphResource.error}). Reload the
          page to try again, or open a member from the list.
        </StatusRow>
      ) : (
        <RepresentationPicker
          bands={bands}
          axes={group.axes}
          // A curated group's axes are its browse facets; without them the row
          // dimensions are the only filters the page has.
          includeRowDimensionFilters={
            !(group.source === "curated" && group.axes.length > 0)
          }
          window={pickerWindow}
          canAdd={regMetaVersion !== "" && steward !== ""}
          committedRows={committedRows}
          studyWindow={projectWindow}
          focusKey={focusKey}
          graph={graphResource.data}
          vintageYear={vintageYear}
          onApply={onApply}
          onStageChange={(hasDiff) => {
            setRefusal(null);
            if (hasDiff) setOutcome(null);
          }}
        />
      )}
      <StagedAddStatus
        outcome={outcome?.under === resolutionKey ? outcome.value : null}
        blocked={refusal?.under === refusalKey ? refusal.message : null}
      />
    </article>
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
  for (const band of bands) {
    for (const row of band.rows) {
      if (row.selectable === false || row.from === null || row.to === null) {
        continue;
      }
      if (row.from !== "0001-01-01") {
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
  return from === null && to === null ? null : { from, to: open ? null : to };
}
