import { type ReactNode, useRef, useState } from "react";
import { Button, Tooltip, TooltipTrigger } from "react-aria-components";
import { segmentType, variableColumns } from "./add_columns";
import {
  type CatalogSegment,
  type Context,
  getGraph,
  getSegments,
  type OrderCheck,
  type VariableShow,
} from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref, humanizeClassificationSlug } from "./catalog";
import {
  loadRegister,
  registerRead,
  registerSubjectNames,
  segmentsRead,
} from "./catalog_names";
import findingStyles from "./Finding.module.css";
import { yearWindowLabel } from "./period";
import { RelatedDocumentsPanel } from "./RelatedDocumentsPanel";
import { ScopedDataWarnings } from "./ScopedDataWarnings";
import { columnYearRuns } from "./study_window";
import { Button as ActionButton } from "./ui/Button";
import { type Definition, Definitions } from "./ui/Definitions";
import { Panel } from "./ui/Panel";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { TypePill } from "./ui/TypePill";
import { useAsyncResource } from "./use_async_resource";
import { useCatalogRead, useDistinct } from "./use_store_read";
import { ValueSetView } from "./ValueSetView";
import { useVariableCoverage } from "./VariableCoverage";
import variableStyles from "./VariableView.module.css";
import { windowBounds } from "./window_bounds";

const readNeighbourSegments = (refs: readonly string[]) =>
  Object.fromEntries(refs.map((fqid) => [fqid, segmentsRead(fqid)]));
const readNeighbourRegisters = (refs: readonly string[]) =>
  Object.fromEntries(refs.map((fqid) => [fqid, registerRead(fqid)]));

/** One fact of a segment, keyed so equal values group; null when the catalog
 * does not state it, or states "no" for a fact shown only when yes, so the
 * row is left out rather than printed as "Not stated" or "No". */
type SegmentFact = { key: string; value: ReactNode } | null;

const FACTS: readonly {
  term: string;
  of: (row: CatalogSegment) => SegmentFact;
}[] = [
  // An identifier shows as the Identifier type pill, so it has no row here.
  {
    term: "Sensitive",
    of: (row) => (row.sensitive ? { key: "yes", value: "Yes" } : null),
  },
  // Value sets have their own panel (ValueSetView); only the classification
  // they code is a fact here.
  {
    term: "Classification",
    of: (row) =>
      row.classification
        ? {
            key: row.classification,
            value: (
              <a className="link" href={catalogHref(row.classification)}>
                {humanizeClassificationSlug(
                  row.classification.split("/").at(-1) ?? "",
                )}
              </a>
            ),
          }
        : null,
  },
];

/** The segments grouped by one fact's value, in first-seen order, and
 * whether any segment leaves it unstated. */
function groupFact(
  segments: readonly CatalogSegment[],
  of: (row: CatalogSegment) => SegmentFact,
) {
  const groups = new Map<
    string,
    { value: ReactNode; segments: CatalogSegment[] }
  >();
  let unstated = false;
  for (const segment of segments) {
    const fact = of(segment);
    if (!fact) {
      unstated = true;
      continue;
    }
    const group = groups.get(fact.key) ?? { value: fact.value, segments: [] };
    group.segments.push(segment);
    groups.set(fact.key, group);
  }
  return { groups: [...groups], unstated };
}

function factYears(
  segments: readonly CatalogSegment[],
  bounds: readonly [number, number],
) {
  return columnYearRuns(segments, bounds).map(yearWindowLabel).join(", ");
}

/** The Type row: one pill per type the columns are written as, with its years
 * inside only where the type changes over time (maintainer 2026-11-04). */
function typeFact(
  segments: readonly CatalogSegment[],
  bounds: readonly [number, number],
): Definition[] {
  const { groups } = groupFact(segments, (row) => ({
    key: segmentType(row),
    value: null,
  }));
  if (groups.length === 0) return [];
  return [
    {
      term: "Type",
      value: (
        <span className={variableStyles.types}>
          {groups.map(([type, group]) => (
            <TypePill
              key={type}
              type={segmentType(group.segments[0])}
              years={
                groups.length > 1
                  ? factYears(group.segments, bounds)
                  : undefined
              }
            />
          ))}
        </span>
      ),
    },
  ];
}

/** The facts of a variable's segments (DESIGN.md → Detail pane): a fact every
 * segment states alike is one value; one that differs lists each value with
 * its years; a fact no segment states is left out. */
function segmentFacts(
  segments: readonly CatalogSegment[],
  bounds: readonly [number, number],
): Definition[] {
  return FACTS.flatMap(({ term, of }) => {
    const { groups, unstated } = groupFact(segments, of);
    if (groups.length === 0) return [];
    if (groups.length === 1 && !unstated)
      return [{ term, value: groups[0][1].value }];
    return [
      {
        term,
        value: (
          <ul className={styles.plainList}>
            {groups.map(([key, group]) => (
              <li key={key}>
                {group.value}{" "}
                <span className={variableStyles.factYears}>
                  {factYears(group.segments, bounds)}
                </span>
              </li>
            ))}
          </ul>
        ),
      },
    ];
  });
}
/** The curated description, else the provider's wording: once when every
 * stated period agrees, otherwise each wording with the years it is used. */
function Description({
  description,
  segments,
  bounds,
}: {
  description: string | undefined;
  segments: readonly CatalogSegment[];
  bounds: readonly [number, number];
}) {
  const lines = (text: string) =>
    text.split("\n").map((line) => (
      <p key={line} className={styles.prose}>
        {line}
      </p>
    ));
  if (description)
    return (
      <Panel title="Description">
        <p className={styles.prose}>{description}</p>
      </Panel>
    );
  const { groups, unstated } = groupFact(segments, (row) =>
    row.wording ? { key: row.wording, value: row.wording } : null,
  );
  if (groups.length === 0) return null;
  return (
    <Panel title="Description">
      {groups.length === 1 && !unstated
        ? lines(groups[0][0])
        : groups.map(([text, group]) => (
            <div key={text} className={variableStyles.wording}>
              <p className={variableStyles.factYears}>
                {factYears(group.segments, bounds)}
              </p>
              {lines(text)}
            </div>
          ))}
    </Panel>
  );
}

export function ValueSetLabel({
  valueSet,
}: {
  valueSet: { names: readonly string[] } | null;
}) {
  if (!valueSet)
    return <span className={variableStyles.singleLine}>Not stated</span>;
  const names = valueSet.names.length ? valueSet.names : ["Unnamed value set"];
  return (
    <TooltipTrigger delay={600}>
      <Button className={variableStyles.valueSet}>
        <span className={variableStyles.firstName}>{names[0]}</span>
        {names.length > 1 && (
          <span className={variableStyles.more}>
            {" "}
            and {names.length - 1} more
          </span>
        )}
      </Button>
      <Tooltip
        className={({ isEntering, isExiting }) =>
          `${findingStyles.tooltip} ${isEntering ? "motion-rise-in" : ""} ${isExiting ? "motion-rise-out" : ""}`
        }
        offset={6}
        style={{ zIndex: "var(--z-popover)" }}
      >
        <ul className={styles.plainList}>
          {names.map((name) => (
            <li key={name}>{name}</li>
          ))}
        </ul>
      </Tooltip>
    </TooltipTrigger>
  );
}

export function VariableView({
  node,
  context,
  validation,
}: {
  node: VariableShow;
  context: Context | null;
  validation: OrderCheck | null;
}) {
  // One counter retries every read the timeline needs: the register's tables
  // and the variable's per-period list.
  const [attempt, setAttempt] = useState(0);
  const timelineRegion = useRef<HTMLDivElement>(null);
  const registerResource = useAsyncResource(
    `${node.register}:${attempt}`,
    (signal) => loadRegister(node.register, signal),
  );
  const register = registerResource.data;
  const segments = useAsyncResource(`${node.fqid}:${attempt}`, (signal) =>
    getSegments(node.fqid, { signal }),
  );
  const graph = useAsyncResource(node.fqid, (signal) =>
    getGraph(node.fqid, { kind: "continues", signal }),
  );
  const neighbours = useCatalogRead(
    readNeighbourSegments,
    useDistinct(
      (graph.data?.nodes ?? [])
        .filter(
          (entry) => entry.kind === "variable" && entry.fqid !== node.fqid,
        )
        .map((entry) => entry.fqid),
    ),
  );
  const neighbourRegisters = useCatalogRead(
    readNeighbourRegisters,
    useDistinct(
      Object.keys(neighbours)
        .map((fqid) => fqid.split("/").slice(0, 2).join("/"))
        .filter((ref) => ref !== node.register),
    ),
  );
  const names = new Map(registerSubjectNames(register));
  for (const read of Object.values(neighbourRegisters)) {
    for (const [fqid, name] of registerSubjectNames(read.value))
      names.set(fqid, name);
  }
  const neighbourColumns = new Map(
    Object.entries(neighbours).map(([fqid, read]) => [
      fqid,
      variableColumns(read.value ?? []),
    ]),
  );
  const successionError = graph.error
    ? "Succession relations did not load."
    : Object.values(neighbours).some(
          (read) => !read.loading && read.value === null,
        )
      ? "Neighbour columns did not load."
      : Object.values(neighbourRegisters).some(
            (read) => !read.loading && read.value === null,
          )
        ? "Neighbour register names did not load."
        : null;
  const successionLoading =
    graph.loading ||
    Object.values(neighbours).some((read) => read.loading) ||
    Object.values(neighbourRegisters).some((read) => read.loading);
  const bounds = windowBounds(context);
  const coverage = useVariableCoverage({
    node,
    segments: segments.data ?? [],
    names,
    context,
    tables: register?.tables ?? null,
    validation,
    succession: graph.data,
    neighbourColumns,
    successionError,
    allowAdd: context !== null,
  });
  const factBounds = [bounds.min, bounds.max] as const;
  const rows = segments.data ?? [];
  return (
    <article className={styles.page}>
      <Definitions
        kind="variable"
        title={node.name}
        memberOf={
          register
            ? [
                {
                  relation: "Register",
                  label: register.name,
                  href: catalogHref(node.register),
                },
              ]
            : []
        }
        definitions={[
          ...typeFact(rows, factBounds),
          ...segmentFacts(rows, factBounds),
          ...(node.tags?.length
            ? [
                {
                  term: "Topics",
                  value: node.tags.map((tag) => tag.name).join(", "),
                },
              ]
            : []),
          ...(node.sets?.length
            ? [
                {
                  term: "Group",
                  value: node.sets.map((set, index) => (
                    <span key={set.fqid}>
                      {index > 0 && ", "}
                      <a className="link" href={catalogHref(set.fqid)}>
                        {set.name}
                      </a>
                    </span>
                  )),
                },
              ]
            : []),
        ]}
      />
      <Description
        description={node.description}
        segments={rows}
        bounds={factBounds}
      />
      <div
        ref={timelineRegion}
        tabIndex={-1}
        className={variableStyles.timeline}
        aria-busy={
          registerResource.loading ||
          successionLoading ||
          segments.loading ||
          undefined
        }
      >
        {registerResource.loading || segments.loading ? (
          <Skeleton count={2} />
        ) : registerResource.error || segments.error ? (
          <StatusRow
            tone="error"
            live="alert"
            action={
              <ActionButton
                variant="ghost"
                onPress={() => {
                  setAttempt((current) => current + 1);
                  timelineRegion.current?.focus();
                }}
              >
                Retry
              </ActionButton>
            }
          >
            {registerResource.error
              ? "Could not load the register's tables."
              : "Could not load this variable's years."}
          </StatusRow>
        ) : (
          coverage.graph
        )}
      </div>
      {segments.data && <ValueSetView segments={segments.data} names={names} />}
      <ScopedDataWarnings fqid={node.fqid} names={names} />
      <RelatedDocumentsPanel fqid={node.fqid} names={names} />
      {coverage.addBar}
    </article>
  );
}
