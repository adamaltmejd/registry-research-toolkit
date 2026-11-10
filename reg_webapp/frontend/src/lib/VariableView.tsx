import { useRef, useState } from "react";
import {
  type CatalogSegment,
  type Context,
  getSegments,
  type OrderCheck,
  type VariableShow,
} from "./api";
import styles from "./CatalogNodeView.module.css";
import { loadRegister, registerSubjectNames } from "./catalog_names";
import { RelatedDocumentsPanel } from "./RelatedDocumentsPanel";
import { Button as ActionButton } from "./ui/Button";
import { Definitions } from "./ui/Definitions";
import { Panel } from "./ui/Panel";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { useAsyncResource } from "./use_async_resource";
import { ValueSetView } from "./ValueSetView";
import { useSuccession, useVariableCoverage } from "./VariableCoverage";
import variableStyles from "./VariableView.module.css";
import {
  factYears,
  groupFact,
  segmentFacts,
  typePills,
} from "./variable_facts";
import { windowBounds } from "./window_bounds";

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
              <p>{factYears(group.segments, bounds)}</p>
              {lines(text)}
            </div>
          ))}
    </Panel>
  );
}

/** An ungrouped variable's page; a grouped one is shown on its group's page
 * (GroupView). */
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
  const succession = useSuccession([node.fqid], node.register);
  const names = new Map([
    ...registerSubjectNames(register),
    ...succession.names,
  ]);
  const bounds = windowBounds(context);
  const rows = segments.data ?? [];
  const coverage = useVariableCoverage({
    subjects: [{ fqid: node.fqid, name: node.name, segments: rows }],
    label: "Variable columns",
    names,
    context,
    tables: register?.tables ?? null,
    validation,
    successions: succession.graphs,
    neighbourColumns: succession.neighbourColumns,
    successionError: succession.error,
    allowAdd: context !== null,
  });
  const factBounds = [bounds.min, bounds.max] as const;
  return (
    <article className={styles.page}>
      <Definitions
        kind="variable"
        pills={typePills(rows, factBounds)}
        title={node.name}
        definitions={[
          ...segmentFacts(rows, factBounds),
          ...(node.tags?.length
            ? [
                {
                  term: "Topics",
                  value: node.tags.map((tag) => tag.name).join(", "),
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
          succession.loading ||
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
      <RelatedDocumentsPanel fqids={[node.fqid]} names={names} />
      {coverage.addBar}
    </article>
  );
}
