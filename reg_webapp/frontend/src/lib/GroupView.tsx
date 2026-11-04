import { useRef, useState } from "react";
import { Button as AriaButton } from "react-aria-components";
import type { CatalogSegment, Context, OrderCheck, SetShow } from "./api";
import styles from "./CatalogNodeView.module.css";
import { CodeTimeline } from "./CodeTimeline";
import { catalogHref } from "./catalog";
import { loadRegister, registerSubjectNames } from "./catalog_names";
import groupStyles from "./GroupView.module.css";
import { RelatedDocumentsPanel } from "./RelatedDocumentsPanel";
import { ScopedDataWarnings } from "./ScopedDataWarnings";
import { Button } from "./ui/Button";
import { Definitions } from "./ui/Definitions";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { useAsyncResource } from "./use_async_resource";
import {
  type CoverageSubject,
  useSuccession,
  useVariableCoverage,
} from "./VariableCoverage";
import variableStyles from "./VariableView.module.css";
import { segmentFacts, typeFact } from "./variable_facts";
import { windowBounds } from "./window_bounds";

// simplify: a description longer than this is cut to two lines with "More";
// measure the overflow instead if short texts start to hide behind it.
const LONG = 90;

/** A member's lane detail: its type pills and facts, then its description
 * where it differs from the group's (the server leaves it out otherwise). */
function MemberDetail({
  segments,
  description,
  bounds,
}: {
  segments: readonly CatalogSegment[];
  description: string | undefined;
  bounds: readonly [number, number];
}) {
  const [open, setOpen] = useState(false);
  const facts = [
    ...typeFact(segments, bounds),
    ...segmentFacts(segments, bounds),
  ];
  return (
    <>
      {facts.length > 0 && (
        <dl className={groupStyles.facts}>
          {facts.map((fact) => (
            <div key={fact.term} className={groupStyles.fact}>
              {/* The Type pills say what they are; the term is for a screen
                  reader there. */}
              <dt
                className={
                  fact.term === "Type" ? "visually-hidden" : groupStyles.term
                }
              >
                {fact.term}
              </dt>
              <dd>{fact.value}</dd>
            </div>
          ))}
        </dl>
      )}
      {description && (
        <>
          <p
            className={groupStyles.description}
            data-open={open || description.length <= LONG || undefined}
          >
            {description}
          </p>
          {description.length > LONG && (
            <AriaButton
              className={`link ${groupStyles.more}`}
              aria-expanded={open}
              onPress={() => setOpen((current) => !current)}
            >
              {open ? "Less" : "More"}
            </AriaButton>
          )}
        </>
      )}
    </>
  );
}

/** A group's page (DESIGN.md → Detail pane): every member as a lane with its
 * facts, description, columns and years to add, then the group's codes,
 * notes and documents. A grouped variable's address opens it with that
 * member's lane in focus. */
export function GroupView({
  node,
  focus = null,
  context,
  validation,
}: {
  node: SetShow;
  /** The member whose address opened the page. */
  focus?: string | null;
  context: Context | null;
  validation: OrderCheck | null;
}) {
  const [attempt, setAttempt] = useState(0);
  const timelineRegion = useRef<HTMLDivElement>(null);
  const registerResource = useAsyncResource(
    `${node.register}:${attempt}`,
    (signal) => loadRegister(node.register, signal),
  );
  const register = registerResource.data;
  const fqids = node.members.map((member) => member.fqid);
  const succession = useSuccession(fqids, node.register);
  const names = new Map([
    ...registerSubjectNames(register),
    ...succession.names,
  ]);
  const bounds = windowBounds(context);
  const factBounds = [bounds.min, bounds.max] as const;
  const axes = node.axes.flatMap((axis) => {
    const facets = node.members.map((member) =>
      member.facets.find((facet) => facet.axis === axis.id),
    );
    if (!facets.some((facet) => facet !== undefined)) return [];
    return [
      {
        ...axis,
        varying: new Set(facets.map((facet) => facet?.value)).size > 1,
        value: facets[0]?.label,
      },
    ];
  });
  const varying = axes.filter((axis) => axis.varying);
  // A lane's title is its varying facets, else its name; the line under it is
  // the name, or the column names where title and name repeat another lane's.
  const titled = node.members.map((member) => {
    const facets = varying.flatMap((axis) => {
      const label = member.facets.find(
        (facet) => facet.axis === axis.id,
      )?.label;
      return label ? [label] : [];
    });
    return {
      member,
      title: facets.length ? facets.join(", ") : member.name,
      name: facets.length ? member.name : null,
    };
  });
  const seen = new Map<string, number>();
  for (const { title, name } of titled) {
    const key = JSON.stringify([title, name]);
    seen.set(key, (seen.get(key) ?? 0) + 1);
  }
  const subjects = titled.map(({ member, title, name }): CoverageSubject => {
    const repeated = (seen.get(JSON.stringify([title, name])) ?? 0) > 1;
    const spellings = [
      ...new Set(member.segments.map((segment) => segment.column)),
    ].join(", ");
    return {
      fqid: member.fqid,
      name: member.name,
      segments: member.segments,
      lane: {
        name: title,
        sub:
          repeated && spellings
            ? name
              ? { text: `${name}, ${spellings}` }
              : { text: spellings, mono: true }
            : name
              ? { text: name }
              : null,
        detail: (
          <MemberDetail
            segments={member.segments}
            description={member.description}
            bounds={factBounds}
          />
        ),
      },
    };
  });
  const coverage = useVariableCoverage({
    subjects,
    label: "Group members",
    names,
    context,
    tables: register?.tables ?? null,
    validation,
    successions: succession.graphs,
    neighbourColumns: succession.neighbourColumns,
    successionError: succession.error,
    allowAdd: context !== null,
    focus,
  });
  return (
    <article className={styles.page}>
      <Definitions
        kind="group"
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
        description={node.description}
        facets={axes.flatMap((axis) =>
          !axis.varying && axis.value
            ? [{ label: axis.label, value: axis.value }]
            : [],
        )}
        definitions={
          node.tags.length
            ? [
                {
                  term: "Topics",
                  value: node.tags.map((tag) => tag.name).join(", "),
                },
              ]
            : []
        }
      />
      <div
        ref={timelineRegion}
        tabIndex={-1}
        className={variableStyles.timeline}
        aria-busy={registerResource.loading || succession.loading || undefined}
      >
        {registerResource.loading ? (
          <Skeleton count={2} />
        ) : registerResource.error ? (
          <StatusRow
            tone="error"
            live="alert"
            action={
              <Button
                variant="ghost"
                onPress={() => {
                  setAttempt((current) => current + 1);
                  timelineRegion.current?.focus();
                }}
              >
                Retry
              </Button>
            }
          >
            Could not load the register's tables.
          </StatusRow>
        ) : (
          coverage.graph
        )}
      </div>
      <CodeTimeline
        members={node.members}
        axis={coverage.axis}
        window={coverage.window}
        bounds={factBounds}
      />
      <ScopedDataWarnings fqids={fqids} names={names} />
      <RelatedDocumentsPanel fqids={fqids} names={names} />
      {coverage.addBar}
    </article>
  );
}
