import { useRef, useState } from "react";
import { Button as AriaButton } from "react-aria-components";
import {
  type CatalogSegment,
  type Context,
  getSegments,
  type OrderCheck,
  type SetShow,
  type VariableShow,
} from "./api";
import styles from "./CatalogNodeView.module.css";
import {
  classFamiliesRead,
  loadRegister,
  registerSubjectNames,
} from "./catalog_names";
import { Paragraphs } from "./Paragraphs";
import { RelatedDocumentsPanel } from "./RelatedDocumentsPanel";
import { SeriesBreaks } from "./SeriesBreaks";
import { type PlacedBreak, placeBreaks } from "./series_breaks";
import { Button as ActionButton } from "./ui/Button";
import { Definitions } from "./ui/Definitions";
import { Panel } from "./ui/Panel";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { useAsyncResource } from "./use_async_resource";
import { useCatalogRead } from "./use_store_read";
import { ValueSetView } from "./ValueSetView";
import {
  type CoverageSubject,
  useSuccession,
  useVariableCoverage,
} from "./VariableCoverage";
import variableStyles from "./VariableView.module.css";
import {
  type ClassFamilies,
  factYears,
  groupFact,
  segmentFacts,
  typePills,
} from "./variable_facts";

// simplify: a description longer than this is cut to two lines with "More";
// measure the overflow instead if short texts start to hide behind it.
const LONG = 90;

/** The curated description, else the provider's wording: once when every
 * stated period agrees, otherwise each wording with the years it is used. */
function Description({
  description,
  segments,
}: {
  description: string | null | undefined;
  /** The rows whose wording stands in for a missing description. */
  segments: readonly CatalogSegment[];
}) {
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
      {groups.length === 1 && !unstated ? (
        <Paragraphs text={groups[0][0]} />
      ) : (
        groups.map(([text, group]) => (
          <div key={text} className={variableStyles.wording}>
            <p>{factYears(group.segments)}</p>
            <Paragraphs text={text} />
          </div>
        ))
      )}
    </Panel>
  );
}

/** A group member's description where it differs from the group's (the
 * server leaves it out otherwise), in its lane's name column. */
function MemberDescription({ description }: { description: string }) {
  const [open, setOpen] = useState(false);
  return (
    <>
      <p
        className={variableStyles.memberDescription}
        data-open={open || description.length <= LONG || undefined}
      >
        {description}
      </p>
      {description.length > LONG && (
        <AriaButton
          className={`link ${variableStyles.more}`}
          aria-expanded={open}
          onPress={() => setOpen((current) => !current)}
        >
          {open ? "Less" : "More"}
        </AriaButton>
      )}
    </>
  );
}

/** Each classification version the catalog lists, to its family. */
function useClassFamilies(): ClassFamilies {
  const families = useCatalogRead(classFamiliesRead, undefined).value ?? [];
  return new Map(
    families.flatMap((family) =>
      family.versions.map((version) => [
        version.fqid,
        { fqid: family.fqid, name: family.name },
      ]),
    ),
  );
}

/** The one page of a variable and of a group (DESIGN.md → Detail pane, #2319):
 * the kind and Type pills, the `h1`, the facts of every column, the
 * description, the coverage drawing with the add flow, the value sets and the
 * documents. Only the kind pill and the address tell the two apart. */
function VariablePage({
  kind,
  title,
  description,
  wording = [],
  facets = [],
  tags,
  fqid,
  register,
  subjects,
  segments,
  loading = false,
  failed = null,
  attempt,
  onRetry,
  focus = null,
  context,
  validation,
  breaks,
}: {
  kind: "variable" | "group";
  title: string;
  description: string | null | undefined;
  /** The rows whose wording stands in for a missing description. */
  wording?: readonly CatalogSegment[];
  facets?: readonly { label: string; value: string }[];
  tags: readonly { name: string }[] | undefined;
  /** The page's own ref: the variable's, or the group's. */
  fqid: string;
  register: string;
  subjects: readonly CoverageSubject[];
  /** Every column's per-period rows: the facts and value sets read them. */
  segments: readonly CatalogSegment[] | null;
  loading?: boolean;
  /** What did not load besides the register's tables. */
  failed?: string | null;
  /** One counter retries every read the drawing needs. */
  attempt: number;
  onRetry: () => void;
  focus?: string | null;
  context: Context | null;
  validation: OrderCheck | null;
  breaks: readonly PlacedBreak[];
}) {
  const timelineRegion = useRef<HTMLDivElement>(null);
  const registerResource = useAsyncResource(
    `${register}:${attempt}`,
    (signal) => loadRegister(register, signal),
  );
  const fqids = subjects.map((subject) => subject.fqid);
  const succession = useSuccession(
    fqids,
    register,
    kind === "group" ? fqid : null,
  );
  const families = useClassFamilies();
  const names = new Map([
    ...registerSubjectNames(registerResource.data),
    ...succession.names,
  ]);
  const rows = segments ?? [];
  const coverage = useVariableCoverage({
    subjects,
    label: kind === "group" ? "Group members" : "Variable columns",
    names,
    context,
    tables: registerResource.data?.tables ?? null,
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
        kind={kind}
        pills={typePills(rows)}
        title={title}
        facets={facets}
        definitions={[
          ...segmentFacts(rows, families),
          ...(tags?.length
            ? [
                {
                  term: "Topics",
                  value: tags.map((tag) => tag.name).join(", "),
                },
              ]
            : []),
        ]}
      />
      <Description description={description} segments={wording} />
      <SeriesBreaks breaks={breaks} />
      <div
        ref={timelineRegion}
        tabIndex={-1}
        className={variableStyles.timeline}
        aria-busy={
          registerResource.loading || succession.loading || loading || undefined
        }
      >
        {registerResource.loading || loading ? (
          <Skeleton count={2} />
        ) : registerResource.error || failed ? (
          <StatusRow
            tone="error"
            live="alert"
            action={
              <ActionButton
                variant="ghost"
                onPress={() => {
                  onRetry();
                  timelineRegion.current?.focus();
                }}
              >
                Retry
              </ActionButton>
            }
          >
            {registerResource.error
              ? "Could not load the register's tables."
              : failed}
          </StatusRow>
        ) : (
          coverage.graph
        )}
      </div>
      {segments && <ValueSetView segments={segments} names={names} />}
      <RelatedDocumentsPanel
        fqid={fqid}
        members={kind === "group" ? fqids : undefined}
        names={names}
      />
      {coverage.addBar}
    </article>
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
  const [attempt, setAttempt] = useState(0);
  const segments = useAsyncResource(`${node.fqid}:${attempt}`, (signal) =>
    getSegments(node.fqid, { signal }),
  );
  const rows = segments.data ?? [];
  return (
    <VariablePage
      kind="variable"
      title={node.name}
      description={node.description}
      wording={rows}
      tags={node.tags}
      fqid={node.fqid}
      register={node.register}
      subjects={[{ fqid: node.fqid, name: node.name, segments: rows }]}
      segments={segments.data}
      loading={segments.loading}
      failed={segments.error ? "Could not load this variable's years." : null}
      attempt={attempt}
      onRetry={() => setAttempt((current) => current + 1)}
      context={context}
      validation={validation}
      breaks={placeBreaks(null, node.series_breaks)}
    />
  );
}

/** A group's page: every member a lane of the one drawing, titled by its
 * varying facets, with its description where it differs from the group's. A
 * grouped variable's address opens it with that member's lane in focus. */
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
        // A member's name and line wrap whether or not it has a description.
        wrap: true,
        detail: member.description ? (
          <MemberDescription description={member.description} />
        ) : undefined,
      },
    };
  });
  return (
    <VariablePage
      kind="group"
      title={node.name}
      description={node.description}
      facets={axes.flatMap((axis) =>
        !axis.varying && axis.value
          ? [{ label: axis.label, value: axis.value }]
          : [],
      )}
      tags={node.tags}
      fqid={node.fqid}
      register={node.register}
      subjects={subjects}
      segments={node.members.flatMap((member) => member.segments)}
      attempt={attempt}
      onRetry={() => setAttempt((current) => current + 1)}
      focus={focus}
      context={context}
      validation={validation}
      breaks={titled.flatMap(({ member, title }) =>
        placeBreaks(title, member.series_breaks),
      )}
    />
  );
}
