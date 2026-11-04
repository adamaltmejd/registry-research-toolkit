import { type ReactNode, useRef, useState } from "react";
import {
  Button,
  Disclosure,
  DisclosurePanel,
  Heading,
  Tooltip,
  TooltipTrigger,
} from "react-aria-components";
import { variableColumns } from "./add_columns";
import {
  type CatalogSegment,
  type Context,
  getGraph,
  getSegments,
  getVariableEvidence,
  type ValidationResultModel,
  type VariableShow,
} from "./api";
import styles from "./CatalogNodeView.module.css";
import {
  catalogHref,
  formatDataType,
  humanizeClassificationSlug,
} from "./catalog";
import { catalogWords, yesNo } from "./catalog_labels";
import {
  loadRegister,
  registerRead,
  registerSubjectNames,
  segmentsRead,
} from "./catalog_names";
import { catalogPeriodLabel } from "./catalog_period";
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
import { TreeChevron } from "./ui/TreeChevron";
import { useAsyncResource } from "./use_async_resource";
import { useCatalogRead, useDistinct } from "./use_store_read";
import { ValueSetView } from "./ValueSetView";
import { useVariableCoverage } from "./VariableCoverage";
import { EvidenceContent } from "./VariableEvidence";
import variableStyles from "./VariableView.module.css";
import { windowBounds } from "./window_bounds";

const readNeighbourSegments = (refs: readonly string[]) =>
  Object.fromEntries(refs.map((fqid) => [fqid, segmentsRead(fqid)]));
const readNeighbourRegisters = (refs: readonly string[]) =>
  Object.fromEntries(refs.map((fqid) => [fqid, registerRead(fqid)]));

/** One fact of a segment, keyed so equal values group; null when the catalog
 * does not state it (or states the default, "clear"), so the row is left out
 * rather than printed as "Not stated". */
type SegmentFact = { key: string; value: ReactNode } | null;

const FACTS: readonly {
  term: string;
  of: (row: CatalogSegment) => SegmentFact;
}[] = [
  {
    term: "Role",
    of: (row) => ({ key: row.role, value: catalogWords(row.role) }),
  },
  {
    term: "Coded",
    of: (row) =>
      row.coded ? { key: row.coded, value: yesNo(row.coded === "yes") } : null,
  },
  {
    term: "State",
    of: (row) =>
      row.state ? { key: row.state, value: catalogWords(row.state) } : null,
  },
  {
    term: "Identifier",
    of: (row) =>
      row.identifier === undefined
        ? null
        : {
            key: `${row.identifier}${row.id_kind ?? ""}`,
            value: `${yesNo(row.identifier)}${row.identifier && row.id_kind ? ` (${catalogWords(row.id_kind)})` : ""}`,
          },
  },
  {
    term: "Sensitive",
    of: (row) =>
      row.sensitive === undefined
        ? null
        : { key: String(row.sensitive), value: yesNo(row.sensitive) },
  },
  {
    term: "Temporality",
    of: (row) =>
      row.temporality
        ? { key: row.temporality, value: catalogWords(row.temporality) }
        : null,
  },
  {
    term: "Storage",
    of: (row) => {
      const type = formatDataType(
        row.type || row.format,
        row.length?.toString(),
      );
      const format =
        row.type && row.format && row.format !== row.type ? row.format : "";
      const storage = [type, format].filter(Boolean).join(" ");
      if (storage) return { key: storage, value: <code>{storage}</code> };
      if (row.length !== undefined)
        return {
          key: `length ${row.length}`,
          value: (
            <>
              Length <code>{row.length}</code>
            </>
          ),
        };
      return null;
    },
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
  validation: ValidationResultModel | null;
}) {
  const [registerAttempt, setRegisterAttempt] = useState(0);
  const registerRegion = useRef<HTMLDivElement>(null);
  const registerResource = useAsyncResource(
    `${node.register}:${registerAttempt}`,
    (signal) => loadRegister(node.register, signal),
  );
  const register = registerResource.data;
  const segments = useAsyncResource(node.fqid, (signal) =>
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
  // Keep the key after closing: the resource (including a failure) lasts for this page.
  const [evidenceRequested, setEvidenceRequested] = useState(false);
  const evidence = useAsyncResource(
    evidenceRequested ? node.fqid : null,
    (signal) => getVariableEvidence(node.fqid, { signal }),
  );
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
  const facts = segmentFacts(segments.data ?? [], [bounds.min, bounds.max]);
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
          ...facts,
          {
            term: "Period",
            value: <code>{catalogPeriodLabel(node.period)}</code>,
          },
          ...(node.tags?.length
            ? [
                {
                  term: "Tags",
                  value: node.tags?.map((tag) => tag.name).join(", "),
                },
              ]
            : []),
          ...(node.sets?.length
            ? [
                {
                  term: "Groups",
                  value: node.sets?.map((set, index) => (
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
      <div
        ref={registerRegion}
        tabIndex={-1}
        className={variableStyles.registerRead}
        aria-busy={
          registerResource.loading ||
          successionLoading ||
          segments.loading ||
          undefined
        }
      >
        {registerResource.loading || segments.loading ? (
          <Skeleton count={2} />
        ) : registerResource.error ? (
          <StatusRow
            tone="error"
            live="alert"
            action={
              <ActionButton
                variant="ghost"
                onPress={() => {
                  setRegisterAttempt((attempt) => attempt + 1);
                  registerRegion.current?.focus();
                }}
              >
                Retry
              </ActionButton>
            }
          >
            Could not load the register's tables.
          </StatusRow>
        ) : segments.error ? (
          <StatusRow tone="error" live="alert">
            The variable's columns did not load ({segments.error}). Reload the
            page to try again.
          </StatusRow>
        ) : (
          coverage.graph
        )}
      </div>
      <Description
        description={node.description}
        segments={segments.data ?? []}
        bounds={[bounds.min, bounds.max]}
      />
      {segments.data && <ValueSetView segments={segments.data} names={names} />}
      <ScopedDataWarnings fqid={node.fqid} names={names} />
      <RelatedDocumentsPanel fqid={node.fqid} names={names} />
      <Disclosure
        className={styles.disclosure}
        onExpandedChange={(expanded) => {
          if (expanded) setEvidenceRequested(true);
        }}
      >
        {({ isExpanded }) => (
          <>
            <Heading level={2} className={styles.disclosureHeading}>
              <Button slot="trigger" className={styles.disclosureButton}>
                <TreeChevron />
                <span>Source and provenance</span>
              </Button>
            </Heading>
            <DisclosurePanel
              className={`motion-disclose ${isExpanded ? "open" : ""}`}
            >
              <div>
                {isExpanded &&
                  (evidence.loading ? (
                    <div aria-busy="true">
                      <Skeleton count={2} />
                    </div>
                  ) : evidence.error ? (
                    <StatusRow tone="error" live="alert">
                      Source and provenance did not load ({evidence.error}).
                      Reload the page to try again.
                    </StatusRow>
                  ) : evidence.data ? (
                    <EvidenceContent
                      evidence={evidence.data}
                      node={node}
                      names={names}
                    />
                  ) : null)}
              </div>
            </DisclosurePanel>
          </>
        )}
      </Disclosure>
      {coverage.addBar}
    </article>
  );
}
