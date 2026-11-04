import { type ReactNode, useRef, useState } from "react";
import {
  Button,
  Disclosure,
  DisclosurePanel,
  Heading,
  Tooltip,
  TooltipTrigger,
} from "react-aria-components";
import { heldColumns, variableColumns } from "./add_columns";
import {
  type CatalogSegment,
  type Context,
  getCoverage,
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
      row.coded === "yes" || row.coded === "no"
        ? { key: row.coded, value: yesNo(row.coded) }
        : null,
  },
  {
    term: "State",
    of: (row) =>
      row.state === "clear" || row.state === "unknown"
        ? null
        : { key: row.state, value: catalogWords(row.state) },
  },
  {
    term: "Identifier",
    of: (row) =>
      row.identifier === null
        ? null
        : {
            key: `${row.identifier}${row.id_kind ?? ""}`,
            value: `${yesNo(row.identifier)}${row.identifier && row.id_kind ? ` (${catalogWords(row.id_kind)})` : ""}`,
          },
  },
  {
    term: "Sensitive",
    of: (row) =>
      row.sensitive === null
        ? null
        : { key: String(row.sensitive), value: yesNo(row.sensitive) },
  },
  {
    term: "Temporality",
    of: (row) =>
      row.temporality === "unknown"
        ? null
        : { key: row.temporality, value: catalogWords(row.temporality) },
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
      if (row.length !== null)
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
  {
    term: "Value set",
    of: (row) =>
      row.value_set === null && row.classification === null
        ? null
        : {
            key: `${row.value_set?.hash ?? ""} ${row.classification ?? ""}`,
            value: (
              <span className={variableStyles.valueSetCell}>
                {row.value_set && <ValueSetLabel valueSet={row.value_set} />}
                {row.classification && (
                  <a
                    className={`link ${variableStyles.classification}`}
                    href={catalogHref(row.classification)}
                  >
                    {humanizeClassificationSlug(
                      row.classification.split("/").at(-1) ?? "",
                    )}
                  </a>
                )}
              </span>
            ),
          },
  },
];

/** The facts of a variable's segments (DESIGN.md → Detail pane): a fact every
 * segment states alike is one value; one that differs lists each value with
 * its years; a fact no segment states is left out. */
function segmentFacts(
  segments: readonly CatalogSegment[],
  bounds: readonly [number, number],
): Definition[] {
  return FACTS.flatMap(({ term, of }) => {
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
    if (groups.size === 0) return [];
    if (groups.size === 1 && !unstated)
      return [{ term, value: [...groups.values()][0].value }];
    return [
      {
        term,
        value: (
          <ul className={styles.plainList}>
            {[...groups].map(([key, group]) => (
              <li key={key}>
                {group.value}{" "}
                <span className={variableStyles.factYears}>
                  {columnYearRuns(group.segments, bounds)
                    .map(yearWindowLabel)
                    .join(", ")}
                </span>
              </li>
            ))}
          </ul>
        ),
      },
    ];
  });
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
  // A steward deployment shows what it holds: the timeline and the facts read
  // its held placements, never the reference catalog's wider periods.
  const steward = context?.steward_config != null;
  const held = useAsyncResource(steward ? node.fqid : null, (signal) =>
    getCoverage(node.fqid, { signal }),
  );
  const heldPlacements = steward ? (held.data?.held ?? []) : null;
  const bounds = windowBounds(context);
  const factSegments = heldPlacements
    ? heldColumns(segments.data ?? [], heldPlacements).flatMap(
        (column) => column.segments,
      )
    : (segments.data ?? []);
  const coverage = useVariableCoverage({
    node,
    segments: segments.data ?? [],
    held: heldPlacements,
    names,
    context,
    tables: register?.tables ?? null,
    validation,
    succession: graph.data,
    neighbourColumns,
    successionError,
    allowAdd: context !== null,
  });
  const facts = segmentFacts(factSegments, [bounds.min, bounds.max]);
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
          // On a steward deployment the timeline's header gives the held years;
          // the reference period would claim more.
          ...(steward
            ? []
            : [
                {
                  term: "Period",
                  value: <code>{catalogPeriodLabel(node.period)}</code>,
                },
              ]),
          ...(node.tags.length
            ? [
                {
                  term: "Tags",
                  value: node.tags.map((tag) => tag.name).join(", "),
                },
              ]
            : []),
          ...(node.sets.length
            ? [
                {
                  term: "Groups",
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
      <div
        ref={registerRegion}
        tabIndex={-1}
        className={variableStyles.registerRead}
        aria-busy={
          registerResource.loading ||
          successionLoading ||
          held.loading ||
          segments.loading ||
          undefined
        }
      >
        {registerResource.loading || held.loading || segments.loading ? (
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
        ) : held.error ? (
          <StatusRow tone="error" live="alert">
            The years held did not load ({held.error}). Reload the page to try
            again.
          </StatusRow>
        ) : (
          coverage.graph
        )}
      </div>
      {(node.description ?? node.source_definition?.text) && (
        <Panel title="Description">
          <p className={styles.prose}>
            {node.description ?? node.source_definition?.text}
          </p>
        </Panel>
      )}
      {segments.error && (
        <StatusRow tone="error" live="alert">
          The variable's columns did not load ({segments.error}). Reload the
          page to try again.
        </StatusRow>
      )}
      {segments.data && <ValueSetView segments={factSegments} names={names} />}
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
