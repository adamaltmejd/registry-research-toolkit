import { useRef, useState } from "react";
import {
  Button,
  Disclosure,
  DisclosurePanel,
  Heading,
  Tooltip,
  TooltipTrigger,
} from "react-aria-components";
import {
  type CatalogSegment,
  type Context,
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
import { loadRegister, registerSubjectNames } from "./catalog_names";
import { catalogPeriodLabel } from "./catalog_period";
import findingStyles from "./Finding.module.css";
import { RelatedDocumentsPanel } from "./RelatedDocumentsPanel";
import { ScopedDataWarnings } from "./ScopedDataWarnings";
import { Button as ActionButton } from "./ui/Button";
import { ColumnChip } from "./ui/ColumnChip";
import { DataTable } from "./ui/DataTable";
import { Definitions } from "./ui/Definitions";
import { EmptyState } from "./ui/EmptyState";
import { Panel } from "./ui/Panel";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { TreeChevron } from "./ui/TreeChevron";
import { useAsyncResource } from "./use_async_resource";
import { ValueSetView } from "./ValueSetView";
import { useVariableCoverage } from "./VariableCoverage";
import { EvidenceContent } from "./VariableEvidence";
import variableStyles from "./VariableView.module.css";

function segmentFacts(row: CatalogSegment) {
  return [
    { term: "Role", value: catalogWords(row.role) },
    { term: "Coded", value: yesNo(row.coded) },
    { term: "State", value: catalogWords(row.state) },
    {
      term: "Identifier",
      value: `${yesNo(row.identifier)}${row.identifier && row.id_kind ? ` (${catalogWords(row.id_kind)})` : ""}`,
    },
    { term: "Sensitive", value: yesNo(row.sensitive) },
    { term: "Temporality", value: catalogWords(row.temporality) },
  ];
}

function Storage({ row }: { row: CatalogSegment }) {
  const type = formatDataType(row.type || row.format, row.length?.toString());
  const format =
    row.type && row.format && row.format !== row.type ? row.format : "";
  const storage = [type, format].filter(Boolean).join(" ");
  if (storage) return <code>{storage}</code>;
  if (row.length !== null)
    return (
      <>
        Length <code>{row.length}</code>
      </>
    );
  return <>Not stated</>;
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
  const names = registerSubjectNames(register);
  const segments = useAsyncResource(node.fqid, (signal) =>
    getSegments(node.fqid, { signal }),
  );
  // Keep the key after closing: the resource (including a failure) lasts for this page.
  const [evidenceRequested, setEvidenceRequested] = useState(false);
  const evidence = useAsyncResource(
    evidenceRequested ? node.fqid : null,
    (signal) => getVariableEvidence(node.fqid, { signal }),
  );
  const coverage = useVariableCoverage({
    node,
    segments: segments.data ?? [],
    names,
    context,
    tables: register?.tables ?? null,
    validation,
  });
  const facts = (segments.data ?? []).map(segmentFacts);
  const common = (facts[0] ?? []).filter((fact, index) =>
    facts.every((row) => row[index].value === fact.value),
  );
  const varying = (facts[0] ?? []).filter(
    (fact) => !common.some((shared) => shared.term === fact.term),
  );
  const rows = (segments.data ?? []).map((row) => ({
    ...row,
    storage: row,
    notes: segmentFacts(row).filter((fact) =>
      varying.some((field) => field.term === fact.term),
    ),
  }));
  const hasCoding = rows.some(
    (row) => row.value_set !== null || row.classification !== null,
  );
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
          ...common,
          {
            term: "Period",
            value: <code>{catalogPeriodLabel(node.period)}</code>,
          },
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
                  term: "Sets",
                  value: node.sets.map((set) => set.name).join(", "),
                },
              ]
            : []),
        ]}
      />
      <div
        ref={registerRegion}
        tabIndex={-1}
        className={variableStyles.registerRead}
        aria-busy={registerResource.loading || undefined}
      >
        {registerResource.loading ? (
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
        ) : (
          coverage.graph
        )}
      </div>
      {node.description ? (
        <Panel title="Description">
          <p className={styles.prose}>{node.description}</p>
        </Panel>
      ) : node.source_definition ? (
        <Panel title="Provider wording">
          <p className={variableStyles.wordingPeriod}>
            Wording for {catalogPeriodLabel(node.source_definition.period)}
          </p>
          <p className={styles.prose}>{node.source_definition.text}</p>
        </Panel>
      ) : null}
      <Panel title="Tables">
        {node.tables.length ? (
          <ul className={`${styles.plainList} ${variableStyles.listGap}`}>
            {node.tables.map((fqid) => (
              <li key={fqid}>
                <a className="link" href={catalogHref(fqid)}>
                  {names.get(fqid) ?? "Table name unavailable"}
                </a>
              </li>
            ))}
          </ul>
        ) : (
          <EmptyState title="No tables recorded for this variable." />
        )}
      </Panel>
      <Panel title="Segments" flush={!!segments.data?.length}>
        {segments.loading ? (
          <div aria-busy="true">
            <Skeleton count={3} />
          </div>
        ) : segments.error ? (
          <StatusRow tone="error" live="alert">
            Segments did not load ({segments.error}). Reload the page to try
            again.
          </StatusRow>
        ) : segments.data?.length ? (
          <div className={variableStyles.segments}>
            <DataTable
              columns={[
                {
                  key: "table",
                  label: "Table",
                  width: varying.length ? "13%" : "18%",
                },
                {
                  key: "period",
                  label: "Period",
                  width: "13%",
                },
                {
                  key: "spelling",
                  label: "Spelling",
                  width: varying.length ? "11%" : "15%",
                },
                {
                  key: "storage",
                  label: "Storage",
                  width: "14%",
                },
                ...(hasCoding
                  ? [
                      {
                        key: "value_set" as const,
                        label: "Value set",
                        width: varying.length ? "35%" : "40%",
                      },
                    ]
                  : []),
                ...(varying.length
                  ? [{ key: "notes" as const, label: "Notes", width: "14%" }]
                  : []),
              ]}
              rows={rows}
              getRowId={(row) => JSON.stringify(row)}
              renderCell={(row, column) => {
                switch (column.key) {
                  case "table":
                    return (
                      <a className="link" href={catalogHref(row.table)}>
                        {names.get(row.table) ?? "Table name unavailable"}
                      </a>
                    );
                  case "period":
                    return <code>{catalogPeriodLabel(row.period)}</code>;
                  case "spelling":
                    return (
                      <ColumnChip
                        name={row.spelling}
                        className={variableStyles.spelling}
                      />
                    );
                  case "storage":
                    return <Storage row={row} />;
                  case "value_set":
                    return (
                      <span className={variableStyles.valueSetCell}>
                        <ValueSetLabel valueSet={row.value_set} />
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
                    );
                  case "notes":
                    return (
                      <ul
                        className={`${styles.plainList} ${variableStyles.listGap}`}
                      >
                        {row.notes.map((fact) => (
                          <li key={fact.term}>
                            {fact.term}: {fact.value}
                          </li>
                        ))}
                      </ul>
                    );
                }
              }}
            />
          </div>
        ) : (
          <EmptyState title="No segments recorded for this variable." />
        )}
      </Panel>
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
