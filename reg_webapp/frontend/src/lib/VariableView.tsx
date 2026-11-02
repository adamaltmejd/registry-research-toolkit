import { useState } from "react";
import {
  Button,
  Disclosure,
  DisclosurePanel,
  Heading,
} from "react-aria-components";
import {
  type CatalogSegment,
  getSegments,
  getVariableEvidence,
  type VariableShow,
} from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref } from "./catalog";
import { catalogPeriodLabel } from "./catalog_period";
import { ScopedDataWarnings } from "./ScopedDataWarnings";
import { ColumnChip } from "./ui/ColumnChip";
import { DataTable } from "./ui/DataTable";
import { Definitions } from "./ui/Definitions";
import { EmptyState } from "./ui/EmptyState";
import { Panel } from "./ui/Panel";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { TreeChevron } from "./ui/TreeChevron";
import { useAsyncResource } from "./use_async_resource";
import { EvidenceContent } from "./VariableEvidence";
import variableStyles from "./VariableView.module.css";

function flag(value: boolean | null): string {
  return value === null ? "Not stated" : value ? "Yes" : "No";
}

export function VariableView({ node }: { node: VariableShow }) {
  const segments = useAsyncResource(node.fqid, (signal) =>
    getSegments(node.fqid, { signal }),
  );
  // Keep the key after closing: the resource (including a failure) lasts for this page.
  const [evidenceRequested, setEvidenceRequested] = useState(false);
  const evidence = useAsyncResource(
    evidenceRequested ? node.fqid : null,
    (signal) => getVariableEvidence(node.fqid, { signal }),
  );
  const segmentCell = (row: CatalogSegment, key: keyof CatalogSegment) => {
    switch (key) {
      case "table":
        return (
          <a className="link" href={catalogHref(row.table)}>
            <code>{row.table}</code>
          </a>
        );
      case "period":
        return catalogPeriodLabel(row.period);
      case "spelling":
        return <ColumnChip name={row.spelling} />;
      case "identifier":
        return (
          <span className={variableStyles.atomic}>
            {flag(row.identifier)}
            {row.id_kind && (
              <>
                {" "}
                (<code>{row.id_kind}</code>)
              </>
            )}
          </span>
        );
      case "sensitive":
        return (
          <span className={variableStyles.atomic}>{flag(row.sensitive)}</span>
        );
      case "value_set":
        return row.value_set ? (
          <>
            {row.value_set.names.join(", ") || (
              <code>{row.value_set.hash}</code>
            )}
          </>
        ) : (
          "Not stated"
        );
      default:
        return (
          <span className={variableStyles.atomic}>
            {String(row[key] ?? "Not stated")}
          </span>
        );
    }
  };
  return (
    <article className={styles.page}>
      <Definitions
        kind="variable"
        title={node.name}
        definitions={[
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
      {node.description ? (
        <Panel title="Description">
          <p className={styles.prose}>{node.description}</p>
        </Panel>
      ) : node.source_definition ? (
        <Panel title="Provider wording">
          <p className={styles.period}>
            {catalogPeriodLabel(node.source_definition.period)}
          </p>
          <p className={styles.prose}>{node.source_definition.text}</p>
        </Panel>
      ) : null}
      <Panel title="Tables">
        {node.tables.length ? (
          <ul className={styles.plainList}>
            {node.tables.map((fqid) => (
              <li key={fqid} className={styles.finding}>
                <a className="link" href={catalogHref(fqid)}>
                  <code>{fqid}</code>
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
          <DataTable
            columns={[
              { key: "table", label: "Table" },
              { key: "period", label: "Period", mono: true },
              { key: "spelling", label: "Spelling" },
              { key: "type", label: "Type", mono: true },
              { key: "format", label: "Format", mono: true },
              { key: "length", label: "Length", numeric: true },
              { key: "role", label: "Role" },
              { key: "coded", label: "Coded" },
              { key: "value_set", label: "Value set" },
              { key: "state", label: "State" },
              { key: "identifier", label: "Identifier" },
              { key: "sensitive", label: "Sensitive" },
              { key: "temporality", label: "Temporality" },
            ]}
            rows={segments.data}
            getRowId={(row) => JSON.stringify(row)}
            renderCell={(row, column) => segmentCell(row, column.key)}
          />
        ) : (
          <EmptyState title="No segments recorded for this variable." />
        )}
      </Panel>
      <ScopedDataWarnings fqid={node.fqid} />
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
                    <EvidenceContent evidence={evidence.data} />
                  ) : null)}
              </div>
            </DisclosurePanel>
          </>
        )}
      </Disclosure>
    </article>
  );
}
