import {
  Button,
  Disclosure,
  DisclosurePanel,
  Heading,
} from "react-aria-components";
import type { VariableEvidence } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref } from "./catalog";
import { catalogPeriodLabel } from "./catalog_period";
import { EmptyState } from "./ui/EmptyState";
import { KeyValue } from "./ui/KeyValue";
import { TreeChevron } from "./ui/TreeChevron";

type Source = VariableEvidence["source_only"][number];

function EvidenceSource({ source }: { source: Source }) {
  return (
    <li className={styles.finding}>
      <a className="link" href={catalogHref(source.table)}>
        <code>{source.table}</code>
      </a>
      <KeyValue
        rows={[
          { label: "Label", value: source.label ?? "Not stated" },
          { label: "Unit", value: source.unit ?? "Not stated" },
          {
            label: "Period",
            value: catalogPeriodLabel(source.period),
            mono: true,
          },
          { label: "Status", value: source.status },
          { label: "Source key", value: source.source_key, mono: true },
          {
            label: "Spelling",
            value: source.spelling ?? "Not stated",
            mono: true,
          },
          ...(source.field
            ? [{ label: "Field", value: source.field, mono: true }]
            : []),
        ]}
      />
      {source.locators.length > 0 && (
        <Disclosure className={styles.disclosure}>
          {({ isExpanded }) => (
            <>
              <Heading level={4} className={styles.disclosureHeading}>
                <Button slot="trigger" className={styles.disclosureButton}>
                  <TreeChevron />
                  <span>Source locators ({source.locators.length})</span>
                </Button>
              </Heading>
              <DisclosurePanel
                className={`motion-disclose ${isExpanded ? "open" : ""}`}
              >
                <div>
                  {isExpanded && (
                    <ul className={styles.plainList}>
                      {source.locators.map((locator) => (
                        <li key={JSON.stringify(locator)}>
                          <code>
                            {locator.input}: {locator.locator}
                          </code>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              </DisclosurePanel>
            </>
          )}
        </Disclosure>
      )}
    </li>
  );
}

/** Evidence stays distinct from the curated definition above the structural data. */
export function EvidenceContent({ evidence }: { evidence: VariableEvidence }) {
  const empty = Object.values(evidence).every((items) => items.length === 0);
  if (empty)
    return (
      <EmptyState title="No source or provenance recorded for this variable." />
    );
  return (
    <div className={styles.page}>
      {evidence.texts.map((text) => (
        <Disclosure key={text.text} className={styles.disclosure}>
          {({ isExpanded }) => (
            <>
              <Heading level={3} className={styles.disclosureHeading}>
                <Button slot="trigger" className={styles.disclosureButton}>
                  <TreeChevron />
                  <span>
                    Provider wording:{" "}
                    <code>
                      {text.periods.map(catalogPeriodLabel).join(", ")}
                    </code>
                  </span>
                </Button>
              </Heading>
              <DisclosurePanel
                className={`motion-disclose ${isExpanded ? "open" : ""}`}
              >
                <div>
                  {isExpanded && (
                    <>
                      <p className={styles.prose}>{text.text}</p>
                      <ul className={styles.findings}>
                        {text.sources.map((source) => (
                          <EvidenceSource
                            key={JSON.stringify(source)}
                            source={source}
                          />
                        ))}
                      </ul>
                    </>
                  )}
                </div>
              </DisclosurePanel>
            </>
          )}
        </Disclosure>
      ))}
      {evidence.spellings.length > 0 && (
        <section>
          <h3>Spellings</h3>
          <KeyValue
            rows={evidence.spellings.map((spelling) => ({
              label: spelling.kind,
              value: spelling.spelling,
              mono: true,
            }))}
          />
        </section>
      )}
      {evidence.statements.length > 0 && (
        <section>
          <h3>Curation statements</h3>
          <ul className={styles.findings}>
            {evidence.statements.map((statement) => (
              <li key={JSON.stringify(statement)}>
                <KeyValue
                  rows={[
                    { label: "Field", value: statement.field, mono: true },
                    { label: "Value", value: statement.value ?? "Not stated" },
                    {
                      label: "Statement",
                      value: `${statement.statement.file}:${statement.statement.line}`,
                      mono: true,
                    },
                    ...(statement.evidence
                      ? [{ label: "Evidence", value: statement.evidence }]
                      : []),
                    ...(statement.caveat
                      ? [{ label: "Caveat", value: statement.caveat }]
                      : []),
                  ]}
                />
              </li>
            ))}
          </ul>
        </section>
      )}
      {evidence.documents.length > 0 && (
        <section>
          <h3>Documents</h3>
          <ul className={styles.findings}>
            {evidence.documents.map((document) => (
              <li key={JSON.stringify(document)}>
                <span>
                  {document.url ? (
                    <a className="link" href={document.url}>
                      {document.title}
                    </a>
                  ) : (
                    document.title
                  )}
                </span>
                <KeyValue
                  rows={[
                    { label: "Document", value: document.doc, mono: true },
                    { label: "Kind", value: document.kind },
                    {
                      label: "Page",
                      value:
                        document.page === 0
                          ? "Whole document"
                          : String(document.page),
                      mono: document.page !== 0,
                    },
                    { label: "Licence", value: document.licence },
                  ]}
                />
              </li>
            ))}
          </ul>
        </section>
      )}
      {evidence.source_only.length > 0 && (
        <section>
          <h3>Sources without wording</h3>
          <ul className={styles.findings}>
            {evidence.source_only.map((source) => (
              <EvidenceSource key={JSON.stringify(source)} source={source} />
            ))}
          </ul>
        </section>
      )}
    </div>
  );
}
