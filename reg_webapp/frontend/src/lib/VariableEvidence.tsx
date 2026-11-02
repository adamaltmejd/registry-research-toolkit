import {
  Button,
  Disclosure,
  DisclosurePanel,
  Heading,
} from "react-aria-components";
import type { CatalogPeriod, VariableEvidence, VariableShow } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref } from "./catalog";
import { catalogWords, yesNo } from "./catalog_labels";
import { catalogPeriodLabel } from "./catalog_period";
import { ColumnChip } from "./ui/ColumnChip";
import { EmptyState } from "./ui/EmptyState";
import { KeyValue } from "./ui/KeyValue";
import { TreeChevron } from "./ui/TreeChevron";
import evidenceStyles from "./VariableEvidence.module.css";

type Source = VariableEvidence["source_only"][number];
type Statement = VariableEvidence["statements"][number];
type SourceGroup = Omit<Source, "period"> & { periods: CatalogPeriod[] };
type StatementGroup = Omit<Statement, "statement"> & {
  locations: Statement["statement"][];
};
type Names = ReadonlyMap<string, string>;

/** Catalog periods are half-open: touching bounds join, but gaps remain visible. */
function mergedPeriods(periods: CatalogPeriod[]): CatalogPeriod[] {
  const runs: CatalogPeriod[] = [];
  for (const period of [...periods].sort((a, b) =>
    a.start === b.start
      ? 0
      : a.start === null
        ? -1
        : b.start === null
          ? 1
          : a.start < b.start
            ? -1
            : 1,
  )) {
    const previous = runs.at(-1);
    if (
      !previous ||
      (previous.end !== null &&
        period.start !== null &&
        period.start > previous.end)
    ) {
      runs.push({ ...period });
    } else {
      previous.end =
        previous.end === null || period.end === null
          ? null
          : previous.end > period.end
            ? previous.end
            : period.end;
    }
  }
  return runs;
}

function periodLabels(periods: CatalogPeriod[]): string {
  return mergedPeriods(periods).map(catalogPeriodLabel).join(", ");
}

function groupSources(sources: Source[]): SourceGroup[] {
  const groups = new Map<string, SourceGroup>();
  for (const { period, locators, ...facts } of sources) {
    const key = JSON.stringify([
      facts.table,
      facts.label,
      facts.unit,
      facts.status,
      facts.source_key,
      facts.spelling,
      facts.field ?? null,
    ]);
    const group = groups.get(key);
    if (group) {
      group.periods.push(period);
      group.locators.push(...locators);
    } else {
      groups.set(key, { ...facts, periods: [period], locators: [...locators] });
    }
  }
  for (const group of groups.values()) {
    group.locators = [
      ...new Map(
        group.locators.map((locator) => [JSON.stringify(locator), locator]),
      ).values(),
    ];
  }
  return [...groups.values()];
}

function groupStatements(statements: Statement[]): StatementGroup[] {
  const groups = new Map<string, StatementGroup>();
  for (const { statement, ...facts } of statements) {
    const key = JSON.stringify([
      facts.field,
      facts.value,
      facts.evidence,
      facts.caveat,
    ]);
    const group = groups.get(key);
    if (group) group.locations.push(statement);
    else groups.set(key, { ...facts, locations: [statement] });
  }
  for (const group of groups.values()) {
    group.locations = [
      ...new Map(
        group.locations.map((location) => [JSON.stringify(location), location]),
      ).values(),
    ];
  }
  return [...groups.values()];
}

function visibleText(value: string | null): string | null {
  // Internal text references may follow useful prose in the same evidence field.
  return (
    value
      ?.replace(/\bcatalog:[^\s]+/g, "")
      .replace(/\n{3,}/g, "\n\n")
      .trim() || null
  );
}

function statementValue(
  statement: StatementGroup,
  node: VariableShow,
): string | null {
  if (statement.value === null) return "Not stated";
  let value: unknown = statement.value;
  try {
    value = JSON.parse(statement.value);
  } catch {
    // A statement may contain plain text rather than a JSON value.
  }
  if (Array.isArray(value)) {
    if (value.length === 0) return "None";
    const members =
      statement.field === "tags"
        ? node.tags
        : statement.field === "sets"
          ? node.sets
          : [];
    return (
      value
        .map(
          (item) =>
            members.find(
              (member) =>
                member.fqid === item || member.fqid.split("/").at(-1) === item,
            )?.name,
        )
        .filter(Boolean)
        .join(", ") || "Not stated"
    );
  }
  if (typeof value === "boolean") return yesNo(value);
  if (typeof value === "number") return String(value);
  if (typeof value !== "string") return "Not stated";
  if (statement.field === "variable") return node.name;
  if (statement.field === "classification" && value.startsWith("class/"))
    return "Assigned";
  if (value.includes("catalog:"))
    return (
      visibleText(value) ??
      (statement.field === "description" ? (node.description ?? null) : null)
    );
  return [
    "role",
    "coded",
    "state",
    "identifier",
    "sensitive",
    "temporality",
  ].includes(statement.field)
    ? catalogWords(value)
    : visibleText(value);
}

function EvidenceSource({
  source,
  names,
}: {
  source: SourceGroup;
  names: Names;
}) {
  const sourceKey = visibleText(source.source_key);
  return (
    <li className={evidenceStyles.source}>
      <a className="link" href={catalogHref(source.table)}>
        {names.get(source.table) ?? "Table"}
      </a>
      <KeyValue
        rows={[
          { label: "Label", value: source.label ?? "Not stated" },
          { label: "Unit", value: source.unit ?? "Not stated" },
          { label: "Period", value: periodLabels(source.periods), mono: true },
          { label: "Status", value: catalogWords(source.status) },
          ...(sourceKey
            ? [{ label: "Source key", value: sourceKey, mono: true }]
            : []),
          {
            label: "Spelling",
            value: source.spelling ? (
              <ColumnChip name={source.spelling.toUpperCase()} />
            ) : (
              "Not stated"
            ),
          },
          ...(source.field
            ? [{ label: "Field", value: catalogWords(source.field) }]
            : []),
        ]}
      />
      {source.locators.length > 0 && (
        <Disclosure className={styles.disclosure}>
          {({ isExpanded }) => (
            <>
              <Heading
                level={4}
                className={`${styles.disclosureHeading} ${evidenceStyles.subheading}`}
              >
                <Button slot="trigger" className={styles.disclosureButton}>
                  <TreeChevron />
                  <span>Source locators</span>
                </Button>
              </Heading>
              <DisclosurePanel
                className={`motion-disclose ${isExpanded ? "open" : ""}`}
              >
                <div>
                  {isExpanded && (
                    <ul
                      className={`${styles.plainList} ${evidenceStyles.list}`}
                    >
                      {source.locators.map((locator) => (
                        <li key={JSON.stringify(locator)}>
                          {visibleText(locator.input) && (
                            <code>{locator.input}: </code>
                          )}
                          {visibleText(locator.locator) ? (
                            <code>{locator.locator}</code>
                          ) : (
                            "Source record"
                          )}
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

/** Provider evidence stays distinct from the curated description above it. */
export function EvidenceContent({
  evidence,
  node,
  names,
}: {
  evidence: VariableEvidence;
  node: VariableShow;
  names: Names;
}) {
  const empty = Object.values(evidence).every((items) => items.length === 0);
  if (empty)
    return (
      <EmptyState title="No source or provenance recorded for this variable." />
    );
  const spellings = new Map<string, string[]>();
  for (const spelling of evidence.spellings) {
    const kind = catalogWords(spelling.kind);
    spellings.set(kind, [
      ...(spellings.get(kind) ?? []),
      spelling.spelling.toUpperCase(),
    ]);
  }
  return (
    <div className={evidenceStyles.content}>
      {evidence.texts.map((text) => (
        <Disclosure key={text.text} className={styles.disclosure}>
          {({ isExpanded }) => (
            <>
              <Heading
                level={3}
                className={`${styles.disclosureHeading} ${evidenceStyles.subheading}`}
              >
                <Button slot="trigger" className={styles.disclosureButton}>
                  <TreeChevron />
                  <span>
                    Provider wording: <code>{periodLabels(text.periods)}</code>
                  </span>
                </Button>
              </Heading>
              <DisclosurePanel
                className={`motion-disclose ${isExpanded ? "open" : ""}`}
              >
                <div>
                  {isExpanded && (
                    <div className={evidenceStyles.content}>
                      <p className={styles.prose}>{text.text}</p>
                      <ul
                        className={`${styles.plainList} ${evidenceStyles.list}`}
                      >
                        {groupSources(text.sources).map((source) => (
                          <EvidenceSource
                            key={JSON.stringify(source)}
                            source={source}
                            names={names}
                          />
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
              </DisclosurePanel>
            </>
          )}
        </Disclosure>
      ))}
      {spellings.size > 0 && (
        <section className={evidenceStyles.section}>
          <h3
            className={`${styles.disclosureHeading} ${evidenceStyles.subheading}`}
          >
            Spellings
          </h3>
          <KeyValue
            rows={[...spellings].map(([kind, values]) => ({
              label: kind,
              value: (
                <span className={evidenceStyles.chips}>
                  {[...new Set(values)].map((spelling) => (
                    <ColumnChip key={spelling} name={spelling} />
                  ))}
                </span>
              ),
            }))}
          />
        </section>
      )}
      {evidence.statements.length > 0 && (
        <section className={evidenceStyles.section}>
          <h3
            className={`${styles.disclosureHeading} ${evidenceStyles.subheading}`}
          >
            Curation statements
          </h3>
          <ul className={`${styles.plainList} ${evidenceStyles.list}`}>
            {groupStatements(evidence.statements).map((statement) => {
              const value = statementValue(statement, node);
              const evidenceText = visibleText(statement.evidence);
              const caveatText = visibleText(statement.caveat);
              return (
                <li
                  key={JSON.stringify(statement)}
                  className={evidenceStyles.source}
                >
                  <KeyValue
                    rows={[
                      { label: "Field", value: catalogWords(statement.field) },
                      ...(value !== null ? [{ label: "Value", value }] : []),
                      ...(evidenceText
                        ? [
                            {
                              label: "Evidence",
                              value: evidenceText,
                            },
                          ]
                        : []),
                      ...(caveatText
                        ? [
                            {
                              label: "Caveat",
                              value: caveatText,
                            },
                          ]
                        : []),
                    ]}
                  />
                  <Disclosure className={styles.disclosure}>
                    {({ isExpanded }) => (
                      <>
                        <Heading
                          level={4}
                          className={`${styles.disclosureHeading} ${evidenceStyles.subheading}`}
                        >
                          <Button
                            slot="trigger"
                            className={styles.disclosureButton}
                          >
                            <TreeChevron />
                            <span>Statement locators</span>
                          </Button>
                        </Heading>
                        <DisclosurePanel
                          className={`motion-disclose ${isExpanded ? "open" : ""}`}
                        >
                          <div>
                            {isExpanded && (
                              <ul
                                className={`${styles.plainList} ${evidenceStyles.list}`}
                              >
                                {statement.locations.map((location) => (
                                  <li key={JSON.stringify(location)}>
                                    <code>
                                      {location.file}:{location.line}
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
                </li>
              );
            })}
          </ul>
        </section>
      )}
      {evidence.documents.length > 0 && (
        <section className={evidenceStyles.section}>
          <h3
            className={`${styles.disclosureHeading} ${evidenceStyles.subheading}`}
          >
            Documents
          </h3>
          <ul className={`${styles.plainList} ${evidenceStyles.list}`}>
            {evidence.documents.map((document) => (
              <li
                key={JSON.stringify(document)}
                className={evidenceStyles.source}
              >
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
                    { label: "Kind", value: catalogWords(document.kind) },
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
        <section className={evidenceStyles.section}>
          <h3
            className={`${styles.disclosureHeading} ${evidenceStyles.subheading}`}
          >
            Sources without wording
          </h3>
          <ul className={`${styles.plainList} ${evidenceStyles.list}`}>
            {groupSources(evidence.source_only).map((source) => (
              <EvidenceSource
                key={JSON.stringify(source)}
                source={source}
                names={names}
              />
            ))}
          </ul>
        </section>
      )}
    </div>
  );
}
