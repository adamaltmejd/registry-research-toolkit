import {
  Button,
  Disclosure,
  DisclosurePanel,
  Heading,
} from "react-aria-components";
import type { CatalogFinding } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref } from "./catalog";
import { catalogPeriodLabel } from "./catalog_period";
import { Panel } from "./ui/Panel";
import { StatusRow } from "./ui/StatusRow";
import { TreeChevron } from "./ui/TreeChevron";

/** Catalog findings, grouped before any entries are mounted. */
export function DataWarnings({
  findings,
}: {
  findings: readonly CatalogFinding[];
}) {
  const severities = new Map<string, number>();
  const groups = new Map<string, CatalogFinding[]>();
  for (const finding of findings) {
    severities.set(
      finding.severity,
      (severities.get(finding.severity) ?? 0) + 1,
    );
    const key = JSON.stringify([finding.severity, finding.code]);
    const group = groups.get(key);
    if (group) group.push(finding);
    else groups.set(key, [finding]);
  }
  return (
    <Panel title="Findings">
      {findings.length === 0 ? (
        <StatusRow tone="info">
          No findings recorded for this subject.
        </StatusRow>
      ) : (
        <>
          <ul className={styles.severities} aria-label="Findings by severity">
            {[...severities]
              .sort(([a], [b]) => a.localeCompare(b))
              .map(([severity, count]) => (
                <li key={severity}>
                  {severity}: <code>{count}</code>
                </li>
              ))}
          </ul>
          {[...groups]
            .sort(([a], [b]) => a.localeCompare(b))
            .map(([key, entries]) => {
              const { severity, code } = entries[0];
              return (
                <Disclosure key={key} className={styles.disclosure}>
                  {({ isExpanded }) => (
                    <>
                      <Heading level={3} className={styles.disclosureHeading}>
                        <Button
                          slot="trigger"
                          className={styles.disclosureButton}
                        >
                          <TreeChevron />
                          <span>
                            {severity}: <code>{code}</code> (
                            <code>{entries.length}</code>)
                          </span>
                        </Button>
                      </Heading>
                      <DisclosurePanel>
                        {isExpanded && (
                          <ul className={styles.findings}>
                            {entries.map((finding) => (
                              <li key={JSON.stringify(finding)}>
                                <StatusRow
                                  tone={severity === "review" ? "warn" : "info"}
                                >
                                  <div className={styles.finding}>
                                    <span>{finding.message}</span>
                                    <span>
                                      Period:{" "}
                                      {finding.period ? (
                                        <code>
                                          {catalogPeriodLabel(finding.period)}
                                        </code>
                                      ) : (
                                        "Not stated"
                                      )}
                                    </span>
                                    {finding.subject.fqid && (
                                      <a
                                        className="link"
                                        href={catalogHref(finding.subject.fqid)}
                                      >
                                        <code>{finding.subject.fqid}</code>
                                      </a>
                                    )}
                                    {finding.subject.table &&
                                      finding.subject.table !==
                                        finding.subject.fqid && (
                                        <span>
                                          Table:{" "}
                                          <a
                                            className="link"
                                            href={catalogHref(
                                              finding.subject.table,
                                            )}
                                          >
                                            <code>{finding.subject.table}</code>
                                          </a>
                                        </span>
                                      )}
                                    {finding.subject.release && (
                                      <span>
                                        Release:{" "}
                                        <code>{finding.subject.release}</code>
                                      </span>
                                    )}
                                    {finding.caveat && (
                                      <span>{finding.caveat}</span>
                                    )}
                                  </div>
                                </StatusRow>
                              </li>
                            ))}
                          </ul>
                        )}
                      </DisclosurePanel>
                    </>
                  )}
                </Disclosure>
              );
            })}
        </>
      )}
    </Panel>
  );
}
