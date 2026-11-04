import { useState } from "react";
import {
  Button,
  Disclosure,
  DisclosurePanel,
  Heading,
  Tooltip,
  TooltipTrigger,
} from "react-aria-components";
import type { CatalogFinding } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref } from "./catalog";
import { catalogPeriodLabel } from "./catalog_period";
import findingStyles from "./Finding.module.css";
import { Panel } from "./ui/Panel";
import { StatusRow } from "./ui/StatusRow";
import { TreeChevron } from "./ui/TreeChevron";
import { plainIssueText } from "./validation";

type Names = ReadonlyMap<string, string>;

function SubjectLink({ fqid, names }: { fqid: string; names?: Names }) {
  return (
    <a className="link" href={catalogHref(fqid)}>
      {names?.get(fqid) ?? <code>{fqid}</code>}
    </a>
  );
}

/** Catalog findings, grouped before any entries are mounted. */
export function DataWarnings({
  findings,
  names,
}: {
  findings: readonly CatalogFinding[];
  names?: Names;
}) {
  if (findings.length === 0) return null;
  const groups = new Map<string, CatalogFinding[]>();
  for (const finding of findings) {
    const key = JSON.stringify([finding.severity, finding.code]);
    const group = groups.get(key);
    if (group) group.push(finding);
    else groups.set(key, [finding]);
  }
  return (
    <Panel title="Notes on this data">
      {[...groups]
        .sort(
          ([, a], [, b]) =>
            Number(b[0].severity === "review") -
              Number(a[0].severity === "review") ||
            a[0].code.localeCompare(b[0].code),
        )
        .map(([key, entries]) => (
          <FindingGroup key={key} entries={entries} names={names} />
        ))}
    </Panel>
  );
}

function FindingGroup({
  entries,
  names,
}: {
  entries: readonly CatalogFinding[];
  names?: Names;
}) {
  const [copied, setCopied] = useState(false);
  const { severity, code } = entries[0];
  const label = code.includes(":")
    ? code.slice(code.indexOf(":") + 1).replaceAll("-", " ")
    : "catalog finding";
  return (
    <Disclosure className={styles.disclosure}>
      {({ isExpanded }) => (
        <>
          <Heading level={3} className={styles.disclosureHeading}>
            <TooltipTrigger
              delay={600}
              shouldCloseOnPress={false}
              onOpenChange={(open) => {
                if (!open) setCopied(false);
              }}
            >
              <Button
                slot="trigger"
                className={styles.disclosureButton}
                onPress={() => {
                  void navigator.clipboard?.writeText(code).then(
                    () => setCopied(true),
                    () => {},
                  );
                }}
              >
                <TreeChevron />
                <span>
                  {severity === "review" ? "Needs review" : "Notice"}: {label} (
                  <code>{entries.length}</code>)
                </span>
              </Button>
              <Tooltip
                className={({ isEntering, isExiting }) =>
                  `${findingStyles.tooltip} ${isEntering ? "motion-rise-in" : ""} ${isExiting ? "motion-rise-out" : ""}`
                }
                offset={6}
                style={{ zIndex: "var(--z-popover)" }}
              >
                <code>{code}</code>
                <span>{copied ? "Copied" : "Press title to copy"}</span>
              </Tooltip>
            </TooltipTrigger>
          </Heading>
          <span className="visually-hidden" aria-live="polite">
            {copied ? `Code ${code} copied` : ""}
          </span>
          <DisclosurePanel
            className={`motion-disclose ${isExpanded ? "open" : ""}`}
          >
            <div>
              {isExpanded && (
                <ul className={styles.findings}>
                  {entries.map((finding) => (
                    <li key={JSON.stringify(finding)}>
                      <StatusRow tone={severity === "review" ? "warn" : "info"}>
                        <div className={styles.finding}>
                          <span>
                            {plainIssueText(
                              finding.message,
                              [],
                              names ?? new Map(),
                            )}
                          </span>
                          {finding.period && (
                            <span>
                              Years:{" "}
                              <code>{catalogPeriodLabel(finding.period)}</code>
                            </span>
                          )}
                          {finding.subject.fqid && (
                            <SubjectLink
                              fqid={finding.subject.fqid}
                              names={names}
                            />
                          )}
                          {finding.subject.table &&
                            finding.subject.table !== finding.subject.fqid && (
                              <span>
                                Table:{" "}
                                <SubjectLink
                                  fqid={finding.subject.table}
                                  names={names}
                                />
                              </span>
                            )}
                          {finding.subject.release && (
                            <span>Release: {finding.subject.release}</span>
                          )}
                          {finding.caveat && <span>{finding.caveat}</span>}
                        </div>
                      </StatusRow>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </DisclosurePanel>
        </>
      )}
    </Disclosure>
  );
}
