import { useEffect, useRef, useState } from "react";
import {
  Button as AriaButton,
  Disclosure,
  DisclosurePanel,
  Heading,
  Tooltip,
  TooltipTrigger,
} from "react-aria-components";
import { type CatalogFinding, getFindings } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref } from "./catalog";
import { catalogPeriodLabel } from "./catalog_period";
import findingStyles from "./Finding.module.css";
import type { WarningGroup } from "./finding_groups";
import { Button } from "./ui/Button";
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

/** Catalog findings by group: each group's count, and its items once opened. */
export function DataWarnings({
  groups,
  names,
}: {
  groups: readonly WarningGroup[];
  names?: Names;
}) {
  if (groups.length === 0) return null;
  return (
    <Panel title="Notes on this data">
      {groups.map((group) => (
        <FindingGroup
          key={JSON.stringify([group.ref, group.severity, group.code])}
          group={group}
          names={names}
        />
      ))}
    </Panel>
  );
}

function FindingGroup({
  group,
  names,
}: {
  group: WarningGroup;
  names?: Names;
}) {
  const [copied, setCopied] = useState(false);
  const [more, setMore] = useState<{
    items: CatalogFinding[];
    cursor: string | null;
    dropped: number;
  }>({ items: [], cursor: group.next_cursor, dropped: 0 });
  const [loading, setLoading] = useState(false);
  const [failure, setFailure] = useState<string | null>(null);
  const moreButton = useRef<HTMLButtonElement>(null);
  // The first item of the page just loaded, focused once it renders, so Tab carries
  // on from where the reader is rather than from the top of the list.
  const focusAt = useRef<number | null>(null);
  const request = useRef<AbortController | null>(null);
  useEffect(() => () => request.current?.abort(), []);
  const { severity, code, ref } = group;
  const entries = [...group.items, ...more.items];
  const count = group.count - more.dropped;
  const showMore = () => {
    const cursor = more.cursor;
    if (loading || cursor === null || ref === null) return;
    setLoading(true);
    setFailure(null);
    const controller = new AbortController();
    request.current = controller;
    getFindings(ref, { code, cursor, signal: controller.signal }).then(
      (page) => {
        if (controller.signal.aborted) return;
        const next = page.find((g) => g.severity === severity);
        // Move focus only if the reader has not moved it elsewhere meanwhile.
        const active = document.activeElement;
        if (active === moreButton.current || active === document.body)
          focusAt.current = entries.length;
        // A merged group's first page may hold another ref's copy of a finding.
        setMore((m) => {
          const shown = new Set(
            [...group.items, ...m.items].map((f) => JSON.stringify(f)),
          );
          const fresh = (next?.items ?? []).filter(
            (f) => !shown.has(JSON.stringify(f)),
          );
          return {
            items: [...m.items, ...fresh],
            cursor: next?.next_cursor ?? null,
            dropped: m.dropped + (next?.items.length ?? 0) - fresh.length,
          };
        });
        setLoading(false);
      },
      (error: unknown) => {
        if (controller.signal.aborted) return;
        setFailure(error instanceof Error ? error.message : String(error));
        setLoading(false);
      },
    );
  };
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
              <AriaButton
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
                  <code>{count}</code>)
                </span>
              </AriaButton>
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
                <ul
                  aria-busy={loading || undefined}
                  className={styles.findings}
                >
                  {entries.map((finding, i) => (
                    <li
                      key={JSON.stringify(finding)}
                      tabIndex={-1}
                      ref={(el) => {
                        if (el && focusAt.current === i) {
                          focusAt.current = null;
                          el.focus();
                        }
                      }}
                    >
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
              {isExpanded && failure && (
                <StatusRow tone="error" live="alert">
                  More notes did not load ({failure}). Press Show more to try
                  again.
                </StatusRow>
              )}
              {isExpanded && group.partial && entries.length < count && (
                <p className={`${styles.shown} ${styles.findingsFoot}`}>
                  {entries.length} of {count} shown
                </p>
              )}
              {isExpanded && more.cursor !== null && ref !== null && (
                // Padded so the panel's clipping box leaves the focus ring whole.
                <div className={styles.findingsFoot}>
                  <Button
                    ref={moreButton}
                    isBusy={loading}
                    onPress={showMore}
                    aria-label={`Show more (${count - entries.length} left): ${label}`}
                  >
                    Show more ({count - entries.length} left)
                  </Button>
                </div>
              )}
            </div>
          </DisclosurePanel>
        </>
      )}
    </Disclosure>
  );
}
