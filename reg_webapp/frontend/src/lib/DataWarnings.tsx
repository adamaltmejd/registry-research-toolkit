import { useRef, useState } from "react";
import {
  Button as AriaButton,
  Disclosure,
  DisclosurePanel,
  Heading,
  Tooltip,
  TooltipTrigger,
} from "react-aria-components";
import {
  type CatalogFinding,
  type CatalogFindingGroup,
  getFindings,
} from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref } from "./catalog";
import { catalogPeriodLabel } from "./catalog_period";
import findingStyles from "./Finding.module.css";
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

/** A group of findings and the ref whose `next_cursor` it carries (`null`: no
 * further page). */
export type WarningGroup = CatalogFindingGroup & { ref: string | null };

const groupKey = (group: { severity: string; code: string }) =>
  JSON.stringify([group.severity, group.code]);

/** The server's group order: review before notice, then by code. */
const byGroup = (a: WarningGroup, b: WarningGroup) =>
  Number(a.severity !== "review") - Number(b.severity !== "review") ||
  (a.code < b.code ? -1 : a.code > b.code ? 1 : 0);

/** Group lists from several refs merged by severity and code. Items are
 * de-duplicated, and the count drops each duplicate seen: exact once every ref's group
 * is fully loaded, otherwise an upper bound (a shared finding not yet loaded counts
 * once per ref). A group pages on only when a single ref has more, since one cursor
 * continues one ref. */
export function mergeFindingGroups(
  sources: readonly {
    ref: string;
    groups: readonly CatalogFindingGroup[];
  }[],
): WarningGroup[] {
  const merged = new Map<
    string,
    { group: WarningGroup; seen: Set<string>; paging: number }
  >();
  for (const { ref, groups } of sources) {
    for (const group of groups) {
      const key = groupKey(group);
      const entry = merged.get(key) ?? {
        group: { ...group, count: 0, items: [], next_cursor: null, ref: null },
        seen: new Set<string>(),
        paging: 0,
      };
      merged.set(key, entry);
      for (const item of group.items) {
        const id = JSON.stringify(item);
        if (entry.seen.has(id)) {
          entry.group.count -= 1;
          continue;
        }
        entry.seen.add(id);
        entry.group.items.push(item);
      }
      entry.group.count += group.count;
      if (group.next_cursor !== null) {
        entry.paging += 1;
        entry.group.next_cursor = group.next_cursor;
        entry.group.ref = ref;
      }
    }
  }
  return [...merged.values()]
    .map(({ group, paging }) =>
      paging > 1 ? { ...group, next_cursor: null, ref: null } : group,
    )
    .sort(byGroup);
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
  const list = useRef<HTMLUListElement>(null);
  const moreButton = useRef<HTMLButtonElement>(null);
  const { severity, code, ref } = group;
  const entries = [...group.items, ...more.items];
  const count = group.count - more.dropped;
  const showMore = () => {
    const cursor = more.cursor;
    if (loading || cursor === null || ref === null) return;
    setLoading(true);
    setFailure(null);
    getFindings(ref, { code, cursor }).then(
      (page) => {
        const next = page.find((g) => g.severity === severity);
        // The last page removes its button. Move focus only if the reader is still on it.
        if (
          !next?.next_cursor &&
          document.activeElement === moreButton.current
        )
          list.current?.focus({ preventScroll: true });
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
                  ref={list}
                  tabIndex={-1}
                  aria-busy={loading || undefined}
                  className={styles.findings}
                >
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
              {isExpanded && failure && (
                <StatusRow tone="error" live="alert">
                  More notes did not load ({failure}). Press Show more to try
                  again.
                </StatusRow>
              )}
              {isExpanded && more.cursor !== null && ref !== null && (
                <Button ref={moreButton} isBusy={loading} onPress={showMore}>
                  Show more
                </Button>
              )}
            </div>
          </DisclosurePanel>
        </>
      )}
    </Disclosure>
  );
}
