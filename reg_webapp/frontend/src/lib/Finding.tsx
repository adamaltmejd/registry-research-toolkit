import { useState } from "react";
import {
  Button as AriaButton,
  Link,
  Tooltip,
  TooltipTrigger,
} from "react-aria-components";
import { catalogHref } from "./catalog";
import { catalogPeriodLabel, sourcePeriodLabel } from "./catalog_period";
import styles from "./Finding.module.css";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { useCatalogName, useCatalogNames } from "./use_catalog_names";
import {
  issueId,
  issuePlace,
  issueRefs,
  issueTitle,
  plainIssueText,
  type ValidationIssue,
} from "./validation";

// One project finding, wherever it renders: under the column lane or at the top
// of the table card it concerns, or under the project's verdict when it has no
// card (#1906). A notice, so a StatusRow in the finding's tone.

/** The finding's title. Its stable code, the identifier people cite, is in a
 * tooltip that opens on hover after a delay or at once on focus; pressing the
 * title copies the code. */
export function FindingTitle({
  title,
  code,
}: {
  title: string;
  code: string | null;
}) {
  const [copied, setCopied] = useState(false);
  if (code === null) {
    return <strong className={styles.title}>{title}</strong>;
  }
  return (
    <>
      {/* The tooltip opens on neither touch nor a screen reader's press, so the
        copy is confirmed here too. */}
      <span className="visually-hidden" aria-live="polite">
        {copied ? `Code ${code} copied` : ""}
      </span>
      {/* Kept open on press, so the tooltip can say "Copied"; it resets only
        once the tooltip closes. */}
      <TooltipTrigger
        delay={600}
        shouldCloseOnPress={false}
        onOpenChange={(isOpen) => {
          if (!isOpen) setCopied(false);
        }}
      >
        <AriaButton
          className={styles.titleButton}
          onPress={() => {
            // A refused clipboard leaves the code in the tooltip to select.
            void navigator.clipboard?.writeText(code).then(
              () => setCopied(true),
              () => {},
            );
          }}
        >
          {title}
        </AriaButton>
        <Tooltip
          className={({ isEntering, isExiting }) =>
            `${styles.tooltip} ${isEntering ? "motion-rise-in" : ""} ${isExiting ? "motion-rise-out" : ""}`
          }
          offset={6}
          style={{ zIndex: "var(--z-popover)" }}
        >
          <code className={styles.code}>{code}</code>
          <span className={styles.copyHint}>
            {copied ? "Copied" : "Press to copy"}
          </span>
        </Tooltip>
      </TooltipTrigger>
    </>
  );
}

/** A plain link to a finding that sits on a table card: it scrolls to the
 * finding and focuses it. */
export function IssueLink({
  issue,
  raw,
}: {
  issue: ValidationIssue;
  raw: string | null;
}) {
  const place = issuePlace(issue, raw);
  const refs = issueRefs(issue, raw);
  const names = useCatalogNames(refs);
  const variable = useCatalogName(place.variable);
  const table = useCatalogName(place.table);
  return (
    <li>
      <Link
        className="link"
        href={`#${encodeURIComponent(issueId(issue))}`}
        onPress={() => document.getElementById(issueId(issue))?.focus()}
      >
        {/* The table's catalog name, as on its card; nothing while it loads,
          rather than the join key flashing first. */}
        {[
          table.loading ? null : (table.value ?? place.source),
          place.variable ? variable.value : null,
        ]
          .filter(Boolean)
          .map((part) => `${part}: `)
          .join("")}
        {plainIssueText(issueTitle(issue), refs, names, raw)}
      </Link>
    </li>
  );
}

/** A finding in full, naming its place in the project and the catalog except
 * where the surrounding card already says it (`omitPlace`). */
export function LocatedIssue({
  issue,
  raw,
  omitPlace = [],
}: {
  issue: ValidationIssue;
  raw: string | null;
  omitPlace?: readonly ("variable" | "table")[];
}) {
  const place = issuePlace(issue, raw);
  const refs = issueRefs(issue, raw);
  const names = useCatalogNames(refs);
  const variable = useCatalogName(place.variable);
  const table = useCatalogName(place.table);
  const successor = useCatalogName(issue.successor_fqid);
  const period = issue.period
    ? catalogPeriodLabel(issue.period)
    : place.period === undefined
      ? null
      : sourcePeriodLabel(place.period);
  return (
    // A jump link lands here: a focus target, not a tab stop.
    <li id={issueId(issue)} className={styles.item} tabIndex={-1}>
      <StatusRow tone={issue.level === "warning" ? "warn" : issue.level}>
        <div className={styles.body}>
          <FindingTitle
            title={plainIssueText(issueTitle(issue), refs, names, raw)}
            code={issue.code}
          />
          {/* A project table is named by the catalog table it reads, as on
            its card; the project's own name for it is the file's join key and
            stands in only where the catalog has no name. */}
          {!omitPlace.includes("table") &&
            !table.loading &&
            (table.value || place.source) && (
              <span>Table: {table.value ?? place.source}</span>
            )}
          {place.panel && <span>{place.panel}</span>}
          {place.variable &&
            !omitPlace.includes("variable") &&
            !variable.loading && (
              <span>Variable: {variable.value ?? "Name unavailable"}</span>
            )}
          {((variable.loading && !omitPlace.includes("variable")) ||
            (table.loading && !omitPlace.includes("table"))) && (
            <div aria-label="Catalog names" aria-busy="true">
              <Skeleton />
            </div>
          )}
          {period && (
            <span>
              Years: {period === "Unbounded" ? period : <code>{period}</code>}
            </span>
          )}
          <span className={styles.message}>
            {plainIssueText(issue.message, refs, names, raw)}
          </span>
          {issue.successor_fqid &&
            (successor.loading ? (
              <div aria-label="Successor name" aria-busy="true">
                <Skeleton />
              </div>
            ) : (
              <a className="link" href={catalogHref(issue.successor_fqid)}>
                Successor: {successor.value ?? "Name unavailable"}
              </a>
            ))}
        </div>
      </StatusRow>
    </li>
  );
}
