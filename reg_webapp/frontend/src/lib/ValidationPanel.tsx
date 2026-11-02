import { Link } from "react-aria-components";
import type { ValidationResultModel } from "./api";
import { catalogHref } from "./catalog";
import { catalogPeriodLabel, sourcePeriodLabel } from "./catalog_period";
import { FindingTitle } from "./Finding";
import styles from "./Finding.module.css";
import { Panel } from "./ui/Panel";
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

export function ValidationPanel({
  title,
  validMessage,
  result,
  raw,
  loading = false,
  error,
  summarizeLocated = false,
}: {
  title: string;
  validMessage: string;
  result: ValidationResultModel | null;
  raw: string | null;
  loading?: boolean;
  error?: string | null;
  summarizeLocated?: boolean;
}) {
  return (
    <Panel title={title}>
      {loading ? (
        <div aria-label={title} aria-busy="true">
          <Skeleton />
        </div>
      ) : error ? (
        <StatusRow tone="error" live="alert">
          The catalog check failed. Check catalog to try again.
        </StatusRow>
      ) : (
        result && (
          <>
            <StatusRow
              tone={
                result.ok
                  ? result.issues.some((issue) => issue.level === "warning")
                    ? "warn"
                    : "ok"
                  : "error"
              }
              live="status"
            >
              {result.ok
                ? result.issues.some((issue) => issue.level === "warning")
                  ? "Project is valid, with warnings to review."
                  : validMessage
                : "Project needs changes. Review the issues below."}
            </StatusRow>
            <ul className={styles.list} aria-label={`${title} issues`}>
              {result.issues.map((issue) =>
                summarizeLocated && issuePlace(issue, raw).source ? (
                  <IssueLink key={issueId(issue)} issue={issue} raw={raw} />
                ) : (
                  <LocatedIssue key={issueId(issue)} issue={issue} raw={raw} />
                ),
              )}
            </ul>
          </>
        )
      )}
    </Panel>
  );
}

function IssueLink({
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
  return (
    <li>
      <Link
        className="link"
        href={`#${encodeURIComponent(issueId(issue))}`}
        onPress={() => document.getElementById(issueId(issue))?.focus()}
      >
        {place.source}
        {place.variable && variable.value ? `: ${variable.value}` : ""}:{" "}
        {plainIssueText(issueTitle(issue), refs, names, raw)}
      </Link>
    </li>
  );
}

export function LocatedIssue({
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
  const successor = useCatalogName(issue.successor_fqid);
  const period = issue.period
    ? catalogPeriodLabel(issue.period)
    : place.period === undefined
      ? null
      : sourcePeriodLabel(place.period);
  return (
    <li id={issueId(issue)} className={styles.item} tabIndex={-1}>
      <StatusRow tone={issue.level === "warning" ? "warn" : issue.level}>
        <div className={styles.body}>
          <FindingTitle
            title={plainIssueText(issueTitle(issue), refs, names, raw)}
            code={issue.code}
          />
          {place.source && <span>Source: {place.source}</span>}
          {place.panel && <span>{place.panel}</span>}
          {place.variable && !variable.loading && (
            <span>Variable: {variable.value ?? "Name unavailable"}</span>
          )}
          {place.table && !table.loading && (
            <span>Table: {table.value ?? "Name unavailable"}</span>
          )}
          {(variable.loading || table.loading) && (
            <div aria-label="Catalog names" aria-busy="true">
              <Skeleton />
            </div>
          )}
          {period && (
            <span>
              Period: {period === "Unbounded" ? period : <code>{period}</code>}
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
