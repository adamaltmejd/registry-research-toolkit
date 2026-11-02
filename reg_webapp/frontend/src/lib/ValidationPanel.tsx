import {
  Button as AriaButton,
  Link,
  Tooltip,
  TooltipTrigger,
} from "react-aria-components";
import type { ValidationResultModel } from "./api";
import { catalogHref } from "./catalog";
import { catalogPeriodLabel } from "./catalog_period";
import styles from "./Finding.module.css";
import { sourcePeriodToWire } from "./reg_core";
import { Panel } from "./ui/Panel";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { useCatalogNames } from "./use_catalog_names";
import {
  issueId,
  issuePlace,
  issueTitle,
  plainIssueText,
  type ValidationIssue,
} from "./validation";

export function ValidationPanel({
  title,
  result,
  raw,
  loading = false,
  error,
  summarizeLocated = false,
}: {
  title: string;
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
          The catalog check failed: {error}. Check catalog to try again.
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
                ? title === "Draft check"
                  ? "Project structure is valid."
                  : "Project is valid against this catalog."
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
  const refs = [place.variable, issue.table, issue.successor_fqid];
  const names = useCatalogNames(
    refs.filter((ref): ref is string => Boolean(ref)),
  );
  return (
    <li>
      <Link
        className="link"
        href={`#${encodeURIComponent(issueId(issue))}`}
        onPress={() => document.getElementById(issueId(issue))?.focus()}
      >
        {place.source}
        {place.variable ? `: ${names.get(place.variable) ?? "Variable"}` : ""} —{" "}
        {plainIssueText(issueTitle(issue), refs, names)}
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
  const names = useCatalogNames(
    [place.variable, issue.table, issue.successor_fqid].filter(
      (ref): ref is string => Boolean(ref),
    ),
  );
  const plain = (text: string) =>
    plainIssueText(
      text,
      [place.variable, issue.variable, issue.table, issue.successor_fqid],
      names,
    );
  const title = plain(issueTitle(issue));
  const period = issue.period
    ? catalogPeriodLabel(issue.period)
    : place.period === undefined
      ? null
      : sourcePeriodToWire(place.period);
  return (
    <li id={issueId(issue)} className={styles.item} tabIndex={-1}>
      <StatusRow tone={issue.level === "warning" ? "warn" : issue.level}>
        <span className={styles.body}>
          <TooltipTrigger delay={600}>
            <AriaButton
              className={styles.titleButton}
              onPress={() => {
                void navigator.clipboard?.writeText(issue.code).catch(() => {});
              }}
            >
              {title}
            </AriaButton>
            <Tooltip className={styles.tooltip} offset={6}>
              <code>{issue.code}</code>
            </Tooltip>
          </TooltipTrigger>
          {place.source && <span>Source: {place.source}</span>}
          {place.variable && (
            <span>
              Variable: {names.get(place.variable) ?? "Unknown variable"}
            </span>
          )}
          {period && <span>Period: {period}</span>}
          {issue.path && (
            <span>
              Field: <code>{issue.path}</code>
            </span>
          )}
          {title !== plain(issue.message) && (
            <span>{plain(issue.message)}</span>
          )}
          {issue.successor_fqid && (
            <a className="link" href={catalogHref(issue.successor_fqid)}>
              Successor: {names.get(issue.successor_fqid) ?? "View successor"}
            </a>
          )}
        </span>
      </StatusRow>
    </li>
  );
}
