import { Button as AriaButton } from "react-aria-components";
import type { OrderBlocking, ValidationResultModel } from "./api";
import type { SafeSource } from "./project_data";
import type { RequestErrorSource, ValidationStatus } from "./project_store";
import { useRevealFindings } from "./reveal_findings";
import { Button } from "./ui/Button";
import { StatusRow } from "./ui/StatusRow";
import styles from "./ValidationPanel.module.css";
import {
  codeLabel,
  type FindingLocation,
  findingLocation,
  orderFindingPointer,
  type ValidationIssue,
  type WindowDisjointFinding,
} from "./validation";

// The validation echo (see crates/reg-core/DESIGN.md → Structural rules and issue
// codes). Renders the `/validate` result's issue list grouped by level (error /
// warning / info), the ok/not-ok summary, and — distinct from a 200 `ok:false`
// issue list — a malformed-REQUEST banner (a true 4xx ApiError).
//
// Each finding LEADS with the human title (`codeLabel`), DEMOTES the raw `code` to
// a small muted chip (still the stable identifier people cite), and — instead of
// leaking the raw JSON pointer — renders a click-to-LOCATE label ("Source
// 'lisa_main', column scb/lisa/adeldag", via `findingLocation`) that scrolls to
// and briefly flashes the relevant source/binding card. `sources` is the draft's
// (possibly malformed) source list, used only to resolve those labels.

interface Props {
  result: ValidationResultModel | null;
  status: ValidationStatus;
  requestError: string | null;
  /** WHICH request `requestError` came from — the discriminator the retry
   * affordance is gated on. */
  requestErrorSource: RequestErrorSource | null;
  /** A blocked order's findings (`/order`'s 422), rendered in the SAME
   * per-finding shape as a validation issue — they are findings, not a message. */
  orderFindings: readonly OrderBlocking[];
  /** Sources the common study window has left with no years inside it — the SPA's
   * own blocking authoring finding (`windowDisjointFindings`), rendered like a
   * blocked order's findings because it blocks the order the same way. */
  windowFindings: readonly WindowDisjointFinding[];
  sources: readonly SafeSource[];
  /** Re-runs `/validate`. Offered ONLY for a failed validation REQUEST. */
  onRetry: () => void;
  /** Re-POSTs `/order`. Offered ONLY for an order request the backend never
   * answered. */
  onRetryOrder: () => void;
}

/** Scroll the located card into view and flash it. Imperative: findings live in
 * this panel and cards in sibling components, and restarting the CSS animation on
 * a re-click needs a reflow between removing the class and adding it back. */
function locate(anchorId: string): void {
  const el = document.getElementById(anchorId);
  if (el == null) {
    return;
  }
  const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  el.scrollIntoView({ behavior: reduce ? "auto" : "smooth", block: "center" });
  el.classList.remove("locate-flash");
  void el.offsetWidth;
  el.classList.add("locate-flash");
}

/** `items` paired with React keys built from their content: `base`, with an
 * occurrence suffix where two items share one (two identical findings). */
function keyed<T>(
  items: readonly T[],
  base: (item: T) => string,
): [string, T][] {
  const seen = new Map<string, number>();
  return items.map((item) => {
    const key = base(item);
    const n = seen.get(key) ?? 0;
    seen.set(key, n + 1);
    return [n === 0 ? key : `${key}#${n}`, item];
  });
}

const LEVELS = ["error", "warning", "info"] as const;
type Level = (typeof LEVELS)[number];

const LEVEL_LABEL: Record<Level, string> = {
  error: "Errors",
  warning: "Warnings",
  info: "Info",
};

function groupByLevel(
  issues: readonly ValidationIssue[],
): Record<Level, ValidationIssue[]> {
  const out: Record<Level, ValidationIssue[]> = {
    error: [],
    warning: [],
    info: [],
  };
  for (const issue of issues) {
    // An unknown level (forward-compat) buckets under `info` so it still shows.
    const level: Level = LEVELS.includes(issue.level as Level)
      ? (issue.level as Level)
      : "info";
    out[level].push(issue);
  }
  return out;
}

/** Where a finding sits in the draft, for both finding lists: a validation issue
 * and an order finding differ in how they name their location, not in how it is
 * presented. */
function Locators({ loc }: { loc: FindingLocation }) {
  return (
    <div className={styles.locators}>
      <AriaButton
        className={styles.locate}
        onPress={() => locate(loc.anchorId)}
      >
        {loc.label}
      </AriaButton>
      {/* The cart is read-only (#991): fixes happen on the catalog subject page.
          Omitted when the finding resolves no catalog coordinate. */}
      {loc.catalogHref && (
        <a className={styles.catalogLink} href={loc.catalogHref}>
          Fix in catalog: <code>{loc.catalogLabel}</code>
        </a>
      )}
    </div>
  );
}

function Finding({
  title,
  code,
  message,
  children,
}: {
  title: string;
  code?: string;
  message: string;
  children?: React.ReactNode;
}) {
  return (
    <li>
      <div className={styles.issueHead}>
        <span className={styles.label}>{title}</span>
        {/* The raw code stays visible but DEMOTED: the stable id people cite. */}
        {code && <code className={styles.code}>{code}</code>}
      </div>
      <p className={styles.message}>{message}</p>
      {children}
    </li>
  );
}

export function ValidationPanel({
  result,
  status,
  requestError,
  requestErrorSource,
  orderFindings,
  windowFindings,
  sources,
  onRetry,
  onRetryOrder,
}: Props) {
  const grouped = groupByLevel(result?.issues ?? []);
  useRevealFindings();
  return (
    <section className={styles.validation} aria-label="Validation results">
      {requestError && (
        // A malformed REQUEST, distinct from the 200 ok:false list. When it came
        // back with FINDINGS (a blocked order), they ARE the message: the banner
        // states the verdict once and the group below carries each reason.
        <StatusRow
          tone="error"
          live="alert"
          className={styles.requestError}
          action={
            // The retry belongs to the request that FAILED, which is what the
            // SOURCE says. Neither is offered for a blocked order: the findings
            // are the materializer's verdict on this draft, and re-running
            // either request changes nothing about it. The draft has to change.
            requestErrorSource === "validate" ? (
              <Button onPress={onRetry}>Retry validation</Button>
            ) : requestErrorSource === "order" && orderFindings.length === 0 ? (
              <Button onPress={onRetryOrder}>Retry download</Button>
            ) : null
          }
        >
          {orderFindings.length > 0 ? (
            <>
              <strong>Order blocked:</strong> the materializer produced no order
              for this project.
            </>
          ) : (
            <>
              <strong>Request failed:</strong> {requestError}
            </>
          )}
        </StatusRow>
      )}

      {orderFindings.length > 0 && (
        <fieldset
          className={`${styles.group} ${styles.error}`}
          aria-labelledby="order-findings-heading"
          data-findings
        >
          <h4 className="micro-label" id="order-findings-heading" tabIndex={-1}>
            Blocking findings ({orderFindings.length})
          </h4>
          <ul>
            {keyed(
              orderFindings,
              (f) => `${f.code}|${f.variable ?? ""}|${f.period ?? ""}`,
            ).map(([key, finding]) => {
              const loc = findingLocation(
                orderFindingPointer(finding, sources),
                sources,
              );
              return (
                <Finding
                  key={key}
                  title={codeLabel(finding.code)}
                  code={finding.code}
                  message={finding.message}
                >
                  {/* The EXACT offending subperiod for the coverage codes — the
                      one edit that fixes the request. */}
                  {finding.period && (
                    <p className={styles.path}>Period {finding.period}</p>
                  )}
                  {loc && <Locators loc={loc} />}
                </Finding>
              );
            })}
          </ul>
        </fieldset>
      )}

      {/* Sources the study window has moved off: each keeps its explicit period,
          so the ORDER is blocked until the researcher decides. */}
      {windowFindings.length > 0 && (
        <fieldset
          className={`${styles.group} ${styles.error}`}
          aria-labelledby="window-findings-heading"
          data-findings
        >
          <h4
            className="micro-label"
            id="window-findings-heading"
            tabIndex={-1}
          >
            Blocking the order: outside the study window (
            {windowFindings.length})
          </h4>
          <ul>
            {keyed(windowFindings, (f) => f.message).map(([key, finding]) => (
              <Finding
                key={key}
                title="No years inside the study window"
                message={finding.message}
              >
                {finding.location && <Locators loc={finding.location} />}
              </Finding>
            ))}
          </ul>
        </fieldset>
      )}

      {/* The summary is suppressed while a request error stands: `/order` can
          fail-close a project that VALIDATES clean, and "Draft valid" under
          "Order blocked" says two contradictory things at once. */}
      {requestError ? null : status === "checking" ? (
        // In flight is not a verdict: plain muted text, no glyph or tint.
        <output className={styles.checking} aria-busy="true">
          Checking the current project…
        </output>
      ) : result == null ? (
        <p className="muted">Validation has not run for this draft yet.</p>
      ) : (
        <>
          <StatusRow
            live="status"
            tone={status === "warnings" ? "warn" : result.ok ? "ok" : "error"}
          >
            {/* Names the check that COMPLETED. With nothing to report the verdict
                is the whole line. */}
            {result.ok
              ? status === "warnings"
                ? `Draft valid with warnings: project rules and catalog metadata checked (${result.issues.length} non-blocking ${result.issues.length === 1 ? "note" : "notes"}).`
                : "Draft valid"
              : `Draft not valid: ${grouped.error.length} ${grouped.error.length === 1 ? "error" : "errors"}.`}
          </StatusRow>
          {LEVELS.map(
            (level) =>
              grouped[level].length > 0 && (
                <div
                  key={level}
                  className={`${styles.group} ${styles[level]}`}
                  data-findings
                >
                  <h4 className="micro-label" tabIndex={-1}>
                    {LEVEL_LABEL[level]} ({grouped[level].length})
                  </h4>
                  <ul>
                    {keyed(
                      grouped[level],
                      (issue) => `${issue.code}|${issue.path}`,
                    ).map(([key, issue]) => {
                      const loc = findingLocation(issue.path, sources);
                      return (
                        <Finding
                          key={key}
                          title={codeLabel(issue.code)}
                          code={issue.code}
                          message={issue.message}
                        >
                          {loc ? (
                            <Locators loc={loc} />
                          ) : issue.path ? (
                            <code className={styles.path}>{issue.path}</code>
                          ) : (
                            <span className={styles.path}>
                              (whole document)
                            </span>
                          )}
                        </Finding>
                      );
                    })}
                  </ul>
                </div>
              ),
          )}
          {/* Order materialization is a SEPARATE check; said only where there is
              already something to read, outside the live region. */}
          {status === "warnings" && (
            <p className="muted">
              Generating the order runs its own order checks.
            </p>
          )}
        </>
      )}
    </section>
  );
}
