import type { ValidationResultModel } from "./api";
import { FindingList, type NameOf } from "./Finding";
import { type SafeSource, safeSourceRegisterVariant } from "./project_data";
import type { RequestErrorSource, ValidationStatus } from "./project_store";
import { useRevealFindings } from "./reveal_findings";
import { Button } from "./ui/Button";
import { StatusRow } from "./ui/StatusRow";
import styles from "./ValidationPanel.module.css";
import {
  bindingVariableAt,
  FINDING_LEVELS,
  type FindingLevel,
  type ProjectFinding,
} from "./validation";

// The project's findings summary (see crates/reg-core/DESIGN.md → Structural
// rules and issue codes). A located finding renders on its source card, under
// what it concerns (#1906); this panel keeps the verdict, the count per level,
// the findings that have no card (the name, the study window, the schema) in
// full, and a jump link to every located one. Distinct from a 200 `ok:false`
// list, a malformed REQUEST (a true 4xx ApiError) is a banner.

interface Props {
  result: ValidationResultModel | null;
  status: ValidationStatus;
  requestError: string | null;
  /** WHICH request `requestError` came from — the discriminator the retry
   * affordance is gated on. */
  requestErrorSource: RequestErrorSource | null;
  /** Every finding on the project (`projectFindings`): validation issues, a
   * blocked order's findings and the study window's disjoint sources. */
  findings: readonly ProjectFinding[];
  /** The draft's sources, to name where a located finding sits. */
  sources: readonly SafeSource[];
  nameOf: NameOf;
  /** Re-runs `/validate`. Offered ONLY for a failed validation REQUEST. */
  onRetry: () => void;
  /** Re-POSTs `/order`. Offered ONLY for an order request the backend never
   * answered. */
  onRetryOrder: () => void;
}

/** Scroll the finding into view, move focus to it and flash it. Imperative:
 * the finding lives on a card in a sibling component, and restarting the CSS
 * animation on a second press needs a reflow between removing the class and
 * adding it back. */
function jump(id: string): void {
  const el = document.getElementById(id);
  if (el == null) {
    return;
  }
  const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  el.focus({ preventScroll: true });
  el.scrollIntoView({ behavior: reduce ? "auto" : "smooth", block: "center" });
  el.classList.remove("locate-flash");
  void el.offsetWidth;
  el.classList.add("locate-flash");
}

const LEVEL_LABEL: Record<FindingLevel, string> = {
  error: "Errors",
  warning: "Warnings",
  info: "Info",
};

/** Where a located finding sits, in the catalog's words: the column's variable
 * and the source's register ("Kön in LISA, Individer"), falling back to
 * positions while the names are unread. */
function placeOf(
  finding: ProjectFinding,
  sources: readonly SafeSource[],
  nameOf: NameOf,
): string {
  const location = finding.location;
  if (location === null) return "";
  const source = sources[location.sourceIndex] ?? null;
  const rv = safeSourceRegisterVariant(source);
  const sourceName = (rv && nameOf(rv)) || `source ${location.sourceIndex + 1}`;
  if (location.bindingIndex === null) return sourceName;
  const variable = bindingVariableAt(source, location.bindingIndex);
  const column =
    (variable && nameOf(variable)) || `column ${location.bindingIndex + 1}`;
  return `${column} in ${sourceName}`;
}

/** One group of findings: the ones with no card in full, then a jump link to
 * each located one. */
function FindingGroup({
  headingId,
  heading,
  level,
  findings,
  sources,
  nameOf,
}: {
  headingId: string;
  heading: string;
  level: FindingLevel;
  findings: readonly ProjectFinding[];
  sources: readonly SafeSource[];
  nameOf: NameOf;
}) {
  const unlocated = findings.filter((f) => f.location === null);
  const located = findings.filter((f) => f.location !== null);
  return (
    <fieldset
      className={`${styles.group} ${styles[level]}`}
      aria-labelledby={headingId}
      data-findings
    >
      <h4 className="micro-label" id={headingId} tabIndex={-1}>
        {heading} ({findings.length})
      </h4>
      <FindingList
        findings={unlocated}
        nameOf={nameOf}
        label={`${heading} on the project`}
      />
      {located.length > 0 && (
        <ul className={styles.jumps}>
          {located.map((finding) => (
            <li key={finding.id}>
              {/* A plain fragment link, so it works without script too; the
                  handler adds the focus and the flash. */}
              <a
                className="link"
                href={`#${finding.id}`}
                onClick={(event) => {
                  event.preventDefault();
                  jump(finding.id);
                }}
              >
                {finding.title} on {placeOf(finding, sources, nameOf)}
              </a>
            </li>
          ))}
        </ul>
      )}
    </fieldset>
  );
}

export function ValidationPanel({
  result,
  status,
  requestError,
  requestErrorSource,
  findings,
  sources,
  nameOf,
  onRetry,
  onRetryOrder,
}: Props) {
  useRevealFindings();
  const orderFindings = findings.filter((f) => f.check === "order");
  const windowFindings = findings.filter((f) => f.check === "window");
  const issues = findings.filter((f) => f.check === "validation");
  const errorCount = issues.filter((f) => f.level === "error").length;
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
        <FindingGroup
          headingId="order-findings-heading"
          heading="Blocking findings"
          level="error"
          findings={orderFindings}
          sources={sources}
          nameOf={nameOf}
        />
      )}

      {/* Sources the study window has moved off: each keeps its explicit period,
          so the ORDER is blocked until the researcher decides. */}
      {windowFindings.length > 0 && (
        <FindingGroup
          headingId="window-findings-heading"
          heading="Blocking the order: outside the study window"
          level="error"
          findings={windowFindings}
          sources={sources}
          nameOf={nameOf}
        />
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
                ? `Draft valid with warnings: project rules and catalog metadata checked (${issues.length} non-blocking ${issues.length === 1 ? "note" : "notes"}).`
                : "Draft valid"
              : `Draft not valid: ${errorCount} ${errorCount === 1 ? "error" : "errors"}.`}
          </StatusRow>
          {FINDING_LEVELS.map((level) => {
            const atLevel = issues.filter((f) => f.level === level);
            return atLevel.length > 0 ? (
              <FindingGroup
                key={level}
                headingId={`${level}-findings-heading`}
                heading={LEVEL_LABEL[level]}
                level={level}
                findings={atLevel}
                sources={sources}
                nameOf={nameOf}
              />
            ) : null;
          })}
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
