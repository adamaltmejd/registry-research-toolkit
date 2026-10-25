import { useState } from "react";
import {
  Button as AriaButton,
  Tooltip,
  TooltipTrigger,
} from "react-aria-components";
import { catalogHref } from "./catalog";
import styles from "./Finding.module.css";
import { StatusRow } from "./ui/StatusRow";
import type { StatusTone } from "./ui/types";
import {
  type FindingLevel,
  type ProjectFinding,
  plainMessage,
} from "./validation";

// One project finding, wherever it renders: under the column lane or at the top
// of the source card it concerns, or in the summary panel when it has no card
// (#1906). A notice, so a StatusRow in the finding's tone.

const TONE: Record<FindingLevel, StatusTone> = {
  error: "error",
  warning: "warn",
  info: "info",
};

/** The catalog's words for a coordinate, or null to keep what is held
 * (`catalogNames`). */
export type NameOf = (fqid: string) => string | null;

/** The finding's title. Its stable code, the identifier people cite, is in a
 * tooltip that opens on hover after a delay or at once on focus; pressing the
 * title copies the code. */
function FindingTitle({ title, code }: { title: string; code: string | null }) {
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

export function FindingItem({
  finding,
  nameOf,
}: {
  finding: ProjectFinding;
  nameOf: NameOf;
}) {
  const fix = finding.location?.catalogFqid ?? null;
  const fixName = fix ? nameOf(fix) : null;
  return (
    // The summary panel's jump link lands here: a focus target, not a tab stop.
    <li id={finding.id} tabIndex={-1} className={styles.item}>
      <StatusRow tone={TONE[finding.level]}>
        <span className={styles.body}>
          <FindingTitle title={finding.title} code={finding.code} />
          <span className={styles.message}>
            {plainMessage(finding.message, finding.fqids, nameOf)}
          </span>
          {finding.period && (
            <span className={styles.meta}>Period {finding.period}</span>
          )}
          {/* The cart is read-only (#991): a fix is a new pick on the catalog
              page. */}
          {fix && (
            <a className={`link ${styles.fix}`} href={catalogHref(fix)}>
              {fixName ? `Fix ${fixName} in the catalog` : "Fix in the catalog"}
            </a>
          )}
        </span>
      </StatusRow>
    </li>
  );
}

/** Findings as a list; nothing when there are none. */
export function FindingList({
  findings,
  nameOf,
  label,
  className,
}: {
  findings: readonly ProjectFinding[];
  nameOf: NameOf;
  label: string;
  className?: string;
}) {
  if (findings.length === 0) return null;
  return (
    <ul className={`${styles.list} ${className ?? ""}`} aria-label={label}>
      {findings.map((finding) => (
        <FindingItem key={finding.id} finding={finding} nameOf={nameOf} />
      ))}
    </ul>
  );
}
