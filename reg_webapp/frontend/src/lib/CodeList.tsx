import type { CSSProperties } from "react";
import type { ClassificationExtensionMemberModel } from "./api";
import styles from "./CodeList.module.css";
import { formatWindow } from "./catalog";

/** A code→label set member: a classification code or a value-set member. */
export interface Code {
  code: string;
  label: string;
  level?: number | null;
  member_kind?: ClassificationExtensionMemberModel["member_kind"];
  sentinel_meaning?: ClassificationExtensionMemberModel["sentinel_meaning"];
  scoped_sentinels?: ClassificationExtensionMemberModel["scoped_sentinels"];
}

function codeLevel(code: Code): number | null {
  return typeof code.level === "number" && Number.isFinite(code.level)
    ? code.level
    : null;
}

/**
 * The one code viewer (#638 PR3): a variable's value set and a classification's
 * codes render alike, as a bounded scroll of code rows. The caller filters and
 * pages on the server and hands over what it has read, so this renders it
 * verbatim — no second filter, and no grouping, since a partial page would
 * group under parents it may not hold.
 */
export function CodeList({ codes }: { codes: Code[] }) {
  if (codes.length === 0) return null;
  // Depth counts from the shallowest level loaded, not from level 1: most
  // classifications start at level 2 or deeper and must render flat.
  const levels = codes
    .map(codeLevel)
    .filter((level): level is number => level != null);
  const topLevel = Math.min(...levels);
  return (
    <div className={styles.scroll}>
      <ul className={styles.codes}>
        {codes.map((code) => {
          const level = codeLevel(code);
          const depth = level == null ? null : level - topLevel + 1;
          return (
            <li
              key={`${code.code}\u0000${code.label}`}
              className={
                depth != null && depth > 1
                  ? `${styles.row} ${styles.levelled}`
                  : styles.row
              }
              aria-level={depth ?? undefined}
              style={
                depth != null
                  ? ({ "--code-depth": depth - 1 } as CSSProperties)
                  : undefined
              }
            >
              <code className={styles.key}>{code.code}</code>
              <span className={styles.label}>
                {code.label}
                {code.member_kind === "sentinel" && (
                  <SentinelNotes code={code} />
                )}
              </span>
            </li>
          );
        })}
      </ul>
    </div>
  );
}

function SentinelNotes({ code }: { code: Code }) {
  return (
    <>
      {code.sentinel_meaning && (
        <span className={styles.sentinel}>
          Special code: {code.sentinel_meaning}
        </span>
      )}
      {(code.scoped_sentinels ?? []).map((evidence) => (
        <span
          key={`${evidence.delivery_column_name}:${evidence.valid_from ?? ""}`}
          className={styles.sentinel}
        >
          Special code for <code>{evidence.delivery_column_name}</code>{" "}
          {formatWindow(
            evidence.valid_from ?? null,
            evidence.valid_to ?? null,
          ) || "unknown period"}
          :{" "}
          {evidence.members
            .filter(([member]) => member === code.code)
            .map(([, meaning]) => meaning)
            .join("; ")}
          {evidence.provenance && (
            <details className={styles.evidence}>
              <summary className={styles.evidenceToggle}>
                Source evidence
              </summary>
              <p>{evidence.provenance}</p>
            </details>
          )}
        </span>
      ))}
    </>
  );
}
