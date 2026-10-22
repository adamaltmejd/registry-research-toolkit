import type { ReactNode } from "react";
import styles from "./Definitions.module.css";

/** A link in the member-of line: what the subject belongs to. */
export interface MemberOf {
  /** What the link is to the subject, in sentence case: "Member of",
   * "Family". */
  relation: string;
  label: string;
  href: string;
}

/** One fact: a `label`-level term over prose, or a value the caller renders
 * (a mono identifier, a link). */
export interface Definition {
  term: string;
  value: ReactNode;
}

interface Props {
  title: string;
  /** The subject's machine identifier under the title, in mono; omitted where
   * the breadcrumb already ends in it. */
  fqid?: string;
  memberOf?: readonly MemberOf[];
  /** The subject's facets as a labelled row. */
  facets?: readonly { label: string; value: string }[];
  definitions?: readonly Definition[];
}

/**
 * The head of a detail pane (DESIGN.md → Detail pane; the Definitions mockup):
 * the `h1`, a member-of line whose links carry the accent underline, the facets
 * as a labelled row, then a definition list of `label` terms and `body` values.
 * The breadcrumb above it is the shell's.
 */
export function Definitions({
  title,
  fqid,
  memberOf = [],
  facets = [],
  definitions = [],
}: Props) {
  return (
    <header className={styles.head}>
      <h1 className={styles.title}>{title}</h1>
      {fqid && <code className={styles.fqid}>{fqid}</code>}
      {memberOf.length > 0 && (
        <ul className={styles.memberOf}>
          {memberOf.map((member) => (
            <li key={member.href}>
              {member.relation} <a href={member.href}>{member.label}</a>
            </li>
          ))}
        </ul>
      )}
      {facets.length > 0 && (
        <dl className={styles.facets}>
          {facets.map((facet) => (
            <div key={facet.label}>
              <dt className="micro-label">{facet.label}</dt>
              <dd>{facet.value}</dd>
            </div>
          ))}
        </dl>
      )}
      {definitions.length > 0 && (
        <dl className={styles.definitions}>
          {definitions.map((row) => (
            <div key={row.term} className={styles.definition}>
              <dt className="micro-label">{row.term}</dt>
              <dd>{row.value}</dd>
            </div>
          ))}
        </dl>
      )}
    </header>
  );
}
