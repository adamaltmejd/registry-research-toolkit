import type { ReactNode } from "react";
import styles from "./Definitions.module.css";
import { type NodeType, TypeTag } from "./TypeTag";

/** One fact: a `label`-level term over prose, or a value the caller renders
 * (a mono identifier, a link). */
export interface Definition {
  term: string;
  value: ReactNode;
}

interface Props {
  /** The page's kind pill, with its glyph, first in the pill row. A
   * classification's page has none (maintainer, #2317). */
  kind?: NodeType;
  /** More pills after the kind's on the same row: a variable's Type pills. */
  pills?: ReactNode;
  title: string;
  /** Prose under the title block: a register's or a group's description. */
  description?: string;
  /** The subject's facets as a labelled row. */
  facets?: readonly { label: string; value: string }[];
  definitions?: readonly Definition[];
}

/**
 * The head of a detail pane (DESIGN.md → Detail pane; the Definitions mockup):
 * one row of pills (the kind, then the caller's), the `h1`, the facets as a
 * labelled row, then a definition list of `label` terms and `body` values. The
 * breadcrumb above it is the shell's and names what the page belongs to, so
 * there is no member-of line (#2317).
 */
export function Definitions({
  kind,
  pills,
  title,
  description,
  facets = [],
  definitions = [],
}: Props) {
  return (
    <header className={styles.head}>
      {(kind || pills) && (
        <div className={styles.pills}>
          {kind && <TypeTag type={kind} />}
          {pills}
        </div>
      )}
      <h1 className={styles.title}>{title}</h1>
      {description && <p className={styles.description}>{description}</p>}
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
