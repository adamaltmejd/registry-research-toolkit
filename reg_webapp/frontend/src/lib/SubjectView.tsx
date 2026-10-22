import { type ComponentProps, type ReactNode, useId } from "react";
import styles from "./SubjectView.module.css";
import { Definitions } from "./ui/Definitions";

/** The catalog-subject shell (#638): one article whose head is the pane's
 * `Definitions` (title, member-of line, facets, definition list), then the
 * sections in the canonical order the caller passes them — period, picker,
 * value set, relationships, docs, technical details. It owns no data and no
 * headings beyond the head's. */
export function SubjectView({
  children,
  ...head
}: ComponentProps<typeof Definitions> & { children: ReactNode }) {
  return (
    <article className={styles.subject}>
      <Definitions {...head} />
      {children}
    </article>
  );
}

/** One titled section of a subject page (provenance, lineage warnings, parsed
 * documentation): an `h2` over a hairline, named by its heading. */
export function SubjectSection({
  title,
  children,
}: {
  title: string;
  children: ReactNode;
}) {
  const id = useId();
  return (
    <section className={styles.section} aria-labelledby={id}>
      <h2 id={id} className={styles.sectionHeading}>
        {title}
      </h2>
      {children}
    </section>
  );
}

/** The shared "Technical details" disclosure (#638 PR4): the backend and
 * structural fields a subject demotes stay available, collapsed, below the
 * fields a researcher reads. A plain `<details>`: find-in-page opens it. */
export function TechnicalDetails({ children }: { children: ReactNode }) {
  return (
    <details className={styles.technical}>
      <summary className={styles.technicalSummary}>Technical details</summary>
      {children}
    </details>
  );
}
