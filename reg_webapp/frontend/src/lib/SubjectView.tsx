import { type ReactNode, useId } from "react";
import styles from "./SubjectView.module.css";

/** The catalog-subject shell (#638): one article, one title header, and the
 * sections in the canonical order the caller passes them — description, period
 * and picker, value set, relationships, docs, technical details. It owns no data
 * and no headings beyond the title. */
export function SubjectView({
  title,
  fqid,
  children,
}: {
  title: string;
  /** The FQID line under the title; omitted where the breadcrumb already ends
   * in the slug (#670). */
  fqid?: string;
  children: ReactNode;
}) {
  return (
    <article className={styles.subject}>
      <header className={styles.header}>
        <h1 className={styles.title}>{title}</h1>
        {fqid && (
          <p className={styles.fqid}>
            <code>{fqid}</code>
          </p>
        )}
      </header>
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
