import { type DocDetail, getDoc } from "./api";
import styles from "./DocView.module.css";
import { type Definition, Definitions } from "./ui/Definitions";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { useAsyncResource } from "./use_async_resource";

function docFacts(doc: DocDetail): Definition[] {
  const facts: Definition[] = [];
  if (doc.register) facts.push({ term: "Register", value: doc.register });
  if (doc.variable) facts.push({ term: "Variable", value: doc.variable });
  // Tags are plain text, not filter controls.
  if (doc.tags.length > 0)
    facts.push({ term: "Tags", value: doc.tags.join(", ") });
  // The source: an off-site link to the SCB PDF where one was resolved at the
  // doc-DB build (#372), else the bare source identifier.
  if (doc.source_url) {
    facts.push({
      term: "Source",
      value: (
        <a
          className={`link ${styles.source}`}
          href={doc.source_url}
          target="_blank"
          rel="noopener noreferrer"
        >
          {doc.source_title ?? doc.source ?? doc.source_url}
        </a>
      ),
    });
  } else if (doc.source) {
    facts.push({ term: "Source", value: doc.source });
  }
  return facts;
}

/**
 * One documentation page (#394): its metadata, a pointer to the source and a
 * bounded excerpt. `docs_get` also carries the full `body` (for agents); the
 * SPA never renders it. The excerpt is text, never markup: it may carry
 * full-text highlight markers, and React's escaping is the republication guard.
 */
export function DocView({ identifier }: { identifier: string }) {
  const resource = useAsyncResource(identifier, () => getDoc(identifier));
  if (resource.loading) {
    return (
      <section className={styles.page} aria-busy="true">
        <span className="visually-hidden">Loading…</span>
        <Skeleton width="min(24rem, 70%)" count={2} />
        <Skeleton variant="block" />
      </section>
    );
  }
  if (resource.error || !resource.data) {
    // A 404 is either no docs database or no such document; the server's
    // message says which, so it is shown as it came.
    return (
      <div className={styles.page}>
        {/* The page keeps its heading: the identifier asked for. */}
        <Definitions title={identifier} />
        <StatusRow tone="error" live="alert">
          {resource.error}
        </StatusRow>
        {resource.status === 404 && (
          <p className={styles.note}>
            Only LISA's documentation is in the catalog so far.
          </p>
        )}
      </div>
    );
  }
  const doc = resource.data;
  return (
    <article className={styles.page}>
      <Definitions title={doc.display_name} definitions={docFacts(doc)} />
      {doc.excerpt ? (
        <>
          <blockquote className={styles.excerpt}>{doc.excerpt}</blockquote>
          <p className={styles.note}>
            An excerpt. The source has the full document.
          </p>
        </>
      ) : (
        <p className={styles.note}>This document has no preview.</p>
      )}
    </article>
  );
}
