import {
  getRelatedDocuments,
  type RelatedDocument,
  relatedDocumentFileHref,
} from "./api";
import styles from "./DocView.module.css";
import { SubjectSection } from "./SubjectView";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { useAsyncResource } from "./use_async_resource";

function bytesLabel(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const kib = bytes / 1024;
  if (kib < 1024) return `${kib.toFixed(kib < 10 ? 1 : 0)} KB`;
  const mib = kib / 1024;
  return `${mib.toFixed(mib < 10 ? 1 : 0)} MB`;
}

function sourceHost(doc: RelatedDocument): string {
  try {
    return new URL(doc.source_url).hostname;
  } catch {
    return "source";
  }
}

/**
 * A register's rehosted register-version PDFs (#742/#967): authoritative
 * register-level source metadata, apart from the fuzzy full-text mentions of
 * `DocMentionsPanel`, and never inherited by variable pages. A deployment
 * without a docs database, or a register without documents, shows nothing.
 */
export function RelatedDocumentsPanel({ register }: { register: string }) {
  const resource = useAsyncResource(register, (signal) =>
    getRelatedDocuments(register, { signal }),
  );
  const documents = resource.data ?? [];
  if (!resource.loading && !resource.error && documents.length === 0) {
    return null;
  }
  return (
    <SubjectSection title="Source documents">
      {resource.loading ? (
        <div aria-busy="true">
          <span className="visually-hidden">Loading source documents…</span>
          <Skeleton count={2} />
        </div>
      ) : resource.error ? (
        <StatusRow tone="error" live="alert">
          The source documents did not load ({resource.error}). Reload the page
          to try again.
        </StatusRow>
      ) : (
        <ul className={styles.documents}>
          {documents.map((doc) => (
            <li key={doc.filename}>
              <a
                className={styles.documentTitle}
                href={relatedDocumentFileHref(register, doc.filename)}
              >
                {doc.title}
              </a>
              <p className={styles.attribution}>
                <span>Källa: SCB, {doc.license}</span>
                <a href={doc.source_url} target="_blank" rel="noreferrer">
                  {sourceHost(doc)}
                </a>
              </p>
              <p className={styles.provenance}>
                <span className={styles.mono}>{bytesLabel(doc.byte_size)}</span>
                , fetched <span className={styles.mono}>{doc.fetched}</span>
              </p>
            </li>
          ))}
        </ul>
      )}
    </SubjectSection>
  );
}
