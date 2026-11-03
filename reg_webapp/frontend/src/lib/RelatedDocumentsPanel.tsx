import { useState } from "react";
import {
  Button,
  Disclosure,
  DisclosurePanel,
  Heading,
} from "react-aria-components";
import {
  docFileHref,
  getDoc,
  getRelatedDocuments,
  type RelatedDocument,
} from "./api";
import pageStyles from "./CatalogNodeView.module.css";
import { catalogHref, matchesFilter } from "./catalog";
import { catalogWords } from "./catalog_labels";
import { FilterInput } from "./FilterInput";
import { parseInlineMarkdown } from "./inline_markdown";
import styles from "./RelatedDocumentsPanel.module.css";
import { EmptyState } from "./ui/EmptyState";
import { Panel } from "./ui/Panel";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { TreeChevron } from "./ui/TreeChevron";
import { useAsyncResource } from "./use_async_resource";

function bytesLabel(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  const kib = bytes / 1024;
  if (kib < 1024) return `${kib.toFixed(kib < 10 ? 1 : 0)} KB`;
  const mib = kib / 1024;
  return `${mib.toFixed(mib < 10 ? 1 : 0)} MB`;
}

type Names = ReadonlyMap<string, string>;

/* oxlint-disable react/no-array-index-key -- Immutable document text has no paragraph or segment ids. */
function DocumentBody({ doc, names }: { doc: string; names: Names }) {
  const resource = useAsyncResource(doc, (signal) => getDoc(doc, { signal }));
  if (resource.data == null && !resource.error)
    return (
      <div aria-busy="true">
        <Skeleton count={2} />
      </div>
    );
  if (resource.error)
    return (
      <StatusRow tone="error" live="alert">
        The document did not load ({resource.error}). Reload the page to try
        again.
      </StatusRow>
    );
  const subjects = new Map(
    (resource.data?.subjects ?? []).flatMap((subject) => {
      const name = subject.kind !== "value_set" && names.get(subject.fqid);
      return name ? [[subject.fqid, name] as const] : [];
    }),
  );
  return (
    <div className={styles.body}>
      {(resource.data?.body ?? "")
        .split(/\n\s*\n/u)
        .filter(Boolean)
        .map((paragraph, index) => (
          // Paragraphs and inline segments have no independent identity in the source text.
          <p className={styles.paragraph} key={index}>
            {parseInlineMarkdown(paragraph).map((segment, part) => {
              const text = segment.text.replace(/\\(.)/gu, "$1");
              if (segment.emphasis === "strong")
                return <strong key={part}>{text}</strong>;
              if (segment.emphasis === "em") return <em key={part}>{text}</em>;
              return text;
            })}
          </p>
        ))}
      {subjects.size > 0 && (
        <div>
          <p className={styles.meta}>About</p>
          <ul className={pageStyles.plainList}>
            {[...subjects].map(([fqid, name]) => (
              <li key={fqid}>
                <a href={catalogHref(fqid)}>{name}</a>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

/* oxlint-enable react/no-array-index-key */

function DocumentRow({
  document,
  names,
  sourceLink,
}: {
  document: RelatedDocument;
  names: Names;
  sourceLink: boolean;
}) {
  const [requested, setRequested] = useState(false);
  const meta = (
    <p className={styles.meta}>
      {document.kind === "parsed" ? "Parsed text" : "PDF"},{" "}
      {bytesLabel(document.bytes)}; {catalogWords(document.licence)}
    </p>
  );
  const source = sourceLink && document.url && (
    <a
      className={`link ${styles.source}`}
      href={document.url}
      title={document.url}
    >
      {document.url}
    </a>
  );
  if (document.kind === "pdf")
    return (
      <div className={styles.row}>
        <a href={docFileHref(document.doc)}>{document.title}</a>
        {meta}
        {source}
      </div>
    );
  return (
    <Disclosure
      className={pageStyles.disclosure}
      onExpandedChange={(expanded) => {
        if (expanded) setRequested(true);
      }}
    >
      {({ isExpanded }) => (
        <>
          <Heading level={3} className={pageStyles.disclosureHeading}>
            <Button slot="trigger" className={pageStyles.disclosureButton}>
              <TreeChevron />
              <span>{document.title}</span>
            </Button>
          </Heading>
          {meta}
          {source}
          <DisclosurePanel
            className={`motion-disclose ${isExpanded ? "open" : ""}`}
          >
            <div>
              {requested && <DocumentBody doc={document.doc} names={names} />}
            </div>
          </DisclosurePanel>
        </>
      )}
    </Disclosure>
  );
}

function DocumentGroup({
  url,
  documents,
  names,
  grouped,
}: {
  grouped: boolean;
  url: string | null;
  documents: RelatedDocument[];
  names: Names;
}) {
  const [requested, setRequested] = useState(false);
  const rows = (
    <ul className={pageStyles.plainList}>
      {documents.map((document) => (
        <li key={document.doc}>
          <DocumentRow
            document={document}
            names={names}
            sourceLink={!grouped}
          />
        </li>
      ))}
    </ul>
  );
  if (!url || !grouped) return rows;
  return (
    <Disclosure
      className={pageStyles.disclosure}
      onExpandedChange={(expanded) => {
        if (expanded) setRequested(true);
      }}
    >
      {({ isExpanded }) => (
        <>
          <Heading
            level={3}
            className={`${pageStyles.disclosureHeading} ${styles.groupHeading}`}
          >
            <Button
              slot="trigger"
              className={`${pageStyles.disclosureButton} ${styles.groupTrigger}`}
              aria-label={`Documents from ${url}`}
            >
              <TreeChevron />
            </Button>
            <a className={`link ${styles.source}`} href={url} title={url}>
              {url}
            </a>
          </Heading>
          <DisclosurePanel
            className={`motion-disclose ${isExpanded ? "open" : ""}`}
          >
            <div>{requested && rows}</div>
          </DisclosurePanel>
        </>
      )}
    </Disclosure>
  );
}

export function RelatedDocumentsPanel({
  fqid,
  names,
  filterable = false,
}: {
  fqid: string;
  names: Names;
  filterable?: boolean;
}) {
  const resource = useAsyncResource(fqid, (signal) =>
    getRelatedDocuments(fqid, { signal }),
  );
  const [filter, setFilter] = useState("");
  const documents = [
    ...new Map(
      (resource.data ?? []).map((document) => [document.doc, document]),
    ).values(),
  ];
  const filtered = documents.filter((document) =>
    matchesFilter(filter, document.title),
  );
  const groups = new Map<string | null, RelatedDocument[]>();
  // Keep the original grouping while filtering, so a lone match retains its source heading.
  for (const document of documents) {
    const group = groups.get(document.url) ?? [];
    group.push(document);
    groups.set(document.url, group);
  }
  if (resource.data?.length === 0) return null;
  return (
    <Panel title="Source documents">
      {resource.data == null && !resource.error ? (
        <div aria-busy="true">
          <Skeleton count={2} />
        </div>
      ) : resource.error ? (
        <StatusRow tone="error" live="alert">
          The source documents did not load ({resource.error}). Reload the page
          to try again.
        </StatusRow>
      ) : (
        <div className={styles.content}>
          {filterable && (
            <FilterInput
              value={filter}
              onChange={setFilter}
              total={documents.length}
              shown={filtered.length}
              label="Filter source documents"
              placeholder="Filter section titles…"
            />
          )}
          {filtered.length === 0 ? (
            <EmptyState
              title="No matching sections."
              description="Try a different section title."
            />
          ) : (
            [...groups].map(([url, group]) => {
              const matches = group.filter((document) =>
                matchesFilter(filter, document.title),
              );
              return (
                matches.length > 0 && (
                  <DocumentGroup
                    key={url ?? "no-source"}
                    url={url}
                    grouped={url !== null && group.length > 1}
                    documents={matches}
                    names={names}
                  />
                )
              );
            })
          )}
        </div>
      )}
    </Panel>
  );
}
