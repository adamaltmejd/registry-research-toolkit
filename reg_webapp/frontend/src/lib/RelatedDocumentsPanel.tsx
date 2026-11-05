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
import { FilterInput } from "./FilterInput";
import { InlineMarkdown } from "./InlineMarkdown";
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
function DocumentBody({
  doc,
  title,
  names,
  pageFqids,
}: {
  doc: string;
  title: string;
  names: Names;
  pageFqids: readonly string[];
}) {
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
      const name =
        subject.kind !== "value_set" &&
        !pageFqids.includes(subject.fqid) &&
        names.get(subject.fqid);
      return name ? [[subject.fqid, name] as const] : [];
    }),
  );
  const paragraphs = (resource.data?.body ?? "")
    .split(/\n\s*\n/u)
    .filter(Boolean);
  if (paragraphs[0]?.trim() === title.trim()) paragraphs.shift();
  return (
    <div className={styles.body}>
      {paragraphs.map((paragraph, index) => (
        // Paragraphs and inline segments have no independent identity in the source text.
        <p className={styles.paragraph} key={index}>
          <InlineMarkdown text={paragraph} />
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
  pageFqids,
}: {
  document: RelatedDocument;
  names: Names;
  sourceLink: boolean;
  pageFqids: readonly string[];
}) {
  const [requested, setRequested] = useState(false);
  const meta = (
    <p className={styles.meta}>
      {document.kind === "parsed"
        ? "Parsed text"
        : `PDF, ${bytesLabel(document.bytes)}`}
      ; {document.licence}
    </p>
  );
  const source = sourceLink && document.url && (
    <a
      className={`link ${styles.source}`}
      href={document.url}
      title={document.url}
    >
      Source
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
          {sourceLink && meta}
          {source}
          <DisclosurePanel
            className={`motion-disclose ${isExpanded ? "open" : ""}`}
          >
            <div>
              {requested && (
                <DocumentBody
                  doc={document.doc}
                  title={document.title}
                  names={names}
                  pageFqids={pageFqids}
                />
              )}
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
  pageFqids,
  filtering,
  licences,
}: {
  url: string;
  documents: RelatedDocument[];
  names: Names;
  pageFqids: readonly string[];
  filtering: boolean;
  licences: string[];
}) {
  const [expanded, setExpanded] = useState(false);
  return (
    <Disclosure
      className={pageStyles.disclosure}
      isExpanded={filtering || expanded}
      onExpandedChange={(open) => {
        if (!filtering) setExpanded(open);
      }}
    >
      {({ isExpanded }) => (
        <>
          <Heading level={3} className={pageStyles.disclosureHeading}>
            <Button slot="trigger" className={pageStyles.disclosureButton}>
              <TreeChevron />
              <span className={styles.groupLabel} title={url}>
                {url}
              </span>
            </Button>
          </Heading>
          <p className={styles.meta}>Parsed text; {licences.join(", ")}</p>
          <a className={`link ${styles.source}`} href={url} title={url}>
            Source
          </a>
          <DisclosurePanel
            className={`motion-disclose ${isExpanded ? "open" : ""}`}
          >
            <div>
              <ul className={pageStyles.plainList}>
                {documents.map((document) => (
                  <li key={document.doc}>
                    <DocumentRow
                      document={document}
                      names={names}
                      pageFqids={pageFqids}
                      sourceLink={false}
                    />
                  </li>
                ))}
              </ul>
            </div>
          </DisclosurePanel>
        </>
      )}
    </Disclosure>
  );
}

/** The documents about the page's subjects: one, or a group's members, whose
 * shared documents are listed once. */
export function RelatedDocumentsPanel({
  fqids,
  names,
  filterable = false,
}: {
  fqids: readonly string[];
  names: Names;
  filterable?: boolean;
}) {
  // simplify: one request per subject, since the server relates documents to
  // a variable, not a group; a group-level read would make it one.
  const resource = useAsyncResource(fqids.join("\n"), async (signal) =>
    (
      await Promise.all(
        fqids.map((fqid) => getRelatedDocuments(fqid, { signal })),
      )
    ).flat(),
  );
  const [filter, setFilter] = useState("");
  const documents = [
    ...new Map(
      (resource.data ?? []).map((document) => [document.doc, document]),
    ).values(),
  ];
  const canFilter =
    filterable && documents.some((document) => document.kind === "parsed");
  const query = canFilter ? filter : "";
  const filtered = documents.filter((document) =>
    matchesFilter(query, document.title),
  );
  const groups = new Map<string, RelatedDocument[]>();
  // Keep original parsed-document groups while filtering, including a lone match.
  for (const document of documents) {
    if (document.kind !== "parsed" || !document.url) continue;
    const group = groups.get(document.url) ?? [];
    group.push(document);
    groups.set(document.url, group);
  }
  if (!resource.error && !resource.data?.length) return null;
  return (
    <Panel title="Source documents">
      {resource.error ? (
        <StatusRow tone="error" live="alert">
          The source documents did not load ({resource.error}). Reload the page
          to try again.
        </StatusRow>
      ) : (
        <div className={styles.content}>
          {canFilter && (
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
            documents.map((document) => {
              const group =
                document.kind === "parsed" && document.url
                  ? groups.get(document.url)
                  : undefined;
              if (group && group.length > 1) {
                if (document.doc !== group[0].doc) return null;
                const matches = group.filter((section) =>
                  matchesFilter(query, section.title),
                );
                return (
                  matches.length > 0 && (
                    <DocumentGroup
                      key={document.doc}
                      url={document.url as string}
                      documents={matches}
                      names={names}
                      pageFqids={fqids}
                      filtering={query.trim() !== ""}
                      licences={[
                        ...new Set(group.map((section) => section.licence)),
                      ]}
                    />
                  )
                );
              }
              return (
                matchesFilter(query, document.title) && (
                  <DocumentRow
                    key={document.doc}
                    document={document}
                    names={names}
                    pageFqids={fqids}
                    sourceLink
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
