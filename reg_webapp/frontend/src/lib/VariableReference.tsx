import {
  getRelatedDocuments,
  relatedDocumentFileHref,
  type VariableShow,
  type VariableStateModel,
} from "./api";
import {
  formatDataType,
  formatStateWindow,
  registerPrefixOf,
  stateKey,
  windowTitle,
} from "./catalog";
import {
  matrixAnswerSource,
  pagesLabel,
  provenanceBlocks,
} from "./curated_source";
import { DocMentionsPanel } from "./DocMentionsPanel";
import { LineageDetails } from "./LineageDetails";
import { TechnicalDetails } from "./SubjectView";
import { ColumnChip } from "./ui/ColumnChip";
import { KeyValue } from "./ui/KeyValue";
import { Tag } from "./ui/Tag";
import type { KeyValueRow } from "./ui/types";
import { useAsyncResource } from "./use_async_resource";
import styles from "./VariableReference.module.css";

/** The parts of a variable's page below its value sets: its tags (shown under
 * the head) and its lineage, document mentions and technical details. Shared by
 * the binding leaf and a concept group's focused member, which is the same
 * variable page (#1909), so neither re-types them. */

interface Correction {
  state: VariableStateModel;
  className: string;
  evidence: string;
  sourceEditions: string[];
  providerDocumented: boolean;
}

/** A state's curated corrections (`errata:` provenance blocks). The class line
 * may stand alone: the catalog publishes only source locators after it, never
 * curator evidence (#1708). A malformed attribution list yields nothing. */
function correctionsFromState(state: VariableStateModel): Correction[] {
  return provenanceBlocks(state.provenance)
    .filter((block) => block.startsWith("errata:"))
    .flatMap((block): Correction[] => {
      const separator = block.indexOf("\n");
      const head = separator < 0 ? block : block.slice(0, separator);
      const className = head.slice("errata:".length).trim();
      const body = separator < 0 ? "" : block.slice(separator + 1).trim();
      const providerDocumented = className === "scoped-attributions";
      if (!providerDocumented && className !== "overlapping-attributions") {
        return className
          ? [
              {
                state,
                className,
                evidence: body,
                sourceEditions: [],
                providerDocumented: false,
              },
            ]
          : [];
      }
      let parsed: unknown;
      try {
        parsed = JSON.parse(body);
      } catch {
        return [];
      }
      if (!Array.isArray(parsed)) return [];
      const corrections: Correction[] = [];
      for (const item of parsed) {
        if (typeof item !== "object" || item === null) return [];
        const record = item as Record<string, unknown>;
        if (
          typeof record.class !== "string" ||
          !record.class.trim() ||
          typeof record.evidence !== "string" ||
          !record.evidence.trim() ||
          !Array.isArray(record.source_editions) ||
          record.source_editions.length === 0 ||
          !record.source_editions.every(
            (edition) => typeof edition === "string" && edition.trim(),
          )
        ) {
          return [];
        }
        corrections.push({
          state,
          className: record.class.trim(),
          evidence: record.evidence.trim(),
          sourceEditions: record.source_editions as string[],
          providerDocumented,
        });
      }
      return corrections;
    });
}

function documentName(url: string): string {
  try {
    const parsed = new URL(url);
    const file = parsed.pathname.split("/").pop();
    return file ? decodeURIComponent(file) : parsed.hostname;
  } catch {
    return url;
  }
}

function interval(state: VariableStateModel): string {
  return windowTitle(state.valid_from, state.valid_to, state.period_scope);
}

/** A variable's thematic tags, under its head. */
export function VariableTags({ tags }: { tags: VariableShow["tags"] }) {
  if (!tags || tags.length === 0) return null;
  return (
    <ul className={styles.tags} aria-label="Thematic tags">
      {tags.map((tag) => (
        <li key={tag.slug} className={styles.tag}>
          <Tag>{tag.label}</Tag>
          {tag.starred && tag.note && (
            <span className={`muted ${styles.small}`}>
              Recommended: {tag.note}
            </span>
          )}
        </li>
      ))}
    </ul>
  );
}

/**
 * A variable's lineage, document mentions and technical details (#638 PR4 /
 * #1038). `states` are the states the page shows; `technicalState` is the one
 * resolved state whose type and column join the technical rows, or null.
 */
export function VariableReference({
  node,
  states,
  technicalState,
}: {
  node: VariableShow;
  states: readonly VariableStateModel[];
  technicalState: VariableStateModel | null;
}) {
  const corrections = states.flatMap(correctionsFromState);
  const inferredGaps = states.filter(
    (s) => s.provenance === "inferred:resolution-gap",
  );
  // Y-202: one marked state over a pooled multi-year edition.
  const pooledStates = states.filter((s) => s.pooled);

  // A curated matrix answer cites a page of an SCB PDF: one line per distinct
  // (document, pages), the state's own column's page where it names one.
  const matrixSources = new Map<
    string,
    { documentUrl: string; sha256: string; pages: string }
  >();
  for (const state of states) {
    const source = matrixAnswerSource(state.provenance);
    if (source === null) continue;
    const own = state.delivery_column_name
      ? source.pages[state.delivery_column_name]
      : undefined;
    const pages = pagesLabel(
      own === undefined ? Object.values(source.pages) : [own],
    );
    matrixSources.set(`${source.sha256} ${pages}`, { ...source, pages });
  }
  const registerPrefix = registerPrefixOf(node.fqid);
  // The register's rehosted PDFs, read only when a state cites one, to title
  // and link it by its digest; otherwise the provider URL stands.
  const cited = useAsyncResource(
    matrixSources.size > 0 ? registerPrefix : null,
    (signal) => getRelatedDocuments(registerPrefix, { signal }),
  );

  return (
    <>
      <LineageDetails node={node} />
      <DocMentionsPanel node={node} />

      {/* The backend and structural rows. The flags are always known; type
          and column join only for one resolved state. */}
      <TechnicalDetails>
        <dl className={styles.meta}>
          <dt className="micro-label">Sensitive</dt>
          <dd>{node.is_sensitive ? "yes" : "no"}</dd>
          <dt className="micro-label">Identifier</dt>
          <dd>{node.is_identifier ? "yes" : "no"}</dd>
          {technicalState?.data_type && (
            <>
              <dt className="micro-label">Data type</dt>
              <dd>
                {formatDataType(
                  technicalState.data_type,
                  technicalState.data_length,
                )}
              </dd>
            </>
          )}
          {technicalState?.delivery_column_name && (
            <>
              <dt className="micro-label">Delivery column</dt>
              <dd>
                <ColumnChip name={technicalState.delivery_column_name} />
              </dd>
            </>
          )}
          {/* Waits for the document lookup, so the link never swaps target
              under the cursor. */}
          {matrixSources.size > 0 && !cited.loading && (
            <>
              <dt className="micro-label">Source</dt>
              <dd>
                {[...matrixSources.entries()].map(([key, source]) => {
                  const doc = cited.data?.find(
                    (d) => d.sha256 === source.sha256,
                  );
                  return (
                    <p key={key} className={styles.matrixSource}>
                      {doc ? (
                        <a
                          href={relatedDocumentFileHref(
                            registerPrefix,
                            doc.filename,
                          )}
                        >
                          {doc.title}
                        </a>
                      ) : (
                        <a
                          href={source.documentUrl}
                          target="_blank"
                          rel="noreferrer"
                        >
                          {documentName(source.documentUrl)}
                        </a>
                      )}
                      , {source.pages}
                    </p>
                  );
                })}
              </dd>
            </>
          )}
          {corrections.length > 0 && (
            <>
              <dt className="micro-label">Corrected deliveries</dt>
              <dd>
                <ul className={styles.facts}>
                  {corrections.map((c) => (
                    <li
                      key={`${stateKey(c.state)}:${c.className}:${c.sourceEditions.join(",")}`}
                      title={interval(c.state)}
                    >
                      <KeyValue rows={correctionRows(c)} />
                      {c.evidence && (
                        <p className={styles.evidence}>{c.evidence}</p>
                      )}
                    </li>
                  ))}
                </ul>
              </dd>
            </>
          )}
          {inferredGaps.length > 0 && (
            <IntervalFacts
              title="Inferred intervals"
              states={inferredGaps}
              attribution="Unattributed; retained by catalog resolution"
            />
          )}
          {pooledStates.length > 0 && (
            <IntervalFacts
              title="Pooled intervals"
              states={pooledStates}
              attribution="Pooled source edition; annual availability inside the interval is not inferred"
            />
          )}
        </dl>
      </TechnicalDetails>
    </>
  );
}

function correctionRows(c: Correction): KeyValueRow[] {
  return [
    { label: "Variant", value: c.state.variant_label ?? c.state.variant },
    {
      label: c.providerDocumented ? "Catalog interval" : "Interval",
      value: formatStateWindow(c.state) ?? interval(c.state),
      mono: true,
    },
    ...(c.providerDocumented
      ? [
          {
            label: "Attribution",
            value: "Provider-documented with a scoped correction",
          },
        ]
      : []),
    ...(c.sourceEditions.length > 0
      ? [
          {
            label:
              c.sourceEditions.length === 1
                ? "Source edition"
                : "Source editions",
            value: c.sourceEditions.join(", "),
            mono: true,
          },
        ]
      : []),
    { label: "Class", value: c.className, mono: true },
  ];
}

function IntervalFacts({
  title,
  states,
  attribution,
}: {
  title: string;
  states: VariableStateModel[];
  attribution: string;
}) {
  return (
    <>
      <dt className="micro-label">{title}</dt>
      <dd>
        <ul className={styles.facts}>
          {states.map((state) => (
            <li key={stateKey(state)} title={interval(state)}>
              <KeyValue
                rows={[
                  {
                    label: "Variant",
                    value: state.variant_label ?? state.variant,
                  },
                  { label: "Interval", value: interval(state), mono: true },
                  { label: "Attribution", value: attribution },
                ]}
              />
            </li>
          ))}
        </ul>
      </dd>
    </>
  );
}
