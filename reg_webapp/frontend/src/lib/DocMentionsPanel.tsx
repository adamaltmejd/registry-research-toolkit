import { getDocsForVariable, type VariableShow } from "./api";
import { fqidSegments, showingOf } from "./catalog";
import { parseInlineMarkdown } from "./inline_markdown";
import styles from "./LeafPanels.module.css";
import { SubjectSection } from "./SubjectView";
import { Skeleton } from "./ui/Skeleton";
import { useAsyncResource } from "./use_async_resource";

function sourceLabel(url: string, title?: string | null): string {
  if (title) return `Source PDF: ${title}`;
  try {
    return `Source PDF: ${new URL(url).hostname}`;
  } catch {
    return "Source PDF";
  }
}

/**
 * "Parsed documentation" on the binding leaf (#402/#967): a fuzzy name match
 * over the separate, optional docs index, so it is its own failure domain — a
 * docs error, timeout or missing index stays one inline line here. The section
 * is omitted once we know there is nothing to show (no index, the register not
 * ingested, no hits), never while loading or on error.
 */
export function DocMentionsPanel({ node }: { node: VariableShow }) {
  const register = fqidSegments(node.fqid).slice(0, 2).join("/");
  const q = node.name?.trim() || (fqidSegments(node.fqid)[2] ?? "");
  const resource = useAsyncResource(`${register}\u0000${q}`, (signal) =>
    getDocsForVariable(q, { register, limit: 5, signal }),
  );
  const data = resource.data;
  const results = data?.items ?? [];
  const show =
    resource.loading ||
    resource.error !== null ||
    (!!data && data.register_ingested && results.length > 0);
  if (!show) return null;
  const caption = data ? showingOf(results.length, data.total) : null;

  return (
    <SubjectSection title="Parsed documentation">
      {resource.loading ? (
        <div aria-busy="true">
          <Skeleton count={2} />
        </div>
      ) : resource.error !== null ? (
        <p className="error" role="alert">
          <span aria-hidden="true">✕</span> Failed to load parsed documentation:{" "}
          {resource.error}
        </p>
      ) : (
        <>
          {/* #670: a grouped member shares its concept name, so these hits are
              the group's, not this member's. */}
          {node.group && (
            <p className={`muted ${styles.note}`}>
              Matches on the shared concept name — these mentions are common to
              the group's members, not specific to this variable.
            </p>
          )}
          <p className={`muted ${styles.note}`}>
            Heuristic name matches, not authoritative links between the variable
            and its documentation.
          </p>
          <ul className={styles.list}>
            {results.map((r, i) => (
              // Replaced wholesale per fetch, and a filename can repeat (#391),
              // so the position is part of the key.
              // biome-ignore lint/suspicious/noArrayIndexKey: positional
              // oxlint-disable-next-line react/no-array-index-key
              <li key={`${i}:${r.filename}`} className={styles.mention}>
                <a href={`/doc/${encodeURIComponent(r.filename)}`}>
                  <span className={styles.docLabel}>
                    {r.display_name ?? r.filename}
                  </span>
                </a>
                {r.snippet && (
                  // An FTS excerpt as data segments, never HTML: `**…**` is the
                  // highlight around the matched term (crates/reg-catalog
                  // ops/docs.rs), so it renders as <mark>.
                  <span className={`muted ${styles.snippet}`}>
                    {parseInlineMarkdown(r.snippet).map((seg, si) =>
                      seg.emphasis === "strong" ? (
                        // biome-ignore lint/suspicious/noArrayIndexKey: positional segments
                        // oxlint-disable-next-line react/no-array-index-key
                        <mark key={si}>{seg.text}</mark>
                      ) : seg.emphasis === "em" ? (
                        // biome-ignore lint/suspicious/noArrayIndexKey: positional segments
                        // oxlint-disable-next-line react/no-array-index-key
                        <em key={si}>{seg.text}</em>
                      ) : (
                        // biome-ignore lint/suspicious/noArrayIndexKey: positional segments
                        // oxlint-disable-next-line react/no-array-index-key
                        <span key={si}>{seg.text}</span>
                      ),
                    )}
                  </span>
                )}
                {r.source_url && (
                  <a
                    className={styles.sourceLink}
                    href={r.source_url}
                    target="_blank"
                    rel="noreferrer"
                  >
                    {sourceLabel(r.source_url, r.source_title)}
                  </a>
                )}
              </li>
            ))}
          </ul>
          {caption && <p className={`muted ${styles.note}`}>{caption}</p>}
        </>
      )}
    </SubjectSection>
  );
}
