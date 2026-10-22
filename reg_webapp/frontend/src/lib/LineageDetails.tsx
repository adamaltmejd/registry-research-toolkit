import { getLineage, type VariableShow } from "./api";
import { catalogHref, formatWindow, windowTitle } from "./catalog";
import styles from "./LeafPanels.module.css";
import { SubjectSection } from "./SubjectView";
import { useAsyncResource } from "./use_async_resource";

/**
 * The lineage the relationship graph (#761) does not carry (#678), on the
 * binding leaf:
 *
 * - PROVENANCE: the composite register's `source_register_text` from `node`,
 *   shown at once, and the `lineage` read's edges, each a validity window and
 *   its source.
 * - WARNINGS: the same read's warnings. The read is one failure domain; its
 *   loading and error render inline here and never blank the leaf.
 *
 * A section is shown when it has data or (warnings) its state is unknown —
 * hiding an unknown would read as a confirmed absence. Both empty: nothing.
 */
export function LineageDetails({ node }: { node: VariableShow }) {
  const lineage = useAsyncResource(node.fqid, () => getLineage(node.fqid));
  const edges = lineage.data?.edges ?? [];
  const sourceRegister = node.source_register_text ?? null;
  const showProvenance = edges.length > 0 || sourceRegister != null;
  const showWarnings =
    lineage.loading ||
    lineage.error !== null ||
    (lineage.data?.warnings.length ?? 0) > 0;

  return (
    <>
      {showProvenance && (
        <SubjectSection title="Provenance">
          {sourceRegister && (
            <p className={styles.sourceRegister}>
              <span className="muted">Source register:</span> {sourceRegister}
            </p>
          )}
          {edges.length > 0 && (
            <ul className={styles.list}>
              {edges.map((edge) => (
                <li
                  key={`${edge.consumer_state_id}:${edge.source_state_id}`}
                  className={styles.item}
                >
                  {/* #309: sentinel-free window, raw ISO on the tooltip. */}
                  <span
                    className={`muted ${styles.small}`}
                    title={windowTitle(edge.valid_from, edge.valid_to)}
                  >
                    {formatWindow(edge.valid_from, edge.valid_to)}
                  </span>
                  {edge.source_fqid ? (
                    <span>
                      ←{" "}
                      <a href={catalogHref(edge.source_fqid)}>
                        {edge.source_fqid}
                      </a>
                    </span>
                  ) : (
                    <span className="muted">
                      ← source state #{edge.source_state_id}
                    </span>
                  )}
                </li>
              ))}
            </ul>
          )}
        </SubjectSection>
      )}
      {showWarnings && (
        <SubjectSection title="Lineage warnings">
          {lineage.loading ? (
            <p className="muted" aria-busy="true">
              Loading…
            </p>
          ) : lineage.error !== null ? (
            <p className="error" role="alert">
              Failed to load lineage: {lineage.error}
            </p>
          ) : (
            <ul className={styles.list}>
              {lineage.data?.warnings.map((w) => (
                <li
                  key={`${w.consumer_state_id}:${w.warning_kind}`}
                  className={styles.item}
                >
                  <code className={styles.warnKind}>{w.warning_kind}</code>
                  <span>{w.message}</span>
                </li>
              ))}
            </ul>
          )}
        </SubjectSection>
      )}
    </>
  );
}
