import { getValues } from "./api";
import styles from "./CatalogNodeView.module.css";
import { humanizeClassificationSlug } from "./catalog";
import { Definitions } from "./ui/Definitions";
import { Panel } from "./ui/Panel";
import { Skeleton } from "./ui/Skeleton";
import { StatusRow } from "./ui/StatusRow";
import { useAsyncResource } from "./use_async_resource";
import { ValueSetCodes } from "./ValueSetCodes";

/** Classifications are values subjects, never Show nodes. */
export function ClassificationView({ fqidPath }: { fqidPath: string }) {
  // Read metadata before choosing a flat list or level-one hierarchy.
  const resource = useAsyncResource(fqidPath, (signal) =>
    getValues({ classification: fqidPath }, { limit: 1 }, { signal }),
  );
  if (resource.loading)
    return (
      <section className={styles.loading} aria-busy="true">
        <Skeleton count={3} />
      </section>
    );
  if (resource.error || !resource.data)
    return (
      <StatusRow tone="error" live="alert">
        {resource.status === 404
          ? "This classification version was not found. Check the link or choose a variable in the catalog."
          : `This classification did not load (${resource.error}). Reload the page to try again.`}
      </StatusRow>
    );
  const valueSet = resource.data.value_set;
  return (
    <article className={styles.page}>
      <Definitions
        title={humanizeClassificationSlug(
          fqidPath.split("/").at(-1) ?? "Classification",
        )}
        definitions={[
          {
            term: "Names",
            value:
              valueSet.names.map(humanizeClassificationSlug).join(", ") ||
              "Not stated",
          },
        ]}
      />
      <Panel title="Codes">
        <ValueSetCodes
          subject={{ classification: fqidPath }}
          valueSet={valueSet}
        />
      </Panel>
    </article>
  );
}
