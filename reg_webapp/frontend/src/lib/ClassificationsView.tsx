import type { ClassFamiliesShow, Context } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref, svCollator } from "./catalog";
import { CLASSIFICATIONS_LABEL } from "./catalog_names";
import { dataAxis } from "./period";
import { datedYearRuns, studyWindow } from "./study_window";
import { Definitions } from "./ui/Definitions";
import { EmptyState } from "./ui/EmptyState";
import { Panel } from "./ui/Panel";
import { VariableRow, VariableRowList } from "./ui/VariableRow";
import { useProject } from "./use_project";
import { windowBounds } from "./window_bounds";

/** The list of classifications (`/catalog/class`), as a provider's page lists
 * its registers: one row per classification, by name in Swedish collation,
 * over the years from its first version's start to its last's end. */
export function ClassificationsView({
  node,
  context,
}: {
  node: ClassFamiliesShow;
  context: Context | null;
}) {
  const { draft } = useProject();
  const study = studyWindow(draft);
  const catalogBounds = windowBounds(context);
  const bounds = [catalogBounds.min, catalogBounds.max] as const;
  const families = [...node.families]
    .sort((a, b) => svCollator.compare(a.name, b.name))
    .map((family) => ({ family, years: datedYearRuns([family], bounds) }));
  const axis = dataAxis(
    families.flatMap(({ years }) => years.flatMap((run) => [run.from, run.to])),
    study,
    bounds,
  );
  return (
    <article className={styles.page}>
      <Definitions title={CLASSIFICATIONS_LABEL} />
      <Panel title="All classifications" flush={families.length > 0}>
        {families.length === 0 ? (
          <EmptyState title="The catalog has no classifications." />
        ) : (
          <VariableRowList label="Classifications" axis={axis}>
            {families.map(({ family, years }) => (
              <VariableRow
                key={family.fqid}
                name={family.name}
                href={catalogHref(family.fqid)}
                inProject={false}
                coverage={years}
                axis={axis}
                window={study ? [study.from, study.to] : null}
                detail={
                  family.version_count > 1
                    ? `${family.version_count} versions`
                    : undefined
                }
              />
            ))}
          </VariableRowList>
        )}
      </Panel>
    </article>
  );
}
