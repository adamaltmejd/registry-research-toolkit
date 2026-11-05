import type { Context, ProviderShow } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref, sortedRegisters } from "./catalog";
import { dataAxis } from "./period";
import { datedYearRuns, studyWindow } from "./study_window";
import { Definitions } from "./ui/Definitions";
import { EmptyState } from "./ui/EmptyState";
import { Panel } from "./ui/Panel";
import { VariableRow, VariableRowList } from "./ui/VariableRow";
import { useProject } from "./use_project";
import { windowBounds } from "./window_bounds";

/** A provider's registers as the register page lists its variables: one row
 * per register, its years as one span on the page's axis, under the panel's
 * one heading. */
export function ProviderView({
  node,
  context,
}: {
  node: ProviderShow;
  context: Context | null;
}) {
  const { draft } = useProject();
  const study = studyWindow(draft);
  const catalogBounds = windowBounds(context);
  const bounds = [catalogBounds.min, catalogBounds.max] as const;
  const registers = sortedRegisters(node.registers).map((register) => ({
    register,
    years: datedYearRuns([register], bounds),
  }));
  const axis = dataAxis(
    registers.flatMap(({ years }) =>
      years.flatMap((run) => [run.from, run.to]),
    ),
    study,
    bounds,
  );
  const bound =
    draft?.sources.flatMap((source) =>
      source.bindings.map((binding) => binding.variable),
    ) ?? [];
  return (
    <article className={styles.page}>
      <Definitions title={node.name} />
      <Panel title="Registers" flush={registers.length > 0}>
        {registers.length === 0 ? (
          <EmptyState title="This provider has no registers in the catalog." />
        ) : (
          <VariableRowList label="Registers" axis={axis}>
            {registers.map(({ register, years }) => (
              <VariableRow
                key={register.fqid}
                name={register.name}
                href={catalogHref(register.fqid)}
                inProject={bound.some((fqid) =>
                  fqid.startsWith(`${register.fqid}/`),
                )}
                coverage={years.map((run) => [run.from, run.to] as const)}
                axis={axis}
                window={study ? [study.from, study.to] : null}
              />
            ))}
          </VariableRowList>
        )}
      </Panel>
    </article>
  );
}
