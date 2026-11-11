import { useState } from "react";
import type { Context, ProviderShow } from "./api";
import styles from "./CatalogNodeView.module.css";
import { catalogHref, leafSlug, rankFilter, sortedRegisters } from "./catalog";
import { segmentYears } from "./catalog_period";
import { FilterInput } from "./FilterInput";
import { Highlight } from "./Highlight";
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
 * one heading, with the register page's filter box over them (#2407). */
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
    years: datedYearRuns(
      register.periods.map((period) => ({ period })),
      bounds,
    ),
  }));
  // The axis spans every register, filtered or not, so a filter never
  // rescales the bars.
  const axis = dataAxis(
    registers.flatMap(({ years }) =>
      years.flatMap((run) => [run.from, run.to]),
    ),
    study,
    bounds,
  );
  // The page is keyed by address, so the filter starts empty on each.
  const [needle, setNeedle] = useState("");
  const shown = rankFilter(registers, needle, ({ register }) => [
    register.name,
    leafSlug(register.fqid),
  ]);
  const bound =
    draft?.sources.flatMap((source) =>
      source.bindings.map((binding) => binding.variable),
    ) ?? [];
  return (
    <article className={styles.page}>
      <Definitions title={node.name} />
      <Panel
        title={registers.length === 0 ? "Registers" : undefined}
        flush={registers.length > 0}
      >
        {registers.length === 0 ? (
          <EmptyState title="This provider has no registers in the catalog." />
        ) : (
          <>
            <div className={styles.listFilter}>
              <FilterInput
                value={needle}
                onChange={setNeedle}
                total={registers.length}
                shown={shown.length}
                label="Filter this provider's registers"
                placeholder="Filter registers…"
              />
            </div>
            {shown.length === 0 ? (
              <EmptyState
                title={`No registers match “${needle.trim()}”.`}
                description="Clear the filter to list them all."
              />
            ) : (
              <VariableRowList label="Registers" axis={axis}>
                {shown.map(({ register, years }) => (
                  <VariableRow
                    key={register.fqid}
                    name={<Highlight text={register.name} needle={needle} />}
                    href={catalogHref(register.fqid)}
                    inProject={bound.some((fqid) =>
                      fqid.startsWith(`${register.fqid}/`),
                    )}
                    coverage={years}
                    years={segmentYears(
                      register.periods.map((period) => ({ period })),
                    )}
                    axis={axis}
                    window={study ? [study.from, study.to] : null}
                  />
                ))}
              </VariableRowList>
            )}
          </>
        )}
      </Panel>
    </article>
  );
}
