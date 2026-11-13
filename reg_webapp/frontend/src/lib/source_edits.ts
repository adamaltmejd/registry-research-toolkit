import {
  isCatalogBinding,
  isCatalogSource,
  type ProjectBinding,
  type ProjectData,
  type ProjectSource,
  type ProjectSourcePeriod,
} from "./project_data";
import { projectStore } from "./project_store";
import { checkProject } from "./reg_core";

/** Check an edited project and hand its sources and panels to the store. Throws
 * the check's message when the edit would not be a valid project. */
export function commitSourceEdit(candidate: ProjectData): void {
  const checked = checkProject(JSON.stringify(candidate));
  if (!checked.ok)
    throw new Error(checked.issues.map((issue) => issue.message).join(" "));
  projectStore.updateField({
    sources: candidate.sources,
    panels: candidate.panels,
  });
}

function editBinding(
  project: ProjectData,
  sourceIndex: number,
  bindingIndex: number,
  edit: (binding: ProjectBinding) => ProjectBinding,
): ProjectData {
  return {
    ...project,
    sources: project.sources.map((source, index) =>
      index === sourceIndex
        ? {
            ...source,
            bindings: source.bindings.map((binding, index) =>
              index === bindingIndex ? edit(binding) : binding,
            ),
          }
        : source,
    ),
  } as ProjectData;
}

export function pinClassification(
  project: ProjectData,
  sourceIndex: number,
  bindingIndex: number,
  classification: string | null,
): ProjectData {
  return editBinding(project, sourceIndex, bindingIndex, (binding) => {
    if (!isCatalogBinding(binding)) return binding;
    const next = { ...binding };
    if (classification) next.classification = classification;
    else delete next.classification;
    return next;
  });
}

/** Whether removing this column takes the whole source with it: its last
 * column, or the last catalog column of a catalog source. A catalog source
 * keeps at least one catalog column (`catalog_source_unbound`), so the columns
 * entered by hand beside it go with it. */
export function removalRemovesSource(
  source: ProjectSource,
  bindingIndex: number,
): boolean {
  const kept = source.bindings.filter((_, index) => index !== bindingIndex);
  return (
    kept.length === 0 ||
    (isCatalogSource(source) && !kept.some(isCatalogBinding))
  );
}

export function removeBinding(
  project: ProjectData,
  sourceIndex: number,
  bindingIndex: number,
): ProjectData {
  const removed = project.sources[sourceIndex];
  if (removed && removalRemovesSource(removed, bindingIndex))
    return removeSource(project, sourceIndex);
  return {
    ...project,
    sources: project.sources.map((source, index) =>
      index === sourceIndex
        ? {
            ...source,
            bindings: source.bindings.filter(
              (_, index) => index !== bindingIndex,
            ),
          }
        : source,
    ),
  } as ProjectData;
}

export function removeSource(
  project: ProjectData,
  sourceIndex: number,
): ProjectData {
  const sourceName = project.sources[sourceIndex]?.name;
  return {
    ...project,
    sources: project.sources.filter((_, index) => index !== sourceIndex),
    ...(project.panels && {
      panels: project.panels
        .map((panel) => ({
          ...panel,
          members: panel.members.filter((member) => member !== sourceName),
        }))
        .filter((panel) => panel.members.length > 0),
    }),
  };
}

export function setSourcePeriod(
  project: ProjectData,
  sourceIndex: number,
  period: ProjectSourcePeriod,
): ProjectData {
  return {
    ...project,
    sources: project.sources.map((source, index) =>
      index === sourceIndex ? { ...source, period } : source,
    ),
  };
}
