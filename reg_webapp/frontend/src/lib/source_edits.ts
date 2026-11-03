import type {
  ProjectBinding,
  ProjectData,
  ProjectSourcePeriod,
} from "./project_data";

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
  };
}

export function setBindingType(
  project: ProjectData,
  sourceIndex: number,
  bindingIndex: number,
  type: ProjectBinding["type"],
): ProjectData {
  return editBinding(project, sourceIndex, bindingIndex, (binding) => {
    const next = { ...binding, type };
    if (type !== "id") delete next.id_subtype;
    if (type !== "numeric") delete next.numeric_subtype;
    if (type !== "date") delete next.date_format;
    if (type !== "datetime") delete next.datetime_format;
    return next;
  });
}

export function pinClassification(
  project: ProjectData,
  sourceIndex: number,
  bindingIndex: number,
  classification: string | null,
): ProjectData {
  return editBinding(project, sourceIndex, bindingIndex, (binding) => {
    const next = { ...binding };
    if (classification) next.classification = classification;
    else delete next.classification;
    return next;
  });
}

export function removeBinding(
  project: ProjectData,
  sourceIndex: number,
  bindingIndex: number,
): ProjectData {
  if (project.sources[sourceIndex]?.bindings.length === 1)
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
  };
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
