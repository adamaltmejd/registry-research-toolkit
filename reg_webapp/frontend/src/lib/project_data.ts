/** The 5.1 project document. reg-core checks its shape at every read boundary. */
import type { components } from "./api-types-rust";
import { projectSchemaVersion } from "./reg_core";

type Schemas = components["schemas"];

export type ProjectData = Schemas["ProjectData"];
export type ProjectSource = Schemas["ProjectSource"];
export type ProjectCatalogSource = Schemas["ProjectCatalogSource"];
export type ProjectManualSource = Schemas["ProjectManualSource"];
export type ProjectBinding = Schemas["ProjectBinding"];
export type ProjectCatalogBinding = Schemas["ProjectCatalogBinding"];
export type ProjectManualBinding = Schemas["ProjectManualBinding"];
export type ProjectColumnType = Schemas["ProjectColumnType"];
export type ProjectSourcePeriod = Schemas["ProjectSourcePeriod"];
export type ProjectPeriodSegment = Schemas["ProjectPeriodSegment"];
export type ProjectStudyWindow = Schemas["ProjectStudyWindow"];

/** A source of a catalog table. The other kind is data entered by hand
 * ("manual"): it names no catalog table, so nothing reads the catalog for it. */
export const isCatalogSource = (
  source: ProjectSource,
): source is ProjectCatalogSource => "table" in source;

export const isCatalogBinding = (
  binding: ProjectBinding,
): binding is ProjectCatalogBinding => "variable" in binding;

export const isManualBinding = (
  binding: ProjectBinding,
): binding is ProjectManualBinding => "manual" in binding;

/** The catalog variables a source binds; none for a manual source. */
export function catalogBindings(
  source: ProjectSource,
): ProjectCatalogBinding[] {
  return isCatalogSource(source)
    ? source.bindings.filter(isCatalogBinding)
    : [];
}

export interface ProjectSeed {
  reg_meta_version: string;
  name?: string;
  window?: ProjectStudyWindow;
}

export function newProjectData(seed: ProjectSeed): ProjectData {
  return {
    schema_version: projectSchemaVersion(),
    reg_meta_version: seed.reg_meta_version,
    name: seed.name ?? "new_project",
    sources: [],
    ...(seed.window ? { window: { ...seed.window } } : {}),
  };
}

export function regMetaReleaseTag(packageVersion: string): string {
  return packageVersion ? `reg_meta/v${packageVersion}` : "";
}

/** This accessor also serves the no-project study-window store. */
export function safeStudyWindow(value: unknown): ProjectStudyWindow | null {
  if (value === null || typeof value !== "object") return null;
  const window = value as { from?: unknown; to?: unknown };
  return Number.isInteger(window.from) && Number.isInteger(window.to)
    ? (value as ProjectStudyWindow)
    : null;
}
