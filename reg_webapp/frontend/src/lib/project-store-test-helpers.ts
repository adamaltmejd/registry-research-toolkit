// Shared fixtures for the project_store tests: the seed, a resolved staged add,
// Open, and the draft read for assertions. Pure: import from any test.
import { newProjectData, type ProjectData } from "./project_data";
import { projectStore, type StagedAdd } from "./project_store";

export const SEED = {
  reg_meta_version: "reg_meta/v1.0.0",
  steward: "global" as const,
};

/** A staged add of `variable` on `registerVariant` at `period`, with a concrete
 * (already-resolved) type — the #991 write-once shape `applyStagedDiff` commits. */
export function add(
  registerVariant: string,
  variable: string,
  period: StagedAdd["period"],
  over: Partial<StagedAdd["binding"]> = {},
): StagedAdd {
  return {
    registerVariant,
    period,
    binding: { variable, type: "categorical", ...over },
  };
}

/** Both halves of an Open in one call: the file ingress, then the commit of what
 * it accepted. The toolbar keeps them apart so the deliberate-replacement
 * confirmation can sit between them (and so a superseded read is dropped before
 * the ingress runs); a caller that has already decided the current draft may go
 * wants the pair. */
export function openFile(json: string): void {
  const parsed = projectStore.parseProjectText(json);
  if (parsed != null) {
    projectStore.loadProject(parsed);
  }
}

/** A fresh draft from `SEED` with a name: the order gate stays closed on a blank
 * name, so a test that expects it open starts from this, not `newProject`. */
export function openNamedProject(): void {
  projectStore.loadProject({ ...newProjectData(SEED), name: "Named" });
}

/** The draft read as the canonical document, for asserting what a write produced.
 * A test-only cast: the draft itself is raw (`RawDraft`), and a test that loads a
 * malformed file reads the malformed value through it unchanged. */
export function storedProject(): ProjectData | null {
  return projectStore.draft as ProjectData | null;
}
