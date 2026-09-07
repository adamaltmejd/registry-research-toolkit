import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { newProjectData, type ProjectData } from "./project_data";
import {
  initDraftLifecycle,
  projectStore,
  setPersistence,
} from "./project_store.svelte";

// The APPLICATION-owned draft lifecycle: the load-at-init restore and the
// `projectStore.restored` gate the catalog's Add path awaits before it creates or
// mutates a draft. Its own file because the store is a MODULE SINGLETON with no
// way back to `draft == null` — restoring onto an EMPTY store is only observable
// in a module registry no other test has already seeded with a draft.

const SEED = {
  reg_meta_version: "reg_meta/v1.0.0",
  steward: "global" as const,
};

/** A draft as it would come back off IndexedDB, named so the assertions can tell
 * it apart from a freshly created one. */
function savedDraft(name: string): ProjectData {
  const draft = newProjectData(SEED);
  draft.name = name;
  return draft;
}

beforeEach(() => {
  // The lifecycle also wires the automatic validation, which POSTs the draft —
  // answer it here so no case reaches the network.
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => ({
      ok: true,
      status: 200,
      json: async () => ({ ok: true, issues: [] }),
    })),
  );
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("draft restore + the authoring gate", () => {
  // FIRST, and it must stay first: the only case that needs a store no earlier
  // test has put a draft into.
  it("restores the autosaved draft onto an empty store before opening the gate", async () => {
    // The gate is already open when no lifecycle is running (a component
    // mounted on its own in a test) — awaiting it must not hang.
    await projectStore.restored;
    expect(projectStore.draft).toBeNull();

    setPersistence({
      save: () => Promise.resolve(),
      load: () => Promise.resolve(savedDraft("recovered")),
    });
    const replacements = projectStore.replacementGeneration;
    const stop = $effect.root(() => {
      initDraftLifecycle();
    });

    await projectStore.restored;
    expect(projectStore.draft?.name).toBe("recovered");
    // A restore is NOT a deliberate replacement. Catalog authoring discards a pick
    // queued behind the gate when this counter moves, so a bump here would throw
    // away the very cold-entry Add the restore exists to serve.
    expect(projectStore.replacementGeneration).toBe(replacements);
    // Recovery is not the durable copy: a restored draft has not been downloaded
    // this session, so it reads as dirty (the unsaved-changes warning).
    expect(projectStore.dirty).toBe(true);
    stop();
  });

  it("does not let a late restore overwrite a deliberate new project", async () => {
    // A restore that settles only on `release` — the in-flight read a cold entry
    // races against.
    const { promise: loaded, resolve: release } =
      Promise.withResolvers<ProjectData | null>();
    setPersistence({ save: () => Promise.resolve(), load: () => loaded });
    const stop = $effect.root(() => {
      initDraftLifecycle();
    });

    // The researcher acts while the restore is still in flight — a deliberate
    // replacement, which a pick queued behind the gate keys off.
    const replacements = projectStore.replacementGeneration;
    projectStore.newProject(SEED);
    projectStore.updateField("name", "deliberate");
    release(savedDraft("late-restore"));

    await projectStore.restored;
    expect(projectStore.draft?.name).toBe("deliberate");
    expect(projectStore.replacementGeneration).toBe(replacements + 1);
    stop();
  });

  it("opens the gate when storage fails, so authoring continues in memory", async () => {
    // The IndexedDB impl degrades to `null` itself, but the gate is now awaited
    // before every catalog Add: a load that REJECTS must still settle, or the
    // researcher's next pick never commits.
    const saves: ProjectData[] = [];
    setPersistence({
      save: (_key, draft) => {
        saves.push(draft);
        return Promise.resolve();
      },
      load: () => Promise.reject(new Error("IndexedDB unavailable")),
    });
    const stop = $effect.root(() => {
      initDraftLifecycle();
    });

    await expect(projectStore.restored).resolves.toBeUndefined();

    // Recoverability: the draft the researcher authors after the failed restore
    // is still autosaved (a later save may well succeed) and still downloadable.
    projectStore.newProject(SEED);
    projectStore.updateField("name", "after-storage-failure");
    await vi.waitFor(() => {
      expect(saves.at(-1)?.name).toBe("after-storage-failure");
    });
    stop();
  });
});
