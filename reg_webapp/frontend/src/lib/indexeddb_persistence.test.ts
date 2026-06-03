import "fake-indexeddb/auto";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { IndexedDBPersistence, restoredDraft } from "./indexeddb_persistence";
import { newProjectData, type ProjectData } from "./project_data";

const SEED = {
  reg_meta_version: "reg_meta/v1.0.0",
  steward: "global" as const,
};

function makeDraft(name: string): ProjectData {
  const draft = newProjectData(SEED);
  draft.name = name;
  return draft;
}

/** Reset the DB between cases so fake-indexeddb state never bleeds. */
beforeEach(async () => {
  await new Promise<void>((resolve) => {
    const req = indexedDB.deleteDatabase("reg_webapp_projects");
    req.onsuccess = () => resolve();
    req.onerror = () => resolve();
    req.onblocked = () => resolve();
  });
});

describe("restoredDraft (pure gate)", () => {
  it("returns the draft when the stamped schemaVersion matches", () => {
    const draft = makeDraft("matched");
    expect(restoredDraft({ draft, schemaVersion: 1 }, 1)).toBe(draft);
  });

  it("returns null when the stamped schemaVersion differs", () => {
    const draft = makeDraft("stale");
    expect(restoredDraft({ draft, schemaVersion: 1 }, 2)).toBeNull();
  });

  it("returns null for a missing record", () => {
    expect(restoredDraft(undefined, 1)).toBeNull();
  });
});

describe("IndexedDBPersistence", () => {
  it("round-trips a saved draft (load deep-equals what was saved)", async () => {
    const draft = makeDraft("round-trip");
    const p = new IndexedDBPersistence("current", 1);
    await p.save("current", draft, 1);
    const loaded = await p.load();
    expect(loaded).toEqual(draft);
  });

  it("gates restore on the stored schemaVersion (mismatch → null)", async () => {
    const draft = makeDraft("v1-only");
    await new IndexedDBPersistence("current", 1).save("current", draft, 1);
    const loaded = await new IndexedDBPersistence("current", 2).load();
    expect(loaded).toBeNull();
  });
});

describe("graceful degradation (the mandatory no-throw contract)", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("save resolves and load returns null when IndexedDB is unavailable", async () => {
    // Private mode / disabled storage: `indexedDB` is absent. The debounced
    // autosave $effect must never see a rejection, and restore must yield null.
    vi.stubGlobal("indexedDB", undefined);
    const p = new IndexedDBPersistence("current", 1);
    await expect(
      p.save("current", makeDraft("offline"), 1),
    ).resolves.toBeUndefined();
    await expect(p.load()).resolves.toBeNull();
  });
});
