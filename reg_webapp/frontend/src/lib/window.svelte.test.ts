import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  type ProjectData,
  type StudyWindow,
  serializeProjectData,
} from "./project_data";
import { projectStore } from "./project_store.svelte";
import { windowStore } from "./window.svelte";

// The window runtime layer is a MODULE SINGLETON over two backing stores (the
// active project draft + a localStorage fallback). Each test establishes the
// state it needs; `beforeEach` resets both backings to a known-empty baseline.

const SEED = {
  reg_meta_version: "reg_meta/v1.0.0",
  steward: "global" as const,
};

// jsdom in the `unit` project doesn't expose `localStorage`, so stub a minimal
// in-memory Storage for the fallback path (mirrors how project_store's tests stub
// `fetch`). The module-init read of localStorage is try/catch-guarded, so its
// absence at import is already harmless — this stub just lets the no-draft
// fallback round-trip in tests.
const storage = new Map<string, string>();
const localStorageStub = {
  getItem: (k: string) => storage.get(k) ?? null,
  setItem: (k: string, v: string) => storage.set(k, v),
  removeItem: (k: string) => storage.delete(k),
  clear: () => storage.clear(),
};

/** Drop any active draft + the localStorage fallback so each test starts from a
 * pristine no-window state. There is no store API to clear the draft to null
 * (the home screen is the only null path), so tests that need the no-draft path
 * open NO project; tests that need a draft call newProject themselves. */
beforeEach(() => {
  vi.stubGlobal("localStorage", localStorageStub);
  storage.clear();
  windowStore.set(null); // clears the fallback (no draft yet)
});

afterEach(() => {
  storage.clear();
  vi.unstubAllGlobals();
});

describe("windowStore — no active draft (localStorage fallback)", () => {
  it("set() with no draft writes the localStorage fallback and reads it back", () => {
    windowStore.set({ from: 2005, to: 2015 });
    expect(windowStore.value).toEqual({ from: 2005, to: 2015 });
    // Persisted to localStorage under the namespaced key.
    expect(
      JSON.parse(localStorage.getItem("reg_webapp:project_window") ?? "null"),
    ).toEqual({ from: 2005, to: 2015 });
  });

  it("set(null) clears the fallback", () => {
    windowStore.set({ from: 2000, to: 2010 });
    windowStore.set(null);
    expect(windowStore.value).toBeNull();
    expect(localStorage.getItem("reg_webapp:project_window")).toBeNull();
  });
});

describe("windowStore — active draft (project hydrate + write-back)", () => {
  it("a draft with no window reads null (its own absence, not the fallback)", () => {
    // Seed the fallback FIRST (no draft), then reach a draft with no window: the
    // first New seeds the fallback onto its draft (#629), a New from within that
    // draft starts windowless (#634).
    windowStore.set({ from: 1970, to: 1980 });
    projectStore.newProject(SEED);
    projectStore.newProject(SEED);
    expect(projectStore.draft?.window).toBeUndefined();
    // The draft is authoritative while it exists: its absent window wins over the
    // localStorage fallback.
    expect(windowStore.value).toBeNull();
  });

  it("set() does NOT mirror into localStorage while a draft is active", () => {
    projectStore.newProject(SEED);
    windowStore.set({ from: 2012, to: 2018 });
    // The draft is the durable copy; the fallback stays untouched so it can't
    // resurrect a stale value once the store goes pristine again.
    expect(localStorage.getItem("reg_webapp:project_window")).toBeNull();
  });

  it("set(null) with a draft omits the window key (additive — serializes unchanged)", () => {
    projectStore.newProject(SEED);
    windowStore.set({ from: 2012, to: 2018 });
    windowStore.set(null);
    expect(windowStore.value).toBeNull();
    // Omitted, not present-as-null: the serialized draft has no `window` key.
    const draft = projectStore.draft;
    expect(draft).not.toBeNull();
    if (draft) {
      expect(JSON.parse(serializeProjectData(draft)).window).toBeUndefined();
    }
  });

  it("clampTo() rewrites an active draft window before export (#1037)", () => {
    projectStore.newProject(SEED);
    projectStore.updateField("window", { from: 1960, to: 2026 });

    windowStore.clampTo(2000, 2010);

    expect(projectStore.draft?.window).toEqual({ from: 2000, to: 2010 });
    const draft = projectStore.draft;
    expect(draft).not.toBeNull();
    if (draft) {
      expect(JSON.parse(serializeProjectData(draft)).window).toEqual({
        from: 2000,
        to: 2010,
      });
    }
  });
});

// The seed-on-create path (#629 item 3) reads the no-DRAFT fallback, but this
// file's module-singleton store keeps any draft a prior test created (there is no
// draft→null API — see the file header) and a lingering draft would route
// `set()` to the draft instead of the fallback. So these cases take a PRISTINE
// store: `vi.resetModules()` + a dynamic import re-runs the module-init read of
// `localStorage`, yielding a no-draft singleton whose fallback is whatever the
// (stubbed) storage holds — isolation-safe regardless of run order.
describe("browse-time window seeded on draft creation (#629 item 3)", () => {
  beforeEach(() => {
    vi.resetModules();
  });

  /** Pre-seed the storage fallback (or leave it empty), then import a fresh
   * (no-draft) pair of stores. */
  async function freshStores(fallback: StudyWindow | null) {
    if (fallback !== null) {
      storage.set("reg_webapp:project_window", JSON.stringify(fallback));
    }
    return {
      projectStore: (await import("./project_store.svelte")).projectStore,
      windowStore: (await import("./window.svelte")).windowStore,
    };
  }

  it("a fresh draft is seeded with the no-draft fallback window", async () => {
    const { projectStore, windowStore } = await freshStores({
      from: 2004,
      to: 2014,
    });
    projectStore.newProject(SEED);
    // The window carried over onto the new draft (not silently dropped to null).
    expect(projectStore.draft?.window).toEqual({ from: 2004, to: 2014 });
    expect(windowStore.value).toEqual({ from: 2004, to: 2014 });
    // The seed is the draft's INITIAL state, not an edit → the draft is clean.
    expect(projectStore.dirty).toBe(false);
  });

  it("New from WITHIN a draft does NOT seed from the stale fallback (#634)", async () => {
    // Fallback holds a browse-time window; first newProject (from the pristine
    // no-draft state) seeds it. The "New" button then calls newProject AGAIN
    // while that draft is active — the fallback is now the stale no-draft value
    // (active-draft window writes/clears don't touch it), so the second project
    // must start windowless, not silently inherit the old browse window.
    const { projectStore } = await freshStores({ from: 2004, to: 2014 });
    projectStore.newProject(SEED);
    expect(projectStore.draft?.window).toEqual({ from: 2004, to: 2014 });
    projectStore.newProject(SEED);
    expect(projectStore.draft?.window).toBeUndefined();
  });

  it("OPENING a project keeps its OWN window — the fallback never overwrites it", async () => {
    const { projectStore } = await freshStores({ from: 1970, to: 1980 });
    const parsed = projectStore.parseProjectText(
      JSON.stringify({
        schema_version: "2.0.0",
        steward: "global",
        reg_meta_version: "reg_meta/v1.0.0",
        name: "opened",
        sources: [],
        window: { from: 1995, to: 2005 },
      }),
    );
    expect(parsed).not.toBeNull();
    projectStore.loadProject(parsed as ProjectData);
    // The opened file's own window wins; the fallback is not seeded over it
    // (an open bypasses the newProject seed entirely).
    expect(projectStore.draft?.window).toEqual({ from: 1995, to: 2005 });
  });
});
