import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { serializeProjectData } from "./project_data";
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
  it("reads null when nothing is set", () => {
    expect(windowStore.value).toBeNull();
  });

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
  it("hydrates from the active project's window field", () => {
    projectStore.newProject(SEED);
    projectStore.updateField("window", { from: 1990, to: 2020 });
    expect(windowStore.value).toEqual({ from: 1990, to: 2020 });
  });

  it("a draft with no window reads null (its own absence, not the fallback)", () => {
    // Seed the fallback FIRST (no draft), then open a project with no window.
    windowStore.set({ from: 1970, to: 1980 });
    projectStore.newProject(SEED);
    // The draft is authoritative while it exists: its absent window wins over the
    // localStorage fallback.
    expect(windowStore.value).toBeNull();
  });

  it("set() with a draft mutates draft.window and marks the store dirty", () => {
    projectStore.newProject(SEED);
    expect(projectStore.dirty).toBe(false);
    windowStore.set({ from: 2012, to: 2018 });
    expect(projectStore.draft?.window).toEqual({ from: 2012, to: 2018 });
    // Write-back rides the store's dirty path (→ existing autosave persists it).
    expect(projectStore.dirty).toBe(true);
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
});

describe("windowStore — precedence", () => {
  it("the project draft wins over the localStorage fallback", () => {
    windowStore.set({ from: 1970, to: 1980 }); // fallback (no draft)
    projectStore.newProject(SEED);
    projectStore.updateField("window", { from: 2001, to: 2002 });
    expect(windowStore.value).toEqual({ from: 2001, to: 2002 });
  });
});
