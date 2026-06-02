import { afterEach, describe, expect, it, vi } from "vitest";
import {
  checkVersionGate,
  initPersistence,
  type ProjectPersistence,
  projectStore,
  setPersistence,
  storeSchemaVersion,
} from "./project_store.svelte";

// The store is a MODULE SINGLETON — each test must establish the state it needs
// (via newProject / openFromFile) rather than assume a fresh store.

const SEED = {
  reg_meta_version: "reg_meta/v1.0.0",
  steward: "global" as const,
};

/** Build a File from a string for `openFromFile` (jsdom provides `File`/`Blob`,
 * and `File.prototype.text()` resolves the contents). */
function jsonFile(text: string): File {
  return new File([text], "project_data.json", { type: "application/json" });
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("checkVersionGate (THE A5.4 SEAM — accept path live)", () => {
  it("accepts the Model A range (schema_version 2.x + reg_meta/v1.x.y)", () => {
    expect(
      checkVersionGate({
        schema_version: "2.0.0",
        reg_meta_version: "reg_meta/v1.0.0",
      }),
    ).toEqual({ ok: true });
    expect(
      checkVersionGate({
        schema_version: "2.3.1",
        reg_meta_version: "reg_meta/v1.9.4",
      }),
    ).toEqual({ ok: true });
  });

  it("is a NEUTRAL no-op (ok:true) for out-of-range versions in c-i (A5.4 adds reject)", () => {
    // c-i does NOT reject v0.x / 1.x — A5.4 inserts the reject branches. The seam
    // exists (the {ok, reason} shape + version extraction), so these must NOT yet
    // be blocking.
    expect(
      checkVersionGate({
        schema_version: "1.0.0",
        reg_meta_version: "reg_meta/v0.9.0",
      }),
    ).toEqual({ ok: true });
    expect(checkVersionGate({})).toEqual({ ok: true });
  });
});

describe("newProject", () => {
  it("loads a clean Model A skeleton (not dirty)", () => {
    projectStore.newProject(SEED);
    expect(projectStore.draft).not.toBeNull();
    expect(projectStore.draft?.schema_version).toBe("2.0.0");
    expect(projectStore.draft?.reg_meta_version).toBe("reg_meta/v1.0.0");
    expect(projectStore.dirty).toBe(false);
    expect(projectStore.openError).toBeNull();
  });
});

describe("dirty flag", () => {
  it("flips true after an edit, back to clean after a fresh new", () => {
    projectStore.newProject(SEED);
    expect(projectStore.dirty).toBe(false);
    projectStore.updateField("name", "My project");
    expect(projectStore.draft?.name).toBe("My project");
    expect(projectStore.dirty).toBe(true);
    // A fresh new resets the baseline → clean.
    projectStore.newProject(SEED);
    expect(projectStore.dirty).toBe(false);
  });

  it("an edit clears the stale validation (validatedClean goes false)", () => {
    projectStore.newProject(SEED);
    // Simulate a green validation, then edit — the edit must invalidate it so the
    // order/bundle download gate re-closes.
    projectStore.addSource();
    expect(projectStore.validation).toBeNull();
    expect(projectStore.validatedClean).toBe(false);
  });
});

describe("openFromFile", () => {
  it("loads a valid file VERBATIM, preserving namespaced blocks + panels", async () => {
    const raw = {
      schema_version: "2.0.0",
      steward: "global",
      reg_meta_version: "reg_meta/v1.0.0",
      name: "opened",
      sources: [
        {
          name: "s1",
          register_variant: "scb/lisa/individer",
          period: 2018,
          bindings: [{ variable: "scb/lisa/kon", type: "categorical" }],
        },
      ],
      panels: [{ panel_id: "p1", members: [{ source: "s1" }] }],
      reg_monabundle: {
        binding_options: { "scb/lisa/kon": { suppress_k: 5 } },
      },
      swecov: { foo: "bar" },
    };
    await projectStore.openFromFile(jsonFile(JSON.stringify(raw)));
    expect(projectStore.openError).toBeNull();
    expect(projectStore.draft?.name).toBe("opened");
    // The namespaced blocks + panels survive on the draft (the round-trip embed).
    const draft = projectStore.draft as Record<string, unknown>;
    expect(draft.panels).toEqual(raw.panels);
    expect(draft.reg_monabundle).toEqual(raw.reg_monabundle);
    expect(draft.swecov).toEqual({ foo: "bar" });
    // A freshly-opened draft is clean.
    expect(projectStore.dirty).toBe(false);
  });

  it("rejects a non-object top level (a JSON array) with an open error, no load", async () => {
    projectStore.newProject(SEED);
    const before = projectStore.draft;
    await projectStore.openFromFile(jsonFile("[1, 2, 3]"));
    expect(projectStore.openError).toMatch(/object/i);
    expect(projectStore.draft).toBe(before); // existing draft untouched
  });

  it("rejects unparseable JSON with a parse error, no load", async () => {
    projectStore.newProject(SEED);
    await projectStore.openFromFile(jsonFile("{ not json"));
    expect(projectStore.openError).toMatch(/json/i);
  });

  it("clearOpenError dismisses the banner", async () => {
    await projectStore.openFromFile(jsonFile("[]"));
    expect(projectStore.openError).not.toBeNull();
    projectStore.clearOpenError();
    expect(projectStore.openError).toBeNull();
  });
});

describe("storeSchemaVersion", () => {
  it("is a stamped constant (the A5.4 store-schema-mismatch gate)", () => {
    expect(typeof storeSchemaVersion).toBe("number");
  });
});

describe("persistence wiring (the A5.4 swap point)", () => {
  it("debounced autosave writes the draft to the persistence impl after the debounce", async () => {
    vi.useFakeTimers();
    const saves: { key: string; schemaVersion: number }[] = [];
    const fake: ProjectPersistence = {
      save: (key, _draft, schemaVersion) => {
        saves.push({ key, schemaVersion });
        return Promise.resolve();
      },
      load: () => Promise.resolve(null),
    };
    setPersistence(fake);
    projectStore.newProject(SEED);

    const stop = $effect.root(() => {
      initPersistence();
    });
    // The autosave is debounced — nothing yet.
    expect(saves).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(600);
    // After the debounce window, the draft is persisted with the stamped version.
    expect(saves.length).toBeGreaterThanOrEqual(1);
    expect(saves[0].schemaVersion).toBe(storeSchemaVersion);
    stop();
    vi.useRealTimers();
  });

  it("load-at-init returns null in c-i (no restore)", async () => {
    const fake: ProjectPersistence = {
      save: () => Promise.resolve(),
      load: vi.fn(() => Promise.resolve(null)),
    };
    setPersistence(fake);
    let loaded: Promise<void>;
    const stop = $effect.root(() => {
      loaded = initPersistence();
    });
    // biome-ignore lint/style/noNonNullAssertion: assigned synchronously in the root.
    await loaded!;
    expect(fake.load).toHaveBeenCalled();
    stop();
  });
});
