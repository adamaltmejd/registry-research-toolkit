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

/** Stub global `fetch` so the store's write endpoints (validate → apiPostJson)
 * resolve against a canned response. */
function stubFetch(
  impl: (url: string, init?: RequestInit) => Promise<unknown>,
): void {
  vi.stubGlobal(
    "fetch",
    vi.fn((url: string, init?: RequestInit) => impl(url, init)),
  );
}

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("checkVersionGate", () => {
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

  it("hard-rejects schema_version 1.x (pre-Model-A)", () => {
    const gate = checkVersionGate({
      schema_version: "1.2.0",
      reg_meta_version: "reg_meta/v1.0.0",
    });
    expect(gate.ok).toBe(false);
    expect(gate.reason).toMatch(/Model A|re-author/i);
  });

  it("hard-rejects reg_meta/v0.x (pre-Model-A) even with a 2.x schema", () => {
    const gate = checkVersionGate({
      schema_version: "2.0.0",
      reg_meta_version: "reg_meta/v0.9.0",
    });
    expect(gate.ok).toBe(false);
    expect(gate.reason).toMatch(/Model A|re-author/i);
  });

  it("hard-rejects when BOTH are v0 (schema 1.x AND reg_meta/v0.x)", () => {
    const gate = checkVersionGate({
      schema_version: "1.0.0",
      reg_meta_version: "reg_meta/v0.9.0",
    });
    expect(gate.ok).toBe(false);
    expect(gate.reason).toMatch(/Model A|re-author/i);
  });

  it("is a NEUTRAL no-op (ok:true) for unrecognized in-range-ish versions", () => {
    // Backend stays canonical: only v0.x is hard-rejected, everything else passes.
    expect(
      checkVersionGate({
        schema_version: "3.0.0",
        reg_meta_version: "reg_meta/v2.0.0",
      }),
    ).toEqual({ ok: true });
    expect(checkVersionGate({})).toEqual({ ok: true });
  });

  it("is a NEUTRAL no-op (ok:true) for malformed/non-numeric version strings", () => {
    expect(
      checkVersionGate({
        schema_version: "not-a-version",
        reg_meta_version: "reg_meta/vbogus",
      }),
    ).toEqual({ ok: true });
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

  it("an edit clears a GREEN validation (validatedClean goes false)", async () => {
    stubFetch(async () => ({
      ok: true,
      status: 200,
      json: async () => ({ ok: true, issues: [] }),
    }));
    projectStore.newProject(SEED);
    // Establish a REAL green validation first — otherwise the assertion is
    // vacuous (newProject already nulls validation).
    await projectStore.validate();
    expect(projectStore.validation?.ok).toBe(true);
    expect(projectStore.validatedClean).toBe(true);
    // An edit must invalidate it so the order/bundle download gate re-closes.
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

describe("validate (200 ok:false vs 4xx split + stale-response guard)", () => {
  it("stores a 200 ok:false result and does NOT set requestError (a validation failure is not a 4xx)", async () => {
    stubFetch(async () => ({
      ok: true,
      status: 200,
      json: async () => ({
        ok: false,
        issues: [{ level: "error", code: "x", path: "", message: "m" }],
      }),
    }));
    projectStore.newProject(SEED);
    const r = await projectStore.validate();
    expect(r?.ok).toBe(false);
    expect(projectStore.validation?.ok).toBe(false);
    expect(projectStore.validatedClean).toBe(false);
    expect(projectStore.requestError).toBeNull();
  });

  it("sets requestError (not validation) on a true 4xx malformed request", async () => {
    stubFetch(async () => ({
      ok: false,
      status: 400,
      json: async () => ({ detail: "request body is not a JSON object" }),
    }));
    projectStore.newProject(SEED);
    const r = await projectStore.validate();
    expect(r).toBeNull();
    expect(projectStore.requestError).toBe("request body is not a JSON object");
    expect(projectStore.validation).toBeNull();
  });

  it("discards a stale response when the draft changed mid-flight (no resurrected validatedClean)", async () => {
    // Defer the fetch resolution so we can edit DURING the request.
    let resolveFetch: (v: unknown) => void = () => {};
    stubFetch(
      () =>
        new Promise((res) => {
          resolveFetch = res;
        }),
    );
    projectStore.newProject(SEED);
    const pending = projectStore.validate();
    // Edit mid-flight → setDraft swaps the draft + clears validation.
    projectStore.updateField("name", "edited mid-flight");
    expect(projectStore.validation).toBeNull();
    // The stale GREEN response now arrives — it must NOT resurrect validation.
    resolveFetch({
      ok: true,
      status: 200,
      json: async () => ({ ok: true, issues: [] }),
    });
    await pending;
    expect(projectStore.validation).toBeNull();
    expect(projectStore.validatedClean).toBe(false);
  });
});

describe("downloadProject (dirty baseline reset)", () => {
  it("marks the draft clean by resetting the dirty baseline to the written text", () => {
    // jsdom doesn't implement object URLs; stub so triggerDownload runs.
    Object.defineProperty(URL, "createObjectURL", {
      value: vi.fn(() => "blob:mock"),
      configurable: true,
      writable: true,
    });
    Object.defineProperty(URL, "revokeObjectURL", {
      value: vi.fn(),
      configurable: true,
      writable: true,
    });
    projectStore.newProject(SEED);
    projectStore.updateField("name", "to download");
    expect(projectStore.dirty).toBe(true);
    projectStore.downloadProject();
    expect(projectStore.dirty).toBe(false);
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
    const saves: { key: string; draft: unknown; schemaVersion: number }[] = [];
    const fake: ProjectPersistence = {
      save: (key, draft, schemaVersion) => {
        saves.push({ key, draft, schemaVersion });
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
    // Regression: the persisted draft must be a plain $state.snapshot, not the live
    // rune proxy. IndexedDB structured-clones the stored value, and a proxy throws
    // DataCloneError — structuredClone here reproduces that exact failure mode.
    expect(() => structuredClone(saves[0].draft)).not.toThrow();
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
