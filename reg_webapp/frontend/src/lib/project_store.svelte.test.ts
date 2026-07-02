import { afterEach, describe, expect, it, vi } from "vitest";
import {
  checkVersionGate,
  initPersistence,
  type ProjectPersistence,
  projectStore,
  type StagedAdd,
  setPersistence,
  storeSchemaVersion,
} from "./project_store.svelte";

// The store is a MODULE SINGLETON — each test must establish the state it needs
// (via newProject / openFromFile) rather than assume a fresh store.

const SEED = {
  reg_meta_version: "reg_meta/v1.0.0",
  steward: "global" as const,
};

/** A staged add of `variable` on `registerVariant` at `period`, with a concrete
 * (already-resolved) type — the #991 write-once shape `applyStagedDiff` commits. */
function add(
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
    // A staged-diff edit must invalidate it so the order download gate re-closes.
    projectStore.applyStagedDiff({
      adds: [add("scb/lisa/v1", "scb/lisa/kon", 2018)],
    });
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
    projectStore.updateField("name", "persisted-name-check");

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
    // …and the snapshot carries the real edited content (not an empty/dropped
    // object that would also clone fine).
    expect((saves[0].draft as { name?: string }).name).toBe(
      "persisted-name-check",
    );
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

describe("applyStagedDiff (#992 — one atomic commit path)", () => {
  it("adds find-or-create by register_variant ALONE (a second add of the same variant appends, not a new source)", () => {
    projectStore.newProject(SEED);
    projectStore.applyStagedDiff({
      adds: [
        add("scb/lisa/v1", "scb/lisa/kon", 2018),
        add("scb/lisa/v1", "scb/lisa/alder", 2018, { type: "numeric" }),
      ],
    });
    // Both bindings land on ONE source (keyed on the variant), not two.
    expect(projectStore.draft?.sources).toHaveLength(1);
    expect(projectStore.draft?.sources[0].register_variant).toBe("scb/lisa/v1");
    expect(
      projectStore.draft?.sources[0].bindings.map((b) => b.variable),
    ).toEqual(["scb/lisa/kon", "scb/lisa/alder"]);
    // The #312 name prefill fired on the created source.
    expect(projectStore.draft?.sources[0].name).toBe("LISA");
  });

  it("commits the write-once final fields verbatim (type + display_name + representation)", () => {
    projectStore.newProject(SEED);
    projectStore.applyStagedDiff({
      adds: [
        add("scb/lisa/v1", "scb/lisa/ssyk", 2018, {
          type: "categorical",
          display_name: "Ssyk3",
          representation: "Ssyk3",
        }),
      ],
    });
    const binding = projectStore.draft?.sources[0].bindings[0];
    expect(binding).toMatchObject({
      variable: "scb/lisa/ssyk",
      type: "categorical",
      display_name: "Ssyk3",
      representation: "Ssyk3",
    });
  });

  it("merges a disjoint year window into the existing source's period (coalesce, sorted, disjoint)", () => {
    projectStore.newProject(SEED);
    projectStore.applyStagedDiff({
      adds: [
        add("scb/lisa/v1", "scb/lisa/kon", { from: 2015, to: 2020 }),
        // A later disjoint window EXTENDS the source period to the #307 list form.
        add("scb/lisa/v1", "scb/lisa/alder", { from: 2005, to: 2010 }),
      ],
    });
    expect(projectStore.draft?.sources).toHaveLength(1);
    // Sorted ascending, non-overlapping — the earlier window sorts first.
    expect(projectStore.draft?.sources[0].period).toEqual([
      { from: 2005, to: 2010 },
      { from: 2015, to: 2020 },
    ]);
  });

  it("adjacency-merges touching year windows into one span", () => {
    projectStore.newProject(SEED);
    projectStore.applyStagedDiff({
      adds: [
        add("scb/lisa/v1", "scb/lisa/kon", { from: 2010, to: 2011 }),
        add("scb/lisa/v1", "scb/lisa/alder", { from: 2012, to: 2013 }),
      ],
    });
    // 2010..2011 + 2012..2013 have a 0-year gap → fuse into one 2010..2013 span.
    expect(projectStore.draft?.sources[0].period).toEqual({
      from: 2010,
      to: 2013,
    });
  });

  it("REPLACES a token-grammar period wholesale (a mixed-grain sort is undefined)", () => {
    projectStore.newProject(SEED);
    projectStore.applyStagedDiff({
      adds: [add("scb/hst/v1", "scb/hst/kon", "HT2020")],
    });
    // A second add with a DIFFERENT token replaces (never coalesces) the period.
    projectStore.applyStagedDiff({
      adds: [add("scb/hst/v1", "scb/hst/alder", "VT2021")],
    });
    expect(projectStore.draft?.sources[0].period).toBe("VT2021");
  });

  it("removes drop matching bindings and prune sources left empty", () => {
    projectStore.newProject(SEED);
    projectStore.applyStagedDiff({
      adds: [
        add("scb/lisa/v1", "scb/lisa/kon", 2018),
        add("scb/lisa/v1", "scb/lisa/alder", 2018),
        add("scb/rtb/v1", "scb/rtb/fodelsear", 2018),
      ],
    });
    expect(projectStore.draft?.sources).toHaveLength(2);
    // Remove one binding of LISA (source survives) + the sole RTB binding (source
    // pruned).
    projectStore.applyStagedDiff({
      removes: [
        { registerVariant: "scb/lisa/v1", variable: "scb/lisa/kon" },
        { registerVariant: "scb/rtb/v1", variable: "scb/rtb/fodelsear" },
      ],
    });
    expect(projectStore.draft?.sources).toHaveLength(1);
    expect(projectStore.draft?.sources[0].register_variant).toBe("scb/lisa/v1");
    expect(
      projectStore.draft?.sources[0].bindings.map((b) => b.variable),
    ).toEqual(["scb/lisa/alder"]);
  });

  it("remove+add of the SAME register_variant in one batch preserves the source (name + merged period, not a fresh source) — review Fix 2", () => {
    projectStore.newProject(SEED);
    // Seed a source with a single binding at 2015..2020, then give it a user-set name
    // (so we can prove the source object survives the swap, not just its coordinate).
    projectStore.applyStagedDiff({
      adds: [
        add(
          "scb/lisa/v1",
          "scb/lisa/ssyk",
          { from: 2015, to: 2020 },
          {
            representation: "Ssyk3",
          },
        ),
      ],
    });
    // Set a user name via the sources mutator (the store's public edit path).
    const named = (projectStore.draft?.sources ?? []).map((s, i) =>
      i === 0 ? { ...s, name: "My cohort" } : s,
    );
    projectStore.updateField("sources", named);
    expect(projectStore.draft?.sources[0].name).toBe("My cohort");

    // ONE batch that removes the source's ONLY binding AND adds a binding for the
    // SAME register_variant (a representation swap). Pre-fix the removes phase would
    // prune the emptied source immediately, so the add would mint a FRESH source and
    // LOSE the user's name + the source's existing period. The deferred prune keeps
    // the source: the add refills it.
    projectStore.applyStagedDiff({
      removes: [
        {
          registerVariant: "scb/lisa/v1",
          variable: "scb/lisa/ssyk",
          representation: "Ssyk3",
        },
      ],
      adds: [
        add(
          "scb/lisa/v1",
          "scb/lisa/ssyk",
          { from: 2005, to: 2010 },
          {
            representation: "Ssyk4",
          },
        ),
      ],
    });

    // Still ONE source, and it is the SAME source (user name preserved).
    expect(projectStore.draft?.sources).toHaveLength(1);
    expect(projectStore.draft?.sources[0].name).toBe("My cohort");
    // Its period MERGED the add's disjoint window into the pre-existing one (the
    // find-or-create found the still-present source), rather than resetting to just
    // the add's window (which a fresh newSource would have done).
    expect(projectStore.draft?.sources[0].period).toEqual([
      { from: 2005, to: 2010 },
      { from: 2015, to: 2020 },
    ]);
    // The old binding is gone, the new one landed.
    expect(
      projectStore.draft?.sources[0].bindings.map((b) => b.representation),
    ).toEqual(["Ssyk4"]);
  });

  it("a remove-only batch that empties a source still prunes it (review Fix 2)", () => {
    projectStore.newProject(SEED);
    projectStore.applyStagedDiff({
      adds: [add("scb/lisa/v1", "scb/lisa/kon", 2018)],
    });
    expect(projectStore.draft?.sources).toHaveLength(1);
    // Removing the sole binding with NO offsetting add prunes the emptied source.
    projectStore.applyStagedDiff({
      removes: [{ registerVariant: "scb/lisa/v1", variable: "scb/lisa/kon" }],
    });
    expect(projectStore.draft?.sources).toHaveLength(0);
  });

  it("a null-representation remove matches the variable's binding regardless of stored column", () => {
    projectStore.newProject(SEED);
    projectStore.applyStagedDiff({
      adds: [
        add("scb/lisa/v1", "scb/lisa/ssyk", 2018, { representation: "Ssyk3" }),
      ],
    });
    // The remove carries NO representation → the null-either-side rule matches the
    // stored Ssyk3 binding and drops it (pruning the emptied source).
    projectStore.applyStagedDiff({
      removes: [{ registerVariant: "scb/lisa/v1", variable: "scb/lisa/ssyk" }],
    });
    expect(projectStore.draft?.sources).toHaveLength(0);
  });

  it("periodChange replaces the matching source's period wholesale", () => {
    projectStore.newProject(SEED);
    projectStore.applyStagedDiff({
      adds: [add("scb/lisa/v1", "scb/lisa/kon", 2018)],
    });
    projectStore.applyStagedDiff({
      periodChange: [
        { registerVariant: "scb/lisa/v1", period: { from: 2010, to: 2020 } },
      ],
    });
    expect(projectStore.draft?.sources[0].period).toEqual({
      from: 2010,
      to: 2020,
    });
  });

  it("commits the whole batch in ONE mutation (id mirror rebuilt once, autosave fires once)", async () => {
    vi.useFakeTimers();
    const saves: unknown[] = [];
    setPersistence({
      save: (_k, d) => {
        saves.push(d);
        return Promise.resolve();
      },
      load: () => Promise.resolve(null),
    });
    projectStore.newProject(SEED);
    const stop = $effect.root(() => {
      initPersistence();
    });
    await vi.advanceTimersByTimeAsync(600);
    saves.length = 0; // ignore the newProject autosave

    projectStore.applyStagedDiff({
      adds: [
        add("scb/lisa/v1", "scb/lisa/kon", 2018),
        add("scb/rtb/v1", "scb/rtb/fodelsear", 2018),
      ],
    });
    // Both sources exist with fresh, distinct stable ids (the mirror rebuilt once).
    expect(projectStore.sourceId(0)).toMatch(/^c\d+$/);
    expect(projectStore.sourceId(1)).toMatch(/^c\d+$/);
    expect(projectStore.sourceId(0)).not.toBe(projectStore.sourceId(1));
    expect(projectStore.bindingId(0, 0)).toBeTruthy();

    // The debounced autosave collapses the single batch mutation into ONE write.
    await vi.advanceTimersByTimeAsync(600);
    expect(saves).toHaveLength(1);
    vi.useRealTimers();
    stop();
  });

  it("an empty diff preserves the draft content (no throw)", () => {
    projectStore.newProject(SEED);
    projectStore.applyStagedDiff({
      adds: [add("scb/lisa/v1", "scb/lisa/kon", 2018)],
    });
    const before = structuredClone($state.snapshot(projectStore.draft));
    // An empty diff commits a fresh draft object but leaves the content identical.
    projectStore.applyStagedDiff({});
    expect($state.snapshot(projectStore.draft)).toEqual(before);
  });
});

describe("stable client-side ids (issue #200)", () => {
  // A 3-source draft, each with 2 bindings, so a MIDDLE remove is meaningful. Built
  // through the atomic staged-diff commit path (the store's structural entry point).
  function seedThreeSources(): void {
    projectStore.newProject(SEED);
    const adds: StagedAdd[] = [];
    for (let s = 0; s < 3; s++) {
      adds.push(add(`scb/r${s}/v1`, `s${s}b0`, 2018));
      adds.push(add(`scb/r${s}/v1`, `s${s}b1`, 2018));
    }
    projectStore.applyStagedDiff({ adds });
  }

  it("keeps a survivor's id stable across a MIDDLE source remove (no rebind to a shifted item)", () => {
    seedThreeSources();
    const id0 = projectStore.sourceId(0);
    const id2 = projectStore.sourceId(2);
    // Remove the middle source (drop both its bindings → the source is pruned). The
    // last source shifts down to index 1.
    projectStore.removeSource(1);
    expect(projectStore.draft?.sources?.[1]?.register_variant).toBe(
      "scb/r2/v1",
    );
    // The shifted survivor MUST carry its own id (id2), not the index-1 id it now
    // sits at — that stability is what remounts the right component instance.
    expect(projectStore.sourceId(0)).toBe(id0);
    expect(projectStore.sourceId(1)).toBe(id2);
  });

  it("keeps a survivor binding's id stable across a MIDDLE binding remove", () => {
    projectStore.newProject(SEED);
    projectStore.applyStagedDiff({
      adds: [
        add("scb/lisa/v1", "b0", 2018),
        add("scb/lisa/v1", "b1", 2018),
        add("scb/lisa/v1", "b2", 2018),
      ],
    });
    const b0 = projectStore.bindingId(0, 0);
    const b2 = projectStore.bindingId(0, 2);
    projectStore.removeBinding(0, 1);
    expect(projectStore.bindingId(0, 0)).toBe(b0);
    expect(projectStore.bindingId(0, 1)).toBe(b2);
  });

  it("seeds ids for an OPENED file's sources + bindings", async () => {
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
          bindings: [
            { variable: "scb/lisa/kon", type: "categorical" },
            { variable: "scb/lisa/alder", type: "numeric" },
          ],
        },
      ],
    };
    await projectStore.openFromFile(jsonFile(JSON.stringify(raw)));
    // Distinct, defined ids for the opened source + its two bindings.
    expect(projectStore.sourceId(0)).toBeTruthy();
    expect(projectStore.bindingId(0, 0)).toBeTruthy();
    expect(projectStore.bindingId(0, 0)).not.toBe(projectStore.bindingId(0, 1));
  });

  describe("malformed drafts do not corrupt the mirror or the store state (review #280)", () => {
    it("opens a draft with a null sources ELEMENT cleanly (no throw, consistent mirror)", async () => {
      // A null/undefined source element must not throw in buildIds — and because the
      // replacement is atomic, the open must land clean (no stale validatedClean from
      // a previous document, no unhandled rejection, openError null on success).
      const raw = {
        schema_version: "2.0.0",
        steward: "global",
        reg_meta_version: "reg_meta/v1.0.0",
        name: "has-null-source",
        sources: [
          {
            name: "ok",
            register_variant: "scb/lisa/v1",
            period: 2018,
            bindings: [],
          },
          null,
          {
            name: "ok2",
            register_variant: "scb/lisa/v1",
            period: 2019,
            bindings: [],
          },
        ],
      };
      // Seed a DIFFERENT prior document first so a mid-update abort would surface as
      // stale state belonging to it.
      projectStore.newProject(SEED);
      projectStore.updateField("name", "prior");

      await expect(
        projectStore.openFromFile(jsonFile(JSON.stringify(raw))),
      ).resolves.toBeUndefined();

      // Clean open: the malformed-but-loadable draft is in, error channels are clear.
      expect(projectStore.openError).toBeNull();
      expect(projectStore.requestError).toBeNull();
      expect(projectStore.draft?.name).toBe("has-null-source");
      // A fresh open is not pre-validated → downloads gated closed (no stale state).
      expect(projectStore.validation).toBeNull();
      expect(projectStore.validatedClean).toBe(false);
      // The mirror mirrors the 3-element sources array (the null slot gets its own id
      // with an empty bindings list — no throw, no divergence).
      expect(projectStore.sourceId(0)).toBeTruthy();
      expect(projectStore.sourceId(1)).toBeTruthy();
      expect(projectStore.sourceId(2)).toBeTruthy();
      expect(projectStore.sourceId(0)).not.toBe(projectStore.sourceId(1));
    });

    it("updateField('sources', …) rebuilds the mirror so it can't desync (review #280)", () => {
      projectStore.newProject(SEED);
      projectStore.applyStagedDiff({
        adds: [add("scb/lisa/v1", "a", 2018), add("scb/rtb/v1", "b", 2018)],
      });
      const beforeId0 = projectStore.sourceId(0);
      // A wholesale `sources` replacement via updateField must rebuild the mirror to
      // the NEW array's shape (here: shrink 2 → 1), not keep the stale 2-entry mirror.
      projectStore.updateField("sources", [
        { name: "only", register_variant: "", period: "", bindings: [] },
      ]);
      expect((projectStore.draft?.sources as unknown[]).length).toBe(1);
      expect(projectStore.sourceId(0)).toBeTruthy();
      // The rebuilt mirror has exactly one entry → index 1 falls back to the index.
      expect(projectStore.sourceId(1)).toBe("i1");
      // It's a genuine rebuild (fresh id), not the pre-replacement id.
      expect(projectStore.sourceId(0)).not.toBe(beforeId0);
    });
  });

  describe("ids NEVER leak into the serialized draft / POST bodies (the closed-object constraint)", () => {
    // Source/Binding are extra=forbid in reg_schema — an injected id would both trip
    // `unexpected_field` AND end up in the downloaded project_data.json. The ids live
    // only in the store, so neither the serialized text nor any POST body may carry
    // a `_uid`/`_id`/client `c<n>` key.

    function assertNoIdLeak(payload: unknown): void {
      const text = JSON.stringify(payload);
      expect(text).not.toMatch(/"_uid"|"_id"|"_clientId"/);
      // The opaque client ids are `c<n>` strings; assert none appear as a value
      // anywhere in the wire payload either.
      expect(text).not.toMatch(/"c\d+"/);
    }

    it("serializeProjectData output carries no client id", () => {
      seedThreeSources();
      // Sanity: ids exist in the store…
      expect(projectStore.sourceId(0)).toMatch(/^c\d+$/);
      // …but never in the serialized draft (the downloaded file / dirty baseline).
      const draft = projectStore.draft as unknown;
      assertNoIdLeak(draft);
    });

    it("the /validate POST body carries no client id", async () => {
      const bodies: unknown[] = [];
      stubFetch(async (_url, init) => {
        if (init?.body != null) {
          bodies.push(JSON.parse(init.body as string));
        }
        return {
          ok: true,
          status: 200,
          json: async () => ({ ok: true, issues: [] }),
        };
      });
      seedThreeSources();
      await projectStore.validate();
      expect(bodies).toHaveLength(1);
      assertNoIdLeak(bodies[0]);
      // The real source/binding content IS there (not an empty body that also passes).
      const sent = bodies[0] as { sources: { register_variant: string }[] };
      expect(sent.sources.map((s) => s.register_variant)).toEqual([
        "scb/r0/v1",
        "scb/r1/v1",
        "scb/r2/v1",
      ]);
    });

    it("the /order POST body carries no client id", async () => {
      const bodies: unknown[] = [];
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
      stubFetch(async (_url, init) => {
        if (init?.body != null) {
          bodies.push(JSON.parse(init.body as string));
        }
        return {
          ok: true,
          status: 200,
          blob: async () => new Blob(["x"]),
          headers: new Headers(),
        };
      });
      seedThreeSources();
      await projectStore.downloadOrder();
      expect(bodies.length).toBeGreaterThanOrEqual(1);
      for (const body of bodies) {
        assertNoIdLeak(body);
      }
    });
  });
});
