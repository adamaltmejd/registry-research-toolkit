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

describe("stable client-side ids (issue #200)", () => {
  // A 3-source draft, each with 2 bindings, so a MIDDLE remove is meaningful.
  function seedThreeSources(): void {
    projectStore.newProject(SEED);
    for (let s = 0; s < 3; s++) {
      projectStore.addSource();
      projectStore.updateSource(s, { name: `s${s}` });
      projectStore.addBinding(s);
      projectStore.addBinding(s);
      projectStore.updateBinding(s, 0, { variable: `s${s}b0` });
      projectStore.updateBinding(s, 1, { variable: `s${s}b1` });
    }
  }

  it("keeps a survivor's id stable across a MIDDLE source remove (no rebind to a shifted item)", () => {
    seedThreeSources();
    const id0 = projectStore.sourceId(0);
    const id2 = projectStore.sourceId(2);
    // Remove the middle source. The last source shifts down to index 1.
    projectStore.removeSource(1);
    expect(projectStore.draft?.sources?.[1]?.name).toBe("s2");
    // The shifted survivor MUST carry its own id (id2), not the index-1 id it now
    // sits at — that stability is what remounts the right component instance.
    expect(projectStore.sourceId(0)).toBe(id0);
    expect(projectStore.sourceId(1)).toBe(id2);
  });

  it("keeps a survivor binding's id stable across a MIDDLE binding remove", () => {
    projectStore.newProject(SEED);
    projectStore.addSource();
    projectStore.addBinding(0);
    projectStore.addBinding(0);
    projectStore.addBinding(0);
    const b0 = projectStore.bindingId(0, 0);
    const b2 = projectStore.bindingId(0, 2);
    projectStore.removeBinding(0, 1);
    expect(projectStore.bindingId(0, 0)).toBe(b0);
    expect(projectStore.bindingId(0, 1)).toBe(b2);
  });

  it("leaves a survivor's id UNCHANGED across an unrelated edit (one stable identity per item)", () => {
    seedThreeSources();
    const id1 = projectStore.sourceId(1);
    const b1 = projectStore.bindingId(1, 1);
    // An immutable edit replaces the source/binding object but must NOT churn its id.
    projectStore.updateSource(0, { name: "renamed" });
    projectStore.updateBinding(1, 1, { type: "categorical" });
    expect(projectStore.sourceId(1)).toBe(id1);
    expect(projectStore.bindingId(1, 1)).toBe(b1);
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

    it("addSource on a malformed non-array `sources` coerces to [] (no char-spread, mirror stays consistent)", async () => {
      // The supported malformed fixture: `sources: "not-an-array"`. addSource must
      // NOT spread the string into 13 single-char "sources"; it coerces to [] then
      // appends one — and the store mirror appends exactly one id to match.
      const raw = {
        schema_version: "2.0.0",
        steward: "global",
        reg_meta_version: "reg_meta/v1.0.0",
        name: "malformed-sources",
        sources: "not-an-array",
      };
      await projectStore.openFromFile(jsonFile(JSON.stringify(raw)));
      // The malformed value is loaded verbatim (the SPA is not the validator).
      expect(projectStore.draft?.sources as unknown).toBe("not-an-array");

      projectStore.addSource();
      // Coerced: exactly ONE well-formed source now (not 14 char-sources).
      const sources = projectStore.draft?.sources as unknown;
      expect(Array.isArray(sources)).toBe(true);
      expect((sources as unknown[]).length).toBe(1);
      // The mirror matches: one source id, distinct from the index fallback.
      expect(projectStore.sourceId(0)).toMatch(/^c\d+$/);
      expect(projectStore.sourceId(1)).toBe("i1"); // out of range → index fallback
    });

    it("updateField('sources', …) rebuilds the mirror so it can't desync (review #280)", () => {
      projectStore.newProject(SEED);
      projectStore.addSource();
      projectStore.addSource();
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
      const sent = bodies[0] as { sources: { name: string }[] };
      expect(sent.sources.map((s) => s.name)).toEqual(["s0", "s1", "s2"]);
    });

    it("the /order + /bundle POST bodies carry no client id", async () => {
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
      await projectStore.downloadBundleFile();
      expect(bodies.length).toBeGreaterThanOrEqual(2);
      for (const body of bodies) {
        assertNoIdLeak(body);
      }
    });
  });
});

describe("source-name prefill on register_variant change (#312)", () => {
  it("prefills the name from the register slug when the variant is set", () => {
    projectStore.newProject(SEED);
    projectStore.addSource();
    projectStore.updateSource(0, { register_variant: "scb/lisa/v1" });
    expect(projectStore.draft?.sources[0]?.name).toBe("LISA");
  });

  it("follows a variant change while the name is still the prefill", () => {
    projectStore.newProject(SEED);
    projectStore.addSource();
    projectStore.updateSource(0, { register_variant: "scb/lisa/v1" });
    projectStore.updateSource(0, { register_variant: "scb/rtb/v1" });
    expect(projectStore.draft?.sources[0]?.name).toBe("RTB");
  });

  it("clears a prefilled name when the variant loses its register segment", () => {
    projectStore.newProject(SEED);
    projectStore.addSource();
    projectStore.updateSource(0, { register_variant: "scb/lisa/v1" });
    projectStore.updateSource(0, { register_variant: "scb" });
    expect(projectStore.draft?.sources[0]?.name).toBe("");
  });

  it("never clobbers a user-entered name", () => {
    projectStore.newProject(SEED);
    projectStore.addSource();
    projectStore.updateSource(0, { name: "my handle" });
    projectStore.updateSource(0, { register_variant: "scb/lisa/v1" });
    expect(projectStore.draft?.sources[0]?.name).toBe("my handle");
  });

  it("an explicit name in the same patch wins over the prefill", () => {
    projectStore.newProject(SEED);
    projectStore.addSource();
    projectStore.updateSource(0, {
      register_variant: "scb/lisa/v1",
      name: "explicit",
    });
    expect(projectStore.draft?.sources[0]?.name).toBe("explicit");
  });

  it("suffixes _2 when another source already took the default", () => {
    projectStore.newProject(SEED);
    projectStore.addSource();
    projectStore.updateSource(0, { register_variant: "scb/lisa/v1" });
    projectStore.addSource();
    projectStore.updateSource(1, { register_variant: "scb/lisa/v2" });
    expect(projectStore.draft?.sources[0]?.name).toBe("LISA");
    expect(projectStore.draft?.sources[1]?.name).toBe("LISA_2");
  });
});

describe("source-name prefill no-op guard (#312, Codex P2)", () => {
  it("re-applying the SAME variant does not recompute a suffixed name", () => {
    projectStore.newProject(SEED);
    projectStore.addSource();
    projectStore.updateSource(0, { register_variant: "scb/lisa/v1" }); // LISA
    projectStore.addSource();
    projectStore.updateSource(1, { register_variant: "scb/lisa/v2" }); // LISA_2
    projectStore.addSource();
    projectStore.updateSource(2, { register_variant: "scb/lisa/v3" }); // LISA_3
    projectStore.removeSource(1); // frees LISA_2
    // Re-picking the same variant on the (now index-1) LISA_3 source must NOT
    // rename it to the freed LISA_2 slot.
    projectStore.updateSource(1, { register_variant: "scb/lisa/v3" });
    expect(projectStore.draft?.sources[1]?.name).toBe("LISA_3");
  });
});
