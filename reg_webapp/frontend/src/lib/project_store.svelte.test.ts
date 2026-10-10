import { afterEach, describe, expect, it, vi } from "vitest";
import {
  initDraftLifecycle,
  type ProjectPersistence,
  projectStore,
  type StagedAdd,
  setPersistence,
  storeSchemaVersion,
} from "./project_store.svelte";
import {
  add,
  openFile,
  SEED,
  storedProject,
} from "./project-store-test-helpers";
import { projectSchemaVersion } from "./reg_core";

// The store is a MODULE SINGLETON — each test must establish the state it needs
// (via newProject / openFile) rather than assume a fresh store.

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

describe("newProject", () => {
  it("round-trips a fresh current pre-v1 reg_meta seed through an open", async () => {
    projectStore.newProject({
      reg_meta_version: "reg_meta/v0.34.0",
      steward: "global",
    });
    const raw = JSON.stringify(projectStore.draft);

    openFile(raw);

    expect(projectStore.openError).toBeNull();
    expect(projectStore.draft?.schema_version).toBe(projectSchemaVersion());
    expect(projectStore.draft?.reg_meta_version).toBe("reg_meta/v0.34.0");
    expect(projectStore.dirty).toBe(false);
  });
});

describe("dirty flag", () => {
  it("an edit clears a GREEN validation (validatedClean goes false)", async () => {
    stubFetch(async () => ({
      ok: true,
      status: 200,
      json: async () => ({ data: { ok: true, issues: [] }, meta: {} }),
    }));
    projectStore.newProject(SEED);
    // Establish a REAL green validation first — otherwise the assertion is
    // vacuous (newProject already nulls validation).
    await projectStore.validate();
    expect(projectStore.validation?.ok).toBe(true);
    expect(projectStore.validatedClean).toBe(true);
    expect(projectStore.validationStatus).toBe("ok");
    expect(projectStore.canDownloadOrder).toBe(true);
    // A staged-diff edit must invalidate it so the order download gate re-closes.
    projectStore.applyStagedDiff({
      adds: [add("scb/lisa/v1", "scb/lisa/kon", 2018)],
    });
    expect(projectStore.validation).toBeNull();
    expect(projectStore.validatedClean).toBe(false);
    expect(projectStore.validationStatus).toBe("unchecked");
    expect(projectStore.canDownloadOrder).toBe(false);
  });
});

describe("the file-open ingress + commit", () => {
  // Fails when the open normalises the draft (dropping unknown keys) or a draft
  // reg-core rejects is still POSTed instead of carrying its own issues.
  it("loads a malformed file VERBATIM and reports its issues without a request", async () => {
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
    const raw = {
      schema_version: projectSchemaVersion(),
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
      typo_object: { nested: true },
      typo_scalar: 7,
    };
    openFile(JSON.stringify(raw));
    expect(projectStore.openError).toBeNull();
    expect(projectStore.draft?.name).toBe("opened");
    // Invalid values + known panels survive on the raw draft.
    const draft = projectStore.draft as Record<string, unknown>;
    expect(draft.panels).toEqual(raw.panels);
    expect(draft.typo_object).toEqual(raw.typo_object);
    expect(draft.typo_scalar).toBe(7);
    expect(projectStore.validation?.ok).toBe(false);
    expect(
      projectStore.validation?.issues.map((i) => [i.code, i.path]),
    ).toEqual([
      ["unexpected_field", "/typo_object"],
      ["unexpected_field", "/typo_scalar"],
    ]);
    expect(projectStore.validationStatus).toBe("errors");
    await projectStore.validate();
    expect(fetchSpy).not.toHaveBeenCalled();
    // A freshly-opened draft is clean.
    expect(projectStore.dirty).toBe(false);
  });
});

describe("validate (200 ok:false vs 4xx split + stale-response guard)", () => {
  it("clears a previous green result when the current draft's recheck request fails", async () => {
    const responses = [
      {
        ok: true,
        status: 200,
        json: async () => ({ data: { ok: true, issues: [] }, meta: {} }),
      },
      {
        ok: false,
        status: 400,
        json: async () => ({
          error: {
            code: "malformed_request",
            message: "request body is not a JSON object",
          },
          meta: {},
        }),
      },
    ];
    stubFetch(async () => responses.shift() ?? responses.at(-1));
    projectStore.newProject(SEED);

    await projectStore.validate();
    expect(projectStore.validationStatus).toBe("ok");
    expect(projectStore.canDownloadOrder).toBe(true);

    const r = await projectStore.validate();

    expect(r).toBeNull();
    expect(projectStore.requestError).toBe("request body is not a JSON object");
    expect(projectStore.validation).toBeNull();
    expect(projectStore.validationStatus).toBe("unchecked");
    expect(projectStore.canDownloadOrder).toBe(false);
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
      json: async () => ({ data: { ok: true, issues: [] }, meta: {} }),
    });
    await pending;
    expect(projectStore.validation).toBeNull();
    expect(projectStore.validatedClean).toBe(false);
    expect(projectStore.canDownloadOrder).toBe(false);
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
      initDraftLifecycle();
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
});

describe("stable client-side ids (issue #200)", () => {
  // A 3-source draft, each with 2 bindings, so a MIDDLE remove is meaningful. Built
  // through the atomic staged-diff commit path (the store's structural entry point).
  function seedThreeSources(): void {
    projectStore.newProject(SEED);
    const adds: StagedAdd[] = [];
    for (let s = 0; s < 3; s++) {
      // Well-formed FQIDs: a draft reg-core rejects is never POSTed.
      adds.push(add(`scb/r${s}/v1`, `scb/r${s}/s${s}b0`, 2018));
      adds.push(add(`scb/r${s}/v1`, `scb/r${s}/s${s}b1`, 2018));
    }
    projectStore.applyStagedDiff({ adds });
  }

  describe("malformed drafts do not corrupt the mirror or the store state (review #280)", () => {
    it("applyStagedDiff does NOT throw on a null sources SLOT (issue #1099 — the cart stays usable)", async () => {
      // The render fix (#1099) lets a `sources: [null, …]` draft LOAD + render, but the
      // store's commit path (applyStagedDiff) iterates EVERY source unconditionally in
      // its prune/find-or-create steps. Any catalog add/remove — even one targeting a
      // DIFFERENT register_variant than the null slot — must not collaterally throw on
      // the malformed slot, and must leave that slot VERBATIM (the load contract).
      const raw = {
        schema_version: "2.0.0",
        steward: "global",
        reg_meta_version: "reg_meta/v1.0.0",
        name: "has-null-source",
        sources: [
          null,
          {
            name: "lisa",
            register_variant: "scb/lisa/v1",
            period: 2020,
            bindings: [{ variable: "scb/lisa/kon", type: "categorical" }],
          },
          {
            name: "rtb",
            register_variant: "scb/rtb/v1",
            period: 2018,
            bindings: [{ variable: "scb/rtb/fodelsear", type: "categorical" }],
          },
        ],
      };
      openFile(JSON.stringify(raw));
      expect(projectStore.openError).toBeNull();

      // A batch that never NAMES the null slot's (empty) register_variant: add a new
      // variant, prune an unrelated one (rtb), and repin the sibling's (lisa) period.
      // Each step iterates the null slot — none may throw.
      expect(() =>
        projectStore.applyStagedDiff({
          adds: [add("scb/hst/v1", "scb/hst/alder", 2019, { type: "numeric" })],
          removes: [
            { registerVariant: "scb/rtb/v1", variable: "scb/rtb/fodelsear" },
          ],
          periodChange: [
            {
              sourceName: "lisa",
              registerVariant: "scb/lisa/v1",
              period: { from: 2010, to: 2020 },
            },
          ],
        }),
      ).not.toThrow();

      const sources = storedProject()?.sources as unknown[];
      // The null slot survives VERBATIM at its original index (load contract holds).
      expect(sources[0]).toBeNull();
      // The intended mutations all applied: rtb pruned, hst added, lisa's period repinned.
      const byVariant = new Map(
        (storedProject()?.sources ?? [])
          .filter((s): s is NonNullable<typeof s> => s != null)
          .map((s) => [s.register_variant, s]),
      );
      expect(byVariant.has("scb/rtb/v1")).toBe(false);
      expect(byVariant.get("scb/lisa/v1")?.period).toEqual({
        from: 2010,
        to: 2020,
      });
      expect(
        byVariant.get("scb/lisa/v1")?.bindings.map((b) => b.variable),
      ).toEqual(["scb/lisa/kon"]);
      expect(
        byVariant.get("scb/hst/v1")?.bindings.map((b) => b.variable),
      ).toEqual(["scb/hst/alder"]);
    });
  });

  describe("ids NEVER leak into the serialized draft / POST bodies (the closed-object constraint)", () => {
    // Source/Binding are closed objects in reg-core — an injected id would both trip
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

    it("the /validate POST body carries no client id", async () => {
      const bodies: unknown[] = [];
      stubFetch(async (_url, init) => {
        if (init?.body != null) {
          bodies.push(JSON.parse(init.body as string));
        }
        return {
          ok: true,
          status: 200,
          json: async () => ({ data: { ok: true, issues: [] }, meta: {} }),
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

describe("a blocked order", () => {
  it("closes the download gate on the 422 so the CTA can't repeat a request that cannot succeed", async () => {
    // The materializer fail-closes projects that VALIDATE clean (a steward-
    // provenance mismatch, an uncovered period), so a green validation is not
    // enough to keep the accent CTA enabled once /order has said no.
    stubFetch(async (url) =>
      String(url).includes("/project/order")
        ? {
            ok: false,
            status: 422,
            json: async () => ({
              error: {
                code: "order_blocked",
                message: "order blocked by 1 finding: steward_mismatch: …",
                fields: {
                  findings: [
                    {
                      code: "steward_mismatch",
                      message: "this project belongs to another deployment",
                      source: null,
                      variable: null,
                      period: null,
                    },
                  ],
                },
              },
              meta: {},
            }),
            headers: new Headers(),
          }
        : {
            ok: true,
            status: 200,
            json: async () => ({ data: { ok: true, issues: [] }, meta: {} }),
          },
    );
    projectStore.newProject(SEED);
    await projectStore.validate();
    expect(projectStore.canDownloadOrder).toBe(true);

    await projectStore.downloadOrder();

    expect(projectStore.requestError).toContain("steward_mismatch");
    expect(projectStore.canDownloadOrder).toBe(false);
    // …and the banner knows WHICH request said no, so it never offers to retry
    // the validation instead (that would clear the block — see the panel).
    expect(projectStore.requestErrorSource).toBe("order");
    // The last /validate result is untouched — the block is an ORDER verdict,
    // not a validation one; the panel just stops announcing it (see
    // ValidationPanel: the summary yields to a standing request error).
    expect(projectStore.validation?.ok).toBe(true);

    // Re-validating (which any edit triggers) clears the error and reopens the
    // gate — the block is not sticky past a change.
    await projectStore.validate();
    expect(projectStore.requestError).toBeNull();
    expect(projectStore.requestErrorSource).toBeNull();
    expect(projectStore.canDownloadOrder).toBe(true);
  });

  it("keeps the findings as DATA, and moves them with the banner", async () => {
    // The 422 carries typed findings (code + coordinates), which the panel
    // renders one by one. They are half of the same channel as `requestError`:
    // findings under no banner (or a banner over stale findings) would each
    // misreport the request.
    stubFetch(async (url) =>
      String(url).includes("/project/order")
        ? {
            ok: false,
            status: 422,
            json: async () => ({
              error: {
                code: "order_blocked",
                message: "order blocked by 2 findings: …",
                fields: {
                  findings: [
                    {
                      code: "variable_unresolved",
                      message: "scb/lisa/ghostvar does not resolve",
                      source: "lisa",
                      variable: "scb/lisa/ghostvar",
                      period: null,
                    },
                    {
                      code: "coverage_gap",
                      message: "the delivery does not cover 2019",
                      source: "lisa",
                      variable: "scb/lisa/kon",
                      period: "2019",
                    },
                  ],
                },
              },
              meta: {},
            }),
            headers: new Headers(),
          }
        : {
            ok: true,
            status: 200,
            json: async () => ({ data: { ok: true, issues: [] }, meta: {} }),
          },
    );
    projectStore.newProject(SEED);
    await projectStore.validate();
    await projectStore.downloadOrder();

    expect(projectStore.orderFindings.map((f) => f.code)).toEqual([
      "variable_unresolved",
      "coverage_gap",
    ]);
    // The coordinates survive as fields — the panel locates a card by them.
    expect(projectStore.orderFindings[1]).toMatchObject({
      source: "lisa",
      variable: "scb/lisa/kon",
      period: "2019",
    });

    await projectStore.validate();
    expect(projectStore.orderFindings).toEqual([]);
  });

  it("discards a blocked-order 422 that lands after a mid-flight draft edit", async () => {
    // A block belongs to the draft that was POSTed. If the researcher edits while
    // the request is in flight, the 422's findings name sources and variables the
    // current draft may no longer have — the panel would locate them against cards
    // that moved, and the gate would close over a draft nothing has judged. Same
    // generation guard the auto-validate path uses (#994).
    let resolveFetch: (v: unknown) => void = () => {};
    stubFetch(
      () =>
        new Promise((res) => {
          resolveFetch = res;
        }),
    );
    projectStore.newProject(SEED);
    const pending = projectStore.downloadOrder();
    // Edit mid-flight → setDraft swaps the draft object + bumps the generation.
    projectStore.updateField("name", "edited mid-flight");

    resolveFetch({
      ok: false,
      status: 422,
      json: async () => ({
        error: {
          code: "order_blocked",
          message: "order blocked by 1 finding: …",
          fields: {
            findings: [
              {
                code: "variable_unresolved",
                message: "scb/lisa/ghostvar does not resolve",
                source: "lisa",
                variable: "scb/lisa/ghostvar",
                period: null,
              },
            ],
          },
        },
        meta: {},
      }),
      headers: new Headers(),
    });
    await pending;

    expect(projectStore.requestError).toBeNull();
    expect(projectStore.orderFindings).toEqual([]);
    expect(projectStore.requestErrorSource).toBeNull();
  });
});
